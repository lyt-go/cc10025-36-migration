"""hot_prefetch 领域服务：预热回迁管理，接收 tier_policy 和 cold_storage 上游事件。

跨模块数据流：
    tier_policy → hot_prefetch: COLD→HOT策略变更触发预热回迁
    cold_storage → hot_prefetch: 冷存完成后检查是否需要预热
    hot_prefetch → cost_tracker: 回迁任务更新成本追踪

读取定位：
    发起回迁时从 cold_storage 的冷存任务版本记录定位实际有资格读取的
    冷存源数据；就绪状态、校验摘要、读取地址取自同一条版本记录，不跨
    记录拼接。切换期间（旧版本已校验、不同版本迁移排队写新校验）提交
    预热会被标明「等待」；找不到具备资格的版本（含缺少任务版本的旧
    数据）会被标明「拒绝」，两者均回写证据。
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "assets")


class HotPrefetchService(BaseModuleService):
    MODULE = "hot_prefetch"
    ENTITY = "回迁任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务编号", "冷存位置", "目标分区", "回迁量", "预计耗时", "优先级", "过期策略", "任务状态"]
    REQUIRED_FIELDS = ["任务编号", "冷存位置", "目标分区"]
    STATUS_ORDER = ["排队中", "回迁中", "已完成", "已取消"]
    ACTION_RULES = {"发起回迁": "回迁中", "确认完成": "已完成", "取消任务": "已取消"}
    NEGATIVE_ACTIONS = []

    def __init__(self):
        super().__init__()
        os.makedirs(DATA_DIR, exist_ok=True)

    def start_prefetch(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        cold_location = str(entry.get("冷存位置", ""))
        target_partition = str(entry.get("目标分区", ""))
        asset_id = self._extract_asset_id(cold_location)

        cold_svc = self._cold_storage_service()
        if cold_svc is None or not asset_id:
            evidence = "cold_storage 服务不可用" if cold_svc is None \
                else f"无法从冷存位置 {cold_location} 解析资产"
            return self._mark_rejected(entry, cold_location, evidence)

        # 切换冲突：旧版本已校验，同时有不同版本迁移在队列里等待写入新校验
        conflict = cold_svc.detect_switchover_conflict(asset_id)
        if conflict is not None:
            return self._mark_waiting(entry, cold_location, conflict["evidence"])

        # 从冷存任务版本记录定位实际有资格读取的源数据
        source, evidence = cold_svc.resolve_read_source(asset_id)
        if source is None:
            return self._mark_rejected(entry, cold_location, evidence)

        # 放行：就绪、校验、地址均取自同一条任务版本记录
        read_location = str(source.get("preheat_location") or cold_location)
        entry["status"] = "回迁中"
        entry["pending"] = True
        entry["任务状态"] = "回迁中"
        entry["预热决策"] = "放行"
        entry["决策证据"] = evidence
        entry["读取任务版本"] = str(source.get("task_id", ""))
        entry["实际读取位置"] = read_location
        entry["读取校验摘要"] = str(source.get("checksum", ""))

        # 写入回迁记录
        self._write_prefetch_record(cold_location, entry, source=source)

        # 更新成本追踪
        self._update_cost_for_prefetch(cold_location)

        return entry, (f"{self.ENTITY} {entry_id} 回迁中，{read_location}→{target_partition}"
                       f"（读取版本 {source.get('task_id')}）")

    def complete_prefetch(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        cold_location = str(entry.get("冷存位置", ""))
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["任务状态"] = "已完成"

        self._write_prefetch_record(cold_location, entry)

        return entry, f"{self.ENTITY} {entry_id} 已完成，数据已从冷存回迁到热层"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "发起回迁":
            return self.start_prefetch(entry_id)
        if action == "确认完成":
            return self.complete_prefetch(entry_id)
        return super().run_action(entry_id, action)

    # ---------- 切换期决策 ----------
    def _mark_waiting(self, entry: dict[str, Any], cold_location: str,
                      evidence: str) -> tuple[dict[str, Any], str]:
        """切换冲突：任务保持排队并标明等待及证据。"""
        entry["status"] = "排队中"
        entry["pending"] = True
        entry["任务状态"] = "排队中"
        entry["预热决策"] = "等待"
        entry["决策证据"] = evidence
        self._write_prefetch_record(cold_location, entry)
        return entry, f"{self.ENTITY} {entry.get('id')} 切换等待：{evidence}"

    def _mark_rejected(self, entry: dict[str, Any], cold_location: str,
                       evidence: str) -> tuple[dict[str, Any], str]:
        """无具备读取资格的冷存版本：任务标明拒绝及证据。"""
        entry["status"] = "排队中"
        entry["pending"] = True
        entry["abnormal"] = True
        entry["任务状态"] = "排队中"
        entry["预热决策"] = "拒绝"
        entry["决策证据"] = evidence
        self._write_prefetch_record(cold_location, entry)
        return entry, f"{self.ENTITY} {entry.get('id')} 已拒绝：{evidence}"

    # ---------- 解析与依赖 ----------
    @staticmethod
    def _extract_asset_id(cold_location: str) -> str:
        """从冷存位置解析资产号，如 S3://glacier/ASSE-0003 → ASSE-0003。"""
        text = cold_location.strip()
        if "://" in text:
            text = text.rsplit("/", 1)[-1]
        return text.strip()

    @staticmethod
    def _cold_storage_service():
        # 延迟导入，避免服务容器初始化期的循环依赖
        from app.services import service_for
        return service_for("cold_storage")

    def _write_prefetch_record(self, cold_location: str, entry: dict[str, Any],
                               source: dict[str, Any] | None = None) -> None:
        safe_key = cold_location.replace("/", "_").replace(":", "_")
        filename = f"hot_prefetch_{safe_key}.json"
        path = os.path.join(DATA_DIR, filename)
        record = {
            "cold_location": cold_location,
            "task_id": str(entry.get("任务编号", "")),
            "target_partition": str(entry.get("目标分区", "")),
            "volume": str(entry.get("回迁量", "")),
            "priority": str(entry.get("优先级", "")),
            "expiry_policy": str(entry.get("过期策略", "")),
            "status": str(entry.get("任务状态", "")),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        # 切换期决策与读取定位证据（放行/等待/拒绝）
        if entry.get("预热决策"):
            record["decision"] = str(entry.get("预热决策"))
            record["decision_evidence"] = str(entry.get("决策证据", ""))
        if entry.get("读取任务版本"):
            record["read_task_version"] = str(entry.get("读取任务版本"))
            record["actual_read_location"] = str(entry.get("实际读取位置", ""))
            record["read_checksum"] = str(entry.get("读取校验摘要", ""))
        if source is not None:
            record["source_record"] = {
                "asset_id": source.get("asset_id"),
                "task_id": source.get("task_id"),
                "status": source.get("status"),
                "checksum": source.get("checksum"),
                "preheat_location": source.get("preheat_location"),
            }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)

    def _update_cost_for_prefetch(self, cold_location: str) -> None:
        cost_rows = store.rows("cost_tracker")
        for row in cost_rows:
            if cold_location.find(str(row.get("关联资产", ""))) >= 0:
                row["传输成本"] = "预热回迁中"
                row["status"] = "已核算"
                row["pending"] = False
                break
