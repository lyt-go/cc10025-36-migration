"""cold_storage 领域服务：冷存储迁移管理，接收 tier_policy 上游事件触发冷存任务。

跨模块数据流：
    tier_policy → cold_storage: 分层策略生效触发冷存储迁移
    cold_storage → hot_prefetch: 冷存完成可能触发预热回迁需求
    cold_storage → cost_tracker: 迁移任务更新成本追踪

记录格式：
    v1（旧格式）：``cold_storage_{asset_id}.json`` 按资产单文件，
        新迁移会覆盖旧任务版本，历史任务无法回查。
    v2（新格式）：``cold_storage_{asset_id}_{task_id}.json`` 按任务版本分辨，
        记录关联迁移数据、任务号与预热位置，多版本并存可回查。
    旧路径保留为兼容指针（record_version=v1-compat），字段保持旧 schema，
        并始终指向最近一个校验通过的 v2 记录，保证旧目录依赖可用；
        指针只在「确认完成」时切换，迁移中/待调度的新版本不会污染读取位置。
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "assets")

RECORD_FORMAT_V2 = "v2"
POINTER_FORMAT = "v1-compat"
VERIFIED_KEYWORD = "校验通过"
UNVERSIONED_TASK = "LEGACY"
IN_FLIGHT_STATUSES = ("待调度", "迁移中")


class ColdStorageService(BaseModuleService):
    MODULE = "cold_storage"
    ENTITY = "冷存任务"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["任务编号", "源资产", "目标存储", "压缩算法", "迁移量", "预计耗时", "迁移状态", "校验摘要"]
    REQUIRED_FIELDS = ["任务编号", "源资产", "目标存储"]
    STATUS_ORDER = ["待调度", "迁移中", "已完成", "已失败", "已回滚"]
    ACTION_RULES = {"发起迁移": "迁移中", "确认完成": "已完成", "标记失败": "已失败", "回滚迁移": "已回滚"}
    NEGATIVE_ACTIONS = []

    def __init__(self):
        super().__init__()
        os.makedirs(DATA_DIR, exist_ok=True)

    # ---------- 迁移动作 ----------
    def start_migration(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """发起冷存储迁移：写任务版本记录（迁移中），并更新成本追踪。

        旧路径指针此时不切换：新版本尚未写入新校验，读取位置仍指向
        最近一个校验通过的版本。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        asset_id = str(entry.get("源资产", ""))
        target_storage = str(entry.get("目标存储", ""))

        entry["status"] = "迁移中"
        entry["pending"] = True
        entry["迁移状态"] = "迁移中"

        # 写新版任务版本记录；supersedes 记录它将要取代的已校验版本
        previous = self._active_verified_task_id(asset_id)
        self._write_version_record(asset_id, entry, supersedes=previous)

        # 更新成本追踪
        self._update_cost_tracker(asset_id, target_storage)

        # 记录审计日志
        self._write_audit_entry(
            entity="冷存任务",
            action="发起迁移",
            target_id=str(entry.get("任务编号", "")),
            detail=f"冷存迁移 {asset_id} → {target_storage}",
        )

        return entry, f"{self.ENTITY} {entry_id} 迁移中，{asset_id}→{target_storage}"

    def complete_migration(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """确认迁移完成：写入校验摘要，切换旧路径指针并检查预热回迁需求。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        asset_id = str(entry.get("源资产", ""))
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["迁移状态"] = "已完成"
        entry["校验摘要"] = "CRC32校验通过"

        # 更新任务版本记录，校验通过后旧路径指针才切换到新版本
        self._write_version_record(asset_id, entry)
        self._update_legacy_pointer(asset_id, entry)

        # 如果存在预热回迁需求，触发hot_prefetch
        self._check_prefetch_need(asset_id)

        return entry, f"{self.ENTITY} {entry_id} 已完成，数据已迁移到冷存储"

    def fail_migration(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """标记迁移失败：版本记录同步置为已失败，指针保持不动。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        asset_id = str(entry.get("源资产", ""))
        entry["status"] = "已失败"
        entry["pending"] = False
        entry["abnormal"] = True
        entry["迁移状态"] = "已失败"

        self._write_version_record(asset_id, entry)
        self._write_audit_entry(
            entity="冷存任务",
            action="标记失败",
            target_id=str(entry.get("任务编号", "")),
            detail=f"冷存迁移 {asset_id} 失败，读取指针保持不变",
        )
        return entry, f"{self.ENTITY} {entry_id} 已失败"

    def rollback_migration(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """回滚待调度/迁移中的任务：版本记录留痕，读取指针恢复至最近校验通过版本。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"
        if entry.get("status") not in IN_FLIGHT_STATUSES:
            return None, (f"{self.ENTITY} {entry_id} 当前状态为"
                          f"「{entry.get('status')}」，仅待调度/迁移中可回滚")

        asset_id = str(entry.get("源资产", ""))
        task_id = str(entry.get("任务编号", ""))
        entry["status"] = "已回滚"
        entry["pending"] = False
        entry["迁移状态"] = "已回滚"

        # 版本记录保留（可回查），标记已回滚
        self._write_version_record(asset_id, entry, rolled_back=True)
        # 指针恢复至最近一个校验通过的版本（通常本来就没动过，这里兜底）
        restored = self._restore_pointer_to_latest_verified(asset_id, exclude_task=task_id)

        self._write_audit_entry(
            entity="冷存任务",
            action="回滚迁移",
            target_id=task_id,
            detail=f"冷存迁移 {asset_id} 任务 {task_id} 已回滚，"
                   f"读取指针恢复至 {restored or '无可用版本'}",
        )
        return entry, (f"{self.ENTITY} {entry_id} 已回滚，"
                       f"读取位置恢复至 {restored or '无可用版本'}")

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "发起迁移":
            return self.start_migration(entry_id)
        if action == "确认完成":
            return self.complete_migration(entry_id)
        if action == "标记失败":
            return self.fail_migration(entry_id)
        if action == "回滚迁移":
            return self.rollback_migration(entry_id)
        return super().run_action(entry_id, action)

    # ---------- 读取定位（供 hot_prefetch 等下游消费） ----------
    def resolve_read_source(self, asset_id: str) -> tuple[dict[str, Any] | None, str]:
        """定位资产实际有资格读取的冷存源数据。

        返回 (记录, 依据)。记录整体来自同一条任务版本文件——
        就绪状态、校验摘要、预热地址不跨记录拼接；
        找不到任务版本的旧数据一律视为未核验。
        """
        self._ensure_asset_migrated(asset_id)
        versions = self._load_version_records(asset_id)
        eligible = [v for v in versions if self._eligibility(v)[0]]
        if eligible:
            chosen = max(eligible, key=lambda v: str(v.get("updated_at", "")))
            return chosen, (f"读取版本 {chosen.get('task_id')}：已完成且校验通过，"
                            f"就绪/校验/地址均取自该任务版本记录")
        if versions:
            reasons = "; ".join(
                f"{v.get('task_id') or UNVERSIONED_TASK}: {self._eligibility(v)[1]}"
                for v in versions
            )
            return None, f"资产 {asset_id} 的任务版本记录均不具备读取资格（{reasons}）"
        legacy = self._read_legacy_record(asset_id)
        if legacy is not None:
            if not str(legacy.get("task_id", "") or ""):
                return None, (f"资产 {asset_id} 仅有缺少任务版本的旧数据，"
                              f"不能当成核验通过")
            return None, f"资产 {asset_id} 的旧记录未完成格式迁移，暂无校验通过的版本"
        return None, f"资产 {asset_id} 不存在冷存任务版本记录"

    def detect_switchover_conflict(self, asset_id: str) -> dict[str, Any] | None:
        """检测切换冲突：旧版本已校验，同时存在不同版本的迁移在队列里等待写入新校验。"""
        source, _ = self.resolve_read_source(asset_id)
        if source is None:
            return None
        pending = [
            v for v in self._load_version_records(asset_id)
            if v.get("task_id") != source.get("task_id")
            and v.get("status") in IN_FLIGHT_STATUSES
            and not v.get("rolled_back")
        ]
        if not pending:
            return None
        pending_ids = [str(v.get("task_id")) for v in pending]
        return {
            "asset_id": asset_id,
            "verified_task": source.get("task_id"),
            "pending_tasks": pending_ids,
            "evidence": (f"已校验版本 {source.get('task_id')} 的就绪数据与队列中迁移 "
                         f"{'、'.join(pending_ids)}（等待写入新校验）的新路径冲突，"
                         f"预热需等待切换完成"),
        }

    def list_task_versions(self, asset_id: str) -> dict[str, Any]:
        """回查资产的全部冷存任务版本记录及每条记录的资格依据。"""
        self._ensure_asset_migrated(asset_id)
        pointer = self._read_legacy_record(asset_id)
        versions = []
        for record in self._load_version_records(asset_id):
            eligible, evidence = self._eligibility(record)
            versions.append({
                "task_id": record.get("task_id"),
                "record_version": record.get("record_version"),
                "status": record.get("status"),
                "checksum": record.get("checksum"),
                "preheat_location": record.get("preheat_location"),
                "supersedes": record.get("supersedes"),
                "rolled_back": bool(record.get("rolled_back")),
                "migrated_from": record.get("migrated_from"),
                "eligible": eligible,
                "evidence": evidence,
                "path": self._version_filename(asset_id, str(record.get("task_id") or UNVERSIONED_TASK)),
                "updated_at": record.get("updated_at"),
            })
        versions.sort(key=lambda v: str(v.get("updated_at", "")))
        return {
            "asset_id": asset_id,
            "legacy_path": self._legacy_filename(asset_id),
            "legacy_exists": pointer is not None,
            "active_task_id": (pointer or {}).get("active_task_id") or (pointer or {}).get("task_id"),
            "versions": versions,
        }

    def get_migration_status(self, asset_id: str) -> dict[str, Any] | None:
        """兼容旧调用：读取旧路径指针文件内容。"""
        return self._read_legacy_record(asset_id)

    # ---------- 存量格式迁移 ----------
    def migrate_legacy_records(self) -> dict[str, Any]:
        """把存量按资产单文件的旧记录迁移为按任务分辨的 v2 版本记录。

        关联旧文件里的迁移数据、任务号与预热位置；旧路径文件保留并标注
        migrated_to。缺少任务号的旧数据迁移为 LEGACY 记录，不具备读取资格。
        """
        summary: dict[str, Any] = {"migrated": [], "skipped": [], "unversioned": []}
        for fname in sorted(os.listdir(DATA_DIR)):
            if not (fname.startswith("cold_storage_") and fname.endswith(".json")):
                continue
            path = os.path.join(DATA_DIR, fname)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(data, dict):
                continue
            if data.get("record_version"):
                # v2 记录或兼容指针，无需迁移
                continue
            if data.get("migrated_to"):
                summary["skipped"].append({
                    "kind": "skipped",
                    "asset_id": str(data.get("asset_id", "")),
                    "task_id": str(data.get("task_id", "") or ""),
                    "evidence": f"旧文件已迁移至 {data.get('migrated_to')}，跳过",
                })
                continue
            result = self._migrate_legacy_file(fname, data)
            summary[result["kind"]].append(result)
        return summary

    def _migrate_legacy_file(self, fname: str, data: dict[str, Any]) -> dict[str, Any]:
        asset_id = str(data.get("asset_id", ""))
        task_id = str(data.get("task_id", "") or "")
        now = datetime.now(timezone.utc).isoformat()
        record = {
            "record_version": RECORD_FORMAT_V2,
            "asset_id": asset_id,
            "task_id": task_id,
            "target_storage": str(data.get("target_storage", "")),
            "compression": str(data.get("compression", "")),
            "migration_volume": str(data.get("migration_volume", "")),
            "status": str(data.get("status", "")),
            "checksum": str(data.get("checksum", "")),
            "preheat_location": self._preheat_location_for(asset_id),
            "supersedes": None,
            "rolled_back": False,
            "migrated_from": fname,
            "migrated_at": now,
            "updated_at": str(data.get("updated_at", "") or now),
        }
        target_name = self._version_filename(asset_id, task_id or UNVERSIONED_TASK)
        target_path = os.path.join(DATA_DIR, target_name)
        if os.path.exists(target_path):
            return {"kind": "skipped", "asset_id": asset_id, "task_id": task_id,
                    "evidence": f"版本记录 {target_name} 已存在，跳过"}
        with open(target_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)

        # 旧路径保留：原字段不动，仅追加 migrated_to 指向新版记录
        data["migrated_to"] = target_name
        with open(os.path.join(DATA_DIR, fname), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

        kind = "unversioned" if not task_id else "migrated"
        evidence = (f"旧记录缺少任务版本，迁移为 {target_name}，不能当成核验通过"
                    if not task_id else
                    f"已关联迁移数据、任务号 {task_id} 与预热位置，生成 {target_name}")
        return {"kind": kind, "asset_id": asset_id, "task_id": task_id,
                "record": target_name, "evidence": evidence}

    def _ensure_asset_migrated(self, asset_id: str) -> None:
        """读取前兜底：资产只有未迁移的旧文件时，先就地迁移再定位。"""
        if self._load_version_records(asset_id):
            return
        legacy = self._read_legacy_record(asset_id)
        if legacy is None or legacy.get("record_version") or legacy.get("migrated_to"):
            return
        self._migrate_legacy_file(self._legacy_filename(asset_id), legacy)

    # ---------- 文件 I/O ----------
    def _legacy_filename(self, asset_id: str) -> str:
        return f"cold_storage_{asset_id}.json"

    def _version_filename(self, asset_id: str, task_id: str) -> str:
        return f"cold_storage_{asset_id}_{task_id}.json"

    def _write_version_record(self, asset_id: str, entry: dict[str, Any],
                              supersedes: str | None = None,
                              rolled_back: bool = False) -> None:
        task_id = str(entry.get("任务编号", ""))
        path = os.path.join(DATA_DIR, self._version_filename(asset_id, task_id))
        existing: dict[str, Any] = {}
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        record = {
            "record_version": RECORD_FORMAT_V2,
            "asset_id": asset_id,
            "task_id": task_id,
            "target_storage": str(entry.get("目标存储", "")),
            "compression": str(entry.get("压缩算法", "")),
            "migration_volume": str(entry.get("迁移量", "")),
            "status": str(entry.get("迁移状态", entry.get("status", ""))),
            "checksum": str(entry.get("校验摘要", "")),
            "preheat_location": self._preheat_location_for(asset_id),
            "supersedes": supersedes if supersedes is not None else existing.get("supersedes"),
            "rolled_back": rolled_back or bool(existing.get("rolled_back")),
            "migrated_from": existing.get("migrated_from"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)

    def _update_legacy_pointer(self, asset_id: str, entry: dict[str, Any]) -> None:
        """旧路径重写为兼容指针：旧 schema 字段 + 指向当前校验通过版本。"""
        task_id = str(entry.get("任务编号", ""))
        record = {
            "asset_id": asset_id,
            "task_id": task_id,
            "target_storage": str(entry.get("目标存储", "")),
            "compression": str(entry.get("压缩算法", "")),
            "migration_volume": str(entry.get("迁移量", "")),
            "status": str(entry.get("迁移状态", "")),
            "checksum": str(entry.get("校验摘要", "")),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "record_version": POINTER_FORMAT,
            "active_task_id": task_id,
            "active_record": self._version_filename(asset_id, task_id),
        }
        with open(os.path.join(DATA_DIR, self._legacy_filename(asset_id)),
                  "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)

    def _restore_pointer_to_latest_verified(self, asset_id: str,
                                            exclude_task: str | None = None) -> str | None:
        """把旧路径指针恢复到最近一个校验通过的版本记录，返回该版本任务号。"""
        candidates = []
        for record in self._load_version_records(asset_id):
            if exclude_task and record.get("task_id") == exclude_task:
                continue
            if self._eligibility(record)[0]:
                candidates.append(record)
        if not candidates:
            return None
        chosen = max(candidates, key=lambda v: str(v.get("updated_at", "")))
        entry_view = {
            "任务编号": chosen.get("task_id"),
            "目标存储": chosen.get("target_storage"),
            "压缩算法": chosen.get("compression"),
            "迁移量": chosen.get("migration_volume"),
            "迁移状态": chosen.get("status"),
            "校验摘要": chosen.get("checksum"),
        }
        self._update_legacy_pointer(asset_id, entry_view)
        return str(chosen.get("task_id"))

    def _read_legacy_record(self, asset_id: str) -> dict[str, Any] | None:
        path = os.path.join(DATA_DIR, self._legacy_filename(asset_id))
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _load_version_records(self, asset_id: str) -> list[dict[str, Any]]:
        prefix = f"cold_storage_{asset_id}_"
        records = []
        for fname in sorted(os.listdir(DATA_DIR)):
            if not (fname.startswith(prefix) and fname.endswith(".json")):
                continue
            path = os.path.join(DATA_DIR, fname)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue
            if (isinstance(data, dict)
                    and data.get("record_version") == RECORD_FORMAT_V2
                    and str(data.get("asset_id")) == asset_id):
                records.append(data)
        return records

    def _active_verified_task_id(self, asset_id: str) -> str | None:
        pointer = self._read_legacy_record(asset_id)
        if not pointer:
            return None
        task_id = pointer.get("active_task_id") or pointer.get("task_id")
        return str(task_id) if task_id else None

    def _eligibility(self, record: dict[str, Any]) -> tuple[bool, str]:
        """判断单条任务版本记录是否具备读取资格，并给出解释依据。"""
        if not str(record.get("task_id", "") or ""):
            return False, "缺少任务版本，旧数据不能当成核验通过"
        if record.get("rolled_back"):
            return False, "任务已回滚，不具备读取资格"
        if record.get("status") != "已完成":
            return False, f"迁移状态为{record.get('status') or '未知'}，尚未完成校验"
        if VERIFIED_KEYWORD not in str(record.get("checksum", "")):
            return False, "校验摘要缺失或未通过"
        return True, "已完成且校验通过，具备读取资格"

    def _preheat_location_for(self, asset_id: str) -> str:
        """关联预热位置：优先取预热回迁模块里该资产的冷存位置。"""
        for row in store.rows("hot_prefetch"):
            location = str(row.get("冷存位置", ""))
            if asset_id and asset_id in location:
                return location
        return f"S3://glacier/{asset_id}"

    # ---------- 跨模块副作用 ----------
    def _update_cost_tracker(self, asset_id: str, target_storage: str) -> None:
        """更新成本追踪记录中的存储成本。"""
        cost_rows = store.rows("cost_tracker")
        for row in cost_rows:
            if str(row.get("关联资产")) == asset_id:
                row["存储成本"] = f"冷存:{target_storage}"
                row["status"] = "已核算"
                row["pending"] = False
                break

    def _check_prefetch_need(self, asset_id: str) -> None:
        """检查冷存资产是否有预热回迁需求。"""
        hot_rows = store.rows("hot_prefetch")
        for row in hot_rows:
            if str(row.get("冷存位置", "")).find(asset_id) >= 0 and row.get("status") == "排队中":
                return
        # 无已有预热任务则创建种子
        next_id = max((int(r.get("id", 0)) for r in hot_rows), default=0) + 1
        prefetch = {
            "id": next_id,
            "status": "排队中",
            "pending": True,
            "abnormal": False,
            "任务编号": f"HOT_-{next_id:04d}",
            "冷存位置": f"S3://glacier/{asset_id}",
            "目标分区": f"{asset_id}_hot",
            "回迁量": "待估算",
            "预计耗时": "待估算",
            "优先级": "低",
            "过期策略": "T+7",
            "任务状态": "排队中",
        }
        hot_rows.append(prefetch)

    def _write_audit_entry(self, entity: str, action: str, target_id: str,
                           detail: str) -> None:
        audit_rows = store.rows("audit_log")
        next_id = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        entry = {
            "id": next_id,
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "记录编号": f"AUDI-{next_id:04d}",
            "操作主体": entity,
            "操作类型": action,
            "操作对象": target_id,
            "操作时间": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "影响范围": detail,
            "审计摘要": detail,
            "归档状态": "已记录",
        }
        audit_rows.append(entry)
