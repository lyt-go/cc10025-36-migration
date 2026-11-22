"""冷存任务版本记录专项测试：格式迁移、切换冲突、读取定位与回滚。

场景覆盖：
    1. 存量按资产单文件 → 按任务分辨的 v2 版本记录，关联迁移数据/任务号/预热位置，旧路径保留
    2. 缺少任务版本的旧数据不能当成核验通过，预热被拒绝并留证据
    3. 切换冲突：旧版本已校验 + 不同版本迁移排队写新校验 → 预热标明等待
    4. 冲突解除后放行：读取位置/版本/校验取自同一条记录，不跨记录拼接
    5. 待核新版任务启动与回滚：读取位置始终对应完整校验，恢复后两版可回查
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(__file__))

from app.store import store
from app.services import service_for
import app.services.cold_storage as cold_mod
import app.services.hot_prefetch as hot_mod

RED = "\033[91m"
GREEN = "\033[92m"
RESET = "\033[0m"

passed = 0
failed = 0

# 隔离数据目录：版本记录文件写到临时目录，不污染 app/data
TMP_DIR = tempfile.mkdtemp(prefix="cold_version_test_")
cold_mod.DATA_DIR = TMP_DIR
hot_mod.DATA_DIR = TMP_DIR

cold_svc = service_for("cold_storage")
hot_svc = service_for("hot_prefetch")


def check(desc, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  {GREEN}PASS{RESET} {desc}")
        passed += 1
    else:
        print(f"  {RED}FAIL{RESET} {desc} {detail}")
        failed += 1
        return False
    return True


def read_file(name):
    with open(os.path.join(TMP_DIR, name), "r", encoding="utf-8") as f:
        return json.load(f)


def write_legacy(asset_id, **fields):
    record = {
        "asset_id": asset_id,
        "task_id": fields.get("task_id", ""),
        "target_storage": fields.get("target_storage", "S3-GLACIER"),
        "compression": fields.get("compression", "ZSTD"),
        "migration_volume": fields.get("migration_volume", "10TB"),
        "status": fields.get("status", "已完成"),
        "checksum": fields.get("checksum", "CRC32校验通过"),
        "updated_at": fields.get("updated_at", "2026-11-20T00:00:00+00:00"),
    }
    if fields.get("drop_task_id"):
        record.pop("task_id")
    with open(os.path.join(TMP_DIR, f"cold_storage_{asset_id}.json"),
              "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)
    return record


def make_cold_task(task_id, asset_id):
    entry, missing = cold_svc.create_entry({
        "任务编号": task_id, "源资产": asset_id, "目标存储": "S3-GLACIER",
        "压缩算法": "ZSTD", "迁移量": "10TB", "预计耗时": "2h",
        "迁移状态": "待调度", "校验摘要": "",
    })
    assert not missing, missing
    return entry


def make_hot_task(task_id, asset_id):
    entry, missing = hot_svc.create_entry({
        "任务编号": task_id, "冷存位置": f"S3://glacier/{asset_id}",
        "目标分区": f"{asset_id}_hot", "回迁量": "10TB", "预计耗时": "1h",
        "优先级": "高", "过期策略": "T+7", "任务状态": "排队中",
    })
    assert not missing, missing
    return entry


def test_legacy_migration():
    """场景1：存量旧文件迁移为按任务分辨的版本记录，旧路径保留。"""
    print("\n=== 场景1: 存量格式迁移（关联迁移数据/任务号/预热位置，保留旧路径） ===")
    asset = "ASSE-9101"
    write_legacy(asset, task_id="COLD-9101")
    make_hot_task("HOT_-9101", asset)  # 预热位置来源

    summary = cold_svc.migrate_legacy_records()
    migrated = [r for r in summary["migrated"] if r["asset_id"] == asset]
    check("旧文件迁移生成任务版本记录", len(migrated) == 1, str(summary))

    version_file = f"cold_storage_{asset}_COLD-9101.json"
    check("新版记录文件存在", os.path.exists(os.path.join(TMP_DIR, version_file)))
    record = read_file(version_file)
    check("记录格式为 v2", record.get("record_version") == "v2")
    check("关联任务号", record.get("task_id") == "COLD-9101")
    check("关联迁移数据", record.get("target_storage") == "S3-GLACIER"
          and record.get("migration_volume") == "10TB")
    check("关联预热位置", record.get("preheat_location") == f"S3://glacier/{asset}",
          str(record.get("preheat_location")))
    check("记录迁移来源", record.get("migrated_from") == f"cold_storage_{asset}.json")

    legacy_path = os.path.join(TMP_DIR, f"cold_storage_{asset}.json")
    check("旧路径文件保留", os.path.exists(legacy_path))
    legacy = read_file(f"cold_storage_{asset}.json")
    check("旧路径原字段仍在", legacy.get("task_id") == "COLD-9101"
          and legacy.get("checksum") == "CRC32校验通过")
    check("旧路径标注迁移去向", legacy.get("migrated_to") == version_file)

    again = cold_svc.migrate_legacy_records()
    check("重复迁移幂等跳过",
          any(r["asset_id"] == asset for r in again["skipped"])
          and not any(r["asset_id"] == asset for r in again["migrated"]))

    status = cold_svc.get_migration_status(asset)
    check("旧目录依赖读取仍可用", status is not None and status.get("task_id") == "COLD-9101")


def test_unversioned_legacy_not_verified():
    """场景2：找不到任务版本的旧数据不能当成核验通过。"""
    print("\n=== 场景2: 缺少任务版本的旧数据不予核验 ===")
    asset = "ASSE-9102"
    write_legacy(asset, drop_task_id=True)  # 旧数据无任务号，但带校验通过字样

    summary = cold_svc.migrate_legacy_records()
    check("无任务版本旧数据归入 unversioned",
          any(r["asset_id"] == asset for r in summary["unversioned"]), str(summary))

    source, evidence = cold_svc.resolve_read_source(asset)
    check("无任务版本旧数据定位不到可读源", source is None, evidence)
    check("依据说明缺少任务版本", "任务版本" in evidence, evidence)

    hot = make_hot_task("HOT_-9102", asset)
    entry, msg = hot_svc.start_prefetch(hot["id"])
    check("预热被拒绝", entry is not None and entry.get("预热决策") == "拒绝", msg)
    check("拒绝任务保持排队并标记异常", entry.get("status") == "排队中"
          and entry.get("abnormal") is True)
    check("拒绝证据回写到任务", "任务版本" in str(entry.get("决策证据", "")),
          str(entry.get("决策证据")))


def test_switchover_conflict_then_release():
    """场景3+4：切换冲突等待 → 新校验写入后放行，读取不跨记录拼接。"""
    print("\n=== 场景3: 切换冲突（旧版本已校验 + 新版本排队写校验）→ 预热等待 ===")
    asset = "ASSE-9103"
    task_a = make_cold_task("COLD-9301", asset)
    cold_svc.start_migration(task_a["id"])
    cold_svc.complete_migration(task_a["id"])

    task_b = make_cold_task("COLD-9302", asset)
    cold_svc.start_migration(task_b["id"])  # 不同版本迁移在队列里等待写入新校验

    pointer = read_file(f"cold_storage_{asset}.json")
    check("迁移中旧路径指针仍指向已校验版本",
          pointer.get("active_task_id") == "COLD-9301", str(pointer))

    conflict = cold_svc.detect_switchover_conflict(asset)
    check("检测到切换冲突", conflict is not None)
    if conflict:
        check("冲突证据含已校验版本与排队版本",
              "COLD-9301" in conflict["evidence"] and "COLD-9302" in conflict["evidence"],
              conflict["evidence"])

    hot = make_hot_task("HOT_-9103", asset)
    entry, msg = hot_svc.start_prefetch(hot["id"])
    check("冲突期预热标明等待", entry is not None and entry.get("预热决策") == "等待", msg)
    check("等待任务保持排队", entry.get("status") == "排队中")
    check("等待证据回写任务", "COLD-9302" in str(entry.get("决策证据", "")),
          str(entry.get("决策证据")))

    record_file = "hot_prefetch_S3___glacier_ASSE-9103.json"
    record = read_file(record_file)
    check("等待证据写入回迁记录文件",
          record.get("decision") == "等待" and "COLD-9302" in record.get("decision_evidence", ""))

    print("\n=== 场景4: 新校验写入后放行，读取字段来自同一条版本记录 ===")
    cold_svc.complete_migration(task_b["id"])
    pointer = read_file(f"cold_storage_{asset}.json")
    check("确认完成后指针切到新版本", pointer.get("active_task_id") == "COLD-9302")
    check("指针 active_record 文件存在",
          os.path.exists(os.path.join(TMP_DIR, pointer.get("active_record", ""))))

    entry, msg = hot_svc.start_prefetch(hot["id"])
    check("冲突解除后预热放行", entry is not None and entry.get("预热决策") == "放行", msg)
    check("任务进入回迁中", entry.get("status") == "回迁中")
    check("读取任务版本为新版本", entry.get("读取任务版本") == "COLD-9302")

    source, evidence = cold_svc.resolve_read_source(asset)
    on_disk = read_file(f"cold_storage_{asset}_COLD-9302.json")
    check("定位结果与磁盘单条记录完全一致（不跨记录拼接）", source == on_disk)
    check("实际读取位置取自该记录", entry.get("实际读取位置") == on_disk.get("preheat_location"))
    check("读取校验摘要取自该记录", entry.get("读取校验摘要") == on_disk.get("checksum"))
    check("该记录校验通过", "校验通过" in str(on_disk.get("checksum")))


def test_rollback_and_traceability():
    """场景5：待核新版任务启动与回滚，读取始终对应完整校验，两版可回查。"""
    print("\n=== 场景5: 新版任务启动→回滚→恢复，两版可回查 ===")
    asset = "ASSE-9104"
    task_a = make_cold_task("COLD-9401", asset)
    cold_svc.start_migration(task_a["id"])
    cold_svc.complete_migration(task_a["id"])

    task_b = make_cold_task("COLD-9402", asset)
    cold_svc.start_migration(task_b["id"])  # 待核新版任务启动

    source, _ = cold_svc.resolve_read_source(asset)
    check("新版迁移中读取仍对应已完整校验的旧版本",
          source is not None and source.get("task_id") == "COLD-9401",
          str((source or {}).get("task_id")))

    entry, msg = cold_svc.rollback_migration(task_b["id"])
    check("回滚执行成功", entry is not None and entry.get("status") == "已回滚", msg)

    pointer = read_file(f"cold_storage_{asset}.json")
    check("回滚后指针恢复至旧版本", pointer.get("active_task_id") == "COLD-9401")

    source, _ = cold_svc.resolve_read_source(asset)
    check("恢复后读取位置仍对应完整校验版本",
          source is not None and source.get("task_id") == "COLD-9401"
          and "校验通过" in str(source.get("checksum")))

    rolled = read_file(f"cold_storage_{asset}_COLD-9402.json")
    check("回滚版本记录留痕", rolled.get("rolled_back") is True
          and rolled.get("status") == "已回滚")

    versions = cold_svc.list_task_versions(asset)
    by_task = {v["task_id"]: v for v in versions["versions"]}
    check("两版均可回查", set(by_task) == {"COLD-9401", "COLD-9402"}, str(by_task.keys()))
    check("旧版本具备读取资格", by_task.get("COLD-9401", {}).get("eligible") is True)
    check("回滚版本不具备资格且带依据",
          by_task.get("COLD-9402", {}).get("eligible") is False
          and "回滚" in by_task.get("COLD-9402", {}).get("evidence", ""),
          str(by_task.get("COLD-9402")))
    check("回查结果含旧路径信息", versions.get("legacy_exists") is True
          and versions.get("active_task_id") == "COLD-9401")

    entry, msg = cold_svc.rollback_migration(task_a["id"])
    check("已完成任务不允许回滚", entry is None, msg)

    audit = [r for r in store.rows("audit_log") if r.get("操作类型") == "回滚迁移"
             and r.get("操作对象") == "COLD-9402"]
    check("回滚写入审计日志", len(audit) >= 1)


def test_pointer_compat_schema():
    """场景6：旧路径兼容指针保留旧 schema 字段，旧目录依赖可用。"""
    print("\n=== 场景6: 旧路径兼容指针字段 ===")
    asset = "ASSE-9105"
    task = make_cold_task("COLD-9501", asset)
    cold_svc.start_migration(task["id"])
    cold_svc.complete_migration(task["id"])

    pointer = read_file(f"cold_storage_{asset}.json")
    for key in ("asset_id", "task_id", "target_storage", "compression",
                "migration_volume", "status", "checksum", "updated_at"):
        check(f"旧 schema 字段 {key} 保留", key in pointer)
    check("指针格式标注 v1-compat", pointer.get("record_version") == "v1-compat")
    check("指针指向校验通过版本", pointer.get("active_task_id") == "COLD-9501"
          and "校验通过" in str(pointer.get("checksum")))


if __name__ == "__main__":
    print("=" * 60)
    print("冷存任务版本记录专项测试")
    print("=" * 60)
    try:
        test_legacy_migration()
        test_unversioned_legacy_not_verified()
        test_switchover_conflict_then_release()
        test_rollback_and_traceability()
        test_pointer_compat_schema()
    finally:
        shutil.rmtree(TMP_DIR, ignore_errors=True)

    print(f"\n{'=' * 60}")
    print(f"结果: {GREEN}{passed} 通过{RESET} / {RED}{failed} 失败{RESET} / "
          f"{passed + failed} 总计")
    print("=" * 60)
    if failed > 0:
        sys.exit(1)
