"""审计写入身份回归测试：导出事件与普通调用撞号时的落盘与恢复。

覆盖的核心不变式：
1. 导出与普通调用即使算出同一个候选编号，也不能写进同一落盘位置；
2. 原审计号仍指向原导出证据，支持人员可以继续复核；
3. 未完成的一方不会把另一方的记录认成本次结果；
4. 导出证据与调用请求/响应各自持久化；
5. 进程重启后能恢复/重登记未完成事件，且不破坏已确认记录。
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("DATA_ROOT", tempfile.mkdtemp(prefix="audit_identity_test_"))


def _fresh_root() -> Path:
    """每个用例一个独立 DATA_ROOT，并同步给已导入的 data_manager。"""
    from app.services import data_manager

    root = Path(tempfile.mkdtemp(prefix="audit_identity_"))
    data_manager.DATA_ROOT = root
    return root


def _reset_store() -> None:
    from app.seed import SEED_ROWS
    from app.store import store as s

    s._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}


def _setup():
    _reset_store()
    _fresh_root()
    from app.services.call_audit import CallAuditService
    from app.services.export_task import ExportTaskService

    return ExportTaskService(), CallAuditService()


def test_export_and_call_same_candidate_keep_separate_evidence():
    """3 行导出先落盘，普通调用拿到同一候选号后不得覆盖导出证据。"""
    from app.services import data_manager as dm

    export_svc, call_svc = _setup()
    result = export_svc.execute_export(1)
    assert result["ok"] and result["rows_count"] == 3
    original_audit_id = result["audit_id"]

    original = dm.read_audit(original_audit_id)
    assert original["producer"] == "export_task"
    assert original["event"] == "export_completed"
    assert original["rows_count"] == 3

    # 普通调用基于过期快照算出与导出相同的候选号。
    call_svc._next_audit_id = lambda: original_audit_id  # type: ignore[method-assign]
    entry = call_svc.record_api_call(
        "support", original["api_name"], params="id=1",
        response_code="200", latency_ms=12.0, result_rows=1)
    call_audit_id = int(entry["审计号"])

    assert call_audit_id != original_audit_id, "调用完成事件不能指向导出的落盘位置"

    reopened = dm.read_audit(original_audit_id)
    assert reopened["producer"] == "export_task"
    assert reopened["rows_count"] == 3
    assert dm.read_export_evidence(original_audit_id)["rows_count"] == 3
    assert dm.read_call_payload(call_audit_id)["response"]["result_rows"] == 1

    # 调用审计查询入口同时返回请求、响应和生产来源。
    items = call_svc.query_audit_log(record_no=entry["记录编号"])
    assert len(items) == 1
    assert items[0]["audit_id"] == call_audit_id
    assert items[0]["request"]["params"] == "id=1"
    assert items[0]["response"]["result_rows"] == 1
    assert items[0]["production_source"]["producer"] == "call_audit"


def test_export_audit_log_rows_match_download_credential():
    """导出任务按审计号读取调用审计日志，行数与下载凭据一致。"""
    from app.services import data_manager as dm

    export_svc, _ = _setup()
    result = export_svc.execute_export(1)
    record = dm.read_audit(result["audit_id"])

    assert record["producer"] == "export_task"
    assert record["artifact_number"] == result["export_id"]
    assert record["rows_count"] == dm.read_export(result["export_id"])["rows_count"] == 3
    assert record["artifact_path"] == f"exports/{result['export_id']:06d}.json"

    log = export_svc.read_audit_log(result["audit_id"])
    assert log["rows_count"] == log["download"]["rows_count"] == 3
    assert log["download"]["artifact_number"] == result["export_id"]
    assert log["evidence"]["rows_count"] == 3


def test_export_audit_log_rejects_call_event():
    """导出任务按审计号读取时不会把普通调用记录当成本次导出结果。"""
    from app.services import data_manager as dm

    export_svc, call_svc = _setup()
    entry = call_svc.record_api_call("support", "导出任务样例1", result_rows=1)
    audit_id = int(entry["审计号"])

    assert export_svc.read_audit_log(audit_id) == {}
    assert dm.read_audit(audit_id)["producer"] == "call_audit"


def test_pending_export_does_not_get_committed_as_call_result():
    """导出写盘尚未完成时，普通调用提交后也保持各自记录独立。"""
    from app.services import data_manager as dm

    export_svc, call_svc = _setup()
    # 普通调用先完成并占到 1 号。
    entry = call_svc.record_api_call("support", "导出任务样例1", result_rows=1)
    call_audit_id = int(entry["审计号"])

    # 导出拿着同一个候选号开始写，但只完成到登记身份阶段。
    pending = dm.reserve_audit(
        "export_task", event="export_completed", candidate_id=call_audit_id,
        business_key="export_task:1:EXPO-0001")
    assert pending["status"] == "reserved"
    assert pending["audit_id"] != call_audit_id

    # 普通调用的记录完整保留，导出尚未完成。
    assert dm.read_audit(call_audit_id)["producer"] == "call_audit"
    assert dm.read_call_payload(call_audit_id)["response"]["result_rows"] == 1
    assert dm.read_audit(pending["audit_id"]) == {}

    # 重启恢复：导出占位没有可恢复结果，释放编号，等待该事件重新登记。
    outcome = dm.recover_pending_audits("export_task")
    assert outcome["discarded_count"] == 1
    assert outcome["discarded"][0]["audit_id"] == pending["audit_id"]
    assert dm.read_audit(call_audit_id)["producer"] == "call_audit"
    assert dm.read_audit(pending["audit_id"]) == {}
    assert not (dm._claim_path(pending["audit_id"])).exists()


def test_recovery_resumes_staged_export_without_touching_call():
    """暂存过的导出写入在恢复后补写，且不挤占已完成的调用记录。"""
    from app.services import data_manager as dm

    export_svc, call_svc = _setup()
    entry = call_svc.record_api_call("support", "导出任务样例1", result_rows=1)
    call_audit_id = int(entry["审计号"])

    pending = dm.reserve_audit(
        "export_task", event="export_completed", candidate_id=call_audit_id,
        business_key="export_task:1:EXPO-0001",
        payload={"api_name": "导出任务样例1", "caller": "export_task",
                 "event": "export_completed", "rows_count": 3})
    dm.stage_audit(pending, {"api_name": "导出任务样例1", "caller": "export_task",
                             "event": "export_completed", "rows_count": 3,
                             "artifact_number": 1})

    outcome = dm.recover_pending_audits()
    assert outcome["recovered_count"] == 1
    recovered = outcome["recovered"][0]
    assert recovered["audit_id"] == pending["audit_id"]
    assert recovered["producer"] == "export_task"
    assert recovered["rows_count"] == 3

    assert dm.read_audit(call_audit_id)["producer"] == "call_audit"
    assert dm.read_audit(call_audit_id)["result_rows"] == 1
    assert dm.read_call_payload(call_audit_id)["response"]["result_rows"] == 1


def test_confirmed_record_keeps_production_source_after_action():
    """重新打开已确认记录，仍能看到正确的生产来源。"""
    from app.services import data_manager as dm

    _, call_svc = _setup()
    entry = call_svc.record_api_call("support", "导出任务样例1", result_rows=1)
    audit_id = int(entry["审计号"])

    updated, message = call_svc.run_action(entry["id"], "确认校验")
    assert updated is not None, message

    reopened = call_svc.query_audit_log(audit_id=audit_id)[0]
    assert reopened["production_source"]["producer"] == "call_audit"
    assert reopened["production_source"]["event"] == "call_completed"
    assert reopened["request"]["params"] == ""
    assert reopened["response"]["result_rows"] == 1

    # 审计文件本身也保持同一生产来源，没有被别的完成事件改写。
    assert dm.read_audit(audit_id)["production_source"]["producer"] == "call_audit"


def test_reregistered_export_after_discard_keeps_original_reference():
    """未完成的导出被释放编号后可重新登记，原普通调用记录不受影响。"""
    from app.services import data_manager as dm

    export_svc, call_svc = _setup()
    entry = call_svc.record_api_call("support", "导出任务样例1", result_rows=1)
    call_audit_id = int(entry["审计号"])

    # 导出拿着普通调用的候选号登记后崩溃，未产生结果。
    pending = dm.reserve_audit(
        "export_task", event="export_completed", candidate_id=call_audit_id,
        business_key="export_task:1:EXPO-0001")
    dm.recover_pending_audits("export_task")

    # 重新登记同一导出任务：原审计号仍指向普通调用，导出拿到新编号。
    result = export_svc.execute_export(1)
    assert result["ok"] and result["rows_count"] == 3
    assert result["audit_id"] != call_audit_id
    assert dm.read_audit(call_audit_id)["producer"] == "call_audit"
    assert dm.read_audit(result["audit_id"])["producer"] == "export_task"
    assert dm.read_export_evidence(result["audit_id"])["rows_count"] == 3


def test_export_service_reallocates_under_contention():
    """候选号被未完成占位占用时，导出顺延编号并保留原引用含义。"""
    from app.services import data_manager as dm

    export_svc, _ = _setup()
    taken = dm.reserve_audit(
        "call_audit", event="call_completed", candidate_id=1,
        business_key="call_audit:CALL-9999")
    export_svc._next_audit_id = lambda: int(taken["audit_id"])  # type: ignore[method-assign]

    result = export_svc.execute_export(1)
    assert result["audit_id"] != taken["audit_id"]
    record = dm.read_audit(result["audit_id"])
    assert record["producer"] == "export_task"
    assert record["reallocated_from"] == taken["audit_id"]
    assert dm.read_audit(taken["audit_id"]) == {}


def test_legacy_write_audit_refuses_to_overwrite_other_event():
    """旧写入口在编号已归属其他事件时拒绝覆盖，原证据保持不变。"""
    from app.services import data_manager as dm

    export_svc, _ = _setup()
    result = export_svc.execute_export(1)
    original_audit_id = result["audit_id"]
    before = json.dumps(dm.read_audit(original_audit_id), ensure_ascii=False, sort_keys=True)

    try:
        dm.write_audit(original_audit_id, {
            "api_name": "导出任务样例1",
            "caller": "call_audit",
            "event": "call_completed",
            "result_rows": 1,
        })
    except dm.AuditCollisionError:
        pass
    else:  # pragma: no cover - 明确失败更利于定位
        raise AssertionError("旧写入口不应允许覆盖其他生产者的审计记录")

    after = json.dumps(dm.read_audit(original_audit_id), ensure_ascii=False, sort_keys=True)
    assert before == after
