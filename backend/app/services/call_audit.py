"""调用审计领域服务：管理API调用的审计记录、归档和查询追溯。

跨模块数据流：
- 消费 usage_billing 快照 → 用量异常触发审计核查
- 消费 subscription 共享记录 → 审批通过的订阅记录审计
- 消费 export_task 快照 → 导出操作记录审计
- 审计记录定期归档 → 写入 data/audit/ 共享目录
- 被 error_classify 消费 → 异常调用触发错误归类
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CallAuditService(BaseModuleService):
    MODULE = "call_audit"
    ENTITY = "调用记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录编号", "调用方", "目标API", "请求参数", "响应码", "响应耗时", "返回行数", "时间戳"]
    REQUIRED_FIELDS = ["记录编号", "调用方", "目标API"]
    STATUS_ORDER = ["已记录", "已校验", "已归档", "已过期"]
    ACTION_RULES = {"确认校验": "已校验", "提交归档": "已归档", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def record_api_call(self, caller: str, api_name: str, params: str = "",
                        response_code: str = "200", latency_ms: float = 0.0,
                        result_rows: int = 0,
                        response_body: str = "") -> dict[str, Any]:
        """记录一次API调用到审计日志。

        审计号先占位再落盘：普通调用即使和导出事件拿到同一候选号，
        也不会覆盖导出的审计记录；请求与响应同时单独持久化。
        """
        from app.services.data_manager import (
            reserve_audit, stage_audit, commit_audit, write_call_payload,
            update_audit,
        )
        from app.store import store as s

        audit_rows = s.rows(self.MODULE)
        new_id = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        record_no = f"CALL-{new_id:04d}"
        timestamp = self._now_iso()

        request_payload = {
            "record_no": record_no,
            "caller": caller,
            "api_name": api_name,
            "params": params,
            "timestamp": timestamp,
        }
        response_payload = {
            "record_no": record_no,
            "response_code": response_code,
            "latency_ms": latency_ms,
            "result_rows": result_rows,
            "body": response_body,
        }

        reservation = reserve_audit(
            "call_audit",
            event="call_completed",
            candidate_id=new_id,
            business_key=f"call_audit:{record_no}",
            payload={
                "api_name": api_name,
                "caller": caller,
                "event": "call_completed",
                "record_no": record_no,
                "request_params": params,
                "response_code": response_code,
                "latency_ms": latency_ms,
                "result_rows": result_rows,
            },
        )

        audit_data = {
            "record_no": record_no,
            "caller": caller,
            "api_name": api_name,
            "event": "call_completed",
            "request_params": params,
            "request": request_payload,
            "response": response_payload,
            "response_code": response_code,
            "latency_ms": latency_ms,
            "result_rows": result_rows,
            "timestamp": timestamp,
            "status": "completed",
            "production_source": {
                "producer": "call_audit",
                "event": "call_completed",
                "record_no": record_no,
            },
        }

        if reservation.get("status") == "committed":
            audit_id = int(reservation.get("audit_id", 0))
            committed = dict(reservation.get("record") or {})
        else:
            stage_audit(reservation, audit_data)
            committed = commit_audit(reservation, audit_data)
            audit_id = int(committed.get("audit_id", reservation.get("audit_id", 0)))

        payload_path = write_call_payload(audit_id, request_payload, response_payload)
        committed.setdefault("production_source", {})
        committed["production_source"]["producer"] = "call_audit"
        committed["production_source"]["event"] = "call_completed"
        committed["production_source"]["record_no"] = record_no
        committed["production_source"]["audit_id"] = audit_id
        committed["payload_path"] = payload_path
        committed["request"] = request_payload
        committed["response"] = response_payload
        updated = update_audit(
            audit_id, producer="call_audit",
            changes={
                "payload_path": payload_path,
                "request": request_payload,
                "response": response_payload,
                "production_source": committed["production_source"],
            },
        )
        if updated:
            committed = updated

        # 同时记录到内存
        entry = {
            "id": new_id,
            "记录编号": record_no,
            "调用方": caller,
            "目标API": api_name,
            "请求参数": params,
            "响应码": response_code,
            "响应耗时": str(latency_ms),
            "返回行数": str(result_rows),
            "时间戳": timestamp,
            "审计号": str(audit_id),
            "响应内容": response_body,
            "生产来源": "call_audit/call_completed",
            "status": "已记录",
            "pending": True,
            "abnormal": response_code != "200",
        }
        audit_rows.append(entry)
        return entry

    def _now_iso(self) -> str:
        from datetime import datetime, timedelta, timezone
        return datetime.now(timezone(timedelta(hours=8))).isoformat()

    def _next_audit_id(self) -> int:
        """候选编号：只作建议，最终由 reserve_audit 独占分配。"""
        from app.services.data_manager import next_audit_id
        return next_audit_id()

    def append_audit_entry(self, entry: dict[str, Any]) -> dict[str, Any]:
        """把工作台手工登记的调用记录补成可追溯的审计事件。

        编号由 reserve_audit 分配，避免和导出或历史审计撞号。
        """
        from app.services.data_manager import (
            reserve_audit, stage_audit, commit_audit, write_call_payload,
        )

        record_no = str(entry.get("记录编号") or "")
        audit_data = {
            "record_no": record_no,
            "caller": entry.get("调用方", ""),
            "api_name": entry.get("目标API", ""),
            "event": "call_completed",
            "request_params": entry.get("请求参数", ""),
            "response_code": entry.get("响应码", ""),
            "latency_ms": entry.get("响应耗时", ""),
            "result_rows": entry.get("返回行数", ""),
            "status": "completed",
            "production_source": {
                "producer": "call_audit",
                "event": "call_completed",
                "record_no": record_no,
            },
        }
        reservation = reserve_audit(
            "call_audit", event="call_completed",
            candidate_id=self._next_audit_id(),
            business_key=f"call_audit:{record_no}", payload=audit_data)
        if reservation.get("status") == "committed":
            record = dict(reservation.get("record") or {})
        else:
            stage_audit(reservation, audit_data)
            record = commit_audit(reservation, audit_data)
        audit_id = int(record.get("audit_id", reservation.get("audit_id", 0)))
        write_call_payload(audit_id, {
            "record_no": record_no,
            "caller": entry.get("调用方", ""),
            "api_name": entry.get("目标API", ""),
            "params": entry.get("请求参数", ""),
        }, {
            "record_no": record_no,
            "response_code": entry.get("响应码", ""),
            "latency_ms": entry.get("响应耗时", ""),
            "result_rows": entry.get("返回行数", ""),
        })
        entry["审计号"] = str(audit_id)
        return entry

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记调用记录时同步落一份审计事件与请求/响应。"""
        entry, missing = super().create_entry(values)
        if entry is not None:
            self.append_audit_entry(entry)
        return entry, missing

    def query_audit_log(self, *, audit_id: int | None = None,
                        record_no: str | None = None,
                        api_name: str | None = None,
                        caller: str | None = None) -> list[dict[str, Any]]:
        """调用审计查询入口：同步返回普通调用的请求、响应和生产来源。

        只读取审计目录里的正式记录；导出证据和调用请求/响应各自从
        自己的落盘位置补充，不会把另一方的完成事件当成本次结果。
        """
        from app.services.data_manager import (
            list_audit, read_audit, read_call_payload, read_export_evidence,
        )

        if audit_id is not None:
            record = read_audit(int(audit_id))
            records = [record] if record else []
        else:
            records = list_audit(api_name=api_name, caller=caller)

        results: list[dict[str, Any]] = []
        for record in records:
            enriched = dict(record)
            rid = int(enriched.get("audit_id", 0) or 0)
            producer = str(enriched.get("producer") or enriched.get("source") or "")
            record_no_value = str(enriched.get("record_no") or "")
            if record_no and record_no_value != record_no:
                continue
            if producer == "call_audit" or enriched.get("event") == "call_completed":
                payload = read_call_payload(rid)
                enriched["request"] = payload.get("request", enriched.get("request", {}))
                enriched["response"] = payload.get("response", enriched.get("response", {}))
                enriched["payload_path"] = (
                    enriched.get("payload_path") or f"audit/calls/{rid:06d}.json")
            elif producer == "export_task" or enriched.get("event") == "export_completed":
                evidence = read_export_evidence(rid)
                enriched["export_evidence"] = evidence
                enriched["evidence_path"] = (
                    enriched.get("evidence_path")
                    or f"audit/export_evidence/{rid:06d}.json")
            enriched.setdefault("production_source", {
                "producer": producer,
                "event": enriched.get("event", ""),
                "audit_id": rid,
            })
            enriched.setdefault("storage", f"audit/{rid:06d}.json")
            results.append(enriched)
        return results

    # 兼容命名：查询入口的几种叫法
    def query_audits(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self.query_audit_log(**kwargs)

    def audit_trail(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self.query_audit_log(**kwargs)

    def list_audit_log(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self.query_audit_log(**kwargs)

    def verify_audits(self, caller: str, api_name: str) -> list[dict[str, Any]]:
        """校验调用方对指定API的审计记录。"""
        from app.services.data_manager import list_audit, read_api_snapshot

        # 检查API是否已注册
        registry = read_api_snapshot()
        api_exists = any(
            isinstance(v, dict) and v.get("api_name") == api_name
            for v in (registry.values() if isinstance(registry, dict) else []))

        if not api_exists:
            return []

        audits = list_audit(api_name=api_name, caller=caller)
        # 校验订阅状态
        from app.services.data_manager import list_subscriptions
        subs = list_subscriptions()
        valid_sub = any(
            s.get("api_name") == api_name and s.get("applicant") == caller and s.get("approval_status") == "已通过"
            for s in subs
        )

        for audit in audits:
            audit["subscription_valid"] = valid_sub
            audit["verified"] = valid_sub
        return audits

    def archive_old_records(self, before_date: str) -> dict[str, Any]:
        """归档指定日期之前的审计记录。"""
        from app.services.data_manager import archive_audit_records
        return archive_audit_records(before_date)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "提交归档":
            self.archive_old_records(self._today_str())

        result = super().run_action(entry_id, action)
        from app.services.data_manager import write_module_snapshot
        write_module_snapshot(self.MODULE, self.domain_snapshot())
        return result

    def _today_str(self) -> str:
        from datetime import datetime, timedelta, timezone
        return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        audit_list = []
        for row in rows:
            audit_list.append({
                "id": row.get("id"),
                "record_no": row.get("记录编号"),
                "caller": row.get("调用方"),
                "api": row.get("目标API"),
                "params": row.get("请求参数"),
                "response_code": row.get("响应码"),
                "latency": row.get("响应耗时"),
                "result_rows": row.get("返回行数"),
                "timestamp": row.get("时间戳"),
                "status": row.get("status"),
            })
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(audit_list),
            "audits": audit_list,
            "archived_count": sum(1 for r in audit_list if r.get("status") == "已归档"),
            "abnormal_count": sum(1 for r in rows if r.get("abnormal")),
        }