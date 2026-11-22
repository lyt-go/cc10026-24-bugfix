"""调用审计与导出证据的专用查询入口。

通用资源路由的 /api/{module}/{entry_id} 会把固定子路径当成数字编号，
所以这里先注册，保证 query / recover / audit-log 这些固定路径可命中。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services import service_for

router = APIRouter(prefix="/api", tags=["审计追溯"])


@router.get("/call_audit/query")
def query_call_audit(
    audit_id: int | None = Query(default=None, ge=1),
    record_no: str | None = Query(default=None),
    api_name: str | None = Query(default=None),
    caller: str | None = Query(default=None),
) -> dict[str, Any]:
    """调用审计查询入口：返回调用记录及其请求、响应与生产来源。"""
    service = service_for("call_audit")
    if service is None or not hasattr(service, "query_audit_log"):
        raise HTTPException(status_code=404, detail="调用审计服务不可用")
    items = service.query_audit_log(
        audit_id=audit_id, record_no=record_no, api_name=api_name, caller=caller)
    return {"items": items, "total": len(items), "page": 1, "size": len(items)}


@router.get("/call_audit/{audit_id}/audit-log")
def read_call_audit(audit_id: int) -> dict[str, Any]:
    """按审计号读取单条调用审计日志。"""
    service = service_for("call_audit")
    if service is None or not hasattr(service, "query_audit_log"):
        raise HTTPException(status_code=404, detail="调用审计服务不可用")
    items = service.query_audit_log(audit_id=audit_id)
    if not items:
        raise HTTPException(status_code=404, detail=f"审计号 {audit_id} 没有对应记录")
    return items[0]


@router.post("/call_audit/recover")
def recover_call_audit(producer: str | None = Query(default=None)) -> dict[str, Any]:
    """恢复未完成的审计写入；只有该事件重新登记后才会产生新记录。"""
    from app.services.data_manager import recover_pending_audits
    return recover_pending_audits(producer)


@router.get("/export_task/audit-log")
def export_audit_log(audit_id: int = Query(ge=1)) -> dict[str, Any]:
    """导出任务按审计号读取调用审计日志。

    只接受导出完成事件；返回的原始行数与下载凭据指向的成品一致。
    """
    service = service_for("export_task")
    if service is None or not hasattr(service, "read_audit_log"):
        raise HTTPException(status_code=404, detail="导出任务服务不可用")
    result = service.read_audit_log(audit_id)
    if not result:
        from app.services.data_manager import read_audit

        record = read_audit(audit_id)
        if not record:
            raise HTTPException(status_code=404, detail=f"审计号 {audit_id} 没有对应记录")
        raise HTTPException(
            status_code=409,
            detail=f"审计号 {audit_id} 属于 "
                   f"{record.get('producer') or record.get('source') or '未知来源'} 的"
                   f"{record.get('event', '')}事件，不是导出完成事件",
        )
    return result


@router.get("/export_task/{export_id}/audit")
def export_audit(export_id: int) -> dict[str, Any]:
    """按导出任务查询其完成事件，不依赖候选编号。"""
    service = service_for("export_task")
    if service is None or not hasattr(service, "audit_for_export"):
        raise HTTPException(status_code=404, detail="导出任务服务不可用")
    record = service.audit_for_export(export_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"导出任务 {export_id} 尚无完成事件")
    return record
