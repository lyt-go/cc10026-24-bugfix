"""跨模块共享数据管理器：读写 data/ 目录下的业务数据产物。

职责：
- 每个服务产出 domain_snapshot 后序列化到 data/{module}/snapshot.json
- 下游服务从 data/ 目录读取上游的标准化数据产物
- 审计记录写入 data/audit/
- 用量记录写入 data/usage/
- 导出任务结果写入 data/exports/
- API 注册快照写入 data/api/
- 订阅审批记录写入 data/subscriptions/
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:  # Linux 下用文件锁串行化编号分配；其他平台退化为进程内锁。
    import fcntl
except ImportError:  # pragma: no cover - 仅非 POSIX 平台走到
    fcntl = None  # type: ignore[assignment]

# 数据目录相对于仓库根
DATA_ROOT = Path(os.environ.get("DATA_ROOT", str(
    Path(__file__).resolve().parents[3] / "data"
)))

# 东八区
TZ = timezone(timedelta(hours=8))


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def _write_json(path: Path, data: Any) -> None:
    _ensure_dir(path.parent)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _now_iso() -> str:
    return datetime.now(TZ).isoformat()


def _atomic_write_json(path: Path, data: Any) -> None:
    """先写临时文件再原子替换，避免进程中断留下半截 JSON。"""
    _ensure_dir(path.parent)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        tmp_path = Path(tmp_name)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


# ========== 审计事件身份 ==========
#
# 导出事件和普通调用原先各自用「目录里最大编号 + 1」猜一个候选号，
# 再按编号直接覆盖写同一个文件。只要两条链路在同一窗口取到同一个
# 候选号，后写的一方就会把先写的一方（例如导出证据）从调用审计里抹掉。
#
# 这里把「编号」拆成两个概念：
# - candidate_id：写入方自己算出的候选号，只用于建议落盘位置；
# - audit_id：在全局审计命名空间里真正分配到的编号。
#
# 分配通过 _claims/ 下的占位凭据完成：占位创建成功才视为拿到该编号，
# 已被别的生产者占用时顺延到下一个空号。提交时仍校验 event_id，
# 因此未完成的提交或进程重启都不会把另一方的记录当成本次结果。

AUDIT_PRODUCER_UNKNOWN = "unknown"


class AuditCollisionError(RuntimeError):
    """候选编号已被另一个审计事件占用，且不能安全改写。"""


_AUDIT_THREAD_LOCK = threading.RLock()
_AUDIT_LOCK_STATE = threading.local()


def _audit_dir() -> Path:
    return DATA_ROOT / "audit"


def _audit_claims_dir() -> Path:
    return _audit_dir() / "_claims"


def _audit_path(audit_id: int) -> Path:
    return _audit_dir() / f"{int(audit_id):06d}.json"


def _audit_rel_path(audit_id: int) -> str:
    return f"audit/{int(audit_id):06d}.json"


def _claim_path(audit_id: int) -> Path:
    return _audit_claims_dir() / f"{int(audit_id):06d}.claim"


def _read_claim(audit_id: int) -> dict[str, Any] | None:
    data = _read_json(_claim_path(audit_id), None)
    return data if isinstance(data, dict) else None


class _audit_guard:
    """进程内线程锁 + 跨进程文件锁，保证编号分配与提交互斥。

    同一线程可重入：内层不再重复 flock（同进程不同 fd 的 flock 会互锁）。
    """

    def __enter__(self) -> "_audit_guard":
        self._thread_lock = _AUDIT_THREAD_LOCK
        self._thread_lock.acquire()
        self._nested = getattr(_AUDIT_LOCK_STATE, "depth", 0) > 0
        _AUDIT_LOCK_STATE.depth = getattr(_AUDIT_LOCK_STATE, "depth", 0) + 1
        self._lock_handle = None
        if not self._nested:
            _ensure_dir(_audit_claims_dir())
            self._lock_handle = open(_audit_dir() / ".audit.lock", "a+", encoding="utf-8")
            if fcntl is not None:
                fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if self._lock_handle is not None:
                if fcntl is not None:
                    fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
                self._lock_handle.close()
        finally:
            _AUDIT_LOCK_STATE.depth = max(getattr(_AUDIT_LOCK_STATE, "depth", 1) - 1, 0)
            self._thread_lock.release()


def _claimed_audit_ids() -> set[int]:
    ids: set[int] = set()
    claims_dir = _audit_claims_dir()
    if claims_dir.exists():
        for path in claims_dir.glob("[0-9]*.claim"):
            try:
                ids.add(int(path.stem))
            except ValueError:
                continue
    return ids


def _stored_audit_ids() -> set[int]:
    ids: set[int] = set()
    audit_dir = _audit_dir()
    if audit_dir.exists():
        for path in audit_dir.glob("[0-9]*.json"):
            try:
                ids.add(int(path.stem))
            except ValueError:
                continue
    return ids


def next_audit_id(candidate_id: int | None = None) -> int:
    """返回第一个未被审计记录或占位凭据使用的编号。

    传入 candidate_id 时把它当作起点；被占用则顺延。这里不加锁，
    真正的占位由 reserve_audit 再次校验并独占创建。
    """
    used = _stored_audit_ids() | _claimed_audit_ids()
    candidate = max(1, int(candidate_id)) if candidate_id is not None else max(used, default=0) + 1
    while candidate in used:
        candidate += 1
    return candidate


allocate_audit_id = next_audit_id


def _identity_of(data: dict[str, Any]) -> tuple[str, str, str, str]:
    producer = str(
        data.get("producer") or data.get("source") or data.get("origin")
        or data.get("caller") or AUDIT_PRODUCER_UNKNOWN)
    event = str(data.get("event") or data.get("kind") or "")
    business_key = str(data.get("business_key") or data.get("businessKey") or "")
    event_id = str(data.get("event_id") or data.get("eventId") or "")
    return producer, event, business_key, event_id


def _same_identity(data: dict[str, Any], producer: str, event: str,
                   business_key: str, event_id: str) -> bool:
    """判断已落盘记录/占位凭据是否属于本次要写入的同一个事件。

    只认 event_id，或 (producer, event, business_key) 三元组。
    绝不因为编号相同就认为记录是自己的。
    """
    if not isinstance(data, dict):
        return False
    other_producer, other_event, other_key, other_id = _identity_of(data)
    if event_id and other_id:
        return other_id == event_id and (
            not producer or other_producer in (producer, AUDIT_PRODUCER_UNKNOWN))
    if business_key and other_key:
        return (other_key == business_key and other_producer == producer
                and (not event or not other_event or other_event == event))
    return False


def _find_existing_event(producer: str, event: str, business_key: str,
                         event_id: str) -> dict[str, Any] | None:
    if not business_key and not event_id:
        return None
    for audit_id in sorted(_stored_audit_ids()):
        record = _read_json(_audit_path(audit_id), None)
        if _same_identity(record or {}, producer, event, business_key, event_id):
            record = dict(record or {})
            record.setdefault("audit_id", audit_id)
            record["_storage_state"] = "committed"
            return record
    for audit_id in sorted(_claimed_audit_ids()):
        claim = _read_claim(audit_id) or {}
        if _same_identity(claim, producer, event, business_key, event_id):
            claim = dict(claim)
            claim["_storage_state"] = "pending"
            return claim
    return None


def _reservation_view(claim: dict[str, Any]) -> dict[str, Any]:
    view = dict(claim)
    audit_id = int(view.get("audit_id", 0))
    view.setdefault("reservation_id", view.get("event_id", ""))
    view.setdefault("path", str(_audit_path(audit_id)))
    view.setdefault("rel_path", _audit_rel_path(audit_id))
    return view


def reserve_audit(producer: str, *, event: str = "", candidate_id: int | None = None,
                  business_key: str = "", event_id: str = "",
                  payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """为一次审计事件独占一个落盘编号。

    - 同一 business_key/event_id 已提交时返回已提交记录（幂等重放）；
    - 同一事件已有未完成占位时返回该占位，供重启后继续提交；
    - 候选号被别的生产者占用时顺延，原编号继续指向原记录；
    - 未完成的占位不会被其他生产者的提交认领。
    """
    producer = str(producer or AUDIT_PRODUCER_UNKNOWN)
    with _audit_guard():
        existing = _find_existing_event(producer, event, business_key, event_id)
        if existing is not None:
            if existing.get("_storage_state") == "committed":
                record = dict(existing)
                record.pop("_storage_state", None)
                return _reservation_view({
                    "audit_id": record.get("audit_id"),
                    "candidate_id": record.get("candidate_id", record.get("audit_id")),
                    "producer": producer,
                    "event": event,
                    "business_key": business_key,
                    "event_id": record.get("event_id", event_id),
                    "status": "committed",
                    "payload": record,
                    "record": record,
                    "reallocated_from": record.get("reallocated_from"),
                })
            existing = dict(existing)
            existing.pop("_storage_state", None)
            return _reservation_view(existing)

        desired = max(1, int(candidate_id)) if candidate_id is not None else None
        probe = next_audit_id(desired)
        reallocated_from: int | None = None
        if desired is not None and probe != desired:
            reallocated_from = desired

        # 再次确认探测到的编号没有被别人刚刚占走（文件锁内通常不会变）。
        while _audit_path(probe).exists() or _claim_path(probe).exists():
            probe += 1
            if reallocated_from is None:
                reallocated_from = desired if desired is not None else probe - 1

        claim = {
            "audit_id": probe,
            "candidate_id": int(candidate_id) if candidate_id is not None else probe,
            "producer": producer,
            "event": event,
            "business_key": business_key,
            "event_id": event_id or uuid.uuid4().hex,
            "status": "reserved",
            "payload": payload or {},
            "reallocated_from": reallocated_from,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }
        _write_json(_claim_path(probe), claim)
        return _reservation_view(claim)


def stage_audit(reservation: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    """把本次事件的完整内容暂存到占位凭据，进程重启后可据此恢复。"""
    audit_id = int(reservation.get("audit_id", 0))
    event_id = str(reservation.get("event_id", ""))
    with _audit_guard():
        claim = _read_claim(audit_id)
        if claim is None:
            if _audit_path(audit_id).exists():
                reservation["status"] = "committed"
            return reservation
        if claim.get("event_id") != event_id:
            raise AuditCollisionError(
                f"审计号 {audit_id} 已被其他事件占位，不能暂存本次事件")
        claim["payload"] = payload
        claim["status"] = "staged"
        claim["updated_at"] = _now_iso()
        _write_json(_claim_path(audit_id), claim)
        return _reservation_view(claim)


def _commit_locked(reservation: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    audit_id = int(reservation.get("audit_id", 0))
    producer = str(reservation.get("producer") or AUDIT_PRODUCER_UNKNOWN)
    event = str(reservation.get("event") or "")
    business_key = str(reservation.get("business_key") or "")
    event_id = str(reservation.get("event_id") or "")

    claim = _read_claim(audit_id)
    if claim is not None and claim.get("event_id") != event_id:
        raise AuditCollisionError(
            f"审计号 {audit_id} 的占位属于其他事件，拒绝覆盖")

    existing = _read_json(_audit_path(audit_id), None)
    if isinstance(existing, dict) and existing:
        if event_id and existing.get("event_id") and existing.get("event_id") != event_id:
            # 占位丢失/编号被抢占：为本次事件重新登记，原编号保持不动。
            stolen_from = audit_id
            audit_id = next_audit_id(audit_id + 1)
            existing = _read_json(_audit_path(audit_id), None)
            reservation["reallocated_from"] = reservation.get("reallocated_from") or stolen_from
        elif not event_id and not _same_identity(existing, producer, event, business_key, ""):
            stolen_from = audit_id
            audit_id = next_audit_id(audit_id + 1)
            existing = _read_json(_audit_path(audit_id), None)
            reservation["reallocated_from"] = reservation.get("reallocated_from") or stolen_from

    record = dict(payload)
    record.setdefault("producer", producer)
    record.setdefault("source", producer)
    record.setdefault("event", event)
    if business_key:
        record.setdefault("business_key", business_key)
    record.setdefault("event_id", event_id)
    record["audit_id"] = audit_id
    record.setdefault("candidate_id", reservation.get("candidate_id", audit_id))
    if reservation.get("reallocated_from") is not None:
        record.setdefault("reallocated_from", reservation["reallocated_from"])
    record["storage"] = _audit_rel_path(audit_id)
    record.setdefault("status", "completed")
    record["_logged_at"] = _now_iso()

    if isinstance(existing, dict) and existing:
        if not event_id or not existing.get("event_id") or existing.get("event_id") == event_id:
            # 同一事件的幂等更新（如归档改状态），保留首次创建时间。
            record["_created_at"] = existing.get("_created_at", record["_logged_at"])
            record = {**existing, **record}
            record["audit_id"] = audit_id
            record["storage"] = _audit_rel_path(audit_id)
        else:  # pragma: no cover - 上面已重新登记
            raise AuditCollisionError(
                f"审计号 {audit_id} 已被其他事件占用，拒绝覆盖")
    else:
        record.setdefault("_created_at", _now_iso())

    source = record.get("production_source")
    if not isinstance(source, dict):
        record["production_source"] = {
            "producer": producer,
            "event": record.get("event", ""),
            "audit_id": audit_id,
            "business_key": record.get("business_key", ""),
        }
    else:
        source.setdefault("producer", producer)
        source.setdefault("event", record.get("event", ""))
        source.setdefault("audit_id", audit_id)

    _ensure_dir(_audit_dir())
    _atomic_write_json(_audit_path(audit_id), record)

    # 恢复补写或顺延编号时，把记录内嵌的附带证据补到最终编号下。
    evidence = record.get("export_evidence")
    if isinstance(evidence, dict) and evidence:
        evidence_file = _export_evidence_dir() / f"{audit_id:06d}.json"
        if not evidence_file.exists():
            _atomic_write_json(evidence_file, {
                **evidence,
                "audit_id": audit_id,
                "production_source": record.get("production_source", {}),
                "_created_at": record.get("_created_at", _now_iso()),
            })

    if claim is not None and claim.get("event_id") == event_id:
        _claim_path(int(reservation.get("audit_id", audit_id))).unlink(missing_ok=True)

    reservation["audit_id"] = audit_id
    reservation["status"] = "committed"
    reservation["record"] = record
    return record


def commit_audit(reservation: dict[str, Any], record: dict[str, Any] | None = None) -> dict[str, Any]:
    """提交审计事件；返回真正落盘的记录（含最终 audit_id）。"""
    if reservation.get("status") == "committed" and reservation.get("record"):
        return dict(reservation["record"])
    with _audit_guard():
        payload = record or reservation.get("payload") or {}
        claim = _read_claim(int(reservation.get("audit_id", 0)))
        if claim is not None and not payload:
            payload = claim.get("payload") or {}
        return _commit_locked(reservation, payload)


def abort_audit(reservation: dict[str, Any]) -> bool:
    """撤销未完成的占位；已提交记录不受影响。"""
    audit_id = int(reservation.get("audit_id", 0))
    event_id = str(reservation.get("event_id", ""))
    with _audit_guard():
        claim = _read_claim(audit_id)
        if claim is None:
            return False
        if event_id and claim.get("event_id") != event_id:
            return False
        _claim_path(audit_id).unlink(missing_ok=True)
        return True


def append_audit_event(producer: str, event: str, payload: dict[str, Any], *,
                       business_key: str = "", candidate_id: int | None = None,
                       event_id: str = "") -> dict[str, Any]:
    """一步完成占位与提交，供各联动增强统一写审计。"""
    reservation = reserve_audit(
        producer, event=event, candidate_id=candidate_id,
        business_key=business_key, event_id=event_id)
    if reservation.get("status") == "committed":
        record = dict(reservation.get("record") or {})
        return record
    # 先暂存完整内容，进程在提交前中断也能被恢复补写。
    stage_audit(reservation, payload)
    return commit_audit(reservation, payload)


def recover_pending_audits(producer: str | None = None) -> dict[str, Any]:
    """恢复进程中断留下的未完成审计写入。

    - 占位对应的记录已落盘：确认并清理占位；
    - 占位里有完整内容：在仍可用的编号（必要时顺延）上补写；
    - 只有空占位：释放编号，等待该事件重新登记。
    """
    recovered: list[dict[str, Any]] = []
    completed: list[dict[str, Any]] = []
    reregistered: list[dict[str, Any]] = []
    discarded: list[dict[str, Any]] = []

    with _audit_guard():
        for claim_id in sorted(_claimed_audit_ids()):
            claim = _read_claim(claim_id) or {}
            if producer and claim.get("producer") != producer:
                continue
            event_id = str(claim.get("event_id", ""))
            existing = _read_json(_audit_path(claim_id), None)
            if isinstance(existing, dict) and existing:
                if claim.get("event_id") and existing.get("event_id") == claim.get("event_id"):
                    _claim_path(claim_id).unlink(missing_ok=True)
                    completed.append(existing)
                    continue
                # 原编号已被别的已完成事件占据：本次事件重新登记。
                reservation = dict(claim)
                reservation["reallocated_from"] = reservation.get("reallocated_from") or claim_id
                payload = claim.get("payload") or {}
                new_id = next_audit_id(claim_id + 1)
                reservation["audit_id"] = new_id
                record = _commit_locked(reservation, payload)
                _claim_path(claim_id).unlink(missing_ok=True)
                reregistered.append(record)
                continue

            payload = claim.get("payload") or {}
            # 只有暂存过完整内容的占位才能自动补写；仅登记过身份、
            # 尚未产生结果的占位释放编号，等待该事件重新登记。
            if payload and claim.get("status") == "staged":
                # 占位本身就是对该编号的独占，没有别的记录占用时直接沿用。
                reservation = dict(claim)
                reservation["audit_id"] = claim_id
                record = _commit_locked(reservation, payload)
                _claim_path(claim_id).unlink(missing_ok=True)
                recovered.append(record)
            else:
                _claim_path(claim_id).unlink(missing_ok=True)
                discarded.append({
                    "audit_id": claim_id,
                    "producer": claim.get("producer", ""),
                    "event": claim.get("event", ""),
                    "event_id": event_id,
                    "reason": "占位未携带可恢复结果，需要重新登记该事件",
                })

    return {
        "ok": True,
        "recovered": recovered,
        "completed": completed,
        "reregistered": reregistered,
        "discarded": discarded,
        "recovered_count": len(recovered),
        "completed_count": len(completed),
        "reregistered_count": len(reregistered),
        "discarded_count": len(discarded),
    }


# 兼容命名：恢复未完成的审计写入
recover_incomplete_audits = recover_pending_audits
resume_pending_audits = recover_pending_audits


def _record_matches(record: dict[str, Any], *, producer: str | None = None,
                    event: str | None = None, business_key: str | None = None,
                    event_id: str | None = None) -> bool:
    if not isinstance(record, dict) or not record:
        return False
    if producer and str(record.get("producer") or "") != producer:
        return False
    if event and str(record.get("event") or "") != event:
        return False
    if business_key and str(record.get("business_key") or "") != business_key:
        return False
    if event_id and str(record.get("event_id") or "") != event_id:
        return False
    return True


def find_audits(*, producer: str | None = None, event: str | None = None,
                business_key: str | None = None,
                event_id: str | None = None) -> list[dict[str, Any]]:
    """按事件身份查找已提交的审计记录。"""
    matches: list[dict[str, Any]] = []
    for audit_id in sorted(_stored_audit_ids()):
        record = _read_json(_audit_path(audit_id), None)
        if _record_matches(record or {}, producer=producer, event=event,
                           business_key=business_key, event_id=event_id):
            record = dict(record or {})
            record.setdefault("audit_id", audit_id)
            matches.append(record)
    return matches


def update_audit(audit_id: int, *, producer: str, changes: dict[str, Any]) -> dict[str, Any]:
    """更新本生产者已落盘记录的附加字段，绝不改写其他生产者的记录。"""
    with _audit_guard():
        record = _read_json(_audit_path(audit_id), None)
        if not _record_matches(record or {}, producer=producer):
            return {}
        updated = {**record, **changes}
        updated["audit_id"] = audit_id
        updated["producer"] = producer
        updated["_updated_at"] = _now_iso()
        source = updated.get("production_source")
        if isinstance(source, dict):
            source.setdefault("producer", producer)
            source.setdefault("audit_id", audit_id)
        _atomic_write_json(_audit_path(audit_id), updated)
        return updated


# ========== API 注册快照 ==========


def write_api_snapshot(api_id: int, api_data: dict[str, Any]) -> None:
    """将单个 API 注册数据写入共享快照，供路由、缓存、限流等模块消费。"""
    snapshot = _read_json(DATA_ROOT / "api" / "registry.json", {})
    key = str(api_id)
    api_data["_synced_at"] = _now_iso()
    snapshot[key] = api_data
    _write_json(DATA_ROOT / "api" / "registry.json", snapshot)


def read_api_snapshot(api_id: int | None = None) -> dict[str, Any] | dict[str, dict[str, Any]]:
    """读取 API 注册快照。返回单个或全部。"""
    snapshot = _read_json(DATA_ROOT / "api" / "registry.json", {})
    if api_id is not None:
        return snapshot.get(str(api_id), {})
    return snapshot


def remove_api_snapshot(api_id: int) -> None:
    """标记 API 为下线。"""
    snapshot = _read_json(DATA_ROOT / "api" / "registry.json", {})
    key = str(api_id)
    if key in snapshot:
        snapshot[key]["status"] = "已下线"
        snapshot[key]["_deleted_at"] = _now_iso()
        _write_json(DATA_ROOT / "api" / "registry.json", snapshot)


# ========== 订阅审批 ==========


def write_subscription(sub_id: int, sub_data: dict[str, Any]) -> None:
    """写入订阅申请记录到共享目录。"""
    record = {
        "subscription_id": sub_id,
        **sub_data,
        "_created_at": sub_data.get("_created_at", _now_iso()),
        "_updated_at": _now_iso(),
    }
    _write_json(DATA_ROOT / "subscriptions" / f"{sub_id:06d}.json", record)


def read_subscription(sub_id: int) -> dict[str, Any]:
    return _read_json(DATA_ROOT / "subscriptions" / f"{sub_id:06d}.json", {})


def list_subscriptions() -> list[dict[str, Any]]:
    """列出所有订阅记录。"""
    _ensure_dir(DATA_ROOT / "subscriptions")
    records = []
    for f in sorted(DATA_ROOT.glob("subscriptions/*.json")):
        record = _read_json(f)
        if record:
            records.append(record)
    return records


def approve_subscription(sub_id: int, approved: bool, approver: str = "system") -> dict[str, Any]:
    """审批订阅，记录审批流程。"""
    record = read_subscription(sub_id)
    if not record:
        return {}
    record["approval_status"] = "已通过" if approved else "已驳回"
    record["approver"] = approver
    record["_approved_at"] = _now_iso()
    write_subscription(sub_id, record)
    return record


# ========== 用量计费 ==========


def write_usage(usage_id: int, usage_data: dict[str, Any]) -> None:
    """写入用量记录到共享目录。"""
    usage_data["_created_at"] = usage_data.get("_created_at", _now_iso())
    usage_data["_updated_at"] = _now_iso()
    _write_json(DATA_ROOT / "usage" / f"{usage_id:06d}.json", usage_data)


def read_usage(usage_id: int) -> dict[str, Any]:
    return _read_json(DATA_ROOT / "usage" / f"{usage_id:06d}.json", {})


def list_usage(billing_cycle: str | None = None) -> list[dict[str, Any]]:
    """列出用量记录，支持按计费周期过滤。"""
    _ensure_dir(DATA_ROOT / "usage")
    records = []
    for f in sorted(DATA_ROOT.glob("usage/*.json")):
        record = _read_json(f)
        if record:
            if billing_cycle is None or record.get("billing_cycle") == billing_cycle:
                records.append(record)
    return records


def settle_billing(billing_cycle: str, use_auto_settlement: bool = True) -> dict[str, Any]:
    """结算指定计费周期，生成结算汇总。"""
    records = list_usage(billing_cycle)
    total_amount = sum(float(r.get("amount", 0)) for r in records)
    total_calls = sum(int(r.get("call_count", 0)) for r in records)
    settlement = {
        "billing_cycle": billing_cycle,
        "total_amount": total_amount,
        "total_calls": total_calls,
        "record_count": len(records),
        "settled_at": _now_iso(),
        "auto_settlement": use_auto_settlement,
    }
    _write_json(DATA_ROOT / "usage" / f"_settlement_{billing_cycle}.json", settlement)

    for r in records:
        r["settlement_status"] = "已结算"
        r["_settled_at"] = _now_iso()
        write_usage(r.get("subscription_id", 0), r)
    return settlement


# ========== 调用审计 ==========


def write_audit(audit_id: int, audit_data: dict[str, Any]) -> None:
    """写入审计记录。

    编号已被另一个审计事件占用时拒绝覆盖，避免把对方的证据改写掉；
    同一事件的重复写入按幂等更新处理。
    """
    audit_id = int(audit_id)
    producer, event, business_key, event_id = _identity_of(audit_data)
    with _audit_guard():
        claim = _read_claim(audit_id)
        if claim is not None and not _same_identity(
                claim, producer, event, business_key, event_id):
            raise AuditCollisionError(
                f"审计号 {audit_id} 已被其他事件占位，保留原引用不被改写")
        existing = _read_json(_audit_path(audit_id), None)
        if isinstance(existing, dict) and existing:
            if not _same_identity(existing, producer, event, business_key, event_id):
                raise AuditCollisionError(
                    f"审计号 {audit_id} 已归属其他事件，拒绝覆盖："
                    f"{existing.get('producer', '')}/{existing.get('event', '')}")
            merged = {**existing, **audit_data}
            merged.setdefault("_created_at", existing.get("_created_at", _now_iso()))
            audit_data = merged
        audit_data["audit_id"] = audit_id
        audit_data.setdefault("event_id", event_id or (claim or {}).get("event_id", ""))
        audit_data.setdefault("producer", producer)
        audit_data.setdefault("source", producer)
        if business_key:
            audit_data.setdefault("business_key", business_key)
        audit_data["storage"] = _audit_rel_path(audit_id)
        audit_data.setdefault("status", "completed")
        audit_data["_logged_at"] = _now_iso()
        if not isinstance(existing, dict) or not existing:
            audit_data.setdefault("_created_at", _now_iso())
        audit_data.setdefault("production_source", {
            "producer": audit_data.get("producer", ""),
            "event": audit_data.get("event", ""),
            "audit_id": audit_id,
            "business_key": audit_data.get("business_key", ""),
        })
        _atomic_write_json(_audit_path(audit_id), audit_data)
        if claim is not None and claim.get("event_id") == audit_data.get("event_id"):
            _claim_path(audit_id).unlink(missing_ok=True)


def read_audit(audit_id: int) -> dict[str, Any]:
    record = _read_json(_audit_path(audit_id), None)
    if not isinstance(record, dict):
        return {}
    record.setdefault("audit_id", int(audit_id))
    record.setdefault("storage", _audit_rel_path(audit_id))
    # 恢复补写时证据内嵌在审计记录里，读的时候补回证据目录。
    evidence = record.get("export_evidence")
    if isinstance(evidence, dict) and evidence:
        evidence_file = _export_evidence_dir() / f"{int(audit_id):06d}.json"
        if not evidence_file.exists():
            _atomic_write_json(evidence_file, {
                **evidence,
                "audit_id": int(audit_id),
                "production_source": record.get("production_source", {}),
                "_created_at": record.get("_created_at", _now_iso()),
            })
    return record


def read_audit_by_identity(*, producer: str, event: str = "",
                          business_key: str = "", event_id: str = "") -> dict[str, Any]:
    """按事件身份读取审计记录，避免只凭编号误认成自己的结果。"""
    matches = find_audits(producer=producer or None, event=event or None,
                          business_key=business_key or None, event_id=event_id or None)
    return matches[-1] if matches else {}


# ========== 审计事件附带产物 ==========
#
# 导出证据与调用请求/响应各自落盘，导出完成事件和普通调用完成事件
# 永远不会指向同一个存储位置。文件按最终分配的审计号命名。


def _call_payload_dir() -> Path:
    return _audit_dir() / "calls"


def _export_evidence_dir() -> Path:
    return _audit_dir() / "export_evidence"


def write_call_payload(audit_id: int, request: dict[str, Any],
                       response: dict[str, Any]) -> str:
    """持久化一次普通调用的请求与响应，返回相对落盘位置。"""
    audit_id = int(audit_id)
    payload = {
        "audit_id": audit_id,
        "request": request,
        "response": response,
        "_created_at": _now_iso(),
    }
    _atomic_write_json(_call_payload_dir() / f"{audit_id:06d}.json", payload)
    return f"audit/calls/{audit_id:06d}.json"


def read_call_payload(audit_id: int) -> dict[str, Any]:
    payload = _read_json(_call_payload_dir() / f"{int(audit_id):06d}.json", None)
    return payload if isinstance(payload, dict) else {}


def write_export_evidence(audit_id: int, evidence: dict[str, Any]) -> str:
    """持久化导出完成证据，返回相对落盘位置。"""
    audit_id = int(audit_id)
    record = {**evidence, "audit_id": audit_id, "_created_at": _now_iso()}
    _atomic_write_json(_export_evidence_dir() / f"{audit_id:06d}.json", record)
    return f"audit/export_evidence/{audit_id:06d}.json"


def read_export_evidence(audit_id: int) -> dict[str, Any]:
    evidence = _read_json(_export_evidence_dir() / f"{int(audit_id):06d}.json", None)
    return evidence if isinstance(evidence, dict) else {}



def list_audit(api_name: str | None = None, caller: str | None = None,
               start_time: str | None = None, end_time: str | None = None,
               *, producer: str | None = None, event: str | None = None,
               business_key: str | None = None,
               event_id: str | None = None) -> list[dict[str, Any]]:
    """按条件查询审计记录。

    只读取真正的审计记录（_claims/ 与附带目录不算），并按事件身份
    可选过滤，避免把别的生产者的完成事件当成自己的。
    """
    records = []
    for audit_id in sorted(_stored_audit_ids()):
        f = _audit_path(audit_id)
        record = _read_json(f)
        if not record:
            continue
        if api_name and record.get("api_name") != api_name:
            continue
        if caller and record.get("caller") != caller:
            continue
        if start_time and record.get("_logged_at", "") < start_time:
            continue
        if end_time and record.get("_logged_at", "") > end_time:
            continue
        if not _record_matches(record, producer=producer, event=event,
                               business_key=business_key, event_id=event_id):
            continue
        record = dict(record)
        record.setdefault("audit_id", audit_id)
        record.setdefault("storage", _audit_rel_path(audit_id))
        records.append(record)
    return records


def archive_audit_records(before_date: str) -> dict[str, Any]:
    """归档指定日期之前的审计记录。

    只更新本模块生产且能确认身份的记录，绝不借用编号改写别人的记录。
    """
    records = list_audit()
    archived = [r for r in records if r.get("_logged_at", "")[:10] < before_date]
    archive_data = {
        "archive_date": _now_iso(),
        "before_date": before_date,
        "archived_count": len(archived),
        "records": archived,
    }
    _write_json(DATA_ROOT / "audit" / f"_archive_{before_date}.json", archive_data)
    # 标记已归档：按记录自身的 producer 与 audit_id 写回，调用方只改自己那一份。
    updated: list[dict[str, Any]] = []
    for r in archived:
        rid = int(r.get("audit_id", 0) or 0)
        producer = str(r.get("producer") or r.get("source") or "")
        if not rid or not producer:
            continue
        current = read_audit(rid)
        if str(current.get("producer") or current.get("source") or "") != producer:
            continue
        current["status"] = "已归档"
        current["audit_id"] = rid
        current["producer"] = producer
        current["source"] = producer
        current["storage"] = _audit_rel_path(rid)
        current["_updated_at"] = _now_iso()
        _atomic_write_json(_audit_path(rid), current)
        updated.append(current)
    archive_data["updated_count"] = len(updated)
    return archive_data


# ========== 导出任务 ==========


def write_export(export_id: int, export_data: dict[str, Any]) -> None:
    """写入导出任务结果。"""
    export_data["_created_at"] = export_data.get("_created_at", _now_iso())
    export_data["_updated_at"] = _now_iso()
    _write_json(DATA_ROOT / "exports" / f"{export_id:06d}.json", export_data)
    # 同时生成一份导出 CSV
    _write_export_csv(export_id, export_data)


def _write_export_csv(export_id: int, export_data: dict[str, Any]) -> None:
    """根据导出数据生成 CSV 文件头。"""
    columns = export_data.get("columns", [])
    rows = export_data.get("rows", [])
    csv_path = DATA_ROOT / "exports" / f"{export_id:06d}.csv"
    lines = [",".join(columns)]
    for row in rows:
        lines.append(",".join(str(v) for v in row))
    csv_path.write_text("\n".join(lines), encoding="utf-8")


def read_export(export_id: int) -> dict[str, Any]:
    return _read_json(DATA_ROOT / "exports" / f"{export_id:06d}.json", {})


def list_exports() -> list[dict[str, Any]]:
    _ensure_dir(DATA_ROOT / "exports")
    records = []
    for f in sorted(DATA_ROOT.glob("exports/[0-9]*.json")):
        record = _read_json(f)
        if record:
            records.append(record)
    return records


# ========== 模块数据快照 ==========


def write_module_snapshot(module: str, snapshot: dict[str, Any]) -> None:
    """各服务将自己的 domain_snapshot 写入共享目录，供跨模块消费。"""
    snapshot["_snapshot_at"] = _now_iso()
    _write_json(DATA_ROOT / module / "snapshot.json", snapshot)


def read_module_snapshot(module: str) -> dict[str, Any]:
    return _read_json(DATA_ROOT / module / "snapshot.json", {})


def list_module_snapshots() -> list[dict[str, Any]]:
    """读取所有模块的快照汇总。"""
    snapshots = []
    for module_dir in DATA_ROOT.iterdir():
        if module_dir.is_dir() and not module_dir.name.startswith("_"):
            snap = _read_json(module_dir / "snapshot.json")
            if snap:
                snapshots.append(snap)
    return snapshots