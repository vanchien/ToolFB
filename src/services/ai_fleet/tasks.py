"""Máy trạng thái tác vụ AI. Thành công chỉ khi chuyển trạng thái tường minh."""

from __future__ import annotations

import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from src.services.ai_fleet.store import FleetStore

STATES = ("queued", "running", "waiting_approval", "succeeded", "failed", "cancelled")
TRANSITIONS = {
    "queued": {"running", "cancelled"},
    "running": {"waiting_approval", "succeeded", "failed", "queued"},
    "waiting_approval": {"succeeded", "failed", "cancelled"},
    "failed": {"queued"},
    "succeeded": set(),
    "cancelled": set(),
}
_SECRET = re.compile(r"(sk-[A-Za-z0-9_\-]+|Bearer\s+\S+|api[_-]?key\s*[:=]\s*\S+)", re.IGNORECASE)


def enqueue(store: FleetStore, *, kind: str, page_id: str, detail: str = "") -> dict[str, Any]:
    """Tạo tác vụ ở hàng chờ. Chưa chạy và chưa thành công."""
    now = _now()
    task = {
        "task_id": f"task_{uuid.uuid4().hex[:12]}",
        "correlation_id": uuid.uuid4().hex[:12],
        "kind": str(kind or "write"),
        "page_id": str(page_id or ""),
        "state": "queued",
        "attempt": 1,
        "error_class": "",
        "error": "",
        "content_id": "",
        "detail": str(detail or "")[:240],
        "events": [{"at": now, "state": "queued", "note": "Vào hàng chờ"}],
        "created_at": now,
        "updated_at": now,
    }
    store.save_task(task)
    return task


def transition(store: FleetStore, task_id: str, state: str, *, note: str = "", **fields: Any) -> dict[str, Any]:
    """Chuyển trạng thái đúng bảng. Trạng thái lạ thì từ chối."""
    target = str(state or "")
    task = _require(store, task_id)
    current = str(task.get("state") or "")
    if target not in TRANSITIONS.get(current, set()):
        raise ValueError(f"Không chuyển {current} sang {target}.")
    task["state"] = target
    task["updated_at"] = _now()
    if target == "running":
        task["owner_pid"] = os.getpid()
    elif current == "running":
        task["owner_pid"] = 0
    for key, value in fields.items():
        task[key] = value
    task.setdefault("events", [])
    task["events"].append({"at": task["updated_at"], "state": target, "note": redact(note)[:300]})
    task["events"] = task["events"][-30:]
    store.save_task(task)
    return task


def recover_running(store: FleetStore) -> int:
    """Tác vụ running của process đã chết quay về hàng chờ. Việc của process này giữ nguyên."""
    recovered = 0

    def change(doc: dict[str, Any]) -> None:
        nonlocal recovered
        for task in doc.get("tasks") or []:
            if str(task.get("state") or "") != "running":
                continue
            if int(task.get("owner_pid") or 0) == os.getpid():
                continue
            task["state"] = "queued"
            task["owner_pid"] = 0
            task["updated_at"] = _now()
            task.setdefault("events", [])
            task["events"].append({"at": task["updated_at"], "state": "queued", "note": "Khôi phục sau khi ứng dụng mở lại"})
            task["events"] = task["events"][-30:]
            recovered += 1

    store.modify(change)
    return recovered


def classify_error(message: str) -> str:
    """Phân loại lỗi để biết có được thử lại hay không."""
    text = redact(message).casefold()
    if any(word in text for word in ("401", "403", "thiếu openai_api_key", "authentication", "unauthorized")):
        return "authentication"
    if any(word in text for word in ("429", "rate limit", "quota", "hết credit")):
        return "rate_limit"
    if any(word in text for word in ("500", "502", "503", "504", "timeout", "timed out")):
        return "retryable"
    if any(word in text for word in ("thiếu", "chưa có", "không có trong danh sách")):
        return "configuration"
    return "permanent"


def can_retry(task: dict[str, Any]) -> bool:
    """Chỉ lỗi mạng hoặc giới hạn tốc độ, và chưa quá 3 lần."""
    return str(task.get("state") or "") == "failed" and str(task.get("error_class") or "") in {"retryable", "rate_limit"} and int(task.get("attempt") or 1) < 3


def fail_task(store: FleetStore, task_id: str, exc: BaseException) -> dict[str, Any]:
    """Ghi lỗi đã che bí mật. Không kéo theo nhà cung cấp khác."""
    kind = classify_error(str(exc))
    return transition(store, task_id, "failed", note=str(exc), error_class=kind, error=redact(str(exc))[:300])


def retry_task(store: FleetStore, task_id: str) -> dict[str, Any]:
    """Đưa tác vụ lỗi được phép thử lại về hàng chờ."""
    task = _require(store, task_id)
    if not can_retry(task):
        raise ValueError("Tác vụ này không được thử lại.")
    return transition(store, task_id, "queued", note="Thử lại có kiểm soát", attempt=int(task.get("attempt") or 1) + 1, error="")


def note_progress(store: FleetStore, task_id: str, **fields: Any) -> None:
    """Ghi content_id khi bản thảo đã có, trước bước dễ lỗi phía sau. Không đổi trạng thái."""
    task = _require(store, task_id)
    for key, value in fields.items():
        task[key] = value
    task["updated_at"] = _now()
    store.save_task(task)


def redact(message: str) -> str:
    """Xóa khóa API khỏi chuỗi hiện cho người dùng và nhật ký."""
    return _SECRET.sub("[đã giấu]", str(message or ""))


def counts(store: FleetStore) -> dict[str, int]:
    """Đếm theo trạng thái thật, không suy ra từ việc tác vụ biến mất."""
    found = {state: 0 for state in STATES}
    for task in store.tasks():
        state = str(task.get("state") or "")
        if state in found:
            found[state] += 1
    return found


def _require(store: FleetStore, task_id: str) -> dict[str, Any]:
    for task in store.tasks():
        if str(task.get("task_id") or "") == task_id:
            return task
    raise ValueError("Không thấy tác vụ.")


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
