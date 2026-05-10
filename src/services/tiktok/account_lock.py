from __future__ import annotations

import threading
from typing import Callable

_global = threading.Lock()
_locks: dict[str, threading.Lock] = {}


def account_run_lock(account_id: str) -> threading.Lock:
    aid = str(account_id or "").strip()
    with _global:
        if aid not in _locks:
            _locks[aid] = threading.Lock()
        return _locks[aid]


def try_run_for_account(account_id: str, fn: Callable[[], None]) -> tuple[bool, str]:
    """
    Chạy ``fn`` nếu khóa account lấy được ngay.

    Returns:
        (ok, message) — message lỗi tiếng Việt nếu ok=False.
    """
    lock = account_run_lock(account_id)
    acquired = lock.acquire(blocking=False)
    if not acquired:
        return False, "Tài khoản này đang chạy job khác — đợi xong hoặc đóng browser."
    try:
        fn()
        return True, ""
    finally:
        lock.release()
