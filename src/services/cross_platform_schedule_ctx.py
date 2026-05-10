"""Cờ chung cho chuỗi job lịch Facebook + TikTok (worker pool không kế thừa ContextVar)."""

from __future__ import annotations

import threading

_lock = threading.Lock()
_active = False


def unified_chain_begin() -> None:
    global _active
    with _lock:
        _active = True


def unified_chain_end() -> None:
    global _active
    with _lock:
        _active = False


def unified_chain_is_active() -> bool:
    with _lock:
        return _active
