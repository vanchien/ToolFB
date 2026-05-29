"""
Tự động tối ưu đa tác vụ: download + video editor + lịch đăng + browser.

- ``apply_multi_task_defaults()`` — mức cơ sở theo CPU khi khởi động.
- ``workload_begin`` / ``workload_end`` — theo dõi hoạt động nền.
- ``reconcile_multi_task_limits()`` — hạ slot browser/FFmpeg khi nhiều chức năng cùng chạy.
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from typing import Any, Generator

from loguru import logger

KIND_DOWNLOAD = "download"
KIND_VIDEO_EDITOR = "video_editor"
KIND_SCHEDULER = "scheduler"
KIND_BROWSER_POST = "browser_post"

_ALL_KINDS = (KIND_DOWNLOAD, KIND_VIDEO_EDITOR, KIND_SCHEDULER, KIND_BROWSER_POST)

_lock = threading.RLock()
_counts: dict[str, int] = {k: 0 for k in _ALL_KINDS}
_base_browser = 3
_base_ffmpeg = 2
_base_dispatch = 4
_last_applied: dict[str, str] = {}


def _truthy(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_setdefault(name: str, value: str) -> None:
    if not str(os.environ.get(name, "")).strip():
        os.environ[name] = str(value)


def _cpu_cores() -> int:
    try:
        return max(2, int(os.cpu_count() or 4))
    except Exception:
        return 4


def apply_multi_task_defaults(*, gui: bool = False) -> None:
    """Gợi ý cấu hình cơ sở — không ghi đè biến người dùng đã đặt."""
    global _base_browser, _base_ffmpeg, _base_dispatch
    cpu = _cpu_cores()
    browser = max(2, min(6, cpu // 2))
    dispatch = max(2, min(8, browser + 1))
    scheduler_pool = max(4, min(16, cpu))
    ffmpeg = 2 if cpu >= 6 else 1

    _base_browser = browser
    _base_ffmpeg = ffmpeg
    _base_dispatch = dispatch

    _env_setdefault("BROWSER_CONCURRENCY", str(browser))
    _env_setdefault("SCHEDULE_POSTS_DISPATCH_WORKERS", str(dispatch))
    _env_setdefault("SCHEDULER_POOL_THREADS", str(scheduler_pool))
    _env_setdefault("TOOLFB_FFMPEG_CONCURRENCY", str(ffmpeg))
    _env_setdefault("TOOLFB_AUTO_MULTITASK", "1")

    if gui:
        _env_setdefault("SCHEDULE_PER_ACCOUNT_MAX_PARALLEL", "1")
        _env_setdefault("SCHEDULE_PER_PAGE_MAX_PARALLEL", "1")
        _env_setdefault("SCHEDULE_ALLOW_SAME_ACCOUNT_PARALLEL", "0")

    _env_setdefault("SCHEDULE_POSTS_POLL_SEC", "10")
    _env_setdefault("SCHEDULE_DRAIN_QUEUE_ON_DONE", "1")
    reconcile_multi_task_limits()


def workload_begin(kind: str, *, units: int = 1) -> None:
    """Đánh dấu bắt đầu một hoạt động nền (download, render, …)."""
    k = str(kind or "").strip()
    if k not in _counts or units <= 0:
        return
    with _lock:
        _counts[k] += int(units)
    if _truthy("TOOLFB_AUTO_MULTITASK", default=True) and not _truthy("TOOLFB_MANUAL_CONCURRENCY"):
        reconcile_multi_task_limits()


def workload_end(kind: str, *, units: int = 1) -> None:
    """Đánh dấu kết thúc hoạt động nền."""
    k = str(kind or "").strip()
    if k not in _counts or units <= 0:
        return
    with _lock:
        _counts[k] = max(0, _counts[k] - int(units))
    if _truthy("TOOLFB_AUTO_MULTITASK", default=True) and not _truthy("TOOLFB_MANUAL_CONCURRENCY"):
        reconcile_multi_task_limits()


@contextmanager
def workload_scope(kind: str) -> Generator[None, None, None]:
    workload_begin(kind)
    try:
        yield
    finally:
        workload_end(kind)


def workload_snapshot(*, browser_slots_in_use: int = 0) -> dict[str, int]:
    """Ảnh chụp tải hiện tại (dùng monitor / test)."""
    with _lock:
        snap = dict(_counts)
    if browser_slots_in_use > 0:
        snap[KIND_BROWSER_POST] = max(snap.get(KIND_BROWSER_POST, 0), browser_slots_in_use)
    return snap


def _active_kind_count(snap: dict[str, int]) -> int:
    return sum(1 for k in _ALL_KINDS if int(snap.get(k, 0)) > 0)


def reconcile_multi_task_limits(*, browser_slots_in_use: int = 0) -> dict[str, str]:
    """
    Điều chỉnh ``BROWSER_CONCURRENCY`` / ``TOOLFB_FFMPEG_CONCURRENCY`` theo số chức năng đang chạy.

    Returns:
        Dict các biến env vừa áp dụng (rỗng nếu tắt auto).
    """
    global _last_applied
    if not _truthy("TOOLFB_AUTO_MULTITASK", default=True):
        return {}
    if _truthy("TOOLFB_MANUAL_CONCURRENCY"):
        return {}

    snap = workload_snapshot(browser_slots_in_use=browser_slots_in_use)
    kinds = _active_kind_count(snap)
    heavy = (
        int(snap.get(KIND_DOWNLOAD, 0))
        + int(snap.get(KIND_VIDEO_EDITOR, 0))
        + int(snap.get(KIND_BROWSER_POST, 0))
    )

    browser = _base_browser
    ffmpeg = _base_ffmpeg
    dispatch = _base_dispatch

    if kinds >= 2:
        browser = max(2, browser - 1)
        ffmpeg = max(1, ffmpeg - 1)
        dispatch = max(2, dispatch - 1)
    if kinds >= 3:
        browser = max(2, browser - 1)
        ffmpeg = 1
        dispatch = max(2, dispatch - 1)
    if heavy >= 3:
        browser = max(2, browser - 1)
        ffmpeg = 1

    if snap.get(KIND_DOWNLOAD, 0) and snap.get(KIND_BROWSER_POST, 0):
        browser = max(2, browser - 1)

    applied: dict[str, str] = {
        "BROWSER_CONCURRENCY": str(browser),
        "TOOLFB_FFMPEG_CONCURRENCY": str(ffmpeg),
        "SCHEDULE_POSTS_DISPATCH_WORKERS": str(dispatch),
    }
    for key, val in applied.items():
        os.environ[key] = val

    if applied != _last_applied:
        _last_applied = dict(applied)
        logger.info(
            "[Đa tác vụ] Tự tối ưu (kinds={} snap={}): browser={} ffmpeg={} dispatch={}",
            kinds,
            {k: v for k, v in snap.items() if v},
            browser,
            ffmpeg,
            dispatch,
        )
    return applied


def get_last_applied_limits() -> dict[str, str]:
    with _lock:
        return dict(_last_applied)
