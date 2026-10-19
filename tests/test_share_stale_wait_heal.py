"""Hàng đống «Chưa tới lượt» + quét nguồn bị chặn — mô tả UI và heal batch dở."""

from __future__ import annotations

import threading
import time

from src.gui.share_tab import describe_page_jobs
from src.services.facebook_groups.source_watch import SourceState, SourceWatcher


def test_pending_without_live_step_asks_to_resume() -> None:
    """Không còn worker → không hiện «Chưa tới lượt» mãi."""
    label, detail, tag = describe_page_jobs(
        [{"status": "PENDING", "error_message": ""}],
        "",
        "",
        wave_active=False,
    )
    assert label == "Đang chờ"
    assert "Chờ chạy tiếp" in detail
    assert tag == "wait"


def test_pending_while_wave_active_keeps_turn_label() -> None:
    label, detail, tag = describe_page_jobs(
        [{"status": "PENDING"}],
        "",
        "",
        wave_active=True,
    )
    assert detail == "Chưa tới lượt"
    assert tag == "wait"


def test_pending_with_live_step_shows_running() -> None:
    label, detail, tag = describe_page_jobs(
        [{"status": "PENDING"}],
        "",
        "Đang mở Page…",
    )
    assert label == "Đang chạy"
    assert tag == "run"


def test_wait_idle_does_not_block_when_share_busy(monkeypatch) -> None:
    """TK đang chia sẻ → vẫn cho quét (bản sao), không chờ hàng giờ."""
    monkeypatch.setattr(
        "src.utils.feature_busy.busy_reason",
        lambda **_k: "Tài khoản đang chia sẻ",
    )
    notes: list[str] = []
    watcher = SourceWatcher(
        sources=["https://www.facebook.com/P"],
        account={"id": "acc1"},
        states={},
        scan_min=5,
        scan_max=5,
        scan_fn=lambda *_a: {"ok": True, "items": []},
        on_new=lambda *_a: None,
        on_status=notes.append,
    )
    assert watcher._wait_account_browser_idle() is True
    assert notes and "bản sao" in notes[0]


def test_share_idle_clears_wait_even_if_wave_flag_set() -> None:
    label, detail, tag = describe_page_jobs(
        [{"status": "PENDING"}],
        "",
        "",
        wave_active=True,
        share_idle=True,
    )
    assert (label, detail, tag) == ("", "", "")


def test_loop_retries_when_scan_lock_busy() -> None:
    """Tới giờ mà force_scan đang giữ khóa — không nghỉ 15 phút như đã quét."""
    calls = {"n": 0}

    def scan_fn(_url: str, _acc: dict) -> dict:
        calls["n"] += 1
        return {"ok": True, "items": []}

    watcher = SourceWatcher(
        sources=["https://www.facebook.com/P"],
        account={"id": "acc"},
        states={},
        scan_min=5,
        scan_max=5,
        scan_fn=scan_fn,
        on_new=lambda *_a: None,
    )
    watcher._scan_lock.acquire()
    try:
        assert watcher.scan_once() == 0
        assert watcher._last_scan_skipped is True
    finally:
        watcher._scan_lock.release()
    assert watcher.scan_once() == 0
    assert watcher._last_scan_skipped is False
    assert calls["n"] == 1


def test_watcher_is_overdue_when_due_passed() -> None:
    watcher = SourceWatcher(
        sources=["https://www.facebook.com/P"],
        account={"id": "acc"},
        states={},
        scan_min=1,
        scan_max=1,
        scan_fn=lambda *_a: {"ok": True, "items": []},
        on_new=lambda *_a: None,
    )
    watcher._due_at_mono = time.monotonic() - 1
    assert watcher.is_overdue() is True
    watcher._scan_lock.acquire()
    try:
        assert watcher.is_overdue() is False
        watcher._scan_started_mono = time.monotonic() - 400
        assert watcher.is_scan_stuck(timeout_sec=60) is True
    finally:
        watcher._scan_lock.release()
    watcher._due_at_mono = time.monotonic() + 600
    watcher._last_cycle_mono = time.monotonic()
    assert watcher.is_overdue() is False


def test_force_scan_runs_while_loop_would_sleep() -> None:
    """Quét ngay chạy scan_once trên luồng riêng, có khóa chống chồng."""
    from src.services.facebook_groups.source_watch import detect_new_videos, source_state_key

    calls = {"n": 0}

    def scan_fn(_url: str, _acc: dict) -> dict:
        calls["n"] += 1
        return {
            "ok": True,
            "items": [{"url": "https://www.facebook.com/reel/1000000500"}],
        }

    source = "https://www.facebook.com/Src"
    watcher = SourceWatcher(
        sources=[source],
        account={"id": "acc"},
        states={},
        scan_min=60,
        scan_max=60,
        scan_fn=scan_fn,
        on_new=lambda *_a: None,
    )
    detect_new_videos(
        watcher.states.setdefault(source_state_key(source), SourceState()),
        ["https://www.facebook.com/reel/1000000400"],
    )
    assert watcher.force_scan() is True
    deadline = time.time() + 5.0
    while calls["n"] < 1 and time.time() < deadline:
        time.sleep(0.05)
    assert calls["n"] >= 1
    assert watcher.force_scan() is True
