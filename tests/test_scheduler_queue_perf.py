"""Kiểm tra tối ưu RAM/IO cho scheduler queue (cache, idle skip, debounce)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from src.scheduler import (
    _maybe_drain_queue_after_job_done,
    _prune_prefetch_cache,
    _should_skip_idle_schedule_tick,
    tick_cross_platform_schedule_jobs,
)
from src.utils.schedule_posts_manager import SchedulePostsManager


def _job(
    jid: str,
    *,
    account_id: str = "acc1",
    page_id: str = "pg1",
    scheduled_at: str = "2099-01-01T00:00:00+00:00",
    status: str = "pending",
) -> dict:
    return {
        "id": jid,
        "account_id": account_id,
        "page_id": page_id,
        "post_type": "text",
        "scheduled_at": scheduled_at,
        "status": status,
    }


def test_reload_from_disk_uses_mtime_cache(tmp_path: Path) -> None:
    p = tmp_path / "schedule_posts.json"
    p.write_text(json.dumps([_job("a1")]), encoding="utf-8")
    mgr = SchedulePostsManager(json_path=p)
    first = mgr.load_all()
    second = mgr.reload_from_disk()
    assert first is not second
    assert len(first) == len(second) == 1
    assert mgr._rows_cache is not None


def test_get_by_id_uses_index(tmp_path: Path) -> None:
    p = tmp_path / "schedule_posts.json"
    p.write_text(
        json.dumps(
            [
                _job("j1"),
                _job("j2", status="ready_queue", scheduled_at="2099-01-01T01:00:00+00:00"),
            ]
        ),
        encoding="utf-8",
    )
    mgr = SchedulePostsManager(json_path=p)
    mgr.load_all()
    assert "j2" in mgr._id_index
    assert mgr.get_by_id("j2")["status"] == "ready_queue"


def test_has_active_queue_work(tmp_path: Path) -> None:
    p = tmp_path / "schedule_posts.json"
    p.write_text(json.dumps([_job("j1", status="ready_queue")]), encoding="utf-8")
    mgr = SchedulePostsManager(json_path=p)
    assert mgr.has_active_queue_work()


def test_prune_prefetch_cache_drops_stale_ids() -> None:
    import src.scheduler as sched

    sched._queue_prefetched_until_iso_by_job.clear()
    for i in range(300):
        sched._queue_prefetched_until_iso_by_job[f"old{i}"] = "t"
    sched._queue_prefetched_until_iso_by_job["keep1"] = "t"
    _prune_prefetch_cache([{"id": "keep1"}])
    assert "keep1" in sched._queue_prefetched_until_iso_by_job
    assert "old0" not in sched._queue_prefetched_until_iso_by_job


def test_should_skip_idle_schedule_tick_respects_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import src.scheduler as sched

    p = tmp_path / "schedule_posts.json"
    p.write_text("[]", encoding="utf-8")
    now = datetime.now(timezone.utc)
    sched._queue_idle_probe_after_utc = now + timedelta(seconds=600)
    sched._queue_hint_refresh_after_utc = now + timedelta(seconds=120)
    monkeypatch.setattr(
        sched,
        "get_default_schedule_posts_manager",
        lambda: SchedulePostsManager(json_path=p),
    )
    assert _should_skip_idle_schedule_tick() is True


def test_tick_cross_platform_skips_when_idle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.scheduler._should_skip_idle_schedule_tick",
        lambda: True,
    )
    with patch("src.scheduler.tick_schedule_post_jobs") as fb_tick:
        tick_cross_platform_schedule_jobs()
        fb_tick.assert_not_called()


def test_drain_debounce_blocks_rapid_ticks(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.scheduler as sched

    sched._queue_last_drain_mono = 1_000_000.0
    calls: list[int] = []

    def _fake_tick() -> None:
        calls.append(1)

    monkeypatch.setattr(sched, "tick_schedule_post_jobs", _fake_tick)
    monkeypatch.setenv("SCHEDULE_DRAIN_DEBOUNCE_SEC", "60")
    _maybe_drain_queue_after_job_done()
    assert calls == []
