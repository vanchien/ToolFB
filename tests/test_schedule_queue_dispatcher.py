"""Tests for schedule queue dispatcher (sort, promote, batch, recovery)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.services import schedule_queue_dispatcher as sqd
from src.utils.schedule_posts_manager import SchedulePostsManager


def _job(
    jid: str,
    *,
    account_id: str = "acc1",
    scheduled_at: str,
    created_at: str = "",
    status: str = "pending",
) -> dict:
    return {
        "id": jid,
        "account_id": account_id,
        "page_id": "page1",
        "status": status,
        "scheduled_at": scheduled_at,
        "created_at": created_at or scheduled_at,
    }


def test_sort_queue_jobs_by_schedule_then_created() -> None:
    t1 = "2026-05-19T10:00:00+00:00"
    t2 = "2026-05-19T10:00:00+00:00"
    jobs = [
        _job("b", scheduled_at=t1, created_at="2026-05-19T09:00:00+00:00", status="ready_queue"),
        _job("a", scheduled_at=t1, created_at="2026-05-19T08:00:00+00:00", status="ready_queue"),
        _job("c", scheduled_at="2026-05-19T11:00:00+00:00", status="ready_queue"),
    ]
    sorted_jobs = sqd.sort_queue_jobs(jobs)  # type: ignore[arg-type]
    assert [j["id"] for j in sorted_jobs] == ["a", "b", "c"]


def test_promote_due_pending_to_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = tmp_path / "schedule_posts.json"
    now = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    past = (now - timedelta(minutes=5)).isoformat()
    future = (now + timedelta(hours=1)).isoformat()
    cfg.write_text(
        json.dumps(
            [
                _job("due", scheduled_at=past, status="pending"),
                _job("later", scheduled_at=future, status="pending"),
            ],
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    sp = SchedulePostsManager(json_path=cfg)
    n = sqd.promote_due_pending_to_ready(sp, now=now)
    assert n == 1
    rows = sp.load_all()
    by_id = {r["id"]: r["status"] for r in rows}
    assert by_id["due"] == "ready_queue"
    assert by_id["later"] == "pending"


def test_select_dispatch_respects_account_busy() -> None:
    ready = [
        _job("j1", account_id="a1", scheduled_at="2026-05-19T10:00:00+00:00", status="ready_queue"),
        _job("j2", account_id="a1", scheduled_at="2026-05-19T10:01:00+00:00", status="ready_queue"),
        _job("j3", account_id="a2", scheduled_at="2026-05-19T10:02:00+00:00", status="ready_queue"),
    ]

    def inflight(aid: str) -> int:
        return 1 if aid == "a1" else 0

    batch = sqd.select_dispatch_batch(ready, account_inflight=inflight, per_account_limit=1)  # type: ignore[arg-type]
    assert [j["id"] for j in batch] == ["j3"]


def test_max_consecutive_per_account_defers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCHEDULE_MAX_CONSECUTIVE_PER_ACCOUNT", "2")
    sqd._account_consecutive_streak.clear()
    sqd._last_dispatched_account = ""
    ready = [
        _job("j1", account_id="a1", scheduled_at="2026-05-19T10:00:00+00:00", status="ready_queue"),
        _job("j2", account_id="a1", scheduled_at="2026-05-19T10:01:00+00:00", status="ready_queue"),
        _job("j3", account_id="a1", scheduled_at="2026-05-19T10:02:00+00:00", status="ready_queue"),
        _job("j4", account_id="a2", scheduled_at="2026-05-19T10:03:00+00:00", status="ready_queue"),
    ]
    batch1 = sqd.select_dispatch_batch(ready, account_inflight=lambda _a: 0, per_account_limit=1)  # type: ignore[arg-type]
    assert batch1[0]["id"] == "j1"
    sqd.note_job_dispatched("a1")
    batch2 = sqd.select_dispatch_batch(ready[1:], account_inflight=lambda _a: 0, per_account_limit=1)  # type: ignore[arg-type]
    assert batch2[0]["id"] == "j2"
    sqd.note_job_dispatched("a1")
    batch3 = sqd.select_dispatch_batch(ready[2:], account_inflight=lambda _a: 0, per_account_limit=1)  # type: ignore[arg-type]
    assert batch3[0]["id"] == "j4"


def test_recover_stale_running_on_startup(tmp_path: Path) -> None:
    cfg = tmp_path / "schedule_posts.json"
    cfg.write_text(
        json.dumps([_job("r1", scheduled_at="2026-05-19T10:00:00+00:00", status="running")], indent=2) + "\n",
        encoding="utf-8",
    )
    sp = SchedulePostsManager(json_path=cfg)
    n = sqd.recover_stale_running_jobs(sp, force_all_running=True)
    assert n == 1
    assert sp.get_by_id("r1")["status"] == "ready_queue"


def test_compute_smart_delay_scales_with_depth() -> None:
    assert sqd.compute_smart_delay_ms(ready_count=1, running_count=0) >= sqd.compute_smart_delay_ms(
        ready_count=20, running_count=5
    )
