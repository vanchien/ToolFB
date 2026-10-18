"""Chạy lịch số lượng lớn 24/7: giới hạn worker, giữ chỗ tài khoản, watchdog, dọn job cũ."""

from __future__ import annotations

import json
import time
from concurrent.futures import Future
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import src.scheduler as sch
from src.services import schedule_queue_dispatcher as sqd
from src.utils.schedule_posts_manager import SchedulePostsManager


def _row(jid: str, account_id: str, *, status: str = "pending", scheduled_at: str, **extra: object) -> dict:
    """Một job schedule_posts hợp lệ tối thiểu."""
    row = {
        "id": jid,
        "account_id": account_id,
        "page_id": f"page_{jid}",
        "post_type": "video",
        "status": status,
        "scheduled_at": scheduled_at,
        "created_at": scheduled_at,
    }
    row.update(extra)
    return row


@pytest.fixture(autouse=True)
def _clean_dispatch_state():
    """Mỗi test bắt đầu với bộ đếm giữ chỗ / slot tài khoản trống."""
    pending_before = dict(sch._dispatch_pending_by_engine)
    sch._dispatch_tracked.clear()
    sch._dispatch_abandoned.clear()
    sch._account_run_inflight.clear()
    sch._scheduler_stop.clear()
    yield
    sch._scheduler_stop.clear()
    sch._dispatch_tracked.clear()
    sch._dispatch_abandoned.clear()
    sch._account_run_inflight.clear()
    sch._dispatch_pending_by_engine.clear()
    sch._dispatch_pending_by_engine.update(pending_before)


class _FakePool:
    """Pool giả: nhận job nhưng không chạy (mô phỏng worker đang bận)."""

    def __init__(self, workers: int) -> None:
        self._max_workers = workers
        self.submitted: list[str] = []

    def submit(self, fn, *args, **kwargs):  # noqa: ANN001, ANN201
        self.submitted.append(str(kwargs.get("schedule_post_job_id")))
        return Future()


def _patch_tick(monkeypatch: pytest.MonkeyPatch, sp: SchedulePostsManager, pool: _FakePool) -> None:
    """Cô lập tick khỏi browser, GUI và file runtime thật."""
    monkeypatch.setattr(sch, "get_default_schedule_posts_manager", lambda: sp)
    monkeypatch.setattr(sch, "get_schedule_posts_dispatch_pool", lambda: pool)
    monkeypatch.setattr(sch, "ensure_schedule_queue_recovery", lambda _sp: None)
    monkeypatch.setattr(sch, "_maybe_archive_finished_jobs", lambda _sp: None)
    monkeypatch.setattr(sch, "_draft_id_for_queue_job", lambda _j: "")
    monkeypatch.setattr(sch, "_resolve_engine_for_account", lambda _a, accounts=None: "firefox")
    monkeypatch.setattr(sch, "write_queue_monitor", lambda **_k: None)
    monkeypatch.setattr(sch, "compute_smart_delay_ms", lambda **_k: 0)
    monkeypatch.setattr(sqd, "_max_consecutive_per_account", lambda: 0)


def test_dispatch_never_exceeds_free_workers_and_keeps_rest_in_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """20 tài khoản đến hạn, 3 worker → chỉ 3 job running, 17 job vẫn ready_queue."""
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    sp = SchedulePostsManager(json_path=tmp_path / "schedule_posts.json")
    sp.upsert_many([_row(f"j{i}", f"acc{i}", scheduled_at=past) for i in range(20)])
    pool = _FakePool(3)
    _patch_tick(monkeypatch, sp, pool)

    sch.tick_schedule_post_jobs()
    assert len(pool.submitted) == 3
    statuses = [r["status"] for r in sp.load_all()]
    assert statuses.count("running") == 3
    assert statuses.count("ready_queue") == 17

    sch.tick_schedule_post_jobs()
    assert len(pool.submitted) == 3, "worker còn bận thì không giao thêm"


def test_same_account_not_dispatched_twice_while_waiting_for_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Job đã giao nhưng chưa mở browser vẫn giữ tài khoản — Page kế của cùng TK phải chờ."""
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    sp = SchedulePostsManager(json_path=tmp_path / "schedule_posts.json")
    sp.upsert_many([_row("a", "acc1", scheduled_at=past), _row("b", "acc1", scheduled_at=past)])
    pool = _FakePool(8)
    _patch_tick(monkeypatch, sp, pool)

    sch.tick_schedule_post_jobs()
    sch.tick_schedule_post_jobs()
    assert pool.submitted == ["a"]
    assert sp.get_by_id("b")["status"] == "ready_queue"
    assert sp.get_by_id("a").get("running_started_at")


def test_watchdog_frees_account_of_hung_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Job treo quá giới hạn → failed, tài khoản được nhả cho Page tiếp theo."""
    now_iso = datetime.now(timezone.utc).isoformat()
    sp = SchedulePostsManager(json_path=tmp_path / "schedule_posts.json")
    sp.upsert(_row("hung", "acc1", status="running", scheduled_at=now_iso, running_started_at=now_iso))
    monkeypatch.setenv("SCHEDULE_JOB_MAX_MINUTES", "10")
    sch._account_run_inflight["acc1"] = 1
    sch._dispatch_tracked["hung"] = ("acc1", time.monotonic() - 11 * 60)

    assert sch._account_busy_count("acc1") == 1
    sch._watchdog_hung_and_orphan_jobs(sp)

    assert sp.get_by_id("hung")["status"] == "failed"
    assert sch._account_busy_count("acc1") == 0
    assert sch._dispatch_is_abandoned("hung")


def test_watchdog_marks_orphan_running_failed(tmp_path: Path) -> None:
    """Job 'running' không còn worker (quá 5 phút) → failed; job đang chạy thật thì giữ nguyên."""
    old = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
    sp = SchedulePostsManager(json_path=tmp_path / "schedule_posts.json")
    sp.upsert_many(
        [
            _row("orphan", "acc1", status="running", scheduled_at=old, running_started_at=old),
            _row("alive", "acc2", status="running", scheduled_at=old, running_started_at=old),
        ]
    )
    sch._dispatch_tracked["alive"] = ("acc2", time.monotonic())

    sch._watchdog_hung_and_orphan_jobs(sp)

    assert sp.get_by_id("orphan")["status"] == "failed"
    assert sp.get_by_id("alive")["status"] == "running"


def test_ram_cap_limits_browsers_on_small_machines() -> None:
    """Máy 8GB không mở quá 4 browser; 32GB không bị giới hạn bởi RAM."""
    from src.utils.concurrency_runtime import ram_browser_cap

    assert ram_browser_cap(8.0) == 4
    assert ram_browser_cap(4.0) == 1
    assert ram_browser_cap(32.0) >= 6
    assert ram_browser_cap(None) is None


def test_archive_moves_only_old_finished_jobs(tmp_path: Path) -> None:
    """Chỉ job success/cancelled cũ hơn N ngày bị chuyển; id vẫn tra được trong archive."""
    now = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)
    old = (now - timedelta(days=10)).isoformat()
    recent = (now - timedelta(days=1)).isoformat()
    sp = SchedulePostsManager(json_path=tmp_path / "schedule_posts.json")
    sp.upsert_many(
        [
            _row("old_ok", "a", status="success", scheduled_at=old, posted_at=old),
            _row("new_ok", "a", status="success", scheduled_at=recent, posted_at=recent),
            _row("old_fail", "a", status="failed", scheduled_at=old),
            _row("old_pending", "a", status="pending", scheduled_at=old),
        ]
    )
    archive = tmp_path / "archive.jsonl"

    n = sqd.archive_finished_jobs(sp, older_than_days=7, now=now, archive_path=archive)

    assert n == 1
    assert sorted(r["id"] for r in sp.load_all()) == ["new_ok", "old_fail", "old_pending"]
    assert json.loads(archive.read_text(encoding="utf-8").strip())["id"] == "old_ok"
    assert sqd.archived_job_ids(archive_path=archive) == frozenset({"old_ok"})


def test_stop_blocks_new_dispatch_and_drain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Bấm Dừng: tick không giao job, drain sau job xong không quét lại."""
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    sp = SchedulePostsManager(json_path=tmp_path / "schedule_posts.json")
    sp.upsert_many([_row("a", "acc1", scheduled_at=past)])
    pool = _FakePool(4)
    _patch_tick(monkeypatch, sp, pool)
    ticks: list[int] = []
    monkeypatch.setattr(sch, "tick_schedule_post_jobs", lambda: ticks.append(1))

    sch.request_scheduler_stop()
    sch._queue_last_drain_mono = 0.0
    sch._maybe_drain_queue_after_job_done()

    assert ticks == []
    monkeypatch.undo()
    _patch_tick(monkeypatch, sp, pool)
    sch.tick_schedule_post_jobs()
    assert pool.submitted == []
    assert sp.get_by_id("a")["status"] == "pending"


def test_job_waiting_in_pool_returns_to_queue_after_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Job đã giao nhưng còn chờ worker khi bấm Dừng → không mở browser, trả về ready_queue."""
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    sp = SchedulePostsManager(json_path=tmp_path / "schedule_posts.json")
    sp.upsert_many([_row("a", "acc1", status="running", scheduled_at=past, running_started_at=past)])
    monkeypatch.setattr(sch, "get_default_schedule_posts_manager", lambda: sp)
    calls: list[str] = []
    monkeypatch.setattr(sch, "run_scheduled_post_for_account", lambda *a, **k: calls.append("run") or True)
    sch._track_dispatch("a", "acc1")

    sch.request_scheduler_stop()
    ok = sch._run_dispatched_post_job("acc1", page_id="page_a", draft_id=None, schedule_post_job_id="a")

    assert ok is False
    assert calls == []
    assert sp.get_by_id("a")["status"] == "ready_queue"
    assert sch.scheduler_inflight_count() == 0


def test_tick_rolls_overdue_series_job_instead_of_posting_late(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Job chuỗi trễ 3 giờ: tick dời ra sau bài cuối của Page, không giao đi đăng bù."""
    now = datetime.now(timezone.utc)
    slots = "05:00-05:30, 11:30-12:00, 18:00-18:30, 23:30"
    common = {"schedule_daily_slots": slots, "schedule_per_day": 4, "timezone": "Asia/Ho_Chi_Minh"}
    sp = SchedulePostsManager(json_path=tmp_path / "schedule_posts.json")
    sp.upsert_many(
        [
            _row("late", "acc1", status="ready_queue", scheduled_at=(now - timedelta(hours=3)).isoformat(), **common),
            _row("next", "acc1", scheduled_at=(now + timedelta(hours=20)).isoformat(), **common),
        ]
    )
    sp.update_job_fields("next", page_id="page_late")
    pool = _FakePool(4)
    _patch_tick(monkeypatch, sp, pool)

    sch.tick_schedule_post_jobs()

    late = sp.get_by_id("late")
    assert pool.submitted == []
    assert late["status"] == "pending"
    assert late["scheduled_at"] > sp.get_by_id("next")["scheduled_at"]
