"""Thông báo sắp đăng, lọc quá hạn và dời giờ lịch đăng."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from src.services.schedule_alerts import (
    build_alert_snapshot,
    format_countdown,
    is_missed,
    job_time_utc,
    reschedule_jobs,
)
from src.utils.schedule_posts_manager import SchedulePostsManager

NOW = datetime(2026, 10, 17, 14, 0, tzinfo=timezone.utc)


def _job(jid: str, status: str, minutes: float, account: str = "acc1", **extra: object) -> dict:
    """Job hẹn đăng lệch ``minutes`` phút so với NOW."""
    row = {
        "id": jid,
        "page_id": f"page_{jid}",
        "account_id": account,
        "post_type": "video",
        "status": status,
        "scheduled_at": (NOW + timedelta(minutes=minutes)).isoformat(),
    }
    row.update(extra)
    return row


def test_missed_counts_unposted_jobs_past_grace_only() -> None:
    """Quá giờ + chưa đăng (kể cả lỗi/tạm dừng) mới là quá hạn; vừa tới giờ thì chưa."""
    assert is_missed(_job("a", "pending", -30), now=NOW)
    assert is_missed(_job("b", "failed", -30), now=NOW)
    assert is_missed(_job("c", "paused", -30), now=NOW)
    assert not is_missed(_job("d", "pending", -2), now=NOW), "còn trong 5 phút chờ scheduler nhận"
    assert not is_missed(_job("e", "success", -30), now=NOW)
    assert not is_missed(_job("f", "running", -30), now=NOW)
    assert not is_missed(_job("g", "pending", 30), now=NOW)


def test_snapshot_orders_running_first_then_by_time() -> None:
    """Bài đang đăng đứng đầu, sau đó theo giờ; bài quá hạn tách riêng; quá 24 giờ bỏ qua."""
    jobs = [
        _job("later", "pending", 90),
        _job("soon", "pending", 3),
        _job("run", "running", -1),
        _job("late", "pending", -60),
        _job("far", "pending", 60 * 30),
        _job("done", "success", -10),
    ]
    snap = build_alert_snapshot(jobs, now=NOW)
    assert [p.job_id for p in snap.upcoming] == ["run", "soon", "later"]
    assert snap.missed_ids == ["late"]
    assert snap.missed == 1 and snap.running == 1


def test_countdown_text() -> None:
    """Đếm ngược tiếng Việt dễ đọc."""
    assert format_countdown(NOW + timedelta(seconds=40), now=NOW) == "còn 40 giây"
    assert format_countdown(NOW + timedelta(minutes=12), now=NOW) == "còn 12 phút"
    assert format_countdown(NOW + timedelta(minutes=65), now=NOW) == "còn 1 giờ 05 phút"
    assert format_countdown(NOW - timedelta(minutes=1), now=NOW) == "tới giờ"
    assert format_countdown(NOW, now=NOW, status="running") == "đang đăng"


def test_reschedule_resets_and_spaces_same_account() -> None:
    """Dời giờ: về pending, xóa lỗi, cùng tài khoản cách 2–10 phút, khác tài khoản cùng bắt đầu."""
    rows = [
        _job("a1", "failed", -120, retry_count=3, error_note="Timeout"),
        _job("a2", "pending", -90),
        _job("a3", "pending", -60),
        _job("b1", "need_manual_check", -30, account="acc2"),
        _job("busy", "running", -5),
        _job("ok", "success", -200),
    ]
    start = NOW + timedelta(minutes=10)
    changed = reschedule_jobs(
        rows, ["a1", "a2", "a3", "b1", "busy", "ok"], start_at=start, rng=random.Random(7), now=NOW
    )
    assert {r["id"] for r in changed} == {"a1", "a2", "a3", "b1"}, "đang chạy / đã đăng giữ nguyên"
    by_id = {r["id"]: r for r in rows}
    assert by_id["busy"]["status"] == "running" and by_id["ok"]["status"] == "success"
    for jid in ("a1", "a2", "a3", "b1"):
        assert by_id[jid]["status"] == "pending"
        assert by_id[jid]["retry_count"] == 0 and by_id[jid]["error_note"] == ""
    times = [job_time_utc(by_id[j]) for j in ("a1", "a2", "a3")]
    assert times[0] == start, "giữ thứ tự cũ, bài sớm nhất đăng đầu"
    gaps = [(b - a).total_seconds() / 60 for a, b in zip(times, times[1:])]
    assert all(2 <= g <= 10 for g in gaps), gaps
    assert job_time_utc(by_id["b1"]) == start


def test_reschedule_avoids_existing_slot_of_same_account() -> None:
    """Không đè lên lịch còn chờ khác của cùng tài khoản."""
    start = NOW + timedelta(minutes=10)
    rows = [_job("late", "pending", -60), _job("keep", "pending", 10)]
    reschedule_jobs(rows, ["late"], start_at=start, rng=random.Random(1))
    moved = job_time_utc(rows[0])
    assert moved is not None and abs((moved - start).total_seconds()) >= 120


def test_stuck_running_job_counts_as_missed_and_can_be_moved() -> None:
    """Job «running» quá 40 phút (app tắt giữa chừng) vào danh sách quá hạn, không hiện «Đang đăng»."""
    stuck = _job("stuck", "running", -200, running_started_at=(NOW - timedelta(minutes=190)).isoformat())
    live = _job("live", "running", -3, running_started_at=(NOW - timedelta(minutes=2)).isoformat())
    snap = build_alert_snapshot([stuck, live], now=NOW)
    assert snap.missed_ids == ["stuck"]
    assert [p.job_id for p in snap.upcoming] == ["live"] and snap.running == 1
    rows = [stuck, live]
    changed = reschedule_jobs(rows, ["stuck", "live"], start_at=NOW + timedelta(minutes=5), now=NOW)
    assert [r["id"] for r in changed] == ["stuck"]
    assert stuck["status"] == "pending" and "running_started_at" not in stuck
    assert live["status"] == "running"


def test_mutate_writes_once_and_skips_when_unchanged(tmp_path) -> None:
    """``mutate`` đọc–sửa–ghi trong một lần khóa; không đổi gì thì không ghi file."""
    mgr = SchedulePostsManager(tmp_path / "schedule_posts.json")
    mgr.upsert_many([_job("x", "failed", -60), _job("y", "success", -60)])
    before = mgr.file_path.stat().st_mtime_ns
    assert mgr.mutate(lambda rows: []) == []
    assert mgr.file_path.stat().st_mtime_ns == before
    start = NOW + timedelta(minutes=10)
    changed = mgr.mutate(lambda rows: reschedule_jobs(rows, ["x", "y"], start_at=start, now=NOW))
    assert [r["id"] for r in changed] == ["x"]
    reloaded = {r["id"]: r for r in mgr.reload_from_disk(force=True)}
    assert reloaded["x"]["status"] == "pending"
    assert reloaded["y"]["status"] == "success"


SLOTS = "05:00-05:30, 11:30-12:00, 18:00-18:30, 23:30"


def _local(day: int, hh: int, mm: int) -> str:
    """Giờ Việt Nam (UTC+7) ngày ``day`` tháng 10/2026 dạng ISO."""
    from zoneinfo import ZoneInfo

    return datetime(2026, 10, day, hh, mm, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh")).isoformat()


def _series(jid: str, page: str, status: str, at: str, **extra: object) -> dict:
    """Job thuộc chuỗi 4 bài/ngày theo khung giờ lặp."""
    row = {
        "id": jid,
        "page_id": page,
        "account_id": "acc1",
        "post_type": "video",
        "status": status,
        "scheduled_at": at,
        "timezone": "Asia/Ho_Chi_Minh",
        "schedule_daily_slots": SLOTS,
        "schedule_per_day": 4,
    }
    row.update(extra)
    return row


def test_overdue_series_job_moves_after_last_post_of_page() -> None:
    """Trễ quá 30 phút → sang khung kế tiếp sau bài cuối của Page; bài trễ ít giữ nguyên."""
    from zoneinfo import ZoneInfo

    from src.services.schedule_alerts import has_overdue_to_roll, roll_overdue_to_series_end

    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    rows = [
        _series("late", "P1", "ready_queue", _local(17, 18, 32)),
        _series("p1_a", "P1", "pending", _local(17, 23, 33)),
        _series("p1_b", "P1", "pending", _local(18, 5, 6)),
        _series("p1_last", "P1", "pending", _local(18, 18, 22)),
        _series("late2", "P2", "pending", _local(17, 18, 40)),
        _series("p2_last", "P2", "pending", _local(18, 18, 30)),
        _series("fresh", "P3", "ready_queue", (NOW - timedelta(minutes=10)).isoformat()),
    ]
    assert has_overdue_to_roll(rows, now=NOW)

    changed = roll_overdue_to_series_end(rows, now=NOW, rng=random.Random(1))

    by_id = {r["id"]: r for r in rows}
    assert sorted(r["id"] for r in changed) == ["late", "late2"]
    for jid, last in (("late", "p1_last"), ("late2", "p2_last")):
        row = by_id[jid]
        at = job_time_utc(row)
        assert at > job_time_utc(by_id[last]), "phải nằm sau bài cuối của Page"
        assert at.astimezone(tz).strftime("%Y-%m-%d") == "2026-10-18"
        assert at.astimezone(tz).hour == 23, "khung kế tiếp sau 18:xx là 23:30"
        assert row["status"] == "pending"
        assert "tự dời sang" in row["error_note"]
        assert row["rolled_from"]
    gap = abs((job_time_utc(by_id["late"]) - job_time_utc(by_id["late2"])).total_seconds()) / 60
    assert gap >= 2, "hai Page cùng tài khoản không đăng cùng phút"
    assert by_id["fresh"]["status"] == "ready_queue"
    assert not has_overdue_to_roll(rows, now=NOW)


def test_overdue_standalone_jobs_reschedule_without_dump() -> None:
    """Đăng lẻ quá giờ (mở lại sau sự cố): báo quá giờ, dời lịch, không dồn ready_queue."""
    from src.services.schedule_alerts import has_overdue_to_roll, roll_overdue_to_series_end

    rows = [
        _job("s1", "pending", -120, account="acc1"),
        _job("s2", "ready_queue", -90, account="acc1"),
        _job("soon", "pending", 30, account="acc1"),
        _job("fresh", "pending", -10, account="acc1"),  # chưa đủ 30 phút → không dời
    ]
    assert has_overdue_to_roll(rows, now=NOW)
    changed = roll_overdue_to_series_end(rows, now=NOW, rng=random.Random(2))
    by_id = {r["id"]: r for r in rows}
    assert {r["id"] for r in changed} == {"s1", "s2"}
    for jid in ("s1", "s2"):
        row = by_id[jid]
        assert row["status"] == "pending"
        assert job_time_utc(row) > NOW
        assert "Quá giờ" in row["error_note"]
        assert "không dồn" in row["error_note"]
        assert row["rolled_from"]
    # Hai bài cùng acc không cùng phút (không dồn).
    gap = abs((job_time_utc(by_id["s1"]) - job_time_utc(by_id["s2"])).total_seconds()) / 60
    assert gap >= 2
    assert by_id["fresh"]["status"] == "pending" and "rolled_from" not in by_id["fresh"]
    assert by_id["soon"]["status"] == "pending"
    assert not has_overdue_to_roll(rows, now=NOW)


def test_overdue_roll_disabled_with_zero_minutes() -> None:
    """SCHEDULE_OVERDUE_ROLL_MINUTES=0 → không dời, bài trễ vẫn đăng bù."""
    from src.services.schedule_alerts import roll_overdue_to_series_end

    rows = [_series("late", "P1", "pending", _local(17, 18, 32))]
    assert roll_overdue_to_series_end(rows, now=NOW, late_minutes=0) == []
    assert rows[0]["scheduled_at"] == _local(17, 18, 32)
