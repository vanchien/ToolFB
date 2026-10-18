"""Nạp job chờ đăng vào lịch: chỉ fanpage đã chỉ định, không trùng, giữ phần còn lại."""

from __future__ import annotations

from pathlib import Path

from src.services.video_editor.page_split import apply_page_split
from src.services.video_editor.pending_import import (
    JOB_STATUS_IMPORTED,
    JOB_STATUS_PARTIAL,
    fb_page_owner_error,
    job_status_after_import,
    select_items_for_schedule,
)


def _items(tmp_path: Path, n: int) -> list[dict[str, str]]:
    out = []
    for i in range(n):
        p = tmp_path / f"v{i}.mp4"
        p.write_bytes(b"x")
        out.append({"video_path": str(p)})
    return out


def test_split_job_keeps_unassigned_for_later(tmp_path: Path) -> None:
    items = _items(tmp_path, 5)
    apply_page_split(items, [{"account_id": "a", "page_id": "p1"}], 2)
    picked = select_items_for_schedule(items)
    assert [i for i, _ in picked["entries"]] == [0, 1]
    assert picked["waiting_page"] == 3


def test_scheduled_items_are_not_imported_twice(tmp_path: Path) -> None:
    items = _items(tmp_path, 2)
    apply_page_split(items, [{"account_id": "a", "page_id": "p1"}], None)
    items[0]["item_scheduled_job_id"] = "sched_1"
    picked = select_items_for_schedule(items)
    assert [i for i, _ in picked["entries"]] == [1]
    assert picked["skipped_scheduled"] == 1
    assert job_status_after_import(items) == JOB_STATUS_PARTIAL
    items[1]["item_scheduled_job_id"] = "sched_2"
    assert job_status_after_import(items) == JOB_STATUS_IMPORTED
    assert len(select_items_for_schedule(items, reimport=True)["entries"]) == 2


def test_missing_file_is_skipped(tmp_path: Path) -> None:
    items = _items(tmp_path, 1) + [{"video_path": str(tmp_path / "gone.mp4")}]
    picked = select_items_for_schedule(items)
    assert picked["skipped_missing"] == 1
    assert len(picked["entries"]) == 1


def test_unsplit_job_uses_dialog_page(tmp_path: Path) -> None:
    items = _items(tmp_path, 3)
    picked = select_items_for_schedule(items)
    assert len(picked["entries"]) == 3
    assert picked["uses_split"] is False


def test_split_and_schedule_per_page_per_day(tmp_path: Path) -> None:
    from datetime import date, datetime
    from zoneinfo import ZoneInfo

    from src.services.video_editor.pending_import import schedule_split_chunks

    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    items = _items(tmp_path, 12)
    dests = [{"account_id": "a", "page_id": "p1"}, {"account_id": "a", "page_id": "p2"}]
    res = apply_page_split(items, dests, None)
    windows = [((8, 0), (10, 0)), ((14, 0), (16, 0)), ((20, 0), (22, 0))]
    now = datetime(2026, 10, 17, 0, 0, tzinfo=tz)
    rows = schedule_split_chunks(
        items,
        {},
        res["chunks"],
        start_date=date(2026, 10, 17),
        windows=windows,
        per_day=3,
        tz_name="Asia/Ho_Chi_Minh",
        now=now,
    )
    assert len(rows) == 12
    by_page: dict[str, list[datetime]] = {}
    for r in rows:
        by_page.setdefault(r["page_id"], []).append(
            datetime.fromisoformat(r["scheduled_at"]).astimezone(tz)
        )
    for times in by_page.values():
        assert len(times) == 6
        days: dict[date, int] = {}
        for t in times:
            days[t.date()] = days.get(t.date(), 0) + 1
            m = t.hour * 60 + t.minute
            assert any(a[0] * 60 + a[1] <= m <= b[0] * 60 + b[1] for a, b in windows)
        assert sorted(days.values()) == [3, 3]
        assert times == sorted(times)
        assert len({t.hour // 6 for t in times[:3]}) == 3
    assert all(it.get("item_scheduled_job_id") for it in items)
    assert job_status_after_import(items) == JOB_STATUS_IMPORTED


def test_next_split_continues_after_existing_page_schedule(tmp_path: Path) -> None:
    from datetime import date, datetime
    from zoneinfo import ZoneInfo

    from src.services.video_editor.pending_import import schedule_split_chunks

    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    items = _items(tmp_path, 2)
    res = apply_page_split(items, [{"account_id": "a", "page_id": "p1"}], None)
    existing = [
        {"page_id": "p1", "status": "pending", "scheduled_at": datetime(2026, 10, 17, 21, 0, tzinfo=tz).isoformat()}
    ]
    rows = schedule_split_chunks(
        items,
        {},
        res["chunks"],
        start_date=date(2026, 10, 17),
        windows=[((8, 0), (10, 0)), ((14, 0), (16, 0)), ((20, 0), (22, 0))],
        per_day=3,
        tz_name="Asia/Ho_Chi_Minh",
        existing_rows=existing,
        now=datetime(2026, 10, 17, 0, 0, tzinfo=tz),
    )
    first = datetime.fromisoformat(rows[0]["scheduled_at"]).astimezone(tz)
    assert first.date() == date(2026, 10, 18)


def test_pages_of_same_account_post_at_least_two_minutes_apart(tmp_path: Path) -> None:
    from datetime import date, datetime, timedelta
    from zoneinfo import ZoneInfo

    from src.services.video_editor.pending_import import schedule_split_chunks

    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    items = _items(tmp_path, 40)
    dests = [{"account_id": "a", "page_id": f"p{i}"} for i in range(13)] + [{"account_id": "b", "page_id": "q"}]
    res = apply_page_split(items, dests, None)
    existing = [
        {"account_id": "a", "page_id": "old", "status": "pending",
         "scheduled_at": datetime(2026, 10, 17, 9, 0, tzinfo=tz).isoformat()}
    ]
    rows = schedule_split_chunks(
        items,
        {},
        res["chunks"],
        start_date=date(2026, 10, 17),
        windows=[((9, 0), (9, 10))],
        per_day=1,
        tz_name="Asia/Ho_Chi_Minh",
        existing_rows=existing,
        now=datetime(2026, 10, 17, 0, 0, tzinfo=tz),
    )
    times = sorted(
        datetime.fromisoformat(r["scheduled_at"]) for r in rows if r["account_id"] == "a"
    ) + [datetime.fromisoformat(existing[0]["scheduled_at"])]
    times.sort()
    assert all(b - a >= timedelta(minutes=2) for a, b in zip(times, times[1:]))


def test_same_minute_pages_are_spread_two_minutes(tmp_path: Path) -> None:
    from datetime import date, datetime, timedelta
    from zoneinfo import ZoneInfo

    from src.services.video_editor.pending_import import schedule_split_chunks

    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    items = _items(tmp_path, 16)
    dests = [{"account_id": "a", "page_id": f"p{i}"} for i in range(16)]
    res = apply_page_split(items, dests, None)
    rows = schedule_split_chunks(
        items,
        {},
        res["chunks"],
        start_date=date(2026, 10, 18),
        windows=[((23, 30), (23, 30))],
        per_day=1,
        tz_name="Asia/Ho_Chi_Minh",
        now=datetime(2026, 10, 17, 0, 0, tzinfo=tz),
    )
    times = sorted(datetime.fromisoformat(r["scheduled_at"]).astimezone(tz) for r in rows)
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert len({t.strftime("%Y-%m-%d %H:%M") for t in times}) == 16
    assert all(timedelta(minutes=2) <= g <= timedelta(minutes=10) for g in gaps)
    assert times[0].strftime("%H:%M") == "23:30"


def test_respace_existing_schedules_same_account() -> None:
    from datetime import datetime, timedelta, timezone as tzm

    from src.services.video_editor.pending_import import respace_account_schedules

    t0 = datetime(2026, 10, 17, 11, 5, tzinfo=tzm.utc)
    rows = [
        {"id": f"s{i}", "account_id": "a", "page_id": f"p{i}", "status": "pending", "scheduled_at": t0.isoformat()}
        for i in range(5)
    ]
    rows.append({"id": "run", "account_id": "a", "page_id": "x", "status": "running", "scheduled_at": t0.isoformat()})
    rows.append({"id": "other", "account_id": "b", "page_id": "y", "status": "pending", "scheduled_at": t0.isoformat()})
    changed = respace_account_schedules(rows)
    assert {r["id"] for r in changed} == {f"s{i}" for i in range(5)}
    a_times = sorted(datetime.fromisoformat(r["scheduled_at"]) for r in rows if r["account_id"] == "a")
    assert all(b - a >= timedelta(minutes=2) for a, b in zip(a_times, a_times[1:]))
    assert rows[-1]["scheduled_at"] == t0.isoformat()


def test_deleted_schedule_returns_videos_to_waiting(tmp_path: Path) -> None:
    from src.services.video_editor.pending_import import release_items_for_deleted_schedules

    items = _items(tmp_path, 3)
    apply_page_split(items, [{"account_id": "a", "page_id": "p1"}], None)
    for i, it in enumerate(items):
        it["item_scheduled_platform"] = "facebook"
        it["item_scheduled_job_id"] = f"sched_{i}"
    job = {"id": "j", "status": JOB_STATUS_IMPORTED, "imported_at": "x", "items": items}
    res = release_items_for_deleted_schedules([job], ["sched_0", "sched_1"])
    assert res == {"released": 2, "jobs": 1}
    assert job["status"] == JOB_STATUS_PARTIAL
    assert not items[0].get("item_scheduled_job_id") and not items[0].get("item_preset_fb_page_id")
    assert items[2]["item_scheduled_job_id"] == "sched_2"
    release_items_for_deleted_schedules([job], ["sched_2"])
    assert job["status"] == "saved" and "imported_at" not in job
    assert len(collect_free(items)) == 3


def collect_free(items: list) -> list[int]:
    from src.services.video_editor.page_split import collect_split_indexes

    return collect_split_indexes(items, only_unassigned=True)


def test_pending_visibility_and_unassign(tmp_path: Path) -> None:
    from src.services.video_editor.pending_import import (
        job_item_counts,
        job_visible_in_pending,
        schedule_ids_of_items,
        unassign_pages,
    )

    items = _items(tmp_path, 4)
    apply_page_split(items, [{"account_id": "a", "page_id": "p1"}], None)
    items[0]["item_scheduled_job_id"] = "sched_0"
    job = {"status": JOB_STATUS_PARTIAL, "items": items}
    c = job_item_counts(job)
    assert c == {"total": 4, "scheduled": 1, "waiting": 3, "missing": 0, "with_page": 3}
    assert job_visible_in_pending(job, c)
    assert unassign_pages(items, [1, 2]) == 2
    assert unassign_pages(items) == 1
    assert items[0].get("item_preset_fb_page_id") == "p1"
    assert schedule_ids_of_items(items) == ["sched_0"]
    done = {"status": JOB_STATUS_IMPORTED, "items": [items[0]]}
    assert not job_visible_in_pending(done, job_item_counts(done))
    assert job_visible_in_pending(done, job_item_counts(done), show_all=True)


def test_continue_after_already_posted_uses_next_slot(tmp_path: Path) -> None:
    """Bài đã đăng (success) vẫn chiếm slot — lần chia sau cộng tiếp, không trùng giờ cũ."""
    from datetime import date, datetime
    from zoneinfo import ZoneInfo

    from src.services.video_editor.pending_import import schedule_split_chunks

    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    items = _items(tmp_path, 1)
    res = apply_page_split(items, [{"account_id": "a", "page_id": "p1"}], None)
    existing = [
        {
            "page_id": "p1",
            "account_id": "a",
            "status": "success",
            "scheduled_at": datetime(2026, 10, 17, 21, 0, tzinfo=tz).isoformat(),
            "schedule_daily_slots": "08:00-10:00, 14:00-16:00, 20:00-22:00",
            "schedule_per_day": 3,
        }
    ]
    rows = schedule_split_chunks(
        items,
        {},
        res["chunks"],
        start_date=date(2026, 10, 17),
        windows=[((8, 0), (10, 0)), ((14, 0), (16, 0)), ((20, 0), (22, 0))],
        per_day=3,
        tz_name="Asia/Ho_Chi_Minh",
        existing_rows=existing,
        now=datetime(2026, 10, 17, 0, 0, tzinfo=tz),
    )
    first = datetime.fromisoformat(rows[0]["scheduled_at"]).astimezone(tz)
    assert first.date() == date(2026, 10, 18)
    assert 8 <= first.hour < 10


def test_infer_schedule_pattern_from_recent_jobs() -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from src.services.video_editor.pending_import import infer_schedule_pattern

    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    rows = [
        {
            "page_id": "p1",
            "status": "success",
            "scheduled_at": datetime(2026, 10, 16, 9, 0, tzinfo=tz).isoformat(),
            "schedule_daily_slots": "07:00-08:00, 12:00-13:00",
            "schedule_per_day": 2,
        },
        {
            "page_id": "p1",
            "status": "pending",
            "scheduled_at": datetime(2026, 10, 17, 12, 10, tzinfo=tz).isoformat(),
            "schedule_daily_slots": "07:00-08:00, 12:00-13:00",
            "schedule_per_day": 2,
        },
        {
            "page_id": "other",
            "status": "pending",
            "scheduled_at": datetime(2026, 10, 17, 20, 0, tzinfo=tz).isoformat(),
            "schedule_daily_slots": "20:00-21:00",
            "schedule_per_day": 1,
        },
    ]
    got = infer_schedule_pattern(rows, page_ids={"p1"})
    assert got["windows"] == "07:00-08:00, 12:00-13:00"
    assert got["per_day"] == 2


def test_cancelled_jobs_do_not_block_next_slot(tmp_path: Path) -> None:
    from datetime import date, datetime
    from zoneinfo import ZoneInfo

    from src.services.video_editor.pending_import import schedule_split_chunks

    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    items = _items(tmp_path, 1)
    res = apply_page_split(items, [{"account_id": "a", "page_id": "p1"}], None)
    existing = [
        {
            "page_id": "p1",
            "account_id": "a",
            "status": "cancelled",
            "scheduled_at": datetime(2026, 10, 20, 21, 0, tzinfo=tz).isoformat(),
        },
        {
            "page_id": "p1",
            "account_id": "a",
            "status": "success",
            "scheduled_at": datetime(2026, 10, 17, 9, 0, tzinfo=tz).isoformat(),
        },
    ]
    rows = schedule_split_chunks(
        items,
        {},
        res["chunks"],
        start_date=date(2026, 10, 17),
        windows=[((8, 0), (10, 0)), ((14, 0), (16, 0)), ((20, 0), (22, 0))],
        per_day=3,
        tz_name="Asia/Ho_Chi_Minh",
        existing_rows=existing,
        now=datetime(2026, 10, 17, 0, 0, tzinfo=tz),
    )
    first = datetime.fromisoformat(rows[0]["scheduled_at"]).astimezone(tz)
    assert first.date() == date(2026, 10, 17)
    assert 14 <= first.hour < 16


def test_page_owner_check() -> None:
    assert fb_page_owner_error({"account_id": "a"}, "a", "p") == ""
    assert fb_page_owner_error({"account_id": "b"}, "a", "p")
    assert fb_page_owner_error(None, "a", "p")
