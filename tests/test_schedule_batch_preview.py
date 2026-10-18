"""Tests cho ``schedule_batch_preview`` (lịch chuỗi + quét video)."""

from __future__ import annotations

import random
from datetime import date, datetime
from pathlib import Path

from src.utils.schedule_batch_preview import (
    build_random_window_schedules_per_group,
    build_schedule_by_daily_slots,
    compute_scheduled_at_series,
    parse_daily_time_windows,
    post_type_for_kind,
    preview_row_to_schedule_job,
    scan_video_files,
)


def test_post_type_for_kind() -> None:
    assert post_type_for_kind("text_image") == "text_image"
    assert post_type_for_kind("VIDEO") == "video"


def test_page_post_style_supports_reel() -> None:
    from src.utils.schedule_batch_preview import page_post_style_for_post_type

    assert page_post_style_for_post_type("reel") == "video"


def test_compute_scheduled_at_series_daily_count_and_iso() -> None:
    series = compute_scheduled_at_series(
        3,
        "daily",
        start_date=date(2026, 4, 21),
        hour=9,
        minute=0,
        interval_unit="days",
        interval_value=1,
        jitter_max_min=0,
    )
    assert len(series) == 3
    for s in series:
        assert "T" in s or s.endswith("+00:00")
        assert s.endswith("+00:00")


def test_scan_video_files_filters_and_sorts(tmp_path: Path) -> None:
    (tmp_path / "a.mp4").write_bytes(b"x")
    (tmp_path / "b.txt").write_text("n")
    (tmp_path / "c.mov").write_bytes(b"y")
    found = scan_video_files(tmp_path, sort="name")
    assert [p.name for p in found] == ["a.mp4", "c.mov"]


def test_preview_row_to_schedule_job_maps_fields() -> None:
    row = {
        "job_type": "text",
        "title": "T",
        "content": "C",
        "hashtags": ["#a"],
        "media_files": [],
        "scheduled_at": "2026-04-21T02:00:00+00:00",
        "status": "preview_ready",
        "error": "",
        "cta": "Mua",
    }
    job = preview_row_to_schedule_job(
        row,
        account_id="acc1",
        page_id="p1",
        post_type="text",
        page_post_style="post",
        schedule_recurrence="",
        schedule_slot="",
    )
    assert job["account_id"] == "acc1"
    assert job["page_id"] == "p1"
    assert job["status"] == "pending"
    assert job["title"] == "T"


def test_build_schedule_by_daily_slots_rolls_to_next_day() -> None:
    out = build_schedule_by_daily_slots(
        start_date=date(2026, 4, 21),
        time_slots=["22:30", "04:30", "10:15"],
        job_count=5,
        delay_min_minutes=0,
        delay_max_minutes=0,
        timezone_name="Asia/Ho_Chi_Minh",
    )
    assert len(out) == 5
    assert out[0]["slot_base_local"].endswith("04:30")
    assert out[1]["slot_base_local"].endswith("10:15")
    assert out[2]["slot_base_local"].endswith("22:30")
    assert out[3]["slot_base_local"].startswith("2026-04-22")
    assert out[4]["slot_base_local"].startswith("2026-04-22")


def test_build_schedule_by_daily_slots_validate_delay_range() -> None:
    try:
        build_schedule_by_daily_slots(
            start_date=date(2026, 4, 21),
            time_slots=["08:00"],
            job_count=1,
            delay_min_minutes=10,
            delay_max_minutes=1,
        )
        assert False, "Expected ValueError"
    except ValueError:
        assert True


def test_random_window_is_independent_per_page() -> None:
    keys = ["fb:p1", "fb:p2", "fb:p1", "fb:p2", "fb:p1"]
    plans = build_random_window_schedules_per_group(
        keys,
        start_date=date(2026, 10, 16),
        end_date=date(2026, 10, 20),
        window_start=(8, 0),
        window_end=(22, 0),
        gap_min_minutes=30,
        gap_max_minutes=40,
        timezone_name="Asia/Ho_Chi_Minh",
        start_jitter_max_min=10,
        rng=random.Random(7),
    )
    assert len(plans) == 5
    walls = [datetime.strptime(p["wall"], "%Y-%m-%d %H:%M") for p in plans]
    p1 = [walls[i] for i, k in enumerate(keys) if k == "fb:p1"]
    p2 = [walls[i] for i, k in enumerate(keys) if k == "fb:p2"]
    assert p1 == sorted(p1)
    assert p2 == sorted(p2)
    assert p1[1] > p1[0]
    assert all(8 <= w.hour < 22 or (w.hour == 22 and w.minute == 0) for w in walls)


def test_parse_daily_time_windows_ranges_and_points() -> None:
    windows = parse_daily_time_windows("20:00-22:00, 08:00-10:00, 14:00")
    assert windows[0] == ((8, 0), (10, 0))
    assert windows[1] == ((14, 0), (14, 0))
    assert windows[2] == ((20, 0), (22, 0))


def test_multi_windows_repeat_next_day() -> None:
    keys = ["fb:p1"] * 4
    plans = build_random_window_schedules_per_group(
        keys,
        start_date=date(2026, 10, 16),
        end_date=date(2026, 10, 20),
        windows=[((8, 0), (8, 20)), ((20, 0), (20, 20))],
        gap_min_minutes=25,
        gap_max_minutes=25,
        timezone_name="Asia/Ho_Chi_Minh",
        start_jitter_max_min=0,
        rng=random.Random(3),
    )
    walls = [datetime.strptime(p["wall"], "%Y-%m-%d %H:%M") for p in plans]
    assert walls == sorted(walls)
    hours = [w.hour for w in walls]
    assert hours[0] == 8
    assert hours[1] == 20
    assert walls[2].date() == date(2026, 10, 17)
    assert hours[2] == 8
    assert hours[3] == 20
