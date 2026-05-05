"""Tests cho ``schedule_batch_preview`` (lịch chuỗi + quét video)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from src.utils.schedule_batch_preview import (
    build_schedule_by_daily_slots,
    compute_scheduled_at_series,
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
