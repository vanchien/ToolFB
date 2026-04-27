"""Kiểm tra parse / format lịch Page (cron vs một lần)."""

from __future__ import annotations

from datetime import date

import pytest

from src.utils.page_schedule import (
    classify_schedule_raw,
    format_once_schedule,
    format_once_schedule_24h,
    h12_to_h24,
    h24_to_h12,
    load_schedule_into_ui_parts,
    normalize_schedule_for_compare,
    parse_cron_hh_mm,
    parse_once_local,
    parse_page_schedule_for_apscheduler,
    scheduler_tz,
)


def test_cron_vs_once_classify() -> None:
    assert classify_schedule_raw("9:30") == "cron"
    assert classify_schedule_raw("09:05") == "cron"
    assert classify_schedule_raw("2026-04-20 14:30") == "date"


def test_h12_roundtrip() -> None:
    assert h12_to_h24(12, "AM") == 0
    assert h12_to_h24(1, "AM") == 1
    assert h12_to_h24(12, "PM") == 12
    assert h12_to_h24(1, "PM") == 13
    assert h24_to_h12(0) == (12, "AM")
    assert h24_to_h12(12) == (12, "PM")


def test_parse_apscheduler_branch() -> None:
    tz = scheduler_tz()
    c = parse_page_schedule_for_apscheduler("08:15", tz=tz)
    assert c[0] == "cron"
    assert c[1:] == (8, 15)
    d = parse_page_schedule_for_apscheduler("2026-06-01 00:00", tz=tz)
    assert d[0] == "date"
    assert d[1].year == 2026 and d[1].month == 6 and d[1].day == 1
    assert d[1].hour == 0 and d[1].minute == 0


def test_format_once() -> None:
    assert format_once_schedule(date(2026, 4, 20), 3, 5, "PM") == "2026-04-20 15:05"
    assert format_once_schedule_24h(date(2026, 4, 20), 15, 5) == "2026-04-20 15:05"


def test_load_ui_parts_24h() -> None:
    tz = scheduler_tz()
    k, d0, h, m = load_schedule_into_ui_parts("14:30", tz=tz)
    assert k == "daily"
    assert h == 14 and m == 30
    k2, d1, h2, m2 = load_schedule_into_ui_parts("2026-05-10 08:09", tz=tz)
    assert k2 == "once"
    assert d1.month == 5 and h2 == 8 and m2 == 9


def test_normalize_compare() -> None:
    assert normalize_schedule_for_compare("9:05") == "09:05"
    tz = scheduler_tz()
    assert normalize_schedule_for_compare("2026-01-02 8:07") == parse_once_local("2026-01-02 08:07", tz).strftime(
        "%Y-%m-%d %H:%M"
    )


def test_invalid_cron() -> None:
    with pytest.raises(ValueError):
        parse_cron_hh_mm("25:00")
    with pytest.raises(ValueError):
        classify_schedule_raw("not-a-schedule")
