"""Hẹn giờ chia sẻ theo list link."""

from datetime import datetime

import pytest

from src.services.facebook_groups.share_schedule import (
    compose_clock,
    hour_choices,
    machine_clock_text,
    make_queue_item,
    minute_choices,
    plan_share_links,
    queue_state_from_results,
)


def test_list_uses_start_clock_and_gap() -> None:
    now = datetime(2026, 10, 10, 20, 0)
    planned = plan_share_links(
        "https://www.facebook.com/reel/1\nhttps://www.facebook.com/reel/2",
        start_clock="21:30",
        gap_minutes=15,
        now=now,
    )
    assert [item.url[-1] for item in planned] == ["1", "2"]
    assert planned[0].when == datetime(2026, 10, 10, 21, 30)
    assert planned[1].when == datetime(2026, 10, 10, 21, 45)


def test_line_clock_overrides_start() -> None:
    now = datetime(2026, 10, 10, 20, 0)
    planned = plan_share_links(
        "21:10 https://www.facebook.com/reel/1\nhttps://www.facebook.com/reel/2",
        start_clock="23:00",
        gap_minutes=20,
        now=now,
    )
    assert planned[0].when == datetime(2026, 10, 10, 21, 10)
    assert planned[1].when == datetime(2026, 10, 10, 21, 30)


def test_past_clock_moves_to_next_day() -> None:
    now = datetime(2026, 10, 10, 22, 0)
    planned = plan_share_links(
        "https://facebook.com/watch/?v=9",
        start_clock="21:00",
        now=now,
    )
    assert planned[0].when == datetime(2026, 10, 11, 21, 0)


def test_blank_list_uses_single_link() -> None:
    now = datetime(2026, 10, 10, 20, 0)
    planned = plan_share_links("", single_url="https://www.facebook.com/reel/9", now=now)
    assert len(planned) == 1
    assert planned[0].when == now
    assert planned[0].url.endswith("/9")


def test_each_added_link_keeps_its_own_time() -> None:
    now = datetime(2026, 10, 10, 20, 0)
    first = make_queue_item("https://www.facebook.com/reel/1", "21:30", now=now)
    second = make_queue_item("https://www.facebook.com/reel/2", "23:00", now=now)
    now_item = make_queue_item("https://www.facebook.com/reel/3", immediate=True, now=now)
    assert first.label == "21:30" and first.when.hour == 21
    assert second.label == "23:00" and second.when.hour == 23
    assert now_item.label == "Ngay" and now_item.when == now
    assert first.state == "waiting"


def test_queue_state_keeps_a_stopped_link_waiting() -> None:
    assert queue_state_from_results(
        created=2,
        job_statuses=["COMPLETED", "PENDING"],
        paused=True,
        cancelled=True,
    ) == "waiting"
    assert queue_state_from_results(
        created=1,
        job_statuses=["FAILED"],
        paused=True,
        cancelled=False,
    ) == "error"
    assert queue_state_from_results(
        created=2,
        job_statuses=["COMPLETED", "COMPLETED"],
        paused=False,
        cancelled=False,
    ) == "done"
    assert queue_state_from_results(
        created=0,
        job_statuses=[],
        paused=False,
        cancelled=False,
    ) == "skipped"


def test_hour_drop_covers_the_machine_day() -> None:
    now = datetime(2026, 10, 10, 22, 7, 40)
    choices = hour_choices(now)
    assert choices[0] == "22"
    assert choices[-1] == "21"
    assert len(choices) == 24
    assert minute_choices()[0] == "00" and minute_choices()[-1] == "59"
    assert compose_clock("22", "07") == "22:07"
    assert compose_clock("", "30") == ""
    assert machine_clock_text(now) == "Giờ máy 22:07"
    item = make_queue_item("https://www.facebook.com/reel/1", compose_clock("22", "07"), now=now)
    assert item.when == datetime(2026, 10, 10, 22, 7)
    assert item.label == "22:07"


def test_add_without_clock_asks_for_a_time() -> None:
    with pytest.raises(ValueError):
        make_queue_item("https://www.facebook.com/reel/1", "", now=datetime(2026, 10, 10, 20, 0))


def test_bad_line_is_rejected() -> None:
    with pytest.raises(ValueError):
        plan_share_links("không phải link", now=datetime(2026, 10, 10, 20, 0))
