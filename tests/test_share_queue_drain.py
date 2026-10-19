"""Hàng chờ: quá giờ phải chạy lần lượt hết video, không kẹt 1 bài / Page."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.services.facebook_groups.share_schedule import (
    PlannedShareLink,
    as_local_naive,
    queue_from_settings,
    queue_state_from_results,
    queue_to_settings,
)
from src.services.facebook_groups.video_engage import shuffle_due_queue_items


def test_as_local_naive_accepts_aware_utc() -> None:
    aware = datetime(2026, 10, 19, 4, 0, tzinfo=timezone.utc)
    naive = as_local_naive(aware)
    assert naive.tzinfo is None
    # So sánh với now không TypeError.
    assert isinstance(naive <= datetime.now(), bool)


def test_queue_roundtrip_normalizes_when() -> None:
    item = PlannedShareLink(
        when=datetime(2026, 10, 19, 5, 7, tzinfo=timezone.utc),
        url="https://www.facebook.com/reel/111",
        label="Mới 05:07",
        source="https://www.facebook.com/src",
    )
    back = queue_from_settings(queue_to_settings([item]))
    assert len(back) == 1
    assert back[0].when.tzinfo is None
    # shuffle không nổ TypeError.
    picked = shuffle_due_queue_items(back, now=datetime(2026, 10, 19, 12, 0))
    assert picked is back[0]


def test_shuffle_picks_all_overdue_not_only_future() -> None:
    now = datetime(2026, 10, 19, 11, 0)
    rows = [
        PlannedShareLink(when=now - timedelta(hours=5), url="https://www.facebook.com/reel/1", state="waiting"),
        PlannedShareLink(when=now - timedelta(hours=1), url="https://www.facebook.com/reel/2", state="waiting"),
        PlannedShareLink(when=now + timedelta(hours=2), url="https://www.facebook.com/reel/3", state="waiting"),
    ]
    urls = set()
    for seed in range(30):
        import random

        picked = shuffle_due_queue_items(rows, now=now, rng=random.Random(seed))
        assert picked is not None
        urls.add(picked.url)
    assert "https://www.facebook.com/reel/3" not in urls
    assert urls == {
        "https://www.facebook.com/reel/1",
        "https://www.facebook.com/reel/2",
    }


def test_queue_state_resume_with_jobs_not_skipped() -> None:
    assert (
        queue_state_from_results(created=0, job_statuses=["COMPLETED"] * 10, paused=False, cancelled=False)
        == "done"
    )
    assert (
        queue_state_from_results(created=0, job_statuses=["PENDING"] * 3, paused=False, cancelled=False)
        == "waiting"
    )
    assert queue_state_from_results(created=0, job_statuses=[], paused=False, cancelled=False) == "skipped"


def test_promote_overdue_logic() -> None:
    """Mô phỏng _promote_overdue_queue: quá giờ → when gần now, cách nhau vài giây."""
    now = datetime(2026, 10, 19, 11, 16, 0)
    items = [
        PlannedShareLink(when=now - timedelta(hours=6), url="https://www.facebook.com/reel/a", label="Mới 05:07"),
        PlannedShareLink(when=now - timedelta(minutes=30), url="https://www.facebook.com/reel/b", label="Mới 10:49"),
        PlannedShareLink(when=now + timedelta(hours=1), url="https://www.facebook.com/reel/c", label="Mới 12:20"),
    ]
    overdue = [i for i in items if i.when <= now]
    overdue.sort(key=lambda i: i.when)
    for index, item in enumerate(overdue):
        item.when = now + timedelta(seconds=2 + index * 3)
    assert items[0].when <= now + timedelta(seconds=5)
    assert items[1].when <= now + timedelta(seconds=8)
    assert items[2].when > now  # chưa đụng video tương lai
