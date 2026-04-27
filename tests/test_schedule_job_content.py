"""merge job AI vào page_row + lịch daily ISO."""

from __future__ import annotations

from datetime import datetime, timezone

from src.utils.page_schedule import scheduler_tz
from src.utils.schedule_job_content import (
    compute_next_daily_scheduled_utc_iso,
    merge_queue_job_content_into_page_row,
)


def test_merge_overrides_page_fields() -> None:
    page = {"id": "p1", "page_name": "N", "topic": "old", "content_style": "x", "post_image_path": "a.png"}
    job = {
        "ai_topic": "new topic",
        "ai_content_style": "humor",
        "job_post_image_path": "b.png",
        "ai_config": {"brand_voice": "Thân thiện", "target_audience": "Gen Z"},
    }
    out = merge_queue_job_content_into_page_row(page, job)
    assert out is not None
    assert out["topic"] == "new topic"
    assert "humor" in str(out.get("content_style", ""))
    assert "Thân thiện" in str(out.get("content_style", ""))
    assert out.get("post_image_path") == "b.png"


def test_compute_next_daily_is_future() -> None:
    tz = scheduler_tz()
    now_l = datetime.now(tz).replace(hour=20, minute=0, second=0, microsecond=0)
    now_u = now_l.astimezone(timezone.utc)
    nxt_s = compute_next_daily_scheduled_utc_iso("09:00", from_utc=now_u)
    nxt = datetime.fromisoformat(nxt_s.replace("Z", "+00:00"))
    assert nxt > now_u
