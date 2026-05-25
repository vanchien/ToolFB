"""merge job AI vào page_row + lịch daily ISO."""

from __future__ import annotations

from datetime import datetime, timezone

from src.utils.page_schedule import scheduler_tz
from src.utils.schedule_job_content import (
    compute_next_daily_scheduled_utc_iso,
    dedupe_post_title_content,
    dedupe_post_title_content_hashtags,
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


def test_dedupe_identical_title_content() -> None:
    t, c = dedupe_post_title_content("Hello world", "Hello world")
    assert t == "Hello world"
    assert c == ""


def test_dedupe_content_starts_with_title() -> None:
    t, c = dedupe_post_title_content("Tiêu đề", "Tiêu đề\n\nNội dung thêm")
    assert t == "Tiêu đề"
    assert c == "Nội dung thêm"


def test_dedupe_title_contains_content() -> None:
    t, c = dedupe_post_title_content("A - B - C", "B - C")
    assert t == "A - B - C"
    assert c == ""


def test_dedupe_hashtag_keywords_same_word() -> None:
    from src.utils.schedule_job_content import dedupe_hashtag_keywords

    assert dedupe_hashtag_keywords(["#fyp", "fyp", "#FYP", "FYP"]) == ["#fyp"]
    assert dedupe_hashtag_keywords(["#fyp", "#viral", "viral"]) == ["#fyp", "#viral"]


def test_dedupe_hashtag_list_comma_separated() -> None:
    from src.utils.schedule_job_content import _normalize_hashtag_list

    assert _normalize_hashtag_list(["fyp, viral", "#fyp"]) == ["#fyp", "#viral"]


def test_prepare_queue_job_no_duplicate_title_content() -> None:
    from src.utils.schedule_job_content import prepare_queue_job_post_fields

    job = {
        "post_type": "text",
        "title": "Hook hay",
        "content": "Hook hay\n\nChi tiết bài viết.",
        "hashtags": ["#fyp", "fyp"],
    }
    prep = prepare_queue_job_post_fields(job, fallback_body="Hook hay\n\nChi tiết từ AI.")
    assert prep["title"] == "Hook hay"
    assert "Hook hay" not in prep["content"] or prep["content"] == ""
    assert prep["content"] == "Chi tiết bài viết."
    cap = prep["caption_text"]
    assert cap.count("Hook hay") == 1
    assert prep["hashtags"] == ["#fyp"]


def test_prepare_reel_job_separates_tags() -> None:
    from src.utils.schedule_job_content import prepare_queue_job_post_fields

    job = {
        "post_type": "reel",
        "title": "Tiêu đề reel",
        "content": "Tiêu đề reel\nMô tả #viral",
        "hashtags": ["viral", "#fyp"],
    }
    prep = prepare_queue_job_post_fields(job)
    assert prep["title"] == "Tiêu đề reel"
    assert "#viral" not in prep["content"].lower()
    assert prep["reel_tags"] == ["viral", "fyp"]
    assert prep["caption_text"] == prep["content"]
    assert "Tiêu đề reel" in prep["reel_description"]
    assert prep["reel_description"].count("Tiêu đề reel") == 1


def test_dedupe_hashtags_moved_out_of_body() -> None:
    t, c, tags = dedupe_post_title_content_hashtags(
        "Sale",
        "Sale #fyp #viral",
        ["#fyp"],
    )
    assert t == "Sale"
    assert "#fyp" not in c.lower()
    assert "#viral" in [x.lower() for x in tags]
