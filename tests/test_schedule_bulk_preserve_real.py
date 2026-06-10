"""Kiểm tra job thật trong config (nếu có) — merge không làm mất trường lịch."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from src.utils.schedule_posts_manager import SchedulePostsManager

ROOT = Path(__file__).resolve().parents[1]
SCHEDULE_PATH = ROOT / "config" / "schedule_posts.json"

SCHEDULE_KEYS_MUST_KEEP = frozenset(
    {
        "id",
        "page_id",
        "account_id",
        "post_type",
        "status",
        "scheduled_at",
        "timezone",
        "video_path",
        "media_files",
        "job_post_image_path",
        "ai_topic",
        "ai_language",
        "ai_content_style",
        "ai_provider_text",
        "ai_provider_image",
        "ai_config",
        "retry_count",
        "max_retry",
        "jitter_minutes",
        "schedule_recurrence",
        "schedule_slot",
        "page_post_style",
        "created_at",
        "hide_browser",
        "reel_thumbnail_choice",
    }
)


@pytest.mark.skipif(not SCHEDULE_PATH.is_file(), reason="Không có schedule_posts.json")
def test_real_job_bulk_patch_preserves_other_fields(tmp_path: Path) -> None:
    raw = json.loads(SCHEDULE_PATH.read_text(encoding="utf-8"))
    if not raw:
        pytest.skip("schedule_posts rỗng")
    sample = next((j for j in raw if str(j.get("status", "")).lower() == "pending"), raw[0])
    before = copy.deepcopy(sample)
    jid = str(before["id"])

    work = tmp_path / "schedule_posts.json"
    work.write_text(json.dumps([before], ensure_ascii=False, indent=2), encoding="utf-8")
    mgr = SchedulePostsManager(json_path=work)

    mgr.update_jobs_fields_batch(
        [jid],
        title="PATCH_TEST_TITLE",
        content="PATCH_TEST_TITLE",
        hashtags=["#patch_test"],
    )
    after = mgr.get_by_id(jid)
    assert after is not None
    assert after["title"] == "PATCH_TEST_TITLE"
    assert after["content"] == "PATCH_TEST_TITLE"
    assert after["hashtags"] == ["#patch_test"]

    for key in SCHEDULE_KEYS_MUST_KEEP:
        if key in before:
            assert after.get(key) == before.get(key), f"Trường {key} bị đổi: {before.get(key)!r} -> {after.get(key)!r}"
