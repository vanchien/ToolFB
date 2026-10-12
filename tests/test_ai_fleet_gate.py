"""Cổng duyệt: bài chưa duyệt hoặc đã sửa sau duyệt không vào lịch và không được đăng."""

from __future__ import annotations

import pytest

from src.services.ai_fleet.gate import human_decide, is_approval_block, publish_block_reason, review_content, schedule_block_reason
from src.services.ai_fleet.store import FleetStore
from src.services.post_executor import execute_facebook_post_sequence
from src.utils.schedule_posts_manager import SchedulePostsManager


def _ready_fields(**extra: object) -> dict:
    fields = {
        "title": "Phim hài cổ",
        "body": "Một bộ phim hài cổ điển đang được nhắc lại.",
        "caption": "Xem lại phim hài cổ điển.",
        "hashtags": ["#comedia"],
        "source_urls": ["https://example.com/filme"],
        "asset_ids": ["asset-1"],
        "language": "pt-BR",
        "score": 90,
    }
    fields.update(extra)
    return fields


def _page(store: FleetStore, page_id: str = "page-br") -> None:
    store.upsert_profile(
        {
            "page_id": page_id,
            "page_name": "Brazil Movies",
            "language": "pt-BR",
            "topic": ["classic movies"],
            "daily_content_target": 3,
        }
    )
    store.add_policy(
        page_id,
        {"language": "pt-BR", "requires_source": True, "requires_media": True, "min_score": 80, "max_revision_attempts": 2},
        requested_by="user",
    )


def test_unreviewed_draft_cannot_schedule_or_publish(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    _page(store)
    content = store.create_draft("page-br", _ready_fields())
    assert schedule_block_reason(content["content_id"], store)
    assert publish_block_reason(content["content_id"], store)
    with pytest.raises(RuntimeError):
        execute_facebook_post_sequence(
            None,
            cookie_path=None,
            entity_dict=None,
            text_body="x",
            draft_media_paths=[],
            page_extra_image=None,
            fleet_content_id=content["content_id"],
        )


def test_revision_and_human_review_cannot_publish(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    _page(store)
    content = store.create_draft("page-br", _ready_fields(source_urls=[], score=99))
    first = review_content(store, content["content_id"])
    assert first["decision"] == "REVISION_REQUIRED"
    assert first["score"] == 99
    assert publish_block_reason(content["content_id"], store)
    store.add_version(content["content_id"], _ready_fields(source_urls=[], score=99))
    second = review_content(store, content["content_id"])
    assert second["decision"] == "HUMAN_REVIEW_REQUIRED"
    assert publish_block_reason(content["content_id"], store)


def test_high_score_does_not_override_hard_fail(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    _page(store)
    content = store.create_draft("page-br", _ready_fields(body="nội dung cấm cá cược", score=100))
    store.add_policy(
        "page-br",
        {"language": "pt-BR", "prohibited_topics": ["cá cược"], "max_revision_attempts": 3, "min_score": 80},
        requested_by="user",
    )
    fresh = store.create_draft("page-br", _ready_fields(body="nội dung cấm cá cược", score=100))
    review = review_content(store, fresh["content_id"])
    assert review["decision"] == "REJECTED"
    assert any(row["criterion"] == "PROHIBITED" and row["status"] == "FAIL" for row in review["checks"])


def test_policy_change_and_edit_after_approval_block_publish(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    _page(store)
    content = store.create_draft("page-br", _ready_fields())
    review = review_content(store, content["content_id"])
    assert review["decision"] == "APPROVED"
    assert schedule_block_reason(content["content_id"], store) == ""
    store.add_policy("page-br", {"language": "pt-BR", "min_score": 80, "max_revision_attempts": 2}, requested_by="user")
    assert "Policy" in schedule_block_reason(content["content_id"], store) or store.content(content["content_id"])["status"] == "RECHECK_REQUIRED"
    assert publish_block_reason(content["content_id"], store)
    again = store.create_draft("page-br", _ready_fields(title="Bài khác", asset_ids=["asset-2"]))
    assert review_content(store, again["content_id"])["decision"] == "APPROVED"
    saved = store.content(again["content_id"])
    saved["versions"][-1]["title"] = "Đã sửa sau duyệt"
    saved["versions"][-1]["asset_ids"] = ["asset-999"]
    store.save_content(saved)
    assert "duyệt lại" in publish_block_reason(again["content_id"], store)


def test_review_writes_audit_and_page_memory_stays_separate(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    _page(store, "page-a")
    _page(store, "page-b")
    content = store.create_draft("page-a", _ready_fields())
    review_content(store, content["content_id"])
    assert any(row["action"] == "content_review" for row in store.load()["audit"])
    store.remember("page-a", "topic_memory", {"lesson": "phim hài", "evidence": "https://example.com"})
    assert store.load()["profiles"]["page-b"]["memory"]["topic_memory"] == []
    assert store.load()["profiles"]["page-a"]["memory"]["topic_memory"][0]["lesson"] == "phim hài"


def test_low_score_ends_in_human_review_and_pause_blocks(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    _page(store)
    content = store.create_draft("page-br", _ready_fields(score=10))
    assert review_content(store, content["content_id"])["decision"] == "REVISION_REQUIRED"
    store.add_version(content["content_id"], _ready_fields(score=10))
    assert review_content(store, content["content_id"])["decision"] == "HUMAN_REVIEW_REQUIRED"
    good = store.create_draft("page-br", _ready_fields(title="Bài đủ điểm", asset_ids=["asset-3"]))
    assert review_content(store, good["content_id"])["decision"] == "APPROVED"
    profile = store.load()["profiles"]["page-br"]
    profile["status"] = "PAUSED"
    doc = store.load()
    doc["profiles"]["page-br"] = profile
    store.save(doc)
    assert "tạm dừng" in schedule_block_reason(good["content_id"], store)


def test_job_text_must_match_approved_version(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    _page(store)
    content = store.create_draft("page-br", _ready_fields())
    review_content(store, content["content_id"])
    assert schedule_block_reason(content["content_id"], store, {"title": "Sai tiêu đề"})
    assert schedule_block_reason(
        content["content_id"],
        store,
        {"title": "Phim hài cổ", "content": "Xem lại phim hài cổ điển."},
    ) == ""
    assert "không khớp" in publish_block_reason(content["content_id"], store, posted_text="bài khác")
    assert publish_block_reason(content["content_id"], store, posted_text="Xem lại phim hài cổ điển.") == ""
    store.mark_published(content["content_id"])
    assert "đã đăng" in publish_block_reason(content["content_id"], store).lower()


def test_human_cannot_approve_over_a_hard_fail(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    _page(store)
    content = store.create_draft("page-br", _ready_fields(source_urls=[]))
    with pytest.raises(ValueError):
        human_decide(store, content["content_id"], "APPROVED")
    review_content(store, content["content_id"])
    store.add_version(content["content_id"], _ready_fields())
    review = human_decide(store, content["content_id"], "APPROVED")
    assert review["decision"] == "APPROVED"
    assert schedule_block_reason(content["content_id"], store) == ""


def test_published_fleet_post_is_not_scheduled_again(tmp_path, monkeypatch) -> None:
    fleet = tmp_path / "fleet.json"
    monkeypatch.setenv("TOOLFB_AI_FLEET_PATH", str(fleet))
    store = FleetStore(fleet)
    _page(store)
    content = store.create_draft("page-br", _ready_fields())
    review_content(store, content["content_id"])
    posts = SchedulePostsManager(tmp_path / "schedule_posts.json")
    posts.upsert(
        {
            "id": "job-daily",
            "page_id": "page-br",
            "account_id": "acc-1",
            "fleet_content_id": content["content_id"],
            "title": "Phim hài cổ",
            "content": "Xem lại phim hài cổ điển.",
            "schedule_recurrence": "daily",
            "schedule_slot": "09:00",
            "status": "pending",
        }
    )
    monkeypatch.setattr("src.scheduler.get_default_schedule_posts_manager", lambda: posts)
    from src.scheduler import _finalize_schedule_post_job_record

    _finalize_schedule_post_job_record("job-daily", True, "")
    saved = posts.get_by_id("job-daily")
    assert saved["status"] == "success"
    assert store.content(content["content_id"])["published"] is True
    reason = publish_block_reason(content["content_id"], store)
    assert is_approval_block(reason)


def test_scheduler_rejects_fleet_job_until_approved(tmp_path, monkeypatch) -> None:
    fleet = tmp_path / "fleet.json"
    monkeypatch.setenv("TOOLFB_AI_FLEET_PATH", str(fleet))
    store = FleetStore(fleet)
    _page(store)
    content = store.create_draft("page-br", _ready_fields())
    posts = SchedulePostsManager(tmp_path / "schedule_posts.json")
    job = {"id": "job-1", "page_id": "page-br", "account_id": "acc-1", "fleet_content_id": content["content_id"]}
    with pytest.raises(ValueError):
        posts.upsert(job)
    review_content(store, content["content_id"])
    posts.upsert(job)
    assert posts.get_by_id("job-1")["fleet_content_id"] == content["content_id"]
    posts.upsert({"id": "manual", "page_id": "page-br", "account_id": "acc-1", "content": "bài tay"})
    assert posts.get_by_id("manual")["content"] == "bài tay"
