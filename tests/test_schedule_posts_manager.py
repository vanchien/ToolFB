"""SchedulePostsManager (config/schedule_posts.json)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.utils.schedule_posts_manager import SchedulePostsManager


@pytest.fixture()
def mgr(tmp_path: Path) -> SchedulePostsManager:
    p = tmp_path / "schedule_posts.json"
    p.write_text("[]", encoding="utf-8")
    return SchedulePostsManager(json_path=p)


def test_schedule_posts_upsert_get_delete(mgr: SchedulePostsManager) -> None:
    mgr.upsert(
        {
            "page_id": "pg1",
            "account_id": "acc1",
            "post_type": "image",
            "title": "Serum",
            "content": "Nội dung…",
            "hashtags": ["#a"],
            "media_files": ["data/pages/pg1/library/images/x.jpg"],
            "scheduled_at": "2026-04-21T09:00:00+00:00",
            "status": "pending",
            "retry_count": 0,
            "created_by": "manual",
        }
    )
    rows = mgr.load_all()
    assert len(rows) == 1
    jid = str(rows[0]["id"])
    assert rows[0]["page_id"] == "pg1"
    assert rows[0]["post_type"] == "image"
    got = mgr.get_by_id(jid)
    assert got is not None
    assert got.get("title") == "Serum"
    assert mgr.list_for_page("pg1")[0]["id"] == jid
    assert mgr.delete_by_id(jid) is True
    assert mgr.load_all() == []


def test_schedule_posts_invalid_post_type(mgr: SchedulePostsManager) -> None:
    with pytest.raises(ValueError):
        mgr.upsert({"page_id": "p", "account_id": "a", "post_type": "invalid"})


def test_schedule_posts_default_status(mgr: SchedulePostsManager) -> None:
    mgr.upsert({"page_id": "p2", "account_id": "a2", "post_type": "text"})
    r = mgr.load_all()[0]
    assert r.get("status") == "pending"
    assert int(r.get("retry_count", -1)) == 0
    assert int(r.get("max_retry", 0)) == 3


def test_schedule_posts_update_job_fields(mgr: SchedulePostsManager) -> None:
    mgr.upsert({"page_id": "p3", "account_id": "a3", "post_type": "text", "title": "T"})
    jid = str(mgr.load_all()[0]["id"])
    assert mgr.update_job_fields(jid, status="processing") is True
    assert mgr.get_by_id(jid)["status"] == "processing"
    assert mgr.update_job_fields(jid, status="failed", error_note="x", retry_count=2) is True
    row = mgr.get_by_id(jid)
    assert row["status"] == "failed"
    assert row.get("retry_count") == 2
