"""E2E logic (không GUI): lọc job theo nhãn page/account + bulk sửa nội dung."""

from __future__ import annotations

from pathlib import Path

from src.utils.schedule_posts_filters import (
    apply_job_filters,
    format_account_filter_label,
    format_page_filter_label,
    split_hashtags_csv,
)
from src.utils.schedule_posts_manager import SchedulePostsManager


def _label_maps(jobs: list[dict], page_names: dict[str, str], account_names: dict[str, str]):
    pg_label_to_id: dict[str, str] = {}
    acc_label_to_id: dict[str, str] = {}
    for j in jobs:
        pid = str(j.get("page_id") or "").strip()
        aid = str(j.get("account_id") or "").strip()
        if pid:
            lbl = format_page_filter_label(pid, page_names.get(pid, ""))
            pg_label_to_id[lbl] = pid
        if aid:
            lbl = format_account_filter_label(aid, account_names.get(aid, ""))
            acc_label_to_id[lbl] = aid
    return pg_label_to_id, acc_label_to_id


def test_filter_by_display_labels_e2e() -> None:
    jobs = [
        {
            "id": "sched_a",
            "page_id": "538b89cc73f2",
            "account_id": "acc_1",
            "post_type": "video",
            "status": "pending",
            "title": "Clip A",
        },
        {
            "id": "sched_b",
            "page_id": "other_page",
            "account_id": "acc_2",
            "post_type": "text",
            "status": "success",
            "title": "Clip B",
        },
    ]
    page_names = {"538b89cc73f2": "Minh Tuyết", "other_page": "Page Khác"}
    account_names = {"acc_1": "facebok1", "acc_2": "TK2"}
    pg_map, acc_map = _label_maps(jobs, page_names, account_names)

    page_lbl = format_page_filter_label("538b89cc73f2", "Minh Tuyết")
    acc_lbl = format_account_filter_label("acc_1", "facebok1")
    assert pg_map[page_lbl] == "538b89cc73f2"
    assert acc_map[acc_lbl] == "acc_1"

    filtered = apply_job_filters(
        jobs,
        page_id=pg_map[page_lbl],
        account=acc_map[acc_lbl],
        status="pending",
    )
    assert len(filtered) == 1
    assert filtered[0]["id"] == "sched_a"


def test_bulk_edit_preserves_schedule_and_media_fields(tmp_path: Path) -> None:
    """Chỉ đổi title/content/hashtags — lịch, video, AI, status… giữ nguyên."""
    p = tmp_path / "schedule_posts.json"
    p.write_text("[]", encoding="utf-8")
    mgr = SchedulePostsManager(json_path=p)
    full_job = {
        "page_id": "538b89cc73f2",
        "account_id": "acc_1",
        "post_type": "video",
        "status": "pending",
        "title": "Old title",
        "content": "Old content",
        "hashtags": ["#old"],
        "scheduled_at": "2026-06-10T09:00:00+00:00",
        "timezone": "Asia/Ho_Chi_Minh",
        "video_path": "data/pages/x/video.mp4",
        "media_files": ["data/pages/x/video.mp4"],
        "ai_topic": "topic keep",
        "ai_language": "English",
        "ai_config": {"tone": "casual"},
        "retry_count": 1,
        "jitter_minutes": 5,
        "page_post_style": "reel",
    }
    mgr.upsert(full_job)  # type: ignore[arg-type]
    jid = str(mgr.load_all()[0]["id"])

    mgr.update_jobs_fields_sequential(
        [jid],
        fields={
            "title": "New title",
            "content": "New title",
            "hashtags": ["#viral", "#fb"],
        },
    )
    row = mgr.get_by_id(jid)
    assert row is not None
    assert row["title"] == "New title"
    assert row["content"] == "New title"
    assert row["hashtags"] == ["#viral", "#fb"]
    assert row["scheduled_at"] == "2026-06-10T09:00:00+00:00"
    assert row["status"] == "pending"
    assert row["post_type"] == "video"
    assert row["video_path"] == "data/pages/x/video.mp4"
    assert row["media_files"] == ["data/pages/x/video.mp4"]
    assert row["ai_topic"] == "topic keep"
    assert row.get("ai_config") == {"tone": "casual"}
    assert row["retry_count"] == 1
    assert row["jitter_minutes"] == 5


def test_bulk_edit_hashtags_full_replace_not_append(tmp_path: Path) -> None:
    p = tmp_path / "schedule_posts.json"
    p.write_text("[]", encoding="utf-8")
    mgr = SchedulePostsManager(json_path=p)
    mgr.upsert(
        {
            "page_id": "p",
            "account_id": "a",
            "post_type": "text",
            "hashtags": ["#old1", "#old2", "#old3"],
            "scheduled_at": "2026-01-01T00:00:00+00:00",
        }
    )
    jid = str(mgr.load_all()[0]["id"])
    mgr.update_jobs_fields_batch([jid], hashtags=["#only_new"])
    row = mgr.get_by_id(jid)
    assert row is not None
    assert row["hashtags"] == ["#only_new"]


def test_bulk_edit_hashtags_empty_clears_old(tmp_path: Path) -> None:
    p = tmp_path / "schedule_posts.json"
    p.write_text("[]", encoding="utf-8")
    mgr = SchedulePostsManager(json_path=p)
    mgr.upsert(
        {
            "page_id": "p",
            "account_id": "a",
            "post_type": "text",
            "hashtags": ["#old"],
        }
    )
    jid = str(mgr.load_all()[0]["id"])
    mgr.update_jobs_fields_batch([jid], hashtags=[])
    row = mgr.get_by_id(jid)
    assert row is not None
    assert row["hashtags"] == []


def test_bulk_edit_title_only_keeps_hashtags(tmp_path: Path) -> None:
    p = tmp_path / "schedule_posts.json"
    p.write_text("[]", encoding="utf-8")
    mgr = SchedulePostsManager(json_path=p)
    mgr.upsert(
        {
            "page_id": "p",
            "account_id": "a",
            "post_type": "text",
            "title": "T",
            "content": "C",
            "hashtags": ["#keep"],
            "scheduled_at": "2026-01-01T00:00:00+00:00",
        }
    )
    jid = str(mgr.load_all()[0]["id"])
    mgr.update_jobs_fields_batch([jid], title="Only title", content="Only title")
    row = mgr.get_by_id(jid)
    assert row is not None
    assert row["hashtags"] == ["#keep"]
    assert row["scheduled_at"] == "2026-01-01T00:00:00+00:00"


def test_bulk_content_update_single_write(tmp_path: Path) -> None:
    p = tmp_path / "schedule_posts.json"
    p.write_text("[]", encoding="utf-8")
    mgr = SchedulePostsManager(json_path=p)

    ids: list[str] = []
    for i in range(5):
        mgr.upsert(
            {
                "page_id": "538b89cc73f2",
                "account_id": "acc_1",
                "post_type": "video",
                "status": "pending",
                "title": f"Old {i}",
                "content": f"Old {i}",
                "hashtags": ["#old"],
            }
        )
        ids.append(str(mgr.load_all()[-1]["id"]))

    new_ht = split_hashtags_csv("viral, facebook")
    n = mgr.update_jobs_fields_batch(
        ids,
        title="Guess Who 🙈 #Short",
        content="Guess Who 🙈 #Short",
        hashtags=new_ht,
    )
    assert n == 5
    rows = mgr.load_all()
    pending = [r for r in rows if str(r.get("status")) == "pending"]
    assert len(pending) == 5
    for r in pending:
        assert r["title"] == "Guess Who 🙈 #Short"
        assert r["content"] == "Guess Who 🙈 #Short"
        assert r["hashtags"] == ["#viral", "#facebook"]

    # Chỉ pending trong tập — mô phỏng bulk edit GUI
    mgr.update_job_fields(ids[0], status="success")
    pending_ids = [jid for jid in ids if mgr.get_by_id(jid) and str(mgr.get_by_id(jid).get("status")) == "pending"]
    assert len(pending_ids) == 4
    n2 = mgr.update_jobs_fields_batch(pending_ids, title="Batch 2")
    assert n2 == 4
    assert mgr.get_by_id(ids[0])["title"] == "Guess Who 🙈 #Short"
    assert mgr.get_by_id(ids[1])["title"] == "Batch 2"
