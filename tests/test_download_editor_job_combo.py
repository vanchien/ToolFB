"""Job tải → combobox Video Editor."""

from __future__ import annotations

from pathlib import Path

from src.services.universal_video_downloader import (
    build_download_job_combo_options,
    clear_pending_video_editor_job,
    read_pending_video_editor_job,
    write_pending_video_editor_job,
)


def test_combo_shows_job_with_videos() -> None:
    jobs = [
        {
            "id": "dl_abc123",
            "name": "FB batch",
            "platform": "facebook",
            "status": "completed",
            "created_at": "2026-06-01T10:00:00",
        }
    ]
    videos = [
        {"id": "v1", "download_job_id": "dl_abc123", "video_path": "/a.mp4"},
        {"id": "v2", "download_job_id": "dl_abc123", "video_path": "/b.mp4"},
    ]
    vals, mapping = build_download_job_combo_options(jobs, videos, show_empty=False)
    assert len(vals) == 1
    assert "2 video" in vals[0]
    assert mapping[vals[0]] == "dl_abc123"


def test_combo_shows_completed_job_without_video_rows() -> None:
    """Job vừa finalize nhưng metadata video chưa kịp — vẫn hiện để user thấy."""
    jobs = [
        {
            "id": "dl_empty1",
            "platform": "youtube",
            "status": "completed",
            "created_at": "2026-06-02T12:00:00",
        }
    ]
    vals, mapping = build_download_job_combo_options(jobs, [], show_empty=False)
    assert len(vals) == 1
    assert mapping[vals[0]] == "dl_empty1"
    assert "0 video" in vals[0]


def test_combo_hides_pending_empty_job() -> None:
    jobs = [{"id": "dl_pend", "platform": "tiktok", "status": "pending"}]
    vals, _ = build_download_job_combo_options(jobs, [], show_empty=False)
    assert vals == []


def test_combo_orphan_videos_without_job_record() -> None:
    videos = [{"id": "v1", "download_job_id": "dl_orphan", "video_path": "/x.mp4"}]
    vals, mapping = build_download_job_combo_options([], videos, show_empty=False)
    assert len(vals) == 1
    assert mapping[vals[0]] == "dl_orphan"


def test_pending_job_file_roundtrip(tmp_path: Path) -> None:
    paths = {
        "root": tmp_path,
        "jobs_file": tmp_path / "download_jobs.json",
        "videos_file": tmp_path / "downloaded_videos.json",
        "archive": tmp_path / "archive.txt",
    }
    write_pending_video_editor_job("dl_pending99", paths=paths)
    assert read_pending_video_editor_job(paths=paths, consume=False) == "dl_pending99"
    assert read_pending_video_editor_job(paths=paths, consume=True) == "dl_pending99"
    assert read_pending_video_editor_job(paths=paths, consume=False) == ""
    clear_pending_video_editor_job(paths=paths)
