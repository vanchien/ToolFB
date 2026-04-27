"""Kiểm tra validation pipeline đăng bài."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.automation.browser_factory import _project_root
from src.services.job_post_runtime import (
    JobRunTracker,
    job_run_monitor_path,
    validate_account_for_post_job,
    validate_page_for_post_job,
    validate_queue_job_payload,
)
from src.scheduler import _compose_job_text_payload, _draft_id_for_queue_job


def test_validate_account_rejects_missing_profile() -> None:
    acc = {
        "id": "a1",
        "status": "active",
        "browser_type": "chromium",
        "portable_path": "data/profiles/__does_not_exist__",
        "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
        "cookie_path": "",
        "use_proxy": False,
    }
    with pytest.raises(ValueError, match="profile"):
        validate_account_for_post_job(acc, project_root=_project_root())


def test_validate_page_mismatch_account() -> None:
    row = {"id": "p1", "account_id": "other", "page_url": "https://www.facebook.com/x", "page_name": "x"}
    with pytest.raises(ValueError, match="không thuộc"):
        validate_page_for_post_job(row, "acc1")


def test_validate_job_image_requires_file(tmp_path) -> None:
    job = {"post_type": "image", "media_files": [str(tmp_path / "nope.jpg")]}
    with pytest.raises(ValueError, match="không tồn tại"):
        validate_queue_job_payload(job, project_root=tmp_path)


def test_job_run_tracker_writes_monitor(tmp_path: Path) -> None:
    t = JobRunTracker("abc12", project_root=tmp_path)
    t.set_step("VERIFY_RESULT", "Đang kiểm tra")
    p = job_run_monitor_path(project_root=tmp_path)
    assert p.is_file()
    data = json.loads(p.read_text(encoding="utf-8"))
    assert data["job_id"] == "abc12"
    assert data["step"] == "VERIFY_RESULT"


def test_compose_job_text_payload() -> None:
    text = "Hello world"
    job = {"title": "My Title", "hashtags": ["abc", "#xyz", "a b c", ""]}
    out = _compose_job_text_payload(text, job)
    assert out.startswith("My Title")
    assert "Hello world" in out
    assert "#abc" in out
    assert "#xyz" in out


def test_compose_job_text_payload_video_does_not_fallback_ai_text() -> None:
    ai_text = "AI sinh caption dài từ topic page"
    job = {"post_type": "video", "title": "", "content": "", "hashtags": []}
    out = _compose_job_text_payload(ai_text, job)
    assert out == ""


def test_draft_id_for_queue_job_keeps_video_media_without_body(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_load(_draft_id: str) -> None:
        return None

    def _fake_save_draft(**kwargs: object) -> object:
        captured.update(kwargs)
        return {"id": kwargs.get("draft_id", "")}

    monkeypatch.setattr("src.scheduler.load_draft", _fake_load)
    monkeypatch.setattr("src.scheduler.save_draft", _fake_save_draft)

    did = _draft_id_for_queue_job(
        {
            "id": "job123",
            "page_id": "p1",
            "post_type": "video",
            "content": "",
            "media_files": [r"C:\video\a.mp4"],
        }
    )
    assert did.startswith("schjjob123")
    assert captured.get("media_paths") == [r"C:\video\a.mp4"]


def test_run_path_resolves_draft_when_draft_id_missing_but_job_has_video() -> None:
    """Giống «Đăng luôn job»: không truyền draft_id nhưng queue_job có media → vẫn có draft."""
    from src.scheduler import _draft_id_for_queue_job

    draft_id = None
    queue_job = {"id": "2546fc3fbd99494f", "content": "", "media_files": [r"C:\clips\a.mp4"]}
    resolved = str(draft_id or "").strip()
    if not resolved and queue_job:
        resolved = _draft_id_for_queue_job(dict(queue_job))
    assert resolved.startswith("schj2546fc3fbd99494f")
