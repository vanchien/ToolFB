"""AIContentService — stub khi không có GEMINI_API_KEY."""

from __future__ import annotations

import pytest

from src.services.ai_content_service import AIContentService
from src.utils import page_workspace as pw


def test_suggest_captions_stub(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(pw, "project_root", lambda: tmp_path)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    ai = AIContentService()
    caps = ai.suggest_captions("pgai01", "Chủ đề test", count=3)
    assert len(caps) == 3
    assert all("stub" in c.lower() or "chủ đề test" in c for c in caps)


def test_save_caption_draft_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(pw, "project_root", lambda: tmp_path)
    ai = AIContentService()
    p = ai.save_caption_as_draft_file("pgai02", "Nội dung draft", stem="t1")
    assert p.is_file()
    assert "Nội dung draft" in p.read_text(encoding="utf-8")
    assert "drafts" in str(p).replace("\\", "/")
