"""Kiểm tra PagesManager (pages.json)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.utils.pages_manager import PagesManager


@pytest.fixture(autouse=True)
def _no_workspace_on_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tránh ghi ``data/pages/`` vào repo khi test dùng tmp_path cho pages.json."""
    monkeypatch.setattr("src.utils.pages_manager.ensure_page_workspace", lambda _pid: None)


def test_pages_upsert_and_get(tmp_path: Path) -> None:
    p = tmp_path / "pages.json"
    p.write_text("[]", encoding="utf-8")
    m = PagesManager(json_path=p)
    m.upsert(
        {
            "id": "",
            "account_id": "acc1",
            "page_name": "Fan A",
            "page_url": "https://www.facebook.com/fana",
            "post_style": "post",
            "topic": "Tin tức công nghệ",
            "content_style": "ngắn gọn",
        }
    )
    rows = m.load_all()
    assert len(rows) == 1
    assert rows[0]["page_name"] == "Fan A"
    assert rows[0]["account_id"] == "acc1"
    got = m.get_by_id(str(rows[0]["id"]))
    assert got is not None
    assert got["post_style"] == "post"
    assert got.get("topic") == "Tin tức công nghệ"
    assert got.get("content_style") == "ngắn gọn"


def test_pages_invalid_page_kind(tmp_path: Path) -> None:
    p = tmp_path / "pages.json"
    p.write_text("[]", encoding="utf-8")
    m = PagesManager(json_path=p)
    try:
        m.save_all(
            [
                {
                    "id": "x",
                    "account_id": "a",
                    "page_name": "n",
                    "page_url": "https://a.com",
                    "post_style": "post",
                    "page_kind": "invalid",
                }
            ]
        )
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def test_pages_invalid_post_style(tmp_path: Path) -> None:
    p = tmp_path / "pages.json"
    p.write_text("[]", encoding="utf-8")
    m = PagesManager(json_path=p)
    try:
        m.save_all(
            [
                {
                    "id": "x",
                    "account_id": "a",
                    "page_name": "n",
                    "page_url": "https://a.com",
                    "post_style": "invalid",
                }
            ]
        )
    except ValueError:
        return
    raise AssertionError("expected ValueError")
