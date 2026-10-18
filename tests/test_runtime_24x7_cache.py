"""Cache JSON / cookie dùng chung process — đọc lặp không đơ GUI khi nhiều Page/TK."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from src.services.facebook_session_persist import (
    cookie_file_has_session,
    invalidate_cookie_session_cache,
)
from src.utils.json_list_cache import (
    forget_json_list,
    peek_json_list_fresh,
    remember_json_list,
)
from src.utils.pages_manager import PagesManager


def test_json_list_cache_fresh_then_stale(tmp_path: Path) -> None:
    path = tmp_path / "rows.json"
    path.write_text("[]", encoding="utf-8")
    mtime = path.stat().st_mtime
    remember_json_list(path, mtime, [{"id": "a"}])
    hit = peek_json_list_fresh(path)
    assert hit == [{"id": "a"}]
    path.write_text("[1]", encoding="utf-8")
    assert peek_json_list_fresh(path) is None
    forget_json_list(path)
    assert peek_json_list_fresh(path) is None


def test_pages_second_manager_skips_disk_when_cache_fresh(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("src.utils.pages_manager.ensure_page_workspace", lambda _pid: None)
    path = tmp_path / "pages.json"
    path.write_text("[]", encoding="utf-8")
    first = PagesManager(json_path=path)
    first.upsert(
        {
            "id": "",
            "account_id": "acc1",
            "page_name": "Fan A",
            "page_url": "https://www.facebook.com/fana",
            "post_style": "post",
        }
    )
    reads = {"n": 0}
    real_read = Path.read_text

    def _counted(self: Path, *args, **kwargs):
        if self.resolve() == path.resolve():
            reads["n"] += 1
        return real_read(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _counted)
    second = PagesManager(json_path=path)
    rows = second.load_all()
    assert len(rows) == 1
    assert rows[0]["page_name"] == "Fan A"
    assert reads["n"] == 0


def test_cookie_file_has_session_uses_mtime_cache(tmp_path: Path) -> None:
    invalidate_cookie_session_cache()
    p = tmp_path / "ck.json"
    p.write_text(
        json.dumps({"cookies": [{"name": "c_user", "value": "123", "domain": ".facebook.com"}]}),
        encoding="utf-8",
    )
    assert cookie_file_has_session(p) is True
    with patch.object(Path, "read_text", side_effect=AssertionError("không được đọc lại cookie")):
        assert cookie_file_has_session(p) is True
    p.write_text("[]", encoding="utf-8")
    assert cookie_file_has_session(p) is False
    invalidate_cookie_session_cache(p)
