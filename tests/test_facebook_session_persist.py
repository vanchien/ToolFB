"""Tests lưu/nạp cookie phiên Facebook."""

from __future__ import annotations

import json
from pathlib import Path

from src.services.facebook_session_persist import (
    apply_saved_cookie_path_to_mapped,
    cookie_file_has_session,
    resolve_cookie_file,
)
from src.models.mapped_account import MappedAccount


def test_cookie_file_has_session_true(tmp_path: Path) -> None:
    p = tmp_path / "ok.json"
    p.write_text(
        json.dumps({"cookies": [{"name": "c_user", "value": "12345", "domain": ".facebook.com"}]}),
        encoding="utf-8",
    )
    assert cookie_file_has_session(p) is True


def test_cookie_file_has_session_false_empty(tmp_path: Path) -> None:
    p = tmp_path / "empty.json"
    p.write_text("[]", encoding="utf-8")
    assert cookie_file_has_session(p) is False


def test_apply_saved_cookie_path_to_mapped() -> None:
    ma = MappedAccount(account_id="UID_123", cookie_path="")
    acc = {"id": "UID_123", "cookie_path": "data/cookies/UID_123.json"}
    path = apply_saved_cookie_path_to_mapped(ma, acc)
    assert path == "data/cookies/UID_123.json"
    assert ma.cookie_path == "data/cookies/UID_123.json"


def test_resolve_cookie_file_relative() -> None:
    p = resolve_cookie_file("data/cookies/UID_test.json")
    assert p is not None
    assert p.name == "UID_test.json"
