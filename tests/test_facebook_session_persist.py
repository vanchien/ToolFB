"""Tests lưu/nạp cookie phiên Facebook."""

from __future__ import annotations

import json
from pathlib import Path

from src.services.facebook_session_persist import (
    apply_saved_cookie_path_to_mapped,
    cookie_file_has_session,
    ensure_session_before_interaction,
    probe_existing_facebook_session,
    resolve_cookie_file,
    restore_facebook_session,
    try_reuse_saved_cookie_session,
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


def test_probe_existing_facebook_session_no_c_user() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    page.url = "https://www.facebook.com/"
    with patch(
        "src.automation.facebook_actions._facebook_context_cookie_names",
        return_value=set(),
    ):
        with patch(
            "src.automation.facebook_actions.prime_facebook_session_page",
        ):
            with patch(
                "src.automation.facebook_actions._force_www_facebook_if_mobile_redirect",
            ):
                ok, detail = probe_existing_facebook_session(page, {"id": "UID_1"})
    assert ok is False
    assert "c_user" in detail


def test_restore_facebook_session_uses_probe_before_fail() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    page.url = "https://www.facebook.com/"
    with patch(
        "src.automation.facebook_actions.prime_facebook_session_page",
    ):
        with patch(
            "src.automation.facebook_actions._force_www_facebook_if_mobile_redirect",
        ):
            with patch(
                "src.automation.facebook_actions.facebook_session_appears_logged_in",
                return_value=False,
            ):
                with patch(
                    "src.services.facebook_session_persist.cookie_file_has_session",
                    return_value=False,
                ):
                    with patch(
                        "src.services.facebook_session_persist.probe_existing_facebook_session",
                        return_value=(True, "Đã vào tài khoản Facebook (UID 99)"),
                    ) as probe:
                        ok, mode = restore_facebook_session(page, {"id": "UID_1"})
    assert ok is True
    assert mode == "profile_probe"
    probe.assert_called_once()


def test_resolve_cookie_file_relative() -> None:
    p = resolve_cookie_file("data/cookies/UID_test.json")
    assert p is not None
    assert p.name == "UID_test.json"


def test_try_reuse_saved_cookie_session_no_file() -> None:
    from unittest.mock import MagicMock

    page = MagicMock()
    ok, detail = try_reuse_saved_cookie_session(page, {"id": "UID_1"}, cookie_path="/missing.json")
    assert ok is False
    assert "cookie" in detail.lower()


def test_ensure_session_reuses_cookie_before_recover_fn(tmp_path: Path) -> None:
    from unittest.mock import MagicMock, patch

    ck = tmp_path / "UID_1.json"
    ck.write_text(
        json.dumps({"cookies": [{"name": "c_user", "value": "12345", "domain": ".facebook.com"}]}),
        encoding="utf-8",
    )
    page = MagicMock()
    recover_called = {"n": 0}

    def _recover() -> bool:
        recover_called["n"] += 1
        return False

    with patch(
        "src.services.facebook_session_recovery.confirm_facebook_session_logged_in",
        side_effect=[(False, "chưa"), (True, "OK")],
    ):
        with patch(
            "src.services.facebook_session_persist.try_reuse_saved_cookie_session",
            return_value=(True, "Tái sử dụng cookie"),
        ) as reuse:
            with patch(
                "src.services.facebook_session_recovery._finalize_successful_recovery",
                return_value=True,
            ):
                ok, detail = ensure_session_before_interaction(
                    page,
                    {"id": "UID_12345", "cookie_path": str(ck)},
                    recover_fn=_recover,
                )
    assert ok is True
    assert "cookie" in detail.lower() or "Tái" in detail
    reuse.assert_called_once()
    assert recover_called["n"] == 0
