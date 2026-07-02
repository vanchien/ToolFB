"""Tests lưu/nạp cookie phiên Facebook."""

from __future__ import annotations

import json
from pathlib import Path

from src.services.facebook_session_persist import (
    accept_facebook_session_after_restore,
    apply_saved_cookie_path_to_mapped,
    auto_save_session_for_account,
    cookie_file_has_session,
    establish_facebook_session,
    ensure_session_before_interaction,
    last_resort_interaction_session,
    probe_existing_facebook_session,
    profile_session_ready_for_interaction,
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


def test_auto_save_falls_back_to_profile_session(tmp_path: Path) -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    with patch(
        "src.services.facebook_session_persist.persist_confirmed_facebook_session",
        return_value=False,
    ):
        with patch(
            "src.services.facebook_session_persist.profile_session_ready_for_interaction",
            return_value=(True, "Phiên profile (UID 1)"),
        ):
            with patch(
                "src.services.facebook_session_persist.persist_facebook_session",
                return_value=True,
            ) as persist:
                saved, path = auto_save_session_for_account(
                    page,
                    {"id": "acc_1", "cookie_path": "data/cookies/acc_1.json"},
                    require_confirm=True,
                )
    assert saved is True
    persist.assert_called_once()


def test_apply_saved_cookie_path_syncs_profile() -> None:
    ma = MappedAccount(account_id="UID_123", cookie_path="")
    acc = {
        "id": "UID_123",
        "cookie_path": "data/cookies/UID_123.json",
        "portable_path": "data/profiles/firefox/acc_123",
    }
    path = apply_saved_cookie_path_to_mapped(ma, acc)
    assert path == "data/cookies/UID_123.json"
    assert ma.cookie_path == "data/cookies/UID_123.json"
    assert ma.storage.profile_path == "data/profiles/firefox/acc_123"


def test_probe_existing_facebook_session_no_c_user() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    page.url = "https://www.facebook.com/"
    with patch(
        "src.services.facebook_session_persist.wait_profile_session_ready",
        return_value=(False, "Profile chưa có cookie c_user"),
    ):
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


def test_try_reuse_skips_cookie_file_when_profile_ready() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    with patch(
        "src.services.facebook_session_persist.profile_session_ready_for_interaction",
        return_value=(True, "Phiên profile sẵn có (UID 99)"),
    ):
        with patch("src.automation.facebook_actions.login_with_cookie") as login_ck:
            ok, detail = try_reuse_saved_cookie_session(
                page,
                {"id": "UID_99"},
                cookie_path="data/cookies/UID_99.json",
            )
    assert ok is True
    assert "profile" in detail.lower()
    login_ck.assert_not_called()


def test_profile_session_ready_detects_c_user() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    page.url = "https://www.facebook.com/"
    with patch(
        "src.services.facebook_session_recovery._read_facebook_c_user",
        return_value="12345",
    ):
        ok, detail = profile_session_ready_for_interaction(page, {"id": "UID_12345"})
    assert ok is True
    assert "12345" in detail


def test_profile_session_ready_escapes_login_when_c_user() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    page.url = "https://www.facebook.com/login/"

    def _leave_login(p: MagicMock) -> bool:
        p.url = "https://www.facebook.com/"
        return True

    with patch(
        "src.services.facebook_session_recovery._read_facebook_c_user",
        return_value="12345",
    ):
        with patch(
            "src.automation.facebook_actions.navigate_away_from_login_if_session_active",
            side_effect=_leave_login,
        ) as nav:
            ok, detail = profile_session_ready_for_interaction(page, {"id": "UID_12345"})
    nav.assert_called_once()
    assert ok is True


def test_navigate_away_from_login_if_session_active() -> None:
    from unittest.mock import MagicMock, patch

    from src.automation.facebook_actions import navigate_away_from_login_if_session_active

    page = MagicMock()
    page.url = "https://www.facebook.com/login/"
    with patch(
        "src.services.facebook_session_recovery._read_facebook_c_user",
        return_value="99",
    ):
        with patch(
            "src.automation.facebook_actions._page_shows_facebook_login_surface",
            return_value=True,
        ):
            ok = navigate_away_from_login_if_session_active(page)
    assert ok is True
    page.goto.assert_called_once()


def test_restore_returns_profile_when_cookies_ready_before_prime() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    page.url = "https://www.facebook.com/"
    with patch(
        "src.services.facebook_session_persist.wait_profile_session_ready",
        side_effect=[(True, "Phiên profile (UID 1)"), (True, "Phiên profile (UID 1)")],
    ):
        with patch(
            "src.automation.facebook_actions.prime_facebook_session_page",
        ) as prime:
            ok, mode = restore_facebook_session(page, {"id": "UID_1"}, prime=True)
    assert ok is True
    assert mode == "profile"
    prime.assert_not_called()


def test_establish_facebook_session_profile_first() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    page.url = "https://www.facebook.com/"
    with patch(
        "src.services.facebook_session_persist.wait_profile_session_ready",
        return_value=(True, "Phiên profile sẵn có (UID 1)"),
    ):
        with patch(
            "src.services.facebook_session_recovery._finalize_successful_recovery",
            return_value=True,
        ):
            ok, detail = establish_facebook_session(page, {"id": "UID_1"})
    assert ok is True
    assert "profile" in detail.lower()


def test_establish_facebook_session_skips_form_when_not_allowed() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    page.url = "https://www.facebook.com/"
    with patch(
        "src.services.facebook_session_persist.wait_profile_session_ready",
        return_value=(False, "no"),
    ):
        with patch(
            "src.services.facebook_session_persist.cookie_file_has_session",
            return_value=False,
        ):
            with patch(
                "src.services.facebook_session_persist.probe_existing_facebook_session",
                return_value=(False, "no"),
            ):
                ok, detail = establish_facebook_session(
                    page,
                    {"id": "UID_1"},
                    allow_form_login=False,
                )
    assert ok is False
    assert "mật khẩu" in detail.lower() or "form" in detail.lower()


def test_establish_facebook_session_tries_cookie_before_form() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    page.url = "about:blank"
    form_called = {"n": 0}

    def _form() -> bool:
        form_called["n"] += 1
        return True

    with patch(
        "src.automation.facebook_actions.prime_facebook_session_page",
    ):
        with patch(
            "src.services.facebook_session_persist.wait_profile_session_ready",
            return_value=(False, "no"),
        ):
            with patch(
                "src.services.facebook_session_persist.cookie_file_has_session",
                return_value=True,
            ):
                with patch(
                    "src.services.facebook_session_persist.try_reuse_saved_cookie_session",
                    return_value=(False, "cookie fail"),
                ) as reuse:
                    with patch(
                        "src.services.facebook_session_persist.probe_existing_facebook_session",
                        return_value=(False, "no"),
                    ):
                        with patch(
                            "src.services.facebook_session_recovery.confirm_facebook_session_logged_in",
                            return_value=(True, "OK"),
                        ):
                            with patch(
                                "src.services.facebook_session_recovery._finalize_successful_recovery",
                                return_value=True,
                            ):
                                ok, _ = establish_facebook_session(
                                    page,
                                    {"id": "UID_1"},
                                    allow_form_login=True,
                                    form_recover_fn=_form,
                                )
    reuse.assert_called_once()
    assert form_called["n"] == 1
    assert ok is True


def test_ensure_session_uses_profile_before_confirm(tmp_path: Path) -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    with patch(
        "src.services.facebook_session_persist.profile_session_ready_for_interaction",
        return_value=(True, "Phiên profile sẵn có (UID 1)"),
    ):
        with patch(
            "src.services.facebook_session_recovery.confirm_facebook_session_logged_in",
        ) as confirm:
            with patch(
                "src.services.facebook_session_recovery._finalize_successful_recovery",
                return_value=True,
            ):
                ok, detail = ensure_session_before_interaction(page, {"id": "UID_1"})
    assert ok is True
    confirm.assert_not_called()
    assert "profile" in detail.lower()


def test_accept_facebook_session_after_restore_fast_path() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    with patch(
        "src.services.facebook_session_persist.profile_session_ready_for_interaction",
        return_value=(True, "Phiên profile sẵn có (UID 1)"),
    ):
        with patch(
            "src.services.facebook_session_recovery.confirm_facebook_session_logged_in",
        ) as confirm:
            ok, persist, detail = accept_facebook_session_after_restore(
                page,
                {"id": "UID_1"},
                cookie_path="data/cookies/UID_1.json",
                session_mode="profile",
            )
    assert ok is True
    assert persist is True
    assert "profile" in detail.lower()
    confirm.assert_not_called()


def test_last_resort_interaction_session_reuse() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    with patch(
        "src.services.facebook_session_persist.establish_facebook_session",
        return_value=(True, "OK reuse"),
    ) as est:
        ok, detail = last_resort_interaction_session(
            page, {"id": "UID_1"}, cookie_path="data/cookies/UID_1.json"
        )
    est.assert_called_once()
    assert ok is True
    assert "reuse" in detail.lower()


def test_last_resort_interaction_session_form_fallback() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    page.url = "https://www.facebook.com/"
    form_called = {"n": 0}

    def _form() -> bool:
        form_called["n"] += 1
        return True

    with patch(
        "src.services.facebook_session_persist.wait_profile_session_ready",
        return_value=(False, "no"),
    ):
        with patch(
            "src.services.facebook_session_persist.cookie_file_has_session",
            return_value=False,
        ):
            with patch(
                "src.services.facebook_session_persist.probe_existing_facebook_session",
                return_value=(False, "no"),
            ):
                with patch(
                    "src.services.facebook_session_recovery.confirm_facebook_session_logged_in",
                    return_value=(True, "OK form"),
                ):
                    with patch(
                        "src.services.facebook_session_recovery._finalize_successful_recovery",
                        return_value=True,
                    ):
                        ok, detail = last_resort_interaction_session(
                            page,
                            {"id": "UID_1"},
                            allow_form_login=True,
                            form_recover_fn=_form,
                        )
    assert ok is True
    assert form_called["n"] == 1


def test_human_session_e2e_restore_then_gate() -> None:
    """E2E logic: accept restore → ensure một cổng, không gọi recover khi profile sẵn."""
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    account = {"id": "UID_99", "cookie_path": "data/cookies/UID_99.json"}
    recover_called = {"n": 0}

    def _recover() -> bool:
        recover_called["n"] += 1
        return False

    with patch(
        "src.services.facebook_session_persist.profile_session_ready_for_interaction",
        return_value=(True, "Phiên profile sẵn có (UID 99)"),
    ):
        with patch(
            "src.services.facebook_session_recovery._finalize_successful_recovery",
            return_value=True,
        ):
            rec, persist, det = accept_facebook_session_after_restore(
                page, account, cookie_path=account["cookie_path"], session_mode="profile"
            )
            ok_g, det_g = ensure_session_before_interaction(
                page, account, cookie_path=account["cookie_path"], recover_fn=_recover
            )

    assert rec and persist and ok_g
    assert "profile" in det.lower()
    assert recover_called["n"] == 0


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
