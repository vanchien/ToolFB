"""Tests khôi phục phiên sau launch persistent context."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.services.facebook_session_persist import prepare_persistent_session_after_launch


def test_prepare_bootstraps_cookie_when_page_not_logged_in() -> None:
    context = MagicMock()
    page = MagicMock()
    account = {"id": "acc1", "portable_path": "data/profiles/firefox/acc1", "cookie_path": "data/cookies/acc1.json"}

    with patch(
        "src.services.facebook_session_persist.cookie_file_has_session",
        return_value=True,
    ), patch(
        "src.services.facebook_session_persist.ensure_account_cookie_path",
        return_value="data/cookies/acc1.json",
    ), patch(
        "src.utils.account_browser_profile.portable_profile_likely_has_session",
        return_value=False,
    ), patch(
        "src.services.facebook_session_persist.profile_session_ready_for_interaction",
        return_value=(False, "Profile chưa có cookie c_user"),
    ) as ready, patch(
        "src.services.facebook_session_persist.bootstrap_cookies_into_context",
        return_value=True,
    ) as boot, patch(
        "src.services.facebook_session_persist.restore_facebook_session",
        return_value=(True, "cookie"),
    ) as restore:
        ok, mode = prepare_persistent_session_after_launch(context, page, account, cookie_path="data/cookies/acc1.json")
        ready.assert_called_once()
        boot.assert_called_once()
        restore.assert_called_once()
        assert ok is True
        assert mode == "cookie"
