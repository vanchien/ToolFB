"""Post Reel: click strategies và xác nhận UI."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.automation.facebook_actions import (
    _click_reel_post_best_effort,
    _dispatch_reel_post_click,
    _reel_post_label_matches,
    _reel_post_submit_acknowledged,
)


def test_reel_post_label_matches() -> None:
    assert _reel_post_label_matches("Post")
    assert _reel_post_label_matches("Đăng")
    assert _reel_post_label_matches("Publish")
    assert not _reel_post_label_matches("Posts")
    assert not _reel_post_label_matches("Create post")
    assert not _reel_post_label_matches("")


def test_ack_false_when_settings_open_and_post_visible() -> None:
    """Regression: không coi «không match strict» là đã đăng khi Reel settings còn mở."""
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._reel_post_submit_strong_signal",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._reel_settings_screen_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._reel_footer_post_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._active_reel_dialog",
            return_value=MagicMock(),
        ),
    ):
        assert not _reel_post_submit_acknowledged(page, timeout_ms=500)


def test_click_reel_post_fails_without_ui_ack() -> None:
    page = MagicMock()
    dialog = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._reel_post_submit_strong_signal",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._reel_post_submit_acknowledged",
            side_effect=[False, False, False, False, False, False, False],
        ),
        patch(
            "src.automation.facebook_actions._dismiss_reel_hashtag_suggestion",
        ),
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_usable",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._reel_settings_screen_visible",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._active_reel_dialog",
            return_value=dialog,
        ),
        patch(
            "src.automation.facebook_actions._dispatch_reel_post_click",
            return_value=True,
        ),
        patch("src.automation.facebook_actions._env_int", return_value=500),
        patch(
            "src.automation.facebook_actions._reel_inter_click_wait_ms",
            return_value=50,
        ),
    ):
        assert not _click_reel_post_best_effort(page)


def test_click_reel_post_succeeds_with_ui_ack() -> None:
    page = MagicMock()
    dialog = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._reel_post_submit_strong_signal",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._reel_post_submit_acknowledged",
            side_effect=[False, True],
        ),
        patch(
            "src.automation.facebook_actions._dismiss_reel_hashtag_suggestion",
        ),
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_usable",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._reel_settings_screen_visible",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._active_reel_dialog",
            return_value=dialog,
        ),
        patch(
            "src.automation.facebook_actions._dispatch_reel_post_click",
            return_value=True,
        ),
        patch("src.automation.facebook_actions._env_int", return_value=500),
        patch(
            "src.automation.facebook_actions._reel_inter_click_wait_ms",
            return_value=50,
        ),
    ):
        assert _click_reel_post_best_effort(page)


def test_dispatch_attempt1_uses_js_first() -> None:
    page = MagicMock()
    dialog = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._reel_settings_screen_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._reel_dialog_post_js_click",
            return_value=True,
        ) as js_click,
        patch(
            "src.automation.facebook_actions._click_reel_post_locators_batch",
        ) as batch,
        patch(
            "src.automation.facebook_actions.human_pause",
        ),
    ):
        assert _dispatch_reel_post_click(page, dialog, attempt=1)
        js_click.assert_called_once()
        batch.assert_not_called()


def test_dispatch_attempt2_uses_mouse_batch() -> None:
    page = MagicMock()
    dialog = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._reel_settings_screen_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._dismiss_reel_hashtag_suggestion",
        ),
        patch(
            "src.automation.facebook_actions._reel_dialog_post_js_click",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._click_reel_post_locators_batch",
            return_value=True,
        ) as batch,
    ):
        assert _dispatch_reel_post_click(page, dialog, attempt=2)
        batch.assert_called_once_with(
            page, dialog, prefer_mouse=True, require_footer=True
        )
