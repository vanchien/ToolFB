"""Logic cổng Next/Post trong wizard Reel dashboard."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.automation.facebook_actions import (
    _reel_wizard_needs_next,
    _reel_wizard_ready_to_post,
    _reel_wizard_ready_to_share,
)


def test_ready_to_post_after_next_with_filled_caption() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_usable",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._reel_settings_screen_visible",
            return_value=False,
        ),
    ):
        assert _reel_wizard_ready_to_post(
            page, payload="hello", filled=True, next_clicks=2
        )


def test_ready_to_post_when_post_enabled_without_caption_fill() -> None:
    """Post đã enable — không bắt buộc nhập tiêu đề/mô tả."""
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_usable",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._reel_settings_screen_visible",
            return_value=False,
        ),
    ):
        assert _reel_wizard_ready_to_post(
            page, payload="hello", filled=False, next_clicks=0
        )


def test_ready_to_post_empty_payload_no_title() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_usable",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._reel_settings_screen_visible",
            return_value=False,
        ),
    ):
        assert _reel_wizard_ready_to_post(page, payload="", filled=False, next_clicks=0)


def test_needs_next_false_when_ready_after_next_clicks() -> None:
    """Regression: trước đây luôn truyền next_clicks=0 → không bao giờ bấm Post."""
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._reel_wizard_ready_to_post",
            return_value=True,
        ) as ready,
    ):
        assert not _reel_wizard_needs_next(
            page, payload="x", filled=True, next_clicks=3
        )
        ready.assert_called_once_with(
            page, payload="x", filled=True, next_clicks=3
        )


def test_needs_next_passes_actual_next_clicks() -> None:
    page = MagicMock()
    with patch(
        "src.automation.facebook_actions._reel_wizard_ready_to_post",
        return_value=False,
    ) as ready:
        _reel_wizard_needs_next(page, payload="", filled=False, next_clicks=5)
        ready.assert_called_once_with(
            page, payload="", filled=False, next_clicks=5
        )


def test_ready_to_post_way2_filled_zero_next() -> None:
    """Post details: caption + Publish, không cần Next."""
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_usable",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._reel_settings_screen_visible",
            return_value=False,
        ),
    ):
        assert _reel_wizard_ready_to_post(
            page, payload="caption", filled=True, next_clicks=0
        )


def test_ready_to_share_filled_zero_next() -> None:
    page = MagicMock()
    with patch(
        "src.automation.facebook_actions._reel_share_button_visible",
        return_value=True,
    ):
        assert _reel_wizard_ready_to_share(
            page, payload="x", filled=True, next_clicks=0
        )


def test_ready_to_post_reel_settings_filled_without_strict_post() -> None:
    """Reel settings: caption đã nhập, Post xanh có thể không khớp xpath strict."""
    page = MagicMock()
    page.get_by_text.return_value.first.is_visible.return_value = True
    with (
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_visible",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_usable",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._reel_settings_screen_visible",
            return_value=True,
        ),
    ):
        assert _reel_wizard_ready_to_post(
            page, payload="caption text", filled=True, next_clicks=2
        )


def test_needs_next_false_on_reel_settings_after_fill() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._reel_wizard_ready_to_post",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_visible",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._reel_settings_screen_visible",
            return_value=True,
        ),
    ):
        assert not _reel_wizard_needs_next(
            page, payload="x", filled=True, next_clicks=2
        )
