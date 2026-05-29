"""Xác nhận Post Reel dashboard: đã bấm Post + có video trên Page."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.automation.facebook_actions import (
    _reel_post_submit_acknowledged,
    verify_reel_dashboard_post_submitted,
)


def test_verify_reel_requires_post_clicked() -> None:
    page = MagicMock()
    with pytest.raises(RuntimeError, match="Chưa xác nhận được đã bấm nút Post"):
        verify_reel_dashboard_post_submitted(page, post_clicked=False)


def test_verify_reel_success_with_video_row() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions.verify_post_submitted",
        ) as vps,
        patch(
            "src.automation.facebook_actions._meta_published_posts_has_video_row",
            return_value=True,
        ),
        patch("src.automation.facebook_actions._env_int", return_value=5000),
    ):
        verify_reel_dashboard_post_submitted(
            page, post_clicked=True, text_snippet="hello caption", page_url=""
        )
        vps.assert_called_once()
        page.wait_for_timeout.assert_called()


def test_verify_reel_fails_without_video_evidence() -> None:
    page = MagicMock()
    with (
        patch("src.automation.facebook_actions.verify_post_submitted"),
        patch(
            "src.automation.facebook_actions._meta_published_posts_has_video_row",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._navigate_meta_published_posts_best_effort",
        ),
        patch(
            "src.automation.facebook_actions._reel_post_likely_submitted",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._reel_post_submit_strong_signal",
            return_value=False,
        ),
        patch("src.automation.facebook_actions._env_int", return_value=5000),
        patch("src.automation.facebook_actions._failure_screenshot"),
    ):
        with pytest.raises(RuntimeError, match="không thấy video/reel mới"):
            verify_reel_dashboard_post_submitted(
                page, post_clicked=True, text_snippet="", page_url="https://www.facebook.com/103833422779877"
            )


def test_post_submit_acknowledged_when_no_settings_and_no_post() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._reel_settings_screen_visible",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._reel_strict_post_button_usable",
            return_value=False,
        ),
    ):
        assert _reel_post_submit_acknowledged(page, timeout_ms=500)
