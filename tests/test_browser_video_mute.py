"""Tắt tiếng preview <video>/<audio> sau import trong automation Facebook."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from src.automation.facebook_actions import (
    _attach_media_automatic,
    _mute_browser_video_previews,
    _mute_browser_video_previews_after_attach,
)


def test_mute_browser_video_previews_page_and_frames() -> None:
    page = MagicMock()
    page.evaluate.return_value = 2
    frame = MagicMock()
    frame.evaluate.return_value = 1
    page.frames = [frame]

    n = _mute_browser_video_previews(page)

    assert n == 3
    assert page.evaluate.call_count >= 1
    frame.evaluate.assert_called_once()


def test_mute_browser_video_previews_with_scope() -> None:
    page = MagicMock()
    page.evaluate.return_value = 0
    page.frames = []
    scope = MagicMock()
    scope.evaluate.return_value = 1

    n = _mute_browser_video_previews(page, scope=scope)

    assert n == 1
    scope.evaluate.assert_called_once()


def test_mute_browser_video_previews_silent_skips_info_log() -> None:
    page = MagicMock()
    page.evaluate.return_value = 1
    page.frames = []

    with patch("src.automation.facebook_actions.logger") as log:
        _mute_browser_video_previews(page, silent=True)
        log.info.assert_not_called()


def test_mute_after_attach_retries() -> None:
    page = MagicMock()
    page.evaluate.return_value = 1
    page.frames = []
    page.wait_for_timeout = MagicMock()

    with patch(
        "src.automation.facebook_actions._mute_browser_video_previews",
        side_effect=[0, 2, 1],
    ) as mute:
        n = _mute_browser_video_previews_after_attach(page, attempts=3, interval_ms=100)

    assert n == 3
    assert mute.call_count == 3
    assert page.wait_for_timeout.call_count == 2


def test_attach_media_automatic_mutes_video_on_success(tmp_path: Path) -> None:
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"x")

    page = MagicMock()
    page.frames = []

    with (
        patch(
            "src.automation.facebook_actions._set_file_via_existing_input",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._mute_browser_video_previews_after_attach",
        ) as after_mute,
        patch(
            "src.automation.facebook_actions._is_meta_business_composer_context",
            return_value=False,
        ),
    ):
        ok = _attach_media_automatic(page, video, kind="video", context="test")

    assert ok is True
    after_mute.assert_called_once_with(page, scope=None)


def test_attach_media_automatic_skips_mute_for_image(tmp_path: Path) -> None:
    img = tmp_path / "pic.jpg"
    img.write_bytes(b"x")

    page = MagicMock()
    page.frames = []

    with (
        patch(
            "src.automation.facebook_actions._set_file_via_existing_input",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._mute_browser_video_previews_after_attach",
        ) as after_mute,
        patch(
            "src.automation.facebook_actions._is_meta_business_composer_context",
            return_value=False,
        ),
    ):
        ok = _attach_media_automatic(page, img, kind="image", context="test")

    assert ok is True
    after_mute.assert_not_called()
