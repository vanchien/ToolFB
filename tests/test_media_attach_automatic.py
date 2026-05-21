"""Upload media tự động — không mở hộp thoại chọn file OS."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.automation.facebook_actions import (
    _attach_media_automatic,
    _dismiss_leaked_native_file_dialog,
    _native_file_chooser_allowed,
    _set_file_via_business_add_button,
)


def test_native_file_chooser_default_off() -> None:
    with patch.dict(os.environ, {"FB_ALLOW_NATIVE_FILE_CHOOSER": "0"}, clear=False):
        assert _native_file_chooser_allowed() is False


def test_attach_media_uses_direct_input_first(tmp_path: Path) -> None:
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"\x00\x00\x00\x18ftypmp42")
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._is_meta_business_composer_context",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._dismiss_blocking_ui_before_business_media",
        ),
        patch(
            "src.automation.facebook_actions._set_file_via_existing_input",
            return_value=True,
        ) as mock_input,
        patch(
            "src.automation.facebook_actions._set_file_via_business_add_button",
        ) as mock_btn,
    ):
        assert _attach_media_automatic(page, video, kind="video", context="test") is True
    mock_input.assert_called()
    mock_btn.assert_not_called()


def test_attach_media_page_fallback_when_dialog_scope_fails(tmp_path: Path) -> None:
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"\x00\x00\x00\x18ftypmp42")
    page = MagicMock()
    scope = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._is_meta_business_composer_context",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._set_file_via_existing_input",
            side_effect=[False, True],
        ) as mock_input,
        patch(
            "src.automation.facebook_actions._set_file_via_business_add_button",
            return_value=False,
        ),
    ):
        assert _attach_media_automatic(page, video, kind="video", scope=scope, context="reel") is True
    assert mock_input.call_count >= 2
    assert mock_input.call_args_list[-1].kwargs.get("scope") is None


def test_business_add_button_dismisses_on_filechooser_fail() -> None:
    page = MagicMock()
    page.expect_file_chooser.side_effect = RuntimeError("timeout")
    with (
        patch(
            "src.automation.facebook_actions._set_file_via_existing_input",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._collect_add_media_button_locators",
            return_value=[MagicMock()],
        ),
        patch(
            "src.automation.facebook_actions._dismiss_leaked_native_file_dialog",
        ) as dismiss,
    ):
        assert _set_file_via_business_add_button(page, Path("x.mp4"), kind="video") is False
    assert dismiss.call_count >= 1


def test_dismiss_leaked_native_file_dialog_presses_escape() -> None:
    page = MagicMock()
    _dismiss_leaked_native_file_dialog(page)
    assert page.keyboard.press.call_count >= 1
