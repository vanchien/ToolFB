"""Checkpoint treo / skeleton: thoát nhanh, không gọi page.content() vô hạn."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.services.facebook_session_recovery import (
    facebook_checkpoint_is_blocking,
    facebook_page_looks_like_stuck_checkpoint,
    try_skip_soft_facebook_checkpoint,
)


def test_stuck_checkpoint_detected_on_empty_body() -> None:
    page = MagicMock()
    page.url = "https://www.facebook.com/checkpoint/1501092823525282/"
    page.locator.return_value.inner_text.return_value = "  "
    page.evaluate.return_value = ""
    page.wait_for_timeout = MagicMock()
    assert facebook_page_looks_like_stuck_checkpoint(page, wait_ms=0) is True


def test_checkpoint_is_blocking_on_skeleton_without_wait() -> None:
    page = MagicMock()
    page.url = "https://www.facebook.com/checkpoint/828281030927956/"
    page.locator.return_value.inner_text.return_value = ""
    page.evaluate.return_value = ""
    assert facebook_checkpoint_is_blocking(page) is True


def test_try_skip_soft_bails_on_stuck_skeleton() -> None:
    page = MagicMock()
    page.url = "https://www.facebook.com/checkpoint/1501092823525282/"
    page.locator.return_value.inner_text.return_value = ""
    page.evaluate.return_value = ""
    page.wait_for_timeout = MagicMock()
    with patch(
        "src.services.facebook_session_recovery.facebook_page_looks_like_stuck_checkpoint",
        return_value=True,
    ):
        assert try_skip_soft_facebook_checkpoint(page) is False


def test_checkpoint_page_text_never_calls_page_content() -> None:
    from src.services.facebook_session_recovery import _checkpoint_page_text

    page = MagicMock()
    page.locator.return_value.inner_text.side_effect = RuntimeError("timeout")
    page.evaluate.return_value = "hello checkpoint"
    page.content = MagicMock(side_effect=AssertionError("không được gọi content()"))
    assert "hello" in _checkpoint_page_text(page)
    page.content.assert_not_called()
