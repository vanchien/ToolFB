"""Nút Switch sidebar Manage Page (div role=none)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.automation.facebook_actions import (
    _click_manage_page_sidebar_switch,
    _ensure_page_role_switched,
    _page_switch_sidebar_hint_visible,
)


def test_sidebar_hint_detects_take_more_actions() -> None:
    page = MagicMock()
    loc = MagicMock()
    loc.first.is_visible.return_value = True
    page.get_by_text.return_value = loc
    assert _page_switch_sidebar_hint_visible(page)


def test_click_sidebar_switch_role_none() -> None:
    page = MagicMock()
    target = MagicMock()
    target.count.return_value = 1
    target.is_visible.return_value = True
    loc = MagicMock()
    loc.count.return_value = 1
    loc.last = target
    page.locator.return_value = loc
    with patch(
        "src.automation.facebook_actions._page_switch_sidebar_hint_visible",
        return_value=False,
    ):
        assert _click_manage_page_sidebar_switch(page)


def test_ensure_page_role_switched_prefers_sidebar() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._click_visible_enabled_button",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._click_manage_page_sidebar_switch",
            return_value=True,
        ) as sidebar,
        patch(
            "src.automation.facebook_actions._confirm_switch_profiles_popup",
        ) as confirm,
        patch(
            "src.automation.facebook_actions._page_switch_sidebar_hint_visible",
            return_value=False,
        ),
    ):
        assert _ensure_page_role_switched(
            page, page_display_name="Best News US", page_url="https://www.facebook.com/123"
        )
        sidebar.assert_called_once()
        confirm.assert_called_once()
