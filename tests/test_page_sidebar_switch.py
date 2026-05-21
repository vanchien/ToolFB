"""Nút Switch sidebar Manage Page (div role=none)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.automation.facebook_actions import (
    _click_manage_page_sidebar_switch,
    _click_switch_in_sidebar_cta_card,
    _ensure_page_role_switched,
    _page_switch_sidebar_hint_visible,
)


def test_sidebar_hint_detects_take_more_actions() -> None:
    page = MagicMock()
    loc = MagicMock()
    loc.first.is_visible.return_value = True
    page.get_by_text.return_value = loc
    assert _page_switch_sidebar_hint_visible(page)


def test_click_manage_page_prefers_meta_exact() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._click_meta_switch_cta_exact",
            return_value=True,
        ) as exact,
        patch(
            "src.automation.facebook_actions._click_switch_in_sidebar_cta_card",
            return_value=False,
        ),
    ):
        assert _click_manage_page_sidebar_switch(page)
    exact.assert_called()


def test_click_manage_page_falls_back_to_cta_card() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._click_meta_switch_cta_exact",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._click_switch_in_sidebar_cta_card",
            return_value=True,
        ) as cta,
        patch(
            "src.automation.facebook_actions._click_sidebar_switch_role_button",
            return_value=False,
        ),
    ):
        assert _click_manage_page_sidebar_switch(page)
    cta.assert_called_once()


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
            "src.automation.facebook_actions._view_only_guard_active_on_page",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._page_role_acting_as_page",
            side_effect=[False, True],
        ),
        patch(
            "src.automation.facebook_actions._page_switch_ui_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._page_switch_sidebar_hint_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._click_visible_enabled_button",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._click_manage_page_sidebar_switch",
            return_value=True,
        ) as sidebar,
        patch(
            "src.automation.facebook_actions._wait_after_page_switch_click",
        ),
        patch(
            "src.automation.facebook_actions._wait_page_role_switch_complete",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._handle_switch_profiles_popup_if_present",
            return_value=True,
        ) as popup,
    ):
        assert _ensure_page_role_switched(
            page, page_display_name="Best News US", page_url="https://www.facebook.com/123"
        )
        sidebar.assert_called()
        popup.assert_called()


def test_ensure_page_role_fails_if_url_ok_but_still_cta() -> None:
    """Không coi đúng URL là đã switch."""
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._view_only_guard_active_on_page",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._page_role_acting_as_page",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._page_switch_ui_visible",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._manage_page_switch_cta_still_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._click_manage_page_sidebar_switch",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._click_visible_enabled_button",
            return_value=False,
        ),
        patch("src.automation.facebook_actions._failure_screenshot"),
    ):
        assert not _ensure_page_role_switched(page, page_url="https://www.facebook.com/103833422779877")


def test_handle_switch_profiles_popup_confirm() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._switch_profiles_dialog_visible",
            side_effect=[True, False, False],
        ),
        patch(
            "src.automation.facebook_actions._click_switch_profiles_popup_confirm",
            return_value=True,
        ) as confirm,
        patch(
            "src.automation.facebook_actions._page_role_acting_as_page",
            return_value=True,
        ),
    ):
        from src.automation.facebook_actions import _handle_switch_profiles_popup_if_present

        assert _handle_switch_profiles_popup_if_present(
            page, page_display_name="G-Force Ghoul", appear_wait_ms=800, settle_ms=2000
        )
    confirm.assert_called_once()


def test_ensure_switched_passes_page_name() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._page_switch_ui_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._ensure_page_role_switched",
        ) as role_sw,
    ):
        from src.automation.facebook_actions import _ensure_switched_into_page_if_needed

        _ensure_switched_into_page_if_needed(
            page,
            page_display_name="My Page",
            page_url="https://www.facebook.com/999",
        )
        role_sw.assert_called_once_with(
            page,
            page_display_name="My Page",
            page_url="https://www.facebook.com/999",
        )
