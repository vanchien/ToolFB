"""Nút Switch sidebar Manage Page (div role=none)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.automation.facebook_actions import (
    _click_manage_page_sidebar_switch,
    _click_switch_in_sidebar_cta_card,
    _ensure_page_role_switched,
    _normalize_compact_page_name,
    _page_switch_name_aliases,
    _page_switch_sidebar_hint_visible,
    _switch_profiles_dialog_mentions_page,
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


def test_ensure_page_role_switched_prefers_switch_now() -> None:
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
            "src.automation.facebook_actions._click_switch_now_banner",
            return_value=True,
        ) as sw_now,
        patch(
            "src.automation.facebook_actions._click_manage_page_sidebar_switch",
            return_value=False,
        ) as sidebar,
        patch(
            "src.automation.facebook_actions._wait_after_page_switch_click",
            return_value=True,
        ) as wait_sw,
        patch(
            "src.automation.facebook_actions._wait_page_role_switch_complete",
            return_value=True,
        ),
    ):
        assert _ensure_page_role_switched(
            page, page_display_name="Best News US", page_url="https://www.facebook.com/123"
        )
        sw_now.assert_called()
        sidebar.assert_not_called()
        wait_sw.assert_called()


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
            "src.automation.facebook_actions._attempt_page_role_switch_clicks",
            return_value=False,
        ),
        patch("src.automation.facebook_actions._failure_screenshot"),
        patch.object(page, "wait_for_timeout"),
    ):
        assert not _ensure_page_role_switched(page, page_url="https://www.facebook.com/103833422779877")


def test_ensure_page_role_stops_early_on_popup_fail_streak() -> None:
    """Popup confirm lỗi liên tiếp — dừng sớm, không lặp 4 lần."""
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
            "src.automation.facebook_actions._attempt_page_role_switch_clicks",
            return_value=True,
        ) as clicks,
        patch(
            "src.automation.facebook_actions._wait_after_page_switch_click",
            return_value=False,
        ),
        patch("src.automation.facebook_actions._failure_screenshot"),
        patch.object(page, "wait_for_timeout"),
    ):
        assert not _ensure_page_role_switched(page, max_attempts=4)
    assert clicks.call_count == 2


def test_slug_alias_matches_display_name_in_popup() -> None:
    """Slug job khớp «Xabre Owners Bandung» trong popup (so sánh compact)."""
    aliases = _page_switch_name_aliases(
        "xabreownersbandung",
        "https://www.facebook.com/xabreownersbandung",
    )
    assert "xabreownersbandung" in aliases
    dlg = MagicMock()
    dlg.inner_text.return_value = (
        "Switch profiles\nSwitch to Xabre Owners Bandung for more features, tools and settings"
    )
    assert _switch_profiles_dialog_mentions_page(dlg, aliases)
    assert _normalize_compact_page_name("Xabre Owners Bandung") == "xabreownersbandung"


def test_handle_switch_profiles_popup_confirm() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._switch_profiles_dialog_visible",
            side_effect=[True, False, False, False],
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
    with patch(
        "src.automation.facebook_actions._robust_switch_to_target_page",
    ) as robust:
        from src.automation.facebook_actions import _ensure_switched_into_page_if_needed

        _ensure_switched_into_page_if_needed(
            page,
            page_display_name="My Page",
            page_url="https://www.facebook.com/999",
        )
        robust.assert_called_once_with(
            page,
            page_display_name="My Page",
            page_url="https://www.facebook.com/999",
        )


def test_attempt_page_role_switch_prefers_switch_now() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._click_switch_now_banner",
            return_value=True,
        ) as sw_now,
        patch(
            "src.automation.facebook_actions._click_manage_page_sidebar_switch",
            return_value=False,
        ) as sidebar,
    ):
        from src.automation.facebook_actions import _attempt_page_role_switch_clicks

        assert _attempt_page_role_switch_clicks(page)
    sw_now.assert_called()
    sidebar.assert_not_called()


def test_switch_personal_not_skipped_on_page_slug_with_cta() -> None:
    """URL slug Page + CTA Switch — không được coi là đã ở account chính."""
    page = MagicMock()
    page.url = "https://www.facebook.com/KikiroCooking/"
    with (
        patch(
            "src.automation.facebook_actions._page_switch_ui_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._manage_page_switch_cta_still_visible",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._page_role_acting_as_page",
            return_value=False,
        ),
    ):
        from src.automation.facebook_actions import _is_likely_personal_account_surface

        assert not _is_likely_personal_account_surface(page)


def test_robust_switch_uses_personal_reset_strategy() -> None:
    page = MagicMock()
    page.url = "https://www.facebook.com/somepage"
    personal_calls: list[bool] = []

    def _personal(_page: MagicMock, *, force_home: bool = False) -> bool:
        personal_calls.append(force_home)
        return True

    quick_results = iter([False, True])

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
            "src.automation.facebook_actions._quick_switch_on_page_surface",
            side_effect=lambda *a, **k: next(quick_results),
        ),
        patch(
            "src.automation.facebook_actions._switch_to_personal_profile",
            side_effect=_personal,
        ),
        patch("src.automation.facebook_actions.navigate_to_url"),
        patch(
            "src.automation.facebook_actions._select_page_via_profile_switcher",
            return_value=False,
        ),
        patch("src.automation.facebook_actions._failure_screenshot"),
        patch.object(page, "wait_for_timeout"),
    ):
        from src.automation.facebook_actions import _robust_switch_to_target_page

        ok = _robust_switch_to_target_page(
            page,
            page_display_name="AI kittens",
            page_url="https://www.facebook.com/123456",
        )
    assert ok is True
    assert personal_calls == [True]


def test_select_page_via_profile_switcher() -> None:
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._open_facebook_profile_switcher_menu",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._select_page_in_switch_profiles_popup",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._wait_after_page_switch_click",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._page_role_acting_as_page",
            return_value=True,
        ),
    ):
        from src.automation.facebook_actions import _select_page_via_profile_switcher

        assert _select_page_via_profile_switcher(
            page,
            page_display_name="Ethereal Birds",
            page_url="https://www.facebook.com/107315425760571",
        )
