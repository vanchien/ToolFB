"""E2E logic: reset về account cá nhân → chọn Page trong Switch profiles."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.automation.facebook_actions import (
    _go_facebook_personal_home,
    _personal_reset_confirmed,
    _robust_switch_to_target_page,
    _select_page_in_switch_profiles_popup,
    _select_page_via_profile_switcher,
    _switch_to_personal_profile,
    go_to_posting_target_and_open_composer,
)


def test_profile_switcher_uses_menu_already_open() -> None:
    """Menu đã mở ở bước trước — không toggle đóng menu lần 2."""
    page = MagicMock()
    with (
        patch(
            "src.automation.facebook_actions._open_facebook_profile_switcher_menu",
            return_value=True,
        ) as open_menu,
        patch(
            "src.automation.facebook_actions._select_page_in_switch_profiles_popup",
            return_value=True,
        ) as popup,
        patch(
            "src.automation.facebook_actions._wait_after_page_switch_click",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._target_page_role_satisfied",
            return_value=True,
        ),
    ):
        assert _select_page_via_profile_switcher(
            page,
            page_display_name="Animals Being Derps",
            page_url="https://www.facebook.com/102949712869335",
        )
    open_menu.assert_called_once()
    assert popup.call_args.kwargs.get("menu_already_open") is True


def test_select_popup_clicks_see_all_when_menu_already_open() -> None:
    """``menu_already_open=True`` vẫn bấm «See all profiles» để mở dialog đầy đủ."""
    page = MagicMock()
    see_all_clicked: list[str] = []

    def _click_btn(loc, **kwargs):
        label = str(kwargs.get("human_label", ""))
        see_all_clicked.append(label)
        return True

    with (
        patch(
            "src.automation.facebook_actions._switch_profiles_dialog_visible",
            side_effect=[False, True, True],
        ),
        patch(
            "src.automation.facebook_actions._open_facebook_profile_switcher_menu",
        ) as open_menu,
        patch(
            "src.automation.facebook_actions._click_visible_enabled_button",
            side_effect=_click_btn,
        ),
        patch(
            "src.automation.facebook_actions._switch_profiles_dialog_scope",
            return_value=page.locator.return_value,
        ),
        patch(
            "src.automation.facebook_actions._try_click_page_match_in_scope",
            return_value=True,
        ),
        patch.object(page, "wait_for_timeout"),
    ):
        assert _select_page_in_switch_profiles_popup(
            page,
            page_display_name="Animals Being Derps",
            page_url="https://www.facebook.com/102949712869335",
            menu_already_open=True,
        )
    open_menu.assert_not_called()
    assert len(see_all_clicked) >= 1


def test_switch_to_personal_force_home_then_menu_fallback() -> None:
    """``force_home``: goto home thất bại → menu profile → home → xác nhận."""
    page = MagicMock()
    page.url = "https://www.facebook.com/AnimalsBeingDerpss/"
    home_results = iter([False, True])

    with (
        patch(
            "src.automation.facebook_actions._personal_reset_confirmed",
            side_effect=[False, False, True],
        ),
        patch(
            "src.automation.facebook_actions._go_facebook_personal_home",
            side_effect=lambda _p: next(home_results),
        ) as go_home,
        patch(
            "src.automation.facebook_actions._open_facebook_profile_switcher_menu",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._click_personal_profile_in_switcher",
            return_value=True,
        ),
        patch.object(page, "wait_for_timeout"),
    ):
        assert _switch_to_personal_profile(page, force_home=True)
    assert go_home.call_count >= 2


def test_personal_reset_rejects_page_slug_with_switch_cta() -> None:
    """URL slug Page + CTA Switch — không được coi là đã về account chính."""
    page = MagicMock()
    page.url = "https://www.facebook.com/AnimalsBeingDerpss/"
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
        assert not _personal_reset_confirmed(page)


def test_e2e_robust_animals_being_derps_personal_then_page() -> None:
    """
    Mô phỏng log thực tế: viewer Page + CTA Switch → reset cá nhân → chọn Page → navigate.
  """
    page = MagicMock()
    page.url = "https://www.facebook.com/AnimalsBeingDerpss/"
    call_order: list[str] = []

    def _personal(_p: MagicMock, *, force_home: bool = False) -> bool:
        call_order.append(f"personal:{force_home}")
        page.url = "https://www.facebook.com/"
        return True

    def _select(*_a, **_k) -> bool:
        call_order.append("select_page")
        return True

    def _nav(_p: MagicMock, url: str) -> None:
        call_order.append(f"navigate:{url}")

    with (
        patch("src.automation.facebook_actions._view_only_guard_active_on_page", return_value=False),
        patch(
            "src.automation.facebook_actions._target_page_role_satisfied",
            side_effect=[False, True],
        ),
        patch("src.automation.facebook_actions._quick_switch_on_page_surface", return_value=False),
        patch("src.automation.facebook_actions._switch_to_personal_profile", side_effect=_personal),
        patch(
            "src.automation.facebook_actions._personal_reset_confirmed",
            return_value=True,
        ),
        patch("src.automation.facebook_actions._select_page_via_profile_switcher", side_effect=_select),
        patch("src.automation.facebook_actions.navigate_to_url", side_effect=_nav),
        patch("src.automation.facebook_actions._failure_screenshot"),
        patch.object(page, "wait_for_timeout"),
    ):
        ok = _robust_switch_to_target_page(
            page,
            page_display_name="Animals Being Derps",
            page_url="https://www.facebook.com/102949712869335",
        )
    assert ok is True
    assert call_order[0] == "personal:True"
    assert "select_page" in call_order
    assert any(c.startswith("navigate:") for c in call_order)
    assert call_order.index("select_page") < max(
        i for i, c in enumerate(call_order) if c.startswith("navigate:")
    )


def test_robust_retries_profile_switcher_once() -> None:
    """Profile switcher lần 1 fail → reset cá nhân → thử lại lần 2."""
    page = MagicMock()
    select_results = iter([False, True])

    with (
        patch("src.automation.facebook_actions._view_only_guard_active_on_page", return_value=False),
        patch(
            "src.automation.facebook_actions._target_page_role_satisfied",
            side_effect=[False, True],
        ),
        patch("src.automation.facebook_actions._quick_switch_on_page_surface", return_value=False),
        patch("src.automation.facebook_actions._switch_to_personal_profile", return_value=True),
        patch("src.automation.facebook_actions._personal_reset_confirmed", return_value=True),
        patch(
            "src.automation.facebook_actions._select_page_via_profile_switcher",
            side_effect=lambda *a, **k: next(select_results),
        ) as select,
        patch("src.automation.facebook_actions.navigate_to_url"),
        patch("src.automation.facebook_actions._failure_screenshot"),
        patch.object(page, "wait_for_timeout"),
    ):
        ok = _robust_switch_to_target_page(
            page,
            page_display_name="AI kittens",
            page_url="https://www.facebook.com/222",
        )
    assert ok is True
    assert select.call_count == 2


def test_robust_does_not_return_true_without_target_role() -> None:
    """Surface switch OK nhưng chưa đúng vai trò/URL — không return sớm."""
    page = MagicMock()
    with (
        patch("src.automation.facebook_actions._view_only_guard_active_on_page", return_value=False),
        patch(
            "src.automation.facebook_actions._target_page_role_satisfied",
            side_effect=[False, False, False, False],
        ),
        patch("src.automation.facebook_actions._quick_switch_on_page_surface", return_value=True),
        patch("src.automation.facebook_actions._switch_to_personal_profile", return_value=False),
        patch("src.automation.facebook_actions._select_page_via_profile_switcher", return_value=False),
        patch("src.automation.facebook_actions.navigate_to_url"),
        patch("src.automation.facebook_actions._failure_screenshot"),
        patch.object(page, "wait_for_timeout"),
    ):
        ok = _robust_switch_to_target_page(
            page,
            page_display_name="AI kittens",
            page_url="https://www.facebook.com/222",
        )
    assert ok is False


def test_go_to_posting_target_e2e_personal_to_page_chain() -> None:
    """Đăng lịch fanpage: navigate → robust (personal→page) → mở composer."""
    page = MagicMock()
    page.url = "https://www.facebook.com/"
    entity = {
        "target_type": "fanpage",
        "target_url": "https://www.facebook.com/102949712869335",
    }
    chain: list[str] = []

    def _robust(_p, **kwargs) -> bool:
        chain.append("robust")
        page.url = str(kwargs.get("page_url", page.url))
        return True

    with (
        patch("src.automation.facebook_actions.navigate_to_url", side_effect=lambda _p, u: chain.append(f"nav:{u}")),
        patch("src.automation.facebook_actions._robust_switch_to_target_page", side_effect=_robust),
        patch("src.automation.facebook_actions._is_on_target_surface", return_value=True),
        patch("src.automation.facebook_actions._page_role_acting_as_page", return_value=True),
        patch("src.automation.facebook_actions.open_post_box", side_effect=lambda _p: chain.append("composer")),
        patch.object(page, "wait_for_timeout"),
    ):
        go_to_posting_target_and_open_composer(
            page,
            entity,
            page_display_name="Animals Being Derps",
        )
    assert chain[0].startswith("nav:")
    assert "robust" in chain
    assert chain[-1] == "composer"


def test_go_facebook_personal_home_confirms_feed() -> None:
    page = MagicMock()
    page.url = "https://www.facebook.com/"
    with (
        patch("src.automation.facebook_actions._personal_reset_confirmed", return_value=True),
        patch("src.automation.facebook_actions._force_www_facebook_if_mobile_redirect"),
        patch.object(page, "goto"),
        patch.object(page, "wait_for_timeout"),
    ):
        assert _go_facebook_personal_home(page)


def test_page_switch_aliases_include_compact_display_name() -> None:
    from src.automation.facebook_actions import _page_switch_name_aliases

    aliases = _page_switch_name_aliases(
        "Animals Being Derps",
        "https://www.facebook.com/102949712869335",
    )
    assert "Animals Being Derps" in aliases
    assert "animalsbeingderps" in aliases
    assert "102949712869335" in aliases


def test_select_page_popup_scroll_then_match_alias() -> None:
    """Cuộn danh sách rồi khớp alias compact tên Page."""
    page = MagicMock()
    dialog_vis = iter([False, True, True, True])
    click_results = iter([False, True])

    with (
        patch(
            "src.automation.facebook_actions._switch_profiles_dialog_visible",
            side_effect=lambda *a, **k: next(dialog_vis, True),
        ),
        patch(
            "src.automation.facebook_actions._open_facebook_profile_switcher_menu",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._click_visible_enabled_button",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._switch_profiles_dialog_scope",
            return_value=page.locator.return_value,
        ),
        patch(
            "src.automation.facebook_actions._try_click_page_match_in_scope",
            side_effect=lambda *a, **k: next(click_results),
        ) as match,
        patch(
            "src.automation.facebook_actions._scroll_switch_profiles_list",
            side_effect=lambda _p: None,
        ),
        patch.object(page, "wait_for_timeout"),
    ):
        assert _select_page_in_switch_profiles_popup(
            page,
            page_display_name="Animals Being Derps",
            page_url="https://www.facebook.com/102949712869335",
            menu_already_open=False,
        )
    assert match.call_count >= 2
