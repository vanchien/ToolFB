"""E2E logic (mock Playwright): đăng lịch → goto Page → switch vai trò Page."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.automation.facebook_actions import (
    go_to_posting_target_and_open_composer,
    post_reel_via_page_dashboard,
)
from src.scheduler import _page_record_to_entity_dict


def test_page_record_to_entity_dict_fanpage() -> None:
    row = {
        "id": "pg1",
        "page_name": "AI kittens",
        "page_url": "https://www.facebook.com/123456789",
    }
    ent = _page_record_to_entity_dict(row)
    assert ent["target_type"] == "fanpage"
    assert "123456789" in str(ent.get("target_url", ""))


def test_go_to_posting_target_calls_robust_switch_twice_on_role_miss() -> None:
    page = MagicMock()
    page.url = "https://www.facebook.com/123456789"
    entity = {
        "target_type": "fanpage",
        "target_url": "https://www.facebook.com/123456789",
    }
    role_checks = iter([False, True, True])

    with (
        patch("src.automation.facebook_actions.navigate_to_url"),
        patch(
            "src.automation.facebook_actions._robust_switch_to_target_page",
            return_value=True,
        ) as robust,
        patch(
            "src.automation.facebook_actions._is_on_target_surface",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._page_role_acting_as_page",
            side_effect=lambda *args, **kwargs: next(role_checks),
        ),
        patch("src.automation.facebook_actions.open_post_box"),
        patch.object(page, "wait_for_timeout"),
    ):
        go_to_posting_target_and_open_composer(
            page,
            entity,
            page_display_name="AI kittens",
        )
    assert robust.call_count >= 2


def test_go_to_posting_target_raises_when_switch_and_role_fail() -> None:
    page = MagicMock()
    entity = {
        "target_type": "fanpage",
        "target_url": "https://www.facebook.com/999",
    }
    with (
        patch("src.automation.facebook_actions.navigate_to_url"),
        patch(
            "src.automation.facebook_actions._robust_switch_to_target_page",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._is_on_target_surface",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._page_role_acting_as_page",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._select_page_in_switch_profiles_popup",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._try_navigate_via_page_name_link",
            return_value=False,
        ),
        patch("src.automation.facebook_actions._failure_screenshot"),
        patch.object(page, "wait_for_timeout"),
    ):
        with pytest.raises(PlaywrightTimeoutError, match="vai trò Page"):
            go_to_posting_target_and_open_composer(
                page,
                entity,
                page_display_name="AI kittens",
            )


def test_post_reel_opens_page_context_via_dashboard_helper(tmp_path) -> None:
    page = MagicMock()
    vid = tmp_path / "clip.mp4"
    vid.write_bytes(b"x")

    def _stop_after_context(*args, **kwargs) -> None:
        raise RuntimeError("e2e_stop_after_page_context")

    with patch(
        "src.automation.facebook_actions._ensure_reel_dashboard_page_context",
        side_effect=_stop_after_context,
    ) as ctx:
        with pytest.raises(RuntimeError, match="e2e_stop_after_page_context"):
            post_reel_via_page_dashboard(
                page,
                page_url="https://www.facebook.com/123",
                page_display_name="AI kittens",
                video_path=vid,
            )
    ctx.assert_called_once()
    assert ctx.call_args.kwargs.get("page_display_name") == "AI kittens"


def test_robust_switch_prefers_direct_before_personal() -> None:
    """Page→page trực tiếp thành công — không gọi reset account chính."""
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
            "src.automation.facebook_actions._quick_switch_on_page_surface",
            return_value=True,
        ) as quick,
        patch(
            "src.automation.facebook_actions._target_page_role_satisfied",
            side_effect=[False, True],
        ),
        patch(
            "src.automation.facebook_actions._switch_to_personal_profile",
        ) as personal,
        patch.object(page, "wait_for_timeout"),
    ):
        from src.automation.facebook_actions import _robust_switch_to_target_page

        ok = _robust_switch_to_target_page(
            page,
            page_display_name="AI kittens",
            page_url="https://www.facebook.com/222",
        )
    assert ok is True
    quick.assert_called()
    personal.assert_not_called()


def test_robust_switch_wrong_page_url_tries_direct_first() -> None:
    """Đang Page khác URL đích — thử direct trước, không return sớm."""
    page = MagicMock()
    page.url = "https://www.facebook.com/111"
    with (
        patch(
            "src.automation.facebook_actions._view_only_guard_active_on_page",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._page_role_acting_as_page",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._urls_refer_same_facebook_page",
            return_value=False,
        ),
        patch(
            "src.automation.facebook_actions._quick_switch_on_page_surface",
            return_value=True,
        ) as quick,
        patch(
            "src.automation.facebook_actions._target_page_role_satisfied",
            side_effect=[False, True],
        ),
        patch(
            "src.automation.facebook_actions._switch_to_personal_profile",
        ) as personal,
        patch.object(page, "wait_for_timeout"),
    ):
        from src.automation.facebook_actions import _robust_switch_to_target_page

        ok = _robust_switch_to_target_page(
            page,
            page_display_name="AI kittens",
            page_url="https://www.facebook.com/222",
        )
    assert ok is True
    quick.assert_called()
    personal.assert_not_called()


def test_robust_switch_selects_page_from_home_before_navigate() -> None:
    """Sau reset cá nhân: chọn Page trong menu trước khi goto URL đích."""
    page = MagicMock()
    call_order: list[str] = []

    def _select(*_a, **_k) -> bool:
        call_order.append("select")
        return True

    def _nav(*_a, **_k) -> None:
        call_order.append("navigate")

    with (
        patch("src.automation.facebook_actions._view_only_guard_active_on_page", return_value=False),
        patch(
            "src.automation.facebook_actions._target_page_role_satisfied",
            side_effect=[False, True],
        ),
        patch("src.automation.facebook_actions._quick_switch_on_page_surface", return_value=False),
        patch("src.automation.facebook_actions._switch_to_personal_profile", return_value=True),
        patch("src.automation.facebook_actions._select_page_via_profile_switcher", side_effect=_select),
        patch("src.automation.facebook_actions.navigate_to_url", side_effect=_nav),
        patch("src.automation.facebook_actions._failure_screenshot"),
        patch.object(page, "wait_for_timeout"),
    ):
        from src.automation.facebook_actions import _robust_switch_to_target_page

        ok = _robust_switch_to_target_page(
            page,
            page_display_name="Animals Being Derps",
            page_url="https://www.facebook.com/102949712869335",
        )
    assert ok is True
    assert call_order == ["select", "navigate"]


def test_robust_switch_uses_personal_after_direct_fails() -> None:
    """Surface switch thất bại → reset account chính (force_home)."""
    page = MagicMock()
    page.url = "https://www.facebook.com/111"
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
        patch(
            "src.automation.facebook_actions._personal_reset_confirmed",
            return_value=True,
        ),
        patch(
            "src.automation.facebook_actions._target_page_role_satisfied",
            side_effect=[False, True],
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
            page_url="https://www.facebook.com/222",
        )
    assert ok is True
    assert personal_calls == [True]
