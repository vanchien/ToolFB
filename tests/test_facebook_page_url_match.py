"""Kiểm tra URL Page: id số vs vanity slug (redirect Facebook)."""

from __future__ import annotations

from src.automation.facebook_actions import (
    _is_on_target_surface,
    _urls_refer_same_facebook_page,
)


def test_numeric_id_target_vanity_current() -> None:
    target = "https://www.facebook.com/103833422779877"
    current = "https://www.facebook.com/G.Force.Ghoul/"
    page = type("P", (), {"content": lambda self: "103833422779877"})()
    assert _urls_refer_same_facebook_page(target, current, page=page)  # type: ignore[arg-type]


def test_same_numeric_path() -> None:
    u = "https://www.facebook.com/103833422779877"
    assert _urls_refer_same_facebook_page(u, u + "/")


def test_different_pages_not_match_without_html_proof() -> None:
    """Không có page/HTML chứa id — không đoán vanity slug là cùng Page."""
    assert not _urls_refer_same_facebook_page(
        "https://www.facebook.com/103833422779877",
        "https://www.facebook.com/SomeOtherPage/",
    )


def test_numeric_target_vanity_with_html_id() -> None:
    page = type("P", (), {"content": lambda self: "page_id=103833422779877"})()
    assert _urls_refer_same_facebook_page(
        "https://www.facebook.com/103833422779877",
        "https://www.facebook.com/G.Force.Ghoul/",
        page=page,  # type: ignore[arg-type]
    )


def test_slug_with_numeric_suffix_matches_page_name() -> None:
    from src.automation.facebook_actions import _slug_matches_page_display_name

    assert _slug_matches_page_display_name(
        "https://www.facebook.com/QV.Entertainment305",
        "QV - Entertainment",
    )
    assert not _slug_matches_page_display_name(
        "https://www.facebook.com/HappyStoriesTime",
        "QV - Entertainment",
    )


def test_new_page_home_counts_as_switched_role() -> None:
    """Home Page mới: Business Dashboard + Ads Manager, không còn Professional dashboard."""
    from src.automation.facebook_actions import _page_role_acting_as_page, _target_page_role_satisfied

    class _Loc:
        def __init__(self, visible: bool) -> None:
            self.visible = visible

        @property
        def first(self) -> "_Loc":
            return self

        def is_visible(self, timeout: int = 0) -> bool:
            return self.visible

        def get_attribute(self, _name: str) -> str:
            return ""

    class _Page:
        url = "https://www.facebook.com/QV.Entertainment305"

        def get_by_role(self, role: str, name: object = None) -> _Loc:
            label = getattr(name, "pattern", "")
            if role == "link" and "Business" in str(label):
                return _Loc(True)
            if role == "link" and "Ads" in str(label):
                return _Loc(True)
            return _Loc(False)

        def get_by_text(self, _name: object = None) -> _Loc:
            return _Loc(False)

        def content(self) -> str:
            return "<html></html>"

    page = _Page()
    assert _page_role_acting_as_page(page, timeout_ms=200, page_display_name="QV - Entertainment")  # type: ignore[arg-type]
    assert _target_page_role_satisfied(
        page,  # type: ignore[arg-type]
        "https://www.facebook.com/137640596584683",
        timeout_ms=200,
        page_display_name="QV - Entertainment",
    )


def test_home_feed_not_same_as_page_id() -> None:
    assert not _urls_refer_same_facebook_page(
        "https://www.facebook.com/103833422779877",
        "https://www.facebook.com/",
    )


class _FakePage:
    def __init__(self, url: str) -> None:
        self.url = url


def test_is_on_target_surface_accepts_slug_redirect() -> None:
    class _PageWithHtml(_FakePage):
        def content(self) -> str:
            return "103833422779877"

    page = _PageWithHtml("https://www.facebook.com/G.Force.Ghoul/")
    assert _is_on_target_surface(
        page,  # type: ignore[arg-type]
        "https://www.facebook.com/103833422779877",
    )
