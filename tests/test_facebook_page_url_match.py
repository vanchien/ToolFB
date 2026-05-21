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
