"""Lướt nhận thiết bị chỉ cuộn, không click."""

from __future__ import annotations

import random

from src.services.device_browse import (
    BROWSE_LIMIT_S,
    browse_limit_reached,
    describe_account_block,
    feed_can_scroll,
    nudge_feed,
    open_home_feed,
)


class _Page:
    def __init__(self, url: str) -> None:
        self.url = url
        self.moves: list[int] = []
        self.clicks = 0
        self.gotos: list[str] = []

    def evaluate(self, _script: str, distance: int) -> None:
        self.moves.append(distance)

    def click(self) -> None:
        self.clicks += 1

    def goto(self, url: str, **_kwargs: object) -> None:
        self.gotos.append(url)
        self.url = url


def test_nudge_feed_only_scrolls() -> None:
    page = _Page("https://www.facebook.com/")
    assert nudge_feed(page, random.Random(1)) is True
    assert page.moves
    assert page.clicks == 0
    assert feed_can_scroll("https://www.facebook.com/checkpoint/") is False
    locked = _Page("https://www.facebook.com/login.php")
    assert nudge_feed(locked, random.Random(1)) is False
    assert locked.moves == []


def test_locked_account_reports_the_facebook_sentence() -> None:
    body = """
    Anna, log in with another device to unlock your account
    We locked your account on 1 October 2026 because it may have been hacked.
    Why can't I use this device?
    We can't match the device that you're trying to use to the account that you're trying to recover, so it isn't safe to go any further.
    """
    detail = describe_account_block(
        "https://www.facebook.com/checkpoint/828281030927956/",
        body,
        expect_session=True,
    )
    assert "another device" in detail
    assert "may have been hacked" in detail
    assert "can't match the device" in detail
    assert describe_account_block("https://www.facebook.com/", "Bảng tin\nBài viết mới") == ""
    logged_out = describe_account_block(
        "https://www.facebook.com/login.php",
        "Log in to Facebook",
        expect_session=True,
    )
    assert "trang đăng nhập" in logged_out
    assert describe_account_block("https://www.facebook.com/login.php", "Log in", expect_session=False) == ""


def test_browse_stops_at_60_seconds() -> None:
    assert BROWSE_LIMIT_S == 60
    assert browse_limit_reached(0, 59.9) is False
    assert browse_limit_reached(10, 70) is True


def test_open_home_feed_skips_when_already_on_facebook() -> None:
    page = _Page("https://www.facebook.com/")
    open_home_feed(page)
    assert page.gotos == []
    blank = _Page("about:blank")
    open_home_feed(blank)
    assert blank.gotos == ["https://www.facebook.com/"]
    locked = _Page("https://www.facebook.com/checkpoint/block/")
    open_home_feed(locked)
    assert locked.gotos == []
