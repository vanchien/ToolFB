"""Xem video nguồn rồi bình luận. Không bấm like, không tải video."""

from __future__ import annotations

import random

import pytest

from src.services.facebook_groups.video_engage import (
    browse_reels_feed,
    clamp_reel_seconds,
    clamp_watch_bounds,
    clamp_watch_max_minutes,
    clamp_watch_seconds,
    engage_source_video,
    share_open_video,
    optional_watch_bounds,
    parse_comment_lines,
    pick_page_watch_seconds,
    pick_share_comment,
)


class _Control:
    def __init__(self, name: str) -> None:
        self.name = name
        self.aria = name
        self.visible = True
        self.clicked = False
        self.typed = ""
        self.pressed = ""

    def is_visible(self) -> bool:
        return self.visible

    def inner_text(self, timeout: int = 0) -> str:
        del timeout
        return self.name

    def get_attribute(self, key: str) -> str:
        return self.aria if key == "aria-label" else ""

    def click(self, timeout: int = 0) -> None:
        del timeout
        self.clicked = True

    def type(self, text: str, delay: int = 0) -> None:
        del delay
        self.typed = text

    def press(self, key: str) -> None:
        self.pressed = key


class _Roles:
    def __init__(self, items: list[_Control]) -> None:
        self._items = items

    def count(self) -> int:
        return len(self._items)

    def nth(self, index: int) -> _Control:
        return self._items[index]


class _Keyboard:
    def __init__(self) -> None:
        self.keys: list[str] = []

    def press(self, key: str) -> None:
        self.keys.append(key)


class _Page:
    def __init__(self, url: str) -> None:
        self.url = url
        self.waits: list[int] = []
        self.keyboard = _Keyboard()
        self.like = _Control("Like")
        self.play = _Control("Play")
        self.comment = _Control("Write a comment")
        self._roles = {"button": [self.like, self.play], "textbox": [self.comment]}

    def get_by_role(self, role: str, name: str | None = None) -> _Roles:
        del name
        return _Roles(self._roles[role])

    def wait_for_timeout(self, millis: int) -> None:
        self.waits.append(millis)


def test_watch_then_comment_does_not_click_like() -> None:
    page = _Page("https://www.facebook.com/watch/?v=1")
    result = engage_source_video(page, watch_seconds=3, comment="Xem hay")
    assert result["commented"] is True
    assert result["watched_seconds"] == 3
    assert sum(page.waits) >= 3000
    assert page.play.clicked is True
    assert page.like.clicked is False
    assert page.comment.typed == "Xem hay"
    assert page.comment.pressed == "Enter"


def test_comment_opens_after_watch_and_ignores_like() -> None:
    page = _Page("https://www.facebook.com/reel/1")
    page.play.name = ""
    page.play.aria = "Play video"
    page.comment.visible = False
    opener = _Control("Viết bình luận")

    def click(timeout: int = 0) -> None:
        del timeout
        opener.clicked = True
        page.comment.visible = True

    opener.click = click
    page._roles["button"] = [page.like, page.play, opener]
    steps: list[str] = []
    original_wait = page.wait_for_timeout
    original_type = page.comment.type

    def wait(millis: int) -> None:
        original_wait(millis)
        if millis >= 1000:
            steps.append("watch")

    def typed(text: str, delay: int = 0) -> None:
        original_type(text, delay=delay)
        steps.append("comment")

    page.wait_for_timeout = wait
    page.comment.type = typed
    result = engage_source_video(page, watch_seconds=2, comment="Dưới video")
    assert result["commented"] is True
    assert steps == ["watch", "watch", "comment"]
    assert page.play.clicked is True
    assert opener.clicked is True
    assert page.like.clicked is False


def test_checkpoint_stops_before_watch() -> None:
    page = _Page("https://www.facebook.com/checkpoint/1")
    result = engage_source_video(page, watch_seconds=10, comment="Không gửi")
    assert result["error_code"] == "CHECKPOINT"
    assert page.waits == []
    assert page.comment.typed == ""


def test_pick_share_comment_skips_lines_already_used() -> None:
    lines = parse_comment_lines("Một\n\nHai\nMột\nBa")
    assert lines == ["Một", "Hai", "Ba"]
    used: list[str] = []
    rng = random.Random(2)
    picked = []
    for _ in range(3):
        choice = pick_share_comment(lines, used, rng)
        picked.append(choice)
        used.append(choice)
    assert sorted(picked) == ["Ba", "Hai", "Một"]
    assert pick_share_comment(lines, used, rng) in lines
    assert pick_share_comment([], []) == ""


def test_clamp_watch_seconds() -> None:
    assert clamp_watch_seconds("20") == 20
    assert clamp_watch_seconds(5) == 5
    assert clamp_watch_seconds("60") == 60
    assert clamp_watch_max_minutes("5") == 5
    assert clamp_watch_seconds(180, 5) == 180
    with pytest.raises(ValueError):
        clamp_watch_seconds("4")
    with pytest.raises(ValueError):
        clamp_watch_seconds("61")
    with pytest.raises(ValueError):
        clamp_watch_seconds(181, 3)
    with pytest.raises(ValueError):
        clamp_watch_max_minutes("31")
    with pytest.raises(ValueError):
        clamp_watch_seconds("abc")


def test_blank_watch_fields_are_skipped() -> None:
    assert optional_watch_bounds("", "") == (0, 0)
    assert optional_watch_bounds("  ", "") == (0, 0)
    assert optional_watch_bounds("15", "") == (15, 15)
    assert optional_watch_bounds("", "40") == (40, 40)


def test_watch_bounds_are_random_per_page() -> None:
    assert clamp_watch_bounds("1", "60") == (1, 60)
    assert clamp_watch_bounds("20", "60") == (20, 60)
    with pytest.raises(ValueError):
        clamp_watch_bounds("0", "30")
    with pytest.raises(ValueError):
        clamp_watch_bounds("1", "61")
    assert clamp_reel_seconds("0") == 0
    assert clamp_reel_seconds("20") == 20
    with pytest.raises(ValueError):
        clamp_watch_bounds("40", "20")
    with pytest.raises(ValueError):
        clamp_reel_seconds("181")
    drawn = {pick_page_watch_seconds(10, 25, random.Random(seed)) for seed in range(6)}
    assert drawn <= set(range(10, 26))
    assert len(drawn) > 1


def test_share_now_picks_the_page_name_and_not_the_video_button() -> None:
    page = _Page("https://www.facebook.com/reel/9")
    video_share = _Control("Share this reel")
    share_now = _Control("Share now")
    page_row = _Control("Cửa hàng")
    post = _Control("Post")
    page._roles = {
        "button": [video_share, share_now, page_row, post],
        "textbox": [_Control("Say something")],
        "menuitem": [],
    }
    sent = share_open_video(page, kind="page", target_id="111", target_name="Cửa hàng", caption="Lên page")
    assert sent["error_code"] == ""
    assert video_share.clicked is True
    assert share_now.clicked is True
    assert page_row.clicked is True
    assert post.clicked is True
    assert page.keyboard.keys[-1] == "Escape"


def test_share_button_on_the_video_goes_to_page_then_group() -> None:
    page = _Page("https://www.facebook.com/watch/?v=9")
    share = _Control("Share")
    to_page = _Control("Share to a Page")
    own_page = _Control("page-1 Cửa hàng")
    post = _Control("Post")
    to_group = _Control("Share to a group")
    group = _Control("Nhóm g-ok")
    comment = _Control("Write a comment")
    caption = _Control("Say something about this")
    page._roles = {
        "button": [page.like, share, to_page, own_page, to_group, group, post],
        "textbox": [comment, caption],
        "menuitem": [],
    }
    sent = share_open_video(page, kind="page", target_id="page-1", caption="Lên page")
    assert sent["error_code"] == ""
    assert share.clicked is True
    assert to_page.clicked is True
    assert own_page.clicked is True
    assert post.clicked is True
    assert page.like.clicked is False
    assert caption.typed == "Lên page"
    assert comment.typed == ""
    share.clicked = False
    post.clicked = False
    caption.typed = ""
    sent_group = share_open_video(page, kind="group", target_id="g-ok", caption="Vào nhóm")
    assert sent_group["error_code"] == ""
    assert to_group.clicked is True
    assert group.clicked is True
    assert post.clicked is True
    assert caption.typed == "Vào nhóm"


def test_browse_reels_moves_down_without_clicking() -> None:
    page = _Page("https://www.facebook.com/")
    opened: list[str] = []

    def goto(url: str, wait_until: str = "", timeout: int = 0) -> None:
        del wait_until, timeout
        opened.append(url)
        page.url = url

    page.goto = goto
    result = browse_reels_feed(page, seconds=4)
    assert result["error_code"] == ""
    assert result["browsed_seconds"] == 4
    assert opened == ["https://www.facebook.com/reels/"]
    assert page.keyboard.keys == ["ArrowDown"]
    assert page.like.clicked is False
    assert page.play.clicked is False


def test_browse_reels_stops_on_checkpoint() -> None:
    page = _Page("https://www.facebook.com/checkpoint/1")
    result = browse_reels_feed(page, seconds=10)
    assert result["error_code"] == "CHECKPOINT"
    assert page.waits == []
    assert page.keyboard.keys == []
