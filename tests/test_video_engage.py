"""Xem video nguồn rồi bình luận. Không bấm like, không tải video."""

from __future__ import annotations

import random

import pytest

from src.services.facebook_groups.video_engage import (
    browse_reels_feed,
    clamp_reel_seconds,
    reel_page_is_broken,
    skip_broken_reel,
    clamp_watch_bounds,
    clamp_watch_max_minutes,
    clamp_watch_seconds,
    engage_source_video,
    like_open_video,
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

    def fill(self, text: str) -> None:
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
        self.visible_text = ""

    def inner_text(self, selector: str = "body") -> str:
        del selector
        return self.visible_text

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
    assert steps[:3] == ["watch", "watch", "comment"]
    assert page.play.clicked is True
    assert opener.clicked is True
    assert page.like.clicked is False


def test_comment_bubble_icon_opens_the_box() -> None:
    page = _Page("https://www.facebook.com/reel/1")
    page.comment.visible = False
    icon = _Control("")

    def click(timeout: int = 0) -> None:
        del timeout
        icon.clicked = True
        page.comment.visible = True

    icon.click = click

    def evaluate_handle(script: str):
        assert "M12 .5C18.351" in script
        return icon

    page.evaluate_handle = evaluate_handle
    result = engage_source_video(page, watch_seconds=0, comment="Hay quá")
    assert result["commented"] is True
    assert icon.clicked is True
    assert page.comment.typed == "Hay quá"
    assert page.like.clicked is False


def _timeline_page() -> tuple[_Page, list[tuple[str, int]]]:
    """Trang giả ghi lại lúc Like / bình luận xảy ra so với tổng thời gian đã chờ."""
    page = _Page("https://www.facebook.com/reel/1")
    events: list[tuple[str, int]] = []
    original_wait = page.wait_for_timeout
    original_like = page.like.click
    original_type = page.comment.type

    def like_click(timeout: int = 0) -> None:
        original_like(timeout)
        events.append(("like", sum(page.waits)))

    def typed(text: str, delay: int = 0) -> None:
        original_type(text, delay=delay)
        events.append(("comment", sum(page.waits)))

    page.wait_for_timeout = original_wait
    page.like.click = like_click
    page.comment.type = typed
    return page, events


def test_like_and_comment_happen_while_watching_one_at_a_time() -> None:
    """Video dài: Like và bình luận chen vào giữa lúc xem, từng việc một, rồi xem nốt."""
    page, events = _timeline_page()
    result = engage_source_video(
        page, watch_seconds=12, comment="Hay", like=True, rng=random.Random(3)
    )
    assert result["liked"] is True and result["commented"] is True
    assert sorted(name for name, _ in events) == ["comment", "like"]
    assert events[0][1] >= 1_000, "đã xem một lúc mới bấm"
    assert events[0][1] < events[1][1], "hai việc không cùng lúc"
    assert sum(page.waits) - events[1][1] >= 1_000, "sau việc cuối vẫn xem tiếp"
    assert sum(page.waits) >= 12_000


def test_like_comment_order_is_random() -> None:
    """Like trước hay bình luận trước đều được — đổi theo từng lần."""
    orders = set()
    for seed in range(12):
        page, events = _timeline_page()
        engage_source_video(page, watch_seconds=10, comment="Hay", like=True, rng=random.Random(seed))
        orders.add(tuple(name for name, _ in events))
    assert orders == {("like", "comment"), ("comment", "like")}


def test_short_video_likes_and_comments_after_watching() -> None:
    """Video ngắn (dưới 6 giây) xem xong mới Like và bình luận."""
    page, events = _timeline_page()
    result = engage_source_video(page, watch_seconds=3, comment="Hay", like=True, rng=random.Random(1))
    assert result["liked"] is True and result["commented"] is True
    assert all(at >= 3_000 for _, at in events)


def test_checkpoint_stops_before_watch() -> None:
    page = _Page("https://www.facebook.com/checkpoint/1")
    result = engage_source_video(page, watch_seconds=10, comment="Không gửi")
    assert result["error_code"] == "CHECKPOINT"
    # Có thể chờ ngắn nhận skeleton checkpoint — không Like/comment.
    assert page.comment.typed == ""
    assert all(w <= 2500 for w in page.waits)


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


def test_share_dialog_without_page_menu_clicks_share_now() -> None:
    page = _Page("https://www.facebook.com/reel/9")
    video_share = _Control("Share this reel")
    share_now = _Control("Share now")
    page._roles = {
        "button": [video_share, share_now],
        "textbox": [_Control("Say something")],
        "menuitem": [],
    }
    sent = share_open_video(page, kind="page", target_id="111", target_name="Cửa hàng", caption="Lên page")
    assert sent["error_code"] == ""
    assert video_share.clicked is True
    assert share_now.clicked is True


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


def test_reel_share_arrow_icon_opens_the_page_sheet() -> None:
    page = _Page("https://www.facebook.com/reel/9")
    icon = _Control("")
    to_page = _Control("Share to a Page")
    own_page = _Control("Cửa hàng")
    post = _Control("Post")
    page._roles = {
        "button": [page.like, to_page, own_page, post],
        "textbox": [],
        "menuitem": [],
    }

    def evaluate_handle(script: str):
        assert "M12.863 3.156A.504.504" in script
        return icon

    page.evaluate_handle = evaluate_handle
    sent = share_open_video(page, kind="page", target_id="111", target_name="Cửa hàng")
    assert sent["error_code"] == ""
    assert icon.clicked is True
    assert to_page.clicked is True
    assert own_page.clicked is True
    assert post.clicked is True
    assert page.like.clicked is False


def test_reel_share_count_label_goes_to_the_page() -> None:
    page = _Page("https://www.facebook.com/reel/9")
    like = _Control("Like, 5 likes")
    share = _Control("Share, 0 shares")
    to_page = _Control("Share to a Page")
    own_page = _Control("Minh Tuyết Family")
    post = _Control("Post")
    page._roles = {
        "button": [like, share, to_page, own_page, post],
        "textbox": [],
        "menuitem": [],
    }
    sent = share_open_video(
        page,
        kind="page",
        target_id="111",
        target_name="Minh Tuyết Family",
        caption="",
        like=True,
    )
    assert sent["error_code"] == ""
    assert like.clicked is True
    assert share.clicked is True
    assert to_page.clicked is True
    assert own_page.clicked is True
    assert post.clicked is True


def test_exact_page_name_wins_over_a_longer_name() -> None:
    page = _Page("https://www.facebook.com/reel/9")
    share = _Control("Share, 0 shares")
    to_page = _Control("Share to a Page")
    longer = _Control("Life of Mine")
    exact = _Control("Life")
    post = _Control("Post")
    page._roles = {
        "button": [share, to_page, longer, exact, post],
        "textbox": [],
        "menuitem": [],
    }
    sent = share_open_video(page, kind="page", target_id="111", target_name="Life")
    assert sent["error_code"] == ""
    assert exact.clicked is True
    assert longer.clicked is False
    assert post.clicked is True


def test_search_box_finds_a_page_below_the_fold() -> None:
    page = _Page("https://www.facebook.com/reel/9")
    share = _Control("Share")
    to_page = _Control("Share to a Page")
    search = _Control("Search for a Page")
    hidden = _Control("Cửa hàng xa")
    hidden.visible = False
    post = _Control("Post")

    def typed(text: str, delay: int = 0) -> None:
        del delay
        search.typed = text
        hidden.visible = True

    search.type = typed  # type: ignore[method-assign]
    page._roles = {
        "button": [share, to_page, hidden, post],
        "textbox": [search],
        "menuitem": [],
    }
    sent = share_open_video(page, kind="page", target_id="111", target_name="Cửa hàng xa")
    assert sent["error_code"] == ""
    assert search.typed == "Cửa hàng xa"
    assert hidden.clicked is True
    assert post.clicked is True


def test_share_now_span_is_clicked_when_it_is_not_a_button() -> None:
    page = _Page("https://www.facebook.com/reel/9")
    share = _Control("Share")
    now = _Control("Share now")
    caption = _Control("Say something about this")
    page._roles = {"button": [share], "textbox": [caption], "menuitem": []}

    def evaluate_handle(script: str):
        assert "share now" in script
        return now

    page.evaluate_handle = evaluate_handle
    sent = share_open_video(page, kind="page", target_id="111", target_name="RicardoInfor", caption="Lên page")
    assert sent["error_code"] == ""
    assert share.clicked is True
    assert now.clicked is True
    assert caption.typed == "Lên page"


def test_reel_end_to_end_opens_share_box_then_clicks_share_now() -> None:
    """Reel: bình luận, Like, mũi tên chia sẻ, hộp Share hiện ra, bấm Share now."""
    page = _Page("https://www.facebook.com/reel/9")
    like = _Control("Like")
    react = _Control("React")
    comment_icon = _Control("")
    share_icon = _Control("")
    now = _Control("Share now")
    feed = _Control("Feed")
    public = _Control("Public")
    whatsapp = _Control("WhatsApp")
    story = _Control("Your story")
    copy = _Control("Copy link")
    group = _Control("Group")
    caption = _Control("Say something about this")
    page.comment.visible = False
    page._roles = {
        "button": [page.play, like, react, feed, public, whatsapp, story, copy, group],
        "textbox": [page.comment, caption],
        "menuitem": [],
    }
    order: list[str] = []

    def mark(name: str, control: _Control):
        original = control.click

        def click(timeout: int = 0) -> None:
            original(timeout)
            order.append(name)
            if name == "comment":
                page.comment.visible = True

        control.click = click

    mark("like", like)
    mark("comment", comment_icon)
    mark("share", share_icon)
    mark("now", now)

    def evaluate(script: str):
        if "M10.999.5a2.5" in script:
            return "ready"
        if "share now" in script:
            return True
        return ""

    def evaluate_handle(script: str):
        if "M12 .5C18.351" in script:
            return comment_icon
        if "M12.863 3.156A.504.504" in script:
            return share_icon
        if "M10.999.5a2.5" in script:
            return like
        if "share now" in script:
            return now
        raise AssertionError(script[:48])

    page.evaluate = evaluate
    page.evaluate_handle = evaluate_handle
    watched = engage_source_video(page, watch_seconds=0, comment="")
    liked = like_open_video(page)
    commented = engage_source_video(page, watch_seconds=0, comment="Hay quá")
    sent = share_open_video(page, kind="page", target_id="111", target_name="RicardoInfor", caption="Lên page")
    assert watched["commented"] is False
    assert liked is True
    assert commented["commented"] is True
    assert sent["error_code"] == ""
    assert page.comment.typed == "Hay quá"
    assert caption.typed == "Lên page"
    assert order.index("like") < order.index("comment") < order.index("share") < order.index("now")
    assert react.clicked is False
    assert feed.clicked is False
    assert public.clicked is False
    assert whatsapp.clicked is False
    assert story.clicked is False
    assert copy.clicked is False
    assert group.clicked is False


def test_page_share_does_not_post_with_share_now() -> None:
    page = _Page("https://www.facebook.com/reel/9")
    share = _Control("Share")
    to_page = _Control("Share to a Page")
    own_page = _Control("Cửa hàng")
    share_now = _Control("Share now")
    page._roles = {
        "button": [share, to_page, own_page, share_now],
        "textbox": [],
        "menuitem": [],
    }
    sent = share_open_video(page, kind="page", target_id="111", target_name="Cửa hàng")
    assert sent["error_code"] == "TIMEOUT"
    assert own_page.clicked is True
    assert share_now.clicked is False


def test_reel_rail_comments_likes_then_shares_to_the_page() -> None:
    """Cột Reel: bình luận bằng bong bóng, Like bằng ngón tay, chia sẻ bằng mũi tên, rồi gửi Page."""
    page = _Page("https://www.facebook.com/reel/9")
    like = _Control("Like")
    react = _Control("React")
    comment_icon = _Control("")
    share_icon = _Control("")
    to_page = _Control("Share to a Page")
    own_page = _Control("Cửa hàng")
    post = _Control("Post")
    page.comment.visible = False
    page._roles = {
        "button": [page.play, like, react, to_page, own_page, post],
        "textbox": [page.comment],
        "menuitem": [],
    }
    order: list[str] = []

    def mark(name: str, control: _Control):
        original = control.click

        def click(timeout: int = 0) -> None:
            original(timeout)
            order.append(name)
            if name == "comment":
                page.comment.visible = True

        control.click = click

    mark("like", like)
    mark("comment", comment_icon)
    mark("share", share_icon)

    def evaluate(script: str) -> str:
        if "M10.999.5a2.5" in script:
            return "ready"
        return ""

    def evaluate_handle(script: str):
        if "M12 .5C18.351" in script:
            return comment_icon
        if "M12.863 3.156A.504.504" in script:
            return share_icon
        if "M10.999.5a2.5" in script:
            return like
        raise AssertionError(script[:40])

    page.evaluate = evaluate
    page.evaluate_handle = evaluate_handle

    watched = engage_source_video(page, watch_seconds=0, comment="Hay quá")
    sent = share_open_video(page, kind="page", target_id="111", target_name="Cửa hàng", like=True)
    assert watched["commented"] is True
    assert sent["error_code"] == ""
    assert page.comment.typed == "Hay quá"
    assert order[0] == "comment"
    assert order.index("like") < order.index("share")
    assert react.clicked is False
    assert to_page.clicked is True
    assert own_page.clicked is True
    assert post.clicked is True


def test_reel_like_is_the_thumbs_up_not_the_react_button() -> None:
    page = _Page("https://www.facebook.com/reel/9")
    react = _Control("React")
    like = _Control("Like")
    page._roles["button"] = [react]

    def evaluate(script: str) -> str:
        assert "M10.999.5a2.5" in script
        return "ready"

    def evaluate_handle(script: str):
        assert "M10.999.5a2.5" in script
        return like

    page.evaluate = evaluate
    page.evaluate_handle = evaluate_handle
    assert like_open_video(page) is True
    assert like.clicked is True
    assert react.clicked is False


def test_like_video_skips_a_button_already_pressed() -> None:
    page = _Page("https://www.facebook.com/reel/9")
    liked = _Control("Unlike")
    page._roles["button"] = [liked]
    assert like_open_video(page) is False
    assert liked.clicked is False


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


def test_broken_reel_is_detected() -> None:
    page = _Page("https://www.facebook.com/reel/29215402584727280")
    page.visible_text = (
        "This page isn't available right now. "
        "This may be because of a technical error that we're working to get fixed. "
        "Try reloading this page."
    )
    assert reel_page_is_broken(page) is True
    page.visible_text = "Like Comment Share"
    assert reel_page_is_broken(page) is False


def test_skip_broken_reel_opens_reels_home() -> None:
    page = _Page("https://www.facebook.com/reel/29215402584727280")
    page.visible_text = "This page isn't available right now. Try reloading this page."
    opened: list[str] = []

    def goto(url: str, wait_until: str = "", timeout: int = 0) -> None:
        del wait_until, timeout
        opened.append(url)
        page.url = url
        page.visible_text = "Like Comment Share"

    page.goto = goto
    assert skip_broken_reel(page) is True
    assert opened == ["https://www.facebook.com/reels/"]
    assert page.keyboard.keys == ["ArrowDown"]


def test_browse_reels_leaves_broken_reel() -> None:
    page = _Page("https://www.facebook.com/reel/29215402584727280")
    page.visible_text = "This page isn't available right now. Try reloading this page."
    opened: list[str] = []

    def goto(url: str, wait_until: str = "", timeout: int = 0) -> None:
        del wait_until, timeout
        opened.append(url)
        page.url = url
        page.visible_text = "Like Comment Share"

    page.goto = goto
    result = browse_reels_feed(page, seconds=1)
    assert result["error_code"] == ""
    assert opened == ["https://www.facebook.com/reels/"]


def test_engage_broken_source_switches_reel_and_skips_comment() -> None:
    page = _Page("https://www.facebook.com/reel/29215402584727280")
    page.visible_text = "This page isn't available right now. Try reloading this page."

    def goto(url: str, wait_until: str = "", timeout: int = 0) -> None:
        del wait_until, timeout
        page.url = "https://www.facebook.com/reel/111"
        page.visible_text = "Like Comment Share"

    page.goto = goto
    result = engage_source_video(page, watch_seconds=2, comment="Hay quá", like=True)
    assert result["error_code"] == "VIDEO_UNAVAILABLE"
    assert result["source_unavailable"] is True
    assert result["commented"] is False
    assert result["liked"] is False
    assert result["watched_seconds"] == 2


def test_browse_reels_stops_on_checkpoint() -> None:
    page = _Page("https://www.facebook.com/checkpoint/1")
    result = browse_reels_feed(page, seconds=10)
    assert result["error_code"] == "CHECKPOINT"
    assert page.keyboard.keys == []
    assert all(w <= 2500 for w in page.waits)


def test_clamp_engage_rate_percent() -> None:
    from src.services.facebook_groups.video_engage import clamp_engage_rate_percent

    assert clamp_engage_rate_percent("") == 65
    assert clamp_engage_rate_percent("80") == 80
    assert clamp_engage_rate_percent("80%") == 80
    with pytest.raises(ValueError):
        clamp_engage_rate_percent("101")
    with pytest.raises(ValueError):
        clamp_engage_rate_percent("abc")


def test_share_engage_plan_balances_like_and_comment_across_source_videos() -> None:
    """Cùng Page nguồn trong ngày: ≥65% có tương tác, Like/comment ~50/50, ít khi cả hai."""
    from src.services.facebook_groups.video_engage import (
        MIN_SHARE_ENGAGE_RATE,
        append_share_engage_history,
        pick_share_engage_plan,
    )

    rng = random.Random(7)
    history: list[dict] = []
    plans = []
    for i in range(20):
        vid = str(1000000100 + i)
        plan = pick_share_engage_plan(
            source="https://www.facebook.com/SrcPage",
            video_id=vid,
            allow_like=True,
            allow_comment=True,
            history=history,
            rng=rng,
            min_engage_rate=0.65,
        )
        plans.append(plan)
        history = append_share_engage_history(history, video_id=vid, plan=plan)

    engaged = [p for p in plans if p.do_like or p.do_comment]
    assert len(engaged) / len(plans) >= MIN_SHARE_ENGAGE_RATE
    like_only = sum(1 for p in plans if p.do_like and not p.do_comment)
    comment_only = sum(1 for p in plans if p.do_comment and not p.do_like)
    both = sum(1 for p in plans if p.do_like and p.do_comment)
    assert like_only >= 1 and comment_only >= 1
    assert abs(like_only - comment_only) <= 4
    assert both <= 3


def test_share_engage_respects_user_min_rate_100() -> None:
    """Người dùng đặt 100% → mọi video đều có Like hoặc comment."""
    from src.services.facebook_groups.video_engage import (
        append_share_engage_history,
        pick_share_engage_plan,
    )

    rng = random.Random(3)
    history: list[dict] = []
    for i in range(12):
        vid = str(2000000100 + i)
        plan = pick_share_engage_plan(
            video_id=vid,
            allow_like=True,
            allow_comment=True,
            history=history,
            rng=rng,
            min_engage_rate=1.0,
        )
        assert plan.do_like or plan.do_comment
        history = append_share_engage_history(history, video_id=vid, plan=plan)


def test_share_engage_plan_reuses_same_video_decision() -> None:
    from src.services.facebook_groups.video_engage import (
        append_share_engage_history,
        pick_share_engage_plan,
    )

    rng = random.Random(1)
    first = pick_share_engage_plan(
        video_id="2482358802253174",
        allow_like=True,
        allow_comment=True,
        history=[],
        rng=rng,
    )
    hist = append_share_engage_history([], video_id="2482358802253174", plan=first)
    second = pick_share_engage_plan(
        video_id="2482358802253174",
        allow_like=True,
        allow_comment=True,
        history=hist,
        rng=random.Random(99),
    )
    assert second.do_like == first.do_like
    assert second.do_comment == first.do_comment


def test_share_engage_respects_disabled_like_or_empty_comments() -> None:
    from src.services.facebook_groups.video_engage import pick_share_engage_plan

    plan = pick_share_engage_plan(
        video_id="2482358802253174",
        allow_like=False,
        allow_comment=True,
        history=[],
        rng=random.Random(2),
    )
    assert plan.do_like is False
    assert plan.mode in {"comment", "none"}
    plan2 = pick_share_engage_plan(
        video_id="1156958673946775",
        allow_like=True,
        allow_comment=False,
        history=[],
        rng=random.Random(2),
    )
    assert plan2.do_comment is False
    assert plan2.mode in {"like", "none"}


def test_allocate_wave_page_engage_sparse_for_13_shares() -> None:
    """13 Page chia sẻ → khoảng 2–3 Like và 3–4 comment, không trùng Page."""
    from src.services.facebook_groups.video_engage import allocate_wave_page_engage

    pages = [f"page_{i}" for i in range(13)]
    likes, comments = allocate_wave_page_engage(
        pages,
        want_like=True,
        want_comment=True,
        rng=random.Random(42),
    )
    assert 2 <= len(likes) <= 3
    assert 3 <= len(comments) <= 4
    assert likes.isdisjoint(comments)
    assert likes | comments <= set(pages)


def test_allocate_wave_none_when_video_skips_engage() -> None:
    from src.services.facebook_groups.video_engage import allocate_wave_page_engage

    likes, comments = allocate_wave_page_engage(
        [f"p{i}" for i in range(10)],
        want_like=False,
        want_comment=False,
        rng=random.Random(1),
    )
    assert likes == set() and comments == set()


def test_shuffle_due_queue_picks_among_ready() -> None:
    from datetime import datetime, timedelta

    from src.services.facebook_groups.share_schedule import PlannedShareLink
    from src.services.facebook_groups.video_engage import shuffle_due_queue_items

    now = datetime(2026, 10, 18, 21, 0, 0)
    rows = [
        PlannedShareLink(when=now - timedelta(seconds=10), url="https://www.facebook.com/reel/1", state="waiting"),
        PlannedShareLink(when=now - timedelta(seconds=5), url="https://www.facebook.com/reel/2", state="waiting"),
        PlannedShareLink(when=now + timedelta(minutes=5), url="https://www.facebook.com/reel/3", state="waiting"),
    ]
    picked = {shuffle_due_queue_items(rows, now=now, rng=random.Random(i)).url for i in range(40)}
    assert "https://www.facebook.com/reel/1" in picked
    assert "https://www.facebook.com/reel/2" in picked
    assert "https://www.facebook.com/reel/3" not in picked


def test_share_engage_day_persist_roundtrip(tmp_path) -> None:
    from src.services.facebook_groups.video_engage import (
        ShareEngagePlan,
        load_share_engage_day,
        plan_share_engage_for_video,
        remember_share_engage_plan,
        save_share_engage_day,
    )

    path = tmp_path / "engage.json"
    plan = ShareEngagePlan(True, False, "like")
    remember_share_engage_plan(
        source="https://www.facebook.com/SrcPage",
        video_url="https://www.facebook.com/reel/2482358802253174",
        plan=plan,
        path=path,
    )
    day = load_share_engage_day(path)
    assert any(r.get("video_id") == "2482358802253174" for rows in day.values() for r in rows)
    again = plan_share_engage_for_video(
        source="https://www.facebook.com/SrcPage",
        video_url="https://www.facebook.com/reel/2482358802253174",
        allow_like=True,
        allow_comment=True,
        path=path,
        persist=True,
    )
    assert again.do_like is True and again.do_comment is False
    save_share_engage_day(day, path)
