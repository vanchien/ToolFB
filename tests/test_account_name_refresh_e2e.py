"""
E2E: thêm tài khoản (placeholder UID) → cập nhật tên lần lượt → không mở chồng Firefox.

Không mở browser thật — fake lookup/cookie + fake browser reader.
"""

from __future__ import annotations

import threading
import time

from src.services.account_name_refresh import (
    assert_sequential_browser_opens,
    plan_name_refresh_ids,
    run_name_refresh_sequential,
    should_apply_discovered_name,
)
from src.utils.schedule_posts_filters import (
    account_name_is_placeholder,
    account_name_should_take_facebook_name,
)


def test_uid_digit_name_is_placeholder() -> None:
    """Thêm từ thư mục/cookie: tên = UID số phải được coi là chưa có tên."""
    assert account_name_is_placeholder("550708732", "UID_550708732")
    assert account_name_is_placeholder("1117418104", "acc_abc")
    assert account_name_is_placeholder("UID_550708732", "acc_1")
    assert account_name_is_placeholder("Tài khoản mới", "acc_1")
    assert not account_name_is_placeholder("Đinh Bảo Trâm", "acc_1")
    assert account_name_should_take_facebook_name("550708732", "Minh Tuyết", "UID_550708732")


def test_plan_refresh_includes_uid_names_without_tick() -> None:
    rows = [
        {"id": "UID_550708732", "name": "550708732"},
        {"id": "acc_named", "name": "Andy Garcia"},
        {"id": "acc_new", "name": "Tài khoản mới"},
    ]
    ids, forced = plan_name_refresh_ids(rows, [])
    assert forced is False
    assert ids == ["UID_550708732", "acc_new"]


def test_plan_refresh_ticked_forced() -> None:
    rows = [
        {"id": "a", "name": "Andy Garcia"},
        {"id": "b", "name": "550708732"},
    ]
    ids, forced = plan_name_refresh_ids(rows, ["a", "b"])
    assert forced is True
    assert ids == ["a", "b"]


def test_e2e_sequential_name_refresh_cookie_then_browser() -> None:
    """
    Giả lập 4 TK: 2 lấy được qua cookie, 2 phải mở browser — luôn lần lượt 1 browser.
    """
    rows = [
        {"id": "acc_cookie_1", "name": "550708732"},
        {"id": "acc_cookie_2", "name": "Tài khoản mới"},
        {"id": "acc_browser_1", "name": "1117418104"},
        {"id": "acc_browser_2", "name": "UID_999"},
    ]
    saved: dict[str, str] = {}
    open_log: list[str] = []
    max_open = 0
    lock = threading.Lock()
    closed_profiles: list[list[str]] = []

    def lookup(acc: dict) -> tuple[str, str]:
        aid = str(acc["id"])
        if aid.endswith("cookie_1"):
            return "Minh Tuyết", ""
        if aid.endswith("cookie_2"):
            return "Đinh Bảo Trâm", ""
        return "", "cookie trống"

    def browser(aid: str) -> tuple[str, str]:
        nonlocal max_open
        with lock:
            open_log.append(f"open:{aid}")
            open_count = sum(1 for e in open_log if e.startswith("open:")) - sum(
                1 for e in open_log if e.startswith("close:")
            )
            max_open = max(max_open, open_count)
        time.sleep(0.05)  # giả lập goto facebook.com
        name = {
            "acc_browser_1": "Andy Garcia",
            "acc_browser_2": "Lan Anh",
        }.get(aid, "")
        with lock:
            open_log.append(f"close:{aid}")
        return name, "" if name else "không đọc được"

    def save(aid: str, name: str) -> None:
        saved[aid] = name

    def close_open(ids: list[str]) -> None:
        closed_profiles.append(list(ids))

    ids, forced = plan_name_refresh_ids(rows, [r["id"] for r in rows])
    result = run_name_refresh_sequential(
        rows,
        ids,
        forced=forced,
        lookup_cookie=lookup,
        read_browser=browser,
        save_name=save,
        close_open_profiles=close_open,
    )

    assert result.total == 4
    assert len(result.updated) == 4
    assert saved == {
        "acc_cookie_1": "Minh Tuyết",
        "acc_cookie_2": "Đinh Bảo Trâm",
        "acc_browser_1": "Andy Garcia",
        "acc_browser_2": "Lan Anh",
    }
    assert max_open == 1
    assert_sequential_browser_opens(open_log)
    assert closed_profiles and closed_profiles[0] == ids
    # Cookie path không mở browser
    assert "open:acc_cookie_1" not in open_log
    assert "open:acc_cookie_2" not in open_log
    assert open_log == [
        "open:acc_browser_1",
        "close:acc_browser_1",
        "open:acc_browser_2",
        "close:acc_browser_2",
    ]


def test_e2e_skips_when_browser_fails_keeps_going() -> None:
    """Một TK treo/ lỗi browser không làm dừng cả đợt — vẫn xử lý TK sau."""
    rows = [
        {"id": "a", "name": "111111"},
        {"id": "b", "name": "222222"},
        {"id": "c", "name": "333333"},
    ]
    saved: dict[str, str] = {}
    open_log: list[str] = []

    def lookup(_acc: dict) -> tuple[str, str]:
        return "", "no cookie"

    def browser(aid: str) -> tuple[str, str]:
        open_log.append(f"open:{aid}")
        if aid == "b":
            open_log.append(f"close:{aid}")
            return "", "profile chưa đăng nhập"
        time.sleep(0.02)
        open_log.append(f"close:{aid}")
        return f"User {aid.upper()}", ""

    result = run_name_refresh_sequential(
        rows,
        ["a", "b", "c"],
        forced=True,
        lookup_cookie=lookup,
        read_browser=browser,
        save_name=lambda aid, name: saved.__setitem__(aid, name),
    )
    assert [u[0] for u in result.updated] == ["a", "c"]
    assert any("b:" in s for s in result.skipped)
    assert_sequential_browser_opens(open_log)
    assert saved["a"] == "User A"
    assert saved["c"] == "User C"
    assert "b" not in saved


def test_should_apply_forced_over_real_name() -> None:
    assert should_apply_discovered_name("Andy", "Minh Tuyết", "acc_1", forced=True)
    assert not should_apply_discovered_name("Andy", "Andy", "acc_1", forced=True)
    assert not should_apply_discovered_name("Andy", "550708732", "acc_1", forced=True)
    assert should_apply_discovered_name("550708732", "Minh Tuyết", "acc_1", forced=False)


def test_name_from_html_embedded_json() -> None:
    """DOM trống nhưng HTML có \"NAME\":\"…\" vẫn lấy được tên."""
    from src.utils.account_proxy_mapper import name_from_profile_html

    html = '<html><title>Facebook</title><script>{"NAME":"Gerhard Van Wyk","id":"1"}</script></html>'
    assert name_from_profile_html(html) == "Gerhard Van Wyk"
    assert name_from_profile_html("<title>Facebook</title>") == ""


def test_chrome_ua_not_firefox_string() -> None:
    """Chromium không được gán UA Firefox (gây Emulation.setUserAgentOverride)."""
    from src.utils.browser_fingerprint import (
        default_desktop_chrome_user_agent,
        default_desktop_firefox_user_agent,
    )

    chrome = default_desktop_chrome_user_agent()
    firefox = default_desktop_firefox_user_agent()
    assert "Chrome/" in chrome
    assert "Firefox/" in firefox
    assert "Firefox/" not in chrome


def test_prepare_then_sequential_never_stacks_browsers() -> None:
    """close_open_profiles chạy 1 lần trước; browser không chồng."""
    rows = [{"id": f"a{i}", "name": f"{100000 + i}"} for i in range(5)]
    open_log: list[str] = []
    prepare_calls: list[list[str]] = []

    def lookup(_acc: dict) -> tuple[str, str]:
        return "", "empty cookie"

    def browser(aid: str) -> tuple[str, str]:
        open_log.append(f"open:{aid}")
        time.sleep(0.01)
        open_log.append(f"close:{aid}")
        return f"Name {aid}", ""

    ids = [r["id"] for r in rows]
    result = run_name_refresh_sequential(
        rows,
        ids,
        forced=True,
        lookup_cookie=lookup,
        read_browser=browser,
        save_name=lambda _a, _n: None,
        close_open_profiles=lambda aids: prepare_calls.append(list(aids)),
    )
    assert len(result.updated) == 5
    assert len(prepare_calls) == 1
    assert_sequential_browser_opens(open_log)
