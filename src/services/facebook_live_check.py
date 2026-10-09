"""Check Live bằng trình duyệt của một tài khoản đã đăng nhập. Không gọi Graph API."""

from __future__ import annotations

import time
from typing import Any, Callable

from loguru import logger


_LOCK_HEADINGS: tuple[str, ...] = (
    "bạn hiện không xem được nội dung này",
    "you can't see this content",
    "you can’t see this content",
    "this content isn't available",
    "this content isn’t available",
    "sorry, this content isn't available",
    "tài khoản này đã bị vô hiệu hóa",
    "tài khoản đã bị vô hiệu hóa",
    "account has been disabled",
)


def _plain(visible: str) -> str:
    return " ".join((visible or "").split()).casefold()


def _rendered_profile_is_live(visible: str) -> bool:
    """Hồ sơ còn mở: có người theo dõi, tab hồ sơ, hoặc nút kết bạn."""
    text = _plain(visible)
    if any(token in text for token in ("người theo dõi", "đang theo dõi", "followers", "people follow this")):
        return True
    if "bài viết" in text and "giới thiệu" in text:
        return True
    if "posts" in text and "about" in text:
        return True
    return "kết bạn" in text or "add friend" in text


def _rendered_profile_is_locked(visible: str) -> bool:
    """Trang khóa chiếm màn hình. Một dòng «đã xóa nội dung» trên hồ sơ sống không tính."""
    text = _plain(visible)
    if _rendered_profile_is_live(text):
        return False
    return any(marker in text for marker in _LOCK_HEADINGS)


def classify_rendered_profile(title: str, visible: str) -> tuple[str, str]:
    """Hồ sơ đã hiện là Live, kể cả khi có bài đã xóa. Chỉ trang khóa, không có hồ sơ, mới là Die."""
    if _rendered_profile_is_live(visible):
        name = " ".join((title or "").split())
        if len(name) < 2 or name.casefold() in {"facebook", "error", "lỗi"}:
            name = "hồ sơ công khai"
        return "login_ok", f"Còn hoạt động — {name[:80]}"
    if _rendered_profile_is_locked(visible):
        return "login_failed", "UID không còn hoạt động"
    return "error", "Trang chưa hiện hồ sơ"


def viewer_session_problem(url: str, title: str) -> str:
    """Phiên tài khoản dùng để check bị đăng xuất hoặc checkpoint. Không dựa vào chữ trên hồ sơ đích."""
    raw_url = str(url or "").casefold()
    raw_title = " ".join(str(title or "").split()).casefold()
    if "checkpoint" in raw_url:
        return "checkpoint"
    if "/login" in raw_url or "login.php" in raw_url:
        return "session"
    if raw_title in {"log in", "log into facebook", "đăng nhập", "login"}:
        return "session"
    return ""


def _watch_profile(page: Any, stop: Callable[[], bool]) -> tuple[str, str]:
    """Đợi trang ổn định. Hồ sơ hiện sau là Live. Trang khóa còn lại đến cuối mới là Die."""
    locked = ("login_failed", "UID không còn hoạt động")
    saw_lock = False
    for _attempt in range(10):
        if stop():
            return "cancelled", ""
        problem = viewer_session_problem(getattr(page, "url", ""), page.title())
        if problem:
            return problem, ""
        visible = page.inner_text("body")
        if _rendered_profile_is_live(visible):
            return classify_rendered_profile(page.title(), visible)
        if _rendered_profile_is_locked(visible):
            saw_lock = True
            locked = classify_rendered_profile(page.title(), visible)
        page.wait_for_timeout(450)
    if saw_lock:
        return locked
    return "error", "Trang chưa hiện hồ sơ"


def check_profiles_on_page(
    page: Any,
    uids: list[str],
    *,
    on_result: Callable[[str, str, str], None],
    on_begin: Callable[[str], None] | None = None,
    should_stop: Callable[[], bool] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> str:
    """Mở lần lượt từng UID trên trang đã đăng nhập. Trả ``session``, ``checkpoint``, ``cancelled`` hoặc chuỗi rỗng."""
    stop = should_stop or (lambda: False)
    pause = sleep or (lambda _seconds: None)
    if stop():
        return "cancelled"
    page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(800)
    problem = viewer_session_problem(getattr(page, "url", ""), page.title())
    if problem:
        return problem
    for uid in uids:
        if stop():
            return "cancelled"
        target = str(uid or "").strip()
        if not target.isdigit():
            on_result(target, "error", "Thiếu UID số")
            continue
        if on_begin is not None:
            on_begin(target)
        logger.info("Check Live UID {}", target)
        url = f"https://www.facebook.com/profile.php?id={target}"
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        status, detail = _watch_profile(page, stop)
        if status in {"session", "checkpoint", "cancelled"}:
            return status
        if status not in {"login_ok", "login_failed"}:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            status, detail = _watch_profile(page, stop)
            if status in {"session", "checkpoint", "cancelled"}:
                return status
        on_result(target, status, detail)
        pause(0.8)
    return ""


def _profile_dir(account: dict[str, Any]) -> str:
    return str(account.get("portable_path") or account.get("profile_path") or "")


def set_account_window_visible(account: dict[str, Any], visible: bool) -> None:
    """Hiện hoặc ẩn cửa sổ Firefox của profile đang check."""
    profile = _profile_dir(account)
    if not profile:
        return
    try:
        if visible:
            from src.utils.win_browser_window import show_firefox_window_for_profile

            show_firefox_window_for_profile(profile, timeout_s=2)
        else:
            from src.utils.win_browser_window import hide_firefox_window_for_profile

            hide_firefox_window_for_profile(profile, timeout_s=2)
    except Exception:  # noqa: BLE001
        logger.debug("Chưa đổi được cửa sổ Check Live")


def run_live_check_in_account_browser(
    account: dict[str, Any],
    uids: list[str],
    *,
    on_result: Callable[[str, str, str], None],
    on_begin: Callable[[str], None] | None = None,
    should_stop: Callable[[], bool] | None = None,
    show_window: Callable[[], bool] | None = None,
) -> str:
    """Mở profile persistent của tài khoản đã chọn, check, rồi đóng trình duyệt."""
    from src.automation.browser_factory import (
        BrowserFactory,
        prepare_playwright_sync_thread,
        sync_close_persistent_context,
    )

    account_id = str(account.get("id") or account.get("account_id") or "")
    want_show = show_window or (lambda: False)
    shown = {"on": None}

    def _sync_window() -> None:
        visible = bool(want_show())
        if shown["on"] is visible:
            return
        shown["on"] = visible
        set_account_window_visible(account, visible)

    prepare_playwright_sync_thread(label=f"live-check:{account_id}")
    factory = BrowserFactory(headless=False, playwright_shared=True)
    context = None
    try:
        context = factory.launch_persistent_context_from_account_dict(
            account,
            headless=False,
            grid_viewport=(1366, 768),
            window_position=(40, 40) if want_show() else (-3200, -3200),
            disable_notifications=True,
            force_desktop_facebook=True,
        )
        _sync_window()
        page = context.pages[0] if context.pages else context.new_page()

        def _begin(uid: str) -> None:
            _sync_window()
            if on_begin is not None:
                on_begin(uid)

        return check_profiles_on_page(
            page,
            uids,
            on_result=on_result,
            on_begin=_begin,
            should_stop=should_stop,
            sleep=time.sleep,
        )
    finally:
        sync_close_persistent_context(context, log_label="live-check", same_thread=True)
        try:
            factory.close()
        except Exception:  # noqa: BLE001
            logger.debug("Đóng trình duyệt Check Live không thành công")
