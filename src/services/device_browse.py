"""Mở Facebook và chỉ cuộn bảng tin. Không bấm like, bình luận hay đăng."""

from __future__ import annotations

import random
import time
from typing import Any

from loguru import logger

BROWSE_LIMIT_S = 60


def browse_limit_reached(started: float, now: float, limit_s: float = BROWSE_LIMIT_S) -> bool:
    """Một lần lướt nhận thiết bị chỉ kéo dài ``limit_s`` giây."""
    return (now - started) >= limit_s


# Câu trên trang khóa / checkpoint. Giữ nguyên chữ Facebook, không tự vượt.
_BLOCK_MARKERS = (
    "another device",
    "unlock your account",
    "locked your account",
    "may have been hacked",
    "can't match the device",
    "cannot match the device",
    "isn't safe to go any further",
    "account has been disabled",
    "account disabled",
    "thiết bị khác",
    "mở khóa tài khoản",
    "khóa tài khoản",
    "bị hack",
    "không khớp thiết bị",
    "không an toàn",
    "tài khoản đã bị vô hiệu",
    "vô hiệu hóa tài khoản",
)


def _page_lines(body: str) -> list[str]:
    """Các dòng chữ nhìn thấy, bỏ dòng quá ngắn."""
    seen: list[str] = []
    for raw in str(body or "").replace("\r", "\n").split("\n"):
        line = " ".join(raw.split())
        if len(line) < 18 or line in seen:
            continue
        seen.append(line)
    return seen


def describe_account_block(url: str, body: str, *, expect_session: bool = False) -> str:
    """Câu lỗi đúng như Facebook đang hiện cho tài khoản này. Rỗng nếu trang dùng được."""
    raw_url = str(url or "").casefold()
    on_checkpoint = "checkpoint" in raw_url
    on_login = "/login" in raw_url or "login.php" in raw_url
    lines = _page_lines(body)
    picked: list[str] = []
    for line in lines:
        low = line.casefold()
        if any(marker in low for marker in _BLOCK_MARKERS):
            picked.append(line)
        if len(picked) >= 3:
            break
    if picked:
        return " ".join(picked)[:220]
    if on_checkpoint and lines:
        return lines[0][:220]
    if on_checkpoint:
        return "Facebook checkpoint — chưa đọc được câu lỗi trên trang"
    if expect_session and on_login:
        return "Facebook mở trang đăng nhập — phiên của tài khoản này chưa vào được"
    return ""


def read_visible_text(page: Any) -> str:
    """Chữ nhìn thấy trên trang, cắt ngắn để phân loại."""
    try:
        text = page.inner_text("body")
    except Exception:
        return ""
    return str(text or "")[:4000]


_READ_DISPLAY_NAME_JS = """(uid) => {
  const labels = [];
  const matcher = /nghĩ gì|on your mind|thông báo của |notification settings for /i;
  for (const el of document.querySelectorAll("[aria-label]")) {
    const value = (el.getAttribute("aria-label") || "").trim();
    if (!value || value.length > 180 || !matcher.test(value)) continue;
    labels.push(value);
    if (labels.length >= 20) break;
  }
  const links = [];
  if (uid) {
    for (const anchor of document.querySelectorAll("a[href]")) {
      const href = anchor.getAttribute("href") || "";
      if (!href.includes(uid)) continue;
      const text = (anchor.innerText || "").trim().split("\\n")[0].trim();
      if (text && text.length <= 80) links.push({href, text});
      if (links.length >= 40) break;
    }
  }
  const headings = [];
  const root = document.querySelector('[role="main"]') || document.body;
  for (const el of root.querySelectorAll("h1")) {
    const text = (el.innerText || "").trim().split("\\n")[0].trim();
    if (text && text.length <= 80) headings.push(text);
    if (headings.length >= 8) break;
  }
  return {labels, links, headings, title: document.title || "", url: location.href || ""};
}"""


def facebook_uid_on_page(page: Any) -> str:
    """UID đang đăng nhập trong trình duyệt. Rỗng nếu chưa có cookie phiên."""
    try:
        cookies = page.context.cookies("https://www.facebook.com")
    except Exception:
        return ""
    for item in cookies or []:
        if str(item.get("name") or "").strip().lower() != "c_user":
            continue
        return "".join(ch for ch in str(item.get("value") or "") if ch.isdigit())
    return ""


_READ_PROFILE_HEADER_JS = """() => {
  const clean = (value) => (value || "").replace(/\\u00a0/g, " ").replace(/\\s+/g, " ").trim();
  const followerRe = /followers|following|người theo dõi|đang theo dõi/i;
  const names = [];
  const push = (value) => {
    const name = clean(value).split("\\n")[0].trim();
    if (!name || name.length < 2 || name.length > 80 || followerRe.test(name)) return;
    if (!names.includes(name)) names.push(name);
  };
  const root = document.body;
  for (const el of root.querySelectorAll("h1, [role='heading']")) push(el.innerText);
  for (const el of root.querySelectorAll("span, div, a, h1")) {
    const text = clean(el.innerText);
    if (!text || text.length > 180 || !followerRe.test(text)) continue;
    let parent = el;
    for (let depth = 0; depth < 10 && parent; depth += 1) {
      parent = parent.parentElement;
      if (!parent) break;
      for (const span of parent.querySelectorAll("h1, [role='heading'], div[role='button'] > span, a[role='link'] > span")) {
        push(span.innerText);
      }
      if (names.length) break;
    }
    if (names.length) break;
  }
  return {title: document.title || "", names};
}"""


def _usable_account_name(name: str) -> str:
    """Tên người dùng đã lọc nút menu và nhãn mặc định."""
    from src.utils.schedule_posts_filters import account_name_is_placeholder

    clean = " ".join(str(name or "").replace("\xa0", " ").split())[:80]
    if not clean or clean.isdigit() or account_name_is_placeholder(clean):
        return ""
    return clean


def read_account_display_name(page: Any, *, visit_profile: bool = False) -> str:
    """Tên tài khoản đang đăng nhập.

    Trang bảng tin thường không có tên. Khi ``visit_profile`` bật, mở trang cá nhân
    để đọc tên trên nút cạnh dòng follower, rồi quay lại trang trước.
    """
    from src.utils.account_proxy_mapper import facebook_href_is_own_profile

    found = _usable_account_name(read_profile_header_name(page)) or _usable_account_name(
        read_logged_in_display_name(page)
    )
    if found or not visit_profile:
        return found
    uid = facebook_uid_on_page(page)
    current = str(getattr(page, "url", "") or "")
    if not uid or facebook_href_is_own_profile(current, uid):
        return ""
    try:
        page.goto(
            f"https://www.facebook.com/profile.php?id={uid}",
            wait_until="domcontentloaded",
            timeout=45_000,
        )
        for _ in range(6):
            found = _usable_account_name(read_profile_header_name(page)) or _usable_account_name(
                read_logged_in_display_name(page)
            )
            if found:
                break
            page.wait_for_timeout(1000)
    except Exception:
        found = found or ""
    finally:
        if "facebook.com" in current.casefold() and not facebook_href_is_own_profile(current, uid):
            try:
                page.goto(current, wait_until="domcontentloaded", timeout=45_000)
            except Exception:
                pass
    return found


def read_profile_header_name(page: Any) -> str:
    """Tên trên đầu trang cá nhân, kể cả khi Facebook để tên trong nút chứ không phải h1."""
    from src.utils.account_proxy_mapper import pick_profile_header_name

    try:
        payload = page.evaluate(_READ_PROFILE_HEADER_JS)
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    names = payload.get("names") if isinstance(payload.get("names"), list) else []
    return pick_profile_header_name([str(item) for item in names], str(payload.get("title") or ""))


def read_logged_in_display_name(page: Any) -> str:
    """Đọc tên người dùng trên Facebook đã đăng nhập. Rỗng nếu trang chưa hiện tên."""
    from src.utils.account_proxy_mapper import (
        facebook_display_name_from_page_bits,
        facebook_href_is_own_profile,
        name_from_facebook_window_title,
    )

    uid = facebook_uid_on_page(page)
    try:
        payload = page.evaluate(_READ_DISPLAY_NAME_JS, uid)
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    labels = payload.get("labels") if isinstance(payload.get("labels"), list) else []
    raw_links = payload.get("links") if isinstance(payload.get("links"), list) else []
    headings = payload.get("headings") if isinstance(payload.get("headings"), list) else []
    url = str(payload.get("url") or "")
    on_profile = bool(uid) and facebook_href_is_own_profile(url, uid)
    own_texts: list[str] = []
    for item in raw_links:
        if isinstance(item, dict):
            href = str(item.get("href") or "")
            text = str(item.get("text") or "")
        else:
            href = ""
            text = str(item or "")
        if uid and not facebook_href_is_own_profile(href, uid):
            continue
        if "edit" in href.casefold():
            continue
        if text:
            own_texts.append(text)
    named = facebook_display_name_from_page_bits(
        [str(item) for item in labels],
        own_texts,
        [str(item) for item in headings] if on_profile else [],
    )
    if named:
        return named
    if on_profile:
        return name_from_facebook_window_title(str(payload.get("title") or ""))
    return ""


def is_old_machine_url(url: str) -> bool:
    """Tab khôi phục từ máy khác, dạng ``http://0.0.x.x``."""
    raw = str(url or "").strip().casefold()
    if not raw or raw.startswith("about:"):
        return False
    return "://0.0." in raw or raw.startswith("0.0.")


def feed_can_scroll(url: str) -> bool:
    """Trang đăng nhập hoặc checkpoint thì không cuộn."""
    raw = str(url or "").casefold()
    if "checkpoint" in raw or "/login" in raw or "login.php" in raw:
        return False
    return "facebook.com" in raw


def browse_page_settled(url: str) -> bool:
    """Đã tới Facebook, trang đăng nhập hoặc checkpoint — không còn link máy cũ."""
    raw = str(url or "").casefold()
    if "checkpoint" in raw or "/login" in raw or "login.php" in raw:
        return "facebook.com" in raw
    return feed_can_scroll(url)


def select_interaction_page(context: Any) -> Any:
    """Giữ một tab để vào Facebook và đóng tab khôi phục địa chỉ cũ.

    Profile chép từ máy khác hay mở lại ``http://0.0.3.188``. Tab đó không phải Facebook
    nên bị đóng. Tab đăng nhập hoặc checkpoint được giữ, không bấm xác nhận thiết bị.
    """
    pages = list(getattr(context, "pages", []) or [])
    if not pages:
        return context.new_page()

    def _url(page: Any) -> str:
        return str(getattr(page, "url", "") or "")

    chosen = None
    for page in pages:
        if "facebook.com" in _url(page).casefold():
            chosen = page
            break
    if chosen is None:
        for page in pages:
            raw = _url(page).casefold()
            if is_old_machine_url(raw):
                continue
            if not raw or raw.startswith("about:"):
                chosen = page
                break
    if chosen is None:
        chosen = pages[0]
    for extra in list(pages):
        if extra is chosen:
            continue
        if "facebook.com" in _url(extra).casefold():
            continue
        close = getattr(extra, "close", None)
        if close is None:
            continue
        try:
            close()
        except Exception:
            logger.debug("Không đóng được tab địa chỉ cũ.")
    return chosen


def foreign_tab_open(context: Any) -> bool:
    """Còn tab không phải Facebook (link proxy cũ, trang lỗi)."""
    for page in list(getattr(context, "pages", []) or []):
        raw = str(getattr(page, "url", "") or "").casefold()
        if browse_page_settled(raw):
            continue
        if not raw or raw.startswith("about:"):
            continue
        return True
    return False


def show_facebook_only(context: Any) -> Any:
    """Đóng mọi tab link cũ rồi đưa tab còn lại tới facebook.com.

    Firefox khôi phục tab proxy sau khi cửa sổ đã hiện. Gọi lại vài nhịp cho đến khi
    không còn tab đó. Trang đăng nhập hoặc checkpoint không bị đóng và không bị bấm.
    """
    page = select_interaction_page(context)
    try:
        open_home_feed(page)
    except Exception:
        logger.debug("Chưa mở được facebook.com, sẽ thử lại.")
    try:
        page.bring_to_front()
    except Exception:
        pass
    return page


def hold_facebook_tabs(context: Any, *, attempts: int = 20, pause_s: float = 0.45) -> Any:
    """Đóng tab proxy cũ nhiều lần. Firefox hay mở lại tab đó vài giây sau khi cửa sổ hiện.

    Dùng cho mọi tài khoản, không chỉ tài khoản mở đầu tiên. Dừng khi đang ở Facebook,
    trang đăng nhập hoặc checkpoint.
    """
    page = show_facebook_only(context)
    for _ in range(max(0, int(attempts) - 1)):
        current = str(getattr(page, "url", "") or "")
        if browse_page_settled(current) and not foreign_tab_open(context):
            return page
        time.sleep(max(0.05, float(pause_s)))
        page = show_facebook_only(context)
    return page


def open_home_feed(page: Any) -> None:
    """Vào bảng tin nếu trình duyệt chưa ở Facebook.

    Trang đăng nhập hoặc checkpoint được giữ nguyên — không bấm xác nhận thiết bị.
    """
    url = str(getattr(page, "url", "") or "")
    raw = url.casefold()
    if "checkpoint" in raw or "/login" in raw or "login.php" in raw:
        return
    if feed_can_scroll(url):
        return
    if url and "facebook.com" not in raw and not url.startswith("about:"):
        logger.warning("Tab đang ở {} — chuyển sang Facebook.", url[:120])
    page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=60_000)


def nudge_feed(page: Any, rng: random.Random | None = None) -> bool:
    """Cuộn một đoạn. Không click. Trả False khi không phải bảng tin."""
    if not feed_can_scroll(str(getattr(page, "url", "") or "")):
        return False
    roll = rng or random.Random()
    distance = roll.randint(350, 950)
    if roll.random() < 0.2:
        distance = -roll.randint(120, 420)
    page.evaluate("(dy) => window.scrollBy(0, dy)", distance)
    return True
