"""Mở Facebook và chỉ cuộn bảng tin. Không bấm like, bình luận hay đăng."""

from __future__ import annotations

import random
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
  for (const el of document.querySelectorAll("[aria-label]")) {
    const value = (el.getAttribute("aria-label") || "").trim();
    if (value && value.length <= 180) labels.push(value);
    if (labels.length >= 300) break;
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
  return {labels, links};
}"""


def read_logged_in_display_name(page: Any) -> str:
    """Đọc tên người dùng trên Facebook đã đăng nhập. Rỗng nếu trang chưa hiện tên."""
    from src.utils.account_proxy_mapper import (
        facebook_display_name_from_page_bits,
        facebook_href_is_own_profile,
    )

    uid = ""
    try:
        cookies = page.context.cookies("https://www.facebook.com")
    except Exception:
        cookies = []
    for item in cookies or []:
        if str(item.get("name") or "").strip().lower() != "c_user":
            continue
        uid = "".join(ch for ch in str(item.get("value") or "") if ch.isdigit())
        break
    try:
        payload = page.evaluate(_READ_DISPLAY_NAME_JS, uid)
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    labels = payload.get("labels") if isinstance(payload.get("labels"), list) else []
    raw_links = payload.get("links") if isinstance(payload.get("links"), list) else []
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
        if text:
            own_texts.append(text)
    return facebook_display_name_from_page_bits(
        [str(item) for item in labels],
        own_texts,
    )


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
