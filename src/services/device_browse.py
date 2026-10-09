"""Mở Facebook và chỉ cuộn bảng tin. Không bấm like, bình luận hay đăng."""

from __future__ import annotations

import random
from typing import Any

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


def feed_can_scroll(url: str) -> bool:
    """Trang đăng nhập hoặc checkpoint thì không cuộn."""
    raw = str(url or "").casefold()
    if "checkpoint" in raw or "/login" in raw or "login.php" in raw:
        return False
    return "facebook.com" in raw


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
