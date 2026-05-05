"""Tuỳ chọn thumbnail Reel Meta (wizard Cách 1) — lưu trên job queue ``schedule_posts.json``."""

from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse

# Meta Business Reel wizard (multi-step): chọn ô preview đầu tiên trong lưới thumbnail.
REEL_THUMBNAIL_OFF = "off"
REEL_THUMBNAIL_METHOD1_FIRST_AUTO = "method1_first_auto"


def normalize_reel_thumbnail_choice(raw: Any) -> str:
    s = str(raw or "").strip().lower().replace(" ", "_").replace("-", "_")
    aliases = {
        "method1_first_auto",
        "method1",
        "cach1",
        "way1",
        "first_auto",
        "first_thumbnail",
        "auto_first",
    }
    if s in aliases or s == REEL_THUMBNAIL_METHOD1_FIRST_AUTO:
        return REEL_THUMBNAIL_METHOD1_FIRST_AUTO
    return REEL_THUMBNAIL_OFF


class _FirstImgSrcParser(HTMLParser):
    """Lấy ``src`` của ``img`` đầu tiên (dùng tham chiếu / test HTML mẫu từ Meta)."""

    def __init__(self) -> None:
        super().__init__()
        self.first_src: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.first_src is not None or tag.lower() != "img":
            return
        ad = {k.lower(): (v or "") for k, v in attrs}
        src = (ad.get("src") or "").strip()
        if not src or src.startswith("data:"):
            return
        self.first_src = src


def first_img_src_from_meta_reel_thumbnail_html(html: str) -> str | None:
    """
    Parse HTML mẫu (Cách 1): trả về URL ``src`` của thẻ ``img`` đầu tiên có ``src`` http(s).

    Dùng để đối chiếu cấu trúc DOM khi Meta đổi UI; luồng đăng thật dùng Playwright trên trang live.
    """
    raw = (html or "").strip()
    if not raw:
        return None
    p = _FirstImgSrcParser()
    try:
        p.feed(raw)
    except Exception:
        return None
    u = p.first_src
    if not u:
        m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', raw, re.I)
        u = (m.group(1) or "").strip() if m else ""
    if not u:
        return None
    try:
        pr = urlparse(u)
        if pr.scheme in ("http", "https"):
            return u
    except Exception:
        return None
    return None
