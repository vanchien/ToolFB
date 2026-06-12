"""
Thu thập link Reel Instagram từ tab ``/{username}/reels/``.

yt-dlp (2026.03) chưa hỗ trợ quét hàng loạt URL dạng ``…/username/reels/`` — dùng Playwright
cuộn trang và trích ``/reel/<shortcode>`` (public hoặc kèm cookie Playwright nếu có).
"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Callable, Optional

from loguru import logger
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

StatusFn = Callable[[str], None]

_IG_REEL_HREF_RE = re.compile(r"/reel/([A-Za-z0-9_-]{5,})", re.IGNORECASE)
_IG_REEL_TEXT_RE = re.compile(
    r"instagram\.com/reel/([A-Za-z0-9_-]{5,})",
    re.IGNORECASE,
)

_JS_SCROLL_SNAP = """
() => {
  const e = document.scrollingElement || document.documentElement;
  const b = document.body;
  const sh = Math.max(e.scrollHeight, b ? b.scrollHeight : 0);
  const st = e.scrollTop;
  const ch = e.clientHeight || window.innerHeight || 0;
  return { st, sh, ch, atBottom: st + ch >= sh - 120 };
}
"""


def _noop_status(_: str) -> None:
    pass


def _env_headless_default() -> bool:
    raw = str(os.environ.get("TOOLFB_IG_REELS_HEADLESS", "1")).strip().lower()
    return raw not in {"0", "false", "no", "off"}


def normalize_instagram_reels_tab_url(url: str) -> str:
    """Chuẩn hoá về tab Reels: ``https://www.instagram.com/{user}/reels/``."""
    u = (url or "").strip()
    if not u:
        return u
    if not u.lower().startswith("http"):
        u = "https://" + u.lstrip("/")
    u = u.split("?")[0].split("#")[0].rstrip("/")
    low = u.lower()
    if "instagram.com" not in low and "instagr.am" not in low:
        return (url or "").strip()
    if re.search(r"instagram\.com/[^/]+/reels/?$", low):
        return u + "/"
    m = re.match(r"https?://(?:www\.)?instagram\.com/([^/?#]+)/?$", u, re.I)
    if m:
        seg = m.group(1).lower()
        reserved = {
            "p",
            "reel",
            "reels",
            "stories",
            "explore",
            "accounts",
            "direct",
            "tv",
            "about",
            "legal",
        }
        if seg not in reserved:
            return f"https://www.instagram.com/{m.group(1)}/reels/"
    return u + ("/" if not u.endswith("/") else "")


def is_instagram_reels_tab_url(url: str) -> bool:
    low = (url or "").strip().lower()
    if "instagram.com" not in low:
        return False
    if re.search(r"instagram\.com/[^/]+/reels", low):
        return True
    if re.match(r"https?://(?:www\.)?instagram\.com/[^/?#]+/?$", (url or "").strip(), re.I):
        return True
    return False


def _extract_reel_urls_from_hrefs(hrefs: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for href in hrefs or []:
        s = str(href or "").strip()
        if not s:
            continue
        m = _IG_REEL_HREF_RE.search(s)
        if not m:
            continue
        code = m.group(1)
        full = f"https://www.instagram.com/reel/{code}/"
        if full in seen:
            continue
        seen.add(full)
        out.append(full)
    return out


def _extract_reel_urls_from_text(text: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for code in _IG_REEL_TEXT_RE.findall(text or ""):
        full = f"https://www.instagram.com/reel/{code}/"
        if full in seen:
            continue
        seen.add(full)
        out.append(full)
    return out


def _resolve_cookie_storage_state(cookie_path: str | None) -> str | None:
    """Trả đường dẫn ``storage_state`` JSON nếu file hợp lệ."""
    p = str(cookie_path or "").strip()
    if not p:
        return None
    path = Path(p).expanduser()
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(raw, dict) and ("cookies" in raw or "origins" in raw):
        return str(path.resolve())
    return None


def scan_instagram_profile_reels_page(
    *,
    page_url: str,
    cookie_path: str | None = None,
    max_reels: int = 200,
    max_scroll_rounds: int = 60,
    max_scan_minutes: float = 15.0,
    scroll_until_end: bool = True,
    scroll_pause_sec: float = 1.1,
    headless: bool | None = None,
    status: StatusFn | None = None,
    on_partial: Optional[Callable[[list[str]], None]] = None,
) -> dict[str, Any]:
    """
    Mở tab Reels Instagram, cuộn và trả về URL reel.

    Returns:
        ``{ "ok": bool, "items": [{"video_id", "url", "title"}], "message": str }``
    """
    st = status or _noop_status
    url = normalize_instagram_reels_tab_url(page_url.strip())
    if "instagram.com" not in url.lower():
        return {"ok": False, "items": [], "message": "URL không phải Instagram."}

    hl = _env_headless_default() if headless is None else bool(headless)
    max_reels = max(1, min(10_000, int(max_reels)))
    max_scroll_rounds = max(5, int(max_scroll_rounds))
    if scroll_until_end:
        max_scroll_rounds = max(max_scroll_rounds, 80)
    max_scan_minutes = max(1.0, min(120.0, float(max_scan_minutes or 15.0)))
    started_at = time.monotonic()
    hard_deadline = started_at + (max_scan_minutes * 60.0)

    ordered_urls: list[str] = []
    seen_urls: set[str] = set()

    def _add_url(u: str) -> None:
        s = str(u or "").strip()
        if not s or s in seen_urls or len(seen_urls) >= max_reels:
            return
        seen_urls.add(s)
        ordered_urls.append(s)

    def _emit_partial() -> None:
        if not on_partial:
            return
        try:
            on_partial(list(ordered_urls))
        except Exception as exc:  # noqa: BLE001
            logger.debug("ig on_partial: {}", exc)

    def _collect_from_page(page: Any) -> None:
        try:
            hrefs = page.eval_on_selector_all(
                "a[href]",
                "els => els.map(a => a.getAttribute('href') || a.href || '')",
            )
        except Exception:
            hrefs = []
        for u in _extract_reel_urls_from_hrefs([str(h) for h in hrefs]):
            _add_url(u)
        try:
            _add_from = _extract_reel_urls_from_text(page.content())
        except Exception:
            _add_from = []
        for u in _add_from:
            _add_url(u)

    def _is_login_wall(url_now: str, body_text: str) -> bool:
        u = str(url_now or "").lower()
        path = u.split("?")[0].rstrip("/")
        if "/accounts/login" in u or path.endswith("/login"):
            return True
        b = str(body_text or "").lower()
        return any(
            x in b
            for x in (
                "log in to instagram",
                "đăng nhập vào instagram",
                "log in to see",
                "sign up to see",
            )
        )

    storage = _resolve_cookie_storage_state(cookie_path)

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=hl)
            try:
                ctx_kwargs: dict[str, Any] = {
                    "viewport": {"width": 1280, "height": 900},
                    "locale": "vi-VN",
                    "user_agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                    ),
                }
                if storage:
                    ctx_kwargs["storage_state"] = storage
                    st("Đang dùng cookie Playwright từ config.")
                context = browser.new_context(**ctx_kwargs)
                page = context.new_page()
                try:
                    Stealth().apply_stealth_sync(page)
                except Exception:
                    pass
                st("Đang tải tab Reels Instagram…")
                page.goto(url, wait_until="domcontentloaded", timeout=120_000)
                time.sleep(min(2.0, scroll_pause_sec + 0.5))
                try:
                    body_text = page.inner_text("body", timeout=4000)
                except Exception:
                    body_text = ""
                if _is_login_wall(page.url, body_text):
                    return {
                        "ok": False,
                        "items": [],
                        "message": (
                            "Instagram yêu cầu đăng nhập. Export cookie Playwright (storage_state JSON) "
                            "và đặt cookie_path trong config universal_video_downloader.json → instagram_reels."
                        ),
                    }
                _collect_from_page(page)
                _emit_partial()

                stable = 0
                prev_count = 0
                for i in range(max_scroll_rounds):
                    if time.monotonic() >= hard_deadline:
                        st(f"Đã chạm giới hạn {max_scan_minutes:.0f} phút — dừng.")
                        break
                    if len(ordered_urls) >= max_reels:
                        break
                    st(f"Cuộn {i + 1}/{max_scroll_rounds} — đã thấy {len(ordered_urls)} reel…")
                    try:
                        page.evaluate(
                            "() => { const e=document.scrollingElement||document.documentElement;"
                            " e.scrollBy(0, Math.floor((window.innerHeight||800)*0.92)); }"
                        )
                        page.mouse.wheel(0, 2400)
                    except Exception:
                        pass
                    time.sleep(scroll_pause_sec)
                    try:
                        body_text = page.inner_text("body", timeout=1500)
                    except Exception:
                        body_text = ""
                    if _is_login_wall(page.url, body_text):
                        break
                    _collect_from_page(page)
                    _emit_partial()
                    if len(ordered_urls) == prev_count:
                        stable += 1
                        if stable >= 15 and not scroll_until_end:
                            break
                        if scroll_until_end and stable >= 28:
                            break
                    else:
                        stable = 0
                    prev_count = len(ordered_urls)
                context.close()
            finally:
                browser.close()
    except Exception as exc:  # noqa: BLE001
        logger.exception("scan_instagram_profile_reels_page")
        return {"ok": False, "items": [], "message": str(exc)}

    elapsed_sec = int(max(0.0, time.monotonic() - started_at))
    items = [
        {"video_id": str(i + 1), "url": u, "title": f"Reel {i + 1}"}
        for i, u in enumerate(ordered_urls)
    ]
    if not items:
        return {
            "ok": False,
            "items": [],
            "message": (
                "Không thu được reel nào. Profile có thể riêng tư, hoặc cần cookie đăng nhập. "
                "Vẫn có thể tải từng reel bằng link /reel/… trực tiếp."
            ),
        }
    return {
        "ok": True,
        "items": items,
        "message": f"Đã quét {len(items)} reel Instagram ({elapsed_sec}s).",
    }
