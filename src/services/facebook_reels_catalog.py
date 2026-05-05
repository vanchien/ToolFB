"""
Thu thập danh sách link ``/reel/<id>`` trên tab Reels của profile Facebook.

yt-dlp không hỗ trợ URL dạng ``…/username/reels/`` — dùng Playwright cuộn trang
và trích href / HTML. Hỗ trợ cả chế độ public và dùng profile account đã login.
"""

from __future__ import annotations

import os
import re
import time
from typing import Any, Callable, Optional

from loguru import logger
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
from src.automation.browser_factory import BrowserFactory, sync_close_persistent_context

StatusFn = Callable[[str], None]

_REEL_ID_RE = re.compile(r"/reel/(\d{8,})", re.IGNORECASE)
_VIDEOS_ID_RE = re.compile(r"/videos/(?:[^/?#]+/)?(\d{8,})", re.IGNORECASE)
# Facebook nhúng trong JSON / query (DOM đôi khi không có href đầy đủ)
_FB_WATCH_V_RE = re.compile(
    r"(?:facebook\.com|fb\.watch)[^\"'\s<>]*[?&]v=(\d{10,})",
    re.IGNORECASE,
)
_FB_PLAYABLE_ID_RE = re.compile(r'"playable_id"\s*:\s*"(\d{11,})"', re.IGNORECASE)

_JS_SCROLL_SNAP = """
() => {
  const e = document.scrollingElement || document.documentElement;
  const b = document.body;
  const sh = Math.max(e.scrollHeight, b ? b.scrollHeight : 0);
  const st = e.scrollTop;
  const ch = e.clientHeight || window.innerHeight || 0;
  return {
    st,
    sh,
    ch,
    atBottom: st + ch >= sh - 120,
  };
}
"""

_JS_SCROLL_DEEP = """
() => {
  const doc = document.scrollingElement || document.documentElement;
  const divs = Array.from(document.querySelectorAll("div"));
  const cands = divs.filter(el => {
    try {
      return (el.scrollHeight || 0) > ((el.clientHeight || 0) + 120);
    } catch (_e) {
      return false;
    }
  });
  cands.sort((a, b) => (b.scrollHeight - a.scrollHeight));
  const target = cands[0] || doc;
  const step = Math.max(700, Math.floor((target.clientHeight || window.innerHeight || 800) * 0.95));
  const before = target.scrollTop || 0;
  target.scrollBy(0, step);
  const after = target.scrollTop || 0;
  return {
    before,
    after,
    moved: Math.abs(after - before) > 8,
    sh: target.scrollHeight || 0,
    ch: target.clientHeight || 0,
  };
}
"""

# Lưới Reels trên profile thường là hàng ngang (virtualized); cần cuộn scrollLeft, không chỉ cuộn dọc document.
_JS_COLLECT_HORIZ_REEL_RAILS = """
() => {
  const rails = [];
  const seen = new Set();
  const pushRail = (el) => {
    if (!el || seen.has(el)) return;
    try {
      const st = window.getComputedStyle(el);
      const ox = st.overflowX;
      const sw = el.scrollWidth || 0;
      const cw = el.clientWidth || 0;
      if ((ox === "auto" || ox === "scroll" || ox === "overlay" || ox === "hidden") && sw > cw + 4) {
        seen.add(el);
        rails.push(el);
      }
    } catch (_e) {}
  };
  for (const a of document.querySelectorAll('a[href*="/reel/"]')) {
    let el = a.parentElement;
    let d = 0;
    while (el && d++ < 42) {
      pushRail(el);
      const st = window.getComputedStyle(el);
      const ox = st.overflowX;
      const sw = el.scrollWidth || 0;
      const cw = el.clientWidth || 0;
      if ((ox === "auto" || ox === "scroll" || ox === "overlay" || ox === "hidden") && sw > cw + 4) {
        break;
      }
      el = el.parentElement;
    }
  }
  for (const el of document.querySelectorAll("div")) {
    if (seen.has(el)) continue;
    try {
      const st = window.getComputedStyle(el);
      if (st.overflowX !== "auto" && st.overflowX !== "scroll") continue;
      const sw = el.scrollWidth || 0;
      const cw = el.clientWidth || 0;
      if (sw <= cw + 30) continue;
      if (!el.querySelector('a[href*="/reel/"]')) continue;
      pushRail(el);
    } catch (_e) {}
  }
  return rails;
}
"""

_JS_SCROLL_HORIZ_REEL_RAILS = """
() => {
  const collect = %s;
  const rails = collect();
  let moved = false;
  let maxDelta = 0;
  for (const el of rails) {
    try {
      const before = el.scrollLeft || 0;
      const maxL = Math.max(0, (el.scrollWidth || 0) - (el.clientWidth || 0));
      const cw = el.clientWidth || 800;
      const step = Math.max(480, Math.floor(cw * 0.94));
      const next = Math.min(maxL, before + step);
      el.scrollLeft = next;
      const after = el.scrollLeft || 0;
      const d = Math.abs(after - before);
      if (d > 2) moved = true;
      if (d > maxDelta) maxDelta = d;
      if (after >= maxL - 6) {
        el.scrollLeft = maxL;
      }
    } catch (_e) {}
  }
  return { railCount: rails.length, moved, maxDelta };
}
""" % (
    _JS_COLLECT_HORIZ_REEL_RAILS.strip().replace("\n", " "),
)

_JS_SNAP_HORIZ_REEL_RAILS_END = """
() => {
  const collect = %s;
  const rails = collect();
  let snapped = 0;
  for (const el of rails) {
    try {
      const maxL = Math.max(0, (el.scrollWidth || 0) - (el.clientWidth || 0));
      const before = el.scrollLeft || 0;
      el.scrollLeft = maxL;
      if (Math.abs((el.scrollLeft || 0) - before) > 2 || maxL > 0) snapped += 1;
    } catch (_e) {}
  }
  return { railCount: rails.length, snapped };
}
""" % (
    _JS_COLLECT_HORIZ_REEL_RAILS.strip().replace("\n", " "),
)


def _noop_status(_: str) -> None:
    pass


def _env_headless_default() -> bool:
    raw = str(os.environ.get("TOOLFB_FB_REELS_HEADLESS", "1")).strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _env_scroll_pause(default: float) -> float:
    raw = str(os.environ.get("TOOLFB_FB_REELS_SCROLL_PAUSE", "")).strip()
    if not raw:
        return default
    try:
        return max(0.5, min(4.0, float(raw)))
    except ValueError:
        return default


_JS_LOADING_NEAR_REEL_GRID = """
() => {
  const loaders = document.querySelectorAll('[data-visualcompletion="loading-state"]');
  let n = 0;
  outer: for (const el of loaders) {
    let p = el;
    for (let d = 0; d < 28 && p; d++) {
      try {
        if (p.querySelector && p.querySelector('a[href*="/reel/"]')) {
          n++;
          continue outer;
        }
      } catch (_e) {}
      p = p.parentElement;
    }
  }
  return n;
}
"""


def _fb_reels_loading_placeholder_count(page: Any) -> int:
    """Skeleton gần lưới Reels (tránh đếm toàn bộ trang FB → chờ vô hạn)."""
    try:
        return int(page.evaluate(_JS_LOADING_NEAR_REEL_GRID))
    except Exception:
        try:
            return int(page.locator('[data-visualcompletion="loading-state"]').count())
        except Exception:
            return 0


def _fb_reels_horiz_rails_end(page: Any) -> tuple[int, int]:
    """
    Trả về ``(rail_count, at_end_count)`` của các rail ngang chứa reel.
    Dùng để dừng sớm khi rail đã chạm cuối và không còn reel mới.
    """
    js = (
        "() => {"
        " const collect = "
        + _JS_COLLECT_HORIZ_REEL_RAILS.strip().replace("\n", " ")
        + "; const rails = collect();"
        " let end = 0;"
        " for (const el of rails) {"
        "   try {"
        "     const maxL = Math.max(0, (el.scrollWidth || 0) - (el.clientWidth || 0));"
        "     const cur = el.scrollLeft || 0;"
        "     if (maxL <= 6 || cur >= maxL - 6) end += 1;"
        "   } catch (_e) {}"
        " }"
        " return { railCount: rails.length, atEnd: end };"
        "}"
    )
    try:
        got = page.evaluate(js) or {}
        return int(got.get("railCount") or 0), int(got.get("atEnd") or 0)
    except Exception:
        return 0, 0


def _wait_fb_reels_loading_placeholders(page: Any, *, max_wait: float) -> None:
    """Chờ ngắn: Facebook public thường giữ skeleton; không được kẹt chờ hết mỗi vòng."""
    if max_wait <= 0:
        return
    deadline = time.monotonic() + max_wait
    quiet_twice = 0
    prev = -1
    while time.monotonic() < deadline:
        try:
            n = _fb_reels_loading_placeholder_count(page)
        except Exception:
            return
        if prev >= 0 and n < prev:
            time.sleep(0.2)
            return
        prev = n
        if n == 0:
            quiet_twice += 1
            if quiet_twice >= 2:
                return
            time.sleep(0.18)
        else:
            quiet_twice = 0
            time.sleep(0.28)


def _scroll_fb_profile_reel_grid(page: Any, *, snap_horiz_to_end: bool = False) -> None:
    """Cuộn hàng Reels (ngang + đưa ô cuối vào viewport) để lazy-load thêm tile.

    ``snap_horiz_to_end``: chỉ thỉnh thoảng bật — snap mỗi vòng dễ nhảy quá vùng lazy-load,
    Facebook không kịp render thêm tile.
    """
    try:
        page.evaluate(_JS_SCROLL_HORIZ_REEL_RAILS)
    except Exception as exc:
        logger.debug("reel grid horiz step: {}", exc)
    if snap_horiz_to_end:
        try:
            page.evaluate(_JS_SNAP_HORIZ_REEL_RAILS_END)
        except Exception as exc:
            logger.debug("reel grid horiz snap: {}", exc)
    try:
        page.locator('a[href*="/reel/"]').last.scroll_into_view_if_needed(timeout=5000)
    except Exception:
        pass
    try:
        for _ in range(22):
            page.keyboard.press("ArrowRight")
            time.sleep(0.028)
    except Exception:
        pass
    try:
        page.mouse.wheel(2200, 0)
    except Exception:
        pass
    try:
        page.keyboard.down("Shift")
        page.mouse.wheel(0, 1600)
        page.keyboard.up("Shift")
    except Exception:
        pass


def normalize_facebook_reels_tab_url(url: str) -> str:
    """Chuẩn hoá về tab Reels: ``…/user/reels/`` (thêm ``/reels`` nếu chỉ có profile)."""
    u = (url or "").strip()
    if not u:
        return u
    u = u.rstrip("/")
    low = u.lower()
    if "facebook.com" not in low:
        return url.strip()
    if re.search(r"facebook\.com/[^/]+/reels", low):
        return u + "/"
    m = re.match(r"https?://(?:[\w-]+\.)?facebook\.com/([^/?#]+)/?(?:[?#].*)?$", u, re.I)
    if m:
        seg = m.group(1).lower()
        reserved = {
            "watch",
            "groups",
            "events",
            "pages",
            "reel",
            "share",
            "stories",
            "ads",
            "marketplace",
            "gaming",
            "login",
            "profile.php",
        }
        if seg not in reserved:
            return u + "/reels/"
    return url.strip()


def is_facebook_reels_tab_url(url: str) -> bool:
    low = (url or "").strip().lower()
    if "facebook.com" not in low:
        return False
    if re.search(r"facebook\.com/[^/]+/reels", low):
        return True
    if re.match(
        r"https?://(?:[\w-]+\.)?facebook\.com/[^/?#]+/?(?:[?#].*)?$",
        (url or "").strip(),
        re.I,
    ):
        return True
    return False


def _extract_reel_ids_from_text(text: str) -> list[str]:
    if not text:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def _add_many(ids: list[str]) -> None:
        for rid in ids:
            if rid in seen or len(rid) < 8:
                continue
            seen.add(rid)
            out.append(rid)

    _add_many(_REEL_ID_RE.findall(text))
    _add_many(_FB_WATCH_V_RE.findall(text))
    _add_many(_FB_PLAYABLE_ID_RE.findall(text))
    return out


def _extract_reel_ids_from_hrefs(hrefs: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for href in hrefs or []:
        s = str(href or "").strip()
        if not s:
            continue
        m = _REEL_ID_RE.search(s)
        if not m:
            continue
        rid = str(m.group(1) or "").strip()
        if not rid or rid in seen:
            continue
        seen.add(rid)
        out.append(rid)
    return out


def _extract_video_ids_from_hrefs(hrefs: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for href in hrefs or []:
        s = str(href or "").strip()
        if not s:
            continue
        m = _VIDEOS_ID_RE.search(s)
        if not m:
            continue
        vid = str(m.group(1) or "").strip()
        if not vid or vid in seen:
            continue
        seen.add(vid)
        out.append(vid)
    return out


def _profile_videos_tab_url(page_url: str) -> str:
    u = (page_url or "").strip()
    m = re.match(r"^(https?://(?:[\w-]+\.)?facebook\.com/[^/?#]+)", u, re.I)
    if m:
        return m.group(1).rstrip("/") + "/videos/"
    m2 = re.match(r"^(https?://(?:[\w-]+\.)?facebook\.com/profile\.php\?id=\d+)", u, re.I)
    if m2:
        return m2.group(1) + "&sk=videos"
    return "https://www.facebook.com/"


def scan_facebook_profile_reels_page(
    *,
    page_url: str,
    max_reels: int = 200,
    max_scroll_rounds: int = 100,
    max_scan_minutes: float = 30.0,
    scroll_until_end: bool = True,
    scroll_pause_sec: float = 1.65,
    headless: bool | None = None,
    account_id: str = "",
    status: StatusFn | None = None,
    on_partial: Optional[Callable[[list[str]], None]] = None,
) -> dict[str, Any]:
    """
    Mở tab Reels, cuộn tải thêm nội dung, trả về danh sách URL ``https://www.facebook.com/reel/<id>``.

    Returns:
        ``{ "ok": bool, "items": [{"video_id", "url"}], "message": str }``
    """
    st = status or _noop_status
    url = normalize_facebook_reels_tab_url(page_url.strip())
    if "facebook.com" not in url.lower():
        return {"ok": False, "items": [], "message": "URL không phải Facebook."}

    hl = _env_headless_default() if headless is None else bool(headless)
    scroll_pause_sec = _env_scroll_pause(scroll_pause_sec)
    max_scan_minutes = max(1.0, min(180.0, float(max_scan_minutes or 30.0)))
    started_at = time.monotonic()
    hard_deadline = started_at + (max_scan_minutes * 60.0)
    if scroll_until_end:
        max_scroll_rounds = max(220, int(max_scroll_rounds))

    ordered_ids: list[str] = []
    seen: set[str] = set()

    def _add_from_text(blob: str) -> None:
        for rid in _extract_reel_ids_from_text(blob):
            if rid in seen:
                continue
            if len(seen) >= max_reels:
                return
            seen.add(rid)
            ordered_ids.append(rid)
            _emit_partial()

    def _emit_partial() -> None:
        if not on_partial:
            return
        urls = [f"https://www.facebook.com/reel/{rid}" for rid in ordered_ids]
        try:
            on_partial(urls)
        except Exception as exc:  # noqa: BLE001
            logger.debug("on_partial: {}", exc)

    def _is_login_or_checkpoint(url_now: str, body_text: str) -> bool:
        u = str(url_now or "").lower()
        if any(x in u for x in ("/login", "checkpoint", "recover")):
            return True
        b = str(body_text or "").lower()
        marks = (
            "log in to continue",
            "đăng nhập để tiếp tục",
            "you must log in",
            "security check",
            "kiểm tra bảo mật",
            "checkpoint",
        )
        return any(x in b for x in marks)

    def _run_scan_on_page(page: Any) -> dict[str, Any] | None:
        st("Đang tải trang Reels (có thể 30–90s)…")
        page.goto(url, wait_until="load", timeout=120_000)
        time.sleep(min(2.2, scroll_pause_sec + 0.5))
        try:
            body_text = page.inner_text("body", timeout=3000)
        except Exception:
            body_text = ""
        if _is_login_or_checkpoint(page.url, body_text):
            return {
                "ok": False,
                "items": [],
                "message": (
                    "Facebook hiển thị đăng nhập / kiểm tra bảo mật — không quét được Reels công khai trên URL này."
                ),
            }
        try:
            hrefs = page.eval_on_selector_all("a[href]", "els => els.map(a => a.getAttribute('href') || a.href || '')")
        except Exception:
            hrefs = []
        for rid in _extract_reel_ids_from_hrefs([str(h) for h in hrefs]):
            if rid in seen:
                continue
            if len(seen) >= max_reels:
                break
            seen.add(rid)
            ordered_ids.append(rid)
            _emit_partial()
        _add_from_text(page.content())

        try:
            _scroll_fb_profile_reel_grid(page, snap_horiz_to_end=True)
            time.sleep(min(1.05, scroll_pause_sec * 0.5))
            hrefs_w = page.eval_on_selector_all(
                "a[href]",
                "els => els.map(a => a.getAttribute('href') || a.href || '')",
            )
        except Exception:
            hrefs_w = []
        for rid in _extract_reel_ids_from_hrefs([str(h) for h in hrefs_w]):
            if rid in seen:
                continue
            if len(seen) >= max_reels:
                break
            seen.add(rid)
            ordered_ids.append(rid)
            _emit_partial()
        _add_from_text(page.content())

        stable = 0
        prev_count = 0
        prev_scroll_h = 0
        idle_at_bottom = 0
        reached_end = False
        for i in range(max_scroll_rounds):
            if time.monotonic() >= hard_deadline:
                st(f"Đã chạm giới hạn thời gian quét ({max_scan_minutes:.0f} phút) — dừng.")
                break
            if len(ordered_ids) >= max_reels:
                break
            elapsed = int(max(0.0, time.monotonic() - started_at))
            before = page.evaluate(_JS_SCROLL_SNAP)
            if prev_scroll_h == 0:
                prev_scroll_h = int(before.get("sh") or 0)
            ph0 = _fb_reels_loading_placeholder_count(page)
            st(
                f"Đang cuộn {i + 1}/{max_scroll_rounds} — "
                f"{len(ordered_ids)} reel — cao trang ~{before.get('sh', 0)}px — "
                f"đang tải ~{ph0} — {elapsed}s…"
            )
            try:
                _scroll_fb_profile_reel_grid(
                    page,
                    snap_horiz_to_end=(i % 8 == 7),
                )
                try:
                    page.get_by_role("button", name=re.compile(r"(xem thêm|see more|show more)", re.I)).first.click(
                        timeout=800
                    )
                except Exception:
                    pass
                if i % 6 == 5:
                    page.evaluate(
                        "() => { const e=document.scrollingElement||document.documentElement;"
                        " e.scrollTop = e.scrollHeight; }"
                    )
                elif i % 4 == 3:
                    page.keyboard.press("End")
                    time.sleep(0.25)
                else:
                    page.evaluate(
                        "() => { const e=document.scrollingElement||document.documentElement;"
                        " e.scrollBy(0, Math.floor((window.innerHeight||800)*0.98)); }"
                    )
                try:
                    page.evaluate(_JS_SCROLL_DEEP)
                except Exception:
                    pass
                try:
                    page.mouse.wheel(0, 2800)
                except Exception:
                    pass
                try:
                    page.keyboard.press("PageDown")
                except Exception:
                    pass
            except Exception as exc:  # noqa: BLE001
                logger.warning("scroll: {}", exc)
            time.sleep(scroll_pause_sec)
            try:
                page.wait_for_timeout(450)
            except Exception:
                time.sleep(0.45)
            ph_n = _fb_reels_loading_placeholder_count(page)
            ph_wait = 1.35 if len(ordered_ids) == 0 else min(2.4, scroll_pause_sec + 0.6)
            if ph_n > 0:
                _wait_fb_reels_loading_placeholders(page, max_wait=ph_wait)
            try:
                body_text = page.inner_text("body", timeout=1500)
            except Exception:
                body_text = ""
            if _is_login_or_checkpoint(page.url, body_text):
                st("Phát hiện login/checkpoint — dừng quét.")
                return {
                    "ok": False,
                    "items": [],
                    "message": (
                        "Facebook hiển thị đăng nhập / kiểm tra bảo mật — không quét được Reels công khai trên URL này."
                    ),
                }
            try:
                hrefs = page.eval_on_selector_all(
                    "a[href]",
                    "els => els.map(a => a.getAttribute('href') || a.href || '')",
                )
            except Exception:
                hrefs = []
            for rid in _extract_reel_ids_from_hrefs([str(h) for h in hrefs]):
                if rid in seen:
                    continue
                if len(seen) >= max_reels:
                    break
                seen.add(rid)
                ordered_ids.append(rid)
                _emit_partial()
            _add_from_text(page.content())
            after = page.evaluate(_JS_SCROLL_SNAP)
            sh = int(after.get("sh") or 0)
            ch = int(after.get("ch") or 0)
            meaningful_doc_scroll = sh > ch + 350
            if sh > prev_scroll_h + 50:
                stable = 0
                idle_at_bottom = 0
            prev_scroll_h = sh
            if bool(after.get("atBottom")) and meaningful_doc_scroll:
                idle_at_bottom += 1
            else:
                idle_at_bottom = 0

            if len(ordered_ids) == prev_count:
                ph_still = _fb_reels_loading_placeholder_count(page)
                if ph_still == 0:
                    stable += 1
                elif not scroll_until_end:
                    stable += 1
                if stable >= 40 and not scroll_until_end:
                    st("Không còn reel mới sau nhiều lần cuộn — dừng.")
                    reached_end = True
                    break
                if (
                    scroll_until_end
                    and meaningful_doc_scroll
                    and i > 55
                    and stable >= 36
                    and idle_at_bottom >= 14
                ):
                    st("Đã ở cuối trang, không thêm reel — dừng.")
                    reached_end = True
                    break
                rail_count, rail_end = _fb_reels_horiz_rails_end(page)
                if (
                    scroll_until_end
                    and not meaningful_doc_scroll
                    and i > 34
                    and stable >= 24
                    and ph_still == 0
                    and rail_count > 0
                    and rail_end >= rail_count
                ):
                    st("Đã cuộn tới cuối dải reel ngang và không có reel mới — dừng.")
                    reached_end = True
                    break
                if (
                    not meaningful_doc_scroll
                    and i > 120
                    and stable >= 85
                    and ph_still == 0
                ):
                    st(
                        "Không thêm reel sau nhiều lần cuộn (lưới ngang). "
                        "Có thể Facebook chỉ hiển thị một phần khi chưa đăng nhập — dừng."
                    )
                    reached_end = True
                    break
            else:
                stable = 0
            prev_count = len(ordered_ids)

            if len(ordered_ids) == 0 and i >= 28:
                st(
                    "Đã cuộn 29 lần mà vẫn không thấy link /reel/ — dừng. "
                    "Thử TOOLFB_FB_REELS_HEADLESS=0 để xem trang, hoặc URL / kênh khác."
                )
                reached_end = True
                break

        if len(ordered_ids) < max_reels and not reached_end:
            try:
                vurl = _profile_videos_tab_url(url)
                st("Đang quét bổ sung tab Videos public…")
                page.goto(vurl, wait_until="load", timeout=90_000)
                time.sleep(min(1.8, scroll_pause_sec + 0.2))
                for _j in range(min(max_scroll_rounds, 140)):
                    if len(ordered_ids) >= max_reels:
                        break
                    try:
                        hrefs2 = page.eval_on_selector_all(
                            "a[href]",
                            "els => els.map(a => a.getAttribute('href') || a.href || '')",
                        )
                    except Exception:
                        hrefs2 = []
                    for vid in _extract_video_ids_from_hrefs([str(h) for h in hrefs2]):
                        if vid in seen:
                            continue
                        seen.add(vid)
                        ordered_ids.append(vid)
                        _emit_partial()
                    try:
                        page.evaluate(_JS_SCROLL_DEEP)
                    except Exception:
                        pass
                    try:
                        page.mouse.wheel(0, 2600)
                    except Exception:
                        pass
                    time.sleep(scroll_pause_sec)
            except Exception as exc:  # noqa: BLE001
                logger.debug("videos fallback scan: {}", exc)
        return None

    aid = str(account_id or "").strip()
    try:
        if aid:
            factory: BrowserFactory | None = None
            context = None
            try:
                st(f"Đang mở profile đã login của tài khoản {aid}…")
                factory = BrowserFactory(headless=hl)
                context = factory.get_browser_context(aid, headless=hl)
                page = context.pages[0] if context.pages else context.new_page()
                try:
                    stealth = Stealth()
                    stealth.apply_stealth_sync(page)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("stealth.apply_stealth_sync(account): {}", exc)
                early = _run_scan_on_page(page)
                if early is not None:
                    return early
            finally:
                sync_close_persistent_context(context, log_label=f"fb_reels_scan:{aid}")
                if factory is not None:
                    factory.close()
        else:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=hl)
                try:
                    context = browser.new_context(
                        viewport={"width": 1400, "height": 960},
                        locale="vi-VN",
                    )
                    try:
                        page = context.new_page()
                        try:
                            stealth = Stealth()
                            stealth.apply_stealth_sync(page)
                        except Exception as exc:  # noqa: BLE001
                            logger.debug("stealth.apply_stealth_sync: {}", exc)
                        early = _run_scan_on_page(page)
                        if early is not None:
                            return early
                    finally:
                        context.close()
                finally:
                    browser.close()
    except Exception as exc:  # noqa: BLE001
        logger.exception("scan_facebook_profile_reels_page")
        return {"ok": False, "items": [], "message": str(exc)}

    elapsed_sec = int(max(0.0, time.monotonic() - started_at))
    items = [{"video_id": rid, "url": f"https://www.facebook.com/reel/{rid}"} for rid in ordered_ids]
    if not items:
        return {
            "ok": False,
            "items": [],
            "message": (
                "Không trích được link reel nào. Thử: (1) TOOLFB_FB_REELS_HEADLESS=0 để xem trình duyệt, "
                "(2) kiểm tra URL tab Reels / profile có Reels công khai."
            ),
        }
    return {
        "ok": True,
        "items": items,
        "message": f"Thu được {len(items)} reel sau {elapsed_sec}s (giới hạn {max_reels}).",
    }
