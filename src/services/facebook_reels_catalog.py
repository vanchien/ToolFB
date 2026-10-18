"""
Thu thập danh sách link ``/reel/<id>`` trên tab Reels / Videos của Page Facebook.

Dùng ``launch_persistent_context`` với ``portable_path`` của tài khoản đã login
(cuộn tab Reels/Videos bằng session thật). yt-dlp không hỗ trợ URL ``…/username/reels/``.
"""

from __future__ import annotations

import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Optional

from loguru import logger
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

StatusFn = Callable[[str], None]

_REEL_ID_RE = re.compile(r"/reel/(\d{8,})", re.IGNORECASE)
_VIDEOS_ID_RE = re.compile(r"/videos/(?:[^/?#]+/)?(\d{8,})", re.IGNORECASE)
# Facebook nhúng trong JSON / query (DOM đôi khi không có href đầy đủ)
_FB_WATCH_V_RE = re.compile(
    r"(?:facebook\.com|fb\.watch)[^\"'\s<>]*[?&]v=(\d{10,})",
    re.IGNORECASE,
)
_FB_PLAYABLE_ID_RE = re.compile(r'"playable_id"\s*:\s*"(\d{11,})"', re.IGNORECASE)

# Ưu tiên vùng [role=main] / feed cuộn được — Facebook thường không cuộn document.
_JS_PICK_SCROLL_EL = """
  const doc = document.scrollingElement || document.documentElement;
  const prefer = [];
  const main = document.querySelector('[role="main"]');
  if (main) prefer.push(main);
  document.querySelectorAll('[role="main"] div, [data-pagelet]').forEach((el) => prefer.push(el));
  const scored = [];
  const seen = new Set();
  for (const el of prefer) {
    if (!el || seen.has(el)) continue;
    seen.add(el);
    try {
      const sh = el.scrollHeight || 0;
      const ch = el.clientHeight || 0;
      if (sh > ch + 120) scored.push([el, sh - ch]);
    } catch (_e) {}
  }
  if (!scored.length) {
    for (const el of Array.from(document.querySelectorAll("div"))) {
      try {
        const sh = el.scrollHeight || 0;
        const ch = el.clientHeight || 0;
        if (sh > ch + 120) scored.push([el, sh - ch]);
      } catch (_e) {}
    }
  }
  scored.sort((a, b) => b[1] - a[1]);
  const target = (scored[0] && scored[0][0]) || doc;
"""

_JS_SCROLL_SNAP = (
    "() => {\n"
    + _JS_PICK_SCROLL_EL
    + """
  const e = document.scrollingElement || document.documentElement;
  const b = document.body;
  const sh = Math.max(target.scrollHeight || 0, e.scrollHeight || 0, b ? b.scrollHeight : 0);
  const st = Math.max(target.scrollTop || 0, e.scrollTop || 0);
  const ch = target.clientHeight || e.clientHeight || window.innerHeight || 0;
  return { st, sh, ch, atBottom: st + ch >= sh - 120 };
}
"""
)

_JS_SCROLL_DEEP = (
    "() => {\n"
    + _JS_PICK_SCROLL_EL
    + """
  const step = Math.max(700, Math.floor((target.clientHeight || window.innerHeight || 800) * 0.95));
  const before = target.scrollTop || 0;
  try { target.scrollBy(0, step); } catch (_e) { doc.scrollBy(0, step); }
  try { window.scrollBy(0, Math.floor((window.innerHeight || 800) * 0.85)); } catch (_e2) {}
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
        return max(0.35, min(4.0, float(raw)))
    except ValueError:
        return default


def _profile_lock_markers(browser_type: str) -> tuple[str, ...]:
    bt = str(browser_type or "").strip().lower()
    if bt == "firefox":
        return ("parent.lock", "lock")
    return ("SingletonLock", "SingletonCookie", "SingletonSocket", "LOCK")


def _is_profile_locked(profile_dir: Path, browser_type: str) -> bool:
    for name in _profile_lock_markers(browser_type):
        if (profile_dir / name).exists():
            return True
    return False


# File/thư mục không cần cho session login — copy chúng lúc Firefox đang mở sẽ WinError 32
# (parent.lock) và làm chậm mở app vì cache2 có hàng nghìn file.
_CLONE_SKIP_NAMES = frozenset(
    {
        "parent.lock",
        "lock",
        ".parentlock",
        "singletonlock",
        "singletoncookie",
        "singletonsocket",
        "cache2",
        "startupcache",
        "shader-cache",
        "thumbnails",
        "datareporting",
        "crashes",
        "minidumps",
        "saved-telemetry-pings",
        "jumpListCache",
        "activity-stream.inferred_personalization_feed.json",
        "activity-stream.weather_feed.json",
    }
)


def _clone_ignore(_directory: str, names: list[str]) -> list[str]:
    """Bỏ lock + cache khi clone profile để quét — tránh lỗi đọc file đang bị Firefox giữ."""
    skip: list[str] = []
    for name in names:
        if str(name).strip().lower() in _CLONE_SKIP_NAMES:
            skip.append(name)
    return skip


def _clone_copy2(src: str, dst: str, *, follow_symlinks: bool = True) -> None:
    """copy2 nhưng bỏ qua file đang khóa (WinError 32) thay vì làm hỏng cả lượt clone."""
    try:
        shutil.copy2(src, dst, follow_symlinks=follow_symlinks)
    except OSError as exc:
        logger.debug("Bỏ file profile đang khóa khi clone quét: {} — {}", src, exc)


def _clone_profile_for_scan(src: Path) -> Path:
    """
    Snapshot profile để quét song song với lịch đăng.

    Không copy ``parent.lock`` / ``cache2`` — Firefox đang mở sẽ chặn đọc các file đó.
    """
    tmp = Path(tempfile.mkdtemp(prefix="toolfb_fbscan_profile_"))
    dst = tmp / "profile"
    try:
        shutil.copytree(
            src,
            dst,
            dirs_exist_ok=True,
            ignore=_clone_ignore,
            copy_function=_clone_copy2,
            ignore_dangling_symlinks=True,
        )
    except shutil.Error as exc:
        logger.warning("Clone profile quét thiếu vài file (bỏ file đang khóa): {}", exc)
    return dst


_FB_RESERVED_SEGMENTS = frozenset(
    {
        "watch",
        "groups",
        "events",
        "pages",
        "reel",
        "reels",
        "share",
        "stories",
        "ads",
        "marketplace",
        "gaming",
        "login",
        "profile.php",
        "home.php",
        "notifications",
        "friends",
        "messages",
        "settings",
        "help",
    }
)


def facebook_page_identity(url: str) -> str:
    """
    Khóa nhận diện Page/profile từ URL (vanity hoặc ``id=``).

    Dùng để kiểm tra trình duyệt còn đúng Page nguồn sau ``goto`` / click tab.
    """
    u = (url or "").strip()
    if not u:
        return ""
    low = u.casefold()
    m_id = re.search(r"facebook\.com/profile\.php\?[^#]*\bid=(\d+)", low)
    if m_id:
        return f"id:{m_id.group(1)}"
    m = re.search(r"facebook\.com/([^/?#]+)", low)
    if not m:
        return ""
    seg = m.group(1).strip().casefold()
    if not seg or seg in _FB_RESERVED_SEGMENTS:
        return ""
    return f"vanity:{seg}"


def url_matches_facebook_page(current_url: str, expected_page_url: str) -> bool:
    """True nếu URL hiện tại vẫn thuộc cùng Page với URL nguồn đã chọn."""
    want = facebook_page_identity(expected_page_url)
    got = facebook_page_identity(current_url)
    if not want or not got:
        return False
    return want == got


def normalize_facebook_reels_tab_url(url: str) -> str:
    """Chuẩn hoá về tab Reels: ``…/user/reels/`` (thêm ``/reels`` nếu chỉ có profile)."""
    u = (url or "").strip()
    if not u:
        return u
    u = u.rstrip("/")
    low = u.lower()
    if "facebook.com" not in low:
        return url.strip()
    # profile.php?id=... -> profile.php?id=...&sk=reels_tab
    if re.search(r"facebook\.com/profile\.php\?[^#]*\bid=\d+", low):
        if "sk=reels_tab" not in low:
            sep = "&" if "?" in u else "?"
            return f"{u}{sep}sk=reels_tab"
        return u
    if re.search(r"facebook\.com/[^/]+/reels", low):
        return u + "/"
    m = re.match(r"https?://(?:[\w-]+\.)?facebook\.com/([^/?#]+)/?(?:[?#].*)?$", u, re.I)
    if m:
        seg = m.group(1).lower()
        if seg not in _FB_RESERVED_SEGMENTS:
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


def _collect_main_hrefs(page: Any) -> list[str]:
    """Chỉ lấy ``a[href]`` trong ``[role=main]`` — bỏ sidebar / Reels toàn cục / gợi ý."""
    try:
        hrefs = page.eval_on_selector_all(
            "[role='main'] a[href]",
            "els => els.map(a => a.getAttribute('href') || a.href || '')",
        )
        if hrefs:
            return [str(h) for h in hrefs]
    except Exception:
        pass
    try:
        hrefs = page.eval_on_selector_all(
            "a[href]",
            "els => els.map(a => a.getAttribute('href') || a.href || '')",
        )
        return [str(h) for h in (hrefs or [])]
    except Exception:
        return []


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
    account: dict[str, Any] | None = None,
    cookie_path: str | None = None,
    max_reels: int = 200,
    max_scroll_rounds: int = 100,
    max_scan_minutes: float = 30.0,
    scroll_until_end: bool = True,
    scroll_pause_sec: float = 0.95,
    headless: bool | None = None,
    status: StatusFn | None = None,
    on_partial: Optional[Callable[[list[str]], None]] = None,
    include_html_ids: bool = True,
    scan_videos_tab: bool = True,
) -> dict[str, Any]:
    """
    Mở tab Reels, cuộn tải thêm nội dung, trả về danh sách URL ``https://www.facebook.com/reel/<id>``.

    ``include_html_ids=False``: chỉ lấy ``a[href]`` trên trang (theo dõi chia sẻ — tránh ID
    trong JSON gợi ý / related bị nhận nhầm là video mới của Page).
    ``scan_videos_tab=False``: không sang tab Videos (tránh video cũ ID cao đẩy mốc sai).

    Returns:
        ``{ "ok": bool, "items": [{"video_id", "url"}], "message": str }``
    """
    st = status or _noop_status
    url = normalize_facebook_reels_tab_url(page_url.strip())
    if "facebook.com" not in url.lower():
        return {"ok": False, "items": [], "message": "URL không phải Facebook."}
    if not facebook_page_identity(url) and not facebook_page_identity(page_url):
        return {
            "ok": False,
            "items": [],
            "message": (
                "Link không phải Page nguồn (vd. facebook.com/TenPage). "
                "Không dùng link /reel/… hay trang Reels toàn cục."
            ),
        }

    hl = _env_headless_default() if headless is None else bool(headless)
    scroll_pause_sec = _env_scroll_pause(scroll_pause_sec)
    max_scan_minutes = max(1.0, min(180.0, float(max_scan_minutes or 30.0)))
    started_at = time.monotonic()
    hard_deadline = started_at + (max_scan_minutes * 60.0)
    if scroll_until_end:
        max_scroll_rounds = max(220, int(max_scroll_rounds))
    if cookie_path:
        st("Đang chạy chế độ account-profile, không dùng cookie JSON riêng.")

    ordered_ids: list[str] = []
    seen: set[str] = set()
    ordered_urls: list[str] = []
    seen_urls: set[str] = set()

    def _add_url(u: str) -> None:
        s = str(u or "").strip()
        if not s or s in seen_urls:
            return
        if len(seen_urls) >= max_reels:
            return
        seen_urls.add(s)
        ordered_urls.append(s)

    def _add_from_text(blob: str) -> None:
        for rid in _extract_reel_ids_from_text(blob):
            if rid in seen:
                continue
            if len(seen) >= max_reels:
                return
            seen.add(rid)
            ordered_ids.append(rid)
            _add_url(f"https://www.facebook.com/reel/{rid}")

    def _emit_partial() -> None:
        if not on_partial:
            return
        try:
            on_partial(list(ordered_urls))
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

    try:
        with sync_playwright() as pw:
            browser = None
            context = None
            try:
                acc = account or {}
                profile_path = str(acc.get("portable_path") or acc.get("profile_path") or "").strip()
                browser_type = str(acc.get("browser_type") or "chromium").strip().lower()
                browser_exe = str(acc.get("browser_exe_path") or "").strip()
                temp_profile_root: Path | None = None
                if profile_path:
                    profile_dir = Path(profile_path).resolve()
                    os.makedirs(str(profile_dir), exist_ok=True)
                    run_profile = profile_dir
                    if _is_profile_locked(profile_dir, browser_type):
                        st("Profile tài khoản đang được dùng, tạo profile tạm để quét bằng session hiện có…")
                        run_profile = _clone_profile_for_scan(profile_dir)
                        temp_profile_root = run_profile.parent
                    user_data_dir = str(run_profile)
                    launch_kwargs: dict[str, Any] = {
                        "user_data_dir": user_data_dir,
                        "headless": hl,
                        "viewport": {"width": 1366, "height": 900},
                        "locale": "vi-VN",
                    }
                    if browser_exe and Path(browser_exe).is_file():
                        launch_kwargs["executable_path"] = str(Path(browser_exe).resolve())
                    from src.automation.browser_factory import account_use_proxy_enabled
                    from src.utils.account_proxy_mapper import require_account_proxy_live
                    from src.utils.proxy_check import build_playwright_proxy_settings

                    if account_use_proxy_enabled(acc):
                        require_account_proxy_live(acc)
                        launch_kwargs["proxy"] = build_playwright_proxy_settings(
                            acc.get("proxy") if isinstance(acc.get("proxy"), dict) else {}
                        )
                    if browser_type in ("firefox",):
                        context = pw.firefox.launch_persistent_context(**launch_kwargs)
                    else:
                        context = pw.chromium.launch_persistent_context(**launch_kwargs)
                    st("Đang dùng browser profile của tài khoản đã chọn.")
                else:
                    return {
                        "ok": False,
                        "items": [],
                        "message": "Không có profile của tài khoản đã chọn. Dừng quét (không mở browser mới).",
                    }
                try:
                    existing_pages = [p for p in (context.pages or []) if not p.is_closed()]
                    if existing_pages:
                        page = existing_pages[0]
                    else:
                        # Profile đã login thường vẫn mở được tab mới trong cùng context.
                        st("Profile chưa có tab — mở tab mới trong session đã đăng nhập…")
                        page = context.new_page()
                    try:
                        stealth = Stealth()
                        stealth.apply_stealth_sync(page)
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("stealth.apply_stealth_sync: {}", exc)

                    expected_identity = facebook_page_identity(url) or facebook_page_identity(page_url)
                    st("Đang tải tab Reels bằng tài khoản đã login (có thể 30–90s)…")
                    page.goto(url, wait_until="load", timeout=120_000)
                    time.sleep(min(1.35, scroll_pause_sec + 0.35))
                    # Chờ lưới video/reel hiện — session login thường render chậm hơn public.
                    try:
                        page.wait_for_selector(
                            "a[href*='/reel/'], a[href*='/videos/'], [role='main']",
                            timeout=12_000,
                        )
                    except Exception:
                        pass
                    try:
                        body_text = page.inner_text("body", timeout=3000)
                    except Exception:
                        body_text = ""
                    if _is_login_or_checkpoint(page.url, body_text):
                        try:
                            from src.services.facebook_recaptcha import auto_solve_facebook_recaptcha_if_present

                            if auto_solve_facebook_recaptcha_if_present(
                                page, acc, stage="reels_catalog:init"
                            ):
                                body_text = page.inner_text("body", timeout=3000)
                        except Exception:
                            pass
                    if _is_login_or_checkpoint(page.url, body_text):
                        return {
                            "ok": False,
                            "items": [],
                            "message": (
                                "Session chưa đăng nhập / checkpoint. "
                                "Mở profile tài khoản Page nhận, đăng nhập Facebook rồi quét lại."
                            ),
                        }
                    # Không bấm link «Reels» trên menu trái (facebook.com/reels) — dễ nhảy nhầm trang.
                    # Chỉ mở tab Reels của đúng Page nguồn (href chứa vanity/id của Page).
                    if not url_matches_facebook_page(page.url, url):
                        st(f"URL lệch Page nguồn ({page.url}) — điều hướng lại…")
                        page.goto(url, wait_until="load", timeout=120_000)
                        time.sleep(min(1.0, scroll_pause_sec + 0.2))
                    if expected_identity and not url_matches_facebook_page(page.url, url):
                        return {
                            "ok": False,
                            "items": [],
                            "message": (
                                f"Không đứng đúng Page nguồn cần quét (đang ở {page.url}). "
                                "Kiểm tra link Page nguồn và session đăng nhập."
                            ),
                        }
                    if "/reels" not in (page.url or "").lower() and "sk=reels" not in (page.url or "").lower():
                        try:
                            from src.utils.dom_input import click_locator_dom

                            vanity = ""
                            if expected_identity.startswith("vanity:"):
                                vanity = expected_identity.split(":", 1)[1]
                            page_id = ""
                            if expected_identity.startswith("id:"):
                                page_id = expected_identity.split(":", 1)[1]
                            tab = None
                            if vanity:
                                tab = page.locator(
                                    f"[role='main'] a[href*='/{vanity}/reels'], "
                                    f"a[href*='/{vanity}/reels']"
                                ).first
                            elif page_id:
                                tab = page.locator(
                                    f"a[href*='profile.php?id={page_id}'][href*='reels'], "
                                    f"a[href*='id={page_id}'][href*='sk=reels']"
                                ).first
                            if tab is not None:
                                click_locator_dom(tab, timeout_ms=2500)
                                time.sleep(0.8)
                                if expected_identity and not url_matches_facebook_page(page.url, url):
                                    st("Click tab Reels lệch trang — quay lại URL Page nguồn.")
                                    page.goto(url, wait_until="load", timeout=90_000)
                                    time.sleep(0.8)
                        except Exception:
                            pass
                    hrefs = _collect_main_hrefs(page)
                    for rid in _extract_reel_ids_from_hrefs([str(h) for h in hrefs]):
                        if rid in seen:
                            continue
                        if len(seen) >= max_reels:
                            break
                        seen.add(rid)
                        ordered_ids.append(rid)
                        _add_url(f"https://www.facebook.com/reel/{rid}")
                    if include_html_ids:
                        _add_from_text(page.content())
                    _emit_partial()

                    stable = 0
                    prev_count = 0
                    prev_scroll_h = 0
                    idle_at_bottom = 0
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
                        st(
                            f"Đang cuộn {i + 1}/{max_scroll_rounds} — "
                            f"{len(ordered_ids)} reel — cao trang ~{before.get('sh', 0)}px — {elapsed}s…"
                        )
                        try:
                            # Public page đôi khi có nút tải thêm nội dung.
                            try:
                                from src.utils.dom_input import click_locator_dom

                                click_locator_dom(
                                    page.get_by_role(
                                        "button",
                                        name=re.compile(r"(xem thêm|see more|show more)", re.I),
                                    ).first,
                                    timeout_ms=800,
                                )
                            except Exception:
                                pass
                            if i % 6 == 5:
                                page.evaluate(
                                    "() => { const e=document.scrollingElement||document.documentElement;"
                                    " e.scrollTop = e.scrollHeight; }"
                                )
                            else:
                                page.evaluate(
                                    "() => { const e=document.scrollingElement||document.documentElement;"
                                    " e.scrollBy(0, Math.floor((window.innerHeight||800)*0.98)); }"
                                )
                            try:
                                page.evaluate(_JS_SCROLL_DEEP)
                            except Exception:
                                pass
                        except Exception as exc:  # noqa: BLE001
                            logger.warning("scroll: {}", exc)
                        time.sleep(scroll_pause_sec)
                        try:
                            page.wait_for_timeout(260)
                        except Exception:
                            time.sleep(0.26)
                        try:
                            body_text = page.inner_text("body", timeout=1500)
                        except Exception:
                            body_text = ""
                        if _is_login_or_checkpoint(page.url, body_text):
                            try:
                                from src.services.facebook_recaptcha import auto_solve_facebook_recaptcha_if_present

                                if auto_solve_facebook_recaptcha_if_present(
                                    page, acc, stage="reels_catalog:scroll"
                                ):
                                    body_text = page.inner_text("body", timeout=1500)
                            except Exception:
                                pass
                        if _is_login_or_checkpoint(page.url, body_text):
                            st("Phát hiện login/checkpoint/reCAPTCHA — dừng quét.")
                            return {
                                "ok": False,
                                "items": [],
                                "message": "Không thể quét tiếp nếu không dùng cookies/session.",
                            }
                        if expected_identity and not url_matches_facebook_page(page.url, url):
                            st("Đã lệch khỏi Page nguồn khi cuộn — dừng để tránh lấy nhầm trang.")
                            break
                        hrefs = _collect_main_hrefs(page)
                        for rid in _extract_reel_ids_from_hrefs([str(h) for h in hrefs]):
                            if rid in seen:
                                continue
                            if len(seen) >= max_reels:
                                break
                            seen.add(rid)
                            ordered_ids.append(rid)
                            _add_url(f"https://www.facebook.com/reel/{rid}")
                        if include_html_ids:
                            _add_from_text(page.content())
                        _emit_partial()
                        after = page.evaluate(_JS_SCROLL_SNAP)
                        sh = int(after.get("sh") or 0)
                        if sh > prev_scroll_h + 50:
                            stable = 0
                            idle_at_bottom = 0
                        prev_scroll_h = sh
                        if bool(after.get("atBottom")):
                            idle_at_bottom += 1
                        else:
                            idle_at_bottom = 0

                        if len(ordered_ids) == prev_count:
                            stable += 1
                            if stable >= 22 and not scroll_until_end:
                                st("Không còn reel mới sau nhiều lần cuộn — dừng.")
                                break
                            # Ít reel trên trang: dừng sớm khi đã ở đáy và không tăng count (tránh chờ >40 vòng).
                            n_found = len(ordered_ids)
                            if n_found <= 25 and i >= 10 and stable >= 7 and idle_at_bottom >= 5:
                                st("Đã thu đủ reel hiển thị (ít mục) — dừng quét.")
                                break
                            # Nhiều reel / cuộn tới hết: giữ ngưỡng cao hơn để tránh dừng nhầm giữa chừng.
                            if i > 45 and stable >= 24 and idle_at_bottom >= 12:
                                st("Đã ở cuối trang, không thêm reel — dừng.")
                                break
                        else:
                            stable = 0
                        prev_count = len(ordered_ids)

                    # Tab Videos: chỉ khi thu thập catalog đầy đủ — theo dõi chia sẻ tắt để tránh video cũ.
                    if scan_videos_tab and len(ordered_ids) < max_reels:
                        try:
                            vurl = _profile_videos_tab_url(url)
                            st("Đang quét bổ sung tab Videos (cùng tài khoản đã login)…")
                            page.goto(vurl, wait_until="load", timeout=90_000)
                            time.sleep(min(1.15, scroll_pause_sec + 0.15))
                            try:
                                page.wait_for_selector(
                                    "a[href*='/videos/'], a[href*='/reel/'], [role='main']",
                                    timeout=10_000,
                                )
                            except Exception:
                                pass
                            if expected_identity and not url_matches_facebook_page(page.url, url):
                                st("Tab Videos lệch Page nguồn — bỏ qua bước Videos.")
                                raise RuntimeError("videos_tab_wrong_page")
                            video_rounds = min(max(6, max_scroll_rounds), 80 if scroll_until_end else 12)
                            for j in range(video_rounds):
                                if len(ordered_ids) >= max_reels:
                                    break
                                hrefs2 = _collect_main_hrefs(page)
                                for vid in _extract_video_ids_from_hrefs([str(h) for h in hrefs2]):
                                    if vid in seen:
                                        continue
                                    seen.add(vid)
                                    ordered_ids.append(vid)
                                    _add_url(f"https://www.facebook.com/reel/{vid}")
                                for rid in _extract_reel_ids_from_hrefs([str(h) for h in hrefs2]):
                                    if rid in seen:
                                        continue
                                    seen.add(rid)
                                    ordered_ids.append(rid)
                                    _add_url(f"https://www.facebook.com/reel/{rid}")
                                if include_html_ids:
                                    _add_from_text(page.content())
                                _emit_partial()
                                try:
                                    page.evaluate(_JS_SCROLL_DEEP)
                                except Exception:
                                    pass
                                time.sleep(scroll_pause_sec)
                                st(
                                    f"Tab Videos — cuộn {j + 1}/{video_rounds} — "
                                    f"đã có {len(ordered_ids)} video/reel…"
                                )
                        except Exception as exc:  # noqa: BLE001
                            logger.debug("videos fallback scan: {}", exc)
                finally:
                    if context:
                        context.close()
                    if temp_profile_root is not None:
                        shutil.rmtree(temp_profile_root, ignore_errors=True)
            finally:
                if browser:
                    browser.close()
    except Exception as exc:  # noqa: BLE001
        logger.exception("scan_facebook_profile_reels_page")
        return {"ok": False, "items": [], "message": str(exc)}

    elapsed_sec = int(max(0.0, time.monotonic() - started_at))
    items = [{"video_id": str(i + 1), "url": u} for i, u in enumerate(ordered_urls)]
    if not items:
        return {
            "ok": False,
            "items": [],
            "message": (
                "Không trích được link reel/video nào. Thử: (1) mở profile tài khoản Page nhận "
                "và đăng nhập Facebook, (2) đặt TOOLFB_FB_REELS_HEADLESS=0 để xem cửa sổ quét, "
                "(3) dán đúng link Page nguồn (có tab Reels/Videos)."
            ),
        }
    return {
        "ok": True,
        "items": items,
        "message": f"Thu được {len(items)} reel sau {elapsed_sec}s (giới hạn {max_reels}).",
    }
