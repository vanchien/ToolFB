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

_REEL_ID_RE = re.compile(r"/reel/(\d{10,17})(?!\d)", re.IGNORECASE)
_VIDEOS_ID_RE = re.compile(r"/videos/(?:[^/?#]+/)?(\d{10,17})(?!\d)", re.IGNORECASE)
# Facebook nhúng trong JSON / query (DOM đôi khi không có href đầy đủ)
_FB_WATCH_V_RE = re.compile(
    r"(?:facebook\.com|fb\.watch)[^\"'\s<>]*[?&]v=(\d{10,17})(?!\d)",
    re.IGNORECASE,
)
_FB_PLAYABLE_ID_RE = re.compile(r'"playable_id"\s*:\s*"(\d{10,17})(?!\d)"', re.IGNORECASE)

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

# Ô video/reel trên lưới Page: ô lớn, sắp theo Y. Bỏ chip gợi ý nhỏ / link trang khác.
_JS_COLLECT_PAGE_VIDEO_TILES = """
(args) => {
  const vanity = String((args && args.vanity) || "").toLowerCase();
  const pageId = String((args && args.pageId) || "");
  const main = document.querySelector("[role='main']") || document.body;
  const rx = /\\/reel\\/(\\d{10,17})(?!\\d)|\\/videos\\/(?:[^/?#]+\\/)?(\\d{10,17})(?!\\d)|[?&]v=(\\d{10,17})(?!\\d)/i;
  const reserved = new Set(["reel", "reels", "watch", "video", "videos", "photo", "photos"]);
  const rows = [];
  const seen = new Set();

  function ownerKind(href) {
    const h = String(href || "");
    try {
      const u = new URL(h, location.origin);
      const parts = u.pathname.split("/").filter(Boolean);
      const host = (u.hostname || "").toLowerCase();
      if (!host.includes("facebook.com") && !host.includes("fb.watch") && h.startsWith("/")) {
        /* relative */
      } else if (host && !host.includes("facebook.com") && !host.includes("fb.watch")) {
        return "other";
      }
      if (!parts.length) {
        if (u.searchParams.get("v")) return "bare";
        return "other";
      }
      const head = String(parts[0] || "").toLowerCase();
      if (head === "reel" || head === "watch" || head === "videos") return "bare";
      if (head === "profile.php") {
        const id = u.searchParams.get("id") || "";
        if (pageId && id === pageId) return "page";
        return id ? "other" : "bare";
      }
      if (reserved.has(head)) return "bare";
      if (vanity && head === vanity) return "page";
      if (pageId && head === pageId) return "page";
      if (/^\\d{8,}$/.test(head) || /^[a-z0-9.]+$/i.test(head)) return "other";
    } catch (_e) {}
    return "bare";
  }

  for (const a of main.querySelectorAll("a[href]")) {
    const href = a.href || a.getAttribute("href") || "";
    if (!href || !rx.test(href) || seen.has(href)) continue;
    const r = a.getBoundingClientRect();
    const w = r.width || 0;
    const hgt = r.height || 0;
    if (w < 72 || hgt < 72) continue;
    const kind = ownerKind(href);
    if (kind === "other") continue;
    seen.add(href);
    rows.push({
      href,
      y: (r.top || 0) + (window.scrollY || 0),
      x: r.left || 0,
      kind,
    });
  }
  // Giữ cả ô «bare» (/reel/id) — tab Reels của Page gần như luôn dùng link trần.
  // Chỉ loại «other» (vanity trang khác). Không ưu tiên owned-only (sẽ mất video mới nhất).
  rows.sort((a, b) => a.y - b.y || a.x - b.x);
  return rows.map((x) => x.href);
}
"""


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


def _wait_profile_unlocked(
    profile_dir: Path,
    browser_type: str,
    *,
    timeout_sec: float = 900.0,
    status: StatusFn | None = None,
    poll_sec: float = 2.0,
) -> bool:
    """
    Chờ profile rảnh — không clone / không mở Firefox thứ 2 cùng tài khoản.

    True nếu hết khóa trước timeout; False nếu hết giờ vẫn bị giữ.
    """
    st = status or _noop_status
    deadline = time.monotonic() + max(5.0, float(timeout_sec))
    noted = False
    while _is_profile_locked(profile_dir, browser_type):
        if time.monotonic() >= deadline:
            return False
        if not noted:
            st(
                "Profile tài khoản đang chia sẻ / mở — chờ xong rồi mới quét "
                "(không mở Firefox thứ 2)…"
            )
            noted = True
        time.sleep(max(0.5, float(poll_sec)))
    return True


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
        "professional_dashboard",
        "latest",
        "business",
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
    if seg.isdigit() and len(seg) >= 8:
        return f"id:{seg}"
    return f"vanity:{seg}"


def url_matches_facebook_page(current_url: str, expected_page_url: str) -> bool:
    """True nếu URL hiện tại vẫn thuộc cùng Page với URL nguồn đã chọn."""
    want = facebook_page_identity(expected_page_url)
    got = facebook_page_identity(current_url)
    if not want or not got:
        return False
    return want == got


def href_belongs_to_source_page(
    href: str,
    expected_page_url: str,
    *,
    allow_bare: bool = True,
) -> bool:
    """
    True nếu link video thuộc Page nguồn.

    ``allow_bare=True``: ``/reel/<id>`` / ``watch?v=`` (không gắn vanity) vẫn giữ.
    ``allow_bare=False``: chỉ href có vanity/id trùng Page — loại reel gợi ý.
    """
    want = facebook_page_identity(expected_page_url)
    got = facebook_page_identity(href)
    if got and want:
        return got == want
    if got:
        return False
    return bool(allow_bare)


def extract_facebook_page_id_from_page(page: Any) -> str:
    """Meta Page ID từ URL hoặc JSON nhúng trên trang đang mở."""
    try:
        ident = facebook_page_identity(str(getattr(page, "url", "") or ""))
        if ident.startswith("id:"):
            return ident.split(":", 1)[1]
    except Exception:
        pass
    try:
        html = str(page.content() or "")
    except Exception:
        html = ""
    if not html:
        return ""
    m = re.search(
        r'"(?:pageID|delegate_page_id|page_id)"\s*:\s*"(\d{8,})"',
        html[:900_000],
        re.I,
    )
    return str(m.group(1) or "") if m else ""


def split_owned_and_bare_video_ids(
    hrefs: list[str],
    expected_page_url: str,
) -> tuple[list[str], list[str]]:
    """Tách ID từ href: gắn đúng Page vs ``/reel/id`` trần (gợi ý)."""
    owned: list[str] = []
    bare: list[str] = []
    seen: set[str] = set()
    for href in hrefs or []:
        ids = _extract_watchable_ids_from_hrefs([href])
        if not ids:
            continue
        vid = ids[0]
        if vid in seen:
            continue
        if href_belongs_to_source_page(href, expected_page_url, allow_bare=False):
            seen.add(vid)
            owned.append(vid)
        elif not facebook_page_identity(href):
            seen.add(vid)
            bare.append(vid)
    return owned, bare


def resolve_newest_page_video_ids(
    *,
    visual_ids: list[str],
    harvest_ids: list[str] | None = None,
    max_ids: int = 24,
) -> list[str]:
    """
    Link reel mới nhất của Page — thứ tự lưới trên → dưới là chuẩn tuyệt đối.

    - Không GraphQL: tin lưới (đã loại vanity trang khác).
    - Có GraphQL: giữ ô lưới đã xác nhận chủ; thêm tối đa 2 ô đầu nằm *trên*
      video đã xác nhận (video vừa publish, GraphQL thường chậm hơn DOM).
    - Không đẩy ID chỉ có trong GraphQL lên đầu (tránh video cũ giả «mới»).
    - Lưới trống: mới dùng GraphQL (đã sắp mới → cũ).
    """
    limit = max(1, int(max_ids or 24))
    visual = [str(x).strip() for x in (visual_ids or []) if _ok_scan_video_id(x)]
    harvest = [str(x).strip() for x in (harvest_ids or []) if _ok_scan_video_id(x)]
    harvest_set = set(harvest)
    out: list[str] = []
    seen: set[str] = set()

    def _push(vid: str) -> bool:
        if not vid or vid in seen:
            return len(out) >= limit
        seen.add(vid)
        out.append(vid)
        return len(out) >= limit

    if not visual:
        for vid in harvest:
            if _push(vid):
                break
        return out

    if not harvest_set:
        for vid in visual:
            if _push(vid):
                break
        return out

    first_confirmed = next((i for i, vid in enumerate(visual) if vid in harvest_set), None)
    for index, vid in enumerate(visual):
        if len(out) >= limit:
            break
        if vid in harvest_set:
            _push(vid)
            continue
        # Video mới trên đầu lưới, GraphQL chưa kịp ghi owner — giữ tối đa 2 ô.
        if first_confirmed is None and index < 2:
            _push(vid)
        elif first_confirmed is not None and index < first_confirmed and index < 2:
            _push(vid)
    return out


def merge_owner_verified_video_ids(
    *,
    harvest_ids: list[str],
    owned_ids: list[str],
    video_tab_ids: list[str] | None = None,
) -> tuple[list[str], list[str]]:
    """
    Gộp lưới Page (mới trên) ∩ GraphQL owner; tab Videos riêng.

    ``owned_ids`` = ID theo thứ tự ô trên lưới (gồm /reel/ trần trên đúng Page).
    """
    reel = resolve_newest_page_video_ids(
        visual_ids=owned_ids,
        harvest_ids=harvest_ids,
        max_ids=max(1, len(owned_ids) + len(harvest_ids) + 4),
    )
    harvest_set = {str(x).strip() for x in (harvest_ids or []) if _ok_scan_video_id(x)}
    video: list[str] = []
    seen = set(reel)
    for vid in video_tab_ids or []:
        s = str(vid or "").strip()
        if not s or s in seen or not _ok_scan_video_id(s):
            continue
        # Có GraphQL: chỉ giữ video đã xác nhận chủ; không thì tin lưới tab Videos.
        if harvest_set and s not in harvest_set:
            continue
        seen.add(s)
        video.append(s)
    return reel, video


def _ok_scan_video_id(raw: object) -> bool:
    """ID video Facebook thật — cùng quy tắc với harvest/source_watch."""
    from src.services.page_video_harvest import is_real_facebook_video_id

    return is_real_facebook_video_id(raw)


def _goto_scan(page: Any, url: str, *, timeout_ms: int = 45_000) -> None:
    """Mở URL quét nhanh (domcontentloaded) — không chờ ``load`` 90–120s."""
    page.goto(str(url), wait_until="domcontentloaded", timeout=int(timeout_ms))


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
            if rid in seen or not _ok_scan_video_id(rid):
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
        if not rid or rid in seen or not _ok_scan_video_id(rid):
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
        if not vid or vid in seen or not _ok_scan_video_id(vid):
            continue
        seen.add(vid)
        out.append(vid)
    return out


def _extract_watchable_ids_from_hrefs(hrefs: list[str]) -> list[str]:
    """ID reel + video + watch?v= theo thứ tự href (đã lọc ID nhiễu)."""
    out: list[str] = []
    seen: set[str] = set()
    for href in hrefs or []:
        s = str(href or "").strip()
        if not s:
            continue
        vid = ""
        for rx in (_REEL_ID_RE, _VIDEOS_ID_RE, _FB_WATCH_V_RE):
            m = rx.search(s)
            if m:
                vid = str(m.group(1) or "").strip()
                break
        if not vid or vid in seen or not _ok_scan_video_id(vid):
            continue
        seen.add(vid)
        out.append(vid)
    return out


def _collect_main_hrefs(page: Any) -> list[str]:
    """Chỉ lấy ``a[href]`` trong ``[role=main]`` — không fallback ra toàn trang (sidebar/gợi ý)."""
    try:
        hrefs = page.eval_on_selector_all(
            "[role='main'] a[href]",
            "els => els.map(a => a.getAttribute('href') || a.href || '')",
        )
        return [str(h) for h in (hrefs or [])]
    except Exception:
        return []


def _collect_visual_page_video_hrefs(page: Any, expected_page_url: str) -> list[str]:
    """Href ô video/reel lớn trên lưới Page, trên → dưới; loại link trang khác."""
    ident = facebook_page_identity(expected_page_url) or facebook_page_identity(
        str(getattr(page, "url", "") or "")
    )
    vanity = ident.split(":", 1)[1] if ident.startswith("vanity:") else ""
    page_id = ident.split(":", 1)[1] if ident.startswith("id:") else ""
    hrefs: list[str] = []
    try:
        raw = page.evaluate(_JS_COLLECT_PAGE_VIDEO_TILES, {"vanity": vanity, "pageId": page_id})
        hrefs = [str(h) for h in (raw or []) if str(h).strip()]
    except Exception as exc:  # noqa: BLE001
        logger.debug("collect visual tiles: {}", exc)
        hrefs = _collect_main_hrefs(page)
    if expected_page_url:
        hrefs = [h for h in hrefs if href_belongs_to_source_page(h, expected_page_url)]
    return hrefs


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
    scan_videos_tab_top: bool = False,
    require_page_owner: bool = False,
    use_graphql_harvest: bool = False,
    newest_only: bool = False,
) -> dict[str, Any]:
    """
    Mở tab Reels, cuộn tải thêm nội dung, trả về danh sách URL ``https://www.facebook.com/reel/<id>``.

    ``include_html_ids=False``: chỉ lấy ô video/reel trên lưới Page (theo dõi chia sẻ —
    tránh ID trong JSON gợi ý / related bị nhận nhầm là video mới).
    ``scan_videos_tab=False``: không sang tab Videos đầy đủ (tránh video cũ ID cao).
    ``scan_videos_tab_top=True``: lấy thêm vài video mới nhất trên tab Videos (feed riêng).
    ``require_page_owner=True``: chỉ giữ video chứng minh được thuộc Page (GraphQL owner
    hoặc href gắn vanity/id) — không lấy reel gợi ý.
    ``use_graphql_harvest=True``: nghe JSON GraphQL lúc tải trang (cách lấy chính xác nhất).
    ``newest_only=True``: chế độ theo dõi chia sẻ — chỉ đầu trang, dừng sớm khi đủ ô,
    bỏ tab Videos nếu đã có đủ reel (nhanh + ít lẫn).

    Returns:
        ``{ "ok": bool, "items": [{"video_id", "url"}], "video_urls": [...], "message": str }``
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
    if newest_only:
        scroll_until_end = False
        max_scroll_rounds = min(int(max_scroll_rounds), 2)
        max_reels = min(int(max_reels), 12)
        scroll_pause_sec = min(float(scroll_pause_sec), 0.65)
    if scroll_until_end:
        max_scroll_rounds = max(220, int(max_scroll_rounds))
    if cookie_path:
        st("Đang chạy chế độ account-profile, không dùng cookie JSON riêng.")

    ordered_ids: list[str] = []
    seen: set[str] = set()
    ordered_urls: list[str] = []
    seen_urls: set[str] = set()
    video_ordered_urls: list[str] = []
    owned_ids: list[str] = []
    video_tab_ids: list[str] = []
    harvest_box: dict[str, Any] = {"obj": None}
    page_meta: dict[str, str] = {"name": "", "id": ""}

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

    def _ingest_page_tiles(page_obj: Any) -> None:
        """Thu ô video/reel đúng Page (thứ tự trên → dưới), gồm /reel/ trần và /videos/."""
        hrefs = _collect_visual_page_video_hrefs(page_obj, page_url or url)
        # Trên đúng tab Page: ô lớn /reel/id là video của Page (FB ít gắn vanity).
        # Chỉ bỏ href vanity trang khác (đã lọc trong collect). Thứ tự Y = mới → cũ.
        for rid in _extract_watchable_ids_from_hrefs(hrefs):
            if rid in seen:
                continue
            if require_page_owner:
                if len(owned_ids) >= max_reels:
                    break
                seen.add(rid)
                owned_ids.append(rid)
                continue
            if len(seen) >= max_reels:
                break
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
                account_id = str(acc.get("id") or acc.get("account_id") or "").strip() or "(scan)"
                profile_slot = None
                temp_profile_root: Path | None = None  # giữ tên cũ — không còn clone song song
                if profile_path:
                    profile_dir = Path(profile_path).resolve()
                    os.makedirs(str(profile_dir), exist_ok=True)
                    # Không clone profile khi đang chia sẻ — chờ 1 Firefox/TK.
                    if not _wait_profile_unlocked(
                        profile_dir,
                        browser_type,
                        timeout_sec=max(120.0, float(max_scan_minutes or 30) * 60.0),
                        status=st,
                    ):
                        return {
                            "ok": False,
                            "items": [],
                            "message": (
                                "Profile tài khoản vẫn đang mở (chia sẻ/đăng lịch). "
                                "Đã chờ hết giờ — không mở Firefox thứ 2. Thử quét lại sau."
                            ),
                        }
                    from src.automation.browser_factory import (
                        account_use_proxy_enabled,
                        acquire_profile_browser_slot,
                        release_profile_browser_slot,
                    )
                    from src.utils.account_proxy_mapper import require_account_proxy_live
                    from src.utils.proxy_check import build_playwright_proxy_settings

                    try:
                        profile_slot = acquire_profile_browser_slot(
                            profile_dir,
                            account_id,
                            timeout_sec=max(120.0, float(max_scan_minutes or 30) * 60.0),
                        )
                    except RuntimeError as exc:
                        return {
                            "ok": False,
                            "items": [],
                            "message": str(exc) or "Không chiếm được slot profile để quét.",
                        }
                    user_data_dir = str(profile_dir)
                    launch_kwargs: dict[str, Any] = {
                        "user_data_dir": user_data_dir,
                        "headless": hl,
                        "viewport": {"width": 1366, "height": 900},
                        "locale": "vi-VN",
                    }
                    if browser_exe and Path(browser_exe).is_file():
                        launch_kwargs["executable_path"] = str(Path(browser_exe).resolve())

                    if account_use_proxy_enabled(acc):
                        require_account_proxy_live(acc)
                        launch_kwargs["proxy"] = build_playwright_proxy_settings(
                            acc.get("proxy") if isinstance(acc.get("proxy"), dict) else {}
                        )
                    try:
                        if browser_type in ("firefox",):
                            context = pw.firefox.launch_persistent_context(**launch_kwargs)
                        else:
                            context = pw.chromium.launch_persistent_context(**launch_kwargs)
                    except Exception:
                        release_profile_browser_slot(profile_dir, account_id, profile_slot)
                        profile_slot = None
                        raise
                    st("Đang dùng browser profile của tài khoản đã chọn (1 cửa sổ / TK).")
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
                    vanity = expected_identity.split(":", 1)[1] if expected_identity.startswith("vanity:") else ""
                    page_id = expected_identity.split(":", 1)[1] if expected_identity.startswith("id:") else ""
                    harvest = None
                    if use_graphql_harvest or require_page_owner:
                        from src.services.page_video_harvest import NetworkVideoHarvest

                        harvest = NetworkVideoHarvest(
                            fb_page_id=page_id,
                            fb_page_vanity=vanity,
                            require_owner=True,
                            rewrite_lifetime=False,
                        )
                        harvest.attach(page)
                        harvest_box["obj"] = harvest
                    st("Đang tải tab Reels của đúng Page nguồn…")
                    _goto_scan(page, url)
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
                        _goto_scan(page, url)
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
                                    _goto_scan(page, url)
                                    time.sleep(0.8)
                        except Exception:
                            pass
                    if harvest is not None and not harvest.fb_page_id:
                        found_id = extract_facebook_page_id_from_page(page)
                        if found_id:
                            harvest.fb_page_id = found_id
                            page_id = found_id
                    # Tên + Meta ID Page — hiện trên Hàng chờ (Tên (id)).
                    try:
                        title = str(page.title() or "").strip()
                        name = re.sub(
                            r"\s*[|\-–—]\s*Facebook.*$",
                            "",
                            title,
                            flags=re.I,
                        ).strip()
                        if not name or name.casefold() in {"facebook", "reels", "watch"}:
                            try:
                                og = page.locator('meta[property="og:title"]').first
                                if og.count():
                                    name = str(og.get_attribute("content") or "").strip()
                                    name = re.sub(
                                        r"\s*[|\-–—]\s*Facebook.*$",
                                        "",
                                        name,
                                        flags=re.I,
                                    ).strip()
                            except Exception:
                                pass
                        if not name or name.casefold() in {"facebook", "reels", "watch"}:
                            try:
                                h1 = page.locator("[role='main'] h1").first
                                if h1.count():
                                    name = str(h1.inner_text(timeout=1500) or "").strip()
                            except Exception:
                                pass
                        if name and name.casefold() not in {"facebook", "reels", "watch"}:
                            page_meta["name"] = name[:120]
                    except Exception:
                        pass
                    try:
                        found_id = page_id or extract_facebook_page_id_from_page(page)
                        if found_id:
                            page_meta["id"] = str(found_id)
                            page_id = str(found_id)
                    except Exception:
                        if page_id:
                            page_meta["id"] = str(page_id)
                    if harvest is not None:
                        try:
                            harvest.ingest_text(page.content(), trusted_surface=False)
                        except Exception as exc:  # noqa: BLE001
                            logger.debug("harvest html: {}", exc)
                    _ingest_page_tiles(page)
                    if include_html_ids and not require_page_owner:
                        _add_from_text(page.content())
                    _emit_partial()

                    def _grid_count() -> int:
                        return len(owned_ids) if require_page_owner else len(ordered_ids)

                    # Theo dõi chia sẻ: đủ ô đầu trang → không cuộn sâu (tránh gợi ý + chậm).
                    skip_scroll = bool(newest_only and _grid_count() >= min(6, max_reels))
                    if skip_scroll:
                        st(f"Đã đủ {_grid_count()} video đầu trang — bỏ cuộn sâu.")

                    stable = 0
                    prev_count = 0
                    prev_scroll_h = 0
                    idle_at_bottom = 0
                    for i in range(0 if skip_scroll else max_scroll_rounds):
                        if time.monotonic() >= hard_deadline:
                            st(f"Đã chạm giới hạn thời gian quét ({max_scan_minutes:.0f} phút) — dừng.")
                            break
                        if _grid_count() >= max_reels:
                            break
                        if newest_only and _grid_count() >= min(8, max_reels) and i >= 1:
                            st("Đã đủ video mới nhất trên đầu trang — dừng cuộn.")
                            break
                        elapsed = int(max(0.0, time.monotonic() - started_at))
                        before = page.evaluate(_JS_SCROLL_SNAP)
                        if prev_scroll_h == 0:
                            prev_scroll_h = int(before.get("sh") or 0)
                        st(
                            f"Đang cuộn {i + 1}/{max_scroll_rounds} — "
                            f"{_grid_count()} reel — cao trang ~{before.get('sh', 0)}px — {elapsed}s…"
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
                        _ingest_page_tiles(page)
                        # newest_only: không đọc lại HTML mỗi vòng (chậm); GraphQL listener vẫn chạy.
                        if harvest is not None and not newest_only and i % 2 == 1:
                            try:
                                harvest.ingest_text(page.content(), trusted_surface=False)
                            except Exception:
                                pass
                        if include_html_ids and not require_page_owner:
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

                        if _grid_count() == prev_count:
                            stable += 1
                            if stable >= (3 if newest_only else 22) and not scroll_until_end:
                                st("Không còn reel mới sau nhiều lần cuộn — dừng.")
                                break
                            # Ít reel trên trang: dừng sớm khi đã ở đáy và không tăng count (tránh chờ >40 vòng).
                            n_found = _grid_count()
                            if n_found <= 25 and i >= 10 and stable >= 7 and idle_at_bottom >= 5:
                                st("Đã thu đủ reel hiển thị (ít mục) — dừng quét.")
                                break
                            # Nhiều reel / cuộn tới hết: giữ ngưỡng cao hơn để tránh dừng nhầm giữa chừng.
                            if i > 45 and stable >= 24 and idle_at_bottom >= 12:
                                st("Đã ở cuối trang, không thêm reel — dừng.")
                                break
                        else:
                            stable = 0
                        prev_count = _grid_count()

                    # Tab Videos — chỉ khi Reels mỏng (video đăng dạng Video, không phải Reel).
                    need_videos_top = bool(scan_videos_tab_top and not scan_videos_tab)
                    if newest_only and _grid_count() >= 6:
                        need_videos_top = False
                        st("Đã đủ reel đầu trang — bỏ tab Videos (nhanh hơn).")
                    if need_videos_top:
                        try:
                            vurl = _profile_videos_tab_url(page_url or url)
                            st("Đang lấy video mới nhất trên tab Videos của cùng Page…")
                            _goto_scan(page, vurl)
                            time.sleep(min(1.0, scroll_pause_sec + 0.15))
                            try:
                                page.wait_for_selector(
                                    "a[href*='/videos/'], a[href*='/reel/'], [role='main']",
                                    timeout=8_000,
                                )
                            except Exception:
                                pass
                            if expected_identity and not url_matches_facebook_page(page.url, url):
                                st("Tab Videos lệch Page nguồn — bỏ qua.")
                                raise RuntimeError("videos_tab_wrong_page")
                            v_seen: set[str] = set()
                            for j in range(3):
                                hrefs_v = _collect_visual_page_video_hrefs(page, page_url or url)
                                # Tab Videos: lấy ô trên → dưới (gồm /videos/ và /reel/ trần).
                                take = _extract_watchable_ids_from_hrefs(hrefs_v)
                                for vid in take:
                                    if vid in v_seen:
                                        continue
                                    v_seen.add(vid)
                                    video_tab_ids.append(vid)
                                    video_ordered_urls.append(f"https://www.facebook.com/reel/{vid}")
                                    if len(video_ordered_urls) >= 12:
                                        break
                                if harvest is not None:
                                    try:
                                        harvest.ingest_text(page.content(), trusted_surface=False)
                                    except Exception:
                                        pass
                                if len(video_ordered_urls) >= 12:
                                    break
                                try:
                                    page.evaluate(_JS_SCROLL_DEEP)
                                except Exception:
                                    pass
                                time.sleep(scroll_pause_sec)
                                st(
                                    f"Tab Videos (đầu trang) — cuộn {j + 1}/3 — "
                                    f"{len(video_ordered_urls)} video của Page."
                                )
                        except Exception as exc:  # noqa: BLE001
                            logger.debug("videos tab top scan: {}", exc)

                    # Tab Videos: chỉ khi thu thập catalog đầy đủ — theo dõi chia sẻ tắt để tránh video cũ.
                    if scan_videos_tab and len(ordered_ids) < max_reels:
                        try:
                            vurl = _profile_videos_tab_url(url)
                            st("Đang quét bổ sung tab Videos (cùng tài khoản đã login)…")
                            _goto_scan(page, vurl, timeout_ms=60_000)
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
                    hv = harvest_box.get("obj")
                    if hv is not None:
                        try:
                            hv.detach(page)
                        except Exception:
                            pass
                    if context:
                        try:
                            context.close()
                        except Exception:  # noqa: BLE001
                            pass
                    if profile_slot is not None and profile_path:
                        try:
                            from src.automation.browser_factory import release_profile_browser_slot

                            release_profile_browser_slot(
                                Path(profile_path).resolve(),
                                account_id,
                                profile_slot,
                            )
                        except Exception as exc:  # noqa: BLE001
                            logger.debug("release scan profile slot: {}", exc)
                        profile_slot = None
                    if temp_profile_root is not None:
                        shutil.rmtree(temp_profile_root, ignore_errors=True)
            finally:
                if browser:
                    browser.close()
    except Exception as exc:  # noqa: BLE001
        logger.exception("scan_facebook_profile_reels_page")
        return {"ok": False, "items": [], "message": str(exc)}

    elapsed_sec = int(max(0.0, time.monotonic() - started_at))
    harvest_ids: list[str] = []
    hv = harvest_box.get("obj")
    if hv is not None:
        try:
            from src.services.page_video_harvest import sort_videos_by_published

            harvest_ids = [
                str(r.get("video_id") or "")
                for r in sort_videos_by_published(list(hv.items()), newest_first=True)
                if r.get("video_id")
            ]
        except Exception as exc:  # noqa: BLE001
            logger.debug("harvest sort: {}", exc)
            harvest_ids = [str(r.get("video_id") or "") for r in hv.items() if r.get("video_id")]
    if require_page_owner or use_graphql_harvest:
        reel_ids, vid_ids = merge_owner_verified_video_ids(
            harvest_ids=harvest_ids[:max_reels],
            owned_ids=owned_ids,
            video_tab_ids=video_tab_ids,
        )
        ordered_urls = [f"https://www.facebook.com/reel/{i}" for i in reel_ids[:max_reels]]
        video_ordered_urls = [f"https://www.facebook.com/reel/{i}" for i in vid_ids]
    items = []
    for u in ordered_urls:
        ids = _extract_watchable_ids_from_hrefs([u])
        if not ids:
            continue
        items.append({"video_id": ids[0], "url": u})
    if not items and not video_ordered_urls:
        return {
            "ok": False,
            "items": [],
            "video_urls": [],
            "message": (
                "Không trích được video thuộc đúng Page nguồn (GraphQL owner / href gắn Page). "
                "Thử: (1) mở profile tài khoản Page nhận và đăng nhập Facebook, "
                "(2) dán đúng link Page (facebook.com/TenPage)."
            ),
        }
    n_vid = len(video_ordered_urls)
    extra = f", {n_vid} video tab Videos" if n_vid else ""
    n_h = len(harvest_ids)
    via = f", GraphQL {n_h}" if n_h else ""
    return {
        "ok": True,
        "items": items,
        "video_urls": list(video_ordered_urls),
        "harvest_urls": [f"https://www.facebook.com/reel/{i}" for i in harvest_ids[:max_reels]],
        "page_name": str(page_meta.get("name") or ""),
        "fb_page_id": str(page_meta.get("id") or ""),
        "message": (
            f"Thu được {len(items)} video của Page{extra}{via} sau {elapsed_sec}s "
            f"(giới hạn {max_reels})."
        ),
    }
