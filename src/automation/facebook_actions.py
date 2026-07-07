"""
Hành động UI trên Facebook (Playwright sync).

Ưu tiên selector: aria-label → role → nội dung text (XPath). Mỗi bước chờ selector trước khi thao tác.
Khi timeout / lỗi tìm phần tử, tự chụp màn hình vào ``logs/screenshots/`` (không dùng OAuth / Graph API).
"""

from __future__ import annotations

import json
import os
import random
import re
import time
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Literal
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from loguru import logger
from playwright.sync_api import Locator, Page, TimeoutError as PlaywrightTimeoutError

from src.automation.browser_factory import _env_bool, _env_int, _project_root
from src.utils.page_schedule import scheduler_tz
from src.utils.reel_thumbnail_choice import (
    REEL_THUMBNAIL_METHOD1_FIRST_AUTO,
    normalize_reel_thumbnail_choice,
)
from src.utils.schedule_job_content import (
    dedupe_post_title_content_hashtags,
    _normalize_hashtag_list,
)

_REEL_STRICT_JOB_ID: ContextVar[str] = ContextVar("_REEL_STRICT_JOB_ID", default="")
_LAST_PASS_FIELD_LOG_AT: float = 0.0


def set_reel_strict_log_job_id(job_id: str | None) -> None:
    _REEL_STRICT_JOB_ID.set(str(job_id or "").strip())


def _reel_strict_prefix(stage: Literal["Upload", "Wizard", "Verify"]) -> str:
    jid = _REEL_STRICT_JOB_ID.get().strip()
    if jid:
        return f"[FB Reel Strict][{stage}][job:{jid[:12]}]"
    return f"[FB Reel Strict][{stage}]"


def assert_safe_facebook_navigation_url(url: str, *, label: str = "nav") -> None:
    """
    Chặn URL http(s) với host IP dạng 0.x.x.x (thường gặp khi proxy hệ thống / cấu hình lỗi).
    """
    u = str(url).strip()
    if not u.startswith(("http://", "https://")):
        raise ValueError(f"{label}: URL phải bắt đầu bằng http(s): {u!r}")
    host = (urlparse(u).hostname or "").strip()
    if not host:
        raise ValueError(f"{label}: thiếu hostname: {u!r}")
    if host.replace(".", "").isdigit():
        parts = host.split(".")
        if len(parts) == 4 and all(p.isdigit() for p in parts):
            if int(parts[0]) == 0:
                raise ValueError(
                    f"{label}: host IP không hợp lệ {host!r} — kiểm tra biến HTTP_PROXY/HTTPS_PROXY trên Windows, "
                    "cấu hình proxy Firefox, và file hosts. Tắt proxy hệ thống hoặc bật đúng proxy trong tài khoản ToolFB."
                )


def prime_facebook_session_page(page: Page) -> None:
    """
    Mở đầu phiên: ép tab hiện tại về Facebook (tránh tab/resume/extension đưa tới URL lạ như 0.0.x.x).
    Gọi trước ``login_with_cookie``.
    """
    u = _fb_normalize_client_url("https://www.facebook.com/")
    assert_safe_facebook_navigation_url(u, label="prime")
    cur = (page.url or "").strip()
    if cur and "facebook.com" not in cur.lower() and not cur.startswith("about:"):
        logger.warning("[FB] Trước prime, URL hiện tại: {} — ép về Facebook.", cur)
    logger.info("[FB] prime_facebook_session_page -> {}", u)
    try:
        from src.services.facebook_recaptcha import reset_recaptcha_network_capture

        reset_recaptcha_network_capture(page, reason="prime")
    except Exception:
        pass
    page.goto(u, wait_until="domcontentloaded", timeout=90_000)
    _force_www_facebook_if_mobile_redirect(page)
    navigate_away_from_login_if_session_active(page)


def _screenshots_dir() -> Path:
    """
    Trả về thư mục ``logs/screenshots/`` để lưu ảnh lỗi UI (tạo sẵn cây thư mục).

    Returns:
        Đường dẫn thư mục ảnh chụp lỗi.
    """
    d = _project_root() / "logs" / "screenshots"
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_ui_failure_screenshot(page: Page, reason: str) -> None:
    """
    Chụp ảnh toàn trang khi thao tác UI thất bại và lưu ``logs/screenshots/error_<timestamp>.png``.

    Dùng khi không có OAuth: sáng hôm sau có thể mở thư mục này và gửi ảnh cho AI phân tích.

    Args:
        page: Trang Playwright hiện tại.
        reason: Mô tả ngắn lỗi (ghi kèm trong log).
    """
    try:
        if page.is_closed():
            logger.warning("Bỏ qua screenshot lỗi UI (trang đã đóng): {}", reason)
            return
    except Exception:
        pass
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    path = _screenshots_dir() / f"error_{ts}.png"
    try:
        page.screenshot(path=str(path), full_page=True)
        logger.error("Đã lưu ảnh lỗi UI: {} — {}", path, reason)
    except Exception as exc:  # noqa: BLE001 — vẫn log nếu không chụp được
        msg = str(exc)
        if "Target page, context or browser has been closed" in msg or type(exc).__name__ == "TargetClosedError":
            logger.warning("Không chụp được screenshot (context đã đóng): {} — {}", reason, msg)
            return
        logger.exception("Không thể chụp màn hình lỗi ({}): {}", reason, exc)


def _failure_screenshot(page: Page, reason: str) -> None:
    """
    Alias nội bộ gọi ``save_ui_failure_screenshot`` (giữ tên cũ trong module).

    Args:
        page: Trang Playwright.
        reason: Lý do lỗi.
    """
    save_ui_failure_screenshot(page, reason)


def _fb_host_key(netloc: str) -> str:
    """Chuẩn hóa host để so khớp www / m / mbasic."""
    h = (netloc or "").strip().lower()
    if h.startswith("www."):
        h = h[4:]
    if h in ("m.facebook.com", "mbasic.facebook.com", "touch.facebook.com"):
        return "facebook.com"
    return h


def _fb_rewrite_www_to_m_host(u: str) -> str:
    """Đổi host www/facebook.com → m.facebook.com (đã kiểm tra là URL http facebook)."""
    low = u.lower()
    if "facebook.com" not in low:
        return u
    if "m.facebook.com" in low or "mbasic.facebook.com" in low:
        return u
    u = u.replace("https://www.facebook.com", "https://m.facebook.com", 1)
    u = u.replace("http://www.facebook.com", "http://m.facebook.com", 1)
    u = u.replace("https://facebook.com", "https://m.facebook.com", 1)
    u = u.replace("http://facebook.com", "http://m.facebook.com", 1)
    return u


def _force_www_facebook_if_mobile_redirect(page: Page) -> None:
    """
    Nếu bị redirect sang m/mbasic/touch host thì ép về www.facebook.com rồi tiếp tục.
    """
    try:
        cur = str(page.url or "").strip()
        if not cur:
            return
        p = urlparse(cur)
        host = (p.netloc or "").strip().lower()
        if host not in ("m.facebook.com", "mbasic.facebook.com", "touch.facebook.com"):
            return
        dst = urlunparse((p.scheme or "https", "www.facebook.com", p.path, p.params, p.query, p.fragment))
        assert_safe_facebook_navigation_url(dst, label="force_www")
        logger.warning("[FB] Redirect mobile host {} -> ép về {}", host, dst)
        page.goto(dst, wait_until="domcontentloaded", timeout=90_000)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không ép lại được từ mobile host về www.facebook.com: {}", exc)


def _page_shows_facebook_login_surface(page: Page) -> bool:
    """True nếu URL hoặc form cho thấy đang ở màn hình đăng nhập Facebook."""
    u = (page.url or "").lower()
    if "facebook.com" not in u:
        return False
    if "/login" in u or u.rstrip("/").endswith("facebook.com/login"):
        return True
    try:
        pw = page.locator(
            "input[name='pass'], input#pass, form[method='post'] input[type='password']"
        ).first
        if pw.is_visible(timeout=600):
            royal = page.locator(
                "form[data-testid='royal_login_form'], #login_form, form[action*='login']"
            )
            if royal.count() > 0 and royal.first.is_visible(timeout=400):
                return True
            return "/login" in u
    except Exception:
        pass
    return False


def navigate_away_from_login_if_session_active(page: Page) -> bool:
    """
  Profile persistent đã có cookie ``c_user`` nhưng tab đang ở /login — về bảng tin, không form login.

    Returns:
        True nếu đã điều hướng khỏi login; False nếu không có phiên hoặc không ở login.
    """
    from src.services.facebook_session_recovery import _read_facebook_c_user

    if not _read_facebook_c_user(page):
        return False
    if not _page_shows_facebook_login_surface(page):
        return False
    home = _fb_normalize_client_url("https://www.facebook.com/")
    assert_safe_facebook_navigation_url(home, label="session_active_home")
    logger.info(
        "[FB] Profile đã có c_user — bỏ trang login, mở bảng tin (url cũ={})",
        (page.url or "")[:90],
    )
    page.goto(home, wait_until="domcontentloaded", timeout=90_000)
    _force_www_facebook_if_mobile_redirect(page)
    try:
        page.wait_for_timeout(700)
    except Exception:
        pass
    return True


def _fb_normalize_client_url(url: str) -> str:
    """
    - ``TOOLFB_NAV_MOBILE_FB=1``: luôn dùng ``m.facebook.com`` (viewport hẹp / shell mobile).
    - Hoặc ``FB_MOBILE_MODE=1`` và ``FB_PREFER_M_FACEBOOK=1``: cùng chuyển host mobile.
    - Ngược lại: giữ ``www.facebook.com``.
    """
    u = str(url).strip()
    if not u.startswith("http"):
        return u
    # Luồng Business Composer phải giữ nguyên host business.facebook.com.
    if "business.facebook.com" in u.lower():
        return u
    if os.environ.get("TOOLFB_NAV_MOBILE_FB") == "1":
        return _fb_rewrite_www_to_m_host(u)
    if not _env_bool("FB_MOBILE_MODE", False) or not _env_bool("FB_PREFER_M_FACEBOOK", False):
        return u
    return _fb_rewrite_www_to_m_host(u)


def _facebook_url_points_at_surface(url: str) -> bool:
    """True nếu URL không phải chỉ newsfeed/home (có path Page/Group cụ thể)."""
    try:
        p = urlparse(str(url).strip())
        if "facebook.com" not in (p.netloc or "").lower():
            return False
        path = (p.path or "/").rstrip("/").lower()
        if not path:
            return False
        noise = (
            "/home",
            "/stories",
            "/watch",
            "/reel",
            "/marketplace",
            "/gaming",
            "/notifications",
            "/messages",
        )
        if any(path.startswith(x) for x in noise):
            return False
        return True
    except Exception:
        return False


_FB_RESERVED_PATH_SEGMENTS = frozenset({
    "home",
    "home.php",
    "watch",
    "gaming",
    "marketplace",
    "groups",
    "events",
    "ads",
    "photo",
    "photos",
    "videos",
    "reel",
    "reels",
    "stories",
    "notifications",
    "messages",
    "friends",
    "login",
    "reg",
    "help",
    "privacy",
    "policies",
    "professional_dashboard",
    "business",
    "settings",
    "search",
    "l.php",
    "share",
    "dialog",
})


def _looks_like_facebook_page_profile_path(path: str) -> bool:
    """Path dạng ``/PageSlug`` hoặc ``/pages/Name/123`` — không phải feed/home."""
    parts = [x for x in (path or "").rstrip("/").split("/") if x]
    if not parts or len(parts) > 3:
        return False
    seg0 = parts[0].lower()
    if seg0 in _FB_RESERVED_PATH_SEGMENTS:
        return False
    if seg0 == "pages":
        return len(parts) >= 2
    return len(parts) == 1


def _page_html_contains_facebook_id(page: Page, page_id: str) -> bool:
    """Meta embed page id trong HTML — dùng xác nhận redirect slug vẫn đúng Page."""
    pid = str(page_id or "").strip()
    if not pid.isdigit() or len(pid) < 8:
        return False
    try:
        return pid in (page.content() or "")
    except Exception:
        return False


def _urls_refer_same_facebook_page(
    target_url: str,
    current_url: str,
    *,
    page: Page | None = None,
) -> bool:
    """
    Hai URL có thể là cùng Page dù path khác (id số vs vanity slug).

    Facebook thường redirect ``/103833422779877`` → ``/G.Force.Ghoul`` — vẫn đúng Page
    nếu HTML trang chứa ``page_id`` (hoặc path/id trùng trực tiếp).
    """
    cur = str(current_url or "").strip()
    tgt = str(target_url or "").strip()
    if not cur or not tgt:
        return False
    try:
        c = urlparse(cur)
        t = urlparse(_fb_normalize_client_url(tgt))
        if c.netloc and t.netloc and _fb_host_key(c.netloc) != _fb_host_key(t.netloc):
            return False
        cpath = (c.path or "/").rstrip("/").lower()
        tpath = (t.path or "/").rstrip("/").lower()
        if not tpath or tpath in ("/", "/home", "/home.php"):
            return True
        if cpath == tpath or cpath.startswith(tpath + "/"):
            return True
        tid = extract_facebook_numeric_id_from_url(tgt)
        cid = extract_facebook_numeric_id_from_url(cur)
        if tid and cid and tid == cid:
            return True
        t_parts = [x for x in tpath.split("/") if x]
        c_parts = [x for x in cpath.split("/") if x]
        # Job lưu /{page_id} — FB redirect sang /VanitySlug (cần xác nhận id trong HTML).
        if tid and len(t_parts) == 1 and t_parts[0] == tid:
            if _looks_like_facebook_page_profile_path(cpath) and cpath != tpath:
                if page is not None and _page_html_contains_facebook_id(page, tid):
                    return True
        # Đích vanity slug, URL hiện tại là id số.
        if len(t_parts) == 1 and not t_parts[0].isdigit() and len(t_parts[0]) >= 2:
            if cid and len(c_parts) == 1 and c_parts[0] == cid:
                return True
        tslug = t_parts[-1] if t_parts else ""
        if tslug and len(tslug) > 2 and tslug.lower() in cur.lower():
            return True
    except Exception:
        return False
    return False


def facebook_urls_align_as_target_surface(entity_url: str, job_page_url: str) -> bool:
    """
    True nếu hai URL Facebook (sau ``_fb_normalize_client_url``) cùng bề mặt Page/Group
    (path khớp hoặc một path là tiền tố của path kia).
    """
    try:
        eu = str(entity_url).strip()
        ju = str(job_page_url).strip()
        if not eu or not ju:
            return False
        c = urlparse(_fb_normalize_client_url(eu))
        t = urlparse(_fb_normalize_client_url(ju))
        if c.netloc and t.netloc and _fb_host_key(c.netloc) != _fb_host_key(t.netloc):
            return False
        cpath = (c.path or "/").rstrip("/").lower()
        tpath = (t.path or "/").rstrip("/").lower()
        if not tpath:
            return not cpath
        if not cpath:
            return not tpath
        return cpath == tpath or cpath.startswith(tpath + "/") or tpath.startswith(cpath + "/")
    except Exception:
        return False


def _facebook_url_looks_like_group(url: str) -> bool:
    try:
        low = str(url).strip().lower()
        if "/groups/" in low or "facebook.com/groups/" in low:
            return True
        p = urlparse(low)
        parts = [x for x in (p.path or "").split("/") if x]
        return len(parts) >= 1 and parts[0].lower() == "groups"
    except Exception:
        return False


def _parse_boolish(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "on")


def extract_facebook_numeric_id_from_url(url: str) -> str | None:
    """
    Lấy Page/User numeric id từ URL Facebook khi có trong path/query (không gọi API).

    Hỗ trợ: ``/123456789012345``, ``profile.php?id=``, ``/pages/Name/123``.
    """
    u = str(url).strip()
    if not u:
        return None
    try:
        low = u.lower()
        if "profile.php" in low:
            q = urlparse(u).query
            for part in q.split("&"):
                if part.lower().startswith("id="):
                    v = part.split("=", 1)[-1].strip()
                    if v.isdigit() and len(v) >= 8:
                        return v
        p = urlparse(u)
        parts = [x for x in (p.path or "").split("/") if x]
        for seg in parts:
            if seg.isdigit() and len(seg) >= 8:
                return seg
        if "pages" in parts:
            i = parts.index("pages")
            if i + 2 < len(parts) and parts[i + 2].isdigit() and len(parts[i + 2]) >= 8:
                return parts[i + 2]
    except Exception:
        pass
    return None


def page_row_facebook_asset_id(page_row: dict[str, Any]) -> str | None:
    """Ưu tiên ``fb_page_id`` trong bản ghi Page; nếu không có thì suy từ ``page_url``."""
    for key in ("fb_page_id", "facebook_page_id", "meta_asset_id"):
        raw = str(page_row.get(key, "")).strip()
        if raw.isdigit() and len(raw) >= 8:
            return raw
    return extract_facebook_numeric_id_from_url(str(page_row.get("page_url", "")))


def default_meta_business_composer_url(asset_id: str) -> str:
    """URL composer Business Suite chuẩn (``asset_id`` = id số của Page trên Meta)."""
    aid = str(asset_id).strip()
    if not aid.isdigit():
        raise ValueError("asset_id Meta phải là chuỗi số.")
    return (
        "https://business.facebook.com/latest/composer/"
        f"?asset_id={aid}&nav_ref=internal_nav&ref=biz_web_content_manager_published_posts&context_ref=POSTS"
    )


def default_meta_published_posts_url(asset_id: str) -> str:
    """URL danh sách bài đã đăng (Posts & reels → Published) trên Business Suite."""
    aid = str(asset_id).strip()
    if not aid.isdigit():
        raise ValueError("asset_id Meta phải là chuỗi số.")
    return (
        "https://business.facebook.com/latest/posts/published_posts"
        f"?asset_id={aid}&nav_ref=internal_nav&ref=biz_web_content_manager_published_posts"
    )


def merge_asset_id_into_business_composer_url(url: str, asset_id: str) -> str:
    """Ghi đè / bổ sung ``asset_id`` trên URL composer (giữ các query khác)."""
    aid = str(asset_id).strip()
    if not aid.isdigit():
        raise ValueError("asset_id không hợp lệ.")
    p = urlparse(str(url).strip())
    qs = parse_qs(p.query, keep_blank_values=True)
    qs["asset_id"] = [aid]
    pairs: list[tuple[str, str]] = []
    for k, vals in qs.items():
        for v in vals:
            pairs.append((k, v))
    new_query = urlencode(pairs, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))


def resolve_target_url_from_page_row(page_row: dict[str, Any]) -> str:
    """
    Quyết định ``target_url`` thực tế khi đăng bài.

    - ``use_business_composer``: dùng URL composer; ``asset_id`` lấy từ ``fb_page_id`` hoặc id trong ``page_url``.
    - ``page_url`` đã là ``business.facebook.com/.../composer``: tự chèn/ghi ``asset_id`` nếu biết.
    """
    raw = str(page_row.get("page_url", "")).strip()
    use_biz = _parse_boolish(page_row.get("use_business_composer"))
    aid = page_row_facebook_asset_id(page_row)

    if use_biz and aid:
        return default_meta_business_composer_url(aid)
    if _is_meta_business_composer_url(raw):
        if aid:
            return merge_asset_id_into_business_composer_url(raw, aid)
        return raw
    return raw


def infer_pages_row_target_type(page_row: dict[str, Any]) -> str:
    """
    Ánh xạ ``page_kind`` + ``page_url`` → ``target_type`` cho pipeline đăng.

    Nếu ``page_kind`` trống/sai nhưng ``page_url`` trỏ tới Page/Group cụ thể, vẫn coi là fanpage/group
    để không rơi nhầm vào timeline (bảng tin cá nhân).
    """
    pk = str(page_row.get("page_kind", "")).strip().lower()
    url = str(page_row.get("page_url", "")).strip()
    if pk == "group":
        return "group"
    if pk in ("fanpage", "profile", "page", "fan_page"):
        return "fanpage"
    if url and _facebook_url_points_at_surface(url):
        return "group" if _facebook_url_looks_like_group(url) else "fanpage"
    return "timeline"


def entity_dict_from_pages_row(page_row: dict[str, Any]) -> dict[str, Any]:
    """Cùng quy tắc ``pages.json`` → entity như scheduler (fanpage/profile → fanpage)."""
    raw_url = str(page_row.get("page_url", "")).strip()
    resolved_url = resolve_target_url_from_page_row(page_row)
    return {
        "id": page_row.get("id"),
        "account_id": page_row.get("account_id"),
        "name": str(page_row.get("page_name", "")),
        "target_type": infer_pages_row_target_type(page_row),
        "target_url": resolved_url,
        # Fallback khi business composer lỗi quyền/asset: quay lại URL Page thường.
        "fallback_target_url": raw_url if raw_url and raw_url != resolved_url else "",
    }


def resolve_posting_entity(
    entity: dict[str, Any] | None,
    pages_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """
    Gắn đích đăng từ ``pages.json`` khi entity thiếu URL hoặc chỉ timeline trong khi job gắn Page.
    """
    row_url = str((pages_row or {}).get("page_url", "")).strip()
    if not row_url or not _facebook_url_points_at_surface(row_url):
        return entity
    if entity is None:
        return entity_dict_from_pages_row(pages_row)  # type: ignore[arg-type]
    tt = str(entity.get("target_type", "timeline")).strip().lower()
    eu = str(entity.get("target_url", "")).strip()
    if tt in ("fanpage", "group") and eu and _facebook_url_points_at_surface(eu):
        return entity
    return entity_dict_from_pages_row(pages_row)  # type: ignore[arg-type]


def _wait_fb_path_matches(page: Page, normalized_url: str, *, timeout_ms: int | None = None) -> None:
    """Chờ pathname hoặc slug trong URL khớp đích (tránh SPA kẹt feed)."""
    if timeout_ms is None:
        timeout_ms = max(5_000, min(120_000, _env_int("FB_NAV_PATH_WAIT_MS", 38_000)))
    p = urlparse(normalized_url)
    path = (p.path or "/").rstrip("/").lower()
    if not path or path in ("/", "/home", "/home.php"):
        return
    parts = [x for x in path.split("/") if x]
    slug = parts[-1] if parts else ""
    try:
        page.wait_for_function(
            """({ expectPath, expectSlug }) => {
              const norm = (s) => (s || '').replace(/\\/+$/, '').toLowerCase();
              const curPath = norm(window.location.pathname);
              const ep = norm(expectPath);
              const href = (location.href || '').toLowerCase();
              const reserved = new Set([
                'home','watch','gaming','marketplace','groups','events','ads',
                'notifications','messages','friends','login','professional_dashboard',
              ]);
              if (!ep || ep === '/' || ep === '/home' || ep === '/home.php') return true;
              if (curPath === ep || curPath.startsWith(ep + '/')) return true;
              if (expectSlug && expectSlug.length > 2 && href.includes(expectSlug.toLowerCase())) return true;
              const parts = curPath.split('/').filter(Boolean);
              const seg0 = parts[0] || '';
              // Target page id số — redirect vanity slug; xác nhận id có trong HTML.
              if (/^\\d{8,}$/.test(expectSlug) && parts.length === 1 && !reserved.has(seg0)) {
                const html = document.documentElement.innerHTML || '';
                if (html.includes(expectSlug)) return true;
              }
              if (/^\\d{8,}$/.test(expectSlug) && parts[0] === 'pages' && parts.length >= 2) return true;
              return false;
            }""",
            arg={"expectPath": path, "expectSlug": slug},
            timeout=timeout_ms,
        )
    except PlaywrightTimeoutError:
        cur = str(page.url or "")
        if _urls_refer_same_facebook_page(normalized_url, cur, page=page):
            logger.info(
                "[FB] URL redirect id/slug — coi là đúng Page (expect={!r} | now={})",
                normalized_url,
                cur,
            )
            return
        logger.warning(
            "[FB] Chờ path slug chưa khớp (expect path={!r} slug={!r}) — url hiện tại: {}",
            path,
            slug,
            cur,
        )
        raise


def _rand_delay_ms(min_ms: int, max_ms: int) -> int:
    lo = max(0, int(min_ms))
    hi = max(lo, int(max_ms))
    return random.randint(lo, hi)


def human_pause(*, label: str = "", kind: str = "action") -> None:
    """
    Tạm dừng ngẫu nhiên giữa click / nhập / chọn / bước đăng (giống người dùng).

    ``kind``:
      - ``action`` — giữa thao tác UI chung (env ``FB_HUMAN_DELAY_*_SEC``, mặc định 1.4–3.2s)
      - ``click`` — trước/sau bấm nút (``FB_CLICK_DELAY_*_MS``, mặc định 450–1100ms)
      - ``input`` — sau focus/nhập/chọn (``FB_INPUT_DELAY_*_MS``, mặc định 650–1400ms)
      - ``step`` — giữa bước pipeline đăng (``FB_STEP_DELAY_*_MS``, mặc định 1200–2800ms)
    """
    k = str(kind or "action").strip().lower()
    if k == "click":
        d_ms = _rand_delay_ms(
            max(80, _env_int("FB_CLICK_DELAY_MIN_MS", 450)),
            max(80, _env_int("FB_CLICK_DELAY_MAX_MS", 1100)),
        )
    elif k == "input":
        d_ms = _rand_delay_ms(
            max(100, _env_int("FB_INPUT_DELAY_MIN_MS", 650)),
            max(100, _env_int("FB_INPUT_DELAY_MAX_MS", 1400)),
        )
    elif k == "step":
        d_ms = _rand_delay_ms(
            max(120, _env_int("FB_STEP_DELAY_MIN_MS", 1200)),
            max(120, _env_int("FB_STEP_DELAY_MAX_MS", 2800)),
        )
    else:
        min_s = max(0.1, float(_env_int("FB_HUMAN_DELAY_MIN_SEC", 14)) / 10.0)
        max_s = max(min_s, float(_env_int("FB_HUMAN_DELAY_MAX_SEC", 32)) / 10.0)
        d_ms = int(random.uniform(min_s, max_s) * 1000.0)
    if d_ms <= 0:
        return
    if label:
        logger.info("[FB human-delay][{}] {}: {} ms", k, label, d_ms)
    time.sleep(d_ms / 1000.0)


def _human_pause(*, label: str = "", kind: str = "action") -> None:
    """Alias nội bộ — dùng :func:`human_pause`."""
    human_pause(label=label, kind=kind)


def _reel_inter_click_wait_ms() -> int:
    """Khoảng chờ giữa các lần bấm Next/Share trong wizard Reel."""
    return _rand_delay_ms(
        max(300, _env_int("FB_REEL_CLICK_GAP_MIN_MS", 750)),
        max(300, _env_int("FB_REEL_CLICK_GAP_MAX_MS", 1450)),
    )


def _view_only_mode_enabled() -> bool:
    # Mặc định mở tương tác để người dùng có thể inspect/copy HTML khi debug UI.
    # Có thể bật khóa thao tác theo job qua FB_LOCK_BROWSER_DURING_JOB=1.
    raw_global = os.environ.get("FB_VIEW_ONLY_MODE", "0").strip().lower()
    raw_job = os.environ.get("FB_LOCK_BROWSER_DURING_JOB", "1").strip().lower()
    global_on = raw_global not in {"0", "false", "off", "no"}
    job_on = raw_job not in {"0", "false", "off", "no"}
    return global_on or job_on


def _native_file_chooser_allowed() -> bool:
    """Mặc định tắt để không bật popup chọn file của OS."""
    raw = os.environ.get("FB_ALLOW_NATIVE_FILE_CHOOSER", "0").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _dismiss_leaked_native_file_dialog(page: Page) -> None:
    """
    Đóng hộp thoại chọn file OS nếu click upload lọt ra ngoài ``expect_file_chooser``.

    Chỉ best-effort (Escape) — luồng chuẩn vẫn phải bọc mọi click upload trong filechooser interception.
    """
    for _ in range(4):
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(180)
        except Exception:
            break


def _enable_view_only_guard(page: Page) -> None:
    """
    Khóa thao tác chuột / cảm ứng trên vùng trang (overlay trong suốt).
    Không chặn sự kiện bàn phím toàn cục để Playwright vẫn ``type`` được; người dùng khó thao tác vì không click được xuống DOM.
    MutationObserver giữ overlay khi Facebook thay DOM (SPA).
    """
    if not _view_only_mode_enabled():
        return
    script = """
(() => {
  if (typeof window.__toolfb_view_guard_cleanup === 'function') {
    try { window.__toolfb_view_guard_cleanup(); } catch (_) {}
  }
  window.__toolfb_view_guard_active = true;
  const blocker = document.createElement('div');
  blocker.id = '__toolfb_view_only_blocker';
  Object.assign(blocker.style, {
    position: 'fixed',
    top: '0',
    left: '0',
    right: '0',
    bottom: '0',
    width: '100vw',
    height: '100vh',
    zIndex: '2147483647',
    background: 'transparent',
    pointerEvents: 'auto',
    touchAction: 'none',
    cursor: 'not-allowed',
    isolation: 'isolate',
  });
  blocker.setAttribute('aria-hidden', 'true');
  blocker.title = 'Automation đang chạy: chỉ xem, không dùng chuột trên vùng trang.';
  const stop = (e) => {
    try {
      e.preventDefault();
      e.stopImmediatePropagation();
      e.stopPropagation();
    } catch (_) {}
  };
  for (const ev of [
    'pointerdown', 'pointerup', 'mousedown', 'mouseup',
    'click', 'dblclick', 'contextmenu', 'wheel', 'touchstart', 'touchend', 'touchmove',
  ]) {
    blocker.addEventListener(ev, stop, { capture: true, passive: false });
  }
  const ensure = () => {
    if (!window.__toolfb_view_guard_active) return;
    if (blocker.isConnected) return;
    try {
      document.documentElement.appendChild(blocker);
    } catch (_) {}
  };
  document.documentElement.appendChild(blocker);
  const mo = new MutationObserver(ensure);
  try {
    mo.observe(document.documentElement, { childList: true, subtree: true });
  } catch (_) {}
  window.__toolfb_view_guard_mo = mo;
  window.__toolfb_view_guard_cleanup = () => {
    window.__toolfb_view_guard_active = false;
    try { mo.disconnect(); } catch (_) {}
    try { blocker.remove(); } catch (_) {}
    window.__toolfb_view_guard_mo = null;
    window.__toolfb_view_guard_cleanup = null;
  };
})();
"""
    try:
        page.evaluate(script)
    except Exception:
        pass


def register_view_only_page_hooks(page: Page) -> None:
    """
    Sau mỗi lần tải trang (domcontentloaded), lắp lại lớp chỉ-xem.
    Gọi một lần trên ``page`` trước khi chạy luồng đăng Facebook.
    """
    if not _view_only_mode_enabled():
        return
    if getattr(page, "_toolfb_view_only_hooks_registered", False):
        return
    setattr(page, "_toolfb_view_only_hooks_registered", True)

    def _on_dom_content_loaded(*_args: object) -> None:
        try:
            _enable_view_only_guard(page)
        except Exception:
            pass

    page.on("domcontentloaded", _on_dom_content_loaded)


def _disable_view_only_guard(page: Page) -> None:
    if not _view_only_mode_enabled():
        return
    try:
        page.evaluate(
            """
(() => {
  const fn = window.__toolfb_view_guard_cleanup;
  if (typeof fn === 'function') fn();
})();
"""
        )
    except Exception:
        pass


def _typing_delay_ms() -> int:
    """
    Trả về độ trễ gõ phím (ms) ngẫu nhiên (env ``FB_TYPING_DELAY_*_MS``, mặc định 50–200).

    Returns:
        Số nguyên milliseconds.
    """
    from src.utils.human_typing import human_typing_delay_ms

    return human_typing_delay_ms()


def _resolve_path(maybe_relative: str | Path) -> Path:
    """
    Chuẩn hóa đường dẫn: nếu relative thì tính từ thư mục gốc dự án.

    Args:
        maybe_relative: Đường dẫn file hoặc thư mục.

    Returns:
        Path tuyệt đối.
    """
    p = Path(maybe_relative)
    if p.is_absolute():
        return p.resolve()
    return (_project_root() / p).resolve()


def _wait_selector_or_fail(page: Page, selector: str, *, timeout_ms: int = 30_000) -> None:
    """
    Chờ một selector hiển thị; nếu timeout thì chụp ảnh lỗi và ném lại ngoại lệ.

    Args:
        page: Trang Playwright.
        selector: Selector Playwright (CSS / XPath với tiền tố ``xpath=``).
        timeout_ms: Thời gian chờ tối đa.

    Raises:
        PlaywrightTimeoutError: Không thấy phần tử trong thời gian chờ.
    """
    try:
        page.wait_for_selector(selector, state="visible", timeout=timeout_ms)
    except PlaywrightTimeoutError:
        _failure_screenshot(page, f"wait_for_selector timeout: {selector!r}")
        raise


def _wait_first_selector(
    page: Page,
    selectors: Iterable[str],
    *,
    step_timeout_ms: int = 12_000,
    error_label: str = "",
    state: Literal["attached", "detached", "hidden", "visible"] = "visible",
) -> str:
    """
    Thử lần lượt danh sách selector (đã sắp theo độ ưu tiên) cho tới khi một cái hiện.

    Args:
        page: Trang Playwright.
        selectors: Các selector theo thứ tự ưu tiên (aria → role → xpath).
        step_timeout_ms: Timeout cho mỗi lần thử một selector.
        error_label: Nhãn mô tả bước UI (phục vụ log khi thất bại).
        state: Trạng thái chờ Playwright (``visible`` hoặc ``attached`` cho input ẩn).

    Returns:
        Selector đã match thành công.

    Raises:
        PlaywrightTimeoutError: Tất cả selector đều thất bại.
    """
    last_exc: Exception | None = None
    for sel in selectors:
        try:
            page.wait_for_selector(sel, state=state, timeout=step_timeout_ms)
            logger.debug("Đã match selector: {}", sel)
            return sel
        except PlaywrightTimeoutError as exc:
            last_exc = exc
            continue
    label = error_label or "không xác định"
    _failure_screenshot(
        page,
        f"{label}: không tìm thấy bất kỳ selector nào trong danh sách ưu tiên",
    )
    if last_exc:
        raise last_exc
    raise PlaywrightTimeoutError("Không có selector hợp lệ.")


def scroll_randomly(page: Page) -> None:
    """
    Cuộn trang ngẫu nhiên để mô phỏng người dùng đang xem bảng tin.

    Luôn chờ ``body`` trước khi cuộn.

    Args:
        page: Trang Facebook đã mở.

    Raises:
        PlaywrightTimeoutError: Không tải được nội dung trang cơ bản.
    """
    try:
        _wait_selector_or_fail(page, "body", timeout_ms=20_000)
        rounds = random.randint(2, 5)
        for _ in range(rounds):
            dy = random.randint(180, 900)
            if random.random() < 0.15:
                dy = -dy
            # Scroll bằng JS để không phụ thuộc sự kiện chuột người dùng.
            page.evaluate("(y) => window.scrollBy(0, y)", dy)
            page.wait_for_timeout(random.randint(250, 1200))
        logger.info("Đã scroll ngẫu nhiên {} nhịp.", rounds)
    except PlaywrightTimeoutError:
        raise
    except Exception as exc:
        _failure_screenshot(page, f"scroll_randomly: {exc}")
        raise


def _facebook_url_is_security_interstitial(url: str) -> bool:
    """
    Facebook chuyển tới trang xác minh (2FA / checkpoint / đăng nhập lại) — automation không xử lý được.
    Chrome/Chromium thường hay gặp khi cookie cũ hoặc Meta nghi ngờ phiên.
    """
    u = (url or "").strip().lower()
    if "facebook.com" not in u:
        return False
    markers = (
        "two_step_verification",
        "two-factor",
        "/checkpoint/",
        "checkpoint?",
        "/login.php",
        "/login/",
        "/device",
        "approvals_code",
        "/recover/initiate",
        "accountquality",
        "suspended",
    )
    return any(m in u for m in markers)


def _facebook_context_cookie_names(page: Page) -> set[str]:
    """
    Tên cookie (chữ thường) trên mọi domain *facebook* trong context.

    ``context.cookies('https://www.facebook.com')`` đôi khi không trả hết cookie gắn với ``.facebook.com``,
    dẫn tới bỏ sót ``c_user`` sau ``add_cookies``.
    """
    out: set[str] = set()
    try:
        for c in page.context.cookies():
            dom = str(c.get("domain", "")).lower()
            if "facebook" not in dom:
                continue
            nm = str(c.get("name", "")).strip().lower()
            if nm:
                out.add(nm)
    except Exception:
        pass
    return out


def _log_facebook_session_diagnostic(page: Page, *, stage: str) -> None:
    try:
        names = _facebook_context_cookie_names(page)
        logger.warning(
            "[FB] Chẩn đoán phiên ({}) url={!r} | n_cookie_facebook={} | có c_user={} | có xs={} | tên_mẫu={}",
            stage,
            page.url,
            len(names),
            "c_user" in names,
            "xs" in names,
            ", ".join(sorted(names)[:30]),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[FB] Không ghi được chẩn đoán phiên: {}", exc)


def facebook_session_appears_logged_in(page: Page) -> bool:
    """
    Heuristic nhanh: profile có phiên Facebook hợp lệ hay không (ưu tiên session sẵn trong profile).

    Không thay thế xác minh đầy đủ; dùng để tránh nạp cookie khi đã đăng nhập.
    """
    try:
        u = (page.url or "").lower()
        if "facebook.com" in u and _facebook_url_is_security_interstitial(page.url or ""):
            logger.debug("[FB] URL checkpoint/2FA — chưa có phiên hợp lệ (đang chờ captcha/TOTP).")
            return False
        names = _facebook_context_cookie_names(page)
        has_c_user = "c_user" in names
        # Ưu tiên cookie phiên: có thể URL tạm thời chưa ở facebook.com nhưng profile vẫn login.
        if has_c_user:
            if "facebook.com" not in u:
                logger.info("[FB] Đã có cookie c_user (url hiện tại={!r}) — coi như đang có phiên.", page.url)
                return True
        if "facebook.com" not in u:
            return False
        if "/login" in u or "/checkpoint" in u or "two_step" in u:
            logger.debug("[FB] URL login/checkpoint — phiên chưa đăng nhập.")
            return False
        loc = page.locator("input[name='pass'], input#pass, form[method='post'] input[type='password']")
        if loc.first.is_visible(timeout=1_200):
            on_login_url = "/login" in u or u.rstrip("/").endswith("facebook.com/login")
            royal = page.locator(
                "form[data-testid='royal_login_form'], #login_form, form[action*='login']"
            )
            primary_login = on_login_url
            try:
                primary_login = primary_login or (
                    royal.count() > 0 and royal.first.is_visible(timeout=400)
                )
            except Exception:
                primary_login = on_login_url
            if primary_login:
                global _LAST_PASS_FIELD_LOG_AT
                now = time.monotonic()
                if now - _LAST_PASS_FIELD_LOG_AT >= 25.0:
                    logger.info(
                        "[FB] Thấy form đăng nhập chính — coi như chưa đăng nhập (url={}).",
                        (page.url or "")[:80],
                    )
                    _LAST_PASS_FIELD_LOG_AT = now
                return False
        # UI mới / Business Suite: đôi khi không có [role=navigation] trên www nhưng cookie phiên vẫn hợp lệ.
        if has_c_user and "xs" in names and "facebook.com" in u:
            if "/login" not in u and "/checkpoint" not in u and "two_step" not in u:
                logger.info("[FB] Cookie c_user+xs, không form đăng nhập — coi như đã đăng nhập (bỏ qua chờ DOM feed).")
                return True
        # Profile đăng nhập tay: đôi khi có c_user trước khi xs/DOM feed kịp load.
        if has_c_user and "facebook.com" in u:
            if "/login" not in u and "/checkpoint" not in u and "two_step" not in u:
                try:
                    page.wait_for_timeout(900)
                except Exception:
                    pass
                names2 = _facebook_context_cookie_names(page)
                if "c_user" in names2:
                    logger.info(
                        "[FB] Cookie c_user ổn định (url={}) — coi như đã đăng nhập (không chờ DOM feed).",
                        (page.url or "")[:80],
                    )
                    return True
    except Exception:
        pass
    try:
        dom_wait = max(3_000, min(60_000, _env_int("FB_SESSION_DOM_WAIT_MS", 18_000)))
        page.wait_for_selector(
            "[role='navigation'], [role='banner'], [role='main'], [role='feed'], "
            "a[href*='/me/'], a[aria-label*='Account'], a[aria-label*='account']",
            state="visible",
            timeout=dom_wait,
        )
        return True
    except Exception:
        return False


def ensure_facebook_session_for_post(
    page: Page,
    cookie_path: str | Path | None,
    account: dict[str, Any] | None = None,
) -> None:
    """
    Ưu tiên phiên profile → refresh → cookie → đăng nhập lại (email/password [+ TOTP]) tối đa một lần.

    Raises:
        RuntimeError: Không thể tiếp tục (checkpoint, cookie hết hạn, v.v.) — nên đánh dấu need_manual_check.
        FileNotFoundError: Cần cookie nhưng file không có.
    """
    from src.services.facebook_recaptcha import (
        auto_solve_facebook_recaptcha_if_present,
        wait_for_recaptcha_and_solve,
    )
    from src.services.facebook_session_recovery import (
        facebook_page_is_hard_checkpoint,
        try_recover_facebook_session,
    )
    from src.utils.account_credentials import account_can_auto_reauth

    def _raise_checkpoint(msg: str) -> None:
        raise RuntimeError(f"{msg} need_manual_check")

    def _raise_interstitial_no_creds(stage: str) -> None:
        _raise_checkpoint(
            f"FACEBOOK_2FA_OR_CHECKPOINT ({stage}): Facebook yêu cầu xác minh (2FA/checkpoint/đăng nhập lại). "
            "Hoàn tất tay trên profile hoặc cấu hình email + mật khẩu (+ TOTP) trong tài khoản để tự đăng nhập lại."
        )

    auto_reauth_tried = False

    def _try_auto_reauth(stage: str) -> bool:
        nonlocal auto_reauth_tried
        if auto_reauth_tried or not account or not account_can_auto_reauth(account):
            return False
        auto_reauth_tried = True
        logger.info("[FB] ensure_session: thử đăng nhập lại ({})", stage)
        return bool(try_recover_facebook_session(page, account, cookie_path=cookie_path))

    _human_pause()
    logger.info("[FB] ensure_session: url={!r}", page.url)
    # Chủ động thử xử lý nếu trang hiện tại đã chứa reCAPTCHA.
    if account:
        auto_solve_facebook_recaptcha_if_present(page, account, stage="ensure_session:start")
    url_now = page.url or ""
    if facebook_page_is_hard_checkpoint(url_now):
        if account and wait_for_recaptcha_and_solve(
            page, account, stage="ensure_session:checkpoint", wait_timeout_ms=12_000
        ):
            if facebook_session_appears_logged_in(page):
                logger.info("[FB] CapSolver đã xử lý reCAPTCHA — tiếp tục phiên.")
                return
        _raise_checkpoint(
            "FACEBOOK_CHECKPOINT: Meta yêu cầu xác minh (checkpoint/captcha/danh tính) — "
            "cấu hình CapSolver (TOOLFB_CAPSOLVER_API_KEY) hoặc xử lý tay."
        )
    if facebook_session_appears_logged_in(page):
        logger.info("[FB] Profile vẫn đăng nhập — bỏ qua nạp cookie.")
        return
    if _facebook_url_is_security_interstitial(url_now):
        if account and wait_for_recaptcha_and_solve(
            page, account, stage="ensure_session:interstitial", wait_timeout_ms=12_000
        ):
            if facebook_session_appears_logged_in(page):
                return
        if _try_auto_reauth("initial_interstitial"):
            return
        _raise_interstitial_no_creds("đầu phiên")
    if _try_auto_reauth("not_logged_in"):
        return
    try:
        page.reload(wait_until="domcontentloaded", timeout=45_000)
        _force_www_facebook_if_mobile_redirect(page)
        if facebook_page_is_hard_checkpoint(page.url or ""):
            if account and wait_for_recaptcha_and_solve(
                page, account, stage="ensure_session:after_refresh_checkpoint", wait_timeout_ms=12_000
            ):
                if facebook_session_appears_logged_in(page):
                    return
            _raise_checkpoint("FACEBOOK_CHECKPOINT: Sau refresh vẫn ở checkpoint Meta.")
        if facebook_session_appears_logged_in(page):
            logger.info("[FB] Sau refresh, profile đã đăng nhập — bỏ qua nạp cookie.")
            return
        if _facebook_url_is_security_interstitial(page.url or ""):
            if account and wait_for_recaptcha_and_solve(
                page,
                account,
                stage="ensure_session:after_refresh_interstitial",
                wait_timeout_ms=12_000,
            ):
                if facebook_session_appears_logged_in(page):
                    return
            if _try_auto_reauth("after_refresh"):
                return
            _raise_interstitial_no_creds("sau refresh")
    except RuntimeError:
        raise
    except Exception:
        pass
    allow = _env_bool("FB_ALLOW_COOKIE_RESTORE", True)
    if not allow:
        raise RuntimeError(
            "Mất phiên Facebook và FB_ALLOW_COOKIE_RESTORE=0 — cần đăng nhập tay trên profile hoặc bật khôi phục cookie."
        )
    raw = str(cookie_path or "").strip()
    if not raw:
        raise RuntimeError(
            "Mất phiên Facebook và không có cookie_path — đăng nhập vào profile hoặc cấu hình cookie."
        )
    path = _resolve_path(raw)
    if not path.is_file():
        raise FileNotFoundError(str(path))
    logger.info("[FB] Thử khôi phục phiên bằng cookie: {}", path)
    login_with_cookie(page, path)
    if facebook_page_is_hard_checkpoint(page.url or ""):
        if account and wait_for_recaptcha_and_solve(
            page, account, stage="ensure_session:after_cookie_checkpoint", wait_timeout_ms=12_000
        ):
            if facebook_session_appears_logged_in(page):
                return
        _raise_checkpoint("FACEBOOK_CHECKPOINT: Sau nạp cookie vẫn ở checkpoint Meta.")
    if _facebook_url_is_security_interstitial(page.url or ""):
        if account and wait_for_recaptcha_and_solve(
            page,
            account,
            stage="ensure_session:after_cookie_interstitial",
            wait_timeout_ms=12_000,
        ):
            if facebook_session_appears_logged_in(page):
                return
        if _try_auto_reauth("after_cookie"):
            return
        _raise_interstitial_no_creds("sau nạp cookie")
    if facebook_session_appears_logged_in(page):
        return
    pause_ms = max(0, min(15_000, _env_int("FB_POST_COOKIE_SESSION_WAIT_MS", 1_200)))
    if pause_ms:
        page.wait_for_timeout(pause_ms)
    if facebook_session_appears_logged_in(page):
        logger.info("[FB] Phiên OK sau chờ bổ sung (FB_POST_COOKIE_SESSION_WAIT_MS).")
        return
    try:
        home = _fb_normalize_client_url("https://www.facebook.com/")
        assert_safe_facebook_navigation_url(home, label="ensure_session_post_cookie_home")
        page.goto(home, wait_until="domcontentloaded", timeout=60_000)
        _force_www_facebook_if_mobile_redirect(page)
        _human_pause()
    except Exception as exc:  # noqa: BLE001
        logger.info("[FB] ensure_session: goto www bổ sung sau cookie bỏ qua: {}", exc)
    if facebook_session_appears_logged_in(page):
        logger.info("[FB] Phiên OK sau goto www bổ sung (sau nạp cookie).")
        return
    if _try_auto_reauth("final"):
        return
    if account and auto_solve_facebook_recaptcha_if_present(page, account, stage="ensure_session:final"):
        if facebook_session_appears_logged_in(page):
            return
    _log_facebook_session_diagnostic(page, stage="after_cookie_restore")
    raise RuntimeError(
        "Sau khi nạp cookie vẫn không có phiên hợp lệ — có thể checkpoint hoặc cookie hết hạn (need_manual_check). "
        "Xem log «Chẩn đoán phiên»: nếu thiếu c_user/xs, hãy «Lấy cookie (Playwright)» lại; nếu có c_user nhưng vẫn lỗi, "
        "mở profile Firefox tay, đăng nhập Facebook, hoàn tất hộp thoại Meta (cookie/consent/checkpoint), rồi chạy lại job."
    )


def _load_playwright_cookies(cookie_path: Path) -> list[dict[str, Any]]:
    """
    Đọc file JSON cookie (mảng cookie hoặc object có khóa ``cookies``).

    Args:
        cookie_path: File JSON.

    Returns:
        Danh sách dict đủ trường cho ``BrowserContext.add_cookies``.

    Raises:
        ValueError: Cấu trúc file không hợp lệ.
        FileNotFoundError: File không tồn tại.
    """
    raw = json.loads(cookie_path.read_text(encoding="utf-8"))
    if isinstance(raw, dict) and "cookies" in raw:
        raw = raw["cookies"]
    if not isinstance(raw, list):
        raise ValueError("File cookie phải là mảng JSON hoặc object chứa khóa 'cookies'.")
    return raw  # type: ignore[return-value]


def login_with_cookie(page: Page, cookie_path: str | Path) -> None:
    """
    Mở Facebook, nạp cookie từ file rồi tải lại trang để tái sử dụng phiên.

    Ưu tiên nạp cookie trước khi đăng nhập thủ công (theo quy tắc dự án).

    Args:
        page: Trang Playwright.
        cookie_path: Đường tới JSON cookie (tương đối hoặc tuyệt đối).

    Raises:
        FileNotFoundError / ValueError / PlaywrightTimeoutError: Theo từng bước thất bại.
    """
    path = _resolve_path(cookie_path)
    try:
        from src.services.facebook_session_persist import profile_session_ready_for_interaction

        ok_prof, prof_detail = profile_session_ready_for_interaction(page)
        if ok_prof:
            logger.info(
                "[FB] login_with_cookie — bỏ qua nạp file, profile đã có phiên: {}",
                prof_detail,
            )
            _enable_view_only_guard(page)
            return
        cookies = _load_playwright_cookies(path)
        start_fb = _fb_normalize_client_url("https://www.facebook.com/")
        assert_safe_facebook_navigation_url(start_fb, label="login_with_cookie")
        page.goto(start_fb, wait_until="domcontentloaded", timeout=60_000)
        _force_www_facebook_if_mobile_redirect(page)
        _wait_selector_or_fail(page, "[role='main'], body", timeout_ms=45_000)
        page.context.add_cookies(cookies)
        logger.info("Đã nạp {} cookie từ {}", len(cookies), path)
        _human_pause()
        page.reload(wait_until="domcontentloaded", timeout=60_000)
        _force_www_facebook_if_mobile_redirect(page)
        # m.facebook (Firefox/mobile) thường không có logo [aria-label='Facebook'][role='img'] — ưu tiên khung chuẩn.
        _wait_first_selector(
            page,
            (
                "[role='banner']",
                "[role='navigation']",
                "[role='main']",
                "[role='feed']",
                "xpath=//div[@role='navigation']",
                "xpath=//div[@role='main']",
                "xpath=//div[@role='feed']",
                "a[aria-label*='Facebook']",
                "[aria-label='Facebook'][role='img']",
                "[aria-label='Facebook']",
                "article",
                "body",
            ),
            step_timeout_ms=15_000,
            error_label="login_with_cookie sau reload",
        )
        _enable_view_only_guard(page)
    except (PlaywrightTimeoutError, FileNotFoundError, ValueError):
        _enable_view_only_guard(page)
        raise
    except Exception as exc:
        _enable_view_only_guard(page)
        _failure_screenshot(page, f"login_with_cookie: {exc}")
        raise


def go_to_home(page: Page) -> None:
    """
    Điều hướng về trang chủ / bảng tin Facebook và chờ khung nội dung chính.

    Args:
        page: Trang Playwright.

    Raises:
        PlaywrightTimeoutError: Timeout điều hướng hoặc không thấy khung chính.
    """
    try:
        home = _fb_normalize_client_url("https://www.facebook.com/")
        assert_safe_facebook_navigation_url(home, label="go_to_home")
        page.goto(home, wait_until="domcontentloaded", timeout=60_000)
        _force_www_facebook_if_mobile_redirect(page)
        _wait_first_selector(
            page,
            (
                "[role='main']",
                "[role='feed']",
                "[role='navigation']",
                "[role='banner']",
                "xpath=//div[@role='main']",
                "xpath=//div[@role='feed']",
            ),
            step_timeout_ms=20_000,
            error_label="go_to_home",
        )
        _human_pause()
        _enable_view_only_guard(page)
    except PlaywrightTimeoutError:
        _enable_view_only_guard(page)
        raise
    except Exception as exc:
        _enable_view_only_guard(page)
        _failure_screenshot(page, f"go_to_home: {exc}")
        raise


def navigate_to_url(page: Page, url: str) -> None:
    """
    Điều hướng tới URL (Page / Group / liên kết Facebook) và chờ khung nội dung.

    Args:
        page: Trang Playwright.
        url: URL đầy đủ (https…).

    Raises:
        ValueError: URL không hợp lệ.
        PlaywrightTimeoutError: Timeout tải trang / selector.
    """
    u = _fb_normalize_client_url(str(url).strip())
    if not u.startswith("http"):
        raise ValueError("URL phải bắt đầu bằng http:// hoặc https://")
    assert_safe_facebook_navigation_url(u, label="navigate_to_url")
    is_biz_composer = _is_meta_business_composer_url(u)
    try:
        logger.info("[FB] navigate_to_url goto={!r}", u)
        # Business composer thường treo ở network idle/load; dùng timeout mềm để không đứng im.
        if is_biz_composer:
            logger.info("[FB] Business composer navigation strategy: non-blocking assign + soft waits.")
            nav_ok = False
            try:
                page.goto(u, wait_until="commit", timeout=20_000)
                nav_ok = True
            except Exception as exc:  # noqa: BLE001
                logger.warning("[FB] goto(commit) lỗi/timeout: {} — thử location.assign.", exc)
            if not nav_ok:
                try:
                    page.evaluate("(dst) => { window.location.assign(dst); }", u)
                    page.wait_for_timeout(3_000)
                    nav_ok = True
                except Exception as exc:  # noqa: BLE001
                    logger.warning("[FB] location.assign lỗi: {}", exc)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=15_000)
            except Exception:
                logger.warning("[FB] Business composer chưa đạt domcontentloaded sau timeout mềm, vẫn tiếp tục.")
        else:
            page.goto(u, wait_until="load", timeout=90_000)
        _force_www_facebook_if_mobile_redirect(page)
        if not is_biz_composer:
            try:
                _wait_fb_path_matches(page, u)
            except PlaywrightTimeoutError:
                if _urls_refer_same_facebook_page(u, str(page.url or ""), page=page):
                    logger.info(
                        "[FB] Đã ở đúng Page sau redirect (không goto lần 2): expect={!r} | now={}",
                        u,
                        page.url,
                    )
                else:
                    logger.warning("[FB] Thử goto lần 2 (load) tới {!r}", u)
                    page.goto(u, wait_until="load", timeout=90_000)
                    _force_www_facebook_if_mobile_redirect(page)
                    _wait_fb_path_matches(page, u)
        else:
            logger.info("[FB] Business composer: bỏ qua kiểm tra path cứng, chuyển sang kiểm tra composer.")
            _wait_meta_business_composer_ready(page, timeout_ms=400)
            logger.info("[FB] Business composer: tiếp tục ngay sang fill_content/media (không chờ).")
            _human_pause()
            _enable_view_only_guard(page)
            return
        _wait_first_selector(
            page,
            (
                "[role='main']",
                "[role='feed']",
                "[role='navigation']",
                "[role='banner']",
                # Business composer anchors
                "div[role='combobox'][contenteditable='true']",
                "div[role='button']:has-text('Add photo/video')",
                "div[role='button']:has-text('Publish')",
                "article",
                "body",
            ),
            step_timeout_ms=18_000 if is_biz_composer else 45_000,
            error_label="navigate_to_url",
        )
        logger.info("[FB] navigate_to_url xong: {}", page.url)
        _human_pause()
        _enable_view_only_guard(page)
    except PlaywrightTimeoutError:
        _enable_view_only_guard(page)
        raise
    except Exception as exc:
        _enable_view_only_guard(page)
        _failure_screenshot(page, f"navigate_to_url: {exc}")
        raise


def _try_navigate_via_page_name_link(page: Page, page_name: str, dest: str) -> bool:
    """Khi URL chưa khớp đích, thử bấm control có nhãn chứa tên Page (UI chuyển Page)."""
    pn = page_name.strip()
    if len(pn) < 2:
        return False
    try:
        link = page.get_by_role("link", name=re.compile(re.escape(pn), re.I)).first
        if link.is_visible(timeout=2500):
            human_pause(kind="click", label="chọn Page (link)")
            link.click(timeout=8000, force=True)
            page.wait_for_timeout(_reel_inter_click_wait_ms())
            if _is_on_target_surface(page, dest):
                return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("[FB] Fallback tên Page (link): {}", exc)
    try:
        btn = page.get_by_role("button", name=re.compile(re.escape(pn), re.I)).first
        if btn.is_visible(timeout=1500):
            human_pause(kind="click", label="chọn Page (nút)")
            btn.click(timeout=6000, force=True)
            page.wait_for_timeout(_reel_inter_click_wait_ms())
            return _is_on_target_surface(page, dest)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[FB] Fallback tên Page (button): {}", exc)
    return False


def go_to_posting_target_and_open_composer(
    page: Page,
    entity: dict[str, Any] | None,
    *,
    page_display_name: str | None = None,
) -> None:
    """
    Tới đúng bề mặt đăng bài rồi mở composer.

    - Không ``entity`` hoặc ``target_type`` = ``timeline``: ``go_to_home`` + ``open_post_box``.
    - ``fanpage`` / ``group``: ``navigate_to_url`` theo ``target_url`` + ``open_post_box`` trên Page/Group.
    - ``target_url`` trỏ tới ``business.facebook.com/.../composer`` (Meta Business): sau khi tải trang,
      nếu ô soạn đã hiện thì bỏ qua ``open_post_box``.

    Args:
        page: Trang sau khi đã nạp cookie.
        entity: Bản ghi entity (dict) hoặc ``None`` cho luồng timeline mặc định.
    """
    if entity is None:
        go_to_home(page)
        open_post_box(page)
        return
    tt = str(entity.get("target_type", "timeline")).strip().lower()
    raw_target = str(entity.get("target_url", "")).strip()
    go_surface = tt in ("fanpage", "group") or (
        tt == "timeline" and raw_target and _facebook_url_points_at_surface(raw_target)
    )
    logger.info(
        "[FB] go_to_posting_target: type={} | surface={} | raw_url={!r} | url_now={}",
        tt,
        go_surface,
        raw_target,
        page.url,
    )
    if not go_surface:
        go_to_home(page)
    else:
        dest = _fb_normalize_client_url(raw_target)
        logger.info("[FB] Đích đăng: target_type={} | goto={!r}", tt, dest)
        pname = (page_display_name or "").strip()
        navigate_to_url(page, dest)
        logger.info("[FB] Sau navigate_to_url: url_now={}", page.url)
        ok = _robust_switch_to_target_page(page, page_display_name=pname, page_url=dest)
        if dest and (
            not ok
            or not _is_on_target_surface(page, dest)
            or not _page_role_acting_as_page(page, timeout_ms=700)
        ):
            logger.warning(
                "[FB] Switch/navigate lần 1 chưa OK — thử điều hướng lại target_url một lần."
            )
            navigate_to_url(page, dest)
            page.wait_for_timeout(1_200)
            _robust_switch_to_target_page(page, page_display_name=pname, page_url=dest)
        if dest and (
            not _is_on_target_surface(page, dest)
            or not _page_role_acting_as_page(page, timeout_ms=900)
        ):
            _failure_screenshot(page, f"go_to_posting_target: chưa vào đúng page đích {dest}")
            raise PlaywrightTimeoutError(
                f"Chưa vào đúng page đích hoặc chưa switch vai trò Page: {dest}"
            )
    raw_tgt = str((entity or {}).get("target_url", "")).strip()
    norm_tgt = _fb_normalize_client_url(raw_tgt) if raw_tgt else ""
    if norm_tgt and _is_meta_business_composer_url(norm_tgt):
        composer_wait = max(5_000, min(120_000, _env_int("FB_META_BUSINESS_COMPOSER_WAIT_MS", 55_000)))
        if _wait_meta_business_composer_ready(page, timeout_ms=composer_wait):
            _enable_view_only_guard(page)
            return
        cur_body = ""
        try:
            cur_body = (page.content() or "").lower()
        except Exception:
            cur_body = ""
        unavailable = ("content isn't available right now" in cur_body) or ("nội dung này hiện không có" in cur_body)
        fbk = str((entity or {}).get("fallback_target_url", "")).strip()
        fbk_norm = _fb_normalize_client_url(fbk) if fbk else ""
        if unavailable and fbk_norm and not _is_meta_business_composer_url(fbk_norm):
            logger.warning(
                "[FB] Business composer không truy cập được (asset/permission). Fallback sang Page URL: {}",
                fbk_norm,
            )
            navigate_to_url(page, fbk_norm)
            _robust_switch_to_target_page(
                page,
                page_display_name=(page_display_name or "").strip(),
                page_url=fbk_norm,
            )
            open_post_box(page)
            return
        logger.warning(
            "[FB] Meta Business composer chưa sẵn sàng sau {}ms — tiếp tục pipeline (fill_content / media có thể chờ thêm).",
            composer_wait,
        )
        _enable_view_only_guard(page)
        return
    open_post_box(page)


def _is_meta_business_composer_url(url: str) -> bool:
    """URL trình soạn Meta Business (Professional dashboard), ví dụ ``/latest/composer``."""
    low = str(url or "").strip().lower()
    return "business.facebook.com" in low and "composer" in low


def _is_meta_business_composer_context(page: Page) -> bool:
    """Heuristic nhận diện đang ở UI Business Composer."""
    try:
        u = str(page.url or "").strip().lower()
        if _is_meta_business_composer_url(u):
            return True
        if "business.facebook.com" not in u:
            return False
        if page.get_by_text("Create post", exact=False).first.is_visible(timeout=1200):
            return True
    except Exception:
        pass
    return False


def _dismiss_blocking_ui_before_business_media(page: Page) -> None:
    """
    Giữa các job / sau Reel: đóng dialog processing, popup "more posts", menu, overlay
    và nhẹ nhàng kích hoạt composer textbox để toolbar Add photo/video render lại.

    Đặc biệt cần thiết khi job 2+ nav về cùng một Business Composer URL ngay sau khi
    job trước publish: Facebook đôi khi giữ popup gợi ý đăng thêm hoặc hiển thị
    composer ở state rút gọn (ẩn nút Add media cho đến khi user click vào ô soạn).
    """
    try:
        dismiss_meta_video_post_processing_modal_best_effort(
            page, timeout_ms=8_000, give_up_if_never_seen_ms=2_000
        )
    except Exception:
        pass
    # Popup "Are there more posts you want to publish?" / "bài viết khác muốn đăng".
    try:
        dismiss_meta_more_posts_prompt_best_effort(page, probe_timeout_ms=2_500)
    except Exception:
        pass
    for _ in range(3):
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(220)
        except Exception:
            break
    # Kích hoạt composer: click vào textbox để FB mở rộng toolbar (bao gồm nút
    # Add photo/video). Một số state chỉ render toolbar sau tương tác đầu tiên.
    textbox_selectors = (
        "div[role='combobox'][contenteditable='true']",
        "div[role='textbox'][contenteditable='true']",
        "[role='textbox'][aria-multiline='true']",
    )
    for sel in textbox_selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible(timeout=800):
                try:
                    loc.click(timeout=1_500, force=True, no_wait_after=True)
                except Exception:
                    try:
                        loc.evaluate("el => { if (el && el.focus) el.focus(); }")
                    except Exception:
                        pass
                try:
                    page.wait_for_timeout(450)
                except Exception:
                    pass
                break
        except Exception:
            continue


def _open_business_add_photo_video(page: Page) -> None:
    """
    Mở action "Add photo/video" trong Business Composer trước khi set input file.

    LƯU Ý: hàm này có thể kích hoạt native file dialog nếu click đúng nút upload.
    Chỉ gọi nó bên trong ``with page.expect_file_chooser(...)`` để Playwright chặn
    popup OS (opt-in native fallback). Không bao giờ gọi trực tiếp bên ngoài.
    """
    if not _native_file_chooser_allowed():
        logger.debug(
            "[FB] Bỏ qua _open_business_add_photo_video do FB_ALLOW_NATIVE_FILE_CHOOSER=0 "
            "(tránh mở native dialog ngoài ý muốn)."
        )
        return
    try:
        cb = page.locator("div[role='combobox'][contenteditable='true']").first
        if cb.is_visible(timeout=2_000):
            cb.click(timeout=5_000, force=True)
            page.wait_for_timeout(350)
    except Exception:
        pass
    selectors = (
        "button:has-text('Add photo/video')",
        "button:has-text('Add photos/videos')",
        "button:has-text('Add media')",
        "button:has-text('Photo/video')",
        "div[role='button']:has-text('Add photo/video')",
        "[role='button']:has-text('Add photo/video')",
        "div[role='button']:has-text('Thêm ảnh/video')",
        "[role='button']:has-text('Thêm ảnh/video')",
        "div[role='button']:has-text('Photo/video')",
        "[role='button']:has-text('Photo/video')",
        "div[role='button']:has-text('Add photos')",
        "[role='button']:has-text('Add media')",
    )
    name_patterns = (
        r"(add photo/video|thêm ảnh/video)",
        r"photo\s*/\s*video",
        r"ảnh\s*/\s*video",
        r"add photos?\s+and\s+videos?",
        r"add media",
        r"thêm\s+ảnh",
    )
    for pat in name_patterns:
        try:
            btn = page.get_by_role("button", name=re.compile(pat, re.I)).first
            btn.scroll_into_view_if_needed(timeout=3_000)
            if btn.is_visible(timeout=2_000):
                btn.click(timeout=10_000, force=True)
                page.wait_for_timeout(900)
                return
        except Exception:
            continue
    # Fallback text match cho UI không expose role button chuẩn.
    for css in (
        "button",
        "div[role='button']",
        "[role='button']",
    ):
        try:
            locs = page.locator(css).filter(has_text=re.compile(r"(add|photo|video|media|thêm|ảnh)", re.I))
            n = min(locs.count(), 24)
            for i in range(n - 1, -1, -1):
                b = locs.nth(i)
                if not b.is_visible(timeout=600):
                    continue
                try:
                    b.scroll_into_view_if_needed(timeout=1_500)
                except Exception:
                    pass
                try:
                    b.click(timeout=6_000, force=True)
                except Exception:
                    b.evaluate("el => { if (el && el.click) el.click(); }")
                page.wait_for_timeout(900)
                return
        except Exception:
            continue
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            loc.scroll_into_view_if_needed(timeout=3_000)
            if not loc.is_visible(timeout=1_800):
                continue
            loc.click(timeout=10_000, force=True)
            page.wait_for_timeout(900)
            return
        except Exception:
            continue
    logger.warning("[FB] Không thấy / không bấm được nút Add photo|video trong Business Composer.")


def _accept_allows_video(accept: str) -> bool:
    a = (accept or "").strip().lower()
    if not a:
        return True
    if "video" in a or "audio" in a:
        return True
    if "*" in a or "/*" in a:
        return True
    # image-only (không có video / wildcard)
    if "image" in a and "video" not in a and "/" not in a:
        return False
    return True


def _accept_allows_image(accept: str) -> bool:
    a = (accept or "").strip().lower()
    if not a:
        return True
    if "image" in a or "*" in a or "/*" in a:
        return True
    if "video" in a and "image" not in a:
        return False
    return True


def _set_file_via_existing_input(
    page: Page,
    file_path: Path,
    *,
    kind: Literal["image", "video"],
    scope: Locator | None = None,
) -> bool:
    """
    Thử set file trực tiếp vào input[type=file] đã có sẵn (không mở native picker).

    Meta đôi khi để ``accept`` rỗng hoặc MIME lạ — job sau cùng profile cần thử lỏng hơn.
    ``scope`` (vd. ``[role=dialog]`` Reel) được quét trước — input thường ẩn nhưng vẫn ``set_input_files`` được.
    """
    def _try_one(loc: Locator) -> bool:
        try:
            if not loc.count():
                return False
            dis = (loc.get_attribute("disabled") or "").lower()
            if dis == "true":
                return False
            loc.set_input_files(str(file_path), timeout=18_000)
            return True
        except Exception:
            return False

    def _scan_inputs_container(container) -> bool:
        try:
            inputs = container.locator("input[type='file']")
            n = inputs.count()
        except Exception:
            return False
        if n <= 0:
            return False
        for i in range(n):
            try:
                loc = inputs.nth(i)
                accept = str(loc.get_attribute("accept") or "").lower()
                if kind == "image" and not _accept_allows_image(accept):
                    continue
                if kind == "video" and not _accept_allows_video(accept):
                    continue
                if _try_one(loc):
                    return True
            except Exception:
                continue
        if kind == "video":
            for i in range(n):
                try:
                    loc = inputs.nth(i)
                    accept = str(loc.get_attribute("accept") or "").lower()
                    if accept and "image" in accept and "video" not in accept and "*" not in accept and "/" not in accept:
                        continue
                    if _try_one(loc):
                        return True
                except Exception:
                    continue
        return False

    if scope is not None:
        for cand in (scope.locator("input[type='file']").last, scope.locator("input[type='file']").first, scope):
            if _scan_inputs_container(cand):
                return True

    # Một số UI Business render input bên trong iframe/portal -> quét tất cả frame.
    for fr in [page.main_frame, *list(page.frames)]:
        try:
            inputs = fr.locator("input[type='file']")
            n = inputs.count()
        except Exception:
            continue
        if n <= 0:
            continue
        for i in range(n):
            try:
                loc = inputs.nth(i)
                accept = str(loc.get_attribute("accept") or "").lower()
                if kind == "image" and not _accept_allows_image(accept):
                    continue
                if kind == "video" and not _accept_allows_video(accept):
                    continue
                if _try_one(loc):
                    return True
            except Exception:
                continue
        if kind == "video":
            for i in range(n):
                try:
                    loc = inputs.nth(i)
                    accept = str(loc.get_attribute("accept") or "").lower()
                    if accept and "image" in accept and "video" not in accept and "*" not in accept and "/" not in accept:
                        continue
                    if _try_one(loc):
                        return True
                except Exception:
                    continue
    return False


def _collect_add_media_button_locators(page: Page, *, scope: Locator | None = None) -> list[Locator]:
    """
    Trả danh sách các locator có khả năng mở filechooser cho media (khởi tạo + "Add more").

    Chiến lược: rộng nhưng không bừa — chỉ lấy element ``button`` / ``[role='button']``
    có text hoặc aria-label chứa từ khóa media. Dùng khi cần click từng cái trong
    ``expect_file_chooser`` để bảo đảm **native dialog không bao giờ thoát ra ngoài**.

    Duplicates được loại bỏ dựa trên bounding-box.
    """
    out: list[Locator] = []
    seen_keys: set[str] = set()
    search_roots: list[Any] = [scope] if scope is not None else [page]

    def _push(loc: Locator) -> None:
        try:
            n = loc.count()
        except Exception:
            return
        for i in range(min(n, 10)):
            cand = loc.nth(i)
            try:
                if not cand.is_visible(timeout=400):
                    continue
            except Exception:
                continue
            try:
                box = cand.bounding_box()
            except Exception:
                box = None
            key = f"{box}" if box else f"{id(cand)}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            out.append(cand)

    # 1) role=button theo name (text hiển thị)
    name_rx = re.compile(
        r"(add\s+photo|add\s+photos|photo\s*/\s*video|add\s+media|"
        r"thêm\s+ảnh|ảnh\s*/\s*video|add\s+more|thêm\s+nữa|add\s+video|"
        r"^upload$|or drag and drop)",
        re.I,
    )
    for root in search_roots:
        if root is None:
            continue
        try:
            _push(root.get_by_role("button", name=name_rx))
        except Exception:
            pass
        try:
            _push(root.get_by_text(re.compile(r"Add video|or drag and drop", re.I)))
        except Exception:
            pass

    # 2) [role=button] với aria-label chứa từ khóa media
    for aria_rx in (r"photo", r"video", r"media", r"image", r"ảnh"):
        for root in search_roots:
            if root is None:
                continue
            try:
                _push(root.locator(f"[role='button'][aria-label*='{aria_rx}' i]"))
            except Exception:
                continue
            try:
                _push(root.locator(f"button[aria-label*='{aria_rx}' i]"))
            except Exception:
                continue

    # 3) selector text-based cho các biến thể phổ biến
    text_selectors = (
        "button:has-text('Add photo/video')",
        "button:has-text('Add photos/videos')",
        "[role='button']:has-text('Add photo/video')",
        "[role='button']:has-text('Add photos/videos')",
        "[role='button']:has-text('Thêm ảnh/video')",
        "[role='button']:has-text('Photo/video')",
        "[role='button']:has-text('Add more')",
        "[role='button']:has-text('Thêm nữa')",
        "[role='button']:has-text('Upload')",
    )
    for root in search_roots:
        if root is None:
            continue
        for css in text_selectors:
            try:
                _push(root.locator(css))
            except Exception:
                continue

    return out


def _set_file_via_business_add_button(
    page: Page,
    file_path: Path,
    *,
    kind: Literal["image", "video"],
    scope: Locator | None = None,
) -> bool:
    """
    Bấm candidate "Add media" trong context ``expect_file_chooser`` để set file tự động.

    An toàn với native dialog: **mọi** click đều nằm trong ``expect_file_chooser``, nếu UI
    phát native file dialog Playwright sẽ chặn và gọi ``set_files`` qua event — user KHÔNG
    thấy popup OS. Nếu click không phát filechooser mà render ``input[type=file]`` mới,
    hàm re-scan bằng ``_set_file_via_existing_input``.
    """
    for attempt in range(1, 4):
        # Thử input[type=file] hiện có trước mỗi vòng — đôi khi Meta render lại DOM.
        if _set_file_via_existing_input(page, file_path, kind=kind, scope=scope):
            logger.info(
                "[FB] Đã set file qua input[type=file] re-scan (attempt={}): {}",
                attempt,
                file_path,
            )
            return True

        candidates = _collect_add_media_button_locators(page, scope=scope)
        if not candidates:
            page.wait_for_timeout(400)
            continue
        for cand in candidates:
            # Luôn bọc click trong expect_file_chooser để native dialog không thoát ra OS.
            try:
                with page.expect_file_chooser(timeout=6_500) as fc_info:
                    try:
                        cand.click(timeout=4_500, force=True, no_wait_after=True)
                    except Exception:
                        cand.evaluate("el => { if (el && el.click) el.click(); }")
                fc_info.value.set_files(str(file_path))
                logger.info(
                    "[FB] Đã set file qua filechooser interception (attempt={}): {}",
                    attempt,
                    file_path,
                )
                return True
            except Exception:
                _dismiss_leaked_native_file_dialog(page)
                # Không phát filechooser → có thể chỉ render input mới; re-scan ngay.
                try:
                    page.wait_for_timeout(350)
                except Exception:
                    pass
                if _set_file_via_existing_input(page, file_path, kind=kind, scope=scope):
                    logger.info(
                        "[FB] Đã set file qua input xuất hiện sau click Add media (attempt={}): {}",
                        attempt,
                        file_path,
                    )
                    return True
                continue
        # Không candidate nào phát filechooser hoặc render input — nghỉ ngắn rồi thử lại.
        page.wait_for_timeout(500)
    return False


def _attach_media_automatic(
    page: Page,
    file_path: str | Path,
    *,
    kind: Literal["image", "video"],
    scope: Locator | None = None,
    context: str = "",
) -> bool:
    """
    Gắn media tự động — chỉ ``set_input_files`` / intercept filechooser, không hiện hộp chọn file OS.

    Dùng cho Business Composer và popup Reel dashboard (``scope`` = dialog).
    """
    path = _resolve_path(file_path)
    if not path.is_file():
        raise FileNotFoundError(str(path))
    tag = f"[FB attach{(' ' + context) if context else ''}]"
    in_business = _is_meta_business_composer_context(page)
    if in_business and scope is None:
        _dismiss_blocking_ui_before_business_media(page)

    def _attach_ok() -> bool:
        if kind == "video":
            _mute_browser_video_previews_after_attach(page, scope=scope)
        return True

    for round_i in range(3):
        if _set_file_via_existing_input(page, path, kind=kind, scope=scope):
            logger.info("{} OK input trực tiếp (round={}): {}", tag, round_i + 1, path)
            return _attach_ok()
        try:
            page.wait_for_timeout(280)
        except Exception:
            pass

    if _set_file_via_business_add_button(page, path, kind=kind, scope=scope):
        logger.info("{} OK qua filechooser interception: {}", tag, path)
        return _attach_ok()

    if scope is not None:
        return _attach_media_automatic(
            page,
            path,
            kind=kind,
            scope=None,
            context=f"{context}_page" if context else "page",
        )

    if in_business and _native_file_chooser_allowed():
        try:
            with page.expect_file_chooser(timeout=10_000) as fc_info:
                _open_business_add_photo_video(page)
            fc_info.value.set_files(str(path))
            logger.info("{} OK filechooser opt-in native: {}", tag, path)
            return _attach_ok()
        except Exception:
            _dismiss_leaked_native_file_dialog(page)

    return False


def _wait_meta_business_composer_ready(page: Page, *, timeout_ms: int = 35_000) -> bool:
    """
    Business Suite thường mở sẵn composer — không cần bấm nút “What's on your mind” của bảng tin cá nhân.

    Dùng ngân sách thời gian tổng (``timeout_ms``), lần lượt thử các anchor; trước đây ``timeout_ms=300``
    + ``return`` sớm trong ``go_to_posting_target`` khiến luồng không bao giờ chờ composer load xong.
    """
    try:
        checks = (
            "div[role='combobox'][contenteditable='true']",
            "div[role='textbox'][contenteditable='true']",
            "div[role='button']:has-text('Add photo/video')",
            "div[role='button']:has-text('Publish')",
        )
        deadline = time.monotonic() + max(0.5, float(timeout_ms) / 1000.0)
        stable_hits = 0
        while time.monotonic() < deadline:
            u = str(page.url or "").strip().lower()
            if _facebook_url_is_security_interstitial(u) or "/login" in u:
                logger.warning("[FB] Composer check: URL chưa qua login/checkpoint: {}", page.url)
                return False
            names = _facebook_context_cookie_names(page)
            if "c_user" not in names or "xs" not in names:
                # Chưa đủ cookie phiên => chưa nên coi là sẵn sàng composer.
                time.sleep(0.35)
                continue
            try:
                if page.locator("input[name='pass'], input#pass").first.is_visible(timeout=300):
                    logger.warning("[FB] Composer check: còn form password, phiên chưa ổn.")
                    return False
            except Exception:
                pass

            remaining_ms = int((deadline - time.monotonic()) * 1000)
            if remaining_ms < 300:
                break
            step = min(6_000, max(500, remaining_ms))
            hit_anchor = False
            for sel in checks:
                try:
                    if page.locator(sel).first.is_visible(timeout=step):
                        hit_anchor = True
                        logger.debug("[FB] Meta composer anchor thấy: {}", sel)
                        break
                except Exception:
                    continue
            if hit_anchor:
                stable_hits += 1
                if stable_hits >= 2:
                    logger.info("[FB] Meta Business composer ready (ổn định {} lần liên tiếp).", stable_hits)
                    return True
            else:
                stable_hits = 0
            time.sleep(0.45)
        return False
    except Exception:
        return False


def _is_on_target_surface(page: Page, target_url: str) -> bool:
    """
    Kiểm tra URL hiện tại có khớp bề mặt Page/Group mục tiêu hay chưa.

    Chấp nhận redirect id số → vanity slug (cùng Page).
    """
    try:
        cur = page.url or ""
        if not cur or not target_url:
            return False
        return _urls_refer_same_facebook_page(target_url, cur, page=page)
    except Exception:
        return False


_SWITCH_EXACT_LABEL_RE = re.compile(r"^\s*Switch\s*$", re.I)
_SWITCH_NOW_RE = re.compile(r"Switch Now|Chuyển ngay", re.I)
_SWITCH_MANAGING_BANNER_RE = re.compile(
    r"Switch\s+into.+to\s+start\s+managing|Switch\s+into.+để\s+bắt đầu\s+quản\s+lý",
    re.I | re.S,
)

# Sidebar Manage Page — HTML Meta: div[role=none] > div.html-div > span «Switch» (+ overlay ignore chặn click)
_PAGE_SWITCH_STRICT_XPATHS: tuple[str, ...] = (
    "(//div[@role='none'][.//div[contains(@class,'html-div')]//span[normalize-space()='Switch']]"
    "[not(@data-visualcompletion='ignore')])[last()]",
    "(//div[contains(@class,'html-div')][.//span[contains(@class,'x6ikm8r') and normalize-space()='Switch']])[last()]",
    "(//div[contains(@class,'html-div')][.//div[@role='none']//span[normalize-space()='Switch']])[last()]",
    "(//span[normalize-space()='Switch']/ancestor::div[@role='none'][not(@data-visualcompletion='ignore')][1])[last()]",
)

_PAGE_SWITCH_CLICK_JS = """(el) => {
  if (!el) return;
  const pick = [
    el.closest("[class*='html-div']"),
    el.closest("[role='button']"),
    el.closest("[tabindex='0']"),
    el.closest("[role='none']"),
    el,
  ];
  for (const node of pick) {
    if (node && typeof node.click === "function") {
      node.click();
      return;
    }
  }
}"""

_PAGE_SIDEBAR_SCROLL_JS = """
() => {
  let card = null;
  for (const el of document.querySelectorAll('div, section, aside')) {
    const raw = (el.innerText || '').replace(/\\s+/g, ' ').trim();
    const low = raw.toLowerCase();
    const isSwitchCard = low.includes('switch into') && (
      low.includes('take more actions') || low.includes('start managing') ||
      low.includes('bắt đầu quản lý') || low.includes('thực hiện thêm')
    );
    if (!isSwitchCard) continue;
    if (raw.length > 260) continue;
    const r = el.getBoundingClientRect();
    if (r.left > window.innerWidth * 0.55) continue;
    if (!card || r.bottom > card.getBoundingClientRect().bottom) card = el;
  }
  if (!card) return false;
  let p = card;
  for (let i = 0; i < 10 && p; i++) {
    const s = getComputedStyle(p);
    if ((s.overflowY === 'auto' || s.overflowY === 'scroll') && p.scrollHeight > p.clientHeight + 24) {
      p.scrollTop = p.scrollHeight;
      return true;
    }
    p = p.parentElement;
  }
  try { card.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (_) {}
  return true;
}
"""

_PAGE_SWITCH_META_EXACT_CLICK_JS = """
() => {
  const isIgnore = (el) => el && el.getAttribute('data-visualcompletion') === 'ignore';
  for (const ov of document.querySelectorAll('[data-visualcompletion="ignore"]')) {
    ov.style.pointerEvents = 'none';
    ov.style.display = 'none';
  }
  let card = null;
  for (const el of document.querySelectorAll('div')) {
    const raw = (el.innerText || '').replace(/\\s+/g, ' ').trim();
    const low = raw.toLowerCase();
    const isSwitchCard = low.includes('switch into') && (
      low.includes('take more actions') || low.includes('start managing') ||
      low.includes('bắt đầu quản lý') || low.includes('thực hiện thêm')
    );
    if (!isSwitchCard) continue;
    if (raw.length > 280) continue;
    const r = el.getBoundingClientRect();
    if (r.left > window.innerWidth * 0.55) continue;
    if (!card || r.bottom > card.getBoundingClientRect().bottom) card = el;
  }
  if (!card) return false;
  let switchSpan = null;
  for (const sp of card.querySelectorAll('span')) {
    if (/^switch$/i.test((sp.textContent || '').trim())) { switchSpan = sp; break; }
  }
  if (!switchSpan) return false;
  const htmlDiv = switchSpan.closest('div.html-div') || switchSpan.closest("[class*='html-div']");
  const innerRole = switchSpan.closest("div[role='none']");
  const outerRole = htmlDiv && htmlDiv.parentElement && htmlDiv.parentElement.getAttribute('role') === 'none'
    ? htmlDiv.parentElement : null;
  const fire = (node) => {
    if (!node || isIgnore(node)) return false;
    try {
      const r = node.getBoundingClientRect();
      const x = r.left + r.width / 2;
      const y = r.top + r.height / 2;
      for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) {
        node.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window, clientX: x, clientY: y }));
      }
      if (typeof node.click === 'function') node.click();
      return true;
    } catch (_) { return false; }
  };
  if (outerRole && fire(outerRole)) return true;
  if (htmlDiv && fire(htmlDiv)) return true;
  if (innerRole && fire(innerRole)) return true;
  return fire(switchSpan);
}
"""

_PAGE_SWITCH_CTA_CARD_JS = _PAGE_SWITCH_META_EXACT_CLICK_JS

_PAGE_SIDEBAR_SWITCH_JS = _PAGE_SWITCH_META_EXACT_CLICK_JS


def _view_only_guard_active_on_page(page: Page) -> bool:
    try:
        return bool(page.evaluate("() => Boolean(window.__toolfb_view_guard_active)"))
    except Exception:
        return False


def _scroll_manage_page_sidebar_switch_cta(page: Page) -> None:
    """Cuộn sidebar trái tới khối «Switch into … to take more actions» (nút Switch ở đáy)."""
    try:
        page.evaluate(_PAGE_SIDEBAR_SCROLL_JS)
    except Exception:
        pass
    page.wait_for_timeout(450)


def _click_meta_switch_cta_exact(page: Page, *, timeout_ms: int = 3000) -> bool:
    """
    Bấm đúng nút Switch Meta (div[role=none] > div.html-div > span «Switch»).

    Facebook thêm ``div[data-visualcompletion='ignore']`` phủ ``inset:0`` — tắt overlay rồi dispatch click.
    """
    _suppress_facebook_click_overlays(page)
    _scroll_manage_page_sidebar_switch_cta(page)
    locators = (
        page.locator(
            "xpath=(//div[@role='none'][.//div[contains(@class,'html-div')]"
            "//span[normalize-space()='Switch']][not(@data-visualcompletion='ignore')])[last()]"
        ),
        page.locator(
            "xpath=(//div[contains(@class,'html-div')]"
            "[.//span[contains(@class,'x6ikm8r') and normalize-space()='Switch']])[last()]"
        ),
        page.locator("div.html-div").filter(
            has=page.locator("span").filter(has_text=_SWITCH_EXACT_LABEL_RE)
        ).last,
    )
    for loc in locators:
        try:
            if loc.count() <= 0 or not loc.is_visible(timeout=1_200):
                continue
            loc.scroll_into_view_if_needed(timeout=2_500)
            # Firefox: ưu tiên mouse (evaluate đôi khi không kích hoạt switch thật)
            try:
                box = loc.bounding_box()
                if box:
                    page.mouse.click(
                        box["x"] + box["width"] / 2,
                        box["y"] + box["height"] / 2,
                    )
                    logger.info("[FB] Đã bấm Switch CTA (mouse tại tâm nút).")
                    page.wait_for_timeout(2_200)
                    return True
            except Exception:
                pass
            try:
                loc.evaluate(_PAGE_SWITCH_META_EXACT_CLICK_JS)
                logger.info("[FB] Đã bấm Switch CTA (evaluate trên html-div/role=none).")
                page.wait_for_timeout(2_200)
                return True
            except Exception:
                pass
            loc.click(timeout=timeout_ms, force=True, no_wait_after=True)
            logger.info("[FB] Đã bấm Switch CTA (force click locator).")
            page.wait_for_timeout(2_200)
            return True
        except Exception:
            continue
    try:
        if page.evaluate(_PAGE_SWITCH_META_EXACT_CLICK_JS):
            logger.info("[FB] Đã bấm Switch CTA (JS Meta exact).")
            page.wait_for_timeout(2_200)
            return True
    except Exception:
        pass
    return False


def _click_switch_in_sidebar_cta_card(page: Page, *, timeout_ms: int = 2500) -> bool:
    """
    Bấm nút Switch trong thẻ CTA sidebar (ảnh Manage Page — khối xám «Switch into … Page»).
    """
    if _click_meta_switch_cta_exact(page, timeout_ms=timeout_ms):
        return True
    _scroll_manage_page_sidebar_switch_cta(page)
    card_pat = re.compile(r"Switch\s+into.+take\s+more\s+actions", re.I | re.S)
    try:
        cards = page.locator("div").filter(has_text=card_pat)
        n = min(cards.count(), 8)
        for i in range(n):
            card = cards.nth(i)
            try:
                if not card.is_visible(timeout=600):
                    continue
                box = card.bounding_box()
                vp_w = float((page.viewport_size or {}).get("width") or 1280)
                if box and float(box.get("x", 9999)) > vp_w * 0.45:
                    continue
            except Exception:
                continue
            for loc in (
                card.locator(
                    "div[role='none']:not([data-visualcompletion='ignore'])"
                ).filter(has=page.locator("span").filter(has_text=_SWITCH_EXACT_LABEL_RE)),
                card.locator("div.html-div").filter(
                    has=page.locator("span").filter(has_text=_SWITCH_EXACT_LABEL_RE)
                ),
                card.get_by_role("button", name=_SWITCH_EXACT_LABEL_RE),
                card.locator("[role='button']").filter(has_text=_SWITCH_EXACT_LABEL_RE),
                card.get_by_text(_SWITCH_EXACT_LABEL_RE),
            ):
                try:
                    if loc.count() <= 0:
                        continue
                    target = loc.first
                    if not target.is_visible(timeout=800):
                        continue
                    target.scroll_into_view_if_needed(timeout=2_000)
                    try:
                        target.evaluate(_PAGE_SWITCH_CLICK_JS)
                    except Exception:
                        target.click(timeout=timeout_ms, force=True, no_wait_after=True)
                    logger.info("[FB] Đã bấm Switch trong thẻ CTA sidebar (card index={}).", i)
                    page.wait_for_timeout(2000)
                    return True
                except Exception:
                    continue
    except Exception:
        pass
    try:
        if page.evaluate(_PAGE_SWITCH_CTA_CARD_JS):
            logger.info("[FB] Đã bấm Switch trong thẻ CTA sidebar (JS).")
            page.wait_for_timeout(2000)
            return True
    except Exception:
        pass
    return False


def _click_sidebar_switch_role_button(page: Page, *, timeout_ms: int = 2000) -> bool:
    """Nút ``role=button`` tên Switch ở cột trái (< 45% chiều ngang viewport)."""
    _scroll_manage_page_sidebar_switch_cta(page)
    try:
        vp = page.viewport_size or {"width": 1280}
        max_x = float(vp.get("width", 1280)) * 0.45
    except Exception:
        max_x = 576.0
    try:
        buttons = page.get_by_role("button", name=_SWITCH_EXACT_LABEL_RE)
        n = min(buttons.count(), 12)
    except Exception:
        return False
    for i in range(n):
        b = buttons.nth(i)
        try:
            if not b.is_visible(timeout=700):
                continue
            box = b.bounding_box()
            if box and float(box.get("x", 9999)) > max_x:
                continue
            b.scroll_into_view_if_needed(timeout=2_000)
            try:
                b.evaluate(_PAGE_SWITCH_CLICK_JS)
            except Exception:
                b.click(timeout=timeout_ms, force=True, no_wait_after=True)
            logger.info("[FB] Đã bấm Switch sidebar (role=button, index={}).", i)
            page.wait_for_timeout(2000)
            return True
        except Exception:
            continue
    return False


def _page_switch_sidebar_hint_visible(page: Page, *, timeout_ms: int = 1500) -> bool:
    """Manage Page: «Switch into … Page to take more actions» (sidebar trái)."""
    for pat in (
        r"Switch into",
        r"to take more actions",
        r"to start managing",
        r"Chuyển sang",
        r"để bắt đầu quản lý",
        r"để thực hiện thêm",
    ):
        try:
            if page.get_by_text(re.compile(pat, re.I)).first.is_visible(timeout=timeout_ms):
                return True
        except Exception:
            continue
    return False


def _click_manage_page_sidebar_switch(page: Page, *, timeout_ms: int = 2000) -> bool:
    """
    Bấm nút Switch sidebar (HTML Meta: ``div[role='none']`` > span «Switch»).

    Khác «Switch Now» trên banner và khác nút Switch trong popup ``role=dialog``.
    """
    if _click_meta_switch_cta_exact(page, timeout_ms=max(timeout_ms, 2800)):
        return True
    if _click_switch_in_sidebar_cta_card(page, timeout_ms=timeout_ms):
        return True
    if _click_sidebar_switch_role_button(page, timeout_ms=timeout_ms):
        return True
    _scroll_manage_page_sidebar_switch_cta(page)
    locators: list[Locator] = []
    for xp in _PAGE_SWITCH_STRICT_XPATHS:
        locators.append(page.locator(f"xpath={xp}"))
    locators.extend(
        (
            page.locator(
                "xpath=(//div[@role='none'][not(ancestor::*[@role='dialog'])]"
                "[.//span[normalize-space()='Switch'] and not(.//*[normalize-space()='Switch Now'])])[last()]"
            ),
            page.locator("div.html-div").filter(has=page.get_by_text(_SWITCH_EXACT_LABEL_RE)).last,
            page.locator(
                "xpath=(//*[contains(translate(normalize-space(.), "
                "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'take more actions')]"
                "//*[@role='none'][not(ancestor::*[@role='dialog'])][.//span[normalize-space()='Switch']])[last()]"
            ),
            page.locator(
                "xpath=(//*[contains(translate(normalize-space(.), "
                "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'switch into')]"
                "//*[@role='none'][not(ancestor::*[@role='dialog'])][.//span[normalize-space()='Switch']])[last()]"
            ),
        )
    )
    for loc in locators:
        try:
            if loc.count() <= 0:
                continue
            target = loc.last
            if not target.is_visible(timeout=timeout_ms):
                continue
            try:
                target.scroll_into_view_if_needed(timeout=2_500)
            except Exception:
                pass
            try:
                target.evaluate(_PAGE_SWITCH_CLICK_JS)
            except Exception:
                target.click(timeout=timeout_ms, force=True, no_wait_after=True)
            logger.info("[FB] Đã bấm Switch sidebar Manage Page (html-div / role=none).")
            page.wait_for_timeout(2000)
            return True
        except Exception:
            continue
    try:
        if page.evaluate(_PAGE_SIDEBAR_SWITCH_JS):
            logger.info("[FB] Đã bấm Switch sidebar (JS span «Switch»).")
            page.wait_for_timeout(2000)
            return True
    except Exception:
        pass
    return False


def _normalize_compact_page_name(value: str) -> str:
    """So sánh slug «xabreownersbandung» với «Xabre Owners Bandung» trong popup."""
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _page_switch_name_aliases(page_display_name: str, page_url: str) -> list[str]:
    """Các nhãn có thể xuất hiện trong popup Switch profiles."""
    out: list[str] = []
    seen: set[str] = set()

    def _add(raw: str) -> None:
        t = str(raw or "").strip()
        if len(t) < 2:
            return
        key = t.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(t)

    _add(page_display_name)
    compact = _normalize_compact_page_name(page_display_name)
    if compact and compact.lower() != str(page_display_name or "").strip().lower():
        _add(compact)
    dest = str(page_url or "").strip()
    if dest:
        try:
            p = urlparse(dest)
            for seg in (x for x in (p.path or "").split("/") if x):
                if seg.lower() in ("pages", "people", "profile.php"):
                    continue
                _add(seg)
        except Exception:
            pass
        pid = extract_facebook_numeric_id_from_url(dest)
        if pid:
            _add(pid)
    return out


def _infer_page_title_from_switch_dialog(page: Page) -> str:
    """Đọc tên Page in đậm trong popup: «Switch to Xabre Owners Bandung for more features»."""
    try:
        dlg = _switch_profiles_dialog_scope(page)
        blob = str(dlg.inner_text(timeout=2_000) or "")
        for pat in (
            r"Switch to\s+(.+?)\s+for more features",
            r"Switch to\s+(.+?)\s+for more",
            r"Chuyển sang\s+(.+?)\s+để",
        ):
            m = re.search(pat, blob, re.I | re.DOTALL)
            if m:
                title = re.sub(r"\s+", " ", m.group(1)).strip()
                if len(title) >= 2:
                    return title
    except Exception:
        pass
    return ""


def _switch_profiles_dialog_mentions_page(
    dlg: Locator,
    aliases: list[str],
) -> bool:
    try:
        blob = str(dlg.inner_text(timeout=2_000) or "")
    except Exception:
        return False
    if not blob.strip():
        return False
    compact_blob = _normalize_compact_page_name(blob)
    for alias in aliases:
        if len(alias) < 2:
            continue
        if alias.lower() in blob.lower():
            return True
        compact_alias = _normalize_compact_page_name(alias)
        if len(compact_alias) >= 4 and compact_alias in compact_blob:
            return True
    return False


def _switch_profiles_dialog_scope(page: Page) -> Locator:
    """Modal «Switch profiles» / «Switch to … for more features»."""
    for pat in (
        r"Switch profiles",
        r"Switch to",
        r"Chuyển hồ sơ",
        r"for more features, tools and settings",
    ):
        try:
            dlg = page.get_by_role("dialog").filter(has_text=re.compile(pat, re.I)).last
            if dlg.count() > 0:
                return dlg
        except Exception:
            continue
    return page.locator("[role='dialog']").last


def _switch_profiles_dialog_visible(page: Page, *, timeout_ms: int = 1200) -> bool:
    try:
        dlg = _switch_profiles_dialog_scope(page)
        if dlg.count() > 0 and dlg.is_visible(timeout=timeout_ms):
            return True
    except Exception:
        pass
    return False


_PAGE_SWITCH_POPUP_CONFIRM_JS = """
() => {
  const dlg = document.querySelector('[role="dialog"]');
  if (!dlg) return false;
  const low = (dlg.innerText || '').toLowerCase();
  if (!low.includes('switch profiles') && !low.includes('switch to') && !low.includes('chuyển')) {
    if (!low.includes('switch')) return false;
  }
  for (const ov of dlg.querySelectorAll('[data-visualcompletion="ignore"]')) {
    ov.style.pointerEvents = 'none';
  }
  const hits = [];
  for (const el of dlg.querySelectorAll('[role="button"], div[role="none"], span, a')) {
    const t = (el.textContent || '').replace(/\\s+/g, ' ').trim();
    if (!/^switch$/i.test(t)) continue;
    const r = el.getBoundingClientRect();
    if (r.width < 36 || r.height < 16) continue;
    const role = (el.getAttribute('role') || '').toLowerCase();
    hits.push({ el, bottom: r.bottom, left: r.left, role });
  }
  if (!hits.length) return false;
  hits.sort((a, b) => {
    const score = (h) => (h.role === 'button' ? 1_000_000 : h.role === 'none' ? 100_000 : 0) + h.bottom * 100 + h.left;
    return score(b) - score(a);
  });
  const fire = (node) => {
    if (!node) return false;
    try {
      const r = node.getBoundingClientRect();
      const x = r.left + r.width / 2;
      const y = r.top + r.height / 2;
      for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) {
        node.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window, clientX: x, clientY: y }));
      }
      if (typeof node.click === 'function') node.click();
      return true;
    } catch (_) { return false; }
  };
  for (const h of hits) {
    let node = h.el;
    for (let i = 0; i < 8 && node; i++) {
      if (node.getAttribute && (node.getAttribute('role') === 'button' || node.getAttribute('role') === 'none')) {
        if (fire(node)) return true;
      }
      node = node.parentElement;
    }
    if (fire(h.el)) return true;
  }
  return false;
}
"""


def _click_switch_profiles_popup_confirm(
    page: Page,
    *,
    page_display_name: str = "",
    page_url: str = "",
    timeout_ms: int = 8_000,
) -> bool:
    """
    Bấm nút Switch xanh trong popup «Switch profiles» (ưu tiên ``role=button`` footer).

    Khớp tên Page: slug URL (xabreownersbandung) hoặc tên hiển thị (Xabre Owners Bandung).
    """
    if not _switch_profiles_dialog_visible(page, timeout_ms=min(2_500, timeout_ms)):
        return False
    dlg = _switch_profiles_dialog_scope(page)
    try:
        dlg.wait_for(state="visible", timeout=min(4_000, timeout_ms))
    except Exception:
        pass

    aliases = _page_switch_name_aliases(page_display_name, page_url)
    inferred = _infer_page_title_from_switch_dialog(page)
    if inferred:
        aliases = [*aliases, inferred]
    if _switch_profiles_dialog_mentions_page(dlg, aliases):
        logger.info(
            "[FB] Popup Switch profiles khớp Page: {}.",
            ", ".join(aliases[:4]),
        )
    elif aliases:
        logger.warning(
            "[FB] Popup Switch profiles không khớp alias {} — vẫn bấm Switch (có thể đúng Page).",
            aliases[0],
        )

    def _popup_closed() -> bool:
        return not _switch_profiles_dialog_visible(page, timeout_ms=450)

    def _after_click_pause() -> bool:
        page.wait_for_timeout(1_200)
        return _popup_closed()

    try:
        page.evaluate(
            """() => {
              const dlg = document.querySelector('[role="dialog"]');
              if (!dlg) return;
              for (const ov of dlg.querySelectorAll('[data-visualcompletion="ignore"]')) {
                ov.style.pointerEvents = 'none';
              }
            }"""
        )
    except Exception:
        pass

    locators = (
        dlg.get_by_role("button", name=_SWITCH_EXACT_LABEL_RE).last,
        dlg.locator("[aria-label='Switch']").last,
        dlg.locator("[aria-label*='Switch' i]").last,
        dlg.locator(
            "xpath=.//*[@role='button'][.//span[normalize-space()='Switch'] or normalize-space()='Switch'][last()]"
        ),
        dlg.locator(
            "xpath=.//div[@role='none'][.//span[normalize-space()='Switch']][last()]"
        ),
        dlg.locator("div[role='none']").filter(
            has=dlg.locator("span").filter(has_text=_SWITCH_EXACT_LABEL_RE)
        ).last,
    )
    for loc in locators:
        try:
            if loc.count() <= 0 or not loc.is_visible(timeout=1_200):
                continue
            loc.scroll_into_view_if_needed(timeout=2_000)
            try:
                box = loc.bounding_box()
                if box:
                    page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                    logger.info("[FB] Đã bấm Switch popup (mouse, role=button).")
                    if _after_click_pause():
                        return True
            except Exception:
                pass
            loc.click(timeout=min(timeout_ms, 5_000), force=True, no_wait_after=True)
            logger.info("[FB] Đã bấm Switch popup (force, role=button).")
            if _after_click_pause():
                return True
        except Exception:
            continue

    for attempt in (1, 2):
        try:
            if dlg.evaluate(_PAGE_SWITCH_POPUP_CONFIRM_JS):
                logger.info("[FB] Đã bấm Switch popup (JS, attempt={}).", attempt)
                if _after_click_pause():
                    return True
        except Exception:
            pass
        try:
            if page.evaluate(_PAGE_SWITCH_POPUP_CONFIRM_JS) and _after_click_pause():
                logger.info("[FB] Đã bấm Switch popup (JS page, attempt={}).", attempt)
                return True
        except Exception:
            pass
        page.wait_for_timeout(500)

    return _popup_closed()


def _handle_switch_profiles_popup_if_present(
    page: Page,
    *,
    page_display_name: str = "",
    page_url: str = "",
    appear_wait_ms: int = 3_200,
    settle_ms: int = 10_000,
) -> bool:
    """
    Sau khi bấm Switch sidebar: nếu có popup thì bấm Switch xanh, chờ đóng + xác nhận vai trò Page.
    """
    _ = page_url  # giữ API; chọn Page trong popup chỉ khi cần danh sách
    pname = str(page_display_name or "").strip()
    popup_seen = False
    appear_deadline = time.time() + max(1.2, appear_wait_ms / 1000.0)
    while time.time() < appear_deadline:
        if _switch_profiles_dialog_visible(page, timeout_ms=350):
            popup_seen = True
            break
        page.wait_for_timeout(220)
    if not popup_seen:
        if _manage_page_switch_cta_still_visible(page, timeout_ms=500) or _page_switch_sidebar_hint_visible(
            page, timeout_ms=450
        ):
            logger.info("[FB] Không có popup Switch profiles nhưng CTA Switch vẫn hiện — chưa switch.")
            return False
        logger.info("[FB] Không có popup Switch profiles (switch trực tiếp hoặc đã đóng).")
        return True

    logger.info("[FB] Popup Switch profiles — bấm nút Switch xác nhận.")
    dest = str(page_url or "").strip()
    confirm_failures = 0
    for popup_click_try in range(1, 3):
        if _click_switch_profiles_popup_confirm(
            page,
            page_display_name=pname,
            page_url=dest,
        ):
            if not _switch_profiles_dialog_visible(page, timeout_ms=450):
                break
        elif not _switch_profiles_dialog_visible(page, timeout_ms=350):
            break
        confirm_failures += 1
        if confirm_failures >= 2:
            logger.warning(
                "[FB] Popup Switch profiles — xác nhận thất bại {} lần, dừng sớm.",
                confirm_failures,
            )
            break
        page.wait_for_timeout(400)
    if _switch_profiles_dialog_visible(page, timeout_ms=450):
        _failure_screenshot(page, "switch_profiles_popup_confirm_fail")
        return False

    settle_deadline = time.time() + max(3.5, settle_ms / 1000.0)
    while time.time() < settle_deadline:
        if _switch_profiles_dialog_visible(page, timeout_ms=300):
            if not _click_switch_profiles_popup_confirm(
                page, page_display_name=pname, page_url=dest
            ):
                return False
            page.wait_for_timeout(350)
            continue
        if _page_role_acting_as_page(page, timeout_ms=500):
            logger.info("[FB] Popup đã đóng — xác nhận vai trò Page sau Switch profiles.")
            return True
        page.wait_for_timeout(320)
    if _switch_profiles_dialog_visible(page, timeout_ms=450):
        return False
    return _page_role_acting_as_page(page, timeout_ms=700)


def _wait_after_page_switch_click(
    page: Page,
    *,
    page_display_name: str = "",
    page_url: str = "",
    timeout_ms: int = 12_000,
) -> bool:
    """Chờ popup Switch profiles (và xử lý) hoặc sidebar CTA biến mất."""
    ok = _handle_switch_profiles_popup_if_present(
        page,
        page_display_name=page_display_name,
        page_url=page_url,
        appear_wait_ms=min(5_000, timeout_ms),
        settle_ms=timeout_ms,
    )
    deadline = time.time() + timeout_ms / 1000.0
    while time.time() < deadline:
        if _page_role_acting_as_page(page, timeout_ms=450):
            return True
        if not _page_switch_sidebar_hint_visible(page, timeout_ms=350) and not _manage_page_switch_cta_still_visible(
            page, timeout_ms=350
        ):
            return ok
        page.wait_for_timeout(320)
    if _manage_page_switch_cta_still_visible(page, timeout_ms=400) or _page_switch_sidebar_hint_visible(
        page, timeout_ms=400
    ):
        return False
    return ok


def _confirm_switch_profiles_popup(page: Page, *, page_display_name: str = "", page_url: str = "") -> bool:
    """Popup «Switch profiles» — bấm Switch xanh; trả True nếu không popup hoặc đã xử lý xong."""
    return _handle_switch_profiles_popup_if_present(
        page,
        page_display_name=page_display_name,
        page_url=page_url,
    )


def _page_switch_ui_visible(page: Page, *, timeout_ms: int = 900) -> bool:
    if _page_switch_sidebar_hint_visible(page, timeout_ms=timeout_ms):
        return True
    try:
        if page.get_by_role("button", name=_SWITCH_NOW_RE).first.is_visible(timeout=timeout_ms):
            return True
    except Exception:
        pass
    try:
        if page.get_by_text(_SWITCH_MANAGING_BANNER_RE).first.is_visible(timeout=timeout_ms):
            return True
    except Exception:
        pass
    return False


def _manage_page_switch_cta_still_visible(page: Page, *, timeout_ms: int = 450) -> bool:
    """Thẻ «Switch into … to take more actions» + nút Switch ở sidebar = chưa vào vai trò Page."""
    card_pat = re.compile(r"Switch\s+into.+take\s+more\s+actions", re.I | re.S)
    try:
        vp_w = float((page.viewport_size or {}).get("width") or 1280)
        cards = page.locator("div").filter(has_text=card_pat)
        n = min(cards.count(), 6)
        for i in range(n):
            card = cards.nth(i)
            try:
                if not card.is_visible(timeout=timeout_ms):
                    continue
                box = card.bounding_box()
                if box and float(box.get("x", 9999)) > vp_w * 0.45:
                    continue
                if card.get_by_text(_SWITCH_EXACT_LABEL_RE).first.is_visible(timeout=250):
                    return True
            except Exception:
                continue
    except Exception:
        pass
    return _page_switch_sidebar_hint_visible(page, timeout_ms=timeout_ms)


def _page_role_acting_as_page(page: Page, *, timeout_ms: int = 700) -> bool:
    """
    Đã switch sang vai trò Page — không chỉ mở đúng URL (vẫn xem bằng profile cá nhân).

    Không được đăng Reel nếu hàm này trả False.
    """
    if _page_switch_ui_visible(page, timeout_ms=timeout_ms):
        return False
    if _manage_page_switch_cta_still_visible(page, timeout_ms=timeout_ms):
        return False
    try:
        dash = page.get_by_role("link", name=re.compile(r"Professional\s+dashboard", re.I)).first
        if dash.is_visible(timeout=timeout_ms):
            if (dash.get_attribute("aria-disabled") or "").strip().lower() == "true":
                return False
            return True
    except Exception:
        pass
    try:
        insights = page.get_by_role("link", name=re.compile(r"Insights", re.I)).first
        if insights.is_visible(timeout=timeout_ms):
            if (insights.get_attribute("aria-disabled") or "").strip().lower() == "true":
                return False
            return True
    except Exception:
        pass
    return False


def _target_page_role_satisfied(
    page: Page,
    dest: str = "",
    *,
    timeout_ms: int = 800,
) -> bool:
    """Đã switch vai trò Page (và nếu có ``dest`` thì cùng Page đích hoặc slug redirect)."""
    if not _page_role_acting_as_page(page, timeout_ms=timeout_ms):
        return False
    d = str(dest or "").strip()
    if not d:
        return True
    return _urls_refer_same_facebook_page(d, str(page.url or ""), page=page)


def _try_click_page_match_in_scope(
    scope: Any,
    *,
    page_display_name: str,
    page_url: str,
    timeout_ms: int = 450,
) -> bool:
    """Bấm đúng Page trong popup/menu — tên, alias, slug, numeric id."""
    pname = str(page_display_name or "").strip()
    dest = str(page_url or "").strip()
    expect_id = extract_facebook_numeric_id_from_url(dest)
    slug = _facebook_slug_from_url(dest)
    aliases = _page_switch_name_aliases(pname, dest)
    compact_names = {_normalize_compact_page_name(a) for a in aliases if len(str(a).strip()) >= 2}

    for name in aliases:
        pat = re.compile(re.escape(str(name)[:80]), re.I)
        for factory in (
            lambda p=pat: scope.get_by_role("button", name=p),
            lambda p=pat: scope.get_by_role("link", name=p),
            lambda p=pat: scope.get_by_text(p).last,
        ):
            try:
                if _click_visible_enabled_button(factory(), timeout_ms=timeout_ms, human_label=""):
                    return True
            except Exception:
                continue

    if expect_id:
        try:
            href_loc = scope.locator(f"a[href*='{expect_id}']").first
            if _click_visible_enabled_button(href_loc, timeout_ms=timeout_ms, human_label=""):
                return True
        except Exception:
            pass

    try:
        rows = scope.locator("[role='button'], [role='menuitem'], [role='option'], a")
        n = min(rows.count(), 28)
        for i in range(n):
            row = rows.nth(i)
            try:
                if not row.is_visible(timeout=220):
                    continue
                txt = str(row.inner_text(timeout=400) or "")
                low = txt.lower()
                if "log out" in low or "đăng xuất" in low:
                    continue
                if expect_id and expect_id in txt:
                    if _click_visible_enabled_button(row, timeout_ms=timeout_ms, human_label=""):
                        return True
                compact = _normalize_compact_page_name(txt)
                if slug and len(slug) >= 4 and slug in compact:
                    if _click_visible_enabled_button(row, timeout_ms=timeout_ms, human_label=""):
                        return True
                for cn in compact_names:
                    if len(cn) < 4:
                        continue
                    if cn in compact or (len(compact) >= 4 and compact in cn):
                        if _click_visible_enabled_button(row, timeout_ms=timeout_ms, human_label=""):
                            return True
            except Exception:
                continue
    except Exception:
        pass
    return False


def _wait_page_role_switch_complete(
    page: Page,
    *,
    timeout_ms: int = 28_000,
    stable_hits: int = 3,
) -> bool:
    """Chờ xác nhận ổn định đã switch (không còn CTA Switch sidebar)."""
    need = max(2, int(stable_hits))
    deadline = time.time() + max(3.0, timeout_ms / 1000.0)
    hits = 0
    while time.time() < deadline:
        if _page_role_acting_as_page(page, timeout_ms=500):
            hits += 1
            if hits >= need:
                return True
        else:
            hits = 0
        page.wait_for_timeout(450)
    return False


def _require_page_role_switched(
    page: Page,
    *,
    page_display_name: str = "",
    page_url: str = "",
    context: str = "reel",
) -> None:
    """Chặn pipeline nếu chưa switch — tránh đăng nhầm profile cá nhân."""
    if _page_role_acting_as_page(page, timeout_ms=900):
        logger.info("[FB] Xác nhận đã switch vai trò Page ({})", context)
        return
    _failure_screenshot(page, f"page_role_not_switched_{context}")
    raise RuntimeError(
        f"[FB] Chưa switch vào vai trò Page ({context}) — vẫn thấy «Switch into …» hoặc "
        f"menu Manage Page bị khóa. Không được đăng video. url={page.url!r}"
    )


def _suppress_facebook_click_overlays(page: Page) -> None:
    """Tắt overlay ``data-visualcompletion=ignore`` che nút Switch / Switch Now."""
    try:
        page.evaluate(
            """() => {
              for (const ov of document.querySelectorAll('[data-visualcompletion="ignore"]')) {
                ov.style.pointerEvents = 'none';
                ov.style.opacity = '0.01';
              }
            }"""
        )
    except Exception:
        pass


_CLICK_SWITCH_NOW_ANY_JS = """
() => {
  for (const ov of document.querySelectorAll('[data-visualcompletion="ignore"]')) {
    ov.style.pointerEvents = 'none';
  }
  const nowRe = /^switch\\s+now$/i;
  const chuyenRe = /^chuyển\\s+ngay$/i;
  const hits = [];
  for (const el of document.querySelectorAll('[role="button"], a, span, div[role="none"]')) {
    const t = (el.textContent || '').replace(/\\s+/g, ' ').trim();
    if (!nowRe.test(t) && !chuyenRe.test(t)) continue;
    const r = el.getBoundingClientRect();
    if (r.width < 36 || r.height < 12 || r.bottom < 0 || r.right < 0) continue;
    if (r.top > window.innerHeight || r.left > window.innerWidth) continue;
    let score = r.width * r.height;
    let p = el;
    for (let i = 0; i < 10 && p; i++) {
      const raw = (p.innerText || '').toLowerCase();
      if (raw.includes('switch into')) score += 80_000;
      if (raw.includes('start managing') || raw.includes('take more actions')) score += 50_000;
      if (raw.includes('bắt đầu quản lý') || raw.includes('thực hiện thêm')) score += 50_000;
      p = p.parentElement;
    }
    hits.push({ el, score, bottom: r.bottom });
  }
  if (!hits.length) return false;
  hits.sort((a, b) => b.score - a.score || b.bottom - a.bottom);
  const fire = (node) => {
    if (!node) return false;
    try {
      const r = node.getBoundingClientRect();
      const x = r.left + r.width / 2;
      const y = r.top + r.height / 2;
      for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) {
        node.dispatchEvent(new MouseEvent(type, {
          bubbles: true, cancelable: true, view: window, clientX: x, clientY: y
        }));
      }
      if (typeof node.click === 'function') node.click();
      return true;
    } catch (_) { return false; }
  };
  for (const h of hits) {
    let node = h.el;
    for (let i = 0; i < 8 && node; i++) {
      const role = (node.getAttribute && node.getAttribute('role')) || '';
      if (role === 'button' || role === 'none' || node.tagName === 'A') {
        if (fire(node)) return true;
      }
      node = node.parentElement;
    }
    if (fire(h.el)) return true;
  }
  return false;
}
"""


def _mouse_click_locator_center(page: Page, loc: Locator, *, label: str = "switch") -> bool:
    """Click tâm locator bằng mouse — tránh overlay chặn Playwright click."""
    try:
        if not loc.is_visible(timeout=1_200):
            return False
        loc.scroll_into_view_if_needed(timeout=2_500)
        box = loc.bounding_box()
        if box:
            human_pause(kind="click", label=label)
            page.mouse.click(
                float(box["x"]) + float(box["width"]) / 2.0,
                float(box["y"]) + float(box["height"]) / 2.0,
            )
            return True
    except Exception:
        pass
    return False


def _click_switch_now_banner(page: Page, *, timeout_ms: int = 2800) -> bool:
    """
    Bấm «Switch Now» / «Chuyển ngay» — banner, sidebar, hoặc bất kỳ nút hiển thị trên Page.
    """
    _suppress_facebook_click_overlays(page)
    try:
        buttons = page.get_by_role("button", name=_SWITCH_NOW_RE)
        n = min(buttons.count(), 8)
    except Exception:
        n = 0
    for i in range(n):
        btn = buttons.nth(i)
        if _mouse_click_locator_center(page, btn, label="Switch Now"):
            logger.info("[FB] Đã bấm Switch Now (mouse, index={}).", i)
            page.wait_for_timeout(random.randint(2200, 3800))
            return True
    if _click_visible_enabled_button(
        page.get_by_role("button", name=_SWITCH_NOW_RE),
        timeout_ms=timeout_ms,
    ):
        logger.info("[FB] Đã bấm Switch Now (role=button).")
        page.wait_for_timeout(random.randint(2200, 3800))
        return True
    for factory in (
        lambda: page.locator("[role='button']").filter(has_text=_SWITCH_NOW_RE),
        lambda: page.locator("a").filter(has_text=_SWITCH_NOW_RE),
        lambda: page.locator("div[role='none']").filter(has_text=_SWITCH_NOW_RE),
        lambda: page.get_by_text(_SWITCH_NOW_RE),
    ):
        try:
            loc = factory()
            if _mouse_click_locator_center(page, loc, label="Switch Now fallback"):
                logger.info("[FB] Đã bấm Switch Now (mouse fallback).")
                page.wait_for_timeout(random.randint(2200, 3800))
                return True
            if _click_visible_enabled_button(loc, timeout_ms=min(1600, timeout_ms)):
                logger.info("[FB] Đã bấm Switch Now (locator fallback).")
                page.wait_for_timeout(random.randint(2200, 3800))
                return True
        except Exception:
            continue
    try:
        if page.evaluate(_CLICK_SWITCH_NOW_ANY_JS):
            logger.info("[FB] Đã bấm Switch Now (JS any visible).")
            page.wait_for_timeout(random.randint(2200, 3800))
            return True
    except Exception:
        pass
    try:
        clicked = page.evaluate(
            """() => {
              const nowRe = /^switch now$/i;
              for (const btn of document.querySelectorAll('[role="button"], a, span')) {
                const t = (btn.textContent || '').replace(/\\s+/g, ' ').trim();
                if (!nowRe.test(t)) continue;
                let p = btn;
                for (let i = 0; i < 10 && p; i++) {
                  const raw = (p.innerText || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                  if (raw.includes('switch into') && (
                    raw.includes('start managing') || raw.includes('take more actions') ||
                    raw.includes('bắt đầu quản lý') || raw.includes('thực hiện thêm')
                  )) {
                    try {
                      const r = btn.getBoundingClientRect();
                      const x = r.left + r.width / 2;
                      const y = r.top + r.height / 2;
                      for (const type of ['pointerdown','mousedown','pointerup','mouseup','click']) {
                        btn.dispatchEvent(new MouseEvent(type, {
                          bubbles: true, cancelable: true, view: window, clientX: x, clientY: y
                        }));
                      }
                      if (typeof btn.click === 'function') btn.click();
                      return true;
                    } catch (_) {}
                  }
                  p = p.parentElement;
                }
              }
              return false;
            }"""
        )
        if clicked:
            logger.info("[FB] Đã bấm Switch Now (JS banner start managing).")
            page.wait_for_timeout(random.randint(2200, 3800))
            return True
    except Exception:
        pass
    return False


def _open_facebook_profile_switcher_menu(page: Page) -> bool:
    """Mở menu avatar / chuyển profile góc phải Facebook."""
    for sel in (
        '[aria-label="Your profile"]',
        '[aria-label*="Your profile" i]',
        '[aria-label*="Account" i]',
        '[aria-label*="Tài khoản" i]',
        '[aria-label*="Profile" i]',
        'div[role="button"][aria-label*="profile" i]',
        'div[role="button"][aria-label*="account" i]',
    ):
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=1_200):
                human_pause(kind="click", label="mở menu profile")
                loc.click(timeout=4_000)
                page.wait_for_timeout(900)
                return True
        except Exception:
            continue
    try:
        banner = page.locator('[role="banner"]').first
        btn = banner.locator('[role="button"][aria-label]').last
        if btn.is_visible(timeout=1_000):
            btn.click(timeout=3_500)
            page.wait_for_timeout(900)
            return True
    except Exception:
        pass
    return False


def _click_personal_profile_in_switcher(page: Page) -> bool:
    """Chọn profile cá nhân trong menu / popup Switch profiles."""
    see_all = re.compile(
        r"See all profiles|Xem tất cả hồ sơ|Switch profiles|Chuyển hồ sơ|Chuyển sang",
        re.I,
    )
    if _click_visible_enabled_button(page.get_by_role("menuitem", name=see_all), timeout_ms=1_400):
        page.wait_for_timeout(1_100)
    elif _click_visible_enabled_button(page.get_by_text(see_all), timeout_ms=1_000):
        page.wait_for_timeout(1_100)

    for pat in (
        r"^Your profile$",
        r"Hồ sơ của bạn",
        r"Tài khoản của bạn",
        r"^Profile$",
    ):
        try:
            if _click_visible_enabled_button(
                page.get_by_role("menuitem", name=re.compile(pat, re.I)),
                timeout_ms=1_200,
            ):
                page.wait_for_timeout(2_000)
                return True
        except Exception:
            continue

    try:
        me = page.locator("a[href*='/me/'], a[href*='profile.php?id=']").first
        if _click_visible_enabled_button(me, timeout_ms=1_400):
            page.wait_for_timeout(2_000)
            return True
    except Exception:
        pass

    if _switch_profiles_dialog_visible(page, timeout_ms=800):
        try:
            dlg = _switch_profiles_dialog_scope(page)
            rows = dlg.locator("[role='button'], [role='menuitem'], a")
            n = min(rows.count(), 16)
            for i in range(n):
                row = rows.nth(i)
                try:
                    if not row.is_visible(timeout=400):
                        continue
                    txt = str(row.inner_text(timeout=500) or "").lower()
                    if "page" in txt and "switch" in txt:
                        continue
                    if "log out" in txt or "đăng xuất" in txt:
                        continue
                    if _click_visible_enabled_button(row, timeout_ms=900):
                        page.wait_for_timeout(2_000)
                        return True
                except Exception:
                    continue
        except Exception:
            pass
    return False


def _personal_reset_confirmed(page: Page) -> bool:
    """True chỉ khi đã về feed/profile cá nhân — không còn CTA Switch Page."""
    if _page_switch_ui_visible(page, timeout_ms=400):
        return False
    if _manage_page_switch_cta_still_visible(page, timeout_ms=400):
        return False
    if _page_role_acting_as_page(page, timeout_ms=350):
        return False
    return _is_likely_personal_account_surface(page)


def _go_facebook_personal_home(page: Page) -> bool:
    """Điều hướng về ``facebook.com/`` và xác nhận không còn bề mặt Page viewer."""
    try:
        home = _fb_normalize_client_url("https://www.facebook.com/")
        assert_safe_facebook_navigation_url(home, label="switch_personal_home")
        page.goto(home, wait_until="domcontentloaded", timeout=50_000)
        _force_www_facebook_if_mobile_redirect(page)
        page.wait_for_timeout(2_000)
        return _personal_reset_confirmed(page)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[FB] goto home reset personal: {}", exc)
        return False


def _facebook_slug_from_url(url: str) -> str:
    """Slug Page/User từ path (vd. ``AnimalsBeingDerpss``) — bỏ qua segment số thuần."""
    try:
        parts = [x for x in (urlparse(str(url or "").strip()).path or "").split("/") if x]
        if not parts:
            return ""
        head = parts[0].strip()
        if head.isdigit():
            return ""
        if head.lower() in {
            "home.php",
            "watch",
            "marketplace",
            "groups",
            "gaming",
            "friends",
            "notifications",
            "me",
            "pages",
            "profile.php",
        }:
            return ""
        return head.lower()
    except Exception:
        return ""


def _is_likely_personal_account_surface(page: Page) -> bool:
    """
    Đang ở feed/profile cá nhân — không phải bề mặt Page đang chờ bấm Switch.

    URL slug Page (vd. ``/KikiroCooking/``) kèm CTA Switch **không** được coi là account chính.
    """
    if _page_switch_ui_visible(page, timeout_ms=450):
        return False
    if _manage_page_switch_cta_still_visible(page, timeout_ms=400):
        return False
    if _page_role_acting_as_page(page, timeout_ms=450):
        return False
    try:
        cur = (page.url or "").lower()
        if "facebook.com" not in cur:
            return False
        if "business.facebook.com" in cur or "/pages/" in cur:
            return False
        path = (urlparse(cur).path or "").strip("/")
        if not path:
            return True
        head = path.split("/")[0].lower()
        if head in ("home.php", "watch", "marketplace", "groups", "gaming", "friends", "notifications"):
            return True
        if head == "me" or "profile.php" in cur:
            return True
    except Exception:
        return False
    return False


def _switch_to_personal_profile(page: Page, *, force_home: bool = False) -> bool:
    """
    Về tài khoản Facebook cá nhân — dùng khi đang ở vai trò Page khác hoặc switch page→page lỗi.
    """
    if not force_home and _personal_reset_confirmed(page):
        logger.info("[FB] Đã ở bề mặt account chính (không cần reset).")
        return True

    logger.info(
        "[FB] Reset về profile cá nhân trước khi switch Page đích (force_home={}).",
        force_home,
    )
    if force_home and _go_facebook_personal_home(page):
        logger.info("[FB] Đã về profile cá nhân (goto home ưu tiên).")
        return True

    for attempt in range(1, 3):
        if _open_facebook_profile_switcher_menu(page):
            if _click_personal_profile_in_switcher(page):
                page.wait_for_timeout(1_800)
                if _go_facebook_personal_home(page):
                    logger.info("[FB] Đã về profile cá nhân (menu + home, attempt={}).", attempt)
                    return True
        if _go_facebook_personal_home(page):
            logger.info("[FB] Đã về profile cá nhân (goto home, attempt={}).", attempt)
            return True
        page.wait_for_timeout(900)
    ok = _personal_reset_confirmed(page)
    if not ok:
        logger.warning("[FB] Chưa xác nhận được profile cá nhân sau reset.")
    return ok


def _quick_switch_on_page_surface(
    page: Page,
    *,
    page_display_name: str = "",
    page_url: str = "",
) -> bool:
    """
    Một lượt switch nhanh trên bề mặt Page hiện tại: Switch Now → sidebar (không recovery/menu).
    """
    pname = str(page_display_name or "").strip()
    dest = str(page_url or "").strip()
    _suppress_facebook_click_overlays(page)
    if _page_role_acting_as_page(page, timeout_ms=600):
        return True
    if _click_switch_now_banner(page, timeout_ms=3_000):
        if _wait_page_role_switch_complete(page, timeout_ms=12_000):
            return True
    if _attempt_page_role_switch_clicks(page, timeout_ms=2_200):
        if _wait_after_page_switch_click(
            page, page_display_name=pname, page_url=dest, timeout_ms=10_000
        ) and _wait_page_role_switch_complete(page, timeout_ms=12_000):
            return True
    return _page_role_acting_as_page(page, timeout_ms=800)


def _select_page_via_profile_switcher(
    page: Page,
    *,
    page_display_name: str,
    page_url: str = "",
    max_list_scrolls: int = 8,
) -> bool:
    """Chọn Page trong menu avatar — ổn định hơn sidebar khi đang xem bề mặt Page."""
    pname = str(page_display_name or "").strip()
    dest = str(page_url or "").strip()
    if len(pname) < 2 and not extract_facebook_numeric_id_from_url(dest):
        return False
    if not _open_facebook_profile_switcher_menu(page):
        return False
    if not _select_page_in_switch_profiles_popup(
        page,
        page_display_name=pname,
        page_url=dest,
        max_list_scrolls=max_list_scrolls,
        menu_already_open=True,
    ):
        return False
    waited = _wait_after_page_switch_click(
        page, page_display_name=pname, page_url=dest, timeout_ms=12_000
    )
    return waited and _target_page_role_satisfied(page, dest, timeout_ms=1_200)


def _attempt_page_role_switch_clicks(page: Page, *, timeout_ms: int = 2_200) -> bool:
    """Thử bấm Switch Page — Switch Now trước, sau đó sidebar Switch."""
    _suppress_facebook_click_overlays(page)
    if _click_switch_now_banner(page, timeout_ms=timeout_ms):
        return True
    if _page_switch_sidebar_hint_visible(page, timeout_ms=500):
        _scroll_manage_page_sidebar_switch_cta(page)
        if _click_manage_page_sidebar_switch(page, timeout_ms=timeout_ms):
            return True
    if _click_switch_now_banner(page, timeout_ms=min(1800, timeout_ms)):
        return True
    if _click_manage_page_sidebar_switch(page, timeout_ms=timeout_ms):
        return True
    try:
        sw_block = page.locator(
            "xpath=(//*[contains(translate(normalize-space(.), "
            "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'switch into')]"
            "//*[@role='none' or self::span][.//span[normalize-space()='Switch'] "
            "or normalize-space()='Switch'])[last()]"
        )
        if _mouse_click_locator_center(page, sw_block, label="Switch block"):
            page.wait_for_timeout(random.randint(1800, 3200))
            return True
        if _click_visible_enabled_button(sw_block, timeout_ms=min(1600, timeout_ms)):
            return True
    except Exception:
        pass
    return False


def _try_page_role_switch_direct(
    page: Page,
    *,
    page_display_name: str = "",
    page_url: str = "",
    max_switch_attempts: int = 1,
) -> bool:
    """
    Switch Page trực tiếp trên trang hiện tại — một lượt nhanh + profile switcher (không lặp ensure).
    """
    pname = str(page_display_name or "").strip()
    dest = str(page_url or "").strip()

    if _page_role_acting_as_page(page, timeout_ms=700):
        if not dest or _urls_refer_same_facebook_page(dest, str(page.url or ""), page=page):
            return True

    if _quick_switch_on_page_surface(page, page_display_name=pname, page_url=dest):
        return True

    if pname and _select_page_via_profile_switcher(
        page, page_display_name=pname, page_url=dest, max_list_scrolls=6
    ):
        return True

    _ = max_switch_attempts  # giữ API tương thích
    return False


def _robust_switch_to_target_page(
    page: Page,
    *,
    page_display_name: str = "",
    page_url: str = "",
) -> bool:
    """
    Luồng switch Page cho đăng lịch.

    ① Page→page trực tiếp (ưu tiên — giữ flow cũ khi chuyển Page bình thường).
    ② Chỉ khi ① thất bại: về account chính → goto Page đích → switch lại.
    """
    pname = str(page_display_name or "").strip()
    dest = str(page_url or "").strip()
    guard_was_blocking = _view_only_guard_active_on_page(page)
    if guard_was_blocking:
        _disable_view_only_guard(page)
    try:
        if _target_page_role_satisfied(page, dest, timeout_ms=900):
            logger.info("[FB] Đã ở vai trò Page đúng đích.")
            return True

        # ① Một lượt switch nhanh trên bề mặt Page (không lặp ensure/recovery)
        if _quick_switch_on_page_surface(page, page_display_name=pname, page_url=dest):
            if not dest or _target_page_role_satisfied(page, dest, timeout_ms=900):
                logger.info("[FB] Switch Page OK — surface nhanh.")
                return True

        # ② Bắt buộc về account chính → chọn Page trong menu → goto đích → surface
        logger.info(
            "[FB] Surface switch thất bại — bắt buộc về account chính rồi switch Page đích."
        )
        if not _switch_to_personal_profile(page, force_home=True):
            logger.warning("[FB] Không xác nhận được account chính — vẫn thử profile switcher.")
        elif not _personal_reset_confirmed(page):
            logger.warning("[FB] Reset cá nhân chưa xác nhận — goto home thêm một lần.")
            _go_facebook_personal_home(page)

        for switcher_attempt in range(1, 3):
            if pname or dest:
                if _select_page_via_profile_switcher(
                    page, page_display_name=pname, page_url=dest, max_list_scrolls=8
                ):
                    logger.info(
                        "[FB] Switch Page OK — profile switcher từ account chính (attempt={}).",
                        switcher_attempt,
                    )
                    if dest:
                        navigate_to_url(page, dest)
                        page.wait_for_timeout(1_400)
                    if _target_page_role_satisfied(page, dest, timeout_ms=1_000):
                        return True
            if switcher_attempt >= 2:
                break
            if not _personal_reset_confirmed(page):
                logger.info("[FB] Profile switcher lần 1 chưa OK — reset cá nhân rồi thử lại.")
                _switch_to_personal_profile(page, force_home=True)
            page.wait_for_timeout(700)

        if dest:
            navigate_to_url(page, dest)
            page.wait_for_timeout(1_600)

        if _quick_switch_on_page_surface(page, page_display_name=pname, page_url=dest):
            if not dest or _target_page_role_satisfied(page, dest, timeout_ms=900):
                logger.info("[FB] Switch Page OK — surface sau account chính.")
                return True

        _failure_screenshot(page, "robust_page_switch_failed")
        return False
    finally:
        if guard_was_blocking:
            _enable_view_only_guard(page)


def _ensure_page_role_switched(
    page: Page,
    *,
    page_display_name: str = "",
    page_url: str = "",
    max_attempts: int = 2,
    wait_timeout_ms: int = 12_000,
) -> bool:
    """
    Chuyển sang vai trò Page — ủy quyền luồng ``_robust_switch_to_target_page`` (một điểm vào).

    Returns:
        True chỉ khi **đã switch vai trò Page** (không còn CTA Switch / menu bị khóa).
    """
    pname = str(page_display_name or "").strip()
    dest = str(page_url or "").strip()
    _ = max(1, min(int(max_attempts), 2))  # giữ API; robust đã giới hạn vòng lặp nội bộ
    _ = wait_timeout_ms
    guard_was_blocking = _view_only_guard_active_on_page(page)
    if guard_was_blocking:
        _disable_view_only_guard(page)
    try:
        if _target_page_role_satisfied(page, dest, timeout_ms=700):
            logger.info("[FB] Đã ở vai trò Page (không cần bấm Switch).")
            return True

        ok = _robust_switch_to_target_page(
            page,
            page_display_name=pname,
            page_url=dest,
        )
        if ok and _target_page_role_satisfied(page, dest, timeout_ms=800):
            return True
        if not ok:
            logger.warning("[FB] Vẫn chưa switch vào vai trò Page (robust thất bại).")
            _failure_screenshot(page, "page_switch_still_visible")
        return _target_page_role_satisfied(page, dest, timeout_ms=800)
    finally:
        if guard_was_blocking:
            _enable_view_only_guard(page)


def _ensure_switched_into_page_if_needed(
    page: Page,
    *,
    page_display_name: str = "",
    page_url: str = "",
) -> None:
    """
    Nếu Facebook hiển thị banner hoặc sidebar yêu cầu switch sang Page thì bấm Switch.

    Luồng đầy đủ: Switch Now → sidebar → reset cá nhân → goto Page đích.
    """
    try:
        need_switch = (
            _page_switch_ui_visible(page, timeout_ms=1_200)
            or not _page_role_acting_as_page(page, timeout_ms=500)
        )
        if need_switch or str(page_url or "").strip():
            _robust_switch_to_target_page(
                page,
                page_display_name=page_display_name,
                page_url=page_url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không xử lý được banner switch page: {}", exc)


_SWITCH_PROFILES_LIST_SCROLL_JS = """
() => {
  const root = document.querySelector('[role="dialog"]') || document.body;
  const nodes = Array.from(root.querySelectorAll('*')).filter(el => {
    try {
      const s = getComputedStyle(el);
      const oy = s.overflowY;
      return (oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight + 48;
    } catch (e) { return false; }
  });
  nodes.sort((a, b) => (b.scrollHeight - b.clientHeight) - (a.scrollHeight - a.clientHeight));
  const el = nodes[0] || root;
  const step = Math.max(180, Math.floor((el.clientHeight || 320) * 0.72));
  el.scrollTop = Math.min(el.scrollTop + step, el.scrollHeight);
  return true;
}
"""


def _scroll_switch_profiles_list(page: Page) -> None:
    """Cuộn danh sách Page/profile trong popup Switch profiles (lazy list)."""
    try:
        page.evaluate(_SWITCH_PROFILES_LIST_SCROLL_JS)
    except Exception:
        pass
    page.wait_for_timeout(220)


def _select_page_in_switch_profiles_popup(
    page: Page,
    *,
    page_display_name: str,
    page_url: str,
    max_list_scrolls: int = 10,
    menu_already_open: bool = False,
) -> bool:
    """Chọn đúng Page trong popup Switch profiles — có cuộn để tìm mục (giới hạn vòng)."""
    pname = str(page_display_name or "").strip()
    dest = str(page_url or "").strip()
    expect_id = extract_facebook_numeric_id_from_url(dest)
    if len(pname) < 2 and not expect_id:
        return False

    see_all = re.compile(
        r"See all profiles|Xem tất cả hồ sơ|Switch profiles|Chuyển hồ sơ|Chuyển sang",
        re.I,
    )
    if not _switch_profiles_dialog_visible(page, timeout_ms=500):
        if not menu_already_open:
            if not _open_facebook_profile_switcher_menu(page):
                return False
        _click_visible_enabled_button(
            page.get_by_role("menuitem", name=see_all), timeout_ms=900, human_label=""
        )
        _click_visible_enabled_button(page.get_by_text(see_all), timeout_ms=700, human_label="")
        page.wait_for_timeout(700)

    scroll_limit = max(3, min(int(max_list_scrolls), 8))
    fast_click_ms = 420

    for scroll_i in range(scroll_limit):
        if _switch_profiles_dialog_visible(page, timeout_ms=250):
            scope = _switch_profiles_dialog_scope(page)
        else:
            scope = page.locator("[role='menu'], [role='listbox']").last
        if _try_click_page_match_in_scope(
            scope,
            page_display_name=pname,
            page_url=dest,
            timeout_ms=fast_click_ms,
        ):
            logger.info("[FB] Đã chọn Page trong Switch profiles: {!r}", pname)
            page.wait_for_timeout(900)
            return True
        if scroll_i + 1 >= scroll_limit:
            break
        if not _switch_profiles_dialog_visible(page, timeout_ms=250):
            if scroll_i >= 1:
                break
        _scroll_switch_profiles_list(page)
    return False


def _ensure_reel_dashboard_page_context(
    page: Page,
    *,
    page_url: str,
    page_display_name: str = "",
) -> None:
    """
    Mở đúng ``page_url``, switch sang vai trò Page (banner / Switch profiles + cuộn),
    xác minh bề mặt trước khi vào Professional Dashboard.
    """
    dest = _fb_normalize_client_url(str(page_url or "").strip())
    pname = str(page_display_name or "").strip()
    if not dest:
        raise ValueError("Thiếu page_url cho luồng Reel dashboard.")
    if _view_only_guard_active_on_page(page):
        _disable_view_only_guard(page)
    navigate_to_url(page, dest)
    page.wait_for_timeout(2_200)
    if not _robust_switch_to_target_page(page, page_display_name=pname, page_url=dest):
        _failure_screenshot(page, "reel_page_switch_failed")
        raise RuntimeError(
            f"Không switch được sang vai trò Page. dest={dest!r} | url={page.url}"
        )
    _require_page_role_switched(
        page,
        page_display_name=pname,
        page_url=dest,
        context="reel_open_page",
    )
    if dest and not _urls_refer_same_facebook_page(dest, str(page.url or ""), page=page):
        logger.warning(
            "[FB] URL sau switch khác đích job (có thể redirect slug): dest={!r} | url={}",
            dest,
            page.url,
        )
    logger.info("[FB] Bề mặt Page + vai trò Page: {}", page.url)


def open_post_box(page: Page) -> None:
    """
    Mở ô soạn bài viết (composer) trên bảng tin.

    Thử theo thứ tự: ``aria-label`` (đa ngôn ngữ) → ``role`` → XPath theo nội dung.

    Args:
        page: Trang Facebook bảng tin.

    Raises:
        PlaywrightTimeoutError: Không mở được composer.
    """
    try:
        # Nếu textbox đã hiện sẵn thì không cần bấm nút mở composer nữa.
        try:
            _wait_first_selector(
                page,
                (
                    "[role='textbox'][data-testid='status-attachment-mentions-input']",
                    "[role='textbox'][aria-multiline='true']",
                    "div[role='textbox'][contenteditable='true']",
                ),
                step_timeout_ms=6_000,
                error_label="open_post_box composer textbox precheck",
            )
            logger.info("Composer đã có sẵn, bỏ qua bước bấm mở ô đăng.")
            _enable_view_only_guard(page)
            return
        except PlaywrightTimeoutError:
            pass

        open_selectors_primary = (
            # Nút mở composer kiểu mới (ưu tiên aria/role).
            "[role='button'][aria-label*='Create']",
            "[role='button'][aria-label*='Write']",
            "[role='button'][aria-label*='on your mind']",
            "[role='button'][aria-label*='Tạo bài viết']",
            "[role='button'][aria-label*='Viết bài']",
            "[role='button'][aria-label*='Bạn viết gì']",
            "[aria-label*='Create a post']",
            "[aria-label*='What\\'s on your mind']",
            "[aria-label*='Tạo bài viết']",
            "[aria-label*='Bạn viết gì']",
            # Các bề mặt page/group hay dùng text trong span/div.
            "xpath=//div[@role='button' and .//span[contains(., \"What's on your mind\")]]",
            "xpath=//div[@role='button' and .//span[contains(., 'Create post')]]",
            "xpath=//div[@role='button' and .//span[contains(., 'Create a post')]]",
            "xpath=//div[@role='button' and .//span[contains(., 'Tạo bài viết')]]",
            "xpath=//div[@role='button' and .//span[contains(., 'Viết bài')]]",
            "xpath=//div[@role='button' and .//span[contains(., 'Bạn viết gì')]]",
            # Fallback cũ vẫn giữ lại
            "xpath=//span[contains(., \"What's on your mind\")]",
            "xpath=//span[contains(., 'Tạo bài viết')]",
            "xpath=//span[contains(., 'Có chuyện gì')]",
        )

        sel = _wait_first_selector(
            page,
            open_selectors_primary,
            step_timeout_ms=9_000,
            error_label="open_post_box primary",
        )
        page.wait_for_selector(sel, state="visible", timeout=12_000)
        page.locator(sel).first.click(timeout=12_000, force=True)
        _human_pause()

        # Chờ textbox; nếu chưa thấy thì scroll nhẹ và thử click lần 2 với fallback mở composer.
        try:
            _wait_first_selector(
                page,
                (
                    "[role='textbox'][data-testid='status-attachment-mentions-input']",
                    "[role='textbox'][aria-multiline='true']",
                    "div[role='textbox'][contenteditable='true']",
                ),
                step_timeout_ms=10_000,
                error_label="open_post_box composer textbox pass1",
            )
        except PlaywrightTimeoutError:
            scroll_randomly(page)
            sel2 = _wait_first_selector(
                page,
                open_selectors_primary,
                step_timeout_ms=8_000,
                error_label="open_post_box retry_open",
            )
            page.locator(sel2).first.click(timeout=10_000, force=True)
            _human_pause()
            _wait_first_selector(
                page,
                (
                    "[role='textbox'][data-testid='status-attachment-mentions-input']",
                    "[role='textbox'][aria-multiline='true']",
                    "div[role='textbox'][contenteditable='true']",
                ),
                step_timeout_ms=15_000,
                error_label="open_post_box composer textbox pass2",
            )
        _enable_view_only_guard(page)
    except PlaywrightTimeoutError:
        _enable_view_only_guard(page)
        raise
    except Exception as exc:
        _enable_view_only_guard(page)
        _failure_screenshot(page, f"open_post_box: {exc}")
        raise


def fill_content(page: Page, text: str) -> None:
    """
    Gõ nội dung bài viết bằng ``page.type`` với delay ngẫu nhiên 100–300 ms mỗi ký tự.

    Args:
        page: Trang Facebook với composer đã mở.
        text: Nội dung văn bản.

    Raises:
        PlaywrightTimeoutError: Không thấy ô nhập nội dung.
    """
    try:
        selector = _wait_first_selector(
            page,
            (
                "[role='textbox'][data-testid='status-attachment-mentions-input']",
                # Cách 2 (composer mới): ô nhập chung caption + hashtag trong post details.
                "div.notranslate._5rpu[role='combobox'][contenteditable='true'][aria-label*='dialogue box' i]",
                "div.notranslate._5rpu[role='combobox'][contenteditable='true']",
                "div[role='combobox'][contenteditable='true'][aria-label*='dialogue box' i]",
                "div[role='combobox'][contenteditable='true'][aria-label*='include text' i]",
                "div[role='combobox'][contenteditable='true'][aria-label*='Write' i]",
                "textarea[placeholder*='Text' i]",
                "textarea[aria-label*='Text' i]",
                "textarea[aria-label*='Nội dung' i]",
                # Composer chính của post (tránh ô comment).
                "div[role='textbox'][contenteditable='true'][aria-label*='Write'][aria-label*='post' i]",
                "div[role='textbox'][contenteditable='true'][aria-label*='Viết'][aria-label*='bài' i]",
                "[role='textbox'][aria-multiline='true']",
                "div[role='textbox'][contenteditable='true']",
            ),
            step_timeout_ms=20_000,
            error_label="fill_content",
        )
        # Nếu match nhầm textbox comment thì thử selector khác.
        try:
            aria_label = str(
                page.locator(selector)
                .first.evaluate("(el) => (el.getAttribute('aria-label') || '').toString()")
            ).strip()
            if "Comment as" in aria_label or "Bình luận với tư cách" in aria_label:
                selector = _wait_first_selector(
                    page,
                    (
                        "div.notranslate._5rpu[role='combobox'][contenteditable='true'][aria-label*='dialogue box' i]",
                        "div.notranslate._5rpu[role='combobox'][contenteditable='true']",
                        "div[role='combobox'][contenteditable='true'][aria-label*='dialogue box' i]",
                        "div[role='combobox'][contenteditable='true'][aria-label*='include text' i]",
                        "div[role='combobox'][contenteditable='true'][aria-label*='Write' i]",
                        "div[role='textbox'][contenteditable='true'][aria-label*='Write'][aria-label*='post' i]",
                        "div[role='textbox'][contenteditable='true'][aria-label*='Viết'][aria-label*='bài' i]",
                        "div[role='textbox'][contenteditable='true']:not([aria-label*='Comment'])",
                    ),
                    step_timeout_ms=12_000,
                    error_label="fill_content non-comment textbox",
                )
        except Exception:
            pass
        page.wait_for_selector(selector, state="visible", timeout=15_000)
        human_pause(kind="click", label="focus ô nhập caption")
        page.click(selector, timeout=10_000, force=True)
        page.wait_for_selector(selector, state="visible", timeout=15_000)
        human_pause(kind="input", label="sau focus ô nhập")
        delay = _typing_delay_ms()
        # Business Composer có thể dùng combobox/contenteditable: ưu tiên nhập nhanh.
        try:
            tag = str(page.locator(selector).first.evaluate("(el) => (el.tagName || '').toLowerCase()"))
        except Exception:
            tag = ""
        sel_low = selector.lower()
        is_fast_path = ("role='combobox'" in sel_low) or ("contenteditable='true'" in sel_low) or len(text) > 350
        if tag in ("textarea", "input"):
            try:
                page.click(selector, timeout=8_000, force=True)
            except Exception:
                pass
            try:
                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
            except Exception:
                pass
            try:
                page.keyboard.insert_text(text)
                used = "paste(input/textarea)"
            except Exception:
                page.fill(selector, text, timeout=30_000)
                used = "fill(input/textarea)"
        else:
            # Ưu tiên copy-dán cho editor rich-text; fallback dần nếu editor không hỗ trợ.
            try:
                page.keyboard.insert_text(text)
                used = "paste(contenteditable)"
            except Exception:
                try:
                    page.locator(selector).first.fill(text, timeout=30_000)
                    used = "fill(contenteditable)"
                except Exception:
                    # Cuối cùng mới dùng type chậm.
                    page.type(selector, text, delay=delay)
                    used = f"type(delay={delay})"
        # Xác minh nội dung đã thực sự vào composer; nếu chưa thì ghi cưỡng bức bằng JS cho contenteditable.
        needle = (text or "").strip().replace("\r", "\n")
        needle_short = " ".join(needle.split())[:80]
        ok = False
        try:
            if tag in ("textarea", "input"):
                cur = str(page.locator(selector).first.input_value(timeout=5_000) or "")
            else:
                cur = str(
                    page.locator(selector)
                    .first.evaluate(
                        "(el) => ((el.innerText || el.textContent || el.value || '').toString())"
                    )
                    or ""
                )
            ok = needle_short and (needle_short.lower() in " ".join(cur.split()).lower())
        except Exception:
            ok = False
        if not ok and tag not in ("textarea", "input"):
            # Fallback chuẩn cho editor rich-text (Lexical/DraftJS trong Business
            # Composer): KHÔNG ghi innerHTML (sẽ lệch state của framework). Thay vào
            # đó bắn synthetic `paste` event kèm DataTransfer + `beforeinput`
            # (inputType=insertFromPaste) — đúng cách browser báo cho editor khi
            # user thật dán nội dung → framework cập nhật state nội tại.
            try:
                page.locator(selector).first.evaluate(
                    """(el, val) => {
                      try { el.focus(); } catch (_) {}
                      // Xoá nội dung cũ bằng selection + execCommand để editor tự cập nhật state.
                      try {
                        const sel = window.getSelection();
                        const r = document.createRange();
                        r.selectNodeContents(el);
                        sel.removeAllRanges();
                        sel.addRange(r);
                        document.execCommand('delete', false);
                      } catch (_) {}
                      const makeDT = () => {
                        try {
                          const dt = new DataTransfer();
                          dt.setData('text/plain', val);
                          return dt;
                        } catch (_) { return null; }
                      };
                      // 1) beforeinput inputType=insertFromPaste → Lexical nghe sự kiện này.
                      try {
                        const dt = makeDT();
                        const ev = new InputEvent('beforeinput', {
                          inputType: 'insertFromPaste',
                          data: val,
                          dataTransfer: dt,
                          bubbles: true,
                          cancelable: true,
                        });
                        el.dispatchEvent(ev);
                      } catch (_) {}
                      // 2) paste event với clipboardData.
                      try {
                        const dt = makeDT();
                        const ev = new ClipboardEvent('paste', {
                          clipboardData: dt,
                          bubbles: true,
                          cancelable: true,
                        });
                        try { Object.defineProperty(ev, 'clipboardData', { value: dt }); } catch (_) {}
                        el.dispatchEvent(ev);
                      } catch (_) {}
                      // 3) Input event finalise để framework flush state.
                      try {
                        el.dispatchEvent(new InputEvent('input', {
                          inputType: 'insertFromPaste',
                          data: val,
                          bubbles: true,
                        }));
                      } catch (_) {
                        try { el.dispatchEvent(new Event('input', { bubbles: true })); } catch (_) {}
                      }
                    }""",
                    text,
                )
                used = used + " -> synthetic_paste"
            except Exception:
                pass
        logger.info("Đã nhập nội dung ({} ký tự, mode={}).", len(text), used)
        human_pause(kind="input", label="sau nhập caption")
        _enable_view_only_guard(page)
    except PlaywrightTimeoutError:
        _enable_view_only_guard(page)
        raise
    except Exception as exc:
        _enable_view_only_guard(page)
        _failure_screenshot(page, f"fill_content: {exc}")
        raise


def ensure_content_present(page: Page, text: str) -> None:
    """Kiểm tra composer còn giữ ``text`` ngay trước khi Publish; nếu mất → nhập lại.

    Sau khi thêm media, Lexical/DraftJS có thể re-render composer và làm mất nội dung
    đã nhập trước đó (đặc biệt khi nhập bằng innerHTML/js_set). Hàm này xác minh nội
    dung vẫn còn trong editor; nếu không, gọi lại :func:`fill_content` một lần nữa.

    Args:
        page: Trang Facebook với composer đã mở.
        text: Nội dung mong đợi đang có trong editor.
    """
    needle = (text or "").strip()
    if not needle:
        return
    needle_short = " ".join(needle.split())[:80].lower()
    if not needle_short:
        return
    # Tạm gỡ view-only guard để composer nhận focus khi cần nhập lại.
    _disable_view_only_guard(page)
    candidates = (
        "[role='textbox'][data-testid='status-attachment-mentions-input']",
        "div.notranslate._5rpu[role='combobox'][contenteditable='true'][aria-label*='dialogue box' i]",
        "div.notranslate._5rpu[role='combobox'][contenteditable='true']",
        "div[role='combobox'][contenteditable='true'][aria-label*='dialogue box' i]",
        "div[role='combobox'][contenteditable='true'][aria-label*='include text' i]",
        "div[role='combobox'][contenteditable='true'][aria-label*='Write' i]",
        "textarea[placeholder*='Text' i]",
        "textarea[aria-label*='Text' i]",
        "textarea[aria-label*='Nội dung' i]",
        "div[role='textbox'][contenteditable='true'][aria-label*='Write'][aria-label*='post' i]",
        "div[role='textbox'][contenteditable='true'][aria-label*='Viết'][aria-label*='bài' i]",
        "[role='textbox'][aria-multiline='true']",
        "div[role='textbox'][contenteditable='true']",
    )
    current = ""
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue
            try:
                tag = str(loc.evaluate("(el) => (el.tagName || '').toLowerCase()"))
            except Exception:
                tag = ""
            if tag in ("textarea", "input"):
                try:
                    val = str(loc.input_value(timeout=2_000) or "")
                except Exception:
                    val = ""
            else:
                try:
                    val = str(
                        loc.evaluate(
                            "(el) => ((el.innerText || el.textContent || el.value || '').toString())"
                        )
                        or ""
                    )
                except Exception:
                    val = ""
            val_norm = " ".join(val.split()).lower()
            if val_norm:
                current = val_norm
                if needle_short in val_norm:
                    logger.info(
                        "[FB verify-content] Nội dung vẫn còn trong composer ({} ký tự).",
                        len(val),
                    )
                    _enable_view_only_guard(page)
                    return
                break
        except Exception:
            continue
    logger.warning(
        "[FB verify-content] Editor trống/không chứa caption trước khi Publish "
        "(snapshot={!r}). Nhập lại nội dung.",
        current[:80],
    )
    try:
        fill_content(page, text)
    except Exception as exc:
        logger.warning("[FB verify-content] Re-fill thất bại: {}", exc)
        _enable_view_only_guard(page)


def upload_photo(page: Page, image_path: str | Path) -> None:
    """
    Đính kèm ảnh vào bài viết qua input file (chờ selector trước khi gán file).

    Args:
        page: Trang Facebook với composer đã mở.
        image_path: Đường dẫn file ảnh.

    Raises:
        PlaywrightTimeoutError: Không tìm thấy ô upload.
        FileNotFoundError: File ảnh không tồn tại.
    """
    path = _resolve_path(image_path)
    if not path.is_file():
        raise FileNotFoundError(str(path))
    unlock_for_upload = _view_only_mode_enabled()
    if unlock_for_upload:
        # Guard chỉ để chặn user; tắt tạm lúc mở file chooser/set_input_files để tránh chặn upload.
        _disable_view_only_guard(page)
    try:
        if _is_meta_business_composer_context(page):
            if _attach_media_automatic(page, path, kind="image", context="composer_photo"):
                logger.info("Đã gắn file ảnh (tự động, không popup OS): {}", path)
                _human_pause()
                _enable_view_only_guard(page)
                return
            raise RuntimeError("Không tìm được input upload ảnh trong Business Composer.")
        sel = _wait_first_selector(
            page,
            (
                "input[type='file'][accept*='image']",
                "input[type='file']",
                "xpath=//input[@type='file' and contains(@accept,'image')]",
            ),
            step_timeout_ms=20_000,
            error_label="upload_photo",
            state="attached",
        )
        page.wait_for_selector(sel, state="attached", timeout=20_000)
        page.set_input_files(sel, str(path))
        logger.info("Đã gắn file ảnh: {}", path)
        _human_pause()
        _enable_view_only_guard(page)
    except PlaywrightTimeoutError:
        _enable_view_only_guard(page)
        raise
    except Exception as exc:
        _enable_view_only_guard(page)
        _failure_screenshot(page, f"upload_photo: {exc}")
        raise


_MUTE_MEDIA_JS = """() => {
    const muteVideo = (v) => {
        if (!v || v.nodeName !== 'VIDEO') return;
        try {
            v.muted = true;
            v.defaultMuted = true;
            v.volume = 0;
            v.setAttribute('muted', '');
        } catch (e) {}
    };
    const muteAudio = (a) => {
        if (!a || a.nodeName !== 'AUDIO') return;
        try {
            a.muted = true;
            a.volume = 0;
            a.setAttribute('muted', '');
        } catch (e) {}
    };
    const muteTree = (root) => {
        let n = 0;
        const el = root || document;
        if (!el || !el.querySelectorAll) return 0;
        el.querySelectorAll('video').forEach((v) => { muteVideo(v); n += 1; });
        el.querySelectorAll('audio').forEach((a) => { muteAudio(a); n += 1; });
        return n;
    };
    let n = muteTree(document);
    if (!window.__toolfb_media_mute_observer) {
        window.__toolfb_media_mute_observer = true;
        try {
            new MutationObserver((mutations) => {
                for (const m of mutations) {
                    m.addedNodes.forEach((node) => {
                        if (!node || node.nodeType !== 1) return;
                        if (node.nodeName === 'VIDEO') muteVideo(node);
                        if (node.nodeName === 'AUDIO') muteAudio(node);
                        muteTree(node);
                    });
                }
            }).observe(document.documentElement, { childList: true, subtree: true });
        } catch (e) {}
    }
    return n;
}"""

_MUTE_MEDIA_IN_ROOT_JS = """(root) => {
    const muteVideo = (v) => {
        if (!v || v.nodeName !== 'VIDEO') return;
        try {
            v.muted = true;
            v.defaultMuted = true;
            v.volume = 0;
            v.setAttribute('muted', '');
        } catch (e) {}
    };
    const muteAudio = (a) => {
        if (!a || a.nodeName !== 'AUDIO') return;
        try {
            a.muted = true;
            a.volume = 0;
            a.setAttribute('muted', '');
        } catch (e) {}
    };
    let n = 0;
    const el = root && root.querySelectorAll ? root : null;
    if (!el) return 0;
    el.querySelectorAll('video').forEach((v) => { muteVideo(v); n += 1; });
    el.querySelectorAll('audio').forEach((a) => { muteAudio(a); n += 1; });
    return n;
}"""


def _mute_browser_video_previews(
    page: Page,
    *,
    scope: Locator | None = None,
    silent: bool = False,
) -> int:
    """
    Tắt tiếng mọi ``<video>`` / ``<audio>`` trong tab (và iframe) sau import preview.

    Gắn ``MutationObserver`` (một lần mỗi frame) để video/audio thêm sau vẫn bị mute.
    """
    total = 0
    try:
        if scope is not None:
            try:
                total += int(scope.evaluate(_MUTE_MEDIA_IN_ROOT_JS) or 0)
            except Exception:
                pass
        total += int(page.evaluate(_MUTE_MEDIA_JS) or 0)
        for frame in page.frames:
            try:
                total += int(frame.evaluate(_MUTE_MEDIA_JS) or 0)
            except Exception:
                pass
    except Exception as exc:
        logger.debug("[FB] mute video preview: {}", exc)
    if total > 0 and not silent:
        logger.info("[FB] Đã tắt tiếng preview media trong trình duyệt ({} phần tử).", total)
    return total


def _mute_browser_video_previews_after_attach(
    page: Page,
    *,
    scope: Locator | None = None,
    attempts: int = 5,
    interval_ms: int = 450,
) -> int:
    """Tắt tiếng lặp vài lần — preview Meta thường mount ``<video>`` chậm sau ``set_input_files``."""
    total = 0
    tries = max(1, int(attempts))
    for i in range(tries):
        total += _mute_browser_video_previews(page, scope=scope, silent=(i > 0))
        if i + 1 < tries:
            try:
                page.wait_for_timeout(max(120, int(interval_ms)))
            except Exception:
                break
    if total > 0:
        logger.info(
            "[FB] Đã tắt tiếng preview media sau import ({} lần quét, {} phần tử).",
            tries,
            total,
        )
    return total


def upload_video(page: Page, video_path: str | Path) -> None:
    """
    Đính kèm video vào composer (input file). Chờ preview/video element lâu hơn ảnh.
    """
    path = _resolve_path(video_path)
    if not path.is_file():
        raise FileNotFoundError(str(path))
    unlock_for_upload = _view_only_mode_enabled()
    if unlock_for_upload:
        # Guard chỉ để chặn user; tắt tạm lúc mở file chooser/set_input_files để tránh chặn upload.
        _disable_view_only_guard(page)
    try:
        if _is_meta_business_composer_context(page):
            if _attach_media_automatic(page, path, kind="video", context="composer_video"):
                logger.info("{} Đã gắn file video (tự động, không popup OS): {}", _reel_strict_prefix("Upload"), path)
                try:
                    page.locator("video").first.wait_for(state="visible", timeout=22_000)
                except PlaywrightTimeoutError:
                    logger.warning(
                        "{} Không thấy thẻ video sau upload — tiếp tục bước tiếp theo, chờ wizard xác nhận.",
                        _reel_strict_prefix("Upload"),
                    )
                _mute_browser_video_previews_after_attach(page)
                _human_pause()
                _enable_view_only_guard(page)
                return
            raise RuntimeError("Không tìm được input upload video trong Business Composer.")
        sel = _wait_first_selector(
            page,
            (
                "input[type='file'][accept*='video']",
                "input[type='file']",
            ),
            step_timeout_ms=25_000,
            error_label="upload_video",
            state="attached",
        )
        page.wait_for_selector(sel, state="attached", timeout=25_000)
        page.set_input_files(sel, str(path))
        logger.info("{} Đã gắn file video: {}", _reel_strict_prefix("Upload"), path)
        try:
            page.locator("video").first.wait_for(state="visible", timeout=22_000)
        except PlaywrightTimeoutError:
            logger.warning("{} Không thấy thẻ video sau upload — tiếp tục bước tiếp theo, chờ wizard xác nhận.", _reel_strict_prefix("Upload"))
        _mute_browser_video_previews_after_attach(page)
        _human_pause()
        _enable_view_only_guard(page)
    except PlaywrightTimeoutError:
        _enable_view_only_guard(page)
        raise
    except Exception as exc:
        _enable_view_only_guard(page)
        _failure_screenshot(page, f"upload_video: {exc}")
        raise


def _wait_click_locator_when_ready(loc: Locator, *, timeout_ms: int = 120_000) -> None:
    """
    Chờ ``aria-busy``/``aria-disabled`` tắt rồi click một locator nút (role=button hoặc tương đương).

    Args:
        loc: Playwright ``Locator`` (``.first`` nên gắn sẵn).
        timeout_ms: Thời gian chờ tối đa.
    """
    loc.wait_for(state="visible", timeout=min(60_000, timeout_ms))
    deadline = time.time() + timeout_ms / 1000.0
    pg = loc.page
    while time.time() < deadline:
        try:
            if not loc.is_visible(timeout=500):
                pg.wait_for_timeout(300)
                continue
            try:
                loc.scroll_into_view_if_needed(timeout=3_000)
            except Exception:
                pass
            busy = (loc.get_attribute("aria-busy") or "").lower() == "true"
            dis = (loc.get_attribute("aria-disabled") or "").lower() == "true"
            if not busy and not dis:
                human_pause(kind="click", label="trước click nút")
                try:
                    loc.click(timeout=15_000, force=True)
                except Exception:
                    # Meta / Firefox: lớp phủ hoặc hit-target lệch — thử HTMLElement.click().
                    loc.evaluate("el => { if (el && typeof el.click === 'function') el.click(); }")
                human_pause(kind="click", label="sau click nút")
                return
        except Exception:
            pass
        pg.wait_for_timeout(350)
    raise PlaywrightTimeoutError("Timeout chờ nút sẵn sàng (busy/disabled).")


def _locator_meta_reel_next_structural(page: Page) -> Locator:
    """``div[role=button]`` có descendant đúng chữ Next (khớp DOM Meta Business, không phụ thuộc class xoay)."""
    return page.locator("div[role='button']").filter(has=page.get_by_text("Next", exact=True))


def _locator_meta_reel_next_role(page: Page) -> Locator:
    """Nút Next theo accessibility tree (Playwright)."""
    return page.get_by_role("button", name=re.compile(r"^\s*Next\s*$", re.I))


def _locator_meta_reel_next_text_parent(page: Page) -> Locator:
    """
    Fallback cho UI Meta không expose role=button:
    <div ...><div ...>Next</div></div> -> click parent của node text "Next".
    """
    return page.locator("xpath=//div[normalize-space()='Next']/parent::div")


def _locator_meta_reel_footer_next_with_cancel(page: Page) -> Locator:
    """
    Ưu tiên nút Next ở footer wizard có đi kèm nút Cancel cùng hàng.
    """
    return page.locator(
        "xpath=//div[(self::div or self::button) and normalize-space()='Next' and "
        "ancestor::*[.//*[normalize-space()='Cancel']]][1]"
    )


def _meta_reel_next_any_visible(page: Page) -> bool:
    for base in (
        _locator_meta_reel_footer_next_with_cancel(page),
        _locator_meta_reel_next_structural(page),
        _locator_meta_reel_next_role(page),
        _locator_meta_reel_next_text_parent(page),
    ):
        try:
            n = min(base.count(), 12)
        except Exception:
            continue
        if n <= 0:
            continue
        for i in range(n):
            try:
                if base.nth(i).is_visible(timeout=400):
                    return True
            except Exception:
                continue
    return False


_REEL_NEXT_LABEL_RE = re.compile(r"^(Next|Tiếp|Tiếp theo)$", re.I)

# Nút Next dashboard Reel — cùng pattern html-div như Post
_REEL_NEXT_STRICT_XPATHS: tuple[str, ...] = (
    "(//div[contains(@class,'html-div')][.//div[@role='none']//span[contains(@class,'x1j85h84') and normalize-space()='Next']])[last()]",
    "(//div[contains(@class,'html-div')][.//div[@role='none']//span[normalize-space()='Next']])[last()]",
    "(//div[contains(@class,'html-div')][.//div[@role='none']//span[normalize-space()='Tiếp']])[last()]",
    "(//span[normalize-space()='Next']/ancestor::div[contains(@class,'html-div')][1])[last()]",
    "(//div[@role='none' and .//span[normalize-space()='Next']])[last()]",
    "(//div[@role='button' and @tabindex='0' and .//div[normalize-space()='Next']])[last()]",
)

_REEL_NEXT_CSS_SELECTORS: tuple[str, ...] = (
    "div.html-div:has(div[role='none'] span:has-text('Next'))",
    "div.html-div div[role='none'] span:has-text('Next')",
    "div.html-div div[role='none'] span:has-text('Tiếp')",
)

_REEL_WIZARD_PROCESSING_MARKERS: tuple[str, ...] = (
    "Checking for copyrighted content",
    "Đang kiểm tra bản quyền",
    "Processing",
    "Uploading",
    "Đang xử lý",
    "đang tải",
    "Your reel is being processed",
)


def _reel_wizard_processing(page: Page, *, timeout_ms: int = 280) -> bool:
    """Upload/copyright/Processing — chờ, chưa bấm Next."""
    for m in _REEL_WIZARD_PROCESSING_MARKERS:
        try:
            if page.get_by_text(m, exact=False).first.is_visible(timeout=timeout_ms):
                return True
        except Exception:
            continue
    if _meta_reel_next_any_visible(page) and not _meta_reel_next_clickable(page):
        return True
    return False


def _try_click_reel_wizard_next(page: Page) -> bool:
    """Bấm Next — mọi biến thể UI (role=button, html-div, dialog/page)."""
    if _click_meta_reel_next_strict(page):
        return True
    for xp in _REEL_NEXT_STRICT_XPATHS:
        try:
            loc = page.locator(f"xpath={xp}")
            if loc.count() <= 0 or not loc.last.is_visible(timeout=600):
                continue
            try:
                loc.last.evaluate(_REEL_POST_CLICK_JS)
            except Exception:
                loc.last.click(timeout=1400, force=True, no_wait_after=True)
            return True
        except Exception:
            continue
    for css in _REEL_NEXT_CSS_SELECTORS:
        try:
            loc = page.locator(css).last
            if loc.count() > 0 and loc.is_visible(timeout=500):
                try:
                    loc.evaluate(_REEL_POST_CLICK_JS)
                except Exception:
                    loc.click(timeout=1400, force=True, no_wait_after=True)
                return True
        except Exception:
            continue
    return _click_meta_reel_next_best_effort(page)


def _wait_after_reel_next_click(page: Page, *, prev_label: str = "") -> None:
    """Chờ UI chuyển màn sau Next (caption / post / hết processing)."""
    inner = time.time() + 20.0
    while time.time() < inner:
        if prev_label and _reel_active_step_label(page) != prev_label:
            break
        if _reel_caption_input_usable(page, timeout_ms=320):
            break
        if _reel_post_button_maybe_visible(page, timeout_ms=320):
            break
        if not _reel_wizard_processing(page, timeout_ms=220):
            break
        page.wait_for_timeout(320)
    page.wait_for_timeout(random.randint(700, 1400))


def _reel_wizard_needs_next(
    page: Page,
    *,
    payload: str,
    filled: bool,
    next_clicks: int = 0,
) -> bool:
    """
    Cần bấm Next khi:
    - Chưa có ô caption (cần Next tới màn nhập), hoặc
    - Đã nhập caption nhưng chưa thấy Post.
    """
    if _reel_wizard_ready_to_post(
        page, payload=payload, filled=filled, next_clicks=next_clicks
    ):
        return False
    if payload and not filled:
        if _reel_caption_input_usable(page, timeout_ms=400):
            return False
        return True
    if payload and filled:
        if _reel_strict_post_button_visible(page, timeout_ms=350):
            return False
        if _reel_settings_screen_visible(page, timeout_ms=280):
            return False
        return True
    return not _reel_strict_post_button_visible(page, timeout_ms=300)


def _reel_edit_reel_header_visible(page: Page, *, timeout_ms: int = 350) -> bool:
    """Tiêu đề màn «Edit reel» — chưa phải màn nhập caption."""
    for rx in (r"^\s*Edit reel\s*$", r"^\s*Chỉnh sửa Thước phim\s*$"):
        try:
            if page.get_by_text(re.compile(rx, re.I)).first.is_visible(timeout=timeout_ms):
                return True
        except Exception:
            continue
    return False


def _reel_caption_screen_markers_visible(page: Page, *, timeout_ms: int = 450) -> bool:
    """Nhãn / ô mô tả caption thật (tránh false positive trên Edit reel)."""
    patterns = (
        r"Describe your reel",
        r"Let viewers know",
        r"Write into the dialogue",
        r"Say something about",
        r"Add a caption",
        r"Viết mô tả",
        r"Chi tiết.*Reel",
    )
    for pat in patterns:
        try:
            if page.get_by_text(re.compile(pat, re.I)).first.is_visible(timeout=timeout_ms):
                return True
        except Exception:
            continue
    try:
        tb = page.get_by_role("textbox", name=re.compile(r"Write into the dialogue box", re.I))
        n = min(int(tb.count()), 8)
        for i in range(n - 1, -1, -1):
            if tb.nth(i).is_visible(timeout=timeout_ms):
                return True
    except Exception:
        pass
    if _reel_lexical_description_usable(page, timeout_ms=timeout_ms):
        return True
    return False


# Lexical editor trên màn Edit reel / Post details (Meta dashboard Reel).
_REEL_LEXICAL_TEXTBOX_SELECTORS: tuple[str, ...] = (
    "div.notranslate[role='textbox'][contenteditable='true'][data-lexical-editor='true'][aria-placeholder*='Describe' i]",
    "[role='textbox'][contenteditable='true'][data-lexical-editor='true'][aria-placeholder*='Describe' i]",
    "[role='textbox'][contenteditable='true'][data-lexical-editor='true'][aria-placeholder*='reel' i]",
    "[role='textbox'][contenteditable='true'][data-lexical-editor='true'][aria-placeholder*='Thước phim' i]",
    "[role='textbox'][contenteditable='true'][data-lexical-editor='true'][aria-placeholder*='Mô tả' i]",
    "div.notranslate[role='textbox'][contenteditable='true'][data-lexical-editor='true']",
    "[role='textbox'][contenteditable='true'][data-lexical-editor='true']",
)


def _reel_lexical_description_locators(page: Page, *, dialog: Locator | None = None) -> list[Locator]:
    """Ô mô tả Lexical (``data-lexical-editor``) — kể cả trên màn «Edit reel»."""
    out: list[Locator] = []
    seen: set[str] = set()
    scopes: list[Locator] = []
    if dialog is not None:
        scopes.append(dialog)
    try:
        dlg = page.locator("[role='dialog']").last
        if dlg.count() > 0:
            scopes.append(dlg)
    except Exception:
        pass
    scopes.append(page)
    for scope in scopes:
        for sel in _REEL_LEXICAL_TEXTBOX_SELECTORS:
            key = f"{id(scope)}:{sel[:72]}"
            if key in seen:
                continue
            seen.add(key)
            try:
                inner = scope.locator(sel)
                if inner.count() <= 0:
                    continue
                out.append(inner.first)
            except Exception:
                continue
    return out


def _reel_lexical_description_usable(page: Page, *, timeout_ms: int = 450) -> bool:
    for loc in _reel_lexical_description_locators(page):
        try:
            if loc.is_visible(timeout=timeout_ms):
                return True
        except Exception:
            continue
    return False


def _lexical_editor_text_content(box: Locator) -> str:
    try:
        return str(
            box.evaluate(
                """(el) => {
                  const t = (el.innerText || el.textContent || '').toString().trim();
                  if (t) return t;
                  const p = el.querySelector('p');
                  return p ? (p.innerText || p.textContent || '').toString().trim() : '';
                }"""
            )
            or ""
        ).strip()
    except Exception:
        return ""


def _fill_lexical_contenteditable(box: Locator, text: str) -> bool:
    """Nhập Lexical editor: focus → insert_text → synthetic paste nếu cần."""
    raw = (text or "").strip()
    if not raw:
        return True
    page = box.page
    needle = " ".join(raw.split())[:80].lower()

    def _verified() -> bool:
        cur = _lexical_editor_text_content(box)
        return bool(needle and needle in " ".join(cur.split()).lower())

    _focus_contenteditable_for_input(box)
    try:
        page.keyboard.press("Control+A")
        page.keyboard.press("Backspace")
    except Exception:
        pass
    page.wait_for_timeout(120)
    try:
        page.keyboard.insert_text(raw)
    except Exception:
        pass
    page.wait_for_timeout(280)
    if _verified():
        return True

    try:
        box.evaluate(
            """(el, val) => {
              try { el.focus(); } catch (_) {}
              try {
                const sel = window.getSelection();
                const r = document.createRange();
                r.selectNodeContents(el);
                sel.removeAllRanges();
                sel.addRange(r);
                document.execCommand('delete', false);
              } catch (_) {}
              const makeDT = () => {
                try {
                  const dt = new DataTransfer();
                  dt.setData('text/plain', val);
                  return dt;
                } catch (_) { return null; }
              };
              try {
                const dt = makeDT();
                const ev = new InputEvent('beforeinput', {
                  inputType: 'insertFromPaste',
                  data: val,
                  dataTransfer: dt,
                  bubbles: true,
                  cancelable: true,
                });
                el.dispatchEvent(ev);
              } catch (_) {}
              try {
                const dt = makeDT();
                const ev = new ClipboardEvent('paste', {
                  clipboardData: dt,
                  bubbles: true,
                  cancelable: true,
                });
                try { Object.defineProperty(ev, 'clipboardData', { value: dt }); } catch (_) {}
                el.dispatchEvent(ev);
              } catch (_) {}
              try {
                el.dispatchEvent(new InputEvent('input', {
                  inputType: 'insertFromPaste',
                  data: val,
                  bubbles: true,
                }));
              } catch (_) {
                try { el.dispatchEvent(new Event('input', { bubbles: true })); } catch (_) {}
              }
            }""",
            raw,
        )
    except Exception:
        pass
    page.wait_for_timeout(350)
    if _verified():
        return True

    try:
        box.type(raw, delay=_typing_delay_ms())
        page.wait_for_timeout(400)
    except Exception:
        pass
    return _verified()


def fill_reel_lexical_description(page: Page, text: str) -> bool:
    """Điền ô «Describe your reel…» (Lexical) trên Edit reel / wizard."""
    raw = (text or "").strip()
    if not raw:
        return True
    stage = _reel_strict_prefix("Wizard")
    last_exc: Exception | None = None
    for idx, box in enumerate(_reel_lexical_description_locators(page)):
        try:
            if box.count() <= 0:
                continue
            box.wait_for(state="visible", timeout=8_000)
        except Exception as exc:
            last_exc = exc
            continue
        try:
            if _fill_lexical_contenteditable(box, raw):
                logger.info(
                    "{} Đã nhập mô tả Lexical ({} ký tự, candidate={}).",
                    stage,
                    len(raw),
                    idx,
                )
                _human_pause()
                return True
        except Exception as exc:
            last_exc = exc
            logger.debug("{} fill Lexical candidate {} lỗi: {}", stage, idx, exc)
    if last_exc:
        logger.warning("{} Không nhập được Lexical: {}", stage, last_exc)
    return False


def _meta_reel_details_visible(page: Page) -> bool:
    """Heuristic: màn Reel sau upload (có nút Next + vùng mô tả)."""
    if _reel_edit_reel_header_visible(page, timeout_ms=350):
        return False
    try:
        if not _meta_reel_next_any_visible(page):
            return False
    except Exception:
        return False
    if _reel_caption_screen_markers_visible(page, timeout_ms=500):
        return True
    hints = (
        "Reel details",
        "Let viewers know",
        "Chi tiết Reel",
    )
    for h in hints:
        try:
            if page.get_by_text(h, exact=False).first.is_visible(timeout=600):
                return True
        except Exception:
            continue
    return False


def _meta_video_attachment_confirmed(page: Page) -> bool:
    """
    Heuristic xác nhận video đã được gắn vào composer (dù wizard Reel chưa hiện rõ):
    - Có nút "Sử dụng lại câu lệnh" trong card output video.
    - Hoặc có overlay play icon ``role=presentation`` chứa SVG player.
    """
    try:
        if page.get_by_text("Sử dụng lại câu lệnh", exact=False).first.is_visible(timeout=500):
            return True
    except Exception:
        pass
    selectors = (
        "div[role='presentation'] svg[viewBox='0 0 24 24']",
        "div[role='presentation'] svg path[d*='12.87 6.82']",
        "div[role='presentation'] svg path[d*='M5 5.16']",
    )
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible(timeout=450):
                return True
        except Exception:
            continue
    return False


def _reel_active_step_label(page: Page) -> str:
    """Đọc step active ở header Reel wizard: create/edit/share."""
    for sel in (
        "[aria-current='step']",
        "[aria-current='true']",
        "[role='radio'][aria-checked='true']",
    ):
        try:
            node = page.locator(sel).first
            if node.count() > 0 and node.is_visible(timeout=200):
                txt = (node.inner_text(timeout=300) or "").strip().lower()
                if txt:
                    return txt
        except Exception:
            continue
    for name in ("create", "edit", "share"):
        try:
            hit = page.locator("div,span,label").filter(has_text=re.compile(rf"^\s*{name}\s*$", re.I))
            n = min(hit.count(), 8)
            for i in range(n):
                c = hit.nth(i)
                if not c.is_visible(timeout=120):
                    continue
                in_header = bool(
                    c.evaluate(
                        """(el) => {
                            const r = el.getBoundingClientRect();
                            return r.top >= 0 && r.top <= 220;
                        }"""
                    )
                )
                if in_header:
                    return name
        except Exception:
            continue
    return "unknown"


def _wait_reel_step_change(page: Page, before_step: str, *, timeout_ms: int = 9_000) -> bool:
    """Chờ step Reel đổi sau khi bấm Next.

    Meta có lúc không cập nhật rõ label step (vẫn "create") dù wizard đã tiến.
    Vì vậy ngoài step label, chấp nhận tín hiệu thực tế: nút Share đã xuất hiện.
    """
    deadline = time.time() + max(1_000, timeout_ms) / 1000.0
    while time.time() < deadline:
        cur = _reel_active_step_label(page)
        if cur != before_step and cur != "unknown":
            return True
        try:
            share_vis = page.locator(
                "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' and .//div[normalize-space()='Share']])[last()]"
            )
            if share_vis.count() > 0 and share_vis.first.is_visible(timeout=250):
                return True
        except Exception:
            pass
        page.wait_for_timeout(260)
    return False


def wait_meta_reel_details_wizard(page: Page, *, timeout_ms: int = 120_000) -> bool:
    """
    Sau khi gắn video, chờ UI chuyển sang bước Reel (Details / Next).

    Returns:
        True nếu thấy wizard; False nếu hết thời gian chờ.
    """
    deadline = time.time() + timeout_ms / 1000.0
    while time.time() < deadline:
        _mute_browser_video_previews(page, silent=True)
        if _meta_reel_details_visible(page):
            _mute_browser_video_previews_after_attach(page, attempts=3)
            logger.info("{} Đã thấy màn Reel details / Next.", _reel_strict_prefix("Wizard"))
            return True
        if _meta_video_attachment_confirmed(page):
            _mute_browser_video_previews_after_attach(page, attempts=3)
            logger.info(
                "{} Xác nhận video đã đính kèm (play/output card) — chuyển bước tiếp theo, không chờ cứng wizard.",
                _reel_strict_prefix("Wizard"),
            )
            return True
        page.wait_for_timeout(450)
    logger.warning("{} Không thấy wizard Reel sau {} ms theo luồng chuẩn.", _reel_strict_prefix("Wizard"), timeout_ms)
    return False


def _extract_reel_tag_keywords_from_caption(text: str, *, limit: int = 12) -> list[str]:
    """
    Lấy từ khóa cho ô Tags Reel từ caption: các cụm ``#tag`` → ``tag`` (bỏ trùng, giữ thứ tự).
    """
    raw = (text or "").strip()
    if not raw:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for m in re.finditer(r"#([^\s#]{1,80})", raw):
        w = (m.group(1) or "").strip()
        if not w:
            continue
        key = w.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(w[:80])
        if len(out) >= limit:
            break
    return out


def _normalize_reel_tags(tags: list[str] | None, *, limit: int = 12) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in (tags or []):
        s = str(raw or "").strip().lstrip("#").strip()
        if not s:
            continue
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(s[:80])
        if len(out) >= limit:
            break
    return out


def _normalize_hashtag(tag: str) -> str:
    s = str(tag or "").strip()
    if not s:
        return ""
    return s if s.startswith("#") else f"#{s.lstrip('#')}"


def _find_reel_tags_input(page: Page) -> Locator | None:
    # Placeholder có thể mất sau tag đầu; chọn đúng input tags bằng điểm ưu tiên theo DOM thực tế.
    base = page.locator("input[role='combobox'][aria-disabled='false']")
    try:
        n = min(base.count(), 12)
    except Exception:
        n = 0
    best: Locator | None = None
    best_score = -10**9
    for i in range(n - 1, -1, -1):
        cand = base.nth(i)
        try:
            if not cand.is_visible(timeout=800):
                continue
            score = int(
                cand.evaluate(
                    """(el) => {
                        let s = 0;
                        const p = String(el.getAttribute("placeholder") || "").toLowerCase();
                        const ac = String(el.getAttribute("aria-autocomplete") || "").toLowerCase();
                        if (ac === "list") s += 30;
                        if (p.includes("add relevant keywords")) s += 80;
                        if (p.includes("help people find your reel")) s += 30;
                        const root = el.closest("div");
                        if (root) {
                            if (root.querySelector("[data-key]")) s += 60; // có chip #tag
                            if (root.querySelector("[role='button'][aria-label^='Clear #']")) s += 40;
                        }
                        const r = el.getBoundingClientRect();
                        // Ưu tiên vùng dưới của wizard (tags thường nằm thấp hơn title/textarea).
                        s += Math.round((r.top || 0) / 8);
                        return s;
                    }"""
                )
            )
            if score > best_score:
                best_score = score
                best = cand
        except Exception:
            continue
    if best is not None:
        return best
    return None


def _pick_reel_next_button(page: Page) -> Locator | None:
    candidates = (
        # Theo HTML thực tế user cung cấp: button thật có role=button + tabindex + aria-busy=false.
        page.locator(
            "xpath=//div[@role='button' and @tabindex='0' and @aria-busy='false' and .//div[normalize-space()='Next']]"
        ),
        # Ưu tiên Next cùng footer có Cancel.
        page.locator(
            "xpath=//div[@role='button' and .//*[normalize-space()='Next'] and ancestor::*[.//*[normalize-space()='Cancel']]]"
        ),
        _locator_meta_reel_next_role(page),
        _locator_meta_reel_next_structural(page),
        _locator_meta_reel_next_text_parent(page),
    )
    best: Locator | None = None
    best_score = -10**9
    for base in candidates:
        try:
            n = min(base.count(), 16)
        except Exception:
            continue
        for i in range(n - 1, -1, -1):
            c = base.nth(i)
            try:
                if not c.is_visible(timeout=500):
                    continue
                dis = (c.get_attribute("aria-disabled") or "").strip().lower()
                if dis == "true":
                    continue
                score = float(
                    c.evaluate(
                        """(el) => {
                            const r = el.getBoundingClientRect();
                            let s = 0;
                            // ưu tiên nút ở góc dưới-phải (footer action).
                            s += (r.left + r.width) + (r.top * 1.7);
                            const txt = (el.textContent || "").toLowerCase();
                            if (txt.includes("next")) s += 120;
                            if (txt.includes("cancel")) s -= 80;
                            return s;
                        }"""
                    )
                )
                if score > best_score:
                    best_score = score
                    best = c
            except Exception:
                continue
    return best


def _wait_reel_tag_accepted(page: Page, tags_input: Locator, typed: str, *, timeout_ms: int = 3_000) -> bool:
    deadline = time.time() + max(1_000, timeout_ms) / 1000.0
    needle = typed.lstrip("#").strip()
    token_like = page.locator("[role='dialog'] *").filter(has_text=re.compile(rf"\b{re.escape(needle)}\b", re.I))
    chip_clear_btn = page.locator(f"[role='button'][aria-label='Clear {typed}']")
    chip_text = page.locator("[role='dialog'] [data-key]").filter(has_text=re.compile(rf"^\s*{re.escape(typed)}\s*$", re.I))
    probe_every = 3
    probe_i = 0
    while time.time() < deadline:
        try:
            val = (tags_input.input_value(timeout=350) or "").strip()
        except Exception:
            val = ""
        if not val:
            return True
        if val.lower() != typed.lower():
            return True
        try:
            expanded = (tags_input.get_attribute("aria-expanded") or "").strip().lower()
            probe_i += 1
            if expanded == "false" and (probe_i % probe_every == 0):
                # Dấu hiệu commit mạnh theo DOM thực tế: chip có nút Clear #tag.
                if chip_clear_btn.count() > 0:
                    return True
                # Fallback: chip text (#tag) trong vùng tags.
                if chip_text.count() > 0:
                    return True
                if token_like.count() > 0:
                    return True
        except Exception:
            pass
        page.wait_for_timeout(220)
    return False


def fill_meta_reel_tags_best_effort(
    page: Page,
    text: str,
    *,
    max_tags: int = 12,
    reel_tags: list[str] | None = None,
) -> None:
    """
    Ô **Tags** (optional) wizard Reel Meta: ``input[role=combobox]`` với placeholder kiểu
    "Add relevant keywords…". Từ ``#foo`` → gõ ``foo`` → **chọn** hàng *Add a new tag "foo"* (hoặc Enter).

    Không có ``#`` trong text, hoặc không thấy ô / lỗi nhập: bỏ qua, không ném exception.
    """
    kws = _normalize_reel_tags(reel_tags, limit=max_tags) if reel_tags else _extract_reel_tag_keywords_from_caption(text, limit=max_tags)
    if not kws:
        return
    stage_pref = _reel_strict_prefix("Wizard")
    # Luồng cứng: input tags combobox -> nhập từng tag -> Enter -> chờ accept.
    try:
        if _find_reel_tags_input(page) is None:
            logger.info("{} Không thấy ô Tags (combobox) theo luồng cứng — bỏ qua.", stage_pref)
            return
        ok_n = 0
        failed: list[str] = []
        for kw in kws:
            typed = _normalize_hashtag(kw)
            if not typed:
                continue
            committed = False
            for _attempt in range(1, 4):
                strict_input = _find_reel_tags_input(page)
                if strict_input is None:
                    page.wait_for_timeout(random.randint(220, 520))
                    continue
                try:
                    strict_input.scroll_into_view_if_needed(timeout=2_000)
                except Exception:
                    pass
                try:
                    strict_input.click(timeout=5_000, force=True)
                except Exception:
                    # UI Reel có thể tạm khóa/đổi node input; thử nhịp sau, không fail cứng cả job.
                    page.wait_for_timeout(random.randint(220, 520))
                    continue
                page.wait_for_timeout(random.randint(200, 500))
                try:
                    # Không dùng Ctrl+A/Backspace để tránh ảnh hưởng chip tag đã commit.
                    strict_input.fill("")
                except Exception:
                    try:
                        strict_input.fill("")
                    except Exception:
                        pass
                try:
                    # Nhập theo kiểu copy/paste nguyên #tag để tránh mất ký tự '#'.
                    strict_input.page.keyboard.insert_text(typed)
                except Exception:
                    try:
                        strict_input.press_sequentially(typed, delay=80)
                    except Exception:
                        strict_input.fill(typed)
                page.wait_for_timeout(random.randint(300, 700))
                strict_input.press("Enter")
                committed = _wait_reel_tag_accepted(page, strict_input, typed, timeout_ms=3_000)
                if committed:
                    break
            if committed:
                ok_n += 1
            else:
                failed.append(typed)
                logger.warning("{} Tag {!r} chưa commit được sau nhiều lần thử.", stage_pref, typed)
            page.wait_for_timeout(random.randint(300, 1000))
        logger.info("{} Đã xử lý Tags: thành công {}/{} từ khóa.", stage_pref, ok_n, len(kws))
        if failed:
            # Theo yêu cầu vận hành: tags là best-effort, fail thì bỏ qua để tiếp tục Next/Share.
            logger.warning("{} Bỏ qua tags lỗi và tiếp tục wizard: {}", stage_pref, ", ".join(failed))
        return
    except Exception as exc:
        # Không fail cứng job vì tags; tiếp tục luồng Next/Share.
        logger.warning("{} Lỗi nhập Tags theo luồng cứng (bỏ qua, tiếp tục): {}", stage_pref, exc)
        return
    finally:
        # Dù thành công hay lỗi đều nghỉ nhịp trước bước kế tiếp (Next/Share).
        _human_pause()


def _meta_reel_description_editor_locators(page: Page) -> list[Locator]:
    """
    Các ứng viên ô mô tả Reel. Meta đôi khi có **hai** ``role=textbox`` cùng aria-label (composer + wizard);
    ``.first`` có thể là layer dưới — không click được — nên ưu tiên **nth từ cuối** và ô trong ``role=dialog``.
    """
    out: list[Locator] = []
    seen: set[str] = set()

    def _add(loc: Locator, key: str) -> None:
        if key in seen:
            return
        seen.add(key)
        out.append(loc)

    for loc in _reel_lexical_description_locators(page):
        _add(loc, "lexical_desc")
    try:
        dlg = page.locator("[role='dialog']").filter(
            has_text=re.compile(
                r"Reel|Thumbnail|Description|Chi tiết|Mô tả|Hashtag|Let viewers know|Edit reel",
                re.I,
            )
        )
        if dlg.count() > 0:
            for loc in _reel_lexical_description_locators(page, dialog=dlg.last):
                _add(loc, f"lexical_dialog_{id(loc)}")
            inner = dlg.last.locator(
                "div.notranslate._5rpu[role='textbox'][contenteditable='true'], "
                "div[role='textbox'][contenteditable='true'][aria-label*='dialogue box' i]"
            )
            if inner.count() > 0:
                _add(inner.first, "dialog_scoped")
    except Exception:
        pass
    try:
        by_role = page.get_by_role(
            "textbox",
            name=re.compile(r"Write into the dialogue box", re.I),
        )
        n = min(by_role.count(), 12)
        for i in range(n - 1, -1, -1):
            _add(by_role.nth(i), f"by_role_{i}")
    except Exception:
        pass
    for sel in (
        "div._5yk2 div.notranslate._5rpu[role='textbox'][contenteditable='true']",
        "div.notranslate._5rpu[role='textbox'][contenteditable='true'][aria-label*='Write into the dialogue box' i]",
        "div[role='textbox'][contenteditable='true'][aria-label*='dialogue box' i]",
        "div[role='textbox'][contenteditable='true'][aria-label*='Write' i]",
        "div.notranslate._5rpu[role='textbox'][contenteditable='true']",
    ):
        _add(page.locator(sel).first, f"css:{sel[:48]}")
    return out


def _focus_contenteditable_for_input(box: Locator) -> None:
    """
    Đưa focus vào ``contenteditable`` — Firefox/Meta: ``click()`` Playwright có thể timeout
    (overlay / hit target); ``focus()`` + ``fill`` thường đủ.
    """
    pg = box.page
    try:
        box.scroll_into_view_if_needed(timeout=5_000)
    except Exception:
        pass
    try:
        box.evaluate("el => { if (el && typeof el.focus === 'function') el.focus(); }")
    except Exception:
        pass
    try:
        box.click(timeout=2_500, force=True)
        return
    except Exception:
        pass
    try:
        box.dispatch_event("click")
    except Exception:
        pass
    try:
        pg.keyboard.press("Tab")
        pg.wait_for_timeout(120)
    except Exception:
        pass


def fill_meta_reel_description(page: Page, text: str, *, relock_guard: bool = True) -> None:
    """
    Điền ô mô tả / hashtag ở bước Reel (``role=textbox`` + ``contenteditable``).

    Khớp DOM Meta Reel: ``._5yk2`` / ``._5rpu`` + ``aria-label`` dialogue box.

    Args:
        page: Trang Business / Reels.
        text: Caption / hashtag (có thể rỗng để bỏ qua).
    """
    raw = (text or "").strip()
    if not raw:
        return
    if fill_reel_lexical_description(page, raw):
        if relock_guard:
            _enable_view_only_guard(page)
        return
    # Luồng chuẩn theo editor DraftJS của Reel (data-editor/data-block).
    try:
        draft_block = page.locator("div[data-editor] div[data-block='true']").last
        if draft_block.count() > 0 and draft_block.is_visible(timeout=1_500):
            _focus_contenteditable_for_input(draft_block)
            try:
                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
            except Exception:
                pass
            try:
                page.keyboard.insert_text(raw)
            except Exception:
                draft_block.fill(raw, timeout=25_000)
            logger.info("{} Đã nhập mô tả Reel ({} ký tự, candidate=strict_draftjs).", _reel_strict_prefix("Wizard"), len(raw))
            _human_pause()
            if relock_guard:
                _enable_view_only_guard(page)
            return
    except Exception:
        pass

    last_exc: Exception | None = None
    candidates = _meta_reel_description_editor_locators(page)
    if not candidates:
        try:
            sel = _wait_first_selector(
                page,
                (
                    "div._5yk2 div.notranslate._5rpu[role='textbox'][contenteditable='true']",
                    "div.notranslate._5rpu[role='textbox'][contenteditable='true']",
                ),
                step_timeout_ms=25_000,
                error_label="fill_meta_reel_description",
            )
            candidates = [page.locator(sel).first]
        except PlaywrightTimeoutError:
            if relock_guard:
                _enable_view_only_guard(page)
            raise
    try:
        for idx, box in enumerate(candidates):
            try:
                if box.count() <= 0:
                    continue
                box.wait_for(state="visible", timeout=6_000)
            except Exception:
                continue
            try:
                _focus_contenteditable_for_input(box)
                try:
                    page.keyboard.press("Control+A")
                    page.keyboard.press("Backspace")
                except Exception:
                    pass
                try:
                    # Ưu tiên copy-dán nguyên văn để giữ đúng title/content từ job.
                    page.keyboard.insert_text(raw)
                except Exception:
                    try:
                        box.fill(raw, timeout=45_000)
                    except Exception:
                        box.type(raw, delay=_typing_delay_ms())
                logger.info("{} Đã nhập mô tả Reel ({} ký tự, candidate={}).", _reel_strict_prefix("Wizard"), len(raw), idx)
                _human_pause()
                if relock_guard:
                    _enable_view_only_guard(page)
                return
            except Exception as exc:
                last_exc = exc
                logger.debug("{} fill mô tả candidate {} lỗi: {}", _reel_strict_prefix("Wizard"), idx, exc)
                continue
        if last_exc:
            raise last_exc
        raise PlaywrightTimeoutError("Không điền được ô mô tả Reel (không có candidate hợp lệ).")
    except PlaywrightTimeoutError:
        if relock_guard:
            _enable_view_only_guard(page)
        raise
    except Exception as exc:
        if relock_guard:
            _enable_view_only_guard(page)
        _failure_screenshot(page, f"fill_meta_reel_description: {exc}")
        raise


def _fill_reel_schedule_datetime_best_effort(page: Page, scheduled_at_utc_iso: str) -> None:
    """Điền ngày/giờ lên lịch Reel (best-effort: ``input[type=date|time]`` theo ``SCHEDULER_TZ``)."""
    s = str(scheduled_at_utc_iso or "").strip().replace("Z", "+00:00")
    if not s:
        return
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        loc = dt.astimezone(scheduler_tz())
    except Exception as exc:
        logger.warning("{} Không parse scheduled_at={!r}: {}", _reel_strict_prefix("Wizard"), scheduled_at_utc_iso, exc)
        return
    date_s = loc.strftime("%Y-%m-%d")
    time_s = loc.strftime("%H:%M")
    for kind, val in (("date", date_s), ("time", time_s)):
        try:
            inp = page.locator(f"input[type='{kind}']").first
            if inp.count() > 0 and inp.is_visible(timeout=2_000):
                inp.fill(val)
                logger.info("{} Đã điền input[type={}]: {}", _reel_strict_prefix("Wizard"), kind, val)
        except Exception:
            continue
    _human_pause()


def dismiss_meta_video_post_processing_modal_best_effort(
    page: Page,
    *,
    timeout_ms: int = 120_000,
    give_up_if_never_seen_ms: int | None = 35_000,
) -> bool:
    """
    Dialog Meta **«Video post processing»** (video đang xử lý, sẽ đăng sau) — coi là chấp nhận thành công,
    bấm **Done** để đóng và tiếp tục job.

    Args:
        timeout_ms: Tối đa chờ (kể từ lúc gọi).
        give_up_if_never_seen_ms: Nếu **chưa từng** thấy nội dung dialog trong khoảng ms này thì thoát False
            (tránh chờ cả ``timeout_ms`` khi không có Reel / không có modal). None = chờ hết ``timeout_ms``.

    Returns:
        True nếu đã thấy dialog và bấm Done; False nếu không có dialog (hoặc hết thời gian).
    """
    deadline = time.time() + timeout_ms / 1000.0
    t0 = time.time()
    saw_processing = False

    def _processing_visible() -> bool:
        try:
            t = page.get_by_text(re.compile(r"Video\s+post\s+processing", re.I))
            if t.count() > 0 and t.first.is_visible(timeout=700):
                return True
        except Exception:
            pass
        try:
            b = page.get_by_text(re.compile(r"Once your video finishes processing", re.I))
            if b.count() > 0 and b.first.is_visible(timeout=700):
                return True
        except Exception:
            pass
        try:
            b2 = page.get_by_text(re.compile(r"finishes processing.*published", re.I | re.DOTALL))
            if b2.count() > 0 and b2.first.is_visible(timeout=700):
                return True
        except Exception:
            pass
        return False

    def _click_done() -> bool:
        """
        Nút Done trong dialog «Video post processing»:
        cấu trúc chuẩn ``div[role='button'][tabindex='0'][aria-busy='false']`` bọc ``div`` text *Done*.
        Ưu tiên JS click để tránh actionability (giống Next/Share strict).
        """
        stage = _reel_strict_prefix("Wizard")
        xpath_done = (
            "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' "
            "and (.//div[normalize-space()='Done'] or .//div[normalize-space()='Xong'])])[last()]"
        )

        def _click_locator(loc: Locator) -> bool:
            try:
                if loc.count() <= 0 or not loc.is_visible(timeout=1_500):
                    return False
                if (loc.get_attribute("aria-disabled") or "").strip().lower() == "true":
                    return False
                try:
                    loc.evaluate("el => el && el.click && el.click()")
                    logger.info("{} _click_done: đã dispatch JS click.", stage)
                    return True
                except Exception as exc_js:
                    logger.debug("{} _click_done: JS click lỗi: {}", stage, exc_js)
                try:
                    loc.click(timeout=5_000, force=True, no_wait_after=True)
                    logger.info("{} _click_done: đã click force (fallback).", stage)
                    return True
                except Exception as exc_force:
                    logger.warning("{} _click_done: force click lỗi: {}", stage, exc_force)
                    return False
            except Exception as exc:
                logger.debug("{} _click_done: lỗi không xác định: {}", stage, exc)
                return False

        # Ưu tiên trong dialog processing để tránh bấm trúng Done nơi khác.
        try:
            dlg = page.get_by_role("dialog").filter(
                has_text=re.compile(r"Video\s+post\s+processing", re.I)
            )
            if dlg.count() > 0 and dlg.first.is_visible(timeout=800):
                if _click_locator(dlg.first.locator(xpath_done).last):
                    return True
        except Exception:
            pass
        try:
            dlg2 = page.get_by_role("dialog").filter(
                has_text=re.compile(r"finishes processing", re.I)
            )
            if dlg2.count() > 0 and dlg2.first.is_visible(timeout=800):
                if _click_locator(dlg2.first.locator(xpath_done).last):
                    return True
        except Exception:
            pass
        return _click_locator(page.locator(xpath_done))

    while time.time() < deadline:
        if give_up_if_never_seen_ms is not None and not saw_processing:
            if (time.time() - t0) * 1000.0 >= float(give_up_if_never_seen_ms):
                return False
        try:
            vis = _processing_visible()
        except Exception as exc:
            # Page/browser đóng ngay sau Share — coi như đã submit xong.
            if "closed" in str(exc).lower() or "targetclosederror" in type(exc).__name__.lower():
                logger.info("{} Page/browser đóng trong lúc chờ processing → coi như đã submit.", _reel_strict_prefix("Wizard"))
                return True
            vis = False
        if vis:
            saw_processing = True
            logger.info("{} Phát hiện dialog Video post processing — bấm Done.", _reel_strict_prefix("Wizard"))
            if _click_done():
                _human_pause()
                logger.info("{} Đã đóng dialog Video post processing (Done).", _reel_strict_prefix("Wizard"))
                return True
            logger.warning("{} Có dialog processing nhưng chưa bấm được Done — chờ thêm.", _reel_strict_prefix("Wizard"))
            try:
                page.wait_for_timeout(600)
            except Exception as exc:
                if "closed" in str(exc).lower():
                    logger.info("{} Page đóng sau khi đã thấy processing → coi như submit xong.", _reel_strict_prefix("Wizard"))
                    return True
                raise
            continue
        try:
            page.wait_for_timeout(450)
        except Exception as exc:
            if "closed" in str(exc).lower():
                logger.info("{} Page đóng trong lúc chờ processing modal → coi như submit xong.", _reel_strict_prefix("Wizard"))
                return True
            raise
    return False


def _choose_first_reel_thumbnail_method1_best_effort(page: Page) -> bool:
    """
    Cách 1 (wizard nhiều bước): chọn **thumbnail / frame preview đầu tiên** trong lưới Meta
    (ô ``role=button`` hoặc ``tabindex=0`` có ``img`` vừa phải — bỏ qua icon nhỏ).

    Khớp ý định HTML mẫu: lưới thumbnail dưới tiêu đề kiểu «Choose thumbnail»; nếu không thấy
    tiêu đề vẫn thử trong ``role=dialog``.
    """
    stage = _reel_strict_prefix("Wizard")
    js = r"""
() => {
  function area(el) {
    const r = el.getBoundingClientRect();
    return Math.max(0, r.width) * Math.max(0, r.height);
  }
  const dialog = document.querySelector('[role="dialog"]');
  const rootWide = dialog || document.body;
  const hdrRx = /choose\s+thumbnail|chọn.*thumbnail|thumbnail.*reel|video\s+thumbnail|edit\s+cover|chỉnh\s+sửa\s+ảnh/i;
  let scope = rootWide;
  const labels = Array.from(rootWide.querySelectorAll("span,div,h1,h2,h3,h4"));
  for (const el of labels) {
    const t = (el.textContent || "").trim();
    if (!t || t.length > 120) continue;
    if (!hdrRx.test(t)) continue;
    let n = el;
    for (let d = 0; d < 22 && n; d++) {
      n = n.parentElement;
      if (!n) break;
      if (area(n) > 80000) {
        scope = n;
        break;
      }
    }
    break;
  }
  const nodes = scope.querySelectorAll('div[role="button"],div[tabindex="0"]');
  for (const btn of nodes) {
    const img = btn.querySelector("img[src],img[srcset]");
    if (!img) continue;
    const r = img.getBoundingClientRect();
    if (r.width < 56 || r.height < 40) continue;
    if (r.width > 560 || r.height > 560) continue;
    try {
      btn.click();
      return true;
    } catch (e) {}
  }
  return false;
}
"""
    try:
        clicked = bool(page.evaluate(js))
    except Exception as exc:
        logger.debug("{} Chọn thumbnail (JS): {}", stage, exc)
        clicked = False
    if clicked:
        logger.info("{} Đã chọn thumbnail đầu tiên (Cách 1).", stage)
        return True
    return False


def complete_meta_business_reel_post_wizard(
    page: Page,
    *,
    description: str,
    reel_tags: list[str] | None = None,
    share_now: bool,
    scheduled_at_utc_iso: str | None,
    reel_thumbnail_choice: str | None = None,
) -> bool:
    """
    Legacy Meta Business **composer** Reel wizard (Share/Done) — không dùng cho job ``video|text_video|reel``
    (các job đó đi ``post_reel_via_page_dashboard`` + ``submit_mode='post'``).

    Hoàn tất đăng Reel sau upload trong composer: Next → nhập caption → Share now;
    nếu Meta hiện **Video post processing** thì bấm **Done**.
    """
    ui_way = detect_meta_reel_ui_way(page)
    if ui_way == "unknown":
        page.wait_for_timeout(1200)
        ui_way = detect_meta_reel_ui_way(page)
    if ui_way == "unknown" and _meta_reel_next_any_visible(page):
        ui_way = "way1"
    if ui_way == "unknown":
        ui_way = "way1"
    logger.info(
        "{} Legacy composer wizard (Share) | detect_meta_reel_ui_way → {}",
        _reel_strict_prefix("Wizard"),
        ui_way,
    )

    if not share_now:
        raise RuntimeError("Luồng chuẩn video/reel hiện chỉ hỗ trợ Share now.")

    complete_reel_wizard_fill_next_and_post(
        page,
        content=str(description or "").strip(),
        hashtags=list(reel_tags or []),
        reel_thumbnail_choice=reel_thumbnail_choice,
        submit_mode="share",
        max_next_clicks=18,
        total_timeout_sec=300.0,
    )
    return True


_REEL_MENU_LABEL_RE = re.compile(
    r"^(?:Reels?|Thước phim|Video(?:\s+ngắn)?|Short\s+video)$",
    re.I,
)
_REEL_MENU_NAME_RE = re.compile(r"Reels?|Thước phim|Video ngắn|Short video", re.I)
_REEL_UPLOAD_READY_RE = re.compile(
    r"Add video|drag and drop|Thêm video|kéo và thả|tải video lên|Upload video",
    re.I,
)

# Professional Dashboard Content Library — hai nút Create (không gộp chung để tránh bấm nhầm).
_CREATE_SIDEBAR_POST_RE = re.compile(r"^Create a post$|^Tạo bài viết$", re.I)
_CREATE_MAIN_BUTTON_RE = re.compile(r"^\+?\s*Create$|^Tạo$", re.I)

_REEL_MENUITEM_CLICK_JS = """
() => {
  const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
  const isReel = (t) => /^reels?$/i.test(t) || /^thước phim$/i.test(t) || /^video$/i.test(t);
  let scope = document.body;
  const menus = Array.from(document.querySelectorAll('[role="menu"], [role="listbox"]'))
    .filter((m) => m.getBoundingClientRect().width > 20);
  if (menus.length) scope = menus[menus.length - 1];
  for (const ov of scope.querySelectorAll('[data-visualcompletion="ignore"]')) {
    ov.style.pointerEvents = 'none';
  }
  const items = scope.querySelectorAll('[role="menuitem"], [role="option"]');
  for (const el of items) {
    const t = norm(el.textContent);
    if (!isReel(t)) continue;
    const spans = el.querySelectorAll('span');
    let label = t;
    for (const sp of spans) {
      const st = norm(sp.textContent);
      if (isReel(st)) { label = st; break; }
    }
    for (const node of [el, el.querySelector('[tabindex="0"]'), el.closest('[role="menuitem"]')]) {
      if (!node) continue;
      try {
        const r = node.getBoundingClientRect();
        const x = r.left + r.width / 2;
        const y = r.top + r.height / 2;
        for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) {
          node.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window, clientX: x, clientY: y }));
        }
        if (typeof node.click === 'function') node.click();
        return { ok: true, label };
      } catch (_) {}
    }
  }
  return { ok: false };
}
"""


def _reel_scope_upload_ready(scope: Locator) -> bool:
    try:
        if scope.get_by_text(_REEL_UPLOAD_READY_RE).first.is_visible(timeout=350):
            return True
    except Exception:
        pass
    try:
        fi = scope.locator("input[type='file']")
        if fi.count() > 0 and fi.first.is_visible(timeout=350):
            return True
    except Exception:
        pass
    try:
        up = scope.get_by_role("button", name=re.compile(r"^Upload$|^Tải lên$", re.I))
        if up.count() > 0 and up.first.is_visible(timeout=350):
            return True
    except Exception:
        pass
    return False


def _reel_composer_already_visible(page: Page) -> bool:
    """True khi wizard/dialog upload Reel đã mở (Create có thể bỏ qua menu Reel)."""
    try:
        dlg = page.locator("[role='dialog']").last
        if dlg.count() > 0 and dlg.is_visible(timeout=450) and _reel_scope_upload_ready(dlg):
            return True
    except Exception:
        pass
    return _reel_scope_upload_ready(page)


def _locate_create_post_menu(page: Page) -> Locator:
    """Popover menu sau khi bấm Create (ưu tiên menu/listbox visible gần nhất)."""
    for sel in ("[role='menu']", "[role='listbox']"):
        loc = page.locator(sel)
        try:
            cnt = int(loc.count())
        except Exception:
            cnt = 0
        for i in range(cnt - 1, -1, -1):
            item = loc.nth(i)
            try:
                if item.is_visible(timeout=350):
                    return item
            except Exception:
                continue
    return page.locator("body")


def _create_post_menu_visible(page: Page, *, timeout_ms: int = 500) -> bool:
    try:
        return page.locator("[role='menuitem'], [role='option']").first.is_visible(timeout=timeout_ms)
    except Exception:
        return False


def _locator_sidebar_create_a_post(page: Page) -> Locator:
    """Nút xanh «Create a post» ở cột trái (Professional Dashboard)."""
    return page.locator(
        "xpath=(//*[@role='button' or @role='none']"
        "[.//span[normalize-space()='Create a post' or normalize-space()='Tạo bài viết']]"
        " | //*[@aria-label and (contains(@aria-label, 'Create a post') or contains(@aria-label, 'Tạo bài viết'))]"
        ")[last()]"
    )


def _locator_main_content_create(page: Page) -> Locator:
    """Nút «+ Create» / «Create» trên vùng Content Library (không phải Create a post)."""
    return page.locator(
        "xpath=(//*[@role='button' or @role='none']"
        "[.//span[normalize-space()='Create' or normalize-space()='Tạo' or normalize-space()='+ Create']"
        " and not(.//span[contains(., 'Create a post')]) and not(.//span[contains(., 'Tạo bài viết')])]"
        ")[1]"
    )


def _click_create_entry_locator(page: Page, loc: Locator, *, timeout_ms: int = 1600) -> bool:
    """Click nút Create — tắt overlay visualcompletion trên nút."""
    try:
        if loc.count() <= 0:
            return False
        btn = loc.last
        if not btn.is_visible(timeout=timeout_ms):
            return False
        try:
            btn.evaluate(
                """(el) => {
                  if (!el) return;
                  for (const ov of el.querySelectorAll('[data-visualcompletion="ignore"]')) {
                    ov.style.pointerEvents = 'none';
                  }
                  const root = el.closest('[role="button"], [tabindex="0"]') || el;
                  if (root && typeof root.click === 'function') root.click();
                }"""
            )
            return True
        except Exception:
            return _click_visible_enabled_button(loc, timeout_ms=timeout_ms)
    except Exception:
        return False


def _click_dashboard_create_entry(
    page: Page,
    *,
    prefer: Literal["sidebar", "main", "auto"] = "auto",
) -> Literal["sidebar", "main", ""]:
    """
    Bấm một trong hai nút Create trên Content Library.

    - ``sidebar``: «Create a post» (cột trái)
    - ``main``: «+ Create» / «Create» (vùng bài đăng)
    - ``auto``: sidebar trước, rồi main (không bấm cả hai nếu menu đã mở)
    """
    if _reel_composer_already_visible(page):
        return ""
    if _create_post_menu_visible(page, timeout_ms=400):
        return ""

    order: tuple[Literal["sidebar", "main"], ...]
    if prefer == "sidebar":
        order = ("sidebar", "main")
    elif prefer == "main":
        order = ("main", "sidebar")
    else:
        order = ("sidebar", "main")

    locators: dict[str, tuple[Locator, ...]] = {
        "sidebar": (
            page.get_by_role("button", name=_CREATE_SIDEBAR_POST_RE),
            _locator_sidebar_create_a_post(page),
        ),
        "main": (
            page.get_by_role("button", name=_CREATE_MAIN_BUTTON_RE),
            _locator_main_content_create(page),
        ),
    }
    for kind in order:
        for loc in locators[kind]:
            if _click_create_entry_locator(page, loc):
                page.wait_for_timeout(random.randint(650, 1200))
                if _create_post_menu_visible(page, timeout_ms=1200) or _reel_composer_already_visible(page):
                    return kind
        if _create_post_menu_visible(page, timeout_ms=500):
            return kind
    return ""


def _click_reel_menuitem_strict(page: Page) -> bool:
    """Bấm ``role=menuitem`` có span «Reel» + icon (DOM Meta dashboard)."""
    stage = _reel_strict_prefix("Wizard")
    try:
        result = page.evaluate(_REEL_MENUITEM_CLICK_JS)
        if isinstance(result, dict) and result.get("ok"):
            logger.info("{} Đã bấm menuitem Reel (JS): {!r}.", stage, result.get("label"))
            page.wait_for_timeout(random.randint(700, 1300))
            return True
    except Exception as exc:
        logger.debug("{} JS menuitem Reel: {}", stage, exc)

    menu = _locate_create_post_menu(page)
    for loc in (
        menu.locator(
            "xpath=.//*[@role='menuitem'][.//span[normalize-space()='Reel' or normalize-space()='Reels']]"
        ),
        menu.locator(
            "xpath=.//*[@role='menuitem'][.//span[normalize-space()='Reel']][.//svg]"
        ),
        menu.get_by_role("menuitem", name=_REEL_MENU_LABEL_RE),
        page.get_by_role("menuitem", name=_REEL_MENU_LABEL_RE),
    ):
        try:
            if _click_visible_enabled_button(loc, timeout_ms=1100):
                page.wait_for_timeout(random.randint(700, 1300))
                return True
        except Exception:
            continue
    return False


def _click_reel_in_create_menu(page: Page, *, timeout_ms: int = 14_000) -> bool:
    """Chọn Reel trong menu sau Create; True nếu composer Reel đã sẵn sàng."""
    if _reel_composer_already_visible(page):
        return True

    deadline = time.time() + max(3.0, float(timeout_ms) / 1000.0)
    try:
        page.locator("[role='menuitem'], [role='option']").first.wait_for(state="visible", timeout=3500)
    except Exception:
        pass

    while time.time() < deadline:
        if _click_reel_menuitem_strict(page):
            if _reel_composer_already_visible(page):
                return True

        menu = _locate_create_post_menu(page)
        for factory in (
            lambda m=menu: m.get_by_role("menuitem", name=_REEL_MENU_NAME_RE),
            lambda m=menu: m.get_by_role("option", name=_REEL_MENU_NAME_RE),
            lambda: page.get_by_role("menuitem", name=_REEL_MENU_NAME_RE),
        ):
            try:
                if _click_visible_enabled_button(factory(), timeout_ms=1100):
                    page.wait_for_timeout(random.randint(700, 1400))
                    if _reel_composer_already_visible(page):
                        return True
            except Exception:
                continue

        for label in ("Reel", "Reels", "Thước phim", "Video"):
            try:
                item = menu.locator(
                    f"xpath=.//*[normalize-space()='{label}']/ancestor::*"
                    f"[@role='menuitem' or @role='option'][1]"
                )
                if _click_visible_enabled_button(item, timeout_ms=1100):
                    page.wait_for_timeout(random.randint(700, 1400))
                    if _reel_composer_already_visible(page):
                        return True
            except Exception:
                continue

        page.wait_for_timeout(320)

    return _reel_composer_already_visible(page)


def _open_reel_composer_from_content_library(page: Page) -> str:
    """
    Mở wizard Reel từ Content Library: Create (sidebar hoặc main) → menuitem Reel.

    Returns:
        ``sidebar`` | ``main`` | ``direct`` | ``already`` — cách đã mở thành công.
    """
    stage = _reel_strict_prefix("Wizard")
    if _reel_composer_already_visible(page):
        logger.info("{} Reel composer đã mở — bỏ qua Create.", stage)
        return "already"

    create_kind = _click_dashboard_create_entry(page, prefer="auto")
    if create_kind:
        logger.info("{} Đã bấm Create ({}) — chờ menu Reel.", stage, create_kind)
    elif not _create_post_menu_visible(page, timeout_ms=800):
        create_kind = _click_dashboard_create_entry(page, prefer="main")
        if create_kind:
            logger.info("{} Retry Create (main): {}.", stage, create_kind)
        if not create_kind:
            create_kind = _click_dashboard_create_entry(page, prefer="sidebar")
            if create_kind:
                logger.info("{} Retry Create (sidebar): {}.", stage, create_kind)

    if _reel_composer_already_visible(page):
        return create_kind or "main"

    if _click_reel_in_create_menu(page, timeout_ms=14_000):
        return create_kind or "main"

    def _try_alternate_create() -> str:
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(350)
        except Exception:
            pass
        return ""

    if create_kind == "sidebar":
        _try_alternate_create()
        alt = _click_dashboard_create_entry(page, prefer="main")
        if alt and _click_reel_in_create_menu(page, timeout_ms=10_000):
            return alt
    elif create_kind == "main":
        _try_alternate_create()
        alt = _click_dashboard_create_entry(page, prefer="sidebar")
        if alt and _click_reel_in_create_menu(page, timeout_ms=10_000):
            return alt
    else:
        for prefer in ("sidebar", "main"):
            _try_alternate_create()
            alt = _click_dashboard_create_entry(page, prefer=prefer)
            if alt and _click_reel_in_create_menu(page, timeout_ms=10_000):
                return alt

    if _open_reel_composer_direct(page):
        return "direct"

    return ""


def _open_reel_composer_direct(page: Page) -> bool:
    """Fallback: mở URL composer Reel khi menu Create không có mục Reel."""
    for url in (
        "https://www.facebook.com/reels/create/",
        "https://www.facebook.com/professional_dashboard/content/create_reel/",
    ):
        try:
            assert_safe_facebook_navigation_url(url, label="reel_composer")
            page.goto(url, wait_until="domcontentloaded", timeout=90_000)
            page.wait_for_timeout(random.randint(2200, 4200))
            if _reel_composer_already_visible(page):
                logger.info("{} Mở composer Reel qua URL: {}", _reel_strict_prefix("Wizard"), url)
                return True
        except Exception as exc:
            logger.debug("{} URL composer Reel {}: {}", _reel_strict_prefix("Wizard"), url, exc)
    return False


def _click_visible_enabled_button(
    candidates: Locator,
    *,
    timeout_ms: int = 1200,
    human_label: str = "trước click nút (dialog)",
) -> bool:
    """Click button đầu tiên visible + enabled trong danh sách locator."""
    try:
        n = int(candidates.count())
    except Exception:
        return False
    for i in range(max(0, n)):
        b = candidates.nth(i)
        try:
            if not b.is_visible(timeout=timeout_ms):
                continue
            if (b.get_attribute("aria-disabled") or "").strip().lower() == "true":
                continue
            if b.get_attribute("disabled") is not None:
                continue
            if human_label:
                human_pause(kind="click", label=human_label)
            try:
                b.click(timeout=timeout_ms)
            except Exception:
                b.click(timeout=timeout_ms, force=True, no_wait_after=True)
            if human_label:
                human_pause(kind="click", label="sau click nút (dialog)")
            return True
        except Exception:
            continue
    return False


def _click_meta_reel_next_strict(page: Page, *, timeout_ms: int = 20_000) -> bool:
    """
    Bấm Next theo DOM Meta chuẩn (role=button, tabindex=0, aria-busy=false).

    Dùng chung luồng Cách 1 (wizard nhiều bước) — ưu tiên JS click tránh scroll loop.
    """
    stage = _reel_strict_prefix("Wizard")
    for locator_factory in (
        lambda: _locator_meta_reel_footer_next_with_cancel(page),
        lambda: page.locator(
            "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' "
            "and .//div[normalize-space()='Next']])[last()]"
        ),
        lambda: page.locator(
            "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' "
            "and .//div[normalize-space()='Next']])[last()]"
        ),
        lambda: page.locator(
            "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' "
            "and .//div[normalize-space()='Tiếp']])[last()]"
        ),
        lambda: page.locator(
            "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' "
            "and .//div[normalize-space()='Next']])[last()]"
        ),
        lambda: page.locator(
            "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' "
            "and .//div[normalize-space()='Next']])[last()]"
        ),
        lambda: _locator_meta_reel_next_structural(page).last,
        lambda: _locator_meta_reel_next_role(page).last,
        lambda: _locator_meta_reel_next_text_parent(page).last,
    ):
        try:
            b = locator_factory()
            if b.count() <= 0:
                continue
            if not b.is_visible(timeout=2_500):
                continue
            if (b.get_attribute("aria-disabled") or "").strip().lower() == "true":
                continue
            human_pause(kind="click", label="trước Next (Reel strict)")
            try:
                b.evaluate("el => el && el.click && el.click()")
                logger.info("{} _click_meta_reel_next_strict: JS click OK.", stage)
                return True
            except Exception as exc_js:
                logger.debug("{} _click_meta_reel_next_strict: JS lỗi: {}", stage, exc_js)
            try:
                b.click(timeout=min(timeout_ms, 6_000), force=True, no_wait_after=True)
                logger.info("{} _click_meta_reel_next_strict: force click OK.", stage)
                return True
            except Exception as exc_force:
                logger.debug("{} _click_meta_reel_next_strict: force lỗi: {}", stage, exc_force)
        except Exception:
            continue
    return False


def detect_meta_reel_ui_way(page: Page) -> Literal["way1", "way2", "unknown"]:
    """
    Phân nhánh UI Meta Reel sau upload.

    - **way1**: wizard nhiều bước (Next → thumbnail / mô tả → Post/Share).
    - **way2**: Post details — ô caption chung + Publish/Post (ít hoặc không cần Next).
    """
    try:
        if page.locator("div.notranslate._5rpu[role='combobox'][contenteditable='true']").first.is_visible(timeout=700):
            return "way2"
    except Exception:
        pass
    for marker in ("Post details", "Chi tiết bài đăng", "Post detail"):
        try:
            if page.get_by_text(marker, exact=False).first.is_visible(timeout=700):
                return "way2"
        except Exception:
            continue
    try:
        dlg = _active_reel_dialog(page)
        has_post = False
        for pat in (r"^Post$", r"^Publish$", r"^Đăng$"):
            try:
                if dlg.get_by_role("button", name=re.compile(pat, re.I)).first.is_visible(timeout=400):
                    has_post = True
                    break
            except Exception:
                continue
        has_next = _meta_reel_next_any_visible(page)
        if has_post and not has_next and _reel_description_screen_ready(page, timeout_ms=400):
            return "way2"
    except Exception:
        pass

    if _meta_reel_next_any_visible(page):
        return "way1"
    step = _reel_active_step_label(page)
    if step in ("create", "edit", "share"):
        return "way1"
    try:
        dlg = _active_reel_dialog(page)
        if dlg.get_by_text(re.compile(r"thumbnail|Choose a cover|ảnh bìa|chọn ảnh", re.I)).first.is_visible(timeout=500):
            return "way1"
    except Exception:
        pass
    if _reel_description_screen_ready(page, timeout_ms=500) and _meta_reel_next_any_visible(page):
        return "way1"
    return "unknown"


def _click_meta_reel_next_with_verify(page: Page, *, step_index: int = 0) -> bool:
    """Bấm Next (strict → dialog fallback) và xác nhận wizard đã chuyển bước."""
    before = _reel_active_step_label(page)
    for attempt in (1, 2):
        clicked = _click_meta_reel_next_strict(page)
        if not clicked:
            try:
                dlg = _active_reel_dialog(page)
                _click_next_in_dialog(page, dlg)
                clicked = True
            except PlaywrightTimeoutError:
                clicked = False
        if not clicked:
            if attempt == 2:
                return False
            page.wait_for_timeout(_reel_inter_click_wait_ms())
            continue
        page.wait_for_timeout(_reel_inter_click_wait_ms())
        if (
            _wait_reel_step_change(page, before, timeout_ms=10_000)
            or _reel_description_screen_ready(page, timeout_ms=450)
            or _reel_post_button_maybe_visible(page, timeout_ms=400)
        ):
            logger.info(
                "{} Next #{} OK (attempt {}, step {} -> {}).",
                _reel_strict_prefix("Wizard"),
                step_index,
                attempt,
                before,
                _reel_active_step_label(page),
            )
            return True
        page.wait_for_timeout(_reel_inter_click_wait_ms())
    return _reel_description_screen_ready(page, timeout_ms=600) or _reel_post_button_maybe_visible(page, timeout_ms=500)


def _click_next_in_dialog(page: Page, dialog: Locator) -> None:
    """Click nút Next/Tiếp theo — ưu tiên strict (Cách 1), fallback role/text trong dialog."""
    if _click_meta_reel_next_strict(page):
        page.wait_for_timeout(_reel_inter_click_wait_ms())
        return
    pat = re.compile(r"Next|Tiếp|Tiếp theo", re.I)
    cands = dialog.get_by_role("button", name=pat)
    if _click_visible_enabled_button(cands, timeout_ms=1400):
        page.wait_for_timeout(_reel_inter_click_wait_ms())
        return
    txt_cands = dialog.get_by_text(pat)
    if _click_visible_enabled_button(txt_cands, timeout_ms=1200):
        page.wait_for_timeout(_reel_inter_click_wait_ms())
        return
    raise PlaywrightTimeoutError("Không tìm thấy nút Next usable trong popup Reel.")


_REEL_POST_LABEL_RE = re.compile(r"^(Post|Đăng|Publish)$", re.I)

_REEL_POST_SPAN_CLASS = "contains(@class,'x6ikm8r') or contains(@class,'x1j85h84')"

_REEL_POST_STRICT_ONLY_XPATHS: tuple[str, ...] = tuple(
    xp.format(_span=_REEL_POST_SPAN_CLASS)
    for xp in (
        "(//div[contains(@class,'html-div')]/div[@role='none'][.//span[contains(@class,'x6ikm8r') and contains(@class,'x1j85h84') and normalize-space()='Post'])[last()]",
        "(//div[contains(@class,'html-div')]/div[@role='none'][.//span[@dir='auto']//span[contains(@class,'x6ikm8r') and contains(@class,'x1j85h84') and normalize-space()='Post'])[last()]",
        "(//div[@role='none' and .//span[{_span}] and normalize-space()='Post'])[last()]",
        "(//div[@role='none' and .//span[{_span}] and normalize-space()='Đăng'])[last()]",
        "(//div[contains(@class,'html-div')][.//div[@role='none']//span[@dir='auto']//span[{_span}] and normalize-space()='Post'])[last()]",
        "(//div[contains(@class,'html-div')][.//div[@role='none']//span[{_span}] and normalize-space()='Post'])[last()]",
        "(//div[contains(@class,'html-div')][.//div[@role='none']//span[{_span}] and normalize-space()='Đăng'])[last()]",
        "(//span[{_span}] and normalize-space()='Post']/ancestor::div[contains(@class,'html-div')][1])[last()]",
        "(//span[{_span}] and normalize-space()='Đăng']/ancestor::div[contains(@class,'html-div')][1])[last()]",
        "(//div[contains(@class,'html-div')][.//span[normalize-space()='Save']]/following::div[contains(@class,'html-div')][.//span[{_span}] and normalize-space()='Post'])[last()]",
    )
)

# Giữ alias cũ — chỉ xpath chặt (không match «Posts», menu Create…)
_REEL_POST_STRICT_XPATHS = _REEL_POST_STRICT_ONLY_XPATHS
_REEL_POST_CSS_SELECTORS: tuple[str, ...] = (
    "div.html-div > div[role='none']:has(span.x6ikm8r.x1j85h84)",
    "div.html-div > div[role='none'] span.x6ikm8r.x1j85h84",
    "div.html-div:has(div[role='none'] span.x6ikm8r.x1j85h84)",
    "div.html-div:has(div[role='none'] span.x6ikm8r)",
    "div.html-div:has(div[role='none'] span.x1j85h84)",
    "div.html-div div[role='none'] span.x6ikm8r",
    "div.html-div div[role='none'] span.x1j85h84",
    "div.html-div [role='none'] span.x6ikm8r:has-text('Post')",
    "div.html-div [role='none'] span.x1j85h84:has-text('Post')",
    "div.html-div [role='none'] span.x6ikm8r:has-text('Đăng')",
    "div.html-div [role='none'] span.x1j85h84:has-text('Đăng')",
)

_REEL_DIALOG_POST_CLICK_JS = """
() => {
  const dlg = document.querySelector('[role="dialog"]');
  if (!dlg) return false;
  for (const ov of dlg.querySelectorAll('[data-visualcompletion="ignore"]')) {
    ov.style.pointerEvents = 'none';
  }
  const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
  const isPost = (t) => /^(post|đăng|publish)$/i.test(t);
  const minBottom = window.innerHeight * 0.42;
  const hits = [];
  const scan = (nodes, role) => {
    for (const el of nodes) {
      if (el.getAttribute && el.getAttribute('data-visualcompletion') === 'ignore') continue;
      const t = norm(el.textContent);
      if (!isPost(t)) continue;
      if (el.closest('[role="listbox"], [role="option"], [role="menu"]')) continue;
      const r = el.getBoundingClientRect();
      if (r.width < 36 || r.height < 14) continue;
      const hasOverlay = !!el.querySelector('[data-visualcompletion="ignore"]');
      let nearSave = false;
      for (const s of dlg.querySelectorAll('*')) {
        if (norm(s.textContent) !== 'Save') continue;
        const sr = s.getBoundingClientRect();
        if (Math.abs(sr.bottom - r.bottom) < 90 && sr.left <= r.left + 40) {
          nearSave = true;
          break;
        }
      }
      hits.push({
        el,
        bottom: r.bottom,
        left: r.left,
        area: r.width * r.height,
        hasOverlay,
        nearSave,
        nearFooter: r.top >= minBottom,
        role,
      });
    }
  };
  scan(dlg.querySelectorAll("div[role='none']"), 'none');
  scan(dlg.querySelectorAll("[role='button']"), 'button');
  if (!hits.length) return false;
  const score = (h) =>
    (h.nearSave ? 2_000_000 : 0) +
    (h.hasOverlay ? 1_000_000 : 0) +
    (h.nearFooter ? 500_000 : 0) +
    (h.role === 'none' ? 80_000 : 20_000) +
    h.bottom * 1000 + h.left * 10 + h.area;
  hits.sort((a, b) => score(b) - score(a));
  const fire = (node) => {
    if (!node) return false;
    try {
      for (const ov of node.querySelectorAll('[data-visualcompletion="ignore"]')) {
        ov.style.pointerEvents = 'none';
      }
      const r = node.getBoundingClientRect();
      const x = r.left + r.width / 2;
      const y = r.top + r.height / 2;
      for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) {
        node.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window, clientX: x, clientY: y }));
      }
      if (typeof node.click === 'function') node.click();
      return true;
    } catch (_) { return false; }
  };
  return fire(hits[0].el);
}
"""

_REEL_SETTINGS_POST_CLICK_JS = _REEL_DIALOG_POST_CLICK_JS
_REEL_POST_FOOTER_CLICK_JS = _REEL_DIALOG_POST_CLICK_JS

_REEL_POST_DISABLE_OVERLAY_JS = """(el) => {
  if (!el) return;
  const root = el.closest("[role='none']") || el;
  for (const ov of root.querySelectorAll('[data-visualcompletion="ignore"]')) {
    ov.style.pointerEvents = 'none';
  }
}"""

_REEL_POST_CLICK_JS = """(el) => {
  if (!el) return false;
  const root = el.closest("[role='none']") || el.closest("[role='button']") || el;
  for (const ov of root.querySelectorAll('[data-visualcompletion="ignore"]')) {
    ov.style.pointerEvents = 'none';
  }
  try {
    const r = root.getBoundingClientRect();
    const x = r.left + r.width / 2;
    const y = r.top + r.height / 2;
    for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) {
      root.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window, clientX: x, clientY: y }));
    }
    if (typeof root.click === 'function') root.click();
    return true;
  } catch (_) { return false; }
}"""


def _reel_settings_screen_visible(page: Page, *, timeout_ms: int = 350) -> bool:
    for pat in (r"^\s*Reel settings\s*$", r"^\s*Cài đặt Thước phim\s*$"):
        try:
            if page.get_by_text(re.compile(pat, re.I)).first.is_visible(timeout=timeout_ms):
                return True
        except Exception:
            continue
    return False


def _reel_post_label_text(loc: Locator) -> str:
    try:
        return str(loc.inner_text(timeout=400) or "").strip()
    except Exception:
        return ""


def _reel_post_label_matches(text: str) -> bool:
    t = str(text or "").strip()
    if not t or not _REEL_POST_LABEL_RE.match(t):
        return False
    if re.search(r"\bposts\b", t, re.I):
        return False
    return True


def _reel_locator_near_footer(loc: Locator, *, min_ratio: float = 0.42) -> bool:
    """Tránh bấm nhầm «Post» ở sidebar khi đang ở Reel settings."""
    try:
        box = loc.bounding_box()
        if not box:
            return True
        vp = loc.page.viewport_size or {"height": 900}
        return float(box["y"]) >= float(vp.get("height", 900)) * min_ratio
    except Exception:
        return True


def _resolve_reel_post_click_target(loc: Locator) -> Locator:
    """Chọn wrapper clickable ngoài cùng (role=none có overlay hoặc role=button)."""
    try:
        outer = loc.locator(
            "xpath=ancestor::div[@role='none']"
            "[.//span[normalize-space()='Post' or normalize-space()='Đăng' or normalize-space()='Publish']]"
        ).last
        if outer.count() > 0:
            return outer
    except Exception:
        pass
    try:
        btn = loc.locator(
            "xpath=ancestor::*[@role='button']"
            "[.//*[normalize-space()='Post' or normalize-space()='Đăng' or normalize-space()='Publish']]"
        ).last
        if btn.count() > 0:
            return btn
    except Exception:
        pass
    return loc


def _reel_locator_post_usable_in_context(
    loc: Locator,
    page: Page,
    *,
    require_footer: bool = False,
) -> bool:
    if not _reel_locator_post_usable(loc):
        return False
    if require_footer and _reel_settings_screen_visible(page, timeout_ms=120):
        return _reel_locator_near_footer(loc)
    return True


def _dismiss_reel_hashtag_suggestion(page: Page) -> None:
    """Đóng dropdown gợi ý hashtag sau nhập caption — tránh chặn bấm Post."""
    for _ in range(3):
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(180)
        except Exception:
            break
    try:
        hdr = page.get_by_text(re.compile(r"^\s*Reel settings\s*$", re.I)).first
        if hdr.is_visible(timeout=500):
            hdr.click(timeout=800, force=True)
            page.wait_for_timeout(280)
    except Exception:
        pass
    try:
        if page.get_by_text(re.compile(r"safe to publish", re.I)).first.is_visible(timeout=400):
            page.get_by_text(re.compile(r"safe to publish", re.I)).first.click(timeout=600, force=True)
            page.wait_for_timeout(200)
    except Exception:
        pass


def _reel_locator_is_post_submit(loc: Locator) -> bool:
    """Xác minh locator là nút Post/Đăng thật (không phải «Posts», «Create post»…)."""
    try:
        if loc.count() <= 0 or not loc.is_visible(timeout=350):
            return False
    except Exception:
        return False
    try:
        aria = (loc.get_attribute("aria-label") or "").strip()
        if aria and _REEL_POST_LABEL_RE.match(aria):
            return True
    except Exception:
        pass
    txt = _reel_post_label_text(loc)
    if _reel_post_label_matches(txt):
        return True
    try:
        span = loc.locator("span.x6ikm8r, span.x1j85h84, span").filter(has_text=_REEL_POST_LABEL_RE).first
        if span.count() > 0 and span.is_visible(timeout=200):
            inner = _reel_post_label_text(span)
            return _reel_post_label_matches(inner)
    except Exception:
        pass
    return False


def _reel_locator_post_usable(loc: Locator) -> bool:
    """Post/Publish visible, đúng nhãn và không bị aria-disabled."""
    if not _reel_locator_is_post_submit(loc):
        return False
    try:
        if (loc.get_attribute("aria-disabled") or "").strip().lower() == "true":
            return False
        if loc.get_attribute("disabled") is not None:
            return False
    except Exception:
        return False
    return True


def _locator_reel_settings_post_html_div(scope: Locator) -> Locator:
    """
    Footer Reel settings — ``div.html-div`` > ``div[role=none]`` > span[dir=auto] > span (x6ikm8r+x1j85h84).
    """
    return scope.locator(
        "xpath=.//div[contains(@class,'html-div')]/div[@role='none']"
        "[.//span[contains(@class,'x6ikm8r') and contains(@class,'x1j85h84') "
        "and (normalize-space()='Post' or normalize-space()='Đăng')]][last()]"
    )


def _reel_footer_post_visible(
    page: Page,
    *,
    dialog: Locator | None = None,
    timeout_ms: int = 350,
) -> bool:
    """Nút Post footer còn hiển thị — strict + html-div (span dual-class)."""
    dlg = dialog if dialog is not None else _active_reel_dialog(page)
    for loc in _reel_strict_post_button_locators(page, dialog=dlg):
        if _reel_locator_post_usable(loc):
            return True
    try:
        html_post = _locator_reel_settings_post_html_div(dlg)
        if html_post.count() > 0 and html_post.is_visible(timeout=timeout_ms):
            return True
    except Exception:
        pass
    try:
        save = dlg.locator("xpath=.//*[normalize-space()='Save']").last
        post = dlg.get_by_text("Post", exact=True).last
        if (
            save.is_visible(timeout=timeout_ms)
            and post.is_visible(timeout=timeout_ms)
            and _reel_locator_near_footer(post)
        ):
            return True
    except Exception:
        pass
    return False


def _reel_strict_post_button_usable(page: Page, *, timeout_ms: int = 450) -> bool:
    return _reel_footer_post_visible(page, timeout_ms=timeout_ms)


def _locator_reel_settings_post_primary(scope: Locator) -> Locator:
    """Nút Post xanh footer Reel settings — ưu tiên html-div + dual-class span."""
    return _locator_reel_settings_post_html_div(scope)


def _locator_reel_post_footer_button(scope: Locator) -> Locator:
    """``role=button`` footer — Post / Đăng / Publish (Meta Business + Reel settings)."""
    return scope.locator(
        "xpath=.//*[@role='button' and @tabindex='0' and @aria-busy='false']"
        "[.//*[normalize-space()='Post' or normalize-space()='Đăng' or normalize-space()='Publish']]"
        "[last()]"
    )


def _reel_strict_post_button_locators(page: Page, *, dialog: Locator | None = None) -> list[Locator]:
    """Chỉ nút Post Reel settings — trong dialog Reel, xpath/css chặt."""
    scopes: list[Locator] = []
    if dialog is not None:
        scopes.append(dialog)
    try:
        dlg = page.locator("[role='dialog']").last
        if dlg.count() > 0:
            scopes.append(dlg)
    except Exception:
        pass
    out: list[Locator] = []
    seen: set[int] = set()

    def _add(loc: Locator) -> None:
        key = id(loc)
        if key in seen:
            return
        seen.add(key)
        out.append(loc)

    for scope in scopes:
        try:
            _add(_locator_reel_settings_post_html_div(scope))
        except Exception:
            pass
        try:
            _add(_locator_reel_settings_post_primary(scope))
        except Exception:
            pass
        try:
            _add(
                scope.locator("div.html-div > div[role='none']").filter(
                    has=scope.locator(
                        "span.x6ikm8r.x1j85h84",
                        has_text=_REEL_POST_LABEL_RE,
                    )
                ).last
            )
        except Exception:
            pass
        try:
            _add(_locator_reel_post_footer_button(scope))
        except Exception:
            pass
        try:
            _add(scope.get_by_role("button", name=_REEL_POST_LABEL_RE).last)
        except Exception:
            pass
        for xp in _REEL_POST_STRICT_ONLY_XPATHS:
            _add(scope.locator(f"xpath={xp}"))
        for css in _REEL_POST_CSS_SELECTORS:
            try:
                _add(scope.locator(css).last)
            except Exception:
                pass
        try:
            _add(
                scope.locator("div.html-div").filter(
                    has=scope.locator(
                        'div[role="none"] span.x6ikm8r, div[role="none"] span.x1j85h84',
                        has_text=_REEL_POST_LABEL_RE,
                    )
                ).last
            )
        except Exception:
            pass
        try:
            _add(
                scope.locator(
                    "xpath=.//*[normalize-space()='Save']/following::*"
                    "[self::div or self::span or self::button]"
                    "[normalize-space()='Post' or normalize-space()='Đăng' or normalize-space()='Publish'][1]"
                )
            )
        except Exception:
            pass
    return out


def _reel_strict_post_button_visible(page: Page, *, timeout_ms: int = 450) -> bool:
    dialog = _active_reel_dialog(page)
    for loc in _reel_strict_post_button_locators(page, dialog=dialog):
        if _reel_locator_is_post_submit(loc):
            return True
    return False


def _reel_share_button_locator(page: Page) -> Locator:
    return page.locator(
        "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' "
        "and .//div[normalize-space()='Share']])[last()]"
    )


def _reel_done_button_locator(page: Page) -> Locator:
    return page.locator(
        "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' "
        "and (.//div[normalize-space()='Done'] or .//div[normalize-space()='Xong'])])[last()]"
    )


def _reel_share_button_visible(page: Page, *, timeout_ms: int = 400) -> bool:
    s = _reel_share_button_locator(page)
    try:
        if s.count() <= 0:
            return False
        if not s.is_visible(timeout=timeout_ms):
            return False
        if (s.get_attribute("aria-disabled") or "").strip().lower() == "true":
            return False
        return True
    except Exception:
        return False


def _click_reel_share_best_effort(page: Page, *, timeout_ms: int = 20_000) -> bool:
    stage = _reel_strict_prefix("Wizard")
    s = _reel_share_button_locator(page)
    try:
        if s.count() <= 0:
            logger.warning("{} _click_share: không thấy Share theo XPath chuẩn.", stage)
            return False
        if not s.is_visible(timeout=2_500):
            logger.warning("{} _click_share: Share tồn tại nhưng chưa visible.", stage)
            return False
        if (s.get_attribute("aria-disabled") or "").strip().lower() == "true":
            logger.warning("{} _click_share: Share đang aria-disabled=true.", stage)
            return False
        human_pause(kind="click", label="trước Share (Reel)")
        try:
            s.evaluate("el => el && el.click && el.click()")
            logger.info("{} _click_share: đã dispatch JS click.", stage)
            return True
        except Exception as exc_js:
            logger.debug("{} _click_share: JS click lỗi: {}", stage, exc_js)
        try:
            s.click(timeout=min(timeout_ms, 6_000), force=True, no_wait_after=True)
            logger.info("{} _click_share: đã click force (fallback).", stage)
            return True
        except Exception as exc_force:
            logger.warning("{} _click_share: force click lỗi: {}", stage, exc_force)
            return False
    except Exception as exc:
        logger.warning("{} _click_share: lỗi không xác định: {}", stage, exc)
        return False


def _click_reel_done_best_effort(page: Page, *, timeout_ms: int = 15_000) -> bool:
    stage = _reel_strict_prefix("Wizard")
    d = _reel_done_button_locator(page)
    try:
        if d.count() <= 0:
            return False
        if not d.is_visible(timeout=2_500):
            return False
        if (d.get_attribute("aria-disabled") or "").strip().lower() == "true":
            return False
        human_pause(kind="click", label="trước Done (Reel)")
        try:
            d.evaluate("el => el && el.click && el.click()")
            logger.info("{} _click_done: đã dispatch JS click.", stage)
            return True
        except Exception as exc_js:
            logger.debug("{} _click_done: JS click lỗi: {}", stage, exc_js)
        try:
            d.click(timeout=min(timeout_ms, 5_000), force=True, no_wait_after=True)
            logger.info("{} _click_done: đã click force (fallback).", stage)
            return True
        except Exception as exc_force:
            logger.warning("{} _click_done: force click lỗi: {}", stage, exc_force)
            return False
    except Exception as exc:
        logger.debug("{} _click_done: lỗi không xác định: {}", stage, exc)
        return False


def _finish_reel_after_share_submit(page: Page) -> bool:
    """Sau Share trên Meta Business composer: Done + chờ modal processing nếu có."""
    stage = _reel_strict_prefix("Wizard")
    _human_pause()
    done_clicked = False
    try:
        db = _reel_done_button_locator(page)
        if db.count() > 0 and db.is_visible(timeout=20_000):
            if _click_reel_done_best_effort(page):
                logger.info("{} Đã bấm Done chuẩn.", stage)
                done_clicked = True
                _human_pause()
    except Exception as exc:
        if "closed" in str(exc).lower():
            logger.info("{} Page đóng sau Share trước khi kiểm tra Done → coi như đã submit.", stage)
            return True
        raise

    try:
        processed = dismiss_meta_video_post_processing_modal_best_effort(
            page, timeout_ms=120_000, give_up_if_never_seen_ms=15_000
        )
    except Exception as exc:
        if "closed" in str(exc).lower():
            logger.info("{} Page đóng khi chờ processing → coi như đã submit.", stage)
            return True
        raise
    submit_clicked = bool(done_clicked or processed)
    try:
        _enable_view_only_guard(page)
    except Exception as exc:
        if "closed" in str(exc).lower():
            logger.info("{} Page đóng khi bật lại lock-ui cuối wizard → coi như đã submit.", stage)
            return True
        logger.debug("{} Không bật lại được lock-ui cuối wizard: {}", stage, exc)
    if not submit_clicked:
        raise PlaywrightTimeoutError("Đã bấm Share nhưng không thấy Done/processing xác nhận.")
    return True


def _reel_wizard_ready_to_share(
    page: Page,
    *,
    payload: str,
    filled: bool,
    next_clicks: int,
) -> bool:
    """Meta Business composer: Share sau wizard (≥1 Next hoặc đã nhập caption trên màn cuối)."""
    if not _reel_share_button_visible(page, timeout_ms=400):
        return False
    if next_clicks >= 1:
        return True
    if filled:
        return True
    if not str(payload or "").strip():
        return True
    # Share đã enable — caption/tiêu đề không bắt buộc.
    return True


def _resolve_reel_submit_action(
    page: Page,
    *,
    payload: str,
    filled: bool,
    next_clicks: int,
    submit_mode: Literal["post", "share", "auto"],
) -> Literal["post", "share", ""]:
    mode = (submit_mode or "auto").strip().lower()  # type: ignore[assignment]
    if mode == "post":
        return "post" if _reel_wizard_ready_to_post(
            page, payload=payload, filled=filled, next_clicks=next_clicks
        ) else ""
    if mode == "share":
        return "share" if _reel_wizard_ready_to_share(
            page, payload=payload, filled=filled, next_clicks=next_clicks
        ) else ""
    if _reel_wizard_ready_to_post(page, payload=payload, filled=filled, next_clicks=next_clicks):
        return "post"
    if _reel_wizard_ready_to_share(page, payload=payload, filled=filled, next_clicks=next_clicks):
        return "share"
    return ""


def _reel_wizard_ready_to_post(
    page: Page,
    *,
    payload: str,
    filled: bool,
    next_clicks: int,
) -> bool:
    """
    Chỉ đăng khi nút Post/Publish strict, enabled VÀ đủ điều kiện wizard:
    - Màn «Reel settings», hoặc
    - Đã bấm Next ≥1 (way1), hoặc
    - Đã nhập caption trên màn Post details (way2, next_clicks có thể = 0), hoặc
    - Không có caption/tiêu đề trong job, hoặc
    - Post đã enable (Meta cho phép đăng không bắt buộc mô tả).
    """
    on_settings = _reel_settings_screen_visible(page, timeout_ms=280)
    if on_settings and filled and str(payload or "").strip():
        try:
            if page.get_by_text(re.compile(r"safe to publish", re.I)).first.is_visible(timeout=450):
                return True
        except Exception:
            pass
        try:
            if page.locator("xpath=.//*[normalize-space()='Save']").first.is_visible(timeout=450):
                return True
        except Exception:
            pass
    if not _reel_strict_post_button_visible(page, timeout_ms=400):
        if on_settings and filled:
            return True
        return False
    post_usable = _reel_strict_post_button_usable(page, timeout_ms=350)
    if on_settings:
        if filled and str(payload or "").strip():
            return True
        return post_usable
    if not post_usable:
        return False
    if next_clicks >= 1:
        return True
    if filled:
        return True
    if not str(payload or "").strip():
        return True
    # Có payload nhưng chưa fill — vẫn đăng nếu Meta đã bật nút Post (không bắt buộc tiêu đề).
    return True


def _reel_post_button_locators(page: Page, *, dialog: Locator | None = None) -> list[Locator]:
    """Ứng viên nút Post — chỉ selector chặt trong dialog Reel."""
    return _reel_strict_post_button_locators(page, dialog=dialog)


def _click_reel_post_locator(
    page: Page,
    loc: Locator,
    *,
    prefer_mouse: bool = False,
    tag: str = "",
) -> None:
    stage = _reel_strict_prefix("Wizard")
    loc.scroll_into_view_if_needed(timeout=2_000)
    target = _resolve_reel_post_click_target(loc)
    try:
        target.evaluate(_REEL_POST_DISABLE_OVERLAY_JS)
    except Exception:
        pass
    guard_on = _view_only_mode_enabled()
    if guard_on:
        _disable_view_only_guard(page)
    try:
        if not prefer_mouse and _js_click_submit_button_locator(
            target, label=tag or "Post/Reel"
        ):
            logger.info("{} [POST_TARGET] reel_post_js_direct {}", stage, tag)
            page.wait_for_timeout(max(350, _env_int("FB_REEL_POST_CLICK_SETTLE_MS", 500)))
            return
        if not prefer_mouse:
            try:
                if target.evaluate(_REEL_POST_CLICK_JS):
                    logger.info("{} [POST_TARGET] reel_post_js_click {}", stage, tag)
                    page.wait_for_timeout(max(350, _env_int("FB_REEL_POST_CLICK_SETTLE_MS", 500)))
                    return
            except Exception:
                pass
        box = target.bounding_box()
        if box:
            page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
            logger.info("{} [POST_TARGET] reel_post_mouse_click {}", stage, tag)
            page.wait_for_timeout(max(350, _env_int("FB_REEL_POST_CLICK_SETTLE_MS", 500)))
            return
        target.click(timeout=1400, force=True, no_wait_after=True)
        logger.info("{} [POST_TARGET] reel_post_force_click {}", stage, tag)
        page.wait_for_timeout(max(350, _env_int("FB_REEL_POST_CLICK_SETTLE_MS", 500)))
    finally:
        if guard_on:
            _enable_view_only_guard(page)


def _reel_dialog_post_js_click(page: Page, dialog: Locator) -> bool:
    stage = _reel_strict_prefix("Wizard")
    for fn, label in (
        (lambda: dialog.evaluate(_REEL_DIALOG_POST_CLICK_JS), "dialog"),
        (lambda: page.evaluate(_REEL_DIALOG_POST_CLICK_JS), "page"),
    ):
        try:
            if fn():
                logger.info("{} [POST_TARGET] reel_dialog_post_js ({})", stage, label)
                return True
        except Exception:
            continue
    return False


def _click_reel_post_locators_batch(
    page: Page,
    dialog: Locator,
    *,
    prefer_mouse: bool,
    require_footer: bool,
) -> bool:
    """Thử lần lượt mọi locator Post trong dialog — dừng khi gửi được click."""
    stage = _reel_strict_prefix("Wizard")
    for idx, loc in enumerate(_reel_strict_post_button_locators(page, dialog=dialog)):
        try:
            if not _reel_locator_post_usable_in_context(
                loc, page, require_footer=require_footer
            ):
                continue
            _click_reel_post_locator(
                page,
                loc,
                prefer_mouse=prefer_mouse,
                tag=f"loc#{idx}",
            )
            logger.info("{} Đã gửi click Post (locator #{}).", stage, idx)
            return True
        except Exception:
            continue
    return False


def _click_reel_publish_button_in_dialog(page: Page, dialog: Locator) -> bool:
    """Meta Business: chờ Publish enable rồi bấm trong dialog."""
    stage = _reel_strict_prefix("Wizard")
    try:
        pub = _locator_reel_post_footer_button(dialog)
        if pub.count() <= 0:
            pub = dialog.get_by_role("button", name=re.compile(r"^Publish$", re.I)).last
        if pub.count() <= 0 or not pub.is_visible(timeout=2_500):
            return False
        try:
            page.wait_for_function(_submit_button_is_enabled_js(), timeout=8_000)
        except Exception:
            pass
        if (pub.get_attribute("aria-disabled") or "").strip().lower() == "true":
            return False
        _click_reel_post_locator(page, pub, prefer_mouse=True, tag="publish")
        logger.info("{} Đã gửi click Publish trong dialog.", stage)
        return True
    except Exception:
        return False


def _dispatch_reel_post_click(page: Page, dialog: Locator, *, attempt: int = 1) -> bool:
    """Gửi click Post/Publish — chiến lược khác nhau theo attempt (1→3)."""
    stage = _reel_strict_prefix("Wizard")
    if attempt == 1:
        human_pause(kind="click", label="trước Post (Reel settings)")
    require_footer = _reel_settings_screen_visible(page, timeout_ms=200)
    if attempt == 1:
        if _reel_dialog_post_js_click(page, dialog):
            return True
        if _click_reel_post_locators_batch(
            page, dialog, prefer_mouse=False, require_footer=require_footer
        ):
            return True
        if _click_reel_publish_button_in_dialog(page, dialog):
            return True
        return False
    if attempt == 2:
        _dismiss_reel_hashtag_suggestion(page)
        if _click_reel_post_locators_batch(
            page, dialog, prefer_mouse=True, require_footer=require_footer
        ):
            return True
        if _reel_dialog_post_js_click(page, dialog):
            return True
        return False
    # attempt 3 — fallback rộng trong dialog, không scroll toàn trang
    _dismiss_reel_hashtag_suggestion(page)
    if _click_reel_publish_button_in_dialog(page, dialog):
        return True
    try:
        _click_post_strict_for_reel(page, dialog)
        return True
    except Exception:
        pass
    try:
        post_btn = _wait_post_button_in_dialog(dialog, timeout_ms=6_000)
        _click_reel_post_locator(page, post_btn, prefer_mouse=True, tag="wait_post")
        return True
    except Exception:
        logger.warning("{} Attempt {}: hết locator trong dialog.", stage, attempt)
        return False


def _wait_post_button_in_dialog(dialog: Locator, *, timeout_ms: int = 20_000) -> Locator:
    page = dialog.page
    candidates = _reel_post_button_locators(page, dialog=dialog)
    deadline = time.time() + (max(1500, timeout_ms) / 1000.0)
    while time.time() < deadline:
        for c in candidates:
            try:
                if _reel_locator_is_post_submit(c):
                    if _reel_settings_screen_visible(page, timeout_ms=120):
                        logger.info(
                            "{} Thấy nút Post (Reel settings / html-div).",
                            _reel_strict_prefix("Wizard"),
                        )
                    return c
            except Exception:
                continue
        page.wait_for_timeout(280)
    raise PlaywrightTimeoutError("Không thấy nút Post/Publish usable trong popup Reel.")


def _click_post_strict_for_reel(page: Page, dialog: Locator) -> None:
    """Click nút Post Reel (html-div / role=none / span.x1j85h84), tránh nhầm Share to groups."""
    for c in _reel_post_button_locators(page, dialog=dialog):
        try:
            if not _reel_locator_post_usable(c):
                continue
            _click_reel_post_locator(page, c)
            return
        except Exception:
            continue
    raise PlaywrightTimeoutError("Không click được nút Post strict theo popup Reel.")


def _build_reel_text_payload(title: str, content: str, hashtags: list[str] | str | None) -> str:
    title, content, tags = dedupe_post_title_content_hashtags(title, content, hashtags)
    parts = [x for x in (title, content) if x]
    if tags:
        parts.append(" ".join(tags))
    return "\n\n".join(parts).strip()


def _resolve_reel_textbox(dialog: Locator) -> Locator:
    """Ô nhập mô tả/title — ưu tiên locator Meta Reel, fallback dialog."""
    pg = dialog.page
    for loc in _meta_reel_description_editor_locators(pg):
        try:
            if loc.is_visible(timeout=500):
                return loc
        except Exception:
            continue
    return dialog.locator("[role='textbox'], textarea, [contenteditable='true']").last


def _input_reel_text_in_dialog(dialog: Locator, text: str) -> None:
    raw = str(text or "").strip()
    if not raw:
        return
    tb = _resolve_reel_textbox(dialog)
    tb.wait_for(state="visible", timeout=10_000)
    try:
        tb.click(timeout=1200)
    except Exception:
        tb.click(timeout=1200, force=True)
    try:
        tb.fill(raw)
    except Exception:
        tb.press_sequentially(raw, delay=30)


def _normalize_hashtags_for_input(hashtags: list[str] | str | None) -> list[str]:
    return _normalize_hashtag_list(hashtags)


def _input_reel_title_content_and_hashtags(
    dialog: Locator,
    *,
    title: str,
    content: str,
    hashtags: list[str] | str | None,
) -> None:
    """
    Nhập theo yêu cầu:
    - Title + Content trước
    - Hashtag nhập từng cái, mỗi hashtag Enter (kèm Space trước Enter).
    """
    tb = _resolve_reel_textbox(dialog)
    tb.wait_for(state="visible", timeout=14_000)
    try:
        tb.click(timeout=1200)
    except Exception:
        tb.click(timeout=1200, force=True)

    title_s, content_s, hashtags = dedupe_post_title_content_hashtags(title, content, hashtags)
    base_parts = [title_s, content_s]
    base_text = "\n\n".join([p for p in base_parts if p]).strip()
    if base_text:
        # Tránh nhập lặp: nếu textbox đã chứa title/content thì bỏ qua phần base.
        existing = ""
        try:
            existing = str(tb.inner_text(timeout=700) or "").strip()
        except Exception:
            existing = ""
        norm_existing = re.sub(r"\s+", " ", existing).lower()
        norm_base = re.sub(r"\s+", " ", base_text).lower()
        already_has_base = bool(norm_base and norm_base in norm_existing)
        if not already_has_base:
            try:
                # Clear trước khi nhập để tránh append đúp.
                tb.press("ControlOrMeta+a")
                tb.press("Backspace")
                dialog.page.wait_for_timeout(180)
            except Exception:
                pass
            # Contenteditable của Meta đôi khi append khi dùng fill(); dùng gõ tuần tự ổn định hơn.
            tb.press_sequentially(base_text, delay=28)
            # Delay sau khi nhập phần title/content để UI ổn định.
            dialog.page.wait_for_timeout(random.randint(700, 1400))
            tb.press("Enter")
            tb.press("Enter")
            dialog.page.wait_for_timeout(random.randint(500, 1100))

    tags = _normalize_hashtags_for_input(hashtags)

    def _pick_first_hashtag_suggestion() -> bool:
        pg = dialog.page
        deadline = time.time() + 4.0
        while time.time() < deadline:
            try:
                lb = pg.locator("ul[role='listbox'][aria-busy='false']").last
                if lb.count() > 0 and lb.is_visible(timeout=180):
                    # Ưu tiên item đang selected=true; nếu không có thì lấy option đầu tiên.
                    opt_selected = lb.locator("li[role='option'][aria-selected='true']").first
                    if opt_selected.count() > 0 and opt_selected.is_visible(timeout=120):
                        try:
                            opt_selected.click(timeout=900)
                        except Exception:
                            opt_selected.click(timeout=900, force=True)
                        return True
                    opt_first = lb.locator("li[role='option']").first
                    if opt_first.count() > 0 and opt_first.is_visible(timeout=120):
                        try:
                            opt_first.click(timeout=900)
                        except Exception:
                            opt_first.click(timeout=900, force=True)
                        return True
            except Exception:
                pass
            pg.wait_for_timeout(160)
        return False

    for t in tags:
        tb.press_sequentially(t, delay=26)
        dialog.page.wait_for_timeout(random.randint(280, 650))
        # Luồng hashtag mới: nếu có list gợi ý thì chọn option đầu tiên.
        picked = _pick_first_hashtag_suggestion()
        if not picked:
            # Fallback khi list không hiện: Enter để commit hashtag hiện tại.
            tb.press("Enter")
            dialog.page.wait_for_timeout(random.randint(180, 420))
        # Sau khi chọn, bấm Space để nhập hashtag tiếp theo.
        tb.press("Space")
        dialog.page.wait_for_timeout(random.randint(550, 1200))


def _active_reel_dialog(page: Page) -> Locator:
    """Dialog Reel hiện tại — luôn lấy mới sau mỗi Next (tránh locator cũ detached)."""
    return page.locator("[role='dialog']").last


def _reel_post_button_maybe_visible(page: Page, *, timeout_ms: int = 450) -> bool:
    return _reel_strict_post_button_visible(page, timeout_ms=timeout_ms)


def _reel_still_on_edit_trim_screen(page: Page, *, timeout_ms: int = 280) -> bool:
    """Còn ở Edit reel / trim / copyright — chưa có ô caption dùng được."""
    if _reel_lexical_description_usable(page, timeout_ms=timeout_ms):
        return False
    if _reel_edit_reel_header_visible(page, timeout_ms=timeout_ms):
        return True
    if _reel_caption_screen_markers_visible(page, timeout_ms=250):
        return False
    for m in (
        "Trim video",
        "Cắt video",
        "Checking for copyrighted content",
        "Đang kiểm tra bản quyền",
    ):
        try:
            if page.get_by_text(m, exact=False).first.is_visible(timeout=timeout_ms):
                return True
        except Exception:
            continue
    return False


def _reel_caption_input_usable(page: Page, *, timeout_ms: int = 450) -> bool:
    """Ô nhập caption — Lexical Edit reel, aria-placeholder, hoặc editor Meta cũ."""
    if _reel_lexical_description_usable(page, timeout_ms=timeout_ms):
        return True
    for sel in (
        "[role='textbox'][contenteditable='true'][aria-placeholder*='Describe' i]",
        "[role='textbox'][contenteditable='true'][aria-placeholder*='reel' i]",
        "[role='textbox'][contenteditable='true'][aria-placeholder*='Mô tả' i]",
        "[role='textbox'][contenteditable='true'][aria-placeholder*='Thước phim' i]",
    ):
        try:
            if page.locator(sel).first.is_visible(timeout=timeout_ms):
                return True
        except Exception:
            continue
    if _reel_caption_screen_markers_visible(page, timeout_ms=timeout_ms):
        return True
    if _reel_edit_reel_header_visible(page, timeout_ms=timeout_ms):
        return False
    if _reel_still_on_edit_trim_screen(page, timeout_ms=timeout_ms):
        return False
    for loc in _meta_reel_description_editor_locators(page):
        try:
            if loc.is_visible(timeout=timeout_ms):
                return True
        except Exception:
            continue
    return False


def _reel_description_screen_ready(page: Page, *, timeout_ms: int = 450) -> bool:
    """Alias: màn nhập caption sẵn sàng."""
    return _reel_caption_input_usable(page, timeout_ms=timeout_ms)


def _fill_reel_dashboard_caption(
    page: Page,
    *,
    title: str,
    content: str,
    hashtags: list[str] | str | None,
) -> bool:
    """Nhập caption Reel dashboard — gọi ngay khi thấy ô text sau Next."""
    payload = _build_reel_text_payload(title, content, hashtags)
    if not payload:
        return False
    stage = _reel_strict_prefix("Wizard")
    try:
        _disable_view_only_guard(page)
    except Exception:
        pass

    deadline = time.time() + 8.0
    while time.time() < deadline:
        if _reel_caption_input_usable(page, timeout_ms=450):
            break
        page.wait_for_timeout(280)
    if not _reel_caption_input_usable(page, timeout_ms=600):
        logger.warning("{} Chưa thấy ô caption để nhập.", stage)
        return False

    dialog = _active_reel_dialog(page)
    last_exc: Exception | None = None

    def _try_lexical_fill() -> None:
        if not fill_reel_lexical_description(page, payload):
            raise RuntimeError("lexical fill failed")

    for label, fn in (
        ("lexical", _try_lexical_fill),
        ("fill_meta", lambda: fill_meta_reel_description(page, payload, relock_guard=False)),
        (
            "dialog_input",
            lambda: _input_reel_title_content_and_hashtags(
                dialog, title=title, content=content, hashtags=hashtags
            ),
        ),
    ):
        try:
            fn()
            logger.info("{} Đã nhập caption Reel qua {} ({} ký tự).", stage, label, len(payload))
            _dismiss_reel_hashtag_suggestion(page)
            human_pause(kind="input", label="sau nhập caption dashboard")
            return True
        except Exception as exc:
            last_exc = exc
            logger.debug("{} Nhập caption {} lỗi: {}", stage, label, exc)
    if last_exc:
        logger.warning("{} Không nhập được caption: {}", stage, last_exc)
    return False


def _reel_pre_text_wizard_screen(page: Page) -> bool:
    """Wizard trước màn nhập caption (Edit reel, trim, kiểm tra bản quyền...)."""
    markers = (
        "Edit reel",
        "Chỉnh sửa Thước phim",
        "Trim video",
        "Cắt video",
        "Checking for copyrighted content",
        "Đang kiểm tra bản quyền",
        "Closed Captions",
        "Audio description",
    )
    for m in markers:
        try:
            if page.get_by_text(m, exact=False).first.is_visible(timeout=280):
                return True
        except Exception:
            continue
    return False


def _meta_reel_next_clickable(page: Page) -> bool:
    """Next/Tiếp đang hiện và không bị disabled."""
    if not _meta_reel_next_any_visible(page):
        return False
    for base in (
        _locator_meta_reel_footer_next_with_cancel(page),
        _locator_meta_reel_next_structural(page),
        _locator_meta_reel_next_role(page),
        page.get_by_role("button", name=re.compile(r"^\s*(Next|Tiếp|Tiếp theo)\s*$", re.I)),
    ):
        try:
            n = min(int(base.count()), 12)
        except Exception:
            continue
        for i in range(n):
            b = base.nth(i)
            try:
                if not b.is_visible(timeout=280):
                    continue
                if (b.get_attribute("aria-disabled") or "").strip().lower() == "true":
                    continue
                if b.get_attribute("disabled") is not None:
                    continue
                return True
            except Exception:
                continue
    return False


def _click_meta_reel_next_best_effort(page: Page) -> bool:
    """Bấm Next — strict, sidebar/page, dialog (màn Edit reel thường dùng nút Next ngoài dialog)."""
    if _click_meta_reel_next_strict(page):
        return True
    pat = re.compile(r"^\s*(Next|Tiếp|Tiếp theo)\s*$", re.I)
    for loc in (
        page.get_by_role("button", name=pat),
        page.locator(
            "xpath=(//*[@role='button' or @tabindex='0'][.//div[normalize-space()='Next'] "
            "or .//div[normalize-space()='Tiếp'] or normalize-space()='Next' or normalize-space()='Tiếp'])[last()]"
        ),
        page.get_by_text(pat),
    ):
        if _click_visible_enabled_button(loc, timeout_ms=1400):
            return True
    try:
        dlg = _active_reel_dialog(page)
        pat2 = re.compile(r"Next|Tiếp|Tiếp theo", re.I)
        if _click_visible_enabled_button(dlg.get_by_role("button", name=pat2), timeout_ms=1200):
            return True
        if _click_visible_enabled_button(dlg.get_by_text(pat2), timeout_ms=1000):
            return True
    except Exception:
        pass
    return False


def _reel_wizard_upload_step_visible(page: Page, *, timeout_ms: int = 320) -> bool:
    """Màn «Create reel» / Add video — chưa tới Reel settings (không coi là đã đăng)."""
    try:
        dlg = page.locator("[role='dialog']").last
        if dlg.count() <= 0 or not dlg.is_visible(timeout=timeout_ms):
            return False
        if _reel_scope_upload_ready(dlg):
            return True
    except Exception:
        pass
    for pat in (r"^\s*Create reel\s*$", r"Add video or drag and drop", r"Thêm video"):
        try:
            if page.get_by_text(re.compile(pat, re.I)).first.is_visible(timeout=timeout_ms):
                if _reel_scope_upload_ready(page.locator("[role='dialog']").last):
                    return True
        except Exception:
            continue
    return False


def _reel_post_submit_strong_signal(page: Page, *, timeout_ms: int = 280) -> bool:
    """Tín hiệu chắc chắn đã gửi Post — không suy ra từ «không thấy nút Post»."""
    try:
        if "published_posts" in str(page.url or "").strip().lower():
            return True
    except Exception:
        pass
    for pat in (
        r"Video\s+post\s+processing",
        r"finishes processing",
        r"Publishing|Đang đăng|Your reel is being",
        r"Posting your reel|Đang đăng thước phim",
        r"Your reel is on its way",
        r"Reel published|reel was published",
        r"more posts you want to publish",
        r"bài viết khác.*muốn đăng",
    ):
        try:
            if page.get_by_text(re.compile(pat, re.I)).first.is_visible(timeout=timeout_ms):
                return True
        except Exception:
            continue
    try:
        busy = page.locator(
            "[role='button'][aria-busy='true'], [role='none'][aria-busy='true']"
        ).filter(has_text=_REEL_POST_LABEL_RE)
        if busy.count() > 0 and busy.first.is_visible(timeout=timeout_ms):
            return True
    except Exception:
        pass
    return False


def _reel_post_likely_submitted(page: Page, *, timeout_ms: int = 600) -> bool:
    """
    FB đã nhận Post nhưng ack nhanh có thể bỏ lỡ (về Content Library, dialog đóng chậm).
    Không True khi vẫn ở màn upload «Create reel» hoặc Reel settings + nút Post.
    """
    if _reel_post_submit_strong_signal(page, timeout_ms=timeout_ms):
        return True
    if _reel_wizard_upload_step_visible(page, timeout_ms=220):
        return False
    try:
        if dismiss_meta_more_posts_prompt_best_effort(
            page, probe_timeout_ms=min(2_500, max(400, timeout_ms))
        ):
            return True
    except Exception:
        pass
    try:
        if dismiss_meta_video_post_processing_modal_best_effort(
            page,
            timeout_ms=min(8_000, max(2_000, timeout_ms)),
            give_up_if_never_seen_ms=600,
        ):
            return True
    except Exception:
        pass
    settings_open = _reel_settings_screen_visible(page, timeout_ms=220)
    post_footer = _reel_footer_post_visible(page, timeout_ms=220)
    if settings_open and post_footer:
        return False
    if not settings_open and not post_footer:
        return True
    if settings_open and not post_footer:
        return True
    return False


def _reel_post_submit_acknowledged(page: Page, *, timeout_ms: int = 8_000) -> bool:
    """Sau khi gọi click Post: processing / wizard đóng và không còn Post footer."""
    deadline = time.time() + max(1.0, timeout_ms / 1000.0)
    while time.time() < deadline:
        if _reel_post_submit_strong_signal(page, timeout_ms=220):
            return True
        if _reel_post_likely_submitted(page, timeout_ms=400):
            return True
        page.wait_for_timeout(280)
    return False


def _click_reel_post_best_effort(page: Page) -> bool:
    """Bấm Post/Publish trong popup Reel. True khi UI xác nhận hoặc có dấu hiệu đã gửi."""
    stage = _reel_strict_prefix("Wizard")
    if _reel_post_submit_strong_signal(page, timeout_ms=350):
        logger.info("{} Post đã được xác nhận trước (đang xử lý / Published).", stage)
        return True
    if _reel_post_likely_submitted(page, timeout_ms=400):
        logger.info("{} Post có vẻ đã gửi trước khi bấm (wizard đã đóng).", stage)
        return True
    _dismiss_reel_hashtag_suggestion(page)
    wait_deadline = time.time() + 24.0
    while time.time() < wait_deadline:
        if _reel_strict_post_button_usable(page, timeout_ms=350):
            break
        if _reel_settings_screen_visible(page, timeout_ms=250):
            break
        page.wait_for_timeout(280)
    dialog = _active_reel_dialog(page)
    try:
        dialog.locator("xpath=.//*[normalize-space()='Save']").last.scroll_into_view_if_needed(timeout=2_000)
    except Exception:
        pass
    ack_ms = max(6_000, _env_int("FB_REEL_POST_ACK_MS", 10_000))
    grace_ms = max(8_000, _env_int("FB_REEL_POST_ACK_GRACE_MS", 14_000))
    settle_ms = max(350, _env_int("FB_REEL_POST_CLICK_SETTLE_MS", 700))
    post_dispatched = False
    for attempt in (1, 2, 3):
        if not _dispatch_reel_post_click(page, dialog, attempt=attempt):
            logger.warning("{} Không gửi được click Post (attempt {}).", stage, attempt)
            page.wait_for_timeout(_reel_inter_click_wait_ms())
            continue
        post_dispatched = True
        page.wait_for_timeout(settle_ms)
        if _reel_post_submit_acknowledged(page, timeout_ms=ack_ms):
            logger.info(
                "{} UI xác nhận đã gửi Post (attempt {}).",
                stage,
                attempt,
            )
            return True
        logger.warning(
            "{} Đã gửi click Post nhưng chưa thấy phản hồi UI ngay (attempt {}).",
            stage,
            attempt,
        )
        page.wait_for_timeout(_reel_inter_click_wait_ms())
    if post_dispatched:
        if _reel_post_submit_acknowledged(page, timeout_ms=grace_ms):
            logger.info("{} UI xác nhận Post sau grace period.", stage)
            return True
        if _reel_post_likely_submitted(page, timeout_ms=grace_ms):
            logger.warning(
                "{} Ack chậm nhưng UI giống đã đăng Reel (grace) — coi thành công.",
                stage,
            )
            return True
    logger.error(
        "{} Post không được xác nhận sau 3 lần thử (và grace {}).",
        stage,
        grace_ms,
    )
    return False


def complete_reel_wizard_fill_next_and_post(
    page: Page,
    *,
    title: str = "",
    content: str = "",
    hashtags: list[str] | str | None = None,
    reel_thumbnail_choice: str | None = None,
    on_step: Callable[[str, str], None] | None = None,
    max_next_clicks: int = 18,
    total_timeout_sec: float = 300.0,
    submit_mode: Literal["post", "share", "auto"] = "auto",
) -> tuple[int, bool, bool]:
    """
    Luồng thống nhất: Next tới ô text → nhập → Next tiếp → submit.

    ``submit_mode``:
    - ``post``: Professional Dashboard (chỉ nút Post strict)
    - ``share``: Meta Business composer legacy (chỉ Share + Done)
    - ``auto``: ưu tiên Post, fallback Share

    Returns:
        (số lần Next, đã nhập caption, đã bấm Post/Share submit)
    """
    stage = _reel_strict_prefix("Wizard")
    payload = _build_reel_text_payload(title, content, hashtags)
    thumb_mode = normalize_reel_thumbnail_choice(reel_thumbnail_choice)
    thumb_done = False
    filled = False
    post_clicked = False
    next_clicks = 0
    deadline = time.time() + max(60.0, float(total_timeout_sec))
    mode = submit_mode or "auto"

    try:
        _disable_view_only_guard(page)
    except Exception:
        pass

    def _fire(step_key: str, message: str) -> None:
        logger.info("{} [REEL WIZARD] {} — {}", stage, step_key, message)
        if on_step is not None:
            try:
                on_step(step_key, message)
            except Exception:
                pass

    def _try_submit() -> bool:
        nonlocal filled, post_clicked
        action = _resolve_reel_submit_action(
            page,
            payload=payload,
            filled=filled,
            next_clicks=next_clicks,
            submit_mode=mode,
        )
        if action == "post":
            if payload and not filled and _reel_caption_input_usable(page, timeout_ms=500):
                _fire("FILL_CAPTION", "Có Post — nhập caption trước khi đăng.")
                if _fill_reel_dashboard_caption(page, title=title, content=content, hashtags=hashtags):
                    filled = True
            if _click_reel_post_best_effort(page):
                post_clicked = True
                _fire("CLICK_POST", "Reel settings — đã bấm Post.")
                logger.info("{} Hoàn tất (Post): Next×{} filled={}.", stage, next_clicks, filled)
                return True
            if _reel_post_likely_submitted(page, timeout_ms=5_000):
                post_clicked = True
                _fire("CLICK_POST", "Reel settings — Post có thể đã gửi (ack chậm).")
                logger.warning(
                    "{} Post có dấu hiệu đã gửi dù ack click chưa kịp — coi thành công.",
                    stage,
                )
                return True
            logger.error("{} Bấm Post thất bại.", stage)
            return False
        if action == "share":
            if payload and not filled and _reel_caption_input_usable(page, timeout_ms=500):
                _fire("FILL_CAPTION", "Có Share — nhập caption trước khi đăng.")
                if _fill_reel_dashboard_caption(page, title=title, content=content, hashtags=hashtags):
                    filled = True
            _fire("CLICK_SHARE", "Meta Business composer — bấm Share.")
            share_ok = False
            for attempt in (1, 2):
                if _click_reel_share_best_effort(page):
                    share_ok = True
                    logger.info("{} Đã bấm Share (attempt {}).", stage, attempt)
                    break
                page.wait_for_timeout(_reel_inter_click_wait_ms())
            if not share_ok:
                return False
            _finish_reel_after_share_submit(page)
            logger.info("{} Hoàn tất (Share): Next×{} filled={}.", stage, next_clicks, filled)
            return True
        return False

    logger.info("{} Bắt đầu wizard (submit_mode={}): payload={} ký tự.", stage, mode, len(payload))
    _mute_browser_video_previews_after_attach(page, attempts=3)

    while time.time() < deadline:
        _mute_browser_video_previews(page, silent=True)
        if _try_submit():
            return next_clicks, filled, post_clicked

        if payload and not filled and _reel_caption_input_usable(page, timeout_ms=550):
            _fire("FILL_CAPTION", "Thấy ô nhập — đang nhập caption.")
            if _fill_reel_dashboard_caption(page, title=title, content=content, hashtags=hashtags):
                filled = True
                logger.info("{} Caption đã nhập ({} ký tự).", stage, len(payload))
                _dismiss_reel_hashtag_suggestion(page)
                if _try_submit():
                    return next_clicks, filled, post_clicked
            else:
                logger.warning("{} Nhập caption thất bại — thử lại.", stage)
            page.wait_for_timeout(random.randint(500, 1100))
            continue

        if filled and _reel_settings_screen_visible(page, timeout_ms=400):
            _dismiss_reel_hashtag_suggestion(page)
            if _reel_strict_post_button_usable(page, timeout_ms=500) and _try_submit():
                return next_clicks, filled, post_clicked

        if thumb_mode == REEL_THUMBNAIL_METHOD1_FIRST_AUTO and not thumb_done:
            if _choose_first_reel_thumbnail_method1_best_effort(page):
                thumb_done = True
                page.wait_for_timeout(random.randint(400, 900))

        if next_clicks < max_next_clicks and _reel_wizard_needs_next(
            page, payload=payload, filled=filled, next_clicks=next_clicks
        ):
            if _meta_reel_next_clickable(page) or _meta_reel_next_any_visible(page):
                before = _reel_active_step_label(page)
                _fire("CLICK_NEXT", f"Bấm Next (lần {next_clicks + 1}) — tới ô text/submit.")
                if _try_click_reel_wizard_next(page):
                    next_clicks += 1
                    _wait_after_reel_next_click(page, prev_label=before)
                    continue
                logger.warning("{} Next visible nhưng click thất bại (lần {}).", stage, next_clicks + 1)

        if _reel_wizard_processing(page, timeout_ms=350):
            page.wait_for_timeout(random.randint(900, 1800))
            continue

        if _reel_strict_post_button_usable(page, timeout_ms=450) and _try_submit():
            return next_clicks, filled, post_clicked

        if next_clicks >= max_next_clicks:
            page.wait_for_timeout(500)
            continue

        page.wait_for_timeout(450)

    if payload and not filled and _reel_caption_input_usable(page, timeout_ms=800):
        filled = _fill_reel_dashboard_caption(page, title=title, content=content, hashtags=hashtags)

    if _try_submit():
        return next_clicks, filled, post_clicked

    if _reel_post_likely_submitted(page, timeout_ms=4_000) or _reel_post_submit_strong_signal(
        page, timeout_ms=2_000
    ):
        post_clicked = True
        logger.warning(
            "{} Wizard timeout nhưng có dấu hiệu Reel đã đăng — coi thành công (Next×{}).",
            stage,
            next_clicks,
        )
        return next_clicks, filled, post_clicked

    _failure_screenshot(page, "reel_wizard_fill_next_post_timeout")
    if payload and not filled:
        raise PlaywrightTimeoutError(
            f"Sau {next_clicks} lần Next vẫn chưa nhập được caption (có payload)."
        )
    submit_label = "Post" if mode == "post" else "Share" if mode == "share" else "Post/Share"
    raise PlaywrightTimeoutError(
        f"Sau {next_clicks} lần Next không thấy {submit_label}. Xem logs/screenshots."
    )


def advance_reel_wizard_until_description_input(
    page: Page,
    *,
    fill_fn: Callable[[], bool] | None = None,
    max_next_clicks: int = 10,
    total_timeout_sec: float = 180.0,
    wait_after_click_sec: float = 18.0,
) -> tuple[int, bool]:
    """
    Bấm Next lặp đến ô caption; nếu ``fill_fn`` có — nhập ngay khi thấy ô text.

    Returns:
        (số lần Next đã bấm, đã nhập caption thành công hay chưa)
    """
    stage = _reel_strict_prefix("Wizard")
    deadline = time.time() + max(30.0, float(total_timeout_sec))
    clicks = 0
    filled = False

    def _try_fill_now() -> bool:
        nonlocal filled
        if fill_fn is None:
            return False
        if not _reel_caption_input_usable(page, timeout_ms=500):
            return False
        if fill_fn():
            filled = True
            logger.info("{} Đã nhập caption ngay sau Next #{}.", stage, clicks)
            return True
        return False

    while time.time() < deadline:
        if _reel_caption_input_usable(page, timeout_ms=550):
            _try_fill_now()
            logger.info("{} Ô caption sau {} lần Next (filled={}).", stage, clicks, filled)
            return clicks, filled
        if _reel_post_button_maybe_visible(page, timeout_ms=450):
            logger.info("{} Thấy Post sau {} lần Next.", stage, clicks)
            return clicks, filled

        if not _meta_reel_next_clickable(page):
            if _reel_wizard_processing(page, timeout_ms=400):
                page.wait_for_timeout(random.randint(700, 1400))
                continue
            page.wait_for_timeout(500)
            if not _meta_reel_next_clickable(page) and not _meta_reel_next_any_visible(page):
                break

        if clicks >= max_next_clicks:
            break

        before = _reel_active_step_label(page)
        if not _try_click_reel_wizard_next(page):
            page.wait_for_timeout(600)
            continue

        clicks += 1
        _wait_after_reel_next_click(page, prev_label=before)

    if _reel_caption_input_usable(page, timeout_ms=900):
        _try_fill_now()
        return clicks, filled
    if _reel_post_button_maybe_visible(page, timeout_ms=700):
        return clicks, filled

    _failure_screenshot(page, "reel_textbox_not_visible_after_next_loop")
    hint = "Edit reel / copyright" if _reel_still_on_edit_trim_screen(page) else "wizard"
    raise PlaywrightTimeoutError(
        f"Đã bấm Next {clicks} lần nhưng chưa thấy ô nhập nội dung ({hint}). "
        "Xem logs/screenshots — có thể cần chờ hết «Checking for copyrighted content»."
    )


def _wait_reel_description_screen(
    page: Page,
    *,
    max_extra_next_clicks: int = 8,
    wait_per_step_sec: float = 18.0,
) -> None:
    """Alias: bấm Next lặp đến khi có ô nhập."""
    advance_reel_wizard_until_description_input(
        page,
        max_next_clicks=max(4, int(max_extra_next_clicks)),
        wait_after_click_sec=max(6.0, float(wait_per_step_sec)),
    )


def _env_reel_pause_after_post() -> bool:
    raw = str(os.environ.get("FB_REEL_PAUSE_AFTER_POST", "")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _reel_post_pending_actions_visible(page: Page, dialog: Locator | None = None) -> bool:
    """Còn action phải xử lý sau Post (Done/Post/modal) thì chưa được coi là success."""
    try:
        done_btn = page.get_by_role("button", name=re.compile(r"^Done$|^Xong$", re.I)).first
        if done_btn.is_visible(timeout=180):
            return True
    except Exception:
        pass
    try:
        post_btn = page.get_by_role("button", name=re.compile(r"Post|Đăng|Publish", re.I)).first
        if post_btn.is_visible(timeout=180):
            return True
    except Exception:
        pass
    # Modal Reel/Share vẫn còn mở -> chưa xong.
    try:
        if dialog is not None and dialog.count() > 0 and dialog.first.is_visible(timeout=180):
            return True
    except Exception:
        pass
    try:
        if page.get_by_text(re.compile(r"Share to groups|Create reel|Edit reel", re.I)).first.is_visible(timeout=180):
            return True
    except Exception:
        pass
    return False


def post_reel_via_page_dashboard(
    page: Page,
    *,
    page_url: str,
    page_display_name: str = "",
    video_path: Path,
    title: str = "",
    content: str = "",
    hashtags: list[str] | str | None = None,
    reel_thumbnail_choice: str | None = None,
    on_step: Callable[[str, str], None] | None = None,
) -> None:
    """
    Luồng Reel mới theo Page + Account:
    page_url -> Switch Now -> Professional Dashboard Content Library -> Create -> Reel -> Upload -> Next -> Post.
    """
    stage = _reel_strict_prefix("Wizard")
    current_step = "INIT"
    _ordered_steps = (
        "OPEN_PAGE_CONTEXT",
        "OPEN_CONTENT_LIBRARY",
        "CLICK_CREATE",
        "SELECT_REEL",
        "WAIT_REEL_POPUP",
        "UPLOAD_VIDEO",
        "DETECT_UI_WAY",
        "WIZARD_FILL_NEXT_POST",
        "VERIFY_POST_SUBMITTED",
        "MARK_SUCCESS",
    )

    def _step(step_key: str, message: str) -> None:
        nonlocal current_step
        current_step = step_key
        try:
            idx = _ordered_steps.index(step_key) + 1
            prog = f"{idx:02d}/{len(_ordered_steps):02d}"
        except ValueError:
            prog = "--/--"
        logger.info("{} [REEL FLOW {}] {} - {}", stage, prog, step_key, message)
        if on_step is not None:
            try:
                on_step(step_key, message)
            except Exception:
                pass

    def _step_pause(min_ms: int = 900, max_ms: int = 1800, *, label: str = "") -> None:
        lo = max(120, int(min_ms))
        hi = max(lo, int(max_ms))
        wait_ms = random.randint(lo, hi)
        if label:
            logger.info("{} [REEL FLOW DELAY] {} ms | {}", stage, wait_ms, label)
        page.wait_for_timeout(wait_ms)
    purl = str(page_url or "").strip()
    if not purl:
        raise ValueError("Thiếu page_url cho luồng Reel dashboard.")
    if not video_path.is_file():
        raise FileNotFoundError(f"video_path không tồn tại: {video_path}")

    _step(
        "OPEN_PAGE_CONTEXT",
        f"Mở Page, switch đúng vai trò (cuộn danh sách nếu cần): {purl!r}",
    )
    _ensure_reel_dashboard_page_context(
        page,
        page_url=purl,
        page_display_name=str(page_display_name or "").strip(),
    )
    try:
        page.mouse.wheel(0, 420)
    except Exception:
        pass
    _step_pause(900, 1800, label="sau OPEN_PAGE_CONTEXT")

    _require_page_role_switched(
        page,
        page_display_name=str(page_display_name or "").strip(),
        page_url=purl,
        context="reel_before_dashboard",
    )

    _step("OPEN_CONTENT_LIBRARY", "Mở Professional Dashboard Content Library.")
    dash_url = "https://www.facebook.com/professional_dashboard/content/content_library/"
    page.goto(dash_url, wait_until="domcontentloaded")
    page.wait_for_timeout(4000)
    _step_pause(1200, 2400, label="sau OPEN_CONTENT_LIBRARY")
    cur = str(page.url or "").lower()
    if "professional_dashboard" not in cur:
        _failure_screenshot(page, "reel_dashboard_not_reachable")
        raise RuntimeError("Không vào được Professional Dashboard Content Library (có thể chưa switch đúng quyền Page).")

    _step("CLICK_CREATE", "Bấm Create a post (sidebar) hoặc + Create (vùng bài đăng).")
    open_via = _open_reel_composer_from_content_library(page)
    if not open_via:
        _failure_screenshot(page, "reel_create_not_found")
        raise PlaywrightTimeoutError(
            "Không mở được Reel từ Content Library (Create a post / + Create → Reel). "
            "Kiểm tra quyền Page và ngôn ngữ UI (English/Việt)."
        )
    logger.info("{} Mở Reel composer qua: {}.", stage, open_via)
    _step_pause(900, 1900, label="sau CLICK_CREATE")

    _step("SELECT_REEL", "Đã chọn Reel trong menu (hoặc composer mở sẵn).")
    _step_pause(1400, 2600, label="sau SELECT_REEL")
    _step("WAIT_REEL_POPUP", "Chờ popup Reel xuất hiện.")
    dialog = page.locator("[role='dialog']").last
    dialog.wait_for(state="visible", timeout=25_000)
    # Sau khi popup xuất hiện, chờ state ổn định.
    # Lưu ý: UI Create reel thường CHƯA có Next trước khi upload, và input[type=file]
    # có thể ẩn. Vì vậy cần chấp nhận tín hiệu "Add video"/"Upload" là ready.
    wait_deadline = time.time() + 25.0
    ready = False
    while time.time() < wait_deadline:
        try:
            fi = dialog.locator("input[type='file']")
            has_file = fi.count() > 0 and fi.first.is_visible(timeout=250)
        except Exception:
            has_file = False
        try:
            has_next = dialog.get_by_role("button", name=re.compile(r"Next|Tiếp|Tiếp theo", re.I)).first.is_visible(timeout=250)
        except Exception:
            has_next = False
        try:
            has_add_video = dialog.get_by_text(re.compile(r"Add video|or drag and drop", re.I)).first.is_visible(timeout=250)
        except Exception:
            has_add_video = False
        try:
            has_upload = dialog.get_by_role("button", name=re.compile(r"Upload", re.I)).first.is_visible(timeout=250)
        except Exception:
            has_upload = False
        if has_file or has_next or has_add_video or has_upload:
            ready = True
            break
        page.wait_for_timeout(350)
    if not ready:
        # Không fail cứng ở đây nữa: để bước UPLOAD_VIDEO thử nhiều chiến lược import.
        logger.warning(
            "{} Popup Reel chưa thấy marker ready rõ ràng; tiếp tục thử import video trực tiếp.",
            stage,
        )
    _step_pause(900, 1700, label="sau WAIT_REEL_POPUP")

    _step("UPLOAD_VIDEO", f"Upload video: {video_path}")
    uploaded = _attach_media_automatic(
        page,
        video_path,
        kind="video",
        scope=dialog,
        context="reel_dashboard",
    )
    if not uploaded:
        _failure_screenshot(page, "reel_file_input_missing")
        raise PlaywrightTimeoutError(
            "Không import được video tự động: không set được input[type=file] "
            "và không intercept được filechooser (Add video/Upload). "
            "Không dùng hộp thoại chọn file Windows — kiểm tra quyền Page và đường dẫn video."
        )
    logger.info("{} Đã import video tự động (không popup OS chọn file)", stage)

    page.wait_for_timeout(1800)
    # Chờ upload thực sự được nhận trước khi Next.
    upload_deadline = time.time() + 120.0
    upload_ok = False
    processing_re = re.compile(r"Processing|Uploading|Đang xử lý|đang tải", re.I)
    while time.time() < upload_deadline:
        dialog = _active_reel_dialog(page)
        _mute_browser_video_previews(page, scope=dialog, silent=True)
        try:
            placeholder_vis = dialog.get_by_text(
                re.compile(r"Upload your video in order to see a preview here", re.I)
            ).first.is_visible(timeout=200)
        except Exception:
            placeholder_vis = False
        try:
            still_processing = dialog.get_by_text(processing_re).first.is_visible(timeout=200)
        except Exception:
            still_processing = False
        try:
            next_btn = dialog.get_by_role("button", name=re.compile(r"Next|Tiếp|Tiếp theo", re.I)).first
            next_ready = next_btn.is_visible(timeout=200) and (next_btn.get_attribute("aria-disabled") or "").lower() != "true"
        except Exception:
            next_ready = False
        if ((not placeholder_vis) and not still_processing) or next_ready:
            upload_ok = True
            break
        page.wait_for_timeout(450)
    if not upload_ok:
        _failure_screenshot(page, "reel_upload_not_accepted")
        raise PlaywrightTimeoutError("Đã import video nhưng UI chưa nhận upload (placeholder vẫn còn / Next chưa sẵn sàng).")

    dialog = _active_reel_dialog(page)
    _mute_browser_video_previews_after_attach(page, scope=dialog)
    _step_pause(1200, 2600, label="sau UPLOAD_VIDEO")
    _mute_browser_video_previews_after_attach(page, scope=_active_reel_dialog(page), attempts=3)

    ui_way = detect_meta_reel_ui_way(page)
    if ui_way == "unknown":
        page.wait_for_timeout(1500)
        ui_way = detect_meta_reel_ui_way(page)
    if ui_way == "unknown" and _meta_reel_next_any_visible(page):
        ui_way = "way1"
    if ui_way == "unknown":
        ui_way = "way1"
    way_labels = {
        "way1": "Cách 1 — wizard Next (trim/thumbnail/mô tả)",
        "way2": "Cách 2 — Post details + Publish",
    }
    _step("DETECT_UI_WAY", f"UI Reel: {way_labels.get(ui_way, ui_way)}")
    logger.info("{} [REEL DASHBOARD] detect_meta_reel_ui_way → {}", stage, ui_way)

    thumb_mode = normalize_reel_thumbnail_choice(reel_thumbnail_choice)
    payload_text = _build_reel_text_payload(title, content, hashtags)
    filled = False
    post_clicked = False
    n_next = 0

    def _wizard_on_step(sub_key: str, message: str) -> None:
        _step(sub_key, message)

    if ui_way == "way2":
        _step(
            "WAY2_FILL_PUBLISH",
            "Cách 2: nhập caption (nếu có) → Publish/Post (không wizard Next).",
        )
        if payload_text:
            _wizard_on_step("FILL_CAPTION", "Post details — nhập caption.")
            filled = _fill_reel_dashboard_caption(
                page,
                title=str(title or "").strip(),
                content=str(content or "").strip(),
                hashtags=hashtags,
            )
            if not filled and _reel_caption_input_usable(page, timeout_ms=1200):
                page.wait_for_timeout(800)
                filled = _fill_reel_dashboard_caption(
                    page,
                    title=str(title or "").strip(),
                    content=str(content or "").strip(),
                    hashtags=hashtags,
                )
        if thumb_mode == REEL_THUMBNAIL_METHOD1_FIRST_AUTO:
            _choose_first_reel_thumbnail_method1_best_effort(page)
        _wizard_on_step("CLICK_POST", "Post details — bấm Publish/Post.")
        post_clicked = _click_reel_post_best_effort(page)
        if not post_clicked:
            raise PlaywrightTimeoutError(
                "Không xác nhận được Post (way2): UI vẫn ở Reel settings hoặc nút Post chưa phản hồi."
            )
        logger.info(
            "{} [REEL DASHBOARD] way2 done: filled={} payload_len={} post_clicked={}.",
            stage,
            filled,
            len(payload_text),
            post_clicked,
        )
    else:
        _step(
            "WIZARD_FILL_NEXT_POST",
            f"Luồng thống nhất ({way_labels.get(ui_way, ui_way)}): nhập khi có ô text → Next → Post.",
        )
        try:
            n_next, filled, post_clicked = complete_reel_wizard_fill_next_and_post(
                page,
                title=str(title or "").strip(),
                content=str(content or "").strip(),
                hashtags=hashtags,
                reel_thumbnail_choice=reel_thumbnail_choice,
                on_step=_wizard_on_step,
                max_next_clicks=18,
                total_timeout_sec=300.0,
                submit_mode="post",
            )
            logger.info(
                "{} [REEL DASHBOARD] wizard done: way={} Next×{} filled={} post_clicked={}.",
                stage,
                ui_way,
                n_next,
                filled,
                post_clicked,
            )
        except PlaywrightTimeoutError:
            raise
    if not post_clicked:
        raise PlaywrightTimeoutError(
            "Reel dashboard: wizard kết thúc nhưng chưa bấm được Post/Publish."
        )
    if payload_text and not filled:
        logger.warning(
            "{} Có nội dung job nhưng không nhập được caption — có thể vẫn đăng được nếu Meta không bắt buộc mô tả.",
            stage,
        )
    if _env_reel_pause_after_post():
        _step(
            "VERIFY_POST_SUBMITTED",
            "Đã bấm Post. TẠM DỪNG để bạn kiểm tra (FB_REEL_PAUSE_AFTER_POST=1), browser sẽ không tự đóng.",
        )
        while True:
            page.wait_for_timeout(5000)

    snippet = (payload_text or "").strip()[:200] or None
    delay_ms = max(5000, _env_int("FB_REEL_POST_VERIFY_DELAY_MS", 5000))
    _step(
        "VERIFY_POST_SUBMITTED",
        f"Đã bấm Post — chờ {delay_ms}ms load rồi xác nhận video trên Page.",
    )
    verify_reel_dashboard_post_submitted(
        page,
        post_clicked=True,
        text_snippet=snippet,
        page_url=purl,
        post_verify_delay_ms=delay_ms,
        timeout_ms=120_000,
    )
    _step("MARK_SUCCESS", "Đăng Reel thành công — đã xác nhận Post và video trên Page.")
    return


def _submit_button_is_enabled_js() -> str:
    """JS check: tồn tại ít nhất một [role=button] có text Publish/Post/Đăng/Schedule enable."""
    return """() => {
      const words = ['publish', 'post', 'đăng', 'schedule', 'lên lịch'];
      const nodes = Array.from(document.querySelectorAll("[role='button'], button"));
      for (const el of nodes) {
        const t = (el.textContent || '').trim().toLowerCase();
        if (!t) continue;
        if (!words.some(w => t === w || t.includes(w))) continue;
        const dis = (el.getAttribute('aria-disabled') || '').toLowerCase();
        if (dis === 'true') continue;
        if (el.hasAttribute('disabled')) continue;
        const rect = el.getBoundingClientRect();
        if (rect.width < 4 || rect.height < 4) continue;
        return true;
      }
      return false;
    }"""


def _js_click_submit_button_locator(loc: Locator, *, label: str) -> bool:
    """Gọi ``el.click()`` trực tiếp trên element → bỏ qua mọi overlay (view-only guard)."""
    try:
        loc.evaluate("el => { if (el && typeof el.click === 'function') el.click(); }")
        logger.info("Đã nhấn nút {} (JS click, bypass overlay).", label)
        return True
    except Exception as exc:
        logger.debug("JS click {} lỗi: {}", label, exc)
        return False


def click_post_button(page: Page) -> None:
    """
    Trước khi đăng: cuộn trang ngẫu nhiên, sau đó bấm nút Đăng/Post.

    Chống overlay view-only: ưu tiên dispatch ``el.click()`` qua JS trực tiếp trên element
    (không đi qua hit-testing chuột nên overlay ``__toolfb_view_only_blocker`` không nuốt event).
    Nếu JS click không khả thi mới tạm gỡ overlay để thực hiện Playwright mouse click.

    Raises:
        PlaywrightTimeoutError: Không thấy nút đăng.
    """
    try:
        scroll_randomly(page)
        submit_delay_ms = max(350, _env_int("FB_REEL_PUBLISH_STEP_DELAY_MS", 1200))
        try:
            page.wait_for_timeout(submit_delay_ms)
        except Exception:
            pass
        # Business Composer: nút Publish có thể tồn tại sớm nhưng aria-disabled="true".
        # Chờ tới khi enable trước khi rơi vào fallback selector chung.
        try:
            pub = page.locator(
                "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' "
                "and .//div[normalize-space()='Publish']])[last()]"
            ).first
            if pub.count() == 0:
                pub = page.locator("[role='button']:has-text('Publish')").first
            if pub.count() > 0:
                pub.wait_for(state="visible", timeout=30_000)
                page.wait_for_function(_submit_button_is_enabled_js(), timeout=120_000)
                # 1) Thử JS click trước — bypass overlay view-only guard.
                if _js_click_submit_button_locator(pub, label="Publish"):
                    try:
                        page.wait_for_timeout(submit_delay_ms)
                    except Exception:
                        pass
                    _human_pause()
                    _enable_view_only_guard(page)
                    return
                # 2) Fallback: tạm gỡ overlay rồi click Playwright bình thường.
                guard_on = _view_only_mode_enabled()
                if guard_on:
                    _disable_view_only_guard(page)
                try:
                    pub.click(timeout=12_000, force=True)
                    logger.info("Đã nhấn nút Publish (mouse click, sau khi gỡ overlay).")
                finally:
                    if guard_on:
                        _enable_view_only_guard(page)
                try:
                    page.wait_for_timeout(submit_delay_ms)
                except Exception:
                    pass
                _human_pause()
                return
        except Exception:
            pass
        sel = _wait_first_selector(
            page,
            (
                "div.html-div:has(div[role='none'] span.x6ikm8r)",
                "div.html-div:has(div[role='none'] span.x1j85h84)",
                "xpath=(//div[contains(@class,'html-div')][.//div[@role='none']//span[contains(@class,'x6ikm8r') and normalize-space()='Post']])[last()]",
                "xpath=(//div[contains(@class,'html-div')][.//div[@role='none']//span[contains(@class,'x1j85h84') and normalize-space()='Post']])[last()]",
                "xpath=(//div[@role='none' and .//span[contains(@class,'x6ikm8r') and normalize-space()='Post']])[last()]",
                "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' and .//div[normalize-space()='Publish']])[last()]",
                "[aria-label='Post'][role='button']",
                "[aria-label='Đăng'][role='button']",
                "[role='button'][aria-disabled='false']:has-text('Publish')",
                "button:has-text('Publish')",
                "[role='button']:has-text('Publish')",
                "button:has-text('Đăng')",
                "[role='button']:has-text('Đăng')",
                "button:has-text('Schedule')",
                "[role='button']:has-text('Schedule')",
                "button:has-text('Lên lịch')",
                "[role='button']:has-text('Lên lịch')",
                "div[role='button'][aria-label='Post']",
                "div[role='button'][aria-label='Đăng']",
                "xpath=//div[@role='button' and .//span[normalize-space()='Post']]",
                "xpath=//div[@role='button' and .//span[normalize-space()='Đăng']]",
            ),
            step_timeout_ms=15_000,
            error_label="click_post_button",
        )
        page.wait_for_selector(sel, state="visible", timeout=15_000)
        submit_loc = page.locator(sel).first
        # 1) JS click.
        if _js_click_submit_button_locator(submit_loc, label="Publish/Post/Schedule"):
            try:
                page.wait_for_timeout(submit_delay_ms)
            except Exception:
                pass
            _human_pause()
            _enable_view_only_guard(page)
            return
        # 2) Fallback mouse click sau khi tạm gỡ overlay.
        guard_on = _view_only_mode_enabled()
        if guard_on:
            _disable_view_only_guard(page)
        try:
            submit_loc.click(timeout=15_000, force=True)
            logger.info("Đã nhấn nút Publish/Post/Schedule (mouse click).")
        finally:
            if guard_on:
                _enable_view_only_guard(page)
        try:
            page.wait_for_timeout(submit_delay_ms)
        except Exception:
            pass
        _human_pause()
    except PlaywrightTimeoutError:
        _enable_view_only_guard(page)
        raise
    except Exception as exc:
        _enable_view_only_guard(page)
        _failure_screenshot(page, f"click_post_button: {exc}")
        raise


def dismiss_meta_more_posts_prompt_best_effort(
    page: Page,
    *,
    probe_timeout_ms: int = 6_000,
) -> bool:
    """Đóng popup **«Are there more posts you want to publish?»** xuất hiện sau Publish.

    Popup gợi ý lập lịch bài tiếp theo chặn luồng tự động — nếu thấy, bấm *Maybe later*
    (hoặc ``X`` đóng dialog) để giải phóng UI. Sự xuất hiện của popup này cũng đồng
    nghĩa bài viết đã được Facebook chấp nhận (đăng thành công).

    Args:
        probe_timeout_ms: Tối đa chờ popup xuất hiện (ms). Không thấy → trả về False
            nhanh, không làm chậm luồng verify nếu popup không hiển thị.

    Returns:
        True nếu đã thấy popup và đóng thành công; False nếu không thấy.
    """
    stage = _reel_strict_prefix("Verify")
    deadline = time.time() + max(500, probe_timeout_ms) / 1000.0

    def _popup_visible() -> bool:
        try:
            t = page.get_by_text(
                re.compile(r"more\s+posts\s+you\s+want\s+to\s+publish", re.I)
            )
            if t.count() > 0 and t.first.is_visible(timeout=500):
                return True
        except Exception:
            pass
        try:
            t2 = page.get_by_text(
                re.compile(r"bài viết khác.*muốn (đăng|xuất bản)", re.I)
            )
            if t2.count() > 0 and t2.first.is_visible(timeout=500):
                return True
        except Exception:
            pass
        return False

    def _click_dismiss() -> bool:
        candidates_text = [
            re.compile(r"^\s*Maybe\s*later\s*$", re.I),
            re.compile(r"^\s*Để\s*sau\s*$", re.I),
            re.compile(r"^\s*Not\s*now\s*$", re.I),
        ]
        for pat in candidates_text:
            try:
                btn = page.get_by_role("button", name=pat)
                if btn.count() > 0 and btn.first.is_visible(timeout=600):
                    try:
                        btn.first.evaluate("el => el && el.click && el.click()")
                        logger.info("{} đã đóng popup 'more posts' (Maybe later).", stage)
                        return True
                    except Exception:
                        try:
                            btn.first.click(timeout=3_000, force=True, no_wait_after=True)
                            logger.info(
                                "{} đã đóng popup 'more posts' (Maybe later, force).",
                                stage,
                            )
                            return True
                        except Exception:
                            pass
            except Exception:
                continue
        # Fallback: nút X đóng dialog.
        try:
            close_btn = page.get_by_role("button", name=re.compile(r"^\s*Close\s*$", re.I))
            if close_btn.count() > 0 and close_btn.first.is_visible(timeout=500):
                try:
                    close_btn.first.evaluate("el => el && el.click && el.click()")
                    logger.info("{} đã đóng popup 'more posts' (nút Close).", stage)
                    return True
                except Exception:
                    pass
        except Exception:
            pass
        # Fallback cuối: Escape.
        try:
            page.keyboard.press("Escape")
            logger.info("{} đã đóng popup 'more posts' (Escape).", stage)
            return True
        except Exception:
            return False

    while time.time() < deadline:
        if _popup_visible():
            if _click_dismiss():
                return True
        try:
            page.wait_for_timeout(300)
        except Exception:
            break
    return False


def _meta_published_posts_has_video_row(page: Page, *, timeout_ms: int = 4_000) -> bool:
    """Có ít nhất một hàng bài đăng dạng video/reel trên màn Published (duration hoặc nhãn Reel)."""
    try:
        rows = page.locator("[role='grid'] [role='row'], [role='table'] [role='row']")
        n = min(int(rows.count()), 8)
        for i in range(n):
            try:
                txt = str(rows.nth(i).inner_text(timeout=min(800, timeout_ms)) or "")
            except Exception:
                continue
            if re.search(r"\b\d{1,2}:\d{2}\b", txt):
                return True
            if re.search(r"\b(reel|reels|video|thước phim)\b", txt, re.I):
                return True
    except Exception:
        pass
    for sel in (
        "a[href*='reel']",
        "[aria-label*='Reel' i]",
        "[aria-label*='video' i]",
    ):
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible(timeout=min(1200, timeout_ms)):
                return True
        except Exception:
            continue
    return False


def _navigate_meta_published_posts_best_effort(page: Page, *, page_url: str = "") -> None:
    """Mở tab Published Posts để kiểm tra video/reel vừa đăng."""
    aid = extract_facebook_numeric_id_from_url(str(page_url or "").strip())
    if not aid:
        try:
            aid = extract_facebook_numeric_id_from_url(str(page.url or ""))
        except Exception:
            aid = None
    if not aid:
        return
    target = default_meta_published_posts_url(aid)
    try:
        assert_safe_facebook_navigation_url(target, label="published_posts")
        page.goto(target, wait_until="domcontentloaded", timeout=90_000)
        page.wait_for_timeout(2200)
    except Exception as exc:
        logger.debug("{} Không mở được published_posts: {}", _reel_strict_prefix("Verify"), exc)


def verify_reel_dashboard_post_submitted(
    page: Page,
    *,
    post_clicked: bool,
    text_snippet: str | None = None,
    page_url: str = "",
    post_verify_delay_ms: int | None = None,
    timeout_ms: int = 120_000,
) -> None:
    """
    Sau Reel dashboard: bắt buộc đã bấm Post, chờ load (mặc định 5s), xác nhận video/reel trên Page.

    Raises:
        RuntimeError: Thiếu tín hiệu Post hoặc không thấy video mới — ``need_manual_check``.
    """
    stage = _reel_strict_prefix("Verify")
    if not post_clicked:
        raise RuntimeError(
            "VERIFY_REEL: Chưa xác nhận được đã bấm nút Post trong wizard. need_manual_check"
        )
    delay_ms = max(5000, int(post_verify_delay_ms or _env_int("FB_REEL_POST_VERIFY_DELAY_MS", 5000)))
    logger.info("{} Chờ {} ms sau Post trước khi xác nhận đăng.", stage, delay_ms)
    page.wait_for_timeout(delay_ms)

    try:
        verify_post_submitted(
            page,
            text_snippet=text_snippet,
            timeout_ms=timeout_ms,
            require_submit_signal=True,
            submit_clicked=True,
        )
    except RuntimeError as exc:
        if _reel_post_likely_submitted(page, timeout_ms=3_000) or _reel_post_submit_strong_signal(
            page, timeout_ms=2_000
        ):
            logger.warning(
                "{} verify_post_submitted chặt — nhưng UI giống đã đăng: {}",
                stage,
                exc,
            )
        else:
            raise

    if _meta_published_posts_has_video_row(page, timeout_ms=5_000):
        logger.info("{} Đã thấy video/reel trên màn Published hiện tại.", stage)
        return
    if text_snippet:
        frag = text_snippet.strip().replace("\n", " ")[:160]
        if len(frag) >= 8:
            try:
                if page.get_by_text(frag, exact=False).first.is_visible(timeout=6_000):
                    logger.info("{} Đã thấy caption trên trang sau Post.", stage)
                    return
            except Exception:
                pass

    _navigate_meta_published_posts_best_effort(page, page_url=page_url)
    page.wait_for_timeout(max(3000, delay_ms // 2))
    if _meta_published_posts_has_video_row(page, timeout_ms=8_000):
        logger.info("{} Đã thấy video/reel trong danh sách Published của Page.", stage)
        return
    if text_snippet:
        frag = text_snippet.strip().replace("\n", " ")[:160]
        if len(frag) >= 8:
            try:
                if page.get_by_text(frag, exact=False).first.is_visible(timeout=8_000):
                    logger.info("{} Đã thấy caption trong danh sách Published.", stage)
                    return
            except Exception:
                pass

    if _reel_post_likely_submitted(page, timeout_ms=2_500) or _reel_post_submit_strong_signal(
        page, timeout_ms=1_500
    ):
        logger.warning(
            "{} Không thấy video trên Published ngay — nhưng Post có vẻ đã gửi (processing/đóng wizard).",
            stage,
        )
        return

    _failure_screenshot(page, "reel_verify_no_video_on_page")
    raise RuntimeError(
        "VERIFY_REEL: Đã bấm Post nhưng không thấy video/reel mới trên Page (Published). need_manual_check"
    )


def verify_post_submitted(
    page: Page,
    *,
    text_snippet: str | None = None,
    timeout_ms: int = 120_000,
    require_submit_signal: bool = False,
    submit_clicked: bool | None = None,
) -> None:
    """
    Sau khi bấm Post: chờ composer đóng (nút Post ẩn) hoặc thấy đoạn nội dung trên feed.

    Reel / video: nếu có dialog **Video post processing** + **Done**, đóng dialog và coi như đã gửi xong bước đăng.

    Raises:
        RuntimeError: Không xác nhận được; thông điệp chứa ``need_manual_check`` để scheduler ghi trạng thái.
    """
    if require_submit_signal and not bool(submit_clicked):
        raise RuntimeError(
            "VERIFY_POST: Chưa có tín hiệu đã bấm submit (Share/Publish/Post/Schedule). need_manual_check"
        )

    def _page_is_closed() -> bool:
        try:
            if hasattr(page, "is_closed") and page.is_closed():
                return True
        except Exception:
            pass
        try:
            _ = page.url
        except Exception as exc:
            if "closed" in str(exc).lower():
                return True
        return False

    if bool(submit_clicked) and _page_is_closed():
        logger.info(
            "{} verify_post_submitted: page đã đóng sau Share (submit_clicked=True) => coi như đăng thành công.",
            _reel_strict_prefix("Verify"),
        )
        return

    # Sau Publish: FB có thể mở popup gợi ý "Are there more posts you want to publish?".
    # Popup này chặn UI → phải đóng (Maybe later) để job tiếp theo chạy được.
    # Popup xuất hiện đồng nghĩa bài viết đã được chấp nhận => coi như đăng thành công.
    try:
        if dismiss_meta_more_posts_prompt_best_effort(page, probe_timeout_ms=6_000):
            logger.info(
                "{} verify_post_submitted: popup 'more posts you want to publish' đã đóng => coi như đăng thành công.",
                _reel_strict_prefix("Verify"),
            )
            return
    except Exception as _exc_mp:
        logger.debug(
            "{} verify_post_submitted: dismiss_more_posts lỗi (bỏ qua): {}",
            _reel_strict_prefix("Verify"),
            _exc_mp,
        )

    if require_submit_signal:
        try:
            cur_step = _reel_active_step_label(page)
            if cur_step == "create" and _meta_reel_next_any_visible(page):
                if _reel_wizard_upload_step_visible(page, timeout_ms=400):
                    if not (
                        _reel_post_likely_submitted(page, timeout_ms=600)
                        or _reel_post_submit_strong_signal(page, timeout_ms=400)
                    ):
                        raise RuntimeError(
                            "VERIFY_POST: Vẫn còn ở bước Create/upload Reel, chưa qua submit cuối. need_manual_check"
                        )
        except RuntimeError:
            raise
        except Exception:
            pass

    def _is_meta_published_posts_screen() -> bool:
        """
        Meta Business Suite đôi khi tự redirect về trang Content > Posts & reels (Published)
        ngay sau khi Share thành công.
        """
        try:
            u = str(page.url or "").strip().lower()
        except Exception:
            u = ""
        if "business.facebook.com" not in u:
            return False
        if "/latest/posts/published_posts" not in u and "published_posts" not in u:
            return False
        try:
            has_content_header = page.get_by_text("Posts & reels", exact=False).first.is_visible(timeout=1_200)
        except Exception:
            has_content_header = False
        try:
            has_published_tab = page.get_by_text("Published", exact=False).first.is_visible(timeout=1_200)
        except Exception:
            has_published_tab = False
        return bool(has_content_header or has_published_tab)

    if _is_meta_published_posts_screen():
        logger.info(
            "{} verify_post_submitted: đã về màn Content > Posts & reels (Published) => coi như đăng thành công.",
            _reel_strict_prefix("Verify"),
        )
        return

    if dismiss_meta_video_post_processing_modal_best_effort(
        page,
        timeout_ms=min(28_000, timeout_ms),
        give_up_if_never_seen_ms=4_000,
    ):
        logger.info("{} verify_post_submitted: đã xử lý dialog Video post processing (Done).", _reel_strict_prefix("Verify"))
        return
    post_loc = page.locator(
        "[aria-label='Post'][role='button'], [aria-label='Đăng'][role='button'], "
        "div[role='button'][aria-label='Post'], div[role='button'][aria-label='Đăng'], "
        "button:has-text('Publish'), [role='button']:has-text('Publish'), "
        "button:has-text('Schedule'), [role='button']:has-text('Schedule'), "
        "button:has-text('Lên lịch'), [role='button']:has-text('Lên lịch')"
    )
    try:
        if post_loc.count() == 0:
            logger.info("{} verify_post_submitted: không còn nút Post (DOM).", _reel_strict_prefix("Verify"))
            return
        post_loc.first.wait_for(state="hidden", timeout=timeout_ms)
        logger.info("{} verify_post_submitted: nút Post trong composer đã ẩn.", _reel_strict_prefix("Verify"))
        return
    except PlaywrightTimeoutError:
        logger.warning(
            "{} verify_post_submitted: nút Post vẫn hiện sau {} ms — thử dialog processing / snippet.",
            _reel_strict_prefix("Verify"),
            timeout_ms,
        )
        try:
            if dismiss_meta_more_posts_prompt_best_effort(page, probe_timeout_ms=4_000):
                logger.info(
                    "{} verify_post_submitted: popup 'more posts' xuất hiện sau timeout => coi như đăng thành công.",
                    _reel_strict_prefix("Verify"),
                )
                return
        except Exception:
            pass
        if _is_meta_published_posts_screen():
            logger.info(
                "{} verify_post_submitted: redirect sang published_posts sau timeout => coi như đăng thành công.",
                _reel_strict_prefix("Verify"),
            )
            return
        if dismiss_meta_video_post_processing_modal_best_effort(
            page,
            timeout_ms=min(75_000, max(20_000, timeout_ms // 2)),
            give_up_if_never_seen_ms=12_000,
        ):
            logger.info("{} verify_post_submitted: đã Done sau timeout composer (processing).", _reel_strict_prefix("Verify"))
            return

    if text_snippet:
        frag = text_snippet.strip().replace("\n", " ")
        if len(frag) >= 10:
            short = frag[:160]
            try:
                if page.get_by_text(short, exact=False).first.is_visible(timeout=12_000):
                    logger.info("{} verify_post_submitted: thấy snippet trên trang.", _reel_strict_prefix("Verify"))
                    return
            except Exception:
                pass

    if _is_meta_published_posts_screen():
        logger.info("{} verify_post_submitted: xác nhận thành công qua màn danh sách bài đã đăng.", _reel_strict_prefix("Verify"))
        return

    raise RuntimeError(
        "VERIFY_POST: Không xác nhận được bài đã đăng (composer có thể vẫn mở). need_manual_check"
    )
