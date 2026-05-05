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

_REEL_STRICT_JOB_ID: ContextVar[str] = ContextVar("_REEL_STRICT_JOB_ID", default="")


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
    page.goto(u, wait_until="domcontentloaded", timeout=90_000)
    _force_www_facebook_if_mobile_redirect(page)


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
              if (!ep || ep === '/' || ep === '/home' || ep === '/home.php') return true;
              if (curPath === ep || curPath.startsWith(ep + '/')) return true;
              if (expectSlug && expectSlug.length > 2 && href.includes(expectSlug.toLowerCase())) return true;
              return false;
            }""",
            arg={"expectPath": path, "expectSlug": slug},
            timeout=timeout_ms,
        )
    except PlaywrightTimeoutError:
        logger.warning(
            "[FB] Chờ path slug chưa khớp (expect path={!r} slug={!r}) — url hiện tại: {}",
            path,
            slug,
            page.url,
        )
        raise


def _human_pause() -> None:
    """
    Tạm dừng ngẫu nhiên 1–3 giây giữa các thao tác (hành vi giống người dùng).
    """
    delay = random.uniform(1.0, 3.0)
    time.sleep(delay)


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
    Trả về độ trễ gõ phím (ms) ngẫu nhiên trong khoảng 100–300.

    Returns:
        Số nguyên milliseconds.
    """
    return random.randint(100, 300)


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
            logger.info("[FB] URL checkpoint/2FA — chưa có phiên hợp lệ cho đăng bài tự động.")
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
            logger.info("[FB] URL login/checkpoint — phiên không dùng được cho đăng bài tự động.")
            return False
        loc = page.locator("input[name='pass'], input#pass, form[method='post'] input[type='password']")
        if loc.first.is_visible(timeout=1_200):
            logger.info("[FB] Thấy ô mật khẩu đăng nhập — coi như chưa đăng nhập.")
            return False
        # UI mới / Business Suite: đôi khi không có [role=navigation] trên www nhưng cookie phiên vẫn hợp lệ.
        if has_c_user and "xs" in names and "facebook.com" in u:
            if "/login" not in u and "/checkpoint" not in u and "two_step" not in u:
                logger.info("[FB] Cookie c_user+xs, không form đăng nhập — coi như đã đăng nhập (bỏ qua chờ DOM feed).")
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


def ensure_facebook_session_for_post(page: Page, cookie_path: str | Path | None) -> None:
    """
    Ưu tiên phiên đã có trong profile; chỉ nạp cookie khi mất phiên và ``FB_ALLOW_COOKIE_RESTORE=1`` (mặc định).

    Raises:
        RuntimeError: Không thể tiếp tục (checkpoint, cookie hết hạn, v.v.) — nên đánh dấu need_manual_check.
        FileNotFoundError: Cần cookie nhưng file không có.
    """
    _human_pause()
    logger.info("[FB] ensure_session: url={!r}", page.url)
    if _facebook_url_is_security_interstitial(page.url or ""):
        raise RuntimeError(
            "FACEBOOK_2FA_OR_CHECKPOINT: Facebook đang yêu cầu xác minh bảo mật (2 lớp / checkpoint / đăng nhập lại). "
            "Mở đúng profile trong trình duyệt, hoàn tất bước Meta hiển thị, rồi chạy lại job hoặc «Lấy cookie (Playwright)». "
            "Chrome/Chromium dễ bị hơn Firefox khi cookie hoặc máy thay đổi. need_manual_check"
        )
    if facebook_session_appears_logged_in(page):
        logger.info("[FB] Profile vẫn đăng nhập — bỏ qua bước nạp cookie.")
        return
    # Có thể vừa load dở hoặc bị chuyển host, thử refresh 1 lần trước khi đè cookie.
    try:
        page.reload(wait_until="domcontentloaded", timeout=45_000)
        _force_www_facebook_if_mobile_redirect(page)
        if _facebook_url_is_security_interstitial(page.url or ""):
            raise RuntimeError(
                "FACEBOOK_2FA_OR_CHECKPOINT: Sau refresh vẫn ở trang xác minh Meta (2FA/checkpoint). "
                "Xử lý tay trên profile rồi chạy lại. need_manual_check"
            )
        if facebook_session_appears_logged_in(page):
            logger.info("[FB] Sau refresh, profile đã đăng nhập — bỏ qua nạp cookie.")
            return
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
    if _facebook_url_is_security_interstitial(page.url or ""):
        raise RuntimeError(
            "FACEBOOK_2FA_OR_CHECKPOINT: Sau khi nạp cookie Facebook vẫn yêu cầu xác minh (2FA/checkpoint). "
            "Cookie không đủ để bỏ qua bước này — đăng nhập/ xác minh tay trong profile, rồi «Lấy cookie» lại. need_manual_check"
        )
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
    if not facebook_session_appears_logged_in(page):
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
            link.click(timeout=8000, force=True)
            page.wait_for_timeout(2800)
            if _is_on_target_surface(page, dest):
                return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("[FB] Fallback tên Page (link): {}", exc)
    try:
        btn = page.get_by_role("button", name=re.compile(re.escape(pn), re.I)).first
        if btn.is_visible(timeout=1500):
            btn.click(timeout=6000, force=True)
            page.wait_for_timeout(2800)
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
        navigate_to_url(page, dest)
        logger.info("[FB] Sau navigate_to_url: url_now={}", page.url)
        _ensure_switched_into_page_if_needed(page)
        # Có trường hợp bấm Switch Now xong bị về trang trung gian; ép quay lại URL đích.
        if dest and not _is_on_target_surface(page, dest):
            logger.warning("Chưa đứng đúng page đích sau lần 1, điều hướng lại target_url.")
            navigate_to_url(page, dest)
            _ensure_switched_into_page_if_needed(page)
        if dest and not _is_on_target_surface(page, dest):
            pname = (page_display_name or "").strip()
            if pname and _try_navigate_via_page_name_link(page, pname, dest):
                logger.info("[FB] Fallback UI: vào Page qua tên hiển thị {!r}.", pname)
        if dest and not _is_on_target_surface(page, dest):
            _failure_screenshot(page, f"go_to_posting_target: chưa vào đúng page đích {dest}")
            raise PlaywrightTimeoutError(f"Chưa vào đúng page đích: {dest}")
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
            _ensure_switched_into_page_if_needed(page)
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


def _set_file_via_existing_input(page: Page, file_path: Path, *, kind: Literal["image", "video"]) -> bool:
    """
    Thử set file trực tiếp vào input[type=file] đã có sẵn (không mở native picker).

    Meta đôi khi để ``accept`` rỗng hoặc MIME lạ — job sau cùng profile cần thử lỏng hơn.
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


def _collect_add_media_button_locators(page: Page) -> list[Locator]:
    """
    Trả danh sách các locator có khả năng mở filechooser cho media (khởi tạo + "Add more").

    Chiến lược: rộng nhưng không bừa — chỉ lấy element ``button`` / ``[role='button']``
    có text hoặc aria-label chứa từ khóa media. Dùng khi cần click từng cái trong
    ``expect_file_chooser`` để bảo đảm **native dialog không bao giờ thoát ra ngoài**.

    Duplicates được loại bỏ dựa trên bounding-box.
    """
    out: list[Locator] = []
    seen_keys: set[str] = set()

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
    name_rx = re.compile(r"(add\s+photo|add\s+photos|photo\s*/\s*video|add\s+media|"
                         r"thêm\s+ảnh|ảnh\s*/\s*video|add\s+more|thêm\s+nữa|add\s+video)", re.I)
    try:
        _push(page.get_by_role("button", name=name_rx))
    except Exception:
        pass

    # 2) [role=button] với aria-label chứa từ khóa media
    for aria_rx in (r"photo", r"video", r"media", r"image", r"ảnh"):
        try:
            _push(page.locator(f"[role='button'][aria-label*='{aria_rx}' i]"))
        except Exception:
            continue
        try:
            _push(page.locator(f"button[aria-label*='{aria_rx}' i]"))
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
    )
    for css in text_selectors:
        try:
            _push(page.locator(css))
        except Exception:
            continue

    return out


def _set_file_via_business_add_button(page: Page, file_path: Path, *, kind: Literal["image", "video"]) -> bool:
    """
    Bấm candidate "Add media" trong context ``expect_file_chooser`` để set file tự động.

    An toàn với native dialog: **mọi** click đều nằm trong ``expect_file_chooser``, nếu UI
    phát native file dialog Playwright sẽ chặn và gọi ``set_files`` qua event — user KHÔNG
    thấy popup OS. Nếu click không phát filechooser mà render ``input[type=file]`` mới,
    hàm re-scan bằng ``_set_file_via_existing_input``.
    """
    for attempt in range(1, 4):
        # Thử input[type=file] hiện có trước mỗi vòng — đôi khi Meta render lại DOM.
        if _set_file_via_existing_input(page, file_path, kind=kind):
            logger.info(
                "[FB] Đã set file qua input[type=file] re-scan (attempt={}): {}",
                attempt,
                file_path,
            )
            return True

        candidates = _collect_add_media_button_locators(page)
        if not candidates:
            page.wait_for_timeout(400)
            continue
        for cand in candidates:
            # Luôn bọc click trong expect_file_chooser để native dialog không thoát ra OS.
            try:
                with page.expect_file_chooser(timeout=3_500) as fc_info:
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
                # Không phát filechooser → có thể chỉ render input mới; re-scan ngay.
                try:
                    page.wait_for_timeout(350)
                except Exception:
                    pass
                if _set_file_via_existing_input(page, file_path, kind=kind):
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
    """
    try:
        cur = page.url or ""
        if not cur or not target_url:
            return False
        c = urlparse(cur)
        t = urlparse(_fb_normalize_client_url(target_url))
        if c.netloc and t.netloc and _fb_host_key(c.netloc) != _fb_host_key(t.netloc):
            return False
        cpath = (c.path or "/").rstrip("/").lower()
        tpath = (t.path or "/").rstrip("/").lower()
        if not tpath or tpath == "/":
            return True
        # Cho phép current path bắt đầu bằng target path (vd /page/posts).
        return cpath == tpath or cpath.startswith(tpath + "/")
    except Exception:
        return False


def _ensure_switched_into_page_if_needed(page: Page) -> None:
    """
    Nếu Facebook hiển thị banner yêu cầu switch sang Page thì bấm "Switch Now".
    """
    try:
        switch_btn_selectors = (
            "button:has-text('Switch Now')",
            "[role='button']:has-text('Switch Now')",
            "a:has-text('Switch Now')",
            "button:has-text('Chuyển ngay')",
            "[role='button']:has-text('Chuyển ngay')",
            "a:has-text('Chuyển ngay')",
        )
        hint_selectors = (
            "text=Switch into",
            "text=to start managing it",
            "text=Chuyển sang Trang",
            "text=để bắt đầu quản lý",
        )

        saw_hint = False
        for hs in hint_selectors:
            try:
                if page.locator(hs).first.is_visible(timeout=1500):  # type: ignore[call-arg]
                    saw_hint = True
                    break
            except Exception:
                continue
        if not saw_hint:
            return

        for sel in switch_btn_selectors:
            try:
                btn = page.locator(sel).first
                if not btn.is_visible():
                    continue
                btn.click(timeout=10_000, force=True)
                logger.info("Đã bấm Switch Now để chuyển sang vai trò Page.")
                # Chờ ngắn cho UI cập nhật role.
                page.wait_for_timeout(1800)
                return
            except Exception:
                continue
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không xử lý được banner switch page: {}", exc)


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
        page.click(selector, timeout=10_000, force=True)
        page.wait_for_selector(selector, state="visible", timeout=15_000)
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
        _human_pause()
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
            _dismiss_blocking_ui_before_business_media(page)
            if _set_file_via_existing_input(page, path, kind="image"):
                logger.info("Đã gắn file ảnh qua input[type=file] có sẵn: {}", path)
                _human_pause()
                _enable_view_only_guard(page)
                return
            if _set_file_via_business_add_button(page, path, kind="image"):
                logger.info("Đã gắn file ảnh qua Add photo/video + filechooser interception: {}", path)
                _human_pause()
                _enable_view_only_guard(page)
                return
            if _set_file_via_existing_input(page, path, kind="image"):
                logger.info("Đã gắn file ảnh qua input fallback: {}", path)
                _human_pause()
                _enable_view_only_guard(page)
                return
            # Fallback cuối (opt-in): chỉ bật khi thật sự muốn dùng popup native.
            if _native_file_chooser_allowed():
                try:
                    with page.expect_file_chooser(timeout=10_000) as fc_info:
                        _open_business_add_photo_video(page)
                    fc_info.value.set_files(str(path))
                    logger.info("Đã gắn file ảnh qua filechooser fallback: {}", path)
                    _human_pause()
                    _enable_view_only_guard(page)
                    return
                except Exception:
                    pass
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
            _dismiss_blocking_ui_before_business_media(page)
            if _set_file_via_existing_input(page, path, kind="video"):
                logger.info("{} Đã gắn file video qua input[type=file] có sẵn: {}", _reel_strict_prefix("Upload"), path)
                try:
                    page.locator("video").first.wait_for(state="visible", timeout=22_000)
                except PlaywrightTimeoutError:
                    logger.warning("{} Không thấy thẻ video sau upload input — tiếp tục bước tiếp theo, chờ wizard xác nhận.", _reel_strict_prefix("Upload"))
                _human_pause()
                _enable_view_only_guard(page)
                return
            if _set_file_via_business_add_button(page, path, kind="video"):
                logger.info("{} Đã gắn file video qua Add photo/video + filechooser interception: {}", _reel_strict_prefix("Upload"), path)
                try:
                    page.locator("video").first.wait_for(state="visible", timeout=22_000)
                except PlaywrightTimeoutError:
                    logger.warning("{} Không thấy thẻ video sau Add photo/video interception — tiếp tục bước tiếp theo, chờ wizard xác nhận.", _reel_strict_prefix("Upload"))
                _human_pause()
                _enable_view_only_guard(page)
                return
            if _set_file_via_existing_input(page, path, kind="video"):
                logger.info("{} Đã gắn file video qua input fallback: {}", _reel_strict_prefix("Upload"), path)
                try:
                    page.locator("video").first.wait_for(state="visible", timeout=22_000)
                except PlaywrightTimeoutError:
                    logger.warning("{} Không thấy thẻ video sau upload input fallback — tiếp tục bước tiếp theo, chờ wizard xác nhận.", _reel_strict_prefix("Upload"))
                _human_pause()
                _enable_view_only_guard(page)
                return
            # Fallback cuối (opt-in): chỉ bật khi thật sự muốn dùng popup native.
            if _native_file_chooser_allowed():
                try:
                    with page.expect_file_chooser(timeout=10_000) as fc_info:
                        _open_business_add_photo_video(page)
                    fc_info.value.set_files(str(path))
                    logger.info("{} Đã gắn file video qua filechooser fallback: {}", _reel_strict_prefix("Upload"), path)
                    try:
                        page.locator("video").first.wait_for(state="visible", timeout=22_000)
                    except PlaywrightTimeoutError:
                        logger.warning("{} Không thấy thẻ video sau upload filechooser fallback — tiếp tục bước tiếp theo, chờ wizard xác nhận.", _reel_strict_prefix("Upload"))
                    _human_pause()
                    _enable_view_only_guard(page)
                    return
                except Exception:
                    pass
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
                try:
                    loc.click(timeout=15_000, force=True)
                except Exception:
                    # Meta / Firefox: lớp phủ hoặc hit-target lệch — thử HTMLElement.click().
                    loc.evaluate("el => { if (el && typeof el.click === 'function') el.click(); }")
                _human_pause()
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


def _meta_reel_details_visible(page: Page) -> bool:
    """Heuristic: màn Reel sau upload (có nút Next + vùng mô tả)."""
    try:
        if not _meta_reel_next_any_visible(page):
            return False
    except Exception:
        return False
    hints = (
        "Reel details",
        "Let viewers know",
        "Chi tiết Reel",
        "Mô tả",
    )
    for h in hints:
        try:
            if page.get_by_text(h, exact=False).first.is_visible(timeout=600):
                return True
        except Exception:
            continue
    try:
        if page.locator("div.notranslate._5rpu[role='textbox'][contenteditable='true']").count():
            return True
    except Exception:
        pass
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
        if _meta_reel_details_visible(page):
            logger.info("{} Đã thấy màn Reel details / Next.", _reel_strict_prefix("Wizard"))
            return True
        if _meta_video_attachment_confirmed(page):
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

    try:
        dlg = page.locator("[role='dialog']").filter(
            has_text=re.compile(
                r"Reel|Thumbnail|Description|Chi tiết|Mô tả|Hashtag|Let viewers know",
                re.I,
            )
        )
        if dlg.count() > 0:
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


def fill_meta_reel_description(page: Page, text: str) -> None:
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
        _enable_view_only_guard(page)
        raise
    except Exception as exc:
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
    Hoàn tất đăng Reel sau upload: mô tả (tuỳ chọn) → Tags từ ``#hashtag`` (tuỳ chọn) → Next → Next → Share now hoặc Schedule + giờ job;
    nếu Meta hiện **Video post processing** thì bấm **Done** để kết thúc luồng.

    Args:
        page: Trang Playwright.
        description: Hashtag / caption cho ô mô tả Reel.
        share_now: True → ``Share now``; False → chọn ``Schedule`` và điền giờ best-effort.
        scheduled_at_utc_iso: ISO UTC của job (khi ``share_now`` = False).

    Returns:
        True nếu đã bấm được ít nhất một nút submit thật (Share/Publish/Post/Schedule),
        False nếu không bấm được submit.
    """
    # Cách 2 (composer mới): có Post details + ô nhập chung + Publish trực tiếp.
    # Nếu rơi vào UI này thì bỏ luồng Next/Share cũ, nhập caption+hashtag vào ô chung rồi Publish.
    way2_visible = False
    try:
        way2_visible = bool(
            page.locator("div.notranslate._5rpu[role='combobox'][contenteditable='true']").first.is_visible(timeout=900)
        )
    except Exception:
        way2_visible = False
    if not way2_visible:
        try:
            way2_visible = bool(page.get_by_text("Post details", exact=False).first.is_visible(timeout=900))
        except Exception:
            way2_visible = False
    if way2_visible:
        logger.info("{} Phát hiện UI cách 2 (Post details + Publish), chuyển nhánh submit trực tiếp.", _reel_strict_prefix("Wizard"))
        if str(description or "").strip():
            fill_content(page, description)
            _human_pause()
        click_post_button(page)
        _human_pause()
        return True

    thumb_mode = normalize_reel_thumbnail_choice(reel_thumbnail_choice)

    fill_meta_reel_description(page, description)
    fill_meta_reel_tags_best_effort(page, description, reel_tags=reel_tags)

    if thumb_mode == REEL_THUMBNAIL_METHOD1_FIRST_AUTO:
        try:
            page.wait_for_timeout(random.randint(380, 780))
        except Exception:
            pass
        _choose_first_reel_thumbnail_method1_best_effort(page)

    if not share_now:
        raise RuntimeError("Luồng chuẩn video/reel hiện chỉ hỗ trợ Share now.")

    def _share_btn() -> Locator:
        return page.locator(
            "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' and .//div[normalize-space()='Share']])[last()]"
        )

    def _done_btn() -> Locator:
        return page.locator(
            "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' "
            "and (.//div[normalize-space()='Done'] or .//div[normalize-space()='Xong'])])[last()]"
        )

    def _click_done_strict(timeout_ms: int = 15_000) -> bool:
        stage = _reel_strict_prefix("Wizard")
        d = _done_btn()
        try:
            if d.count() <= 0:
                return False
            if not d.is_visible(timeout=2_500):
                return False
            if (d.get_attribute("aria-disabled") or "").strip().lower() == "true":
                return False
            try:
                d.evaluate("el => el && el.click && el.click()")
                logger.info("{} _click_done_strict: đã dispatch JS click.", stage)
                return True
            except Exception as exc_js:
                logger.debug("{} _click_done_strict: JS click lỗi: {}", stage, exc_js)
            try:
                d.click(timeout=min(timeout_ms, 5_000), force=True, no_wait_after=True)
                logger.info("{} _click_done_strict: đã click force (fallback).", stage)
                return True
            except Exception as exc_force:
                logger.warning("{} _click_done_strict: force click lỗi: {}", stage, exc_force)
                return False
        except Exception as exc:
            logger.debug("{} _click_done_strict: lỗi không xác định: {}", stage, exc)
            return False

    def _wait_next_button_ready(next_btn: Locator, *, timeout_ms: int = 10_000) -> None:
        next_btn.wait_for(state="visible", timeout=timeout_ms)
        dis = (next_btn.get_attribute("aria-disabled") or "").strip().lower()
        if dis == "true":
            raise RuntimeError("Nút Next đang disabled.")

    def _click_next_strict(timeout_ms: int = 20_000) -> bool:
        stage = _reel_strict_prefix("Wizard")
        # Khớp đúng HTML user cung cấp: role=button + tabindex=0 + aria-busy=false + text Next
        b = page.locator(
            "xpath=(//div[@role='button' and @tabindex='0' and @aria-busy='false' and .//div[normalize-space()='Next']])[last()]"
        )
        try:
            if b.count() <= 0:
                logger.warning("{} _click_next: không thấy Next theo XPath chuẩn.", stage)
                return False
            if not b.is_visible(timeout=2_500):
                logger.warning("{} _click_next: Next tồn tại nhưng chưa visible.", stage)
                return False
            if (b.get_attribute("aria-disabled") or "").strip().lower() == "true":
                logger.warning("{} _click_next: Next đang aria-disabled=true.", stage)
                return False
            # Ưu tiên click qua JS để tránh actionability của Playwright kéo scroll liên tục.
            try:
                b.evaluate("el => el && el.click && el.click()")
                logger.info("{} _click_next: đã dispatch JS click.", stage)
                return True
            except Exception as exc_js:
                logger.debug("{} _click_next: JS click lỗi: {}", stage, exc_js)
            try:
                b.click(timeout=min(timeout_ms, 6_000), force=True, no_wait_after=True)
                logger.info("{} _click_next: đã click force (fallback).", stage)
                return True
            except Exception as exc_force:
                logger.warning("{} _click_next: force click lỗi: {}", stage, exc_force)
                return False
        except Exception as exc:
            logger.warning("{} _click_next: lỗi không xác định: {}", stage, exc)
            return False

    for idx in (1, 2):
        before = _reel_active_step_label(page)
        advanced = False
        for attempt in (1, 2):
            page.wait_for_timeout(random.randint(500, 900))
            clicked = _click_next_strict(timeout_ms=20_000)
            if not clicked:
                if attempt == 2:
                    raise PlaywrightTimeoutError(f"Không bấm được nút Next (lần {idx}) trong luồng chuẩn.")
                page.wait_for_timeout(700)
                continue
            _human_pause()
            if _wait_reel_step_change(page, before, timeout_ms=10_000):
                logger.info("{} Đã bấm Next chuẩn (lần {}, attempt {}).", _reel_strict_prefix("Wizard"), idx, attempt)
                advanced = True
                break
            # Click có thể bị "ăn" nhưng step label chưa đổi; thử lại 1 lần có kiểm soát.
            page.wait_for_timeout(650)
        if not advanced:
            raise PlaywrightTimeoutError(f"Next lần {idx} không đổi step ({before}).")
        if idx == 1 and thumb_mode == REEL_THUMBNAIL_METHOD1_FIRST_AUTO:
            try:
                page.wait_for_timeout(random.randint(420, 900))
            except Exception:
                pass
            _choose_first_reel_thumbnail_method1_best_effort(page)

    def _click_share_strict(timeout_ms: int = 20_000) -> bool:
        stage = _reel_strict_prefix("Wizard")
        s = _share_btn()
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

    sb = _share_btn()
    if sb.count() <= 0 or not sb.is_visible(timeout=4_000):
        raise PlaywrightTimeoutError("Không tìm thấy nút Share trong luồng chuẩn.")
    sdis = (sb.get_attribute("aria-disabled") or "").strip().lower()
    if sdis == "true":
        raise RuntimeError("Nút Share đang disabled.")
    page.wait_for_timeout(random.randint(800, 1400))
    share_ok = False
    for attempt in (1, 2):
        if _click_share_strict(timeout_ms=20_000):
            share_ok = True
            logger.info("{} Đã bấm Share chuẩn (attempt {}).", _reel_strict_prefix("Wizard"), attempt)
            break
        try:
            page.wait_for_timeout(700)
        except Exception as exc:
            if "closed" in str(exc).lower():
                logger.info("{} Page đóng trong lúc retry Share → coi như đã submit.", _reel_strict_prefix("Wizard"))
                return True
            raise
    if not share_ok:
        raise PlaywrightTimeoutError("Không bấm được nút Share trong luồng chuẩn.")
    _human_pause()

    done_clicked = False
    try:
        db = _done_btn()
        if db.count() > 0 and db.is_visible(timeout=20_000):
            if _click_done_strict(timeout_ms=15_000):
                logger.info("{} Đã bấm Done chuẩn.", _reel_strict_prefix("Wizard"))
                done_clicked = True
                _human_pause()
    except Exception as exc:
        # Page/browser có thể đóng sau Share → xem như đã submit.
        if "closed" in str(exc).lower():
            logger.info("{} Page đóng sau Share trước khi kiểm tra Done → coi như đã submit.", _reel_strict_prefix("Wizard"))
            return True
        raise

    try:
        processed = dismiss_meta_video_post_processing_modal_best_effort(
            page, timeout_ms=120_000, give_up_if_never_seen_ms=15_000
        )
    except Exception as exc:
        if "closed" in str(exc).lower():
            logger.info("{} Page đóng khi chờ processing → coi như đã submit.", _reel_strict_prefix("Wizard"))
            return True
        raise
    submit_clicked = bool(done_clicked or processed)
    try:
        _enable_view_only_guard(page)
    except Exception as exc:
        if "closed" in str(exc).lower():
            logger.info("{} Page đóng khi bật lại lock-ui cuối wizard → coi như đã submit.", _reel_strict_prefix("Wizard"))
            return True
        logger.debug("{} Không bật lại được lock-ui cuối wizard: {}", _reel_strict_prefix("Wizard"), exc)
    if not submit_clicked:
        raise PlaywrightTimeoutError("Đã bấm Share nhưng không thấy Done/processing xác nhận.")
    return True


def _click_visible_enabled_button(candidates: Locator, *, timeout_ms: int = 1200) -> bool:
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
            try:
                b.click(timeout=timeout_ms)
            except Exception:
                b.click(timeout=timeout_ms, force=True, no_wait_after=True)
            return True
        except Exception:
            continue
    return False


def _click_next_in_dialog(page: Page, dialog: Locator) -> None:
    """Click nút Next/Tiếp theo đang usable trong dialog."""
    pat = re.compile(r"Next|Tiếp|Tiếp theo", re.I)
    cands = dialog.get_by_role("button", name=pat)
    if _click_visible_enabled_button(cands, timeout_ms=1400):
        page.wait_for_timeout(1800)
        return
    txt_cands = dialog.get_by_text(pat)
    if _click_visible_enabled_button(txt_cands, timeout_ms=1200):
        page.wait_for_timeout(1800)
        return
    raise PlaywrightTimeoutError("Không tìm thấy nút Next usable trong popup Reel.")


def _wait_post_button_in_dialog(dialog: Locator, *, timeout_ms: int = 20_000) -> Locator:
    pat = re.compile(r"Post|Đăng|Publish", re.I)
    candidates: list[Locator] = [
        # Khớp đúng HTML bạn cung cấp: div[role='none'] chứa span text "Post".
        dialog.locator(
            "xpath=(//div[@role='none' and .//span[normalize-space()='Post' and contains(@class,'x1j85h84')])[last()]"
        ).first,
        dialog.get_by_role("button", name=pat).first,
        dialog.locator("xpath=(//div[@role='none' and .//span[normalize-space()='Post']])[last()]").first,
        dialog.locator("xpath=(//*[self::div or self::span][normalize-space()='Post'])[last()]").first,
    ]
    deadline = time.time() + (max(1500, timeout_ms) / 1000.0)
    while time.time() < deadline:
        for c in candidates:
            try:
                if c.count() > 0 and c.is_visible(timeout=250):
                    return c
            except Exception:
                continue
        dialog.page.wait_for_timeout(280)
    raise PlaywrightTimeoutError("Không thấy nút Post/Publish usable trong popup Reel.")


def _click_post_strict_for_reel(page: Page, dialog: Locator) -> None:
    """
    Click đúng nút Post theo cấu trúc popup Reel, tránh click nhầm nhánh Share to groups.
    """
    cands: list[Locator] = [
        dialog.locator(
            "xpath=(//div[@role='none' and .//span[normalize-space()='Post' and contains(@class,'x1j85h84')])[last()]"
        ).first,
        dialog.locator("xpath=(//div[@role='none' and .//span[normalize-space()='Post']])[last()]").first,
        page.locator("xpath=(//div[@role='none' and .//span[normalize-space()='Post']])[last()]").first,
        dialog.locator("xpath=(//*[self::div or self::span][normalize-space()='Post']/ancestor::div[@role='none'][1])[last()]").first,
    ]
    for c in cands:
        try:
            if c.count() <= 0 or not c.is_visible(timeout=600):
                continue
            try:
                c.evaluate(
                    """el => {
                        if (!el) return;
                        const clickable = el.closest("[tabindex='0'], [role='button'], [role='none']") || el;
                        if (clickable && typeof clickable.click === "function") clickable.click();
                    }"""
                )
                logger.info("{} [POST_TARGET] strict_post_js_click", _reel_strict_prefix("Wizard"))
            except Exception:
                c.click(timeout=1400, force=True, no_wait_after=True)
                logger.info("{} [POST_TARGET] strict_post_force_click", _reel_strict_prefix("Wizard"))
            page.wait_for_timeout(900)
            return
        except Exception:
            continue
    raise PlaywrightTimeoutError("Không click được nút Post strict theo popup Reel.")


def _build_reel_text_payload(title: str, content: str, hashtags: list[str] | str | None) -> str:
    t = str(title or "").strip()
    c = str(content or "").strip()
    htxt = ""
    if isinstance(hashtags, list):
        vals: list[str] = []
        for h in hashtags:
            s = str(h or "").strip()
            if not s:
                continue
            if not s.startswith("#"):
                s = "#" + s.lstrip("#")
            vals.append(s.replace(" ", ""))
        htxt = " ".join(vals).strip()
    else:
        htxt = str(hashtags or "").strip()
    parts = [x for x in (t, c, htxt) if x]
    return "\n\n".join(parts).strip()


def _input_reel_text_in_dialog(dialog: Locator, text: str) -> None:
    raw = str(text or "").strip()
    if not raw:
        return
    tb = dialog.locator("[role='textbox'], textarea, [contenteditable='true']").last
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
    if isinstance(hashtags, str):
        raw_items = [x.strip() for x in hashtags.split() if x.strip()]
    elif isinstance(hashtags, list):
        raw_items = [str(x or "").strip() for x in hashtags if str(x or "").strip()]
    else:
        raw_items = []
    out: list[str] = []
    seen: set[str] = set()
    for x in raw_items:
        tag = x if x.startswith("#") else "#" + x.lstrip("#")
        tag = tag.replace(" ", "")
        key = tag.lower()
        if not tag or key in seen:
            continue
        seen.add(key)
        out.append(tag)
    return out


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
    tb = dialog.locator("[role='textbox'], textarea, [contenteditable='true']").last
    tb.wait_for(state="visible", timeout=10_000)
    try:
        tb.click(timeout=1200)
    except Exception:
        tb.click(timeout=1200, force=True)

    title_s = str(title or "").strip()
    content_s = str(content or "").strip()
    # Tránh lặp title khi content đã bắt đầu bằng title.
    if title_s and content_s.lower().startswith(title_s.lower()):
        content_s = content_s[len(title_s) :].lstrip(" \n\r\t-:|")
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


def _reel_textbox_visible(dialog: Locator, *, timeout_ms: int = 700) -> bool:
    """Kiểm tra ô nhập mô tả Reel (Describe your reel...) đã sẵn sàng chưa."""
    try:
        if dialog.locator("[role='textbox'][contenteditable='true']").first.is_visible(timeout=timeout_ms):
            return True
    except Exception:
        pass
    try:
        if dialog.get_by_text(re.compile(r"Describe your reel", re.I)).first.is_visible(timeout=timeout_ms):
            return True
    except Exception:
        pass
    return False


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
    video_path: Path,
    title: str = "",
    content: str = "",
    hashtags: list[str] | str | None = None,
    on_step: Callable[[str, str], None] | None = None,
) -> None:
    """
    Luồng Reel mới theo Page + Account:
    page_url -> Switch Now -> Professional Dashboard Content Library -> Create -> Reel -> Upload -> Next -> Post.
    """
    stage = _reel_strict_prefix("Wizard")
    current_step = "INIT"
    _ordered_steps = (
        "OPEN_PAGE_URL",
        "SCROLL_PAGE",
        "CLICK_SWITCH_NOW",
        "CLICK_SWITCH_CONFIRM",
        "OPEN_CONTENT_LIBRARY",
        "CLICK_CREATE",
        "SELECT_REEL",
        "WAIT_REEL_POPUP",
        "UPLOAD_VIDEO",
        "CLICK_NEXT_1",
        "CLICK_NEXT_2",
        "INPUT_TITLE_CONTENT_HASHTAGS",
        "CLICK_NEXT_3",
        "WAIT_POST_BUTTON",
        "CLICK_POST",
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

    _step("OPEN_PAGE_URL", f"Mở page_url: {purl}")
    page.goto(purl, wait_until="domcontentloaded")
    page.wait_for_timeout(3000)
    _step_pause(800, 1600, label="sau OPEN_PAGE_URL")
    _step("SCROLL_PAGE", "Scroll nhẹ để kích hoạt UI page.")
    try:
        page.mouse.wheel(0, 500)
    except Exception:
        pass
    page.wait_for_timeout(1400)
    _step_pause(700, 1400, label="sau SCROLL_PAGE")

    _step("CLICK_SWITCH_NOW", "Tìm và bấm Switch (cách 1: Switch Now, cách 2: từ panel Page).")
    sw_pat = re.compile(r"Switch Now|Chuyển ngay|Switch", re.I)
    sw_btns = page.get_by_role("button", name=sw_pat)
    clicked_switch = _click_visible_enabled_button(sw_btns, timeout_ms=1300)
    if not clicked_switch:
        # Cách 2: từ trang Page có block "Switch into ... Page ..." và nút/label "Switch" dạng div.
        try:
            sw_method2 = page.locator(
                "xpath=(//*[contains(translate(normalize-space(.), "
                "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'switch into')]"
                "//*[self::div or self::span][normalize-space()='Switch'])[last()]"
            )
            clicked_switch = _click_visible_enabled_button(sw_method2, timeout_ms=1500)
        except Exception:
            clicked_switch = False
    if not clicked_switch:
        # Fallback nhẹ: text "Switch" visible đầu tiên (chỉ dùng khi 2 cách trên không match).
        try:
            sw_text_any = page.get_by_text(re.compile(r"^Switch$", re.I))
            clicked_switch = _click_visible_enabled_button(sw_text_any, timeout_ms=1000)
        except Exception:
            clicked_switch = False

    if clicked_switch:
        logger.info("{} Đã bấm Switch (cách 1/2).", stage)
        page.wait_for_timeout(random.randint(3200, 7800))
        _step_pause(1000, 2200, label="sau CLICK_SWITCH_NOW")
        _step("CLICK_SWITCH_CONFIRM", "Nếu có popup Switch profiles thì bấm nút Switch để xác nhận.")
        sw_confirm = page.get_by_role("button", name=re.compile(r"^Switch$", re.I))
        if _click_visible_enabled_button(sw_confirm, timeout_ms=1600):
            logger.info("{} Đã bấm Switch trong popup Switch profiles.", stage)
            page.wait_for_timeout(random.randint(2200, 4200))
            _step_pause(1000, 2200, label="sau CLICK_SWITCH_CONFIRM(button)")
        else:
            # Fallback theo HTML user cung cấp: text span "Switch" trong popup.
            sw_text = page.get_by_text(re.compile(r"^Switch$", re.I))
            if _click_visible_enabled_button(sw_text, timeout_ms=1200):
                logger.info("{} Đã bấm Switch (fallback text) trong popup.", stage)
                page.wait_for_timeout(random.randint(2200, 4200))
                _step_pause(1000, 2200, label="sau CLICK_SWITCH_CONFIRM(text)")
    else:
        logger.info("{} Không thấy Switch Now, tiếp tục kiểm tra dashboard.", stage)

    _step("OPEN_CONTENT_LIBRARY", "Mở Professional Dashboard Content Library.")
    dash_url = "https://www.facebook.com/professional_dashboard/content/content_library/"
    page.goto(dash_url, wait_until="domcontentloaded")
    page.wait_for_timeout(4000)
    _step_pause(1200, 2400, label="sau OPEN_CONTENT_LIBRARY")
    cur = str(page.url or "").lower()
    if "professional_dashboard" not in cur:
        _failure_screenshot(page, "reel_dashboard_not_reachable")
        raise RuntimeError("Không vào được Professional Dashboard Content Library (có thể chưa switch đúng quyền Page).")

    _step("CLICK_CREATE", "Tìm và bấm Create / Create a post.")
    create_btns = page.get_by_role("button", name=re.compile(r"Create|Tạo|Create a post|Tạo bài viết", re.I))
    clicked_create = _click_visible_enabled_button(create_btns, timeout_ms=1800)
    if not clicked_create:
        # Một số UI chỉ render theo menuitem/card thay vì button.
        create_menu = page.get_by_role("menuitem", name=re.compile(r"Create|Tạo|Create a post|Tạo bài viết", re.I))
        clicked_create = _click_visible_enabled_button(create_menu, timeout_ms=1400)
    if not clicked_create:
        _failure_screenshot(page, "reel_create_not_found")
        raise PlaywrightTimeoutError("Không thấy nút Create/Create a post trong Content Library.")
    page.wait_for_timeout(900)
    _step_pause(900, 1900, label="sau CLICK_CREATE")

    _step("SELECT_REEL", "Chọn mục Reel trong menu tạo bài.")
    # Ưu tiên đúng cấu trúc bạn gửi: role=menuitem chứa text "Reel".
    reel_menu = page.get_by_role("menuitem", name=re.compile(r"Reel|Thước phim", re.I))
    if not _click_visible_enabled_button(reel_menu, timeout_ms=1800):
        reel_item = page.get_by_text(re.compile(r"Reel|Thước phim", re.I)).first
        try:
            reel_item.wait_for(state="visible", timeout=8_000)
            reel_item.click(timeout=1600)
        except Exception as exc:
            _failure_screenshot(page, f"reel_menu_item_not_clickable: {exc}")
            raise PlaywrightTimeoutError("Không chọn được mục Reel trong menu Create.") from exc

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
    abs_video = str(video_path.resolve())

    def _try_set_input_direct() -> bool:
        # Ưu tiên input trong dialog hiện tại; không yêu cầu visible.
        for cand in (
            dialog.locator("input[type='file']").last,
            dialog.locator("input[type='file']").first,
            page.locator("input[type='file']").last,
        ):
            try:
                if cand.count() <= 0:
                    continue
                cand.set_input_files(abs_video)
                return True
            except Exception:
                continue
        return False

    def _try_set_via_filechooser(trigger: Locator, *, label: str) -> bool:
        try:
            if trigger.count() <= 0:
                return False
            with page.expect_file_chooser(timeout=5_000) as fc_info:
                if not _click_visible_enabled_button(trigger, timeout_ms=1800):
                    return False
            fc = fc_info.value
            fc.set_files(abs_video)
            logger.info("{} Đã set video qua file chooser ({})", stage, label)
            return True
        except Exception:
            return False

    uploaded = _try_set_input_direct()
    if not uploaded:
        # Theo HTML bạn gửi: khu Add video + nút Upload trong popup Create reel.
        uploaded = _try_set_via_filechooser(
            dialog.get_by_text(re.compile(r"Add video|or drag and drop", re.I)),
            label="Add video",
        )
    if not uploaded:
        uploaded = _try_set_via_filechooser(
            dialog.get_by_role("button", name=re.compile(r"Upload", re.I)),
            label="Upload button",
        )
    if not uploaded:
        uploaded = _try_set_via_filechooser(
            dialog.get_by_text(re.compile(r"^Upload$", re.I)),
            label="Upload text",
        )
    if not uploaded:
        _failure_screenshot(page, "reel_file_input_missing")
        raise PlaywrightTimeoutError(
            "Không import được video: không tìm thấy input[type=file] usable hoặc trigger Add video/Upload."
        )

    page.wait_for_timeout(1800)
    # Chờ upload thực sự được nhận trước khi Next.
    upload_deadline = time.time() + 60.0
    upload_ok = False
    while time.time() < upload_deadline:
        try:
            # Placeholder trước upload thường chứa text này.
            placeholder_vis = dialog.get_by_text(
                re.compile(r"Upload your video in order to see a preview here", re.I)
            ).first.is_visible(timeout=200)
        except Exception:
            placeholder_vis = False
        try:
            next_btn = dialog.get_by_role("button", name=re.compile(r"Next|Tiếp|Tiếp theo", re.I)).first
            next_ready = next_btn.is_visible(timeout=200) and (next_btn.get_attribute("aria-disabled") or "").lower() != "true"
        except Exception:
            next_ready = False
        if (not placeholder_vis) or next_ready:
            upload_ok = True
            break
        page.wait_for_timeout(450)
    if not upload_ok:
        _failure_screenshot(page, "reel_upload_not_accepted")
        raise PlaywrightTimeoutError("Đã import video nhưng UI chưa nhận upload (placeholder vẫn còn / Next chưa sẵn sàng).")

    _step_pause(1200, 2600, label="sau UPLOAD_VIDEO")

    _step("CLICK_NEXT_1", "Bấm Next lần 1.")
    _click_next_in_dialog(page, dialog)
    _step_pause(900, 1800, label="sau CLICK_NEXT_1")

    # Hỗ trợ 2 flow:
    # - Flow A: Next1 đã có ô text.
    # - Flow B: Next1 chưa có ô text -> Next2 mới có ô text.
    def _wait_textbox(timeout_s: float) -> bool:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if _reel_textbox_visible(dialog, timeout_ms=350):
                return True
            page.wait_for_timeout(240)
        return _reel_textbox_visible(dialog, timeout_ms=300)

    text_ready = _wait_textbox(9.0)
    if not text_ready:
        _step("CLICK_NEXT_2", "Sau Next 1 chưa có ô text -> bấm Next lần 2 để mở màn nhập.")
        _click_next_in_dialog(page, dialog)
        _step_pause(900, 1800, label="sau CLICK_NEXT_2 (mở màn nhập)")
        text_ready = _wait_textbox(10.0)
        if not text_ready:
            _failure_screenshot(page, "reel_textbox_not_visible_after_next2")
            raise PlaywrightTimeoutError("Sau Next lần 2 vẫn chưa thấy ô nhập mô tả Reel.")

    _step("INPUT_TITLE_CONTENT_HASHTAGS", "Nhập title/content/hashtags.")
    _input_reel_title_content_and_hashtags(
        dialog,
        title=str(title or "").strip(),
        content=str(content or "").strip(),
        hashtags=hashtags,
    )
    _step_pause(700, 1500, label="sau INPUT_TITLE_CONTENT_HASHTAGS")

    # Nếu đã hiện Post sau khi nhập thì bấm luôn; nếu chưa, mới Next thêm 1 bước.
    post_btn: Locator | None = None
    try:
        post_btn = _wait_post_button_in_dialog(dialog, timeout_ms=2200)
    except Exception:
        post_btn = None
    if post_btn is None:
        _step("CLICK_NEXT_2", "Chưa thấy Post sau khi nhập -> bấm Next để sang màn Post.")
        _click_next_in_dialog(page, dialog)
        _step_pause(900, 1800, label="sau CLICK_NEXT_2 (sang màn Post)")
        post_btn = _wait_post_button_in_dialog(dialog, timeout_ms=20_000)
    _step("WAIT_POST_BUTTON", "Chờ nút Post/Publish xuất hiện.")
    if post_btn is None:
        post_btn = _wait_post_button_in_dialog(dialog, timeout_ms=20_000)
    _step("CLICK_POST", "Bấm Post.")
    logger.info(
        "{} [STRICT_FLOW] NEXT1 -> INPUT -> NEXT2 -> POST (khóa luồng thẳng)",
        _reel_strict_prefix("Wizard"),
    )
    try:
        _click_post_strict_for_reel(page, dialog)
    except Exception:
        # fallback cuối: dùng locator đã wait được trước đó
        try:
            post_btn.evaluate("el => el && el.click && el.click()")
            logger.info("{} [POST_TARGET] fallback_post_btn_js_click", _reel_strict_prefix("Wizard"))
        except Exception:
            post_btn.click(timeout=1800, force=True, no_wait_after=True)
            logger.info("{} [POST_TARGET] fallback_post_btn_force_click", _reel_strict_prefix("Wizard"))
    page.wait_for_timeout(1400)
    if _env_reel_pause_after_post():
        _step(
            "VERIFY_POST_SUBMITTED",
            "Đã bấm Post. TẠM DỪNG để bạn kiểm tra (FB_REEL_PAUSE_AFTER_POST=1), browser sẽ không tự đóng.",
        )
        # Giữ để người dùng kiểm tra thủ công. Có thể tắt bằng cách unset env rồi chạy lại.
        while True:
            page.wait_for_timeout(5000)

    _step("VERIFY_POST_SUBMITTED", "Đã bấm Post, chờ 8 giây theo cấu hình rồi kết thúc job.")
    page.wait_for_timeout(8000)
    _step("MARK_SUCCESS", "Đăng thành công (sau delay 8 giây hậu Post).")
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
        submit_delay_ms = max(200, _env_int("FB_REEL_PUBLISH_STEP_DELAY_MS", 900))
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
                raise RuntimeError(
                    "VERIFY_POST: Vẫn còn ở bước Create của Reel wizard, chưa qua submit cuối. need_manual_check"
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
