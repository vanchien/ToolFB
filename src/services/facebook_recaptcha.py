"""
Phát hiện và giải reCAPTCHA trên Facebook qua CapSolver.

Tích hợp khi đăng nhập lại / checkpoint (không thay thế xác minh danh tính / khóa tài khoản).
"""

from __future__ import annotations

import os
import re
import threading
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from loguru import logger
from playwright.sync_api import Page

_SITEKEY_RE = re.compile(r'data-sitekey=["\']([0-9A-Za-z_-]{20,})["\']', re.I)
_SITEKEY_JSON_RE = re.compile(r'"sitekey"\s*:\s*"([0-9A-Za-z_-]{20,})"', re.I)
_SITEKEY_RENDER_RE = re.compile(
    r'(?:\?|&)(?:render|k)=([0-9A-Za-z_-]{20,})|render=([0-9A-Za-z_-]{20,})',
    re.I,
)
_ANCHOR_K_RE = re.compile(r"[?&]k=([0-9A-Za-z_-]{20,})")
_S_HTML_PATTERNS = (
    re.compile(r'data-s=["\']([^"\']{8,})["\']', re.I),
    re.compile(r'["\']data-s["\']\s*:\s*["\']([^"\']{8,})["\']', re.I),
    re.compile(r'"enterprisePayload"\s*:\s*\{\s*"s"\s*:\s*"([^"]{8,})"', re.I),
    re.compile(r'enterprisePayload\s*[=:]\s*\{\s*s\s*:\s*["\']([^"\']{8,})["\']', re.I),
    re.compile(r'[?&]s=([A-Za-z0-9_\-+/=]{16,})', re.I),
)
_MIN_SITEKEY_LEN = 20
_MIN_ENTERPRISE_S_LEN = 8
_META_ENTERPRISE_SOLVE_S_LEN = 80
_EXTRACT_DOM_JS = """() => {
  const out = { sitekey: '', s: '', enterprise: false, action: '' };
  const pickS = (v) => {
    const t = String(v || '').trim();
    if (t.length > out.s.length) out.s = t;
  };
  const pickSk = (v) => {
    const t = String(v || '').trim();
    if (t.length >= 20 && t.length >= out.sitekey.length) out.sitekey = t;
  };
  if (window.grecaptcha?.enterprise) out.enterprise = true;
  document.querySelectorAll('[data-sitekey]').forEach((el) => {
    pickSk(el.getAttribute('data-sitekey'));
    pickS(el.getAttribute('data-s'));
  });
  document.querySelectorAll('[data-s]').forEach((el) => pickS(el.getAttribute('data-s')));
  const walk = (obj, depth) => {
    if (!obj || typeof obj !== 'object' || depth > 10) return;
    if (typeof obj.s === 'string') pickS(obj.s);
    if (typeof obj.sitekey === 'string') pickSk(obj.sitekey);
    if (typeof obj.action === 'string' && !out.action) out.action = obj.action;
    for (const v of Object.values(obj)) {
      if (v && typeof v === 'object') walk(v, depth + 1);
    }
  };
  try {
    const cfg = window.___grecaptcha_cfg;
    if (cfg?.clients) Object.values(cfg.clients).forEach((c) => walk(c, 0));
  } catch (e) {}
  return out;
}"""
_META_ENTERPRISE_MARKERS = (
    "recaptcha enterprise",
    "recaptcha enterprise của google",
    "enterprise của google",
    "không phải là người máy",
)
_ATTEMPT_COOLDOWN_SEC = 18.0
_LAST_SOLVE_ATTEMPT: dict[str, float] = {}
_LAST_SOLVE_AT: float = 0.0
_PIPELINE_IN_FLIGHT: set[str] = set()
_PIPELINE_LOCK = threading.Lock()
_POST_SOLVE_RETRY_WAIT_MS = 4_500
_HARD_FAIL_BLOCK_SEC = 180.0
_LAST_HARD_FAIL: dict[str, float] = {}
_HARD_FAIL_LOG_AT: dict[str, float] = {}
_CONTEXT_CAPTURE_ATTR = "_toolfb_recaptcha_network_capture"
_PAGE_NAV_HOOK_ATTR = "_toolfb_recaptcha_nav_hook"
_NAV_RESET_URL_ATTR = "_toolfb_recaptcha_last_reset_url"
# Regex bóc ``s`` / ``k`` trực tiếp từ URL request (anchor/reload Enterprise).
_S_URL_PARAM_RE = re.compile(r"[?&]s=([A-Za-z0-9_\-+/=%]+)", re.I)
_K_URL_PARAM_RE = re.compile(r"[?&]k=([0-9A-Za-z_-]{20,})", re.I)
_GOOGLE_RECAPTCHA_PATH_MARKERS = (
    "recaptcha",
    "/recaptcha/",
    "enterprise/anchor",
    "enterprise/reload",
    "api2/anchor",
    "api2/bframe",
    "api2/reload",
    "enterprise.js",
    "enterprise/",
)


_FB_RECAPTCHA_FLOW_URL_MARKERS = (
    "two_step",
    "two_factor",
    "two-factor",
    "authentication",
    "pre_authentication",
    "checkpoint",
    "confirmemail",
    "encrypted_context",
)


def is_plain_facebook_login_url(url: str = "") -> bool:
    """
    Trang form đăng nhập thường (email/mật khẩu) — **không** coi là luồng captcha chỉ vì URL.

    Meta có thể nhúng script recaptcha trên /login nhưng widget chỉ hiện sau submit.
    """
    u = str(url or "").strip().lower()
    if "facebook.com" not in u and "fb.com" not in u:
        return False
    if any(
        m in u
        for m in (
            "two_step",
            "two_factor",
            "two-factor",
            "authentication",
            "pre_authentication",
            "encrypted_context",
            "/checkpoint",
            "checkpoint?",
        )
    ):
        return False
    path = u.split("?", 1)[0].rstrip("/")
    return path.endswith("/login") or path.endswith("facebook.com/login")


def facebook_page_on_recaptcha_flow_url(page: Page | None = None, *, url: str = "") -> bool:
    """URL Facebook thường kèm reCAPTCHA (2FA/checkpoint) — không gồm /login thuần."""
    u = str(url or (getattr(page, "url", None) if page else "") or "").strip().lower()
    if "facebook.com" not in u and "fb.com" not in u:
        return False
    if is_plain_facebook_login_url(u):
        return False
    return any(m in u for m in _FB_RECAPTCHA_FLOW_URL_MARKERS)


def facebook_page_may_need_recaptcha(page: Page) -> bool:
    """Cần thử giải captcha: có widget/iframe HOẶC URL 2FA/checkpoint (không phải /login trống)."""
    if facebook_page_has_recaptcha(page):
        return True
    return facebook_page_on_recaptcha_flow_url(page)


def auto_recaptcha_providers_available() -> bool:
    """Có ít nhất một dịch vụ giải tự động (2Captcha hoặc CapSolver)."""
    from src.utils.capsolver_config import capsolver_auto_solve_enabled
    from src.utils.twocaptcha_config import twocaptcha_configured

    return twocaptcha_configured() or capsolver_auto_solve_enabled()


def facebook_recaptcha_task_urls(page: Page, *, page_url: str = "") -> list[str]:
    """
    Danh sách ``websiteURL`` cho CapSolver/2Captcha — ưu tiên URL trang hiện tại (m/www).
    """
    from src.services.capsolver_client import (
        capsolver_website_url_for_task,
        strip_facebook_page_url_for_capsolver,
    )

    raw = str(page_url or page.url or "").strip()
    u = raw.lower()
    urls: list[str] = []
    stripped = strip_facebook_page_url_for_capsolver(raw)
    if stripped:
        urls.append(capsolver_website_url_for_task(stripped, proxyless=False))

    on_auth_flow = any(
        m in u
        for m in ("two_step", "authentication", "pre_authentication", "encrypted_context")
    )
    if on_auth_flow:
        canonical = (
            "https://m.facebook.com/two_step_verification/authentication"
            if "m.facebook.com" in u
            else "https://www.facebook.com/two_step_verification/authentication"
        )
        norm = capsolver_website_url_for_task(canonical, proxyless=False)
        if norm and norm not in urls:
            urls.append(norm)
        return urls[:2]

    host_order: list[str] = []
    if "m.facebook.com" in u:
        host_order.extend(
            (
                "https://m.facebook.com/two_step_verification/authentication",
                "https://m.facebook.com/login",
            )
        )
    host_order.extend(
        (
            "https://www.facebook.com/two_step_verification/authentication",
            "https://www.facebook.com/login",
            "https://www.facebook.com",
        )
    )
    for host in host_order:
        norm = capsolver_website_url_for_task(host, proxyless=False)
        if norm and norm not in urls:
            urls.append(norm)
    return urls[:4]


def facebook_page_has_recaptcha(page: Page) -> bool:
    """Heuristic: iframe Google reCAPTCHA hoặc widget trên trang."""
    try:
        for frame in page.frames:
            fu = (frame.url or "").lower()
            if "google.com/recaptcha" in fu or "recaptcha.net" in fu:
                return True
    except Exception:
        pass
    try:
        if page.locator("iframe[src*='recaptcha']").count() > 0:
            return True
        if page.locator("[data-sitekey]").count() > 0:
            return True
        if page.locator("textarea[name='g-recaptcha-response']").count() > 0:
            return True
    except Exception:
        pass
    try:
        page_url = str(page.url or "")
        if is_plain_facebook_login_url(page_url):
            return False
        body = (page.locator("body").inner_text(timeout=2_000) or "").lower()
        if (
            "recaptcha" in body
            or "không phải người máy" in body
            or "không phải là người máy" in body
            or "tôi không phải" in body
            or "not a robot" in body
            or "recaptcha enterprise" in body
        ):
            return True
    except Exception:
        pass
    return False


def _pick_longest_s(*values: str) -> str:
    """Chọn chuỗi ``s`` dài nhất (Meta Enterprise payload thường rất dài)."""
    best = ""
    for raw in values:
        t = str(raw or "").strip()
        if len(t) > len(best):
            best = t
    return best


def _extract_s_from_html(html: str) -> str:
    """Tìm ``s`` / ``data-s`` / ``enterprisePayload`` trong HTML hoặc script nhúng."""
    text = str(html or "")
    if not text:
        return ""
    return _pick_longest_s(*(m.group(1).strip() for pat in _S_HTML_PATTERNS for m in pat.finditer(text)))


def _extract_dom_recaptcha_fields(page: Page) -> dict[str, str]:
    """Đọc sitekey/s/action từ DOM và ``___grecaptcha_cfg``."""
    try:
        found = page.evaluate(_EXTRACT_DOM_JS)
    except Exception:
        return {}
    if not isinstance(found, dict):
        return {}
    return {
        "sitekey": str(found.get("sitekey") or "").strip(),
        "s": str(found.get("s") or "").strip(),
        "page_action": str(found.get("action") or "").strip(),
        "enterprise": "true" if found.get("enterprise") else "false",
    }


def _get_browser_user_agent(page: Page) -> str:
    """User-Agent thật của trình duyệt Playwright — phải trùng khi gửi CapSolver."""
    try:
        ua = str(page.evaluate("() => navigator.userAgent || ''") or "").strip()
        if ua:
            return ua
    except Exception:
        pass
    try:
        for ctx_page in page.context.pages:
            ua = str(ctx_page.evaluate("() => navigator.userAgent || ''") or "").strip()
            if ua:
                return ua
    except Exception:
        pass
    return ""


def _capsolver_require_enterprise_s() -> bool:
    """
    Nếu True: không gọi CapSolver khi không đọc được ``s`` trên trình duyệt.

    Mặc định False — vẫn gửi CapSolver (worker có thể tự mở anchor); ta vẫn cố harvest ``s`` trước.
    """
    raw = os.environ.get("FB_CAPSOLVER_REQUIRE_S", "0").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _recaptcha_s_wait_ms() -> int:
    raw = os.environ.get("FB_RECAPTCHA_S_WAIT_MS", "30000")
    try:
        return max(5_000, int(raw))
    except ValueError:
        return 30_000


def _new_network_capture_store() -> dict[str, Any]:
    """Store theo BrowserContext — ``s`` gắn với ``load_generation`` (mỗi lần tải trang)."""
    return {
        "s": "",
        "sitekey": "",
        "page_action": "",
        "api_domain": "",
        "events": 0,
        "load_generation": 0,
        "s_generation": -1,
        "last_captured_url": "",
    }


def _is_google_recaptcha_network_url(url: str) -> bool:
    """
    True nếu request thuộc Google reCAPTCHA (anchor/reload/enterprise).

    Lắng nghe mọi URL có ``google.com`` hoặc ``recaptcha.net`` kèm path reCAPTCHA.
    """
    lower = str(url or "").strip().lower()
    if not lower:
        return False
    if "google.com" not in lower and "recaptcha.net" not in lower and "gstatic.com" not in lower:
        return False
    return any(marker in lower for marker in _GOOGLE_RECAPTCHA_PATH_MARKERS)


def _extract_s_from_url(url: str) -> str:
    """Bóc ``s=`` từ query URL bằng regex + parse_qs (ưu tiên chuỗi dài nhất)."""
    u = str(url or "").strip()
    if not u:
        return ""
    found: list[str] = []
    try:
        q = parse_qs(urlparse(u).query)
        s_q = str((q.get("s") or [""])[0]).strip()
        if s_q:
            found.append(unquote(s_q))
    except Exception:
        pass
    for m in _S_URL_PARAM_RE.finditer(u):
        found.append(unquote(m.group(1).strip()))
    return _pick_longest_s(*found)


def _extract_k_from_url(url: str) -> str:
    u = str(url or "").strip()
    if not u:
        return ""
    found: list[str] = []
    try:
        q = parse_qs(urlparse(u).query)
        k_q = str((q.get("k") or [""])[0]).strip()
        if k_q:
            found.append(k_q)
    except Exception:
        pass
    for m in _K_URL_PARAM_RE.finditer(u):
        found.append(m.group(1).strip())
    return _pick_longest_s(*found)


def _note_network_s(store: dict[str, Any], s_val: str, *, url: str = "") -> None:
    s_clean = str(s_val or "").strip()
    if len(s_clean) < _MIN_ENTERPRISE_S_LEN:
        return
    prev = str(store.get("s") or "")
    if s_clean == prev:
        return
    store["s"] = _pick_longest_s(prev, s_clean)
    store["s_generation"] = int(store.get("load_generation", 0))
    if url:
        store["last_captured_url"] = str(url)[:200]


def install_recaptcha_network_interception(context: Any) -> None:
    """
    Cài chặn network trên ``BrowserContext`` ngay khi mở browser.

    Mọi request/response tới ``google.com`` (reCAPTCHA) sẽ tự bóc ``s=`` — không cần F12.
    """
    if getattr(context, _CONTEXT_CAPTURE_ATTR, None) is not None:
        return
    store = _new_network_capture_store()

    def _on_request(request: Any) -> None:
        try:
            _network_ingest_request(store, request)
        except Exception:
            pass

    def _on_response(response: Any) -> None:
        try:
            _network_ingest_response(store, response)
        except Exception:
            pass

    context.on("request", _on_request)
    context.on("response", _on_response)

    def _on_page(page: Page) -> None:
        attach_recaptcha_navigation_reset(page)

    context.on("page", _on_page)
    try:
        for existing in context.pages:
            attach_recaptcha_navigation_reset(existing)
    except Exception:
        pass

    setattr(context, _CONTEXT_CAPTURE_ATTR, store)
    logger.info(
        "[FB reCAPTCHA] Network interception: lắng nghe google.com/recaptcha (request+response)."
    )


def attach_recaptcha_navigation_reset(page: Page) -> None:
    """Reset ``s`` khi main frame đổi URL — ``s`` Meta đổi mỗi lần tải trang."""
    if getattr(page, _PAGE_NAV_HOOK_ATTR, False):
        return
    setattr(page, _PAGE_NAV_HOOK_ATTR, True)

    def _on_frame_navigated(frame: Any) -> None:
        try:
            if frame != page.main_frame:
                return
        except Exception:
            return
        try:
            cur = str(page.url or "").strip()
        except Exception:
            return
        if not cur:
            return
        prev = str(getattr(page, _NAV_RESET_URL_ATTR, "") or "")
        if cur == prev:
            return
        setattr(page, _NAV_RESET_URL_ATTR, cur)
        reset_recaptcha_network_capture(page, reason="navigation")

    try:
        page.on("framenavigated", _on_frame_navigated)
    except Exception:
        pass


def reset_recaptcha_network_capture(page: Page, *, reason: str = "") -> None:
    """
    Xóa ``s`` đã bắt — gọi trước ``goto`` login/checkpoint để lần CapSolver sau dùng ``s`` mới.
    """
    store = _get_network_capture_store(page, install_if_missing=False)
    if store is None:
        ctx = page.context
        store = _new_network_capture_store()
        setattr(ctx, _CONTEXT_CAPTURE_ATTR, store)
    store["load_generation"] = int(store.get("load_generation", 0)) + 1
    store["s"] = ""
    store["sitekey"] = ""
    store["page_action"] = ""
    store["s_generation"] = -1
    store["last_captured_url"] = ""
    if reason:
        logger.debug(
            "[FB reCAPTCHA] Reset network capture ({}) — generation={}",
            reason,
            store["load_generation"],
        )


def get_fresh_network_s(page: Page) -> str:
    """``s`` chỉ hợp lệ nếu bắt sau lần reset/navigation gần nhất."""
    store = _get_network_capture_store(page, install_if_missing=True)
    s_val = str(store.get("s") or "").strip()
    if not s_val:
        return ""
    if int(store.get("s_generation", -1)) != int(store.get("load_generation", 0)):
        return ""
    return s_val


def _get_network_capture_store(page: Page, *, install_if_missing: bool = True) -> dict[str, Any]:
    ctx = page.context
    store = getattr(ctx, _CONTEXT_CAPTURE_ATTR, None)
    if store is None and install_if_missing:
        install_recaptcha_network_interception(ctx)
        store = getattr(ctx, _CONTEXT_CAPTURE_ATTR, None)
    if store is None:
        store = _new_network_capture_store()
        setattr(ctx, _CONTEXT_CAPTURE_ATTR, store)
    return store


def _network_ingest_url(store: dict[str, Any], url: str) -> None:
    u = str(url or "").strip()
    if not u or not _is_google_recaptcha_network_url(u):
        return
    store["events"] = int(store.get("events", 0)) + 1
    lower = u.lower()
    sk = _extract_k_from_url(u)
    if len(sk) >= _MIN_SITEKEY_LEN:
        prev_sk = str(store.get("sitekey") or "")
        if len(sk) >= len(prev_sk):
            store["sitekey"] = sk
    s_val = _extract_s_from_url(u)
    if s_val:
        _note_network_s(store, s_val, url=u)
    try:
        q = parse_qs(urlparse(u).query)
        sa = str((q.get("sa") or [""])[0]).strip()
        if sa and not str(store.get("page_action") or "").strip():
            store["page_action"] = sa
    except Exception:
        pass
    if "recaptcha.net" in lower:
        store["api_domain"] = "www.recaptcha.net"
    elif "google.com" in lower:
        store["api_domain"] = "www.google.com"
    body_s = _extract_s_from_html(u)
    if body_s:
        _note_network_s(store, body_s, url=u)


def _network_ingest_text(store: dict[str, Any], text: str) -> None:
    body = str(text or "")
    if not body:
        return
    store["events"] = int(store.get("events", 0)) + 1
    store["s"] = _pick_longest_s(str(store.get("s") or ""), _extract_s_from_html(body))
    for pat in (
        re.compile(r'\\"s\\"\s*:\s*\\"([^\\"]{16,})\\"', re.I),
        re.compile(r'"s"\s*:\s*"([A-Za-z0-9_\-+/=]{16,})"', re.I),
        re.compile(r'(?:^|[&?])s=([A-Za-z0-9_\-+/=%]{16,})', re.I),
    ):
        for m in pat.finditer(body):
            _note_network_s(store, m.group(1).strip())


def _network_ingest_request(store: dict[str, Any], request: Any) -> None:
    _network_ingest_url(store, str(getattr(request, "url", "") or ""))
    try:
        post = getattr(request, "post_data", None)
        if post:
            _network_ingest_text(store, str(post))
    except Exception:
        pass


def _network_ingest_response(store: dict[str, Any], response: Any) -> None:
    _network_ingest_url(store, str(getattr(response, "url", "") or ""))
    try:
        if not bool(getattr(response, "ok", False)):
            return
        ctype = str((getattr(response, "headers", None) or {}).get("content-type", "")).lower()
        if "json" in ctype or "javascript" in ctype or "text" in ctype:
            _network_ingest_text(store, str(response.text()))
    except Exception:
        pass


def _extract_iframe_src_params(page: Page) -> dict[str, str]:
    """Đọc ``src`` của iframe recaptcha trên trang cha (trước khi frame navigate xong)."""
    out = {"sitekey": "", "s": "", "page_action": ""}
    for sel in (
        "iframe[src*='recaptcha']",
        "iframe[src*='google.com/recaptcha']",
        "iframe[title*='reCAPTCHA']",
    ):
        try:
            loc = page.locator(sel)
            if loc.count() <= 0:
                continue
            src = str(loc.first.get_attribute("src") or "").strip()
            if not src:
                continue
            q = parse_qs(urlparse(src).query)
            sk = str((q.get("k") or [""])[0]).strip()
            if len(sk) >= _MIN_SITEKEY_LEN:
                out["sitekey"] = sk
            s_val = str((q.get("s") or [""])[0]).strip()
            out["s"] = _pick_longest_s(out["s"], s_val)
            sa = str((q.get("sa") or [""])[0]).strip()
            if sa:
                out["page_action"] = sa
            out["s"] = _pick_longest_s(out["s"], _extract_s_from_html(src))
        except Exception:
            continue
    return out


def _stimulate_recaptcha_widget(page: Page) -> None:
    """Cuộn tới widget + click nhẹ để Meta/Google load anchor (xuất hiện ``s`` trên network)."""
    for sel in (
        "iframe[src*='recaptcha']",
        "[data-sitekey]",
        "div[class*='recaptcha']",
    ):
        try:
            loc = page.locator(sel).first
            if loc.count() > 0:
                loc.scroll_into_view_if_needed(timeout=4_000)
                page.wait_for_timeout(400)
                break
        except Exception:
            continue
    try:
        fl = page.frame_locator("iframe[src*='recaptcha']").first
        for inner in ("#recaptcha-anchor", ".recaptcha-checkbox-border", "[role='checkbox']"):
            try:
                fl.locator(inner).click(timeout=2_500)
                page.wait_for_timeout(600)
                return
            except Exception:
                continue
    except Exception:
        pass
    try:
        page.locator("iframe[src*='recaptcha']").first.click(timeout=2_000, force=True)
        page.wait_for_timeout(500)
    except Exception:
        pass


def _harvest_enterprise_s(
    page: Page,
    *,
    timeout_ms: int | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> str:
    """
    Tự động thu ``s``: network listener + iframe + DOM + kích hoạt widget.
    """
    wait_ms = _recaptcha_s_wait_ms() if timeout_ms is None else max(3_000, int(timeout_ms))
    store = _get_network_capture_store(page)
    _stimulate_recaptcha_widget(page)
    deadline = time.monotonic() + wait_ms / 1000.0
    last_stim = time.monotonic()
    best = ""
    while time.monotonic() < deadline:
        if should_stop and should_stop():
            break
        best = _collect_enterprise_s(page)
        if len(best) >= _MIN_ENTERPRISE_S_LEN:
            logger.info(
                "[FB reCAPTCHA] Đã bóc s từ network (len={}) | events={} | gen={} | url={}",
                len(best),
                int(store.get("events", 0)),
                int(store.get("load_generation", 0)),
                str(store.get("last_captured_url") or "")[:72],
            )
            return best
        if time.monotonic() - last_stim > 4.0:
            _stimulate_recaptcha_widget(page)
            last_stim = time.monotonic()
        try:
            page.wait_for_timeout(500)
        except Exception:
            time.sleep(0.5)
    best = _collect_enterprise_s(page)
    if best:
        logger.info("[FB reCAPTCHA] Thu s sau chờ (len={}) | network={}", len(best), int(store.get("events", 0)))
    else:
        logger.warning(
            "[FB reCAPTCHA] Không thu được s sau {}ms (network events={}) — {}",
            wait_ms,
            int(store.get("events", 0)),
            "sẽ thử CapSolver không kèm s" if not _capsolver_require_enterprise_s() else "bỏ qua CapSolver",
        )
    return best


def _collect_enterprise_s(page: Page) -> str:
    """
    Gom ``s`` từ iframe anchor, DOM ``data-s``, ``___grecaptcha_cfg`` và HTML.

    CapSolver cần ``enterprisePayload: {"s": "..."}`` — thiếu ``s`` thì token Meta thường bị hủy.
    """
    candidates: list[str] = []
    try:
        fresh_s = get_fresh_network_s(page)
        if fresh_s:
            candidates.append(fresh_s)
    except Exception:
        pass
    iframe_src = _extract_iframe_src_params(page)
    if iframe_src.get("s"):
        candidates.append(iframe_src["s"])
    scanned = _scan_frames_for_recaptcha_params(page)
    if scanned.get("s"):
        candidates.append(scanned["s"])
    dom = _extract_dom_recaptcha_fields(page)
    if dom.get("s"):
        candidates.append(dom["s"])
    try:
        for frame in page.frames:
            fu = (frame.url or "").lower()
            if "recaptcha" not in fu:
                continue
            try:
                fr = frame.evaluate(_EXTRACT_DOM_JS)
                if isinstance(fr, dict) and fr.get("s"):
                    candidates.append(str(fr["s"]))
            except Exception:
                pass
            try:
                html_fr = frame.content()
                if html_fr:
                    candidates.append(_extract_s_from_html(html_fr))
            except Exception:
                pass
    except Exception:
        pass
    try:
        candidates.append(_extract_s_from_html(page.content()))
    except Exception:
        pass
    return _pick_longest_s(*candidates)


def _meta_enterprise_page(page: Page) -> bool:
    """Meta thường ghi rõ reCAPTCHA Enterprise trong nội dung trang."""
    try:
        body = (page.locator("body").inner_text(timeout=2_000) or "").lower()
    except Exception:
        return False
    return any(m in body for m in _META_ENTERPRISE_MARKERS)


def _scan_frames_for_recaptcha_params(page: Page) -> dict[str, str]:
    """Đọc sitekey đầy đủ + ``s``/``sa`` từ URL iframe anchor/bframe (ưu tiên key dài nhất)."""
    out: dict[str, str] = {
        "sitekey": "",
        "s": "",
        "page_action": "",
        "api_domain": "",
        "enterprise": "false",
    }
    try:
        frames = list(page.frames)
    except Exception:
        frames = []
    for frame in frames:
        fu = str(frame.url or "")
        if "recaptcha" not in fu.lower():
            continue
        try:
            q = parse_qs(urlparse(fu).query)
            sk = str((q.get("k") or [""])[0]).strip()
            if len(sk) >= _MIN_SITEKEY_LEN and len(sk) >= len(out["sitekey"]):
                out["sitekey"] = sk
            if not sk:
                m = _ANCHOR_K_RE.search(fu)
                if m:
                    sk2 = m.group(1).strip()
                    if len(sk2) >= _MIN_SITEKEY_LEN and len(sk2) >= len(out["sitekey"]):
                        out["sitekey"] = sk2
            s_val = str((q.get("s") or [""])[0]).strip()
            if s_val and len(s_val) >= len(out["s"]):
                out["s"] = s_val
            sa = str((q.get("sa") or [""])[0]).strip()
            if sa and not out["page_action"]:
                out["page_action"] = sa
            if "enterprise" in fu.lower():
                out["enterprise"] = "true"
            host = (urlparse(fu).hostname or "").strip().lower()
            if "recaptcha.net" in host:
                out["api_domain"] = "www.recaptcha.net"
            elif "google.com" in host:
                out["api_domain"] = "www.google.com"
        except Exception:
            continue
    return out


def _wait_recaptcha_anchor_frame(page: Page, *, timeout_ms: int = 10_000) -> bool:
    """Chờ iframe anchor (có ``k=`` sitekey đủ dài) trước khi đọc tham số."""
    deadline = time.monotonic() + max(1.0, timeout_ms / 1000.0)
    while time.monotonic() < deadline:
        scanned = _scan_frames_for_recaptcha_params(page)
        if len(scanned.get("sitekey", "")) >= _MIN_SITEKEY_LEN:
            return True
        try:
            for frame in page.frames:
                fu = (frame.url or "").lower()
                if "recaptcha" in fu and ("enterprise" in fu or "k=" in fu):
                    return True
            if page.locator("iframe[src*='recaptcha']").count() > 0:
                return True
        except Exception:
            pass
        try:
            page.wait_for_timeout(350)
        except Exception:
            time.sleep(0.35)
    return False


def _wait_recaptcha_s_parameter(
    page: Page,
    *,
    timeout_ms: int | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> str:
    """Meta Enterprise: tự harvest ``s`` (network + widget + DOM)."""
    return _harvest_enterprise_s(page, timeout_ms=timeout_ms, should_stop=should_stop)


def extract_recaptcha_params(page: Page) -> dict[str, Any] | None:
    """
    Lấy sitekey + URL trang cho CapSolver.

    Returns:
        ``{"website_key", "website_url", "is_invisible"}`` hoặc None.
    """
    website_url = str(page.url or "").strip()
    if not website_url or (
        "facebook.com" not in website_url.lower() and "fb.com" not in website_url.lower()
    ):
        website_url = "https://www.facebook.com/"
    else:
        from src.services.capsolver_client import strip_facebook_page_url_for_capsolver

        canonical = strip_facebook_page_url_for_capsolver(website_url)
        if canonical:
            website_url = canonical

    sitekey = ""
    is_invisible = False
    is_enterprise = False
    recaptcha_data_s_value = ""
    page_action = ""
    api_domain = ""

    dom_fields = _extract_dom_recaptcha_fields(page)
    if dom_fields.get("sitekey") and len(dom_fields["sitekey"]) >= len(sitekey):
        sitekey = dom_fields["sitekey"]
    if dom_fields.get("enterprise") == "true":
        is_enterprise = True
    recaptcha_data_s_value = _pick_longest_s(recaptcha_data_s_value, dom_fields.get("s", ""))
    if not page_action and dom_fields.get("page_action"):
        page_action = dom_fields["page_action"]

    try:
        el = page.locator("[data-sitekey]").first
        if el.count() > 0:
            sk_attr = str(el.get_attribute("data-sitekey") or "").strip()
            if len(sk_attr) >= _MIN_SITEKEY_LEN and len(sk_attr) >= len(sitekey):
                sitekey = sk_attr
            s_attr = str(el.get_attribute("data-s") or "").strip()
            recaptcha_data_s_value = _pick_longest_s(recaptcha_data_s_value, s_attr)
            if str(el.get_attribute("data-size") or "").strip().lower() == "invisible":
                is_invisible = True
    except Exception:
        pass

    if not sitekey or not recaptcha_data_s_value:
        try:
            html = page.content()
            if not sitekey:
                for pat in (_SITEKEY_RE, _SITEKEY_JSON_RE):
                    m = pat.search(html)
                    if m:
                        sitekey = m.group(1)
                        break
                if not sitekey:
                    m = _SITEKEY_RENDER_RE.search(html)
                    if m:
                        sitekey = (m.group(1) or m.group(2) or "").strip()
            recaptcha_data_s_value = _pick_longest_s(recaptcha_data_s_value, _extract_s_from_html(html))
        except Exception:
            pass

    if not sitekey:
        for frame in page.frames:
            try:
                sk_fr = frame.evaluate(
                    """() => {
                      const el = document.querySelector('[data-sitekey]');
                      return el ? (el.getAttribute('data-sitekey') || '') : '';
                    }"""
                )
                if str(sk_fr or "").strip():
                    sitekey = str(sk_fr).strip()
                    if "enterprise" in (frame.url or "").lower():
                        is_enterprise = True
                    break
            except Exception:
                continue

    frame_params = _scan_frames_for_recaptcha_params(page)
    if len(frame_params.get("sitekey", "")) >= _MIN_SITEKEY_LEN:
        if len(frame_params["sitekey"]) >= len(sitekey):
            sitekey = frame_params["sitekey"]
        if frame_params.get("enterprise") == "true":
            is_enterprise = True
        recaptcha_data_s_value = _pick_longest_s(recaptcha_data_s_value, frame_params.get("s", ""))
        if not page_action and frame_params.get("page_action"):
            page_action = frame_params["page_action"]
        if not api_domain and frame_params.get("api_domain"):
            api_domain = frame_params["api_domain"]

    if not is_enterprise:
        try:
            html = page.content().lower()
            if "recaptcha/enterprise" in html or "grecaptcha.enterprise" in html:
                is_enterprise = True
        except Exception:
            pass

    if _meta_enterprise_page(page):
        is_enterprise = True
        is_invisible = False

    if "facebook.com" in website_url.lower():
        if sitekey.startswith("6Le"):
            is_enterprise = True
            is_invisible = False
        if any(
            x in website_url.lower()
            for x in ("two_step", "authentication", "checkpoint", "/login")
        ):
            is_enterprise = True
            is_invisible = False

    if sitekey and len(sitekey) < _MIN_SITEKEY_LEN:
        logger.warning(
            "[FB reCAPTCHA] Sitekey quá ngắn (len={}) — có thể đọc thiếu từ iframe.",
            len(sitekey),
        )
    recaptcha_data_s_value = _pick_longest_s(recaptcha_data_s_value, _collect_enterprise_s(page))
    try:
        net = _get_network_capture_store(page)
        nsk = str(net.get("sitekey") or "").strip()
        if len(nsk) >= _MIN_SITEKEY_LEN and len(nsk) >= len(sitekey):
            sitekey = nsk
        recaptcha_data_s_value = _pick_longest_s(recaptcha_data_s_value, get_fresh_network_s(page))
        if not page_action and net.get("page_action"):
            page_action = str(net["page_action"])
        if not api_domain and net.get("api_domain"):
            api_domain = str(net["api_domain"])
    except Exception:
        pass
    if not sitekey:
        return None
    return {
        "website_key": sitekey,
        "website_url": website_url,
        "is_invisible": is_invisible,
        "is_enterprise": is_enterprise,
        "recaptcha_data_s_value": recaptcha_data_s_value,
        "page_action": page_action,
        "api_domain": api_domain,
        "user_agent": _get_browser_user_agent(page),
    }


def _is_checkpoint_captcha_page(page_url: str) -> bool:
    """True trên URL checkpoint / 2FA / two_step của Facebook."""
    u = str(page_url or "").lower()
    if "facebook.com" not in u:
        return False
    return any(
        marker in u
        for marker in (
            "two_step",
            "checkpoint",
            "authentication",
            "/login/",
            "confirmemail",
        )
    )


def _capsolver_hybrid_proxyless_enabled() -> bool:
    """
    Sau khi proxy CapSolver lỗi (CONNECT_REFUSED / whitelist), thử Enterprise ProxyLess.

    Mặc định bật — phù hợp proxy VN chỉ whitelist IP máy local.
    """
    raw = os.environ.get("FB_CAPSOLVER_HYBRID_PROXYLESS", "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _capsolver_proxyless_first() -> bool:
    """Bỏ qua proxy CapSolver ngay từ đầu (chỉ ProxyLess) — khi chắc chắn IP Whitelist chặn DC."""
    raw = os.environ.get("FB_CAPSOLVER_PROXYLESS_FIRST", "0").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _normalize_proxy_config_for_capsolver(px: dict[str, Any]) -> dict[str, Any]:
    """
    Chuẩn hóa port int + scheme socks5 + User/Pass trước khi gửi CapSolver.

    Hỗ trợ ``user``/``pass``, ``username``/``password``, ``proxy_username``/``proxy_password``.
    """
    from src.services.capsolver_client import _coerce_proxy_port
    from src.utils.proxy_check import _strip_proxy_scheme

    out = dict(px)
    user = str(
        out.get("user")
        or out.get("username")
        or out.get("proxy_username")
        or ""
    ).strip()
    password = str(
        out.get("pass")
        or out.get("password")
        or out.get("proxy_password")
        or ""
    ).strip()
    out["user"] = user
    out["pass"] = password
    port = _coerce_proxy_port(out.get("port"))
    if port > 0:
        out["port"] = port
    host_raw = str(out.get("host") or "").strip()
    scheme = str(out.get("scheme_hint") or "").strip().lower()
    if not scheme:
        low = host_raw.lower()
        if "socks5" in low:
            scheme = "socks5"
        elif "socks4" in low:
            scheme = "socks4"
        else:
            scheme = "socks5"
    out["scheme_hint"] = scheme
    if host_raw:
        out["host"] = _strip_proxy_scheme(host_raw) if scheme.startswith("socks") else host_raw
    return out


def _submit_delay_ms_after_inject(*, hybrid_ip_mismatch: bool) -> int:
    """Chờ trước click Tiếp tục — ProxyLess cần 3–5s để giảm lệch IP realtime."""
    import random

    if hybrid_ip_mismatch:
        lo = max(2_000, int(os.environ.get("FB_CAPSOLVER_SUBMIT_DELAY_MS_MIN", "3000")))
        hi = max(lo, int(os.environ.get("FB_CAPSOLVER_SUBMIT_DELAY_MS_MAX", "5000")))
        return random.randint(lo, hi)
    return random.randint(1_000, 2_000)


def _capsolver_use_account_proxy(
    account: dict[str, Any] | None = None,
    *,
    page_url: str = "",
) -> bool:
    """
    Có gửi proxy tài khoản lên CapSolver hay không.

    **Mặc định:** dùng proxy khi account ``use_proxy``.

    Bỏ proxy CapSolver (chỉ IP mạng máy — ProxyLess):
    - ``FB_CAPSOLVER_USE_ACCOUNT_PROXY=0``
    - ``FB_CAPSOLVER_PROXYLESS_FIRST=1``
    - ``config/app_secrets.json`` → ``"capsolver_use_account_proxy": false``
    """
    from src.utils.capsolver_config import capsolver_use_account_proxy_setting

    setting = capsolver_use_account_proxy_setting()
    if setting is False or _capsolver_proxyless_first():
        return False
    if setting is True:
        return bool(account and account.get("use_proxy"))
    if account and bool(account.get("use_proxy")):
        return True
    return False


# Nút gửi form sau khi đã chèn token — ưu tiên submit / Tiếp tục / Xác nhận.
_CHECKPOINT_CAPTCHA_SUBMIT_SELECTORS: tuple[str, ...] = (
    "#checkpointSubmitButton",
    "button#checkpointSubmitButton",
    "button[type='submit']",
    "button[name='submit[Continue]']",
    "button:has-text('Tiếp tục')",
    "button:has-text('Continue')",
    "button:has-text('Xác nhận')",
    "button:has-text('Confirm')",
    "div[role='button']:has-text('Tiếp tục')",
    "div[role='button']:has-text('Continue')",
    "div[role='button']:has-text('Xác nhận')",
    "[role='button']:has-text('Tiếp tục')",
    "input[type='submit']",
)


def _capsolver_split_proxy_mode(
    account: dict[str, Any] | None,
    *,
    page_url: str = "",
) -> bool:
    """True nếu trình duyệt có proxy nhưng CapSolver chạy ProxyLess (``FB_CAPSOLVER_USE_ACCOUNT_PROXY=0``)."""
    return bool(
        account
        and account.get("use_proxy")
        and "facebook.com" in str(page_url or "").lower()
        and not _capsolver_use_account_proxy(account, page_url=page_url)
    )


def _capsolver_task_has_proxy_fields(solve_kwargs: dict[str, Any]) -> bool:
    """Có proxy thật gửi CapSolver — không chỉ ``use_proxy`` trên tài khoản."""
    if solve_kwargs.get("proxy_config"):
        return True
    return bool(str(solve_kwargs.get("proxy") or "").strip())


def _resolve_capsolver_solve_mode(
    account: dict[str, Any] | None,
    *,
    page_url: str,
    use_enterprise: bool,
    solve_kwargs: dict[str, Any] | None = None,
) -> str:
    """
    Chế độ tạo task CapSolver — tránh chồng ProxyLess / proxy / v2.

    Returns:
        ``standard_checkpoint`` | ``enterprise_proxy`` | ``proxyless``
    """
    has_px = _capsolver_task_has_proxy_fields(solve_kwargs or {})
    if (
        use_enterprise
        and _is_checkpoint_captcha_page(page_url)
        and account
        and bool(account.get("use_proxy"))
        and has_px
        and _capsolver_use_account_proxy(account, page_url=page_url)
    ):
        return "standard_checkpoint"
    if (
        account
        and bool(account.get("use_proxy"))
        and has_px
        and _capsolver_use_account_proxy(account, page_url=page_url)
    ):
        return "enterprise_proxy"
    return "proxyless"


def _proxy_config_for_account(
    account: dict[str, Any] | None,
    *,
    page_url: str = "",
) -> dict[str, Any] | None:
    if not _capsolver_use_account_proxy(account, page_url=page_url):
        return None
    if not account or not bool(account.get("use_proxy", False)):
        return None
    px = account.get("proxy")
    if not isinstance(px, dict):
        return None
    from src.utils.proxy_check import proxy_host_port_configured

    if not proxy_host_port_configured(px):  # type: ignore[arg-type]
        return None
    norm = _normalize_proxy_config_for_capsolver(dict(px))
    if norm and str(norm.get("user") or "").strip():
        logger.info(
            "[FB reCAPTCHA] CapSolver dùng proxy tài khoản có User/Pass ({}:{}) — tránh CONNECT_REFUSED.",
            str(norm.get("host") or "")[:32],
            norm.get("port"),
        )
    elif norm:
        logger.warning(
            "[FB reCAPTCHA] Proxy tài khoản không có User/Pass — CapSolver có thể CONNECT_REFUSED. "
            "Thêm admin254:admin254 vào accounts.json hoặc dòng proxy tab Đăng nhập."
        )
    return norm


def _proxy_for_account(
    account: dict[str, Any] | None,
    *,
    page_url: str = "",
) -> str | None:
    px = _proxy_config_for_account(account, page_url=page_url)
    if not px:
        return None
    from src.utils.proxy_check import format_proxy_server_url

    return format_proxy_server_url(px)  # type: ignore[arg-type]


def _facebook_root_website_urls_only() -> list[str]:
    """
    Chỉ domain gốc Facebook — Meta Enterprise sitekey từ chối path ``/two_step_verification/...``.
    """
    from src.services.capsolver_client import (
        CAPSOLVER_FB_PROXYLESS_WEBSITE_URL,
        CAPSOLVER_FB_SUBDOMAIN_HOSTS,
    )

    override = os.environ.get("FB_CAPSOLVER_WEBSITE_URL", "").strip()
    if override and "facebook.com" in override.lower():
        u = override if override.startswith("http") else f"https://{override}"
        return [u.rstrip("/") or CAPSOLVER_FB_PROXYLESS_WEBSITE_URL]

    ret: list[str] = []
    seen: set[str] = set()
    for host in CAPSOLVER_FB_SUBDOMAIN_HOSTS:
        key = f"https://{host}".rstrip("/")
        if key not in seen:
            seen.add(key)
            ret.append(key)
    for fallback in (CAPSOLVER_FB_PROXYLESS_WEBSITE_URL, "https://www.facebook.com"):
        key = fallback.rstrip("/")
        if key not in seen:
            seen.add(key)
            ret.append(key)
    return ret or [CAPSOLVER_FB_PROXYLESS_WEBSITE_URL]


def _build_hybrid_proxyless_attempts(
    solve_kwargs: dict[str, Any],
    *,
    prefer_enterprise: bool = True,
) -> list[dict[str, Any]]:
    """Sau CONNECT_REFUSED — chỉ ProxyLess + URL gốc (không path checkpoint)."""
    from src.services.capsolver_client import capsolver_website_url_for_task

    base = dict(solve_kwargs)
    base.pop("proxy", None)
    base.pop("proxy_config", None)
    enterprise = bool(base.get("is_enterprise")) if prefer_enterprise else False
    if str(base.get("website_key") or "").startswith("6Le") and prefer_enterprise:
        enterprise = True
        base["is_enterprise"] = True
    attempts: list[dict[str, Any]] = []
    page_url = str(base.get("website_url") or "").lower()
    if any(m in page_url for m in ("two_step", "authentication", "pre_authentication")):
        from src.services.capsolver_client import strip_facebook_page_url_for_capsolver

        stripped = strip_facebook_page_url_for_capsolver(str(base.get("website_url") or ""))
        url_list = [stripped] if stripped else []
        canonical = (
            "https://m.facebook.com/two_step_verification/authentication"
            if "m.facebook.com" in page_url
            else "https://www.facebook.com/two_step_verification/authentication"
        )
        if canonical not in url_list:
            url_list.append(canonical)
    else:
        url_list = _facebook_root_website_urls_only()[:3]
    for u in url_list:
        t = dict(base)
        t["website_url"] = capsolver_website_url_for_task(u, proxyless=True)
        t["is_enterprise"] = enterprise
        t["is_invisible"] = False
        t["proxy"] = None
        t.pop("proxy_config", None)
        attempts.append(t)
    return attempts


def _capsolver_website_url_candidates(page_url: str, *, proxyless: bool) -> list[str]:
    """
    Danh sách ``websiteURL`` thử cho CapSolver (Cách 1).

    ProxyLess / Hybrid: **chỉ domain gốc** (tránh Invalid domain for site key trên path checkpoint).
    Có proxy: URL trang đã bỏ query + path checkpoint trên subdomain (nếu cần).
    """
    from src.services.capsolver_client import (
        CAPSOLVER_FB_PROXYLESS_WEBSITE_URL,
        CAPSOLVER_FB_SUBDOMAIN_HOSTS,
        strip_facebook_page_url_for_capsolver,
    )

    raw = str(page_url or "").strip()
    lower = raw.lower()
    override = os.environ.get("FB_CAPSOLVER_WEBSITE_URL", "").strip()
    if override and "facebook.com" in override.lower():
        u = override if override.startswith("http") else f"https://{override}"
        return [u.rstrip("/") or CAPSOLVER_FB_PROXYLESS_WEBSITE_URL]

    if proxyless or "facebook.com" not in lower:
        if proxyless:
            return _facebook_root_website_urls_only()
        return [raw] if raw else [CAPSOLVER_FB_PROXYLESS_WEBSITE_URL]

    stripped = strip_facebook_page_url_for_capsolver(raw)
    out: list[str] = []

    if stripped:
        out.append(stripped)

    path = urlparse(stripped or raw).path or ""
    if not path or path == "/":
        path = "/two_step_verification/authentication/" if (
            "two_step" in lower or "authentication" in lower or "checkpoint" in lower
        ) else "/"

    if any(m in lower for m in ("two_step", "authentication", "checkpoint", "captcha")):
        for host in CAPSOLVER_FB_SUBDOMAIN_HOSTS:
            candidate = f"https://{host}{path}".rstrip("/")
            if path.endswith("/") or path == "/":
                candidate = f"https://{host}{path}"
            out.append(candidate)

    out.append(CAPSOLVER_FB_PROXYLESS_WEBSITE_URL)
    out.append("https://www.facebook.com/")

    ret: list[str] = []
    seen: set[str] = set()
    max_n = 3
    for x in out:
        key = str(x).split("?")[0].split("#")[0].rstrip("/")
        if not key or key in seen:
            continue
        seen.add(key)
        ret.append(key)
        if len(ret) >= max_n:
            break
    return ret or [CAPSOLVER_FB_PROXYLESS_WEBSITE_URL]


def _capsolver_website_urls(page_url: str) -> list[str]:
    """Alias — danh sách URL cho attempt có proxy (ưu tiên URL trình duyệt đã cắt query)."""
    return _capsolver_website_url_candidates(page_url, proxyless=False)


def _build_capsolver_attempts(
    solve_kwargs: dict[str, Any],
    *,
    prefer_enterprise: bool = True,
) -> list[dict[str, Any]]:
    """
    Danh sách biến thể CapSolver — một chế độ duy nhất mỗi lần solve (không trộn ProxyLess + proxy + v2).
    """
    base = dict(solve_kwargs)
    page_url = str(base.get("website_url") or "")
    urls = _capsolver_website_url_candidates(page_url, proxyless=False)
    on_fb = "facebook.com" in page_url.lower()
    enterprise = bool(base.get("is_enterprise")) if prefer_enterprise else False
    if on_fb and str(base.get("website_key") or "").startswith("6Le") and prefer_enterprise:
        enterprise = True
        base["is_enterprise"] = True

    proxy_url = base.get("proxy")
    proxy_cfg = base.get("proxy_config")
    mode = _resolve_capsolver_solve_mode(
        base.get("_account"),
        page_url=page_url,
        use_enterprise=enterprise,
        solve_kwargs=base,
    )
    attempts: list[dict[str, Any]] = []

    def _task_for_url(u: str, *, with_proxy: bool) -> dict[str, Any]:
        t = dict(base)
        t["website_url"] = u
        t["is_enterprise"] = enterprise
        t["is_invisible"] = False
        if with_proxy and proxy_cfg:
            t["proxy"] = None
            t["proxy_config"] = proxy_cfg
        else:
            t["proxy"] = None
            t.pop("proxy_config", None)
        return t

    pl_urls = (
        _capsolver_website_url_candidates(page_url, proxyless=True)
        if on_fb
        else [page_url or "https://www.facebook.com"]
    )

    if mode == "standard_checkpoint":
        for u in urls[: min(2, len(urls))]:
            attempts.append(_task_for_url(u, with_proxy=True))
        return attempts[:2]

    if mode == "proxyless":
        for u in pl_urls[:3]:
            attempts.append(_task_for_url(u, with_proxy=False))
        return attempts[:3]

    # enterprise_proxy: proxy (URL trang) + ProxyLess thử lần lượt subdomain
    for u in urls[: min(2, len(urls))]:
        attempts.append(_task_for_url(u, with_proxy=True))
    for u in pl_urls[:1]:
        attempts.append(_task_for_url(u, with_proxy=False))
    return attempts[:3]


def inject_recaptcha_token(
    page: Page,
    token: str,
    solution: dict[str, Any] | None = None,
    *,
    page_action: str = "",
) -> bool:
    """Chèn token vào ``#g-recaptcha-response`` + callback Enterprise."""
    tok = str(token or "").strip()
    if not tok:
        return False
    sol = solution or {}
    ca_e = str(sol.get("recaptcha-ca-e") or "").strip()
    ca_t = str(sol.get("recaptcha-ca-t") or "").strip()
    injected = False

    for sel in (
        "#g-recaptcha-response",
        "textarea[name='g-recaptcha-response']",
        "textarea#g-recaptcha-response",
    ):
        try:
            loc = page.locator(sel).first
            if page.locator(sel).count() > 0:
                loc.evaluate(
                    """(el, token) => {
                      el.value = token;
                      el.innerHTML = token;
                      el.dispatchEvent(new Event('input', { bubbles: true }));
                      el.dispatchEvent(new Event('change', { bubbles: true }));
                    }""",
                    tok,
                )
                injected = True
                logger.info("[FB reCAPTCHA] Đã chèn token vào {}", sel)
                break
        except Exception as exc:  # noqa: BLE001
            logger.debug("[FB reCAPTCHA] fill {}: {}", sel, exc)

    action = str(page_action or "").strip()
    script = """
    (args) => {
      const token = args.token;
      const cae = args.ca_e || '';
      const cat = args.ca_t || '';
      const pageAction = args.page_action || '';
      let ok = false;
      const touched = [];

      const invokeNamedCallback = (name) => {
        if (!name) return;
        try {
          const fn = window[name];
          if (typeof fn === 'function') {
            fn(token);
            ok = true;
            touched.push('cb:' + name);
          }
        } catch (e) {}
      };

      const callAllCallbacks = (obj, seen = new Set()) => {
        if (!obj || typeof obj !== 'object' || seen.has(obj)) return;
        seen.add(obj);
        for (const [k, v] of Object.entries(obj)) {
          if (typeof v === 'function' && /callback|promise-callback|onSuccess|verifyCallback/i.test(k)) {
            try { v(token); ok = true; touched.push('fn:' + k); } catch (e) {}
          } else if (v && typeof v === 'object') {
            callAllCallbacks(v, seen);
          }
        }
      };

      document.querySelectorAll(
        'textarea[name="g-recaptcha-response"], #g-recaptcha-response, textarea#g-recaptcha-response'
      ).forEach((el) => {
        el.value = token;
        el.innerHTML = token;
        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
        ok = true;
        touched.push('textarea');
      });

      document.querySelectorAll('[data-sitekey][data-callback], .g-recaptcha[data-callback]').forEach((el) => {
        invokeNamedCallback(el.getAttribute('data-callback'));
      });

      let cfg = null;
      try {
        cfg = window.___grecaptcha_cfg;
        if (cfg && cfg.clients) {
          Object.keys(cfg.clients).forEach((id) => callAllCallbacks(cfg.clients[id]));
        }
      } catch (e) {}

      try {
        const g = window.grecaptcha;
        const ge = g && g.enterprise;
        const api = ge || g;
        if (api) {
          const runCb = (wid) => {
            try {
              if (ge && typeof ge.execute === 'function') {
                const act = pageAction || 'verify';
                ge.execute(wid, { action: act });
                touched.push('enterprise.execute:' + wid);
              }
            } catch (e) {}
          };
          if (cfg && cfg.clients) {
            Object.keys(cfg.clients).forEach((wid) => {
              try {
                const c = cfg.clients[wid];
                if (c && typeof c.callback === 'function') {
                  c.callback(token);
                  ok = true;
                  touched.push('client-callback:' + wid);
                }
                if (c && c.W && c.W.W && typeof c.W.W.callback === 'function') {
                  c.W.W.callback(token);
                  ok = true;
                  touched.push('client-W-callback:' + wid);
                }
              } catch (e) {}
              runCb(wid);
            });
          }
        }
      } catch (e) {}

      if (cae) {
        try { document.cookie = 'recaptcha-ca-e=' + encodeURIComponent(cae) + '; path=/; domain=.facebook.com'; touched.push('cookie-ca-e'); } catch (e) {}
      }
      if (cat) {
        try { document.cookie = 'recaptcha-ca-t=' + encodeURIComponent(cat) + '; path=/; domain=.facebook.com'; touched.push('cookie-ca-t'); } catch (e) {}
      }
      return { ok, touched };
    }
    """
    try:
        res = page.evaluate(script, {"token": tok, "ca_e": ca_e, "ca_t": ca_t, "page_action": action})
        if isinstance(res, dict):
            if bool(res.get("ok")):
                injected = True
            logger.info(
                "[FB reCAPTCHA] Inject main frame ok={} touched={}",
                bool(res.get("ok")),
                ",".join(list(res.get("touched") or [])[:8]),
            )
        elif res:
            injected = True
    except Exception as exc:  # noqa: BLE001
        logger.debug("[FB reCAPTCHA] inject main frame: {}", exc)

    for frame in page.frames:
        fu = (frame.url or "").lower()
        if "google.com/recaptcha" not in fu and "recaptcha" not in fu:
            continue
        try:
            if frame.evaluate(
                """(token) => {
                  const el = document.querySelector('textarea[name="g-recaptcha-response"]');
                  if (el) {
                    el.value = token; el.innerHTML = token;
                    try { el.dispatchEvent(new Event('input', { bubbles: true })); } catch (e) {}
                    try { el.dispatchEvent(new Event('change', { bubbles: true })); } catch (e) {}
                    return true;
                  }
                  return false;
                }""",
                tok,
            ):
                injected = True
        except Exception:
            continue
    return injected


def submit_checkpoint_after_captcha(
    page: Page,
    *,
    hybrid_ip_mismatch: bool = False,
) -> bool:
    """
    Bấm nút Tiếp tục / Xác nhận / submit để Facebook gửi form sau khi có token.

    ``hybrid_ip_mismatch=True``: token từ CapSolver ProxyLess (IP khác Firefox) — chờ 3–5s trước click.
    """
    from src.automation.facebook_actions import human_pause
    from src.services.facebook_session_recovery import _click_first, _login_browser_contexts

    delay_ms = _submit_delay_ms_after_inject(hybrid_ip_mismatch=hybrid_ip_mismatch)
    if hybrid_ip_mismatch:
        logger.info(
            "[FB reCAPTCHA] Hybrid ProxyLess: chờ {}ms sau inject trước khi click Tiếp tục (giảm lệch IP).",
            delay_ms,
        )
    try:
        page.wait_for_timeout(delay_ms)
    except Exception:
        time.sleep(delay_ms / 1000.0)
    human_pause(label="trước nút Tiếp tục sau captcha", kind="click")
    for ctx in _login_browser_contexts(page):
        for name_pat in (re.compile(r"tiếp tục", re.I), re.compile(r"^continue$", re.I), re.compile(r"confirm", re.I)):
            try:
                btn = ctx.get_by_role("button", name=name_pat)
                if btn.count() and btn.first.is_enabled(timeout=2_000):
                    btn.first.click(timeout=5_000)
                    logger.info("[FB reCAPTCHA] Đã bấm nút theo role ({})", name_pat.pattern)
                    human_pause(label="sau nút Tiếp tục captcha", kind="step")
                    return True
            except Exception:
                continue
        if _click_first(ctx, _CHECKPOINT_CAPTCHA_SUBMIT_SELECTORS, label="Tiếp tục/Xác nhận captcha"):
            logger.info("[FB reCAPTCHA] Đã bấm nút gửi form checkpoint (page.click).")
            human_pause(label="sau nút Tiếp tục captcha", kind="step")
            return True
    logger.warning("[FB reCAPTCHA] Không tìm thấy nút Tiếp tục/Xác nhận sau captcha — thử tick thủ công.")
    return False


def _click_facebook_after_captcha(page: Page) -> None:
    """Alias — gọi ``submit_checkpoint_after_captcha``."""
    submit_checkpoint_after_captcha(page)


def _apply_capsolver_token_to_page(
    page: Page,
    token: str,
    solution: dict[str, Any],
    *,
    page_action: str = "",
    sitekey_short: str = "",
    capsolver_proxyless: bool = False,
) -> bool:
    """Bước 3–4 pipeline: inject token → chờ (hybrid) → click submit checkpoint."""
    injected = inject_recaptcha_token(page, token, solution, page_action=page_action)
    if not injected:
        logger.warning("[FB reCAPTCHA] Inject lần 1 thất bại — kích hoạt widget và thử lại.")
        _stimulate_recaptcha_widget(page)
        try:
            page.wait_for_timeout(1_500)
        except Exception:
            time.sleep(1.5)
        injected = inject_recaptcha_token(page, token, solution, page_action=page_action)
    if not injected:
        logger.warning("[FB reCAPTCHA] Inject token thất bại — tick checkbox reCAPTCHA tay.")
        return False
    logger.info("[FB reCAPTCHA] Đã inject token — sitekey={}", sitekey_short[:12])
    _stimulate_recaptcha_widget(page)
    try:
        page.wait_for_timeout(800)
    except Exception:
        pass
    submit_checkpoint_after_captcha(page, hybrid_ip_mismatch=capsolver_proxyless)
    try:
        page.wait_for_timeout(2_000)
    except Exception:
        pass
    if not facebook_page_has_recaptcha(page):
        return True
    try:
        from src.services.facebook_session_recovery import _recovery_flow_advanced

        return _recovery_flow_advanced(page)
    except Exception:
        from src.automation.facebook_actions import facebook_session_appears_logged_in

        return facebook_session_appears_logged_in(page)


def try_solve_facebook_recaptcha(
    page: Page,
    account: dict[str, Any] | None = None,
    *,
    force_retry: bool = False,
    should_stop: Callable[[], bool] | None = None,
) -> bool:
    """
    Thử giải reCAPTCHA trên trang Facebook hiện tại.

    Returns:
        True nếu đã inject token và phiên có vẻ đã qua captcha (hoặc không còn widget).
    """
    from src.utils.capsolver_config import capsolver_auto_solve_enabled, get_capsolver_api_key
    from src.utils.twocaptcha_config import twocaptcha_configured

    if should_stop and should_stop():
        return False
    if not auto_recaptcha_providers_available():
        logger.info(
            "[FB reCAPTCHA] Không có CapSolver/2Captcha API key — chỉ captcha thủ công."
        )
        return False
    if not facebook_page_may_need_recaptcha(page):
        return False
    try:
        from src.services.facebook_session_recovery import facebook_page_looks_like_totp_prompt

        if facebook_page_looks_like_totp_prompt(page) and not facebook_page_has_recaptcha(page):
            logger.debug("[FB reCAPTCHA] Màn TOTP — bỏ qua pipeline captcha.")
            return False
    except Exception:
        pass

    _get_network_capture_store(page, install_if_missing=True)
    anchor_ms = 18_000 if facebook_page_on_recaptcha_flow_url(page) else 12_000
    _wait_recaptcha_anchor_frame(page, timeout_ms=anchor_ms)
    params = extract_recaptcha_params(page)
    if not params:
        try:
            page.wait_for_timeout(1_500)
        except Exception:
            time.sleep(1.5)
        params = extract_recaptcha_params(page)
    if not params:
        logger.warning("[FB reCAPTCHA] Thấy reCAPTCHA nhưng không đọc được sitekey.")
        return False
    if "facebook.com" in str(params.get("website_url") or "").lower():
        params["is_enterprise"] = True
        params["is_invisible"] = False
        s_wait = _harvest_enterprise_s(page, should_stop=should_stop)
        if s_wait:
            params["recaptcha_data_s_value"] = _pick_longest_s(
                str(params.get("recaptcha_data_s_value") or ""), s_wait
            )
        s_cur = str(params.get("recaptcha_data_s_value") or "").strip()
        if len(s_cur) < _META_ENTERPRISE_SOLVE_S_LEN:
            logger.info(
                "[FB reCAPTCHA] s ngắn (len={}) — chờ thêm anchor/network (mục tiêu ≥{}).",
                len(s_cur),
                _META_ENTERPRISE_SOLVE_S_LEN,
            )
            extra_s = _harvest_enterprise_s(
                page, timeout_ms=20_000, should_stop=should_stop
            )
            if extra_s:
                params["recaptcha_data_s_value"] = _pick_longest_s(s_cur, extra_s)
        refreshed = extract_recaptcha_params(page)
        if refreshed:
            params = refreshed
            params["is_enterprise"] = True
            if s_wait:
                params["recaptcha_data_s_value"] = _pick_longest_s(
                    str(params.get("recaptcha_data_s_value") or ""), s_wait
                )
    sk_full = str(params.get("website_key") or "")
    if len(sk_full) < _MIN_SITEKEY_LEN:
        logger.warning(
            "[FB reCAPTCHA] Sitekey iframe chưa đủ dài (len={}) — bỏ qua CapSolver.",
            len(sk_full),
        )
        return False
    s_full = str(params.get("recaptcha_data_s_value") or "").strip()
    ua = str(params.get("user_agent") or "").strip() or _get_browser_user_agent(page)
    if bool(params.get("is_enterprise")) and not s_full:
        if _capsolver_require_enterprise_s():
            logger.warning(
                "[FB reCAPTCHA] Enterprise Meta thiếu s sau harvest tự động — "
                "không gọi CapSolver (đặt FB_CAPSOLVER_REQUIRE_S=0 để vẫn thử)."
            )
            return False
        logger.info(
            "[FB reCAPTCHA] Chưa có s trên trình duyệt — vẫn gửi CapSolver Enterprise "
            "(worker có thể tự lấy từ anchor)."
        )
    logger.info(
        "[FB reCAPTCHA] Params: enterprise={} invisible={} s_len={} action={} domain={} "
        "sitekey_len={} ua_len={}",
        bool(params.get("is_enterprise")),
        bool(params.get("is_invisible")),
        len(s_full),
        str(params.get("page_action") or "")[:24],
        str(params.get("api_domain") or ""),
        len(sk_full),
        len(ua),
    )

    api_key = get_capsolver_api_key()
    page_url_for_proxy = str(params.get("website_url") or page.url or "")
    proxy_config = _proxy_config_for_account(account, page_url=page_url_for_proxy)
    solve_mode = _resolve_capsolver_solve_mode(
        account,
        page_url=page_url_for_proxy,
        use_enterprise=True,
        solve_kwargs={"proxy_config": proxy_config},
    )
    if solve_mode == "standard_checkpoint":
        if not proxy_config:
            logger.warning(
                "[FB reCAPTCHA] Checkpoint/2FA: thiếu proxy tài khoản — không gọi Enterprise+proxy."
            )
            return False
        logger.info(
            "[FB reCAPTCHA] Pipeline anti-block: (1) harvest s/sitekey "
            "(2) Tầng1 2Captcha+proxy → Tầng2 2Captcha ProxyLess → Tầng3 CapSolver (dự phòng) "
            "(3) inject token (4) click Tiếp tục — hoặc captcha tay 180s."
        )
    elif solve_mode == "proxyless":
        logger.info(
            "[FB reCAPTCHA] CapSolver ProxyLess — chỉ IP mạng máy (không gửi proxy SOCKS lên CapSolver). "
            "Firefox vẫn dùng proxy tài khoản nếu có. "
            "(Tắt: FB_CAPSOLVER_USE_ACCOUNT_PROXY=0 hoặc capsolver_use_account_proxy:false trong app_secrets.json)"
        )
    user_agent = ua
    if not user_agent:
        logger.warning("[FB reCAPTCHA] Không đọc được User-Agent trình duyệt — token có thể bị Google hủy.")
    sitekey_short = str(params["website_key"])[:12]
    aid = str((account or {}).get("id") or (account or {}).get("facebook_uid") or "").strip()
    attempt_key = f"{params['website_key']}:{aid or 'default'}"
    now = time.monotonic()
    with _PIPELINE_LOCK:
        if attempt_key in _PIPELINE_IN_FLIGHT:
            logger.debug(
                "[FB reCAPTCHA] Pipeline đang chạy cho sitekey {} — bỏ qua gọi chồng.",
                sitekey_short,
            )
            return False
        last = _LAST_SOLVE_ATTEMPT.get(attempt_key, 0.0)
        if (now - last) < _ATTEMPT_COOLDOWN_SEC and not force_retry:
            logger.info(
                "[FB reCAPTCHA] Bỏ qua solve lặp quá nhanh (cooldown {:.0f}s) key={}",
                _ATTEMPT_COOLDOWN_SEC,
                sitekey_short,
            )
            return False
        _LAST_SOLVE_ATTEMPT[attempt_key] = now
        global _LAST_SOLVE_AT
        _LAST_SOLVE_AT = now
        _PIPELINE_IN_FLIGHT.add(attempt_key)
    try:
        solve_kwargs = dict(
            website_url=str(params["website_url"]),
            website_key=str(params["website_key"]),
            api_key=api_key,
            proxy=None,
            proxy_config=proxy_config,
            _account=account,
            is_invisible=bool(params.get("is_invisible")),
            is_enterprise=bool(params.get("is_enterprise")),
            recaptcha_data_s_value=str(params.get("recaptcha_data_s_value") or ""),
            page_action=str(params.get("page_action") or ""),
            api_domain=str(params.get("api_domain") or ""),
            user_agent=user_agent,
        )
        if _meta_enterprise_page(page):
            solve_kwargs["is_enterprise"] = True
            solve_kwargs["is_invisible"] = False
        if "facebook.com" in str(solve_kwargs["website_url"]).lower():
            if str(solve_kwargs["website_key"]).startswith("6Le"):
                solve_kwargs["is_enterprise"] = True
                solve_kwargs["is_invisible"] = False

        hard_key = str(solve_kwargs["website_key"])
        hard_last = _LAST_HARD_FAIL.get(hard_key, 0.0)
        if (time.monotonic() - hard_last) < _HARD_FAIL_BLOCK_SEC:
            _log_hard_fail_skip(hard_key)
            return False

        from src.services.captcha_tier_pipeline import run_anti_block_captcha_pipeline

        tier_result = run_anti_block_captcha_pipeline(
            page,
            solve_kwargs,
            account,
            page_url=page_url_for_proxy,
            should_stop=should_stop,
        )
        if tier_result is None:
            hard_key = str(solve_kwargs.get("website_key") or "")
            if hard_key.startswith("6Le"):
                _LAST_HARD_FAIL[hard_key] = time.monotonic()
            return False

        solution = tier_result.solution
        capsolver_proxyless = tier_result.proxyless
        logger.info(
            "[FB reCAPTCHA] Pipeline anti-block OK — provider={} proxyless={}",
            tier_result.provider,
            capsolver_proxyless,
        )

        token = str(solution.get("gRecaptchaResponse") or "").strip()
        sol_ua = str(solution.get("userAgent") or "").strip()
        if sol_ua and user_agent and sol_ua != user_agent:
            logger.warning(
                "[FB reCAPTCHA] User-Agent CapSolver khác trình duyệt — ưu tiên UA bot khi tạo task lần sau."
            )
        return _apply_capsolver_token_to_page(
            page,
            token,
            solution,
            page_action=str(params.get("page_action") or ""),
            sitekey_short=str(params["website_key"]),
            capsolver_proxyless=capsolver_proxyless,
        )
    finally:
        with _PIPELINE_LOCK:
            _PIPELINE_IN_FLIGHT.discard(attempt_key)


def _log_hard_fail_skip(sitekey: str) -> None:
    """Giảm spam log khi vòng lặp wait_state gọi liên tục trong cooldown."""
    key = str(sitekey or "")[:20]
    now = time.monotonic()
    if now - _HARD_FAIL_LOG_AT.get(key, 0.0) < 30.0:
        return
    _HARD_FAIL_LOG_AT[key] = now
    logger.info(
        "[FB reCAPTCHA] Tạm bỏ qua solve: sitekey {} đang hard-fail cooldown ({}s).",
        key[:12],
        int(_HARD_FAIL_BLOCK_SEC),
    )


def _meta_enterprise_sitekey(sitekey: str) -> bool:
    """Sitekey Meta/Facebook Enterprise (6Le…) — CapSolver thường trả Invalid domain for site key."""
    sk = str(sitekey or "").strip()
    return sk.startswith("6Le")


def _capsolver_tier_disabled(page: Page, account: dict[str, Any] | None) -> bool:
    """
    Chỉ bỏ qua **tầng CapSolver** (2Captcha vẫn chạy trong pipeline).
    """
    from src.utils.capsolver_config import (
        capsolver_auto_solve_enabled,
        capsolver_skip_meta_enterprise,
    )

    if not capsolver_auto_solve_enabled():
        return True
    if recaptcha_domain_hard_failed(page):
        return True
    if capsolver_skip_meta_enterprise():
        params = extract_recaptcha_params(page)
        sk = str((params or {}).get("website_key") or "")
        if _meta_enterprise_sitekey(sk):
            logger.debug(
                "[FB reCAPTCHA] Bỏ qua tầng CapSolver (sitekey Meta Enterprise / capsolver_skip_meta)."
            )
            return True
    return False


def _should_skip_capsolver_auto(page: Page, account: dict[str, Any] | None) -> bool:
    """Alias — chỉ dùng trong tầng CapSolver, không chặn 2Captcha."""
    return _capsolver_tier_disabled(page, account)


def recaptcha_auto_solve_paused() -> bool:
    """True nếu vừa chạy pipeline — vòng poll không gọi lại cho đến hết cooldown."""
    return (time.monotonic() - _LAST_SOLVE_AT) < _ATTEMPT_COOLDOWN_SEC


def recaptcha_domain_hard_failed(page: Page) -> bool:
    """
    True nếu sitekey hiện tại vừa bị CapSolver từ chối mọi URL canonical (domain/sitekey).
    """
    params = extract_recaptcha_params(page)
    if not params:
        return False
    hard_key = str(params["website_key"])
    last = _LAST_HARD_FAIL.get(hard_key, 0.0)
    return (time.monotonic() - last) < _HARD_FAIL_BLOCK_SEC


def _page_eligible_for_recaptcha_solve(page: Page) -> bool:
    """URL/body phù hợp để gọi pipeline giải captcha."""
    from src.services.facebook_session_recovery import facebook_page_blocks_recovery_email

    u = str(page.url or "").lower()
    if "facebook.com" not in u and "fb.com" not in u:
        return False
    if facebook_page_blocks_recovery_email(u) and not facebook_page_has_recaptcha(page):
        return False
    return facebook_page_may_need_recaptcha(page)


def try_solve_facebook_recaptcha_checkpoint(
    page: Page,
    account: dict[str, Any] | None = None,
    *,
    should_stop: Callable[[], bool] | None = None,
) -> bool:
    """Checkpoint/2FA có captcha — cùng pipeline ``try_solve_facebook_recaptcha``."""
    if should_stop and should_stop():
        return False
    if not _page_eligible_for_recaptcha_solve(page):
        return False
    return try_solve_facebook_recaptcha(page, account, should_stop=should_stop)


def resolve_facebook_recaptcha(
    page: Page,
    account: dict[str, Any] | None = None,
    *,
    stage: str = "",
    wait_timeout_ms: int = 15_000,
    should_stop: Callable[[], bool] | None = None,
) -> bool:
    """
    Điểm vào thống nhất — login, checkpoint, 2FA, post-login, remember_browser.

    Returns:
        True nếu đã inject token / không còn widget / đã đăng nhập.
    """
    if should_stop and should_stop():
        return False
    try:
        from src.automation.facebook_actions import facebook_session_appears_logged_in

        if facebook_session_appears_logged_in(page):
            return True
    except Exception:
        pass
    if not auto_recaptcha_providers_available():
        return False
    if not facebook_page_may_need_recaptcha(page):
        return False
    tag = stage.strip() or "resolve"
    return wait_and_auto_solve_facebook_recaptcha(
        page,
        account,
        stage=tag,
        wait_timeout_ms=wait_timeout_ms,
        should_stop=should_stop,
    )


def auto_solve_facebook_recaptcha_if_present(
    page: Page,
    account: dict[str, Any] | None = None,
    *,
    stage: str = "",
    should_stop: Callable[[], bool] | None = None,
    allow_force_retry: bool = True,
) -> bool:
    """
    Một lần gọi pipeline solve khi trang có widget (không lặp wait+checkpoint wrapper).

    ``allow_force_retry=False`` khi gọi từ vòng poll — tránh 2× CapSolver mỗi 500ms.
    """
    if should_stop and should_stop():
        return False
    if not facebook_page_may_need_recaptcha(page):
        return False
    tag = stage.strip() or "runtime"
    logger.info("[FB reCAPTCHA] Phát hiện captcha tại stage={} — thử auto solve.", tag)
    ok = try_solve_facebook_recaptcha(page, account, should_stop=should_stop)
    if (
        not ok
        and allow_force_retry
        and not recaptcha_domain_hard_failed(page)
        and not recaptcha_auto_solve_paused()
        and not (should_stop and should_stop())
    ):
        ok = try_solve_facebook_recaptcha(page, account, force_retry=True, should_stop=should_stop)
    if ok:
        logger.info("[FB reCAPTCHA] Auto solve thành công tại stage={}.", tag)
    else:
        logger.info("[FB reCAPTCHA] Auto solve chưa thành công tại stage={}.", tag)
    return ok


def wait_and_auto_solve_facebook_recaptcha(
    page: Page,
    account: dict[str, Any] | None = None,
    *,
    stage: str = "",
    wait_timeout_ms: int = 10_000,
    poll_ms: int = 500,
    should_stop: Callable[[], bool] | None = None,
) -> bool:
    """
    Chờ widget render rồi **một** pipeline solve (không gọi thêm checkpoint wrapper).
    """
    tag = stage.strip() or "runtime_wait"
    if not auto_recaptcha_providers_available():
        return False
    if not _page_eligible_for_recaptcha_solve(page) and wait_timeout_ms <= 0:
        return False
    if auto_solve_facebook_recaptcha_if_present(
        page, account, stage=tag, should_stop=should_stop, allow_force_retry=True
    ):
        return True
    timeout_ms = max(0, int(wait_timeout_ms))
    if timeout_ms <= 0:
        return False
    interval = max(100, int(poll_ms))
    deadline = time.monotonic() + (timeout_ms / 1000.0)
    while time.monotonic() < deadline:
        if should_stop and should_stop():
            return False
        if not facebook_page_may_need_recaptcha(page):
            return False
        try:
            page.wait_for_timeout(interval)
        except Exception:
            time.sleep(interval / 1000.0)
        if recaptcha_auto_solve_paused() or recaptcha_domain_hard_failed(page):
            continue
        if auto_solve_facebook_recaptcha_if_present(
            page, account, stage=tag, should_stop=should_stop, allow_force_retry=False
        ):
            return True
    logger.info(
        "[FB reCAPTCHA] Hết thời gian chờ widget ({}ms) stage={}.",
        timeout_ms,
        tag,
    )
    return False


def wait_for_recaptcha_and_solve(
    page: Page,
    account: dict[str, Any] | None = None,
    *,
    stage: str = "",
    wait_timeout_ms: int = 15_000,
    should_stop: Callable[[], bool] | None = None,
) -> bool:
    """Điểm gọi từ recovery/GUI — bọc ``resolve_facebook_recaptcha``."""
    return resolve_facebook_recaptcha(
        page,
        account,
        stage=stage,
        wait_timeout_ms=wait_timeout_ms,
        should_stop=should_stop,
    )
