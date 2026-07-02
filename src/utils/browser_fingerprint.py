"""
Cấu hình fingerprint trình duyệt — giảm dấu vết automation (Playwright Firefox).

Meta reCAPTCHA Enterprise chấm điểm tin cậy theo UA, WebRTC, timezone/locale vs IP proxy.
"""

from __future__ import annotations

import os
import platform
import re
from typing import Any

import requests
from loguru import logger

from src.utils.proxy_check import (
    _requests_proxies_url,
    _strip_proxy_scheme,
    proxy_host_port_configured,
)


def _proxy_configured(proxy: dict[str, Any] | None) -> bool:
    return proxy_host_port_configured(proxy)

_COUNTRY_LOCALE_TZ: dict[str, tuple[str, str]] = {
    "US": ("en-US", "America/New_York"),
    "GB": ("en-GB", "Europe/London"),
    "DE": ("de-DE", "Europe/Berlin"),
    "FR": ("fr-FR", "Europe/Paris"),
    "VN": ("vi-VN", "Asia/Ho_Chi_Minh"),
    "TH": ("th-TH", "Asia/Bangkok"),
    "SG": ("en-SG", "Asia/Singapore"),
    "JP": ("ja-JP", "Asia/Tokyo"),
    "KR": ("ko-KR", "Asia/Seoul"),
    "ID": ("id-ID", "Asia/Jakarta"),
    "PH": ("en-PH", "Asia/Manila"),
    "MY": ("ms-MY", "Asia/Kuala_Lumpur"),
    "AU": ("en-AU", "Australia/Sydney"),
    "CA": ("en-CA", "America/Toronto"),
    "BR": ("pt-BR", "America/Sao_Paulo"),
    "IN": ("en-IN", "Asia/Kolkata"),
}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return raw.lower() not in {"0", "false", "off", "no"}


def default_desktop_firefox_user_agent() -> str:
    """
    User-Agent Firefox desktop khớp OS host (Windows/macOS/Linux).

    Tránh UA mặc định Playwright lệch nền tảng — Meta dùng để chấm điểm Enterprise.
    """
    override = os.environ.get("PLAYWRIGHT_USER_AGENT", "").strip() or os.environ.get(
        "FB_FIREFOX_UA", ""
    ).strip()
    if override:
        return override
    sys_name = platform.system()
    if sys_name == "Windows":
        ver = platform.release() or "10.0"
        nt = "10.0" if ver in ("10", "11", "10.0") else ver
        return (
            f"Mozilla/5.0 (Windows NT {nt}; Win64; x64; rv:128.0) "
            "Gecko/20100101 Firefox/128.0"
        )
    if sys_name == "Darwin":
        return (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) "
            "Gecko/20100101 Firefox/128.0"
        )
    return (
        "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0"
    )


def firefox_anti_detection_user_prefs(*, webrtc_block: bool = True) -> dict[str, Any]:
    """
    ``firefox_user_prefs`` giảm tín hiệu bot / rò IP WebRTC.

    Không tắt Marionette — Playwright Firefox cần Marionette để điều khiển.
    """
    prefs: dict[str, Any] = {
        "dom.webdriver.enabled": False,
        "useAutomationExtension": False,
        "devtools.jsonview.enabled": False,
        "browser.safebrowsing.malware.enabled": False,
        "browser.safebrowsing.phishing.enabled": False,
        # Giữ cookie + lịch sử giữa các lần mở profile portable (không xóa khi thoát).
        "browser.privatebrowsing.autostart": False,
        "privacy.clearOnShutdown.cookies": False,
        "privacy.clearOnShutdown.history": False,
        "privacy.sanitize.sanitizeOnShutdown": False,
    }
    if webrtc_block:
        prefs.update(
            {
                "media.peerconnection.enabled": False,
                "media.peerconnection.ice.default_address_only": True,
                "media.peerconnection.ice.no_host": True,
                "media.peerconnection.ice.proxy_only": True,
                "media.navigator.enabled": False,
            }
        )
    return prefs


def fingerprint_init_script() -> str:
    """Script chạy trước mọi document — bổ sung playwright-stealth trên Firefox."""
    return """
(() => {
  try {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined, configurable: true });
  } catch (e) {}
  try {
    if (navigator.webdriver === true) {
      Object.defineProperty(navigator, 'webdriver', { get: () => false, configurable: true });
    }
  } catch (e) {}
  try {
    const plat = navigator.platform || '';
    if (!plat && /Windows/i.test(navigator.userAgent || '')) {
      Object.defineProperty(navigator, 'platform', { get: () => 'Win32', configurable: true });
    }
  } catch (e) {}
})();
"""


def lookup_geo_via_proxy(
    proxy: dict[str, Any],
    *,
    timeout: float = 14.0,
) -> dict[str, str] | None:
    """
    Tra timezone/locale qua IP exit của proxy (ip-api.com qua proxy).

    Returns:
        ``{"ip", "timezone", "country_code", "locale"}`` hoặc None.
    """
    if not _proxy_configured(proxy):
        return None
    scheme = str(proxy.get("scheme_hint") or "socks5").strip().lower()
    if scheme not in ("http", "https", "socks4", "socks5"):
        scheme = "socks5"
    bare = _strip_proxy_scheme(str(proxy.get("host") or ""))
    port = int(proxy.get("port") or 0)
    user = str(proxy.get("user") or "").strip()
    password = str(proxy.get("pass") or "").strip()
    if not bare or port <= 0:
        return None
    try:
        proxy_url = _requests_proxies_url(scheme, bare, port, user, password)
        proxies = {"http": proxy_url, "https": proxy_url}
        r = requests.get(
            "http://ip-api.com/json/?fields=status,timezone,countryCode,query",
            proxies=proxies,
            timeout=timeout,
        )
        r.raise_for_status()
        data = r.json()
        if str(data.get("status") or "").lower() != "success":
            return None
        tz = str(data.get("timezone") or "").strip()
        cc = str(data.get("countryCode") or "").strip().upper()
        ip = str(data.get("query") or "").strip()
        locale = _COUNTRY_LOCALE_TZ.get(cc, ("en-US", tz))[0] if cc else ""
        if not tz:
            return None
        out = {"ip": ip, "timezone": tz, "country_code": cc, "locale": locale}
        logger.info(
            "[Browser FP] Geo proxy → IP={} country={} timezone={} locale={}",
            ip,
            cc or "?",
            tz,
            locale or "(mặc định)",
        )
        return out
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Browser FP] Không tra geo qua proxy: {}", exc)
        return None


def resolve_browser_locale(
    account: dict[str, Any] | None,
    *,
    proxy: dict[str, Any] | None = None,
    geo: dict[str, str] | None = None,
) -> str:
    """Locale Playwright — ưu tiên account → env → geo proxy → vi-VN."""
    if account:
        loc = str(account.get("locale") or account.get("browser_locale") or "").strip()
        if loc:
            return loc
    env_loc = os.environ.get("PLAYWRIGHT_LOCALE", "").strip()
    if env_loc:
        return env_loc
    if geo and geo.get("locale"):
        return str(geo["locale"])
    if geo and geo.get("country_code"):
        cc = str(geo["country_code"]).upper()
        if cc in _COUNTRY_LOCALE_TZ:
            return _COUNTRY_LOCALE_TZ[cc][0]
    if proxy and _env_bool("FB_SYNC_LOCALE_FROM_PROXY", True) and geo is None:
        pass
    return "vi-VN"


def resolve_browser_timezone(
    account: dict[str, Any] | None,
    *,
    proxy: dict[str, Any] | None = None,
    geo: dict[str, str] | None = None,
) -> str:
    """``timezone_id`` Playwright — ưu tiên account → env → geo proxy → Asia/Ho_Chi_Minh."""
    if account:
        tz = str(account.get("timezone") or account.get("browser_timezone") or "").strip()
        if tz and _valid_timezone(tz):
            return tz
    env_tz = os.environ.get("PLAYWRIGHT_TIMEZONE", "").strip()
    if env_tz and _valid_timezone(env_tz):
        return env_tz
    if geo and geo.get("timezone") and _valid_timezone(geo["timezone"]):
        return str(geo["timezone"])
    if geo and geo.get("country_code"):
        cc = str(geo["country_code"]).upper()
        if cc in _COUNTRY_LOCALE_TZ:
            return _COUNTRY_LOCALE_TZ[cc][1]
    return "Asia/Ho_Chi_Minh"


def _valid_timezone(tz: str) -> bool:
    name = str(tz or "").strip()
    if not name or len(name) > 64:
        return False
    return bool(re.match(r"^[A-Za-z_]+(?:/[A-Za-z_]+)+$", name))


def resolve_proxy_geo(
    account: dict[str, Any] | None,
    proxy: dict[str, Any] | None,
) -> dict[str, str] | None:
    """Tra geo qua proxy nếu bật ``FB_SYNC_TZ_FROM_PROXY`` (mặc định khi có proxy)."""
    use_proxy = bool(account and account.get("use_proxy")) or bool(
        proxy and _proxy_configured(proxy)
    )
    default_sync = use_proxy
    if not _env_bool("FB_SYNC_TZ_FROM_PROXY", default_sync):
        return None
    if not proxy or not _proxy_configured(proxy):
        return None
    return lookup_geo_via_proxy(proxy)


def apply_fingerprint_init_script(context: Any) -> None:
    """Gắn init script chống ``navigator.webdriver`` lên BrowserContext."""
    try:
        context.add_init_script(fingerprint_init_script())
        logger.debug("[Browser FP] Đã gắn fingerprint init script.")
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Browser FP] Không gắn init script: {}", exc)


def merge_firefox_user_prefs(existing: dict[str, Any] | None) -> dict[str, Any]:
    """Gộp prefs chống bot với prefs có sẵn (notification, proxy, …)."""
    merged = dict(existing or {})
    merged.update(firefox_anti_detection_user_prefs())
    return merged
