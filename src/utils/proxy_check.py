"""
Kiểm tra proxy (HTTP / HTTPS / SOCKS4 / SOCKS5) và chuẩn hóa cho Playwright.

Một module dùng chung: parse dòng dán → check LIVE → gắn vào trình duyệt.
"""

from __future__ import annotations

import re
from typing import Any, Literal, TypedDict
from urllib.parse import quote, unquote, urlparse

import requests
from loguru import logger

ProxyScheme = Literal["http", "https", "socks4", "socks5", "none"]

_IPIFY_HTTPS = "https://api.ipify.org"

_SCHEME_PREFIXES = (
    "socks5h://",
    "socks5://",
    "socks4a://",
    "socks4://",
    "https://",
    "http://",
)


class ProxyDict(TypedDict, total=False):
    host: str
    port: int
    user: str
    password: str
    scheme_hint: str


def _strip_proxy_scheme(host: str) -> str:
    h = str(host or "").strip()
    for prefix in _SCHEME_PREFIXES:
        if h.lower().startswith(prefix):
            return h[len(prefix) :].split("/")[0].strip()
    return h.split("/")[0].strip()


def proxy_host_port_configured(proxy: dict[str, Any] | None) -> bool:
    """True nếu có host không rỗng và port > 0."""
    if not isinstance(proxy, dict):
        return False
    host = str(proxy.get("host", "")).strip()
    try:
        port = int(proxy.get("port", 0))
    except (TypeError, ValueError):
        return False
    return bool(host) and port > 0


def _scheme_from_raw(raw: str) -> ProxyScheme | None:
    rl = str(raw or "").strip().lower()
    for prefix, scheme in (
        ("socks5h://", "socks5"),
        ("socks5://", "socks5"),
        ("socks4a://", "socks4"),
        ("socks4://", "socks4"),
        ("https://", "https"),
        ("http://", "http"),
    ):
        if rl.startswith(prefix):
            return scheme
    return None


def proxy_dict_from_accounts_json(px: Any) -> dict[str, Any]:
    """
    Chuẩn hóa trường ``proxy`` trong accounts.json.

    Hỗ trợ:
    - object ``{host, port, user, pass}`` (chuẩn Tool)
    - chuỗi ``socks5://user:pass@ip:port`` hoặc ``ip:port:user:pass``
    """
    empty: dict[str, Any] = {"host": "", "port": 0, "user": "", "pass": ""}
    if px is None:
        return empty
    if isinstance(px, str) and str(px).strip():
        try:
            return dict(parse_proxy_line(str(px).strip()))
        except ValueError:
            return empty
    if isinstance(px, dict):
        out = dict(px)
        out["user"] = str(
            out.get("user") or out.get("username") or out.get("proxy_username") or ""
        ).strip()
        out["pass"] = str(
            out.get("pass") or out.get("password") or out.get("proxy_password") or ""
        ).strip()
        try:
            out["port"] = int(out.get("port") or 0)
        except (TypeError, ValueError):
            out["port"] = 0
        out["host"] = str(out.get("host") or "").strip()
        return out
    return empty


def parse_proxy_line(line: str) -> ProxyDict:
    """
    Parse một dòng proxy.

    Hỗ trợ:
    - ``host:port:user:pass`` (IPv4)
    - ``host:port``
    - ``socks5://host:port`` / ``socks4://…`` / ``http(s)://user:pass@host:port``
    """
    raw = str(line or "").strip()
    if not raw:
        raise ValueError("Chuỗi proxy rỗng.")

    scheme_hint = _scheme_from_raw(raw) or ""

    if "://" in raw and "@" in raw:
        parsed = urlparse(raw)
        host = (parsed.hostname or "").strip()
        port = int(parsed.port or 0)
        user = unquote(parsed.username or "")
        password = unquote(parsed.password or "")
        scheme = (parsed.scheme or "http").lower()
        if scheme not in ("http", "https", "socks4", "socks5"):
            scheme = "http"
        if not host or port <= 0:
            raise ValueError(f"Không parse được URL proxy: {raw!r}")
        out_host = host if scheme in ("http", "https") else f"{scheme}://{host}"
        return {
            "host": out_host,
            "port": port,
            "user": user,
            "pass": password,
            "scheme_hint": scheme,
        }

    for prefix in ("socks5h://", "socks5://", "socks4a://", "socks4://"):
        if raw.lower().startswith(prefix):
            rest = raw[len(prefix) :]
            scheme = "socks5" if "socks5" in prefix else "socks4"
            parts = rest.split(":")
            if len(parts) == 4 and re.fullmatch(r"\d+\.\d+\.\d+\.\d+", parts[0]):
                host, port_s, user, password = parts[0], parts[1], parts[2], parts[3]
                port = int(port_s)
                return {
                    "host": f"{scheme}://{host}",
                    "port": port,
                    "user": user,
                    "pass": password,
                    "scheme_hint": scheme,
                }
            if "@" in rest:
                auth, server = rest.rsplit("@", 1)
                user, _, password = auth.partition(":")
                host, _, port_s = server.rpartition(":")
            else:
                user, password = "", ""
                host, _, port_s = rest.rpartition(":")
            port = int(port_s)
            if not host or port <= 0:
                raise ValueError(f"{scheme.upper()} không hợp lệ: {raw!r}")
            return {
                "host": f"{scheme}://{host}",
                "port": port,
                "user": user,
                "pass": password,
                "scheme_hint": scheme,
            }

    parts = raw.split(":")
    if len(parts) == 4 and re.fullmatch(r"\d+\.\d+\.\d+\.\d+", parts[0]):
        host, port_s, user, password = parts[0], parts[1], parts[2], parts[3]
        port = int(port_s)
        if port <= 0:
            raise ValueError(f"Port không hợp lệ: {port_s!r}")
        return {
            "host": host,
            "port": port,
            "user": user,
            "pass": password,
            "scheme_hint": scheme_hint,
        }
    if len(parts) == 2:
        host, port_s = parts[0].strip(), parts[1].strip()
        port = int(port_s)
        if not host or port <= 0:
            raise ValueError(f"host:port không hợp lệ: {raw!r}")
        return {
            "host": host,
            "port": port,
            "user": "",
            "pass": "",
            "scheme_hint": scheme_hint,
        }

    raise ValueError(
        "Định dạng không nhận ra. Ví dụ: host:port:user:pass | socks5://ip:port:user:pass | "
        "http://user:pass@host:port"
    )


def playwright_host_for_scheme(bare_host: str, scheme: ProxyScheme) -> str:
    """Host trong accounts.json — Playwright cần tiền tố scheme cho SOCKS."""
    bare = _strip_proxy_scheme(bare_host)
    if scheme == "socks5":
        return f"socks5://{bare}"
    if scheme == "socks4":
        return f"socks4://{bare}"
    return bare


def apply_proxy_scheme_to_config(px: dict[str, Any], scheme: ProxyScheme) -> dict[str, Any]:
    """Chuẩn hóa ``host`` sau khi biết scheme LIVE (dùng cho browser + lưu dòng)."""
    out = dict(px)
    host = str(out.get("host") or "").strip()
    port = int(out.get("port") or 0)
    if scheme in ("socks5", "socks4"):
        out["host"] = playwright_host_for_scheme(host, scheme)
    elif scheme == "https":
        bare = _strip_proxy_scheme(host)
        out["host"] = bare
    elif scheme == "http":
        out["host"] = _strip_proxy_scheme(host)
    out["scheme_hint"] = scheme
    _ = port
    return out


def format_proxy_line(px: dict[str, Any], scheme: ProxyScheme | None = None) -> str:
    """Sinh lại dòng proxy để dán (có scheme khi SOCKS)."""
    sch = scheme or str(px.get("scheme_hint") or "").strip() or "http"
    if sch not in ("http", "https", "socks4", "socks5"):
        sch = "http"
    host = str(px.get("host") or "").strip()
    port = int(px.get("port") or 0)
    user = str(px.get("user") or "").strip()
    password = str(px.get("pass") or "").strip()
    bare = _strip_proxy_scheme(host)
    if sch in ("socks5", "socks4"):
        base = f"{sch}://{bare}:{port}"
    else:
        base = f"{bare}:{port}"
    if user:
        if "://" in base:
            scheme_part, rest = base.split("://", 1)
            return f"{scheme_part}://{user}:{password}@{rest}"
        return f"{base}:{user}:{password}"
    return base


def format_proxy_server_url(px: dict[str, Any], scheme: ProxyScheme | None = None) -> str:
    """URL ``proxy_server`` trong MappedAccount (socks5://… hoặc http://…)."""
    sch = scheme or str(px.get("scheme_hint") or "http")
    host = str(px.get("host") or "").strip()
    port = int(px.get("port") or 0)
    user = str(px.get("user") or "").strip()
    password = str(px.get("pass") or "").strip()
    bare = _strip_proxy_scheme(host)
    if sch in ("socks5", "socks4"):
        base = f"{sch}://{bare}"
    else:
        base = bare
    if user:
        u, p = quote(user, safe=""), quote(password, safe="")
        return f"http://{u}:{p}@{base}:{port}" if sch in ("http", "https") else f"{sch}://{u}:{p}@{bare}:{port}"
    return f"{base}:{port}" if sch in ("http", "https") else f"{sch}://{bare}:{port}"


def _requests_proxies_url(scheme: ProxyScheme, bare: str, port: int, user: str, password: str) -> str:
    u = quote(user, safe="") if user else ""
    p = quote(password, safe="") if password else ""
    if scheme == "socks5":
        proto = "socks5h"
    elif scheme == "socks4":
        proto = "socks4a"
    else:
        proto = "http"
    if user:
        return f"{proto}://{u}:{p}@{bare}:{int(port)}"
    return f"{proto}://{bare}:{int(port)}"


def _check_via_requests(scheme: ProxyScheme, bare: str, port: int, *, user: str, password: str, timeout: float) -> tuple[bool, str]:
    if scheme in ("socks4", "socks5"):
        try:
            import socks  # noqa: F401
        except ImportError:
            return False, "Chưa cài PySocks — pip install PySocks"
    try:
        proxy_url = _requests_proxies_url(scheme, bare, port, user, password)
        proxies = {"http": proxy_url, "https": proxy_url}
        r = requests.get(_IPIFY_HTTPS, proxies=proxies, timeout=timeout)
        r.raise_for_status()
        ip = (r.text or "").strip()
        logger.info("Proxy {} LIVE, IP: {}", scheme.upper(), ip)
        return True, ip
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)[:240]


def check_http_proxy(
    host: str,
    port: int,
    *,
    user: str = "",
    password: str = "",
    timeout: float = 18.0,
) -> tuple[bool, str]:
    bare = _strip_proxy_scheme(host)
    if not bare:
        return False, "Thiếu host proxy."
    return _check_via_requests("http", bare, port, user=user, password=password, timeout=timeout)


def check_https_proxy(
    host: str,
    port: int,
    *,
    user: str = "",
    password: str = "",
    timeout: float = 18.0,
) -> tuple[bool, str]:
    """HTTPS proxy — kiểm tra giống HTTP CONNECT."""
    bare = _strip_proxy_scheme(host)
    if not bare:
        return False, "Thiếu host proxy."
    ok, msg = _check_via_requests("http", bare, port, user=user, password=password, timeout=timeout)
    return ok, msg


def check_socks5_proxy(
    host: str,
    port: int,
    *,
    user: str = "",
    password: str = "",
    timeout: float = 18.0,
) -> tuple[bool, str]:
    bare = _strip_proxy_scheme(host)
    if not bare:
        return False, "Thiếu host proxy."
    return _check_via_requests("socks5", bare, port, user=user, password=password, timeout=timeout)


def check_socks4_proxy(
    host: str,
    port: int,
    *,
    user: str = "",
    password: str = "",
    timeout: float = 18.0,
) -> tuple[bool, str]:
    bare = _strip_proxy_scheme(host)
    if not bare:
        return False, "Thiếu host proxy."
    return _check_via_requests("socks4", bare, port, user=user, password=password, timeout=timeout)


def check_proxy(
    host: str,
    port: int,
    *,
    user: str = "",
    password: str = "",
    timeout: float = 18.0,
    preferred_scheme: str | None = None,
) -> tuple[bool, str, ProxyScheme]:
    """
    Kiểm tra LIVE — thử đúng loại nếu có gợi ý scheme, không thì HTTP → SOCKS5 → SOCKS4.

    Returns:
        ``(ok, ip_or_error, scheme)``.
    """
    host_s = str(host or "").strip()
    pref = str(preferred_scheme or "").strip().lower()
    explicit = _scheme_from_raw(host_s)

    order: list[ProxyScheme] = []
    if explicit:
        order.append(explicit)
    elif pref in ("http", "https", "socks4", "socks5"):
        order.append(pref)  # type: ignore[arg-type]
    for s in ("http", "socks5", "socks4"):
        if s not in order:
            order.append(s)

    errors: list[str] = []
    for scheme in order:
        if scheme == "http":
            ok, msg = check_http_proxy(host_s, port, user=user, password=password, timeout=timeout)
        elif scheme == "https":
            ok, msg = check_https_proxy(host_s, port, user=user, password=password, timeout=timeout)
        elif scheme == "socks5":
            ok, msg = check_socks5_proxy(host_s, port, user=user, password=password, timeout=timeout)
        else:
            ok, msg = check_socks4_proxy(host_s, port, user=user, password=password, timeout=timeout)
        if ok:
            return True, msg, scheme
        errors.append(f"{scheme.upper()}: {msg.split(chr(10))[0][:120]}")

    return False, " | ".join(errors[:3]), "none"


_PROXY_BROWSER_ERR_HINTS = (
    "ns_error_proxy",
    "err_proxy",
    "proxy connection failed",
    "proxy_connect_failure",
    "unable to find the proxy",
    "err_tunnel",
    "err_connection_reset",
    "err_connection_refused",
    "err_timed_out",
    "net::err_",
)


def verify_browser_facebook_via_proxy(
    page: Any,
    *,
    timeout_ms: int = 50_000,
) -> tuple[bool, str]:
    """
    Sau khi mở Playwright + proxy: thử tải Facebook — phát hiện lỗi proxy thực tế.

    Returns:
        ``(True, "OK")`` hoặc ``(False, mô_tả lỗi)``.
    """
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    try:
        page.goto(
            "https://www.facebook.com/",
            wait_until="domcontentloaded",
            timeout=max(8_000, int(timeout_ms)),
        )
    except PlaywrightTimeoutError as exc:
        return False, f"Proxy/timeout — không tải được Facebook ({str(exc)[:160]})"
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        low = msg.lower()
        if any(h in low for h in _PROXY_BROWSER_ERR_HINTS):
            return False, f"Proxy không kết nối được: {msg[:200]}"
        return False, f"Không mở được Facebook qua proxy: {msg[:200]}"

    url = (page.url or "").lower()
    if "chrome-error" in url or "about:neterror" in url:
        return False, "Proxy lỗi — trình duyệt không vào được Facebook (neterror)"
    try:
        snippet = (page.content() or "")[:4000].lower()
    except Exception:
        snippet = ""
    for hint in _PROXY_BROWSER_ERR_HINTS:
        if hint in url or hint in snippet:
            return False, f"Proxy không kết nối được ({hint})"
    if "facebook.com" not in url:
        return False, f"Không vào được Facebook (url={url[:80]})"
    return True, "Proxy OK — đã tải Facebook"


def build_playwright_proxy_settings(proxy: dict[str, Any]) -> dict[str, Any]:
    """
    Cấu hình ``proxy`` cho ``launch_persistent_context``.

    HTTP/HTTPS → ``http://host:port``; SOCKS5 → ``socks5://…``;
    SOCKS4 (Playwright không hỗ trợ trực tiếp) → thử ``socks5://`` cùng host:port.
    """
    raw_host = str(proxy.get("host", "")).strip()
    try:
        port = int(proxy.get("port", 0))
    except (TypeError, ValueError):
        port = 0
    user = str(proxy.get("user") or "").strip()
    password = str(proxy.get("pass") or "").strip()
    rl = raw_host.lower()

    if rl.startswith("socks5://") or rl.startswith("socks5h://"):
        rest = raw_host.split("://", 1)[-1].rstrip("/")
        if port > 0 and ":" not in rest.rsplit("@", 1)[-1]:
            rest = f"{rest}:{port}"
        settings: dict[str, Any] = {"server": f"socks5://{rest}"}
    elif rl.startswith("socks4://") or rl.startswith("socks4a://"):
        bare = _strip_proxy_scheme(raw_host)
        logger.debug(
            "[Proxy] SOCKS4 → Playwright dùng socks5://{}:{} (tương thích; nếu lỗi hãy đổi sang socks5:// trong dòng proxy).",
            bare,
            port,
        )
        settings = {"server": f"socks5://{bare}:{port}"}
    else:
        host = _strip_proxy_scheme(raw_host)
        settings = {"server": f"http://{host}:{port}"}
    if user:
        settings["username"] = user
        settings["password"] = password
    return settings


def check_proxy_line(line: str, *, timeout: float = 18.0) -> tuple[bool, str, ProxyScheme, ProxyDict]:
    """
    Parse + check một dòng proxy (dùng GUI / lọc danh sách).

    Returns:
        ``(ok, message, scheme, parsed_dict)``.
    """
    px = parse_proxy_line(line)
    hint = str(px.get("scheme_hint") or "")
    ok, msg, scheme = check_proxy(
        str(px.get("host") or ""),
        int(px.get("port") or 0),
        user=str(px.get("user") or ""),
        password=str(px.get("pass") or ""),
        timeout=timeout,
        preferred_scheme=hint or None,
    )
    if ok:
        px = apply_proxy_scheme_to_config(px, scheme)
    return ok, msg, scheme, px


def proxy_needs_socks_http_relay(proxy: dict[str, Any]) -> bool:
    """SOCKS có user/pass → cần relay HTTP local (Chromium/Playwright)."""
    host = str(proxy.get("host", "")).strip().lower()
    user = str(proxy.get("user", "") or proxy.get("username", "")).strip()
    return (host.startswith("socks5://") or host.startswith("socks4://")) and bool(user)
