"""
Client CapSolver — createTask / getTaskResult.

Tài liệu: https://docs.capsolver.com/en/guide/getting-started/
reCAPTCHA v2 / v2 Enterprise: https://docs.capsolver.com/en/guide/captcha/ReCaptchaV2/
(Meta: ``ReCaptchaV2EnterpriseTask`` + ``enterprisePayload.s`` khi có tham số ``s`` từ iframe anchor.)
"""

from __future__ import annotations

import os
import time
from typing import Any

import requests
from loguru import logger

_CAPSOLVER_CREATE = "https://api.capsolver.com/createTask"
_CAPSOLVER_RESULT = "https://api.capsolver.com/getTaskResult"

# Meta Enterprise ProxyLess: domain gốc — tránh URL dài/query gây Invalid domain for site key.
CAPSOLVER_FB_PROXYLESS_WEBSITE_URL = "https://www.facebook.com"

# Thứ tự subdomain ProxyLess — www trước (Meta Enterprise sitekey).
CAPSOLVER_FB_SUBDOMAIN_HOSTS = (
    "www.facebook.com",
    "m.facebook.com",
    "upload.facebook.com",
    "web.facebook.com",
)


def strip_facebook_page_url_for_capsolver(page_url: str) -> str:
    """
    Lấy URL trình duyệt, bỏ ``?encrypted_context=...`` và hash — giữ scheme/host/path.

    Ví dụ: ``.../authentication/?encrypted_context=AWS...`` → ``.../authentication``
    """
    from urllib.parse import urlparse

    raw = str(page_url or "").strip()
    if not raw:
        return ""
    try:
        p = urlparse(raw)
        if p.scheme and p.netloc:
            path = p.path or ""
            base = f"{p.scheme}://{p.netloc}{path}"
            return base.rstrip("/") if path and path != "/" else base
    except Exception:
        pass
    return raw.split("?")[0].split("#")[0].rstrip("/")


def capsolver_website_url_for_task(website_url: str, *, proxyless: bool) -> str:
    """
    Chuẩn hóa URL trước khi gửi CapSolver (lớp phòng thủ — attempt đã chọn URL ở ``facebook_recaptcha``).

    ProxyLess: bỏ query; không ép www nếu caller đã truyền URL trang.
    Ghi đè: ``FB_CAPSOLVER_WEBSITE_URL=https://m.facebook.com/...``
    """
    raw = str(website_url or "").strip()
    if not proxyless:
        if "facebook.com" in raw.lower():
            return strip_facebook_page_url_for_capsolver(raw) or raw
        return raw
    low = raw.lower()
    if "facebook.com" not in low and "fb.com" not in low:
        return raw or CAPSOLVER_FB_PROXYLESS_WEBSITE_URL
    override = os.environ.get("FB_CAPSOLVER_WEBSITE_URL", "").strip()
    if override and "facebook.com" in override.lower():
        u = override.rstrip("/")
        return u if u.startswith("http") else f"https://{u}"
    stripped = strip_facebook_page_url_for_capsolver(raw)
    return stripped or CAPSOLVER_FB_PROXYLESS_WEBSITE_URL


class CapSolverError(RuntimeError):
    """Lỗi API CapSolver."""


def _coerce_proxy_port(value: Any) -> int:
    """CapSolver yêu cầu ``proxyPort`` là số nguyên, không phải chuỗi."""
    if value is None or value == "":
        return 0
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value if value > 0 else 0
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return 0


def _sanitize_proxy_credential(value: Any) -> str:
    """User/Pass proxy — chỉ chuỗi; không gửi bool/None lên API."""
    if value is None or isinstance(value, bool):
        return ""
    return str(value).strip()


def proxy_config_to_capsolver_task_fields(proxy: dict[str, Any]) -> dict[str, Any]:
    """
    CapSolver định dạng 1: ``proxyType`` + ``proxyAddress`` + ``proxyPort`` (+ auth).

    - ``proxyType``: ``socks5`` / ``socks4`` / ``http`` (chữ thường).
    - ``proxyPort``: ``int`` (không gửi string).
    - Ưu tiên User/Pass — tránh IP Whitelist chặn máy chủ CapSolver (Châu Âu/DC).
    """
    from src.utils.proxy_check import _strip_proxy_scheme

    host_raw = str(proxy.get("host") or "").strip()
    scheme = str(proxy.get("scheme_hint") or "").strip().lower()
    if not scheme:
        low = host_raw.lower()
        if low.startswith("socks5"):
            scheme = "socks5"
        elif low.startswith("socks4"):
            scheme = "socks4"
        elif low.startswith("http"):
            scheme = "http"
        else:
            scheme = "socks5"
    if scheme in ("socks5h", "socks5a"):
        scheme = "socks5"
    if scheme == "socks4a":
        scheme = "socks4"
    if scheme not in ("http", "https", "socks4", "socks5"):
        scheme = "socks5"
    host = _strip_proxy_scheme(host_raw)
    port = _coerce_proxy_port(proxy.get("port"))
    user = _sanitize_proxy_credential(proxy.get("user"))
    password = _sanitize_proxy_credential(proxy.get("pass"))
    if not host or port <= 0:
        return {}
    out: dict[str, Any] = {
        "proxyType": scheme,
        "proxyAddress": host,
        "proxyPort": port,
    }
    if user:
        out["proxyLogin"] = user
        out["proxyPassword"] = password
    return out


def proxy_url_to_capsolver_task_fields(proxy_url: str) -> dict[str, Any]:
    """
    Bóc tách ``socks5://host:port`` hoặc ``socks5://user:pass@host:port`` → structured fields.

    Không dùng chuỗi ``proxy`` gộp trên task JSON.
    """
    from urllib.parse import unquote, urlparse

    u = str(proxy_url or "").strip()
    if not u:
        return {}
    if "://" not in u:
        u = f"socks5://{u}"
    p = urlparse(u)
    scheme = (p.scheme or "socks5").lower()
    if scheme in ("socks5h", "socks5a"):
        scheme = "socks5"
    if scheme == "socks4a":
        scheme = "socks4"
    if scheme not in ("http", "https", "socks4", "socks5"):
        scheme = "socks5"
    host = (p.hostname or "").strip()
    port = _coerce_proxy_port(p.port or (1080 if scheme.startswith("socks") else 80))
    if not host or port <= 0:
        return {}
    return proxy_config_to_capsolver_task_fields(
        {
            "host": host,
            "port": port,
            "user": unquote(p.username or ""),
            "pass": unquote(p.password or ""),
            "scheme_hint": scheme,
        }
    )


def _capsolver_task_has_proxy(task: dict[str, Any]) -> bool:
    return bool(task.get("proxy") or task.get("proxyAddress"))


def _apply_capsolver_proxy(
    task: dict[str, Any],
    *,
    proxy: str | None,
    proxy_config: dict[str, Any] | None,
) -> None:
    """Gắn proxy — **chỉ** ``proxyType`` / ``proxyAddress`` / ``proxyPort`` (+ login nếu có)."""
    task.pop("proxy", None)
    px_cfg = proxy_config if isinstance(proxy_config, dict) else None
    fields = proxy_config_to_capsolver_task_fields(px_cfg) if px_cfg else {}
    if not fields and proxy:
        fields = proxy_url_to_capsolver_task_fields(str(proxy))
    if fields:
        task.update(fields)
        if not fields.get("proxyLogin"):
            logger.warning(
                "[CapSolver] Proxy không có user/pass — nếu nhà cung cấp bật IP Whitelist, "
                "máy chủ CapSolver (IP nước ngoài) sẽ bị CONNECT_REFUSED. "
                "Chuyển sang xác thực User:Pass hoặc bật FB_CAPSOLVER_HYBRID_PROXYLESS=1."
            )
        return
    if proxy:
        logger.error(
            "[CapSolver] Không parse được proxy structured từ config/URL — bỏ qua proxy trên task."
        )


def _redact_capsolver_proxy_log(task: dict[str, Any]) -> str:
    if task.get("proxy"):
        raw = str(task["proxy"])
        parts = raw.split(":")
        if len(parts) >= 3:
            return f"{parts[0]}:{parts[1]}:{parts[2]}:***"
        return raw[:48]
    ptype = task.get("proxyType")
    addr = task.get("proxyAddress")
    port = task.get("proxyPort")
    if ptype and addr:
        return f"{ptype}://{addr}:{port} auth={bool(task.get('proxyLogin'))}"
    return ""


def proxy_url_to_capsolver_format(proxy_url: str) -> str:
    """
    Chuyển URL proxy ToolFB → ``scheme:host:port[:user:pass]`` (định dạng CapSolver).
    """
    from urllib.parse import unquote, urlparse

    u = str(proxy_url or "").strip()
    if not u:
        return ""
    if "://" not in u:
        u = f"http://{u}"
    p = urlparse(u)
    scheme = (p.scheme or "http").lower()
    if scheme in ("socks5h", "socks5a"):
        scheme = "socks5"
    if scheme == "socks4a":
        scheme = "socks4"
    host = (p.hostname or "").strip()
    if host.startswith("socks5://") or host.startswith("socks4://"):
        inner = urlparse(host if "://" in host else f"{scheme}://{host}")
        host = inner.hostname or host.split("://", 1)[-1].split(":")[0]
        if not p.port and inner.port:
            port = inner.port
        else:
            port = p.port or (1080 if scheme.startswith("socks") else 80)
    else:
        port = p.port or (1080 if scheme.startswith("socks") else 80)
    user = unquote(p.username or "")
    password = unquote(p.password or "")
    if user:
        return f"{scheme}:{host}:{port}:{user}:{password}"
    return f"{scheme}:{host}:{port}"


def solve_recaptcha_v2(
    *,
    website_url: str,
    website_key: str,
    api_key: str,
    proxy: str | None = None,
    proxy_config: dict[str, Any] | None = None,
    is_invisible: bool = False,
    is_enterprise: bool = False,
    recaptcha_data_s_value: str | None = None,
    page_action: str | None = None,
    api_domain: str | None = None,
    user_agent: str | None = None,
    poll_interval_sec: float = 2.0,
    timeout_sec: float = 120.0,
) -> dict[str, Any]:
    """
    Giải reCAPTCHA v2 qua CapSolver.

    Returns:
        ``solution`` dict (``gRecaptchaResponse``, ``recaptcha-ca-e``, …).

    Raises:
        CapSolverError: Thiếu key, task lỗi hoặc hết thời gian chờ.
    """
    key = str(api_key or "").strip()
    if not key:
        raise CapSolverError("Thiếu CapSolver API key (TOOLFB_CAPSOLVER_API_KEY hoặc config/app_secrets.json).")
    site = str(website_key or "").strip()
    url = str(website_url or "").strip()
    if not site or not url:
        raise CapSolverError("Thiếu websiteKey hoặc websiteURL cho reCAPTCHA.")

    px_fields = (
        proxy_config_to_capsolver_task_fields(proxy_config or {})
        if isinstance(proxy_config, dict)
        else {}
    )
    if not px_fields and proxy:
        px_fields = proxy_url_to_capsolver_task_fields(str(proxy))
    use_proxy = bool(px_fields)
    url = capsolver_website_url_for_task(url, proxyless=not use_proxy)
    task_type = "ReCaptchaV2Task" if use_proxy else "ReCaptchaV2TaskProxyLess"
    if is_enterprise:
        task_type = "ReCaptchaV2EnterpriseTask" if use_proxy else "ReCaptchaV2EnterpriseTaskProxyLess"
    task: dict[str, Any] = {
        "type": task_type,
        "websiteURL": url,
        "websiteKey": site,
    }
    if use_proxy:
        _apply_capsolver_proxy(task, proxy=proxy, proxy_config=proxy_config)
        if not task.get("proxyAddress"):
            raise CapSolverError(
                "Proxy CapSolver không hợp lệ — cần proxyType/proxyAddress/proxyPort (int) tách riêng, "
                "không gửi chuỗi socks5://... gộp một field."
            )
    if is_invisible:
        task["isInvisible"] = True
    if page_action:
        task["pageAction"] = str(page_action).strip()
    if api_domain:
        task["apiDomain"] = str(api_domain).strip()
    ua = str(user_agent or "").strip()
    if ua:
        task["userAgent"] = ua
    s_value = str(recaptcha_data_s_value or "").strip()
    if s_value:
        if is_enterprise:
            task["enterprisePayload"] = {"s": s_value}
        else:
            task["recaptchaDataSValue"] = s_value

    payload = {"clientKey": key, "task": task}
    proxy_log = _redact_capsolver_proxy_log(task)
    logger.info(
        "[CapSolver] Tạo task {} | proxy={} | {} | enterprise={} | enterprisePayload.s={} | ua_len={} | url={}",
        task_type,
        use_proxy,
        proxy_log or "ProxyLess",
        is_enterprise,
        len(s_value),
        len(ua),
        url[:80],
    )
    try:
        r = requests.post(_CAPSOLVER_CREATE, json=payload, timeout=45)
        body_text = r.text
        try:
            status_code = int(getattr(r, "status_code", 200))
        except Exception:
            status_code = 200
        if status_code >= 400:
            raise CapSolverError(f"createTask HTTP {status_code}: {body_text[:400]}")
        created = r.json()
    except CapSolverError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise CapSolverError(f"createTask thất bại: {exc}") from exc

    if int(created.get("errorId", 0)) != 0:
        raise CapSolverError(
            f"createTask: {created.get('errorCode')} — {created.get('errorDescription')}"
        )
    task_id = str(created.get("taskId") or "").strip()
    if not task_id:
        raise CapSolverError(f"createTask không trả taskId: {created}")

    deadline = time.time() + max(10.0, float(timeout_sec))
    while time.time() < deadline:
        time.sleep(max(0.5, float(poll_interval_sec)))
        try:
            rr = requests.post(
                _CAPSOLVER_RESULT,
                json={"clientKey": key, "taskId": task_id},
                timeout=45,
            )
            rr.raise_for_status()
            result = rr.json()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[CapSolver] getTaskResult lỗi tạm: {}", exc)
            continue

        if int(result.get("errorId", 0)) != 0:
            raise CapSolverError(
                f"getTaskResult: {result.get('errorCode')} — {result.get('errorDescription')}"
            )
        status = str(result.get("status") or "").strip().lower()
        if status == "ready":
            solution = result.get("solution")
            if not isinstance(solution, dict):
                raise CapSolverError("CapSolver ready nhưng thiếu solution.")
            token = str(solution.get("gRecaptchaResponse") or "").strip()
            if not token:
                raise CapSolverError("CapSolver không trả gRecaptchaResponse.")
            logger.info("[CapSolver] Đã có token reCAPTCHA (len={})", len(token))
            return solution
        if status == "failed":
            raise CapSolverError(f"CapSolver task failed: {result}")

    raise CapSolverError("Hết thời gian chờ CapSolver giải reCAPTCHA.")
