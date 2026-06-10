"""
Client 2Captcha — in.php / res.php (reCAPTCHA v2 Enterprise + proxy / ProxyLess).

Tài liệu: https://2captcha.com/2captcha-api
"""

from __future__ import annotations

import time
from typing import Any
from urllib.parse import quote

import requests
from loguru import logger

_TWOCAPTCHA_IN = "https://2captcha.com/in.php"
_TWOCAPTCHA_RES = "https://2captcha.com/res.php"

_ZERO_BALANCE_MARKERS = ("ERROR_ZERO_BALANCE", "ZERO_BALANCE", "zero balance")


class TwoCaptchaError(RuntimeError):
    """Lỗi API 2Captcha."""


def _proxy_config_to_twocaptcha_fields(proxy_config: dict[str, Any] | None) -> dict[str, str]:
    """Chuyển proxy ToolFB → tham số 2Captcha (proxy + proxytype)."""
    if not isinstance(proxy_config, dict):
        return {}
    host = str(proxy_config.get("host") or "").strip()
    port = proxy_config.get("port")
    if not host or not port:
        return {}
    try:
        port_i = int(port)
    except (TypeError, ValueError):
        return {}
    scheme = str(proxy_config.get("scheme_hint") or "http").strip().lower()
    if scheme in ("socks5h", "socks5a"):
        scheme = "socks5"
    if scheme == "socks4a":
        scheme = "socks4"
    if scheme not in ("http", "https", "socks4", "socks5"):
        scheme = "http"
    ptype = scheme.upper()
    if ptype == "HTTPS":
        ptype = "HTTP"
    user = str(proxy_config.get("user") or "").strip()
    password = str(proxy_config.get("pass") or "").strip()
    if user:
        proxy_val = f"{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port_i}"
    else:
        proxy_val = f"{host}:{port_i}"
    return {"proxy": proxy_val, "proxytype": ptype}


def fetch_balance(*, api_key: str, timeout_sec: float = 15.0) -> float:
    """``action=getbalance`` — trả số dư USD."""
    key = str(api_key or "").strip()
    if not key:
        raise TwoCaptchaError("Thiếu 2Captcha API key.")
    try:
        r = requests.get(
            _TWOCAPTCHA_RES,
            params={"key": key, "action": "getbalance", "json": 1},
            timeout=timeout_sec,
        )
        data = r.json()
    except Exception as exc:  # noqa: BLE001
        raise TwoCaptchaError(f"getbalance thất bại: {exc}") from exc
    if int(data.get("status", 0)) != 1:
        err = str(data.get("request") or data.get("error_text") or data)
        if any(m in err.upper() for m in _ZERO_BALANCE_MARKERS):
            return 0.0
        raise TwoCaptchaError(f"getbalance: {err}")
    try:
        return float(str(data.get("request", "0")).strip())
    except ValueError as exc:
        raise TwoCaptchaError(f"getbalance không parse được: {data}") from exc


def cancel_task(*, api_key: str, task_id: str) -> None:
    """Hủy task đang chờ (tiết kiệm queue khi timeout tầng)."""
    key = str(api_key or "").strip()
    tid = str(task_id or "").strip()
    if not key or not tid:
        return
    try:
        requests.get(
            _TWOCAPTCHA_RES,
            params={"key": key, "action": "cancel", "id": tid, "json": 1},
            timeout=12,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("[2Captcha] cancel task {}: {}", tid[:12], exc)


def solve_recaptcha_v2_enterprise(
    *,
    website_url: str,
    website_key: str,
    api_key: str,
    proxy_config: dict[str, Any] | None = None,
    recaptcha_data_s_value: str | None = None,
    user_agent: str | None = None,
    page_action: str | None = None,
    is_invisible: bool = False,
    poll_interval_sec: float = 3.0,
    timeout_sec: float = 40.0,
) -> dict[str, Any]:
    """
    Giải reCAPTCHA v2 Enterprise qua 2Captcha.

    Returns:
        ``{"gRecaptchaResponse": "..."}`` (tương thích inject ToolFB).
    """
    key = str(api_key or "").strip()
    if not key:
        raise TwoCaptchaError("Thiếu 2Captcha API key.")
    site = str(website_key or "").strip()
    url = str(website_url or "").strip()
    if not site or not url:
        raise TwoCaptchaError("Thiếu googlekey hoặc pageurl.")

    use_proxy = bool(proxy_config and _proxy_config_to_twocaptcha_fields(proxy_config))
    params: dict[str, Any] = {
        "key": key,
        "method": "userrecaptcha",
        "googlekey": site,
        "pageurl": url,
        "enterprise": 1,
        "json": 1,
    }
    s_val = str(recaptcha_data_s_value or "").strip()
    if s_val:
        params["data-s"] = s_val
    action = str(page_action or "").strip()
    if action:
        params["action"] = action
    if is_invisible:
        params["invisible"] = 1
    ua = str(user_agent or "").strip()
    if ua:
        params["userAgent"] = ua
    if use_proxy:
        params.update(_proxy_config_to_twocaptcha_fields(proxy_config))

    logger.info(
        "[2Captcha] Tạo task userrecaptcha enterprise={} proxy={} s_len={} url={}",
        True,
        use_proxy,
        len(s_val),
        url[:80],
    )
    try:
        r = requests.post(_TWOCAPTCHA_IN, data=params, timeout=30)
        created = r.json()
    except Exception as exc:  # noqa: BLE001
        raise TwoCaptchaError(f"in.php thất bại: {exc}") from exc

    if int(created.get("status", 0)) != 1:
        err = str(created.get("request") or created.get("error_text") or created)
        if any(m in err.upper() for m in _ZERO_BALANCE_MARKERS):
            raise TwoCaptchaError("ERROR_ZERO_BALANCE")
        raise TwoCaptchaError(f"in.php: {err}")

    task_id = str(created.get("request") or "").strip()
    if not task_id.isdigit():
        raise TwoCaptchaError(f"in.php không trả task id: {created}")

    deadline = time.time() + max(10.0, float(timeout_sec))
    while time.time() < deadline:
        time.sleep(max(2.0, float(poll_interval_sec)))
        try:
            rr = requests.get(
                _TWOCAPTCHA_RES,
                params={"key": key, "action": "get", "id": task_id, "json": 1},
                timeout=25,
            )
            result = rr.json()
        except Exception as exc:  # noqa: BLE001
            logger.debug("[2Captcha] poll tạm: {}", exc)
            continue

        if int(result.get("status", 0)) == 1:
            token = str(result.get("request") or "").strip()
            if not token:
                raise TwoCaptchaError("2Captcha ready nhưng thiếu token.")
            logger.info("[2Captcha] Đã có token (len={})", len(token))
            return {"gRecaptchaResponse": token}

        req = str(result.get("request") or "")
        if req == "CAPCHA_NOT_READY":
            continue
        if any(m in req.upper() for m in _ZERO_BALANCE_MARKERS):
            cancel_task(api_key=key, task_id=task_id)
            raise TwoCaptchaError("ERROR_ZERO_BALANCE")
        cancel_task(api_key=key, task_id=task_id)
        raise TwoCaptchaError(f"get: {req}")

    cancel_task(api_key=key, task_id=task_id)
    raise TwoCaptchaError("Hết thời gian chờ 2Captcha (tier timeout).")
