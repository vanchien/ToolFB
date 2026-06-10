"""Cấu hình API 2Captcha — env hoặc ``config/app_secrets.json``."""

from __future__ import annotations

import os
import time

from src.utils.app_secrets import load_app_secrets

_BALANCE_CACHE: tuple[float, float] | None = None
_BALANCE_CACHE_TTL_SEC = 90.0


def get_twocaptcha_api_key() -> str:
    for name in ("TOOLFB_TWOCAPTCHA_API_KEY", "TWOCAPTCHA_API_KEY", "APIKEY_2CAPTCHA"):
        v = str(os.environ.get(name, "")).strip()
        if v:
            return v
    data = load_app_secrets()
    return str(data.get("twocaptcha_api_key") or data.get("2captcha_api_key") or "").strip()


def twocaptcha_configured() -> bool:
    return bool(get_twocaptcha_api_key())


def captcha_tier_timeout_sec() -> float:
    """Thời gian chờ tối đa mỗi lần gọi API trong một tầng (Meta Enterprise thường cần 60–120s)."""
    raw = str(os.environ.get("FB_CAPTCHA_TIER_TIMEOUT_SEC", "")).strip()
    if not raw:
        data = load_app_secrets()
        raw = str(data.get("captcha_tier_timeout_sec") or "90").strip()
    try:
        v = float(raw)
    except ValueError:
        v = 90.0
    return max(30.0, min(120.0, v))


def captcha_enterprise_tier_timeout_sec() -> float:
    """Timeout riêng cho sitekey Meta Enterprise ``6Le…`` — ít nhất 90s."""
    return max(captcha_tier_timeout_sec(), 90.0)


def twocaptcha_get_balance(*, force_refresh: bool = False) -> float | None:
    """
    Số dư 2Captcha (USD). Cache ngắn để không gọi API mỗi UID.

    Returns:
        Số dư hoặc None nếu lỗi mạng / thiếu key.
    """
    global _BALANCE_CACHE
    key = get_twocaptcha_api_key()
    if not key:
        return None
    now = time.monotonic()
    if not force_refresh and _BALANCE_CACHE is not None:
        bal, ts = _BALANCE_CACHE
        if (now - ts) < _BALANCE_CACHE_TTL_SEC:
            return bal
    from src.services.twocaptcha_client import TwoCaptchaError, fetch_balance

    try:
        bal = fetch_balance(api_key=key)
        _BALANCE_CACHE = (bal, now)
        return bal
    except TwoCaptchaError:
        return None


def twocaptcha_has_sufficient_balance(*, min_usd: float = 0.01) -> bool:
    """False nếu số dư 0 hoặc không đọc được — bỏ qua các tầng 2Captcha."""
    bal = twocaptcha_get_balance()
    if bal is None:
        return True
    return bal >= min_usd


def captcha_prefer_twocaptcha_first() -> bool:
    """
    True = chạy 2Captcha (proxy → ProxyLess) trước CapSolver.

    Mặc định True. Tắt: ``captcha_prefer_twocaptcha: false`` hoặc ``FB_CAPTCHA_2CAPTCHA_FIRST=0``.
    """
    raw = str(os.environ.get("FB_CAPTCHA_2CAPTCHA_FIRST", "")).strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    data = load_app_secrets()
    val = data.get("captcha_prefer_twocaptcha")
    if val is None:
        return True
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("1", "true", "yes", "on")
