"""Cấu hình API key CapSolver — env hoặc ``config/app_secrets.json``."""

from __future__ import annotations

import os

from src.utils.app_secrets import load_app_secrets


def get_capsolver_api_key() -> str:
    for name in ("TOOLFB_CAPSOLVER_API_KEY", "CAPSOLVER_API_KEY"):
        v = str(os.environ.get(name, "")).strip()
        if v:
            return v
    data = load_app_secrets()
    return str(data.get("capsolver_api_key") or "").strip()


def capsolver_configured() -> bool:
    return bool(get_capsolver_api_key())


def capsolver_auto_solve_enabled() -> bool:
    raw = str(os.environ.get("TOOLFB_CAPSOLVER_AUTO_SOLVE", "")).strip().lower()
    if raw in ("0", "false", "off", "no"):
        return False
    if raw in ("1", "true", "on", "yes"):
        return capsolver_configured()
    return capsolver_configured()


def capsolver_use_account_proxy_setting() -> bool | None:
    """
    CapSolver có gửi proxy tài khoản lên API hay chỉ ProxyLess (IP mạng máy).

    Returns:
        True — luôn dùng proxy khi account ``use_proxy``.
        False — bỏ proxy CapSolver (test IP thường / tránh CONNECT_REFUSED).
        None — mặc định: dùng proxy nếu account bật ``use_proxy``.
    """
    forced = str(os.environ.get("FB_CAPSOLVER_USE_ACCOUNT_PROXY", "")).strip().lower()
    if forced in ("1", "true", "yes", "on"):
        return True
    if forced in ("0", "false", "no", "off"):
        return False
    data = load_app_secrets()
    raw = data.get("capsolver_use_account_proxy")
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def capsolver_skip_meta_enterprise() -> bool:
    """
    Không gọi CapSolver cho sitekey Meta Enterprise (6Le…) — chỉ captcha tay trên Firefox.

    Bật: ``FB_CAPSOLVER_SKIP_META=1`` hoặc ``capsolver_skip_meta_enterprise: true`` trong app_secrets.
    """
    raw = str(os.environ.get("FB_CAPSOLVER_SKIP_META", "")).strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    data = load_app_secrets()
    val = data.get("capsolver_skip_meta_enterprise")
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("1", "true", "yes", "on")
