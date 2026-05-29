"""Sinh mã TOTP (Google Authenticator / Authy) — secret do user cấu hình."""

from __future__ import annotations

import re

from loguru import logger

try:
    import pyotp
except ImportError:  # pragma: no cover
    pyotp = None  # type: ignore[assignment]


def normalize_totp_secret(secret: str) -> str:
    s = str(secret or "").strip().replace(" ", "").upper()
    s = re.sub(r"[^A-Z2-7=]", "", s)
    return s


def generate_totp_code(secret: str) -> str:
    """Sinh mã TOTP 6 chữ số hiện tại."""
    norm = normalize_totp_secret(secret)
    if not norm:
        return ""
    if pyotp is None:
        logger.error("Thiếu thư viện pyotp — cài: pip install pyotp")
        return ""
    try:
        return str(pyotp.TOTP(norm).now())
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không sinh được TOTP: {}", exc)
        return ""
