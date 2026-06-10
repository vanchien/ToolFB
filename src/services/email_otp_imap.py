"""
Lấy mã OTP Facebook qua IMAP (checkpoint xác minh email).

Dùng thư viện chuẩn ``imaplib`` — không lưu mật khẩu trong code.
"""

from __future__ import annotations

import imaplib
import re
import time
from email import message_from_bytes
from typing import Any

from loguru import logger

_FB_OTP_RE = re.compile(r"\b(\d{5,8})\b")
_FB_FROM_MARKERS = ("facebook", "facebookmail", "meta", "account.meta")


def _guess_imap_host(email: str) -> str:
    domain = (email.split("@")[-1] if "@" in email else "").lower()
    hosts = {
        "gmail.com": "imap.gmail.com",
        "googlemail.com": "imap.gmail.com",
        "outlook.com": "outlook.office365.com",
        "hotmail.com": "outlook.office365.com",
        "live.com": "outlook.office365.com",
        "yahoo.com": "imap.mail.yahoo.com",
    }
    return hosts.get(domain, f"imap.{domain}" if domain else "")


def _message_from_facebook(msg: Any) -> bool:
    from_hdr = str(msg.get("From", "")).lower()
    return any(m in from_hdr for m in _FB_FROM_MARKERS)


def _extract_otp_from_text(text: str) -> str:
    if not text:
        return ""
    for pat in (
        r"(?:code|mã|confirmation|xác nhận)[^\d]{0,40}(\d{5,8})",
        r"(\d{6})\s+is your",
        r"(\d{6})\s+là mã",
    ):
        m = re.search(pat, text, re.I)
        if m:
            return m.group(1)
    nums = _FB_OTP_RE.findall(text)
    for n in nums:
        if len(n) in (5, 6, 8):
            return n
    return ""


def fetch_facebook_email_otp(
    email: str,
    password: str,
    *,
    imap_host: str = "",
    timeout_sec: float = 45.0,
    poll_interval_sec: float = 3.0,
) -> str:
    """
    Đọc hộp thư IMAP, tìm thư Facebook mới nhất và trích OTP.

    Args:
        email: Địa chỉ đăng nhập IMAP.
        password: Mật khẩu ứng dụng / mật khẩu email.
        imap_host: Host IMAP; rỗng → đoán theo domain.
        timeout_sec: Thời gian chờ tối đa.
        poll_interval_sec: Khoảng cách giữa các lần quét.

    Returns:
        Chuỗi OTP hoặc rỗng nếu không tìm thấy.
    """
    em = str(email or "").strip()
    pw = str(password or "").strip()
    if not em or not pw:
        return ""

    host = str(imap_host or "").strip() or _guess_imap_host(em)
    if not host:
        logger.warning("[IMAP] Không đoán được host cho {}", em)
        return ""

    deadline = time.monotonic() + max(5.0, float(timeout_sec))
    last_err: Exception | None = None

    while time.monotonic() < deadline:
        try:
            mail = imaplib.IMAP4_SSL(host, timeout=20)
            mail.login(em, pw)
            mail.select("INBOX")
            _typ, data = mail.search(None, "ALL")
            if not data or not data[0]:
                mail.logout()
                time.sleep(poll_interval_sec)
                continue
            ids = data[0].split()
            for mid in reversed(ids[-15:]):
                _typ, msg_data = mail.fetch(mid, "(RFC822)")
                if not msg_data or not msg_data[0]:
                    continue
                raw = msg_data[0][1]
                if not isinstance(raw, (bytes, bytearray)):
                    continue
                msg = message_from_bytes(raw)
                if not _message_from_facebook(msg):
                    continue
                parts: list[str] = []
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            payload = part.get_payload(decode=True)
                            if isinstance(payload, bytes):
                                parts.append(payload.decode(errors="ignore"))
                else:
                    payload = msg.get_payload(decode=True)
                    if isinstance(payload, bytes):
                        parts.append(payload.decode(errors="ignore"))
                otp = _extract_otp_from_text("\n".join(parts))
                mail.logout()
                if otp:
                    logger.info("[IMAP] Đã lấy OTP từ thư Facebook ({} ký tự).", len(otp))
                    return otp
            mail.logout()
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            logger.debug("[IMAP] Lỗi tạm: {}", exc)
        time.sleep(poll_interval_sec)

    if last_err:
        logger.warning("[IMAP] Hết thời gian chờ OTP: {}", last_err)
    return ""
