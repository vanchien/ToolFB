"""
Kiểm tra proxy HTTP (Live/Die) bằng request qua ipify — không dùng OAuth Facebook.
"""

from __future__ import annotations

from typing import Tuple
from urllib.parse import quote

import requests
from loguru import logger


def check_http_proxy(
    host: str,
    port: int,
    *,
    user: str = "",
    password: str = "",
    timeout: float = 18.0,
) -> Tuple[bool, str]:
    """
    Thử kết nối HTTPS qua proxy tới ``https://api.ipify.org``.

    Args:
        host: Host proxy.
        port: Cổng proxy.
        user: User auth (có thể rỗng).
        password: Mật khẩu auth.
        timeout: Timeout giây.

    Returns:
        ``(True, ip)`` nếu thành công, ``(False, thông báo lỗi)`` nếu thất bại.
    """
    host = str(host).strip()
    if not host:
        return False, "Thiếu host proxy."
    try:
        if user:
            u = quote(user, safe="")
            p = quote(password, safe="")
            proxy_url = f"http://{u}:{p}@{host}:{int(port)}"
        else:
            proxy_url = f"http://{host}:{int(port)}"
    except (TypeError, ValueError) as exc:
        return False, f"Cổng không hợp lệ: {exc}"

    proxies = {"http": proxy_url, "https": proxy_url}
    try:
        r = requests.get("https://api.ipify.org", proxies=proxies, timeout=timeout)
        r.raise_for_status()
        ip = (r.text or "").strip()
        logger.info("Proxy live, IP công cộng: {}", ip)
        return True, ip
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        logger.warning("Proxy check thất bại: {}", msg)
        return False, msg
