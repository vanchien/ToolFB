"""Tests for proxy parsing and scheme detection."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.utils.proxy_check import (
    build_playwright_proxy_settings,
    check_proxy,
    check_proxy_line,
    format_proxy_line,
    parse_proxy_line,
    playwright_host_for_scheme,
    verify_browser_facebook_via_proxy,
)


def test_parse_proxy_line_four_parts() -> None:
    px = parse_proxy_line("203.175.96.175:25308:admin254:admin254")
    assert px["host"] == "203.175.96.175"
    assert px["port"] == 25308
    assert px["user"] == "admin254"


def test_parse_proxy_line_socks5_url() -> None:
    px = parse_proxy_line("socks5://1.2.3.4:1080:user:pass")
    assert px["scheme_hint"] == "socks5"
    assert "1.2.3.4" in px["host"]


def test_parse_proxy_line_http_auth_url() -> None:
    px = parse_proxy_line("http://user:secret@203.0.0.1:8080")
    assert px["scheme_hint"] == "http"
    assert px["port"] == 8080


def test_playwright_host_socks5() -> None:
    assert playwright_host_for_scheme("203.175.96.175", "socks5") == "socks5://203.175.96.175"


def test_build_playwright_proxy_settings_socks5() -> None:
    cfg = build_playwright_proxy_settings(
        {"host": "socks5://1.2.3.4", "port": 1080, "user": "u", "pass": "p"}
    )
    assert cfg["server"].startswith("socks5://")
    assert cfg["username"] == "u"


def test_build_playwright_proxy_settings_socks4_maps_to_socks5() -> None:
    cfg = build_playwright_proxy_settings({"host": "socks4://9.9.9.9", "port": 1080})
    assert cfg["server"].startswith("socks5://")


def test_check_proxy_line_formats_output() -> None:
    with patch(
        "src.utils.proxy_check.check_proxy",
        return_value=(True, "1.2.3.4", "socks5"),
    ):
        ok, _msg, scheme, px = check_proxy_line("203.0.0.1:1080:u:p", timeout=5)
    assert ok
    assert scheme == "socks5"
    line = format_proxy_line(px, "socks5")
    assert line.lower().startswith("socks5://")


def test_verify_browser_facebook_via_proxy_ok() -> None:
    page = MagicMock()
    page.url = "https://www.facebook.com/"
    page.content.return_value = "<html>facebook</html>"
    ok, msg = verify_browser_facebook_via_proxy(page)
    assert ok
    assert "OK" in msg


def test_verify_browser_facebook_via_proxy_neterror() -> None:
    page = MagicMock()
    page.url = "about:neterror?e=proxy"
    page.content.return_value = ""
    ok, msg = verify_browser_facebook_via_proxy(page)
    assert not ok
    assert "neterror" in msg.lower() or "proxy" in msg.lower()


def test_user_proxy_socks5_live() -> None:
    """Proxy mẫu user — SOCKS5, không phải HTTP."""
    ok, ip, scheme = check_proxy(
        "203.175.96.175",
        25308,
        user="admin254",
        password="admin254",
        timeout=25.0,
    )
    assert ok, f"expected LIVE: {ip} {scheme}"
    assert scheme == "socks5"
    assert ip == "203.175.96.175"
