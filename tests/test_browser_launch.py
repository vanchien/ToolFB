"""
Kiểm tra khởi chạy profile portable và proxy qua Playwright.

Chạy tích hợp: ``pytest tests/test_browser_launch.py -m integration`` (cần ``playwright install chromium``).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from loguru import logger

from src.automation.browser_factory import (
    BrowserFactory,
    _project_root,
    account_use_proxy_enabled,
    format_proxy_server_url,
    playwright_proxy_settings,
    proxy_host_port_configured,
)
from src.utils.db_manager import AccountsDatabaseManager


def test_format_proxy_server_url_http_basic() -> None:
    """Không user/pass: server chỉ là http://host:port."""
    url = format_proxy_server_url(
        {"host": "10.0.0.5", "port": 8888, "user": "", "pass": ""},
    )
    assert url == "http://10.0.0.5:8888"


def test_format_proxy_server_url_encodes_credentials() -> None:
    """Có user/pass: dạng http://user:pass@host:port với ký tự đặc biệt được encode."""
    url = format_proxy_server_url(
        {"host": "proxy.local", "port": 1080, "user": "user@dom", "pass": "p:ass"},
    )
    assert url.startswith("http://")
    assert "proxy.local:1080" in url
    assert "@" in url
    assert "user@dom" not in url


def test_playwright_proxy_settings_http_basic() -> None:
    s = playwright_proxy_settings({"host": "10.0.0.5", "port": 8888, "user": "", "pass": ""})
    assert s == {"server": "http://10.0.0.5:8888"}


def test_playwright_proxy_settings_strips_http_prefix_on_host() -> None:
    s = playwright_proxy_settings({"host": "http://10.0.0.5", "port": 8888, "user": "", "pass": ""})
    assert s["server"] == "http://10.0.0.5:8888"


def test_playwright_proxy_settings_splits_credentials() -> None:
    s = playwright_proxy_settings(
        {"host": "proxy.local", "port": 1080, "user": "user@dom", "pass": "p:ass"},
    )
    assert s["server"] == "http://proxy.local:1080"
    assert s["username"] == "user@dom"
    assert s["password"] == "p:ass"


def test_playwright_proxy_settings_socks5_appends_port_when_missing() -> None:
    s = playwright_proxy_settings({"host": "socks5://203.0.113.9", "port": 1080, "user": "", "pass": ""})
    assert s == {"server": "socks5://203.0.113.9:1080"}


def test_account_use_proxy_and_host_port_flags() -> None:
    assert account_use_proxy_enabled({}) is True
    assert account_use_proxy_enabled({"use_proxy": False}) is False
    assert account_use_proxy_enabled({"use_proxy": "false"}) is False
    assert proxy_host_port_configured({"host": "10.0.0.1", "port": 8080, "user": "", "pass": ""})
    assert not proxy_host_port_configured({"host": "", "port": 0, "user": "", "pass": ""})


@pytest.mark.integration
def test_persistent_context_uses_portable_profile_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Mở persistent context theo account mẫu và xác minh Chromium ghi dữ liệu vào đúng thư mục profile.

    Dùng ``about:blank`` để tránh phụ thuộc proxy hoạt động cho bước khởi tạo profile.
    """
    monkeypatch.setenv("FB_PLAYWRIGHT_CHROMIUM_CHANNEL", "bundled")
    mgr = AccountsDatabaseManager()
    acc = mgr.get_by_id("acc_demo_001")
    assert acc is not None
    portable = (_project_root() / str(acc["portable_path"])).resolve()
    portable.mkdir(parents=True, exist_ok=True)

    factory = BrowserFactory(headless=True)
    try:
        ctx = factory.get_browser_context("acc_demo_001", headless=True)
        try:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto("about:blank", timeout=30_000)
            page.wait_for_load_state("domcontentloaded")
            # Chromium ghi user-data không đồng bộ; chờ một chút để tránh race khi đóng context.
            for _ in range(50):
                default_dir = portable / "Default"
                session_marker = default_dir / "Session Storage" / "CURRENT"
                if default_dir.is_dir() and session_marker.is_file():
                    break
                page.wait_for_timeout(100)
            else:
                pytest.fail(
                    "Không thấy cây profile Chromium (Default/Session Storage) trong portable_path: "
                    f"{portable}"
                )
            logger.info("Profile portable hợp lệ (có cache/session Chromium) tại: {}", portable)
        finally:
            ctx.close()
    finally:
        factory.close()


@pytest.mark.integration
def test_ipify_shows_ip_through_configured_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Truy cập api.ipify.org qua proxy trong accounts.json và ghi log IP trả về.

    Nếu proxy mẫu không kết nối được, bỏ qua test với lý do rõ ràng (tránh fail CI mặc định).
    """
    monkeypatch.setenv("FB_PLAYWRIGHT_CHROMIUM_CHANNEL", "bundled")
    mgr = AccountsDatabaseManager()
    acc = mgr.get_by_id("acc_demo_002")
    assert acc is not None
    proxy = acc["proxy"]
    assert isinstance(proxy, dict)
    server = format_proxy_server_url(proxy)
    logger.info("Đang kiểm tra proxy server (đã rút gọn): {}", _safe_proxy_log(server))

    factory = BrowserFactory(headless=True)
    try:
        ctx = factory.get_browser_context("acc_demo_002", headless=True)
        try:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                page.goto("https://api.ipify.org", timeout=60_000, wait_until="domcontentloaded")
            except Exception as exc:  # noqa: BLE001 — muốn bỏ qua mềm khi proxy die
                pytest.skip(f"Không thể mở ipify qua proxy đã cấu hình: {exc}")

            body = page.locator("body").inner_text(timeout=15_000).strip()
            assert re.fullmatch(r"(\d{1,3}\.){3}\d{1,3}", body), f"Phản hồi ipify không giống IPv4: {body!r}"
            logger.info("IP hiện tại (qua ipify, account acc_demo_002): {}", body)
        finally:
            ctx.close()
    finally:
        factory.close()


def _safe_proxy_log(server: str) -> str:
    """
    Tạo chuỗi proxy an toàn cho log (ẩn credentials).

    Args:
        server: URL proxy đầy đủ.

    Returns:
        Chuỗi đã che nhạy cảm.
    """
    if "@" in server:
        head, tail = server.split("@", 1)
        return f"{head.split('://', 1)[0]}://***@{tail}"
    return server
