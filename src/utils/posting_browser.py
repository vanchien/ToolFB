"""
Chọn engine luồng đăng bài theo ``browser_type`` của tài khoản (profile portable).

- ``firefox``: profile Gecko / Playwright Firefox — nhánh UI đăng bài Firefox trong job.
- ``chromium``: Chrome/Chromium/WebKit (đăng bài kiểu Chromium mặc định).
"""

from __future__ import annotations

from typing import Literal

PostingBrowserEngine = Literal["chromium", "firefox"]


def resolve_posting_browser_engine(account: dict | None) -> PostingBrowserEngine:
    """
    Map ``accounts.json`` → engine dùng cho pipeline đăng (Playwright đã mở đúng browser).

    Phiên bản hiện tại ưu tiên Firefox; giá trị không rõ / thiếu → ``firefox``.

    Args:
        account: Bản ghi tài khoản (dict) hoặc None.

    Returns:
        ``firefox`` hoặc ``chromium``.
    """
    if not account:
        return "firefox"
    _raw_bt = account.get("browser_type", "firefox")
    bt = str(_raw_bt if _raw_bt is not None else "firefox").strip().lower()
    if not bt:
        return "firefox"
    if bt in ("firefox", "ff"):
        return "firefox"
    if bt in ("chromium", "chrome", "chrome-ms", "msedge", "edge", "webkit"):
        return "chromium"
    return "firefox"
