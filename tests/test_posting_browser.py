"""Mapping ``browser_type`` tài khoản → engine luồng đăng bài."""

from __future__ import annotations

import pytest

from src.utils.posting_browser import resolve_posting_browser_engine


@pytest.mark.parametrize(
    ("browser_type", "expected"),
    [
        ("firefox", "firefox"),
        ("FF", "firefox"),
        ("ff", "firefox"),
        ("chromium", "chromium"),
        ("chrome", "chromium"),
        ("chrome-ms", "chromium"),
        ("msedge", "chromium"),
        ("edge", "chromium"),
        ("webkit", "chromium"),
        ("", "firefox"),
        ("unknown_browser", "firefox"),
    ],
)
def test_resolve_posting_browser_engine(browser_type: str, expected: str) -> None:
    assert resolve_posting_browser_engine({"browser_type": browser_type}) == expected


def test_resolve_none_defaults_firefox() -> None:
    assert resolve_posting_browser_engine(None) == "firefox"


def test_resolve_missing_key_defaults_firefox() -> None:
    assert resolve_posting_browser_engine({"id": "x"}) == "firefox"
