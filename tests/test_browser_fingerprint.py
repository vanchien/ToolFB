"""Tests browser fingerprint helpers."""

from __future__ import annotations

import platform

from src.utils.browser_fingerprint import (
    default_desktop_firefox_user_agent,
    firefox_anti_detection_user_prefs,
    merge_firefox_user_prefs,
    resolve_browser_locale,
    resolve_browser_timezone,
)


def test_default_firefox_ua_matches_windows() -> None:
    ua = default_desktop_firefox_user_agent()
    assert "Firefox" in ua
    if platform.system() == "Windows":
        assert "Windows NT" in ua


def test_firefox_anti_detection_prefs_webdriver_off() -> None:
    prefs = firefox_anti_detection_user_prefs()
    assert prefs.get("dom.webdriver.enabled") is False
    assert prefs.get("media.peerconnection.enabled") is False


def test_merge_firefox_user_prefs_keeps_existing() -> None:
    merged = merge_firefox_user_prefs({"dom.webnotifications.enabled": False})
    assert merged["dom.webnotifications.enabled"] is False
    assert merged["dom.webdriver.enabled"] is False


def test_resolve_locale_timezone_from_account() -> None:
    acc = {"locale": "en-US", "timezone": "America/New_York"}
    assert resolve_browser_locale(acc) == "en-US"
    assert resolve_browser_timezone(acc) == "America/New_York"


def test_resolve_timezone_from_geo() -> None:
    geo = {"timezone": "Europe/Berlin", "country_code": "DE", "locale": "de-DE"}
    assert resolve_browser_timezone(None, geo=geo) == "Europe/Berlin"
    assert resolve_browser_locale(None, geo=geo) == "de-DE"
