"""SOCKS5 authenticated proxy → local HTTP relay for Playwright."""

from __future__ import annotations

import requests

from src.utils.socks5_http_relay import Socks5HttpRelay, socks_proxy_needs_http_relay


def test_socks_proxy_needs_relay() -> None:
    assert socks_proxy_needs_http_relay(
        {"host": "socks5://1.2.3.4", "port": 1080, "user": "u", "pass": "p"}
    )
    assert not socks_proxy_needs_http_relay({"host": "socks5://1.2.3.4", "port": 1080})
    assert not socks_proxy_needs_http_relay({"host": "203.1.1.1", "port": 8080, "user": "u"})


def test_relay_forwards_to_ipify() -> None:
    relay = Socks5HttpRelay(
        "203.175.96.175",
        25308,
        username="admin254",
        password="admin254",
    )
    relay.start()
    try:
        r = requests.get(
            "https://api.ipify.org",
            proxies={"http": relay.local_url, "https": relay.local_url},
            timeout=25,
        )
        r.raise_for_status()
        assert r.text.strip() == "203.175.96.175"
    finally:
        relay.stop()
