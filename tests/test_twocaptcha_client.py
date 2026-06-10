"""Unit test client 2Captcha (mock HTTP)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.services.twocaptcha_client import (
    TwoCaptchaError,
    _proxy_config_to_twocaptcha_fields,
    fetch_balance,
    solve_recaptcha_v2_enterprise,
)
from src.utils.twocaptcha_config import captcha_tier_timeout_sec, twocaptcha_has_sufficient_balance


def test_proxy_fields_with_auth() -> None:
    fields = _proxy_config_to_twocaptcha_fields(
        {
            "host": "1.2.3.4",
            "port": 8080,
            "user": "u",
            "pass": "p",
            "scheme_hint": "socks5",
        }
    )
    assert fields["proxytype"] == "SOCKS5"
    assert "u" in fields["proxy"] and "1.2.3.4" in fields["proxy"]


@patch("src.services.twocaptcha_client.requests.get")
def test_fetch_balance_ok(mock_get: MagicMock) -> None:
    mock_get.return_value.json.return_value = {"status": 1, "request": "12.34"}
    assert fetch_balance(api_key="k") == 12.34


@patch("src.services.twocaptcha_client.requests.post")
@patch("src.services.twocaptcha_client.requests.get")
def test_solve_poll_success(mock_get: MagicMock, mock_post: MagicMock) -> None:
    mock_post.return_value.json.return_value = {"status": 1, "request": "999"}
    mock_get.return_value.json.side_effect = [
        {"status": 0, "request": "CAPCHA_NOT_READY"},
        {"status": 1, "request": "TOKEN_ABC123"},
    ]
    sol = solve_recaptcha_v2_enterprise(
        website_url="https://www.facebook.com",
        website_key="6LeyIlkaAAAA",
        api_key="key",
        timeout_sec=15.0,
        poll_interval_sec=0.01,
    )
    assert sol["gRecaptchaResponse"] == "TOKEN_ABC123"


def test_tier_timeout_bounds() -> None:
    v = captcha_tier_timeout_sec()
    assert 30.0 <= v <= 120.0


@patch("src.utils.twocaptcha_config.twocaptcha_get_balance", return_value=0.0)
def test_zero_balance_skips_tier(_mock_bal: MagicMock) -> None:
    assert twocaptcha_has_sufficient_balance() is False
