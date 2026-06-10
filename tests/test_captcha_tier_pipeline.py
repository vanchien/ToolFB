"""Pipeline 3 tầng — thứ tự 2Captcha trước."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.services.captcha_tier_pipeline import (
    CaptchaTierResult,
    run_anti_block_captcha_pipeline,
)


class _FakePage:
    url = "https://www.facebook.com/two_step_verification/authentication"


def test_pipeline_twocaptcha_first() -> None:
    kw = {
        "website_url": "https://www.facebook.com/two_step_verification/authentication",
        "website_key": "6LeyIlkaAAAA",
        "proxy_config": {"host": "h", "port": 80},
    }
    tier2_ok = CaptchaTierResult(
        solution={"gRecaptchaResponse": "tok2"},
        provider="twocaptcha_proxy",
        proxyless=False,
    )
    with (
        patch(
            "src.services.captcha_tier_pipeline.captcha_prefer_twocaptcha_first",
            return_value=True,
        ),
        patch(
            "src.services.captcha_tier_pipeline.get_twocaptcha_api_key",
            return_value="2cap-key",
        ),
        patch(
            "src.services.captcha_tier_pipeline._run_twocaptcha_tiers",
            return_value=tier2_ok,
        ) as mock_2c,
        patch(
            "src.services.captcha_tier_pipeline._run_capsolver_fallback",
        ) as mock_cap,
    ):
        r = run_anti_block_captcha_pipeline(
            _FakePage(),
            kw,
            {"use_proxy": True},
            page_url=kw["website_url"],
        )
    assert r is not None
    assert r.provider == "twocaptcha_proxy"
    mock_2c.assert_called_once()
    mock_cap.assert_not_called()


def test_pipeline_twocaptcha_fail_then_capsolver() -> None:
    kw = {
        "website_url": "https://www.facebook.com",
        "website_key": "6LeyIlkaAAAA",
        "proxy_config": {"host": "h", "port": 80},
    }
    cap_ok = CaptchaTierResult(
        solution={"gRecaptchaResponse": "tok1"},
        provider="capsolver",
        proxyless=False,
    )
    with (
        patch(
            "src.services.captcha_tier_pipeline.captcha_prefer_twocaptcha_first",
            return_value=True,
        ),
        patch(
            "src.services.captcha_tier_pipeline.get_twocaptcha_api_key",
            return_value="2cap-key",
        ),
        patch(
            "src.services.captcha_tier_pipeline._run_twocaptcha_tiers",
            return_value=None,
        ),
        patch(
            "src.services.captcha_tier_pipeline._run_capsolver_fallback",
            return_value=cap_ok,
        ) as mock_cap,
    ):
        r = run_anti_block_captcha_pipeline(
            _FakePage(), kw, {}, page_url=kw["website_url"]
        )
    assert r is not None
    assert r.provider == "capsolver"
    mock_cap.assert_called_once()
