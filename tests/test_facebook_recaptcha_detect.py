"""Phát hiện sitekey reCAPTCHA trên HTML tĩnh."""

from __future__ import annotations

import os

from src.services.facebook_recaptcha import (
    _build_capsolver_attempts,
    _capsolver_split_proxy_mode,
    _capsolver_use_account_proxy,
    _capsolver_website_urls,
    _is_checkpoint_captcha_page,
    _extract_s_from_html,
    _extract_s_from_url,
    _is_google_recaptcha_network_url,
    _meta_enterprise_page,
    _meta_enterprise_sitekey,
    _network_ingest_url,
    _new_network_capture_store,
    _scan_frames_for_recaptcha_params,
    _capsolver_tier_disabled,
    facebook_page_may_need_recaptcha,
    facebook_page_on_recaptcha_flow_url,
    is_plain_facebook_login_url,
    facebook_recaptcha_task_urls,
    extract_recaptcha_params,
    get_fresh_network_s,
    reset_recaptcha_network_capture,
)


class _FakeFrame:
    def __init__(self, url: str = "") -> None:
        self.url = url


class _FakeContext:
    def __init__(self) -> None:
        self.pages = []
        self._handlers: dict[str, list] = {}

    def on(self, event: str, handler) -> None:  # noqa: ANN001
        self._handlers.setdefault(event, []).append(handler)


class _FakePage:
    def __init__(
        self,
        html: str,
        url: str = "https://www.facebook.com/checkpoint",
        *,
        body_text: str = "",
    ) -> None:
        self._html = html
        self.url = url
        self.frames = [_FakeFrame()]
        self._body_text = body_text
        self.context = _FakeContext()
        self.main_frame = self.frames[0]

    def content(self) -> str:
        return self._html

    def evaluate(self, _script: str) -> dict:
        return {"sitekey": "", "invisible": False, "enterprise": False, "action": "", "s": ""}

    def locator(self, _sel: str):
        class _Loc:
            def __init__(self, text: str) -> None:
                self._text = text

            def count(self) -> int:
                return 1 if self._text else 0

            def inner_text(self, timeout: int = 0) -> str:  # noqa: ARG002
                return self._text

        if _sel == "body":
            return _Loc(self._body_text)
        return _Loc("")


def test_extract_sitekey_from_data_attribute() -> None:
    html = '<div class="g-recaptcha" data-sitekey="6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe"></div>'
    page = _FakePage(html)
    params = extract_recaptcha_params(page)
    assert params is not None
    assert params["website_key"] == "6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe"


def test_meta_enterprise_detected_from_vietnamese_body() -> None:
    page = _FakePage(
        "<html></html>",
        body_text="Chúng tôi đã sử dụng sản phẩm reCAPTCHA Enterprise của Google. Tôi không phải là người máy.",
    )
    assert _meta_enterprise_page(page) is True


def test_capsolver_urls_full_page_first_for_2fa() -> None:
    long_url = (
        "https://www.facebook.com/two_step_verification/authentication/"
        "?encrypted_context=AWSrSvuHWufaygooCat7nrUftM7xN9LYceE1HzLqR51ApDKb25mcaemv4bGgNSOqoT_MbZnzoLpjeIHoUdDs8R5hmLBVgKYoGlY"
        "&flow=pre_authentication&next"
    )
    urls = _capsolver_website_urls(long_url)
    assert urls[0] == "https://www.facebook.com/two_step_verification/authentication"
    assert "encrypted_context" not in urls[0]
    assert any("m.facebook.com" in u for u in urls)


def test_strip_fb_url_removes_encrypted_context_query() -> None:
    from src.services.capsolver_client import strip_facebook_page_url_for_capsolver

    long_url = (
        "https://www.facebook.com/two_step_verification/authentication/"
        "?encrypted_context=AWSrSvuHWufaygooCat7nrUftM7xN9LYceE1HzLqR51ApDKb25mcaemv4bGgNSOqoT_MbZnzoLpjeIHoUdDs8R5hmLBVgKYoGlY"
    )
    assert "encrypted_context" not in strip_facebook_page_url_for_capsolver(long_url)
    assert strip_facebook_page_url_for_capsolver(long_url).endswith("/authentication")


def test_proxyless_candidates_use_root_domain_not_checkpoint_path() -> None:
    from src.services.facebook_recaptcha import _capsolver_website_url_candidates

    long_url = (
        "https://www.facebook.com/two_step_verification/authentication/?encrypted_context=abc"
    )
    candidates = _capsolver_website_url_candidates(long_url, proxyless=True)
    assert all("two_step" not in u for u in candidates)
    assert "https://www.facebook.com" in candidates
    assert any("m.facebook.com" in u for u in candidates)


def test_normalize_proxy_accepts_proxy_username_fields() -> None:
    from src.services.facebook_recaptcha import _normalize_proxy_config_for_capsolver

    px = _normalize_proxy_config_for_capsolver(
        {
            "host": "160.30.191.116",
            "port": 20608,
            "proxy_username": "user_cua_proxy",
            "proxy_password": "pass_cua_proxy",
            "scheme_hint": "socks5",
        }
    )
    assert px["user"] == "user_cua_proxy"
    assert px["pass"] == "pass_cua_proxy"


def test_build_capsolver_attempts_proxyless_only_when_no_proxy() -> None:
    attempts = _build_capsolver_attempts(
        {
            "website_url": "https://www.facebook.com/login",
            "website_key": "6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe",
            "api_key": "k",
            "proxy": None,
            "is_enterprise": True,
            "recaptcha_data_s_value": "s1",
            "page_action": "verify",
            "api_domain": "www.google.com",
            "user_agent": "Mozilla/5.0",
            "is_invisible": False,
        }
    )
    assert len(attempts) >= 1
    assert "encrypted_context" not in attempts[0]["website_url"]
    assert all(a.get("proxy") is None for a in attempts)
    assert attempts[0]["is_enterprise"] is True


def test_hybrid_proxyless_uses_root_domain_only() -> None:
    from src.services.facebook_recaptcha import _build_hybrid_proxyless_attempts

    attempts = _build_hybrid_proxyless_attempts(
        {
            "website_url": "https://www.facebook.com/two_step_verification/authentication/?x=1",
            "website_key": "6LeyIlkaAAAA",
            "is_enterprise": True,
            "recaptcha_data_s_value": "s" * 52,
        }
    )
    assert len(attempts) >= 1
    for a in attempts:
        assert "authentication" in a["website_url"]
        assert a.get("proxy") is None


def test_hybrid_kwargs_without_proxy_resolve_proxyless_mode() -> None:
    from src.services.facebook_recaptcha import _build_capsolver_attempts

    attempts = _build_capsolver_attempts(
        {
            "website_url": "https://www.facebook.com/two_step_verification/authentication/?encrypted_context=abc",
            "website_key": "6LeyIlkaAAAA",
            "_account": {"use_proxy": True},
            "proxy": None,
            "is_enterprise": True,
            "recaptcha_data_s_value": "s" * 40,
        },
        prefer_enterprise=True,
    )
    assert len(attempts) >= 1
    assert "two_step" not in attempts[0]["website_url"]
    assert "facebook.com" in attempts[0]["website_url"]
    assert "encrypted_context" not in attempts[0]["website_url"]


def test_build_capsolver_compact_meta_proxyless() -> None:
    prev = os.environ.get("FB_CAPSOLVER_USE_ACCOUNT_PROXY")
    os.environ["FB_CAPSOLVER_USE_ACCOUNT_PROXY"] = "0"
    try:
        attempts = _build_capsolver_attempts(
        {
            "website_url": "https://www.facebook.com/two_step_verification/authentication/?x=1",
            "website_key": "6LeyIlkaAAAA",
            "api_key": "k",
            "proxy": None,
            "_account": {"use_proxy": True},
            "is_enterprise": True,
            "recaptcha_data_s_value": "s" * 52,
            "page_action": "fb_login_recaptcha",
            "api_domain": "www.google.com",
            "user_agent": "Mozilla/5.0",
            "is_invisible": False,
        }
    )
        assert len(attempts) >= 1
        assert "encrypted_context" not in attempts[0]["website_url"]
        assert all(a.get("proxy") is None for a in attempts)
    finally:
        if prev is None:
            os.environ.pop("FB_CAPSOLVER_USE_ACCOUNT_PROXY", None)
        else:
            os.environ["FB_CAPSOLVER_USE_ACCOUNT_PROXY"] = prev


def test_capsolver_uses_account_proxy_on_checkpoint_by_default() -> None:
    acc = {"use_proxy": True, "proxy": {"host": "socks5://1.2.3.4", "port": 1080}}
    url = "https://www.facebook.com/two_step_verification/authentication/"
    assert _is_checkpoint_captcha_page(url) is True
    assert _capsolver_use_account_proxy(acc, page_url=url) is True
    assert _capsolver_split_proxy_mode(acc, page_url=url) is False


def test_capsolver_use_account_proxy_from_app_secrets_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FB_CAPSOLVER_USE_ACCOUNT_PROXY", raising=False)
    monkeypatch.setattr(
        "src.utils.capsolver_config.load_app_secrets",
        lambda: {"capsolver_use_account_proxy": False},
    )
    from src.utils.capsolver_config import capsolver_use_account_proxy_setting

    assert capsolver_use_account_proxy_setting() is False
    acc = {"use_proxy": True}
    assert _capsolver_use_account_proxy(acc, page_url="https://www.facebook.com/x") is False


def test_capsolver_proxyless_when_env_off() -> None:
    acc = {"use_proxy": True}
    url = "https://www.facebook.com/two_step_verification/authentication/"
    prev = os.environ.get("FB_CAPSOLVER_USE_ACCOUNT_PROXY")
    os.environ["FB_CAPSOLVER_USE_ACCOUNT_PROXY"] = "0"
    try:
        assert _capsolver_use_account_proxy(acc, page_url=url) is False
        assert _capsolver_split_proxy_mode(acc, page_url=url) is True
    finally:
        if prev is None:
            os.environ.pop("FB_CAPSOLVER_USE_ACCOUNT_PROXY", None)
        else:
            os.environ["FB_CAPSOLVER_USE_ACCOUNT_PROXY"] = prev


def test_build_standard_flow_enterprise_with_proxy() -> None:
    px = {
        "host": "socks5://160.30.191.116",
        "port": 20608,
        "user": "u",
        "pass": "p",
        "scheme_hint": "socks5",
    }
    attempts = _build_capsolver_attempts(
        {
            "website_url": "https://www.facebook.com/two_step_verification/authentication/?x=1",
            "website_key": "6LeyIlkaAAAA",
            "proxy": "socks5://u:p@160.30.191.116:20608",
            "proxy_config": px,
            "_account": {"use_proxy": True},
            "is_enterprise": True,
            "recaptcha_data_s_value": "s" * 52,
        },
    )
    assert len(attempts) >= 1
    assert all(a.get("is_enterprise") for a in attempts)
    assert all(a.get("proxy") or a.get("proxy_config") for a in attempts)
    assert all(a.get("proxy") is not None or a.get("proxy_config") for a in attempts)


def test_capsolver_use_account_proxy_forced_on() -> None:
    acc = {"use_proxy": True}
    url = "https://www.facebook.com/login"
    prev = os.environ.get("FB_CAPSOLVER_USE_ACCOUNT_PROXY")
    os.environ["FB_CAPSOLVER_USE_ACCOUNT_PROXY"] = "1"
    try:
        assert _capsolver_use_account_proxy(acc, page_url=url) is True
        assert _capsolver_split_proxy_mode(acc, page_url=url) is False
    finally:
        if prev is None:
            os.environ.pop("FB_CAPSOLVER_USE_ACCOUNT_PROXY", None)
        else:
            os.environ["FB_CAPSOLVER_USE_ACCOUNT_PROXY"] = prev


def test_build_capsolver_attempts_proxy_first_when_has_s(monkeypatch) -> None:
    monkeypatch.setenv("FB_CAPSOLVER_USE_ACCOUNT_PROXY", "1")
    attempts = _build_capsolver_attempts(
        {
            "website_url": "https://www.facebook.com/login",
            "website_key": "6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe",
            "api_key": "k",
            "proxy": "http://1.2.3.4:8080",
            "proxy_config": {"host": "1.2.3.4", "port": 8080, "scheme_hint": "http"},
            "_account": {"use_proxy": True},
            "is_enterprise": True,
            "recaptcha_data_s_value": "s1",
            "page_action": "verify",
            "api_domain": "www.google.com",
            "user_agent": "Mozilla/5.0",
            "is_invisible": False,
        }
    )
    assert attempts
    assert attempts[0]["is_enterprise"] is True
    assert attempts[0]["proxy"] is None
    assert attempts[0]["proxy_config"] is not None
    assert any(a.get("proxy_config") is None for a in attempts)


def test_facebook_6le_key_forces_enterprise_attempts() -> None:
    from src.services.capsolver_client import CAPSOLVER_FB_PROXYLESS_WEBSITE_URL

    attempts = _build_capsolver_attempts(
        {
            "website_url": "https://www.facebook.com/login",
            "website_key": "6LeyIlkaAAAA",
            "api_key": "k",
            "proxy": None,
            "proxy_config": {
                "host": "1.2.3.4",
                "port": 8080,
                "scheme_hint": "http",
            },
            "_account": {"use_proxy": True},
            "is_enterprise": False,
            "recaptcha_data_s_value": "",
            "page_action": "",
            "api_domain": "",
            "user_agent": "",
            "is_invisible": False,
        }
    )
    assert attempts
    assert attempts[0]["is_enterprise"] is True
    assert attempts[-1].get("proxy_config") is None
    assert "facebook.com" in attempts[-1]["website_url"]


def test_extract_params_from_enterprise_anchor_frame() -> None:
    page = _FakePage("<html></html>")
    page.frames = [
        _FakeFrame(
            "https://www.google.com/recaptcha/enterprise/anchor?ar=1&k=6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe&s=s-value-123&sa=verify"
        )
    ]
    params = extract_recaptcha_params(page)
    assert params is not None
    assert params["website_key"] == "6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe"
    assert params["is_enterprise"] is True
    assert params["recaptcha_data_s_value"] == "s-value-123"
    assert params["page_action"] == "verify"
    assert params["api_domain"] == "www.google.com"


def test_is_google_recaptcha_network_url() -> None:
    assert _is_google_recaptcha_network_url(
        "https://www.google.com/recaptcha/enterprise/anchor?k=6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe"
    )
    assert not _is_google_recaptcha_network_url("https://www.google.com/maps")


def test_extract_s_from_url_regex() -> None:
    url = (
        "https://www.google.com/recaptcha/enterprise/anchor?"
        "k=6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe&"
        "s=DynamicSValueFromNetworkInterception12345"
    )
    assert "DynamicSValueFromNetworkInterception" in _extract_s_from_url(url)


def test_fresh_network_s_invalid_after_reset() -> None:
    page = _FakePage("<html></html>")
    store = _new_network_capture_store()
    setattr(page.context, "_toolfb_recaptcha_network_capture", store)
    _network_ingest_url(
        store,
        "https://www.google.com/recaptcha/enterprise/anchor?s=OldSessionValue1234567890",
    )
    assert "OldSessionValue" in get_fresh_network_s(page)
    reset_recaptcha_network_capture(page, reason="test")
    assert get_fresh_network_s(page) == ""
    _network_ingest_url(
        store,
        "https://www.google.com/recaptcha/enterprise/anchor?s=NewSessionValue1234567890",
    )
    assert "NewSessionValue" in get_fresh_network_s(page)


def test_network_ingest_url_captures_anchor_s() -> None:
    store: dict = {"s": "", "sitekey": "", "page_action": "", "api_domain": "", "events": 0}
    url = (
        "https://www.google.com/recaptcha/enterprise/anchor?ar=1&k=6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe"
        "&s=CapturedFromNetworkRequestValue1234567890&sa=verify"
    )
    _network_ingest_url(store, url)
    assert store["sitekey"] == "6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe"
    assert "CapturedFromNetworkRequestValue" in store["s"]
    assert store["page_action"] == "verify"
    assert store["events"] >= 1


def test_extract_s_from_html_data_s_and_enterprise_payload() -> None:
    html = (
        '<div data-sitekey="6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe" '
        'data-s="meta-enterprise-s-token-abcdefghijklmnop"></div>'
        '<script>{"enterprisePayload":{"s":"payload-s-value-xyz123456789"}}</script>'
    )
    s = _extract_s_from_html(html)
    assert "meta-enterprise-s-token" in s or "payload-s-value" in s
    page = _FakePage(html)
    params = extract_recaptcha_params(page)
    assert params is not None
    assert params["recaptcha_data_s_value"]
    assert params["is_enterprise"] is True


def test_meta_enterprise_sitekey_prefix() -> None:
    assert _meta_enterprise_sitekey("6LeyIlkaAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe")
    assert _meta_enterprise_sitekey("6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe")
    assert not _meta_enterprise_sitekey("6Labc")


def test_auth_flow_task_urls_skip_login() -> None:
    page = _FakePage(
        "<html></html>",
        url="https://www.facebook.com/two_step_verification/authentication/?flow=pre_authentication",
    )
    urls = facebook_recaptcha_task_urls(page)
    assert len(urls) <= 2
    assert all("login" not in u for u in urls)
    assert any("authentication" in u for u in urls)


def test_plain_login_url_not_recaptcha_flow() -> None:
    url = "https://www.facebook.com/login"
    assert is_plain_facebook_login_url(url) is True
    page = _FakePage("<html><body>Đăng nhập</body></html>", url=url)
    assert facebook_page_on_recaptcha_flow_url(page) is False
    assert facebook_page_may_need_recaptcha(page) is False


def test_may_need_recaptcha_on_auth_url_without_widget() -> None:
    page = _FakePage(
        "<html><body>Continue</body></html>",
        url="https://www.facebook.com/two_step_verification/authentication/?flow=pre_authentication",
    )
    assert facebook_page_on_recaptcha_flow_url(page) is True
    assert facebook_page_may_need_recaptcha(page) is True
    urls = facebook_recaptcha_task_urls(page)
    assert any("facebook.com" in u for u in urls)
    assert urls[0].startswith("https://")


def test_should_skip_capsolver_when_skip_meta_env(monkeypatch) -> None:
    html = (
        '<div data-sitekey="6LeyIlkaAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe" '
        'data-s="x" * 52></div>'
    )
    page = _FakePage(
        html,
        url="https://www.facebook.com/two_step_verification/authentication",
    )
    monkeypatch.setenv("FB_CAPSOLVER_SKIP_META", "1")
    monkeypatch.setenv("TOOLFB_CAPSOLVER_AUTO_SOLVE", "1")
    monkeypatch.setenv("TOOLFB_CAPSOLVER_API_KEY", "test-key")
    assert _capsolver_tier_disabled(page, {"use_proxy": True}) is True


def test_scan_frames_prefers_longest_sitekey() -> None:
    page = _FakePage("<html></html>")
    page.frames = [
        _FakeFrame("https://www.google.com/recaptcha/enterprise/anchor?k=6LeyIlkaAAAA"),
        _FakeFrame(
            "https://www.google.com/recaptcha/enterprise/anchor?k=6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe&s=abc"
        ),
    ]
    scanned = _scan_frames_for_recaptcha_params(page)
    assert scanned["sitekey"] == "6LeIxAcTAAAAAGG-vFI1TnRWxMZNFuojJ4WifJWe"
    assert scanned["s"] == "abc"
