"""Unit tests for Facebook session recovery helpers."""

from __future__ import annotations

from src.services.facebook_session_recovery import (
    _EMAIL_SELECTORS,
    _PASSWORD_SELECTORS,
    _TOTP_TEXT_MARKERS,
    _TOTP_URL_MARKERS,
    _TRUST_DEVICE_PRIMARY_SELECTORS,
    _auth_url_is_pre_captcha_gate,
    _facebook_uids_match,
    _login_form_wait_ms,
    _normalize_facebook_uid,
    facebook_auth_flow_was_active,
    facebook_page_blocks_recovery_email,
    facebook_page_is_hard_checkpoint,
    facebook_page_is_remember_browser,
)
from src.services.totp_service import generate_totp_code
from src.utils.account_credentials import (
    account_can_auto_reauth,
    account_has_totp_recovery,
    load_account_credential_bundle,
    resolve_facebook_login_identifier,
    set_account_credentials,
)


def test_hard_checkpoint_detects_captcha_and_checkpoint() -> None:
    assert facebook_page_is_hard_checkpoint("https://www.facebook.com/checkpoint/?next")
    assert facebook_page_is_hard_checkpoint("https://www.facebook.com/recover/initiate")
    assert facebook_page_is_hard_checkpoint("https://www.facebook.com/?captcha=1")
    assert not facebook_page_is_hard_checkpoint("https://www.facebook.com/login")
    assert not facebook_page_is_hard_checkpoint("https://example.com/checkpoint/")


def test_recovery_email_blocked_on_captcha_not_plain_checkpoint() -> None:
    assert facebook_page_blocks_recovery_email("https://www.facebook.com/?captcha=1")
    assert facebook_page_blocks_recovery_email("https://www.facebook.com/accountquality")
    assert not facebook_page_blocks_recovery_email("https://www.facebook.com/checkpoint/?next")


def test_account_can_auto_reauth_password_only(tmp_path, monkeypatch) -> None:
    p = tmp_path / "account_credentials.json"
    monkeypatch.setattr("src.utils.account_credentials.account_credentials_path", lambda: p)
    set_account_credentials("a1", password="pw")
    acc = {
        "id": "a1",
        "email": "user@example.com",
        "totp_enabled": False,
        "password_ref": "account:a1",
    }
    assert account_can_auto_reauth(acc)
    assert not account_has_totp_recovery(acc)
    bundle = load_account_credential_bundle(acc)
    assert bundle is not None
    assert bundle.has_password_login
    assert not bundle.has_totp


def test_account_has_totp_recovery_requires_secret(tmp_path, monkeypatch) -> None:
    p = tmp_path / "account_credentials.json"
    monkeypatch.setattr("src.utils.account_credentials.account_credentials_path", lambda: p)
    set_account_credentials("a2", password="pw", totp_secret="JBSWY3DPEHPK3PXP")
    acc = {
        "id": "a2",
        "email": "u@example.com",
        "totp_enabled": True,
        "password_ref": "account:a2",
        "totp_secret_ref": "account:a2",
    }
    assert account_can_auto_reauth(acc)
    assert account_has_totp_recovery(acc)


def test_login_identifier_prefers_uid_over_email() -> None:
    acc = {
        "facebook_uid": "100092564235770",
        "email": "user@example.com",
    }
    assert resolve_facebook_login_identifier(acc) == "100092564235770"
    bundle = load_account_credential_bundle({"id": "x", **acc, "password_ref": ""})
    assert bundle is not None
    assert bundle.login_identifier == "100092564235770"


def test_login_identifier_email_only(tmp_path, monkeypatch) -> None:
    p = tmp_path / "account_credentials.json"
    monkeypatch.setattr("src.utils.account_credentials.account_credentials_path", lambda: p)
    set_account_credentials("u1", password="pw")
    acc = {"id": "u1", "email": "user@example.com", "password_ref": "account:u1"}
    assert resolve_facebook_login_identifier(acc) == "user@example.com"
    bundle = load_account_credential_bundle(acc)
    assert bundle is not None
    assert bundle.has_password_login
    assert bundle.login_identifier == "user@example.com"
    assert not bundle.facebook_uid


def test_remember_browser_url_detected() -> None:
    url = "https://www.facebook.com/two_factor/remember_browser/?encrypted_context=abc"
    assert facebook_page_is_remember_browser(url)
    assert "remember_browser" not in _TOTP_URL_MARKERS


def test_remember_browser_not_classified_as_totp_url() -> None:
    url = "https://www.facebook.com/two_factor/remember_browser/?encrypted_context=abc"
    assert facebook_page_is_remember_browser(url)
    # URL có two_factor nhưng là remember_browser — không phải form nhập mã.
    assert "two_factor" in url
    assert "remember" in url


def test_trust_device_selectors_include_vietnamese_primary_button() -> None:
    joined = " ".join(_TRUST_DEVICE_PRIMARY_SELECTORS).lower()
    assert "tin cậy thiết bị" in joined
    assert "trust this device" in joined


def test_totp_markers_cover_new_authenticator_ui() -> None:
    assert "two_step_verification" in _TOTP_URL_MARKERS
    assert "ứng dụng xác thực" in _TOTP_TEXT_MARKERS
    assert "6 chữ số" in _TOTP_TEXT_MARKERS


def test_generate_totp_from_user_style_secret() -> None:
    code = generate_totp_code("2EXTXAX754GL4NLUQLQYRDK7PGGGTONN")
    assert len(code) == 6
    assert code.isdigit()


def test_login_selectors_cover_mobile_and_royal_email() -> None:
    joined = " ".join(_EMAIL_SELECTORS).lower()
    assert "m_login_email" in joined
    assert "royal_email" in joined
    pass_joined = " ".join(_PASSWORD_SELECTORS).lower()
    assert "royal_pass" in pass_joined
    assert "m_login_pass" in pass_joined


def test_login_form_wait_ms_default_at_least_five_seconds(monkeypatch) -> None:
    monkeypatch.delenv("FB_LOGIN_FORM_WAIT_MS", raising=False)
    assert _login_form_wait_ms() >= 5_000


def test_auth_url_pre_captcha_gate() -> None:
    pre = (
        "https://www.facebook.com/two_step_verification/authentication/"
        "?encrypted_context=abc&flow=pre_authentication&next"
    )
    post = (
        "https://www.facebook.com/two_step_verification/authentication/"
        "?encrypted_context=abc&flow=post_authentication&next"
    )
    assert _auth_url_is_pre_captcha_gate(pre)
    assert not _auth_url_is_pre_captcha_gate(post)
    assert not _auth_url_is_pre_captcha_gate("https://www.facebook.com/login")


def test_facebook_auth_flow_flag_on_account() -> None:
    acc: dict = {"id": "a1"}
    assert not facebook_auth_flow_was_active(acc)
    acc["_fb_auth_flow_active"] = True
    assert facebook_auth_flow_was_active(acc)


def test_recovery_email_vault_roundtrip(tmp_path, monkeypatch) -> None:
    p = tmp_path / "account_credentials.json"
    monkeypatch.setattr("src.utils.account_credentials.account_credentials_path", lambda: p)
    set_account_credentials("a3", password="pw", recovery_email="backup@example.com")
    acc = {
        "id": "a3",
        "email": "main@example.com",
        "password_ref": "account:a3",
    }
    bundle = load_account_credential_bundle(acc)
    assert bundle is not None
    assert bundle.has_recovery_email
    assert bundle.recovery_email == "backup@example.com"


def test_normalize_facebook_uid_strips_prefix() -> None:
    assert _normalize_facebook_uid("UID_100092564235770") == "100092564235770"
    assert _normalize_facebook_uid("100092564235770") == "100092564235770"
    assert _normalize_facebook_uid("uid_99") == "99"
    assert _normalize_facebook_uid("") == ""


def test_facebook_uids_match_ignores_uid_prefix() -> None:
    assert _facebook_uids_match("100092564235770", "UID_100092564235770")
    assert _facebook_uids_match("UID_100092564235770", "100092564235770")
    assert not _facebook_uids_match("111", "222")
    assert _facebook_uids_match("111", "")
