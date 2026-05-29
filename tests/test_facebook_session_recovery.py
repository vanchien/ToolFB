"""Unit tests for Facebook session recovery helpers."""

from __future__ import annotations

from src.services.facebook_session_recovery import (
    facebook_page_is_hard_checkpoint,
)
from src.utils.account_credentials import (
    account_can_auto_reauth,
    account_has_totp_recovery,
    load_account_credential_bundle,
    set_account_credentials,
)


def test_hard_checkpoint_detects_captcha_and_checkpoint() -> None:
    assert facebook_page_is_hard_checkpoint("https://www.facebook.com/checkpoint/?next")
    assert facebook_page_is_hard_checkpoint("https://www.facebook.com/recover/initiate")
    assert facebook_page_is_hard_checkpoint("https://www.facebook.com/?captcha=1")
    assert not facebook_page_is_hard_checkpoint("https://www.facebook.com/login")
    assert not facebook_page_is_hard_checkpoint("https://example.com/checkpoint/")


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
