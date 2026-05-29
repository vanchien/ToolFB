"""TOTP service + credentials vault."""

from __future__ import annotations

from src.services.totp_service import generate_totp_code, normalize_totp_secret
from src.utils.account_credentials import get_account_password, set_account_credentials


def test_normalize_totp_secret_strips_spaces() -> None:
    assert normalize_totp_secret("jbsw y3dp ehpk3pxp") == "JBSWY3DPEHPK3PXP"


def test_generate_totp_code_known_secret() -> None:
    code = generate_totp_code("JBSWY3DPEHPK3PXP")
    assert len(code) == 6
    assert code.isdigit()


def test_credentials_roundtrip(tmp_path, monkeypatch) -> None:
    p = tmp_path / "account_credentials.json"
    monkeypatch.setattr("src.utils.account_credentials.account_credentials_path", lambda: p)
    set_account_credentials("acc1", password="secret-pass", totp_secret="JBSWY3DPEHPK3PXP")
    assert get_account_password("acc1") == "secret-pass"
    assert p.is_file()
