"""Tests lưu/khôi phục danh sách đã ghép tab Tương tác người dùng."""

from __future__ import annotations

from src.models.mapped_account import MappedAccount
from src.utils.human_interaction_settings import load_mapped_accounts_from_settings


def test_load_mapped_accounts_from_settings() -> None:
    settings = {
        "mapped_accounts": [
            {
                "account_id": "UID_123",
                "auth": {"username": "123", "password": "pw", "two_fa_secret": "ABC"},
                "network": {"proxy_server": "socks5://1.2.3.4:1080"},
                "status": "login_ok",
                "status_detail": "OK",
            }
        ]
    }
    raw = load_mapped_accounts_from_settings(settings)
    assert len(raw) == 1
    ma = MappedAccount.from_dict(raw[0])
    assert ma.account_id == "UID_123"
    assert ma.status == "login_ok"
    assert ma.auth.password == "pw"
