"""Chuẩn bị profile/cookie trước mỗi lượt chạy tương tác."""

from __future__ import annotations

from pathlib import Path

from src.models.mapped_account import MappedAccount
from src.utils.account_proxy_mapper import (
    persist_mapped_storage_to_registry,
    prepare_mapped_account_for_browser_run,
)


def test_prepare_mapped_account_picks_session_profile(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)

    session = tmp_path / "data" / "profiles" / "firefox" / "UID_42"
    session.mkdir(parents=True)
    (session / "cookies.sqlite").write_bytes(b"x")
    (session / ".toolfb_account_id").write_text("acc_x\n", encoding="utf-8")

    ck = tmp_path / "data" / "cookies" / "acc_x.json"
    ck.parent.mkdir(parents=True)
    ck.write_text(
        '{"cookies":[{"name":"c_user","value":"42","domain":".facebook.com","path":"/"}],"origins":[]}',
        encoding="utf-8",
    )

    class _FakeDb:
        def load_all(self):
            return [
                {
                    "id": "acc_x",
                    "portable_path": "data/profiles/firefox/acc_x",
                    "profile_path": "data/profiles/firefox/acc_x",
                    "cookie_path": "data/cookies/acc_x.json",
                }
            ]

    monkeypatch.setattr("src.utils.db_manager.AccountsDatabaseManager", _FakeDb)

    ma = MappedAccount.from_dict(
        {
            "account_id": "acc_x",
            "uid": "UID_42",
            "username": "UID_42",
            "proxy_server": "socks5://127.0.0.1:9001",
            "storage": {"profile_path": "data/profiles/firefox/acc_x"},
        }
    )
    acc = prepare_mapped_account_for_browser_run(ma)
    assert "UID_42" in str(acc.get("portable_path") or "")
    assert ma.storage.profile_path.replace("\\", "/").endswith("UID_42")
    assert ma.cookie_path.replace("\\", "/").endswith("acc_x.json")


def test_persist_mapped_storage_writes_registry_profile(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)

    prof = tmp_path / "data" / "profiles" / "firefox" / "acc_x"
    prof.mkdir(parents=True)
    (prof / "cookies.sqlite").write_bytes(b"x" * 128)
    ck = tmp_path / "data" / "cookies" / "acc_x.json"
    ck.parent.mkdir(parents=True)
    ck.write_text(
        '{"cookies":[{"name":"c_user","value":"1","domain":".facebook.com","path":"/"}],"origins":[]}',
        encoding="utf-8",
    )

    stored: dict = {}

    class _FakeDb:
        def load_all(self):
            return [
                {
                    "id": "acc_x",
                    "portable_path": "data/profiles/firefox/acc_x",
                    "cookie_path": "data/cookies/acc_x.json",
                }
            ]

        def update_account_fields(self, account_id: str, updates: dict) -> None:
            stored["id"] = account_id
            stored.update(updates)

    monkeypatch.setattr("src.utils.db_manager.AccountsDatabaseManager", _FakeDb)

    ma = MappedAccount.from_dict(
        {
            "account_id": "UID_100",
            "username": "100",
            "storage": {"profile_path": "data/profiles/firefox/UID_100"},
            "cookie_path": "data/cookies/UID_100.json",
        }
    )
    acc = {
        "id": "acc_x",
        "registry_id": "acc_x",
        "portable_path": "data/profiles/firefox/acc_x",
        "profile_path": "data/profiles/firefox/acc_x",
        "cookie_path": "data/cookies/acc_x.json",
    }
    persist_mapped_storage_to_registry(ma, acc)
    assert stored.get("id") == "acc_x"
    assert "acc_x" in str(stored.get("portable_path") or "")
    assert ma.storage.profile_path.replace("\\", "/").endswith("acc_x")
