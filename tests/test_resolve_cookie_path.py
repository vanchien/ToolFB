"""Tests for cookie path resolution (UID vs acc_ registry)."""

from __future__ import annotations

import json
from pathlib import Path

from src.services.facebook_session_persist import (
    cookie_file_has_session,
    resolve_best_cookie_path_for_account,
)
from src.utils.account_proxy_mapper import sync_mapped_account_storage_from_registry
from src.models.mapped_account import MappedAccount, MappedAccountAuth, MappedAccountStorage


def _write_cookie(path: Path, uid: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "cookies": [
            {"name": "c_user", "value": uid, "domain": ".facebook.com", "path": "/"},
            {"name": "xs", "value": "abc", "domain": ".facebook.com", "path": "/"},
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_resolve_best_cookie_prefers_uid_file(tmp_path, monkeypatch) -> None:
    root = tmp_path
    monkeypatch.setattr("src.services.facebook_session_persist.project_root", lambda: root)
    uid = "100092507808326"
    uid_file = root / "data" / "cookies" / f"UID_{uid}.json"
    acc_file = root / "data" / "cookies" / "acc_missing.json"
    _write_cookie(uid_file, uid)

    acc = {"id": f"UID_{uid}", "cookie_path": "data/cookies/acc_missing.json"}
    chosen = resolve_best_cookie_path_for_account(acc, facebook_uid=uid)
    assert chosen.replace("\\", "/").endswith(f"UID_{uid}.json")
    assert cookie_file_has_session(chosen)


def test_mapped_ready_when_cookie_exists_pending_status(tmp_path, monkeypatch) -> None:
    from src.services.facebook_session_persist import mapped_account_ready_for_interaction

    root = tmp_path
    monkeypatch.setattr("src.services.facebook_session_persist.project_root", lambda: root)
    uid = "100092507808326"
    _write_cookie(root / "data" / "cookies" / f"UID_{uid}.json", uid)

    ma = MappedAccount(
        account_id=f"UID_{uid}",
        status="pending",
        auth=MappedAccountAuth(username=uid),
        storage=MappedAccountStorage(profile_path=f"data/profiles/firefox/UID_{uid}"),
        cookie_path=f"data/cookies/UID_{uid}.json",
    )
    ok, msg = mapped_account_ready_for_interaction(ma)
    assert ok, msg
    assert ma.status == "login_ok"


def test_sync_mapped_storage_finds_uid_cookie(tmp_path, monkeypatch) -> None:
    root = tmp_path
    monkeypatch.setattr("src.services.facebook_session_persist.project_root", lambda: root)
    monkeypatch.setattr("src.utils.account_proxy_mapper.AccountsDatabaseManager", None, raising=False)
    uid = "100091753671636"
    _write_cookie(root / "data" / "cookies" / f"UID_{uid}.json", uid)

    ma = MappedAccount(
        account_id=f"UID_{uid}",
        auth=MappedAccountAuth(username=uid, password="secret"),
        storage=MappedAccountStorage(profile_path=f"data/profiles/firefox/UID_{uid}"),
    )
    out = sync_mapped_account_storage_from_registry(ma)
    assert "UID_" in ma.cookie_path
    assert cookie_file_has_session(ma.cookie_path)
    assert out["cookie_path"] == ma.cookie_path
