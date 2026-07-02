"""Tests for smart interaction partition (cookie vs soft login)."""

from __future__ import annotations

import json
from pathlib import Path

from src.models.mapped_account import MappedAccount, MappedAccountAuth
from src.services.facebook_session_persist import mapped_account_ready_for_interaction


def _write_cookie(path: Path, uid: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "cookies": [
            {"name": "c_user", "value": uid, "domain": ".facebook.com", "path": "/"},
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_ready_vs_blocked_by_cookie(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.services.facebook_session_persist.project_root", lambda: tmp_path)
    uid_ok = "100092507808326"
    uid_no = "100099999999901"
    _write_cookie(tmp_path / "data" / "cookies" / f"UID_{uid_ok}.json", uid_ok)

    ready_ma = MappedAccount(
        account_id=f"UID_{uid_ok}",
        auth=MappedAccountAuth(username=uid_ok, password="p1"),
        cookie_path=f"data/cookies/UID_{uid_ok}.json",
    )
    blocked_ma = MappedAccount(
        account_id=f"UID_{uid_no}",
        auth=MappedAccountAuth(username=uid_no, password="p2"),
        cookie_path=f"data/cookies/UID_{uid_no}.json",
    )
    ok_r, _ = mapped_account_ready_for_interaction(ready_ma)
    ok_b, msg_b = mapped_account_ready_for_interaction(blocked_ma)
    assert ok_r
    assert not ok_b
    assert "cookie" in msg_b.lower() or "phiên" in msg_b.lower()

    ready_ma.soft_login_if_needed = False
    blocked_ma.soft_login_if_needed = True
    assert ready_ma.soft_login_if_needed is False
    assert blocked_ma.soft_login_if_needed is True


def test_cancelled_with_profile_history_is_ready(tmp_path, monkeypatch) -> None:
    from src.models.mapped_account import MappedAccountStorage
    from src.services.facebook_session_persist import mapped_account_ready_for_interaction

    root = tmp_path
    monkeypatch.setattr("src.services.facebook_session_persist.project_root", lambda: root)
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: root)
    uid = "100092209774814"
    prof = root / "data" / "profiles" / "firefox" / f"UID_{uid}"
    prof.mkdir(parents=True)
    (prof / ".toolfb_account_id").write_text(f"UID_{uid}\n", encoding="utf-8")
    (prof / "cookies.sqlite").write_bytes(b"sqlite" * 32)

    ma = MappedAccount(
        account_id=f"UID_{uid}",
        status="cancelled",
        status_detail="Đã hủy — người dùng bấm Dừng",
        auth=MappedAccountAuth(username=uid, password="pw"),
        storage=MappedAccountStorage(profile_path=f"data/profiles/firefox/UID_{uid}"),
    )
    ok, msg = mapped_account_ready_for_interaction(ma)
    assert ok, msg
    assert ma.status == "login_ok"
    assert "profile" in ma.status_detail.lower()
