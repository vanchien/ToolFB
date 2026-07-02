"""Tests resolve profile portable theo tài khoản."""

from __future__ import annotations

from pathlib import Path

from src.utils.account_browser_profile import (
    default_portable_path,
    portable_profile_likely_has_session,
    resolve_account_portable_profile,
)


def test_resolve_account_portable_profile_prefers_existing_dir(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "src.utils.account_browser_profile.project_root",
        lambda: tmp_path,
    )
    aid = "acc_test1"
    existing = tmp_path / "data" / "profiles" / "firefox" / aid
    existing.mkdir(parents=True)
    (existing / ".toolfb_account_id").write_text(f"{aid}\n", encoding="utf-8")

    acc = {"id": aid, "browser_type": "firefox", "portable_path": ""}
    rel = resolve_account_portable_profile(acc)
    assert rel.replace("\\", "/") == f"data/profiles/firefox/{aid}"
    assert acc["portable_path"] == rel
    assert acc["profile_path"] == rel


def test_resolve_account_portable_profile_default_when_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "src.utils.account_browser_profile.project_root",
        lambda: tmp_path,
    )
    aid = "UID_999"
    acc = {"id": aid, "browser_type": "firefox"}
    rel = resolve_account_portable_profile(acc)
    assert rel == default_portable_path(aid, "firefox")


def test_portable_profile_likely_has_session_detects_cookies_db(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "src.utils.account_browser_profile.project_root",
        lambda: tmp_path,
    )
    prof = tmp_path / "data" / "profiles" / "firefox" / "UID_1"
    prof.mkdir(parents=True)
    (prof / ".toolfb_account_id").write_text("UID_1\n", encoding="utf-8")
    assert portable_profile_likely_has_session(str(prof)) is False
    (prof / "cookies.sqlite").write_bytes(b"x" * 128)
    assert portable_profile_likely_has_session("data/profiles/firefox/UID_1") is True


def test_resolve_uses_registry_path_not_uid_folder(tmp_path: Path, monkeypatch) -> None:
    """Có registry acc_ → không mở profile UID_ trống."""
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)

    reg_prof = tmp_path / "data" / "profiles" / "firefox" / "acc_real01"
    reg_prof.mkdir(parents=True)
    (reg_prof / ".toolfb_account_id").write_text("acc_real01\n", encoding="utf-8")
    (reg_prof / "cookies.sqlite").write_bytes(b"x" * 128)
    (reg_prof / "places.sqlite").write_bytes(b"y" * 2048)

    uid_prof = tmp_path / "data" / "profiles" / "firefox" / "UID_555"
    uid_prof.mkdir(parents=True)
    (uid_prof / ".toolfb_account_id").write_text("UID_555\n", encoding="utf-8")
    # playwright junk without real session
    for i in range(8):
        (uid_prof / f"junk{i}.txt").write_text("x", encoding="utf-8")

    reg = [
        {
            "id": "acc_real01",
            "facebook_uid": "555",
            "portable_path": "data/profiles/firefox/acc_real01",
            "profile_path": "data/profiles/firefox/acc_real01",
            "cookie_path": "data/cookies/acc_real01.json",
        }
    ]

    class _FakeDb:
        def load_all(self):
            return reg

    monkeypatch.setattr("src.utils.db_manager.AccountsDatabaseManager", _FakeDb)

    acc = {
        "id": "UID_555",
        "display_account_id": "UID_555",
        "facebook_uid": "555",
        "browser_type": "firefox",
    }
    from src.utils.account_browser_profile import resolve_account_portable_profile

    rel = resolve_account_portable_profile(acc)
    assert rel.replace("\\", "/") == "data/profiles/firefox/acc_real01"
    assert acc.get("registry_id") == "acc_real01"
    assert acc.get("id") == "acc_real01"


def test_resolve_prefers_session_profile_over_empty_configured(tmp_path: Path, monkeypatch) -> None:
    """Profile trống trong registry không được ưu tiên hơn thư mục đã có cookies.sqlite."""
    monkeypatch.setattr(
        "src.utils.account_browser_profile.project_root",
        lambda: tmp_path,
    )
    aid = "acc_real"
    empty = tmp_path / "data" / "profiles" / "firefox" / aid
    empty.mkdir(parents=True)
    (empty / ".toolfb_account_id").write_text(f"{aid}\n", encoding="utf-8")

    session = tmp_path / "data" / "profiles" / "firefox" / "UID_555"
    session.mkdir(parents=True)
    (session / ".toolfb_account_id").write_text("UID_555\n", encoding="utf-8")
    (session / "cookies.sqlite").write_bytes(b"session" * 32)

    class _FakeDb:
        def load_all(self):
            return []

    monkeypatch.setattr("src.utils.db_manager.AccountsDatabaseManager", _FakeDb)

    acc = {
        "id": aid,
        "facebook_uid": "555",
        "browser_type": "firefox",
        "portable_path": f"data/profiles/firefox/{aid}",
    }
    rel = resolve_account_portable_profile(acc)
    assert rel.replace("\\", "/") == "data/profiles/firefox/UID_555"
    assert acc["portable_path"] == rel
