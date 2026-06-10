"""Profile Playwright riêng theo account — tạo/xóa cùng tài khoản."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.utils.account_browser_profile import (
    assert_portable_path_not_shared,
    assert_profile_directory_owned_by,
    delete_account_browser_bundle,
    provision_fresh_browser_profile,
)
from src.utils.account_credentials import get_account_password, set_account_credentials
from src.utils.db_manager import AccountsDatabaseManager


def test_provision_creates_fresh_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)
    portable, cookie = provision_fresh_browser_profile("acc_a", "firefox")
    prof = tmp_path / portable
    ck = tmp_path / cookie
    assert prof.is_dir()
    assert (prof / ".toolfb_account_id").read_text(encoding="utf-8").strip() == "acc_a"
    assert ck.is_file()
    provision_fresh_browser_profile("acc_a", "firefox")
    assert prof.is_dir()


def test_assert_portable_path_not_shared(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)
    p, _ = provision_fresh_browser_profile("a1", "firefox")
    others = [{"id": "a2", "portable_path": p}]
    with pytest.raises(ValueError, match="trùng"):
        assert_portable_path_not_shared("a3", p, others)


def test_delete_removes_profile_and_vault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)
    cred = tmp_path / "config" / "account_credentials.json"
    monkeypatch.setattr("src.utils.account_credentials.account_credentials_path", lambda: cred)
    portable, cookie = provision_fresh_browser_profile("del1", "firefox")
    set_account_credentials("del1", password="secret")
    acc = {"id": "del1", "portable_path": portable, "cookie_path": cookie}
    deleted = delete_account_browser_bundle(acc)
    assert not (tmp_path / portable).exists()
    assert not (tmp_path / cookie).exists()
    assert get_account_password("del1") == ""
    assert any("vault" in d for d in deleted)


def test_assert_profile_directory_owned_by_blocks_wrong_account(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)
    portable, _ = provision_fresh_browser_profile("owner_a", "firefox")
    prof = (tmp_path / portable).resolve()
    with pytest.raises(ValueError, match="thuộc tài khoản"):
        assert_profile_directory_owned_by("other_b", prof, create_marker_if_missing=False)


def test_delete_removes_default_profile_when_custom_path_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)
    default_p, cookie = provision_fresh_browser_profile("mix1", "firefox")
    custom = tmp_path / "data" / "profiles" / "firefox" / "mix1_custom"
    custom.mkdir(parents=True)
    (custom / ".toolfb_account_id").write_text("mix1\n", encoding="utf-8")
    acc = {
        "id": "mix1",
        "portable_path": str(custom.relative_to(tmp_path)).replace("\\", "/"),
        "cookie_path": cookie,
    }
    assert (tmp_path / default_p).is_dir()
    delete_account_browser_bundle(acc)
    assert not custom.exists()
    assert not (tmp_path / default_p).exists()


def test_upsert_new_account_provisions_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    acc_json = tmp_path / "accounts.json"
    acc_json.write_text("[]\n", encoding="utf-8")
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)
    monkeypatch.setattr("src.utils.account_credentials.account_credentials_path", lambda: tmp_path / "creds.json")
    mgr = AccountsDatabaseManager(acc_json)
    mgr.upsert(
        {
            "id": "new1",
            "name": "N",
            "browser_type": "firefox",
            "portable_path": "data/profiles/firefox/new1",
            "cookie_path": "data/cookies/new1.json",
            "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
            "use_proxy": False,
            "import_type": "new",
        }
    )
    assert (tmp_path / "data/profiles/firefox/new1").is_dir()
    assert mgr.delete_by_id("new1")
    assert not (tmp_path / "data/profiles/firefox/new1").exists()


def test_update_account_fields_rejects_shared_portable_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    acc_json = tmp_path / "accounts.json"
    acc_json.write_text("[]\n", encoding="utf-8")
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)
    monkeypatch.setattr("src.utils.account_credentials.account_credentials_path", lambda: tmp_path / "creds.json")
    mgr = AccountsDatabaseManager(acc_json)
    p1, c1 = provision_fresh_browser_profile("acc1", "firefox")
    mgr.upsert(
        {
            "id": "acc1",
            "name": "A1",
            "browser_type": "firefox",
            "portable_path": p1,
            "cookie_path": c1,
            "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
            "use_proxy": False,
            "import_type": "existing",
        }
    )
    p2, c2 = provision_fresh_browser_profile("acc2", "firefox")
    mgr.upsert(
        {
            "id": "acc2",
            "name": "A2",
            "browser_type": "firefox",
            "portable_path": p2,
            "cookie_path": c2,
            "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
            "use_proxy": False,
            "import_type": "existing",
        }
    )
    with pytest.raises(ValueError, match="trùng"):
        mgr.update_account_fields("acc2", {"portable_path": p1})
