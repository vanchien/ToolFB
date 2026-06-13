"""Tests xuất tài khoản tab Tương tác → accounts.json."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.models.mapped_account import MappedAccount
from src.utils.account_proxy_mapper import (
    export_mapped_accounts_to_registry,
    mapped_account_eligible_for_registry_export,
    mapped_account_to_registry_record,
    parse_proxy_line_to_network,
)
from src.utils.db_manager import AccountsDatabaseManager


def _mapped(*, status: str = "login_ok", uid: str = "100092507808326") -> MappedAccount:
    return MappedAccount(
        account_id=f"UID_{uid}",
        auth=__import__("src.utils.account_proxy_mapper", fromlist=["parse_account_line"]).parse_account_line(
            f"{uid}|secret|OTP|mail@x.com||"
        ),
        network=parse_proxy_line_to_network("1.2.3.4:8080:user:pass"),
        use_proxy=True,
        status=status,
        cookie_path=f"data/cookies/UID_{uid}.json",
    )


def test_mapped_account_to_registry_record_uses_folder_import(tmp_path: Path) -> None:
    ma = _mapped()
    with patch(
        "src.services.facebook_session_persist.resolve_best_cookie_path_for_account",
        return_value=ma.cookie_path,
    ), patch("src.utils.account_proxy_mapper.enrich_account_dict_from_registry"):
        rec = mapped_account_to_registry_record(ma)
    assert rec["import_type"] == "folder"
    assert rec["facebook_uid"] == "100092507808326"
    assert rec["login_status"] == "active"
    assert "1.2.3.4" in str(rec["proxy"].get("host", ""))


def test_export_mapped_accounts_to_registry_add_and_update(tmp_path: Path) -> None:
    uid = "999000111222333"
    acc_json = tmp_path / "accounts.json"
    acc_json.write_text("[]\n", encoding="utf-8")
    profile_dir = tmp_path / "profiles" / "firefox" / "acc_export_test"
    profile_dir.mkdir(parents=True)
    db = AccountsDatabaseManager(acc_json)
    ma = _mapped(uid=uid)
    ma.storage.profile_path = str(profile_dir)

    with patch(
        "src.utils.account_proxy_mapper.mapped_account_eligible_for_registry_export",
        return_value=(True, ""),
    ), patch(
        "src.utils.account_proxy_mapper.sync_mapped_account_storage_from_registry",
    ), patch(
        "src.utils.account_proxy_mapper.apply_mapped_secrets_to_vault",
    ), patch(
        "src.utils.account_browser_profile.assert_portable_path_not_shared",
    ):
        res1 = export_mapped_accounts_to_registry([ma], db=db)
    assert len(res1["added"]) == 1
    rows = json.loads(acc_json.read_text(encoding="utf-8"))
    assert len(rows) == 1
    reg_id = res1["added"][0]

    with patch(
        "src.utils.account_proxy_mapper.mapped_account_eligible_for_registry_export",
        return_value=(True, ""),
    ), patch(
        "src.utils.account_proxy_mapper.sync_mapped_account_storage_from_registry",
    ), patch(
        "src.utils.account_proxy_mapper.apply_mapped_secrets_to_vault",
    ), patch(
        "src.utils.account_browser_profile.assert_portable_path_not_shared",
    ):
        res2 = export_mapped_accounts_to_registry([ma], db=db)
    assert reg_id in res2["updated"]
    assert len(json.loads(acc_json.read_text(encoding="utf-8"))) == 1


def test_eligible_requires_session() -> None:
    ma = _mapped(status="pending")
    with patch(
        "src.services.facebook_session_persist.cookie_file_has_session",
        return_value=False,
    ), patch(
        "src.utils.account_proxy_mapper.sync_mapped_account_storage_from_registry",
    ):
        ok, reason = mapped_account_eligible_for_registry_export(ma)
    assert ok is False
    assert "cookie" in reason.lower() or "trạng thái" in reason.lower()
