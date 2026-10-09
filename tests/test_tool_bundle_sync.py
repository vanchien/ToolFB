"""Gộp gói dữ liệu: giữ máy đích, thêm tài khoản và page mới."""

from __future__ import annotations

import json

from src.services.tool_bundle_sync import (
    apply_settings,
    build_bundle,
    export_profiles,
    install_profiles,
    merge_accounts,
    merge_pages,
    plan_merge,
    write_missing_cookies,
)


def test_merge_keeps_local_account_and_adds_new_page() -> None:
    local_accounts = [
        {
            "id": "local-1",
            "facebook_uid": "10001",
            "name": "Máy này",
            "email": "a@example.com",
            "portable_path": "data/profiles/local",
            "cookie_path": "data/cookies/local.json",
            "proxy": {"host": "10.0.0.1", "port": 1, "user": "u", "pass": "p"},
        }
    ]
    incoming_accounts = [
        {
            "id": "other-1",
            "facebook_uid": "10001",
            "name": "Máy kia",
            "email": "",
            "notes": "ghi chú mới",
            "portable_path": "D:/Old/data/profiles/other",
            "cookie_path": "D:/Old/data/cookies/other.json",
        },
        {
            "id": "other-2",
            "facebook_uid": "20002",
            "name": "Tài khoản mới",
            "email": "b@example.com",
            "portable_path": "C:/ToolFB/data/profiles/new",
        },
    ]
    accounts, id_map, account_report = merge_accounts(local_accounts, incoming_accounts)
    assert id_map["other-1"] == "local-1"
    assert id_map["other-2"] == "other-2"
    assert account_report.accounts_added == 1
    kept = next(row for row in accounts if row["id"] == "local-1")
    assert kept["name"] == "Máy này"
    assert kept["portable_path"] == "data/profiles/local"
    assert kept["notes"] == "ghi chú mới"
    added = next(row for row in accounts if row["id"] == "other-2")
    assert added["portable_path"] == "data/profiles/new"

    pages, _page_map, page_report = merge_pages(
        [
            {
                "id": "p-local",
                "account_id": "local-1",
                "page_name": "Page sẵn",
                "page_url": "https://www.facebook.com/ready",
                "fb_page_id": "555",
                "topic": "động vật",
            }
        ],
        [
            {
                "id": "p-old",
                "account_id": "other-1",
                "page_name": "Tên máy kia",
                "page_url": "https://www.facebook.com/ready",
                "fb_page_id": "555",
                "content_style": "vui",
            },
            {
                "id": "p-new",
                "account_id": "other-2",
                "page_name": "Page mới",
                "page_url": "https://www.facebook.com/new",
                "fb_page_id": "777",
            },
        ],
        id_map,
    )
    assert page_report.pages_added == 1
    assert page_report.pages_filled == 1
    ready = next(row for row in pages if row["id"] == "p-local")
    assert ready["page_name"] == "Page sẵn"
    assert ready["topic"] == "động vật"
    assert ready["content_style"] == "vui"
    new_page = next(row for row in pages if row["id"] == "p-new")
    assert new_page["account_id"] == "other-2"
    assert len(pages) == 2


def test_plan_merge_does_not_drop_local_only_rows() -> None:
    merged, report = plan_merge(
        local_accounts=[{"id": "a", "facebook_uid": "111111", "name": "A"}],
        local_pages=[
            {
                "id": "lp",
                "account_id": "a",
                "page_name": "Chỉ máy này",
                "page_url": "https://www.facebook.com/only-local",
            }
        ],
        local_jobs=[{"id": "job-local", "account_id": "a", "page_id": "lp"}],
        local_credentials={"accounts": {"a": {"password": "giữ"}}},
        bundle={
            "accounts": [{"id": "b", "facebook_uid": "222222", "name": "B"}],
            "pages": [
                {
                    "id": "ip",
                    "account_id": "b",
                    "page_name": "Page B",
                    "page_url": "https://www.facebook.com/b",
                }
            ],
            "schedule_posts": [{"id": "job-new", "account_id": "b", "page_id": "ip"}],
            "credentials": {"accounts": {"b": {"password": "mới"}, "a": {"password": "đừng ghi đè"}}},
        },
    )
    assert report.accounts_added == 1
    assert report.accounts_kept == 1
    assert {row["id"] for row in merged["accounts"]} == {"a", "b"}
    assert {row["page_name"] for row in merged["pages"]} == {"Chỉ máy này", "Page B"}
    assert {row["id"] for row in merged["schedule_posts"]} == {"job-local", "job-new"}
    assert merged["credentials"]["accounts"]["a"]["password"] == "giữ"
    assert merged["credentials"]["accounts"]["b"]["password"] == "mới"


def test_write_missing_cookies_keeps_existing_file(tmp_path) -> None:
    existing = tmp_path / "data" / "cookies" / "a.json"
    existing.parent.mkdir(parents=True)
    existing.write_text('{"keep":true}', encoding="utf-8")
    copied, kept = write_missing_cookies(
        tmp_path,
        [
            {"relative_path": "data/cookies/a.json", "text": '{"keep":false}'},
            {"relative_path": "data/cookies/b.json", "text": '{"new":true}'},
        ],
    )
    assert copied == 1
    assert kept == 1
    assert json.loads(existing.read_text(encoding="utf-8"))["keep"] is True
    assert (tmp_path / "data" / "cookies" / "b.json").is_file()


def test_export_and_install_profile_keeps_existing_login(tmp_path) -> None:
    source_root = tmp_path / "source"
    profile = source_root / "data" / "profiles" / "firefox" / "acc"
    profile.mkdir(parents=True)
    (profile / "cookies.sqlite").write_bytes(b"session")
    (profile / "cache2").mkdir()
    (profile / "cache2" / "junk").write_text("bo", encoding="utf-8")
    (profile / "parent.lock").write_text("lock", encoding="utf-8")
    package = tmp_path / "package"
    seen: list[str] = []
    copied, missing = export_profiles(
        source_root,
        [{"id": "acc", "name": "Acc", "portable_path": "data/profiles/firefox/acc"}],
        package,
        on_progress=lambda done, total, name: seen.append(f"{done}/{total}:{name}"),
    )
    assert seen == ["1/1:Acc"]
    assert copied == 1
    assert missing == 0
    packed = package / "data" / "profiles" / "firefox" / "acc"
    assert (packed / "cookies.sqlite").is_file()
    assert not (packed / "cache2").exists()
    assert not (packed / "parent.lock").exists()

    dest_root = tmp_path / "dest"
    kept_profile = dest_root / "data" / "profiles" / "firefox" / "local"
    kept_profile.mkdir(parents=True)
    (kept_profile / "cookies.sqlite").write_bytes(b"may-nay")
    merged, _report = plan_merge(
        local_accounts=[
            {
                "id": "local",
                "facebook_uid": "10001",
                "name": "Máy này",
                "portable_path": "data/profiles/firefox/local",
            }
        ],
        local_pages=[],
        local_jobs=[],
        local_credentials={"accounts": {}},
        bundle={
            "accounts": [
                {
                    "id": "acc",
                    "facebook_uid": "10001",
                    "name": "Máy kia",
                    "portable_path": "data/profiles/firefox/acc",
                },
                {
                    "id": "new",
                    "facebook_uid": "20002",
                    "name": "Mới",
                    "portable_path": "data/profiles/firefox/new",
                },
            ],
            "pages": [],
            "schedule_posts": [],
        },
    )
    new_profile = source_root / "data" / "profiles" / "firefox" / "new"
    new_profile.mkdir(parents=True)
    (new_profile / "cookies.sqlite").write_bytes(b"moi")
    export_profiles(source_root, [{"portable_path": "data/profiles/firefox/new"}], package)
    installed, kept, _missing = install_profiles(package, dest_root, list(merged["profile_jobs"]))
    assert kept == 1
    assert (kept_profile / "cookies.sqlite").read_bytes() == b"may-nay"
    assert installed == 1
    assert (dest_root / "data" / "profiles" / "firefox" / "new" / "cookies.sqlite").read_bytes() == b"moi"

    empty = dest_root / "data" / "profiles" / "firefox" / "trong"
    empty.mkdir()
    filled, held, absent = install_profiles(
        package,
        dest_root,
        [{"dest": "data/profiles/firefox/trong", "source": "data/profiles/firefox/acc"}],
    )
    assert filled == 1
    assert held == 0
    assert absent == 0
    assert (empty / "cookies.sqlite").read_bytes() == b"session"


def test_settings_keep_local_values_and_add_new_file(tmp_path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    (config / "human_interaction_settings.json").write_text(
        json.dumps({"watch_seconds": 20, "note": ""}),
        encoding="utf-8",
    )
    updated = apply_settings(
        tmp_path,
        {
            "human_interaction_settings.json": {"watch_seconds": 5, "note": "máy kia", "threads": 4},
            "facebook_groups.json": [{"id": "g1", "name": "Nhóm"}],
            "../accounts.json": [{"id": "x"}],
        },
    )
    assert updated == 2
    kept = json.loads((config / "human_interaction_settings.json").read_text(encoding="utf-8"))
    assert kept["watch_seconds"] == 20
    assert kept["note"] == "máy kia"
    assert kept["threads"] == 4
    groups = json.loads((config / "facebook_groups.json").read_text(encoding="utf-8"))
    assert groups[0]["id"] == "g1"
    assert not (tmp_path / "accounts.json").exists()


def test_bundle_rewrites_old_machine_profile_path(tmp_path) -> None:
    payload = build_bundle(
        accounts=[
            {
                "id": "acc",
                "portable_path": "D:/MayCu/ToolFB/data/profiles/firefox/acc",
                "cookie_path": "D:/MayCu/ToolFB/data/cookies/acc.json",
            }
        ],
        pages=[],
        jobs=[],
        project_root=tmp_path,
    )
    row = payload["data"]["accounts"][0]
    assert row["portable_path"] == "data/profiles/firefox/acc"
    assert row["profile_path"] == "data/profiles/firefox/acc"
    assert row["cookie_path"] == "data/cookies/acc.json"


def test_existing_firefox_wal_is_not_replaced(tmp_path) -> None:
    source = tmp_path / "pkg" / "data" / "profiles" / "firefox" / "acc"
    source.mkdir(parents=True)
    (source / "cookies.sqlite").write_bytes(b"tu-may-kia")
    dest = tmp_path / "app" / "data" / "profiles" / "firefox" / "acc"
    dest.mkdir(parents=True)
    (dest / "cookies.sqlite").write_bytes(b"")
    (dest / "cookies.sqlite-wal").write_bytes(b"dang-dang-nhap")
    copied, kept, missing = install_profiles(
        tmp_path / "pkg",
        tmp_path / "app",
        [{"dest": "data/profiles/firefox/acc", "source": "data/profiles/firefox/acc"}],
    )
    assert copied == 0
    assert kept == 1
    assert missing == 0
    assert (dest / "cookies.sqlite-wal").read_bytes() == b"dang-dang-nhap"
