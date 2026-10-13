"""Gộp gói dữ liệu: giữ máy đích, thêm tài khoản và page mới."""

from __future__ import annotations

import json

from src.services.tool_bundle_sync import (
    apply_settings,
    build_bundle,
    export_ai_account_dirs,
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
    copied_again, kept_again = write_missing_cookies(
        tmp_path,
        [{"relative_path": "data/cookies/a.json", "text": '{"keep":false}'}],
        replace=True,
    )
    assert copied_again == 1
    assert kept_again == 0
    assert json.loads(existing.read_text(encoding="utf-8"))["keep"] is False


def test_export_and_install_replaces_profile_folder(tmp_path) -> None:
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
    assert kept == 0
    assert (kept_profile / "cookies.sqlite").read_bytes() == b"session"
    assert installed == 2
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


def test_empty_proxy_is_replaced_by_synced_proxy() -> None:
    accounts, _id_map, report = merge_accounts(
        [
            {
                "id": "local-1",
                "facebook_uid": "10001",
                "name": "Máy này",
                "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
                "use_proxy": True,
            }
        ],
        [
            {
                "id": "other-1",
                "facebook_uid": "10001",
                "name": "Máy kia",
                "proxy": {"host": "9.9.9.9", "port": 1080, "user": "u", "pass": "p"},
                "use_proxy": True,
            }
        ],
    )
    assert report.accounts_added == 0
    kept = accounts[0]
    assert kept["proxy"]["host"] == "9.9.9.9"
    assert kept["proxy"]["port"] == 1080
    assert kept["use_proxy"] is True


def test_plan_merge_stamps_registry_proxy_from_interaction() -> None:
    merged, _report = plan_merge(
        local_accounts=[
            {
                "id": "acc_registry",
                "facebook_uid": "10001",
                "name": "Page",
                "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
            }
        ],
        local_pages=[],
        local_jobs=[],
        local_credentials={"accounts": {}},
        bundle={
            "accounts": [],
            "settings": {
                "human_interaction_settings.json": {
                    "mapped_accounts_interaction": [
                        {
                            "account_id": "UID_10001",
                            "auth": {"username": "10001"},
                            "network": {
                                "proxy_server": "socks5://8.8.8.8:1080",
                                "proxy_username": "uu",
                                "proxy_password": "pp",
                            },
                            "use_proxy": True,
                            "status": "login_ok",
                        }
                    ]
                }
            },
        },
    )
    proxy = merged["accounts"][0]["proxy"]
    assert proxy["user"] == "uu"
    assert "8.8.8.8" in str(proxy["host"])
    assert merged["accounts"][0]["use_proxy"] is True


def test_proxy_follows_account_onto_another_machine(tmp_path) -> None:
    """Xuất máy này rồi nạp máy kia: proxy, profile và TikTok vẫn gắn đúng tài khoản."""
    source = tmp_path / "source"
    interaction_profile = source / "data" / "profiles" / "firefox" / "UID_10001"
    interaction_profile.mkdir(parents=True)
    (interaction_profile / "cookies.sqlite").write_bytes(b"session")
    (source / "data" / "profiles" / "firefox" / "acc_local").mkdir(parents=True)
    (source / "config").mkdir()
    (source / "config" / "accounts.json").write_text(
        json.dumps(
            [
                {
                    "id": "acc_local",
                    "facebook_uid": "10001",
                    "name": "Nick",
                    "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
                    "portable_path": "data/profiles/firefox/acc_local",
                }
            ]
        ),
        encoding="utf-8",
    )
    (source / "config" / "pages.json").write_text("[]", encoding="utf-8")
    (source / "config" / "schedule_posts.json").write_text("[]", encoding="utf-8")
    (source / "config" / "human_interaction_settings.json").write_text(
        json.dumps(
            {
                "accounts_text": "10001|pass",
                "proxies_text": "9.9.9.9:1080:uu:pp",
                "mapped_accounts_interaction": [
                    {
                        "account_id": "UID_10001",
                        "auth": {"username": "10001", "password": "pass"},
                        "network": {
                            "proxy_server": "socks5://9.9.9.9:1080",
                            "proxy_username": "uu",
                            "proxy_password": "pp",
                        },
                        "storage": {"profile_path": "data/profiles/firefox/UID_10001"},
                        "use_proxy": True,
                        "status": "login_ok",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    tiktok_profile = source / "data" / "tiktok" / "profiles" / "tt1"
    tiktok_profile.mkdir(parents=True)
    (tiktok_profile / "cookies.sqlite").write_bytes(b"tt")
    (source / "data" / "tiktok" / "accounts.json").write_text(
        json.dumps(
            [
                {
                    "id": "tt1",
                    "name": "TT",
                    "profile_path": "data/tiktok/profiles/tt1",
                    "proxy": {
                        "enabled": True,
                        "server": "socks5://7.7.7.7:1080",
                        "username": "tu",
                        "password": "tp",
                    },
                }
            ]
        ),
        encoding="utf-8",
    )

    from src.services.tool_bundle_sync import bundle_from_project, export_profiles, install_profiles

    payload = bundle_from_project(source)
    data = payload["data"]
    assert data["accounts"][0]["proxy"]["user"] == "uu"
    assert data["accounts"][0]["use_proxy"] is True
    assert any(row["portable_path"].endswith("UID_10001") for row in data["profile_rows"])
    assert any(row["portable_path"].endswith("tt1") for row in data["profile_rows"])

    package = tmp_path / "pkg"
    package.mkdir()
    export_profiles(source, list(data["accounts"]) + list(data["profile_rows"]), package)
    assert (package / "data" / "profiles" / "firefox" / "UID_10001" / "cookies.sqlite").is_file()
    assert (package / "data" / "tiktok" / "profiles" / "tt1" / "cookies.sqlite").is_file()

    merged, _report = plan_merge(
        local_accounts=[
            {
                "id": "acc_other",
                "facebook_uid": "10001",
                "name": "Máy kia",
                "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
            }
        ],
        local_pages=[],
        local_jobs=[],
        local_credentials={"accounts": {}},
        bundle=data,
    )
    assert merged["accounts"][0]["id"] == "acc_other"
    assert merged["accounts"][0]["proxy"]["user"] == "uu"
    assert any(str(job.get("source") or "").endswith("UID_10001") for job in merged["profile_jobs"])

    dest = tmp_path / "dest"
    (dest / "config").mkdir(parents=True)
    (dest / "config" / "human_interaction_settings.json").write_text(
        json.dumps(
            {
                "mapped_accounts_interaction": [
                    {
                        "account_id": "import_10001",
                        "auth": {"username": "10001", "password": ""},
                        "network": {"proxy_server": "", "proxy_username": "", "proxy_password": ""},
                        "use_proxy": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    assert apply_settings(dest, data["settings"]) >= 1
    saved = json.loads((dest / "config" / "human_interaction_settings.json").read_text(encoding="utf-8"))
    mapped = saved["mapped_accounts_interaction"]
    assert len(mapped) == 1
    assert mapped[0]["account_id"] == "import_10001"
    assert mapped[0]["network"]["proxy_username"] == "uu"
    assert "9.9.9.9" in mapped[0]["network"]["proxy_server"]
    assert mapped[0]["auth"]["password"] == "pass"
    tiktok = json.loads((dest / "data" / "tiktok" / "accounts.json").read_text(encoding="utf-8"))
    assert "7.7.7.7" in tiktok[0]["proxy"]["server"]
    install_profiles(package, dest, merged["profile_jobs"])
    assert (dest / "data" / "profiles" / "firefox" / "UID_10001" / "cookies.sqlite").is_file()


def test_interaction_accounts_merge_by_account_id(tmp_path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    (config / "human_interaction_settings.json").write_text(
        json.dumps(
            {
                "accounts_text": "cu@local",
                "mapped_accounts_interaction": [{"account_id": "a1", "status": "login_ok", "auth": {"password": ""}}],
            }
        ),
        encoding="utf-8",
    )
    updated = apply_settings(
        tmp_path,
        {
            "human_interaction_settings.json": {
                "accounts_text": "cu@local\nmoi@maykia",
                "proxies_text": "1.1.1.1:1080\n2.2.2.2:1080",
                "mapped_accounts_interaction": [
                    {"account_id": "a1", "status": "login_ok", "auth": {"password": "da-co"}},
                    {"account_id": "a2", "status": "login_ok", "auth": {"password": "mk2"}},
                ],
            }
        },
    )
    assert updated == 1
    saved = json.loads((config / "human_interaction_settings.json").read_text(encoding="utf-8"))
    assert "moi@maykia" in saved["accounts_text"]
    assert saved["proxies_text"].splitlines()[-1] == "2.2.2.2:1080"
    ids = [row["account_id"] for row in saved["mapped_accounts_interaction"]]
    assert ids == ["a1", "a2"]
    assert saved["mapped_accounts_interaction"][0]["auth"]["password"] == "da-co"


def test_bundle_includes_settings_cookies_and_ai_login(tmp_path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    (config / "ai_fleet.json").write_text(json.dumps({"auto_enabled": True}), encoding="utf-8")
    (config / "app_secrets.json").write_text(json.dumps({"openai_keys": [{"id": "k1"}]}), encoding="utf-8")
    (config / "update_channel.json").write_text(json.dumps({"channel": "stable"}), encoding="utf-8")
    cookies = tmp_path / "data" / "cookies"
    cookies.mkdir(parents=True)
    (cookies / "acc.json").write_text("[]", encoding="utf-8")
    (tmp_path / "data" / "tiktok").mkdir(parents=True)
    (tmp_path / "data" / "tiktok" / "accounts.json").write_text(json.dumps([{"id": "tt1"}]), encoding="utf-8")
    profile = tmp_path / "data" / "ai_accounts" / "chatgpt" / "k1" / "Default" / "Network"
    profile.mkdir(parents=True)
    (profile / "Cookies").write_bytes(b"session")
    payload = build_bundle(
        accounts=[{"id": "acc", "name": "A"}],
        pages=[],
        jobs=[],
        credentials={"accounts": {"acc": {"password": "secret"}}},
        project_root=tmp_path,
    )
    settings = payload["data"]["settings"]
    assert settings["ai_fleet.json"]["auto_enabled"] is True
    assert settings["app_secrets.json"]["openai_keys"][0]["id"] == "k1"
    assert settings["update_channel.json"]["channel"] == "stable"
    assert settings["data/tiktok/accounts.json"][0]["id"] == "tt1"
    assert payload["data"]["cookies"][0]["relative_path"] == "data/cookies/acc.json"
    package = tmp_path / "pkg"
    package.mkdir()
    copied = export_ai_account_dirs(tmp_path, package)
    assert copied == 1
    assert (package / "data" / "ai_accounts" / "chatgpt" / "k1" / "Default" / "Network" / "Cookies").is_file()


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


def test_profile_folder_replaces_cookie_only_dest(tmp_path) -> None:
    source = tmp_path / "pkg" / "data" / "profiles" / "firefox" / "acc"
    source.mkdir(parents=True)
    (source / "cookies.sqlite").write_bytes(b"tu-may-kia")
    (source / "prefs.js").write_text("user_pref('browser.startup.page', 1);\n", encoding="utf-8")
    (source / "storage").mkdir()
    (source / "storage" / "default").mkdir()
    dest = tmp_path / "app" / "data" / "profiles" / "firefox" / "acc"
    dest.mkdir(parents=True)
    (dest / "cookies.sqlite").write_bytes(b"")
    (dest / "cookies.sqlite-wal").write_bytes(b"dang-dang-nhap")
    copied, kept, missing = install_profiles(
        tmp_path / "pkg",
        tmp_path / "app",
        [{"dest": "data/profiles/firefox/acc", "source": "data/profiles/firefox/acc"}],
    )
    assert copied == 1
    assert kept == 0
    assert missing == 0
    assert (dest / "cookies.sqlite").read_bytes() == b"tu-may-kia"
    assert (dest / "prefs.js").is_file()
    assert (dest / "storage" / "default").is_dir()
    assert not (dest / "cookies.sqlite-wal").exists()


def test_tiktok_profile_from_other_machine_path_is_packed(tmp_path) -> None:
    profile = tmp_path / "data" / "tiktok" / "profiles" / "tt_acc_1"
    profile.mkdir(parents=True)
    (profile / "cookies.sqlite").write_bytes(b"tiktok")
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "human_interaction_settings.json").write_text("{}", encoding="utf-8")
    accounts_file = tmp_path / "data" / "tiktok" / "accounts.json"
    accounts_file.write_text(
        json.dumps(
            [
                {
                    "id": "tt_acc_1",
                    "profile_path": str(profile),
                    "proxy": {"enabled": True, "server": "socks5://203.0.113.8:1080"},
                }
            ]
        ),
        encoding="utf-8",
    )
    payload = build_bundle(accounts=[], pages=[], jobs=[], project_root=tmp_path)
    paths = {row["portable_path"] for row in payload["data"]["profile_rows"]}
    assert "data/tiktok/profiles/tt_acc_1" in paths
    saved = payload["data"]["settings"]["data/tiktok/accounts.json"][0]["profile_path"]
    assert saved == "data/tiktok/profiles/tt_acc_1"


def test_logged_in_firefox_is_packed_without_old_machine_lock(tmp_path) -> None:
    profile = tmp_path / "data" / "profiles" / "firefox" / "UID_61580000000001"
    profile.mkdir(parents=True)
    (profile / "cookies.sqlite").write_bytes(b"facebook-session")
    (profile / "compatibility.ini").write_text("LastPlatformDir=C:\\Users\\Old\\firefox", encoding="utf-8")
    (profile / "user.js").write_text('user_pref("network.proxy.http", "10.0.0.1");\n', encoding="utf-8")
    payload = build_bundle(accounts=[], pages=[], jobs=[], project_root=tmp_path)
    rows = payload["data"]["profile_rows"]
    assert rows[0]["portable_path"] == "data/profiles/firefox/UID_61580000000001"
    package = tmp_path / "package"
    copied, missing = export_profiles(tmp_path, rows, package)
    assert copied == 1
    assert missing == 0
    packed = package / "data" / "profiles" / "firefox" / "UID_61580000000001"
    assert (packed / "cookies.sqlite").is_file()
    assert not (packed / "compatibility.ini").exists()
    assert "network.proxy" not in (packed / "user.js").read_text(encoding="utf-8")
