"""Tests ghép account/proxy."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from src.models.mapped_account import MappedAccount, MappedAccountAuth, MappedAccountNetwork
from src.utils.account_proxy_mapper import (
    AccountProxyMappingError,
    duplicate_proxy_assignments,
    enrich_account_dict_from_registry,
    ensure_account_dict_proxy_live,
    ensure_mapped_proxy_live,
    filter_lines_by_live_proxy,
    map_accounts_with_proxies,
    mapped_account_to_account_dict,
    network_to_proxy_config,
    attach_imported_cookie,
    parse_account_line,
    parse_proxy_line_to_network,
    playwright_cookies_from_import,
    write_imported_cookie_file,
    prepare_account_dict_for_browser_run,
    proxy_dict_to_network,
    proxy_identity_key_for_network,
    reassign_proxies_from_pool,
)
from src.services.human_interaction_pool import validate_pool_start


def test_parse_account_line_pipe() -> None:
    auth = parse_account_line("1000001|Pass1|SECRET|a@b.com|mailpass|backup@x.com")
    assert auth.username == "1000001"
    assert auth.password == "Pass1"
    assert auth.two_fa_secret == "SECRET"
    assert auth.email == "a@b.com"


def test_map_accounts_with_proxies_ok() -> None:
    acc = ["1000001|p1||||"]
    px = ["203.0.0.1:8080:u:p", "203.0.0.2:8080:u2:p2"]
    mapped = map_accounts_with_proxies(acc, px, max_concurrent=2, persist_secrets=False)
    assert len(mapped) == 1
    assert mapped[0].account_id == "UID_1000001"
    assert "203.0.0.1" in mapped[0].network.proxy_server


def test_map_allows_fewer_proxies_than_threads_for_preview() -> None:
    """Ghép/hiển thị không chặn khi proxy < số luồng — chỉ cần đủ cặp account/proxy."""
    mapped = map_accounts_with_proxies(
        ["1000001|p1||||", "1000002|p2||||"],
        ["1.2.3.4:80::"],
        max_concurrent=4,
        persist_secrets=False,
    )
    assert len(mapped) == 1


def test_parse_account_line_tab_excel() -> None:
    auth = parse_account_line("1000001\tPass1\tSECRET\ta@b.com\tmailpass\tbackup@x.com")
    assert auth.username == "1000001"
    assert auth.email == "a@b.com"
    assert auth.recovery_email == "backup@x.com"


def test_parse_cookie_nick_format() -> None:
    """UID|mật khẩu|2FA|cookie|mail khôi phục|mật khẩu mail."""
    auth = parse_account_line(
        "1000001|Pass1|JBSWY3DPEHPK3PXP|c_user=1000001; xs=abc|rec@x.com|mailpass",
        account_format="cookie",
    )
    assert auth.username == "1000001"
    assert auth.password == "Pass1"
    assert auth.two_fa_secret == "JBSWY3DPEHPK3PXP"
    assert auth.recovery_email == "rec@x.com"
    assert auth.email == ""
    assert auth.email_password == "mailpass"
    assert auth.imported_cookie == "c_user=1000001; xs=abc"


def test_recovery_email_is_not_stored_as_cookie() -> None:
    auth = parse_account_line(
        "1000001|Pass1|TOTP|rec@x.com|mailpass|c_user=1000001; xs=abc",
        account_format="cookie",
    )
    assert auth.imported_cookie == "c_user=1000001; xs=abc"
    assert auth.recovery_email == "rec@x.com"
    assert "rec@x.com" not in auth.imported_cookie


def test_cookie_nick_keeps_pipe_inside_cookie() -> None:
    auth = parse_account_line(
        "1000001|Pass1|TOTP|c_user=1000001; xs=ab|cd|rec@x.com|mp",
        account_format="cookie",
    )
    assert auth.imported_cookie == "c_user=1000001; xs=ab|cd"
    assert auth.recovery_email == "rec@x.com"
    assert auth.email_password == "mp"


def test_auto_format_mail_vs_cookie() -> None:
    mail = parse_account_line(
        "1000001|Pass1|S|a@b.com|mp|r@x.com",
        account_format="auto",
    )
    assert mail.email == "a@b.com"
    assert mail.imported_cookie == ""
    cookie = parse_account_line(
        "1000001|Pass1|S|c_user=1000001; xs=z|r@x.com|mp",
        account_format="auto",
    )
    assert cookie.imported_cookie.startswith("c_user=")
    assert cookie.recovery_email == "r@x.com"


def test_attach_imported_cookie_marks_cookie_login(tmp_path: Path) -> None:
    dest = tmp_path / "uid.json"
    ma = MappedAccount(
        account_id="UID_1000001",
        cookie_path=str(dest),
        auth=MappedAccountAuth(
            username="1000001",
            password="Pass1",
            imported_cookie="c_user=1000001; xs=abc",
        ),
    )
    attach_imported_cookie(ma)
    assert ma.login_via_cookie is True
    assert ma.auth.imported_cookie == ""
    assert "cookie" in ma.status_detail.lower()


def test_write_imported_cookie_file_has_session(tmp_path: Path) -> None:
    from src.services.facebook_session_persist import cookie_file_has_session

    dest = tmp_path / "uid.json"
    count = write_imported_cookie_file(str(dest), "c_user=1000001; xs=abc; datr=zz")
    assert count == 3
    assert cookie_file_has_session(dest)
    baked = playwright_cookies_from_import(
        '[{"name":"c_user","value":"9","domain":".facebook.com","path":"/"}]'
    )
    assert baked[0]["value"] == "9"


def test_parse_proxy_line_to_network() -> None:
    net = parse_proxy_line_to_network("203.175.96.175:25308:admin:secret")
    assert "203.175.96.175" in net.proxy_server
    assert net.proxy_username == "admin"


def test_map_duplicate_uid_gets_unique_profile_ids() -> None:
    acc = ["1000001|p1||||", "1000001|p2||||"]
    px = ["1.2.3.4:80::", "5.6.7.8:80::"]
    mapped = map_accounts_with_proxies(acc, px, max_concurrent=2, persist_secrets=False)
    assert len(mapped) == 2
    assert mapped[0].account_id == "UID_1000001"
    assert mapped[1].account_id == "UID_1000001_L2"
    assert mapped[0].storage.profile_path != mapped[1].storage.profile_path


def test_filter_lines_by_live_proxy_keeps_pairs() -> None:
    acc = ["1000001|p1||||", "1000002|p2||||", "1000003|p3||||"]
    px = ["1.1.1.1:80:u:p", "2.2.2.2:80:u:p", "3.3.3.3:80:u:p"]

    def _fake_line(line: str, *, timeout: float = 18.0):
        from src.utils.proxy_check import apply_proxy_scheme_to_config, parse_proxy_line

        if "2.2.2.2" in line:
            return False, "timeout", "none", {}
        px = apply_proxy_scheme_to_config(parse_proxy_line(line), "http")
        return True, "9.9.9.9", "http", px

    with patch("src.utils.account_proxy_mapper.check_proxy_line", side_effect=_fake_line):
        live_acc, live_px, dead, schemes = filter_lines_by_live_proxy(acc, px, max_workers=2)

    assert len(live_px) == 2
    assert schemes.get("http") == 2
    assert len(live_acc) == 2
    assert live_acc[0].startswith("1000001")
    assert live_acc[1].startswith("1000003")
    assert len(dead) == 1
    assert dead[0]["line_no"] == 2


def test_proxy_identity_key_splits_gateway_sessions() -> None:
    """Cùng host:port nhưng user khác = proxy mới (gateway SOCKS)."""
    n1 = parse_proxy_line_to_network("1.2.3.4:8080:userA:passA")
    n2 = parse_proxy_line_to_network("1.2.3.4:8080:userB:passB")
    bare = parse_proxy_line_to_network("1.2.3.4:8080")
    assert proxy_identity_key_for_network(n1) != proxy_identity_key_for_network(n2)
    assert proxy_identity_key_for_network(bare) == "1.2.3.4:8080"
    assert "usera" in proxy_identity_key_for_network(n1)


def test_map_skips_duplicate_proxy_when_no_spare() -> None:
    """Hai dòng cùng một proxy và không còn proxy khác — chỉ ghép tài khoản đầu."""
    mapped = map_accounts_with_proxies(
        ["1000001|p1||||", "1000002|p2||||"],
        ["1.2.3.4:80:u:p", "1.2.3.4:80:u:p"],
        max_concurrent=2,
        persist_secrets=False,
    )
    assert len(mapped) == 1
    assert mapped[0].account_id == "UID_1000001"


def test_map_skips_taken_proxy_and_uses_next_in_list(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proxy đã gắn accounts.json thì bỏ, lấy dòng proxy kế tiếp còn trống."""
    taken = parse_proxy_line_to_network("1.1.1.1:80:u:p")
    taken_key = proxy_identity_key_for_network(taken)
    monkeypatch.setattr(
        "src.utils.account_proxy_mapper.load_registry_proxy_index",
        lambda: {taken_key: "acc_other"},
    )
    mapped = map_accounts_with_proxies(
        ["1000001|p1||||", "1000002|p2||||"],
        ["1.1.1.1:80:u:p", "2.2.2.2:80:u:p", "3.3.3.3:80:u:p"],
        max_concurrent=2,
        persist_secrets=False,
    )
    assert len(mapped) == 2
    assert "2.2.2.2" in mapped[0].network.proxy_server
    assert "3.3.3.3" in mapped[1].network.proxy_server


def test_map_skips_proxy_already_in_open_queue() -> None:
    """Proxy đang nằm trong hàng đợi mở thì bỏ, lấy dòng kế tiếp."""
    taken = parse_proxy_line_to_network("9.9.9.9:80:u:p")
    taken_key = proxy_identity_key_for_network(taken)
    mapped = map_accounts_with_proxies(
        ["1000001|p1||||"],
        ["9.9.9.9:80:u:p", "8.8.8.8:80:u:p"],
        max_concurrent=1,
        persist_secrets=False,
        extra_blocked={taken_key: "UID_other"},
    )
    assert len(mapped) == 1
    assert "8.8.8.8" in mapped[0].network.proxy_server


def test_map_allows_same_gateway_different_user() -> None:
    mapped = map_accounts_with_proxies(
        ["1000001|p1||||", "1000002|p2||||"],
        ["1.2.3.4:80:userA:passA", "1.2.3.4:80:userB:passB"],
        max_concurrent=2,
        persist_secrets=False,
    )
    assert len(mapped) == 2
    assert proxy_identity_key_for_network(mapped[0].network) != proxy_identity_key_for_network(
        mapped[1].network
    )


def test_reassign_prefers_newly_added_gateway_proxy() -> None:
    """Dòng proxy mới (cùng host, user khác) được gán thay dòng đang lỗi."""
    old = parse_proxy_line_to_network("10.0.0.1:1080:olduser:oldpass")
    ma = MappedAccount(
        account_id="UID_9",
        network=old,
        use_proxy=True,
        status="proxy_error",
    )
    res = reassign_proxies_from_pool(
        [ma],
        all_accounts=[ma],
        proxy_lines=[
            "10.0.0.1:1080:olduser:oldpass",
            "10.0.0.1:1080:newuser:newpass",
        ],
    )
    assert "UID_9" in res["updated"]
    assert "newuser" in (ma.network.proxy_username or "")


def test_validate_pool_start_rejects_duplicate_proxies() -> None:
    mapped = [
        MappedAccount(
            account_id="UID_1",
            network=parse_proxy_line_to_network("1.2.3.4:80:u:p"),
            use_proxy=True,
        ),
        MappedAccount(
            account_id="UID_2",
            network=parse_proxy_line_to_network("1.2.3.4:80:u2:p"),
            use_proxy=True,
        ),
    ]
    with pytest.raises(AccountProxyMappingError, match="IP:port"):
        validate_pool_start(2, 2, 2, unique_proxy_count=1, accounts=mapped)


def test_validate_pool_start_rejects_duplicate_account_ids() -> None:
    px = parse_proxy_line_to_network("1.2.3.4:80:u:p")
    mapped = [
        MappedAccount(account_id="UID_1", network=px, use_proxy=True),
        MappedAccount(account_id="UID_1", network=parse_proxy_line_to_network("5.6.7.8:80:u:p"), use_proxy=True),
    ]
    with pytest.raises(AccountProxyMappingError, match="Trùng account_id"):
        validate_pool_start(2, 2, 2, unique_proxy_count=2, accounts=mapped)


def test_ensure_mapped_proxy_live_normalizes_socks5() -> None:
    ma = MappedAccount(
        account_id="UID_1",
        network=proxy_dict_to_network({"host": "1.2.3.4", "port": 1080, "user": "u", "pass": "p"}),
        use_proxy=True,
    )
    with patch("src.utils.account_proxy_mapper.check_proxy", return_value=(True, "9.9.9.9", "socks5")):
        ok, msg = ensure_mapped_proxy_live(ma)
    assert ok is True
    px = network_to_proxy_config(ma.network)
    assert str(px["host"]).startswith("socks5://")


def test_ensure_account_dict_proxy_live_normalizes_socks5() -> None:
    acc = {
        "id": "acc_test",
        "use_proxy": True,
        "proxy": {"host": "1.2.3.4", "port": 1080, "user": "u", "pass": "p"},
    }
    with patch("src.utils.proxy_check.check_proxy", return_value=(True, "9.9.9.9", "socks5")):
        ok, msg = ensure_account_dict_proxy_live(acc)
    assert ok is True
    assert str(acc["proxy"]["host"]).startswith("socks5://")


def test_prepare_account_dict_for_browser_run_raises_on_dead_proxy() -> None:
    acc = {
        "id": "acc_test",
        "use_proxy": True,
        "portable_path": "data/profiles/firefox/acc_test",
        "proxy": {"host": "1.2.3.4", "port": 1080, "user": "", "pass": ""},
    }
    with patch("src.utils.account_proxy_mapper.enrich_account_dict_from_registry"):
        with patch("src.utils.account_browser_profile.ensure_account_browser_profile_ready"):
            with patch(
                "src.utils.account_proxy_mapper.ensure_account_dict_proxy_live",
                return_value=(False, "TIMEOUT"),
            ):
                with pytest.raises(ValueError, match="Proxy chưa kết nối"):
                    prepare_account_dict_for_browser_run(acc)


def test_enrich_registry_sets_canonical_id_for_uid_import() -> None:
    """Dòng ghép ``UID_…`` + profile ``acc_…`` trong JSON — id dict phải khớp marker profile."""
    acc: dict = {
        "id": "UID_100092564235770",
        "facebook_uid": "100092564235770",
        "portable_path": "data/profiles/firefox/UID_100092564235770",
    }
    registry_row = {
        "id": "acc_04e7df5e18",
        "facebook_uid": "100092564235770",
        "portable_path": "data/profiles/firefox/acc_04e7df5e18",
        "cookie_path": "data/cookies/acc_04e7df5e18.json",
    }

    class _FakeDb:
        def load_all(self):
            return [registry_row]

    with patch("src.utils.db_manager.AccountsDatabaseManager", _FakeDb):
        enrich_account_dict_from_registry(acc)

    assert acc["id"] == "acc_04e7df5e18"
    assert acc["portable_path"] == "data/profiles/firefox/acc_04e7df5e18"


def test_mapped_account_to_account_dict_profile_owner_matches_registry() -> None:
    ma = MappedAccount(
        account_id="UID_100092564235770",
        auth=parse_account_line("100092564235770|secret||||"),
        use_proxy=False,
    )
    registry_row = {
        "id": "acc_04e7df5e18",
        "facebook_uid": "100092564235770",
        "portable_path": "data/profiles/firefox/acc_04e7df5e18",
    }

    class _FakeDb:
        def load_all(self):
            return [registry_row]

    with patch("src.utils.db_manager.AccountsDatabaseManager", _FakeDb):
        out = mapped_account_to_account_dict(ma)

    assert out["id"] == "acc_04e7df5e18"
    assert out["portable_path"] == "data/profiles/firefox/acc_04e7df5e18"
    assert out["password_ref"] == "account:UID_100092564235770"


def test_enrich_merges_registry_proxy_auth_for_capsolver() -> None:
    """Tab Đăng nhập chỉ dán ip:port — vẫn lấy admin254 từ accounts.json cho CapSolver."""
    acc = {
        "id": "UID_100092564235770",
        "facebook_uid": "100092564235770",
        "use_proxy": True,
        "proxy": {"host": "socks5://160.250.183.92", "port": 17999, "user": "", "pass": ""},
    }
    registry_row = {
        "id": "acc_04e7df5e18",
        "facebook_uid": "100092564235770",
        "use_proxy": True,
        "proxy": {
            "host": "socks5://160.250.183.92",
            "port": 17999,
            "user": "admin254",
            "pass": "admin254",
        },
    }

    class _FakeDb:
        def load_all(self):
            return [registry_row]

    with patch("src.utils.db_manager.AccountsDatabaseManager", _FakeDb):
        enrich_account_dict_from_registry(acc)

    assert acc["proxy"]["user"] == "admin254"
    assert acc["proxy"]["pass"] == "admin254"
    assert acc["use_proxy"] is True


def test_reassign_proxies_from_pool_skips_duplicate_and_failed() -> None:
    ma1 = MappedAccount(
        account_id="UID_1",
        network=parse_proxy_line_to_network("1.1.1.1:80::"),
        use_proxy=True,
        status="proxy_error",
    )
    ma2 = MappedAccount(
        account_id="UID_2",
        network=parse_proxy_line_to_network("2.2.2.2:80::"),
        use_proxy=True,
        status="running",
    )
    pool = ["1.1.1.1:80::", "2.2.2.2:80::", "3.3.3.3:80::"]
    res = reassign_proxies_from_pool(
        [ma1],
        all_accounts=[ma1, ma2],
        proxy_lines=pool,
    )
    assert "UID_1" in res["updated"]
    assert proxy_identity_key_for_network(ma1.network) == "3.3.3.3:80"
    assert ma1.status == "pending"


def test_reassign_proxies_exhausted_pool() -> None:
    ma1 = MappedAccount(
        account_id="UID_1",
        network=parse_proxy_line_to_network("1.1.1.1:80::"),
        use_proxy=True,
        status="proxy_error",
    )
    ma2 = MappedAccount(
        account_id="UID_2",
        network=parse_proxy_line_to_network("2.2.2.2:80::"),
        use_proxy=True,
        status="running",
    )
    res = reassign_proxies_from_pool(
        [ma1],
        all_accounts=[ma1, ma2],
        proxy_lines=["2.2.2.2:80::"],
    )
    assert not res["updated"]
    assert res["skipped"][0][0] == "UID_1"


def test_proxy_dict_from_accounts_json_parses_url_string() -> None:
    from src.utils.proxy_check import proxy_dict_from_accounts_json

    px = proxy_dict_from_accounts_json("socks5://admin254:secret@157.15.38.223:29620")
    assert px["user"] == "admin254"
    assert px["pass"] == "secret"
    assert "157.15.38.223" in str(px["host"])
    assert px["port"] == 29620


def test_assert_proxy_allows_uid_and_registry_same_account(tmp_path, monkeypatch) -> None:
    """UID_ trên tab Tương tác + acc_ trong registry — cùng proxy, cùng facebook_uid."""
    from src.models.mapped_account import MappedAccountAuth, MappedAccountNetwork, MappedAccountStorage
    from src.utils.account_proxy_mapper import (
        assert_proxy_exclusive_among_accounts,
        enrich_account_dict_from_registry,
        mapped_account_to_account_dict,
    )

    reg = [
        {
            "id": "acc_04e7df5e18",
            "facebook_uid": "100092564235770",
            "portable_path": "data/profiles/firefox/acc_04e7df5e18",
            "profile_path": "data/profiles/firefox/acc_04e7df5e18",
            "cookie_path": "data/cookies/acc_04e7df5e18.json",
            "proxy": {
                "host": "socks5://157.66.252.120",
                "port": 20402,
                "user": "admin254",
                "pass": "admin254",
            },
            "use_proxy": True,
        }
    ]

    class _FakeDb:
        def load_all(self):
            return reg

    monkeypatch.setattr("src.utils.db_manager.AccountsDatabaseManager", _FakeDb)

    net = MappedAccountNetwork(
        proxy_server="socks5://157.66.252.120:20402",
        proxy_username="admin254",
        proxy_password="admin254",
    )
    login_ma = MappedAccount(
        account_id="acc_04e7df5e18",
        auth=MappedAccountAuth(username="100092564235770", password="pw"),
        network=net,
        use_proxy=True,
        storage=MappedAccountStorage(profile_path="data/profiles/firefox/acc_04e7df5e18"),
    )
    interaction_ma = MappedAccount(
        account_id="UID_100092564235770",
        auth=MappedAccountAuth(username="huiylhtg7503@hotmail.com", password="pw"),
        network=net,
        use_proxy=True,
        storage=MappedAccountStorage(profile_path="data/profiles/firefox/UID_100092564235770"),
    )

    registry_index = {"157.66.252.120:20402": "acc_04e7df5e18"}
    assert_proxy_exclusive_among_accounts(
        [login_ma, interaction_ma],
        registry_index=registry_index,
        context="mở profile trình duyệt",
    )

    acc = mapped_account_to_account_dict(interaction_ma)
    enrich_account_dict_from_registry(acc)
    assert acc["id"] == "acc_04e7df5e18"
    assert acc["portable_path"] == "data/profiles/firefox/acc_04e7df5e18"
    assert acc["cookie_path"] == "data/cookies/acc_04e7df5e18.json"


def test_public_profile_html_live_and_die() -> None:
    from src.utils.account_proxy_mapper import (
        classify_public_profile_html,
        facebook_uid_for_live_check,
    )
    from src.models.mapped_account import MappedAccountAuth

    assert classify_public_profile_html(
        "<html><title>This content isn't available</title></html>"
    )[0] == "login_failed"
    locked, locked_detail = classify_public_profile_html(
        "<html><title>Nguyen Van A</title>"
        "<body>Bạn hiện không xem được nội dung này. "
        "Lỗi này thường do chủ sở hữu chỉ chia sẻ nội dung với một nhóm nhỏ "
        "hoặc đã xóa nội dung.</body></html>"
    )
    assert locked == "login_failed"
    assert locked_detail == "UID không còn hoạt động"
    status, detail = classify_public_profile_html("<html><title>Animals Being Derps</title></html>")
    assert status == "login_ok"
    assert "Animals Being Derps" in detail
    live_status, live_detail = classify_public_profile_html(
        "<html><title>Chung Chiến (Chung Văn Chiến) | Facebook</title>"
        "<script>this content isn't available đã xóa nội dung</script>"
        "<body><h1>Chung Chiến (Chung Văn Chiến)</h1>"
        "1,3K người theo dõi · 414 đang theo dõi"
        "Bài viết Giới thiệu Reels Ảnh</body></html>"
    )
    assert live_status == "login_ok"
    assert "Chung Chiến" in live_detail
    locked_entities, locked_entities_detail = classify_public_profile_html(
        "<html><title>Nguyen Van A</title><body>"
        "B&#7841;n hi&#7879;n kh&#244;ng xem &#273;&#432;&#7907;c n&#7897;i dung n&#224;y"
        "</body></html>"
    )
    assert locked_entities == "login_failed"
    assert locked_entities_detail == "UID không còn hoạt động"
    assert classify_public_profile_html(
        "<html><title>Lỗi</title>"
        '<meta property="og:title" content="Đăng nhập hoặc đăng ký để xem" />'
        "<body>Trình duyệt này không hỗ trợ Facebook</body></html>"
    )[0] == "error"
    assert classify_public_profile_html(
        "<html><title>Facebook</title>"
        '<meta property="og:title" content="Chung Chiến" />'
        "<body>Sorry, something went wrong</body></html>"
    )[0] == "error"
    assert classify_public_profile_html(
        "<html><title>Facebook</title><body>Sorry, something went wrong</body></html>"
    )[0] == "error"
    assert classify_public_profile_html("<html><title>Log in</title><form></form></html>")[0] == "error"
    err_status, err_detail = classify_public_profile_html("<html><title>Error Facebook</title></html>")
    assert err_status == "error"
    assert "Còn hoạt động" not in err_detail
    assert "hoạt động" in err_detail

    mail = MappedAccount(
        account_id="import_mail",
        auth=MappedAccountAuth(username="a@b.c", password="pw"),
    )
    numeric = MappedAccount(
        account_id="UID_100092564235770",
        auth=MappedAccountAuth(username="100092564235770", password="pw"),
    )
    assert facebook_uid_for_live_check(mail) == ""
    assert facebook_uid_for_live_check(numeric) == "100092564235770"
    cookie_mail = MappedAccount(
        account_id="import_mail",
        auth=MappedAccountAuth(
            username="a@b.c",
            imported_cookie="c_user=100025964301122; xs=abc",
        ),
    )
    assert facebook_uid_for_live_check(cookie_mail) == "100025964301122"


def test_live_check_reads_c_user_and_sorts_live_above_die() -> None:
    from src.utils.account_proxy_mapper import facebook_uid_in_text, live_result_rank, map_pasted_accounts_for_live_check

    assert facebook_uid_in_text("c_user=100025964301122") == "100025964301122"
    assert facebook_uid_in_text('[{"name":"c_user","value":"100025964301122"}]') == "100025964301122"
    rows = map_pasted_accounts_for_live_check(
        [
            "AnhNguyen@gamesniperonline.xyz|pass|2fa|c_user=100025964301122; xs=abc|rec@x.com|mp",
            "100092564235770|pass",
        ]
    )
    assert rows[0].auth.username == "100025964301122"
    assert rows[0].auth.email == "AnhNguyen@gamesniperonline.xyz"
    assert [live_result_rank(label) for label in ("Live", "Lỗi", "Die")] == [0, 2, 3]


def test_extract_uid_lines_keeps_only_numbers() -> None:
    from src.utils.account_proxy_mapper import extract_uid_lines

    rows = extract_uid_lines(
        "\n".join(
            [
                "AnhNguyen@gamesniperonline.xyz|pass|2fa|c_user=100026106481037; xs=abc|rec@x.com",
                "100025928093406|pass|totp",
                "không có số",
                "c_user=100026106481037",
            ]
        )
    )
    assert rows == ["100026106481037", "100025928093406"]


def test_classify_account_live_labels() -> None:
    from src.utils.account_proxy_mapper import classify_account_live

    assert classify_account_live("login_ok", "") == "Live"
    assert classify_account_live("pending", "Đang chờ") == "Đang chờ"
    assert classify_account_live("running", "Đang đăng nhập") == "Đang check"
    assert classify_account_live("login_failed", "sai mật khẩu") == "Die"
    assert classify_account_live("login_failed", "vẫn ở checkpoint") == "Checkpoint"
    assert classify_account_live("proxy_error", "timeout") == "Lỗi proxy"


def test_map_pasted_live_check_isolated(tmp_path, monkeypatch) -> None:
    from src.utils.account_proxy_mapper import map_pasted_accounts_for_live_check

    monkeypatch.setattr("src.utils.paths.project_root", lambda: tmp_path)
    rows = map_pasted_accounts_for_live_check(
        ["10001|pass|totp|a@b.c||"],
        ["9.9.9.9:1080:u:p"],
    )
    assert len(rows) == 1
    assert "live_check" in rows[0].storage.profile_path.replace("\\", "/")
    assert rows[0].use_proxy is True


def test_refresh_post_account_uses_latest_login_page(tmp_path, monkeypatch) -> None:
    """Job đăng lấy proxy/cookie/profile/mật khẩu mới từ tab tương tác, không giữ bản cũ."""
    from src.utils.account_proxy_mapper import refresh_post_account_from_login_page

    profile = tmp_path / "profile_new"
    profile.mkdir()
    cookie = tmp_path / "cookie_new.json"
    cookie.write_text("{}", encoding="utf-8")
    vault_calls: list[tuple[str, dict]] = []

    def _fake_set(account_id: str, **kwargs):
        vault_calls.append((account_id, kwargs))

    monkeypatch.setattr("src.utils.account_credentials.set_account_credentials", _fake_set)
    monkeypatch.setattr(
        "src.utils.account_credentials.load_account_credential_bundle",
        lambda _stub: None,
    )

    old_net = {
        "proxy_server": "socks5://1.1.1.1:1080",
        "proxy_username": "olduser",
        "proxy_password": "oldpass",
    }
    new_net = {
        "proxy_server": "socks5://9.9.9.9:1080",
        "proxy_username": "newuser",
        "proxy_password": "newpass",
    }
    settings = {
        "mapped_accounts_login": [
            {
                "account_id": "UID_10001",
                "auth": {"username": "10001", "password": "pass-login", "email": "a@b.c"},
                "network": old_net,
                "storage": {"profile_path": str(tmp_path / "missing")},
                "cookie_path": str(tmp_path / "missing.json"),
                "use_proxy": True,
                "status": "pending",
            }
        ],
        "mapped_accounts_interaction": [
            {
                "account_id": "UID_10001",
                "auth": {
                    "username": "10001",
                    "password": "pass-moi",
                    "email": "a@b.c",
                    "two_fa_secret": "TOTPNEW",
                    "recovery_email": "rec@b.c",
                },
                "network": new_net,
                "storage": {"profile_path": str(profile)},
                "cookie_path": str(cookie),
                "use_proxy": True,
                "status": "login_ok",
            }
        ],
    }
    acc = {
        "id": "acc_registry",
        "facebook_uid": "10001",
        "email": "a@b.c",
        "proxy": {"host": "socks5://1.1.1.1", "port": 1080, "user": "olduser", "pass": "oldpass"},
        "use_proxy": True,
        "portable_path": str(tmp_path / "old_profile"),
        "cookie_path": str(tmp_path / "old_cookie.json"),
    }
    out = refresh_post_account_from_login_page(acc, settings=settings, persist=False)
    assert out["proxy"]["user"] == "newuser"
    assert out["proxy"]["pass"] == "newpass"
    assert "9.9.9.9" in str(out["proxy"]["host"])
    assert out["cookie_path"] == str(cookie)
    assert out["portable_path"] == str(profile)
    assert out["email"] == "a@b.c"
    assert out["recovery_email"] == "rec@b.c"
    assert out["totp_enabled"] is True
    written_ids = {item[0] for item in vault_calls}
    assert "UID_10001" in written_ids
    assert "acc_registry" in written_ids
    assert any(item[1].get("password") == "pass-moi" for item in vault_calls if item[0] == "acc_registry")

