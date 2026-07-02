"""Tests ghép account/proxy."""

from __future__ import annotations

import pytest

from unittest.mock import patch

from src.models.mapped_account import MappedAccount, MappedAccountNetwork
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
    parse_account_line,
    parse_proxy_line_to_network,
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


def test_proxy_identity_key_ignores_user_pass() -> None:
    n1 = parse_proxy_line_to_network("1.2.3.4:8080:userA:passA")
    n2 = parse_proxy_line_to_network("1.2.3.4:8080:userB:passB")
    assert proxy_identity_key_for_network(n1) == proxy_identity_key_for_network(n2) == "1.2.3.4:8080"


def test_map_rejects_same_ip_for_two_accounts() -> None:
    with pytest.raises(AccountProxyMappingError, match="1.2.3.4:80"):
        map_accounts_with_proxies(
            ["1000001|p1||||", "1000002|p2||||"],
            ["1.2.3.4:80:u:p", "1.2.3.4:80:u2:p2"],
            max_concurrent=2,
            persist_secrets=False,
        )


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

