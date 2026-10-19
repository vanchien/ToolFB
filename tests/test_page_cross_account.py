"""Cùng Page Facebook trên nhiều tài khoản — chọn 1 TK chạy."""

from __future__ import annotations

from pathlib import Path

from src.utils.page_cross_account import (
    facebook_page_group_key,
    filter_rows_one_account_per_facebook_page,
    find_cross_account_page_groups,
    load_page_owner_choices,
    receiving_identity_key,
    save_page_owner_choices,
    unresolved_owner_groups,
)


def _row(aid: str, *, name: str, url: str = "", fb: str = "", local: str = "") -> dict:
    return {
        "account_id": aid,
        "account_label": aid,
        "page_name": name,
        "page_label": name,
        "page_url": url,
        "target_url": url,
        "fb_page_id": fb,
        "page_id": local or fb or name,
        "id": local or f"{aid}-{name}",
        "iid": local or f"{aid}-{name}",
    }


def test_group_key_from_url_and_fb_id() -> None:
    a = _row("acc1", name="Happy Stories Time", url="https://www.facebook.com/HappyStoriesTime")
    b = _row("acc2", name="Happy Stories Time", url="https://www.facebook.com/happystoriestime/reels/")
    assert facebook_page_group_key(a) == facebook_page_group_key(b)
    assert facebook_page_group_key(a).startswith("vanity:")
    c = _row("acc3", name="X", fb="310447015484122", url="https://facebook.com/other")
    d = _row("acc4", name="X", fb="310447015484122")
    assert facebook_page_group_key(c) == facebook_page_group_key(d) == "id:310447015484122"


def test_find_cross_account_groups_ignores_single_owner() -> None:
    rows = [
        _row("a", name="Solo", url="https://www.facebook.com/SoloPage"),
        _row("a", name="Solo2", url="https://www.facebook.com/SoloTwo"),
        _row("b", name="Dup", url="https://www.facebook.com/DupPage"),
        _row("c", name="Dup", url="https://www.facebook.com/DupPage"),
    ]
    groups = find_cross_account_page_groups(rows)
    assert len(groups) == 1
    assert groups[0].title == "Dup"
    assert set(groups[0].account_ids) == {"b", "c"}


def test_filter_keeps_chosen_account_only() -> None:
    rows = [
        _row("b", name="Dup", url="https://www.facebook.com/DupPage", local="p-b"),
        _row("c", name="Dup", url="https://www.facebook.com/DupPage", local="p-c"),
        _row("a", name="Solo", url="https://www.facebook.com/SoloPage", local="p-a"),
    ]
    key = facebook_page_group_key(rows[0])
    out = filter_rows_one_account_per_facebook_page(rows, {key: "c"})
    ids = {(r["account_id"], r["iid"]) for r in out}
    assert ids == {("c", "p-c"), ("a", "p-a")}


def test_unresolved_until_choice_saved(tmp_path: Path) -> None:
    rows = [
        _row("b", name="Dup", url="https://www.facebook.com/DupPage"),
        _row("c", name="Dup", url="https://www.facebook.com/DupPage"),
    ]
    groups = find_cross_account_page_groups(rows)
    assert unresolved_owner_groups(groups, {}) == groups
    saved = save_page_owner_choices({groups[0].key: "b"}, root=tmp_path)
    assert saved[groups[0].key] == "b"
    loaded = load_page_owner_choices(root=tmp_path)
    assert unresolved_owner_groups(groups, loaded) == []


def test_receiving_identity_falls_back_to_local_id() -> None:
    row = {"iid": "local-1", "account_id": "a", "page_name": "1"}
    assert receiving_identity_key(row).startswith("row:")


def test_filter_collapses_same_url_without_account() -> None:
    rows = [
        {"iid": "x", "page_id": "", "target_url": "https://facebook.com/foo/"},
        {"iid": "y", "page_id": "", "target_url": "https://facebook.com/foo"},
    ]
    out = filter_rows_one_account_per_facebook_page(rows, {})
    assert [r["iid"] for r in out] == ["x"]


def test_should_run_only_chosen_account() -> None:
    from src.utils.page_cross_account import should_run_page_on_account

    rows = [
        _row("b", name="Dup", url="https://www.facebook.com/DupPage", local="p-b"),
        _row("c", name="Dup", url="https://www.facebook.com/DupPage", local="p-c"),
    ]
    key = facebook_page_group_key(rows[0])
    assert should_run_page_on_account(rows[0], "c", sibling_rows=rows, choices={key: "c"})
    assert not should_run_page_on_account(rows[0], "b", sibling_rows=rows, choices={key: "c"})
    assert should_run_page_on_account(rows[0], "b", sibling_rows=rows, choices={})
    assert not should_run_page_on_account(rows[1], "c", sibling_rows=rows, choices={})
