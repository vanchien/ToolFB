"""Chia video job chờ đăng cho nhiều Page."""

from __future__ import annotations

from src.services.video_editor.page_split import (
    apply_page_split,
    auto_split_counts,
    collect_split_indexes,
    pages_already_used,
    preview_page_split,
)


def _items(n: int) -> list[dict[str, str]]:
    return [{"video_path": f"/v{i}.mp4"} for i in range(n)]


def _dests(n: int) -> list[dict[str, str]]:
    return [{"account_id": "acc", "page_id": f"p{i}"} for i in range(n)]


def test_auto_split_counts_even() -> None:
    assert auto_split_counts(98, 16) == [7, 7] + [6] * 14
    assert sum(auto_split_counts(98, 16)) == 98
    assert auto_split_counts(3, 5) == [1, 1, 1]
    assert auto_split_counts(0, 5) == []


def test_auto_mode_assigns_all_videos() -> None:
    items = _items(98)
    res = apply_page_split(items, _dests(16), None)
    assert res["assigned"] == 98
    assert res["leftover_indexes"] == []
    assert res["pages_used"] == 16
    info = preview_page_split(98, 16, None)
    assert info["leftover"] == 0 and info["min_per_page"] == 6 and info["max_per_page"] == 7


def test_fixed_mode_leaves_rest_for_other_pages_next_time() -> None:
    items = _items(98)
    first = apply_page_split(items, _dests(16), 5)
    assert first["assigned"] == 80
    assert len(first["leftover_indexes"]) == 18
    used = pages_already_used(items)
    assert used == {f"p{i}" for i in range(16)}
    free = collect_split_indexes(items, only_unassigned=True)
    assert len(free) == 18
    others = [{"account_id": "acc", "page_id": f"q{i}"} for i in range(4)]
    second = apply_page_split(items, others, None, only_unassigned=True)
    assert second["assigned"] == 18
    assert collect_split_indexes(items, only_unassigned=True) == []


def test_apply_page_split_keeps_one_account_per_facebook_page(monkeypatch) -> None:
    monkeypatch.setattr("src.utils.page_cross_account.load_page_owner_choices", lambda **_k: {})
    items = _items(4)
    dests = [
        {
            "account_id": "acc1",
            "page_id": "local-a",
            "page_name": "Happy",
            "page_url": "https://www.facebook.com/HappyStoriesTime",
        },
        {
            "account_id": "acc2",
            "page_id": "local-b",
            "page_name": "Happy",
            "page_url": "https://www.facebook.com/happystoriestime",
        },
        {
            "account_id": "acc1",
            "page_id": "local-c",
            "page_name": "Other",
            "page_url": "https://www.facebook.com/OtherPage",
        },
    ]
    res = apply_page_split(items, dests, None)
    assert res["pages_used"] == 2
    pages = {it["item_preset_fb_page_id"] for it in items if it.get("item_preset_fb_page_id")}
    assert pages == {"local-a", "local-c"}
