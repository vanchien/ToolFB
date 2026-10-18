"""Chia đều video đã edit cho nhiều Page theo số clip/page."""

import random

from src.services.universal_video_downloader import parse_video_url_list
from src.services.video_editor.page_split import (
    apply_page_split,
    collect_split_indexes,
    format_split_preview,
    preview_page_split,
)


def _clip(i: int, *, page: str = "") -> dict:
    row = {"video_path": f"C:/v/{i}.mp4", "title": f"v{i}"}
    if page:
        row["item_publish_target"] = "facebook"
        row["item_preset_fb_account_id"] = "acc1"
        row["item_preset_fb_page_id"] = page
    return row


def test_preview_needs_pages_and_leftover() -> None:
    info = preview_page_split(50, 8, 5)
    assert info["need_pages"] == 10
    assert info["used_pages"] == 8
    assert info["assigned"] == 40
    assert info["leftover"] == 10
    text = format_split_preview(info)
    assert "50 video" in text
    assert "thừa 10" in text


def test_apply_chunks_in_list_order() -> None:
    items = [_clip(i) for i in range(12)]
    dests = [
        {"account_id": "a1", "page_id": "p1"},
        {"account_id": "a1", "page_id": "p2"},
        {"account_id": "a2", "page_id": "p3"},
    ]
    res = apply_page_split(items, dests, 5)
    assert res["assigned"] == 12
    assert res["pages_used"] == 3
    assert res["leftover_indexes"] == []
    assert [items[i]["item_preset_fb_page_id"] for i in range(5)] == ["p1"] * 5
    assert [items[i]["item_preset_fb_page_id"] for i in range(5, 10)] == ["p2"] * 5
    assert [items[i]["item_preset_fb_page_id"] for i in range(10, 12)] == ["p3"] * 2
    assert items[0]["item_preset_fb_account_id"] == "a1"
    assert items[10]["item_preset_fb_account_id"] == "a2"


def test_only_unassigned_skips_already_set() -> None:
    items = [_clip(0, page="old"), _clip(1), _clip(2), _clip(3)]
    dests = [{"account_id": "a1", "page_id": "p9"}]
    res = apply_page_split(items, dests, 2, only_unassigned=True)
    assert res["assigned"] == 2
    assert items[0]["item_preset_fb_page_id"] == "old"
    assert items[1]["item_preset_fb_page_id"] == "p9"
    assert items[2]["item_preset_fb_page_id"] == "p9"
    assert "item_preset_fb_page_id" not in items[3]


def test_selected_indexes_keep_order() -> None:
    items = [_clip(i) for i in range(6)]
    dests = [{"account_id": "a1", "page_id": "p1"}]
    res = apply_page_split(items, dests, 2, item_indexes=[5, 1, 2])
    assert collect_split_indexes(items, item_indexes=[5, 1, 2]) == [1, 2, 5]
    assert res["assigned"] == 2
    assert items[1]["item_preset_fb_page_id"] == "p1"
    assert items[2]["item_preset_fb_page_id"] == "p1"
    assert "item_preset_fb_page_id" not in items[5]


def test_shuffle_dests_uses_rng() -> None:
    items = [_clip(i) for i in range(6)]
    dests = [
        {"account_id": "a1", "page_id": "p1"},
        {"account_id": "a1", "page_id": "p2"},
        {"account_id": "a1", "page_id": "p3"},
    ]
    apply_page_split(items, dests, 2, shuffle_dests=True, rng=random.Random(1))
    pages = [items[i]["item_preset_fb_page_id"] for i in range(6)]
    assert pages[:2] == [pages[0]] * 2
    assert pages[2:4] == [pages[2]] * 2
    assert set(pages) <= {"p1", "p2", "p3"}


def test_parse_many_links_into_one_list() -> None:
    text = (
        "https://www.tiktok.com/@a/video/1\n"
        "https://youtu.be/abcdefghijk, https://www.facebook.com/reel/99\n"
        "https://www.tiktok.com/@a/video/1"
    )
    urls = parse_video_url_list(text)
    assert len(urls) == 3
    assert urls[0].endswith("/video/1")


def test_too_few_pages_leaves_leftover() -> None:
    items = [_clip(i) for i in range(7)]
    dests = [{"account_id": "a1", "page_id": "p1"}]
    res = apply_page_split(items, dests, 3)
    assert res["assigned"] == 3
    assert res["leftover_indexes"] == [3, 4, 5, 6]
