"""Bảng Page nhận: lọc tài khoản / phân loại không được để list trống."""

from src.gui.share_tab import preferred_account_id_for_ticks, resolve_account_filter_id
from src.utils.page_categories import page_matches_category


def test_resolve_account_filter_by_name_when_label_map_misses() -> None:
    pages = [
        {"account_id": "acc_mt", "account_name": "Minh Tuyết", "iid": "p1"},
        {"account_id": "acc_ag", "account_name": "Andy Garcia", "iid": "p2"},
    ]
    assert resolve_account_filter_id("Minh Tuyết", {}, pages) == "acc_mt"
    assert resolve_account_filter_id("Andy Garcia", {"Andy Garcia": "acc_ag"}, pages) == "acc_ag"
    assert resolve_account_filter_id("Tất cả tài khoản", {}, pages) == "*"
    assert resolve_account_filter_id("Chọn tài khoản", {}, pages) == ""


def test_preferred_account_follows_ticked_pages_not_first_in_list() -> None:
    pages = [
        {"iid": "a1", "account_id": "acc_andy"},
        {"iid": "a2", "account_id": "acc_andy"},
        {"iid": "m1", "account_id": "acc_mt"},
        {"iid": "m2", "account_id": "acc_mt"},
        {"iid": "m3", "account_id": "acc_mt"},
    ]
    ticks = {"m1", "m2", "m3"}
    assert preferred_account_id_for_ticks(pages, ticks) == "acc_mt"
    assert preferred_account_id_for_ticks(pages, {"a1"}) == "acc_andy"
    assert preferred_account_id_for_ticks(pages, set()) == ""


def test_monetized_filter_hides_uncategorized_pages() -> None:
    row = {"page_category": "", "account_id": "acc_mt"}
    assert page_matches_category(row, "")
    assert not page_matches_category(row, "monetized")
    assert page_matches_category(row, "none")
