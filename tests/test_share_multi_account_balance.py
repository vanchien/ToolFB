"""Phân bổ Page nhận đều theo nhiều tài khoản."""

from __future__ import annotations

from src.gui.share_tab import interleave_pages_by_account


def test_interleave_pages_by_account_round_robin() -> None:
    rows = [
        {"account_id": "a", "page_id": "1", "iid": "a1"},
        {"account_id": "a", "page_id": "2", "iid": "a2"},
        {"account_id": "a", "page_id": "3", "iid": "a3"},
        {"account_id": "b", "page_id": "4", "iid": "b1"},
        {"account_id": "b", "page_id": "5", "iid": "b2"},
    ]
    out = interleave_pages_by_account(rows)
    assert [r["iid"] for r in out] == ["a1", "b1", "a2", "b2", "a3"]


def test_interleave_single_account_unchanged() -> None:
    rows = [
        {"account_id": "a", "page_id": "1", "iid": "a1"},
        {"account_id": "a", "page_id": "2", "iid": "a2"},
    ]
    assert interleave_pages_by_account(rows) == rows
