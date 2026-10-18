"""Phân loại Page: Bật kiếm tiền / Tiêu chí / Kéo view."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.utils.page_categories import (
    UNCATEGORIZED_KEY,
    category_filter_options,
    normalize_page_category,
    page_category_label,
    page_matches_category,
)
from src.utils.pages_manager import PagesManager


@pytest.fixture(autouse=True)
def _no_workspace_on_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.utils.pages_manager.ensure_page_workspace", lambda _pid: None)


def _page(pid: str, **extra: str) -> dict[str, str]:
    row = {
        "id": pid,
        "account_id": "acc1",
        "page_name": f"Page {pid}",
        "page_url": f"https://www.facebook.com/{pid}",
        "post_style": "post",
    }
    row.update(extra)
    return row


def test_normalize_accepts_key_and_label() -> None:
    assert normalize_page_category("monetized") == "monetized"
    assert normalize_page_category("Bật kiếm tiền") == "monetized"
    assert normalize_page_category("kéo view") == "views"
    assert normalize_page_category("") == ""
    assert normalize_page_category("Chưa phân loại") == ""
    assert normalize_page_category("lạ") == ""
    assert page_category_label("criteria") == "Tiêu chí"
    assert page_category_label("") == "Chưa phân loại"


def test_filter_options_count_and_match() -> None:
    rows = [_page("a", page_category="monetized"), _page("b", page_category="views"), _page("c")]
    options = dict((key, label) for label, key in category_filter_options(rows))
    assert options[""].endswith("(3)")
    assert options["monetized"] == "Bật kiếm tiền (1)"
    assert options["criteria"] == "Tiêu chí (0)"
    assert options[UNCATEGORIZED_KEY] == "Chưa phân loại (1)"
    assert [r["id"] for r in rows if page_matches_category(r, UNCATEGORIZED_KEY)] == ["c"]
    assert [r["id"] for r in rows if page_matches_category(r, "views")] == ["b"]


def test_set_category_for_many_pages_in_one_write(tmp_path: Path) -> None:
    path = tmp_path / "pages.json"
    path.write_text("[]", encoding="utf-8")
    mgr = PagesManager(json_path=path)
    mgr.upsert_many([_page("a"), _page("b"), _page("c")])

    assert mgr.set_category_for_ids(["a", "b"], "Tiêu chí") == 2
    by_id = {r["id"]: r for r in mgr.reload_from_disk()}
    assert by_id["a"]["page_category"] == "criteria"
    assert by_id["b"]["page_category"] == "criteria"
    assert "page_category" not in by_id["c"]

    assert mgr.set_category_for_ids(["a"], "criteria") == 0
    assert mgr.set_category_for_ids(["a"], "") == 1
    assert "page_category" not in {r["id"]: r for r in mgr.reload_from_disk()}["a"]


def test_choice_label_and_csv_show_category(tmp_path: Path) -> None:
    from src.gui.page_category_filter import page_choice_label
    from src.utils.pages_csv_export import write_pages_csv

    assert page_choice_label("Fan A", "views", "123") == "Fan A · Kéo view (123)"
    assert page_choice_label("Fan A", "", "123") == "Fan A (123)"
    out = tmp_path / "pages.csv"
    write_pages_csv(out, [_page("a", page_category="monetized"), _page("b")])
    text = out.read_text(encoding="utf-8-sig")
    assert "Phân loại" in text and "Bật kiếm tiền" in text


def test_created_page_keeps_chosen_category(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.services.page_creation import runtime

    path = tmp_path / "pages.json"
    path.write_text("[]", encoding="utf-8")
    monkeypatch.setattr("src.utils.pages_manager._default_pages_path", lambda: path)
    runtime.record_created_page(
        {"page_id": "999", "page_name": "New", "account_id": "acc1", "business_id": "", "page_category": "views"}
    )
    rows = PagesManager(json_path=path).load_all()
    assert rows[0]["page_category"] == "views"


def test_invalid_category_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "pages.json"
    path.write_text("[]", encoding="utf-8")
    mgr = PagesManager(json_path=path)
    with pytest.raises(ValueError):
        mgr.upsert(_page("a", page_category="khác"))
    with pytest.raises(ValueError):
        mgr.set_category_for_ids(["a"], "khác")
