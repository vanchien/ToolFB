"""
Phân loại Page để quản lý số lượng lớn: Bật kiếm tiền / Tiêu chí / Kéo view.

Giá trị lưu trong ``pages.json`` là khóa ngắn (``page_category``), giao diện hiện nhãn tiếng Việt.
"""

from __future__ import annotations

from typing import Any, Iterable

PAGE_CATEGORY_CHOICES: tuple[tuple[str, str], ...] = (
    ("monetized", "Bật kiếm tiền"),
    ("criteria", "Tiêu chí"),
    ("views", "Kéo view"),
)
PAGE_CATEGORY_KEYS: tuple[str, ...] = tuple(key for key, _label in PAGE_CATEGORY_CHOICES)
UNCATEGORIZED_KEY = "none"
UNCATEGORIZED_LABEL = "Chưa phân loại"
ALL_CATEGORIES_LABEL = "Tất cả phân loại"

_LABEL_BY_KEY = dict(PAGE_CATEGORY_CHOICES)
_KEY_BY_LABEL = {label.lower(): key for key, label in PAGE_CATEGORY_CHOICES}


def normalize_page_category(raw: object) -> str:
    """Trả về khóa phân loại hợp lệ (nhận cả khóa lẫn nhãn), hoặc ``""`` nếu chưa phân loại."""
    text = str(raw or "").strip().lower()
    if not text or text == UNCATEGORIZED_KEY or text == UNCATEGORIZED_LABEL.lower():
        return ""
    if text in _LABEL_BY_KEY:
        return text
    return _KEY_BY_LABEL.get(text, "")


def is_valid_page_category(raw: object) -> bool:
    """``True`` nếu rỗng hoặc là một phân loại đã biết."""
    text = str(raw or "").strip()
    return not text or bool(normalize_page_category(text))


def page_category_label(raw: object) -> str:
    """Nhãn hiển thị của một Page; rỗng → «Chưa phân loại»."""
    key = normalize_page_category(raw)
    return _LABEL_BY_KEY.get(key, UNCATEGORIZED_LABEL)


def count_page_categories(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Đếm số Page theo từng khóa phân loại (``UNCATEGORIZED_KEY`` cho Page chưa gắn)."""
    counts = {key: 0 for key in PAGE_CATEGORY_KEYS}
    counts[UNCATEGORIZED_KEY] = 0
    for row in rows:
        key = normalize_page_category(row.get("page_category")) or UNCATEGORIZED_KEY
        counts[key] = counts.get(key, 0) + 1
    return counts


def category_filter_options(rows: Iterable[dict[str, Any]]) -> list[tuple[str, str]]:
    """
    Danh sách ``(nhãn có số lượng, khóa lọc)`` cho combobox lọc.

    Khóa ``""`` = tất cả, ``UNCATEGORIZED_KEY`` = chưa phân loại.
    """
    row_list = list(rows)
    counts = count_page_categories(row_list)
    options: list[tuple[str, str]] = [(f"{ALL_CATEGORIES_LABEL} ({len(row_list)})", "")]
    for key, label in PAGE_CATEGORY_CHOICES:
        options.append((f"{label} ({counts.get(key, 0)})", key))
    options.append((f"{UNCATEGORIZED_LABEL} ({counts.get(UNCATEGORIZED_KEY, 0)})", UNCATEGORIZED_KEY))
    return options


def page_matches_category(row: dict[str, Any], filter_key: str) -> bool:
    """Page có khớp khóa lọc không (``""`` = luôn khớp)."""
    if not filter_key:
        return True
    key = normalize_page_category(row.get("page_category"))
    if filter_key == UNCATEGORIZED_KEY:
        return not key
    return key == filter_key
