"""
Ô lọc «Phân loại» dùng chung cho mọi nơi chọn Page (Lịch đăng, Chia sẻ, AI Agent, Nhóm…).
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Callable, Iterable

from src.utils.page_categories import (
    category_filter_options,
    normalize_page_category,
    page_category_label,
    page_matches_category,
)


def page_choice_label(name: str, category: object, page_id: str = "") -> str:
    """Nhãn chọn Page: ``Tên · Phân loại (id)`` — bỏ phần trống."""
    text = str(name or "").strip() or str(page_id or "").strip()
    key = normalize_page_category(category)
    if key:
        text = f"{text} · {page_category_label(key)}"
    pid = str(page_id or "").strip()
    if pid and pid != str(name or "").strip():
        text = f"{text} ({pid})"
    return text


class PageCategoryFilter:
    """Nhãn + combobox «Phân loại» có số lượng; giữ lựa chọn khi danh sách đổi."""

    def __init__(
        self,
        parent: tk.Misc,
        *,
        on_change: Callable[[], None] | None = None,
        width: int = 22,
        label: str = "Phân loại",
    ) -> None:
        self.frame = ttk.Frame(parent)
        ttk.Label(self.frame, text=label).pack(side=tk.LEFT, padx=(0, 4))
        self._var = tk.StringVar(value="")
        self._label_to_key: dict[str, str] = {}
        self._box = ttk.Combobox(self.frame, textvariable=self._var, state="readonly", width=width)
        self._box.pack(side=tk.LEFT)
        self._on_change = on_change
        self._box.bind("<<ComboboxSelected>>", lambda _e: self._changed())
        self.refresh([])

    def _changed(self) -> None:
        if self._on_change is not None:
            self._on_change()

    def key(self) -> str:
        """Khóa đang lọc (``""`` = tất cả)."""
        return self._label_to_key.get(self._var.get(), "")

    def matches(self, row: dict[str, Any]) -> bool:
        """Page (có trường ``page_category``) có nằm trong lựa chọn hiện tại không."""
        return page_matches_category(row, self.key())

    def refresh(self, rows: Iterable[dict[str, Any]]) -> None:
        """Cập nhật số lượng theo ``rows`` (danh sách trước khi lọc phân loại)."""
        current = self.key()
        options = category_filter_options(rows)
        self._label_to_key = {label: key for label, key in options}
        self._box.configure(values=tuple(label for label, _key in options))
        self._var.set(next((label for label, key in options if key == current), options[0][0]))

    def reset(self) -> None:
        """Về «Tất cả phân loại»."""
        labels = list(self._label_to_key)
        if labels:
            self._var.set(labels[0])
