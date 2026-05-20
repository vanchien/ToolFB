"""
Tiện ích giữ Tkinter phản hồi khi nạp/làm mới danh sách lớn (Treeview, JSON).

Nguyên tắc:
- Đọc JSON / kiểm tra file nặng → thread nền.
- Xóa/insert Treeview → main thread, insert từng lô qua ``after``.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import tkinter as tk
from tkinter import ttk

DEFAULT_TREE_CHUNK = 45
ASYNC_PREP_MIN_ROWS = 30


def tree_delete_all(tree: ttk.Treeview) -> None:
    """Xóa mọi dòng — một lệnh ``delete(*children)`` nhanh hơn vòng lặp."""
    children = tree.get_children()
    if children:
        tree.delete(*children)


def tree_insert_chunked(
    root: tk.Misc,
    tree: ttk.Treeview,
    specs: list[dict[str, Any]],
    *,
    generation: int,
    is_current: Callable[[int], bool],
    on_complete: Callable[[], None] | None = None,
    start: int = 0,
    chunk: int = DEFAULT_TREE_CHUNK,
) -> None:
    """
    Insert lần lượt ``specs`` (mỗi phần tử là kwargs cho ``tree.insert``).

    ``is_current(generation)`` trả False → hủy (render mới đã thay thế).
    """
    end = min(start + max(1, chunk), len(specs))
    if not is_current(generation):
        return
    for spec in specs[start:end]:
        try:
            tree.insert("", tk.END, **spec)
        except tk.TclError:
            pass
    if end < len(specs):
        root.after(
            1,
            lambda g=generation, n=end: tree_insert_chunked(
                root,
                tree,
                specs,
                generation=g,
                is_current=is_current,
                on_complete=on_complete,
                start=n,
                chunk=chunk,
            ),
        )
        return
    if on_complete is not None and is_current(generation):
        on_complete()


def run_background_then_main(
    root: tk.Misc,
    worker: Callable[[], Any],
    on_main: Callable[[Any], None],
    *,
    on_error: Callable[[BaseException], None] | None = None,
) -> None:
    """Chạy ``worker`` trên thread nền, ``on_main(result)`` trên main thread."""

    def _thread() -> None:
        result: Any = None
        err: BaseException | None = None
        try:
            result = worker()
        except BaseException as exc:  # noqa: BLE001
            err = exc

        def _done() -> None:
            if err is not None:
                if on_error is not None:
                    on_error(err)
                return
            on_main(result)

        try:
            root.after(0, _done)
        except tk.TclError:
            pass

    threading.Thread(target=_thread, daemon=True).start()
