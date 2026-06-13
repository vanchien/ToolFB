"""
Tiện ích giữ Tkinter phản hồi khi nạp/làm mới danh sách lớn (Treeview, JSON).

Nguyên tắc:
- Đọc JSON / kiểm tra file nặng → thread nền.
- Xóa/insert Treeview → main thread, insert từng lô qua ``after``.
- Python 3.14+: **không** gọi ``root.after`` từ worker — dùng ``schedule_on_main_thread``.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from typing import Any
from weakref import WeakKeyDictionary

import tkinter as tk
from tkinter import ttk

from loguru import logger

DEFAULT_TREE_CHUNK = 45
DEFAULT_TREE_APPEND_CHUNK = 60
_DEFAULT_MAIN_PUMP_MS = 25
_MAIN_DRAIN_PER_TICK = 48

_ROOT_QUEUES: WeakKeyDictionary[tk.Misc, queue.Queue[Callable[[], None]]] = WeakKeyDictionary()
_PUMP_REGISTERED: WeakKeyDictionary[tk.Misc, bool] = WeakKeyDictionary()


def flush_main_thread_queue(root: tk.Misc, *, max_items: int = _MAIN_DRAIN_PER_TICK) -> int:
    """Xử lý callback đang chờ — **chỉ gọi trên main thread** (test / pump)."""
    q = _ROOT_QUEUES.get(root)
    if q is None:
        return 0
    n = 0
    try:
        while n < max_items:
            fn = q.get_nowait()
            n += 1
            try:
                fn()
            except Exception as exc:  # noqa: BLE001
                logger.debug("[UI] main-thread callback: {}", exc)
    except queue.Empty:
        pass
    return n


def register_main_thread_dispatcher(root: tk.Misc, *, interval_ms: int = _DEFAULT_MAIN_PUMP_MS) -> None:
    """
    Bật pump queue UI trên main thread (gọi **một lần** sau khi tạo ``Tk`` / ``Toplevel``).

    Python 3.14+ từ chối ``root.after`` khi gọi từ thread phụ (``main thread is not in main loop``).
    """
    if root in _PUMP_REGISTERED:
        return
    _PUMP_REGISTERED[root] = True
    if root not in _ROOT_QUEUES:
        _ROOT_QUEUES[root] = queue.Queue()

    def _pump() -> None:
        flush_main_thread_queue(root)
        try:
            root.after(max(8, int(interval_ms)), _pump)
        except (tk.TclError, RuntimeError):
            _PUMP_REGISTERED.pop(root, None)

    try:
        root.after(max(8, int(interval_ms)), _pump)
    except (tk.TclError, RuntimeError):
        _PUMP_REGISTERED.pop(root, None)


def schedule_on_main_thread(root: tk.Misc, fn: Callable[[], None]) -> None:
    """Đưa callback về main thread — an toàn khi gọi từ worker (Python 3.14+)."""
    if threading.current_thread() is threading.main_thread():
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            logger.debug("[UI] main-thread callback (inline): {}", exc)
        return
    if root not in _ROOT_QUEUES:
        register_main_thread_dispatcher(root)
    q = _ROOT_QUEUES.get(root)
    if q is None:
        logger.warning("[UI] Không có queue dispatcher cho root — bỏ qua callback")
        return
    q.put(fn)


DEFAULT_TREE_SELECT_CHUNK = 120
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


def tree_select_all_chunked(
    root: tk.Misc,
    tree: ttk.Treeview,
    *,
    start: int = 0,
    chunk: int = DEFAULT_TREE_SELECT_CHUNK,
    on_complete: Callable[[], None] | None = None,
) -> None:
    """Chọn dần mọi dòng Treeview — tránh Not Responding khi hàng nghìn dòng."""
    children = list(tree.get_children())
    end = min(start + max(1, chunk), len(children))
    if start < end:
        try:
            tree.selection_add(*children[start:end])
        except tk.TclError:
            pass
    if end < len(children):
        root.after(
            1,
            lambda n=end: tree_select_all_chunked(
                root,
                tree,
                start=n,
                chunk=chunk,
                on_complete=on_complete,
            ),
        )
        return
    if on_complete is not None:
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

        schedule_on_main_thread(root, _done)

    threading.Thread(target=_thread, daemon=True).start()
