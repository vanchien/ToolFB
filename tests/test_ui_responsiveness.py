"""Tests dispatcher main thread (Python 3.14+)."""

from __future__ import annotations

import queue
import threading
import time
import tkinter as tk

from src.gui.ui_responsiveness import (
    flush_main_thread_queue,
    register_main_thread_dispatcher,
    run_background_then_main,
    schedule_on_main_thread,
)


def test_schedule_on_main_thread_from_worker() -> None:
    root = tk.Tk()
    root.withdraw()
    register_main_thread_dispatcher(root, interval_ms=15)
    done: queue.Queue[str] = queue.Queue()

    def worker() -> None:
        schedule_on_main_thread(root, lambda: done.put("ok"))

    threading.Thread(target=worker, daemon=True).start()
    try:
        for _ in range(50):
            flush_main_thread_queue(root)
            try:
                assert done.get_nowait() == "ok"
                break
            except queue.Empty:
                time.sleep(0.01)
        else:
            raise AssertionError("callback chưa chạy trên main thread")
    finally:
        root.destroy()


def test_schedule_on_main_thread_inline_on_main() -> None:
    root = tk.Tk()
    root.withdraw()
    register_main_thread_dispatcher(root)
    seen: list[str] = []

    schedule_on_main_thread(root, lambda: seen.append("ok"))
    assert seen == ["ok"]
    root.destroy()


def test_run_background_then_main_uses_queue() -> None:
    root = tk.Tk()
    root.withdraw()
    register_main_thread_dispatcher(root, interval_ms=15)
    out: queue.Queue[int] = queue.Queue()

    run_background_then_main(root, lambda: 42, lambda v: out.put(int(v)))
    try:
        for _ in range(50):
            flush_main_thread_queue(root)
            try:
                assert out.get_nowait() == 42
                break
            except queue.Empty:
                time.sleep(0.01)
        else:
            raise AssertionError("on_main chưa được gọi")
    finally:
        root.destroy()
