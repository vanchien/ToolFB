"""
Đăng nhập Facebook qua Playwright persistent context → ghi ``storage_state`` (cookie JSON).

Dùng chung cho form «Thêm tài khoản» và bảng quản lý chính.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import tkinter as tk
from tkinter import messagebox, ttk

from loguru import logger

from src.automation.browser_factory import BrowserFactory, sync_close_persistent_context
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.paths import project_root


def cookie_storage_dest(ck_rel: str, root: Path) -> Path:
    """Đường dẫn tuyệt đối tới file storage_state / cookie JSON."""
    ck_path = Path(ck_rel.strip())
    return ck_path.resolve() if ck_path.is_absolute() else (root / ck_path).resolve()


def account_cookie_path_field(dest: Path) -> str:
    """Chuỗi lưu vào ``cookie_path`` (tương đối dự án nếu nằm trong repo)."""
    r = project_root().resolve()
    try:
        return dest.resolve().relative_to(r).as_posix()
    except ValueError:
        return str(dest.resolve())


def run_fb_cookie_capture_dialog(
    parent: tk.Misc,
    manager: AccountsDatabaseManager,
    acc_preview: dict[str, Any],
    ck_rel: str,
    *,
    log_label: str,
    tip_extra: str = "(File ghi đúng cookie_path trong form.)",
    on_after_save: Callable[[], None] | None = None,
) -> None:
    """
    Mở trình duyệt persistent theo ``acc_preview``, vào Facebook login; user bấm Lưu → ``storage_state``.
    """
    root = project_root()

    cmd_q: queue.Queue[str] = queue.Queue()
    done_evt = threading.Event()
    err_holder: list[str] = []
    action_holder: list[str] = []

    def worker() -> None:
        factory: BrowserFactory | None = None
        ctx = None
        try:
            factory = BrowserFactory(accounts=manager, headless=False)
            ctx = factory.launch_persistent_context_from_account_dict(acc_preview, headless=False)
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto("https://www.facebook.com/login", wait_until="domcontentloaded", timeout=120_000)
            cmd = cmd_q.get()
            if cmd == "save":
                dest = cookie_storage_dest(ck_rel, root)
                dest.parent.mkdir(parents=True, exist_ok=True)
                ctx.storage_state(path=str(dest))
        except Exception as exc:  # noqa: BLE001
            err_holder.append(str(exc))
            logger.exception("login_capture_cookie: {}", log_label)
        finally:
            sync_close_persistent_context(ctx, log_label=log_label)
            if factory is not None:
                try:
                    factory.close()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Đóng factory sau login capture: {}", exc)
            done_evt.set()

    threading.Thread(target=worker, name="fb_login_capture", daemon=True).start()

    tip = tk.Toplevel(parent)
    tip.title("Đăng nhập Facebook")
    tip.transient(parent)
    tip.geometry("420x150")
    ttk.Label(
        tip,
        text=f"Trình duyệt đã mở tới trang đăng nhập.\nSau khi đăng nhập xong, bấm «Lưu cookie vào file».\n{tip_extra}",
        wraplength=400,
    ).pack(padx=12, pady=12)

    def send(cmd: str) -> None:
        action_holder.append(cmd)
        try:
            cmd_q.put(cmd)
        except Exception:
            pass
        try:
            tip.destroy()
        except tk.TclError:
            pass

    bf = ttk.Frame(tip, padding=8)
    bf.pack(fill=tk.X)
    ttk.Button(bf, text="Lưu cookie vào file", command=lambda: send("save")).pack(side=tk.RIGHT, padx=4)
    ttk.Button(bf, text="Hủy (đóng trình duyệt)", command=lambda: send("cancel")).pack(side=tk.RIGHT)

    def on_tip_close() -> None:
        send("cancel")

    tip.protocol("WM_DELETE_WINDOW", on_tip_close)

    def poll() -> None:
        if not done_evt.is_set():
            parent.after(400, poll)
            return
        try:
            if tip.winfo_exists():
                tip.destroy()
        except tk.TclError:
            pass
        if err_holder:
            messagebox.showerror("Trình duyệt", err_holder[0], parent=parent)
        elif action_holder and action_holder[-1] == "save":
            messagebox.showinfo("Cookie", f"Đã ghi cookie_path:\n{ck_rel}", parent=parent)
            if on_after_save is not None:
                on_after_save()

    parent.after(500, poll)
