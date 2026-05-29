"""Hộp thoại xuất Page/Group ra CSV."""

from __future__ import annotations

import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable

from src.utils.pages_csv_export import write_pages_csv


class PagesExportDialog:
    """
    Chọn phạm vi xuất: Page đang chọn / theo owner / theo bộ lọc bảng.
    """

    def __init__(
        self,
        parent: tk.Tk | tk.Toplevel,
        *,
        owner_account_ids: list[str],
        selected_count: int,
        filtered_count: int,
        resolve_rows: Callable[[str, str], list[dict[str, Any]]],
    ) -> None:
        self._resolve_rows = resolve_rows
        self._exported_path: str | None = None
        self._top = tk.Toplevel(parent)
        self._top.title("Xuất Page CSV")
        self._top.transient(parent)
        self._top.grab_set()
        self._top.geometry("480x280")
        self._top.minsize(420, 240)
        self._top.columnconfigure(0, weight=1)

        body = ttk.Frame(self._top, padding=12)
        body.grid(row=0, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)

        ttk.Label(
            body,
            text="File CSV gồm 2 cột: Tên page, Link page (page_url).",
            wraplength=440,
        ).grid(row=0, column=0, sticky="w", pady=(0, 10))

        self._mode = tk.StringVar(value="selected" if selected_count > 0 else "filtered")
        modes = ttk.LabelFrame(body, text="Phạm vi xuất", padding=8)
        modes.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        modes.columnconfigure(1, weight=1)

        rb_sel = ttk.Radiobutton(
            modes,
            text=f"Page đang chọn trong bảng ({selected_count})",
            variable=self._mode,
            value="selected",
        )
        rb_sel.grid(row=0, column=0, columnspan=2, sticky="w", pady=2)
        if selected_count <= 0:
            rb_sel.configure(state=tk.DISABLED)

        ttk.Radiobutton(
            modes,
            text=f"Theo bộ lọc hiện tại ({filtered_count} Page)",
            variable=self._mode,
            value="filtered",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=2)

        ttk.Radiobutton(
            modes,
            text="Theo tài khoản (owner):",
            variable=self._mode,
            value="account",
        ).grid(row=2, column=0, sticky="w", pady=2)
        owners = [str(x).strip() for x in owner_account_ids if str(x).strip()]
        self._cb_owner = ttk.Combobox(
            modes,
            values=owners or ["(chưa có owner)"],
            state="readonly" if owners else "disabled",
            width=28,
        )
        self._cb_owner.grid(row=2, column=1, sticky="ew", padx=(8, 0))
        if owners:
            self._cb_owner.set(owners[0])

        btnf = ttk.Frame(self._top, padding=(12, 0, 12, 12))
        btnf.grid(row=1, column=0, sticky="ew")
        ttk.Button(btnf, text="Hủy", command=self._on_cancel).pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Button(btnf, text="Xuất CSV…", command=self._on_export).pack(side=tk.RIGHT)

        self._top.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self._top.wait_window()

    @property
    def exported_path(self) -> str | None:
        return self._exported_path

    def _on_cancel(self) -> None:
        try:
            self._top.grab_release()
        except tk.TclError:
            pass
        self._top.destroy()

    def _on_export(self) -> None:
        mode = self._mode.get().strip()
        owner = self._cb_owner.get().strip() if mode == "account" else ""
        try:
            rows = self._resolve_rows(mode, owner)
        except ValueError as exc:
            messagebox.showerror("Xuất CSV", str(exc), parent=self._top)
            return
        if not rows:
            messagebox.showwarning("Xuất CSV", "Không có Page nào để xuất.", parent=self._top)
            return
        default_name = "pages_export.csv"
        if mode == "account" and owner:
            safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in owner)[:40]
            default_name = f"pages_{safe}.csv"
        path = filedialog.asksaveasfilename(
            parent=self._top,
            title="Lưu file CSV",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv"), ("Tất cả", "*.*")],
            initialfile=default_name,
        )
        if not path:
            return
        try:
            n = write_pages_csv(path, rows)
        except OSError as exc:
            messagebox.showerror("Xuất CSV", f"Không ghi được file:\n{exc}", parent=self._top)
            return
        self._exported_path = str(Path(path).resolve())
        messagebox.showinfo(
            "Xuất CSV",
            f"Đã xuất {n} Page vào:\n{self._exported_path}",
            parent=self._top,
        )
        self._on_cancel()
