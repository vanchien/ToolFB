"""
Xuất / nhập JSON tài khoản và re-export form từ ``account_workbench``.

Form chi tiết: ``AccountFormDialog`` trong ``src.gui.account_workbench``.
"""

from __future__ import annotations

import json
from pathlib import Path

import tkinter as tk
from tkinter import filedialog, messagebox

from loguru import logger

from src.gui.account_workbench import (
    AccountFormDialog,
    _coerce_use_proxy,
    _normalize_post_status,
    template_new_account,
)
from src.utils.db_manager import AccountsDatabaseManager


def export_accounts_json(manager: AccountsDatabaseManager, parent: tk.Tk) -> None:
    """
    Xuất toàn bộ ``accounts.json`` hiện tại ra file do người dùng chọn.

    Args:
        manager: Bộ quản lý JSON.
        parent: Cửa sổ cha cho hộp thoại lưu file.
    """
    path = filedialog.asksaveasfilename(
        parent=parent,
        title="Xuất danh sách tài khoản",
        defaultextension=".json",
        filetypes=[("JSON", "*.json"), ("Tất cả", "*.*")],
    )
    if not path:
        return
    try:
        rows = manager.reload_from_disk()
        Path(path).write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        logger.info("Đã xuất {} tài khoản ra {}", len(rows), path)
        messagebox.showinfo("Xuất JSON", f"Đã lưu {len(rows)} tài khoản.\n{path}", parent=parent)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Xuất JSON thất bại: {}", exc)
        messagebox.showerror("Lỗi", str(exc), parent=parent)


def import_accounts_append(manager: AccountsDatabaseManager, parent: tk.Tk) -> tuple[int, int]:
    """
    Nhập mảng JSON tài khoản: chỉ **thêm** bản ghi có ``id`` chưa tồn tại (bỏ qua trùng).

    Args:
        manager: Bộ quản lý JSON.
        parent: Cửa sổ cha.

    Returns:
        ``(số đã thêm, số đã bỏ qua do trùng id)``.
    """
    path = filedialog.askopenfilename(
        parent=parent,
        title="Nhập JSON (chỉ thêm id mới)",
        filetypes=[("JSON", "*.json"), ("Tất cả", "*.*")],
    )
    if not path:
        return 0, 0
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(raw, dict) and "accounts" in raw:
            raw = raw["accounts"]
        if not isinstance(raw, list):
            raise ValueError("File phải là mảng JSON hoặc object có khóa 'accounts'.")
    except Exception as exc:  # noqa: BLE001
        messagebox.showerror("Lỗi đọc file", str(exc), parent=parent)
        return 0, 0

    existing_ids = {str(a.get("id", "")) for a in manager.load_all()}
    added = 0
    skipped = 0
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            messagebox.showwarning("Bỏ qua", f"Phần tử index {idx} không phải object.")
            skipped += 1
            continue
        aid = str(item.get("id", "")).strip()
        if not aid:
            skipped += 1
            continue
        if aid in existing_ids:
            skipped += 1
            continue
        try:
            manager.validate_account(item)
        except ValueError as exc:
            messagebox.showerror("Bản ghi không hợp lệ", f"id={aid!r}:\n{exc}", parent=parent)
            return added, skipped
        manager.upsert(item)  # type: ignore[arg-type]
        existing_ids.add(aid)
        added += 1

    logger.info("Nhập JSON: thêm {}, bỏ qua {}", added, skipped)
    messagebox.showinfo(
        "Nhập xong",
        f"Đã thêm: {added} tài khoản.\nĐã bỏ qua (trùng id hoặc lỗi): {skipped}.",
        parent=parent,
    )
    return added, skipped


__all__ = [
    "AccountFormDialog",
    "template_new_account",
    "_normalize_post_status",
    "_coerce_use_proxy",
    "export_accounts_json",
    "import_accounts_append",
]
