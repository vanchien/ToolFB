"""
Hộp thoại CRUD Page/Group (``config/pages.json``) — tách khỏi quản lý tài khoản.

Chỉ còn «Thông tin Page»: URL, owner, loại… — lịch + AI theo từng job ở tab «Job lịch đăng».
"""

from __future__ import annotations

import re
import uuid
from typing import Any

import tkinter as tk
from tkinter import messagebox, ttk

from src.utils.pages_manager import PageRecord, PagesManager


def _parse_bool_init(raw: object) -> bool:
    if isinstance(raw, bool):
        return raw
    s = str(raw).strip().lower()
    return s in ("1", "true", "yes", "on")


def _extract_numeric_id_from_url(url: str) -> str:
    """
    Tách ID số từ URL Facebook nếu có (profile.php?id=..., /pages/.../<id>, /<id>).
    """
    u = str(url or "").strip()
    if not u:
        return ""
    m = re.search(r"[?&]id=(\d{8,})", u, flags=re.I)
    if m:
        return m.group(1)
    m = re.search(r"/pages/[^/]+/(\d{8,})", u, flags=re.I)
    if m:
        return m.group(1)
    m = re.search(r"/(\d{8,})(?:[/?#]|$)", u)
    if m:
        return m.group(1)
    return ""


class PageFormDialog:
    """
    Form thêm / sửa một bản ghi ``pages.json`` (chỉ thông tin Page/Group).
    """

    def __init__(
        self,
        parent: tk.Tk | tk.Toplevel,
        pages: PagesManager,
        owner_account_ids: list[str],
        *,
        title: str,
        initial: PageRecord | None,
        id_readonly: bool,
    ) -> None:
        self._pages = pages
        self._owner_ids = [str(x).strip() for x in owner_account_ids if str(x).strip()]
        self._result: dict[str, Any] | None = None
        self._id_readonly = id_readonly
        init = dict(initial) if initial else {}
        self._init_snapshot = dict(init)

        self._top = tk.Toplevel(parent)
        self._top.title(title)
        self._top.transient(parent)
        self._top.grab_set()
        self._top.geometry("580x560")
        self._top.columnconfigure(0, weight=1)
        self._top.rowconfigure(0, weight=1)

        form = ttk.Frame(self._top, padding=10)
        form.grid(row=0, column=0, sticky="nsew")
        form.columnconfigure(1, weight=1)

        self._build_basics_form(form, init)

        btnf = ttk.Frame(self._top, padding=8)
        btnf.grid(row=1, column=0, sticky="ew")
        ttk.Button(btnf, text="Hủy", command=self._on_cancel).pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Button(btnf, text="Lưu", command=self._on_ok).pack(side=tk.RIGHT)

        self._top.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self._top.wait_window()

    def _build_basics_form(self, form: ttk.Frame, init: dict[str, Any]) -> None:
        row = 0

        def add_row(label: str, widget: ttk.Widget) -> None:
            nonlocal row
            ttk.Label(form, text=label).grid(row=row, column=0, sticky="nw", pady=2, padx=(0, 8))
            widget.grid(row=row, column=1, sticky="ew", pady=2)
            row += 1

        self._e_id = ttk.Entry(form)
        self._e_id.insert(0, str(init.get("id", "")))
        if self._id_readonly:
            self._e_id.configure(state="readonly")
        add_row("Page id (để trống = tạo mới)", self._e_id)

        self._cb_owner = ttk.Combobox(form, values=self._owner_ids or [""], state="readonly", width=42)
        oid = str(init.get("account_id", "")).strip()
        if oid and oid in self._owner_ids:
            self._cb_owner.set(oid)
        elif self._owner_ids:
            self._cb_owner.set(self._owner_ids[0])
        add_row("Owner (account_id) *", self._cb_owner)

        kinds = ("", "fanpage", "profile", "group")
        self._cb_kind = ttk.Combobox(form, values=kinds, width=40)
        pk = str(init.get("page_kind", "")).strip().lower()
        self._cb_kind.set(pk if pk in kinds else "")
        add_row("page_kind (Fanpage/Profile/Group)", self._cb_kind)

        self._e_name = ttk.Entry(form)
        self._e_name.insert(0, str(init.get("page_name", "")))
        add_row("Page_Name *", self._e_name)

        self._e_url = ttk.Entry(form)
        self._e_url.insert(0, str(init.get("page_url", "https://www.facebook.com/")))
        add_row("Page_URL *", self._e_url)

        fb_row = ttk.Frame(form)
        fb_row.columnconfigure(0, weight=1)
        self._e_fb_page_id = ttk.Entry(fb_row)
        self._e_fb_page_id.insert(0, str(init.get("fb_page_id", "")))
        self._e_fb_page_id.grid(row=0, column=0, sticky="ew")
        ttk.Button(fb_row, text="Lấy từ URL", command=self._on_fill_fb_page_id_from_url).grid(
            row=0, column=1, padx=(6, 0)
        )
        add_row("Meta Page ID (asset_id, số)", fb_row)

        self._biz_var = tk.BooleanVar(value=_parse_bool_init(init.get("use_business_composer")))
        biz_cb = ttk.Checkbutton(
            form,
            text="Đăng qua Meta Business Composer (tự gắn asset_id từ fb_page_id / URL)",
            variable=self._biz_var,
        )
        add_row("", biz_cb)

        ttk.Label(
            form,
            text="Lịch đăng (giờ 24h), post_style, chủ đề AI… nằm ở tab «3. Job lịch đăng» — mỗi job một cấu hình. "
            "Scheduler vẫn có thể đăng theo lịch Page trong pages.json nếu còn schedule_time (tương thích cũ).",
            foreground="gray",
            wraplength=520,
            font=("Segoe UI", 8),
        ).grid(row=row, column=0, columnspan=2, sticky="ew", pady=(12, 0))

    @property
    def result(self) -> dict[str, Any] | None:
        return self._result

    def _collect_basics(self) -> dict[str, Any]:
        if not self._owner_ids:
            raise ValueError("Chưa có tài khoản nào — không thể gán owner.")
        owner = self._cb_owner.get().strip()
        if not owner:
            raise ValueError("Chọn Owner (tài khoản).")
        name = self._e_name.get().strip()
        if not name:
            raise ValueError("Page_Name không được để trống.")
        url = self._e_url.get().strip()
        if not url:
            raise ValueError("Page_URL không được để trống.")
        row: dict[str, Any] = {}
        if self._init_snapshot:
            row.update(dict(self._init_snapshot))
        row["account_id"] = owner
        row["page_name"] = name
        row["page_url"] = url
        fb_id = self._e_fb_page_id.get().strip()
        if not fb_id:
            fb_id = _extract_numeric_id_from_url(url)
        if fb_id and not fb_id.isdigit():
            raise ValueError("Meta Page ID phải là chuỗi số.")
        if fb_id:
            row["fb_page_id"] = fb_id
        else:
            row.pop("fb_page_id", None)
        if self._biz_var.get():
            row["use_business_composer"] = True
        else:
            row.pop("use_business_composer", None)
        eid = self._e_id.get().strip()
        if self._id_readonly:
            row["id"] = str(self._init_snapshot.get("id", "")).strip()
        else:
            row["id"] = eid
        kind = self._cb_kind.get().strip().lower()
        if kind:
            row["page_kind"] = kind
        else:
            row.pop("page_kind", None)
        if not str(row.get("id", "")).strip():
            row["id"] = ""
        chk = dict(row)
        if not str(chk.get("id", "")).strip():
            chk["id"] = uuid.uuid4().hex[:12]
        self._pages.validate_record(chk)
        row["post_style"] = chk.get("post_style", "post")
        return row

    def _on_fill_fb_page_id_from_url(self) -> None:
        url = self._e_url.get().strip()
        found = _extract_numeric_id_from_url(url)
        if not found:
            messagebox.showinfo(
                "Không tự lấy được ID",
                "URL này không chứa ID số của Page.\nBạn hãy nhập Meta Page ID (asset_id) thủ công.",
                parent=self._top,
            )
            return
        self._e_fb_page_id.delete(0, tk.END)
        self._e_fb_page_id.insert(0, found)

    def _on_ok(self) -> None:
        try:
            self._result = self._collect_basics()
        except ValueError as exc:
            messagebox.showerror("Dữ liệu không hợp lệ", str(exc), parent=self._top)
            return
        self._top.grab_release()
        self._top.destroy()

    def _on_cancel(self) -> None:
        self._result = None
        try:
            self._top.grab_release()
        except tk.TclError:
            pass
        self._top.destroy()
