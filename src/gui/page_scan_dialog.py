"""
Hộp thoại «Quét Page theo tài khoản» — mở profile Playwright đã đăng nhập, quét qua
**Meta Business Suite** (danh sách portfolio / business, View more, lưới Page),
rồi cho phép tick lưu vào ``pages.json``.
"""

from __future__ import annotations

import os
import queue
import threading
import uuid
from typing import Any

import tkinter as tk
from tkinter import messagebox, ttk

from loguru import logger

from src.automation.browser_factory import BrowserFactory, sync_close_persistent_context
from src.automation.facebook_page_scanner import ScannedPage
from src.automation.meta_business_scanner import scan_meta_business_pages_for_account
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.pages_manager import PagesManager


_DEFAULT_GEOMETRY = "860x620"


class PageScanDialog:
    """UI: chọn account → scan nền → duyệt danh sách → lưu selected vào ``pages.json``."""

    def __init__(
        self,
        parent: tk.Misc,
        accounts: AccountsDatabaseManager,
        pages: PagesManager,
    ) -> None:
        self._parent = parent
        self._accounts = accounts
        self._pages = pages
        self._results: list[ScannedPage] = []
        self._scan_thread: threading.Thread | None = None
        self._status_q: queue.Queue[str] = queue.Queue()
        self._done_evt = threading.Event()
        self._err_holder: list[str] = []
        self._selected_account_id: str = ""
        self._saved_count: int = 0
        self._scan_running: bool = False

        self._top = tk.Toplevel(parent)
        self._top.title("Quét Page — Meta Business Suite")
        self._top.transient(parent)
        self._top.grab_set()
        self._top.geometry(_DEFAULT_GEOMETRY)
        self._top.columnconfigure(0, weight=1)
        self._top.rowconfigure(2, weight=1)

        self._build_header()
        self._build_log()
        self._build_tree()
        self._build_footer()

        self._top.protocol("WM_DELETE_WINDOW", self._on_close)
        self._top.wait_window()

    def _build_header(self) -> None:
        top = ttk.Frame(self._top, padding=(10, 8, 10, 4))
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(1, weight=1)

        ttk.Label(top, text="Tài khoản:").grid(row=0, column=0, sticky="w", padx=(0, 8))
        account_ids = [str(a.get("id", "")).strip() for a in self._accounts.load_all() if a.get("id")]
        self._cb_account = ttk.Combobox(top, values=account_ids, state="readonly")
        if account_ids:
            self._cb_account.set(account_ids[0])
        self._cb_account.grid(row=0, column=1, sticky="ew")

        btnf = ttk.Frame(top)
        btnf.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        self._btn_scan = ttk.Button(btnf, text="Quét", command=self._on_start_scan)
        self._btn_scan.pack(side=tk.LEFT)

        # Tuỳ chọn chạy quét.
        # Mặc định: ẩn browser; user có thể bỏ tick để quan sát trực quan khi cần.
        headless_default = os.environ.get("HEADLESS", "1").strip().lower() not in {"0", "false", "off", "no"}
        self._headless_var = tk.BooleanVar(value=headless_default)
        self._lock_ui_var = tk.BooleanVar(value=True)  # True = khoá thao tác app khi quét
        self._cb_headless = ttk.Checkbutton(
            btnf,
            text="Ẩn browser khi quét",
            variable=self._headless_var,
        )
        self._cb_headless.pack(side=tk.LEFT, padx=(10, 0))
        self._cb_lock_ui = ttk.Checkbutton(
            btnf,
            text="Khóa thao tác ứng dụng khi quét",
            variable=self._lock_ui_var,
        )
        self._cb_lock_ui.pack(side=tk.LEFT, padx=(8, 0))
        ttk.Label(
            btnf,
            text="Sẽ mở trình duyệt với profile của tài khoản (không cần nhập lại mật khẩu nếu đã login).",
            foreground="gray",
            wraplength=640,
            font=("Segoe UI", 8),
        ).pack(side=tk.LEFT, padx=(10, 0))

    def _build_log(self) -> None:
        frm = ttk.LabelFrame(self._top, text="Tiến trình", padding=6)
        frm.grid(row=1, column=0, sticky="ew", padx=10, pady=4)
        frm.columnconfigure(0, weight=1)
        self._log_var = tk.StringVar(value="Sẵn sàng. Chọn tài khoản và bấm «Quét».")
        ttk.Label(frm, textvariable=self._log_var, foreground="#0a4fa0", wraplength=800).grid(
            row=0, column=0, sticky="w"
        )

    def _build_tree(self) -> None:
        tfrm = ttk.LabelFrame(
            self._top,
            text="Kết quả — Business / Meta Page ID / URL composer (tick để lưu pages.json)",
            padding=6,
        )
        tfrm.grid(row=2, column=0, sticky="nsew", padx=10, pady=4)
        tfrm.columnconfigure(0, weight=1)
        tfrm.rowconfigure(0, weight=1)

        cols = (
            "select",
            "page_name",
            "fb_page_id",
            "business_name",
            "business_id",
            "page_url",
            "source",
            "role",
        )
        self._tree = ttk.Treeview(tfrm, columns=cols, show="headings", selectmode="extended")
        headings = {
            "select": "✓",
            "page_name": "Tên Page",
            "fb_page_id": "Meta Page ID",
            "business_name": "Business",
            "business_id": "Business ID",
            "page_url": "URL composer",
            "source": "Nguồn",
            "role": "Vai trò",
        }
        widths = {
            "select": 40,
            "page_name": 180,
            "fb_page_id": 110,
            "business_name": 120,
            "business_id": 90,
            "page_url": 280,
            "source": 90,
            "role": 72,
        }
        for c in cols:
            self._tree.heading(c, text=headings[c])
            self._tree.column(
                c,
                width=widths[c],
                stretch=(c in ("page_url", "page_name", "business_name")),
            )
        sy = ttk.Scrollbar(tfrm, orient=tk.VERTICAL, command=self._tree.yview)
        self._tree.configure(yscrollcommand=sy.set)
        self._tree.grid(row=0, column=0, sticky="nsew")
        sy.grid(row=0, column=1, sticky="ns")

        # Toggle checkbox khi click cột đầu.
        self._tree.bind("<Button-1>", self._on_tree_click)

        qfrm = ttk.Frame(tfrm)
        qfrm.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(4, 0))
        ttk.Button(qfrm, text="Chọn tất cả", command=lambda: self._toggle_all(True)).pack(side=tk.LEFT)
        ttk.Button(qfrm, text="Bỏ chọn tất cả", command=lambda: self._toggle_all(False)).pack(
            side=tk.LEFT, padx=(6, 0)
        )
        ttk.Button(
            qfrm,
            text="Chỉ chọn Page có Meta ID",
            command=self._select_only_with_id,
        ).pack(side=tk.LEFT, padx=(6, 0))

    def _build_footer(self) -> None:
        f = ttk.Frame(self._top, padding=(10, 4, 10, 10))
        f.grid(row=3, column=0, sticky="ew")
        self._count_var = tk.StringVar(value="0 Page tìm thấy.")
        ttk.Label(f, textvariable=self._count_var, foreground="gray").pack(side=tk.LEFT)
        ttk.Button(f, text="Đóng", command=self._on_close).pack(side=tk.RIGHT)
        self._btn_save = ttk.Button(f, text="Lưu Page đã tick vào pages.json", command=self._on_save_selected)
        self._btn_save.pack(side=tk.RIGHT, padx=(0, 8))
        self._btn_save.configure(state=tk.DISABLED)

    # --- Scan ---

    def _append_status(self, msg: str) -> None:
        self._log_var.set(msg)

    def _on_start_scan(self) -> None:
        aid = self._cb_account.get().strip()
        if not aid:
            messagebox.showwarning("Chưa chọn", "Chọn một tài khoản để quét.", parent=self._top)
            return
        if self._scan_thread and self._scan_thread.is_alive():
            messagebox.showinfo("Đang chạy", "Đang có tiến trình quét, vui lòng chờ.", parent=self._top)
            return
        self._selected_account_id = aid
        self._results = []
        for i in self._tree.get_children():
            self._tree.delete(i)
        self._btn_save.configure(state=tk.DISABLED)
        self._btn_scan.configure(state=tk.DISABLED)
        self._cb_account.configure(state=tk.DISABLED)
        self._cb_headless.configure(state=tk.DISABLED)
        self._cb_lock_ui.configure(state=tk.DISABLED)
        self._count_var.set("Đang quét…")
        self._done_evt.clear()
        self._err_holder.clear()
        self._scan_running = True
        run_headless = bool(self._headless_var.get())
        lock_ui = bool(self._lock_ui_var.get())
        if lock_ui:
            try:
                self._top.grab_set()
            except tk.TclError:
                pass
        else:
            try:
                self._top.grab_release()
            except tk.TclError:
                pass

        def _status(msg: str) -> None:
            try:
                self._status_q.put_nowait(msg)
            except Exception:
                pass

        def worker() -> None:
            factory: BrowserFactory | None = None
            ctx = None
            pages_out: list[ScannedPage] = []
            try:
                factory = BrowserFactory(accounts=self._accounts, headless=run_headless)
                ctx = factory.get_browser_context(aid, headless=run_headless)
                acc_row = self._accounts.get_by_id(aid)
                acc_dict = dict(acc_row) if acc_row else None
                pages_out = scan_meta_business_pages_for_account(
                    ctx,
                    account_id=aid,
                    account=acc_dict,
                    status_cb=_status,
                )
            except Exception as exc:  # noqa: BLE001
                self._err_holder.append(str(exc))
                logger.exception("scan_meta_business_pages_for_account({}) lỗi", aid)
            finally:
                sync_close_persistent_context(ctx, log_label=f"page_scan:{aid}")
                if factory is not None:
                    try:
                        factory.close()
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Đóng factory sau scan: {}", exc)
                self._results = pages_out
                self._done_evt.set()

        self._scan_thread = threading.Thread(target=worker, name=f"page_scan_{aid}", daemon=True)
        self._scan_thread.start()
        self._poll()

    def _poll(self) -> None:
        # Kéo status từ queue.
        try:
            while True:
                msg = self._status_q.get_nowait()
                self._append_status(msg)
        except queue.Empty:
            pass
        if not self._done_evt.is_set():
            try:
                self._top.after(250, self._poll)
            except tk.TclError:
                return
            return
        # Scan xong → render bảng.
        self._btn_scan.configure(state=tk.NORMAL)
        self._cb_account.configure(state="readonly")
        self._cb_headless.configure(state=tk.NORMAL)
        self._cb_lock_ui.configure(state=tk.NORMAL)
        self._scan_running = False
        try:
            self._top.grab_set()
        except tk.TclError:
            pass
        if self._err_holder:
            self._append_status(f"Lỗi: {self._err_holder[0]}")
            messagebox.showerror(
                "Quét thất bại",
                self._err_holder[0],
                parent=self._top,
            )
            return
        self._populate_tree(self._results)
        self._count_var.set(
            f"{len(self._results)} Page — có Meta ID: {sum(1 for p in self._results if p.get('fb_page_id'))}."
        )
        if self._results:
            self._btn_save.configure(state=tk.NORMAL)

    # --- Tree helpers ---

    def _populate_tree(self, rows: list[ScannedPage]) -> None:
        for i in self._tree.get_children():
            self._tree.delete(i)
        for p in rows:
            checked = "☑" if p.get("fb_page_id") else "☐"
            self._tree.insert(
                "",
                tk.END,
                values=(
                    checked,
                    p.get("page_name", ""),
                    p.get("fb_page_id", ""),
                    p.get("business_name", ""),
                    p.get("business_id", ""),
                    p.get("page_url", ""),
                    p.get("source", ""),
                    p.get("role", "unknown"),
                ),
            )

    def _on_tree_click(self, event: tk.Event) -> None:
        col = self._tree.identify_column(event.x)
        row = self._tree.identify_row(event.y)
        if not row or col != "#1":
            return
        vals = list(self._tree.item(row, "values"))
        if not vals:
            return
        vals[0] = "☐" if vals[0] == "☑" else "☑"
        self._tree.item(row, values=vals)

    def _toggle_all(self, checked: bool) -> None:
        mark = "☑" if checked else "☐"
        for iid in self._tree.get_children():
            vals = list(self._tree.item(iid, "values"))
            vals[0] = mark
            self._tree.item(iid, values=vals)

    def _select_only_with_id(self) -> None:
        for iid in self._tree.get_children():
            vals = list(self._tree.item(iid, "values"))
            has_id = bool((vals[2] if len(vals) > 2 else "").strip())  # Meta Page ID
            vals[0] = "☑" if has_id else "☐"
            self._tree.item(iid, values=vals)

    # --- Save ---

    def _on_save_selected(self) -> None:
        if not self._selected_account_id:
            messagebox.showwarning("Thiếu account", "Không có account chủ sở hữu.", parent=self._top)
            return
        to_save: list[ScannedPage] = []
        for iid in self._tree.get_children():
            vals = self._tree.item(iid, "values")
            if not vals or vals[0] != "☑":
                continue
            to_save.append(
                ScannedPage(
                    page_name=str(vals[1]),
                    fb_page_id=str(vals[2]).strip(),
                    business_name=str(vals[3]).strip(),
                    business_id=str(vals[4]).strip(),
                    page_url=str(vals[5]),
                    source=str(vals[6]),
                    role=str(vals[7]).strip() if len(vals) > 7 else "unknown",
                )
            )
        if not to_save:
            messagebox.showinfo("Chưa chọn", "Tick ít nhất một Page để lưu.", parent=self._top)
            return
        if not messagebox.askyesno(
            "Xác nhận",
            f"Sẽ thêm/cập nhật {len(to_save)} Page cho account {self._selected_account_id!r} "
            f"vào pages.json. Tiếp tục?",
            parent=self._top,
        ):
            return
        existing = {(str(r.get("page_url", "")).strip().lower()): r for r in self._pages.load_all()}
        count_new = 0
        count_update = 0
        batch_rows: list[dict[str, Any]] = []
        for sp in to_save:
            url = sp.get("page_url", "").strip()
            if not url:
                continue
            row_dict: dict[str, Any] = {
                "account_id": self._selected_account_id,
                "page_name": sp.get("page_name", "").strip() or url,
                "page_url": url,
                "post_style": "post",
                "page_kind": "fanpage",
            }
            if sp.get("business_name"):
                row_dict["business_name"] = sp["business_name"]
            if sp.get("business_id"):
                row_dict["business_id"] = sp["business_id"]
            if sp.get("role"):
                row_dict["role"] = sp["role"]
            if sp.get("source"):
                row_dict["source"] = sp["source"]
            if sp.get("fb_page_id"):
                row_dict["fb_page_id"] = sp["fb_page_id"]
                row_dict["use_business_composer"] = True
            key = url.lower()
            if key in existing:
                cur = dict(existing[key])
                cur.update(
                    {k: v for k, v in row_dict.items() if v not in (None, "")}
                )
                cur["id"] = cur.get("id") or uuid.uuid4().hex[:12]
                batch_rows.append(cur)
                count_update += 1
            else:
                row_dict["id"] = uuid.uuid4().hex[:12]
                batch_rows.append(row_dict)
                count_new += 1
            existing[key] = batch_rows[-1]
        stats = {"new": 0, "updated": 0, "skipped_duplicate_meta_id": 0}
        if batch_rows:
            stats = self._pages.upsert_many(batch_rows)  # type: ignore[arg-type]
        self._saved_count = int(stats.get("new", 0)) + int(stats.get("updated", 0))
        dup_skip = int(stats.get("skipped_duplicate_meta_id", 0))
        messagebox.showinfo(
            "Đã lưu",
            f"Thêm mới: {int(stats.get('new', 0))} | Cập nhật: {int(stats.get('updated', 0))}"
            + (f" | Bỏ qua trùng Meta ID: {dup_skip}" if dup_skip > 0 else "."),
            parent=self._top,
        )
        logger.info(
            "[PageScan] account={} new={} update={} skip_dup_meta={}",
            self._selected_account_id,
            int(stats.get("new", 0)),
            int(stats.get("updated", 0)),
            dup_skip,
        )

    # --- Close ---

    def _on_close(self) -> None:
        if self._scan_running:
            if self._lock_ui_var.get():
                messagebox.showinfo(
                    "Đang quét",
                    "Đang quét và đang bật khóa thao tác. Vui lòng chờ quét xong hoặc tắt khóa trước khi quét.",
                    parent=self._top,
                )
                return
            if not messagebox.askyesno(
                "Đang quét",
                "Tiến trình quét vẫn đang chạy nền. Đóng cửa sổ này?",
                parent=self._top,
            ):
                return
        try:
            self._top.grab_release()
        except tk.TclError:
            pass
        self._top.destroy()

    @property
    def saved_count(self) -> int:
        return self._saved_count
