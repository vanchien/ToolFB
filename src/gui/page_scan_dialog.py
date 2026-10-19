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
from src.gui.treeview_shortcuts import install_treeview_shortcuts
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.page_categories import PAGE_CATEGORY_CHOICES, UNCATEGORIZED_LABEL, normalize_page_category
from src.services.page_health import pages_per_account
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
        self._selected_account_label: str = ""
        self._saved_count: int = 0
        self._scan_running: bool = False
        self._scan_queue: list[str] = []
        self._auto_save_meta: bool = False
        self._page_counts: dict[str, int] = {}
        self._account_rows_by_id: dict[str, dict[str, Any]] = {}

        self._top = tk.Toplevel(parent)
        self._top.title("Quét Page — Meta Business Suite")
        self._top.transient(parent)
        self._top.grab_set()
        self._top.geometry(_DEFAULT_GEOMETRY)
        self._top.minsize(680, 460)
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
        self._reload_account_choices()
        self._cb_account = ttk.Combobox(top, values=list(self._account_id_by_label), state="readonly")
        first_empty = next(
            (label for label, aid in self._account_id_by_label.items() if not self._page_counts.get(aid, 0)),
            "",
        )
        labels = list(self._account_id_by_label)
        if first_empty:
            self._cb_account.set(first_empty)
        elif labels:
            self._cb_account.set(labels[0])
        self._cb_account.grid(row=0, column=1, sticky="ew")

        btnf = ttk.Frame(top)
        btnf.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        self._btn_scan = ttk.Button(btnf, text="Quét", command=self._on_start_scan)
        self._btn_scan.pack(side=tk.LEFT)
        self._btn_scan_missing = ttk.Button(
            btnf, text="Quét TK chưa có Page", command=self._on_scan_accounts_without_pages
        )
        self._btn_scan_missing.pack(side=tk.LEFT, padx=(8, 0))

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
        lbl_scan_hint = ttk.Label(
            btnf,
            text="Sẽ mở trình duyệt với profile của tài khoản (không cần nhập lại mật khẩu nếu đã login).",
            foreground="gray",
            wraplength=640,
            font=("Segoe UI", 8),
        )
        lbl_scan_hint.pack(side=tk.LEFT, padx=(10, 0))
        self._top.bind(
            "<Configure>",
            lambda _e: lbl_scan_hint.configure(wraplength=max(320, int(self._top.winfo_width()) - 220)),
            add="+",
        )

    def _build_log(self) -> None:
        frm = ttk.LabelFrame(self._top, text="Tiến trình", padding=6)
        frm.grid(row=1, column=0, sticky="ew", padx=10, pady=4)
        frm.columnconfigure(0, weight=1)
        self._log_var = tk.StringVar(value="Sẵn sàng. Chọn tài khoản và bấm «Quét».")
        self._lbl_log = ttk.Label(frm, textvariable=self._log_var, foreground="#0a4fa0", wraplength=800)
        self._lbl_log.grid(
            row=0, column=0, sticky="w"
        )
        self._top.bind(
            "<Configure>",
            lambda _e: self._lbl_log.configure(wraplength=max(360, int(self._top.winfo_width()) - 80)),
            add="+",
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
            "stt",
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
            "stt": "STT",
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
            "stt": 44,
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
        install_treeview_shortcuts(self._tree, owner=self._top, info_callback=self._append_status)

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
        self._save_category_var = tk.StringVar(value=UNCATEGORIZED_LABEL)
        ttk.Combobox(
            f,
            textvariable=self._save_category_var,
            values=[UNCATEGORIZED_LABEL] + [label for _key, label in PAGE_CATEGORY_CHOICES],
            state="readonly",
            width=16,
        ).pack(side=tk.RIGHT, padx=(0, 8))
        ttk.Label(f, text="Phân loại khi lưu").pack(side=tk.RIGHT, padx=(0, 4))

    # --- Scan ---

    def _reload_account_choices(self) -> None:
        """Đọc lại accounts.json + pages.json — TK mới thêm hiện trong ô quét."""
        from src.services.page_creation.naming import account_display_label

        try:
            self._accounts.reload_from_disk()
        except Exception:  # noqa: BLE001
            logger.debug("reload accounts cho Quét Page", exc_info=True)
        try:
            page_rows = list(self._pages.load_all())
        except Exception:  # noqa: BLE001
            page_rows = []
        self._page_counts = pages_per_account(page_rows)
        self._account_id_by_label = {}
        self._account_rows_by_id = {}
        rows = []
        for acc in self._accounts.load_all():
            aid = str(acc.get("id") or "").strip()
            if not aid:
                continue
            self._account_rows_by_id[aid] = dict(acc)
            n_pages = int(self._page_counts.get(aid, 0) or 0)
            label = account_display_label(acc)
            label = f"{label} · {n_pages} Page" if n_pages else f"{label} · chưa có Page"
            if label in self._account_id_by_label:
                label = f"{label} · {aid}"
            rows.append((n_pages > 0, label.casefold(), aid, label))
        rows.sort()
        for _has, _cf, aid, label in rows:
            self._account_id_by_label[label] = aid

    def _missing_page_account_ids(self) -> list[str]:
        """Tài khoản trong registry chưa có dòng nào trong pages.json."""
        return [
            aid
            for aid in (self._account_id_by_label.values())
            if int(self._page_counts.get(aid, 0) or 0) <= 0
        ]

    def _label_for_account_id(self, account_id: str) -> str:
        for label, aid in self._account_id_by_label.items():
            if aid == account_id:
                return label
        return account_id

    def _append_status(self, msg: str) -> None:
        self._log_var.set(msg)

    def _on_scan_accounts_without_pages(self) -> None:
        """Quét lần lượt mọi tài khoản mới / chưa có Page, tự lưu Page có Meta ID."""
        if self._scan_thread and self._scan_thread.is_alive():
            messagebox.showinfo("Đang chạy", "Đang có tiến trình quét, vui lòng chờ.", parent=self._top)
            return
        self._reload_account_choices()
        if hasattr(self, "_cb_account"):
            self._cb_account.configure(values=list(self._account_id_by_label))
        missing = list(dict.fromkeys(self._missing_page_account_ids()))
        if not missing:
            messagebox.showinfo(
                "Quét Page",
                "Mọi tài khoản đã có Page trong pages.json. Chọn từng TK rồi bấm Quét nếu muốn quét lại.",
                parent=self._top,
            )
            return
        if not messagebox.askyesno(
            "Quét tài khoản chưa có Page",
            f"Sẽ quét lần lượt {len(missing)} tài khoản chưa có Page và tự lưu Page có Meta ID.\n"
            "Tài khoản lỗi sẽ bỏ qua, sang TK tiếp. Tiếp tục?",
            parent=self._top,
        ):
            return
        self._scan_queue = list(missing)
        self._auto_save_meta = True
        self._saved_count = 0
        self._start_next_queued_scan()

    def _start_next_queued_scan(self) -> None:
        try:
            if not self._top.winfo_exists():
                self._scan_queue = []
                self._auto_save_meta = False
                return
        except tk.TclError:
            self._scan_queue = []
            self._auto_save_meta = False
            return
        if not self._scan_queue:
            n = int(self._saved_count or 0)
            self._auto_save_meta = False
            self._append_status(f"Đã quét xong các tài khoản chưa có Page. Đã lưu {n} Page.")
            try:
                messagebox.showinfo(
                    "Quét xong",
                    f"Đã quét hết tài khoản chưa có Page.\nĐã lưu {n} Page vào pages.json.",
                    parent=self._top,
                )
            except tk.TclError:
                pass
            return
        aid = self._scan_queue.pop(0)
        label = self._label_for_account_id(aid)
        if hasattr(self, "_cb_account"):
            try:
                self._cb_account.set(label)
            except tk.TclError:
                pass
        left = len(self._scan_queue)
        self._append_status(f"Hàng đợi: đang quét {label} (còn {left} TK).")
        self._begin_scan(aid, label)

    def _on_start_scan(self) -> None:
        label = self._cb_account.get().strip()
        aid = self._account_id_by_label.get(label, label)
        if not aid:
            messagebox.showwarning("Chưa chọn", "Chọn một tài khoản để quét.", parent=self._top)
            return
        if self._scan_thread and self._scan_thread.is_alive():
            messagebox.showinfo("Đang chạy", "Đang có tiến trình quét, vui lòng chờ.", parent=self._top)
            return
        self._scan_queue = []
        self._auto_save_meta = False
        self._begin_scan(aid, label)

    def _begin_scan(self, aid: str, label: str) -> None:
        self._selected_account_id = aid
        self._selected_account_label = label
        self._results = []
        for i in self._tree.get_children():
            self._tree.delete(i)
        self._btn_save.configure(state=tk.DISABLED)
        self._btn_scan.configure(state=tk.DISABLED)
        if hasattr(self, "_btn_scan_missing"):
            self._btn_scan_missing.configure(state=tk.DISABLED)
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
            from src.utils.concurrency_runtime import KIND_BROWSER_POST, workload_begin, workload_end
            from src.utils.feature_busy import busy_reason

            factory: BrowserFactory | None = None
            ctx = None
            pages_out: list[ScannedPage] = []
            workload_begin(KIND_BROWSER_POST)
            try:
                reason = busy_reason(account_id=aid) or ""
                if reason:
                    self._err_holder.append(
                        f"Bỏ qua — {reason} (tránh mở trùng Firefox với chia sẻ/lịch đăng)."
                    )
                    return
                factory = BrowserFactory(accounts=self._accounts, headless=run_headless)
                acc_row = self._accounts.get_by_id(aid)
                acc_dict = dict(acc_row) if acc_row else None
                if acc_dict:
                    ctx = factory.launch_persistent_context_from_account_dict(
                        acc_dict,
                        headless=run_headless,
                        launch_slot_timeout_sec=40.0,
                    )
                else:
                    ctx = factory.get_browser_context(aid, headless=run_headless)
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
                workload_end(KIND_BROWSER_POST)
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
        if hasattr(self, "_btn_scan_missing"):
            self._btn_scan_missing.configure(state=tk.NORMAL)
        self._cb_account.configure(state="readonly")
        self._cb_headless.configure(state=tk.NORMAL)
        self._cb_lock_ui.configure(state=tk.NORMAL)
        self._scan_running = False
        try:
            self._top.grab_set()
        except tk.TclError:
            pass
        queued = bool(self._scan_queue or self._auto_save_meta)
        if self._err_holder:
            self._append_status(f"Lỗi {self._selected_account_label}: {self._err_holder[0]}")
            if queued:
                logger.warning("Quét Page {} lỗi — sang TK tiếp: {}", self._selected_account_id, self._err_holder[0])
                try:
                    self._top.after(200, self._start_next_queued_scan)
                except tk.TclError:
                    return
                return
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
        if self._auto_save_meta:
            saved = 0
            if self._results:
                saved = self._save_results(only_with_meta_id=True, confirm=False)
            self._append_status(
                f"{self._selected_account_label}: lưu {saved} Page có Meta ID. "
                f"Còn {len(self._scan_queue)} TK."
            )
            try:
                self._top.after(250, self._start_next_queued_scan)
            except tk.TclError:
                return

    # --- Tree helpers ---

    def _populate_tree(self, rows: list[ScannedPage]) -> None:
        for i in self._tree.get_children():
            self._tree.delete(i)
        for index, p in enumerate(rows, start=1):
            checked = "☑" if p.get("fb_page_id") else "☐"
            self._tree.insert(
                "",
                tk.END,
                values=(
                    index,
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
        if not row or col != "#2":
            return
        vals = list(self._tree.item(row, "values"))
        if len(vals) < 2:
            return
        vals[1] = "☐" if vals[1] == "☑" else "☑"
        self._tree.item(row, values=vals)

    def _toggle_all(self, checked: bool) -> None:
        mark = "☑" if checked else "☐"
        for iid in self._tree.get_children():
            vals = list(self._tree.item(iid, "values"))
            if len(vals) > 1:
                vals[1] = mark
            self._tree.item(iid, values=vals)

    def _select_only_with_id(self) -> None:
        for iid in self._tree.get_children():
            vals = list(self._tree.item(iid, "values"))
            has_id = bool((vals[3] if len(vals) > 3 else "").strip())  # Meta Page ID
            if len(vals) > 1:
                vals[1] = "☑" if has_id else "☐"
            self._tree.item(iid, values=vals)

    # --- Save ---

    def _on_save_selected(self) -> None:
        self._save_results(only_with_meta_id=False, confirm=True)

    def _save_results(self, *, only_with_meta_id: bool, confirm: bool) -> int:
        if not self._selected_account_id:
            if confirm:
                messagebox.showwarning("Thiếu account", "Không có account chủ sở hữu.", parent=self._top)
            return 0
        to_save: list[ScannedPage] = []
        if self._tree.get_children():
            for iid in self._tree.get_children():
                vals = self._tree.item(iid, "values")
                if len(vals) < 2:
                    continue
                if vals[1] != "☑" and not only_with_meta_id:
                    continue
                meta_id = str(vals[3]).strip() if len(vals) > 3 else ""
                if only_with_meta_id and not meta_id:
                    continue
                if only_with_meta_id or vals[1] == "☑":
                    to_save.append(
                        ScannedPage(
                            page_name=str(vals[2]),
                            fb_page_id=meta_id,
                            business_name=str(vals[4]).strip(),
                            business_id=str(vals[5]).strip(),
                            page_url=str(vals[6]),
                            source=str(vals[7]),
                            role=str(vals[8]).strip() if len(vals) > 8 else "unknown",
                        )
                    )
        elif only_with_meta_id:
            for p in self._results:
                if str(p.get("fb_page_id") or "").strip():
                    to_save.append(p)
        if not to_save:
            if confirm:
                messagebox.showinfo("Chưa chọn", "Tick ít nhất một Page để lưu.", parent=self._top)
            return 0
        if confirm and not messagebox.askyesno(
            "Xác nhận",
            f"Sẽ thêm/cập nhật {len(to_save)} Page cho tài khoản «{self._selected_account_label or self._selected_account_id}» "
            f"vào pages.json. Tiếp tục?",
            parent=self._top,
        ):
            return 0
        existing = {(str(r.get("page_url", "")).strip().lower()): r for r in self._pages.load_all()}
        save_category = normalize_page_category(self._save_category_var.get())
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
                # Không ghi đè phân loại người dùng đã gán trước đó.
                if save_category and not normalize_page_category(cur.get("page_category")):
                    cur["page_category"] = save_category
                batch_rows.append(cur)
                count_update += 1
            else:
                row_dict["id"] = uuid.uuid4().hex[:12]
                if save_category:
                    row_dict["page_category"] = save_category
                batch_rows.append(row_dict)
                count_new += 1
            existing[key] = batch_rows[-1]
        stats = {"new": 0, "updated": 0, "skipped_duplicate_meta_id": 0}
        if batch_rows:
            stats = self._pages.upsert_many(batch_rows)  # type: ignore[arg-type]
        saved_now = int(stats.get("new", 0)) + int(stats.get("updated", 0))
        if only_with_meta_id:
            self._saved_count += saved_now
        else:
            self._saved_count = saved_now
        dup_skip = int(stats.get("skipped_duplicate_meta_id", 0))
        if confirm:
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
        self._page_counts[self._selected_account_id] = int(
            self._page_counts.get(self._selected_account_id, 0) or 0
        ) + int(stats.get("new", 0) or 0)
        return saved_now

    # --- Close ---

    def _on_close(self) -> None:
        if self._scan_queue or self._auto_save_meta:
            if not messagebox.askyesno(
                "Đang quét lần lượt",
                "Còn tài khoản trong hàng đợi quét Page. Đóng và dừng hàng đợi?",
                parent=self._top,
            ):
                return
            self._scan_queue = []
            self._auto_save_meta = False
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
