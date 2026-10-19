"""
Hộp thoại «Xóa video trên Page»: quét list, lọc, tick một/nhiều/tất cả rồi xóa trên Facebook.
"""

from __future__ import annotations

import queue
import threading
from pathlib import Path
from typing import Any

import tkinter as tk
from tkinter import messagebox, ttk

from loguru import logger

from src.gui.treeview_shortcuts import install_treeview_shortcuts
from src.gui.ui_responsiveness import run_background_then_main, schedule_on_main_thread, tree_delete_all
from src.services.page_video_cleanup import (
    PageVideoCleanupRunner,
    filter_page_videos,
    resolve_page_cleanup_target,
)
from src.services.page_video_harvest import sort_videos_by_published
from src.utils.db_manager import AccountsDatabaseManager


def open_page_video_cleanup_dialog(parent: tk.Misc, page_row: dict[str, Any]) -> None:
    """Mở hộp thoại xóa video cho một bản ghi Page."""
    PageVideoCleanupDialog(parent, page_row)


class PageVideoCleanupDialog:
    """Quét video của Page, lọc, chọn rồi xóa trên Facebook."""

    def __init__(self, parent: tk.Misc, page_row: dict[str, Any]) -> None:
        self._parent = parent
        self._page = dict(page_row or {})
        self._items: list[dict[str, str]] = []
        self._row_by_iid: dict[str, dict[str, str]] = {}
        self._photos: list[Any] = []
        self._preview_photo: Any = None
        self._busy = False
        self._cancel = threading.Event()
        self._status_q: queue.Queue[str] = queue.Queue()
        self._runner = PageVideoCleanupRunner()
        self._target = resolve_page_cleanup_target(self._page)

        name = self._target.get("page_name") or "Page"
        self._top = tk.Toplevel(parent)
        self._top.title(f"Xóa video trên Page — {name}")
        self._top.transient(parent)
        self._top.geometry("1100x660")
        self._top.minsize(860, 520)
        self._top.columnconfigure(0, weight=1)
        self._top.rowconfigure(2, weight=1)

        self._build()
        self._top.protocol("WM_DELETE_WINDOW", self._on_close)
        self._pump_status()

    def _page_url(self) -> str:
        return str(self._page.get("page_url") or "").strip()

    def _page_name(self) -> str:
        return str(self._page.get("page_name") or "").strip()

    def _account_id(self) -> str:
        return str(self._page.get("account_id") or "").strip()

    def _build(self) -> None:
        head = ttk.Frame(self._top, padding=(10, 8, 10, 4))
        head.grid(row=0, column=0, sticky="ew")
        head.columnconfigure(1, weight=1)
        ttk.Label(head, text="Page").grid(row=0, column=0, sticky="w")
        ttk.Label(head, text=self._page_name() or "—", font=("Segoe UI", 10, "bold")).grid(
            row=0, column=1, sticky="w", padx=(8, 0)
        )
        meta = self._target.get("fb_page_id") or "—"
        ttk.Label(
            head,
            text=f"{self._page_url()}  ·  Meta ID {meta}",
            foreground="#555",
        ).grid(row=1, column=1, sticky="w", padx=(8, 0))

        bar = ttk.Frame(self._top, padding=(10, 0, 10, 4))
        bar.grid(row=1, column=0, sticky="ew")
        self._btn_scan = ttk.Button(bar, text="Quét video", command=self._on_scan)
        self._btn_scan.pack(side=tk.LEFT, padx=(0, 6))
        self._show_browser = tk.BooleanVar(value=False)
        ttk.Checkbutton(bar, text="Hiện trình duyệt (mặc định ẩn)", variable=self._show_browser).pack(
            side=tk.LEFT, padx=(0, 10)
        )
        ttk.Label(bar, text="Tối đa").pack(side=tk.LEFT)
        self._max_var = tk.StringVar(value="2000")
        ttk.Entry(bar, textvariable=self._max_var, width=6).pack(side=tk.LEFT, padx=(4, 10))
        ttk.Label(bar, text="Lọc").pack(side=tk.LEFT)
        self._filter_var = tk.StringVar()
        ent = ttk.Entry(bar, textvariable=self._filter_var, width=28)
        ent.pack(side=tk.LEFT, padx=(4, 8))
        ent.bind("<KeyRelease>", lambda _e: self._render_tree())
        ttk.Label(bar, text="Sắp xếp").pack(side=tk.LEFT, padx=(8, 0))
        self._sort_mode = tk.StringVar(value="Mới → cũ")
        sort_cb = ttk.Combobox(
            bar,
            textvariable=self._sort_mode,
            values=("Mới → cũ", "Cũ → mới"),
            state="readonly",
            width=12,
        )
        sort_cb.pack(side=tk.LEFT, padx=(4, 8))
        sort_cb.bind("<<ComboboxSelected>>", lambda _e: self._render_tree())
        ttk.Button(bar, text="Chọn tất cả", command=lambda: self._toggle_all(True)).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(bar, text="Bỏ chọn", command=lambda: self._toggle_all(False)).pack(side=tk.LEFT)

        body = ttk.Frame(self._top, padding=(10, 0, 10, 4))
        body.grid(row=2, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)
        body.rowconfigure(0, weight=1)
        cols = ("tick", "stt", "kind", "title", "published", "video_id")
        style = ttk.Style(self._top)
        style.configure("VideoCleanup.Treeview", rowheight=52)
        self._tree = ttk.Treeview(
            body,
            columns=cols,
            show="tree headings",
            selectmode="extended",
            style="VideoCleanup.Treeview",
        )
        self._tree.heading("#0", text="Ảnh")
        self._tree.column("#0", width=58, minwidth=52, stretch=False, anchor="center")
        heads = {
            "tick": "✓",
            "stt": "STT",
            "kind": "Loại",
            "title": "Tiêu đề",
            "published": "Đăng lúc",
            "video_id": "Video ID",
        }
        widths = {"tick": 36, "stt": 48, "kind": 70, "title": 260, "published": 160, "video_id": 150}
        for c in cols:
            self._tree.heading(c, text=heads[c], command=lambda col=c: self._on_heading(col))
            self._tree.column(c, width=widths[c], stretch=(c == "title"), anchor="w")
        sy = ttk.Scrollbar(body, orient=tk.VERTICAL, command=self._tree.yview)
        self._tree.configure(yscrollcommand=sy.set)
        self._tree.grid(row=0, column=0, sticky="nsew")
        sy.grid(row=0, column=1, sticky="ns")
        preview = ttk.Frame(body, padding=(10, 0, 0, 0), width=240)
        preview.grid(row=0, column=2, sticky="ns")
        preview.pack_propagate(False)
        ttk.Label(preview, text="Xem trước", font=("Segoe UI", 9, "bold")).pack(anchor="w")
        self._preview_img = ttk.Label(
            preview,
            text="Chọn một video để xem ảnh",
            anchor="center",
            justify="center",
            width=28,
        )
        self._preview_img.pack(fill=tk.X, pady=(6, 8))
        self._preview_title = ttk.Label(preview, text="", wraplength=220, font=("Segoe UI", 9, "bold"))
        self._preview_title.pack(fill=tk.X, anchor="w")
        self._preview_meta = ttk.Label(preview, text="", wraplength=220, foreground="#555")
        self._preview_meta.pack(fill=tk.X, anchor="w", pady=(4, 0))
        install_treeview_shortcuts(self._tree, owner=self._top, info_callback=self._set_status)
        self._tree.bind("<Button-1>", self._on_tree_click, add="+")
        self._tree.bind("<<TreeviewSelect>>", lambda _e: self._show_preview())
        self._tree.bind("<space>", lambda _e: self._toggle_selection())
        self._tree.bind("<Delete>", lambda _e: self._on_delete_selected())

        foot = ttk.Frame(self._top, padding=(10, 4, 10, 10))
        foot.grid(row=3, column=0, sticky="ew")
        self._status = tk.StringVar(
            value="Bấm «Quét video» — chạy ẩn, vào Content Library của Page này rồi lấy list Published."
        )
        ttk.Label(foot, textvariable=self._status, foreground="#334155").pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(foot, text="Đóng", command=self._on_close).pack(side=tk.RIGHT)
        self._btn_stop = ttk.Button(foot, text="Dừng", command=self._on_stop, state=tk.DISABLED)
        self._btn_stop.pack(side=tk.RIGHT, padx=(0, 8))
        ttk.Button(foot, text="Xóa tất cả", command=self._on_delete_all).pack(side=tk.RIGHT, padx=(0, 8))
        ttk.Button(foot, text="Xóa đã chọn", command=self._on_delete_selected).pack(side=tk.RIGHT, padx=(0, 8))

    def _set_status(self, text: str) -> None:
        self._status_q.put(str(text or ""))

    def _pump_status(self) -> None:
        last = ""
        try:
            while True:
                last = self._status_q.get_nowait()
        except queue.Empty:
            pass
        if last:
            self._status.set(last)
        try:
            self._top.after(200, self._pump_status)
        except tk.TclError:
            pass

    def _newest_first(self) -> bool:
        return "cũ → mới" not in (self._sort_mode.get() or "").casefold()

    def _on_heading(self, col: str) -> None:
        if col != "published":
            return
        self._sort_mode.set("Cũ → mới" if self._newest_first() else "Mới → cũ")
        self._render_tree()

    def _visible_items(self) -> list[dict[str, str]]:
        rows = filter_page_videos(self._items, self._filter_var.get())
        return sort_videos_by_published(rows, newest_first=self._newest_first())

    def _thumb_photo(self, rec: dict[str, str], *, size: tuple[int, int]) -> Any:
        """Tạo PhotoImage từ file cache; None nếu thiếu ảnh hoặc chưa có Pillow."""
        path = str(rec.get("thumb_path") or "").strip()
        if not path or not Path(path).is_file():
            return None
        try:
            from PIL import Image, ImageTk

            im = Image.open(path)
            im.thumbnail(size)
            return ImageTk.PhotoImage(im)
        except Exception:
            return None

    def _show_preview(self) -> None:
        sel = self._tree.selection()
        rec = self._row_by_iid.get(str(sel[0])) if sel else None
        self._preview_photo = None
        if rec is None:
            self._preview_img.configure(image="", text="Chọn một video để xem ảnh")
            self._preview_title.configure(text="")
            self._preview_meta.configure(text="")
            return
        photo = self._thumb_photo(rec, size=(220, 300))
        title = str(rec.get("title") or "").strip() or "Không có tiêu đề"
        meta = " · ".join(
            x
            for x in (
                rec.get("kind") or "",
                rec.get("published") or rec.get("published_raw") or "",
                rec.get("video_id") or "",
            )
            if x
        )
        self._preview_title.configure(text=title)
        self._preview_meta.configure(text=meta)
        if photo is None:
            self._preview_img.configure(image="", text="Chưa có ảnh thumbnail")
            return
        self._preview_photo = photo
        self._preview_img.configure(image=photo, text="")

    def _render_tree(self) -> None:
        ticked = self._ticked_ids()
        rows = self._visible_items()
        tree_delete_all(self._tree)
        self._row_by_iid = {}
        self._photos = []
        used: set[str] = set()
        for i, rec in enumerate(rows, start=1):
            iid = str(rec.get("video_id") or rec.get("row_sig") or rec.get("url") or i)
            if iid in used:
                iid = f"{iid}#{i}"
            used.add(iid)
            self._row_by_iid[iid] = rec
            mark = "☑" if iid in ticked else "☐"
            photo = self._thumb_photo(rec, size=(48, 64))
            if photo is not None:
                self._photos.append(photo)
            title = str(rec.get("title") or "").strip() or "—"
            self._tree.insert(
                "",
                tk.END,
                iid=iid,
                text="",
                image=photo or "",
                values=(
                    mark,
                    i,
                    rec.get("kind") or "",
                    title,
                    rec.get("published") or rec.get("published_raw") or "",
                    rec.get("video_id") or "",
                ),
            )
        filt = str(self._filter_var.get() or "").strip()
        if filt:
            self._status.set(f"Hiện {len(rows)} / {len(self._items)} video (lọc «{filt}»).")
        elif self._items:
            self._status.set(f"{len(self._items)} video trên Page.")
        self._show_preview()

    def _ticked_ids(self) -> set[str]:
        out: set[str] = set()
        for iid in self._tree.get_children():
            vals = self._tree.item(iid, "values")
            if vals and str(vals[0]) == "☑":
                out.add(str(iid))
        return out

    def _ticked_items(self) -> list[dict[str, str]]:
        wanted = self._ticked_ids()
        return [self._row_by_iid[i] for i in wanted if i in self._row_by_iid]

    def _on_tree_click(self, event: tk.Event) -> str | None:
        col = self._tree.identify_column(event.x)
        row = self._tree.identify_row(event.y)
        if not row or col != "#1":
            return None
        vals = list(self._tree.item(row, "values"))
        if not vals:
            return "break"
        vals[0] = "☐" if vals[0] == "☑" else "☑"
        self._tree.item(row, values=vals)
        return "break"

    def _toggle_selection(self) -> None:
        for iid in self._tree.selection() or ():
            vals = list(self._tree.item(iid, "values"))
            if not vals:
                continue
            vals[0] = "☐" if vals[0] == "☑" else "☑"
            self._tree.item(iid, values=vals)

    def _toggle_all(self, checked: bool) -> None:
        mark = "☑" if checked else "☐"
        for iid in self._tree.get_children():
            vals = list(self._tree.item(iid, "values"))
            if vals:
                vals[0] = mark
                self._tree.item(iid, values=vals)

    def _load_account(self) -> dict[str, Any] | None:
        aid = self._account_id()
        if not aid:
            messagebox.showwarning("Thiếu tài khoản", "Page chưa gắn account_id.", parent=self._top)
            return None
        row = AccountsDatabaseManager().get_by_id(aid)
        if row is None:
            messagebox.showwarning("Thiếu tài khoản", f"Không thấy tài khoản {aid} trong accounts.json.", parent=self._top)
            return None
        return dict(row)

    def _set_busy(self, busy: bool) -> None:
        self._busy = busy
        try:
            self._btn_stop.configure(state=tk.NORMAL if busy else tk.DISABLED)
        except tk.TclError:
            pass
        try:
            self._btn_scan.configure(state=tk.DISABLED if busy else tk.NORMAL)
        except tk.TclError:
            pass

    def _on_scan(self) -> None:
        if self._busy:
            self._set_status("Đang quét — bấm «Dừng» nếu kẹt, rồi quét lại.")
            return
        acc = self._load_account()
        if acc is None:
            return
        url = self._page_url()
        if not url:
            messagebox.showwarning("Thiếu link", "Page chưa có page_url.", parent=self._top)
            return
        try:
            cap = max(10, min(8000, int(str(self._max_var.get() or "2000").strip())))
        except ValueError:
            cap = 400
        self._cancel.clear()
        self._set_busy(True)
        self._set_status(f"Bắt đầu quét Content Library «{self._page_name()}»…")
        headless = not bool(self._show_browser.get())
        page_row = dict(self._page)

        def _work() -> dict[str, Any]:
            return self._runner.call(
                "scan",
                {
                    "page_row": page_row,
                    "account": acc,
                    "status": lambda m: schedule_on_main_thread(self._top, lambda t=m: self._set_status(t)),
                    "max_videos": cap,
                    "headless": headless,
                },
            )

        def _done(ret: dict[str, Any]) -> None:
            self._set_busy(False)
            self._items = list(ret.get("items") or [])
            self._render_tree()
            self._set_status(str(ret.get("message") or ""))
            if not self._items:
                messagebox.showinfo("Không có video", str(ret.get("message") or "Không quét được video."), parent=self._top)

        def _err(exc: BaseException) -> None:
            self._set_busy(False)
            logger.exception("Quét video Page")
            messagebox.showerror("Lỗi quét", str(exc), parent=self._top)

        run_background_then_main(self._top, _work, _done, on_error=_err)

    def _on_stop(self) -> None:
        self._cancel.set()
        self._runner.cancel()
        self._set_status("Đang dừng…")

    def _on_delete_selected(self) -> None:
        picked = self._ticked_items()
        if not picked and self._tree.selection():
            picked = [self._row_by_iid[i] for i in self._tree.selection() if i in self._row_by_iid]
            if picked:
                for iid in self._tree.selection():
                    vals = list(self._tree.item(iid, "values"))
                    if vals:
                        vals[0] = "☑"
                        self._tree.item(iid, values=vals)
        if not picked:
            messagebox.showinfo("Chưa chọn", "Tick (cột ✓) một hoặc nhiều video, hoặc chọn dòng rồi Xóa đã chọn.", parent=self._top)
            return
        self._confirm_and_delete(picked)

    def _on_delete_all(self) -> None:
        rows = self._visible_items()
        if not rows:
            messagebox.showinfo("Trống", "Chưa có list video. Bấm «Quét video» trước.", parent=self._top)
            return
        self._toggle_all(True)
        filtered = bool(str(self._filter_var.get() or "").strip())
        self._confirm_and_delete(rows, delete_all=not filtered)

    def _confirm_and_delete(self, items: list[dict[str, str]], *, delete_all: bool = False) -> None:
        if self._busy:
            return
        n = len(items)
        if not messagebox.askyesno(
            "Xóa video trên Facebook",
            f"Xóa {n} video/reel trên Content Library Page «{self._page_name()}» "
            f"({'tất cả trên lưới Published' if delete_all else 'các hàng đã chọn'})?\n"
            "Chạy ẩn trình duyệt. Thao tác trên Facebook (Delete posts). Không hoàn tác từ ToolFB.",
            parent=self._top,
            icon="warning",
        ):
            return
        acc = self._load_account()
        if acc is None:
            return
        self._cancel.clear()
        self._set_busy(True)
        self._set_status(f"Xóa {n} video trên đúng Page «{self._page_name()}» (cùng phiên trình duyệt)…")
        headless = not bool(self._show_browser.get())
        page_row = dict(self._page)

        def _work() -> dict[str, Any]:
            return self._runner.call(
                "delete",
                {
                    "page_row": page_row,
                    "account": acc,
                    "items": items,
                    "status": lambda m: schedule_on_main_thread(self._top, lambda t=m: self._set_status(t)),
                    "headless": headless,
                    "delete_all": delete_all,
                },
            )

        def _done(ret: dict[str, Any]) -> None:
            self._set_busy(False)
            deleted = int(ret.get("deleted") or 0)
            ok_ids = {
                str(r.get("video_id") or r.get("url"))
                for r in (ret.get("results") or [])
                if str(r.get("ok")) == "1"
            }
            if delete_all and deleted > 0 and int(ret.get("failed") or 0) == 0:
                self._items = []
                self._render_tree()
            elif ok_ids:
                self._items = [
                    it
                    for it in self._items
                    if str(it.get("video_id") or it.get("url")) not in ok_ids
                ]
                self._render_tree()
            self._set_status(str(ret.get("message") or ""))
            messagebox.showinfo("Xóa video", str(ret.get("message") or f"Đã xóa {deleted}."), parent=self._top)

        def _err(exc: BaseException) -> None:
            self._set_busy(False)
            logger.exception("Xóa video Page")
            messagebox.showerror("Lỗi xóa video", str(exc), parent=self._top)

        run_background_then_main(self._top, _work, _done, on_error=_err)

    def _on_close(self) -> None:
        if self._busy:
            if not messagebox.askyesno(
                "Đang chạy",
                "Đang quét/xóa. Đóng cửa sổ sẽ gửi lệnh dừng (phiên hiện tại kết thúc sau video đang làm).",
                parent=self._top,
            ):
                return
            self._cancel.set()
            self._runner.cancel()
        self._runner.shutdown_async()
        try:
            self._top.destroy()
        except tk.TclError:
            pass
