"""Tab chia sẻ một link lên nhiều Page, nhiều tài khoản. Không tải video."""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Any

from src.gui.ui_responsiveness import schedule_on_main_thread
from src.services.facebook_groups.engine import GroupEngine, parse_group_uids
from src.services.facebook_groups.video_engage import clamp_watch_max_minutes, clamp_watch_seconds
from src.services.facebook_groups.facebook_provider import FacebookGroupProvider
from src.services.facebook_groups.fingerprint import normalize_share_images
from src.services.facebook_groups.store import GroupStore

_STATUS = {
    "PENDING": "Đang chờ",
    "SUBMIT": "Đang gửi",
    "CHECKING_PERMISSION": "Đang kiểm tra",
    "VERIFYING": "Đang kiểm tra",
    "COMPLETED": "Xong",
    "FAILED": "Lỗi",
    "SKIPPED": "Bỏ qua",
    "DUPLICATE": "Trùng",
    "PAUSED": "Tạm dừng",
    "DELAYING": "Nghỉ",
}


class ShareTab:
    """Chọn nhiều Page rồi gửi cùng một link. Mỗi tài khoản chỉ mở trình duyệt một lần."""

    def __init__(self, parent: ttk.Frame, root: tk.Misc) -> None:
        self.parent = parent
        self.root = root
        self.store = GroupStore()
        self.engine = GroupEngine(self.store, FacebookGroupProvider())
        self.engine.on_progress = self._on_progress
        self.pages: list[dict[str, str]] = []
        self.image_paths: list[str] = []
        self.batch_ids: list[str] = []
        self._running = False
        self._build()
        self.reload_pages()

    def _build(self) -> None:
        parent = self.parent
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)
        parent.rowconfigure(2, weight=1)

        form = ttk.LabelFrame(
            parent,
            text="Xem video, bình luận, rồi chia sẻ link lên Page. Không tải video",
            padding=8,
        )
        form.grid(row=0, column=0, sticky="ew", padx=4, pady=4)
        form.columnconfigure(1, weight=1)
        ttk.Label(form, text="Link bài").grid(row=0, column=0, sticky="w")
        self.source_url = tk.StringVar()
        ttk.Entry(form, textvariable=self.source_url).grid(row=0, column=1, columnspan=3, sticky="ew")
        ttk.Label(form, text="Nội dung lên Page").grid(row=1, column=0, sticky="w", pady=4)
        self.caption = tk.StringVar()
        ttk.Entry(form, textvariable=self.caption).grid(row=1, column=1, columnspan=3, sticky="ew")
        ttk.Label(form, text="Bình luận").grid(row=2, column=0, sticky="nw")
        self.comment_box = tk.Text(form, height=4, wrap="word", font=("Segoe UI", 9))
        self.comment_box.grid(row=2, column=1, columnspan=3, sticky="ew", pady=4)
        ttk.Label(form, text="Ảnh").grid(row=3, column=0, sticky="w")
        self.image_label = tk.StringVar(value="Chưa chọn ảnh")
        images = ttk.Frame(form)
        images.grid(row=3, column=1, sticky="w")
        ttk.Button(images, text="Chọn ảnh", command=self._pick_images).pack(side=tk.LEFT)
        ttk.Label(images, textvariable=self.image_label).pack(side=tk.LEFT, padx=6)
        self.destination = tk.StringVar(value="page")
        dest = ttk.Frame(form)
        dest.grid(row=4, column=1, columnspan=3, sticky="ew", pady=4)
        ttk.Radiobutton(dest, text="Lên Page của tôi", variable=self.destination, value="page", command=self._estimate).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Radiobutton(dest, text="Vào nhóm đã tham gia", variable=self.destination, value="joined", command=self._estimate).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Radiobutton(dest, text="Cả Page và nhóm", variable=self.destination, value="both", command=self._estimate).pack(side=tk.LEFT)
        ttk.Label(form, text="List UID nhóm").grid(row=5, column=0, sticky="nw")
        self.group_uid_box = tk.Text(form, height=3, wrap="word", font=("Consolas", 9))
        self.group_uid_box.grid(row=5, column=1, columnspan=3, sticky="ew", pady=4)
        self.group_uid_box.bind("<KeyRelease>", lambda _event: self._estimate())
        self.group_uid_count = tk.StringVar(value="Mỗi dòng một UID, hoặc cách nhau bằng dấu phẩy.")
        ttk.Label(form, textvariable=self.group_uid_count).grid(row=6, column=1, columnspan=3, sticky="w")
        timing = ttk.Frame(form)
        timing.grid(row=7, column=0, columnspan=4, sticky="ew", pady=(6, 0))
        ttk.Label(timing, text="Xem video (giây)").pack(side=tk.LEFT)
        self.watch_seconds = tk.StringVar(value="20")
        ttk.Entry(timing, textvariable=self.watch_seconds, width=6).pack(side=tk.LEFT, padx=(4, 8))
        ttk.Label(timing, text="Tối đa (phút)").pack(side=tk.LEFT)
        self.watch_max_minutes = tk.StringVar(value="5")
        ttk.Entry(timing, textvariable=self.watch_max_minutes, width=4).pack(side=tk.LEFT, padx=(4, 12))
        ttk.Label(timing, text="Nghỉ mỗi bài (giây)").pack(side=tk.LEFT)
        self.cooldown = tk.StringVar(value="30")
        ttk.Entry(timing, textvariable=self.cooldown, width=6).pack(side=tk.LEFT, padx=4)
        actions = ttk.Frame(form)
        actions.grid(row=8, column=0, columnspan=4, sticky="w", pady=4)
        ttk.Button(actions, text="Chia sẻ", command=self.start).pack(side=tk.LEFT)
        ttk.Button(actions, text="Dừng", command=self.stop).pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Tiếp tục", command=self.resume).pack(side=tk.LEFT)
        self.status = tk.StringVar(
            value="Mỗi dòng một bình luận. Mỗi tài khoản lấy ngẫu nhiên một câu, không trùng đến khi hết danh sách."
        )
        status_lbl = ttk.Label(form, textvariable=self.status, wraplength=360, justify=tk.LEFT)
        status_lbl.grid(row=9, column=0, columnspan=4, sticky="ew")

        def _fit_status(_event: object = None) -> None:
            width = int(form.winfo_width() or 0)
            if width > 80:
                status_lbl.configure(wraplength=max(160, width - 24))

        form.bind("<Configure>", _fit_status, add="+")

        listing = ttk.Frame(parent)
        listing.grid(row=1, column=0, sticky="nsew", padx=4)
        listing.columnconfigure(0, weight=1)
        listing.rowconfigure(1, weight=1)
        bar = ttk.Frame(listing)
        bar.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        ttk.Label(bar, text="Lọc").pack(side=tk.LEFT)
        self.filter_text = tk.StringVar()
        self.filter_text.trace_add("write", lambda *_args: self._fill_pages())
        ttk.Entry(bar, textvariable=self.filter_text, width=28).pack(side=tk.LEFT, padx=4)
        ttk.Button(bar, text="Chọn tất cả", command=self._select_all).pack(side=tk.LEFT, padx=4)
        ttk.Button(bar, text="Bỏ chọn", command=self._clear_selection).pack(side=tk.LEFT)
        ttk.Button(bar, text="Làm mới", command=self.reload_pages).pack(side=tk.LEFT, padx=4)
        self.estimate = tk.StringVar(value="")
        ttk.Label(bar, textvariable=self.estimate).pack(side=tk.LEFT, padx=8)

        cols = ("account", "page", "groups", "url")
        self.tree = ttk.Treeview(listing, columns=cols, show="headings", selectmode="extended")
        for key, title, width in (
            ("account", "Tài khoản", 180),
            ("page", "Page", 220),
            ("groups", "Nhóm đã vào", 110),
            ("url", "Link Page", 280),
        ):
            self.tree.heading(key, text=title)
            self.tree.column(key, width=width, stretch=True)
        self.tree.grid(row=1, column=0, sticky="nsew")
        self.tree.bind("<<TreeviewSelect>>", lambda _event: self._estimate())
        scroll = ttk.Scrollbar(listing, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        scroll.grid(row=1, column=1, sticky="ns")

        result = ttk.LabelFrame(parent, text="Tiến độ", padding=4)
        result.grid(row=2, column=0, sticky="nsew", padx=4, pady=4)
        result.columnconfigure(0, weight=1)
        result.rowconfigure(0, weight=1)
        result_cols = ("account", "page", "where", "status", "detail")
        self.results = ttk.Treeview(result, columns=result_cols, show="headings", height=8)
        for key, title, width in (
            ("account", "Tài khoản", 140),
            ("page", "Page", 180),
            ("where", "Đích", 140),
            ("status", "Kết quả", 100),
            ("detail", "Chi tiết", 280),
        ):
            self.results.heading(key, text=title)
            self.results.column(key, width=width, stretch=True)
        self.results.grid(row=0, column=0, sticky="nsew")
        result_scroll = ttk.Scrollbar(result, orient=tk.VERTICAL, command=self.results.yview)
        self.results.configure(yscrollcommand=result_scroll.set)
        result_scroll.grid(row=0, column=1, sticky="ns")

    def reload_pages(self) -> None:
        """Đọc Page đã lưu và số nhóm đã tham gia của từng Page."""
        names = self._account_names()
        joined = self._joined_counts()
        rows: list[dict[str, str]] = []
        try:
            from src.utils.pages_manager import PagesManager

            for row in PagesManager().load_all():
                account_id = str(row.get("account_id") or "")
                page_id = str(row.get("fb_page_id") or row.get("id") or "")
                if not account_id or not page_id:
                    continue
                name = str(row.get("page_name") or page_id)
                rows.append(
                    {
                        "iid": str(row.get("id") or page_id),
                        "account_id": account_id,
                        "account_name": names.get(account_id, account_id),
                        "page_id": page_id,
                        "page_name": name,
                        "target_url": str(row.get("page_url") or ""),
                        "joined": str(joined.get((account_id, page_id), 0)),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            self.status.set(f"Không đọc được danh sách Page: {exc}")
        self.pages = rows
        self._fill_pages()

    def _account_names(self) -> dict[str, str]:
        try:
            from src.utils.db_manager import AccountsDatabaseManager

            return {
                str(row.get("id") or ""): str(row.get("name") or row.get("id") or "")
                for row in AccountsDatabaseManager().load_all()
                if row.get("id")
            }
        except Exception:  # noqa: BLE001
            return {}

    def _joined_counts(self) -> dict[tuple[str, str], int]:
        counts: dict[tuple[str, str], int] = {}
        for row in self.store.load().get("memberships") or []:
            if row.get("membership_status") != "JOINED":
                continue
            if str(row.get("posting_permission") or "") == "DENIED":
                continue
            key = (str(row.get("account_id") or ""), str(row.get("page_id") or ""))
            counts[key] = counts.get(key, 0) + 1
        return counts

    def _fill_pages(self) -> None:
        needle = self.filter_text.get().strip().casefold()
        self.tree.delete(*self.tree.get_children())
        for row in self.pages:
            blob = " ".join((row["account_name"], row["page_name"], row["page_id"])).casefold()
            if needle and needle not in blob:
                continue
            self.tree.insert(
                "",
                tk.END,
                iid=row["iid"],
                values=(row["account_name"], row["page_name"], row["joined"], row["target_url"]),
            )
        self._estimate()

    def _selected_pages(self) -> list[dict[str, str]]:
        chosen = set(self.tree.selection())
        return [row for row in self.pages if row["iid"] in chosen]

    def _select_all(self) -> None:
        self.tree.selection_set(self.tree.get_children())
        self._estimate()

    def _clear_selection(self) -> None:
        self.tree.selection_remove(self.tree.selection())
        self._estimate()

    def _group_uid_raw(self) -> str:
        """Nội dung ô list UID nhóm. Một ô, nhiều dòng."""
        return self.group_uid_box.get("1.0", tk.END)

    def _comment_raw(self) -> str:
        """Nhiều bình luận, mỗi dòng một câu."""
        return self.comment_box.get("1.0", tk.END)

    def _estimate(self) -> None:
        selected = self._selected_pages()
        accounts = len({row["account_id"] for row in selected})
        groups = sum(int(row["joined"] or 0) for row in selected)
        place = self.destination.get()
        typed = len(parse_group_uids(self._group_uid_raw()))
        if typed:
            posts = len(selected) * typed
            if place in {"page", "both"}:
                posts += len(selected)
        elif place == "page":
            posts = len(selected)
        elif place == "joined":
            posts = groups
        else:
            posts = len(selected) + groups
        extra = f" · {typed} nhóm đã nhập" if typed else ""
        self.estimate.set(f"{len(selected)} page · {accounts} tài khoản · khoảng {posts} bài{extra}")
        self.group_uid_count.set(
            f"{typed} UID trong list" if typed else "Mỗi dòng một UID, hoặc cách nhau bằng dấu phẩy."
        )

    def _pick_images(self) -> None:
        picked = filedialog.askopenfilenames(
            parent=self.root,
            title="Chọn ảnh để kèm bài",
            filetypes=[("Ảnh", "*.jpg *.jpeg *.png *.webp *.gif")],
        )
        self.image_paths = [str(path) for path in picked]
        self.image_label.set(f"{len(self.image_paths)} ảnh" if self.image_paths else "Chưa chọn ảnh")

    def _cooldown_seconds(self) -> int:
        raw = self.cooldown.get().strip() or "30"
        try:
            seconds = int(raw)
        except ValueError:
            raise ValueError("Nghỉ giữa mỗi bài phải là số giây") from None
        if seconds < 0:
            raise ValueError("Nghỉ giữa mỗi bài phải từ 0 giây")
        return seconds

    def start(self) -> None:
        if self._running:
            self.status.set("Đang chia sẻ. Bấm Dừng nếu muốn ngắt.")
            return
        url = self.source_url.get().strip()
        if not url:
            messagebox.showwarning("Thiếu link", "Dán link bài. Tool không tải video.", parent=self.root)
            return
        selected = self._selected_pages()
        if not selected:
            messagebox.showwarning("Chưa chọn Page", "Chọn một hoặc nhiều Page.", parent=self.root)
            return
        try:
            images = normalize_share_images(self.image_paths)
            cooldown = self._cooldown_seconds()
            max_minutes = clamp_watch_max_minutes(self.watch_max_minutes.get())
            watch_seconds = clamp_watch_seconds(self.watch_seconds.get(), max_minutes)
        except ValueError as exc:
            messagebox.showwarning("Chia sẻ", str(exc), parent=self.root)
            return
        place = self.destination.get()
        group_ids = parse_group_uids(self._group_uid_raw())
        if self._group_uid_raw().strip() and not group_ids:
            messagebox.showwarning(
                "UID nhóm",
                "Dán list UID số, mỗi dòng một UID, hoặc link facebook.com/groups/123.",
                parent=self.root,
            )
            return
        if place == "joined" and not group_ids and not any(int(row["joined"] or 0) for row in selected):
            messagebox.showwarning(
                "Chưa có nhóm",
                "Các Page đã chọn chưa có nhóm đã tham gia. Hãy tham gia nhóm trước, hoặc chọn lên Page.",
                parent=self.root,
            )
            return
        self._running = True
        self.status.set("Đang xếp hàng…")

        def _work() -> None:
            try:
                result = self.engine.create_share_wave(
                    pages=selected,
                    source_url=url,
                    text=self.caption.get().strip(),
                    image_paths=images,
                    destination=place,
                    group_ids=group_ids,
                    cooldown_fixed=cooldown,
                    watch_seconds=watch_seconds,
                    comment=self._comment_raw(),
                )
                self.batch_ids = list(result.get("batch_ids") or [])
                created = int(result.get("created") or 0)
                accounts = int(result.get("accounts") or 0)
                schedule_on_main_thread(
                    self.root,
                    lambda: self.status.set(f"Đã xếp {created} bài trên {accounts} tài khoản. Đang gửi…"),
                )
                schedule_on_main_thread(self.root, self._fill_results)
                if created:
                    self.engine.run_share_wave(self.batch_ids)
            except Exception as exc:  # noqa: BLE001
                message = str(exc)
                schedule_on_main_thread(self.root, lambda message=message: self.status.set(f"Lỗi chia sẻ: {message}"))
            finally:
                self._running = False
                schedule_on_main_thread(self.root, self._fill_results)
                schedule_on_main_thread(self.root, lambda: self._finish_status())

        threading.Thread(target=_work, name="share-wave", daemon=True).start()

    def stop(self) -> None:
        for batch_id in list(self.batch_ids):
            try:
                self.engine.pause(batch_id)
            except Exception:  # noqa: BLE001
                continue
        self.status.set("Sẽ dừng sau bài đang gửi. Bấm Tiếp tục để chạy nốt hàng chờ.")

    def resume(self) -> None:
        if self._running or not self.batch_ids:
            return
        self._running = True
        ids = list(self.batch_ids)

        def _work() -> None:
            try:
                self.engine.resume_share_wave(ids)
            finally:
                self._running = False
                schedule_on_main_thread(self.root, self._fill_results)
                schedule_on_main_thread(self.root, self._finish_status)

        threading.Thread(target=_work, name="share-wave-resume", daemon=True).start()

    def _finish_status(self) -> None:
        if self._running:
            return
        rows = self._jobs()
        done = sum(1 for row in rows if str(row.get("status")) == "COMPLETED")
        failed = sum(1 for row in rows if str(row.get("status")) == "FAILED")
        self.status.set(f"Xong {done} bài. Lỗi {failed}. Mỗi tài khoản đã dùng một lần mở trình duyệt.")

    def _on_progress(self, text: str) -> None:
        schedule_on_main_thread(self.root, lambda: self.status.set(text))
        schedule_on_main_thread(self.root, self._fill_results)

    def _jobs(self) -> list[dict[str, Any]]:
        wanted = set(self.batch_ids)
        if not wanted:
            return []
        return [row for row in self.store.share_jobs() if str(row.get("batch_id") or "") in wanted]

    def _fill_results(self) -> None:
        names = {(row["account_id"], row["page_id"]): row for row in self.pages}
        self.results.delete(*self.results.get_children())
        for job in self._jobs():
            page_id = str(job.get("page_id") or "")
            account_id = str(job.get("account_id") or "")
            known = names.get((account_id, page_id), {})
            where = "Page" if str(job.get("destination") or "") == "page" else str(job.get("group_id") or "nhóm")
            status = _STATUS.get(str(job.get("status") or ""), str(job.get("status") or ""))
            detail = str(job.get("error_message") or job.get("post_url") or "")
            self.results.insert(
                "",
                tk.END,
                values=(known.get("account_name", account_id), known.get("page_name", page_id), where, status, detail),
            )


def build_share_tab(parent: ttk.Frame, root: tk.Misc) -> ShareTab:
    """Gắn tab chia sẻ vào notebook chính."""
    return ShareTab(parent, root)
