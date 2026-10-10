"""Tab chia sẻ một link lên nhiều Page, nhiều tài khoản. Không tải video."""

from __future__ import annotations

import threading
import time
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox, ttk
from typing import Any

from src.gui.ui_responsiveness import schedule_on_main_thread
from src.services.facebook_groups.engine import GroupEngine, parse_group_uids
from src.services.facebook_groups.video_engage import clamp_reel_seconds, optional_watch_bounds
from src.services.facebook_groups.facebook_provider import FacebookGroupProvider
from src.services.facebook_groups.fingerprint import normalize_share_images
from src.services.facebook_groups.share_schedule import (
    PlannedShareLink,
    compose_clock,
    hour_choices,
    machine_clock_text,
    make_queue_item,
    minute_choices,
    queue_state_from_results,
)
from src.services.facebook_groups.store import GroupStore

def describe_page_jobs(jobs: list[dict[str, Any]], paused_note: str = "") -> tuple[str, str, str]:
    """Kết quả một page: nhãn, câu chi tiết, và màu (ok, fail, part, run, wait)."""
    if not jobs:
        if paused_note:
            return "Lỗi", paused_note, "fail"
        return "", "", ""
    done = [row for row in jobs if str(row.get("status") or "") == "COMPLETED"]
    failed = [row for row in jobs if str(row.get("status") or "") in {"FAILED", "SKIPPED"}]
    active = [row for row in jobs if str(row.get("status") or "") in {"SUBMIT", "CHECKING_PERMISSION", "VERIFYING", "DELAYING"}]
    paused = [row for row in jobs if str(row.get("status") or "") == "PAUSED"]
    pending = [row for row in jobs if str(row.get("status") or "") == "PENDING"]
    errors = [str(row.get("error_message") or "").strip() for row in failed + paused if str(row.get("error_message") or "").strip()]
    if done and not failed and not active and not paused and not pending:
        return "Thành công", f"Đã chia sẻ {len(done)} bài", "ok"
    if failed and not done and not active and not pending:
        return "Lỗi", errors[0] if errors else "Chưa chia sẻ được", "fail"
    if paused and not done and not active:
        return "Lỗi", paused_note or (errors[0] if errors else "Đã tạm dừng"), "fail"
    if done and (failed or paused):
        return "Một phần", errors[0] if errors else f"Thành công {len(done)}, lỗi {len(failed)}", "part"
    if active:
        return "Đang gửi", "Đang chia sẻ link", "run"
    if pending and paused_note:
        return "Lỗi", paused_note, "fail"
    if pending:
        return "Đang chờ", "Chưa tới lượt", "wait"
    return "Đang chờ", "", "wait"


class ShareTab:
    """Chọn nhiều Page rồi gửi cùng một link. Mỗi tài khoản chỉ mở trình duyệt một lần."""

    def __init__(self, parent: ttk.Frame, root: tk.Misc) -> None:
        self.parent = parent
        self.root = root
        self.store = GroupStore()
        self.engine = GroupEngine(self.store, FacebookGroupProvider())
        self.engine.on_progress = self._on_progress
        self.pages: list[dict[str, str]] = []
        self._checked: set[str] = set()
        self._outcomes: dict[tuple[str, str], tuple[str, str, str]] = {}
        self.image_paths: list[str] = []
        self.batch_ids: list[str] = []
        self._running = False
        self._cancel_schedule = threading.Event()
        self._queue: list[PlannedShareLink] = []
        self._queue_lock = threading.Lock()
        self._queue_seq = 0
        self._build()
        self.reload_pages()

    def _build(self) -> None:
        parent = self.parent
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)
        parent.rowconfigure(2, weight=1)

        form = ttk.LabelFrame(
            parent,
            text="Lướt Reel, xem video, bình luận, rồi chia sẻ link. Không tải video",
            padding=8,
        )
        form.grid(row=0, column=0, sticky="ew", padx=4, pady=4)
        form.columnconfigure(1, weight=1)
        ttk.Label(form, text="Link bài").grid(row=0, column=0, sticky="w")
        self.source_url = tk.StringVar()
        ttk.Entry(form, textvariable=self.source_url).grid(row=0, column=1, sticky="ew")
        ttk.Label(form, text="Giờ").grid(row=0, column=2, sticky="e", padx=(8, 4))
        clock = ttk.Frame(form)
        clock.grid(row=0, column=3, sticky="w")
        self.clock_hour = tk.StringVar()
        self.clock_minute = tk.StringVar(value="00")
        self.machine_clock = tk.StringVar()
        self.hour_box = ttk.Combobox(
            clock,
            textvariable=self.clock_hour,
            width=4,
            state="readonly",
            values=hour_choices(),
        )
        self.hour_box.pack(side=tk.LEFT)
        self.hour_box.bind("<Button-1>", lambda _event: self._sync_hour_choices())
        ttk.Label(clock, text=":").pack(side=tk.LEFT, padx=2)
        ttk.Combobox(
            clock,
            textvariable=self.clock_minute,
            width=4,
            state="readonly",
            values=minute_choices(),
        ).pack(side=tk.LEFT)
        ttk.Label(clock, textvariable=self.machine_clock).pack(side=tk.LEFT, padx=(8, 0))
        self._sync_clock_to_machine()
        add_row = ttk.Frame(form)
        add_row.grid(row=1, column=1, columnspan=3, sticky="w", pady=(4, 0))
        ttk.Button(add_row, text="Thêm vào list", command=self._add_scheduled).pack(side=tk.LEFT)
        ttk.Button(add_row, text="Chia sẻ ngay", command=self._add_now).pack(side=tk.LEFT, padx=4)
        ttk.Button(add_row, text="Xóa dòng", command=self._remove_queued).pack(side=tk.LEFT)
        self.queue_tree = ttk.Treeview(form, columns=("when", "url", "state"), show="headings", height=4)
        for key, title, width in (("when", "Giờ", 110), ("url", "Link chờ", 420), ("state", "Trạng thái", 110)):
            self.queue_tree.heading(key, text=title)
            self.queue_tree.column(key, width=width, stretch=(key == "url"))
        self.queue_tree.grid(row=2, column=0, columnspan=4, sticky="ew", pady=4)
        ttk.Label(form, text="Nội dung lên Page").grid(row=3, column=0, sticky="w", pady=4)
        self.caption = tk.StringVar()
        ttk.Entry(form, textvariable=self.caption).grid(row=3, column=1, columnspan=3, sticky="ew")
        ttk.Label(form, text="Bình luận").grid(row=4, column=0, sticky="nw")
        self.comment_box = tk.Text(form, height=3, wrap="word", font=("Segoe UI", 9))
        self.comment_box.grid(row=4, column=1, columnspan=3, sticky="ew", pady=4)
        ttk.Label(form, text="Ảnh").grid(row=5, column=0, sticky="w")
        self.image_label = tk.StringVar(value="Chưa chọn ảnh")
        images = ttk.Frame(form)
        images.grid(row=5, column=1, sticky="w")
        ttk.Button(images, text="Chọn ảnh", command=self._pick_images).pack(side=tk.LEFT)
        ttk.Label(images, textvariable=self.image_label).pack(side=tk.LEFT, padx=6)
        self.destination = tk.StringVar(value="page")
        dest = ttk.Frame(form)
        dest.grid(row=6, column=1, columnspan=3, sticky="ew", pady=4)
        ttk.Radiobutton(dest, text="Lên Page của tôi", variable=self.destination, value="page", command=self._estimate).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Radiobutton(dest, text="Vào nhóm đã tham gia", variable=self.destination, value="joined", command=self._estimate).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Radiobutton(dest, text="Cả Page và nhóm", variable=self.destination, value="both", command=self._estimate).pack(side=tk.LEFT)
        ttk.Label(form, text="List UID nhóm").grid(row=7, column=0, sticky="nw")
        self.group_uid_box = tk.Text(form, height=3, wrap="word", font=("Consolas", 9))
        self.group_uid_box.grid(row=7, column=1, columnspan=3, sticky="ew", pady=4)
        self.group_uid_box.bind("<KeyRelease>", lambda _event: self._estimate())
        self.group_uid_count = tk.StringVar(value="Mỗi dòng một UID, hoặc cách nhau bằng dấu phẩy.")
        ttk.Label(form, textvariable=self.group_uid_count).grid(row=8, column=1, columnspan=3, sticky="w")
        timing = ttk.Frame(form)
        timing.grid(row=9, column=0, columnspan=4, sticky="ew", pady=(6, 0))
        ttk.Label(timing, text="Lướt Reel (giây)").pack(side=tk.LEFT)
        self.reel_seconds = tk.StringVar(value="20")
        ttk.Entry(timing, textvariable=self.reel_seconds, width=5).pack(side=tk.LEFT, padx=(4, 8))
        ttk.Label(timing, text="Xem từ (1–60 giây)").pack(side=tk.LEFT)
        self.watch_seconds = tk.StringVar(value="20")
        ttk.Entry(timing, textvariable=self.watch_seconds, width=6).pack(side=tk.LEFT, padx=(4, 8))
        ttk.Label(timing, text="đến (giây)").pack(side=tk.LEFT)
        self.watch_until = tk.StringVar(value="60")
        ttk.Entry(timing, textvariable=self.watch_until, width=6).pack(side=tk.LEFT, padx=(4, 12))
        ttk.Label(timing, text="Nghỉ mỗi bài (giây)").pack(side=tk.LEFT)
        self.cooldown = tk.StringVar(value="30")
        ttk.Entry(timing, textvariable=self.cooldown, width=6).pack(side=tk.LEFT, padx=4)
        actions = ttk.Frame(form)
        actions.grid(row=10, column=0, columnspan=4, sticky="w", pady=4)
        ttk.Button(actions, text="Chia sẻ", command=self.start).pack(side=tk.LEFT)
        ttk.Button(actions, text="Dừng", command=self.stop).pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Tiếp tục", command=self.resume).pack(side=tk.LEFT)
        self.status = tk.StringVar(
            value="Chọn giờ trong khung 24 giờ của máy, rồi bấm Thêm vào list. Chia sẻ ngay thì vào list với chữ Ngay."
        )
        status_lbl = ttk.Label(form, textvariable=self.status, wraplength=360, justify=tk.LEFT)
        status_lbl.grid(row=11, column=0, columnspan=4, sticky="ew")

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

        cols = ("pick", "account", "page", "result", "url")
        self.tree = ttk.Treeview(listing, columns=cols, show="headings", selectmode="browse")
        for key, title, width, stretch in (
            ("pick", "Đăng", 52, False),
            ("account", "Tài khoản", 140, False),
            ("page", "Page", 200, True),
            ("result", "Kết quả", 280, True),
            ("url", "Link Page", 220, True),
        ):
            self.tree.heading(key, text=title)
            self.tree.column(key, width=width, stretch=stretch, anchor=tk.W)
        self._paint_result_tags(self.tree)
        self.tree.grid(row=1, column=0, sticky="nsew")
        self.tree.bind("<Button-1>", self._on_page_click)
        scroll = ttk.Scrollbar(listing, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        scroll.grid(row=1, column=1, sticky="ns")

        result = ttk.LabelFrame(parent, text="Kết quả từng page", padding=4)
        result.grid(row=2, column=0, sticky="nsew", padx=4, pady=4)
        result.columnconfigure(0, weight=1)
        result.rowconfigure(1, weight=1)
        self.outcome = tk.StringVar(value="Chưa chia sẻ. Cột Kết quả sẽ hiện Thành công hoặc Lỗi theo từng page.")
        outcome_lbl = ttk.Label(result, textvariable=self.outcome, wraplength=480, justify=tk.LEFT)
        outcome_lbl.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 4))

        def _fit_outcome(_event: object = None) -> None:
            width = int(result.winfo_width() or 0)
            if width > 80:
                outcome_lbl.configure(wraplength=max(200, width - 16))

        result.bind("<Configure>", _fit_outcome, add="+")
        result_cols = ("page", "status", "detail")
        self.results = ttk.Treeview(result, columns=result_cols, show="headings", height=6)
        for key, title, width in (
            ("page", "Page", 220),
            ("status", "Kết quả", 110),
            ("detail", "Chi tiết", 420),
        ):
            self.results.heading(key, text=title)
            self.results.column(key, width=width, stretch=True)
        self._paint_result_tags(self.results)
        self.results.grid(row=1, column=0, sticky="nsew")
        result_scroll = ttk.Scrollbar(result, orient=tk.VERTICAL, command=self.results.yview)
        self.results.configure(yscrollcommand=result_scroll.set)
        result_scroll.grid(row=1, column=1, sticky="ns")

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
        alive = {row["iid"] for row in rows}
        self._checked &= alive
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
        self._refresh_outcomes()
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
                values=self._page_values(row),
                tags=self._page_tags(row),
            )
        self._estimate()

    def _paint_result_tags(self, tree: ttk.Treeview) -> None:
        """Màu chữ: xanh là thành công, đỏ là lỗi."""
        tree.tag_configure("ok", foreground="#0b7a32")
        tree.tag_configure("fail", foreground="#b00020")
        tree.tag_configure("part", foreground="#8a5a00")
        tree.tag_configure("run", foreground="#0b5394")
        tree.tag_configure("wait", foreground="#555555")

    def _refresh_outcomes(self) -> None:
        """Gom job theo page. Lỗi xem video hiện lên mọi page của tài khoản đó chưa đăng được."""
        notes: dict[str, str] = {}
        wanted = set(self.batch_ids)
        if wanted:
            for batch in self.store.share_batches():
                if str(batch.get("id") or "") not in wanted:
                    continue
                note = str(batch.get("watch_note") or "").strip()
                if note:
                    notes[str(batch.get("account_id") or "")] = note
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for job in self._jobs():
            key = (str(job.get("account_id") or ""), str(job.get("page_id") or ""))
            grouped.setdefault(key, []).append(job)
        found: dict[tuple[str, str], tuple[str, str, str]] = {}
        for row in self.pages:
            key = (row["account_id"], row["page_id"])
            found[key] = describe_page_jobs(grouped.get(key, []), notes.get(row["account_id"], ""))
        self._outcomes = found

    def _page_values(self, row: dict[str, str]) -> tuple[str, str, str, str, str]:
        label, detail, _tag = self._outcomes.get((row["account_id"], row["page_id"]), ("", "", ""))
        shown = ""
        if label and detail:
            shown = f"{label} — {detail}"
        elif label:
            shown = label
        return (
            "☑" if row["iid"] in self._checked else "☐",
            row["account_name"],
            row["page_name"],
            shown,
            row["target_url"],
        )

    def _page_tags(self, row: dict[str, str]) -> tuple[str, ...]:
        tag = self._outcomes.get((row["account_id"], row["page_id"]), ("", "", ""))[2]
        return (tag,) if tag else ()

    def _on_page_click(self, event: tk.Event) -> str | None:
        """Bấm một dòng để tích hoặc bỏ page đó. Có thể tích nhiều page."""
        row = self.tree.identify_row(event.y)
        if not row:
            return None
        if row in self._checked:
            self._checked.discard(row)
        else:
            self._checked.add(row)
        values = list(self.tree.item(row, "values"))
        if values:
            values[0] = "☑" if row in self._checked else "☐"
            self.tree.item(row, values=values)
        self._estimate()
        return "break"

    def _selected_pages(self) -> list[dict[str, str]]:
        """Các page đang được tích. Một page hoặc nhiều page cùng lúc."""
        return [row for row in self.pages if row["iid"] in self._checked]

    def _select_all(self) -> None:
        self._checked.update(row["iid"] for row in self.pages if self._row_visible(row))
        self._fill_pages()

    def _clear_selection(self) -> None:
        visible = {row["iid"] for row in self.pages if self._row_visible(row)}
        self._checked.difference_update(visible)
        self._fill_pages()

    def _row_visible(self, row: dict[str, str]) -> bool:
        needle = self.filter_text.get().strip().casefold()
        if not needle:
            return True
        blob = " ".join((row["account_name"], row["page_name"], row["page_id"])).casefold()
        return needle in blob

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
        """Ô nghỉ để trống thì không nghỉ giữa các bài."""
        raw = self.cooldown.get().strip()
        if not raw:
            return 0
        try:
            seconds = int(raw)
        except ValueError:
            raise ValueError("Nghỉ giữa mỗi bài phải là số giây") from None
        if seconds < 0:
            raise ValueError("Nghỉ giữa mỗi bài phải từ 0 giây")
        return seconds

    def _clock_text(self) -> str:
        """Giờ và phút đang chọn trên dropdown, theo đồng hồ máy."""
        return compose_clock(self.clock_hour.get(), self.clock_minute.get())

    def _sync_hour_choices(self) -> None:
        """Xếp lại 24 giờ bắt đầu từ giờ máy hiện tại."""
        selected = self.clock_hour.get()
        choices = hour_choices()
        self.hour_box.configure(values=choices)
        if selected not in choices:
            self.clock_hour.set(choices[0])
        self.machine_clock.set(machine_clock_text())

    def _sync_clock_to_machine(self) -> None:
        """Đưa dropdown về đúng giờ và phút của máy."""
        moment = datetime.now()
        self._sync_hour_choices()
        self.clock_hour.set(f"{moment.hour:02d}")
        self.clock_minute.set(f"{moment.minute:02d}")

    def _add_scheduled(self) -> None:
        """Đưa link và giờ riêng vào list chờ."""
        self._enqueue(immediate=False)

    def _add_now(self) -> None:
        """Đưa link vào list chờ để chia sẻ ngay, không cần giờ."""
        self._enqueue(immediate=True)

    def _remember_queue_item(self, item: PlannedShareLink) -> None:
        """Gắn mã dòng rồi xếp theo giờ, link «Ngay» đứng trước giờ hẹn."""
        self._queue_seq += 1
        item.qid = f"q{self._queue_seq}"
        with self._queue_lock:
            self._queue.append(item)
            self._queue.sort(key=lambda row: row.when)

    def _enqueue(self, *, immediate: bool) -> None:
        try:
            item = make_queue_item(
                self.source_url.get(),
                self._clock_text(),
                immediate=immediate,
            )
        except ValueError as exc:
            messagebox.showwarning("List chờ", str(exc), parent=self.root)
            return
        self._remember_queue_item(item)
        self.source_url.set("")
        self._sync_clock_to_machine()
        self._refresh_queue()
        waiting = sum(1 for row in self._queue if row.state == "waiting")
        self.status.set(f"Đã thêm vào list chờ. Còn {waiting} link.")

    def _remove_queued(self) -> None:
        """Bỏ link đang chọn khỏi list chờ nếu chưa chạy."""
        picked = set(self.queue_tree.selection())
        if not picked:
            return
        with self._queue_lock:
            self._queue = [
                item
                for item in self._queue
                if item.qid not in picked or item.state == "running"
            ]
        self._refresh_queue()

    def _refresh_queue(self) -> None:
        """Vẽ lại list chờ: giờ, link và trạng thái."""
        labels = {
            "waiting": "Đang chờ",
            "running": "Đang chia sẻ",
            "done": "Xong",
            "partial": "Một phần",
            "error": "Lỗi",
            "skipped": "Bỏ qua",
        }
        with self._queue_lock:
            rows = list(self._queue)
        self.queue_tree.delete(*self.queue_tree.get_children())
        for item in rows:
            self.queue_tree.insert(
                "",
                tk.END,
                iid=item.qid or item.label,
                values=(item.label or item.when.strftime("%H:%M"), item.url, labels.get(item.state, item.state)),
            )

    def start(self) -> None:
        if self._running:
            self.status.set("Đang chia sẻ. Bấm Dừng nếu muốn ngắt.")
            return
        selected = self._selected_pages()
        if not selected:
            messagebox.showwarning("Chưa tích Page", "Tích một hoặc nhiều page cần đăng.", parent=self.root)
            return
        try:
            images = normalize_share_images(self.image_paths) if self.image_paths else []
            cooldown = self._cooldown_seconds()
            reel_seconds = clamp_reel_seconds(self.reel_seconds.get())
            watch_low, watch_high = optional_watch_bounds(self.watch_seconds.get(), self.watch_until.get())
            if self.source_url.get().strip():
                self._remember_queue_item(
                    make_queue_item(
                        self.source_url.get(),
                        self._clock_text(),
                        immediate=not self._clock_text().strip(),
                    )
                )
                self.source_url.set("")
                self._sync_clock_to_machine()
                self._refresh_queue()
        except ValueError as exc:
            messagebox.showwarning("Chia sẻ", str(exc), parent=self.root)
            return
        if not any(item.state == "waiting" for item in self._queue):
            messagebox.showwarning(
                "Thiếu link",
                "Thêm link vào list chờ, hoặc dán link rồi bấm Chia sẻ.",
                parent=self.root,
            )
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
        self.batch_ids = []
        self._cancel_schedule.clear()
        waiting = [item for item in self._queue if item.state == "waiting"]
        first = min(waiting, key=lambda item: item.when)
        self._running = True
        self.status.set(f"List chờ {len(waiting)} link. Link tới lượt lúc {first.label}.")

        def _set_status(text: str) -> None:
            schedule_on_main_thread(self.root, lambda text=text: self.status.set(text))

        def _paint_queue() -> None:
            schedule_on_main_thread(self.root, self._refresh_queue)

        def _next_waiting() -> PlannedShareLink | None:
            with self._queue_lock:
                pending = [item for item in self._queue if item.state == "waiting"]
            if not pending:
                return None
            return min(pending, key=lambda item: item.when)

        def _wait_turn(item: PlannedShareLink) -> bool:
            """Chờ đúng giờ của link này. Link mới sớm hơn thì nhường lượt."""
            noted_at = 0.0
            while True:
                if self._cancel_schedule.is_set() or item.state != "waiting":
                    return False
                sooner = _next_waiting()
                if sooner is not item:
                    return False
                left = (item.when - datetime.now()).total_seconds()
                if left <= 0:
                    with self._queue_lock:
                        if item not in self._queue or item.state != "waiting":
                            return False
                        item.state = "running"
                    return True
                now_s = time.monotonic()
                if now_s - noted_at >= 5:
                    noted_at = now_s
                    pending_left = sum(1 for row in self._queue if row.state == "waiting")
                    if left >= 60:
                        remain = f"còn khoảng {max(1, int(left // 60))} phút"
                    else:
                        remain = f"còn {max(1, int(left))} giây"
                    _set_status(f"Chờ {item.label} — {remain}. List còn {pending_left} link.")
                time.sleep(min(1.0, left))

        def _work() -> None:
            try:
                while True:
                    item = _next_waiting()
                    if item is None:
                        return
                    if not _wait_turn(item):
                        if self._cancel_schedule.is_set():
                            _set_status("Đã dừng. Link chưa tới giờ vẫn nằm trong list chờ.")
                            return
                        continue
                    item.state = "running"
                    _paint_queue()
                    pending_left = sum(1 for row in self._queue if row.state == "waiting")
                    _set_status(f"{item.label} — đang chia sẻ. Còn {pending_left} link trong list.")
                    result = self.engine.create_share_wave(
                        pages=selected,
                        source_url=item.url,
                        text=self.caption.get().strip(),
                        image_paths=images,
                        destination=place,
                        group_ids=group_ids,
                        cooldown_fixed=cooldown,
                        watch_seconds=watch_low,
                        watch_max_seconds=watch_high,
                        reel_seconds=reel_seconds,
                        comment=self._comment_raw(),
                    )
                    ids = list(result.get("batch_ids") or [])
                    self.batch_ids.extend(ids)
                    schedule_on_main_thread(self.root, self._fill_results)
                    if ids and not self._cancel_schedule.is_set():
                        self.engine.run_share_wave(ids)
                    jobs = [
                        str(row.get("status") or "")
                        for row in self.store.share_jobs()
                        if str(row.get("batch_id") or "") in set(ids)
                    ]
                    paused = any(
                        str(row.get("status") or "") == "PAUSED"
                        for row in self.store.share_batches()
                        if str(row.get("id") or "") in set(ids)
                    )
                    item.state = queue_state_from_results(
                        created=int(result.get("created") or 0),
                        job_statuses=jobs,
                        paused=paused,
                        cancelled=self._cancel_schedule.is_set(),
                    )
                    _paint_queue()
                    if self._cancel_schedule.is_set():
                        _set_status("Đã dừng. Link chưa tới giờ vẫn nằm trong list chờ.")
                        return
            except Exception as exc:  # noqa: BLE001
                for queued in self._queue:
                    if queued.state == "running":
                        queued.state = "waiting"
                _paint_queue()
                message = str(exc)
                _set_status(f"Lỗi chia sẻ: {message}")
            finally:
                self._running = False
                schedule_on_main_thread(self.root, self._fill_results)
                schedule_on_main_thread(self.root, lambda: self._finish_status())

        threading.Thread(target=_work, name="share-wave", daemon=True).start()

    def stop(self) -> None:
        self._cancel_schedule.set()
        for batch_id in list(self.batch_ids):
            try:
                self.engine.pause(batch_id)
            except Exception:  # noqa: BLE001
                continue
        self.status.set("Đã dừng lịch. Bài đang gửi sẽ dừng sau bước hiện tại.")

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
        if self._running or self._cancel_schedule.is_set():
            return
        self.status.set(self.outcome.get())

    def _on_progress(self, text: str) -> None:
        schedule_on_main_thread(self.root, lambda: self.status.set(text))
        schedule_on_main_thread(self.root, self._fill_results)

    def _jobs(self) -> list[dict[str, Any]]:
        wanted = set(self.batch_ids)
        if not wanted:
            return []
        return [row for row in self.store.share_jobs() if str(row.get("batch_id") or "") in wanted]

    def _fill_results(self) -> None:
        """Một dòng một page: Thành công hoặc Lỗi, kèm câu lỗi."""
        self._fill_pages()
        self.results.delete(*self.results.get_children())
        ok_names: list[str] = []
        bad_lines: list[str] = []
        for row in self.pages:
            label, detail, tag = self._outcomes.get((row["account_id"], row["page_id"]), ("", "", ""))
            if not label:
                continue
            self.results.insert(
                "",
                tk.END,
                values=(row["page_name"], label, detail),
                tags=(tag,) if tag else (),
            )
            if tag == "ok":
                ok_names.append(row["page_name"])
            elif tag in {"fail", "part"}:
                bad_lines.append(f"{row['page_name']}: {detail}")
        parts: list[str] = []
        if ok_names:
            parts.append(f"Thành công ({len(ok_names)}): {', '.join(ok_names)}")
        if bad_lines:
            parts.append(f"Lỗi ({len(bad_lines)}): {' · '.join(bad_lines)}")
        if parts:
            self.outcome.set(". ".join(parts))
        elif self.batch_ids:
            self.outcome.set("Đang chờ kết quả. Cột Kết quả cập nhật theo từng page.")
        else:
            self.outcome.set("Chưa chia sẻ. Cột Kết quả sẽ hiện Thành công hoặc Lỗi theo từng page.")


def build_share_tab(parent: ttk.Frame, root: tk.Misc) -> ShareTab:
    """Gắn tab chia sẻ vào notebook chính."""
    return ShareTab(parent, root)
