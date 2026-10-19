"""Tab chia sẻ một link lên nhiều Page, nhiều tài khoản. Không tải video."""

from __future__ import annotations

import random
import threading
import time
import tkinter as tk
from datetime import datetime, timedelta
from tkinter import filedialog, messagebox, ttk
from typing import Any, Iterable

from src.gui.ui_responsiveness import (
    ASYNC_PREP_MIN_ROWS,
    schedule_on_main_thread,
    tree_delete_all,
    tree_insert_chunked,
)
from src.services.facebook_groups.engine import GroupEngine, parse_group_uids
from src.services.facebook_groups.video_engage import clamp_reel_seconds, optional_watch_bounds
from src.services.facebook_groups.facebook_provider import FacebookGroupProvider
from src.services.facebook_groups.fingerprint import normalize_share_images
from src.services.facebook_groups.share_schedule import (
    PlannedShareLink,
    as_local_naive,
    compose_stamp,
    date_choice_is_today,
    date_choices,
    hour_choices,
    machine_clock_text,
    make_queue_item,
    minute_choices,
    queue_from_settings,
    queue_state_from_results,
    queue_to_settings,
)
from src.services.facebook_groups.source_watch import (
    SourceState,
    SourceWatcher,
    is_watch_source_active,
    merge_source_states,
    next_share_time,
    parse_minute_range,
    parse_source_urls,
    source_page_display,
    source_state_key,
    video_id_from_url,
)
from src.utils.feature_busy import (
    describe_conflict_gap,
    parse_conflict_gap_minutes,
    set_conflict_gap_auto,
    set_conflict_gap_minutes,
)
from src.services.facebook_groups.store import GroupStore
from src.gui.page_category_filter import PageCategoryFilter
from src.utils.page_categories import normalize_page_category, page_category_label
from src.utils.schedule_posts_filters import format_account_filter_label

def receiving_page_key(row: dict[str, str]) -> str:
    """
    Khóa nhận diện cùng một Page Facebook dù gắn nhiều tài khoản / nhiều bản ghi.

    Ưu tiên ``page_id`` (Meta), không có thì dùng ``target_url``.
    """
    page_id = str(row.get("page_id") or "").strip()
    if page_id:
        return f"id:{page_id}"
    url = str(row.get("target_url") or "").strip().rstrip("/").casefold()
    if url:
        return f"url:{url}"
    return f"iid:{str(row.get('iid') or '').strip()}"


def dedupe_receiving_pages(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Giữ đúng một dòng đầu tiên cho mỗi Page Facebook trùng."""
    seen: set[str] = set()
    unique: list[dict[str, str]] = []
    for row in rows:
        key = receiving_page_key(row)
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def interleave_pages_by_account(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    Xếp Page luân phiên theo tài khoản (A1, B1, A2, B2…).

    Khi chọn «Tất cả tài khoản», bảng không còn toàn một TK ở đầu —
    dễ thấy cả hai đang chạy song song.
    """
    buckets: dict[str, list[dict[str, str]]] = {}
    order: list[str] = []
    for row in rows:
        aid = str(row.get("account_id") or "")
        if aid not in buckets:
            order.append(aid)
            buckets[aid] = []
        buckets[aid].append(row)
    if len(order) <= 1:
        return list(rows)
    queues = [list(buckets[aid]) for aid in order]
    out: list[dict[str, str]] = []
    while any(queues):
        for queue in queues:
            if queue:
                out.append(queue.pop(0))
    return out


def describe_page_jobs(
    jobs: list[dict[str, Any]],
    paused_note: str = "",
    live_step: str = "",
) -> tuple[str, str, str]:
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
        return "Đang gửi", live_step or "Đang chia sẻ link", "run"
    if pending and live_step:
        # Cùng TK đang xử lý Page khác → vẫn hiện rõ, không để cột trống.
        if live_step.startswith("Chờ trong TK"):
            return "Chờ TK", live_step, "wait"
        return "Đang chạy", live_step, "run"
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
        # Video user đã xóa khỏi hàng chờ — không xếp lại / không chia sẻ lại trong phiên.
        self._cancelled_video_ids: set[str] = set()
        self._settings_save_id: str | None = None
        self._results_after: str | None = None
        self._queue_refresh_after: str | None = None
        self._pages_tree_gen = 0
        self._queue_tree_gen = 0
        self._loading_settings = False
        self._watcher: SourceWatcher | None = None
        self._watch_enabled = False
        self._watch_targets: list[str] = []
        self._watch_states: dict[str, SourceState] = {}
        self._watch_last_planned: datetime | None = None
        # Dấu vân tay plan chờ (share_now + gap) — đổi → xếp lại giờ waiting.
        self._live_plan_key: tuple[Any, ...] | None = None
        self._build()
        self._load_share_settings()
        self.reload_pages()
        self._fill_results()
        try:
            recovered = self.engine.recover()
            if int(recovered.get("share_batches") or 0) or int(recovered.get("share") or 0):
                self.status.set(
                    f"Khôi phục chia sẻ: {recovered.get('share_batches', 0)} batch dở, "
                    f"{recovered.get('share', 0)} job — sẽ chạy tiếp."
                )
        except Exception:  # noqa: BLE001
            pass
        # Watch sau hàng chờ — tránh mở Firefox quét trùng lúc resume chia sẻ.
        self.root.after(5500, self._resume_queue_if_needed)
        self.root.after(12000, self._watch_resume_saved)

    def _build(self) -> None:
        """
        Bố cục ưu tiên luồng tự động:

        1. Theo dõi Page nguồn → tự chia sẻ (luôn hiện trên cùng)
        2. Bảng Page nhận (vùng chính)
        3. Tab dưới: Hàng chờ | Kết quả | Thủ công | Cài đặt
        """
        parent = self.parent
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=3)
        parent.rowconfigure(2, weight=1)

        self.status = tk.StringVar(
            value="① Dán Page nguồn → ② Tích Page nhận → ③ Bật theo dõi. Video cũ bỏ qua; video mới tự chia sẻ."
        )
        self.outcome = tk.StringVar(value="Chưa chạy. Kết quả từng Page hiện ở tab «Kết quả».")
        self.estimate = tk.StringVar(value="")
        self.watch_status = tk.StringVar(value="Chưa bật theo dõi.")
        self.watch_stats = tk.StringVar(value="Thống kê: 0 Page nguồn · 0 Page nhận")
        self.watch_share_now = tk.BooleanVar(value=True)
        self.watch_scan_min = tk.StringVar(value="5")
        self.watch_scan_max = tk.StringVar(value="12")
        self.watch_gap_min = tk.StringVar(value="0")
        self.watch_gap_max = tk.StringVar(value="3")
        self.conflict_gap_min = tk.StringVar(value="2")
        self.conflict_gap_max = tk.StringVar(value="5")
        self.conflict_gap_auto = tk.BooleanVar(value=True)
        self.source_url = tk.StringVar()
        self.caption = tk.StringVar()
        self.destination = tk.StringVar(value="page")
        self.image_label = tk.StringVar(value="Chưa chọn ảnh")
        self.reel_seconds = tk.StringVar(value="20")
        self.watch_seconds = tk.StringVar(value="20")
        self.watch_until = tk.StringVar(value="60")
        self.like_video = tk.BooleanVar(value=True)
        self.engage_rate_percent = tk.StringVar(value="65")
        self.cooldown = tk.StringVar(value="30")
        self.clock_date = tk.StringVar()
        self.clock_hour = tk.StringVar()
        self.clock_minute = tk.StringVar(value="00")
        self.machine_clock = tk.StringVar()
        self.group_uid_count = tk.StringVar(value="Mỗi dòng một UID, hoặc cách nhau bằng dấu phẩy.")
        self.filter_account = tk.StringVar(value="Chọn tài khoản")
        self.filter_text = tk.StringVar()
        self._account_chosen = False
        self._account_labels: dict[str, str] = {}

        self._build_auto_panel(parent)
        self._build_pages_panel(parent)
        self._build_bottom_notebook(parent)
        self._sync_clock_to_machine()
        self._sync_gap_widgets()
        self._sync_conflict_gap_widgets()
        self._refresh_conflict_gap_hint()
        self._refresh_watch_stats()

    def _build_auto_panel(self, parent: ttk.Frame) -> None:
        """Khối chính: theo dõi Page nguồn và tự chia sẻ video mới."""
        box = ttk.LabelFrame(parent, text="1. Tự chia sẻ video mới từ Page nguồn", padding=8)
        box.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 2))
        box.columnconfigure(1, weight=1)
        self._watch_panel = box

        ttk.Label(
            box,
            text=(
                "Dán link Page nguồn (mỗi dòng một). Bật theo dõi / Quét ngay lần đầu "
                "ghi nhớ video mới nhất trên Page — lần sau chỉ lấy video đăng sau mốc đó."
            ),
            foreground="gray",
        ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 4))

        ttk.Label(box, text="Page nguồn").grid(row=1, column=0, sticky="nw", padx=(0, 8))
        self.watch_sources_box = tk.Text(box, height=3, wrap="none", font=("Consolas", 9))
        self.watch_sources_box.grid(row=1, column=1, columnspan=2, sticky="ew")
        self.watch_sources_box.bind(
            "<KeyRelease>",
            lambda _event: (
                self._refresh_watch_stats(),
                self._sync_watcher_sources_from_box(),
                self._schedule_save_settings(),
            ),
        )

        stats_row = ttk.Frame(box)
        stats_row.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(6, 0))
        ttk.Label(
            stats_row,
            textvariable=self.watch_stats,
            foreground="#0b5394",
            font=("Segoe UI", 9, "bold"),
        ).pack(side=tk.LEFT)

        opts = ttk.Frame(box)
        opts.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(6, 0))
        self.watch_share_now_cb = ttk.Checkbutton(
            opts,
            text="Có video mới → chia sẻ ngay",
            variable=self.watch_share_now,
            command=lambda: (self._sync_gap_widgets(), self._schedule_save_settings()),
        )
        self.watch_share_now_cb.pack(side=tk.LEFT, padx=(0, 12))
        ttk.Label(opts, text="Quét lại mỗi").pack(side=tk.LEFT)
        ttk.Entry(opts, textvariable=self.watch_scan_min, width=4).pack(side=tk.LEFT, padx=(4, 2))
        ttk.Label(opts, text="–").pack(side=tk.LEFT)
        ttk.Entry(opts, textvariable=self.watch_scan_max, width=4).pack(side=tk.LEFT, padx=(2, 2))
        ttk.Label(opts, text="phút").pack(side=tk.LEFT, padx=(0, 10))
        self._gap_frame = ttk.Frame(opts)
        self._gap_frame.pack(side=tk.LEFT)
        ttk.Label(self._gap_frame, text="Cách mỗi lượt").pack(side=tk.LEFT)
        ttk.Entry(self._gap_frame, textvariable=self.watch_gap_min, width=4).pack(side=tk.LEFT, padx=(4, 2))
        ttk.Label(self._gap_frame, text="–").pack(side=tk.LEFT)
        ttk.Entry(self._gap_frame, textvariable=self.watch_gap_max, width=4).pack(side=tk.LEFT, padx=(2, 2))
        ttk.Label(self._gap_frame, text="phút").pack(side=tk.LEFT, padx=(0, 8))
        for var in (self.watch_scan_min, self.watch_scan_max, self.watch_gap_min, self.watch_gap_max):
            var.trace_add("write", lambda *_args: self._schedule_save_settings())

        gap_box = ttk.LabelFrame(
            box,
            text="Giãn cách đăng lịch → chia sẻ",
            padding=(8, 4),
        )
        gap_box.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(8, 0))
        gap_row = ttk.Frame(gap_box)
        gap_row.pack(side=tk.LEFT, fill=tk.X)
        self.conflict_gap_auto_cb = ttk.Checkbutton(
            gap_row,
            text="Tự động theo tải lịch",
            variable=self.conflict_gap_auto,
            command=self._on_conflict_gap_auto_toggled,
        )
        self.conflict_gap_auto_cb.pack(side=tk.LEFT, padx=(0, 10))
        ttk.Label(gap_row, text="Hoặc chỉnh tay").pack(side=tk.LEFT)
        self._conflict_gap_min_entry = ttk.Entry(gap_row, textvariable=self.conflict_gap_min, width=4)
        self._conflict_gap_min_entry.pack(side=tk.LEFT, padx=(6, 2))
        ttk.Label(gap_row, text="–").pack(side=tk.LEFT)
        self._conflict_gap_max_entry = ttk.Entry(gap_row, textvariable=self.conflict_gap_max, width=4)
        self._conflict_gap_max_entry.pack(side=tk.LEFT, padx=(2, 2))
        ttk.Label(gap_row, text="phút").pack(side=tk.LEFT, padx=(4, 0))
        self.conflict_gap_hint = tk.StringVar(value="")
        ttk.Label(gap_box, textvariable=self.conflict_gap_hint, foreground="#0b5394").pack(
            side=tk.RIGHT, padx=(8, 0)
        )
        for var in (self.conflict_gap_min, self.conflict_gap_max):
            var.trace_add("write", lambda *_args: self._on_conflict_gap_changed())
        self._sync_conflict_gap_widgets()

        actions = ttk.Frame(box)
        actions.grid(row=5, column=0, columnspan=3, sticky="ew", pady=(8, 0))
        self.watch_toggle_btn = ttk.Button(actions, text="Bật theo dõi", command=self._watch_toggle)
        self.watch_toggle_btn.pack(side=tk.LEFT)
        ttk.Button(actions, text="Quét ngay", command=self._watch_scan_now).pack(side=tk.LEFT, padx=6)
        ttk.Label(actions, textvariable=self.watch_status, foreground="#0b5394").pack(side=tk.LEFT, padx=8)
        status_lbl = ttk.Label(actions, textvariable=self.status, foreground="#57606a", wraplength=420)
        status_lbl.pack(side=tk.RIGHT, padx=(8, 0))

        def _fit(_event: object = None) -> None:
            width = int(box.winfo_width() or 0)
            if width > 200:
                status_lbl.configure(wraplength=max(180, width // 3))

        box.bind("<Configure>", _fit, add="+")

    def _build_pages_panel(self, parent: ttk.Frame) -> None:
        """Bảng Page nhận — vùng làm việc chính."""
        listing = ttk.LabelFrame(parent, text="2. Page nhận (tích các Page sẽ đăng video mới)", padding=4)
        listing.grid(row=1, column=0, sticky="nsew", padx=4, pady=2)
        listing.columnconfigure(0, weight=1)
        listing.rowconfigure(1, weight=1)

        bar = ttk.Frame(listing)
        bar.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        ttk.Button(bar, text="Chạy list chờ", command=self.start).pack(side=tk.LEFT)
        ttk.Button(bar, text="Dừng", command=self.stop).pack(side=tk.LEFT, padx=4)
        ttk.Button(bar, text="Tiếp tục", command=self.resume).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Label(bar, text="Tài khoản").pack(side=tk.LEFT)
        self.filter_account_box = ttk.Combobox(
            bar, textvariable=self.filter_account, width=36, state="readonly"
        )
        self.filter_account_box.pack(side=tk.LEFT, padx=(4, 8))
        self.filter_account_box.bind("<<ComboboxSelected>>", self._on_account_filter)
        ttk.Label(bar, text="Tìm").pack(side=tk.LEFT)
        self.filter_text.trace_add("write", lambda *_args: self._fill_pages())
        ttk.Entry(bar, textvariable=self.filter_text, width=18).pack(side=tk.LEFT, padx=4)
        self.category_filter = PageCategoryFilter(bar, on_change=self._fill_pages, width=16)
        self.category_filter.frame.pack(side=tk.LEFT, padx=(4, 4))
        ttk.Button(bar, text="Chọn tất cả", command=self._select_all).pack(side=tk.LEFT, padx=4)
        ttk.Button(bar, text="Bỏ chọn", command=self._clear_selection).pack(side=tk.LEFT)
        ttk.Button(bar, text="Làm mới", command=self.reload_pages).pack(side=tk.LEFT, padx=4)
        ttk.Label(bar, textvariable=self.estimate).pack(side=tk.LEFT, padx=8)

        cols = ("stt", "pick", "account", "page", "category", "result", "url")
        self.tree = ttk.Treeview(listing, columns=cols, show="headings", selectmode="browse")
        for key, title, width, stretch in (
            ("stt", "STT", 40, False),
            ("pick", "Nhận", 48, False),
            ("account", "Tài khoản", 120, False),
            ("page", "Page", 200, True),
            ("category", "Phân loại", 90, False),
            ("result", "Kết quả", 260, True),
            ("url", "Link Page", 180, True),
        ):
            self.tree.heading(key, text=title)
            self.tree.column(key, width=width, stretch=stretch, anchor=tk.W)
        self._paint_result_tags(self.tree)
        self.tree.grid(row=1, column=0, sticky="nsew")
        self.tree.bind("<Button-1>", self._on_page_click)
        scroll = ttk.Scrollbar(listing, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        scroll.grid(row=1, column=1, sticky="ns")

    def _build_bottom_notebook(self, parent: ttk.Frame) -> None:
        """Tab phụ: hàng chờ, kết quả, chia sẻ tay, cài đặt nội dung."""
        nb = ttk.Notebook(parent)
        nb.grid(row=2, column=0, sticky="nsew", padx=4, pady=(2, 4))
        self._share_nb = nb

        tab_queue = ttk.Frame(nb, padding=6)
        tab_result = ttk.Frame(nb, padding=6)
        tab_manual = ttk.Frame(nb, padding=6)
        tab_settings = ttk.Frame(nb, padding=6)
        nb.add(tab_queue, text="  Hàng chờ  ")
        nb.add(tab_result, text="  Kết quả  ")
        nb.add(tab_manual, text="  Chia sẻ tay  ")
        nb.add(tab_settings, text="  Nội dung & cài đặt  ")

        tab_queue.columnconfigure(0, weight=1)
        tab_queue.rowconfigure(1, weight=1)
        ttk.Label(
            tab_queue,
            text=(
                "Video mới từ theo dõi và link thêm tay. Cột «Page nguồn» cho biết video thuộc Page nào. "
                "Chọn dòng → Ctrl+C / chuột phải «Copy link» để chép link video. "
                "«Chạy list chờ» / theo dõi bật sẽ tự xử lý."
            ),
            foreground="gray",
        ).grid(row=0, column=0, sticky="w", pady=(0, 4))
        self.queue_tree = ttk.Treeview(
            tab_queue,
            columns=("stt", "when", "source", "url", "state"),
            show="headings",
            height=5,
            selectmode="extended",
        )
        for key, title, width in (
            ("stt", "STT", 44),
            ("when", "Ngày giờ", 110),
            ("source", "Page nguồn", 220),
            ("url", "Link video", 320),
            ("state", "Trạng thái", 100),
        ):
            self.queue_tree.heading(key, text=title)
            self.queue_tree.column(key, width=width, stretch=(key in {"url", "source"}))
        self.queue_tree.grid(row=1, column=0, sticky="nsew")
        q_scroll = ttk.Scrollbar(tab_queue, orient=tk.VERTICAL, command=self.queue_tree.yview)
        self.queue_tree.configure(yscrollcommand=q_scroll.set)
        q_scroll.grid(row=1, column=1, sticky="ns")
        q_actions = ttk.Frame(tab_queue)
        q_actions.grid(row=2, column=0, sticky="w", pady=(4, 0))
        ttk.Button(q_actions, text="Copy link đã chọn", command=self._copy_queued_video_links).pack(
            side=tk.LEFT
        )
        ttk.Button(q_actions, text="Xóa / bỏ qua đã chọn", command=self._remove_queued).pack(
            side=tk.LEFT, padx=(8, 0)
        )
        ttk.Label(
            q_actions,
            text="Ctrl+C / chuột phải = copy link · Delete = bỏ qua.",
            foreground="gray",
        ).pack(side=tk.LEFT, padx=(8, 0))
        self.queue_tree.bind("<Delete>", lambda _e: self._remove_queued())
        self.queue_tree.bind("<Control-c>", lambda _e: self._copy_queued_video_links())
        self.queue_tree.bind("<Control-C>", lambda _e: self._copy_queued_video_links())
        self.queue_tree.bind("<Double-1>", self._on_queue_double_click_copy)
        try:
            from src.gui.treeview_shortcuts import install_treeview_shortcuts

            install_treeview_shortcuts(
                self.queue_tree,
                owner=self.root,
                info_callback=lambda msg: self.status.set(msg),
                extra_context_items=lambda menu: menu.add_command(
                    label="Copy link video đã chọn",
                    command=self._copy_queued_video_links,
                ),
            )
        except Exception:
            pass

        tab_result.columnconfigure(0, weight=1)
        tab_result.rowconfigure(1, weight=1)
        outcome_lbl = ttk.Label(tab_result, textvariable=self.outcome, wraplength=720, justify=tk.LEFT)
        outcome_lbl.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        result_cols = ("stt", "page", "status", "comment", "detail")
        self.results = ttk.Treeview(tab_result, columns=result_cols, show="headings", height=6)
        for key, title, width in (
            ("stt", "STT", 44),
            ("page", "Page", 180),
            ("status", "Kết quả", 100),
            ("comment", "Bình luận", 180),
            ("detail", "Chi tiết", 360),
        ):
            self.results.heading(key, text=title)
            self.results.column(key, width=width, stretch=True)
        self._paint_result_tags(self.results)
        self.results.grid(row=1, column=0, sticky="nsew")
        result_scroll = ttk.Scrollbar(tab_result, orient=tk.VERTICAL, command=self.results.yview)
        self.results.configure(yscrollcommand=result_scroll.set)
        result_scroll.grid(row=1, column=1, sticky="ns")

        def _fit_outcome(_event: object = None) -> None:
            width = int(tab_result.winfo_width() or 0)
            if width > 80:
                outcome_lbl.configure(wraplength=max(200, width - 24))

        tab_result.bind("<Configure>", _fit_outcome, add="+")

        tab_manual.columnconfigure(1, weight=1)
        ttk.Label(
            tab_manual,
            text="Dán một link cụ thể vào hàng chờ (không qua theo dõi).",
            foreground="gray",
        ).grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 6))
        ttk.Label(tab_manual, text="Link bài").grid(row=1, column=0, sticky="w")
        ttk.Entry(tab_manual, textvariable=self.source_url).grid(row=1, column=1, sticky="ew", padx=4)
        clock = ttk.Frame(tab_manual)
        clock.grid(row=1, column=2, columnspan=2, sticky="w")
        ttk.Label(clock, text="Ngày").pack(side=tk.LEFT)
        self.date_box = ttk.Combobox(
            clock, textvariable=self.clock_date, width=14, state="readonly", values=date_choices()
        )
        self.date_box.pack(side=tk.LEFT, padx=4)
        self.date_box.bind("<<ComboboxSelected>>", lambda _event: self._sync_hour_choices())
        ttk.Label(clock, text="Giờ").pack(side=tk.LEFT)
        self.hour_box = ttk.Combobox(
            clock, textvariable=self.clock_hour, width=4, state="readonly", values=hour_choices()
        )
        self.hour_box.pack(side=tk.LEFT, padx=2)
        self.hour_box.bind("<Button-1>", lambda _event: self._sync_hour_choices())
        ttk.Label(clock, text=":").pack(side=tk.LEFT)
        ttk.Combobox(
            clock, textvariable=self.clock_minute, width=4, state="readonly", values=minute_choices()
        ).pack(side=tk.LEFT)
        ttk.Label(clock, textvariable=self.machine_clock).pack(side=tk.LEFT, padx=(8, 0))
        add_row = ttk.Frame(tab_manual)
        add_row.grid(row=2, column=1, columnspan=3, sticky="w", pady=8)
        ttk.Button(add_row, text="Thêm vào hàng chờ", command=self._add_scheduled).pack(side=tk.LEFT)
        ttk.Button(add_row, text="Chia sẻ ngay", command=self._add_now).pack(side=tk.LEFT, padx=6)
        ttk.Button(add_row, text="Chạy list chờ", command=self.start).pack(side=tk.LEFT)

        tab_settings.columnconfigure(1, weight=1)
        ttk.Label(tab_settings, text="Caption").grid(row=0, column=0, sticky="w")
        ttk.Entry(tab_settings, textvariable=self.caption).grid(row=0, column=1, columnspan=3, sticky="ew", pady=2)
        self.caption.trace_add("write", lambda *_args: self._schedule_save_settings())
        ttk.Label(tab_settings, text="Bình luận").grid(row=1, column=0, sticky="nw")
        comment_fr = ttk.Frame(tab_settings)
        comment_fr.grid(row=1, column=1, columnspan=3, sticky="ew", pady=2)
        comment_fr.columnconfigure(0, weight=1)
        self.comment_box = tk.Text(comment_fr, height=3, wrap="word", font=("Segoe UI", 9))
        self.comment_box.grid(row=0, column=0, sticky="ew")
        ttk.Label(
            comment_fr,
            text="Mỗi dòng một câu — mỗi Page bốc một câu (không trùng nếu còn).",
            foreground="gray",
        ).grid(row=1, column=0, sticky="w")
        self.comment_box.bind("<KeyRelease>", lambda _event: self._schedule_save_settings())
        ttk.Label(tab_settings, text="Ảnh").grid(row=2, column=0, sticky="w")
        images = ttk.Frame(tab_settings)
        images.grid(row=2, column=1, sticky="w", pady=2)
        ttk.Button(images, text="Chọn ảnh", command=self._pick_images).pack(side=tk.LEFT)
        ttk.Label(images, textvariable=self.image_label).pack(side=tk.LEFT, padx=6)
        dest = ttk.Frame(tab_settings)
        dest.grid(row=3, column=1, columnspan=3, sticky="w", pady=4)
        for text, value in (
            ("Lên Page của tôi", "page"),
            ("Vào nhóm đã tham gia", "joined"),
            ("Cả Page và nhóm", "both"),
        ):
            ttk.Radiobutton(
                dest,
                text=text,
                variable=self.destination,
                value=value,
                command=lambda: (self._estimate(), self._schedule_save_settings()),
            ).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Label(tab_settings, text="UID nhóm").grid(row=4, column=0, sticky="nw")
        self.group_uid_box = tk.Text(tab_settings, height=2, wrap="word", font=("Consolas", 9))
        self.group_uid_box.grid(row=4, column=1, columnspan=3, sticky="ew", pady=2)
        self.group_uid_box.bind("<KeyRelease>", lambda _event: (self._estimate(), self._schedule_save_settings()))
        ttk.Label(tab_settings, textvariable=self.group_uid_count).grid(row=5, column=1, sticky="w")
        timing = ttk.Frame(tab_settings)
        timing.grid(row=6, column=0, columnspan=4, sticky="w", pady=(8, 0))
        ttk.Label(timing, text="Lướt Reel").pack(side=tk.LEFT)
        ttk.Entry(timing, textvariable=self.reel_seconds, width=5).pack(side=tk.LEFT, padx=(4, 10))
        ttk.Label(timing, text="Xem từ").pack(side=tk.LEFT)
        ttk.Entry(timing, textvariable=self.watch_seconds, width=5).pack(side=tk.LEFT, padx=4)
        ttk.Label(timing, text="đến").pack(side=tk.LEFT)
        ttk.Entry(timing, textvariable=self.watch_until, width=5).pack(side=tk.LEFT, padx=(4, 10))
        ttk.Checkbutton(
            timing,
            text="Cho phép Like",
            variable=self.like_video,
            command=self._schedule_save_settings,
        ).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Label(timing, text="Tỷ lệ Like/comment ≥").pack(side=tk.LEFT)
        ttk.Entry(timing, textvariable=self.engage_rate_percent, width=4).pack(side=tk.LEFT, padx=(2, 0))
        ttk.Label(timing, text="%").pack(side=tk.LEFT, padx=(0, 10))
        ttk.Label(timing, text="Nghỉ (giây)").pack(side=tk.LEFT)
        ttk.Entry(timing, textvariable=self.cooldown, width=5).pack(side=tk.LEFT, padx=4)
        for var in (
            self.reel_seconds,
            self.watch_seconds,
            self.watch_until,
            self.cooldown,
            self.engage_rate_percent,
        ):
            var.trace_add("write", lambda *_args: self._schedule_save_settings())
        conflict = ttk.Frame(tab_settings)
        conflict.grid(row=7, column=0, columnspan=4, sticky="w", pady=(10, 0))
        ttk.Checkbutton(
            conflict,
            text="Tự động giãn sau đăng lịch",
            variable=self.conflict_gap_auto,
            command=self._on_conflict_gap_auto_toggled,
        ).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Label(conflict, text="Tay").pack(side=tk.LEFT)
        self._conflict_gap_min_entry_s = ttk.Entry(conflict, textvariable=self.conflict_gap_min, width=4)
        self._conflict_gap_min_entry_s.pack(side=tk.LEFT, padx=(6, 2))
        ttk.Label(conflict, text="–").pack(side=tk.LEFT)
        self._conflict_gap_max_entry_s = ttk.Entry(conflict, textvariable=self.conflict_gap_max, width=4)
        self._conflict_gap_max_entry_s.pack(side=tk.LEFT, padx=(2, 2))
        ttk.Label(
            conflict,
            text="phút (TK khác vẫn chạy; ưu tiên đăng lịch)",
            foreground="gray",
        ).pack(side=tk.LEFT, padx=(6, 0))

    def _on_conflict_gap_auto_toggled(self) -> None:
        """Bật/tắt giãn tự động — đồng bộ FeatureBusy + UI."""
        set_conflict_gap_auto(bool(self.conflict_gap_auto.get()))
        self._sync_conflict_gap_widgets()
        self._refresh_conflict_gap_hint()
        self._schedule_save_settings()

    def _sync_conflict_gap_widgets(self) -> None:
        """Khóa ô chỉnh tay khi đang dùng chế độ tự động."""
        auto = bool(self.conflict_gap_auto.get())
        state = tk.DISABLED if auto else tk.NORMAL
        for entry in (
            getattr(self, "_conflict_gap_min_entry", None),
            getattr(self, "_conflict_gap_max_entry", None),
            getattr(self, "_conflict_gap_min_entry_s", None),
            getattr(self, "_conflict_gap_max_entry_s", None),
        ):
            if entry is not None:
                try:
                    entry.configure(state=state)
                except tk.TclError:
                    pass

    def _on_conflict_gap_changed(self) -> None:
        """Cập nhật gợi ý + lưu khi người dùng đổi khoảng giãn cách."""
        if not self.conflict_gap_auto.get():
            try:
                parse_conflict_gap_minutes(self.conflict_gap_min.get(), self.conflict_gap_max.get())
            except Exception:
                pass
        self._refresh_conflict_gap_hint()
        self._schedule_save_settings()

    def _refresh_conflict_gap_hint(self) -> None:
        """Hiện khoảng đang dùng (auto theo tải hoặc chỉnh tay)."""
        if not hasattr(self, "conflict_gap_hint"):
            return
        set_conflict_gap_auto(bool(self.conflict_gap_auto.get()))
        if not self.conflict_gap_auto.get():
            try:
                parse_conflict_gap_minutes(self.conflict_gap_min.get(), self.conflict_gap_max.get())
            except Exception:
                self.conflict_gap_hint.set("Nhập số phút hợp lệ")
                return
        self.conflict_gap_hint.set(describe_conflict_gap())

    def _sync_gap_widgets(self) -> None:
        """Ẩn khoảng cách lượt khi đang chọn «chia sẻ ngay»."""
        if not hasattr(self, "_gap_frame"):
            return
        if self.watch_share_now.get():
            self._gap_frame.pack_forget()
        else:
            self._gap_frame.pack(side=tk.LEFT)

    def _watch_set_status(self, text: str) -> None:
        """Cập nhật dòng trạng thái theo dõi (gọi được từ luồng nền)."""
        schedule_on_main_thread(self.root, lambda text=text: self.watch_status.set(text))

    def _watch_toggle(self) -> None:
        """Bật hoặc tắt theo dõi Page nguồn."""
        if self._watcher is not None and self._watcher.running:
            self._watch_disable()
        else:
            # Tắt→bật lại: quét sớm + chạy list chờ ngay (không chờ vài phút).
            self._cancel_schedule.clear()
            try:
                self.engine.recover()
            except Exception:  # noqa: BLE001
                pass
            self._watch_enable(interactive=True, delay_first_scan=False)

    def _sync_watcher_sources_from_box(self) -> None:
        """Đang theo dõi: cập nhật list Page nguồn từ ô dán (tránh watcher còn list cũ/rỗng)."""
        if self._loading_settings or self._watcher is None or not self._watcher.running:
            return
        sources = self._safe_parse_sources()
        if not sources:
            return
        try:
            scan_min, scan_max = parse_minute_range(
                self.watch_scan_min.get(), self.watch_scan_max.get(), label="Quét lại mỗi", floor=1
            )
        except ValueError:
            return
        self._watch_states = merge_source_states(self._watch_states, sources)
        self._watcher.update_settings(sources=sources, scan_min=scan_min, scan_max=scan_max)

    def _watch_scan_now(self) -> None:
        """
        Quét Page nguồn ngay: ghi mốc video mới nhất (lần đầu) hoặc phát hiện video mới.

        Chưa bật theo dõi thì tự bật (cần đã tích Page nhận), rồi quét.
        """
        sources = self._safe_parse_sources()
        if not sources:
            self.watch_status.set("Dán ít nhất một link Page nguồn rồi bấm Quét ngay.")
            messagebox.showwarning(
                "Quét Page nguồn",
                "Dán link Page nguồn (mỗi dòng một link) trước khi quét.",
                parent=self.root,
            )
            return
        if self._watcher is None or not self._watcher.running:
            self.watch_status.set("Đang bật theo dõi để ghi mốc video cuối trên Page nguồn…")
            if not self._watch_enable(interactive=True, delay_first_scan=False):
                return
            # Bật xong đã quét ngay (scan_immediately=True) — ghi mốc / lấy mới.
            return
        try:
            scan_min, scan_max = parse_minute_range(
                self.watch_scan_min.get(), self.watch_scan_max.get(), label="Quét lại mỗi", floor=1
            )
        except ValueError as exc:
            self.watch_status.set(str(exc))
            return
        # Đồng bộ list nguồn trên ô dán → watcher (tránh quét list cũ / rỗng).
        self._watch_states = merge_source_states(self._watch_states, sources)
        self._watcher.update_settings(sources=sources, scan_min=scan_min, scan_max=scan_max)
        self.watch_status.set(f"Đang quét {len(sources)} Page nguồn — ghi mốc / tìm video mới…")
        self._watcher.scan_now()

    def _watch_enable(self, *, interactive: bool, delay_first_scan: bool = False) -> bool:
        """Kiểm tra cài đặt, chốt Page nhận rồi chạy luồng theo dõi."""

        def _warn(text: str) -> bool:
            if interactive:
                messagebox.showwarning("Theo dõi Page nguồn", text, parent=self.root)
            self.watch_status.set(text)
            return False

        try:
            sources = parse_source_urls(self.watch_sources_box.get("1.0", tk.END))
            scan_min, scan_max = parse_minute_range(
                self.watch_scan_min.get(), self.watch_scan_max.get(), label="Quét lại mỗi", floor=1
            )
            if not self.watch_share_now.get():
                parse_minute_range(
                    self.watch_gap_min.get(), self.watch_gap_max.get(), label="Cách nhau mỗi lượt", floor=0
                )
        except ValueError as exc:
            return _warn(str(exc))
        if not sources:
            return _warn("Dán ít nhất một link Page nguồn.")
        if interactive:
            targets = [row["iid"] for row in self._selected_pages()]
            if not targets:
                return _warn("Tích cột «Nhận» ở bảng Page nhận trước khi bật theo dõi.")
            self._watch_targets = targets
        target_rows = dedupe_receiving_pages(
            [row for row in self.pages if row["iid"] in set(self._watch_targets)]
        )
        self._watch_targets = [row["iid"] for row in target_rows]
        self._checked = set(self._watch_targets)
        if not target_rows:
            return _warn("Không còn Page nhận nào. Tích Page rồi bấm Bật theo dõi.")
        try:
            from src.utils.db_manager import AccountsDatabaseManager

            account = AccountsDatabaseManager().get_by_id(target_rows[0]["account_id"])
        except Exception:  # noqa: BLE001
            account = None
        if not account:
            return _warn("Không tìm thấy tài khoản của Page nhận để quét Page nguồn.")
        if self._watcher is not None:
            try:
                self._watcher.stop()
            except Exception:
                pass
            self._watcher = None
        # Gộp mốc theo khóa chuẩn trước khi giao cho watcher (tránh ghi mốc lại từ đầu).
        self._watch_states = merge_source_states(self._watch_states, sources)
        self._watcher = SourceWatcher(
            sources=sources,
            account=dict(account),
            states=self._watch_states,
            scan_min=scan_min,
            scan_max=scan_max,
            scan_fn=lambda page_url, acc: _scan_source_page(
                page_url, acc, on_status=self._watch_set_status
            ),
            on_new=lambda src, urls: schedule_on_main_thread(
                self.root, lambda src=src, urls=urls: self._enqueue_watch_videos(src, urls)
            ),
            on_state=lambda: schedule_on_main_thread(
                self.root,
                lambda: (
                    self._enrich_queue_source_labels(),
                    self._schedule_refresh_queue(),
                    # Debounce dài hơn — tránh ghi disk mỗi Page nguồn khi quét wave lớn.
                    self._schedule_save_settings(delay_ms=2000),
                ),
            ),
            on_status=self._watch_set_status,
        )
        self._watch_enabled = True
        self._cancel_schedule.clear()
        self._watcher.start(scan_immediately=not delay_first_scan)
        self.watch_toggle_btn.configure(text="Tắt theo dõi")
        mode = "chia sẻ ngay khi có video mới" if self.watch_share_now.get() else "xếp lịch với khoảng cách ngẫu nhiên"
        if delay_first_scan:
            self.watch_status.set(
                f"Đang theo dõi {len(sources)} nguồn → {len(target_rows)} Page nhận ({mode}). "
                "Chờ ngắn nhường lịch đăng rồi quét (tránh đụng cùng profile)."
            )
        else:
            self.watch_status.set(
                f"Đang theo dõi {len(sources)} nguồn → {len(target_rows)} Page nhận ({mode}). Đang quét…"
            )
        self._refresh_watch_stats()
        self._remember_share_watch_wanted(True)
        self._live_plan_key = self._current_plan_key()
        self._schedule_save_settings()
        self._ensure_queue_worker()
        return True

    def _watch_disable(self) -> None:
        """Tắt theo dõi. Link đã vào list chờ vẫn giữ nguyên."""
        if self._watcher is not None:
            self._watcher.stop()
        self._watcher = None
        self._watch_enabled = False
        self.watch_toggle_btn.configure(text="Bật theo dõi")
        self.watch_status.set("Đã tắt theo dõi. Video mới đã vào list chờ vẫn được giữ.")
        self._refresh_watch_stats()
        self._remember_share_watch_wanted(False)
        self._schedule_save_settings()

    def _remember_share_watch_wanted(self, wanted: bool) -> None:
        """Ghi app_runtime để mở lại máy vẫn biết có nên tự bật theo dõi."""
        try:
            from src.utils.app_runtime_prefs import set_share_watch_wanted

            set_share_watch_wanted(bool(wanted))
        except Exception:  # noqa: BLE001
            pass

    def _watch_resume_saved(self) -> None:
        """Mở lại app: lần trước đang bật theo dõi thì bật lại; hoãn quét để khỏi đụng lịch đăng."""
        try:
            from src.utils.app_runtime_prefs import load_runtime_prefs, should_auto_start_share_watch

            hint = should_auto_start_share_watch(load_runtime_prefs())
            if hint is True:
                self._watch_enabled = True
            elif hint is False:
                self._watch_enabled = False
        except Exception:  # noqa: BLE001
            pass
        if not self._watch_enabled:
            return
        if not self._watch_targets and self._checked:
            self._watch_targets = list(dict.fromkeys(self._checked))
        if not self._watch_targets:
            self._watch_enabled = False
            self.watch_toggle_btn.configure(text="Bật theo dõi")
            self.watch_status.set(
                "Lần trước đang theo dõi nhưng mất list Page nhận — tích lại Page rồi bấm Bật theo dõi."
            )
            self._remember_share_watch_wanted(False)
            self._schedule_save_settings()
            return
        self.watch_toggle_btn.configure(text="Đang khôi phục…")
        ok = self._watch_enable(interactive=False, delay_first_scan=True)
        if not ok:
            self._watch_enabled = False
            self.watch_toggle_btn.configure(text="Bật theo dõi")
            self._remember_share_watch_wanted(False)
            self._schedule_save_settings()
            return

    def _account_scope_ids(self) -> set[str] | None:
        """
        Tài khoản đang lọc trên UI.

        ``None`` = «Tất cả tài khoản» (mọi TK đang tick).
        ``set`` một id = chỉ chạy TK đó — tránh mở Firefox của TK khác khi đang xem 1 TK.
        """
        choice = str(self.filter_account.get() or "").strip()
        if choice in ("", "Chọn tài khoản", "Tất cả tài khoản"):
            return None
        aid = str(self._account_labels.get(choice) or "").strip()
        return {aid} if aid else None

    def _scope_page_rows(self, rows: list[dict[str, str]]) -> list[dict[str, str]]:
        """Lọc Page nhận theo tài khoản đang chọn trên combobox."""
        scope = self._account_scope_ids()
        if not scope:
            return list(rows)
        return [row for row in rows if str(row.get("account_id") or "") in scope]

    def _kill_stale_browsers_for_accounts(self, account_ids: Iterable[str]) -> None:
        """Đóng Firefox còn sót của các TK sắp chia sẻ (sau tắt/mở app)."""
        from src.services.facebook_groups.facebook_provider import FacebookGroupProvider

        seen: set[str] = set()
        for raw in account_ids:
            aid = str(raw or "").strip()
            if not aid or aid in seen:
                continue
            seen.add(aid)
            try:
                FacebookGroupProvider._kill_stale_firefox(aid)
            except Exception:  # noqa: BLE001
                continue

    def _resume_queue_if_needed(self) -> None:
        """Mở lại app: chạy tiếp batch dở + list chờ (chỉ TK đang lọc nếu không chọn «Tất cả»)."""
        if self._running or self._cancel_schedule.is_set():
            return
        scope = self._account_scope_ids()
        if any(item.state == "waiting" for item in self._queue):
            overdue = self._promote_overdue_queue()
            waiting_n = sum(1 for i in self._queue if i.state == "waiting")
            extra = f" · {overdue} video quá giờ xếp chạy ngay" if overdue else ""
            scope_hint = ""
            if scope:
                label = self.filter_account.get().strip()
                scope_hint = f" · chỉ TK «{label}»"
                # Dọn Firefox sót trước khi mở lại — tránh 2 cửa sổ cùng profile.
                self._kill_stale_browsers_for_accounts(scope)
            else:
                aids = {
                    str(row.get("account_id") or "")
                    for row in self._scope_page_rows(self._live_receiving_pages())
                    if str(row.get("account_id") or "")
                }
                self._kill_stale_browsers_for_accounts(aids)
            self.status.set(
                f"Mở lại app — tiếp tục {waiting_n} video trong hàng chờ{extra}{scope_hint}."
            )
            self._ensure_queue_worker()
            return
        # Không còn waiting nhưng còn batch READY sau recover — bấm Tiếp tục giúp.
        orphan_ids: list[str] = []
        orphan_accounts: list[str] = []
        for batch in self.store.share_batches():
            if batch.get("status") != "READY":
                continue
            aid = str(batch.get("account_id") or "").strip()
            if scope is not None and aid not in scope:
                continue
            bid = str(batch.get("id") or "")
            if not bid:
                continue
            pending = [
                job
                for job in self.store.share_jobs()
                if str(job.get("batch_id") or "") == bid
                and str(job.get("status") or "") in {"PENDING", "VERIFY_FIRST"}
            ]
            if pending:
                orphan_ids.append(bid)
                if aid:
                    orphan_accounts.append(aid)
        if orphan_ids:
            self._kill_stale_browsers_for_accounts(orphan_accounts)
            self.batch_ids = list(dict.fromkeys([*self.batch_ids, *orphan_ids]))
            scope_hint = f" (lọc {self.filter_account.get().strip()})" if scope else ""
            self.status.set(f"Còn {len(orphan_ids)} batch chia sẻ dở{scope_hint} — đang chạy tiếp.")
            self.resume()

    def _safe_parse_sources(self) -> list[str]:
        """Đọc Page nguồn trên form; ô trống → []; dòng lỗi bỏ qua (đang sửa)."""
        raw = self.watch_sources_box.get("1.0", tk.END)
        if not str(raw or "").strip():
            return []
        try:
            return parse_source_urls(raw)
        except ValueError:
            found: list[str] = []
            for line in str(raw).replace(",", "\n").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    found.extend(parse_source_urls(line))
                except ValueError:
                    continue
            return found

    def _prune_queue_for_removed_sources(self, sources: list[str] | None = None) -> int:
        """Bỏ (skipped) link chờ tới từ Page nguồn đã xóa — không chia sẻ tiếp."""
        active = sources if sources is not None else self._safe_parse_sources()
        dropped = 0
        with self._queue_lock:
            keep: list[PlannedShareLink] = []
            for item in self._queue:
                if item.state == "waiting" and item.source and not is_watch_source_active(item.source, active):
                    vid = video_id_from_url(item.url)
                    if vid:
                        self._cancelled_video_ids.add(vid)
                    item.state = "skipped"
                    dropped += 1
                    continue
                keep.append(item)
            self._queue = keep
        return dropped

    def _enqueue_watch_videos(self, source: str, urls: list[str]) -> None:
        """Đưa video mới vào hàng chờ — chia sẻ ngay hoặc cách lượt trước một khoảng ngẫu nhiên.

        Nhiều video cùng đợt: xáo thứ tự (không luôn chạy từ trên xuống),
        «chia sẻ ngay» cách nhau ~1–2s để chạy nhanh hơn.
        """
        if not self._watch_enabled:
            return
        active = self._safe_parse_sources()
        if not active or not is_watch_source_active(source, active):
            # Page nguồn đã xóa / tắt — không xếp thêm video từ nguồn này.
            return
        if not self._watch_targets:
            return
        share_now = bool(self.watch_share_now.get())
        if share_now:
            gap_min, gap_max = 0, 0
        else:
            try:
                gap_min, gap_max = parse_minute_range(
                    self.watch_gap_min.get(), self.watch_gap_max.get(), label="Cách nhau mỗi lượt", floor=0
                )
            except ValueError:
                gap_min, gap_max = 0, 3
        known = {video_id_from_url(item.url) for item in self._queue}
        known |= set(self._cancelled_video_ids)
        pending_urls: list[str] = []
        for url in urls:
            vid = video_id_from_url(url)
            if not vid or vid in known:
                continue
            known.add(vid)
            pending_urls.append(url)
        # Chia đều / không kẹt luôn video đầu list — xáo thứ tự đợt mới.
        random.shuffle(pending_urls)
        added: list[PlannedShareLink] = []
        now = datetime.now().replace(microsecond=0)
        for url in pending_urls:
            with self._queue_lock:
                planned = [
                    row.when for row in self._queue if row.source and row.state in {"waiting", "running"}
                ]
            last = max([*planned, *([self._watch_last_planned] if self._watch_last_planned else [])], default=None)
            if share_now:
                # Nhiều video: cách 1–2s (không 5s) — worker chạy liên tiếp nhanh hơn.
                stagger = random.randint(1, 2) if added else 0
                when = (now + timedelta(seconds=stagger * len(added))).replace(microsecond=0)
            else:
                when = next_share_time(now=now, last_planned=last, gap_min=gap_min, gap_max=gap_max)
            label = "Ngay" if share_now else (
                when.strftime("%H:%M") if when.date() == now.date() else when.strftime("%d/%m %H:%M")
            )
            src_name, src_id = self._source_page_meta(source)
            item = PlannedShareLink(
                when=when,
                url=url,
                label=f"Mới {label}",
                targets=list(self._watch_targets),
                source=source,
                source_name=src_name,
                source_id=src_id,
            )
            self._remember_queue_item(item)
            self._watch_last_planned = when
            added.append(item)
        if not added:
            return
        self._schedule_refresh_queue()
        self._save_share_settings()
        if share_now:
            self.status.set(
                f"Có {len(added)} video mới từ Page nguồn — lần lượt chia sẻ hết lên Page nhận."
            )
            try:
                self._share_nb.select(1)
            except Exception:
                pass
        else:
            times = ", ".join(item.label.replace("Mới ", "") for item in added[:5])
            more = f" (+{len(added) - 5})" if len(added) > 5 else ""
            self.status.set(
                f"Page nguồn có {len(added)} video mới — xếp lịch {times}{more}; "
                "tự chạy lần lượt khi tới giờ."
            )
        # Đảm bảo worker đang chạy; nếu đang bận wave trước thì video mới vào hàng chờ.
        self._ensure_queue_worker()

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
                        "page_category": normalize_page_category(row.get("page_category")),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            self.status.set(f"Không đọc được danh sách Page: {exc}")
        alive = {row["iid"] for row in rows}
        self.pages = rows
        # Khôi phục tick đã lưu + giữ tick hiện tại (đổi lọc tài khoản không mất chọn).
        remembered = (set(self._watch_targets) | set(self._checked)) & alive
        if remembered:
            self._checked = {
                row["iid"]
                for row in dedupe_receiving_pages([r for r in rows if r["iid"] in remembered])
            }
            self._sync_checked_targets()
        elif self._watch_enabled and self._watch_targets:
            # Đang theo dõi: không xóa targets đã lưu dù list Page tạm chưa khớp.
            self._checked = set(self._watch_targets)
        else:
            self._sync_checked_targets()
        self._refresh_account_filter()
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

    def _on_account_filter(self, _event: object = None) -> None:
        """Ghi nhận người dùng đã chọn tài khoản rồi vẽ lại bảng."""
        self._account_chosen = True
        self._fill_pages()

    def _refresh_account_filter(self) -> None:
        """Đổ danh sách tài khoản. Lần đầu chỉ hiện Page của tài khoản đầu tiên."""
        labels = ["Chọn tài khoản"]
        mapping: dict[str, str] = {}
        seen: set[str] = set()
        for row in self.pages:
            aid = row["account_id"]
            if aid in seen:
                continue
            seen.add(aid)
            label = format_account_filter_label(aid, row["account_name"])
            if label in mapping:
                label = f"{label} ({aid})"
            mapping[label] = aid
            labels.append(label)
        if seen:
            labels.append("Tất cả tài khoản")
        self._account_labels = mapping
        self.filter_account_box.configure(values=tuple(labels))
        current = self.filter_account.get()
        if current in labels and self._account_chosen:
            return
        if not self._account_chosen and len(labels) > 1 and labels[1] != "Tất cả tài khoản":
            self.filter_account.set(labels[1])
            self._account_chosen = True
            return
        if current not in labels:
            self.filter_account.set(labels[1] if len(labels) > 1 else "Chọn tài khoản")

    def _visible_pages(self) -> list[dict[str, str]]:
        """Page đang được lọc theo tài khoản, ô tìm và phân loại."""
        rows = [row for row in self.pages if self._row_visible(row)]
        if self.filter_account.get().strip() == "Tất cả tài khoản":
            return interleave_pages_by_account(rows)
        return rows

    def _account_scope_pages(self) -> list[dict[str, str]]:
        """Page của tài khoản đang chọn, chưa lọc phân loại — để đếm số trên ô Phân loại."""
        choice = self.filter_account.get().strip()
        if choice in ("", "Chọn tài khoản"):
            return []
        account_id = self._account_labels.get(choice, "")
        return [row for row in self.pages if choice == "Tất cả tài khoản" or row["account_id"] == account_id]

    def _fill_pages(self) -> None:
        self.category_filter.refresh(self._account_scope_pages())
        self._refresh_outcomes()
        visible = self._visible_pages()
        tree_delete_all(self.tree)
        self._pages_tree_gen += 1
        gen = self._pages_tree_gen
        specs = [
            {
                "iid": row["iid"],
                "values": self._page_values(row, index),
                "tags": self._page_tags(row),
            }
            for index, row in enumerate(visible, start=1)
        ]

        def _done() -> None:
            if gen != self._pages_tree_gen:
                return
            self._estimate()

        if len(specs) < ASYNC_PREP_MIN_ROWS:
            for spec in specs:
                try:
                    self.tree.insert("", tk.END, **spec)
                except tk.TclError:
                    pass
            _done()
            return
        tree_insert_chunked(
            self.root,
            self.tree,
            specs,
            generation=gen,
            is_current=lambda g: g == self._pages_tree_gen,
            on_complete=_done,
        )

    def _paint_result_tags(self, tree: ttk.Treeview) -> None:
        """Màu chữ: xanh là thành công, đỏ là lỗi."""
        tree.tag_configure("ok", foreground="#0b7a32")
        tree.tag_configure("fail", foreground="#b00020")
        tree.tag_configure("part", foreground="#8a5a00")
        tree.tag_configure("run", foreground="#0b5394")
        tree.tag_configure("wait", foreground="#555555")

    def _refresh_outcomes(self, jobs: list[dict[str, Any]] | None = None) -> None:
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
        job_rows = jobs if jobs is not None else self._jobs()
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for job in job_rows:
            key = (str(job.get("account_id") or ""), str(job.get("page_id") or ""))
            grouped.setdefault(key, []).append(job)
        found: dict[tuple[str, str], tuple[str, str, str]] = {}
        for row in self.pages:
            key = (row["account_id"], row["page_id"])
            found[key] = describe_page_jobs(
                grouped.get(key, []),
                notes.get(row["account_id"], ""),
                self.engine.live_step_for(row["account_id"], row["page_id"]),
            )
        self._outcomes = found

    def _page_values(self, row: dict[str, str], index: int = 0) -> tuple[str, ...]:
        label, detail, _tag = self._outcomes.get((row["account_id"], row["page_id"]), ("", "", ""))
        shown = ""
        if label and detail:
            shown = f"{label} — {detail}"
        elif label:
            shown = label
        return (
            str(index),
            "☑" if row["iid"] in self._checked else "☐",
            row["account_name"],
            row["page_name"],
            page_category_label(row.get("page_category")) if row.get("page_category") else "—",
            shown,
            row["target_url"],
        )

    def _page_tags(self, row: dict[str, str]) -> tuple[str, ...]:
        tag = self._outcomes.get((row["account_id"], row["page_id"]), ("", "", ""))[2]
        return (tag,) if tag else ()

    def _sync_checked_targets(self) -> None:
        """Đồng bộ tick UI ↔ targets lưu. Đổi lọc tài khoản không xóa tick đã chọn."""
        if not self.pages:
            # Chưa nạp bảng Page — giữ nguyên tick đã nhớ từ cấu hình, không ghi đè rỗng.
            if self._checked:
                self._watch_targets = list(dict.fromkeys(self._checked))
            return
        selected = dedupe_receiving_pages([row for row in self.pages if row["iid"] in self._checked])
        if not selected and self._watch_enabled and self._watch_targets:
            # Đang theo dõi: không ghi đè targets bằng list rỗng (tránh mất resume khi mở lại).
            return
        self._checked = {row["iid"] for row in selected}
        self._watch_targets = [row["iid"] for row in selected]

    def _on_page_click(self, event: tk.Event) -> str | None:
        """Bấm một dòng để tích hoặc bỏ page đó. Cùng Page Facebook chỉ nhận một tài khoản."""
        row = self.tree.identify_row(event.y)
        if not row:
            return None
        page = next((item for item in self.pages if item["iid"] == row), None)
        if row in self._checked:
            self._checked.discard(row)
        else:
            dropped = self._uncheck_same_page(page) if page else []
            self._checked.add(row)
            if dropped:
                name = (page or {}).get("page_name") or "Page"
                self.status.set(
                    f"«{name}» trùng trên nhiều tài khoản — chỉ nhận 1 dòng, đã bỏ {len(dropped)} dòng trùng."
                )
        self._sync_checked_targets()
        self._refresh_checkmarks()
        self._estimate()
        self._schedule_save_settings()
        return "break"

    def _uncheck_same_page(self, page: dict[str, str]) -> list[str]:
        """Bỏ tích mọi dòng cùng khóa Page Facebook với ``page`` (trừ chính nó sau này)."""
        key = receiving_page_key(page)
        dropped: list[str] = []
        for other in self.pages:
            if other["iid"] == page["iid"]:
                continue
            if receiving_page_key(other) != key:
                continue
            if other["iid"] in self._checked:
                self._checked.discard(other["iid"])
                dropped.append(other["iid"])
        return dropped

    def _refresh_checkmarks(self) -> None:
        """Cập nhật cột «Nhận» trên bảng đang hiện."""
        for iid in self.tree.get_children():
            values = list(self.tree.item(iid, "values"))
            if len(values) > 1:
                values[1] = "☑" if iid in self._checked else "☐"
                self.tree.item(iid, values=values)

    def _selected_pages(self) -> list[dict[str, str]]:
        """Các page đang được tích — mỗi Page Facebook chỉ một tài khoản."""
        rows = dedupe_receiving_pages([row for row in self.pages if row["iid"] in self._checked])
        rows = self._scope_page_rows(rows)
        return interleave_pages_by_account(rows)

    def _select_all(self) -> None:
        """Tích mọi Page đang hiện (theo tài khoản đang lọc); giữ tick của tài khoản khác."""
        visible = [row for row in self.pages if self._row_visible(row)]
        self._checked.difference_update(row["iid"] for row in visible)
        taken = {receiving_page_key(row) for row in self.pages if row["iid"] in self._checked}
        skipped = 0
        for row in visible:
            key = receiving_page_key(row)
            if key in taken:
                skipped += 1
                continue
            self._checked.add(row["iid"])
            taken.add(key)
        if skipped:
            self.status.set(f"Đã bỏ qua {skipped} dòng Page trùng (cùng một Page Facebook).")
        self._sync_checked_targets()
        self._fill_pages()
        self._schedule_save_settings()

    def _clear_selection(self) -> None:
        """Bỏ tick các Page đang hiện; tick tài khoản khác vẫn giữ."""
        visible = {row["iid"] for row in self.pages if self._row_visible(row)}
        self._checked.difference_update(visible)
        self._sync_checked_targets()
        self._fill_pages()
        self._schedule_save_settings()

    def _row_visible(self, row: dict[str, str]) -> bool:
        """Page này có đang nằm trong bộ lọc tài khoản và ô tìm hay không."""
        choice = self.filter_account.get().strip()
        if choice in ("", "Chọn tài khoản"):
            return False
        if choice != "Tất cả tài khoản" and row["account_id"] != self._account_labels.get(choice, ""):
            return False
        if not self.category_filter.matches(row):
            return False
        needle = self.filter_text.get().strip().casefold()
        if not needle:
            return True
        blob = " ".join(
            (row["account_name"], row["page_name"], row["page_id"], page_category_label(row.get("page_category")))
        ).casefold()
        return needle in blob

    def _group_uid_raw(self) -> str:
        """Nội dung ô list UID nhóm. Một ô, nhiều dòng."""
        return self.group_uid_box.get("1.0", tk.END)

    def _comment_raw(self) -> str:
        """Nhiều bình luận, mỗi dòng một câu."""
        return self.comment_box.get("1.0", tk.END)

    def _count_source_pages(self) -> int:
        """Số Page nguồn hợp lệ trong ô dán (bỏ dòng trống / trùng)."""
        try:
            return len(parse_source_urls(self.watch_sources_box.get("1.0", tk.END)))
        except ValueError:
            # Có dòng sai định dạng — vẫn đếm dòng facebook.com để người dùng thấy đang nhập.
            raw = self.watch_sources_box.get("1.0", tk.END)
            seen: set[str] = set()
            count = 0
            for part in raw.replace(",", "\n").splitlines():
                line = part.strip()
                if not line or line.startswith("#"):
                    continue
                key = line.rstrip("/").casefold()
                if key in seen:
                    continue
                seen.add(key)
                count += 1
            return count

    def _receiving_pages_for_stats(self) -> list[dict[str, str]]:
        """
        Page nhận dùng để thống kê: đang theo dõi thì lấy Page đã chốt;
        chưa bật thì lấy các dòng đang tích (đã lọc trùng).
        """
        if self._watch_enabled and self._watch_targets:
            wanted = set(self._watch_targets)
            rows = [row for row in self.pages if row["iid"] in wanted]
            return dedupe_receiving_pages(rows)
        return self._selected_pages()

    def _refresh_watch_stats(self) -> None:
        """Cập nhật dòng thống kê Page nguồn / Page nhận chạy chia sẻ."""
        sources = self._count_source_pages()
        receivers = self._receiving_pages_for_stats()
        recv_n = len(receivers)
        accounts = len({row["account_id"] for row in receivers})
        if self._watch_enabled:
            mode = "đang theo dõi · đang chạy chia sẻ"
        elif sources or recv_n:
            mode = "đã cấu hình (chưa bật theo dõi)"
        else:
            mode = "chưa cấu hình"
        acc = f" · {accounts} tài khoản" if recv_n else ""
        if self.conflict_gap_auto.get():
            gap = " · giãn sau đăng: tự động"
        else:
            try:
                glo = float(str(self.conflict_gap_min.get()).strip() or "2")
                ghi = float(str(self.conflict_gap_max.get()).strip() or "5")
                glo, ghi = sorted((max(0.0, glo), max(0.0, ghi)))
                gap = f" · giãn sau đăng {glo:g}–{ghi:g} phút"
            except ValueError:
                gap = ""
        self.watch_stats.set(
            f"Thống kê: {sources} Page nguồn · {recv_n} Page nhận{acc}{gap} — {mode}"
        )
        self._refresh_conflict_gap_hint()

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
        self._refresh_watch_stats()

    def _pick_images(self) -> None:
        picked = filedialog.askopenfilenames(
            parent=self.root,
            title="Chọn ảnh để kèm bài",
            filetypes=[("Ảnh", "*.jpg *.jpeg *.png *.webp *.gif")],
        )
        self.image_paths = [str(path) for path in picked]
        self.image_label.set(f"{len(self.image_paths)} ảnh" if self.image_paths else "Chưa chọn ảnh")
        self._schedule_save_settings()

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
        """Ngày, giờ và phút đang chọn trên dropdown."""
        return compose_stamp(self.clock_date.get(), self.clock_hour.get(), self.clock_minute.get())

    def _sync_hour_choices(self) -> None:
        """Ngày khác hôm nay thì giờ từ 00. Hôm nay thì bắt đầu từ giờ máy."""
        selected = self.clock_hour.get()
        later_day = not date_choice_is_today(self.clock_date.get())
        choices = hour_choices(from_midnight=later_day)
        self.hour_box.configure(values=choices)
        if selected not in choices:
            self.clock_hour.set(choices[0])
        self.machine_clock.set(machine_clock_text())
        days = date_choices()
        self.date_box.configure(values=days)
        if self.clock_date.get() not in days:
            self.clock_date.set(days[0])

    def _sync_clock_to_machine(self) -> None:
        """Đưa dropdown về đúng ngày, giờ và phút của máy."""
        moment = datetime.now()
        days = date_choices(moment)
        self.date_box.configure(values=days)
        self.clock_date.set(days[0])
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
        self._schedule_save_settings()
        waiting = sum(1 for row in self._queue if row.state == "waiting")
        self.status.set(f"Đã thêm vào list chờ. Còn {waiting} link.")

    def _queued_links_from_selection(self) -> list[str]:
        """Link video của các dòng đang chọn trên Hàng chờ (theo thứ tự bảng)."""
        picked = list(self.queue_tree.selection())
        if not picked:
            return []
        links: list[str] = []
        for iid in picked:
            vals = list(self.queue_tree.item(iid, "values") or ())
            # Cột: stt, when, source, url, state
            url = str(vals[3] if len(vals) > 3 else "").strip()
            if url.startswith("http"):
                links.append(url)
                continue
            # Fallback: tìm URL trong mọi cột.
            for raw in vals:
                text = str(raw or "").strip()
                if text.startswith("http://") or text.startswith("https://"):
                    links.append(text)
                    break
        return list(dict.fromkeys(links))

    def _copy_queued_video_links(self, _event: object | None = None) -> str:
        """Copy link video đã chọn vào clipboard (Ctrl+C / nút / chuột phải)."""
        links = self._queued_links_from_selection()
        if not links:
            self.status.set("Chọn ít nhất một dòng trên Hàng chờ để copy link video.")
            return "break"
        try:
            self.root.clipboard_clear()
            self.root.clipboard_append("\n".join(links))
        except Exception as exc:  # noqa: BLE001
            self.status.set(f"Không copy được link: {exc}")
            return "break"
        n = len(links)
        self.status.set(f"Đã copy {n} link video vào clipboard.")
        return "break"

    def _on_queue_double_click_copy(self, event: object) -> None:
        """Double-click dòng → copy link video của dòng đó."""
        try:
            row = self.queue_tree.identify_row(getattr(event, "y", 0))
        except Exception:
            return
        if not row:
            return
        self.queue_tree.selection_set((row,))
        self.queue_tree.focus(row)
        self._copy_queued_video_links()

    def _remove_queued(self) -> None:
        """
        Xóa / bỏ qua video đã chọn trong hàng chờ.

        - Đang chờ: đánh dấu bỏ qua + nhớ ID (không xếp lại từ theo dõi).
        - Worker đang chờ tới lượt video đó sẽ bỏ qua và chạy video còn lại.
        - Đang chạy: không xóa giữa chừng.
        """
        picked = set(self.queue_tree.selection())
        if not picked:
            return
        skipped = 0
        with self._queue_lock:
            keep: list[PlannedShareLink] = []
            for item in self._queue:
                if item.qid not in picked:
                    keep.append(item)
                    continue
                if item.state == "running":
                    keep.append(item)
                    continue
                vid = video_id_from_url(item.url)
                if vid:
                    self._cancelled_video_ids.add(vid)
                item.state = "skipped"
                skipped += 1
                # Không giữ dòng đã bỏ qua trong list — tránh rối UI.
            self._queue = keep
        self._refresh_queue()
        self._schedule_save_settings()
        waiting = sum(1 for row in self._queue if row.state == "waiting")
        if skipped and waiting:
            self.status.set(f"Đã bỏ qua {skipped} video. Còn {waiting} video — tiếp tục chia sẻ.")
            self._ensure_queue_worker()
        elif skipped:
            self.status.set(f"Đã bỏ qua {skipped} video. Không còn video trong hàng chờ.")
        else:
            self.status.set("Không xóa được: video đang chia sẻ (đợi xong) hoặc chưa chọn dòng.")

    def _schedule_refresh_queue(self) -> None:
        """Gom nhiều lần vẽ hàng chờ (watch + worker) thành một lần — tránh đơ Treeview."""
        if self._queue_refresh_after:
            return

        def _flush() -> None:
            self._queue_refresh_after = None
            self._refresh_queue()

        try:
            self._queue_refresh_after = self.root.after(280, _flush)
        except tk.TclError:
            self._queue_refresh_after = None
            self._refresh_queue()

    def _source_page_meta(self, source: str) -> tuple[str, str]:
        """Tên + Meta ID Page nguồn từ mốc quét / bảng Page đã lưu."""
        raw = str(source or "").strip()
        if not raw:
            return "", ""
        key = source_state_key(raw)
        st = self._watch_states.get(key)
        name = str(getattr(st, "page_name", "") or "").strip() if st else ""
        pid = str(getattr(st, "page_id", "") or "").strip() if st else ""
        if name and pid:
            return name, pid
        # Tra bảng Page nhận / PagesManager (nếu nguồn cũng là Page đã lưu).
        try:
            from src.services.facebook_reels_catalog import facebook_page_identity

            want = facebook_page_identity(raw)
        except Exception:
            want = ""
        want_id = want.split(":", 1)[1] if want.startswith("id:") else ""
        for row in self.pages:
            try:
                from src.services.facebook_reels_catalog import facebook_page_identity

                got = facebook_page_identity(str(row.get("target_url") or ""))
            except Exception:
                got = ""
            row_fb = str(row.get("fb_page_id") or "").strip()
            row_id = row_fb if row_fb.isdigit() else str(row.get("page_id") or "").strip()
            row_name = str(row.get("page_name") or "").strip()
            if want and got and want == got:
                return name or row_name, pid or (row_fb if row_fb.isdigit() else row_id)
            if want_id and (
                row_fb == want_id
                or (str(row.get("page_id") or "").strip() == want_id and want_id.isdigit())
            ):
                return name or row_name, pid or want_id
        if not pid and want_id:
            pid = want_id
        return name, pid

    def _enrich_queue_source_labels(self) -> None:
        """Điền thiếu source_name / source_id trên hàng chờ từ mốc quét."""
        with self._queue_lock:
            rows = list(self._queue)
        for item in rows:
            if not item.source:
                continue
            if item.source_name and item.source_id:
                continue
            n2, i2 = self._source_page_meta(item.source)
            if n2:
                item.source_name = item.source_name or n2
            if i2:
                item.source_id = item.source_id or i2

    def _queue_source_label(self, item: PlannedShareLink) -> str:
        """Nhãn cột Page nguồn: ``Tên (id)``."""
        if not item.source:
            return "— (tay)"
        name = str(item.source_name or "").strip()
        pid = str(item.source_id or "").strip()
        if not name or not pid:
            n2, i2 = self._source_page_meta(item.source)
            name = name or n2
            pid = pid or i2
            if n2 or i2:
                item.source_name = name
                item.source_id = pid
        return source_page_display(item.source, page_name=name, page_id=pid) or item.source

    def _refresh_queue(self) -> None:
        """Vẽ lại list chờ: giờ, Page nguồn, link video và trạng thái."""
        self._enrich_queue_source_labels()
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
        tree_delete_all(self.queue_tree)
        self._queue_tree_gen += 1
        gen = self._queue_tree_gen
        specs = [
            {
                "iid": item.qid or item.label or f"q{index}",
                "values": (
                    index,
                    item.label or item.when.strftime("%H:%M"),
                    self._queue_source_label(item),
                    item.url,
                    labels.get(item.state, item.state),
                ),
            }
            for index, item in enumerate(rows, start=1)
        ]
        if len(specs) < ASYNC_PREP_MIN_ROWS:
            for spec in specs:
                try:
                    self.queue_tree.insert("", tk.END, **spec)
                except tk.TclError:
                    pass
            return
        tree_insert_chunked(
            self.root,
            self.queue_tree,
            specs,
            generation=gen,
            is_current=lambda g: g == self._queue_tree_gen,
        )

    def _share_params(self) -> dict[str, Any]:
        """Đọc cài đặt chia sẻ trên form (ảnh, nghỉ, thời gian xem, nơi đăng, UID nhóm)."""
        from src.services.facebook_groups.video_engage import clamp_engage_rate_percent

        watch_low, watch_high = optional_watch_bounds(self.watch_seconds.get(), self.watch_until.get())
        return {
            "images": normalize_share_images(self.image_paths) if self.image_paths else [],
            "cooldown": self._cooldown_seconds(),
            "reel_seconds": clamp_reel_seconds(self.reel_seconds.get()),
            "watch_low": watch_low,
            "watch_high": watch_high,
            "place": self.destination.get(),
            "group_ids": parse_group_uids(self._group_uid_raw()),
            "engage_rate_percent": clamp_engage_rate_percent(self.engage_rate_percent.get()),
        }

    def _pages_for_item(self, item: PlannedShareLink, selected: list[dict[str, str]]) -> list[dict[str, str]]:
        """Page nhận của một link: link từ Page nguồn dùng Page đã chọn lúc bật theo dõi."""
        if item.targets:
            wanted = set(item.targets)
            rows = [row for row in self.pages if row["iid"] in wanted]
        else:
            rows = selected
        return dedupe_receiving_pages(self._scope_page_rows(rows))

    def start(self) -> None:
        if self._running:
            self.status.set("Đang chia sẻ. Bấm Dừng nếu muốn ngắt.")
            return
        selected = self._selected_pages()
        typed = bool(self.source_url.get().strip())
        needs_pick = typed or any(item.state == "waiting" and not item.targets for item in self._queue)
        if needs_pick and not selected:
            messagebox.showwarning("Chưa tích Page", "Tích một hoặc nhiều page cần đăng.", parent=self.root)
            return
        try:
            params = self._share_params()
            if typed:
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
        place = params["place"]
        group_ids = params["group_ids"]
        if self._group_uid_raw().strip() and not group_ids:
            messagebox.showwarning(
                "UID nhóm",
                "Dán list UID số, mỗi dòng một UID, hoặc link facebook.com/groups/123.",
                parent=self.root,
            )
            return
        if (
            place == "joined"
            and selected
            and not group_ids
            and not any(int(row["joined"] or 0) for row in selected)
        ):
            messagebox.showwarning(
                "Chưa có nhóm",
                "Các Page đã chọn chưa có nhóm đã tham gia. Hãy tham gia nhóm trước, hoặc chọn lên Page.",
                parent=self.root,
            )
            return
        self._launch_queue_worker(selected, params)

    def _promote_overdue_queue(self) -> int:
        """
        Video đã quá giờ vẫn ``waiting`` → xếp chạy lần lượt ngay (cách 2–4 giây).

        Tránh kẹt cả đống «Đang chờ» khi worker từng dừng giữa chừng hoặc gap 5–30 phút
        đẩy lịch quá xa trong khi Page nhận chỉ mới chia sẻ 1 video.
        """
        now = as_local_naive(datetime.now())
        promoted = 0
        with self._queue_lock:
            overdue = [
                item
                for item in self._queue
                if item.state == "waiting" and as_local_naive(item.when) <= now
            ]
            if not overdue:
                return 0
            overdue.sort(key=lambda item: as_local_naive(item.when))
            for index, item in enumerate(overdue):
                item.when = as_local_naive(now + timedelta(seconds=2 + index * 3))
                stamp = "Ngay" if index == 0 else item.when.strftime("%H:%M:%S")
                # Giữ tiền tố «Mới» nếu có — dễ nhận video từ theo dõi.
                base = item.label.replace("Mới ", "", 1) if item.label.startswith("Mới ") else item.label
                if not base or base in {"Ngay"} or ":" in base or "/" in base:
                    item.label = f"Mới {stamp}"
                promoted += 1
            self._queue.sort(key=lambda row: as_local_naive(row.when))
        if promoted:
            self._schedule_refresh_queue()
        return promoted

    def _kick_queue_after_worker(self) -> None:
        """Worker vừa dừng: nếu còn video chờ và không bị Dừng tay → tự chạy tiếp (trễ 1.5s)."""
        if self._running or self._cancel_schedule.is_set():
            return
        if not any(item.state == "waiting" for item in self._queue):
            return

        def _go() -> None:
            if self._running or self._cancel_schedule.is_set():
                return
            if not any(item.state == "waiting" for item in self._queue):
                return
            n = self._promote_overdue_queue()
            if n:
                self.status.set(f"Còn video chờ — xếp lại {n} video quá giờ và chạy tiếp.")
            self._ensure_queue_worker()

        self.root.after(1500, _go)

    def _ensure_queue_worker(self) -> None:
        """Theo dõi nguồn vừa thêm link: tự chạy list chờ nếu đang nghỉ (trừ khi người dùng đã bấm Dừng)."""
        if self._running:
            return
        if self._cancel_schedule.is_set():
            self.status.set("Có video mới trong list chờ. Đang ở trạng thái Dừng — bấm Chia sẻ để chạy.")
            return
        try:
            params = self._share_params()
        except ValueError as exc:
            self.status.set(f"Không tự chia sẻ được: {exc}")
            return
        self._launch_queue_worker(self._selected_pages(), params)

    def _launch_queue_worker(self, selected: list[dict[str, str]], params: dict[str, Any]) -> None:
        """Chạy list chờ ở luồng nền: tới giờ link nào thì chia sẻ link đó lên Page nhận của nó."""
        del params  # Mỗi lượt đọc lại form — thay đổi nút/ô cài đặt áp dụng ngay cho link kế tiếp.
        # Giữ batch_ids cũ để cột Kết quả còn đếm bài đã chia; chỉ thêm id wave mới.
        self._cancel_schedule.clear()
        overdue = self._promote_overdue_queue()
        waiting = [item for item in self._queue if item.state == "waiting"]
        if not waiting:
            return
        first = min(waiting, key=lambda item: as_local_naive(item.when))
        self._running = True
        due_n = sum(1 for item in waiting if as_local_naive(item.when) <= as_local_naive(datetime.now()))
        hint = f" · {overdue or due_n} video tới lượt" if (overdue or due_n) else f" · tới lượt lúc {first.label}"
        self.status.set(f"List chờ {len(waiting)} link{hint} — lần lượt chia sẻ hết lên Page nhận.")

        def _set_status(text: str) -> None:
            schedule_on_main_thread(self.root, lambda text=text: self.status.set(text))

        def _paint_queue() -> None:
            schedule_on_main_thread(self.root, self._schedule_refresh_queue)

        def _next_waiting() -> PlannedShareLink | None:
            """Video đã tới giờ: chọn ngẫu nhiên — không luôn chạy từ trên xuống."""
            from src.services.facebook_groups.video_engage import shuffle_due_queue_items

            with self._queue_lock:
                pending = [item for item in self._queue if item.state == "waiting"]
            return shuffle_due_queue_items(pending, now=datetime.now())

        def _item_still_pending(item: PlannedShareLink) -> bool:
            """Còn trong list và đang chờ — False nếu user đã xóa / bỏ qua."""
            with self._queue_lock:
                if item not in self._queue:
                    return False
                if item.state != "waiting":
                    return False
                vid = video_id_from_url(item.url)
                if vid and vid in self._cancelled_video_ids:
                    item.state = "skipped"
                    return False
                return True

        def _wait_turn(item: PlannedShareLink) -> bool:
            """Chờ đúng giờ của link này. Link mới sớm hơn / bị xóa thì nhường lượt."""
            noted_at = 0.0
            while True:
                if self._cancel_schedule.is_set():
                    return False
                if not _item_still_pending(item):
                    return False
                sooner = _next_waiting()
                if sooner is not item:
                    return False
                left = (as_local_naive(item.when) - as_local_naive(datetime.now())).total_seconds()
                if left <= 0:
                    with self._queue_lock:
                        if item not in self._queue or item.state != "waiting":
                            return False
                        vid = video_id_from_url(item.url)
                        if vid and vid in self._cancelled_video_ids:
                            item.state = "skipped"
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
                        _set_status("Hàng chờ trống — hết video cần chia sẻ.")
                        return
                    if not _wait_turn(item):
                        if self._cancel_schedule.is_set():
                            _set_status("Đã dừng. Link chưa tới giờ vẫn nằm trong list chờ.")
                            return
                        # Video bị xóa / nhường lượt — chạy video còn lại.
                        _paint_queue()
                        continue
                    with self._queue_lock:
                        still_here = item in self._queue and item.state == "running"
                        cancelled = bool(
                            (vid := video_id_from_url(item.url)) and vid in self._cancelled_video_ids
                        )
                    if not still_here or cancelled:
                        if cancelled and item in self._queue:
                            item.state = "skipped"
                        _paint_queue()
                        left = sum(1 for row in self._queue if row.state == "waiting")
                        _set_status(f"Đã bỏ qua video đã xóa. Còn {left} video — tiếp tục.")
                        continue
                    _paint_queue()
                    try:
                        live = self._share_params()
                    except ValueError as exc:
                        item.state = "waiting"
                        _paint_queue()
                        _set_status(f"Cài đặt chưa hợp lệ — sửa rồi chạy lại: {exc}")
                        time.sleep(2)
                        continue
                    fallback = self._live_receiving_pages() or selected
                    if item.source:
                        live_sources = self._safe_parse_sources()
                        if not is_watch_source_active(item.source, live_sources):
                            item.state = "skipped"
                            _paint_queue()
                            _set_status(
                                f"{item.label} — bỏ qua: Page nguồn đã xóa, không chia sẻ tiếp."
                            )
                            schedule_on_main_thread(self.root, self._schedule_save_settings)
                            continue
                        if self._watch_targets:
                            item.targets = list(self._watch_targets)
                    targets = self._pages_for_item(item, fallback)
                    if not targets:
                        item.state = "skipped"
                        _paint_queue()
                        _set_status(f"{item.label} — bỏ qua: không còn Page nhận nào (Page đã bị xóa?).")
                        continue
                    pending_left = sum(1 for row in self._queue if row.state == "waiting")
                    # Ưu tiên chạy tiếp batch dở cùng video (sau tắt/mở) — tránh tạo wave trùng.
                    resume_ids = self.engine.incomplete_share_batch_ids_for_url(item.url)
                    scope = self._account_scope_ids()
                    if resume_ids and scope is not None:
                        by_id = {
                            str(row.get("id") or ""): str(row.get("account_id") or "")
                            for row in self.store.share_batches()
                        }
                        resume_ids = [bid for bid in resume_ids if by_id.get(str(bid)) in scope]
                    if resume_ids:
                        ids = list(resume_ids)
                        self.batch_ids = list(dict.fromkeys([*self.batch_ids, *ids]))
                        _set_status(
                            f"{item.label} — chạy tiếp {len(ids)} batch dở "
                            f"(nhường lịch đăng nếu trùng TK). Còn {pending_left} link chờ."
                        )
                        schedule_on_main_thread(self.root, self._fill_results)
                        if not self._cancel_schedule.is_set():
                            self.engine.resume_share_wave(ids)
                        # Đếm job thật — tránh created=0 → đánh skipped nhầm.
                        result = {
                            "created": max(
                                1,
                                sum(
                                    1
                                    for row in self.store.share_jobs()
                                    if str(row.get("batch_id") or "") in set(ids)
                                ),
                            ),
                            "batch_ids": ids,
                        }
                    else:
                        _set_status(
                            f"{item.label} — đang chia sẻ lần lượt lên Page nhận "
                            f"({len(targets)} Page). Còn {pending_left} video chờ."
                        )
                        from src.services.facebook_groups.video_engage import (
                            clamp_engage_rate_percent,
                            parse_comment_lines,
                            plan_share_engage_for_video,
                        )

                        comment_raw = self._comment_raw()
                        engage_pct = clamp_engage_rate_percent(self.engage_rate_percent.get())
                        engage = plan_share_engage_for_video(
                            source=str(item.source or ""),
                            video_url=item.url,
                            allow_like=bool(self.like_video.get()),
                            allow_comment=bool(parse_comment_lines(comment_raw)),
                            engage_rate_percent=engage_pct,
                        )
                        result = self.engine.create_share_wave(
                            pages=targets,
                            source_url=item.url,
                            text=self.caption.get().strip(),
                            image_paths=live["images"],
                            destination=live["place"],
                            group_ids=live["group_ids"],
                            cooldown_fixed=live["cooldown"],
                            watch_seconds=live["watch_low"],
                            watch_max_seconds=live["watch_high"],
                            reel_seconds=live["reel_seconds"],
                            comment=comment_raw if engage.do_comment else "",
                            like_video=bool(engage.do_like),
                            engage_mode=engage.mode,
                            source_page=str(item.source or ""),
                        )
                        ids = list(result.get("batch_ids") or [])
                        self.batch_ids = list(dict.fromkeys([*self.batch_ids, *ids]))
                        # Không ghi settings giữa wave — giảm I/O khi nhiều Page.
                        schedule_on_main_thread(self.root, self._fill_results)
                        if ids and not self._cancel_schedule.is_set():
                            self.engine.run_share_wave(ids)
                        elif not ids:
                            _set_status(
                                f"{item.label} — không tạo được batch (trùng / thiếu Page). "
                                f"Bỏ qua, chạy video kế. Còn {pending_left} chờ."
                            )
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
                    # Wave lỗi / skipped nhưng còn video khác → không kẹt.
                    if item.state in {"error", "skipped"} and not self._cancel_schedule.is_set():
                        left = sum(1 for row in self._queue if row.state == "waiting")
                        _set_status(
                            f"{item.label} — {item.state}. Tiếp tục {left} video còn lại trong hàng chờ."
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
                _set_status(f"Lỗi chia sẻ: {message} — sẽ thử chạy tiếp hàng chờ.")
            finally:
                self._running = False
                schedule_on_main_thread(self.root, self._fill_results)
                schedule_on_main_thread(self.root, self._save_share_settings)
                schedule_on_main_thread(self.root, self._finish_status)
                # Quan trọng: tự chạy lại nếu còn video chờ (worker từng crash / hết lượt sớm).
                schedule_on_main_thread(self.root, self._kick_queue_after_worker)

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
        """Hiện bước từng TK/Page ngay trên thanh trạng thái + cột Kết quả."""

        def _apply() -> None:
            board = (text or "").strip() or self.engine.live_board()
            if board:
                # Nhiều TK: gộp một dòng dễ đọc trên thanh status.
                self.status.set(" | ".join(line.strip() for line in board.splitlines() if line.strip()))
                self.outcome.set(board)
            self._paint_live_result_column()

        schedule_on_main_thread(self.root, _apply)
        schedule_on_main_thread(self.root, self._schedule_results)

    def _paint_live_result_column(self) -> None:
        """
        Cập nhật nhanh cột «Kết quả» từ live_steps (không đợi flush 0.4s).

        Khi 2+ TK chạy song song, người dùng thấy ngay Page nào của TK nào đang xử lý.
        """
        live = self.engine.live_steps_snapshot()
        if not live and not self._outcomes:
            return
        # Gộp bước live vào outcomes hiện có (hoặc tạo tạm nếu chưa có job).
        for row in self._visible_pages():
            aid = row["account_id"]
            pid = row["page_id"]
            step = self.engine.live_step_for(aid, pid)
            key = (aid, pid)
            prev = self._outcomes.get(key, ("", "", ""))
            if step:
                if step.startswith("Chờ trong TK"):
                    self._outcomes[key] = ("Chờ TK", step, "wait")
                else:
                    self._outcomes[key] = ("Đang chạy", step, "run")
            elif prev[2] == "run" and key[0] not in live:
                # TK vừa xong bước — giữ nhãn cũ đến khi _fill_results đọc store.
                pass
            iid = row["iid"]
            if not self.tree.exists(iid):
                continue
            # STT lấy từ values hiện tại nếu có.
            try:
                old = self.tree.item(iid, "values") or ()
                index = int(old[0]) if old else 0
            except (TypeError, ValueError, IndexError):
                index = 0
            self.tree.item(
                iid,
                values=self._page_values(row, index),
                tags=self._page_tags(row),
            )

    def _schedule_results(self) -> None:
        """Trailing debounce — luôn lấy bước mới nhất, không bỏ cập nhật khi nhiều luồng."""
        if self._results_after:
            try:
                self.root.after_cancel(self._results_after)
            except Exception:
                pass
            self._results_after = None
        # 0.4s: đủ mượt, vẫn thấy Page đang chạy kịp thời.
        self._results_after = self.root.after(400, self._flush_results)

    def _flush_results(self) -> None:
        self._results_after = None
        self._fill_results()

    def _jobs(self) -> list[dict[str, Any]]:
        wanted = set(self.batch_ids)
        if not wanted:
            wanted = set(self.store.recent_share_batch_ids())
            self.batch_ids = list(wanted)
        return [row for row in self.store.share_jobs() if str(row.get("batch_id") or "") in wanted]

    def _page_job_comment(
        self,
        account_id: str,
        page_id: str,
        jobs: list[dict[str, Any]] | None = None,
    ) -> str:
        """Câu đã bốc cho Page trong lần chạy đang xem."""
        for job in jobs if jobs is not None else self._jobs():
            if str(job.get("account_id") or "") != account_id:
                continue
            if str(job.get("page_id") or "") != page_id:
                continue
            text = str(job.get("comment") or "").strip()
            if text:
                return text
        return ""

    def _schedule_save_settings(self, *, delay_ms: int = 400) -> None:
        """Ghi cài đặt sau khi người dùng ngừng gõ, không ghi từng phím."""
        if self._loading_settings:
            return
        if self._settings_save_id:
            try:
                self.root.after_cancel(self._settings_save_id)
            except Exception:
                pass
        self._settings_save_id = self.root.after(max(200, int(delay_ms)), self._persist_and_apply_settings)

    def _persist_and_apply_settings(self) -> None:
        """Lưu form rồi đẩy ngay vào luồng theo dõi / list chờ đang chạy."""
        self._settings_save_id = None
        self._sync_checked_targets()
        self._save_share_settings()
        self._apply_live_share_settings()

    def _live_receiving_pages(self) -> list[dict[str, str]]:
        """Page nhận mới nhất: ưu tiên đang tích, không thì Page đã chốt khi bật theo dõi."""
        selected = self._selected_pages()
        if selected:
            return selected
        if self._watch_targets:
            wanted = set(self._watch_targets)
            return dedupe_receiving_pages(
                self._scope_page_rows([row for row in self.pages if row["iid"] in wanted])
            )
        return []

    def _current_plan_key(self) -> tuple[Any, ...]:
        """Khóa so sánh chế độ xếp hàng chờ (chia sẻ ngay / khoảng cách)."""
        share_now = bool(self.watch_share_now.get())
        if share_now:
            return (True, 0, 0)
        try:
            gap_min, gap_max = parse_minute_range(
                self.watch_gap_min.get(), self.watch_gap_max.get(), label="Cách nhau mỗi lượt", floor=0
            )
        except ValueError:
            gap_min, gap_max = 0, 3
        return (False, int(gap_min), int(gap_max))

    def _replan_waiting_queue(self) -> int:
        """
        Xếp lại giờ các link ``waiting`` khi đổi «chia sẻ ngay» / khoảng cách lượt.

        Trả về số mục đã chỉnh — không đụng link đang ``running``.
        """
        plan = self._current_plan_key()
        share_now = bool(plan[0])
        gap_min, gap_max = int(plan[1]), int(plan[2])
        now = datetime.now().replace(microsecond=0)
        changed = 0
        with self._queue_lock:
            waiting = [row for row in self._queue if row.state == "waiting"]
            if not waiting:
                self._live_plan_key = plan
                return 0
            waiting.sort(key=lambda row: row.when)
            last: datetime | None = None
            for index, item in enumerate(waiting):
                if share_now:
                    when = (now + timedelta(seconds=1 * index)).replace(microsecond=0)
                    label = "Mới Ngay"
                else:
                    when = next_share_time(now=now, last_planned=last, gap_min=gap_min, gap_max=gap_max)
                    stamp = when.strftime("%H:%M") if when.date() == now.date() else when.strftime("%d/%m %H:%M")
                    label = f"Mới {stamp}"
                if item.when != when or item.label != label:
                    item.when = when
                    item.label = label
                    changed += 1
                last = when
                self._watch_last_planned = when
        self._live_plan_key = plan
        if changed:
            self._queue.sort(key=lambda row: row.when)
            self._schedule_refresh_queue()
            self._ensure_queue_worker()
        return changed

    def _apply_live_share_settings(self) -> None:
        """
        Áp dụng thay đổi nút / ô cài đặt vào luồng đang chạy — không cần tắt theo dõi.

        - Cập nhật list Page nguồn + khoảng quét của watcher (đánh thức nếu mau hơn)
        - Xóa Page nguồn → tắt theo dõi + bỏ hàng chờ tới từ nguồn đó
        - Đồng bộ Page nhận; xếp lại hàng chờ khi đổi share_now / gap
        - Giãn đăng lịch→chia sẻ (auto/tay) áp dụng ngay
        """
        if self._loading_settings:
            return
        self._sync_checked_targets()
        sources = self._safe_parse_sources()
        dropped = self._prune_queue_for_removed_sources(sources)
        if dropped:
            self._refresh_queue()
            self.status.set(f"Đã bỏ {dropped} link chờ vì Page nguồn đã xóa.")
            self._schedule_save_settings(delay_ms=200)
        self._refresh_watch_stats()
        selected = self._selected_pages()
        if self._watch_targets:
            with self._queue_lock:
                for item in self._queue:
                    if item.state == "waiting" and item.source:
                        if is_watch_source_active(item.source, sources):
                            item.targets = list(self._watch_targets)
        # Giãn cách đăng lịch → chia sẻ: áp dụng ngay cho lượt chờ kế tiếp.
        set_conflict_gap_auto(bool(self.conflict_gap_auto.get()))
        try:
            parse_conflict_gap_minutes(self.conflict_gap_min.get(), self.conflict_gap_max.get())
        except Exception:
            set_conflict_gap_minutes(2, 5)
        plan_key = self._current_plan_key()
        replanned = 0
        if self._live_plan_key is not None and plan_key != self._live_plan_key:
            replanned = self._replan_waiting_queue()
        else:
            self._live_plan_key = plan_key
        if not self._watch_enabled or self._watcher is None:
            if replanned:
                mode = "chia sẻ ngay" if self.watch_share_now.get() else "xếp lịch cách lượt"
                self.status.set(f"Đã xếp lại {replanned} link chờ ({mode}).")
            return
        if not sources:
            # Xóa hết Page nguồn → dừng quét, không chia sẻ tiếp từ theo dõi.
            self._watch_disable()
            self.watch_status.set("Đã xóa hết Page nguồn — tắt theo dõi, bỏ link chờ từ nguồn.")
            return
        try:
            scan_min, scan_max = parse_minute_range(
                self.watch_scan_min.get(), self.watch_scan_max.get(), label="Quét lại mỗi", floor=1
            )
        except ValueError:
            return
        watch_changed = self._watcher.update_settings(
            sources=sources, scan_min=scan_min, scan_max=scan_max, wake=True
        )
        if watch_changed or replanned or dropped:
            mode = "chia sẻ ngay" if self.watch_share_now.get() else "xếp lịch cách lượt"
            recv_n = len(selected) if selected else len(self._watch_targets)
            extra = f" · xếp lại {replanned} link chờ" if replanned else ""
            self.status.set(
                f"Đã nhận cài đặt mới: {len(sources)} nguồn → {recv_n} Page nhận ({mode}){extra}."
            )

    def _collect_share_settings(self) -> dict[str, Any]:
        return {
            "caption": self.caption.get(),
            "comments": self._comment_raw().strip(),
            "destination": self.destination.get(),
            "group_uids": self._group_uid_raw().strip(),
            "reel_seconds": self.reel_seconds.get(),
            "watch_seconds": self.watch_seconds.get(),
            "watch_until": self.watch_until.get(),
            "cooldown": self.cooldown.get(),
            "like_video": bool(self.like_video.get()),
            "engage_rate_percent": self.engage_rate_percent.get(),
            "last_batch_ids": list(self.batch_ids),
            "queue": queue_to_settings(self._queue),
            "cancelled_video_ids": list(self._cancelled_video_ids)[-500:],
            "conflict_gap_min": self.conflict_gap_min.get(),
            "conflict_gap_max": self.conflict_gap_max.get(),
            "conflict_gap_auto": bool(self.conflict_gap_auto.get()),
            "watch": {
                "sources": self.watch_sources_box.get("1.0", tk.END).strip(),
                "scan_min": self.watch_scan_min.get(),
                "scan_max": self.watch_scan_max.get(),
                "gap_min": self.watch_gap_min.get(),
                "gap_max": self.watch_gap_max.get(),
                "share_now": bool(self.watch_share_now.get()),
                "enabled": bool(self._watch_enabled),
                "targets": list(self._watch_targets),
                "checked": list(self._checked) if self._checked else list(self._watch_targets),
                "states": {url: st.to_dict() for url, st in list(self._watch_states.items())},
                "last_planned": self._watch_last_planned.isoformat() if self._watch_last_planned else "",
            },
        }

    def _save_share_settings(self) -> None:
        self._settings_save_id = None
        if self._loading_settings:
            return
        try:
            self.store.save_share_settings(self._collect_share_settings())
        except Exception:
            return

    def _load_share_settings(self) -> None:
        """Nạp bình luận, thời lượng và mã lần chạy trước để còn xem kết quả."""
        self._loading_settings = True
        try:
            data = self.store.share_settings()
            if data.get("caption") is not None:
                self.caption.set(str(data.get("caption") or ""))
            comments = str(data.get("comments") or "")
            if comments:
                self.comment_box.delete("1.0", tk.END)
                self.comment_box.insert("1.0", comments)
            dest = str(data.get("destination") or "").strip()
            if dest in {"page", "joined", "both"}:
                self.destination.set(dest)
            uids = str(data.get("group_uids") or "")
            if uids:
                self.group_uid_box.delete("1.0", tk.END)
                self.group_uid_box.insert("1.0", uids)
            for key, var in (
                ("reel_seconds", self.reel_seconds),
                ("watch_seconds", self.watch_seconds),
                ("watch_until", self.watch_until),
                ("cooldown", self.cooldown),
                ("conflict_gap_min", self.conflict_gap_min),
                ("conflict_gap_max", self.conflict_gap_max),
                ("engage_rate_percent", self.engage_rate_percent),
            ):
                if data.get(key) not in (None, ""):
                    var.set(str(data.get(key)))
            if "like_video" in data:
                self.like_video.set(bool(data.get("like_video")))
            # Mặc định bật auto nếu chưa có key (bản cũ).
            auto_raw = data.get("conflict_gap_auto")
            self.conflict_gap_auto.set(True if auto_raw is None else bool(auto_raw))
            set_conflict_gap_auto(bool(self.conflict_gap_auto.get()))
            parse_conflict_gap_minutes(self.conflict_gap_min.get(), self.conflict_gap_max.get())
            self._sync_conflict_gap_widgets()
            self._refresh_conflict_gap_hint()
            saved_ids = [str(item).strip() for item in (data.get("last_batch_ids") or []) if str(item).strip()]
            self.batch_ids = saved_ids or self.store.recent_share_batch_ids()
            if "queue" in data:
                self._queue = queue_from_settings(data.get("queue"))
                self._queue.sort(key=lambda row: row.when)
                self._queue_seq = len(self._queue)
                for index, item in enumerate(self._queue, start=1):
                    item.qid = f"q{index}"
            cancelled = data.get("cancelled_video_ids")
            if isinstance(cancelled, list):
                self._cancelled_video_ids = {
                    str(x).strip() for x in cancelled if str(x).strip().isdigit()
                }
            watch = data.get("watch") if isinstance(data.get("watch"), dict) else {}
            if watch:
                sources = str(watch.get("sources") or "")
                if sources:
                    self.watch_sources_box.delete("1.0", tk.END)
                    self.watch_sources_box.insert("1.0", sources)
                for key, var in (
                    ("scan_min", self.watch_scan_min),
                    ("scan_max", self.watch_scan_max),
                    ("gap_min", self.watch_gap_min),
                    ("gap_max", self.watch_gap_max),
                ):
                    if watch.get(key) not in (None, ""):
                        var.set(str(watch.get(key)))
                if "share_now" in watch:
                    self.watch_share_now.set(bool(watch.get("share_now")))
                else:
                    # Cấu hình cũ: khoảng cách lớn → giữ lịch cách lượt; khoảng cách nhỏ → chia sẻ ngay.
                    try:
                        legacy_gap = int(str(watch.get("gap_max") or "90"))
                    except ValueError:
                        legacy_gap = 90
                    self.watch_share_now.set(legacy_gap <= 5)
                self._watch_enabled = bool(watch.get("enabled"))
                saved_targets = [str(x) for x in (watch.get("targets") or []) if str(x).strip()]
                saved_checked = [str(x) for x in (watch.get("checked") or []) if str(x).strip()]
                # Gộp checked + targets (list rỗng không được nuốt mất targets cũ).
                remembered = list(dict.fromkeys([*saved_checked, *saved_targets]))
                self._watch_targets = list(remembered)
                self._checked = set(remembered)
                states = watch.get("states") if isinstance(watch.get("states"), dict) else {}
                loaded_states = {str(url): SourceState.from_dict(row) for url, row in states.items()}
                try:
                    source_urls = parse_source_urls(self.watch_sources_box.get("1.0", tk.END))
                except ValueError:
                    source_urls = []
                self._watch_states = merge_source_states(loaded_states, source_urls)
                self._enrich_queue_source_labels()
                raw_last = str(watch.get("last_planned") or "").strip()
                try:
                    self._watch_last_planned = datetime.fromisoformat(raw_last) if raw_last else None
                except ValueError:
                    self._watch_last_planned = None
                if self._watch_enabled and self._watch_targets:
                    self.watch_toggle_btn.configure(text="Đang khôi phục…")
                    self.watch_status.set("Đang khôi phục theo dõi Page nguồn sau khi mở lại…")
        except Exception:
            self.batch_ids = self.store.recent_share_batch_ids()
        finally:
            self._loading_settings = False
            self._sync_gap_widgets()
            self._refresh_queue()
            self._refresh_watch_stats()

    def _fill_results(self) -> None:
        """Một dòng một page: Thành công hoặc Lỗi, kèm câu lỗi."""
        jobs = self._jobs()
        self._refresh_outcomes(jobs=jobs)
        # Cập nhật cột Kết quả tại chỗ — không xóa/vẽ lại cả bảng Page (tối ưu 50–200 dòng).
        for index, row in enumerate(self._visible_pages(), start=1):
            iid = row["iid"]
            if not self.tree.exists(iid):
                continue
            self.tree.item(
                iid,
                values=self._page_values(row, index),
                tags=self._page_tags(row),
            )
        self._estimate()
        self.results.delete(*self.results.get_children())
        ok_names: list[str] = []
        bad_lines: list[str] = []
        shown = 0
        for row in self._visible_pages():
            label, detail, tag = self._outcomes.get((row["account_id"], row["page_id"]), ("", "", ""))
            if not label:
                continue
            shown += 1
            comment = self._page_job_comment(row["account_id"], row["page_id"], jobs=jobs)
            self.results.insert(
                "",
                tk.END,
                values=(shown, row["page_name"], label, comment, detail),
                tags=(tag,) if tag else (),
            )
            if tag == "ok":
                ok_names.append(row["page_name"])
            elif tag in {"fail", "part"}:
                bad_lines.append(f"{row['page_name']}: {detail}")
        parts: list[str] = []
        board = self.engine.live_board()
        if board:
            parts.append(board)
        if ok_names:
            parts.append(f"Thành công {len(ok_names)} page")
        if bad_lines:
            parts.append(f"Lỗi {len(bad_lines)} page. Xem cột Chi tiết, không liệt kê hết ở đây.")
        if parts:
            self.outcome.set(". ".join(parts))
        elif self.batch_ids:
            self.outcome.set("Đang chờ kết quả. Cột Kết quả cập nhật theo từng page.")
        else:
            self.outcome.set("Chưa chia sẻ. Cột Kết quả sẽ hiện Thành công hoặc Lỗi theo từng page.")


def _scan_source_page(
    url: str,
    account: dict[str, Any],
    *,
    on_status: Any | None = None,
) -> dict[str, Any]:
    """
    Quét Page nguồn bằng browser profile tài khoản Page nhận (đã login).

    Lấy ô video/reel đúng Page trên tab Reels (đầu trang) và vài video mới nhất
    trên tab Videos. Không lấy ID trong HTML gợi ý / không cuộn hết tab Videos.
    """
    from src.services.facebook_reels_catalog import scan_facebook_profile_reels_page

    profile = str(account.get("portable_path") or account.get("profile_path") or "").strip()
    if not profile:
        return {
            "ok": False,
            "items": [],
            "message": "Tài khoản Page nhận chưa có portable_path — không quét được bằng session đã login.",
        }
    return scan_facebook_profile_reels_page(
        page_url=url,
        account=account,
        # Chế độ chia sẻ: đầu trang đủ sâu để nhớ lịch sử + đối chiếu video mới.
        max_reels=12,
        max_scroll_rounds=3,
        scroll_until_end=False,
        max_scan_minutes=2.5,
        scroll_pause_sec=0.55,
        status=on_status,
        include_html_ids=False,
        scan_videos_tab=False,
        scan_videos_tab_top=True,
        require_page_owner=True,
        use_graphql_harvest=True,
        newest_only=True,
    )


def build_share_tab(parent: ttk.Frame, root: tk.Misc) -> ShareTab:
    """Gắn tab chia sẻ vào notebook chính."""
    return ShareTab(parent, root)
