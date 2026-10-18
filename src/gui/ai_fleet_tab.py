"""Màn AI Agent: agent tổng, agent từng Page, viết bài, rồi duyệt trước khi vào lịch."""

from __future__ import annotations

import re
import threading
import tkinter as tk
from datetime import datetime, timezone
from tkinter import messagebox, ttk
from typing import Any

from src.gui.page_category_filter import PageCategoryFilter
from src.gui.ui_responsiveness import (
    ASYNC_PREP_MIN_ROWS,
    schedule_on_main_thread,
    tree_delete_all,
    tree_insert_chunked,
)
from src.utils.page_categories import normalize_page_category, page_category_label
from src.services.ai_fleet.gate import human_decide, review_content, schedule_block_reason
from src.services.ai_fleet.pipeline import (
    IMAGE_PROVIDERS,
    TEXT_PROVIDERS,
    attach_post_image,
    create_batch,
    create_post,
    delete_post,
    note_review,
    page_report,
    resume_task,
    run_automatic,
    place_approved_post,
    save_post_edit,
    sync_approved_job,
)
from src.services.ai_fleet.store import FleetStore
from src.services.ai_fleet.tasks import can_retry, counts, recover_running, retry_task

LANGUAGES = (
    ("vi", "Tiếng Việt"),
    ("en", "English"),
    ("en-US", "English (US)"),
    ("en-GB", "English (UK)"),
    ("pt-BR", "Português (Brasil)"),
    ("pt-PT", "Português (Portugal)"),
    ("es", "Español"),
    ("es-MX", "Español (México)"),
    ("fr", "Français"),
    ("de", "Deutsch"),
    ("it", "Italiano"),
    ("nl", "Nederlands"),
    ("pl", "Polski"),
    ("ru", "Русский"),
    ("uk", "Українська"),
    ("tr", "Türkçe"),
    ("ar", "العربية"),
    ("hi", "हिन्दी"),
    ("id", "Bahasa Indonesia"),
    ("ms", "Bahasa Melayu"),
    ("th", "ไทย"),
    ("tl", "Filipino"),
    ("ja", "日本語"),
    ("ko", "한국어"),
    ("zh", "中文"),
    ("zh-TW", "中文（繁體）"),
    ("sv", "Svenska"),
    ("da", "Dansk"),
    ("fi", "Suomi"),
    ("cs", "Čeština"),
    ("ro", "Română"),
    ("hu", "Magyar"),
    ("el", "Ελληνικά"),
    ("he", "עברית"),
)
VOICES = (
    "tự nhiên",
    "thân mật",
    "chuyên gia",
    "hài hước",
    "ngắn gọn",
    "trang trọng",
    "gây tò mò",
    "gây chú ý",
    "đồng cảm hoàn cảnh",
    "gây tranh cãi",
    "thẳng thắn",
    "kể chuyện",
    "truyền cảm hứng",
    "cấp bách",
    "gần gũi đời thường",
    "hỏi và mời bình luận",
)
DAILY_CHOICES = tuple(str(number) for number in range(1, 11))
POST_HOUR_CHOICES = (
    "Tự rải trong ngày",
    "09:00",
    "09:00, 18:00",
    "08:00, 13:00, 19:00",
    "09:00, 15:00, 21:00",
    "07:00, 12:00, 18:00, 22:00",
)
SCORE_CHOICES = ("60", "70", "80", "90")
REVISION_CHOICES = ("1", "2", "3")
STATUS_LABELS = {"ACTIVE": "Đang chạy", "PAUSED": "Tạm dừng"}
BUDGET_CHOICES = ("0 — không giới hạn", "500", "1000", "2000", "5000")
TASK_FILTERS = (
    ("", "Mọi trạng thái"),
    ("queued", "Hàng chờ"),
    ("running", "Đang chạy"),
    ("waiting_approval", "Chờ duyệt"),
    ("succeeded", "Xong"),
    ("failed", "Lỗi"),
    ("cancelled", "Đã hủy"),
)
CONTENT_LABELS = {
    "PENDING_REVIEW": "Chờ duyệt",
    "APPROVED": "Đã duyệt",
    "REJECTED": "Từ chối",
    "REVISION_REQUIRED": "Cần sửa",
    "HUMAN_REVIEW_REQUIRED": "Cần người duyệt",
    "RECHECK_REQUIRED": "Duyệt lại",
    "PUBLISHED": "Đã đăng",
}
ERROR_LABELS = {
    "retryable": "Lỗi mạng",
    "rate_limit": "Quá giới hạn",
    "authentication": "Sai khóa",
    "configuration": "Thiếu cấu hình",
    "permanent": "Lỗi cố định",
}


def _language_label(code: str) -> str:
    for key, label in LANGUAGES:
        if key == code:
            return f"{key} — {label}"
    return code


def _language_code(label: str) -> str:
    text = str(label or "").strip()
    if " — " in text:
        return text.split(" — ", 1)[0].strip()
    return text


def _status_label(code: str) -> str:
    return STATUS_LABELS.get(str(code or "").upper(), "Chưa lưu")


def _merge_hours(current: str, pick: str) -> str:
    """Ghép một mốc vào lịch đang có. Mốc có dấu phẩy thì thay cả lịch."""
    choice = str(pick or "").strip()
    if choice in {"", "Thêm giờ"}:
        return str(current or "").strip()
    if choice == "Tự rải trong ngày":
        return choice
    if "," in choice:
        return _normalize_hours(choice) or choice
    parts = [part for part in _normalize_hours(current).split(", ") if part]
    clock = _normalize_hours(choice) or choice
    if clock not in parts:
        parts.append(clock)
    parts.sort()
    return ", ".join(parts)


def _normalize_hours(text: str) -> str:
    """Chuẩn hóa chuỗi giờ người dùng gõ, ví dụ 8:00, 13:30."""
    found: list[str] = []
    seen: set[str] = set()
    for hour_text, minute_text in re.findall(r"(\d{1,2}):(\d{2})", text or ""):
        hour, minute = int(hour_text), int(minute_text)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            continue
        item = f"{hour:02d}:{minute:02d}"
        if item in seen:
            continue
        seen.add(item)
        found.append(item)
    return ", ".join(found)


def _status_code(label: str) -> str:
    for code, text in STATUS_LABELS.items():
        if text == label:
            return code
    raw = str(label or "").strip().upper()
    return raw if raw in STATUS_LABELS else "ACTIVE"


def _budget_number(label: str) -> int:
    digits = "".join(ch for ch in str(label or "") if ch.isdigit())
    return int(digits or "0")


def build_ai_fleet_tab(parent: ttk.Frame, root: tk.Misc, on_scheduler_start=None) -> None:
    """Ghép màn quản lý agent vào notebook chính."""
    AiFleetTab(parent, root, on_scheduler_start=on_scheduler_start)


class AiFleetTab:
    """Bốn bước trên một tab. Bài chưa duyệt không được đưa vào lịch."""

    def __init__(self, parent: ttk.Frame, root: tk.Misc, on_scheduler_start=None) -> None:
        self.parent = parent
        self.root = root
        self._on_scheduler_start = on_scheduler_start
        self.store = FleetStore()
        self._pages: list[dict[str, str]] = []
        self._chosen_page_id = ""
        self._pages_tree_gen = 0
        self._build()
        self.refresh()
        parent.reload_from_sync = self.refresh

    def _build(self) -> None:
        parent = self.parent
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)
        guide = ttk.Label(
            parent,
            text="Lịch tự viết, tự duyệt và tự đăng. Mở Agent tổng khi có dòng cần xem.",
        )
        guide.grid(row=0, column=0, sticky="w", padx=8, pady=(4, 0))
        book = ttk.Notebook(parent)
        self._book = book
        book.grid(row=1, column=0, sticky="nsew", padx=4, pady=4)
        self._build_master(book)
        self._build_pages(book)
        self._build_writer(book)
        self._build_review(book)

    def _build_master(self, book: ttk.Notebook) -> None:
        frame = ttk.Frame(book, padding=12)
        book.add(frame, text="  Agent tổng  ")
        frame.columnconfigure(0, weight=1)
        ttk.Label(
            frame,
            text="Agent tổng theo dõi Page, dừng khẩn cấp và không cho đăng bài chưa duyệt.",
            wraplength=720,
        ).grid(row=0, column=0, sticky="w")
        self.master_status = tk.StringVar()
        ttk.Label(frame, textvariable=self.master_status, wraplength=720).grid(row=1, column=0, sticky="w", pady=8)
        actions = ttk.Frame(frame)
        actions.grid(row=2, column=0, sticky="w")
        ttk.Button(actions, text="Dừng khẩn cấp", command=lambda: self._set_stop(True)).pack(side=tk.LEFT)
        ttk.Button(actions, text="Cho chạy lại", command=lambda: self._set_stop(False)).pack(side=tk.LEFT, padx=8)
        ttk.Button(actions, text="Làm mới", command=self.refresh).pack(side=tk.LEFT)
        login = ttk.LabelFrame(frame, text="Tài khoản viết bài", padding=8)
        login.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        login.columnconfigure(1, weight=1)
        self.login_kind = tk.StringVar(value="ChatGPT")
        self.login_key = tk.StringVar()
        self.login_status = tk.StringVar(value="Dán khóa ChatGPT, bấm Kiểm tra và lưu. Tài khoản đang chọn vừa viết bài vừa tạo ảnh.")
        ttk.Label(login, text="Loại").grid(row=0, column=0, sticky="w", pady=3)
        ttk.Combobox(login, textvariable=self.login_kind, values=("ChatGPT", "Gemini"), state="readonly", width=16).grid(row=0, column=1, sticky="w")
        ttk.Label(login, text="Khóa").grid(row=1, column=0, sticky="w", pady=3)
        self.login_entry = ttk.Entry(login, textvariable=self.login_key, show="*")
        self.login_entry.grid(row=1, column=1, sticky="ew", padx=(0, 8))
        login_actions = ttk.Frame(login)
        login_actions.grid(row=1, column=2, sticky="w")
        ttk.Button(login_actions, text="Kiểm tra và lưu", command=self._check_and_save).pack(side=tk.LEFT)
        ttk.Button(login_actions, text="Mở đăng nhập", command=self._open_login_selected).pack(side=tk.LEFT, padx=6)
        ttk.Button(login_actions, text="Mở ChatGPT", command=self._open_chatgpt_browser).pack(side=tk.LEFT)
        ttk.Button(login_actions, text="Xóa tài khoản", command=self._delete_ready_accounts).pack(side=tk.LEFT, padx=6)
        ttk.Label(login, textvariable=self.login_status, wraplength=720).grid(row=2, column=0, columnspan=3, sticky="w", pady=(4, 0))
        orders = ttk.LabelFrame(frame, text="Lệnh cho agent tổng", padding=8)
        orders.grid(row=4, column=0, sticky="ew", pady=(12, 0))
        orders.columnconfigure(0, weight=1)
        self.master_order = tk.Text(orders, height=4, wrap="word", font=("Segoe UI", 10))
        self.master_order.grid(row=0, column=0, sticky="ew")
        order_actions = ttk.Frame(orders)
        order_actions.grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Button(order_actions, text="Áp dụng ngay", command=self._apply_master_order).pack(side=tk.LEFT)
        ttk.Button(order_actions, text="Xóa lệnh", command=self._clear_master_order).pack(side=tk.LEFT, padx=8)
        self.order_status = tk.StringVar(value="Chưa có lệnh. Lệnh mới được dùng cho bài viết tiếp theo, không cần tắt app.")
        ttk.Label(orders, textvariable=self.order_status, wraplength=720).grid(row=2, column=0, sticky="w", pady=(4, 0))
        ttk.Label(
            frame,
            text="Hàng tác vụ lấy từ nhật ký thật. Tác vụ đang chạy dở được đưa lại hàng chờ khi mở app, không bị tính là thành công.",
            wraplength=720,
        ).grid(row=5, column=0, sticky="w", pady=(12, 4))
        filters = ttk.Frame(frame)
        filters.grid(row=6, column=0, sticky="ew")
        filters.columnconfigure(1, weight=1)
        self.task_state = tk.StringVar(value="Mọi trạng thái")
        ttk.Label(filters, text="Lọc").grid(row=0, column=0, sticky="w")
        state_box = ttk.Combobox(
            filters,
            textvariable=self.task_state,
            values=tuple(label for _code, label in TASK_FILTERS),
            state="readonly",
            width=18,
        )
        state_box.grid(row=0, column=1, sticky="w", padx=6)
        state_box.bind("<<ComboboxSelected>>", lambda _event: self._fill_tasks())
        self.task_query = tk.StringVar()
        ttk.Entry(filters, textvariable=self.task_query).grid(row=0, column=2, sticky="ew", padx=(8, 0))
        self.task_query.trace_add("write", lambda *_args: self._fill_tasks())
        self.task_tree = ttk.Treeview(frame, columns=("stt", "state", "page", "error", "note"), show="headings", height=8)
        for key, title, width in (
            ("stt", "STT", 48),
            ("state", "Trạng thái", 130),
            ("page", "Page", 200),
            ("error", "Loại lỗi", 120),
            ("note", "Ghi chú", 320),
        ):
            self.task_tree.heading(key, text=title)
            self.task_tree.column(key, width=width, anchor=tk.CENTER if key == "stt" else tk.W, stretch=key != "stt")
        self.task_tree.grid(row=7, column=0, sticky="nsew", pady=4)
        frame.rowconfigure(7, weight=1)
        task_actions = ttk.Frame(frame)
        task_actions.grid(row=8, column=0, sticky="w")
        ttk.Button(task_actions, text="Thử lại tác vụ được phép", command=self._retry_task).pack(side=tk.LEFT)
        ttk.Button(task_actions, text="Xóa dòng đang chọn", command=self._delete_selected_tasks).pack(side=tk.LEFT, padx=8)
        ttk.Button(task_actions, text="Xóa hết lỗi", command=self._delete_failed_tasks).pack(side=tk.LEFT)

    def _build_pages(self, book: ttk.Notebook) -> None:
        frame = ttk.Frame(book, padding=8)
        book.add(frame, text="  Page  ")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)
        bar = ttk.Frame(frame)
        bar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(bar, text="Tài khoản").pack(side=tk.LEFT)
        self.account_pick = tk.StringVar(value="Tất cả tài khoản")
        self._account_by_label: dict[str, str] = {"Tất cả tài khoản": ""}
        self.account_box = ttk.Combobox(bar, textvariable=self.account_pick, state="readonly", width=28)
        self.account_box.pack(side=tk.LEFT, padx=(6, 12))
        self.account_box.bind("<<ComboboxSelected>>", self._on_fleet_account_picked)
        ttk.Label(bar, text="Trạng thái").pack(side=tk.LEFT)
        self.page_status_filter = tk.StringVar(value="Mọi trạng thái")
        status_filter = ttk.Combobox(
            bar,
            textvariable=self.page_status_filter,
            values=("Mọi trạng thái", "Đang chạy", "Tạm dừng", "Chưa lưu"),
            state="readonly",
            width=16,
        )
        status_filter.pack(side=tk.LEFT, padx=(6, 12))
        status_filter.bind("<<ComboboxSelected>>", lambda _event: self._fill_pages())
        self.page_category_filter = PageCategoryFilter(bar, on_change=self._fill_pages, width=20)
        self.page_category_filter.frame.pack(side=tk.LEFT, padx=(0, 12))
        ttk.Label(bar, text="Tìm").pack(side=tk.LEFT)
        self.page_query = tk.StringVar()
        ttk.Entry(bar, textvariable=self.page_query, width=28).pack(side=tk.LEFT, padx=6)
        self.page_query.trace_add("write", lambda *_args: self._fill_pages())
        self.page_count = tk.StringVar(value="")
        ttk.Label(bar, textvariable=self.page_count).pack(side=tk.LEFT, padx=8)
        panes = ttk.Panedwindow(frame, orient=tk.HORIZONTAL)
        panes.grid(row=1, column=0, sticky="nsew")
        list_wrap = ttk.Frame(panes, padding=(0, 0, 8, 0))
        list_wrap.rowconfigure(0, weight=1)
        list_wrap.columnconfigure(0, weight=1)
        self.page_tree = ttk.Treeview(
            list_wrap, columns=("stt", "name", "category", "account", "status"), show="headings", height=12
        )
        for key, title, width, stretch in (
            ("stt", "STT", 48, False),
            ("name", "Page", 180, True),
            ("category", "Phân loại", 96, False),
            ("account", "Tài khoản", 130, True),
            ("status", "Trạng thái", 90, False),
        ):
            self.page_tree.heading(key, text=title)
            self.page_tree.column(key, width=width, minwidth=48 if key == "stt" else 80, stretch=stretch, anchor=tk.CENTER if key == "stt" else tk.W)
        self.page_tree.grid(row=0, column=0, sticky="nsew")
        page_scroll = ttk.Scrollbar(list_wrap, orient="vertical", command=self.page_tree.yview)
        page_scroll.grid(row=0, column=1, sticky="ns")
        self.page_tree.configure(yscrollcommand=page_scroll.set)
        self.page_tree.bind("<<TreeviewSelect>>", lambda _event: self._load_selected_page())
        panes.add(list_wrap, weight=2)
        form_host, form_inner = self._scroll_host(panes)
        panes.add(form_host, weight=3)
        form_inner.columnconfigure(0, weight=1)
        form = ttk.LabelFrame(form_inner, text="Hồ sơ agent của Page đang chọn", padding=12)
        form.grid(row=0, column=0, sticky="nsew")
        form.columnconfigure(1, weight=1)
        self.page_name = tk.StringVar()
        self.language = tk.StringVar(value=_language_label("vi"))
        self.voice = tk.StringVar(value="tự nhiên")
        self.topic_pick = tk.StringVar()
        self.topic_edit = tk.StringVar()
        self._page_topics: list[str] = []
        self.daily = tk.StringVar(value="1")
        self.post_hours = tk.StringVar(value="Tự rải trong ngày")
        self.page_status = tk.StringVar(value="Đang chạy")
        self.min_score = tk.StringVar(value="80")
        self.max_rev = tk.StringVar(value="2")
        self.banned = tk.StringVar()
        self.require_source = tk.BooleanVar(value=False)
        self.require_media = tk.BooleanVar(value=False)
        fields = (
            ("Tên Page", ttk.Entry(form, textvariable=self.page_name)),
            ("Ngôn ngữ", ttk.Combobox(form, textvariable=self.language, values=tuple(_language_label(code) for code, _label in LANGUAGES), state="readonly")),
            ("Giọng văn", ttk.Combobox(form, textvariable=self.voice, values=VOICES)),
            ("Số bài mỗi ngày", ttk.Combobox(form, textvariable=self.daily, values=DAILY_CHOICES, state="readonly", width=8)),
        )
        for index, (label, widget) in enumerate(fields):
            ttk.Label(form, text=label).grid(row=index, column=0, sticky="w", pady=4, padx=(0, 8))
            widget.grid(row=index, column=1, sticky="ew", pady=4)
        hour_row = len(fields)
        ttk.Label(form, text="Giờ đăng").grid(row=hour_row, column=0, sticky="w", pady=4, padx=(0, 8))
        hour_box = ttk.Frame(form)
        hour_box.grid(row=hour_row, column=1, sticky="ew", pady=4)
        hour_box.columnconfigure(0, weight=1)
        self.hour_box = ttk.Entry(hour_box, textvariable=self.post_hours)
        self.hour_box.grid(row=0, column=0, sticky="ew")
        self.hour_box.bind("<Return>", lambda _event: self._save_hour_frame())
        self.hour_add = tk.StringVar(value="Thêm giờ")
        self.hour_add_box = ttk.Combobox(hour_box, textvariable=self.hour_add, state="readonly", width=14)
        self.hour_add_box.grid(row=0, column=1, padx=(6, 0))
        self.hour_add_box.bind("<<ComboboxSelected>>", lambda _event: self._append_picked_hour())
        ttk.Button(hour_box, text="Lưu khung giờ", command=self._save_hour_frame).grid(row=0, column=2, padx=(6, 0))
        ttk.Button(hour_box, text="Xóa khung", command=self._delete_hour_frame).grid(row=0, column=3, padx=(6, 0))
        self.hour_saved = tk.StringVar(value="Chọn khung đã lưu")
        self.hour_saved_box = ttk.Combobox(hour_box, textvariable=self.hour_saved, state="readonly")
        self.hour_saved_box.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(4, 0))
        self.hour_saved_box.bind("<<ComboboxSelected>>", lambda _event: self._apply_saved_frame())
        rest = (
            ("Điểm duyệt", ttk.Combobox(form, textvariable=self.min_score, values=SCORE_CHOICES, state="readonly", width=8)),
            ("Số lần sửa", ttk.Combobox(form, textvariable=self.max_rev, values=REVISION_CHOICES, state="readonly", width=8)),
            ("Trạng thái", ttk.Combobox(form, textvariable=self.page_status, values=tuple(STATUS_LABELS.values()), state="readonly", width=14)),
        )
        for offset, (label, widget) in enumerate(rest, start=1):
            ttk.Label(form, text=label).grid(row=hour_row + offset, column=0, sticky="w", pady=4, padx=(0, 8))
            widget.grid(row=hour_row + offset, column=1, sticky="ew", pady=4)
        fields_end = hour_row + len(rest)
        ttk.Label(form, text="Chủ đề").grid(row=fields_end + 1, column=0, sticky="nw", pady=4)
        topic_box = ttk.Frame(form)
        topic_box.grid(row=fields_end + 1, column=1, sticky="ew", pady=4)
        topic_box.columnconfigure(0, weight=1)
        self.topic_combo = ttk.Combobox(topic_box, textvariable=self.topic_pick)
        self.topic_combo.grid(row=0, column=0, sticky="ew")
        ttk.Button(topic_box, text="Thêm", command=self._add_topic).grid(row=0, column=1, padx=(6, 0))
        self.topic_list = tk.Listbox(topic_box, height=5, exportselection=False)
        self.topic_list.grid(row=1, column=0, columnspan=2, sticky="ew", pady=4)
        self.topic_list.bind("<<ListboxSelect>>", lambda _event: self._fill_topic_edit())
        edit_row = ttk.Frame(topic_box)
        edit_row.grid(row=2, column=0, columnspan=2, sticky="ew")
        edit_row.columnconfigure(0, weight=1)
        ttk.Entry(edit_row, textvariable=self.topic_edit).grid(row=0, column=0, sticky="ew")
        ttk.Button(edit_row, text="Sửa", command=self._rename_topic).grid(row=0, column=1, padx=6)
        ttk.Button(edit_row, text="Xóa", command=self._delete_topic).grid(row=0, column=2)
        ttk.Label(form, text="Chủ đề cấm").grid(row=fields_end + 2, column=0, sticky="nw", pady=4)
        ttk.Entry(form, textvariable=self.banned).grid(row=fields_end + 2, column=1, sticky="ew", pady=4)
        flags = ttk.Frame(form)
        flags.grid(row=fields_end + 3, column=1, sticky="w", pady=6)
        ttk.Checkbutton(flags, text="Bắt buộc có nguồn", variable=self.require_source).pack(side=tk.LEFT)
        ttk.Checkbutton(flags, text="Bắt buộc có ảnh", variable=self.require_media).pack(side=tk.LEFT, padx=12)
        ttk.Button(form, text="Lưu agent Page", command=self._save_page).grid(row=fields_end + 4, column=1, sticky="w", pady=(8, 4))

    def _scroll_host(self, parent: tk.Misc) -> tuple[ttk.Frame, ttk.Frame]:
        """Khung cuộn dọc. Form dài vẫn xem được, không đè lên danh sách."""
        host = ttk.Frame(parent)
        host.columnconfigure(0, weight=1)
        host.rowconfigure(0, weight=1)
        canvas = tk.Canvas(host, highlightthickness=0, borderwidth=0)
        bar = ttk.Scrollbar(host, orient="vertical", command=canvas.yview)
        inner = ttk.Frame(canvas)
        window = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _fit_width(event: tk.Event) -> None:
            canvas.itemconfigure(window, width=event.width)

        def _fit_scroll(_event: tk.Event) -> None:
            canvas.configure(scrollregion=canvas.bbox("all"))

        canvas.bind("<Configure>", _fit_width)
        inner.bind("<Configure>", _fit_scroll)
        canvas.configure(yscrollcommand=bar.set)
        canvas.grid(row=0, column=0, sticky="nsew")
        bar.grid(row=0, column=1, sticky="ns")

        def _wheel(event: tk.Event) -> None:
            canvas.yview_scroll(int(-event.delta / 120), "units")

        def _bind(_event: tk.Event) -> None:
            canvas.bind_all("<MouseWheel>", _wheel)

        def _unbind(_event: tk.Event) -> None:
            canvas.unbind_all("<MouseWheel>")

        canvas.bind("<Enter>", _bind)
        canvas.bind("<Leave>", _unbind)
        return host, inner

    def _build_writer(self, book: ttk.Notebook) -> None:
        outer = ttk.Frame(book, padding=8)
        book.add(outer, text="  Viết bài  ")
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)
        host, frame = self._scroll_host(outer)
        host.grid(row=0, column=0, sticky="nsew")
        frame.columnconfigure(1, weight=1)
        pick = ttk.LabelFrame(frame, text="Chọn tài khoản đã kiểm tra đạt. Bấm một hoặc nhiều dòng rồi bật tự động.", padding=8)
        pick.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        pick.columnconfigure(0, weight=1)
        self._load_saved_keys()
        choices = self.store.provider_choices()
        self.text_provider = tk.StringVar(value=choices["text"] if choices["text"] in TEXT_PROVIDERS else "openai")
        self.image_provider = tk.StringVar(value=choices["image"] if choices["image"] in IMAGE_PROVIDERS else "openai")
        self.make_image = tk.BooleanVar(value=bool(choices.get("make_image", True)))
        self.auto_all = tk.BooleanVar(value=False)
        self.max_items = tk.StringVar(value="3")
        self.token_budget = tk.StringVar(value="0")
        self.link_status = tk.StringVar(value="Chưa có tài khoản đạt. Mở tab Agent tổng để kiểm tra.")
        self._ready_rows: list[dict[str, str]] = []
        ready_bar = ttk.Frame(pick)
        ready_bar.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        ttk.Label(ready_bar, text="Lọc").pack(side=tk.LEFT)
        self.ready_kind = tk.StringVar(value="Mọi nhà")
        ready_kind = ttk.Combobox(
            ready_bar,
            textvariable=self.ready_kind,
            values=("Mọi nhà", "ChatGPT", "Gemini"),
            state="readonly",
            width=12,
        )
        ready_kind.pack(side=tk.LEFT, padx=6)
        ready_kind.bind("<<ComboboxSelected>>", lambda _event: self._fill_ready_accounts())
        self.ready_query = tk.StringVar()
        ttk.Entry(ready_bar, textvariable=self.ready_query, width=28).pack(side=tk.LEFT)
        self.ready_query.trace_add("write", lambda *_args: self._fill_ready_accounts())
        self.ready_list = tk.Listbox(pick, height=6, selectmode=tk.MULTIPLE, exportselection=False, font=("Segoe UI", 10))
        self.ready_list.grid(row=1, column=0, columnspan=2, sticky="ew")
        self.ready_list.bind("<<ListboxSelect>>", lambda _event: self._save_ready_selection())
        flags = ttk.Frame(pick)
        flags.grid(row=2, column=0, columnspan=2, sticky="w", pady=(6, 0))
        ttk.Checkbutton(flags, text="Ảnh đi kèm để agent tổng duyệt", variable=self.make_image).pack(side=tk.LEFT)
        ttk.Checkbutton(flags, text="Khi bật, nhận mọi Page (theo Phân loại đang lọc)", variable=self.auto_all).pack(side=tk.LEFT, padx=12)
        ttk.Button(flags, text="Mở ChatGPT", command=self._open_chatgpt_browser).pack(side=tk.LEFT, padx=12)
        ttk.Button(flags, text="Thêm tài khoản", command=self._focus_new_account).pack(side=tk.LEFT)
        ttk.Button(flags, text="Xóa tài khoản", command=self._delete_ready_accounts).pack(side=tk.LEFT, padx=8)
        ttk.Label(pick, textvariable=self.link_status, wraplength=720).grid(row=3, column=0, columnspan=2, sticky="w", pady=(4, 0))
        self._fill_ready_accounts()
        self.chosen_page_label = tk.StringVar(value="Chưa chọn Page ở tab Page.")
        auto_row = ttk.Frame(frame)
        auto_row.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(4, 8))
        self.auto_switch = tk.Button(
            auto_row,
            text="Tự động: TẮT",
            command=self._toggle_auto,
            font=("Segoe UI", 9, "bold"),
            width=16,
            relief="raised",
            bd=1,
        )
        self.auto_switch.pack(side=tk.LEFT, padx=(0, 8))
        self.test_write_button = ttk.Button(auto_row, text="Test tạo bài viết", command=self._test_write)
        self.test_write_button.pack(side=tk.LEFT, padx=(0, 12))
        ttk.Label(auto_row, textvariable=self.chosen_page_label, wraplength=420).pack(side=tk.LEFT, fill=tk.X, expand=True)
        self._paint_auto_switch()
        ttk.Label(frame, text="Nguồn (tuỳ chọn)").grid(row=2, column=0, sticky="w", pady=6)
        ttk.Label(frame, text="Viết tay khi cần một chủ đề riêng. Luồng chính là nút chạy tự động phía trên.", wraplength=720).grid(
            row=7, column=0, columnspan=2, sticky="w"
        )
        self.source_url = tk.StringVar()
        ttk.Entry(frame, textvariable=self.source_url).grid(row=2, column=1, sticky="ew", pady=6)
        ttk.Label(frame, text="Chủ đề, mỗi dòng một bài").grid(row=3, column=0, sticky="w", pady=6)
        self.topic_box = tk.Text(frame, height=4, wrap="word", font=("Segoe UI", 10))
        self.topic_box.grid(row=3, column=1, sticky="ew", pady=6)
        limits = ttk.Frame(frame)
        limits.grid(row=4, column=1, sticky="w")
        ttk.Label(limits, text="Tối đa").pack(side=tk.LEFT)
        ttk.Combobox(limits, textvariable=self.max_items, values=DAILY_CHOICES, state="readonly", width=4).pack(side=tk.LEFT, padx=4)
        ttk.Label(limits, text="bài. Ngân sách token đầu ra").pack(side=tk.LEFT)
        self.token_budget.set(BUDGET_CHOICES[0])
        ttk.Combobox(limits, textvariable=self.token_budget, values=BUDGET_CHOICES, state="readonly", width=18).pack(side=tk.LEFT, padx=4)
        buttons = ttk.Frame(frame)
        buttons.grid(row=5, column=1, sticky="w", pady=6)
        ttk.Button(buttons, text="Viết một bài", command=lambda: self._write(batch=False)).pack(side=tk.LEFT)
        ttk.Button(buttons, text="Viết hàng loạt", command=lambda: self._write(batch=True)).pack(side=tk.LEFT, padx=8)
        self.write_status = tk.StringVar(value="Chưa viết.")
        ttk.Label(frame, textvariable=self.write_status, wraplength=720).grid(row=8, column=0, columnspan=2, sticky="w", pady=8)

    def _build_review(self, book: ttk.Notebook) -> None:
        outer = ttk.Frame(book, padding=8)
        book.add(outer, text="  Duyệt  ")
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)
        host, frame = self._scroll_host(outer)
        host.grid(row=0, column=0, sticky="nsew")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(2, weight=1)
        content_bar = ttk.Frame(frame)
        content_bar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(content_bar, text="Lọc").pack(side=tk.LEFT)
        self.content_status_filter = tk.StringVar(value="Mọi trạng thái")
        content_status = ttk.Combobox(
            content_bar,
            textvariable=self.content_status_filter,
            values=("Mọi trạng thái", *CONTENT_LABELS.values()),
            state="readonly",
            width=16,
        )
        content_status.pack(side=tk.LEFT, padx=6)
        content_status.bind("<<ComboboxSelected>>", lambda _event: self._fill_contents())
        self.content_image_filter = tk.StringVar(value="Mọi ảnh")
        content_image = ttk.Combobox(
            content_bar,
            textvariable=self.content_image_filter,
            values=("Mọi ảnh", "Có ảnh", "Chưa có ảnh"),
            state="readonly",
            width=14,
        )
        content_image.pack(side=tk.LEFT, padx=(0, 6))
        content_image.bind("<<ComboboxSelected>>", lambda _event: self._fill_contents())
        self.content_query = tk.StringVar()
        ttk.Entry(content_bar, textvariable=self.content_query, width=32).pack(side=tk.LEFT)
        self.content_query.trace_add("write", lambda *_args: self._fill_contents())
        self.content_tree = ttk.Treeview(
            frame,
            columns=("stt", "page", "title", "when", "image", "status"),
            show="headings",
            height=6,
        )
        for key, title, width in (
            ("stt", "STT", 48),
            ("page", "Page", 180),
            ("title", "Tiêu đề", 220),
            ("when", "Giờ đăng", 110),
            ("image", "Ảnh", 90),
            ("status", "Trạng thái", 110),
        ):
            self.content_tree.heading(key, text=title)
            self.content_tree.column(key, width=width, anchor=tk.CENTER if key == "stt" else tk.W, stretch=key != "stt")
        self.content_tree.grid(row=1, column=0, sticky="ew")
        self.content_tree.bind("<<TreeviewSelect>>", lambda _event: self._remember_contents())
        editor = ttk.LabelFrame(frame, text="Xem và sửa bài. Phần Caption là chữ sẽ đăng.", padding=8)
        editor.grid(row=2, column=0, sticky="nsew", pady=6)
        editor.columnconfigure(1, weight=1)
        editor.rowconfigure(2, weight=1)
        editor.rowconfigure(4, weight=1)
        self.edit_title = tk.StringVar()
        self.edit_tags = tk.StringVar()
        ttk.Label(editor, text="Tiêu đề").grid(row=0, column=0, sticky="w", pady=3, padx=(0, 8))
        ttk.Entry(editor, textvariable=self.edit_title).grid(row=0, column=1, sticky="ew", pady=3)
        ttk.Label(editor, text="Caption").grid(row=1, column=0, sticky="nw", pady=3, padx=(0, 8))
        self.edit_caption = tk.Text(editor, height=4, wrap="word", font=("Segoe UI", 10))
        self.edit_caption.grid(row=1, column=1, rowspan=2, sticky="nsew", pady=3)
        ttk.Label(editor, text="Nội dung").grid(row=3, column=0, sticky="nw", pady=3, padx=(0, 8))
        self.edit_body = tk.Text(editor, height=4, wrap="word", font=("Segoe UI", 10))
        self.edit_body.grid(row=3, column=1, rowspan=2, sticky="nsew", pady=3)
        ttk.Label(editor, text="Hashtag").grid(row=5, column=0, sticky="w", pady=3, padx=(0, 8))
        ttk.Entry(editor, textvariable=self.edit_tags).grid(row=5, column=1, sticky="ew", pady=3)
        actions = ttk.Frame(frame)
        actions.grid(row=3, column=0, sticky="w")
        ttk.Button(actions, text="Lưu sửa", command=self._save_edit).pack(side=tk.LEFT)
        ttk.Button(actions, text="Tôi duyệt", command=lambda: self._human("APPROVED")).pack(side=tk.LEFT, padx=6)
        ttk.Button(actions, text="Từ chối", command=lambda: self._human("REJECTED")).pack(side=tk.LEFT)
        ttk.Button(actions, text="Chạy duyệt", command=self._review).pack(side=tk.LEFT, padx=6)
        ttk.Button(actions, text="Đăng theo lịch", command=lambda: self._place(immediate=False)).pack(side=tk.LEFT, padx=6)
        ttk.Button(actions, text="Đăng luôn", command=lambda: self._place(immediate=True)).pack(side=tk.LEFT)
        ttk.Button(actions, text="Tạo ảnh", command=self._create_image_for_selected).pack(side=tk.LEFT, padx=6)
        ttk.Button(actions, text="Xóa bài", command=self._delete_posts).pack(side=tk.LEFT)
        ttk.Button(actions, text="Xóa hết bài chưa đăng", command=self._delete_unpublished).pack(side=tk.LEFT, padx=6)
        self.image_preview = ttk.Label(frame, text="Chọn một bài để xem ảnh.")
        self.image_preview.grid(row=4, column=0, sticky="w", pady=(4, 0))
        self._preview_photo = None
        self.review_status = tk.StringVar(value="Chọn một bài để xem và sửa. Sau khi sửa, bấm Tôi duyệt để giữ giờ đăng.")
        ttk.Label(frame, textvariable=self.review_status, wraplength=760).grid(row=5, column=0, sticky="w", pady=6)

    def refresh(self) -> None:
        """Đọc lại Page, hồ sơ agent và hàng chờ duyệt."""
        recover_running(self.store)
        self._pages = self._load_pages()
        self._accounts = self._load_accounts()
        self._fill_accounts()
        self._refresh_hour_choices()
        self._fill_pages()
        self._refresh_topic_choices()
        self._fill_contents()
        doc = self.store.load()
        contents = doc.get("contents") or {}
        choices = doc.get("providers") or {"text": "openai", "image": "openai"}
        stopped = bool(doc.get("emergency_stop"))
        pending = sum(1 for row in contents.values() if row.get("status") != "APPROVED")
        attention = [row for row in (doc.get("attention") or []) if isinstance(row, dict) and str(row.get("message") or "").strip()]
        page_labels = self._page_labels()
        if attention:
            watch = " Cần xem: " + " | ".join(
                f"{self._page_label(str(row.get('page_id') or ''), page_labels)}: {row.get('message')}" for row in attention[:6]
            )
        else:
            watch = " Không có lỗi cần xem. Lịch tự viết và đăng."
        self.master_status.set(
            f"{'Đang dừng khẩn cấp.' if stopped else 'Đang chạy tự động.'} "
            f"{len(doc.get('profiles') or {})} agent Page. {len(contents)} bản thảo, {pending} bản chưa ở trạng thái đã duyệt. "
            f"Viết bài: {choices['text']}. Tạo ảnh: {choices['image']}. Tác vụ: {counts(self.store)}.{watch}"
        )
        self._fill_tasks()
        self._show_master_order()
        self._fill_ready_accounts()
        self._paint_auto_switch()

    def _fill_contents(self) -> None:
        """Đổ bảng duyệt theo bộ lọc. Số thứ tự là số của các dòng đang hiện."""
        if not hasattr(self, "content_tree"):
            return
        contents = self.store.load().get("contents") or {}
        selected = self._selected_content_id()
        slots = self._scheduled_labels()
        page_labels = self._page_labels()
        status_filter = self.content_status_filter.get() if hasattr(self, "content_status_filter") else "Mọi trạng thái"
        image_filter = self.content_image_filter.get() if hasattr(self, "content_image_filter") else "Mọi ảnh"
        query = self.content_query.get().strip().casefold() if hasattr(self, "content_query") else ""
        self.content_tree.delete(*self.content_tree.get_children())
        shown = 0
        for content_id, content in contents.items():
            version = _current(content)
            page_name = self._page_label(str(content.get("page_id") or ""), page_labels)
            title = str((version or {}).get("title") or content_id)
            when = slots.get(content_id) or ""
            image = "Có ảnh" if (version or {}).get("asset_ids") else "Chưa có ảnh"
            status = CONTENT_LABELS.get(str(content.get("status") or ""), str(content.get("status") or ""))
            if status_filter not in {"", "Mọi trạng thái"} and status != status_filter:
                continue
            if image_filter not in {"", "Mọi ảnh"} and image != image_filter:
                continue
            blob = f"{page_name} {title} {when} {image} {status}".casefold()
            if query and query not in blob:
                continue
            shown += 1
            self.content_tree.insert("", tk.END, iid=content_id, values=(shown, page_name, title, when, image, status))
        if selected and self.content_tree.exists(selected):
            self.content_tree.selection_set(selected)
            self.content_tree.see(selected)

    def _fill_accounts(self) -> None:
        current = self.account_pick.get() if hasattr(self, "account_pick") else "Tất cả tài khoản"
        labels = ["Tất cả tài khoản"]
        mapping = {"Tất cả tài khoản": ""}
        used = {"Tất cả tài khoản"}
        for account in getattr(self, "_accounts", []):
            label = account["name"]
            if label in used:
                label = f"{account['name']} · {account['id'][:6]}"
            used.add(label)
            mapping[label] = account["id"]
            labels.append(label)
        self._account_by_label = mapping
        self.account_box["values"] = labels
        if not getattr(self, "_fleet_account_chosen", False) and len(labels) > 1:
            self.account_pick.set(labels[1])
            self._fleet_account_chosen = True
        elif current in mapping:
            self.account_pick.set(current)
        else:
            self.account_pick.set(labels[1] if len(labels) > 1 else labels[0])

    def _on_fleet_account_picked(self, _event: object = None) -> None:
        """Người dùng chọn tài khoản trên tab đội AI, rồi chỉ hiện Page của tài khoản đó."""
        self._fleet_account_chosen = True
        self._fill_pages()

    def _fill_pages(self) -> None:
        if not hasattr(self, "page_tree"):
            return
        profiles = self.store.load().get("profiles") or {}
        query = self.page_query.get().strip().casefold() if hasattr(self, "page_query") else ""
        account_id = self._account_by_label.get(self.account_pick.get(), "") if hasattr(self, "account_pick") else ""
        names = {row["id"]: row["name"] for row in getattr(self, "_accounts", [])}
        tree_delete_all(self.page_tree)
        shown = 0
        scope = [row for row in self._pages if not account_id or row.get("account_id") == account_id]
        self.page_category_filter.refresh(scope)
        specs: list[dict[str, Any]] = []
        for row in scope:
            if not self.page_category_filter.matches(row):
                continue
            category = page_category_label(row.get("page_category")) if row.get("page_category") else "—"
            profile = profiles.get(row["page_id"]) or {}
            status = _status_label(str(profile.get("status") or ""))
            account_name = names.get(row.get("account_id") or "", row.get("account_id") or "")
            wanted = self.page_status_filter.get() if hasattr(self, "page_status_filter") else "Mọi trạng thái"
            if wanted and wanted != "Mọi trạng thái" and status != wanted:
                continue
            blob = f"{row['page_name']} {row['page_id']} {account_name} {status} {category}".casefold()
            if query and query not in blob:
                continue
            shown += 1
            specs.append(
                {
                    "iid": row["page_id"],
                    "values": (shown, row["page_name"], category, account_name, status),
                }
            )
        self._pages_tree_gen += 1
        gen = self._pages_tree_gen

        def _restore() -> None:
            if gen != self._pages_tree_gen:
                return
            if hasattr(self, "page_count"):
                self.page_count.set(f"{shown} Page")
            chosen = getattr(self, "_chosen_page_id", "")
            if chosen and self.page_tree.exists(chosen):
                self.page_tree.selection_set(chosen)
                self.page_tree.see(chosen)

        if len(specs) < ASYNC_PREP_MIN_ROWS:
            for spec in specs:
                try:
                    self.page_tree.insert("", tk.END, **spec)
                except tk.TclError:
                    pass
            _restore()
            return
        tree_insert_chunked(
            self.root,
            self.page_tree,
            specs,
            generation=gen,
            is_current=lambda g: g == self._pages_tree_gen,
            on_complete=_restore,
        )

    def _load_accounts(self) -> list[dict[str, str]]:
        try:
            from src.utils.db_manager import AccountsDatabaseManager

            rows = []
            for row in AccountsDatabaseManager().load_all():
                account_id = str(row.get("id") or "").strip()
                if not account_id:
                    continue
                rows.append({"id": account_id, "name": str(row.get("name") or account_id)})
            return rows
        except Exception as exc:  # noqa: BLE001
            self.master_status.set(f"Không đọc được tài khoản: {exc}")
            return []

    def _clock_choices(self) -> list[str]:
        clocks = [f"{hour:02d}:{minute:02d}" for hour in range(6, 24) for minute in (0, 30)]
        for item in self.store.hour_catalog():
            for part in item.split(","):
                text = part.strip()
                if text and text not in clocks and _normalize_hours(text) == text:
                    clocks.append(text)
        return clocks

    def _saved_frame_choices(self) -> tuple[str, ...]:
        saved = [item for item in self.store.hour_catalog() if item not in POST_HOUR_CHOICES]
        return ("Chọn khung đã lưu", *POST_HOUR_CHOICES, *saved)

    def _refresh_hour_choices(self) -> None:
        if hasattr(self, "hour_add_box"):
            self.hour_add_box["values"] = ("Thêm giờ", *self._clock_choices())
        if hasattr(self, "hour_saved_box"):
            self.hour_saved_box["values"] = self._saved_frame_choices()

    def _append_picked_hour(self) -> None:
        """Chọn một mốc thì gắn vào khung đang soạn, chưa ghi vào danh sách lưu."""
        pick = self.hour_add.get().strip()
        self.hour_add.set("Thêm giờ")
        if pick in {"", "Thêm giờ"}:
            return
        if pick == "Tự rải trong ngày":
            self.post_hours.set(pick)
            return
        self.post_hours.set(_merge_hours(self.post_hours.get(), pick))

    def _apply_saved_frame(self) -> None:
        """Chọn một khung đã lưu để Page dùng khung đó khi đăng."""
        pick = self.hour_saved.get().strip()
        if pick in {"", "Chọn khung đã lưu"}:
            return
        self.post_hours.set(pick)

    def _save_hour_frame(self) -> None:
        """Ghi khung giờ đang soạn để lần sau chọn lại trong danh sách."""
        saved = self._remember_hours(self.post_hours.get())
        if saved:
            self.hour_saved.set(saved)

    def _remember_hours(self, raw: str) -> str | None:
        """Trả về giờ đã chuẩn hóa. None khi người dùng gõ giờ không hợp lệ."""
        text = str(raw or "").strip()
        if text in {"", "Tự rải trong ngày"}:
            return ""
        normalized = _normalize_hours(text)
        if not normalized:
            messagebox.showwarning("Giờ đăng", "Nhập giờ dạng 08:00 hoặc 08:00, 14:30, 20:00.", parent=self.root)
            return None
        self.store.save_hour_catalog([*self.store.hour_catalog(), normalized])
        self.post_hours.set(normalized)
        clocks = [part.strip() for part in normalized.split(",") if part.strip()]
        if clocks:
            self.daily.set(str(min(10, len(clocks))))
        self._apply_hours_to_selected_page(normalized)
        self._refresh_hour_choices()
        return normalized

    def _apply_hours_to_selected_page(self, hours: str) -> None:
        """Ghi khung giờ vào Page đang chọn để mỗi ngày đăng đúng các mốc đó."""
        page_id = self._selected_page_id()
        if not page_id:
            return
        clocks = [part.strip() for part in hours.split(",") if part.strip()]
        self.store.upsert_profile(
            {
                "page_id": page_id,
                "posting_schedule": hours,
                "daily_content_target": max(1, min(10, len(clocks) or 1)),
            }
        )

    def _delete_hour_frame(self) -> None:
        """Bỏ khung đã lưu khỏi danh sách chọn. Giờ Page đang chạy không bị xóa theo."""
        pick = self.hour_saved.get().strip()
        if pick in {"", "Chọn khung đã lưu"} or pick in POST_HOUR_CHOICES:
            messagebox.showwarning("Giờ đăng", "Chọn một khung đã lưu, không phải khung mẫu.", parent=self.root)
            return
        self.store.delete_hour_frame(pick)
        self.hour_saved.set("Chọn khung đã lưu")
        self._refresh_hour_choices()

    def _apply_master_order(self) -> None:
        """Ghi lệnh xuống file ngay. Bài viết sau đọc lại lệnh này, không cần tắt app."""
        text = self.master_order.get("1.0", "end").strip()
        if not text:
            messagebox.showwarning("Agent tổng", "Nhập lệnh trước khi áp dụng.", parent=self.root)
            return
        self.store.set_master_instruction(text)
        self.order_status.set("Đã nhận lệnh. Bài viết tiếp theo sẽ theo lệnh này, kể cả vòng đang chạy.")

    def _clear_master_order(self) -> None:
        self.master_order.delete("1.0", "end")
        self.store.set_master_instruction("")
        self.order_status.set("Đã xóa lệnh đang áp dụng. Các lệnh đã học vẫn được nhớ.")

    def _show_master_order(self) -> None:
        if not hasattr(self, "master_order"):
            return
        try:
            if self.master_order.focus_get() == self.master_order:
                return
        except KeyError:
            return
        stored = self.store.master_instruction()
        current = self.master_order.get("1.0", "end").strip()
        if current == stored:
            if stored:
                self.order_status.set("Lệnh này đang được vòng chạy dùng.")
            return
        self.master_order.delete("1.0", "end")
        if stored:
            self.master_order.insert("1.0", stored)
            self.order_status.set("Lệnh này đang được vòng chạy dùng.")

    def _fill_tasks(self) -> None:
        if not hasattr(self, "task_tree"):
            return
        query = self.task_query.get().strip().casefold()
        selected = next((code for code, label in TASK_FILTERS if label == self.task_state.get()), "")
        self.task_tree.delete(*self.task_tree.get_children())
        state_labels = dict(TASK_FILTERS)
        page_labels = self._page_labels()
        for task in reversed(self.store.tasks()):
            state = str(task.get("state") or "")
            if selected and state != selected:
                continue
            page_name = self._page_label(str(task.get("page_id") or ""), page_labels)
            blob = " ".join(
                str(task.get(key) or "")
                for key in ("state", "page_id", "error_class", "detail", "error", "correlation_id")
            )
            blob = f"{blob} {page_name}"
            if query and query not in blob.casefold():
                continue
            shown = len(self.task_tree.get_children()) + 1
            self.task_tree.insert(
                "",
                tk.END,
                iid=str(task["task_id"]),
                values=(
                    shown,
                    state_labels.get(state, state),
                    page_name,
                    ERROR_LABELS.get(str(task.get("error_class") or ""), task.get("error_class") or ""),
                    task.get("detail") or task.get("error") or "",
                ),
            )

    def _retry_task(self) -> None:
        picked = self.task_tree.selection()
        if not picked:
            return
        task = next((row for row in self.store.tasks() if row.get("task_id") == picked[0]), None)
        if task is None or not can_retry(task):
            messagebox.showwarning("Tác vụ", "Chỉ thử lại lỗi mạng hoặc quá giới hạn tốc độ, tối đa 3 lần.", parent=self.root)
            return
        if not messagebox.askyesno("Tác vụ", "Chạy lại đúng nhà cung cấp của tác vụ này?", parent=self.root):
            return
        task_id = str(picked[0])

        def work() -> None:
            try:
                retry_task(self.store, task_id)
                resume_task(self.store, task_id)
                message = "Đã chạy lại. Xem trạng thái ở bảng tác vụ."
            except Exception as exc:  # noqa: BLE001
                message = str(exc)
            schedule_on_main_thread(self.root, lambda: self._after_retry(message))

        threading.Thread(target=work, name="ai-fleet-retry", daemon=True).start()

    def _delete_selected_tasks(self) -> None:
        picked = list(self.task_tree.selection())
        if not picked:
            messagebox.showwarning("Tác vụ", "Chọn dòng cần xóa.", parent=self.root)
            return
        removed = self.store.delete_tasks(picked)
        self._fill_tasks()
        self.master_status.set(f"Đã xóa {removed} dòng. Dòng đang chạy được giữ lại.")

    def _delete_failed_tasks(self) -> None:
        failed = [str(row.get("task_id") or "") for row in self.store.tasks() if str(row.get("state") or "") == "failed"]
        if not failed:
            self.master_status.set("Không có dòng lỗi để xóa.")
            return
        removed = self.store.delete_tasks(failed)
        self._fill_tasks()
        self.master_status.set(f"Đã xóa {removed} dòng lỗi.")

    def _after_retry(self, message: str) -> None:
        self.refresh()
        if not message.startswith("Đã chạy lại"):
            messagebox.showwarning("Tác vụ", message, parent=self.root)

    def _load_pages(self) -> list[dict[str, str]]:
        try:
            from src.utils.pages_manager import PagesManager

            rows = []
            for row in PagesManager().load_all():
                page_id = str(row.get("id") or "").strip()
                if not page_id:
                    continue
                rows.append(
                    {
                        "page_id": page_id,
                        "page_name": str(row.get("page_name") or page_id),
                        "account_id": str(row.get("account_id") or "").strip(),
                        "topic": str(row.get("topic") or "").strip(),
                        "page_category": normalize_page_category(row.get("page_category")),
                    }
                )
            return rows
        except Exception as exc:  # noqa: BLE001
            self.master_status.set(f"Không đọc được danh sách Page: {exc}")
            return []

    def _page_labels(self) -> dict[str, str]:
        """Mã Page sang tên. Danh sách Page ghi đè tên trong hồ sơ agent."""
        labels: dict[str, str] = {}
        for page_id, profile in (self.store.load().get("profiles") or {}).items():
            name = str((profile or {}).get("page_name") or "").strip()
            if name:
                labels[str(page_id)] = name
        for row in getattr(self, "_pages", []):
            name = str(row.get("page_name") or "").strip()
            if name:
                labels[str(row.get("page_id") or "")] = name
        return labels

    def _page_label(self, page_id: str, labels: dict[str, str] | None = None) -> str:
        """Tên Page để hiện trên bảng. Không có tên thì giữ mã."""
        ident = str(page_id or "").strip()
        if not ident:
            return ""
        table = self._page_labels() if labels is None else labels
        return table.get(ident) or ident

    def _selected_page_id(self) -> str:
        picked = self.page_tree.selection()
        if picked:
            self._chosen_page_id = str(picked[0])
            return self._chosen_page_id
        remembered = str(getattr(self, "_chosen_page_id", "") or "")
        if remembered and any(row["page_id"] == remembered for row in self._pages):
            return remembered
        return ""

    def _load_selected_page(self) -> None:
        picked = self.page_tree.selection()
        if not picked:
            return
        page_id = str(picked[0])
        self._chosen_page_id = page_id
        page = next((row for row in self._pages if row["page_id"] == page_id), None)
        if hasattr(self, "chosen_page_label"):
            self.chosen_page_label.set(f"Page đang chọn: {(page or {}).get('page_name') or page_id}")
        profile = (self.store.load().get("profiles") or {}).get(page_id) or {}
        page = next((row for row in self._pages if row["page_id"] == page_id), None)
        policy = self.store.current_policy(page_id) or {}
        self.page_name.set(str(profile.get("page_name") or (page or {}).get("page_name") or ""))
        self.language.set(_language_label(str(profile.get("language") or "vi")))
        self.voice.set(str(profile.get("brand_voice") or "tự nhiên"))
        self._page_topics = [str(item).strip() for item in (profile.get("topic") or []) if str(item).strip()]
        if not self._page_topics and page and page.get("topic"):
            self._page_topics = [part.strip() for part in str(page["topic"]).split(",") if part.strip()]
        self._show_page_topics()
        self._refresh_topic_choices()
        self.daily.set(str(profile.get("daily_content_target") or 1))
        self.post_hours.set(str(profile.get("posting_schedule") or "").strip() or "Tự rải trong ngày")
        self.page_status.set(_status_label(str(profile.get("status") or "ACTIVE")))
        self.min_score.set(str(policy.get("min_score") or 80))
        self.max_rev.set(str(policy.get("max_revision_attempts") or 2))
        self.banned.set(", ".join(policy.get("prohibited_topics") or profile.get("prohibited_topics") or []))
        self.require_source.set(bool(policy.get("requires_source", False)))
        self.require_media.set(bool(policy.get("requires_media", False)))

    def _save_page(self) -> None:
        page_id = self._selected_page_id()
        if not page_id:
            messagebox.showwarning("Agent Page", "Chọn một Page trong danh sách.", parent=self.root)
            return
        try:
            daily = max(1, int(self.daily.get().strip() or "1"))
            score = max(1, int(self.min_score.get().strip() or "80"))
            revisions = max(1, int(self.max_rev.get().strip() or "2"))
        except ValueError:
            messagebox.showwarning("Agent Page", "Số bài, điểm duyệt và số lần sửa phải là số.", parent=self.root)
            return
        topics = list(self._page_topics)
        banned = [part.strip() for part in self.banned.get().split(",") if part.strip()]
        schedule = self._remember_hours(self.post_hours.get())
        if schedule is None:
            return
        if schedule:
            daily = max(1, min(10, len([part for part in schedule.split(",") if part.strip()])))
            self.daily.set(str(daily))
        self.store.save_topic_catalog([*self.store.topic_catalog(), *topics])
        self.store.upsert_profile(
            {
                "page_id": page_id,
                "page_name": self.page_name.get().strip() or page_id,
                "language": _language_code(self.language.get()),
                "brand_voice": self.voice.get().strip(),
                "topic": topics,
                "daily_content_target": daily,
                "posting_schedule": schedule,
                "status": _status_code(self.page_status.get()),
                "prohibited_topics": banned,
            }
        )
        self.store.add_policy(
            page_id,
            {
                "language": _language_code(self.language.get()),
                "prohibited_topics": banned,
                "min_score": score,
                "max_revision_attempts": revisions,
                "requires_source": bool(self.require_source.get()),
                "requires_media": bool(self.require_media.get()),
            },
            requested_by="user",
        )
        self.refresh()
        self._refresh_topic_choices()
        self.review_status.set("Đã lưu agent Page. Bài chưa đăng của Page này cần duyệt lại nếu policy đổi.")

    def _known_topics(self) -> list[str]:
        found: list[str] = []
        seen: set[str] = set()

        def add(text: str) -> None:
            item = " ".join(str(text or "").split())
            key = item.casefold()
            if not item or key in seen:
                return
            seen.add(key)
            found.append(item)

        for item in self.store.topic_catalog():
            add(item)
        for profile in (self.store.load().get("profiles") or {}).values():
            for item in profile.get("topic") or []:
                add(str(item))
        for row in self._pages:
            for part in str(row.get("topic") or "").split(","):
                add(part)
        for item in self._page_topics:
            add(item)
        return found

    def _refresh_topic_choices(self) -> None:
        if not hasattr(self, "topic_combo"):
            return
        choices = self._known_topics()
        self.topic_combo.configure(values=choices)
        if choices and not self.topic_pick.get().strip():
            self.topic_pick.set(choices[0])

    def _show_page_topics(self) -> None:
        self.topic_list.delete(0, "end")
        for index, item in enumerate(self._page_topics, start=1):
            self.topic_list.insert("end", f"{index}. {item}")

    def _fill_topic_edit(self) -> None:
        picked = self.topic_list.curselection()
        if not picked:
            return
        self.topic_edit.set(self._page_topics[picked[0]])

    def _add_topic(self) -> None:
        text = self.topic_pick.get().strip() or self.topic_edit.get().strip()
        if not text:
            return
        if text.casefold() not in {item.casefold() for item in self._page_topics}:
            self._page_topics.append(text)
            self._show_page_topics()
        self.store.save_topic_catalog([*self.store.topic_catalog(), text])
        self.topic_edit.set("")
        self._refresh_topic_choices()

    def _rename_topic(self) -> None:
        picked = self.topic_list.curselection()
        new = self.topic_edit.get().strip()
        if not picked or not new:
            messagebox.showwarning("Chủ đề", "Chọn một chủ đề và nhập tên mới.", parent=self.root)
            return
        old = self._page_topics[picked[0]]
        try:
            self.store.rename_topic(old, new)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("Chủ đề", str(exc), parent=self.root)
            return
        self._page_topics = [new if item.casefold() == old.casefold() else item for item in self._page_topics]
        self._show_page_topics()
        self._refresh_topic_choices()

    def _delete_topic(self) -> None:
        picked = self.topic_list.curselection()
        if not picked:
            return
        name = self._page_topics[picked[0]]
        everywhere = messagebox.askyesnocancel(
            "Xóa chủ đề",
            f"Xóa “{name}” khỏi mọi Page?\nCó = mọi Page. Không = chỉ Page đang chọn. Hủy = giữ nguyên.",
            parent=self.root,
        )
        if everywhere is None:
            return
        if everywhere:
            self.store.delete_topic(name)
            self._page_topics = [item for item in self._page_topics if item.casefold() != name.casefold()]
        else:
            del self._page_topics[picked[0]]
        self._show_page_topics()
        self._refresh_topic_choices()
        self._show_page_report(page_id)

    def _show_page_report(self, page_id: str) -> None:
        report = page_report(self.store, page_id)
        self.write_status.set(
            f"Page này: {report['counts'] or 'chưa có bài'}. "
            f"Token vào {report['input_tokens']}, token ra {report['output_tokens']}, "
            f"{report['image_calls']} ảnh. Đã ghi nhớ {report['learned']} bài đạt, {report['errors']} bài lỗi."
        )

    def _load_saved_keys(self) -> None:
        try:
            from src.utils.app_secrets import (
                apply_nanobanana_key_to_environ,
                apply_openai_key_to_environ,
                apply_saved_gemini_key_to_environ,
            )

            apply_openai_key_to_environ(None)
            apply_saved_gemini_key_to_environ()
            apply_nanobanana_key_to_environ(None)
        except Exception:
            return

    def _fill_ready_accounts(self) -> None:
        """Chỉ liệt kê tài khoản đã kiểm tra đạt và còn tồn tại trong kho khóa."""
        if not hasattr(self, "ready_list"):
            return
        from src.utils.app_secrets import list_gemini_key_rows_for_ui, list_openai_key_rows_for_ui

        catalogs = {"openai": list_openai_key_rows_for_ui(), "gemini": list_gemini_key_rows_for_ui()}
        names = {"openai": "ChatGPT", "gemini": "Gemini"}
        chosen = self.store.selected_accounts()
        kind_filter = self.ready_kind.get() if hasattr(self, "ready_kind") else "Mọi nhà"
        query = self.ready_query.get().strip().casefold() if hasattr(self, "ready_query") else ""
        self._filling_ready = True
        self.ready_list.delete(0, "end")
        self._ready_rows = []
        for item in self.store.verified_accounts():
            kind = item["kind"]
            row = next((entry for entry in catalogs.get(kind, []) if str(entry.get("id") or "") == item["id"]), None)
            if row is None:
                continue
            label = names.get(kind, kind)
            if kind_filter not in {"", "Mọi nhà"} and label != kind_filter:
                continue
            text = f"{label} — {row.get('label') or 'Tài khoản'} — {row.get('preview') or 'đã che'}"
            if query and query not in text.casefold():
                continue
            self._ready_rows.append({"kind": kind, "id": item["id"]})
            self.ready_list.insert("end", f"{len(self._ready_rows)}. {text}")
            if item["id"] in chosen.get(kind, []) or not any(chosen.values()):
                self.ready_list.selection_set(len(self._ready_rows) - 1)
        if self._ready_rows and not any(chosen.values()):
            self.store.set_selected_accounts(
                [row["id"] for row in self._ready_rows if row["kind"] == "openai"],
                [row["id"] for row in self._ready_rows if row["kind"] == "gemini"],
            )
        self._filling_ready = False
        count = len(self._ready_rows)
        self.link_status.set(
            f"{count} tài khoản đã kiểm tra đạt. Chọn một hoặc nhiều dòng rồi bật tự động."
            if count
            else "Chưa có tài khoản đạt. Mở tab Agent tổng, dán khóa và bấm Kiểm tra và lưu."
        )

    def _image_name(self) -> str:
        """Ảnh và bài dùng cùng tài khoản ChatGPT đang chọn."""
        return "openai"

    def _focus_new_account(self) -> None:
        """Đưa người dùng tới ô khóa để thêm tài khoản mới ngay."""
        self._book.select(self._book.tabs()[0])
        self.login_kind.set("ChatGPT")
        self.login_entry.focus_set()
        self.login_status.set("Dán khóa mới vào ô Khóa, rồi bấm Kiểm tra và lưu. Tài khoản sẽ hiện ở tab Viết bài.")

    def _delete_ready_accounts(self) -> None:
        """Xóa tài khoản đang chọn khỏi danh sách và khỏi kho khóa."""
        if not hasattr(self, "ready_list"):
            return
        indexes = list(self.ready_list.curselection())
        if not indexes:
            messagebox.showwarning("Tài khoản AI", "Chọn tài khoản cần xóa ở tab Viết bài.", parent=self.root)
            return
        rows = [self._ready_rows[index] for index in indexes]
        if not messagebox.askyesno("Tài khoản AI", f"Xóa {len(rows)} tài khoản đã chọn? Có thể thêm khóa mới ngay sau đó.", parent=self.root):
            return
        from src.utils.app_secrets import delete_gemini_key_entry, delete_openai_key_entry

        for row in rows:
            try:
                if row["kind"] == "openai":
                    delete_openai_key_entry(row["id"])
                else:
                    delete_gemini_key_entry(row["id"])
            except Exception:
                pass
            self.store.forget_verified(row["kind"], row["id"])
        self._fill_ready_accounts()
        self.link_status.set("Đã xóa tài khoản. Bấm Thêm tài khoản để dán khóa mới.")
        self.login_status.set("Đã xóa tài khoản. Dán khóa mới rồi bấm Kiểm tra và lưu.")

    def _save_ready_selection(self) -> None:
        if getattr(self, "_filling_ready", False):
            return
        openai_ids: list[str] = []
        gemini_ids: list[str] = []
        for index in self.ready_list.curselection():
            row = self._ready_rows[index]
            if row["kind"] == "openai":
                openai_ids.append(row["id"])
            else:
                gemini_ids.append(row["id"])
        self.store.set_selected_accounts(openai_ids, gemini_ids)
        self.text_provider.set("openai")
        self.image_provider.set("gemini" if gemini_ids and self.make_image.get() else "openai")

    def _checked_kind(self) -> str:
        return "gemini" if self.login_kind.get() == "Gemini" else "openai"

    def _check_and_save(self) -> None:
        kind = self._checked_kind()
        secret = self.login_key.get().strip()
        if not secret:
            messagebox.showwarning("Tài khoản AI", "Dán khóa rồi bấm Kiểm tra và lưu.", parent=self.root)
            return
        self.login_key.set("")
        self.login_status.set("Đang kiểm tra tài khoản. Khóa không được hiện lại.")

        def work() -> None:
            from src.services.ai_fleet.account_check import check_provider_key, existing_key_id

            reason = check_provider_key(kind, secret)
            key_id = ""
            if not reason:
                try:
                    if kind == "openai":
                        from src.utils.app_secrets import add_openai_key_entry

                        key_id = add_openai_key_entry("ChatGPT", secret)
                    else:
                        from src.utils.app_secrets import add_gemini_key_entry

                        key_id = add_gemini_key_entry("Gemini", secret)
                except Exception as exc:  # noqa: BLE001
                    if "đã có" in str(exc):
                        key_id = existing_key_id(kind, secret)
                    else:
                        reason = "Không lưu được tài khoản."
            schedule_on_main_thread(self.root, lambda: self._after_check(kind, key_id, reason))

        threading.Thread(target=work, name="ai-key-check", daemon=True).start()

    def _after_check(self, kind: str, key_id: str, reason: str) -> None:
        if reason or not key_id:
            self.login_status.set(reason or "Kiểm tra chưa đạt. Tài khoản chưa hiện ở tab Viết bài.")
            return
        self.store.mark_verified(kind, key_id)
        self._fill_ready_accounts()
        self.login_status.set("Kiểm tra đạt. Tài khoản đã hiện ở tab Viết bài để chọn.")

    def _open_chatgpt_browser(self) -> None:
        """Mở chatgpt.com khi người dùng bấm nút. Không tự mở nếu chưa cho phép."""
        import os

        os.environ["TOOLFB_ALLOW_CHATGPT_BROWSER"] = "1"
        selected = []
        if hasattr(self, "ready_list"):
            for index in self.ready_list.curselection():
                row = self._ready_rows[index]
                if row["kind"] == "openai":
                    selected.append(row["id"])
        if not selected:
            selected = ["default"]
        selected = selected[:3]
        self._chatgpt_open = getattr(self, "_chatgpt_open", set())
        opened = 0
        for account_id in selected:
            if account_id in self._chatgpt_open:
                continue
            self._chatgpt_open.add(account_id)
            opened += 1
            threading.Thread(
                target=self._chatgpt_browser_work,
                args=(account_id,),
                name=f"chatgpt-browser-{account_id[:8]}",
                daemon=True,
            ).start()
        if opened == 0:
            self.link_status.set("Trình duyệt ChatGPT của tài khoản này đang mở.")
            return
        self.link_status.set("Đã mở ChatGPT. Đăng nhập một lần, lần sau cửa sổ vẫn vào đúng tài khoản. Đóng cửa sổ khi dùng xong.")
        if hasattr(self, "login_status"):
            self.login_status.set("Đã mở ChatGPT. Đăng nhập một lần, phiên được giữ trong thư mục tài khoản.")

    def _chatgpt_browser_work(self, account_id: str) -> None:
        try:
            from src.ai.image_generation import chatgpt_profile_dir, open_nanobanana_login_browser

            open_nanobanana_login_browser(
                wait_sec=0,
                web_url="https://chatgpt.com/",
                profile_dir=chatgpt_profile_dir(account_id),
                fallback_clean=False,
            )
        except Exception as exc:  # noqa: BLE001
            message = "Không mở được ChatGPT. Nếu cửa sổ đó đang mở thì dùng cửa sổ hiện có."
            logger_message = str(exc)
            schedule_on_main_thread(self.root, lambda: self._chatgpt_browser_failed(account_id, message, logger_message))
            return
        schedule_on_main_thread(self.root, lambda: self._chatgpt_browser_closed(account_id))

    def _chatgpt_browser_failed(self, account_id: str, message: str, detail: str) -> None:
        self._chatgpt_open.discard(account_id)
        self.link_status.set(message)
        from loguru import logger

        logger.warning("Mở ChatGPT thất bại: {}", detail[:180])

    def _chatgpt_browser_closed(self, account_id: str) -> None:
        self._chatgpt_open.discard(account_id)
        self.link_status.set("Đã đóng ChatGPT. Lần mở sau vẫn dùng phiên vừa đăng nhập.")

    def _open_login_selected(self) -> None:
        self._open_login(self._checked_kind())

    def _open_login(self, kind: str) -> None:
        if not messagebox.askyesno(
            "Đăng nhập AI",
            "Mở trình duyệt để đăng nhập. Sau khi có khóa, dán vào ô và bấm Kiểm tra và lưu. Tiếp tục?",
            parent=self.root,
        ):
            return
        self.login_status.set("Đang mở trình duyệt đăng nhập. Đóng cửa sổ đó khi đã vào tài khoản, rồi dán khóa để kiểm tra.")

        def work() -> None:
            try:
                from src.ai.image_generation import open_nanobanana_login_browser
                from src.utils.paths import project_root

                if kind == "openai":
                    info = open_nanobanana_login_browser(
                        wait_sec=0,
                        web_url="https://platform.openai.com/api-keys",
                        profile_dir=project_root() / "data" / "ai_accounts" / "openai",
                    )
                else:
                    info = open_nanobanana_login_browser(wait_sec=0)
                title = str((info or {}).get("title") or "").strip()
            except Exception as exc:  # noqa: BLE001
                schedule_on_main_thread(self.root, lambda err=str(exc): self._after_login(kind, "", err))
                return
            schedule_on_main_thread(self.root, lambda name=title: self._after_login(kind, name, ""))

        threading.Thread(target=work, name=f"ai-login-{kind}", daemon=True).start()

    def _after_login(self, kind: str, title: str, error: str) -> None:
        if error:
            messagebox.showwarning("Đăng nhập AI", error, parent=self.root)
            return
        self.login_status.set(
            f"Đã đóng trình duyệt{(' — ' + title) if title else ''}. Dán khóa vào ô rồi bấm Kiểm tra và lưu."
        )

    def _paint_auto_switch(self) -> None:
        if not hasattr(self, "auto_switch"):
            return
        enabled = self.store.auto_enabled()
        self.auto_switch.configure(
            text="Tự động: BẬT" if enabled else "Tự động: TẮT",
            bg="#2e7d32" if enabled else "#b94a48",
            fg="white",
            activebackground="#1b5e20" if enabled else "#8d3a38",
            activeforeground="white",
        )

    def _toggle_auto(self) -> None:
        """Bật thì nhận Page đang chọn và để lịch tự viết, tự đăng. Tắt thì ngừng viết mới."""
        if self.store.auto_enabled():
            self.store.set_auto_enabled(False)
            self._paint_auto_switch()
            self.write_status.set("Đã tắt tự động. Bài đã xếp vẫn đăng đúng giờ.")
            return
        self._run_auto()

    def _pages_for_auto(self) -> list[dict[str, str]]:
        selected = self._selected_page_id()
        if self.auto_all.get():
            category_filter = getattr(self, "page_category_filter", None)
            if category_filter is None:
                return list(self._pages)
            return [row for row in self._pages if category_filter.matches(row)]
        if not selected:
            return []
        return [row for row in self._pages if row["page_id"] == selected]

    def _accept_running_pages(self, pages: list[dict[str, str]]) -> None:
        """Đưa Page được chọn vào trạng thái đang chạy để vòng tự động nhận."""
        selected = self._selected_page_id()
        for page in pages:
            page_id = page["page_id"]
            current = (self.store.load().get("profiles") or {}).get(page_id) or {}
            use_form = page_id == selected
            language = _language_code(self.language.get()) if use_form else str(current.get("language") or "vi")
            language = language or "vi"
            topics = list(self._page_topics) if use_form and self._page_topics else [str(item) for item in (current.get("topic") or []) if str(item).strip()]
            if not topics:
                topics = [str(page.get("page_name") or page_id)]
            try:
                daily = max(1, int(self.daily.get().strip() or "1")) if use_form else max(1, int(current.get("daily_content_target") or 1))
            except ValueError:
                daily = 1
            if use_form and self.post_hours.get().strip() in {"", "Tự rải trong ngày"}:
                hours = ""
            elif use_form:
                hours = _normalize_hours(self.post_hours.get()) or str(current.get("posting_schedule") or "")
            else:
                hours = str(current.get("posting_schedule") or "")
            self.store.upsert_profile(
                {
                    "page_id": page_id,
                    "page_name": str(page.get("page_name") or current.get("page_name") or page_id),
                    "language": language,
                    "brand_voice": self.voice.get().strip() if use_form else str(current.get("brand_voice") or "tự nhiên"),
                    "topic": topics,
                    "daily_content_target": daily,
                    "posting_schedule": hours,
                    "status": "ACTIVE",
                }
            )
            if self.store.current_policy(page_id) is None:
                self.store.add_policy(
                    page_id,
                    {
                        "language": language,
                        "requires_source": bool(self.require_source.get()) if use_form else False,
                        "requires_media": bool(self.require_media.get()) if use_form else False,
                        "min_score": 80,
                        "max_revision_attempts": 2,
                    },
                    requested_by="user",
                )

    def _run_auto(self) -> None:
        self._save_ready_selection()
        chosen = self.store.selected_accounts()
        if not chosen["openai"]:
            messagebox.showwarning(
                "Tự động",
                "Chọn ít nhất một tài khoản ChatGPT đã kiểm tra đạt. Gemini chỉ dùng để tạo ảnh.",
                parent=self.root,
            )
            return
        pages = self._pages_for_auto()
        if not pages:
            messagebox.showwarning("Tự động", "Chọn một Page ở tab Page, hoặc tick nhận mọi Page.", parent=self.root)
            return
        text_name = "openai"
        image_name = self._image_name()
        self.text_provider.set(text_name)
        self.image_provider.set(image_name)
        self.store.set_provider_choices(text=text_name, image=image_name, make_image=bool(self.make_image.get()))
        self._accept_running_pages(pages)
        self.store.set_auto_enabled(True)
        self._paint_auto_switch()
        name = pages[0]["page_name"] if len(pages) == 1 else f"{len(pages)} Page"
        self.chosen_page_label.set(f"Đang nhận: {name}")
        self.write_status.set(f"Đã bật tự động cho {name}. Đang viết bằng {text_name}.")
        if self._on_scheduler_start is not None:
            self._on_scheduler_start()

        def work() -> None:
            try:
                from src.utils.schedule_posts_manager import get_default_schedule_posts_manager

                rows = run_automatic(
                    self.store,
                    pages=pages,
                    text_name=text_name,
                    image_name=image_name,
                    make_image=True,
                    posts=get_default_schedule_posts_manager(),
                    max_output_tokens=_budget_number(self.token_budget.get()),
                )
                queued = [row for row in rows if row.get("result") == "đã vào lịch"]
                if queued and self._on_scheduler_start is not None:
                    schedule_on_main_thread(self.root, self._on_scheduler_start)
                message = " ".join(f"{self._page_label(row['page_id'])}: {row['result']}" for row in rows[:8]) or "Không có Page đang chạy."
            except Exception as exc:  # noqa: BLE001
                message = str(exc)
            schedule_on_main_thread(self.root, lambda: self._after_write(message))

        threading.Thread(target=work, name="ai-fleet-auto", daemon=True).start()

    def _test_write(self) -> None:
        """Viết thử đúng một bài cho Page đang chọn. Không xếp lịch đăng."""
        page_id = self._selected_page_id()
        if not page_id:
            messagebox.showwarning("Test tạo bài viết", "Chọn một Page ở tab Page.", parent=self.root)
            return
        self._save_ready_selection()
        from src.services.ai_fleet.account_check import apply_account_rotation

        apply_account_rotation(self.store, 0)
        if not self.store.selected_accounts()["openai"] and not any(
            row.get("kind") == "openai" for row in self.store.verified_accounts()
        ):
            messagebox.showwarning(
                "Test tạo bài viết",
                "Mở tab Agent tổng, dán khóa ChatGPT rồi bấm Kiểm tra và lưu trước.",
                parent=self.root,
            )
            return
        page = next((row for row in self._pages if row["page_id"] == page_id), {"page_id": page_id, "page_name": page_id})
        profile = (self.store.load().get("profiles") or {}).get(page_id) or {}
        if str(profile.get("status") or "").upper() == "PAUSED":
            messagebox.showwarning("Test tạo bài viết", "Page này đang tạm dừng.", parent=self.root)
            return
        if not profile:
            self._accept_running_pages([page])
            profile = (self.store.load().get("profiles") or {}).get(page_id) or {}
        typed = [line.strip() for line in self.topic_box.get("1.0", "end").splitlines() if line.strip()]
        topics = typed or [str(item).strip() for item in (profile.get("topic") or []) if str(item).strip()]
        topic = topics[0] if topics else str(page.get("page_name") or page_id)
        chosen = self.store.selected_accounts()
        text_name = "openai"
        image_name = self._image_name()
        self.text_provider.set(text_name)
        self.image_provider.set(image_name)
        self.store.set_provider_choices(text=text_name, image=image_name, make_image=bool(self.make_image.get()))
        self.write_status.set(f"Đang test viết «{topic}»…")
        self.test_write_button.configure(state="disabled")

        def work() -> None:
            content_id = ""
            try:
                created = create_post(
                    self.store,
                    page_id=page_id,
                    topic=topic,
                    text_name=text_name,
                    image_name=image_name,
                    make_image=True,
                )
                content = created.get("content") or {}
                content_id = str(content.get("content_id") or "")
                version = (content.get("versions") or [{}])[-1]
                title = str(version.get("title") or topic)
                credit = "ChatGPT hết credit nên viết bằng Gemini. " if created.get("text_provider") == "gemini" else ""
                if created.get("skipped"):
                    message = "Bài này trùng bài đã có nên không viết lại."
                elif created.get("image_error"):
                    message = f"{credit}Chưa lưu bài vì chưa có ảnh: {created['image_error']}"
                elif created.get("asset_id"):
                    message = f"{credit}Đã viết «{title}» và đã tạo ảnh. Mở tab Duyệt để xem."
                elif self.make_image.get():
                    message = f"{credit}Đã viết «{title}». Ảnh chưa được gắn vào bài."
                else:
                    message = f"{credit}Đã viết «{title}». Ô Tự tạo ảnh đang tắt nên chưa tạo ảnh."
            except Exception as exc:  # noqa: BLE001
                message = f"Chưa viết được: {exc}"
            schedule_on_main_thread(self.root, lambda: self._after_write(message, content_id))

        threading.Thread(target=work, name="ai-fleet-test-write", daemon=True).start()

    def _write(self, *, batch: bool) -> None:
        page_id = self._selected_page_id()
        topics = [line.strip() for line in self.topic_box.get("1.0", "end").splitlines() if line.strip()]
        if not page_id or not topics:
            messagebox.showwarning("Viết bài", "Chọn Page ở tab Page và nhập chủ đề.", parent=self.root)
            return
        text_name = self.text_provider.get().strip().lower()
        image_name = self._image_name()
        self.store.set_provider_choices(text=text_name, image=image_name, make_image=bool(self.make_image.get()))
        try:
            limit = max(1, min(10, int(self.max_items.get().strip() or "1")))
            budget = _budget_number(self.token_budget.get())
        except ValueError:
            messagebox.showwarning("Viết bài", "Số bài và ngân sách token phải là số.", parent=self.root)
            return
        source = self.source_url.get().strip()
        self.write_status.set(f"Đang viết bằng {text_name}, ảnh bằng {image_name}…")

        def work() -> None:
            try:
                result = create_batch(
                    self.store,
                    page_id=page_id,
                    topics=topics[:1] if not batch else topics,
                    text_name=text_name,
                    image_name=image_name,
                    make_image=True,
                    source_urls=[source] if source else None,
                    max_items=1 if not batch else limit,
                    max_output_tokens=budget,
                )
                made = [row for row in result["items"] if row.get("content")]
                skipped = [row for row in result["items"] if row.get("skipped")]
                used = "Gemini" if any(row.get("text_provider") == "gemini" for row in result["items"]) else text_name
                credit = "ChatGPT hết credit nên viết bằng Gemini. " if used == "Gemini" and text_name == "openai" else ""
                message = (
                    f"{credit}Đã tạo {len(made)} bài bằng {used}. "
                    f"Ảnh, nếu có, do {image_name} và đã vào thư viện media. "
                    f"Bỏ qua {len(skipped)}. Token ra đã dùng {result['output_tokens']}. Mở tab Duyệt để xem."
                )
            except Exception as exc:  # noqa: BLE001
                message = f"Chưa viết được: {exc}"
            schedule_on_main_thread(self.root, lambda: self._after_write(message))

        threading.Thread(target=work, name="ai-fleet-write", daemon=True).start()

    def _after_write(self, message: str, content_id: str = "") -> None:
        self.write_status.set(message)
        if hasattr(self, "test_write_button"):
            self.test_write_button.configure(state="normal")
        self.refresh()
        if content_id and self.content_tree.exists(content_id):
            self._book.select(self._book.tabs()[-1])
            self.content_tree.selection_set(content_id)
            self.content_tree.see(content_id)
            self._show_content()

    def _selected_content_id(self) -> str:
        picked = self.content_tree.selection()
        return str(picked[0]) if picked else ""

    def _show_content(self) -> None:
        if getattr(self, "_keep_review_status", False):
            return
        content_id = self._selected_content_id()
        content = self.store.content(content_id) or {}
        version = _current(content) or {}
        self.edit_title.set(str(version.get("title") or ""))
        self.edit_tags.set(" ".join(str(item) for item in (version.get("hashtags") or [])))
        self._fill_text(self.edit_caption, str(version.get("caption") or ""))
        self._fill_text(self.edit_body, str(version.get("body") or ""))
        reviews = list(content.get("reviews") or [])
        last = reviews[-1] if reviews else {}
        reason = schedule_block_reason(content_id, self.store) if content_id else "Chọn một bài để xem và sửa."
        notes = [reason or "Bài này đủ điều kiện đăng."]
        slot = self._scheduled_labels().get(content_id)
        if slot:
            notes.append(f"Giờ đăng đã xếp: {slot}.")
        if last.get("summary"):
            notes.append(str(last.get("summary")))
        for check in last.get("checks") or []:
            if check.get("status") == "FAIL":
                notes.append(f"{check.get('criterion')}: {check.get('reason')}")
        self.review_status.set(" ".join(notes))
        self._show_image(version)

    def _show_image(self, version: dict[str, Any]) -> None:
        """Hiện ảnh của bài đang chọn. Chưa có file thì nói rõ."""
        if not hasattr(self, "image_preview"):
            return
        self._preview_photo = None
        self.image_preview.configure(image="", text="Bài chưa có ảnh.")
        asset_id = str(((version or {}).get("asset_ids") or [""])[0] or "")
        if not asset_id:
            return
        from src.utils.media_library import media_library_dir

        path = media_library_dir() / asset_id
        if not path.is_file():
            self.image_preview.configure(text=f"Chưa thấy file ảnh {asset_id}.")
            return
        try:
            from PIL import Image, ImageTk

            image = Image.open(path)
            image.thumbnail((420, 240))
            self._preview_photo = ImageTk.PhotoImage(image)
            self.image_preview.configure(image=self._preview_photo, text="")
        except Exception:
            self.image_preview.configure(text=f"Đã có ảnh: {asset_id}")

    def _remember_contents(self) -> None:
        """Giữ bài đang chọn. Bấm nút xóa làm mất highlight của bảng."""
        picked = [str(item) for item in self.content_tree.selection()]
        if picked:
            self._chosen_content_ids = picked
        self._show_content()

    def _selected_content_ids(self) -> list[str]:
        picked = [str(item) for item in self.content_tree.selection()]
        if picked:
            self._chosen_content_ids = picked
            return picked
        return [item for item in getattr(self, "_chosen_content_ids", []) if self.content_tree.exists(item)]

    def _delete_posts(self) -> None:
        """Xóa các bài đang chọn và gỡ khỏi lịch nếu chưa đăng."""
        content_ids = self._selected_content_ids()
        if not content_ids:
            messagebox.showwarning("Xóa bài", "Chọn một hoặc nhiều bài.", parent=self.root)
            return
        if not messagebox.askyesno("Xóa bài", f"Xóa {len(content_ids)} bài đã chọn? Bài chưa đăng sẽ bị gỡ khỏi lịch.", parent=self.root):
            return
        self._delete_posts_now(content_ids)

    def _delete_unpublished(self) -> None:
        """Xóa mọi bài chưa đăng đang hiện, kể cả khi highlight của bảng bị mất."""
        content_ids = [
            str(content_id)
            for content_id, content in (self.store.load().get("contents") or {}).items()
            if not content.get("published") and str(content.get("status") or "") != "PUBLISHED"
        ]
        if not content_ids:
            self.review_status.set("Không có bài chưa đăng để xóa.")
            return
        if not messagebox.askyesno(
            "Xóa bài",
            f"Xóa {len(content_ids)} bài chưa đăng? Các giờ này sẽ không bị viết lại.",
            parent=self.root,
        ):
            return
        self._delete_posts_now(content_ids)

    def _delete_posts_now(self, content_ids: list[str]) -> None:
        """Gỡ bài khỏi kho và khỏi lịch. Không hỏi lại."""
        from src.utils.schedule_posts_manager import get_default_schedule_posts_manager

        posts = get_default_schedule_posts_manager()
        removed = 0
        errors: list[str] = []
        for content_id in content_ids:
            try:
                delete_post(self.store, content_id, posts)
                removed += 1
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
        self._chosen_content_ids = []
        self._keep_review_status = True
        self.refresh()
        self.review_status.set(
            f"Đã xóa {removed} bài. Giờ đã xóa sẽ không bị viết lại."
            + (f" {errors[0]}" if errors else "")
        )
        self._keep_review_status = False

    def _create_image_for_selected(self) -> None:
        """Tạo ảnh cho các bài đang chọn. Bài đã duyệt vẫn giữ giờ đăng."""
        content_ids = self._selected_content_ids()
        if not content_ids:
            messagebox.showwarning("Tạo ảnh", "Chọn một hoặc nhiều bài.", parent=self.root)
            return
        blocked = [
            content_id
            for content_id in content_ids
            if str((self.store.content(content_id) or {}).get("status") or "") in {"PUBLISHED", "REJECTED"}
        ]
        if blocked:
            messagebox.showwarning("Tạo ảnh", "Bài đã đăng hoặc đã từ chối thì không tạo ảnh.", parent=self.root)
            return
        if len(content_ids) > 1 and not messagebox.askyesno(
            "Tạo ảnh",
            f"Tạo ảnh bằng Gemini cho {len(content_ids)} bài đang chọn?",
            parent=self.root,
        ):
            return
        self.review_status.set("Đang tạo ảnh…")

        def work() -> None:
            from src.services.ai_fleet.account_check import apply_account_rotation
            from src.utils.schedule_posts_manager import get_default_schedule_posts_manager

            apply_account_rotation(self.store, 0)
            posts = get_default_schedule_posts_manager()
            made = 0
            error = ""
            for content_id in content_ids:
                try:
                    attach_post_image(self.store, content_id, image_name="openai", posts=posts)
                    made += 1
                except Exception as exc:  # noqa: BLE001
                    error = str(exc)
                    break
            message = f"Đã tạo ảnh cho {made} bài."
            if error:
                message = f"{message} Chưa tạo tiếp được: {error}"
            schedule_on_main_thread(self.root, lambda: self._after_image(message, content_ids[0]))

        threading.Thread(target=work, name="ai-fleet-image", daemon=True).start()

    def _after_image(self, message: str, content_id: str) -> None:
        self.review_status.set(message)
        self.refresh()
        if content_id and self.content_tree.exists(content_id):
            self.content_tree.selection_set(content_id)
            self._show_content()

    def _save_edit(self) -> None:
        content_id = self._selected_content_id()
        if not content_id:
            messagebox.showwarning("Sửa bài", "Chọn một bài trong danh sách.", parent=self.root)
            return
        try:
            result = save_post_edit(
                self.store,
                content_id,
                title=self.edit_title.get(),
                body=self.edit_body.get("1.0", "end"),
                caption=self.edit_caption.get("1.0", "end"),
                hashtags=self.edit_tags.get(),
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("Sửa bài", str(exc), parent=self.root)
            return
        self.refresh()
        if result.get("changed"):
            self.review_status.set("Đã lưu bản sửa. Bấm Tôi duyệt để bài mới được đăng đúng giờ đã xếp.")
        else:
            self.review_status.set("Nội dung chưa đổi.")

    def _review(self) -> None:
        content_id = self._selected_content_id()
        if not content_id:
            return
        try:
            review = review_content(self.store, content_id)
            note_review(self.store, content_id, review)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("Duyệt", str(exc), parent=self.root)
            return
        self.refresh()
        self.review_status.set(f"{review['decision']}: {review['summary']}")

    def _human(self, decision: str) -> None:
        content_id = self._selected_content_id()
        if not content_id:
            return
        try:
            review = human_decide(self.store, content_id, decision)
            note_review(self.store, content_id, review)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("Duyệt", str(exc), parent=self.root)
            return
        if decision == "APPROVED":
            try:
                from src.utils.schedule_posts_manager import get_default_schedule_posts_manager

                sync_approved_job(self.store, content_id, get_default_schedule_posts_manager())
            except Exception as exc:  # noqa: BLE001
                messagebox.showwarning("Lịch", str(exc), parent=self.root)
        self.refresh()
        self.review_status.set(review["summary"])

    def _place(self, *, immediate: bool) -> None:
        content_id = self._selected_content_id()
        content = self.store.content(content_id) if content_id else None
        if not content:
            messagebox.showwarning("Đăng bài", "Chọn một bài trong danh sách.", parent=self.root)
            return
        if immediate and not messagebox.askyesno(
            "Đăng luôn",
            "Đăng bài này ngay khi lịch đang chạy? Bài không cần sửa thì để Đăng theo lịch.",
            parent=self.root,
        ):
            return
        page = next((row for row in self._pages if row["page_id"] == str(content.get("page_id") or "")), None)
        account_id = str((page or {}).get("account_id") or "")
        try:
            from src.utils.schedule_posts_manager import get_default_schedule_posts_manager

            reason = place_approved_post(
                self.store,
                content_id,
                account_id=account_id,
                posts=get_default_schedule_posts_manager(),
                when=datetime.now(timezone.utc) if immediate else None,
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("Đăng bài", str(exc), parent=self.root)
            return
        if reason:
            messagebox.showwarning("Đăng bài", reason, parent=self.root)
            return
        if self._on_scheduler_start is not None:
            self._on_scheduler_start()
        self.refresh()
        self.review_status.set(
            "Đã đưa lên đăng ngay. Lịch đang chạy sẽ đăng trong vòng quét kế tiếp."
            if immediate
            else "Giữ giờ đã xếp. Không cần sửa thì lịch tự đăng đúng giờ."
        )

    def _set_stop(self, stopped: bool) -> None:
        self.store.set_emergency_stop(stopped)
        self.refresh()

    def _fill_text(self, widget: tk.Text, text: str) -> None:
        widget.delete("1.0", "end")
        widget.insert("1.0", text)

    def _scheduled_labels(self) -> dict[str, str]:
        """Giờ đăng local của bài fleet chưa chạy."""
        import os
        from zoneinfo import ZoneInfo

        try:
            from src.utils.schedule_posts_manager import get_default_schedule_posts_manager

            zone = ZoneInfo(os.environ.get("SCHEDULER_TZ", "Asia/Ho_Chi_Minh").strip() or "Asia/Ho_Chi_Minh")
            labels: dict[str, str] = {}
            for job in get_default_schedule_posts_manager().load_all():
                content_id = str(job.get("fleet_content_id") or "")
                if not content_id or str(job.get("status") or "").lower() not in {"pending", "ready_queue", "running"}:
                    continue
                raw = str(job.get("scheduled_at") or "").replace("Z", "+00:00")
                try:
                    when = datetime.fromisoformat(raw).astimezone(zone)
                    labels[content_id] = when.strftime("%d/%m %H:%M")
                except ValueError:
                    labels[content_id] = raw
            return labels
        except Exception:
            return {}


def _current(content: dict[str, Any]) -> dict[str, Any] | None:
    number = int(content.get("current_version") or 0)
    for row in content.get("versions") or []:
        if int(row.get("version") or 0) == number:
            return dict(row)
    return None
