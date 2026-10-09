"""Màn hình tạo nhiều Facebook Page: danh sách, delay, hàng đợi, tiến độ."""

from __future__ import annotations

import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any

from loguru import logger

from src.services.page_creation.importer import parse_page_import
from src.services.page_creation.naming import (
    account_display_label,
    assign_random_avatars,
    business_display_label,
    generate_page_names,
    image_files_in_folder,
    topic_from_account_name,
)
from src.services.page_creation.page_details import attach_generated_details, summarize_page_details
from src.gui.ui_responsiveness import (
    register_main_thread_dispatcher,
    schedule_on_main_thread,
    tree_delete_all,
    tree_insert_chunked,
)
from src.services.page_creation.runtime import get_engine
from src.services.page_creation.store import PageCreationStore
from src.services.page_creation.validate import validate_page_list

_STATUS_VI = {
    "PENDING": "Đang chờ",
    "VALIDATING": "Đang kiểm tra",
    "CHECKING_SESSION": "Đang kiểm tra phiên",
    "CREATING": "Đang tạo",
    "UPLOADING_AVATAR": "Đang tải ảnh",
    "FILLING_DETAILS": "Đang điền thông tin",
    "ADDING_ADMIN": "Đang thêm quản trị viên",
    "VERIFYING": "Đang xác minh",
    "RECOVERY_REQUIRED": "Cần đối soát",
    "COMPLETED": "Hoàn tất",
    "FAILED": "Thất bại",
    "CANCELLED": "Đã hủy",
    "DELAYED": "Đang chờ delay",
    "WAITING_REAUTH": "Cần đăng nhập lại",
    "CHECKPOINT": "Checkpoint",
    "CAPTCHA_DETECTED": "CAPTCHA",
    "RATE_LIMITED": "Bị giới hạn",
    "SMS_VERIFICATION_REQUIRED": "Cần xác minh SMS",
    "ACCOUNT_SECURITY_CHECK": "Đang kiểm tra bảo mật",
    "SECURITY_VERIFICATION_REQUIRED": "Cần xác minh bảo mật",
    "BLOCKED_BY_ACCOUNT_VERIFICATION": "Đang chờ xác minh account",
    "READY_TO_RESUME": "Sẵn sàng chạy tiếp",
    "PAUSED_FOR_USER": "Tạm dừng chờ bạn",
    "PAUSED": "Tạm dừng",
    "RUNNING": "Đang chạy",
    "DELAYING": "Đang chờ",
    "READY": "Sẵn sàng",
    "CREATED": "Đã tạo",
    "COMPLETED_BATCH": "Hoàn tất",
}


def _status_vi(code: str) -> str:
    return _STATUS_VI.get(str(code or "").strip(), str(code or ""))


def changed_job_rows(
    known: dict[str, tuple[str, int, str]],
    rows: list[tuple],
) -> list[tuple]:
    """Chỉ những dòng đổi trạng thái, số lần thử hoặc UID. Bảng lớn không vẽ lại từ đầu."""
    changed: list[tuple] = []
    for row in rows:
        page_id = str(row[6]) if len(row) > 6 else ""
        marker = (row[3], row[4], page_id)
        if known.get(row[0]) != marker:
            changed.append(row)
    return changed


def _job_values(row: tuple) -> tuple[int, str, str, str, str, str, str]:
    status = row[3]
    action = "Thử lại" if status == "FAILED" else ("Hủy" if status in {"PENDING", "DELAYED"} else "Xem")
    page_id = str(row[6]) if len(row) > 6 else ""
    page_url = str(row[7]) if len(row) > 7 else ""
    return (
        row[1],
        row[2],
        _status_vi(status),
        f"{row[4]}/{row[5]}",
        page_id or "—",
        page_url or "—",
        action,
    )


class PageCreatorDialog(tk.Toplevel):
    """Chọn account, Business Manager, danh sách Page, rồi đẩy vào queue."""

    def __init__(self, master: tk.Misc) -> None:
        super().__init__(master)
        self.title("Tạo Facebook Page")
        self.geometry("980x780")
        self.minsize(900, 560)
        self._account_by_label: dict[str, str] = {}
        self._business_by_label: dict[str, str] = {}
        self._rows: list[dict[str, str]] = []
        self._batch_id = ""
        self._page_gen = 0
        self._job_gen = 0
        self._job_ids: tuple[str, ...] = ()
        self._job_state: dict[str, tuple[str, int, str]] = {}
        self._jobs_filling = False
        self._ui_busy = False
        self._manual_browser = False
        self._asked_close_browser = False
        self._close_browser = threading.Event()
        self._last_current: tuple = ("", "", "")
        self._last_progress_message = ""
        self._security_alerted = ""
        register_main_thread_dispatcher(self)
        self._accounts: list[dict[str, Any]] = []
        self._business_ids: list[str] = []
        self._build()
        self._saved_prefs: dict[str, Any] = {}
        self._pref_save_job: str | None = None
        self._load_ui_prefs()
        self._load_sources()
        self._restore_saved_selection()
        self._bind_ui_pref_autosave()
        self._on_create_path_changed()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(400, self._tick)

    def _on_create_path_changed(self) -> None:
        """Ẩn/hiện phần BM khi chọn tạo qua Profile."""
        profile = self._create_path.get() == "profile"
        try:
            self._business_box.configure(state="disabled" if profile else "normal")
        except Exception:  # noqa: BLE001
            pass
        for widget in (self._bm_create_btn, self._bm_load_btn, self._bm_open_btn):
            try:
                widget.configure(state="disabled" if profile else "normal")
            except Exception:  # noqa: BLE001
                pass

    def _admin_targets(self) -> list[str]:
        from src.services.page_creation.page_admin import parse_admin_targets

        return parse_admin_targets(self._admins.get("1.0", tk.END))

    def _build(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        scroll_host = ttk.Frame(self)
        scroll_host.grid(row=0, column=0, sticky="nsew")
        body = self._build_scroll_body(scroll_host)

        footer = ttk.Frame(self)
        footer.grid(row=1, column=0, sticky="ew")
        footer.columnconfigure(0, weight=1)

        pad = {"padx": 8, "pady": 4}
        account = ttk.LabelFrame(body, text="Tài khoản")
        account.pack(fill=tk.X, **pad)
        ttk.Label(account, text="Tài khoản:").grid(row=0, column=0, sticky="w", padx=6, pady=4)
        self._account = tk.StringVar()
        self._account_box = ttk.Combobox(account, textvariable=self._account, state="readonly", width=36)
        self._account_box.grid(row=0, column=1, sticky="we", padx=6, pady=4)
        ttk.Label(account, text="Business Manager:").grid(row=1, column=0, sticky="w", padx=6, pady=4)
        self._business = tk.StringVar()
        self._business_box = ttk.Combobox(account, textvariable=self._business, width=36)
        self._business_box.grid(row=1, column=1, sticky="we", padx=6, pady=4)
        bm_actions = ttk.Frame(account)
        bm_actions.grid(row=2, column=1, sticky="w", padx=6, pady=(0, 4))
        self._bm_create_btn = ttk.Button(bm_actions, text="Tạo Business Manager", command=self._on_create_business)
        self._bm_create_btn.pack(side=tk.LEFT)
        self._bm_load_btn = ttk.Button(bm_actions, text="Tải BM đã có", command=self._on_open_business_browser)
        self._bm_load_btn.pack(side=tk.LEFT, padx=(6, 0))
        self._bm_open_btn = ttk.Button(bm_actions, text="Mở trình duyệt", command=self._on_open_business_browser)
        self._bm_open_btn.pack(side=tk.LEFT, padx=(6, 0))
        self._bm_frame_widgets = (self._business_box, self._bm_create_btn, self._bm_load_btn, self._bm_open_btn)
        self._account_box.bind("<<ComboboxSelected>>", lambda _e: self._reload_businesses())
        account.columnconfigure(1, weight=1)

        path = ttk.LabelFrame(body, text="Cách tạo Page")
        path.pack(fill=tk.X, **pad)
        self._create_path = tk.StringVar(value="bm")
        ttk.Radiobutton(
            path,
            text="Qua Business Manager",
            variable=self._create_path,
            value="bm",
            command=self._on_create_path_changed,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Radiobutton(
            path,
            text="Qua Profile (không cần BM)",
            variable=self._create_path,
            value="profile",
            command=self._on_create_path_changed,
        ).pack(side=tk.LEFT, padx=8)

        mode = ttk.LabelFrame(body, text="Số lượng")
        mode.pack(fill=tk.X, **pad)
        self._mode = tk.StringVar(value="multiple")
        ttk.Radiobutton(mode, text="Một Page", variable=self._mode, value="single").pack(side=tk.LEFT, padx=8)
        ttk.Radiobutton(mode, text="Nhiều Page", variable=self._mode, value="multiple").pack(side=tk.LEFT, padx=8)

        listing = ttk.LabelFrame(body, text="Danh sách Page")
        listing.pack(fill=tk.X, **pad)
        form = ttk.Frame(listing)
        form.pack(fill=tk.X, padx=6, pady=4)
        ttk.Label(form, text="Số Page").grid(row=0, column=0, sticky="w")
        self._count = tk.StringVar(value="5")
        ttk.Entry(form, textvariable=self._count, width=6).grid(row=0, column=1, sticky="w", padx=(4, 12))
        ttk.Label(form, text="Chủ đề").grid(row=0, column=2, sticky="w")
        self._topic = tk.StringVar()
        ttk.Entry(form, textvariable=self._topic, width=28).grid(row=0, column=3, sticky="we", padx=(4, 12))
        lang = ttk.Frame(form)
        lang.grid(row=1, column=0, columnspan=4, sticky="w", pady=(4, 0))
        ttk.Label(lang, text="Ngôn ngữ tên & thông tin").pack(side=tk.LEFT, padx=(0, 8))
        self._name_lang = tk.StringVar(value="mix")
        ttk.Radiobutton(lang, text="Tiếng Việt", variable=self._name_lang, value="vi").pack(side=tk.LEFT, padx=(0, 6))
        ttk.Radiobutton(lang, text="Tiếng Anh", variable=self._name_lang, value="en").pack(side=tk.LEFT, padx=(0, 6))
        ttk.Radiobutton(lang, text="Cả hai", variable=self._name_lang, value="mix").pack(side=tk.LEFT)
        self._auto_details = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            form,
            text="Tự sinh mô tả, SĐT, email, website, địa chỉ theo ngôn ngữ đã chọn",
            variable=self._auto_details,
        ).grid(row=4, column=0, columnspan=4, sticky="w", pady=(4, 0))
        ttk.Label(form, text="Thư mục ảnh").grid(row=2, column=0, sticky="w", pady=(4, 0))
        self._avatar_dir = tk.StringVar()
        ttk.Entry(form, textvariable=self._avatar_dir, width=42).grid(
            row=2, column=1, columnspan=2, sticky="we", padx=(4, 4), pady=(4, 0)
        )
        ttk.Button(form, text="Chọn thư mục", command=self._pick_avatar_folder).grid(
            row=2, column=3, sticky="w", pady=(4, 0)
        )
        self._folder_hint = tk.StringVar(
            value="Không chọn thư mục thì tạo Page chỉ với tên. Có thư mục thì mỗi Page nhận một ảnh ngẫu nhiên."
        )
        ttk.Label(form, textvariable=self._folder_hint).grid(row=3, column=0, columnspan=4, sticky="w", pady=(2, 0))
        form.columnconfigure(3, weight=1)
        bar = ttk.Frame(listing)
        bar.pack(fill=tk.X, padx=6, pady=4)
        bar_row = ttk.Frame(bar)
        bar_row.pack(fill=tk.X)
        ttk.Button(bar_row, text="Sinh danh sách", command=self._on_generate).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(bar_row, text="Đổi ảnh ngẫu nhiên", command=self._reshuffle_avatars).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(bar_row, text="Sinh lại thông tin", command=self._reshuffle_details).pack(side=tk.LEFT, padx=(0, 4))
        bar_row2 = ttk.Frame(bar)
        bar_row2.pack(fill=tk.X, pady=(4, 0))
        ttk.Button(bar_row2, text="Thêm Page", command=self._add_page).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(bar_row2, text="Xóa dòng", command=self._remove_page).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(bar_row2, text="Nhập CSV/JSON", command=self._import_file).pack(side=tk.LEFT)
        cols = ("idx", "name", "details", "avatar", "status")
        self._pages = ttk.Treeview(listing, columns=cols, show="headings", height=5)
        for key, title, width in (
            ("idx", "#", 40),
            ("name", "Tên Page", 220),
            ("details", "Thông tin", 280),
            ("avatar", "Ảnh đại diện", 160),
            ("status", "Trạng thái", 100),
        ):
            self._pages.heading(key, text=title)
            self._pages.column(key, width=width, anchor="w")
        self._pages.pack(fill=tk.X, expand=False, padx=6, pady=4)

        delay = ttk.LabelFrame(body, text="Chờ giữa các Page")
        delay.pack(fill=tk.X, **pad)
        self._delay_on = tk.BooleanVar(value=True)
        ttk.Checkbutton(delay, text="Bật delay", variable=self._delay_on).grid(row=0, column=0, sticky="w", padx=6)
        self._delay_mode = tk.StringVar(value="fixed")
        ttk.Radiobutton(delay, text="Cố định", variable=self._delay_mode, value="fixed").grid(
            row=1, column=0, sticky="w", padx=6
        )
        ttk.Radiobutton(delay, text="Ngẫu nhiên", variable=self._delay_mode, value="range").grid(
            row=1, column=1, sticky="w"
        )
        ttk.Label(delay, text="Số giây").grid(row=2, column=0, sticky="w", padx=6)
        self._fixed = tk.StringVar(value="30")
        ttk.Entry(delay, textvariable=self._fixed, width=8).grid(row=2, column=1, sticky="w")
        ttk.Label(delay, text="Từ (giây)").grid(row=3, column=0, sticky="w", padx=6)
        self._delay_min = tk.StringVar(value="30")
        ttk.Entry(delay, textvariable=self._delay_min, width=8).grid(row=3, column=1, sticky="w")
        ttk.Label(delay, text="Đến (giây)").grid(row=3, column=2, sticky="w")
        self._delay_max = tk.StringVar(value="60")
        ttk.Entry(delay, textvariable=self._delay_max, width=8).grid(row=3, column=3, sticky="w")
        ttk.Label(
            delay,
            text="Khoảng chờ chỉ nằm giữa các Page trong hàng đợi. Không dùng để vượt CAPTCHA, checkpoint hay giới hạn của Facebook.",
            wraplength=900,
        ).grid(row=4, column=0, columnspan=4, sticky="w", padx=6, pady=(4, 2))

        opts = ttk.LabelFrame(body, text="Tuỳ chọn batch")
        opts.pack(fill=tk.X, **pad)
        self._continue = tk.BooleanVar(value=True)
        ttk.Checkbutton(opts, text="Chạy tiếp khi lỗi tạm thời", variable=self._continue).pack(side=tk.LEFT, padx=6)
        ttk.Label(opts, text="Số lần thử lại").pack(side=tk.LEFT, padx=(12, 4))
        self._max_retry = tk.StringVar(value="3")
        ttk.Entry(opts, textvariable=self._max_retry, width=6).pack(side=tk.LEFT)

        admins = ttk.LabelFrame(body, text="Thêm quản trị viên (tuỳ chọn)")
        admins.pack(fill=tk.X, **pad)
        ttk.Label(
            admins,
            text="Sau khi tạo Page, gửi quyền Admin cho UID / username / link profile (mỗi dòng một người).",
        ).pack(anchor="w", padx=6, pady=(4, 0))
        self._admins = tk.Text(admins, height=3, wrap="word")
        self._admins.pack(fill=tk.X, padx=6, pady=4)

        jobs = ttk.LabelFrame(body, text="Hàng đợi")
        jobs.pack(fill=tk.X, **pad)
        jcols = ("idx", "name", "status", "retry", "uid", "link", "action")
        self._jobs = ttk.Treeview(jobs, columns=jcols, show="headings", height=5)
        for key, title, width in (
            ("idx", "#", 36),
            ("name", "Page", 200),
            ("status", "Trạng thái", 110),
            ("retry", "Thử lại", 64),
            ("uid", "UID Page", 140),
            ("link", "Link Page", 240),
            ("action", "Thao tác", 72),
        ):
            self._jobs.heading(key, text=title)
            self._jobs.column(key, width=width, anchor="w")
        self._jobs.pack(fill=tk.X, expand=False, padx=6, pady=4)
        self._jobs.bind("<Double-1>", self._on_job_action)
        ttk.Label(jobs, text="Nhấp đúp dòng Thất bại để thử lại, dòng Đang chờ để hủy dòng đó.").pack(
            anchor="w", padx=6
        )

        log_box = ttk.LabelFrame(body, text="Nhật ký")
        log_box.pack(fill=tk.X, padx=8, pady=(4, 10))
        self._log = tk.Text(log_box, height=5, wrap="word")
        self._log.pack(fill=tk.X, padx=6, pady=4)

        # Nút thao tác luôn neo dưới — không bị che khi cuộn cấu hình.
        actions = ttk.Frame(footer)
        actions.pack(fill=tk.X, padx=8, pady=(6, 2))
        ttk.Button(actions, text="Kiểm tra", command=self._on_validate).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(actions, text="Tạo tất cả", command=self._on_create).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(actions, text="Tạm dừng", command=self._on_pause).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(actions, text="Tiếp tục", command=self._on_resume).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(actions, text="Kiểm tra lại", command=self._on_check_again).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(actions, text="Hủy batch", command=self._on_cancel).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(actions, text="Mở tài khoản", command=self._on_open_account).pack(side=tk.LEFT)

        self._progress = tk.StringVar(value="Chưa có batch.")
        ttk.Label(footer, textvariable=self._progress, wraplength=940).pack(fill=tk.X, padx=8, pady=(0, 6))

        self.after_idle(self._refresh_scroll_bindings)

    def _build_scroll_body(self, host: ttk.Frame) -> ttk.Frame:
        """Canvas + thanh cuộn dọc cho phần cấu hình dài."""
        host.columnconfigure(0, weight=1)
        host.rowconfigure(0, weight=1)
        canvas = tk.Canvas(host, highlightthickness=0, borderwidth=0)
        vsb = ttk.Scrollbar(host, orient=tk.VERTICAL, command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        inner = ttk.Frame(canvas)
        inner.columnconfigure(0, weight=1)
        win_id = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _sync_region(_event: object | None = None) -> None:
            bbox = canvas.bbox("all")
            if bbox:
                canvas.configure(scrollregion=bbox)

        def _on_canvas_configure(event: tk.Event) -> None:
            if event.widget is not canvas:
                return
            width = int(event.width)
            if width > 1:
                canvas.itemconfigure(win_id, width=width)
            _sync_region()

        def _on_wheel(event: tk.Event) -> None:
            widget = getattr(event, "widget", None)
            if widget is not None and not self._wheel_targets_scroll(widget, canvas, inner):
                return
            # Text/Treeview tự cuộn nội dung riêng — bỏ qua để không nhảy cả form.
            try:
                cls = widget.winfo_class() if widget is not None else ""
            except Exception:  # noqa: BLE001
                cls = ""
            if cls in {"Text", "Treeview"}:
                return
            delta = getattr(event, "delta", 0) or 0
            if delta:
                canvas.yview_scroll(int(-delta / 120), "units")
                return "break"
            num = getattr(event, "num", None)
            if num == 4:
                canvas.yview_scroll(-3, "units")
                return "break"
            if num == 5:
                canvas.yview_scroll(3, "units")
                return "break"
            return None

        def _bind_wheel_recursive(widget: tk.Misc) -> None:
            widget.bind("<MouseWheel>", _on_wheel, add="+")
            widget.bind("<Button-4>", _on_wheel, add="+")
            widget.bind("<Button-5>", _on_wheel, add="+")
            for child in widget.winfo_children():
                _bind_wheel_recursive(child)

        inner.bind("<Configure>", _sync_region)
        canvas.bind("<Configure>", _on_canvas_configure)
        canvas.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        self._scroll_canvas = canvas
        self._scroll_inner = inner
        self._bind_wheel_recursive = _bind_wheel_recursive
        self._sync_scroll_region = _sync_region
        self.after(80, _sync_region)
        return inner

    def _wheel_targets_scroll(self, widget: tk.Misc, canvas: tk.Canvas, inner: ttk.Frame) -> bool:
        """True nếu sự kiện chuột nằm trong vùng cuộn của dialog."""
        current: tk.Misc | None = widget
        while current is not None:
            if current in (canvas, inner):
                return True
            try:
                current = current.master  # type: ignore[assignment]
            except Exception:  # noqa: BLE001
                break
        return False

    def _refresh_scroll_bindings(self) -> None:
        """Gắn lại MouseWheel sau khi dựng xong toàn bộ control con."""
        binder = getattr(self, "_bind_wheel_recursive", None)
        inner = getattr(self, "_scroll_inner", None)
        sync = getattr(self, "_sync_scroll_region", None)
        if callable(binder) and inner is not None:
            binder(inner)
        if callable(sync):
            sync()

    def _load_sources(self) -> None:
        from src.utils.db_manager import AccountsDatabaseManager

        self._accounts = []
        labels: list[str] = []
        self._account_by_label = {}
        try:
            for row in AccountsDatabaseManager().load_all():
                item = dict(row)
                self._accounts.append(item)
                label = account_display_label(item)
                acc_id = str(item.get("id") or "").strip()
                if label in self._account_by_label and acc_id:
                    label = f"{label} · {acc_id}"
                self._account_by_label[label] = acc_id
                labels.append(label)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không đọc được tài khoản cho tạo Page: {}", exc)
        self._account_box["values"] = labels
        if labels:
            self._account_box.current(0)
        self._reload_businesses()
        self._attach_open_batch()

    def _reload_businesses(self) -> None:
        """BM của account đang chọn: Page đã lưu và BM vừa tạo."""
        from src.services.page_creation.business_store import BusinessManagerStore
        from src.utils.pages_manager import PagesManager

        account_id = self._selected_account_id()
        account_keys = self._selected_account_keys()
        previous = self._selected_business_id()
        found: dict[str, str] = {}
        try:
            for row in PagesManager().load_all():
                owner = str(row.get("account_id") or "").strip()
                if account_keys and owner not in account_keys:
                    continue
                bid = str(row.get("business_id") or "").strip()
                if bid and bid not in found:
                    found[bid] = str(row.get("business_name") or "").strip()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không đọc được Business Manager từ pages.json: {}", exc)
        try:
            for row in BusinessManagerStore().for_account(account_id):
                bid = str(row.get("business_id") or "").strip()
                if not bid:
                    continue
                title = str(row.get("business_name") or "").strip()
                if bid not in found or (title and not found[bid]):
                    found[bid] = title
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không đọc được kho Business Manager: {}", exc)
        self._business_ids = list(found)
        self._business_by_label = {}
        labels: list[str] = []
        selected_label = ""
        for bid, title in found.items():
            label = business_display_label(title, bid)
            if label in self._business_by_label:
                label = f"{label} · {bid}"
            self._business_by_label[label] = bid
            labels.append(label)
            if bid == previous:
                selected_label = label
        self._business_box["values"] = labels
        if selected_label:
            self._business.set(selected_label)
        elif labels:
            self._business_box.current(0)
        else:
            self._business.set("")
        self._attach_open_batch()

    def _attach_open_batch(self) -> None:
        """Hiện batch đang dở của account để Tiếp tục hoặc Hủy, không chặn im."""
        from src.services.page_creation.validate import open_batch_for_account

        batch = open_batch_for_account(PageCreationStore().list_batches(), self._selected_account_id())
        self._batch_id = str(batch.get("id") or "") if batch else ""

    def _release_stale_batch(self) -> bool:
        """Batch không còn chạy thì hỏi hủy trước khi kiểm tra hoặc tạo mới."""
        from src.services.page_creation.validate import open_batch_for_account

        account_id = self._selected_account_id()
        batch = open_batch_for_account(PageCreationStore().list_batches(), account_id)
        if not batch:
            return True
        batch_id = str(batch.get("id") or "")
        self._batch_id = batch_id
        engine = get_engine()
        if engine.is_running(batch_id):
            messagebox.showinfo(
                "Đang tạo",
                "Batch này đang chạy. Hãy đợi, hoặc bấm Tạm dừng / Hủy batch ở hàng đợi.",
                parent=self,
            )
            return False
        failed = int(batch.get("failed_jobs") or 0)
        pending = int(batch.get("pending_jobs") or 0)
        if not messagebox.askyesno(
            "Batch cũ",
            "Batch tạo Page trước đang tạm dừng "
            f"({failed} lỗi, {pending} trang đang chờ) nên không tạo mới được.\n\n"
            "Hủy batch cũ để chạy lại? Page đã tạo xong vẫn được giữ.",
            parent=self,
        ):
            self._append_log("Giữ batch cũ. Bấm Tiếp tục để chạy tiếp, hoặc Hủy batch.")
            return False
        engine.cancel(batch_id)
        self._batch_id = ""
        self._append_log(f"Đã hủy batch cũ {batch_id}.")
        return True

    def _selected_account_keys(self) -> set[str]:
        """Id nội bộ và UID Facebook, vì Page cũ có thể gắn một trong hai."""
        row = self._selected_account_row()
        keys = {
            self._selected_account_id(),
            str(row.get("facebook_uid") or "").strip(),
            str(row.get("uid") or "").strip(),
        }
        return {key for key in keys if key}

    def _selected_account_id(self) -> str:
        return self._account_by_label.get(self._account.get().strip(), "")

    def _selected_account_row(self) -> dict[str, Any]:
        acc_id = self._selected_account_id()
        for row in self._accounts:
            if str(row.get("id") or "") == acc_id:
                return row
        return {}

    def _selected_business_id(self) -> str:
        label = self._business.get().strip()
        if label in self._business_by_label:
            return self._business_by_label[label]
        if " · " in label:
            tail = label.rsplit(" · ", 1)[-1].strip()
            if tail.isdigit():
                return tail
        return label

    def _selected_pages(self) -> list[dict[str, str]]:
        rows = list(self._rows)
        if self._mode.get() == "single":
            return rows[:1]
        return rows

    def _settings(self) -> dict[str, Any]:
        def _num(raw: str, default: int) -> int:
            try:
                return max(0, int(str(raw).strip()))
            except ValueError:
                return default

        return {
            "delay_enabled": bool(self._delay_on.get()),
            "delay_mode": self._delay_mode.get(),
            "delay_fixed_seconds": _num(self._fixed.get(), 30),
            "delay_min_seconds": _num(self._delay_min.get(), 30),
            "delay_max_seconds": _num(self._delay_max.get(), 60),
            "max_retry": _num(self._max_retry.get(), 3),
            "continue_on_error": bool(self._continue.get()),
            "pause_on_checkpoint": True,
            "pause_on_captcha": True,
            "pause_on_rate_limit": True,
            "auto_resume_after_restart": False,
            "create_mode": self._create_path.get() or "bm",
            "admin_targets": self._admin_targets(),
        }

    def _known_business_ids(self) -> set[str]:
        known = set(self._business_ids)
        selected = self._selected_business_id()
        if selected.isdigit() and len(selected) >= 6:
            known.add(selected)
        return known

    def _busy_accounts(self) -> set[str]:
        """Chỉ chặn account khi batch thật sự còn vòng lặp. Batch tạm dừng xử lý riêng."""
        engine = get_engine()
        busy: set[str] = set()
        for batch in PageCreationStore().list_batches():
            batch_id = str(batch.get("id") or "")
            if str(batch.get("status")) in {"RUNNING", "DELAYING", "PAUSED", "READY"} and engine.is_running(batch_id):
                busy.add(str(batch.get("account_id") or ""))
        return busy

    def _errors(self) -> list[str]:
        return validate_page_list(
            account_id=self._selected_account_id(),
            business_id=self._selected_business_id(),
            pages=self._selected_pages(),
            known_account_ids={str(a.get("id") or "") for a in self._accounts},
            known_business_ids=self._known_business_ids(),
            busy_account_ids=self._busy_accounts(),
        )

    def _paint_pages(self) -> None:
        """Vẽ danh sách theo lô để vài nghìn dòng không khóa cửa sổ."""
        self._page_gen += 1
        generation = self._page_gen
        specs = [
            {
                "values": (
                    index,
                    row.get("page_name") or "",
                    summarize_page_details(row) or "—",
                    Path(row.get("avatar_path") or "").name or "Không ảnh",
                    "Đang chờ",
                )
            }
            for index, row in enumerate(self._selected_pages(), start=1)
        ]
        tree_delete_all(self._pages)

        def _current(gen: int) -> bool:
            return gen == self._page_gen and self.winfo_exists()

        if not specs:
            return
        tree_insert_chunked(
            self,
            self._pages,
            specs,
            generation=generation,
            is_current=_current,
            chunk=80,
        )

    def _folder_images(self) -> list[str]:
        images = image_files_in_folder(self._avatar_dir.get().strip())
        if images:
            self._folder_hint.set(f"Thư mục có {len(images)} ảnh. Mỗi Page nhận một ảnh ngẫu nhiên.")
        return images

    def _pick_avatar_folder(self) -> None:
        initial = self._avatar_dir.get().strip()
        path = filedialog.askdirectory(
            parent=self,
            title="Chọn thư mục chứa ảnh đại diện",
            initialdir=initial if initial and Path(initial).is_dir() else None,
        )
        if not path:
            return
        self._avatar_dir.set(path)
        self._persist_ui_prefs()
        self._folder_hint.set("Đang đọc thư mục ảnh...")

        def _work() -> None:
            images = image_files_in_folder(path)

            def _apply() -> None:
                if not self.winfo_exists():
                    return
                if not images:
                    self._folder_hint.set("Thư mục này chưa có ảnh jpg, png, webp hoặc gif.")
                    messagebox.showwarning("Thư mục ảnh", "Không thấy file ảnh hợp lệ trong thư mục.", parent=self)
                    return
                self._folder_hint.set(f"Thư mục có {len(images)} ảnh. Mỗi Page nhận một ảnh ngẫu nhiên.")

            schedule_on_main_thread(self, _apply)

        threading.Thread(target=_work, daemon=True, name="page_avatar_folder").start()

    def _load_ui_prefs(self) -> None:
        """Nạp thư mục ảnh, delay, số lần thử và các tuỳ chọn form đã lưu."""
        try:
            prefs = PageCreationStore().ui_prefs()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không đọc prefs tạo Page: {}", exc)
            return
        if "delay_mode" not in prefs:
            for key, value in self._latest_batch_form().items():
                prefs.setdefault(key, value)
        self._saved_prefs = dict(prefs)
        folder = str(prefs.get("avatar_dir") or "").strip()
        if folder:
            self._avatar_dir.set(folder)
            if Path(folder).is_dir():
                self._folder_hint.set(f"Thư mục đã lưu: {folder}")
            else:
                self._folder_hint.set("Thư mục ảnh đã lưu không còn tồn tại. Chọn lại thư mục.")
        lang = str(prefs.get("name_lang") or "").strip().lower()
        if lang in {"vi", "en", "mix"}:
            self._name_lang.set(lang)
        if "auto_details" in prefs:
            self._auto_details.set(bool(prefs.get("auto_details")))
        topic = str(prefs.get("topic") or "").strip()
        if topic:
            self._topic.set(topic)
        count = str(prefs.get("count") or "").strip()
        if count.isdigit():
            self._count.set(count)
        create_path = str(prefs.get("create_path") or "").strip().lower()
        if create_path in {"bm", "profile"}:
            self._create_path.set(create_path)
        mode = str(prefs.get("mode") or "").strip().lower()
        if mode in {"single", "multiple"}:
            self._mode.set(mode)
        if "delay_enabled" in prefs:
            self._delay_on.set(bool(prefs.get("delay_enabled")))
        delay_mode = str(prefs.get("delay_mode") or "").strip().lower()
        if delay_mode in {"fixed", "range"}:
            self._delay_mode.set(delay_mode)
        self._set_saved_number(self._fixed, prefs.get("delay_fixed_seconds"))
        self._set_saved_number(self._delay_min, prefs.get("delay_min_seconds"))
        self._set_saved_number(self._delay_max, prefs.get("delay_max_seconds"))
        if "continue_on_error" in prefs:
            self._continue.set(bool(prefs.get("continue_on_error")))
        self._set_saved_number(self._max_retry, prefs.get("max_retry"))
        admins = prefs.get("admin_targets_text")
        if isinstance(admins, str):
            self._admins.delete("1.0", tk.END)
            if admins.strip():
                self._admins.insert("1.0", admins.strip())
            self._admins.edit_modified(False)

    def _latest_batch_form(self) -> dict[str, Any]:
        """Thông số của batch gần nhất, dùng khi form chưa từng được lưu."""
        try:
            batches = PageCreationStore().list_batches()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Không đọc batch để khôi phục form: {}", exc)
            return {}
        if not batches:
            return {}
        batch = batches[-1]
        targets = batch.get("admin_targets") or []
        lines = [str(item).strip() for item in targets if str(item).strip()]
        return {
            "create_path": str(batch.get("create_mode") or "").strip().lower(),
            "delay_enabled": bool(batch.get("delay_enabled", True)),
            "delay_mode": str(batch.get("delay_mode") or "").strip().lower(),
            "delay_fixed_seconds": batch.get("delay_fixed_seconds"),
            "delay_min_seconds": batch.get("delay_min_seconds"),
            "delay_max_seconds": batch.get("delay_max_seconds"),
            "continue_on_error": bool(batch.get("continue_on_error", True)),
            "max_retry": batch.get("max_retry"),
            "admin_targets_text": "\n".join(lines),
            "account_id": str(batch.get("account_id") or "").strip(),
            "business_id": str(batch.get("business_id") or "").strip(),
        }

    @staticmethod
    def _set_saved_number(variable: tk.StringVar, raw: object) -> None:
        text = str(raw if raw is not None else "").strip()
        if text.isdigit():
            variable.set(text)

    def _restore_saved_selection(self) -> None:
        """Chọn lại tài khoản và Business Manager đã dùng lần trước."""
        prefs = self._saved_prefs
        acc_id = str(prefs.get("account_id") or "").strip()
        if acc_id:
            for label, stored in self._account_by_label.items():
                if stored == acc_id:
                    self._account.set(label)
                    break
        business_id = str(prefs.get("business_id") or "").strip()
        if business_id:
            self._business.set(business_id)
        self._reload_businesses()

    def _bind_ui_pref_autosave(self) -> None:
        """Đổi thông số là ghi lại, không cần bấm nút lưu."""

        def _schedule(*_args: object) -> None:
            self._schedule_pref_save()

        for variable in (
            self._mode,
            self._count,
            self._topic,
            self._name_lang,
            self._auto_details,
            self._avatar_dir,
            self._create_path,
            self._delay_on,
            self._delay_mode,
            self._fixed,
            self._delay_min,
            self._delay_max,
            self._continue,
            self._max_retry,
            self._account,
            self._business,
        ):
            variable.trace_add("write", _schedule)
        self._admins.bind("<<Modified>>", self._on_admins_modified, add="+")

    def _on_admins_modified(self, _event: object | None = None) -> None:
        try:
            if not self._admins.edit_modified():
                return
            self._admins.edit_modified(False)
        except tk.TclError:
            return
        self._schedule_pref_save()

    def _schedule_pref_save(self) -> None:
        job = self._pref_save_job
        if job:
            try:
                self.after_cancel(job)
            except tk.TclError:
                pass
        try:
            self._pref_save_job = self.after(400, self._persist_ui_prefs)
        except tk.TclError:
            self._pref_save_job = None

    def _persist_ui_prefs(self) -> None:
        """Ghi toàn bộ thông số form để lần mở sau không phải chọn lại."""
        self._pref_save_job = None
        try:
            if not self.winfo_exists():
                return
        except tk.TclError:
            return
        try:
            admins = self._admins.get("1.0", tk.END).strip()
        except tk.TclError:
            admins = ""
        try:
            PageCreationStore().save_ui_prefs(
                {
                    "avatar_dir": self._avatar_dir.get().strip(),
                    "name_lang": self._name_lang.get(),
                    "auto_details": bool(self._auto_details.get()),
                    "topic": self._topic.get().strip(),
                    "count": self._count.get().strip(),
                    "create_path": self._create_path.get(),
                    "mode": self._mode.get(),
                    "delay_enabled": bool(self._delay_on.get()),
                    "delay_mode": self._delay_mode.get(),
                    "delay_fixed_seconds": self._fixed.get().strip(),
                    "delay_min_seconds": self._delay_min.get().strip(),
                    "delay_max_seconds": self._delay_max.get().strip(),
                    "continue_on_error": bool(self._continue.get()),
                    "max_retry": self._max_retry.get().strip(),
                    "admin_targets_text": admins,
                    "account_id": self._selected_account_id(),
                    "business_id": self._selected_business_id(),
                }
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không lưu prefs tạo Page: {}", exc)

    def _on_close(self) -> None:
        """Đóng dialog và lưu prefs form."""
        job = self._pref_save_job
        if job:
            try:
                self.after_cancel(job)
            except tk.TclError:
                pass
            self._pref_save_job = None
        self._persist_ui_prefs()
        self.destroy()

    def _topic_text(self) -> str:
        typed = self._topic.get().strip()
        if typed:
            return typed
        account = self._selected_account_row()
        return topic_from_account_name(str(account.get("name") or account.get("email") or ""))

    def _page_count(self) -> int:
        try:
            count = int(self._count.get().strip())
        except ValueError:
            return 0
        return count

    def _on_generate(self) -> None:
        count = self._page_count()
        if self._mode.get() == "single":
            count = 1
        if count < 1 or count > 2000:
            messagebox.showerror("Số Page", "Nhập số Page từ 1 đến 2000.", parent=self)
            return
        folder = self._avatar_dir.get().strip()
        topic = self._topic_text()
        if self._ui_busy:
            return
        if self._rows and not messagebox.askyesno("Sinh lại", "Thay danh sách Page hiện tại?", parent=self):
            return
        self._ui_busy = True
        self._folder_hint.set(f"Đang sinh {count} tên...")
        language = self._name_lang.get()
        auto_details = bool(self._auto_details.get())
        self._persist_ui_prefs()

        def _work() -> None:
            try:
                images = image_files_in_folder(folder) if folder else []
                names = generate_page_names(topic, count, language=language)
                if images:
                    avatars = assign_random_avatars(len(names), images)
                    rows = [{"page_name": name, "avatar_path": avatar} for name, avatar in zip(names, avatars)]
                else:
                    rows = [{"page_name": name, "avatar_path": ""} for name in names]
                rows = attach_generated_details(rows, language=language, enabled=auto_details)
                error = ""
            except Exception as exc:  # noqa: BLE001
                images, names, rows, error = [], [], [], str(exc)
                logger.warning("Sinh danh sách Page lỗi: {}", exc)

            def _apply() -> None:
                self._ui_busy = False
                if not self.winfo_exists():
                    return
                if error:
                    self._folder_hint.set("Không sinh được danh sách.")
                    messagebox.showerror("Sinh danh sách", error, parent=self)
                    return
                self._rows = rows
                detail_note = " kèm thông tin tự sinh" if auto_details else ""
                if images:
                    self._folder_hint.set(f"Đã sinh {len(rows)} Page{detail_note}. Thư mục có {len(images)} ảnh.")
                else:
                    self._folder_hint.set(f"Đã sinh {len(rows)} Page{detail_note}, không gắn ảnh đại diện.")
                if len(names) < count:
                    messagebox.showwarning("Sinh danh sách", f"Chỉ sinh được {len(names)} tên không trùng.", parent=self)
                self._paint_pages()

            schedule_on_main_thread(self, _apply)

        threading.Thread(target=_work, daemon=True, name="page_name_gen").start()

    def _reshuffle_details(self) -> None:
        """Sinh lại mô tả/SĐT/email/web/địa chỉ cho danh sách hiện tại."""
        if not self._rows:
            messagebox.showinfo("Thông tin Page", "Chưa có Page trong danh sách.", parent=self)
            return
        language = self._name_lang.get()
        self._rows = attach_generated_details(self._rows, language=language, enabled=True)
        self._auto_details.set(True)
        self._paint_pages()
        self._append_log(f"Đã sinh lại thông tin cho {len(self._rows)} Page ({language}).")

    def _reshuffle_avatars(self) -> None:
        if not self._rows:
            messagebox.showinfo("Đổi ảnh", "Chưa có Page trong danh sách.", parent=self)
            return
        images = self._folder_images()
        if not images:
            messagebox.showinfo("Đổi ảnh", "Chưa có thư mục ảnh. Các Page sẽ tạo chỉ với tên.", parent=self)
            return
        avatars = assign_random_avatars(len(self._rows), images)
        for row, path in zip(self._rows, avatars):
            row["avatar_path"] = path
        self._paint_pages()

    def _add_page(self) -> None:
        images = self._folder_images()
        name = simple_prompt(self, "Thêm Page", "Tên Page. Để trống rồi bấm OK nếu muốn tự sinh.")
        if name is None:
            return
        if not name:
            topic = self._topic_text()
            if not topic:
                messagebox.showerror("Chủ đề", "Nhập chủ đề hoặc gõ tên Page.", parent=self)
                return
            existing = [row.get("page_name") or "" for row in self._rows]
            generated = generate_page_names(
                topic, 1, language=self._name_lang.get(), avoid=existing
            )
            if not generated:
                messagebox.showerror("Thêm Page", "Không sinh được tên Page.", parent=self)
                return
            name = generated[0]
        avatar = assign_random_avatars(1, images)[0] if images else ""
        row = {"page_name": name.strip(), "avatar_path": avatar}
        if self._auto_details.get():
            row = attach_generated_details([row], language=self._name_lang.get(), enabled=True)[0]
        self._rows.append(row)
        self._paint_pages()

    def _remove_page(self) -> None:
        selected = self._pages.selection()
        if not selected:
            return
        index = int(self._pages.item(selected[0], "values")[0]) - 1
        if 0 <= index < len(self._rows):
            del self._rows[index]
        self._paint_pages()

    def _import_file(self) -> None:
        path = filedialog.askopenfilename(
            parent=self,
            title="Import Page",
            filetypes=[("CSV/JSON", "*.csv *.json"), ("Tất cả", "*.*")],
        )
        if not path:
            return
        text = Path(path).read_text(encoding="utf-8")
        kind = "json" if path.lower().endswith(".json") else "csv"
        try:
            rows = parse_page_import(text, kind=kind)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Import", str(exc), parent=self)
            return
        if not rows:
            messagebox.showwarning("Import", "Không đọc được dòng Page nào.", parent=self)
            return
        preview = "\n".join(f"{i}. {r['page_name']} — {r['avatar_path']}" for i, r in enumerate(rows, start=1))
        if not messagebox.askyesno("Xác nhận import", preview[:2000], parent=self):
            return
        self._rows.extend(rows)
        self._paint_pages()

    def _on_validate(self) -> None:
        self._check_pages(create=False)

    def _on_create(self) -> None:
        self._check_pages(create=True)

    def _check_pages(self, *, create: bool) -> None:
        """Kiểm tra cả danh sách trên thread nền rồi mới hỏi tạo."""
        if self._ui_busy:
            return
        if not self._release_stale_batch():
            return
        pages = self._selected_pages()
        account_id = self._selected_account_id()
        create_mode = self._create_path.get() or "bm"
        business_id = "" if create_mode == "profile" else self._selected_business_id()
        known_accounts = {str(row.get("id") or "") for row in self._accounts}
        known_business = self._known_business_ids()
        busy = self._busy_accounts()
        self._ui_busy = True
        self._progress.set("Đang kiểm tra danh sách...")

        def _work() -> None:
            try:
                errors = validate_page_list(
                    account_id=account_id,
                    business_id=business_id,
                    pages=pages,
                    known_account_ids=known_accounts,
                    known_business_ids=known_business,
                    busy_account_ids=busy,
                    create_mode=create_mode,
                )
                error = ""
            except Exception as exc:  # noqa: BLE001
                errors, error = [], str(exc)
                logger.warning("Kiểm tra danh sách Page lỗi: {}", exc)

            def _apply() -> None:
                self._ui_busy = False
                if not self.winfo_exists():
                    return
                if error:
                    messagebox.showerror("Kiểm tra", error, parent=self)
                    self._progress.set("Không kiểm tra được danh sách.")
                    return
                if errors:
                    shown = errors[:30]
                    if len(errors) > 30:
                        shown.append(f"... và {len(errors) - 30} lỗi nữa.")
                    messagebox.showerror("Kiểm tra", "\n".join(shown), parent=self)
                    self._progress.set("Danh sách chưa hợp lệ.")
                    return
                if not create:
                    mode_label = "Profile" if create_mode == "profile" else "Business Manager"
                    admins = self._admin_targets()
                    extra = f" · {len(admins)} quản trị viên" if admins else ""
                    messagebox.showinfo(
                        "Kiểm tra",
                        f"Sẵn sàng tạo {len(pages)} Page qua {mode_label}{extra}.",
                        parent=self,
                    )
                    self._progress.set(f"Hợp lệ {len(pages)} Page ({mode_label}).")
                    return
                head = pages[:12]
                preview = "\n".join(f"{index}. {row['page_name']}" for index, row in enumerate(head, start=1))
                if len(pages) > len(head):
                    preview += f"\n... và {len(pages) - len(head)} Page nữa"
                mode_label = "Profile" if create_mode == "profile" else "Business Manager"
                admins = self._admin_targets()
                admin_note = f"\nThêm quản trị viên: {len(admins)} người" if admins else ""
                if not messagebox.askyesno(
                    "Tạo batch",
                    f"Tạo {len(pages)} Page qua {mode_label}?{admin_note}\n\n{preview}",
                    parent=self,
                ):
                    self._progress.set("Đã hủy tạo batch.")
                    return
                self._start_batch(pages)

            schedule_on_main_thread(self, _apply)

        threading.Thread(target=_work, daemon=True, name="page_validate").start()

    def _start_batch(self, pages: list[dict[str, str]]) -> None:
        """Ghi hàng đợi ngoài luồng giao diện rồi mới chạy batch."""
        account_id = self._selected_account_id()
        create_mode = self._create_path.get() or "bm"
        business_id = "" if create_mode == "profile" else self._selected_business_id()
        settings = self._settings()
        self._progress.set(f"Đang ghi hàng đợi {len(pages)} Page...")

        def _work() -> None:
            try:
                store = PageCreationStore()
                batch, _jobs = store.create_batch(
                    account_id=account_id,
                    business_id=business_id,
                    pages=pages,
                    settings=settings,
                )
                error = ""
            except Exception as exc:  # noqa: BLE001
                batch, error = {}, str(exc)
                logger.warning("Ghi hàng đợi Page lỗi: {}", exc)

            def _apply() -> None:
                if not self.winfo_exists():
                    return
                if error or not batch.get("id"):
                    messagebox.showerror("Tạo batch", error or "Không ghi được hàng đợi.", parent=self)
                    return
                self._batch_id = str(batch["id"])
                engine = get_engine()
                threading.Thread(
                    target=engine.run_batch,
                    args=(self._batch_id,),
                    daemon=True,
                    name=f"page_batch_{self._batch_id}",
                ).start()
                mode_label = "Profile" if create_mode == "profile" else "BM"
                self._append_log(f"Đã tạo batch {self._batch_id} · {mode_label}")

            schedule_on_main_thread(self, _apply)

        threading.Thread(target=_work, daemon=True, name="page_batch_write").start()

    def _on_create_business(self) -> None:
        account_id = self._selected_account_id()
        if not account_id:
            messagebox.showerror("Tài khoản", "Hãy chọn tài khoản trước khi tạo Business Manager.", parent=self)
            return
        account = self._selected_account_row()
        suggested = topic_from_account_name(str(account.get("name") or "")) or "Business"
        typed = simple_prompt(self, "Tạo Business Manager", f"Tên doanh nghiệp cho {self._account.get()}")
        if typed is None:
            return
        business_name = typed or suggested
        self._append_log(f"Đang tạo Business Manager «{business_name}» cho tài khoản đã chọn...")

        def _work() -> None:
            from src.services.page_creation.business_creator import create_business_for_account

            result = create_business_for_account(account_id, business_name)

            def _apply() -> None:
                if self.winfo_exists():
                    self._finish_business(account_id, business_name, result)

            schedule_on_main_thread(self, _apply)

        threading.Thread(target=_work, daemon=True, name="create_business_manager").start()

    def _finish_business(self, account_id: str, business_name: str, result) -> None:
        from src.services.page_creation.business_store import BusinessManagerStore

        if not result.ok or not result.page_id:
            detail = result.error_message or result.error_code or "Không tạo được Business Manager."
            self._append_log(detail)
            if result.error_code in {
                "CAPTCHA_DETECTED",
                "CHECKPOINT",
                "SESSION_EXPIRED",
                "RATE_LIMITED",
                "SMS_VERIFICATION_REQUIRED",
                "SECURITY_VERIFICATION_REQUIRED",
            }:
                detail += "\n\nHãy bấm Mở tài khoản, xử lý trên trình duyệt, rồi tạo lại. Tool không tự vượt bước xác minh."
            messagebox.showwarning("Business Manager", detail, parent=self)
            return
        BusinessManagerStore().save(
            account_id=account_id,
            business_id=result.page_id,
            business_name=business_name,
        )
        self._reload_businesses()
        self._append_log(f"Business Manager {business_name} · {result.page_id}")
        messagebox.showinfo(
            "Business Manager",
            f"Đã gắn {business_name}\nID: {result.page_id}",
            parent=self,
        )

    def _on_pause(self) -> None:
        if self._batch_id:
            get_engine().pause(self._batch_id)

    def _on_resume(self) -> None:
        if not self._batch_id:
            return
        engine = get_engine()
        batch = PageCreationStore().get_batch(self._batch_id)
        if batch and str(batch.get("account_creation_lock") or "") == "SECURITY_VERIFICATION_REQUIRED":
            messagebox.showinfo(
                "Xác minh bảo mật",
                "Account đang bị Facebook khóa tạo Page (SMS / CAPTCHA / checkpoint).\n"
                "Hãy xử lý trên mobile app hoặc trình duyệt, rồi bấm Kiểm tra lại.",
                parent=self,
            )
            return
        engine.resume(self._batch_id)
        if not engine.is_running(self._batch_id):
            threading.Thread(
                target=engine.run_batch,
                args=(self._batch_id,),
                daemon=True,
                name=f"page_batch_{self._batch_id}",
            ).start()

    def _on_check_again(self) -> None:
        """Sau khi user hoàn tất SMS/CAPTCHA: kiểm tra lại rồi chạy tiếp queue."""
        if not self._batch_id:
            messagebox.showinfo("Kiểm tra lại", "Chưa có batch để kiểm tra.", parent=self)
            return
        if self._ui_busy:
            return
        self._ui_busy = True
        self._append_log("Đang kiểm tra lại trạng thái account trên Facebook…")

        def _work() -> None:
            try:
                outcome = get_engine().check_again(self._batch_id)
            except Exception as exc:  # noqa: BLE001
                outcome = None
                error = str(exc)
            else:
                error = ""

            def _apply() -> None:
                self._ui_busy = False
                if not self.winfo_exists():
                    return
                if error:
                    self._append_log(error)
                    messagebox.showwarning("Kiểm tra lại", error, parent=self)
                    return
                assert outcome is not None
                if not outcome.ok:
                    detail = outcome.error_message or outcome.error_code or "Account vẫn bị khóa xác minh."
                    self._append_log(detail)
                    messagebox.showwarning("Kiểm tra lại", detail, parent=self)
                    return
                self._security_alerted = ""
                self._append_log(outcome.error_message or "Account READY — tiếp tục queue.")
                engine = get_engine()
                if not engine.is_running(self._batch_id):
                    threading.Thread(
                        target=engine.run_batch,
                        args=(self._batch_id,),
                        daemon=True,
                        name=f"page_batch_{self._batch_id}",
                    ).start()
                messagebox.showinfo(
                    "Kiểm tra lại",
                    "Facebook cho phép tạo Page. Đang chạy tiếp từ job đang dừng.",
                    parent=self,
                )

            schedule_on_main_thread(self, _apply)

        threading.Thread(target=_work, daemon=True, name="page_check_again").start()

    def _on_cancel(self) -> None:
        if self._batch_id and messagebox.askyesno("Hủy batch", "Job đang chờ sẽ bị hủy. Page đã tạo được giữ.", parent=self):
            get_engine().cancel(self._batch_id)

    def _on_open_account(self) -> None:
        account_id = self._selected_account_id()
        if not account_id:
            return
        def _open() -> None:
            try:
                _hold_account_open(account_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Mở tài khoản dừng: {}", exc)

        threading.Thread(target=_open, daemon=True, name="page_open_account").start()
        self._append_log("Đang mở tài khoản để bạn xác minh. Hãy đóng cửa sổ trình duyệt trước khi bấm Tiếp tục.")

    def _on_open_business_browser(self) -> None:
        """Mở Business Suite để tự tạo Business Manager hoặc xem danh mục."""
        account_id = self._selected_account_id()
        if not account_id:
            messagebox.showerror("Tài khoản", "Hãy chọn tài khoản trước khi mở trình duyệt.", parent=self)
            return
        if self._manual_browser:
            messagebox.showinfo("Trình duyệt", "Cửa sổ trình duyệt đang mở. Hãy dùng cửa sổ đó, hoặc đóng rồi mở lại.", parent=self)
            return
        from src.services.page_creation.business_creator import business_preview_url

        self._manual_browser = True
        self._asked_close_browser = False
        self._close_browser.clear()
        start_url = business_preview_url(self._selected_business_id())
        self._append_log("Đang mở Business Suite và đọc Business Manager đã có trên tài khoản.")

        def _on_rows(code: str, rows: list[dict[str, str]]) -> None:
            def _apply() -> None:
                if not self.winfo_exists():
                    return
                if code in {
                    "CAPTCHA_DETECTED",
                    "CHECKPOINT",
                    "SESSION_EXPIRED",
                    "RATE_LIMITED",
                    "SMS_VERIFICATION_REQUIRED",
                    "SECURITY_VERIFICATION_REQUIRED",
                }:
                    self._append_log("Facebook đang yêu cầu xác minh. Hãy xử lý trên trình duyệt, BM sẽ được đọc khi trang có id.")
                    return
                saved = self._store_portfolios(account_id, rows)
                if saved and not self._asked_close_browser:
                    self._asked_close_browser = True
                    self._ask_close_browser(saved)

            schedule_on_main_thread(self, _apply)

        def _work() -> None:
            try:
                last_url = _hold_account_open(
                    account_id,
                    start_url=start_url,
                    on_portfolios=_on_rows,
                    should_close=self._close_browser,
                )
                error = ""
            except Exception as exc:  # noqa: BLE001
                last_url, error = "", str(exc)
                logger.warning("Không mở được Business Suite: {}", exc)

            def _apply() -> None:
                self._manual_browser = False
                if not self.winfo_exists():
                    return
                if error:
                    self._append_log(error)
                    messagebox.showwarning("Trình duyệt", error, parent=self)
                    return
                self._remember_business_from_url(account_id, last_url)

            schedule_on_main_thread(self, _apply)

        threading.Thread(target=_work, daemon=True, name="page_open_business").start()

    def _store_portfolios(self, account_id: str, rows: list[dict[str, str]]) -> str:
        """Ghi BM đọc được và chọn trên combobox. Tên đã có thì không bị id ghi đè."""
        from src.services.page_creation.business_store import BusinessManagerStore

        if not rows:
            self._append_log("Chưa đọc được Business Manager nào. Nếu cửa sổ vẫn mở, hãy đứng ở danh mục rồi đóng để tải lại.")
            return ""
        store = BusinessManagerStore()
        known = {
            str(row.get("business_id") or ""): str(row.get("business_name") or "")
            for row in store.for_account(account_id)
        }
        prefer = ""
        for row in rows:
            business_id = str(row.get("business_id") or "").strip()
            if not business_id:
                continue
            incoming = str(row.get("business_name") or "").strip()
            current = known.get(business_id, "")
            if current and (not incoming or incoming == f"BM {business_id}"):
                incoming = current
            store.save(account_id=account_id, business_id=business_id, business_name=incoming or f"BM {business_id}")
            known[business_id] = incoming or f"BM {business_id}"
            prefer = prefer or business_id
        if prefer and not self._selected_business_id():
            self._business.set(prefer)
        self._reload_businesses()
        names = ", ".join(
            business_display_label(known[bid], bid) for bid in known if any(str(row.get("business_id") or "") == bid for row in rows)
        )
        self._append_log(f"Đã tải {len(rows)} Business Manager: {names}")
        return names

    def _ask_close_browser(self, names: str) -> None:
        """Hỏi sau khi đã có BM. Có thì đóng, không thì giữ cửa sổ để xem tiếp."""
        try:
            self.lift()
            self.attributes("-topmost", True)
            self.focus_force()
        except tk.TclError:
            pass
        try:
            close_it = messagebox.askyesno(
                "Business Manager",
                f"Đã tải Business Manager:\n{names}\n\nBạn có muốn đóng trình duyệt không?",
                parent=self,
            )
        finally:
            try:
                self.attributes("-topmost", False)
            except tk.TclError:
                pass
        if close_it:
            self._close_browser.set()
            self._append_log("Đang đóng trình duyệt.")
            return
        self._append_log("Giữ trình duyệt mở để bạn xem tiếp.")

    def _remember_business_from_url(self, account_id: str, url: str) -> None:
        """Nếu trang cuối có business id thì chọn BM đó. BM mới được lưu vào danh sách."""
        from src.services.page_creation.business_creator import extract_business_id
        from src.services.page_creation.business_store import BusinessManagerStore

        business_id = extract_business_id(url)
        if not business_id:
            if not self._asked_close_browser:
                self._append_log("Đã đóng trình duyệt. Trang cuối chưa có Business Manager id.")
            self._reload_businesses()
            return
        store = BusinessManagerStore()
        known = {str(row.get("business_id") or ""): str(row.get("business_name") or "") for row in store.for_account(account_id)}
        if business_id not in known:
            store.save(account_id=account_id, business_id=business_id, business_name=f"BM {business_id}")
            self._append_log(f"Đã lưu Business Manager {business_id} vừa xem trên trình duyệt.")
        else:
            self._append_log(f"Đã chọn Business Manager {known[business_id] or business_id}.")
        self._business.set(business_id)
        self._reload_businesses()

    def _on_job_action(self, _event: object) -> None:
        if not self._batch_id:
            return
        selected = self._jobs.selection()
        if not selected:
            return
        tags = self._jobs.item(selected[0], "tags")
        if len(tags) < 2:
            return
        job_id = str(tags[0])
        status = str(tags[1])
        engine = get_engine()
        if status == "FAILED" and job_id:
            engine.retry_job(self._batch_id, job_id)
            if not engine.is_running(self._batch_id):
                threading.Thread(target=engine.run_batch, args=(self._batch_id,), daemon=True).start()
        elif status in {"PENDING", "DELAYED"} and job_id:
            for job in PageCreationStore().jobs_for_batch(self._batch_id):
                if str(job.get("id")) == job_id:
                    job["status"] = "CANCELLED"
                    PageCreationStore().save_job(job)

    def _tick(self) -> None:
        try:
            self._paint_progress()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Cập nhật tiến độ Page Creator: {}", exc)
        if self.winfo_exists():
            self.after(400, self._tick)

    def _paint_progress(self) -> None:
        if not self._batch_id:
            return
        snap = get_engine().progress(self._batch_id)
        current = snap.get("current")
        nxt = snap.get("next")
        current_name = current[2] if current else "-"
        current_status = _status_vi(current[3]) if current else ""
        next_name = nxt[2] if nxt else "-"
        step_message = str(snap.get("progress_message") or "").strip()
        line = (
            f"Batch {self._batch_id}  {_status_vi(str(snap.get('status') or ''))}  "
            f"Hoàn tất {snap.get('completed')}/{snap.get('total')} ({snap.get('percent')}%)  "
            f"Đang làm: {current_name} {current_status}  "
            f"Tiếp theo: {next_name}  "
            f"Chờ: {snap.get('delay_remaining')} giây"
        )
        if step_message:
            line = f"{line}\n→ {step_message}"
        self._progress.set(line)
        if current:
            marker = (current[0], current[3], step_message)
            if marker != self._last_current:
                self._last_current = marker
                if step_message and step_message != self._last_progress_message:
                    self._last_progress_message = step_message
                    self._append_log(f"{current_name}: {step_message}")
                else:
                    self._append_log(f"{current_name}: {current_status}")
            self._maybe_alert_security(current[0], current[3], snap.get("batch") or {})
        self._apply_job_rows(list(snap.get("rows") or []))

    def _maybe_alert_security(self, job_id: str, status: str, batch: dict[str, Any]) -> None:
        """Hiện một lần khi Facebook yêu cầu SMS / bảo mật."""
        if status not in {
            "SMS_VERIFICATION_REQUIRED",
            "CAPTCHA_DETECTED",
            "CHECKPOINT",
            "WAITING_REAUTH",
            "RATE_LIMITED",
        }:
            return
        key = f"{job_id}:{status}"
        if key == self._security_alerted:
            return
        self._security_alerted = key
        message = ""
        for job in PageCreationStore().jobs_for_batch(self._batch_id):
            if str(job.get("id")) == str(job_id):
                message = str(job.get("error_message") or "")
                break
        if not message and status == "SMS_VERIFICATION_REQUIRED":
            message = "Facebook yêu cầu hoàn tất SMS verification trên mobile app."
        if not message:
            message = "Facebook đang yêu cầu xác minh. Hãy xử lý thủ công rồi bấm Kiểm tra lại."
        self._append_log(message)
        messagebox.showwarning(
            "Xác minh bảo mật",
            f"{message}\n\nCác Page tiếp theo trên account này tạm dừng. Account khác không bị ảnh hưởng.\n"
            "Sau khi xác minh xong, bấm Kiểm tra lại.",
            parent=self,
        )

    def _apply_job_rows(self, rows: list[tuple[str, int, str, str, int, int]]) -> None:
        ids = tuple(row[0] for row in rows)
        if ids != self._job_ids:
            self._start_job_fill(rows)
            return
        if self._jobs_filling:
            return
        for row in changed_job_rows(self._job_state, rows)[:40]:
            if not self._jobs.exists(row[0]):
                continue
            self._jobs.item(row[0], tags=(row[0], row[3]), values=_job_values(row))
            page_id = str(row[6]) if len(row) > 6 else ""
            self._job_state[row[0]] = (row[3], row[4], page_id)

    def _start_job_fill(self, rows: list[tuple[str, int, str, str, int, int]]) -> None:
        self._job_gen += 1
        generation = self._job_gen
        self._jobs_filling = True
        self._job_ids = tuple(row[0] for row in rows)
        self._job_state = {}
        tree_delete_all(self._jobs)
        specs = [
            {"iid": row[0], "tags": (row[0], row[3]), "values": _job_values(row)}
            for row in rows
        ]

        def _current(gen: int) -> bool:
            return gen == self._job_gen and self.winfo_exists()

        def _done() -> None:
            if generation != self._job_gen:
                return
            self._job_state = {
                row[0]: (row[3], row[4], str(row[6]) if len(row) > 6 else "") for row in rows
            }
            self._jobs_filling = False

        if not specs:
            self._jobs_filling = False
            return
        tree_insert_chunked(
            self,
            self._jobs,
            specs,
            generation=generation,
            is_current=_current,
            on_complete=_done,
            chunk=80,
        )

    def _append_log(self, line: str) -> None:
        self._log.insert(tk.END, line + "\n")
        try:
            last = int(self._log.index("end-1c").split(".", 1)[0])
        except (tk.TclError, ValueError):
            last = 0
        if last > 400:
            self._log.delete("1.0", f"{last - 300}.0")
        self._log.see(tk.END)


def simple_prompt(master: tk.Misc, title: str, label: str) -> str | None:
    """Hộp nhập một dòng. Đóng cửa sổ thì trả None."""
    win = tk.Toplevel(master)
    win.title(title)
    value = tk.StringVar()
    ttk.Label(win, text=label).pack(padx=8, pady=6)
    entry = ttk.Entry(win, textvariable=value, width=42)
    entry.pack(padx=8, pady=4)
    entry.focus_set()
    out: dict[str, str | None] = {"text": None}

    def _ok() -> None:
        out["text"] = value.get().strip()
        win.destroy()

    ttk.Button(win, text="OK", command=_ok).pack(pady=8)
    win.grab_set()
    win.wait_window()
    return out["text"]


def _report_open_portfolios(page, on_portfolios) -> None:
    """Đọc BM đang có trên trang rồi báo về giao diện."""
    from src.services.page_creation.business_creator import read_business_portfolios

    try:
        code, rows = read_business_portfolios(page)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không đọc được Business Manager đang có: {}", exc)
        return
    on_portfolios(code, rows)


def _hold_account_open(
    account_id: str,
    start_url: str = "https://www.facebook.com/",
    on_portfolios=None,
    should_close: threading.Event | None = None,
) -> str:
    """Mở profile và giữ đến khi người dùng đóng cửa sổ. Trả URL cuối. Không tự giải CAPTCHA."""
    from src.automation.browser_factory import BrowserFactory, prepare_playwright_sync_thread, sync_close_persistent_context
    from src.utils.account_proxy_mapper import prepare_account_dict_for_browser_run
    from src.utils.db_manager import AccountsDatabaseManager

    factory = None
    context = None
    try:
        row = AccountsDatabaseManager().get_by_id(account_id)
        if row is None:
            raise RuntimeError("Không tìm thấy tài khoản.")
        prepare_playwright_sync_thread(label=f"page-open:{account_id}")
        acc = prepare_account_dict_for_browser_run(dict(row), require_proxy_live=False)
        factory = BrowserFactory(headless=False, playwright_shared=True)
        context = factory.launch_persistent_context_from_account_dict(
            acc,
            headless=False,
            disable_notifications=True,
            force_desktop_facebook=True,
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(start_url, wait_until="domcontentloaded", timeout=60_000)
        last_url = page.url or start_url
        if on_portfolios is not None and "business.facebook.com" in start_url:
            _report_open_portfolios(page, on_portfolios)
        seen_url = ""
        for _ in range(3600):
            if should_close is not None and should_close.is_set():
                break
            if page.is_closed():
                break
            try:
                last_url = page.url or last_url
            except Exception:  # noqa: BLE001
                break
            if on_portfolios is not None and last_url != seen_url:
                seen_url = last_url
                from src.services.page_creation.business_creator import portfolios_from_markup

                found = portfolios_from_markup(last_url)
                if found:
                    on_portfolios("", found)
            page.wait_for_timeout(1000)
        return last_url
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không mở được account {}: {}", account_id, exc)
        raise
    finally:
        sync_close_persistent_context(context, log_label="page-open-account", same_thread=True)
        if factory is not None:
            try:
                factory.close()
            except Exception:  # noqa: BLE001
                pass


def open_page_creator_dialog(master: tk.Misc) -> None:
    """Mở cửa sổ Page Creator."""
    PageCreatorDialog(master)
