"""
Bảng điều khiển đồ họa: tab Tài khoản (accounts.json) + tab Page/Group (pages.json),
Verify Profile / kiểm tra proxy, nhật ký, bật–tắt lịch APScheduler.

Chạy: ``python main.py --gui``.
"""

from __future__ import annotations

import copy
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import tkinter as tk
from datetime import datetime, timezone
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable
from zoneinfo import ZoneInfo

from loguru import logger

from src.automation.browser_factory import BrowserFactory, sync_close_persistent_context
from src.gui.account_management import (
    AccountFormDialog,
    _coerce_use_proxy,
    _normalize_post_status,
    export_accounts_json,
    import_accounts_append,
    template_new_account,
)
from src.gui.cookie_capture import account_cookie_path_field, cookie_storage_dest, run_fb_cookie_capture_dialog
from src.gui.page_management import PageFormDialog
from src.gui.page_scan_dialog import PageScanDialog
from src.gui.schedule_job_dialog import SchedulePostJobDialog
from src.modules.browser_engine import BrowserEngine
from src.services.app_updater import (
    UpdateManifest,
    apply_update_package,
    github_latest_manifest_url,
    is_newer_version,
    read_local_version,
    read_manifest_from_url,
    resolve_manifest_url,
)
from src.scheduler import run_forever, run_scheduled_post_for_account
from src.utils.app_secrets import (
    add_openai_key_entry,
    apply_openai_key_to_environ,
    add_gemini_key_entry,
    apply_gemini_key_to_environ,
    add_nanobanana_key_entry,
    apply_nanobanana_key_to_environ,
    clear_saved_gemini_key_and_sync_environ,
    clear_saved_openai_keys_and_sync_environ,
    clear_saved_nanobanana_keys_and_sync_environ,
    delete_gemini_key_entry,
    delete_openai_key_entry,
    delete_nanobanana_key_entry,
    gemini_key_status_lines,
    list_gemini_key_rows_for_ui,
    list_openai_key_rows_for_ui,
    list_nanobanana_key_rows_for_ui,
    nanobanana_key_status_lines,
    openai_key_status_lines,
    get_nanobanana_runtime_config,
    save_nanobanana_runtime_config,
    set_preferred_gemini_key_id,
    set_preferred_openai_key_id,
    set_preferred_nanobanana_key_id,
)
from src.utils.app_restart import relaunch_same_app_and_exit
from src.utils.db_manager import AccountRecord, AccountsDatabaseManager
from src.utils.pages_manager import PageRecord, PagesManager
from src.utils.page_schedule import scheduler_tz
from src.utils.schedule_posts_filters import (
    RETRY_MODES,
    apply_job_filters,
    is_overdue,
    sort_jobs,
)
from src.utils.schedule_posts_manager import get_default_schedule_posts_manager
from src.utils.schedule_posts_missing_fields import (
    MISSING_FIELD_LABELS,
    MISSING_FIELD_PRESETS,
    filter_jobs_by_missing_fields,
    format_missing_fields_for_display,
    get_missing_fields,
    preset_by_label,
)
from src.utils.browser_exe_discover import find_browser_exe_in_directory
from src.utils.github_repo_detect import github_owner_repo_from_git
from src.utils.paths import project_root
from src.utils.proxy_check import check_http_proxy


class _GuiLogStream:
    """
    Stream ghi log (tương thích Loguru) đẩy dòng chữ vào ``tk.Text`` qua ``after`` (thread-safe với Tk).
    """

    def __init__(self, root: tk.Tk, text: tk.Text) -> None:
        """
        Khởi tạo stream gắn với widget log.

        Args:
            root: Cửa sổ Tk chính.
            text: Ô văn bản hiển thị log.
        """
        self._root = root
        self._text = text
        self._max_chars = 200_000

    def write(self, s: str) -> int:
        """
        Ghi một đoạn đã format vào hàng đợi cập nhật UI.

        Args:
            s: Chuỗi log (có thể nhiều dòng).

        Returns:
            Số ký tự đã nhận (API file-like).
        """
        if not s:
            return 0

        def append() -> None:
            self._text.configure(state="normal")
            self._text.insert("end", s)
            line_no = int(self._text.index("end-1c").split(".")[0])
            if line_no > 4000:
                self._text.delete("1.0", "800.0")
            self._text.see("end")
            self._text.configure(state="disabled")

        self._root.after(0, append)
        return len(s)

    def flush(self) -> None:
        """
        No-op (Loguru có thể gọi sau khi ghi).
        """
        return None


def _log_playwright_runtime_paths() -> None:
    """Ghi log đường dẫn trình duyệt Playwright khi mở GUI (hỗ trợ bản EXE / máy lạ)."""
    frozen = getattr(sys, "frozen", False)
    pw_raw = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "").strip()
    ch_raw = os.environ.get("FB_PLAYWRIGHT_CHROMIUM_CHANNEL", "").strip()
    proot = project_root()
    if pw_raw:
        p = Path(pw_raw)
        exists = p.is_dir()
        preview = ""
        if exists:
            try:
                dirs = sorted(x.name for x in p.iterdir() if x.is_dir())
                preview = ", ".join(dirs[:6]) + (" …" if len(dirs) > 6 else "")
            except OSError:
                preview = "(không đọc được thư mục)"
        logger.info(
            "Playwright runtime | frozen={} | PLAYWRIGHT_BROWSERS_PATH={} | tồn_tại={} | gói_con={!r} | FB_PLAYWRIGHT_CHROMIUM_CHANNEL={!r} | project_root={}",
            frozen,
            pw_raw,
            exists,
            preview,
            ch_raw,
            proot,
        )
    else:
        logger.info(
            "Playwright runtime | frozen={} | PLAYWRIGHT_BROWSERS_PATH=(mặc định cache hệ thống, vd. %LOCALAPPDATA%\\ms-playwright) | FB_PLAYWRIGHT_CHROMIUM_CHANNEL={!r} | project_root={}",
            frozen,
            ch_raw,
            proot,
        )


def run_manager_gui(*, accounts: AccountsDatabaseManager) -> None:
    """
    Mở cửa sổ quản lý và chạy vòng lặp ``mainloop`` Tk.

    Args:
        accounts: ``AccountsDatabaseManager`` đã preflight (dùng chung cho scheduler).
    """
    _log_playwright_runtime_paths()
    app = _ManagerWindow(accounts)
    app.run()


class _ManagerWindow:
    """
    Cửa sổ chính: bảng tài khoản, vùng log, nút bật/tắt lịch.
    """

    def __init__(self, accounts: AccountsDatabaseManager) -> None:
        """
        Dựng toàn bộ widget và trạng thái worker.

        Args:
            accounts: Manager JSON dùng cho scheduler và làm mới bảng.
        """
        self._accounts = accounts
        self._pages = PagesManager()
        self._schedule_posts = get_default_schedule_posts_manager()
        self._worker: threading.Thread | None = None
        self._stop_event: threading.Event | None = None
        self._log_sink_id: int | None = None
        self._acc_drag_anchor: str | None = None
        self._account_tick: dict[str, bool] = {}
        # Mỗi phiên: luồng Playwright riêng vẫn sống cho tới khi shutdown — đóng context/factory trên đúng luồng đó.
        self._manual_profile_sessions: list[dict[str, Any]] = []
        # Mặc định toàn hệ thống: ẩn browser (HEADLESS=1), người dùng có thể bật lại bằng nút «Hiện browser».
        self._show_browser = os.environ.get("HEADLESS", "1").strip().lower() in {"0", "false", "off", "no"}

        self._root = tk.Tk()
        self._app_version_str = read_local_version(project_root())
        self._root.title(f"Facebook Automation — Bảng điều khiển (v{self._app_version_str})")
        self._root.protocol("WM_DELETE_WINDOW", self._on_close)
        lock_raw = os.environ.get("FB_LOCK_BROWSER_DURING_JOB", "1").strip().lower()
        self._var_lock_browser_job = tk.BooleanVar(value=lock_raw not in {"0", "false", "off", "no"})
        os.environ["FB_LOCK_BROWSER_DURING_JOB"] = "1" if self._var_lock_browser_job.get() else "0"
        raw_pa = os.environ.get("SCHEDULE_PER_ACCOUNT_MAX_PARALLEL", "2").strip()
        try:
            per_acc = max(1, min(8, int(raw_pa)))
        except ValueError:
            per_acc = 2
        self._var_per_account_parallel = tk.StringVar(value=str(per_acc))
        os.environ["SCHEDULE_PER_ACCOUNT_MAX_PARALLEL"] = str(per_acc)
        os.environ["SCHEDULE_ALLOW_SAME_ACCOUNT_PARALLEL"] = "1" if per_acc > 1 else "0"

        # State cho tìm kiếm/lọc/sort danh sách job.
        self._all_jobs: list[dict[str, Any]] = []
        self._filtered_jobs: list[dict[str, Any]] = []
        self._job_page_name_by_id: dict[str, str] = {}
        self._jobs_sort_key: str = "scheduled_at"
        self._jobs_sort_asc: bool = True
        self._jobs_search_after_id: str | None = None
        self._all_pages: list[dict[str, Any]] = []
        self._pages_sort_key: str = "page_name"
        self._pages_sort_asc: bool = True
        self._pages_search_after_id: str | None = None
        self._ai_provider_view_var = tk.StringVar(value=self._load_ai_provider_pref_label())
        self._ai_provider_selector: ttk.Combobox | None = None
        self._tab_ai_canvas: tk.Canvas | None = None
        self._tab_ai_scrollbar: ttk.Scrollbar | None = None
        self._tab_ai_content: ttk.Frame | None = None
        self._tab_ai_window_id: int | None = None
        self._ai_widgets_gemini: list[tk.Widget] = []
        self._ai_widgets_openai: list[tk.Widget] = []
        self._latest_update_manifest: UpdateManifest | None = None
        # Watchdog UI: phát hiện main-thread bị block (dễ gây "Not Responding").
        self._ui_watchdog_interval_ms = 250
        self._ui_watchdog_threshold_sec = 1.5
        self._ui_watchdog_last_tick = time.monotonic()
        self._ui_watchdog_after_id: str | None = None
        self._ui_busy_label: str = ""

        main = ttk.Frame(self._root, padding=8)
        main.pack(fill=tk.BOTH, expand=True)

        title = ttk.Label(
            main,
            text="Facebook Automation — Lịch (scheduler) + Account Source + Page/Group",
            font=("Segoe UI", 12, "bold"),
        )
        title.pack(anchor="w", pady=(0, 6))

        bar = ttk.Frame(main)
        bar.pack(fill=tk.X, pady=(0, 6))
        self._btn_start = ttk.Button(bar, text="Bắt đầu lịch", command=self._on_start)
        self._btn_start.pack(side=tk.LEFT, padx=(0, 6))
        self._btn_stop = ttk.Button(bar, text="Dừng lịch", command=self._on_stop, state=tk.DISABLED)
        self._btn_stop.pack(side=tk.LEFT, padx=(0, 6))
        self._btn_refresh = ttk.Button(
            bar, text="Làm mới tất cả (accounts + pages + job lịch)", command=self._refresh_all
        )
        self._btn_refresh.pack(side=tk.LEFT, padx=(0, 6))
        self._btn_migrate = ttk.Button(
            bar,
            text="Migrate dữ liệu cũ → mới",
            command=self._on_migrate_user_data,
        )
        self._btn_migrate.pack(side=tk.LEFT, padx=(0, 6))
        self._btn_show_browser = ttk.Button(bar, text="Hiện browser", command=lambda: self._set_browser_visibility(True))
        self._btn_show_browser.pack(side=tk.LEFT, padx=(12, 4))
        self._btn_hide_browser = ttk.Button(bar, text="Ẩn browser", command=lambda: self._set_browser_visibility(False))
        self._btn_hide_browser.pack(side=tk.LEFT, padx=(0, 6))
        self._btn_compact_multi = ttk.Button(
            bar,
            text="Preset multi-page compact",
            command=self._apply_multi_page_compact_preset,
        )
        self._btn_compact_multi.pack(side=tk.LEFT, padx=(0, 6))
        self._btn_check_updates = ttk.Button(bar, text="Kiểm tra cập nhật", command=self._on_check_updates)
        self._btn_check_updates.pack(side=tk.LEFT, padx=(0, 6))
        self._btn_apply_update = ttk.Button(bar, text="Cập nhật ngay", command=self._on_apply_update, state=tk.DISABLED)
        self._btn_apply_update.pack(side=tk.LEFT, padx=(0, 6))
        self._btn_update_channel = ttk.Button(
            bar,
            text="Cấu hình kênh cập nhật",
            command=self._on_configure_update_channel,
        )
        self._btn_update_channel.pack(side=tk.LEFT, padx=(0, 6))
        self._btn_reset_veo3_profile = ttk.Button(
            bar,
            text="Reset profile VEO3",
            command=self._on_reset_veo3_profiles,
        )
        self._btn_reset_veo3_profile.pack(side=tk.LEFT, padx=(0, 6))
        self._btn_ai_video = ttk.Button(bar, text="AI Video Gemini/Veo", command=self._on_open_ai_video_dialog)
        self._btn_ai_video.pack(side=tk.LEFT, padx=(0, 6))
        self._lbl_browser_mode = ttk.Label(bar, text="")
        self._lbl_browser_mode.pack(side=tk.LEFT, padx=(0, 8))
        self._lbl_state = ttk.Label(bar, text="Lịch: đang tắt")
        self._lbl_state.pack(side=tk.RIGHT)
        self._lbl_app_version = ttk.Label(
            bar,
            text=f"Phiên bản {self._app_version_str}",
            foreground="gray",
        )
        self._lbl_app_version.pack(side=tk.RIGHT, padx=(0, 12))
        self._set_browser_visibility(self._show_browser, update_env=False)

        body = ttk.PanedWindow(main, orient=tk.VERTICAL)
        body.pack(fill=tk.BOTH, expand=True)

        nb_host = ttk.Frame(body)
        nb_host.columnconfigure(0, weight=1)
        nb_host.rowconfigure(0, weight=1)
        nb = ttk.Notebook(nb_host)
        nb.grid(row=0, column=0, sticky="nsew", padx=2, pady=2)
        self._nb = nb

        tab_acc = ttk.Frame(nb, padding=4)
        nb.add(tab_acc, text="  1. Tài khoản (accounts.json)  ")
        tab_acc.columnconfigure(0, weight=1)
        tab_acc.rowconfigure(2, weight=1)
        ttk.Label(
            tab_acc,
            text="Danh tính: profile portable + proxy + cookie — không gộp Page/Group. "
            "Cột «☐»: tick các profile cần thao tác — «Xóa» / «Verify Profile» / «Kiểm tra proxy» ưu tiên các dòng đã tick; "
            "nếu không có tick nào thì dùng dòng đang chọn (Ctrl/Shift, kéo chuột, «Chọn tất cả»). "
            "Chuột phải: «Tick ☑ các dòng đang chọn» / «Bỏ tick» (giữ vùng bôi xanh nếu click phải trên dòng đã chọn).",
            font=("Segoe UI", 9),
            wraplength=920,
        ).grid(row=0, column=0, sticky="ew", pady=(0, 4))

        acc_bar = ttk.Frame(tab_acc)
        acc_bar.grid(row=1, column=0, sticky="ew", pady=(0, 4))
        ttk.Button(acc_bar, text="Thêm", command=self._on_add_account).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(acc_bar, text="Sửa", command=self._on_edit_account).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(acc_bar, text="Xóa", command=self._on_delete_account).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(acc_bar, text="Nhân bản", command=self._on_duplicate_account).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(acc_bar, text="Xuất JSON…", command=self._on_export_json).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(acc_bar, text="Nhập JSON…", command=self._on_import_json).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(acc_bar, text="Xuất dữ liệu tool…", command=self._on_export_tool_bundle).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(acc_bar, text="Nhập dữ liệu tool…", command=self._on_import_tool_bundle).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Separator(acc_bar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=8)
        ttk.Button(acc_bar, text="Verify Profile", command=self._on_verify_profile).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(acc_bar, text="Mở profile browser", command=self._on_open_profile_browser).pack(side=tk.LEFT, padx=(0, 4))
        self._btn_close_open_profiles = ttk.Button(
            acc_bar,
            text="Đóng profile đang mở",
            command=self._on_close_open_profiles,
        )
        self._btn_close_open_profiles.pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(acc_bar, text="Lấy cookie (Playwright)", command=self._on_capture_cookie_account).pack(
            side=tk.LEFT, padx=(0, 4)
        )
        ttk.Button(acc_bar, text="Kiểm tra proxy", command=self._on_check_proxy).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(acc_bar, text="Làm mới tab này", command=self._refresh_tree).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Separator(acc_bar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=8)
        ttk.Button(acc_bar, text="Chọn tất cả", command=self._on_accounts_select_all).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(acc_bar, text="Bỏ chọn", command=self._on_accounts_clear_selection).pack(side=tk.LEFT)

        acc_tree_fr = ttk.Frame(tab_acc)
        acc_tree_fr.grid(row=2, column=0, sticky="nsew")
        acc_tree_fr.columnconfigure(0, weight=1)
        acc_tree_fr.rowconfigure(0, weight=1)

        cols_acc = ("tick", "id", "name", "browser", "portable", "proxy", "cookie")
        self._tree_accounts = ttk.Treeview(
            acc_tree_fr, columns=cols_acc, show="headings", height=11, selectmode="extended"
        )
        headings_acc = {
            "tick": "Chọn",
            "id": "id",
            "name": "Tên",
            "browser": "Trình duyệt",
            "portable": "portable_path",
            "proxy": "proxy host:port",
            "cookie": "cookie_path",
        }
        widths_acc = (44, 96, 112, 68, 150, 100, 180)
        for c, w in zip(cols_acc, widths_acc):
            self._tree_accounts.heading(c, text=headings_acc[c])
            stretch = c in ("name", "portable", "cookie")
            self._tree_accounts.column(c, width=w, minwidth=28, stretch=stretch)
        sy_acc = ttk.Scrollbar(acc_tree_fr, orient=tk.VERTICAL, command=self._tree_accounts.yview)
        self._tree_accounts.configure(yscrollcommand=sy_acc.set)
        self._tree_accounts.grid(row=0, column=0, sticky="nsew")
        self._tree_accounts.bind("<Double-1>", lambda _e: self._on_edit_account())
        self._tree_accounts.bind("<Button-3>", self._on_tree_accounts_rclick)
        self._tree_accounts.bind("<ButtonPress-1>", self._on_account_tree_press_drag, add=True)
        self._tree_accounts.bind("<B1-Motion>", self._on_account_tree_motion_drag, add=True)
        self._tree_accounts.bind("<ButtonRelease-1>", self._on_account_tree_release_drag, add=True)
        sy_acc.grid(row=0, column=1, sticky="ns")

        tab_pg = ttk.Frame(nb, padding=4)
        nb.add(tab_pg, text="  2. Page / Group (pages.json)  ")
        tab_pg.columnconfigure(0, weight=1)
        tab_pg.rowconfigure(3, weight=1)
        ttk.Label(
            tab_pg,
            text="Thêm / sửa Page (URL, owner…). Lịch + AI theo từng bài: tab «3. Job lịch đăng» — «Job lịch đăng…» từ đây mở nhanh tab đó. "
            "Owner = id tài khoản; file pages.json.",
            font=("Segoe UI", 9),
            wraplength=920,
        ).grid(row=0, column=0, sticky="w", pady=(0, 4))
        pg_bar = ttk.Frame(tab_pg)
        pg_bar.grid(row=1, column=0, sticky="ew", pady=(0, 4))
        ttk.Button(pg_bar, text="Thêm Page/Group", command=self._on_add_page).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(
            pg_bar,
            text="Quét Page theo tài khoản",
            command=self._on_scan_pages_from_account,
        ).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(pg_bar, text="Sửa", command=self._on_edit_page).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(pg_bar, text="Job lịch đăng…", command=self._on_goto_jobs_for_page).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(pg_bar, text="Xóa", command=self._on_delete_page).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(pg_bar, text="Dọn trùng Meta ID", command=self._on_dedupe_pages_by_meta_id).pack(
            side=tk.LEFT, padx=(0, 4)
        )
        ttk.Button(pg_bar, text="Làm mới tab này", command=self._on_refresh_pages).pack(side=tk.LEFT)
        self._build_pages_filter_bar(tab_pg, row=2)

        pg_tree_fr = ttk.Frame(tab_pg)
        pg_tree_fr.grid(row=3, column=0, sticky="nsew")
        pg_tree_fr.columnconfigure(0, weight=1)
        pg_tree_fr.rowconfigure(0, weight=1)

        cols_pg = (
            "id",
            "account_id",
            "page_kind",
            "page_name",
            "ai_topic",
            "post_style",
            "schedule",
            "status",
            "last_post",
            "fb_page_id",
            "url",
        )
        self._tree_pages = ttk.Treeview(
            pg_tree_fr,
            columns=cols_pg,
            show="headings",
            height=11,
            selectmode="extended",
        )
        headings_pg = {
            "id": "id",
            "account_id": "owner",
            "page_kind": "Loại",
            "page_name": "Tên Page",
            "ai_topic": "Chủ đề AI",
            "post_style": "post_style",
            "schedule": "Lịch",
            "status": "Trạng thái",
            "last_post": "Đăng gần nhất",
            "fb_page_id": "Meta Page ID",
            "url": "Page_URL",
        }
        widths_pg = (72, 72, 56, 88, 100, 56, 52, 72, 88, 110, 140)
        for c, w in zip(cols_pg, widths_pg):
            self._tree_pages.heading(c, text=headings_pg[c], command=lambda k=c: self._on_pages_sort_click(k))
            self._tree_pages.column(c, width=w, stretch=True)
        sy_pg = ttk.Scrollbar(pg_tree_fr, orient=tk.VERTICAL, command=self._tree_pages.yview)
        self._tree_pages.configure(yscrollcommand=sy_pg.set)
        self._tree_pages.grid(row=0, column=0, sticky="nsew")
        self._tree_pages.bind("<Double-1>", lambda _e: self._on_edit_page())
        sy_pg.grid(row=0, column=1, sticky="ns")

        tab_jobs = ttk.Frame(nb, padding=4)
        nb.add(tab_jobs, text="  3. Job lịch đăng (schedule_posts.json)  ")
        tab_jobs.columnconfigure(0, weight=1)
        tab_jobs.rowconfigure(3, weight=1)
        ttk.Label(
            tab_jobs,
            text="Mỗi job: lịch (một lần / hàng ngày), post_style, AI (topic, phong cách, ảnh, ai_config…). "
            "Scheduler quét SCHEDULE_POSTS_POLL_SEC (mặc định 60s). Nội dung trống → AI (ưu tiên cấu hình trên job, fallback Page). "
            "Hàng ngày: sau đăng thành công job tự pending với scheduled_at ngày kế.",
            font=("Segoe UI", 9),
            wraplength=920,
        ).grid(row=0, column=0, sticky="ew", pady=(0, 4))
        jb = ttk.Frame(tab_jobs)
        jb.grid(row=1, column=0, sticky="ew", pady=(0, 4))
        ttk.Button(jb, text="Thêm job", command=self._on_add_schedule_job).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(jb, text="Thêm batch job…", command=self._on_add_batch_schedule_job).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(jb, text="Sửa job", command=self._on_edit_schedule_job).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(jb, text="Xóa job", command=self._on_delete_schedule_job).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(jb, text="Đăng luôn job đã chọn", command=self._on_run_selected_jobs_now).pack(side=tk.LEFT, padx=(8, 4))
        ttk.Button(jb, text="Chọn tất cả", command=self._on_jobs_select_all).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(jb, text="Bỏ chọn", command=self._on_jobs_clear_selection).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Checkbutton(
            jb,
            text="Khóa thao tác browser khi chạy job",
            variable=self._var_lock_browser_job,
            command=self._on_toggle_lock_browser_job,
        ).pack(side=tk.LEFT, padx=(8, 4))
        self._lbl_lock_browser_job = ttk.Label(jb, text="", foreground="gray")
        self._lbl_lock_browser_job.pack(side=tk.LEFT, padx=(0, 8))
        self._sync_lock_browser_job_label()
        ttk.Label(jb, text="Song song/account").pack(side=tk.LEFT, padx=(8, 4))
        self._cb_per_account_parallel = ttk.Combobox(
            jb,
            textvariable=self._var_per_account_parallel,
            state="readonly",
            width=4,
            values=("1", "2", "3", "4", "5", "6", "7", "8"),
        )
        self._cb_per_account_parallel.pack(side=tk.LEFT, padx=(0, 4))
        self._cb_per_account_parallel.bind("<<ComboboxSelected>>", lambda _e: self._on_change_per_account_parallel())
        self._lbl_per_account_parallel = ttk.Label(jb, text="", foreground="gray")
        self._lbl_per_account_parallel.pack(side=tk.LEFT, padx=(0, 8))
        self._sync_per_account_parallel_label()
        ttk.Button(jb, text="Màn hình trực quan đăng bài", command=self._open_posting_visual_monitor).pack(
            side=tk.LEFT, padx=(8, 4)
        )
        ttk.Button(jb, text="Làm mới tab này", command=self._on_refresh_schedule_jobs).pack(side=tk.LEFT)

        self._build_schedule_jobs_filter_bar(tab_jobs, row=2)

        j_tree_fr = ttk.Frame(tab_jobs)
        j_tree_fr.grid(row=3, column=0, sticky="nsew")
        j_tree_fr.columnconfigure(0, weight=1)
        j_tree_fr.rowconfigure(0, weight=1)
        cols_j = (
            "id",
            "page_id",
            "account_id",
            "post_type",
            "ai_language",
            "title",
            "image_prompt",
            "scheduled_at",
            "status",
            "retry",
            "missing",
        )
        self._tree_jobs = ttk.Treeview(j_tree_fr, columns=cols_j, show="headings", height=10, selectmode="extended")
        heads_j = {
            "id": "id",
            "page_id": "page",
            "account_id": "account",
            "post_type": "post_type",
            "ai_language": "AI lang",
            "title": "Tiêu đề",
            "image_prompt": "Prompt ảnh (EN)",
            "scheduled_at": "Hẹn đăng (Local)",
            "status": "Trạng thái",
            "retry": "retry",
            "missing": "Thiếu field",
        }
        widths_j = (120, 72, 72, 88, 78, 120, 240, 160, 88, 44, 160)
        for c, w in zip(cols_j, widths_j):
            self._tree_jobs.heading(c, text=heads_j[c])
            self._tree_jobs.column(c, width=w, stretch=c in ("title", "image_prompt", "scheduled_at", "missing"))
        sy_j = ttk.Scrollbar(j_tree_fr, orient=tk.VERTICAL, command=self._tree_jobs.yview)
        self._tree_jobs.configure(yscrollcommand=sy_j.set)
        self._tree_jobs.grid(row=0, column=0, sticky="nsew")
        self._tree_jobs.bind("<Double-1>", lambda _e: self._on_edit_schedule_job())
        self._tree_jobs.bind("<<TreeviewSelect>>", lambda _e: self._update_schedule_jobs_stats_label())
        sy_j.grid(row=0, column=1, sticky="ns")
        self._install_schedule_jobs_column_sort()

        status_fr = ttk.Frame(tab_jobs)
        status_fr.grid(row=4, column=0, sticky="ew", pady=(4, 0))
        status_fr.columnconfigure(99, weight=1)
        self._lbl_jobs_stats = ttk.Label(status_fr, text="Tổng: 0  |  Đang hiển thị: 0  |  Đang chọn: 0", font=("Segoe UI", 9))
        self._lbl_jobs_stats.grid(row=0, column=0, sticky="w")
        ttk.Button(
            status_fr, text="Chọn tất cả đang hiển thị", command=self._on_jobs_select_all_visible
        ).grid(row=0, column=1, padx=(12, 4))
        ttk.Button(
            status_fr, text="Chọn pending", command=lambda: self._on_jobs_select_by_status_visible("pending")
        ).grid(row=0, column=2, padx=(0, 4))
        ttk.Button(
            status_fr, text="Chọn failed", command=lambda: self._on_jobs_select_by_status_visible("failed")
        ).grid(row=0, column=3, padx=(0, 4))
        ttk.Button(
            status_fr, text="Chọn quá hạn", command=self._on_jobs_select_overdue_visible
        ).grid(row=0, column=4, padx=(0, 4))
        ttk.Separator(status_fr, orient=tk.VERTICAL).grid(row=0, column=5, sticky="ns", padx=6)
        ttk.Button(
            status_fr, text="Xem field thiếu", command=self._on_jobs_show_missing_fields
        ).grid(row=0, column=6, padx=(0, 4))
        ttk.Button(
            status_fr, text="Tạo lại field thiếu", command=self._on_jobs_regenerate_missing
        ).grid(row=0, column=7, padx=(0, 4))
        ttk.Button(
            status_fr, text="Tạo lại field đã chọn…", command=self._on_jobs_regenerate_selected_fields
        ).grid(row=0, column=8, padx=(0, 4))
        self._lbl_jobs_regen_status = ttk.Label(status_fr, text="", foreground="gray")
        self._lbl_jobs_regen_status.grid(row=0, column=9, sticky="w", padx=(8, 0))

        tab_ai_host = ttk.Frame(nb, padding=0)
        nb.add(tab_ai_host, text="  4. Cài đặt AI Providers  ")
        tab_ai_host.columnconfigure(0, weight=1)
        tab_ai_host.rowconfigure(0, weight=1)
        ai_canvas = tk.Canvas(tab_ai_host, highlightthickness=0, borderwidth=0)
        ai_vsb = ttk.Scrollbar(tab_ai_host, orient=tk.VERTICAL, command=ai_canvas.yview)
        ai_canvas.configure(yscrollcommand=ai_vsb.set)
        ai_canvas.grid(row=0, column=0, sticky="nsew")
        ai_vsb.grid(row=0, column=1, sticky="ns")
        tab_ai = ttk.Frame(ai_canvas, padding=8)
        self._tab_ai_window_id = ai_canvas.create_window((0, 0), window=tab_ai, anchor="nw")
        tab_ai.columnconfigure(0, weight=1)
        tab_ai.columnconfigure(1, weight=1)
        tab_ai.rowconfigure(3, weight=1)
        tab_ai.rowconfigure(12, weight=1)
        tab_ai.rowconfigure(18, weight=1)
        tab_ai.rowconfigure(22, weight=1)
        self._tab_ai_canvas = ai_canvas
        self._tab_ai_scrollbar = ai_vsb
        self._tab_ai_content = tab_ai
        tab_ai.bind("<Configure>", lambda _e: self._sync_ai_tab_scrollregion())
        ai_canvas.bind("<Configure>", lambda _e: self._sync_ai_tab_scrollregion())
        ai_canvas.bind("<Enter>", lambda _e: self._bind_ai_mousewheel(True))
        ai_canvas.bind("<Leave>", lambda _e: self._bind_ai_mousewheel(False))
        ttk.Label(
            tab_ai,
            text="Lưu key theo từng provider (Gemini/OpenAI/NanoBanana). Cột «Key»: hiển thị rút gọn an toàn. "
            "«Kích hoạt» = dùng ngay cho phiên này; «Mặc định» = key khi mở lại app (env trống). "
            "Nếu biến môi trường đã có sẵn thì sẽ được ưu tiên khi khởi động.",
            wraplength=880,
            font=("Segoe UI", 9),
        ).grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ai_pick_fr = ttk.Frame(tab_ai)
        ai_pick_fr.grid(row=0, column=1, sticky="e", pady=(0, 8))
        ttk.Label(ai_pick_fr, text="Hiển thị theo provider:").pack(side=tk.LEFT, padx=(0, 6))
        self._ai_provider_selector = ttk.Combobox(
            ai_pick_fr,
            state="readonly",
            width=12,
            textvariable=self._ai_provider_view_var,
            values=("Gemini", "OpenAI"),
        )
        self._ai_provider_selector.pack(side=tk.LEFT)
        self._ai_provider_selector.bind("<<ComboboxSelected>>", lambda _e: self._apply_ai_provider_view())
        self._lbl_gemini_sess = ttk.Label(tab_ai, text="", font=("Segoe UI", 9))
        self._lbl_gemini_file = ttk.Label(tab_ai, text="", font=("Segoe UI", 9))
        self._lbl_gemini_sess.grid(row=1, column=0, columnspan=2, sticky="w")
        self._lbl_gemini_file.grid(row=2, column=0, columnspan=2, sticky="w")

        gtree_fr = ttk.Frame(tab_ai)
        gtree_fr.grid(row=3, column=0, columnspan=2, sticky="nsew", pady=(8, 4))
        gtree_fr.columnconfigure(0, weight=1)
        gtree_fr.rowconfigure(0, weight=1)
        gcols = ("mark", "label", "preview")
        self._tree_gemini = ttk.Treeview(
            gtree_fr, columns=gcols, show="headings", height=7, selectmode="browse"
        )
        self._tree_gemini.heading("mark", text="Mặc định")
        self._tree_gemini.heading("label", text="Nhãn")
        self._tree_gemini.heading("preview", text="Key (đã che)")
        self._tree_gemini.column("mark", width=72, stretch=False)
        self._tree_gemini.column("label", width=160, stretch=False)
        self._tree_gemini.column("preview", width=320, stretch=True)
        sgy = ttk.Scrollbar(gtree_fr, orient=tk.VERTICAL, command=self._tree_gemini.yview)
        self._tree_gemini.configure(yscrollcommand=sgy.set)
        self._tree_gemini.grid(row=0, column=0, sticky="nsew")
        sgy.grid(row=0, column=1, sticky="ns")
        self._tree_gemini.bind("<Double-1>", lambda _e: self._on_activate_selected_gemini_key())

        ttk.Label(tab_ai, text="Nhãn (gợi nhớ)").grid(row=4, column=0, sticky="nw", pady=(8, 2))
        self._ent_gemini_label = ttk.Entry(tab_ai, width=48)
        self._ent_gemini_label.grid(row=4, column=1, sticky="ew", pady=(8, 2))
        ttk.Label(tab_ai, text="API key mới").grid(row=5, column=0, sticky="nw", pady=2)
        self._ent_gemini_key = tk.Entry(tab_ai, width=56, show="*")
        self._ent_gemini_key.grid(row=5, column=1, sticky="ew", pady=2)
        add_fr = ttk.Frame(tab_ai)
        add_fr.grid(row=6, column=1, sticky="w", pady=(4, 8))
        ttk.Button(add_fr, text="Thêm key", command=self._on_add_gemini_key).pack(side=tk.LEFT, padx=(0, 8))

        act_fr = ttk.Frame(tab_ai)
        act_fr.grid(row=7, column=0, columnspan=2, sticky="w")
        ttk.Button(act_fr, text="Kích hoạt (phiên này)", command=self._on_activate_selected_gemini_key).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(act_fr, text="Đặt làm mặc định", command=self._on_set_default_gemini_key).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(act_fr, text="Xóa key chọn", command=self._on_delete_selected_gemini_key).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(act_fr, text="Xóa tất cả trong file", command=self._on_clear_all_gemini_keys).pack(side=tk.LEFT)

        ttk.Label(
            tab_ai,
            text="File: config/app_secrets.json — không commit. Double-click dòng = kích hoạt.",
            font=("Segoe UI", 8),
            foreground="gray",
        ).grid(row=8, column=0, columnspan=2, sticky="w", pady=(8, 0))

        ttk.Separator(tab_ai, orient=tk.HORIZONTAL).grid(row=9, column=0, columnspan=2, sticky="ew", pady=(10, 8))
        ttk.Label(
            tab_ai,
            text="OpenAI API key",
            font=("Segoe UI", 9, "bold"),
        ).grid(row=10, column=0, columnspan=2, sticky="w")
        self._lbl_openai_sess = ttk.Label(tab_ai, text="", font=("Segoe UI", 9))
        self._lbl_openai_file = ttk.Label(tab_ai, text="", font=("Segoe UI", 9))
        self._lbl_openai_sess.grid(row=11, column=0, columnspan=2, sticky="w")
        self._lbl_openai_file.grid(row=12, column=0, columnspan=2, sticky="w")

        otree_fr = ttk.Frame(tab_ai)
        otree_fr.grid(row=13, column=0, columnspan=2, sticky="nsew", pady=(8, 4))
        otree_fr.columnconfigure(0, weight=1)
        otree_fr.rowconfigure(0, weight=1)
        ocols = ("mark", "label", "preview")
        self._tree_openai = ttk.Treeview(otree_fr, columns=ocols, show="headings", height=6, selectmode="browse")
        self._tree_openai.heading("mark", text="Mặc định")
        self._tree_openai.heading("label", text="Nhãn")
        self._tree_openai.heading("preview", text="Key (đã che)")
        self._tree_openai.column("mark", width=72, stretch=False)
        self._tree_openai.column("label", width=160, stretch=False)
        self._tree_openai.column("preview", width=320, stretch=True)
        osb = ttk.Scrollbar(otree_fr, orient=tk.VERTICAL, command=self._tree_openai.yview)
        self._tree_openai.configure(yscrollcommand=osb.set)
        self._tree_openai.grid(row=0, column=0, sticky="nsew")
        osb.grid(row=0, column=1, sticky="ns")
        self._tree_openai.bind("<Double-1>", lambda _e: self._on_activate_selected_openai_key())

        ttk.Label(tab_ai, text="Nhãn OpenAI").grid(row=14, column=0, sticky="nw", pady=(8, 2))
        self._ent_openai_label = ttk.Entry(tab_ai, width=48)
        self._ent_openai_label.grid(row=14, column=1, sticky="ew", pady=(8, 2))
        ttk.Label(tab_ai, text="OpenAI API key mới").grid(row=15, column=0, sticky="nw", pady=2)
        self._ent_openai_key = tk.Entry(tab_ai, width=56, show="*")
        self._ent_openai_key.grid(row=15, column=1, sticky="ew", pady=2)
        o_add_fr = ttk.Frame(tab_ai)
        o_add_fr.grid(row=16, column=1, sticky="w", pady=(4, 8))
        ttk.Button(o_add_fr, text="Thêm key OpenAI", command=self._on_add_openai_key).pack(side=tk.LEFT, padx=(0, 8))

        o_act_fr = ttk.Frame(tab_ai)
        o_act_fr.grid(row=17, column=0, columnspan=2, sticky="w")
        ttk.Button(o_act_fr, text="Kích hoạt (phiên này)", command=self._on_activate_selected_openai_key).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(o_act_fr, text="Đặt làm mặc định", command=self._on_set_default_openai_key).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(o_act_fr, text="Xóa key chọn", command=self._on_delete_selected_openai_key).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(o_act_fr, text="Xóa tất cả key OpenAI", command=self._on_clear_all_openai_keys).pack(side=tk.LEFT)

        ttk.Separator(tab_ai, orient=tk.HORIZONTAL).grid(row=18, column=0, columnspan=2, sticky="ew", pady=(10, 8))
        ttk.Label(
            tab_ai,
            text="NanoBanana / VEO3 key pool (dùng cho sinh ảnh nhanh, phân tải nhiều key).",
            font=("Segoe UI", 9, "bold"),
        ).grid(row=19, column=0, columnspan=2, sticky="w")
        self._lbl_nb_sess = ttk.Label(tab_ai, text="", font=("Segoe UI", 9))
        self._lbl_nb_file = ttk.Label(tab_ai, text="", font=("Segoe UI", 9))
        self._lbl_nb_sess.grid(row=20, column=0, columnspan=2, sticky="w")
        self._lbl_nb_file.grid(row=21, column=0, columnspan=2, sticky="w")

        nbtree_fr = ttk.Frame(tab_ai)
        nbtree_fr.grid(row=22, column=0, columnspan=2, sticky="nsew", pady=(8, 4))
        nbtree_fr.columnconfigure(0, weight=1)
        nbtree_fr.rowconfigure(0, weight=1)
        nbcols = ("mark", "label", "preview")
        self._tree_nanobanana = ttk.Treeview(nbtree_fr, columns=nbcols, show="headings", height=6, selectmode="browse")
        self._tree_nanobanana.heading("mark", text="Mặc định")
        self._tree_nanobanana.heading("label", text="Nhãn")
        self._tree_nanobanana.heading("preview", text="Key (đã che)")
        self._tree_nanobanana.column("mark", width=72, stretch=False)
        self._tree_nanobanana.column("label", width=160, stretch=False)
        self._tree_nanobanana.column("preview", width=320, stretch=True)
        nsb = ttk.Scrollbar(nbtree_fr, orient=tk.VERTICAL, command=self._tree_nanobanana.yview)
        self._tree_nanobanana.configure(yscrollcommand=nsb.set)
        self._tree_nanobanana.grid(row=0, column=0, sticky="nsew")
        nsb.grid(row=0, column=1, sticky="ns")
        self._tree_nanobanana.bind("<Double-1>", lambda _e: self._on_activate_selected_nanobanana_key())

        ttk.Label(tab_ai, text="Nhãn NanoBanana/VEO3").grid(row=23, column=0, sticky="nw", pady=(8, 2))
        self._ent_nb_label = ttk.Entry(tab_ai, width=48)
        self._ent_nb_label.grid(row=23, column=1, sticky="ew", pady=(8, 2))
        ttk.Label(tab_ai, text="API key mới").grid(row=24, column=0, sticky="nw", pady=2)
        self._ent_nb_key = tk.Entry(tab_ai, width=56, show="*")
        self._ent_nb_key.grid(row=24, column=1, sticky="ew", pady=2)
        nb_add_fr = ttk.Frame(tab_ai)
        nb_add_fr.grid(row=25, column=1, sticky="w", pady=(4, 8))
        ttk.Button(nb_add_fr, text="Thêm key", command=self._on_add_nanobanana_key).pack(side=tk.LEFT, padx=(0, 8))

        nb_act_fr = ttk.Frame(tab_ai)
        nb_act_fr.grid(row=26, column=0, columnspan=2, sticky="w")
        ttk.Button(nb_act_fr, text="Kích hoạt (phiên này)", command=self._on_activate_selected_nanobanana_key).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(nb_act_fr, text="Đặt làm mặc định", command=self._on_set_default_nanobanana_key).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(nb_act_fr, text="Xóa key chọn", command=self._on_delete_selected_nanobanana_key).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(nb_act_fr, text="Xóa tất cả key NB", command=self._on_clear_all_nanobanana_keys).pack(side=tk.LEFT)

        ttk.Label(tab_ai, text="Gemini / Veo3 URL (browser)").grid(row=27, column=0, sticky="nw", pady=(8, 2))
        self._ent_nb_web_url = ttk.Entry(tab_ai, width=72)
        self._ent_nb_web_url.grid(row=27, column=1, sticky="ew", pady=(8, 2))
        ttk.Label(tab_ai, text="Tài khoản Gemini/Veo3 đã đăng nhập (ghi chú)").grid(row=28, column=0, sticky="nw", pady=2)
        self._ent_nb_account = ttk.Entry(tab_ai, width=72)
        self._ent_nb_account.grid(row=28, column=1, sticky="ew", pady=2)
        ttk.Label(tab_ai, text="Model VEO3 mặc định").grid(row=29, column=0, sticky="nw", pady=2)
        self._ent_nb_video_model = ttk.Entry(tab_ai, width=72)
        self._ent_nb_video_model.grid(row=29, column=1, sticky="ew", pady=2)
        nb_login_fr = ttk.Frame(tab_ai)
        nb_login_fr.grid(row=30, column=1, sticky="w", pady=(4, 8))
        ttk.Button(nb_login_fr, text="Lưu URL/Model", command=self._on_save_nanobanana_runtime_config).pack(
            side=tk.LEFT, padx=(0, 8)
        )
        ttk.Button(nb_login_fr, text="Đăng nhập Gemini/Veo3 (Browser)", command=self._on_login_nanobanana_browser).pack(
            side=tk.LEFT, padx=(0, 8)
        )

        self._jobs_tab_index = nb.index(tab_jobs)

        body.add(nb_host, weight=5)

        log_fr = ttk.Frame(body, padding=4)
        log_bar = ttk.Frame(log_fr)
        log_bar.pack(fill=tk.X, anchor="w")
        ttk.Label(log_bar, text="Nhật ký (INFO)").pack(side=tk.LEFT, anchor="w")
        ttk.Button(log_bar, text="Clear", command=self._on_clear_log_text, width=8).pack(side=tk.RIGHT)
        self._log_text = tk.Text(log_fr, height=10, state="disabled", wrap="word", font=("Consolas", 9))
        ly = ttk.Scrollbar(log_fr, orient=tk.VERTICAL, command=self._log_text.yview)
        self._log_text.configure(yscrollcommand=ly.set)
        self._log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ly.pack(side=tk.RIGHT, fill=tk.Y)
        body.add(log_fr, weight=2)

        self._root.minsize(960, 620)

        self._attach_log_sink()
        self._fill_tree(self._accounts.load_all())
        self._fill_pages_tree()
        self._fill_schedule_jobs_tree()
        self._refresh_openai_tab()
        self._refresh_gemini_tab()
        self._refresh_nanobanana_tab()
        self._capture_ai_provider_widgets()
        self._apply_ai_provider_view()
        self._sync_ai_tab_scrollregion()
        self._start_ui_watchdog()
        logger.info(
            "Đã mở giao diện quản lý — tab Tài khoản / Page / Job lịch / Cài đặt AI; «Bắt đầu lịch» chạy scheduler nền."
        )

    def run(self) -> None:
        """
        Chạy vòng lặp sự kiện Tk cho tới khi đóng cửa sổ.
        """
        self._root.mainloop()

    def _set_ui_busy(self, label: str) -> None:
        """Đánh dấu tác vụ UI hiện tại để watchdog log đúng ngữ cảnh."""
        self._ui_busy_label = str(label or "").strip()

    def _clear_ui_busy(self) -> None:
        """Xóa nhãn tác vụ UI hiện tại."""
        self._ui_busy_label = ""

    def _start_ui_watchdog(self) -> None:
        """Khởi động watchdog phát hiện block UI > ngưỡng."""
        if self._ui_watchdog_after_id is not None:
            return
        self._ui_watchdog_last_tick = time.monotonic()

        def _tick() -> None:
            now = time.monotonic()
            gap = now - self._ui_watchdog_last_tick
            if gap > self._ui_watchdog_threshold_sec:
                label = self._ui_busy_label or "(không rõ tác vụ)"
                logger.warning("UI watchdog: main thread bị block {:.2f}s | tác vụ={}", gap, label)
            self._ui_watchdog_last_tick = now
            self._ui_watchdog_after_id = self._root.after(self._ui_watchdog_interval_ms, _tick)

        self._ui_watchdog_after_id = self._root.after(self._ui_watchdog_interval_ms, _tick)

    def _stop_ui_watchdog(self) -> None:
        """Dừng watchdog UI khi chuẩn bị destroy root."""
        if self._ui_watchdog_after_id is None:
            return
        try:
            self._root.after_cancel(self._ui_watchdog_after_id)
        except Exception:
            pass
        self._ui_watchdog_after_id = None

    def _sync_ai_tab_scrollregion(self) -> None:
        """Đồng bộ vùng cuộn cho tab AI Providers."""
        if self._tab_ai_canvas is None or self._tab_ai_content is None:
            return
        self._tab_ai_content.update_idletasks()
        bbox = self._tab_ai_canvas.bbox("all")
        if bbox:
            self._tab_ai_canvas.configure(scrollregion=bbox)
        width = self._tab_ai_canvas.winfo_width()
        if width > 1 and self._tab_ai_window_id is not None:
            self._tab_ai_canvas.itemconfigure(self._tab_ai_window_id, width=width)

    def _bind_ai_mousewheel(self, enable: bool) -> None:
        """Bật/tắt cuộn chuột cho tab AI khi con trỏ đi vào/ra canvas."""
        if enable:
            self._root.bind("<MouseWheel>", self._on_ai_mousewheel, add="+")
            self._root.bind("<Button-4>", self._on_ai_mousewheel, add="+")
            self._root.bind("<Button-5>", self._on_ai_mousewheel, add="+")
            return
        self._root.unbind("<MouseWheel>")
        self._root.unbind("<Button-4>")
        self._root.unbind("<Button-5>")

    def _on_ai_mousewheel(self, event: tk.Event) -> None:
        """Cuộn dọc tab AI Providers."""
        if self._tab_ai_canvas is None:
            return
        if hasattr(event, "delta") and event.delta:
            step = -1 if event.delta > 0 else 1
            self._tab_ai_canvas.yview_scroll(step, "units")
            return
        num = getattr(event, "num", None)
        if num == 4:
            self._tab_ai_canvas.yview_scroll(-1, "units")
        elif num == 5:
            self._tab_ai_canvas.yview_scroll(1, "units")

    def _ai_provider_pref_path(self) -> Path:
        """File lưu lựa chọn provider hiển thị cho tab AI."""
        return project_root() / "data" / "runtime" / "gui_prefs.json"

    def _normalize_ai_provider_key(self, raw: str | None) -> str:
        """Chuẩn hóa key provider nội bộ: ``gemini`` hoặc ``openai``."""
        s = (raw or "").strip().lower()
        if s in {"openai", "open ai", "open_ai"}:
            return "openai"
        return "gemini"

    def _provider_label_from_key(self, key: str) -> str:
        return "OpenAI" if self._normalize_ai_provider_key(key) == "openai" else "Gemini"

    def _provider_key_from_label(self, label: str | None) -> str:
        return self._normalize_ai_provider_key(label)

    def _load_ai_provider_pref_label(self) -> str:
        """Đọc lựa chọn provider đã lưu ở phiên trước."""
        p = self._ai_provider_pref_path()
        if not p.is_file():
            return "Gemini"
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return "Gemini"
        if not isinstance(raw, dict):
            return "Gemini"
        key = self._normalize_ai_provider_key(str(raw.get("ai_provider_view", "gemini")))
        return self._provider_label_from_key(key)

    def _save_ai_provider_pref(self, key: str) -> None:
        """Lưu lựa chọn provider để lần sau mở app vẫn giữ đúng chế độ xem."""
        p = self._ai_provider_pref_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {}
        if p.is_file():
            try:
                raw = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    payload = raw
            except Exception:
                payload = {}
        payload["ai_provider_view"] = self._normalize_ai_provider_key(key)
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _apply_ai_provider_view(self) -> None:
        """
        Chỉ hiển thị cụm cấu hình theo provider để giảm rối mắt.

        - gemini: hiện Gemini + NanoBanana/VEO3
        - openai: hiện OpenAI
        """
        if self._tab_ai_content is None:
            return
        view_key = self._provider_key_from_label(self._ai_provider_view_var.get())
        show_openai = view_key == "openai"
        # Tránh khoảng trắng lớn khi ẩn nhóm widget: reset weight rồi bật lại cho phần đang hiển thị.
        for r in (3, 12, 22):
            self._tab_ai_content.rowconfigure(r, weight=0)
        for widget in self._ai_widgets_gemini:
            if show_openai:
                widget.grid_remove()
            else:
                widget.grid()
        for widget in self._ai_widgets_openai:
            if show_openai:
                widget.grid()
            else:
                widget.grid_remove()
        if show_openai:
            self._tab_ai_content.rowconfigure(12, weight=1)
        else:
            self._tab_ai_content.rowconfigure(3, weight=1)
            self._tab_ai_content.rowconfigure(22, weight=1)
        if self._ai_provider_selector is not None:
            self._ai_provider_selector.set(self._provider_label_from_key(view_key))
        self._save_ai_provider_pref(view_key)
        self._sync_ai_tab_scrollregion()

    def _capture_ai_provider_widgets(self) -> None:
        """
        Chụp danh sách widget theo nhóm provider một lần sau khi dựng UI.

        Lý do: widget đã ``grid_remove()`` có thể không còn xuất hiện ổn định qua ``grid_slaves()`` ở
        các lần toggle sau, dẫn tới không khôi phục lại được giao diện.
        """
        if self._tab_ai_content is None:
            return
        self._ai_widgets_gemini = []
        self._ai_widgets_openai = []
        for widget in self._tab_ai_content.grid_slaves():
            info = widget.grid_info()
            row_val = info.get("row")
            if row_val is None:
                continue
            row = int(row_val)
            if 10 <= row <= 18:
                self._ai_widgets_openai.append(widget)
            elif row in range(1, 10) or row in range(19, 34):
                self._ai_widgets_gemini.append(widget)

    def _attach_log_sink(self) -> None:
        """
        Thêm sink Loguru ghi thêm vào ô log GUI (giữ sink stderr từ ``main._configure_logging``).
        """
        stream = _GuiLogStream(self._root, self._log_text)
        self._log_sink_id = logger.add(
            stream,
            level="INFO",
            format="{time:HH:mm:ss} | {level:<7} | {message}\n",
            colorize=False,
            enqueue=True,
        )

    def _detach_log_sink(self) -> None:
        """
        Gỡ sink GUI khỏi Loguru (khi đóng cửa sổ).
        """
        if self._log_sink_id is not None:
            try:
                logger.remove(self._log_sink_id)
            except ValueError:
                pass
            self._log_sink_id = None

    def _on_clear_log_text(self) -> None:
        """Xóa toàn bộ nội dung ô nhật ký INFO trong GUI."""
        try:
            self._log_text.configure(state="normal")
            self._log_text.delete("1.0", tk.END)
            self._log_text.configure(state="disabled")
        except tk.TclError:
            return
        logger.info("Đã xóa nội dung Nhật ký (INFO) trên giao diện.")

    def _fill_tree(self, rows: list[AccountRecord]) -> None:
        """
        Xóa bảng và điền lại từ danh sách bản ghi đã có trong bộ nhớ.

        Args:
            rows: Danh sách dict tài khoản (cùng kiểu ``AccountRecord``).
        """
        prev_sel = set(self._selected_account_ids())
        for i in self._tree_accounts.get_children():
            self._tree_accounts.delete(i)
        for idx, acc in enumerate(rows):
            ck = str(acc.get("cookie_path", ""))
            if len(ck) > 40:
                ck = ck[:37] + "..."
            port = str(acc.get("portable_path", ""))
            if len(port) > 42:
                port = port[:39] + "..."
            px = acc.get("proxy") or {}
            ph = str(px.get("host", "")).strip()
            try:
                pp = int(px.get("port", 0))
            except (TypeError, ValueError):
                pp = 0
            proxy_s = f"{ph}:{pp}" if ph else f":{pp}"
            if len(proxy_s) > 28:
                proxy_s = proxy_s[:25] + "..."
            aid = str(acc.get("id", "")).strip()
            if not aid:
                aid = f"__row_{idx}"
            tick_sym = "☑" if self._account_tick.get(aid, False) else "☐"
            self._tree_accounts.insert(
                "",
                tk.END,
                iid=aid,
                values=(
                    tick_sym,
                    acc.get("id", ""),
                    acc.get("name", ""),
                    (
                        "Chrome"
                        if str(acc.get("browser_type", "")).lower() in ("chromium", "chrome")
                        else "Firefox"
                        if str(acc.get("browser_type", "")).lower() == "firefox"
                        else str(acc.get("browser_type", ""))
                    ),
                    port,
                    proxy_s,
                    ck,
                ),
            )
        kids = list(self._tree_accounts.get_children())
        self._account_tick = {str(k): self._account_tick.get(str(k), False) for k in kids}
        restore = [i for i in kids if i in prev_sel]
        if restore:
            self._tree_accounts.selection_set(restore)

    def _refresh_tree(self) -> None:
        """
        Đọc lại ``accounts.json`` từ đĩa (bỏ cache) và cập nhật bảng Treeview.
        """
        try:
            rows = self._accounts.reload_from_disk()
        except Exception as exc:  # noqa: BLE001
            logger.exception("Làm mới danh sách thất bại: {}", exc)
            messagebox.showerror("Lỗi", f"Không đọc được accounts.json:\n{exc}")
            return
        self._fill_tree(rows)
        logger.info("Đã làm mới danh sách từ đĩa: {} tài khoản.", len(rows))

    def _acc_tree_col_at_event_x(self, event_x: int) -> str | None:
        cid = self._tree_accounts.identify_column(event_x)
        if not cid.startswith("#"):
            return None
        try:
            xi = int(cid[1:])
        except ValueError:
            return None
        col_tuple = self._tree_accounts.cget("columns")
        parts = list(col_tuple) if not isinstance(col_tuple, str) else col_tuple.split()
        if xi < 1 or xi > len(parts):
            return None
        return str(parts[xi - 1])

    def _profile_ids_for_bulk(self) -> list[str]:
        """Các id đã tick; nếu không có tick nào thì dùng dòng đang chọn."""
        ticked: list[str] = []
        for iid in self._tree_accounts.get_children():
            s = str(iid)
            if s.startswith("__row_"):
                continue
            if self._account_tick.get(s, False):
                ticked.append(s)
        if ticked:
            return ticked
        return self._selected_account_ids()

    def _selected_account_ids(self) -> list[str]:
        """Danh sách ``id`` các dòng đang highlight (``iid`` = ``id`` tài khoản)."""
        out: list[str] = []
        for iid in self._tree_accounts.selection():
            s = str(iid).strip()
            if not s or s.startswith("__row_"):
                continue
            out.append(s)
        return out

    def _selected_account_id(self) -> str | None:
        """
        Trả về ``id`` của dòng đầu tiên đang chọn (tương thích thao tác đơn).

        Returns:
            Chuỗi ``id`` hoặc ``None`` nếu không có chọn.
        """
        ids = self._selected_account_ids()
        return ids[0] if ids else None

    def _on_accounts_select_all(self) -> None:
        kids = list(self._tree_accounts.get_children())
        for k in kids:
            if str(k).startswith("__row_"):
                continue
            self._account_tick[str(k)] = True
            try:
                self._tree_accounts.set(k, "tick", "☑")
            except tk.TclError:
                pass
        if kids:
            self._tree_accounts.selection_set(kids)

    def _on_accounts_clear_selection(self) -> None:
        for k in self._tree_accounts.get_children():
            if str(k).startswith("__row_"):
                continue
            self._account_tick[str(k)] = False
            try:
                self._tree_accounts.set(k, "tick", "☐")
            except tk.TclError:
                pass
        for iid in list(self._tree_accounts.selection()):
            self._tree_accounts.selection_remove(iid)

    def _on_account_tree_press_drag(self, event: tk.Event) -> None:
        if self._tree_accounts.identify_region(event.x, event.y) == "cell":
            if self._acc_tree_col_at_event_x(event.x) == "tick":
                row = self._tree_accounts.identify_row(event.y)
                if row and not str(row).startswith("__row_"):
                    aid = str(row)
                    self._account_tick[aid] = not self._account_tick.get(aid, False)
                    sym = "☑" if self._account_tick[aid] else "☐"
                    try:
                        self._tree_accounts.set(row, "tick", sym)
                    except tk.TclError:
                        pass
                self._acc_drag_anchor = None
                return
        row = self._tree_accounts.identify_row(event.y)
        self._acc_drag_anchor = row if row else None

    def _on_account_tree_motion_drag(self, event: tk.Event) -> None:
        if not (event.state & 0x0100):
            return
        anchor = self._acc_drag_anchor
        if not anchor:
            return
        row = self._tree_accounts.identify_row(event.y)
        if not row:
            return
        kids = list(self._tree_accounts.get_children())
        try:
            ia, ib = kids.index(anchor), kids.index(row)
        except ValueError:
            return
        lo, hi = min(ia, ib), max(ia, ib)
        self._tree_accounts.selection_set(kids[lo : hi + 1])

    def _on_account_tree_release_drag(self, _event: tk.Event) -> None:
        self._acc_drag_anchor = None

    def _record_by_id(self, account_id: str) -> AccountRecord | None:
        """
        Tìm bản ghi đầy đủ theo ``id`` trong bộ nhớ (cache ``load_all``).

        Args:
            account_id: id tài khoản.

        Returns:
            Bản ghi hoặc ``None``.
        """
        for acc in self._accounts.load_all():
            if str(acc.get("id", "")) == account_id:
                return acc
        return None

    def _show_failed_accounts_log_tail(self) -> None:
        """Mở hộp thoại với vài dòng cuối ``logs/failed_accounts.log``."""
        p = project_root() / "logs" / "failed_accounts.log"
        try:
            if p.is_file():
                lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
                snippet = "\n".join(lines[-20:]) if lines else "(file rỗng)"
            else:
                snippet = "(chưa có file — chưa ghi lỗi lần nào)"
        except OSError as exc:
            snippet = f"(không đọc được: {exc})"
        messagebox.showinfo("failed_accounts.log (tail)", f"{p}\n\n{snippet}", parent=self._root)

    def _tick_account_rows(self, iids: tuple[str, ...], ticked: bool) -> None:
        """Gán tick ☑/☐ cho các ``iid`` dòng (bỏ qua ``__row_*``)."""
        for iid in iids:
            s = str(iid)
            if s.startswith("__row_"):
                continue
            self._account_tick[s] = ticked
            try:
                self._tree_accounts.set(iid, "tick", "☑" if ticked else "☐")
            except tk.TclError:
                pass

    def _on_menu_tick_selection(self) -> None:
        self._tick_account_rows(self._tree_accounts.selection(), True)

    def _on_menu_untick_selection(self) -> None:
        self._tick_account_rows(self._tree_accounts.selection(), False)

    def _on_tree_accounts_rclick(self, event: tk.Event) -> None:
        """Menu ngữ cảnh: tick theo vùng chọn, sửa / cookie; nếu ``failed`` thì xem tail log."""
        row = self._tree_accounts.identify_row(event.y)
        if not row:
            return
        cur_sel = self._tree_accounts.selection()
        if row not in cur_sel:
            self._tree_accounts.selection_set(row)
        aid = self._selected_account_id()
        if not aid:
            return
        acc = self._record_by_id(aid)
        menu = tk.Menu(self._root, tearoff=0)
        n_sel = len(self._tree_accounts.selection())
        tick_lbl = "Tick ☑ các dòng đang chọn" if n_sel > 1 else "Tick ☑ dòng này"
        untick_lbl = "Bỏ tick ☐ các dòng đang chọn" if n_sel > 1 else "Bỏ tick ☐ dòng này"
        menu.add_command(label=tick_lbl, command=self._on_menu_tick_selection)
        menu.add_command(label=untick_lbl, command=self._on_menu_untick_selection)
        menu.add_separator()
        menu.add_command(label="Sửa tài khoản…", command=self._on_edit_account)
        menu.add_command(label="Lấy cookie (Playwright)…", command=self._on_capture_cookie_account)
        st = _normalize_post_status((acc or {}).get("status", "pending")) if acc else "pending"
        if st == "failed":
            menu.add_command(label="Xem tail failed_accounts.log…", command=self._show_failed_accounts_log_tail)
        try:
            menu.tk_popup(int(event.x_root), int(event.y_root))
        finally:
            try:
                menu.grab_release()
            except tk.TclError:
                pass

    def _fill_pages_tree(self) -> None:
        """Đọc dữ liệu gốc vào ``self._all_pages`` rồi filter/sort và render."""
        try:
            rows = self._pages.load_all()
        except Exception as exc:  # noqa: BLE001
            logger.exception("Đọc pages.json: {}", exc)
            return
        self._all_pages = [dict(r) for r in rows]
        self._refresh_pages_filter_choices()
        self._render_pages_tree()

    def _render_pages_tree(self) -> None:
        for i in self._tree_pages.get_children():
            self._tree_pages.delete(i)
        rows = list(getattr(self, "_all_pages", []) or [])
        q = (self._var_pages_search.get() if hasattr(self, "_var_pages_search") else "").strip().lower()
        owner_val = (
            self._var_pages_filter_account.get()
            if hasattr(self, "_var_pages_filter_account")
            else "Tất cả owner"
        )
        kind_val = (
            self._var_pages_filter_kind.get()
            if hasattr(self, "_var_pages_filter_kind")
            else "Tất cả loại"
        )
        status_val = (
            self._var_pages_filter_status.get()
            if hasattr(self, "_var_pages_filter_status")
            else "Tất cả trạng thái"
        )
        if owner_val and owner_val != "Tất cả owner":
            rows = [r for r in rows if str(r.get("account_id", "")).strip() == owner_val]
        if kind_val and kind_val != "Tất cả loại":
            rows = [r for r in rows if str(r.get("page_kind", "")).strip().lower() == kind_val]
        if status_val and status_val != "Tất cả trạng thái":
            rows = [
                r
                for r in rows
                if _normalize_post_status(str(r.get("status", "")).strip() or "pending") == status_val
            ]
        if q:
            def _row_hit(r: dict[str, Any]) -> bool:
                blob = " ".join(
                    str(r.get(k, "") or "")
                    for k in (
                        "id",
                        "account_id",
                        "page_name",
                        "page_url",
                        "fb_page_id",
                        "business_name",
                        "business_id",
                        "topic",
                        "source",
                    )
                ).lower()
                return q in blob
            rows = [r for r in rows if _row_hit(r)]

        sk = getattr(self, "_pages_sort_key", "page_name")
        asc = bool(getattr(self, "_pages_sort_asc", True))
        rev = not asc
        if sk == "page_name":
            rows.sort(key=lambda r: str(r.get("page_name", "")).strip().lower(), reverse=rev)
        elif sk == "account_id":
            rows.sort(key=lambda r: str(r.get("account_id", "")).strip().lower(), reverse=rev)
        elif sk == "fb_page_id":
            rows.sort(key=lambda r: str(r.get("fb_page_id", "")).strip(), reverse=rev)
        elif sk == "last_post_at":
            rows.sort(key=lambda r: str(r.get("last_post_at", "")).strip(), reverse=rev)
        elif sk == "status":
            rows.sort(
                key=lambda r: _normalize_post_status(str(r.get("status", "")).strip() or "pending"),
                reverse=rev,
            )
        elif sk == "page_kind":
            rows.sort(key=lambda r: str(r.get("page_kind", "")).strip().lower(), reverse=rev)
        elif sk == "post_style":
            rows.sort(key=lambda r: str(r.get("post_style", "")).strip().lower(), reverse=rev)

        for p in rows:
            url = str(p.get("page_url", ""))
            if len(url) > 36:
                url = url[:33] + "..."
            top = str(p.get("topic", "") or "")
            if len(top) > 28:
                top = top[:25] + "..."
            raw_st = str(p.get("status", "")).strip()
            st_disp = _normalize_post_status(raw_st if raw_st else "pending")
            last_post = str(p.get("last_post_at", "") or "")
            if len(last_post) > 14:
                last_post = last_post[:11] + "..."
            fb_pid = str(p.get("fb_page_id", "") or "")
            if len(fb_pid) > 16:
                fb_pid = fb_pid[:13] + "..."
            row_tag = (
                "pg_failed"
                if st_disp == "failed"
                else "pg_success"
                if st_disp == "success"
                else "pg_pending"
            )
            self._tree_pages.insert(
                "",
                tk.END,
                values=(
                    p.get("id", ""),
                    p.get("account_id", ""),
                    p.get("page_kind", "") or "—",
                    p.get("page_name", ""),
                    top or "—",
                    p.get("post_style", ""),
                    p.get("schedule_time", "") or "—",
                    st_disp,
                    last_post or "—",
                    fb_pid or "—",
                    url,
                ),
                tags=(row_tag,),
            )
        self._tree_pages.tag_configure("pg_pending", foreground="#6b6b6b")
        self._tree_pages.tag_configure("pg_success", foreground="#0a7a2e")
        self._tree_pages.tag_configure("pg_failed", foreground="#b00020")
        self._update_pages_heading_sort_indicator()
        if hasattr(self, "_lbl_pages_stats"):
            try:
                self._lbl_pages_stats.configure(text=f"Hiển thị {len(rows)} / {len(getattr(self, '_all_pages', []))} Page")
            except Exception:
                pass

    def _pages_sort_label_to_key(self, label: str) -> str:
        mp = {
            "Tên Page (A-Z)": "page_name",
            "Owner": "account_id",
            "Meta Page ID": "fb_page_id",
            "Trạng thái": "status",
            "Lần đăng gần nhất": "last_post_at",
        }
        return mp.get(label, "page_name")

    def _pages_sort_key_to_label(self, key: str) -> str:
        mp = {
            "page_name": "Tên Page (A-Z)",
            "account_id": "Owner",
            "fb_page_id": "Meta Page ID",
            "status": "Trạng thái",
            "last_post_at": "Lần đăng gần nhất",
        }
        return mp.get(key, "Tên Page (A-Z)")

    def _on_pages_sort_click(self, col_key: str) -> None:
        supported = {
            "id": "id",
            "account_id": "account_id",
            "page_kind": "page_kind",
            "page_name": "page_name",
            "post_style": "post_style",
            "status": "status",
            "last_post": "last_post_at",
            "fb_page_id": "fb_page_id",
            "url": "page_url",
        }
        sk = supported.get(col_key)
        if not sk:
            return
        if self._pages_sort_key == sk:
            self._pages_sort_asc = not self._pages_sort_asc
        else:
            self._pages_sort_key = sk
            self._pages_sort_asc = True
        if hasattr(self, "_var_pages_sort"):
            self._var_pages_sort.set(self._pages_sort_key_to_label(self._pages_sort_key))
        if hasattr(self, "_var_pages_sort_desc"):
            self._var_pages_sort_desc.set(not self._pages_sort_asc)
        self._render_pages_tree()

    def _update_pages_heading_sort_indicator(self) -> None:
        base = {
            "id": "id",
            "account_id": "owner",
            "page_kind": "Loại",
            "page_name": "Tên Page",
            "ai_topic": "Chủ đề AI",
            "post_style": "post_style",
            "schedule": "Lịch",
            "status": "Trạng thái",
            "last_post": "Đăng gần nhất",
            "fb_page_id": "Meta Page ID",
            "url": "Page_URL",
        }
        key_to_col = {
            "id": "id",
            "account_id": "account_id",
            "page_kind": "page_kind",
            "page_name": "page_name",
            "post_style": "post_style",
            "status": "status",
            "last_post_at": "last_post",
            "fb_page_id": "fb_page_id",
            "page_url": "url",
        }
        arrow = " ↑" if self._pages_sort_asc else " ↓"
        for col, text in base.items():
            self._tree_pages.heading(col, text=text, command=lambda k=col: self._on_pages_sort_click(k))
        active_col = key_to_col.get(self._pages_sort_key)
        if active_col:
            self._tree_pages.heading(
                active_col,
                text=base[active_col] + arrow,
                command=lambda k=active_col: self._on_pages_sort_click(k),
            )

    def _build_pages_filter_bar(self, parent: ttk.Frame, *, row: int) -> None:
        fr = ttk.Frame(parent)
        fr.grid(row=row, column=0, sticky="ew", pady=(0, 4))
        fr.columnconfigure(1, weight=1)
        self._var_pages_search = tk.StringVar()
        ttk.Label(fr, text="Tìm:").grid(row=0, column=0, sticky="w")
        ent = ttk.Entry(fr, textvariable=self._var_pages_search, width=46)
        ent.grid(row=0, column=1, sticky="ew", padx=(4, 6))
        ent.bind("<KeyRelease>", self._on_pages_search_changed)

        self._var_pages_filter_account = tk.StringVar(value="Tất cả owner")
        self._cb_pages_filter_account = ttk.Combobox(
            fr,
            textvariable=self._var_pages_filter_account,
            state="readonly",
            width=18,
        )
        self._cb_pages_filter_account.grid(row=0, column=2, padx=(0, 4))
        self._cb_pages_filter_account.bind("<<ComboboxSelected>>", lambda _e: self._render_pages_tree())

        self._var_pages_filter_kind = tk.StringVar(value="Tất cả loại")
        self._cb_pages_filter_kind = ttk.Combobox(
            fr,
            textvariable=self._var_pages_filter_kind,
            state="readonly",
            width=12,
            values=("Tất cả loại", "fanpage", "profile", "group"),
        )
        self._cb_pages_filter_kind.grid(row=0, column=3, padx=(0, 4))
        self._cb_pages_filter_kind.bind("<<ComboboxSelected>>", lambda _e: self._render_pages_tree())

        self._var_pages_filter_status = tk.StringVar(value="Tất cả trạng thái")
        self._cb_pages_filter_status = ttk.Combobox(
            fr,
            textvariable=self._var_pages_filter_status,
            state="readonly",
            width=16,
            values=("Tất cả trạng thái", "pending", "success", "failed"),
        )
        self._cb_pages_filter_status.grid(row=0, column=4, padx=(0, 4))
        self._cb_pages_filter_status.bind("<<ComboboxSelected>>", lambda _e: self._render_pages_tree())

        self._var_pages_sort = tk.StringVar(value="Tên Page (A-Z)")
        self._cb_pages_sort = ttk.Combobox(
            fr,
            textvariable=self._var_pages_sort,
            state="readonly",
            width=16,
            values=("Tên Page (A-Z)", "Owner", "Meta Page ID", "Trạng thái", "Lần đăng gần nhất"),
        )
        self._cb_pages_sort.grid(row=0, column=5, padx=(0, 4))
        self._cb_pages_sort.bind("<<ComboboxSelected>>", self._on_pages_sort_combo_changed)

        self._var_pages_sort_desc = tk.BooleanVar(value=False)
        ttk.Checkbutton(fr, text="Giảm dần", variable=self._var_pages_sort_desc, command=self._on_pages_sort_desc_changed).grid(
            row=0, column=6, padx=(0, 4)
        )
        ttk.Button(fr, text="Xóa lọc", command=self._on_pages_clear_filters).grid(row=0, column=7, padx=(4, 0))
        self._lbl_pages_stats = ttk.Label(fr, text="", foreground="gray")
        self._lbl_pages_stats.grid(row=0, column=8, sticky="e", padx=(10, 0))
        self._refresh_pages_filter_choices()

    def _refresh_pages_filter_choices(self) -> None:
        rows = list(getattr(self, "_all_pages", []) or [])
        owners = sorted({str(r.get("account_id", "")).strip() for r in rows if str(r.get("account_id", "")).strip()})
        values = ("Tất cả owner", *owners)
        if hasattr(self, "_cb_pages_filter_account"):
            cur = self._var_pages_filter_account.get()
            self._cb_pages_filter_account.configure(values=values)
            if cur in values:
                self._var_pages_filter_account.set(cur)
            else:
                self._var_pages_filter_account.set("Tất cả owner")

    def _on_pages_search_changed(self, _event: tk.Event | None = None) -> None:
        if hasattr(self, "_pages_search_after_id") and self._pages_search_after_id:
            try:
                self._root.after_cancel(self._pages_search_after_id)
            except Exception:
                pass
        self._pages_search_after_id = self._root.after(150, self._render_pages_tree)

    def _on_pages_clear_filters(self) -> None:
        self._var_pages_search.set("")
        self._var_pages_filter_account.set("Tất cả owner")
        self._var_pages_filter_kind.set("Tất cả loại")
        self._var_pages_filter_status.set("Tất cả trạng thái")
        self._var_pages_sort.set("Tên Page (A-Z)")
        self._var_pages_sort_desc.set(False)
        self._pages_sort_key = "page_name"
        self._pages_sort_asc = True
        self._render_pages_tree()

    def _on_pages_sort_combo_changed(self, _event: Any = None) -> None:
        self._pages_sort_key = self._pages_sort_label_to_key(self._var_pages_sort.get())
        self._pages_sort_asc = not bool(self._var_pages_sort_desc.get())
        self._render_pages_tree()

    def _on_pages_sort_desc_changed(self) -> None:
        self._pages_sort_asc = not bool(self._var_pages_sort_desc.get())
        self._render_pages_tree()

    def _on_refresh_pages(self) -> None:
        try:
            self._pages.reload_from_disk()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi", str(exc), parent=self._root)
            return
        self._fill_pages_tree()
        logger.info("Đã làm mới pages.json.")

    def _on_open_ai_video_dialog(self) -> None:
        try:
            from src.gui.ai_video_dialog import AIVideoDialog, ai_video_project_gate_dialog

            spec = ai_video_project_gate_dialog(self._root)
            if spec is None:
                return
            AIVideoDialog(self._root, project_spec=spec)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", str(exc), parent=self._root)

    def _refresh_all(self) -> None:
        self._refresh_tree()
        self._on_refresh_pages()
        self._on_refresh_schedule_jobs()

    def _on_migrate_user_data(self) -> None:
        """Migrate nhanh dữ liệu từ thư mục ToolFB cũ sang thư mục hiện tại."""
        old_dir = filedialog.askdirectory(parent=self._root, title="Chọn thư mục ToolFB CŨ")
        if not old_dir:
            return
        new_dir = filedialog.askdirectory(
            parent=self._root,
            title="Chọn thư mục ToolFB MỚI (đích migrate)",
            initialdir=str(project_root().resolve()),
        )
        if not new_dir:
            return
        script = project_root() / "tools" / "migrate_user_data.py"
        if not script.is_file():
            messagebox.showerror("Migrate", f"Không tìm thấy script:\n{script}", parent=self._root)
            return
        try:
            cp = subprocess.run(
                [sys.executable, str(script), "--from", old_dir, "--to", new_dir],
                capture_output=True,
                text=True,
                timeout=180,
                check=False,
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Migrate", f"Chạy migrate thất bại:\n{exc}", parent=self._root)
            return
        if cp.returncode != 0:
            msg = (cp.stderr or cp.stdout or "Unknown error").strip()
            messagebox.showerror("Migrate lỗi", msg[:2000], parent=self._root)
            return
        self._refresh_all()
        out = (cp.stdout or "").strip()
        preview = "\n".join(out.splitlines()[:12])
        messagebox.showinfo(
            "Migrate thành công",
            f"Đã migrate dữ liệu từ:\n{old_dir}\n\nSang:\n{new_dir}\n\n{preview}",
            parent=self._root,
        )

    def _fill_schedule_jobs_tree(self) -> None:
        """Đọc dữ liệu gốc vào ``self._all_jobs`` rồi áp filter/sort và render."""
        try:
            rows = self._schedule_posts.load_all()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không đọc schedule_posts: {}", exc)
            return
        self._all_jobs = [dict(r) for r in rows]
        self._refresh_job_page_name_map()
        # Nạp lại danh sách Account/Page cho các combobox filter khi có thể.
        self._refresh_job_filter_choices()
        self._render_schedule_jobs_tree()

    def _refresh_job_page_name_map(self) -> None:
        """Nạp map ``page_id -> page_name`` để hiển thị cột page dễ đọc."""
        mp: dict[str, str] = {}
        try:
            for p in self._pages.load_all():
                pid = str(p.get("id", "")).strip()
                if not pid:
                    continue
                name = str(p.get("page_name", "") or "").strip()
                if name:
                    mp[pid] = name
        except Exception as exc:  # noqa: BLE001
            logger.debug("Không nạp được map tên page cho tab jobs: {}", exc)
        self._job_page_name_by_id = mp

    def _job_page_display(self, page_id: str) -> str:
        pid = str(page_id or "").strip()
        if not pid:
            return ""
        return str(self._job_page_name_by_id.get(pid) or pid)

    # ---------- Filter / Search / Sort cho danh sách job ----------

    def _build_schedule_jobs_filter_bar(self, parent: ttk.Frame, *, row: int) -> None:
        fr = ttk.Frame(parent)
        fr.grid(row=row, column=0, sticky="ew", pady=(0, 4))
        fr.columnconfigure(1, weight=1)

        self._var_jobs_search = tk.StringVar()
        ttk.Label(fr, text="Tìm:").grid(row=0, column=0, sticky="w")
        ent = ttk.Entry(fr, textvariable=self._var_jobs_search, width=48)
        ent.grid(row=0, column=1, sticky="ew", padx=(4, 6))
        try:
            self._ent_jobs_search = ent
        except Exception:  # pragma: no cover
            pass
        ent.bind("<KeyRelease>", self._on_jobs_search_changed)

        self._var_jobs_filter_account = tk.StringVar(value="Tất cả account")
        self._cb_jobs_filter_account = ttk.Combobox(fr, textvariable=self._var_jobs_filter_account, state="readonly", width=18)
        self._cb_jobs_filter_account.grid(row=0, column=2, padx=(0, 4))
        self._cb_jobs_filter_account.bind("<<ComboboxSelected>>", lambda _e: self._render_schedule_jobs_tree())

        self._var_jobs_filter_page = tk.StringVar(value="Tất cả page")
        self._cb_jobs_filter_page = ttk.Combobox(fr, textvariable=self._var_jobs_filter_page, state="readonly", width=18)
        self._cb_jobs_filter_page.grid(row=0, column=3, padx=(0, 4))
        self._cb_jobs_filter_page.bind("<<ComboboxSelected>>", lambda _e: self._render_schedule_jobs_tree())

        self._var_jobs_filter_post_type = tk.StringVar(value="Tất cả loại")
        self._cb_jobs_filter_post_type = ttk.Combobox(
            fr,
            textvariable=self._var_jobs_filter_post_type,
            state="readonly",
            width=14,
            values=("Tất cả loại", "text", "image", "video", "text_image", "text_video"),
        )
        self._cb_jobs_filter_post_type.grid(row=0, column=4, padx=(0, 4))
        self._cb_jobs_filter_post_type.bind("<<ComboboxSelected>>", lambda _e: self._render_schedule_jobs_tree())

        self._var_jobs_filter_status = tk.StringVar(value="Tất cả trạng thái")
        self._cb_jobs_filter_status = ttk.Combobox(
            fr,
            textvariable=self._var_jobs_filter_status,
            state="readonly",
            width=18,
            values=(
                "Tất cả trạng thái",
                "pending",
                "running",
                "success",
                "failed",
                "paused",
                "need_manual_check",
            ),
        )
        self._cb_jobs_filter_status.grid(row=0, column=5, padx=(0, 4))
        self._cb_jobs_filter_status.bind("<<ComboboxSelected>>", lambda _e: self._render_schedule_jobs_tree())

        self._var_jobs_filter_retry = tk.StringVar(value="Retry: tất cả")
        self._cb_jobs_filter_retry = ttk.Combobox(
            fr,
            textvariable=self._var_jobs_filter_retry,
            state="readonly",
            width=14,
            values=("Retry: tất cả", "Retry = 0", "Retry > 0", "Retry ≥ 2"),
        )
        self._cb_jobs_filter_retry.grid(row=0, column=6, padx=(0, 4))
        self._cb_jobs_filter_retry.bind("<<ComboboxSelected>>", lambda _e: self._render_schedule_jobs_tree())

        self._var_jobs_filter_missing = tk.StringVar(value=MISSING_FIELD_LABELS[0])
        self._cb_jobs_filter_missing = ttk.Combobox(
            fr,
            textvariable=self._var_jobs_filter_missing,
            state="readonly",
            width=30,
            values=MISSING_FIELD_LABELS,
        )
        self._cb_jobs_filter_missing.grid(row=0, column=7, padx=(0, 4))
        self._cb_jobs_filter_missing.bind("<<ComboboxSelected>>", lambda _e: self._render_schedule_jobs_tree())

        ttk.Button(fr, text="Xóa lọc", command=self._on_jobs_clear_filters).grid(row=0, column=8, padx=(4, 0))

    def _refresh_job_filter_choices(self) -> None:
        """Cập nhật options Account/Page combobox theo dữ liệu đang có."""
        if not hasattr(self, "_cb_jobs_filter_account"):
            return
        accs = sorted({str(j.get("account_id", "")).strip() for j in self._all_jobs if j.get("account_id")})
        pages = sorted({str(j.get("page_id", "")).strip() for j in self._all_jobs if j.get("page_id")})
        cur_acc = self._var_jobs_filter_account.get()
        self._cb_jobs_filter_account.configure(values=("Tất cả account", *accs))
        if cur_acc not in ("Tất cả account", *accs):
            self._var_jobs_filter_account.set("Tất cả account")
        cur_pg = self._var_jobs_filter_page.get()
        self._cb_jobs_filter_page.configure(values=("Tất cả page", *pages))
        if cur_pg not in ("Tất cả page", *pages):
            self._var_jobs_filter_page.set("Tất cả page")

    def _current_jobs_filters(self) -> dict[str, str]:
        def _clean(var_get: str, placeholder_prefix: str) -> str:
            if var_get.startswith(placeholder_prefix) or var_get in {"Tất cả", "Retry: tất cả"}:
                return ""
            return var_get

        retry_label = self._var_jobs_filter_retry.get()
        retry_map = {
            "Retry: tất cả": "all",
            "Retry = 0": "retry_0",
            "Retry > 0": "retry_gt_0",
            "Retry ≥ 2": "retry_ge_2",
        }
        return {
            "search_text": self._var_jobs_search.get().strip(),
            "account": _clean(self._var_jobs_filter_account.get(), "Tất cả account"),
            "page_id": _clean(self._var_jobs_filter_page.get(), "Tất cả page"),
            "post_type": _clean(self._var_jobs_filter_post_type.get(), "Tất cả loại"),
            "status": _clean(self._var_jobs_filter_status.get(), "Tất cả trạng thái"),
            "retry_mode": retry_map.get(retry_label, "all"),
        }

    def _on_jobs_search_changed(self, _event: Any = None) -> None:
        if self._jobs_search_after_id is not None:
            try:
                self._root.after_cancel(self._jobs_search_after_id)
            except Exception:
                pass
        self._jobs_search_after_id = self._root.after(180, self._render_schedule_jobs_tree)

    def _on_jobs_clear_filters(self) -> None:
        self._var_jobs_search.set("")
        self._var_jobs_filter_account.set("Tất cả account")
        self._var_jobs_filter_page.set("Tất cả page")
        self._var_jobs_filter_post_type.set("Tất cả loại")
        self._var_jobs_filter_status.set("Tất cả trạng thái")
        self._var_jobs_filter_retry.set("Retry: tất cả")
        if hasattr(self, "_var_jobs_filter_missing"):
            self._var_jobs_filter_missing.set(MISSING_FIELD_LABELS[0])
        self._render_schedule_jobs_tree()

    def _install_schedule_jobs_column_sort(self) -> None:
        col_to_key = {
            "id": "id",
            "page_id": "page_id",
            "account_id": "account_id",
            "post_type": "post_type",
            "ai_language": "ai_language",
            "title": "title",
            "image_prompt": "image_prompt",
            "scheduled_at": "scheduled_at",
            "status": "status",
            "retry": "retry_count",
        }
        for col, key in col_to_key.items():
            self._tree_jobs.heading(
                col,
                command=lambda k=key: self._on_jobs_sort_click(k),
            )

    def _on_jobs_sort_click(self, sort_key: str) -> None:
        if self._jobs_sort_key == sort_key:
            self._jobs_sort_asc = not self._jobs_sort_asc
        else:
            self._jobs_sort_key = sort_key
            self._jobs_sort_asc = True
        self._render_schedule_jobs_tree()

    def _render_schedule_jobs_tree(self) -> None:
        for i in self._tree_jobs.get_children():
            self._tree_jobs.delete(i)
        # Gán chuỗi local + snapshot field thiếu cho search + hiển thị.
        for j in self._all_jobs:
            j["_display_scheduled_local"] = self._format_scheduled_for_ui(j)
            j["_missing_fields"] = get_missing_fields(j)
        filters = self._current_jobs_filters()
        filtered = apply_job_filters(self._all_jobs, **filters)
        # Bộ lọc «Thiếu field» — áp sau filters cơ bản, AND-logic.
        if hasattr(self, "_var_jobs_filter_missing"):
            preset = preset_by_label(self._var_jobs_filter_missing.get())
            if preset.get("match_mode") != "none" and preset.get("fields"):
                filtered = filter_jobs_by_missing_fields(
                    filtered,
                    preset["fields"],
                    match_mode=preset.get("match_mode", "any"),
                )
        filtered = sort_jobs(filtered, sort_key=self._jobs_sort_key, ascending=self._jobs_sort_asc)
        self._filtered_jobs = filtered
        for j in filtered:
            tit = str(j.get("title", "") or "")
            if len(tit) > 40:
                tit = tit[:37] + "…"
            img_prompt = str(j.get("image_prompt", "") or "")
            if len(img_prompt) > 90:
                img_prompt = img_prompt[:87] + "…"
            miss_txt = format_missing_fields_for_display(j.get("_missing_fields") or [])
            status_txt = self._job_status_with_retry(j)
            self._tree_jobs.insert(
                "",
                tk.END,
                values=(
                    j.get("id", ""),
                    self._job_page_display(str(j.get("page_id", "") or "")),
                    j.get("account_id", ""),
                    j.get("post_type", ""),
                    j.get("ai_language", ""),
                    tit,
                    img_prompt,
                    j.get("_display_scheduled_local", "—"),
                    status_txt,
                    j.get("retry_count", 0),
                    miss_txt,
                ),
            )
        self._update_schedule_jobs_sort_indicator()
        self._update_schedule_jobs_stats_label()

    def _job_status_with_retry(self, job: dict[str, Any]) -> str:
        """Hiển thị status kèm retry trực quan: pending (retry 1/3)."""
        st = str(job.get("status", "") or "").strip().lower()
        try:
            rc = max(0, int(job.get("retry_count", 0)))
        except (TypeError, ValueError):
            rc = 0
        max_retry = 3
        if rc <= 0:
            return st
        if st in {"pending", "failed", "need_manual_check"}:
            return f"{st} (retry {rc}/{max_retry})"
        return st

    def _update_schedule_jobs_sort_indicator(self) -> None:
        key_to_col = {
            "id": "id",
            "page_id": "page_id",
            "account_id": "account_id",
            "post_type": "post_type",
            "ai_language": "ai_language",
            "title": "title",
            "image_prompt": "image_prompt",
            "scheduled_at": "scheduled_at",
            "status": "status",
            "retry_count": "retry",
        }
        base_headings = {
            "id": "id",
            "page_id": "page",
            "account_id": "account",
            "post_type": "post_type",
            "ai_language": "AI lang",
            "title": "Tiêu đề",
            "image_prompt": "Prompt ảnh (EN)",
            "scheduled_at": "Hẹn đăng (Local)",
            "status": "Trạng thái",
            "retry": "retry",
        }
        arrow = " ↑" if self._jobs_sort_asc else " ↓"
        for col, text in base_headings.items():
            self._tree_jobs.heading(col, text=text)
        active_col = key_to_col.get(self._jobs_sort_key)
        if active_col:
            self._tree_jobs.heading(active_col, text=base_headings[active_col] + arrow)

    def _update_schedule_jobs_stats_label(self) -> None:
        if not hasattr(self, "_lbl_jobs_stats"):
            return
        total = len(self._all_jobs)
        shown = len(self._filtered_jobs)
        sel = len(self._tree_jobs.selection())
        pending = sum(1 for j in self._all_jobs if str(j.get("status", "")).lower() == "pending")
        failed = sum(1 for j in self._all_jobs if str(j.get("status", "")).lower() == "failed")
        success = sum(1 for j in self._all_jobs if str(j.get("status", "")).lower() == "success")
        self._lbl_jobs_stats.configure(
            text=(
                f"Tổng: {total}  |  Đang hiển thị: {shown}  |  Đang chọn: {sel}  "
                f"|  pending: {pending}  failed: {failed}  success: {success}"
            )
        )

    def _on_jobs_select_all_visible(self) -> None:
        kids = self._tree_jobs.get_children()
        if kids:
            self._tree_jobs.selection_set(kids)
        self._update_schedule_jobs_stats_label()

    def _on_jobs_select_by_status_visible(self, status: str) -> None:
        target = str(status or "").strip().lower()
        if not target:
            return
        sel: list[str] = []
        for iid in self._tree_jobs.get_children():
            vals = self._tree_jobs.item(iid, "values")
            if not vals or len(vals) < 9:
                continue
            st = str(vals[8]).strip().lower()
            if st == target:
                sel.append(iid)
        if sel:
            self._tree_jobs.selection_set(sel)
        else:
            cur = self._tree_jobs.selection()
            if cur:
                self._tree_jobs.selection_remove(*cur)
        self._update_schedule_jobs_stats_label()

    def _on_jobs_select_overdue_visible(self) -> None:
        now_utc = datetime.now(timezone.utc)
        overdue_ids: set[str] = set()
        for j in self._filtered_jobs:
            try:
                if is_overdue(j, now_utc=now_utc):
                    jid = str(j.get("id", "")).strip()
                    if jid:
                        overdue_ids.add(jid)
            except Exception:
                continue
        sel: list[str] = []
        for iid in self._tree_jobs.get_children():
            vals = self._tree_jobs.item(iid, "values")
            if not vals:
                continue
            jid = str(vals[0]).strip()
            if jid in overdue_ids:
                sel.append(iid)
        if sel:
            self._tree_jobs.selection_set(sel)
        else:
            cur = self._tree_jobs.selection()
            if cur:
                self._tree_jobs.selection_remove(*cur)
        self._update_schedule_jobs_stats_label()

    # ---------- Xem / tái tạo field thiếu ----------

    def _selected_jobs_full(self) -> list[dict[str, Any]]:
        """Trả danh sách job (dict) tương ứng với các ID đang được chọn trong tree."""
        ids = set(self._selected_job_ids())
        if not ids:
            return []
        by_id = {str(j.get("id", "")).strip(): j for j in self._all_jobs}
        out: list[dict[str, Any]] = []
        for jid in ids:
            j = by_id.get(str(jid).strip())
            if j is not None:
                out.append(j)
        return out

    def _on_jobs_show_missing_fields(self) -> None:
        jobs = self._selected_jobs_full()
        if not jobs:
            messagebox.showwarning("Chưa chọn", "Chọn ít nhất 1 job để xem field thiếu.", parent=self._root)
            return
        top = tk.Toplevel(self._root)
        top.title("Field thiếu của job đang chọn")
        top.transient(self._root)
        txt = tk.Text(top, width=80, height=min(24, max(8, len(jobs) + 2)))
        txt.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        for j in jobs:
            missing = get_missing_fields(j)
            jid = str(j.get("id", ""))[:12]
            line = f"[{jid}] post_type={j.get('post_type', '')}  → "
            line += (", ".join(missing) if missing else "(đủ field)") + "\n"
            txt.insert(tk.END, line)
        txt.configure(state=tk.DISABLED)
        ttk.Button(top, text="Đóng", command=top.destroy).pack(pady=(0, 8))

    def _on_jobs_regenerate_missing(self) -> None:
        """Tự phát hiện field thiếu và sinh lại — tuân preset «Thiếu field» nếu user đã chọn."""
        jobs = self._selected_jobs_full()
        if not jobs:
            messagebox.showwarning(
                "Chưa chọn",
                "Chọn ít nhất 1 job để tái tạo field thiếu.",
                parent=self._root,
            )
            return
        preset = preset_by_label(
            self._var_jobs_filter_missing.get() if hasattr(self, "_var_jobs_filter_missing") else ""
        )
        allowed: list[str] | None = None
        if preset.get("match_mode") != "none" and preset.get("fields"):
            allowed = [f for f in preset["fields"] if f]
        scope_txt = (
            f"theo preset «{preset['label']}»" if allowed else "toàn bộ field thiếu"
        )
        if not messagebox.askyesno(
            "Xác nhận",
            f"Sinh lại {scope_txt} cho {len(jobs)} job đã chọn?\n"
            "- Field đã có dữ liệu hợp lệ sẽ được giữ nguyên.\n"
            "- Thao tác cần API key theo provider đã gán cho từng job và có thể mất vài phút.",
            parent=self._root,
        ):
            return
        self._run_regen_in_background(jobs, allowed_fields=allowed)

    def _on_jobs_regenerate_selected_fields(self) -> None:
        """Cho phép user chọn đích danh field cần tái tạo (chỉ áp vào các field đang thiếu)."""
        jobs = self._selected_jobs_full()
        if not jobs:
            messagebox.showwarning(
                "Chưa chọn",
                "Chọn ít nhất 1 job.",
                parent=self._root,
            )
            return
        choice = self._ask_fields_to_regenerate()
        if not choice:
            return
        self._run_regen_in_background(jobs, allowed_fields=choice)

    def _ask_fields_to_regenerate(self) -> list[str] | None:
        """Dialog checkbox cho phép chọn field cần tái tạo."""
        from src.utils.schedule_posts_missing_fields import REGENERABLE_FIELDS

        dlg = tk.Toplevel(self._root)
        dlg.title("Chọn field cần tái tạo")
        dlg.transient(self._root)
        dlg.grab_set()
        ttk.Label(
            dlg,
            text="Chỉ tái tạo những field được tick dưới đây và ĐANG THIẾU trên job.\n"
            "Các field đã có dữ liệu hợp lệ sẽ được giữ nguyên.",
            justify="left",
        ).pack(anchor="w", padx=10, pady=(10, 6))
        vars_: dict[str, tk.BooleanVar] = {}
        fr = ttk.Frame(dlg)
        fr.pack(fill=tk.BOTH, expand=True, padx=10)
        for i, f in enumerate(REGENERABLE_FIELDS):
            v = tk.BooleanVar(value=False)
            vars_[f] = v
            ttk.Checkbutton(fr, text=f, variable=v).grid(row=i, column=0, sticky="w", pady=2)
        result: dict[str, list[str] | None] = {"val": None}

        def ok() -> None:
            sel = [k for k, v in vars_.items() if v.get()]
            if not sel:
                messagebox.showwarning(
                    "Chưa chọn",
                    "Tick ít nhất một field cần tái tạo.",
                    parent=dlg,
                )
                return
            result["val"] = sel
            dlg.destroy()

        bb = ttk.Frame(dlg)
        bb.pack(fill=tk.X, padx=10, pady=10)
        ttk.Button(bb, text="Hủy", command=dlg.destroy).pack(side=tk.RIGHT)
        ttk.Button(bb, text="OK", command=ok).pack(side=tk.RIGHT, padx=(0, 6))
        self._root.wait_window(dlg)
        return result["val"]

    def _run_regen_in_background(
        self,
        jobs: list[dict[str, Any]],
        *,
        allowed_fields: list[str] | None,
    ) -> None:
        """Chạy regenerate tuần tự ở thread nền, progress cập nhật qua label; refresh bảng cuối."""
        if not hasattr(self, "_lbl_jobs_regen_status"):
            self._lbl_jobs_regen_status = ttk.Label(self._root, text="")  # fallback, không hiển thị
        total = len(jobs)
        self._lbl_jobs_regen_status.configure(
            text=f"Đang tái tạo… 0/{total}", foreground="#1a73e8"
        )

        def worker() -> None:
            from src.services.job_field_regenerator import regenerate_many_jobs

            results_summary: list[str] = []
            patched_count = 0

            def progress(i: int, tot: int, jid: str, regen: list[str]) -> None:
                msg = f"Đang tái tạo… {i}/{tot} (job {jid[:8]}: {', '.join(regen) or '—'})"
                self._root.after(0, lambda m=msg: self._lbl_jobs_regen_status.configure(text=m))

            try:
                results = regenerate_many_jobs(
                    jobs,
                    allowed_fields=allowed_fields,
                    include_image_generation=True,
                    on_progress=progress,
                )
                for (orig, (updated, regen)) in zip(jobs, results):
                    if not regen:
                        results_summary.append(
                            f"- {str(orig.get('id',''))[:12]}: (không có field nào được sinh lại)"
                        )
                        continue
                    jid = str(orig.get("id", "")).strip()
                    patch: dict[str, Any] = {}
                    for f in regen:
                        if f == "image_path":
                            if "media_files" in updated:
                                patch["media_files"] = updated["media_files"]
                            if "job_post_image_path" in updated:
                                patch["job_post_image_path"] = updated["job_post_image_path"]
                        elif f == "content":
                            patch["content"] = updated.get("content", "")
                            if updated.get("image_alt") and not str(orig.get("image_alt") or "").strip():
                                patch["image_alt"] = updated.get("image_alt")
                        else:
                            patch[f] = updated.get(f)
                    ok = self._schedule_posts.update_job_fields(jid, **patch)
                    if ok:
                        patched_count += 1
                        results_summary.append(
                            f"- {jid[:12]}: {', '.join(regen)}"
                        )
                    else:
                        results_summary.append(f"- {jid[:12]}: LƯU THẤT BẠI")
            except Exception as exc:  # noqa: BLE001
                logger.exception("Regenerate missing fields lỗi: {}", exc)
                self._root.after(
                    0,
                    lambda e=exc: messagebox.showerror(
                        "Lỗi tái tạo", f"{e}", parent=self._root
                    ),
                )
                self._root.after(
                    0,
                    lambda: self._lbl_jobs_regen_status.configure(
                        text="Lỗi khi tái tạo field.", foreground="#c5221f"
                    ),
                )
                return

            def done() -> None:
                self._lbl_jobs_regen_status.configure(
                    text=f"Đã cập nhật {patched_count}/{total} job.",
                    foreground="#188038",
                )
                self._fill_schedule_jobs_tree()
                messagebox.showinfo(
                    "Hoàn tất",
                    f"Đã tái tạo field thiếu cho {patched_count}/{total} job.\n\n"
                    + ("\n".join(results_summary)[:2400] if results_summary else ""),
                    parent=self._root,
                )

            self._root.after(0, done)

        threading.Thread(target=worker, daemon=True).start()

    def _format_scheduled_for_ui(self, job: dict[str, Any]) -> str:
        s = str((job or {}).get("scheduled_at", "") or "").strip()
        if not s:
            return "—"
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            tz_name = str((job or {}).get("timezone", "")).strip() or "Asia/Ho_Chi_Minh"
            try:
                tz = ZoneInfo(tz_name)
            except Exception:
                tz = scheduler_tz()
            local = dt.astimezone(tz)
            return local.strftime("%Y-%m-%d %H:%M")
        except Exception:
            return s

    def _on_refresh_schedule_jobs(self) -> None:
        try:
            self._schedule_posts.reload_from_disk()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi", str(exc), parent=self._root)
            return
        self._fill_schedule_jobs_tree()
        logger.info("Đã làm mới schedule_posts.json.")

    def _on_toggle_lock_browser_job(self) -> None:
        enabled = bool(self._var_lock_browser_job.get())
        os.environ["FB_LOCK_BROWSER_DURING_JOB"] = "1" if enabled else "0"
        self._sync_lock_browser_job_label()
        logger.info("FB_LOCK_BROWSER_DURING_JOB={}", os.environ["FB_LOCK_BROWSER_DURING_JOB"])

    def _sync_lock_browser_job_label(self) -> None:
        enabled = bool(self._var_lock_browser_job.get())
        if hasattr(self, "_lbl_lock_browser_job"):
            self._lbl_lock_browser_job.configure(text=f"Lock browser: {'ON' if enabled else 'OFF'}")

    def _on_change_per_account_parallel(self) -> None:
        raw = str(self._var_per_account_parallel.get() or "").strip()
        try:
            n = max(1, min(8, int(raw)))
        except ValueError:
            n = 2
        self._var_per_account_parallel.set(str(n))
        os.environ["SCHEDULE_PER_ACCOUNT_MAX_PARALLEL"] = str(n)
        os.environ["SCHEDULE_ALLOW_SAME_ACCOUNT_PARALLEL"] = "1" if n > 1 else "0"
        self._sync_per_account_parallel_label()
        logger.info(
            "SCHEDULE_PER_ACCOUNT_MAX_PARALLEL={} | SCHEDULE_ALLOW_SAME_ACCOUNT_PARALLEL={}",
            os.environ["SCHEDULE_PER_ACCOUNT_MAX_PARALLEL"],
            os.environ["SCHEDULE_ALLOW_SAME_ACCOUNT_PARALLEL"],
        )

    def _sync_per_account_parallel_label(self) -> None:
        if not hasattr(self, "_lbl_per_account_parallel"):
            return
        n = str(os.environ.get("SCHEDULE_PER_ACCOUNT_MAX_PARALLEL", "2")).strip() or "2"
        self._lbl_per_account_parallel.configure(text=f"Giới hạn: {n} job/account")

    def _refresh_openai_key_labels(self) -> None:
        s1, s2 = openai_key_status_lines()
        self._lbl_openai_sess.configure(text=s1)
        self._lbl_openai_file.configure(text=s2)

    def _fill_openai_keys_tree(self) -> None:
        for iid in self._tree_openai.get_children():
            self._tree_openai.delete(iid)
        for row in list_openai_key_rows_for_ui():
            self._tree_openai.insert(
                "",
                tk.END,
                iid=row["id"],
                values=("★" if row["is_active"] else "", row["label"], row["preview"]),
            )

    def _selected_openai_key_id(self) -> str | None:
        sel = self._tree_openai.selection()
        if not sel:
            return None
        return str(sel[0]).strip() or None

    def _refresh_openai_tab(self) -> None:
        self._refresh_openai_key_labels()
        self._fill_openai_keys_tree()

    def _refresh_gemini_key_labels(self) -> None:
        s1, s2 = gemini_key_status_lines()
        self._lbl_gemini_sess.configure(text=s1)
        self._lbl_gemini_file.configure(text=s2)

    def _fill_gemini_keys_tree(self) -> None:
        for iid in self._tree_gemini.get_children():
            self._tree_gemini.delete(iid)
        for row in list_gemini_key_rows_for_ui():
            self._tree_gemini.insert(
                "",
                tk.END,
                iid=row["id"],
                values=("★" if row["is_active"] else "", row["label"], row["preview"]),
            )

    def _selected_gemini_key_id(self) -> str | None:
        sel = self._tree_gemini.selection()
        if not sel:
            return None
        return str(sel[0]).strip() or None

    def _refresh_gemini_tab(self) -> None:
        self._refresh_gemini_key_labels()
        self._fill_gemini_keys_tree()

    def _refresh_nanobanana_key_labels(self) -> None:
        s1, s2 = nanobanana_key_status_lines()
        self._lbl_nb_sess.configure(text=s1)
        self._lbl_nb_file.configure(text=s2)

    def _fill_nanobanana_keys_tree(self) -> None:
        for iid in self._tree_nanobanana.get_children():
            self._tree_nanobanana.delete(iid)
        for row in list_nanobanana_key_rows_for_ui():
            self._tree_nanobanana.insert(
                "",
                tk.END,
                iid=row["id"],
                values=("★" if row["is_active"] else "", row["label"], row["preview"]),
            )

    def _selected_nanobanana_key_id(self) -> str | None:
        sel = self._tree_nanobanana.selection()
        if not sel:
            return None
        return str(sel[0]).strip() or None

    def _refresh_nanobanana_tab(self) -> None:
        self._refresh_nanobanana_key_labels()
        self._fill_nanobanana_keys_tree()
        cfg = get_nanobanana_runtime_config()
        self._ent_nb_web_url.delete(0, tk.END)
        self._ent_nb_account.delete(0, tk.END)
        self._ent_nb_video_model.delete(0, tk.END)
        self._ent_nb_web_url.insert(0, cfg.get("web_url", "") or "https://gemini.google.com/app?hl=en")
        self._ent_nb_account.insert(0, cfg.get("account_label", ""))
        self._ent_nb_video_model.insert(0, cfg.get("video_model", "") or "veo-3.1-generate-preview")

    def _on_add_openai_key(self) -> None:
        key = self._ent_openai_key.get().strip()
        if not key:
            messagebox.showwarning("Thiếu key", "Nhập OpenAI API key.", parent=self._root)
            return
        label = self._ent_openai_label.get().strip() or "OpenAI key mới"
        try:
            nid = add_openai_key_entry(label, key)
            set_preferred_openai_key_id(nid)
        except ValueError as exc:
            messagebox.showwarning("Không thêm được", str(exc), parent=self._root)
            return
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi", str(exc), parent=self._root)
            return
        apply_openai_key_to_environ(nid)
        self._ent_openai_key.delete(0, tk.END)
        self._refresh_openai_tab()
        messagebox.showinfo("Đã lưu", "Đã thêm OpenAI key và kích hoạt cho phiên này.", parent=self._root)

    def _on_activate_selected_openai_key(self) -> None:
        kid = self._selected_openai_key_id()
        if not kid:
            messagebox.showwarning("Chưa chọn", "Chọn một dòng key OpenAI.", parent=self._root)
            return
        key = apply_openai_key_to_environ(kid)
        if not key:
            messagebox.showerror("Lỗi", "Không đọc được key OpenAI.", parent=self._root)
            return
        self._refresh_openai_tab()

    def _on_set_default_openai_key(self) -> None:
        kid = self._selected_openai_key_id()
        if not kid:
            messagebox.showwarning("Chưa chọn", "Chọn một dòng key OpenAI.", parent=self._root)
            return
        try:
            set_preferred_openai_key_id(kid)
        except ValueError as exc:
            messagebox.showwarning("Lỗi", str(exc), parent=self._root)
            return
        apply_openai_key_to_environ(kid)
        self._refresh_openai_tab()

    def _on_delete_selected_openai_key(self) -> None:
        kid = self._selected_openai_key_id()
        if not kid:
            messagebox.showwarning("Chưa chọn", "Chọn một key để xóa.", parent=self._root)
            return
        if not messagebox.askyesno("Xác nhận", "Xóa key OpenAI này khỏi file?", parent=self._root):
            return
        cur = os.environ.get("OPENAI_API_KEY", "").strip()
        try:
            removed = delete_openai_key_entry(kid)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi", str(exc), parent=self._root)
            return
        if removed and cur == removed:
            k = apply_openai_key_to_environ(None)
            if not k:
                os.environ.pop("OPENAI_API_KEY", None)
        self._refresh_openai_tab()

    def _on_clear_all_openai_keys(self) -> None:
        if not messagebox.askyesno(
            "Xác nhận",
            "Xóa toàn bộ key OpenAI trong app_secrets.json?",
            parent=self._root,
        ):
            return
        try:
            clear_saved_openai_keys_and_sync_environ()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi", str(exc), parent=self._root)
            return
        self._ent_openai_key.delete(0, tk.END)
        self._refresh_openai_tab()

    def _on_add_gemini_key(self) -> None:
        key = self._ent_gemini_key.get().strip()
        if not key:
            messagebox.showwarning("Thiếu key", "Nhập API key.", parent=self._root)
            return
        label = self._ent_gemini_label.get().strip() or "Key mới"
        try:
            nid = add_gemini_key_entry(label, key)
            set_preferred_gemini_key_id(nid)
        except ValueError as exc:
            messagebox.showwarning("Không thêm được", str(exc), parent=self._root)
            return
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi", str(exc), parent=self._root)
            return
        apply_gemini_key_to_environ(nid)
        self._ent_gemini_key.delete(0, tk.END)
        self._refresh_gemini_tab()
        messagebox.showinfo("Đã lưu", "Đã thêm key, đặt làm mặc định và kích hoạt cho phiên này.", parent=self._root)
        logger.info("Đã thêm Gemini key id={} (GUI).", nid)

    def _on_activate_selected_gemini_key(self) -> None:
        kid = self._selected_gemini_key_id()
        if not kid:
            messagebox.showwarning("Chưa chọn", "Chọn một dòng trong bảng.", parent=self._root)
            return
        k = apply_gemini_key_to_environ(kid)
        if not k:
            messagebox.showerror("Lỗi", "Không đọc được key.", parent=self._root)
            return
        self._refresh_gemini_tab()
        logger.info("Đã kích hoạt Gemini key id={} (phiên này).", kid)

    def _on_set_default_gemini_key(self) -> None:
        kid = self._selected_gemini_key_id()
        if not kid:
            messagebox.showwarning("Chưa chọn", "Chọn một dòng trong bảng.", parent=self._root)
            return
        try:
            set_preferred_gemini_key_id(kid)
        except ValueError as exc:
            messagebox.showwarning("Lỗi", str(exc), parent=self._root)
            return
        apply_gemini_key_to_environ(kid)
        self._refresh_gemini_tab()
        messagebox.showinfo("Đã đặt", "Key này là mặc định khi mở app (và đã áp dụng cho phiên này).", parent=self._root)

    def _on_delete_selected_gemini_key(self) -> None:
        kid = self._selected_gemini_key_id()
        if not kid:
            messagebox.showwarning("Chưa chọn", "Chọn một dòng để xóa.", parent=self._root)
            return
        if not messagebox.askyesno("Xác nhận", "Xóa key này khỏi file?", parent=self._root):
            return
        cur = os.environ.get("GEMINI_API_KEY", "").strip()
        try:
            removed = delete_gemini_key_entry(kid)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi", str(exc), parent=self._root)
            return
        if removed and cur == removed:
            k = apply_gemini_key_to_environ(None)
            if not k:
                os.environ.pop("GEMINI_API_KEY", None)
        self._refresh_gemini_tab()
        logger.info("Đã xóa Gemini key id={} (GUI).", kid)

    def _on_clear_all_gemini_keys(self) -> None:
        if not messagebox.askyesno(
            "Xác nhận",
            "Xóa toàn bộ key trong app_secrets.json? (Env sẽ gỡ nếu đang trùng một key đã lưu.)",
            parent=self._root,
        ):
            return
        try:
            clear_saved_gemini_key_and_sync_environ()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi", str(exc), parent=self._root)
            return
        self._ent_gemini_key.delete(0, tk.END)
        self._refresh_gemini_tab()
        messagebox.showinfo("Đã xóa", "Đã xóa tất cả key trong file.", parent=self._root)
        logger.info("Đã xóa toàn bộ Gemini keys (GUI).")

    def _on_add_nanobanana_key(self) -> None:
        key = self._ent_nb_key.get().strip()
        if not key:
            messagebox.showwarning("Thiếu key", "Nhập API key NanoBanana/VEO3.", parent=self._root)
            return
        label = self._ent_nb_label.get().strip() or "NB key mới"
        try:
            nid = add_nanobanana_key_entry(label, key)
            set_preferred_nanobanana_key_id(nid)
        except ValueError as exc:
            messagebox.showwarning("Không thêm được", str(exc), parent=self._root)
            return
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi", str(exc), parent=self._root)
            return
        apply_nanobanana_key_to_environ(nid)
        self._ent_nb_key.delete(0, tk.END)
        self._refresh_nanobanana_tab()
        messagebox.showinfo("Đã lưu", "Đã thêm key NanoBanana và kích hoạt cho phiên này.", parent=self._root)

    def _on_activate_selected_nanobanana_key(self) -> None:
        kid = self._selected_nanobanana_key_id()
        if not kid:
            messagebox.showwarning("Chưa chọn", "Chọn một dòng key NanoBanana.", parent=self._root)
            return
        key = apply_nanobanana_key_to_environ(kid)
        if not key:
            messagebox.showerror("Lỗi", "Không đọc được key NanoBanana.", parent=self._root)
            return
        self._refresh_nanobanana_tab()

    def _on_set_default_nanobanana_key(self) -> None:
        kid = self._selected_nanobanana_key_id()
        if not kid:
            messagebox.showwarning("Chưa chọn", "Chọn một dòng key NanoBanana.", parent=self._root)
            return
        try:
            set_preferred_nanobanana_key_id(kid)
        except ValueError as exc:
            messagebox.showwarning("Lỗi", str(exc), parent=self._root)
            return
        apply_nanobanana_key_to_environ(kid)
        self._refresh_nanobanana_tab()

    def _on_delete_selected_nanobanana_key(self) -> None:
        kid = self._selected_nanobanana_key_id()
        if not kid:
            messagebox.showwarning("Chưa chọn", "Chọn một key để xóa.", parent=self._root)
            return
        if not messagebox.askyesno("Xác nhận", "Xóa key NanoBanana này khỏi file?", parent=self._root):
            return
        cur = os.environ.get("NANOBANANA_API_KEY", "").strip()
        try:
            removed = delete_nanobanana_key_entry(kid)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi", str(exc), parent=self._root)
            return
        if removed and cur == removed:
            k = apply_nanobanana_key_to_environ(None)
            if not k:
                os.environ.pop("NANOBANANA_API_KEY", None)
        self._refresh_nanobanana_tab()

    def _on_clear_all_nanobanana_keys(self) -> None:
        if not messagebox.askyesno(
            "Xác nhận",
            "Xóa toàn bộ key NanoBanana trong app_secrets.json?",
            parent=self._root,
        ):
            return
        try:
            clear_saved_nanobanana_keys_and_sync_environ()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi", str(exc), parent=self._root)
            return
        self._ent_nb_key.delete(0, tk.END)
        self._refresh_nanobanana_tab()

    def _on_save_nanobanana_runtime_config(self) -> None:
        web_url = self._ent_nb_web_url.get().strip()
        account_label = self._ent_nb_account.get().strip()
        video_model = self._ent_nb_video_model.get().strip()
        cfg = get_nanobanana_runtime_config()
        api_url = str(cfg.get("api_url", "")).strip()
        record_url = str(cfg.get("record_info_url", "")).strip()
        callback_url = str(cfg.get("callback_url", "")).strip()
        try:
            save_nanobanana_runtime_config(
                web_url=web_url,
                api_url=api_url,
                record_info_url=record_url,
                callback_url=callback_url,
                account_label=account_label,
                video_model=video_model,
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi", str(exc), parent=self._root)
            return
        if web_url:
            os.environ["NANOBANANA_WEB_URL"] = web_url
            os.environ["VEO3_WEB_URL"] = web_url
        else:
            os.environ.pop("NANOBANANA_WEB_URL", None)
            os.environ.pop("VEO3_WEB_URL", None)
        if api_url:
            os.environ["NANOBANANA_API_URL"] = api_url
        else:
            os.environ.pop("NANOBANANA_API_URL", None)
        if record_url:
            os.environ["NANOBANANA_RECORD_INFO_URL"] = record_url
        else:
            os.environ.pop("NANOBANANA_RECORD_INFO_URL", None)
        if callback_url:
            os.environ["NANOBANANA_CALLBACK_URL"] = callback_url
        else:
            os.environ.pop("NANOBANANA_CALLBACK_URL", None)
        if video_model:
            os.environ["GEMINI_VIDEO_MODEL"] = video_model
        else:
            os.environ.pop("GEMINI_VIDEO_MODEL", None)
        messagebox.showinfo("Đã lưu", "Đã lưu URL/Model Gemini-VEO3 cho phiên hiện tại.", parent=self._root)

    def _on_apply_nanobanana_locked_ui_preset(self) -> None:
        """
        Áp preset ổn định cho nhiều máy: UI tiếng Anh + selector cứng + delay click.
        """
        if not self._ent_nb_web_url.get().strip():
            self._ent_nb_web_url.insert(0, "https://gemini.google.com/app?hl=en")
        web_url = self._ent_nb_web_url.get().strip() or "https://gemini.google.com/app?hl=en"
        # Áp dụng cho phiên hiện tại
        os.environ["NANOBANANA_WEB_URL"] = web_url
        os.environ["NANOBANANA_USE_BROWSER"] = "1"
        os.environ["NANOBANANA_BROWSER_STRICT"] = "1"
        os.environ["VEO3_WEB_URL"] = web_url
        os.environ["VEO3_USE_BROWSER"] = "1"
        os.environ["VEO3_BROWSER_STRICT"] = "1"
        os.environ["NANOBANANA_LOCKED_UI"] = "1"
        os.environ["NANOBANANA_ENFORCE_MODEL"] = "0"
        os.environ["NANOBANANA_ACTION_DELAY_MS"] = "900"
        # Lưu persist vào app_secrets để mở app sau vẫn giữ preset.
        try:
            save_nanobanana_runtime_config(
                web_url=web_url,
                api_url=str(cfg.get("api_url", "")).strip(),
                record_info_url=str(cfg.get("record_info_url", "")).strip(),
                callback_url=str(cfg.get("callback_url", "")).strip(),
                account_label=self._ent_nb_account.get().strip(),
                video_model=self._ent_nb_video_model.get().strip(),
                locked_ui="1",
                enforce_model="0",
                action_delay_ms="900",
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi preset", str(exc), parent=self._root)
            return
        messagebox.showinfo(
            "Đã áp preset",
            "Đã bật preset locked-ui đa máy.\n"
            "- URL: Gemini app (EN)\n"
            "- Chế độ browser strict + locked UI\n"
            "- Không chặn luồng nếu fail chọn model\n"
            "- Delay thao tác 900ms",
            parent=self._root,
        )

    def _on_disable_nanobanana_locked_ui_preset(self) -> None:
        """
        Tắt preset locked-ui: quay về mode linh hoạt.
        """
        os.environ["NANOBANANA_LOCKED_UI"] = "0"
        os.environ["NANOBANANA_ENFORCE_MODEL"] = "0"
        os.environ["NANOBANANA_ACTION_DELAY_MS"] = "350"
        os.environ["VEO3_USE_BROWSER"] = "1"
        os.environ["VEO3_BROWSER_STRICT"] = "1"
        cfg = get_nanobanana_runtime_config()
        try:
            save_nanobanana_runtime_config(
                web_url=self._ent_nb_web_url.get().strip(),
                api_url=str(cfg.get("api_url", "")).strip(),
                record_info_url=str(cfg.get("record_info_url", "")).strip(),
                callback_url=str(cfg.get("callback_url", "")).strip(),
                account_label=self._ent_nb_account.get().strip(),
                video_model=self._ent_nb_video_model.get().strip(),
                locked_ui="0",
                enforce_model="0",
                action_delay_ms="350",
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi preset", str(exc), parent=self._root)
            return
        messagebox.showinfo(
            "Đã tắt preset",
            "Đã tắt locked-ui.\n"
            "- Selector linh hoạt hơn\n"
            "- Không ép chọn model cứng\n"
            "- Delay thao tác giảm còn 350ms",
            parent=self._root,
        )

    def _on_login_nanobanana_browser(self) -> None:
        web_url = self._ent_nb_web_url.get().strip() or "https://gemini.google.com/app?hl=en"
        os.environ["NANOBANANA_WEB_URL"] = web_url
        os.environ["NANOBANANA_USE_BROWSER"] = "1"
        os.environ["NANOBANANA_BROWSER_STRICT"] = "1"
        os.environ["VEO3_WEB_URL"] = web_url
        os.environ["VEO3_USE_BROWSER"] = "1"
        os.environ["VEO3_BROWSER_STRICT"] = "1"
        if not messagebox.askyesno(
            "Đăng nhập Gemini/Veo3",
            "Sẽ mở browser profile riêng để bạn đăng nhập Gemini/Veo3.\n"
            "Browser sẽ mở cho tới khi bạn tự đóng (không tự tắt).\n"
            "Sau khi đăng nhập xong, hãy tự đóng cửa sổ browser để app lưu phiên.\nTiếp tục?",
            parent=self._root,
        ):
            return

        def worker() -> None:
            try:
                from src.ai.image_generation import open_nanobanana_login_browser

                # wait_sec=0 => không auto-close, chờ user tự đóng browser.
                info = open_nanobanana_login_browser(wait_sec=0)
                self._root.after(0, lambda: self._finish_nanobanana_login(web_url, info, None))
            except Exception as exc:  # noqa: BLE001
                # Chốt giá trị exc vào default arg để tránh NameError closure trong callback Tkinter.
                self._root.after(0, lambda err=exc: self._finish_nanobanana_login(web_url, None, err))

        threading.Thread(target=worker, name="nanobanana_login_browser", daemon=True).start()

    def _finish_nanobanana_login(self, web_url: str, info: dict[str, str] | None, err: Exception | None) -> None:
        self._root.configure(cursor="")
        if err is not None:
            messagebox.showerror("Lỗi đăng nhập", str(err), parent=self._root)
            return
        info = info or {}
        title = str(info.get("title", "")).strip()
        if title and not self._ent_nb_account.get().strip():
            self._ent_nb_account.insert(0, title[:120])
        cfg = get_nanobanana_runtime_config()
        try:
            save_nanobanana_runtime_config(
                web_url=self._ent_nb_web_url.get().strip(),
                api_url=str(cfg.get("api_url", "")).strip(),
                record_info_url=str(cfg.get("record_info_url", "")).strip(),
                callback_url=str(cfg.get("callback_url", "")).strip(),
                account_label=self._ent_nb_account.get().strip(),
                video_model=self._ent_nb_video_model.get().strip(),
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi lưu cấu hình", str(exc), parent=self._root)
            return
        os.environ["NANOBANANA_WEB_URL"] = self._ent_nb_web_url.get().strip() or web_url
        os.environ["VEO3_WEB_URL"] = self._ent_nb_web_url.get().strip() or web_url
        if self._ent_nb_video_model.get().strip():
            os.environ["GEMINI_VIDEO_MODEL"] = self._ent_nb_video_model.get().strip()
        os.environ["VEO3_USE_BROWSER"] = "1"
        os.environ["VEO3_BROWSER_STRICT"] = "1"
        messagebox.showinfo(
            "Đã lưu đăng nhập",
            "Đã lưu profile đăng nhập trình duyệt cho Gemini/Veo3.\n"
            "Bạn có thể đóng browser ngay, app sẽ không bị treo.",
            parent=self._root,
        )

    def _selected_job_ids(self) -> list[str]:
        sel = self._tree_jobs.selection()
        out: list[str] = []
        for iid in sel:
            vals = self._tree_jobs.item(iid, "values")
            if not vals:
                continue
            jid = str(vals[0]).strip()
            if jid:
                out.append(jid)
        return out

    def _selected_job_id(self) -> str | None:
        ids = self._selected_job_ids()
        return ids[0] if ids else None

    def _on_jobs_select_all(self) -> None:
        kids = self._tree_jobs.get_children()
        if kids:
            self._tree_jobs.selection_set(kids)

    def _on_jobs_clear_selection(self) -> None:
        for iid in list(self._tree_jobs.selection()):
            self._tree_jobs.selection_remove(iid)

    def _on_run_selected_jobs_now(self) -> None:
        job_ids = self._selected_job_ids()
        if not job_ids:
            messagebox.showwarning("Chưa chọn", "Chọn ít nhất 1 job để đăng ngay.", parent=self._root)
            return
        if not messagebox.askyesno(
            "Đăng luôn",
            f"Chạy ngay {len(job_ids)} job đã chọn?\n"
            "Lưu ý: thao tác này sẽ đăng bài ngay, không chờ lịch.",
            parent=self._root,
        ):
            return
        self._root.configure(cursor="watch")

        def worker() -> None:
            ok = 0
            fail = 0
            skipped_notes: list[str] = []
            for jid in job_ids:
                row = self._schedule_posts.get_by_id(jid)
                if not row:
                    fail += 1
                    skipped_notes.append(f"- {jid}: không tìm thấy job")
                    continue
                st = str(row.get("status", "")).strip().lower()
                if st in {"paused", "cancelled", "success"}:
                    skipped_notes.append(f"- {jid}: bỏ qua do status={st}")
                    continue
                account_id = str(row.get("account_id", "")).strip()
                page_id = str(row.get("page_id", "")).strip() or None
                if not account_id:
                    fail += 1
                    skipped_notes.append(f"- {jid}: thiếu account_id")
                    continue
                try:
                    # Đồng bộ với tick scheduler: đánh dấu running trước khi chạy tay.
                    try:
                        self._schedule_posts.update_job_fields(jid, status="running")
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Không chuyển job {} sang running trước khi đăng ngay: {}", jid, exc)
                    posted_ok = run_scheduled_post_for_account(
                        account_id=account_id,
                        page_id=page_id,
                        schedule_post_job_id=jid,
                        headless=not self._show_browser,
                        force_post_now=True,
                    )
                    if posted_ok:
                        ok += 1
                    else:
                        fail += 1
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Đăng ngay job {} lỗi: {}", jid, exc)
                    fail += 1
                    skipped_notes.append(f"- {jid}: lỗi ngoại lệ {exc}")
            self._root.after(0, lambda: self._finish_run_selected_jobs_now(ok, fail, skipped_notes))

        threading.Thread(target=worker, name="run_selected_jobs_now", daemon=True).start()

    def _finish_run_selected_jobs_now(self, ok: int, fail: int, skipped_notes: list[str] | None = None) -> None:
        self._root.configure(cursor="")
        self._fill_schedule_jobs_tree()
        skipped_notes = skipped_notes or []
        skip_count = len(skipped_notes)
        detail = ""
        if skipped_notes:
            # Giữ popup gọn: hiển thị tối đa 8 dòng đầu.
            shown = skipped_notes[:8]
            detail = "\n\nBỏ qua:\n" + "\n".join(shown)
            if skip_count > len(shown):
                detail += f"\n... và {skip_count - len(shown)} mục khác"
        messagebox.showinfo(
            "Kết quả đăng ngay",
            f"Thành công: {ok}\nThất bại: {fail}\nBỏ qua: {skip_count}{detail}",
            parent=self._root,
        )

    def _open_posting_visual_monitor(self) -> None:
        """
        Cửa sổ 450×400: bước FSM từ ``data/runtime/job_run_monitor.json`` + screenshot mới nhất.
        """
        top = tk.Toplevel(self._root)
        top.title("Màn hình trực quan đăng bài")
        top.geometry("450x400")
        top.minsize(440, 360)
        frm = ttk.Frame(top, padding=6)
        frm.pack(fill=tk.BOTH, expand=True)
        step_var = tk.StringVar(value="Đang đọc tiến trình job…")
        ttk.Label(frm, textvariable=step_var, font=("Segoe UI", 8), wraplength=420, justify=tk.LEFT).pack(
            anchor="w", pady=(0, 4)
        )
        status_var = tk.StringVar(value="Đang chờ screenshot mới...")
        ttk.Label(frm, textvariable=status_var, font=("Segoe UI", 8)).pack(anchor="w", pady=(0, 4))
        img_label = ttk.Label(frm, text="(Chưa có ảnh)")
        img_label.pack(fill=tk.BOTH, expand=True)
        ctrl = ttk.Frame(frm)
        ctrl.pack(fill=tk.X, pady=(6, 0))
        ttk.Button(
            ctrl,
            text="Refresh ngay",
            command=lambda: self._refresh_posting_visual_frame(top, img_label, status_var, step_var),
        ).pack(side=tk.LEFT)
        ttk.Label(
            ctrl,
            text="JSON + logs/screenshots",
            font=("Segoe UI", 8),
            foreground="gray",
        ).pack(side=tk.LEFT, padx=(8, 0))

        # Lưu state lên window để tránh GC ảnh.
        top._visual_img_obj = None  # type: ignore[attr-defined]
        top._visual_last_file = ""  # type: ignore[attr-defined]

        def on_close() -> None:
            try:
                if hasattr(top, "_visual_after_id") and top._visual_after_id:  # type: ignore[attr-defined]
                    top.after_cancel(top._visual_after_id)  # type: ignore[attr-defined]
            except Exception:
                pass
            top.destroy()

        top.protocol("WM_DELETE_WINDOW", on_close)
        self._refresh_posting_visual_frame(top, img_label, status_var, step_var)

    def _refresh_posting_visual_frame(
        self,
        top: tk.Toplevel,
        img_label: ttk.Label,
        status_var: tk.StringVar,
        step_var: tk.StringVar,
    ) -> None:
        """
        Refresh: ``job_run_monitor.json`` + ảnh screenshot mới nhất (thu nhỏ nếu quá rộng).
        """
        try:
            mon = project_root() / "data" / "runtime" / "job_run_monitor.json"
            if mon.is_file():
                try:
                    raw = json.loads(mon.read_text(encoding="utf-8"))
                    jid = raw.get("job_id", "—")
                    st = raw.get("step", "—")
                    msg = str(raw.get("message", "") or "")
                    ts = str(raw.get("updated_at", "") or "")
                    step_var.set(f"Job {jid}\nBước: {st}\n{msg}\n{ts}")
                except Exception as exc:  # noqa: BLE001
                    step_var.set(f"Lỗi đọc job_run_monitor.json: {exc}")
            else:
                step_var.set("(Chưa có job_run_monitor.json — chưa chạy job đăng hoặc chưa ghi bước.)")

            shots_dir = project_root() / "logs" / "screenshots"
            shots_dir.mkdir(parents=True, exist_ok=True)
            files = sorted([p for p in shots_dir.glob("*.png") if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)
            if not files:
                img_label.configure(text="(Chưa có screenshot trong logs/screenshots)", image="")
                status_var.set("Chưa có screenshot.")
            else:
                latest = files[0]
                last_file = str(getattr(top, "_visual_last_file", ""))
                if str(latest) != last_file:
                    try:
                        if latest.stat().st_size > 4 * 1024 * 1024:
                            img_label.configure(text=f"Ảnh lớn >4MB, bỏ render:\n{latest.name}", image="")
                        else:
                            img = tk.PhotoImage(file=str(latest))
                            max_w, max_h = 420, 220
                            while (img.width() > max_w or img.height() > max_h) and img.width() > 8 and img.height() > 8:
                                img = img.subsample(2, 2)
                            top._visual_img_obj = img  # type: ignore[attr-defined]
                            img_label.configure(image=img, text="")
                        top._visual_last_file = str(latest)  # type: ignore[attr-defined]
                    except tk.TclError:
                        img_label.configure(
                            text=f"Không render PNG.\n{latest.name}",
                            image="",
                        )
                ts = datetime.fromtimestamp(latest.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                status_var.set(f"Screenshot: {latest.name} | {ts}")
        except Exception as exc:  # noqa: BLE001
            status_var.set(f"Lỗi refresh monitor: {exc}")
        finally:
            try:
                top._visual_after_id = top.after(  # type: ignore[attr-defined]
                    2000,
                    lambda: self._refresh_posting_visual_frame(top, img_label, status_var, step_var),
                )
            except Exception:
                pass

    def _on_add_batch_schedule_job(self) -> None:
        owner_ids = [str(a.get("id", "")) for a in self._accounts.load_all() if a.get("id")]
        if not owner_ids:
            messagebox.showwarning("Chưa có tài khoản", "Thêm tài khoản ở tab 1 trước.", parent=self._root)
            return
        from src.gui.schedule_batch_job_dialog import ScheduleBatchJobDialog

        dlg = ScheduleBatchJobDialog(
            self._root,
            self._schedule_posts,
            self._pages,
            owner_ids,
            title="Thêm batch job lịch đăng",
        )
        self._root.wait_window(dlg.window)
        if getattr(dlg, "saved_count", 0):
            self._fill_schedule_jobs_tree()
            logger.info("Đã lưu {} job từ batch dialog.", dlg.saved_count)

    def _on_add_schedule_job(self) -> None:
        owner_ids = [str(a.get("id", "")) for a in self._accounts.load_all() if a.get("id")]
        if not owner_ids:
            messagebox.showwarning("Chưa có tài khoản", "Thêm tài khoản ở tab 1 trước.", parent=self._root)
            return
        dlg = SchedulePostJobDialog(
            self._root,
            self._schedule_posts,
            self._pages,
            owner_ids,
            title="Thêm job lịch đăng",
            initial=None,
        )
        if dlg.result:
            try:
                self._schedule_posts.upsert(dlg.result)  # type: ignore[arg-type]
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("Lỗi lưu", str(exc), parent=self._root)
                return
            self._fill_schedule_jobs_tree()
            logger.info("Đã thêm schedule job id={}", dlg.result.get("id"))

    def _on_edit_schedule_job(self) -> None:
        jid = self._selected_job_id()
        if not jid:
            messagebox.showwarning("Chưa chọn", "Chọn một job trong bảng.", parent=self._root)
            return
        rec = self._schedule_posts.get_by_id(jid)
        if rec is None:
            messagebox.showerror("Lỗi", f"Không tìm thấy job id={jid!r}", parent=self._root)
            return
        owner_ids = [str(a.get("id", "")) for a in self._accounts.load_all() if a.get("id")]
        dlg = SchedulePostJobDialog(
            self._root,
            self._schedule_posts,
            self._pages,
            owner_ids,
            title=f"Sửa job — {jid}",
            initial=rec,
        )
        if dlg.result:
            try:
                self._schedule_posts.upsert(dlg.result)  # type: ignore[arg-type]
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("Lỗi lưu", str(exc), parent=self._root)
                return
            self._fill_schedule_jobs_tree()
            logger.info("Đã cập nhật schedule job id={}", jid)

    def _on_delete_schedule_job(self) -> None:
        jids = list(dict.fromkeys(self._selected_job_ids()))
        if not jids:
            messagebox.showwarning("Chưa chọn", "Chọn ít nhất một job để xóa.", parent=self._root)
            return
        n = len(jids)
        preview = ", ".join(jids[:8])
        if n > 8:
            preview = f"{preview} … (+{n - 8} job)"
        if not messagebox.askyesno("Xác nhận", f"Xóa {n} job đã chọn?\n{preview}", parent=self._root):
            return
        removed, missing = self._schedule_posts.delete_by_ids(jids)
        self._fill_schedule_jobs_tree()
        if missing:
            messagebox.showwarning(
                "Một phần không xóa được",
                "Không tìm thấy trong hàng đợi:\n"
                + "\n".join(missing[:25])
                + (f"\n… (+{len(missing) - 25} id)" if len(missing) > 25 else ""),
                parent=self._root,
            )
        if removed:
            logger.info("Đã xóa {} schedule job", removed)

    def _selected_page_id(self) -> str | None:
        sel = self._tree_pages.selection()
        if not sel:
            return None
        vals = self._tree_pages.item(sel[0], "values")
        if not vals:
            return None
        return str(vals[0]).strip() or None

    def _selected_page_ids(self) -> list[str]:
        ids: list[str] = []
        for iid in self._tree_pages.selection():
            vals = self._tree_pages.item(iid, "values")
            if not vals:
                continue
            pid = str(vals[0]).strip()
            if pid:
                ids.append(pid)
        # giữ thứ tự, bỏ trùng
        return list(dict.fromkeys(ids))

    def _record_page_by_id(self, page_id: str) -> PageRecord | None:
        return self._pages.get_by_id(page_id)

    def _run_page_io_task(
        self,
        *,
        title: str,
        worker: Callable[[], Any],
        on_done: Callable[[Any], None],
    ) -> None:
        """
        Chạy thao tác I/O ``pages.json`` ở thread nền để tránh đơ UI Tk.
        """
        top = tk.Toplevel(self._root)
        top.title("Đang xử lý")
        top.transient(self._root)
        top.grab_set()
        top.resizable(False, False)
        top.geometry("400x124")
        ttk.Label(top, text=title, anchor="w", justify="left", wraplength=340).pack(
            fill=tk.X, padx=14, pady=(14, 8)
        )
        ttk.Label(top, text="Vui lòng chờ…", foreground="#6b6b6b").pack(anchor="w", padx=14, pady=(0, 6))
        pbar = ttk.Progressbar(top, mode="indeterminate", length=350)
        pbar.pack(fill=tk.X, padx=14, pady=(0, 10))
        pbar.start(10)

        done_evt = threading.Event()
        out: dict[str, Any] = {"result": None, "error": None}

        def _bg() -> None:
            try:
                out["result"] = worker()
            except Exception as exc:  # noqa: BLE001
                out["error"] = exc
                logger.exception("Page I/O task lỗi: {}", exc)
            finally:
                done_evt.set()

        def _poll() -> None:
            if not top.winfo_exists():
                return
            if not done_evt.is_set():
                top.after(120, _poll)
                return
            try:
                pbar.stop()
            except tk.TclError:
                pass
            try:
                top.grab_release()
            except tk.TclError:
                pass
            top.destroy()
            err = out.get("error")
            if err is not None:
                messagebox.showerror("Lỗi", str(err), parent=self._root)
                return
            on_done(out.get("result"))

        threading.Thread(target=_bg, name="pages_io_task", daemon=True).start()
        top.after(120, _poll)

    def _on_add_page(self) -> None:
        owner_ids = [str(a.get("id", "")) for a in self._accounts.load_all() if a.get("id")]
        if not owner_ids:
            messagebox.showwarning("Chưa có tài khoản", "Thêm tài khoản ở tab 1 trước.", parent=self._root)
            return
        dlg = PageFormDialog(
            self._root,
            self._pages,
            owner_ids,
            title="Thêm Page / Group",
            initial=None,
            id_readonly=False,
        )
        if dlg.result:
            row = dict(dlg.result)

            def worker() -> bool:
                self._pages.upsert(row)  # type: ignore[arg-type]
                return True

            def done(_ok: Any) -> None:
                self._fill_pages_tree()
                logger.info("Đã thêm page id={}", row.get("id"))

            self._run_page_io_task(
                title="Đang lưu Page/Group…",
                worker=worker,
                on_done=done,
            )

    def _on_scan_pages_from_account(self) -> None:
        """Mở dialog quét toàn bộ Page từ một tài khoản đã login."""
        owner_ids = [str(a.get("id", "")) for a in self._accounts.load_all() if a.get("id")]
        if not owner_ids:
            messagebox.showwarning(
                "Chưa có tài khoản",
                "Thêm tài khoản ở tab 1 và đăng nhập xong rồi mới quét Page.",
                parent=self._root,
            )
            return
        dlg = PageScanDialog(self._root, self._accounts, self._pages)
        if dlg.saved_count > 0:
            self._fill_pages_tree()
            logger.info("Đã thêm/cập nhật {} page từ scan.", dlg.saved_count)

    def _on_edit_page(self) -> None:
        pid = self._selected_page_id()
        if not pid:
            messagebox.showwarning("Chưa chọn", "Chọn một Page trong bảng.", parent=self._root)
            return
        rec = self._record_page_by_id(pid)
        if rec is None:
            messagebox.showerror("Lỗi", f"Không tìm thấy id={pid!r}", parent=self._root)
            return
        owner_ids = [str(a.get("id", "")) for a in self._accounts.load_all() if a.get("id")]
        dlg = PageFormDialog(
            self._root,
            self._pages,
            owner_ids,
            title=f"Sửa Page — {pid}",
            initial=rec,
            id_readonly=True,
        )
        if dlg.result:
            row = dict(dlg.result)

            def worker() -> bool:
                self._pages.upsert(row)  # type: ignore[arg-type]
                return True

            def done(_ok: Any) -> None:
                self._fill_pages_tree()
                logger.info("Đã cập nhật page id={}", pid)

            self._run_page_io_task(
                title="Đang cập nhật Page/Group…",
                worker=worker,
                on_done=done,
            )

    def _on_goto_jobs_for_page(self) -> None:
        """Chuyển sang tab Job; gợi ý tạo job cho Page đang chọn."""
        try:
            self._nb.select(self._jobs_tab_index)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không chuyển tab: {}", exc)
            return
        pid = self._selected_page_id()
        if pid:
            messagebox.showinfo(
                "Job lịch đăng",
                f"Page đang chọn: {pid}\nBấm «Thêm job», chọn Page và điền lịch + AI, rồi «Lưu».",
                parent=self._root,
            )
        else:
            messagebox.showinfo(
                "Job lịch đăng",
                "Chọn một Page ở bảng tab này rồi bấm lại «Job lịch đăng…», hoặc sang tab 3 và «Thêm job».",
                parent=self._root,
            )

    def _on_delete_page(self) -> None:
        pids = self._selected_page_ids()
        if not pids:
            messagebox.showwarning(
                "Chưa chọn",
                "Chọn một hoặc nhiều Page để xóa (Ctrl/Shift hoặc kéo chuột).",
                parent=self._root,
            )
            return
        n = len(pids)
        preview = ", ".join(pids[:8])
        if n > 8:
            preview = f"{preview} … (+{n - 8} page)"
        if not messagebox.askyesno(
            "Xác nhận",
            f"Xóa {n} Page đã chọn?\n{preview}",
            parent=self._root,
        ):
            return
        def worker() -> tuple[int, list[str]]:
            return self._pages.delete_by_ids(pids)

        def done(result: Any) -> None:
            removed, failed = result if isinstance(result, tuple) else (0, [])
            self._fill_pages_tree()
            if failed:
                messagebox.showwarning(
                    "Một phần không xóa được",
                    "Không tìm thấy / không xóa được các id:\n"
                    + ", ".join(failed[:20])
                    + (f" … (+{len(failed) - 20})" if len(failed) > 20 else ""),
                    parent=self._root,
                )
            logger.info("Đã xóa {} page (lỗi: {})", removed, len(failed))

        self._run_page_io_task(
            title=f"Đang xóa {n} Page đã chọn…",
            worker=worker,
            on_done=done,
        )

    def _on_dedupe_pages_by_meta_id(self) -> None:
        if not messagebox.askyesno(
            "Xác nhận",
            "Dọn các bản ghi trùng Meta Page ID (fb_page_id)?\n"
            "Hệ thống sẽ giữ 1 bản tốt nhất cho mỗi Meta ID và xóa các bản trùng còn lại.",
            parent=self._root,
        ):
            return

        def worker() -> dict[str, int]:
            return self._pages.dedupe_by_fb_page_id()

        def done(result: Any) -> None:
            stats = result if isinstance(result, dict) else {}
            groups = int(stats.get("groups", 0))
            removed = int(stats.get("removed", 0))
            self._fill_pages_tree()
            if removed <= 0:
                messagebox.showinfo(
                    "Dọn trùng",
                    "Không phát hiện bản ghi trùng Meta Page ID.",
                    parent=self._root,
                )
            else:
                messagebox.showinfo(
                    "Dọn trùng xong",
                    f"Đã xử lý {groups} nhóm trùng, xóa {removed} bản ghi trùng.",
                    parent=self._root,
                )
            logger.info("Dedup pages by meta id: groups={} removed={}", groups, removed)

        self._run_page_io_task(
            title="Đang dọn các Page trùng Meta ID…",
            worker=worker,
            on_done=done,
        )

    def _on_capture_cookie_account(self) -> None:
        """Mở profile + .exe của tài khoản đang chọn → đăng nhập FB → ghi ``storage_state`` vào ``cookie_path``."""
        ids = self._profile_ids_for_bulk()
        if not ids:
            messagebox.showwarning(
                "Chưa chọn",
                "Tick một ô «Chọn» (☑) hoặc chọn đúng một dòng trong bảng tài khoản.",
                parent=self._root,
            )
            return
        if len(ids) > 1:
            messagebox.showwarning(
                "Cookie",
                "«Lấy cookie (Playwright)» chỉ một tài khoản — chỉ tick một ô, hoặc bỏ tick và chọn một dòng.",
                parent=self._root,
            )
            return
        aid = ids[0]
        rec = self._record_by_id(aid)
        if rec is None:
            messagebox.showerror("Cookie", f"Không tìm thấy id={aid!r}.", parent=self._root)
            return
        acc = dict(rec)
        portable = str(acc.get("portable_path") or acc.get("profile_path") or "").strip()
        if not portable:
            messagebox.showwarning("Cookie", "Thiếu portable_path / profile_path.", parent=self._root)
            return
        prof_dir = Path(portable)
        if not prof_dir.is_absolute():
            prof_dir = (project_root() / prof_dir).resolve()
        else:
            prof_dir = prof_dir.resolve()
        exe_one = str(acc.get("browser_exe_path", "")).strip()
        if exe_one and not Path(exe_one).is_file():
            exe_one = ""
        if not exe_one:
            found = find_browser_exe_in_directory(prof_dir)
            if found:
                exe_one = found
                logger.info("Cookie (Playwright): tự tìm .exe trong profile → {}", exe_one)
        if not exe_one or not Path(exe_one).is_file():
            messagebox.showwarning(
                "Cookie",
                "Cần file .exe trình duyệt (firefox.exe / chrome.exe…).\n"
                "Đã quét thư mục portable nhưng không thấy — bấm «Sửa» và điền browser_exe_path, "
                "hoặc đặt portable đúng cấu trúc (ví dụ firefox.exe nằm trong profile).",
                parent=self._root,
            )
            return
        ck_rel = str(acc.get("cookie_path", "")).strip() or f"data/cookies/{aid}.json"
        proxy = acc.get("proxy")
        if not isinstance(proxy, dict):
            proxy = {"host": "", "port": 0, "user": "", "pass": ""}
        acc_preview: dict[str, Any] = {
            **acc,
            "id": aid,
            "portable_path": portable,
            "profile_path": portable,
            "proxy": proxy,
            "cookie_path": ck_rel,
            "browser_exe_path": exe_one,
        }

        def after_save() -> None:
            dest = cookie_storage_dest(ck_rel, project_root())
            new_ck = account_cookie_path_field(dest)
            try:
                self._accounts.update_account_fields(aid, {"cookie_path": new_ck})
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("Lỗi lưu accounts.json", str(exc), parent=self._root)
                return
            self._refresh_tree()
            self._warn_if_scheduler_running_after_config_change()

        run_fb_cookie_capture_dialog(
            self._root,
            self._accounts,
            acc_preview,
            ck_rel,
            log_label=aid,
            tip_extra="File ghi vào cookie_path của tài khoản (cập nhật accounts.json sau khi lưu).",
            on_after_save=after_save,
        )

    def _on_open_profile_browser(self) -> None:
        """Mở profile browser thật của 1 tài khoản (giữ phiên login hiện có)."""
        ids = self._profile_ids_for_bulk()
        if not ids:
            messagebox.showwarning(
                "Chưa chọn",
                "Tick một ô «Chọn» (☑) hoặc chọn đúng một dòng tài khoản.",
                parent=self._root,
            )
            return
        if len(ids) > 1:
            messagebox.showwarning(
                "Mở profile",
                "Chỉ mở 1 profile/lần. Hãy chọn đúng một tài khoản.",
                parent=self._root,
            )
            return
        aid = ids[0]
        if self._record_by_id(aid) is None:
            messagebox.showerror("Mở profile", f"Không tìm thấy id={aid!r}.", parent=self._root)
            return
        # Tránh mở trùng cùng 1 profile khi phiên manual trước vẫn đang sống:
        # Firefox thường sẽ thoát sớm (exitCode=0) nếu profile đang bị giữ bởi phiên khác.
        alive_sessions: list[dict[str, Any]] = []
        already_open = False
        for sess in list(self._manual_profile_sessions):
            try:
                th = sess.get("thread")
                if th is not None and th.is_alive():
                    alive_sessions.append(sess)
                    if str(sess.get("account_id", "")).strip() == aid:
                        already_open = True
            except Exception:
                continue
        self._manual_profile_sessions = alive_sessions
        if already_open:
            messagebox.showinfo(
                "Profile đang mở",
                (
                    f"Profile của tài khoản {aid} đã được mở ở phiên trước.\n"
                    "Hãy dùng lại cửa sổ đó hoặc bấm «Đóng profile đang mở» để reset phiên manual profile."
                ),
                parent=self._root,
            )
            return
        # Playwright Sync API phải mở/đóng trên cùng một worker thread (không đóng từ luồng Tk).
        shutdown_evt = threading.Event()
        state: dict[str, Any] = {"factory": None, "ctx": None, "err": None, "ready": threading.Event()}

        def _open_worker() -> None:
            factory: BrowserFactory | None = None
            ctx_hold = None
            try:
                factory = BrowserFactory(accounts=self._accounts, headless=not self._show_browser)
                ctx_hold = factory.get_browser_context(aid, headless=not self._show_browser)
                page = ctx_hold.pages[0] if ctx_hold.pages else ctx_hold.new_page()
                start_url = os.environ.get("FB_OPEN_PROFILE_START_URL", "about:blank").strip() or "about:blank"
                page.goto(start_url, wait_until="domcontentloaded", timeout=60_000)
                state["factory"] = factory
                state["ctx"] = ctx_hold
                state["err"] = None
                state["ready"].set()
                shutdown_evt.wait()
            except Exception as exc:  # noqa: BLE001
                state["err"] = exc
            finally:
                ctx_f = state.get("ctx") or ctx_hold
                fac_f = state.get("factory") or factory
                try:
                    sync_close_persistent_context(ctx_f, log_label=f"manual_profile:{aid}")
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Đóng context manual profile ({}): {}", aid, exc)
                if fac_f is not None:
                    try:
                        fac_f.close()
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Đóng factory manual profile ({}): {}", aid, exc)
                state["ctx"] = None
                state["factory"] = None

        self._root.configure(cursor="watch")
        self._root.update_idletasks()
        th = threading.Thread(target=_open_worker, name="open_profile_playwright", daemon=True)
        th.start()
        t_deadline = time.monotonic() + 120.0
        while time.monotonic() <= t_deadline:
            if state["ready"].is_set():
                break
            if state.get("err") is not None:
                break
            if not th.is_alive() and not state["ready"].is_set():
                break
            th.join(timeout=0.08)
            try:
                self._root.update_idletasks()
                self._root.update()
            except tk.TclError:
                break
        self._root.configure(cursor="")
        if not state["ready"].is_set():
            shutdown_evt.set()
            th.join(timeout=15.0)
            if state.get("err"):
                messagebox.showerror("Mở profile thất bại", str(state["err"]), parent=self._root)
            elif not th.is_alive():
                messagebox.showerror(
                    "Mở profile thất bại",
                    "Luồng mở trình duyệt đã kết thúc sớm.",
                    parent=self._root,
                )
            else:
                messagebox.showerror(
                    "Mở profile",
                    "Hết thời gian chờ (120s) — luồng mở trình duyệt chưa xong.",
                    parent=self._root,
                )
            return
        self._manual_profile_sessions.append({"account_id": aid, "thread": th, "shutdown": shutdown_evt})
        messagebox.showinfo(
            "Đã mở profile",
            f"Đã mở profile browser cho tài khoản {aid}.\nBạn có thể thao tác/login trực tiếp trên cửa sổ này.",
            parent=self._root,
        )

    def _on_close_open_profiles(self) -> None:
        """
        Đóng các phiên manual profile đang mở (không cần thoát app).
        """
        self._set_ui_busy("close_open_profiles")
        sessions = [s for s in list(self._manual_profile_sessions) if s.get("thread") is not None]
        if not sessions:
            self._manual_profile_sessions = []
            self._clear_ui_busy()
            messagebox.showinfo("Profile", "Hiện không có profile manual nào đang mở.", parent=self._root)
            return

        for sess in sessions:
            try:
                sess["shutdown"].set()
            except Exception:
                pass

        if hasattr(self, "_btn_close_open_profiles"):
            self._btn_close_open_profiles.configure(state=tk.DISABLED)
        if hasattr(self, "_lbl_state"):
            self._lbl_state.configure(text="Profile: đang đóng…")

        started = time.monotonic()
        timeout_sec = 8.0

        def _poll() -> None:
            alive: list[dict[str, Any]] = []
            for sess in sessions:
                try:
                    th = sess.get("thread")
                    if th is not None and th.is_alive():
                        alive.append(sess)
                except Exception:
                    continue
            if not alive:
                self._manual_profile_sessions = []
                if hasattr(self, "_btn_close_open_profiles"):
                    self._btn_close_open_profiles.configure(state=tk.NORMAL)
                if hasattr(self, "_lbl_state"):
                    self._lbl_state.configure(text="Profile: đã đóng")
                self._clear_ui_busy()
                messagebox.showinfo("Profile", "Đã đóng các profile manual đang mở.", parent=self._root)
                return
            if time.monotonic() - started >= timeout_sec:
                # Giữ lại session còn sống để lần sau tiếp tục đóng được.
                self._manual_profile_sessions = alive
                if hasattr(self, "_btn_close_open_profiles"):
                    self._btn_close_open_profiles.configure(state=tk.NORMAL)
                if hasattr(self, "_lbl_state"):
                    self._lbl_state.configure(text="Profile: còn phiên chưa đóng")
                self._clear_ui_busy()
                messagebox.showwarning(
                    "Profile",
                    "Một số profile chưa đóng kịp. Hãy đợi thêm vài giây rồi bấm lại.",
                    parent=self._root,
                )
                return
            self._root.after(120, _poll)

        self._root.after(120, _poll)

    def _on_verify_profile(self) -> None:
        ids = self._profile_ids_for_bulk()
        if not ids:
            messagebox.showwarning(
                "Chưa chọn",
                "Tick ít nhất một ô «Chọn» (☑), hoặc chọn dòng trong bảng (Ctrl/Shift / kéo chuột).",
                parent=self._root,
            )
            return
        self._root.configure(cursor="watch")
        self._root.update_idletasks()
        lines: list[str] = []
        n_ok = 0
        n_fail = 0
        try:
            for aid in ids:
                try:
                    ok, msg = BrowserEngine.verify_profile_ready(self._accounts, aid, headless=True)
                    if ok:
                        n_ok += 1
                    else:
                        n_fail += 1
                    flag = "OK" if ok else "LỖI"
                    short = (msg or "").replace("\n", " ")
                    if len(short) > 180:
                        short = short[:177] + "…"
                    lines.append(f"• {aid}: {flag} — {short}")
                except Exception as exc:  # noqa: BLE001
                    n_fail += 1
                    lines.append(f"• {aid}: LỖI — {exc}")
        finally:
            self._root.configure(cursor="")
        body = "\n".join(lines[:80])
        if len(lines) > 80:
            body += f"\n… (+{len(lines) - 80} tài khoản)"
        title = f"Verify Profile ({len(ids)} tài khoản — OK {n_ok}, lỗi {n_fail})"
        if n_fail == 0:
            messagebox.showinfo(title, body, parent=self._root)
        elif n_ok == 0:
            messagebox.showerror(title, body, parent=self._root)
        else:
            messagebox.showwarning(title, body, parent=self._root)

    def _on_check_proxy(self) -> None:
        ids = self._profile_ids_for_bulk()
        if not ids:
            messagebox.showwarning(
                "Chưa chọn",
                "Tick ít nhất một ô «Chọn» (☑), hoặc chọn dòng trong bảng (Ctrl/Shift / kéo chuột).",
                parent=self._root,
            )
            return
        lines: list[str] = []
        n_live = 0
        n_die = 0
        n_skip = 0
        for aid in ids:
            rec = self._record_by_id(aid)
            if rec is None:
                n_die += 1
                lines.append(f"• {aid}: LỖI — không tìm thấy bản ghi")
                continue
            if not _coerce_use_proxy(rec.get("use_proxy", True)):
                n_skip += 1
                lines.append(f"• {aid}: (bỏ qua — tắt «Dùng proxy»)")
                continue
            px = rec.get("proxy") or {}
            try:
                port = int(px.get("port", 0))
            except (TypeError, ValueError):
                n_die += 1
                lines.append(f"• {aid}: LỖI — port proxy không hợp lệ")
                continue
            ok, msg = check_http_proxy(
                str(px.get("host", "")),
                port,
                user=str(px.get("user", "")),
                password=str(px.get("pass", "")),
            )
            if ok:
                n_live += 1
                ip = (msg or "").replace("\n", " ")
                if len(ip) > 80:
                    ip = ip[:77] + "…"
                lines.append(f"• {aid}: LIVE — {ip}")
            else:
                n_die += 1
                err = (msg or "").replace("\n", " ")
                if len(err) > 120:
                    err = err[:117] + "…"
                lines.append(f"• {aid}: DIE / lỗi — {err}")
        body = "\n".join(lines[:80])
        if len(lines) > 80:
            body += f"\n… (+{len(lines) - 80} tài khoản)"
        title = f"Kiểm tra proxy ({len(ids)} dòng — LIVE {n_live}, DIE/lỗi {n_die}, bỏ qua {n_skip})"
        if n_die == 0:
            messagebox.showinfo(title, body, parent=self._root)
        elif n_live == 0:
            messagebox.showerror(title, body, parent=self._root)
        else:
            messagebox.showwarning(title, body, parent=self._root)

    def _warn_if_scheduler_running_after_config_change(self) -> None:
        """
        Nhắc khởi động lại lịch nếu đã sửa JSON trong khi scheduler còn chạy.
        """
        if self._worker is not None and self._worker.is_alive():
            messagebox.showwarning(
                "Cập nhật cấu hình",
                "Lịch đang chạy. Hãy «Dừng lịch» rồi «Bắt đầu lịch» để cron áp dụng giờ đăng trên Page / danh sách mới.",
                parent=self._root,
            )

    def _on_add_account(self) -> None:
        """
        Mở form thêm tài khoản với mẫu mặc định, ``upsert`` nếu người dùng lưu.
        """
        init = template_new_account()
        dlg = AccountFormDialog(
            self._root,
            self._accounts,
            title="Thêm tài khoản Facebook",
            initial=init,
            id_readonly=False,
        )
        if dlg.result:
            try:
                rows = dlg.result if isinstance(dlg.result, list) else [dlg.result]
                for row in rows:
                    self._accounts.upsert(row)  # type: ignore[arg-type]
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("Lỗi lưu", str(exc), parent=self._root)
                return
            self._refresh_tree()
            self._warn_if_scheduler_running_after_config_change()
            n = len(dlg.result) if isinstance(dlg.result, list) else 1
            logger.info("Đã lưu {} tài khoản từ form thêm.", n)

    def _on_edit_account(self) -> None:
        """
        Sửa tài khoản đang chọn (double-click cũng gọi hàm này).
        """
        ids = self._profile_ids_for_bulk()
        if not ids:
            messagebox.showwarning(
                "Chưa chọn",
                "Tick một ô «Chọn» (☑) hoặc chọn một dòng trong bảng tài khoản.",
                parent=self._root,
            )
            return
        if len(ids) > 1:
            messagebox.showwarning(
                "Sửa",
                "Chỉ sửa một tài khoản — chỉ tick một ô, hoặc bỏ tick và chọn một dòng.",
                parent=self._root,
            )
            return
        aid = ids[0]
        rec = self._record_by_id(aid)
        if rec is None:
            messagebox.showerror("Lỗi", f"Không tìm thấy id={aid!r}", parent=self._root)
            return
        dlg = AccountFormDialog(
            self._root,
            self._accounts,
            title=f"Sửa tài khoản — {aid}",
            initial=rec,
            id_readonly=True,
        )
        if dlg.result:
            try:
                rows = dlg.result if isinstance(dlg.result, list) else [dlg.result]
                for row in rows:
                    self._accounts.upsert(row)  # type: ignore[arg-type]
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("Lỗi lưu", str(exc), parent=self._root)
                return
            self._refresh_tree()
            self._warn_if_scheduler_running_after_config_change()
            logger.info("Đã cập nhật tài khoản id={}", aid)

    def _on_delete_account(self) -> None:
        """
        Xóa một hoặc nhiều tài khoản đang chọn sau khi xác nhận.
        """
        ids = self._profile_ids_for_bulk()
        if not ids:
            messagebox.showwarning(
                "Chưa chọn",
                "Tick ít nhất một ô «Chọn» (☑), hoặc chọn dòng cần xóa (Ctrl/Shift / kéo chuột).",
                parent=self._root,
            )
            return
        if len(ids) == 1:
            q = f"Xóa vĩnh viễn tài khoản {ids[0]!r}?"
        else:
            preview = ", ".join(ids[:15])
            if len(ids) > 15:
                preview += f", … (+{len(ids) - 15})"
            q = f"Xóa vĩnh viễn {len(ids)} tài khoản?\n\n{preview}"
        if not messagebox.askyesno("Xác nhận xóa", q, parent=self._root):
            return
        failed: list[str] = []
        for aid in ids:
            if not self._accounts.delete_by_id(aid):
                failed.append(aid)
        self._refresh_tree()
        self._warn_if_scheduler_running_after_config_change()
        if failed:
            messagebox.showwarning("Xóa", f"Không xóa được các id:\n{', '.join(failed)}", parent=self._root)
        logger.info("Đã xóa {} tài khoản (lỗi: {})", len(ids) - len(failed), len(failed))

    def _on_duplicate_account(self) -> None:
        """
        Nhân bản tài khoản đang chọn (id mới mặc định kèm hậu tố ``_copy``).
        """
        ids = self._profile_ids_for_bulk()
        if not ids:
            messagebox.showwarning(
                "Chưa chọn",
                "Tick một ô «Chọn» (☑) hoặc chọn một dòng để nhân bản.",
                parent=self._root,
            )
            return
        if len(ids) > 1:
            messagebox.showwarning(
                "Nhân bản",
                "Chỉ nhân bản một tài khoản — chỉ tick một ô, hoặc bỏ tick và chọn một dòng.",
                parent=self._root,
            )
            return
        aid = ids[0]
        rec = self._record_by_id(aid)
        if rec is None:
            return
        base = copy.deepcopy(dict(rec))
        base.pop("last_post_at", None)
        base["status"] = "pending"
        new_id = f"{aid}_copy"
        base["id"] = new_id
        base["portable_path"] = f"data/profiles/chromium/{new_id}"
        base["cookie_path"] = f"data/cookies/{new_id}.json"
        dlg = AccountFormDialog(
            self._root,
            self._accounts,
            title="Nhân bản tài khoản",
            initial=base,  # type: ignore[arg-type]
            id_readonly=False,
        )
        if dlg.result:
            try:
                self._accounts.upsert(dlg.result)  # type: ignore[arg-type]
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("Lỗi lưu", str(exc), parent=self._root)
                return
            self._refresh_tree()
            self._warn_if_scheduler_running_after_config_change()
            logger.info("Đã nhân bản từ {} → {}", aid, dlg.result.get("id"))

    def _on_export_json(self) -> None:
        """
        Xuất toàn bộ danh sách ra file JSON.
        """
        export_accounts_json(self._accounts, self._root)
        self._refresh_tree()

    def _on_import_json(self) -> None:
        """
        Nhập JSON và thêm tài khoản id mới.
        """
        import_accounts_append(self._accounts, self._root)
        self._refresh_tree()
        self._warn_if_scheduler_running_after_config_change()

    def _on_export_tool_bundle(self) -> None:
        """
        Xuất gói dữ liệu để chuyển tool sang máy khác:
        accounts + pages + schedule_posts.
        """
        self._set_ui_busy("export_tool_bundle")
        try:
            accounts = [dict(x) for x in self._accounts.load_all()]
            pages = [dict(x) for x in self._pages.load_all()]
            jobs = [dict(x) for x in self._schedule_posts.load_all()]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Xuất dữ liệu", f"Không đọc được dữ liệu hiện tại:\n{exc}", parent=self._root)
            return

        payload = {
            "bundle_type": "toolfb_data_bundle",
            "bundle_version": 1,
            "exported_at": datetime.now().replace(microsecond=0).isoformat(),
            "project": "ToolFB",
            "data": {
                "accounts": accounts,
                "pages": pages,
                "schedule_posts": jobs,
            },
        }
        default_name = f"toolfb_bundle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        target = filedialog.asksaveasfilename(
            parent=self._root,
            title="Xuất dữ liệu ToolFB",
            defaultextension=".json",
            initialfile=default_name,
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not target:
            return
        try:
            Path(target).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Xuất dữ liệu", f"Không ghi được file bundle:\n{exc}", parent=self._root)
            return
        messagebox.showinfo(
            "Xuất dữ liệu",
            (
                f"Đã xuất bundle thành công:\n{target}\n\n"
                f"Tài khoản: {len(accounts)}\n"
                f"Page/Group: {len(pages)}\n"
                f"Job lịch: {len(jobs)}"
            ),
            parent=self._root,
        )
        self._clear_ui_busy()

    def _on_import_tool_bundle(self) -> None:
        """
        Nhập gói dữ liệu ToolFB (ghi đè accounts/pages/schedule_posts).
        """
        self._set_ui_busy("import_tool_bundle")
        source = filedialog.askopenfilename(
            parent=self._root,
            title="Nhập dữ liệu ToolFB",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not source:
            self._clear_ui_busy()
            return
        try:
            raw = json.loads(Path(source).read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Nhập dữ liệu", f"Không đọc được file bundle:\n{exc}", parent=self._root)
            self._clear_ui_busy()
            return
        if not isinstance(raw, dict):
            messagebox.showerror("Nhập dữ liệu", "Bundle không hợp lệ: JSON gốc phải là object.", parent=self._root)
            self._clear_ui_busy()
            return

        data = raw.get("data")
        if not isinstance(data, dict):
            # tương thích bundle tối giản chỉ chứa 3 key
            data = raw
        accounts = data.get("accounts")
        pages = data.get("pages")
        jobs = data.get("schedule_posts")
        if not isinstance(accounts, list) or not isinstance(pages, list) or not isinstance(jobs, list):
            messagebox.showerror(
                "Nhập dữ liệu",
                "Bundle không hợp lệ: cần có mảng accounts, pages, schedule_posts.",
                parent=self._root,
            )
            self._clear_ui_busy()
            return
        if not all(isinstance(x, dict) for x in accounts + pages + jobs):
            messagebox.showerror(
                "Nhập dữ liệu",
                "Bundle không hợp lệ: mỗi phần tử trong accounts/pages/schedule_posts phải là object.",
                parent=self._root,
            )
            self._clear_ui_busy()
            return

        confirm = messagebox.askyesno(
            "Xác nhận nhập dữ liệu",
            (
                "Nhập bundle sẽ GHI ĐÈ dữ liệu hiện tại:\n"
                "- accounts.json\n"
                "- pages.json\n"
                "- schedule_posts.json\n\n"
                "Tool sẽ tự tạo backup trước khi ghi đè.\n"
                "Bạn có muốn tiếp tục?"
            ),
            parent=self._root,
        )
        if not confirm:
            self._clear_ui_busy()
            return

        backup_dir = project_root() / "data" / "backups" / f"bundle_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            backup_dir.mkdir(parents=True, exist_ok=True)
            Path(backup_dir / "accounts.json").write_text(
                json.dumps(self._accounts.load_all(), ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            Path(backup_dir / "pages.json").write_text(
                json.dumps(self._pages.load_all(), ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            Path(backup_dir / "schedule_posts.json").write_text(
                json.dumps(self._schedule_posts.load_all(), ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Nhập dữ liệu", f"Không tạo được backup trước khi import:\n{exc}", parent=self._root)
            self._clear_ui_busy()
            return

        normalized_accounts: list[dict[str, Any]] = []
        auto_fixed_exe = 0
        remapped_profile_path = 0
        for acc_raw in accounts:
            acc = dict(acc_raw)
            portable = str(acc.get("portable_path", "") or acc.get("profile_path", "")).strip()
            if portable:
                p = Path(portable)
                resolved = p if p.is_absolute() else (project_root() / p)
                if not resolved.exists():
                    portable_norm = portable.replace("\\", "/").lower()
                    marker = "/data/profiles/"
                    idx = portable_norm.find(marker)
                    if idx >= 0:
                        tail = portable_norm[idx + 1 :]  # data/profiles/...
                        guess = (project_root() / Path(*tail.split("/"))).resolve()
                        if guess.exists():
                            resolved = guess
                            remapped_profile_path += 1
                acc["portable_path"] = str(resolved)
                acc["profile_path"] = str(resolved)
            exe = str(acc.get("browser_exe_path", "")).strip()
            exe_ok = bool(exe) and Path(exe).is_file()
            if not exe_ok and portable:
                try:
                    found = find_browser_exe_in_directory(Path(acc["portable_path"]))
                except Exception:
                    found = ""
                if found:
                    acc["browser_exe_path"] = found
                    auto_fixed_exe += 1
                    exe = found
                    exe_ok = True
            bt = str(acc.get("browser_type", "")).strip().lower()
            exe_name = Path(exe).name.lower() if exe_ok else ""
            if exe_name:
                if "firefox" in exe_name:
                    acc["browser_type"] = "firefox"
                elif any(x in exe_name for x in ("chrome", "chromium", "msedge", "edge")):
                    acc["browser_type"] = "chromium"
            elif bt not in {"firefox", "chromium", "chrome"}:
                acc["browser_type"] = "firefox"
            normalized_accounts.append(acc)

        try:
            self._accounts.save_all(normalized_accounts)  # type: ignore[arg-type]
            self._pages.save_all([dict(x) for x in pages])  # type: ignore[arg-type]
            self._schedule_posts.save_all([dict(x) for x in jobs])  # type: ignore[arg-type]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror(
                "Nhập dữ liệu",
                (
                    "Import thất bại khi validate/ghi dữ liệu.\n"
                    f"Lỗi: {exc}\n\n"
                    f"Backup đã lưu tại:\n{backup_dir}"
                ),
                parent=self._root,
            )
            self._clear_ui_busy()
            return

        self._refresh_tree()
        self._fill_pages_tree()
        self._on_refresh_schedule_jobs()
        self._warn_if_scheduler_running_after_config_change()
        messagebox.showinfo(
            "Nhập dữ liệu",
            (
                "Đã import bundle thành công.\n\n"
                f"Tài khoản: {len(accounts)}\n"
                f"Page/Group: {len(pages)}\n"
                f"Job lịch: {len(jobs)}\n\n"
                f"Tự dò browser_exe_path: {auto_fixed_exe}\n"
                f"Remap profile path: {remapped_profile_path}\n\n"
                f"Backup dữ liệu cũ: {backup_dir}"
            ),
            parent=self._root,
        )
        self._clear_ui_busy()

    def _on_configure_update_channel(self) -> None:
        """
        Hộp thoại nhập URL manifest (latest.json), hoặc ghép URL chuẩn từ owner/repo GitHub.
        Ghi ``config/update_channel.json`` (giữ các khóa khác nếu có).
        """
        root = project_root()
        cf = root / "config" / "update_channel.json"
        data: dict[str, Any] = {}
        if cf.is_file():
            try:
                raw = json.loads(cf.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    data = dict(raw)
            except Exception:
                data = {}

        top = tk.Toplevel(self._root)
        top.title("Cấu hình kênh cập nhật")
        top.transient(self._root)
        top.grab_set()
        frm = ttk.Frame(top, padding=12)
        frm.pack(fill=tk.BOTH, expand=True)

        ttk.Label(
            frm,
            text="URL manifest (file latest.json trên máy chủ, ví dụ GitHub Release):",
            wraplength=520,
        ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 4))
        var_manifest = tk.StringVar(value=str(data.get("manifest_url", "")).strip())
        ent_manifest = ttk.Entry(frm, textvariable=var_manifest, width=72)
        ent_manifest.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(0, 12))

        ttk.Label(frm, text="Hoặc repo GitHub (owner/repo) rồi bấm «Tạo URL GitHub»:", wraplength=520).grid(
            row=2, column=0, columnspan=3, sticky="w", pady=(0, 4)
        )
        var_repo = tk.StringVar(value="")
        gh_url = str(data.get("manifest_url", "")).strip()
        if gh_url.startswith("https://github.com/") and "/releases/latest/download/latest.json" in gh_url:
            try:
                tail = gh_url.replace("https://github.com/", "", 1).split("/releases/", 1)[0]
                if "/" in tail:
                    var_repo.set(tail.strip("/"))
            except Exception:
                pass
        ent_repo = ttk.Entry(frm, textvariable=var_repo, width=36)
        ent_repo.grid(row=3, column=0, sticky="w", pady=(0, 8))

        def on_fill_github() -> None:
            try:
                var_manifest.set(github_latest_manifest_url(var_repo.get()))
            except Exception as exc:
                messagebox.showerror("Kênh cập nhật", str(exc), parent=top)

        ttk.Button(frm, text="Tạo URL GitHub", command=on_fill_github).grid(row=3, column=1, padx=(8, 0), sticky="w")

        def on_auto_git_remote() -> None:
            """Điền owner/repo + URL manifest từ ``git remote origin`` (GitHub)."""
            rid = github_owner_repo_from_git(project_root())
            if not rid:
                messagebox.showwarning(
                    "Kênh cập nhật",
                    (
                        "Không đọc được GitHub từ git (origin).\n"
                        "Cần chạy app trong thư mục clone có ``git remote origin`` trỏ tới github.com."
                    ),
                    parent=top,
                )
                return
            var_repo.set(rid)
            on_fill_github()

        ttk.Button(frm, text="Tự động từ Git remote", command=on_auto_git_remote).grid(
            row=3, column=2, padx=(8, 0), sticky="w"
        )

        hint = (
            "«Tự động từ Git remote»: điền URL từ ``git remote origin`` (máy dev / bản portable có .git).\n"
            "Ví dụ URL: https://github.com/vanchien/ToolFB/releases/latest/download/latest.json\n"
            "Sau khi lưu, dùng «Kiểm tra cập nhật». Biến môi trường TOOLFB_UPDATE_MANIFEST_URL (nếu có) vẫn được ưu tiên."
        )
        ttk.Label(frm, text=hint, wraplength=520, foreground="#555").grid(
            row=4, column=0, columnspan=3, sticky="w", pady=(4, 12)
        )

        def on_save() -> None:
            url = var_manifest.get().strip()
            if url and not (url.startswith("http://") or url.startswith("https://")):
                messagebox.showerror(
                    "Kênh cập nhật",
                    "URL manifest phải bắt đầu bằng http:// hoặc https://",
                    parent=top,
                )
                return
            out = dict(data)
            if url:
                out["manifest_url"] = url
            else:
                out.pop("manifest_url", None)
            try:
                cf.parent.mkdir(parents=True, exist_ok=True)
                cf.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            except OSError as exc:
                messagebox.showerror("Kênh cập nhật", f"Không ghi được file:\n{exc}", parent=top)
                return
            messagebox.showinfo(
                "Kênh cập nhật",
                "Đã lưu config/update_channel.json.\nBấm «Kiểm tra cập nhật» để kiểm tra bản mới.",
                parent=self._root,
            )
            top.destroy()

        def on_cancel() -> None:
            top.destroy()

        btn_row = ttk.Frame(frm)
        btn_row.grid(row=5, column=0, columnspan=3, sticky="e", pady=(8, 0))
        ttk.Button(btn_row, text="Lưu", command=on_save).pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Button(btn_row, text="Hủy", command=on_cancel).pack(side=tk.RIGHT)
        frm.columnconfigure(0, weight=1)

    def _on_reset_veo3_profiles(self) -> None:
        """
        Reset profile browser VEO3 bằng cách chuyển profile cũ sang backup rồi tạo profile mới.
        """
        if not messagebox.askyesno(
            "Reset profile VEO3",
            (
                "Thao tác này sẽ reset profile VEO3 (chính + recovery).\n"
                "Profile hiện tại sẽ được chuyển sang data/backups để có thể khôi phục thủ công.\n\n"
                "Tiếp tục?"
            ),
            parent=self._root,
        ):
            return
        root = project_root()
        base = root / "data" / "nanobanana"
        main_profile = base / "browser_profile"
        recovery_profile = base / "browser_profile_recovery"
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_root = root / "data" / "backups" / f"veo3_profile_reset_{ts}"
        backup_root.mkdir(parents=True, exist_ok=True)

        def _release_veo3_profile_locks() -> None:
            """
            Cố gắng giải phóng tiến trình đang giữ file profile VEO3 (Windows).
            Chỉ kill process có command line chứa path profile để tránh ảnh hưởng browser khác.
            """
            if os.name != "nt":
                return
            targets = [str(main_profile).lower(), str(recovery_profile).lower()]
            escaped_targets = ["'" + t.replace("'", "''") + "'" for t in targets]
            ps_script = (
                "$targets = @("
                + ",".join(escaped_targets)
                + ");"
                "$procs = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine };"
                "foreach ($p in $procs) {"
                "  $cl = ($p.CommandLine + '').ToLowerInvariant();"
                "  $hit = $false;"
                "  foreach ($t in $targets) { if ($cl.Contains($t)) { $hit = $true; break } }"
                "  if ($hit -and $p.ProcessId -ne $PID) {"
                "    try { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue } catch {}"
                "  }"
                "}"
            )
            try:
                subprocess.run(
                    ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_script],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=20,
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("Reset VEO3: bỏ qua lỗi khi giải phóng lock profile: {}", exc)

        moved: list[str] = []
        errors: list[str] = []
        for src, backup_name in (
            (main_profile, "browser_profile"),
            (recovery_profile, "browser_profile_recovery"),
        ):
            try:
                if not src.exists():
                    continue
                dst = backup_root / backup_name
                if dst.exists():
                    shutil.rmtree(dst, ignore_errors=True)
                try:
                    shutil.move(str(src), str(dst))
                except Exception as first_exc:  # noqa: BLE001
                    # Nếu profile đang bị lock (WinError 32), thử giải phóng process và move lại 1 lần.
                    msg = str(first_exc)
                    locked = "WinError 32" in msg or "being used by another process" in msg.lower()
                    if not locked:
                        raise
                    _release_veo3_profile_locks()
                    time.sleep(0.8)
                    shutil.move(str(src), str(dst))
                moved.append(str(dst))
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{src}: {exc}")

        # Tạo profile mới trống để lần chạy sau có thể login lại.
        try:
            main_profile.mkdir(parents=True, exist_ok=True)
            recovery_profile.mkdir(parents=True, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Tạo profile mới: {exc}")

        if errors:
            messagebox.showerror(
                "Reset profile VEO3",
                (
                    "Reset có lỗi:\n- "
                    + "\n- ".join(errors)
                    + "\n\nGợi ý: đóng tất cả cửa sổ Chrome/Edge đang dùng profile VEO3 rồi thử lại."
                ),
                parent=self._root,
            )
            return
        info = "Đã reset profile VEO3 thành công."
        if moved:
            info += "\n\nBackup cũ:\n- " + "\n- ".join(moved)
        info += "\n\nLưu ý: hãy đăng nhập lại Google khi chạy VEO3 lần tới."
        messagebox.showinfo("Reset profile VEO3", info, parent=self._root)

    def _on_check_updates(self) -> None:
        """Kiểm tra phiên bản mới từ kênh update (manifest URL)."""
        manifest_url = resolve_manifest_url(project_root())
        if not manifest_url:
            if messagebox.askyesno(
                "Cập nhật",
                (
                    "Chưa cấu hình URL manifest (latest.json).\n\n"
                    "Mở «Cấu hình kênh cập nhật» để nhập URL GitHub hoặc manifest khác?\n\n"
                    "(Có thể dùng biến TOOLFB_UPDATE_MANIFEST_URL; dev: manifest_file / dist/latest.json.)"
                ),
                parent=self._root,
            ):
                self._on_configure_update_channel()
            return
        self._set_ui_busy("check_updates")
        self._btn_check_updates.configure(state=tk.DISABLED)
        self._btn_apply_update.configure(state=tk.DISABLED)

        def worker() -> None:
            try:
                local_v = read_local_version(project_root())
                mf = read_manifest_from_url(manifest_url)
                has_new = is_newer_version(mf.version, local_v)

                def done_ok() -> None:
                    self._btn_check_updates.configure(state=tk.NORMAL)
                    self._clear_ui_busy()
                    self._latest_update_manifest = mf if has_new else None
                    if has_new:
                        self._btn_apply_update.configure(state=tk.NORMAL)
                        messagebox.showinfo(
                            "Cập nhật",
                            (
                                f"Đã tìm thấy bản mới: {mf.version}\n"
                                f"Bản hiện tại: {local_v}\n\n"
                                f"Ghi chú: {mf.notes or '—'}"
                            ),
                            parent=self._root,
                        )
                    else:
                        self._btn_apply_update.configure(state=tk.DISABLED)
                        messagebox.showinfo(
                            "Cập nhật",
                            f"Bạn đang dùng bản mới nhất ({local_v}).",
                            parent=self._root,
                        )

                self._root.after(0, done_ok)
            except Exception as exc:  # noqa: BLE001
                err_text = str(exc)

                def done_err() -> None:
                    self._btn_check_updates.configure(state=tk.NORMAL)
                    self._btn_apply_update.configure(state=tk.DISABLED)
                    self._clear_ui_busy()
                    messagebox.showerror("Cập nhật", f"Kiểm tra bản mới thất bại:\n{err_text}", parent=self._root)

                self._root.after(0, done_err)

        threading.Thread(target=worker, name="check_updates", daemon=True).start()

    def _show_update_success_restart_dialog(self, *, version: str, backup_dir: Path) -> None:
        """
        Sau cập nhật thành công: nút mở lại chương trình ngay (khuyến nghị) + để sau.
        """
        top = tk.Toplevel(self._root)
        top.title("Cập nhật xong — mở lại chương trình")
        top.transient(self._root)
        top.resizable(False, False)
        try:
            top.grab_set()
        except Exception:
            pass
        fr = ttk.Frame(top, padding=16)
        fr.pack(fill=tk.BOTH, expand=True)
        msg = (
            f"Đã cập nhật lên phiên bản {version}.\n\n"
            f"Backup trước update:\n{backup_dir}\n\n"
            "Nên bấm «Mở lại chương trình ngay» để dùng bản mới (cửa sổ hiện tại sẽ đóng và app mở lại).\n"
            "Phím Enter = mở lại ngay. Esc = để sau."
        )
        ttk.Label(fr, text=msg, wraplength=480, justify=tk.LEFT).pack(anchor="w", pady=(0, 14))
        btn_row = ttk.Frame(fr)
        btn_row.pack(fill=tk.X)

        def do_restart() -> None:
            try:
                top.grab_release()
            except Exception:
                pass
            try:
                top.destroy()
            except Exception:
                pass
            relaunch_same_app_and_exit(cwd=project_root(), tk_root=self._root)

        def do_later() -> None:
            try:
                top.grab_release()
            except Exception:
                pass
            top.destroy()

        btn_restart = ttk.Button(
            btn_row,
            text="Mở lại chương trình ngay (khuyến nghị)",
            command=do_restart,
        )
        btn_restart.pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(btn_row, text="Để sau", command=do_later).pack(side=tk.LEFT)
        top.protocol("WM_DELETE_WINDOW", do_later)
        top.bind("<Return>", lambda _e: do_restart())
        top.bind("<Escape>", lambda _e: do_later())
        try:
            top.after(80, lambda: btn_restart.focus_set())
        except Exception:
            pass

    def _on_apply_update(self) -> None:
        """Tải và áp dụng bản mới đã check được từ manifest."""
        mf = self._latest_update_manifest
        if mf is None:
            messagebox.showwarning("Cập nhật", "Chưa có thông tin bản mới. Hãy bấm «Kiểm tra cập nhật» trước.", parent=self._root)
            return
        if not messagebox.askyesno(
            "Xác nhận cập nhật",
            (
                f"Cập nhật lên phiên bản {mf.version}?\n\n"
                "App sẽ backup trước khi cập nhật và yêu cầu khởi động lại sau khi hoàn tất."
            ),
            parent=self._root,
        ):
            return

        self._set_ui_busy("apply_update")
        self._btn_check_updates.configure(state=tk.DISABLED)
        self._btn_apply_update.configure(state=tk.DISABLED)
        self._lbl_state.configure(text="Update: đang tải & áp dụng…")

        def worker() -> None:
            try:
                backup_dir = apply_update_package(project_root=project_root(), manifest=mf)

                def done_ok() -> None:
                    self._lbl_state.configure(text="Update: hoàn tất — khởi động lại để dùng bản mới")
                    self._clear_ui_busy()
                    self._btn_check_updates.configure(state=tk.NORMAL)
                    self._btn_apply_update.configure(state=tk.DISABLED)
                    self._show_update_success_restart_dialog(version=str(mf.version), backup_dir=backup_dir)

                self._root.after(0, done_ok)
            except Exception as exc:  # noqa: BLE001
                err_text = str(exc)

                def done_err() -> None:
                    self._btn_check_updates.configure(state=tk.NORMAL)
                    self._btn_apply_update.configure(state=tk.NORMAL if self._latest_update_manifest else tk.DISABLED)
                    self._lbl_state.configure(text="Update: lỗi")
                    self._clear_ui_busy()
                    messagebox.showerror("Cập nhật", f"Cập nhật thất bại:\n{err_text}", parent=self._root)

                self._root.after(0, done_err)

        threading.Thread(target=worker, name="apply_update", daemon=True).start()

    def _on_start(self) -> None:
        """
        Khởi chạy thread nền gọi ``run_forever`` với ``stop_event``.
        """
        if self._worker is not None and self._worker.is_alive():
            messagebox.showinfo("Lịch", "Scheduler đã đang chạy.")
            return
        os.environ["HEADLESS"] = "0" if self._show_browser else "1"
        self._stop_event = threading.Event()

        def runner() -> None:
            try:
                run_forever(accounts=self._accounts, stop_event=self._stop_event)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Luồng scheduler kết thúc lỗi: {}", exc)

        self._worker = threading.Thread(target=runner, name="fb_scheduler", daemon=True)
        self._worker.start()
        self._btn_start.configure(state=tk.DISABLED)
        self._btn_stop.configure(state=tk.NORMAL)
        self._lbl_state.configure(text="Lịch: đang chạy")
        logger.info("Đã bật lịch (scheduler trong thread nền, HEADLESS={}).", os.environ.get("HEADLESS", "1"))

    def _set_browser_visibility(self, show: bool, *, update_env: bool = True) -> None:
        self._show_browser = bool(show)
        if update_env:
            os.environ["HEADLESS"] = "0" if self._show_browser else "1"
        mode = "Browser: Hiện (quan sát trực quan)" if self._show_browser else "Browser: Ẩn (chạy nền)"
        self._lbl_browser_mode.configure(text=mode)
        if self._show_browser:
            self._btn_show_browser.configure(state=tk.DISABLED)
            self._btn_hide_browser.configure(state=tk.NORMAL)
        else:
            self._btn_show_browser.configure(state=tk.NORMAL)
            self._btn_hide_browser.configure(state=tk.DISABLED)

    def _apply_multi_page_compact_preset(self) -> None:
        """
        Preset chạy ổn định cho automation: desktop www + viewport cố định 1280x900.
        """
        vp_w, vp_h = 1280, 900

        os.environ["FB_MOBILE_MODE"] = "0"
        os.environ["FB_MOBILE_AUTO_VIEWPORT"] = "0"
        os.environ["FB_PREFER_M_FACEBOOK"] = "0"
        os.environ["FB_AUTO_MOBILE_WEB_WHEN_NARROW"] = "0"
        os.environ["FB_VIEWPORT_WIDTH"] = str(vp_w)
        os.environ["FB_VIEWPORT_HEIGHT"] = str(vp_h)
        os.environ.pop("TOOLFB_NAV_MOBILE_FB", None)
        for k in ("FB_MOBILE_DEVICE",):
            if k in os.environ:
                del os.environ[k]
        os.environ["PLAYWRIGHT_LOCALE"] = "en-US"
        messagebox.showinfo(
            "Đã áp preset",
            "Đã bật preset multi-page compact:\n"
            f"- FB_MOBILE_MODE=0 (www.facebook — trình duyệt bình thường)\n"
            f"- Viewport automation cố định {vp_w}x{vp_h}\n"
            "- FB_AUTO_MOBILE_WEB_WHEN_NARROW=0 (không tự rơi vào m.facebook)\n"
            "- FB_PREFER_M_FACEBOOK=0\n"
            "- Locale=en-US\n"
            "Đổi kích thước: FB_VIEWPORT_WIDTH / FB_VIEWPORT_HEIGHT.",
            parent=self._root,
        )
        logger.info(
            "Đã áp preset multi-page compact (desktop www, viewport {}x{}).",
            vp_w,
            vp_h,
        )

    def _on_stop(self) -> None:
        """
        Báo hiệu dừng scheduler theo kiểu non-blocking (không chặn UI thread).
        """
        self._set_ui_busy("stop_scheduler")
        if self._stop_event is not None:
            self._stop_event.set()
        worker = self._worker
        self._worker = None
        self._stop_event = None
        self._btn_start.configure(state=tk.NORMAL)
        self._btn_stop.configure(state=tk.DISABLED)
        if worker is None:
            self._lbl_state.configure(text="Lịch: đang tắt")
            logger.info("Đã gửi lệnh dừng lịch.")
            self._clear_ui_busy()
            return

        # Không join ở đây để tránh treo UI khi worker đang bận I/O.
        self._lbl_state.configure(text="Lịch: đang dừng…")

        def _poll_stop() -> None:
            try:
                if worker.is_alive():
                    self._root.after(120, _poll_stop)
                    return
                self._lbl_state.configure(text="Lịch: đang tắt")
                logger.info("Scheduler worker đã dừng.")
                self._clear_ui_busy()
            except Exception:  # noqa: BLE001
                # Dù polling lỗi vẫn ưu tiên giữ UI responsive.
                self._lbl_state.configure(text="Lịch: đang tắt")
                self._clear_ui_busy()

        self._root.after(120, _poll_stop)
        logger.info("Đã gửi lệnh dừng lịch (non-blocking).")

    def _on_close(self) -> None:
        """
        Đóng ứng dụng nhanh, tránh block UI gây ``Not Responding``.

        Chiến lược:
        - Chỉ phát tín hiệu dừng cho scheduler/manual-profile threads.
        - Không ``join`` lâu trên main thread Tk.
        - Gỡ log sink và hủy cửa sổ ngay.
        """
        self._set_ui_busy("close_app")
        try:
            if self._worker is not None and self._worker.is_alive() and self._stop_event is not None:
                self._stop_event.set()
        except Exception:  # noqa: BLE001
            pass
        for sess in list(self._manual_profile_sessions):
            try:
                sess["shutdown"].set()
            except Exception:  # noqa: BLE001
                pass
        self._manual_profile_sessions.clear()
        self._stop_ui_watchdog()
        self._detach_log_sink()
        try:
            self._root.after(0, self._root.destroy)
        except Exception:  # noqa: BLE001
            self._root.destroy()
