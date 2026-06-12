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
import uuid
from datetime import datetime, timedelta, timezone
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
from src.gui.pages_export_dialog import PagesExportDialog
from src.gui.schedule_job_dialog import SchedulePostJobDialog
from src.gui.ui_responsiveness import (
    ASYNC_PREP_MIN_ROWS,
    DEFAULT_TREE_CHUNK,
    run_background_then_main,
    tree_delete_all,
    tree_insert_chunked,
)
from src.modules.browser_engine import BrowserEngine
from src.services.app_updater import (
    TOOLFB_PUBLIC_REPO,
    GitUpdateCheckResult,
    UpdateManifest,
    apply_git_pull_ff,
    apply_update_package,
    check_git_updates,
    github_latest_manifest_url,
    is_newer_version,
    maybe_auto_git_pull_on_startup,
    prefer_repo_raw_manifest_url,
    read_local_version,
    read_manifest_from_url,
    read_remote_version_from_github_raw,
    resolve_github_owner_repo_for_version_check,
    resolve_manifest_url,
    should_use_git_updates,
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
from src.utils.app_restart import DEFERRED_GUI_BAT_NAME, relaunch_same_app_and_exit
from src.utils.db_manager import AccountRecord, AccountsDatabaseManager
from src.utils.pages_manager import PageRecord, PagesManager
from src.utils.page_insights_format import format_metric
from src.utils.page_insights_policy import PageInsightsPolicy, plan_page_insights_fetch
from src.utils.page_insights_store import PageInsightsPeriod, PageInsightsStore
from src.utils.page_schedule import parse_date_only_yyyy_mm_dd, scheduler_tz
from src.utils.schedule_batch_preview import build_schedule_by_daily_slots, page_post_style_for_post_type
from src.utils.schedule_job_content import build_schedule_slot_hhmm, internal_post_title_from_body
from src.utils.schedule_posts_filters import (
    format_account_filter_label,
    format_page_filter_label,
    split_hashtags_csv,
    apply_job_filters,
    is_overdue,
    sort_jobs,
)
from src.utils.schedule_posts_manager import get_default_schedule_posts_manager
from src.utils.schedule_posts_missing_fields import (
    clear_file_exists_cache,
    MISSING_FIELD_LABELS,
    filter_jobs_by_missing_fields,
    format_missing_fields_for_display,
    get_missing_fields,
    preset_by_label,
)
from src.utils.browser_exe_discover import find_browser_exe_in_directory
from src.utils.github_repo_detect import github_owner_repo_from_git
from src.utils.paths import project_root
from src.services.tiktok.account_manager import TikTokAccountStore
from src.services.tiktok.job_manager import TikTokJobStore, default_job_dict
from src.services.universal_video_downloader import ensure_downloader_layout
from src.services.video_editor.layout import video_editor_schedule_jobs_json_path
from src.gui.treeview_shortcuts import install_treeview_shortcuts
from src.gui.tiktok_manager_tab import build_tiktok_manager_tab
from src.gui.human_interaction_tab import build_human_interaction_tab
from src.gui.video_editor_tab import build_video_editor_tab
from src.utils.proxy_check import check_proxy

_VE_PENDING_SORT_LABEL_TO_COL: dict[str, str] = {
    "Tạo lúc": "created",
    "Tên job": "job_name",
    "id": "id",
    "Đích gợi ý": "target",
    "Số video": "n",
    "Trạng thái": "status",
}
_VE_PENDING_COL_TO_SORT_LABEL: dict[str, str] = {v: k for k, v in _VE_PENDING_SORT_LABEL_TO_COL.items()}


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
    from src.utils.playwright_browser_lock import (
        enforce_bundled_browser_policy,
        format_browser_status_lines,
    )

    proot = project_root()
    ok, msgs = enforce_bundled_browser_policy(project_root=proot)
    for line in format_browser_status_lines(project_root=proot):
        logger.info("Playwright | {}", line)
    for m in msgs:
        if ok:
            logger.warning("Playwright | {}", m)
        else:
            logger.error("Playwright | {}", m)


def run_manager_gui(*, accounts: AccountsDatabaseManager) -> None:
    """
    Mở cửa sổ quản lý và chạy vòng lặp ``mainloop`` Tk.

    Args:
        accounts: ``AccountsDatabaseManager`` đã preflight (dùng chung cho scheduler).
    """
    _log_playwright_runtime_paths()
    if getattr(sys, "frozen", False):
        from src.utils.playwright_browser_lock import (
            enforce_bundled_browser_policy,
            validate_browser_bundle,
        )

        ok, msgs = enforce_bundled_browser_policy(project_root=project_root())
        if not ok and msgs:
            try:
                import tkinter as _tk
                from tkinter import messagebox as _mb

                _r = _tk.Tk()
                _r.withdraw()
                _mb.showwarning(
                    "Trình duyệt không khớp bản cài",
                    "Máy này cần cùng gói trình duyệt với máy chính:\n\n"
                    + "\n".join(msgs[:8])
                    + (
                        f"\n\n… và {len(msgs) - 8} lỗi khác."
                        if len(msgs) > 8
                        else ""
                    )
                    + "\n\n→ Tải bản zip release ĐẦY ĐỦ (có _internal/ms-playwright). "
                    "Không chạy «playwright install» riêng.",
                    parent=_r,
                )
                _r.destroy()
            except Exception:
                pass
        elif ok:
            val = validate_browser_bundle(project_root=project_root())
            if val:
                logger.warning("Playwright validate: {}", "; ".join(val[:3]))
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
        from src.utils.concurrency_runtime import apply_multi_task_defaults

        apply_multi_task_defaults(gui=True)
        self._multitask_reconcile_after_id: str | None = None
        self._accounts = accounts
        self._pages = PagesManager()
        self._page_insights = PageInsightsStore()
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
        try:
            sw = int(self._root.winfo_screenwidth() or 1280)
            sh = int(self._root.winfo_screenheight() or 800)
            ww = min(1280, max(860, sw - 48))
            wh = min(900, max(560, sh - 72))
            self._root.geometry(f"{ww}x{wh}+{(sw - ww) // 2}+{(sh - wh) // 2}")
        except tk.TclError:
            pass
        self._compact_ui = int(self._root.winfo_screenheight() or 900) <= 820
        self._tree_rows_main = 9 if self._compact_ui else 11
        self._tree_rows_jobs = 8 if self._compact_ui else 10
        self._tree_rows_small = 5 if self._compact_ui else 7
        self._log_rows = 7 if self._compact_ui else 10
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
        self._job_account_name_by_id: dict[str, str] = {}
        self._jobs_filter_account_label_to_id: dict[str, str] = {}
        self._jobs_filter_page_label_to_id: dict[str, str] = {}
        self._jobs_sort_key: str = "scheduled_at"
        self._jobs_sort_asc: bool = True
        self._jobs_search_after_id: str | None = None
        self._jobs_tree_render_gen: int = 0
        self._schedule_jobs_load_busy: bool = False
        self._accounts_tree_gen: int = 0
        self._accounts_load_busy: bool = False
        self._pages_tree_gen: int = 0
        self._pages_load_busy: bool = False
        self._var_ve_pending_search = tk.StringVar(value="")
        self._ve_pending_sort_col: str = "created"
        self._ve_pending_sort_asc: bool = False
        self._var_ve_pending_job = tk.StringVar(value="")
        self._ve_pending_job_by_label: dict[str, dict[str, Any]] = {}
        self._ve_pending_selected_id: str = ""
        self._ve_pending_meta_section_open: dict[str, bool] = {}
        self._suppress_ve_pending_job_cb: dict[str, bool] = {"v": False}
        self._suppress_ve_pending_sort_ui: dict[str, bool] = {"v": False}
        self._var_ve_pending_sort_ui = tk.StringVar(value="Tạo lúc")
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
        self._git_update_result: GitUpdateCheckResult | None = None
        # Watchdog UI: phát hiện main-thread bị block (dễ gây "Not Responding").
        self._ui_watchdog_interval_ms = 250
        self._ui_watchdog_threshold_sec = 1.5
        self._ui_watchdog_last_tick = time.monotonic()
        self._ui_watchdog_after_id: str | None = None
        self._ui_busy_label: str = ""
        self._schedule_ve_background_fill: Callable[[], None] | None = None
        self._tab_ve_notebook_child: tk.Misc | None = None
        self._tab_dl_notebook_child: tk.Misc | None = None
        self._embedded_download_panel: Any | None = None

        main = ttk.Frame(self._root, padding=(6 if self._compact_ui else 8))
        main.pack(fill=tk.BOTH, expand=True)

        title_fr = ttk.Frame(main)
        title_fr.pack(fill=tk.X, pady=(0, 4 if self._compact_ui else 6))
        ttk.Label(
            title_fr,
            text="Facebook Automation — Lịch + Tài khoản + Page/Group",
            font=("Segoe UI", 11 if self._compact_ui else 12, "bold"),
        ).pack(anchor="w")
        hint_row = ttk.Frame(title_fr)
        hint_row.pack(fill=tk.X, pady=(2, 0))
        self._setup_banner = hint_row
        self._lbl_setup_hint = ttk.Label(
            hint_row,
            text="",
            wraplength=920 if not self._compact_ui else 720,
            justify=tk.LEFT,
            foreground="#5c4b00",
            font=("Segoe UI", 9),
        )
        self._lbl_setup_hint.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(hint_row, text="Hướng dẫn", command=self._on_setup_guide, width=10).pack(
            side=tk.RIGHT, padx=(6, 0)
        )
        hint_row.pack_forget()

        bar = ttk.Frame(main)
        bar.pack(fill=tk.X, pady=(0, 6))
        bar.columnconfigure(0, weight=1)
        bar.columnconfigure(1, weight=0)

        bar_rows = ttk.Frame(bar)
        bar_rows.grid(row=0, column=0, sticky="ew")
        bar_status = ttk.Frame(bar)
        bar_status.grid(row=0, column=1, sticky="ne", padx=(8, 0))

        row0 = ttk.Frame(bar_rows)
        row0.pack(fill=tk.X, anchor="w")
        row1 = ttk.Frame(bar_rows)
        row1.pack(fill=tk.X, anchor="w", pady=(4, 0))

        # --- Hàng 1: lịch + làm mới + browser + chế độ giao diện (luôn thấy khi thu cửa sổ) ---
        self._btn_start = ttk.Button(row0, text="Bắt đầu lịch", command=self._on_start)
        self._btn_start.pack(side=tk.LEFT, padx=(0, 4))
        self._btn_stop = ttk.Button(row0, text="Dừng lịch", command=self._on_stop, state=tk.DISABLED)
        self._btn_stop.pack(side=tk.LEFT, padx=(0, 4))
        self._btn_refresh = ttk.Button(
            row0,
            text="Làm mới" if self._compact_ui else "Làm mới tất cả",
            command=self._refresh_all,
        )
        self._btn_refresh.pack(side=tk.LEFT, padx=(0, 4))
        self._btn_show_browser = ttk.Button(row0, text="Hiện browser", command=lambda: self._set_browser_visibility(True))
        self._btn_show_browser.pack(side=tk.LEFT, padx=(8, 4))
        self._btn_hide_browser = ttk.Button(row0, text="Ẩn browser", command=lambda: self._set_browser_visibility(False))
        self._btn_hide_browser.pack(side=tk.LEFT, padx=(0, 4))
        ttk.Separator(row0, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=6)
        ttk.Label(row0, text="Giao diện:").pack(side=tk.LEFT, padx=(0, 4))
        self._platform_view_var = tk.StringVar(value="TikTok")
        self._cb_platform_view = ttk.Combobox(
            row0,
            state="readonly",
            width=10,
            values=("Facebook", "TikTok"),
            textvariable=self._platform_view_var,
        )
        self._cb_platform_view.pack(side=tk.LEFT, padx=(0, 4))
        self._cb_platform_view.bind(
            "<<ComboboxSelected>>",
            lambda _e: self._apply_platform_view(self._platform_view_var.get()),
        )

        # --- Hàng 2: dữ liệu / preset / cập nhật / công cụ (tách khỏi hàng lịch để kéo ngang không chồng nút) ---
        self._btn_migrate = ttk.Button(
            row1,
            text="Migrate dữ liệu",
            command=self._on_migrate_user_data,
        )
        self._btn_migrate.pack(side=tk.LEFT, padx=(0, 4))
        ttk.Separator(row1, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=6)
        self._btn_compact_multi = ttk.Button(
            row1,
            text="Preset multi-page",
            command=self._apply_multi_page_compact_preset,
        )
        self._btn_compact_multi.pack(side=tk.LEFT, padx=(0, 4))
        self._btn_setup_help = ttk.Button(row1, text="?", width=3, command=self._on_setup_guide)
        self._btn_setup_help.pack(side=tk.LEFT, padx=(0, 4))
        self._btn_check_updates = ttk.Button(row1, text="Chỉ kiểm tra", command=self._on_check_updates)
        self._btn_check_updates.pack(side=tk.LEFT, padx=(0, 4))
        self._btn_apply_update = ttk.Button(row1, text="Cập nhật", command=self._on_apply_update)
        self._apply_update_pack_after = self._btn_check_updates
        self._btn_update_channel = ttk.Button(
            row1,
            text="Kênh cập nhật",
            command=self._on_configure_update_channel,
        )
        self._btn_update_channel.pack(side=tk.LEFT, padx=(0, 4))
        self._btn_reset_veo3_profile = ttk.Button(
            row1,
            text="Reset VEO3",
            command=self._on_reset_veo3_profiles,
        )
        self._btn_reset_veo3_profile.pack(side=tk.LEFT, padx=(0, 4))
        self._btn_ai_video = ttk.Button(row1, text="AI Video (Gemini/Veo)", command=self._on_open_ai_video_dialog)
        self._btn_ai_video.pack(side=tk.LEFT, padx=(0, 4))
        ttk.Separator(row1, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=6)
        self._lbl_browser_mode = ttk.Label(row1, text="", wraplength=260, justify=tk.LEFT)
        self._lbl_browser_mode.pack(side=tk.LEFT, padx=(0, 8))

        self._lbl_state = ttk.Label(bar_status, text="Lịch: đang tắt")
        self._lbl_state.pack(anchor="e")
        self._lbl_app_version = ttk.Label(
            bar_status,
            text=f"Phiên bản {self._app_version_str}",
            foreground="gray",
        )
        self._lbl_app_version.pack(anchor="e", pady=(4, 0))
        self._lbl_multitask = ttk.Label(
            bar_status,
            text="",
            foreground="gray",
            font=("Segoe UI", 8),
            wraplength=280,
            justify=tk.RIGHT,
        )
        self._lbl_multitask.pack(anchor="e", pady=(2, 0))
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
            wraplength=640,
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
            acc_tree_fr, columns=cols_acc, show="headings", height=self._tree_rows_main, selectmode="extended"
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
        install_treeview_shortcuts(
            self._tree_accounts,
            owner=self._root,
            enable_context_menu=False,
            info_callback=lambda msg: logger.info(msg),
        )
        sy_acc.grid(row=0, column=1, sticky="ns")

        tab_pg = ttk.Frame(nb, padding=4)
        nb.add(tab_pg, text="  2. Page / Group (pages.json)  ")
        tab_pg.columnconfigure(0, weight=1)
        tab_pg.rowconfigure(3, weight=1)
        ttk.Label(
            tab_pg,
            text="Thêm / sửa Page (URL, owner…). Thống kê: tối đa vài Page/lần, cache 12h (tránh checkpoint) — "
            "chỉ bật «Bỏ qua cache» khi cần. Cần fb_page_id + đăng nhập. Lịch/AI: tab «3. Job lịch đăng».",
            font=("Segoe UI", 9),
            wraplength=640,
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
        ttk.Button(pg_bar, text="Xuất CSV…", command=self._on_export_pages_csv).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(pg_bar, text="Dọn trùng Meta ID", command=self._on_dedupe_pages_by_meta_id).pack(
            side=tk.LEFT, padx=(0, 4)
        )
        ttk.Label(pg_bar, text="Thống kê:").pack(side=tk.LEFT, padx=(8, 2))
        self._var_pages_insights_period = tk.StringVar(value="7 ngày")
        self._cb_pages_insights_period = ttk.Combobox(
            pg_bar,
            textvariable=self._var_pages_insights_period,
            state="readonly",
            width=14,
            values=("7 ngày", "28 ngày (tháng)"),
        )
        self._cb_pages_insights_period.pack(side=tk.LEFT, padx=(0, 4))
        self._cb_pages_insights_period.bind("<<ComboboxSelected>>", lambda _e: self._render_pages_tree())
        self._var_pages_insights_force = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            pg_bar,
            text="Bỏ qua cache",
            variable=self._var_pages_insights_force,
        ).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(pg_bar, text="Lấy thống kê (đã chọn)", command=self._on_fetch_page_insights_selected).pack(
            side=tk.LEFT, padx=(0, 4)
        )
        ttk.Button(pg_bar, text="Lấy thống kê (đã lọc)", command=self._on_fetch_page_insights_filtered).pack(
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
            "followers",
            "views",
            "stats_at",
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
            height=self._tree_rows_main,
            selectmode="extended",
        )
        headings_pg = {
            "id": "id",
            "account_id": "owner",
            "page_kind": "Loại",
            "page_name": "Tên Page",
            "followers": "Followers",
            "views": "Views",
            "stats_at": "Cập nhật TK",
            "ai_topic": "Chủ đề AI",
            "post_style": "post_style",
            "schedule": "Lịch",
            "status": "Trạng thái",
            "last_post": "Đăng gần nhất",
            "fb_page_id": "Meta Page ID",
            "url": "Page_URL",
        }
        widths_pg = (72, 72, 56, 88, 72, 72, 88, 100, 56, 52, 72, 88, 110, 140)
        for c, w in zip(cols_pg, widths_pg):
            self._tree_pages.heading(c, text=headings_pg[c], command=lambda k=c: self._on_pages_sort_click(k))
            self._tree_pages.column(c, width=w, stretch=True)
        sy_pg = ttk.Scrollbar(pg_tree_fr, orient=tk.VERTICAL, command=self._tree_pages.yview)
        self._tree_pages.configure(yscrollcommand=sy_pg.set)
        self._tree_pages.grid(row=0, column=0, sticky="nsew")
        self._tree_pages.bind("<Double-1>", lambda _e: self._on_edit_page())
        install_treeview_shortcuts(self._tree_pages, owner=self._root, info_callback=lambda msg: logger.info(msg))
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
            wraplength=640,
        ).grid(row=0, column=0, sticky="ew", pady=(0, 4))
        jb = ttk.Frame(tab_jobs)
        jb.grid(row=1, column=0, sticky="ew", pady=(0, 4))
        ttk.Button(jb, text="Thêm job", command=self._on_add_schedule_job).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(jb, text="Thêm batch job…", command=self._on_add_batch_schedule_job).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(jb, text="Nạp job chờ đăng từ Export…", command=self._on_import_saved_export_job).pack(
            side=tk.LEFT, padx=(0, 4)
        )
        ttk.Button(jb, text="Sửa job", command=self._on_edit_schedule_job).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(jb, text="Sửa nội dung hàng loạt…", command=self._on_jobs_bulk_edit_content).pack(
            side=tk.LEFT, padx=(0, 4)
        )
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
            "last_error",
            "retry",
            "missing",
        )
        self._tree_jobs = ttk.Treeview(
            j_tree_fr, columns=cols_j, show="headings", height=self._tree_rows_jobs, selectmode="extended"
        )
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
            "last_error": "Lỗi gần nhất",
            "retry": "retry",
            "missing": "Thiếu field",
        }
        widths_j = (120, 72, 72, 88, 78, 120, 240, 160, 88, 220, 44, 160)
        for c, w in zip(cols_j, widths_j):
            self._tree_jobs.heading(c, text=heads_j[c])
            self._tree_jobs.column(
                c,
                width=w,
                stretch=c in ("title", "image_prompt", "scheduled_at", "last_error", "missing"),
            )
        sy_j = ttk.Scrollbar(j_tree_fr, orient=tk.VERTICAL, command=self._tree_jobs.yview)
        self._tree_jobs.configure(yscrollcommand=sy_j.set)
        self._tree_jobs.grid(row=0, column=0, sticky="nsew")
        self._tree_jobs.bind("<Double-1>", lambda _e: self._on_edit_schedule_job())
        self._tree_jobs.bind("<<TreeviewSelect>>", lambda _e: self._update_schedule_jobs_stats_label())
        install_treeview_shortcuts(self._tree_jobs, owner=self._root, info_callback=lambda msg: logger.info(msg))
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
            wraplength=620,
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
            gtree_fr, columns=gcols, show="headings", height=self._tree_rows_small, selectmode="browse"
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
        install_treeview_shortcuts(self._tree_gemini, owner=self._root, info_callback=lambda msg: logger.info(msg))

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
        self._tree_openai = ttk.Treeview(
            otree_fr, columns=ocols, show="headings", height=self._tree_rows_small, selectmode="browse"
        )
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
        install_treeview_shortcuts(self._tree_openai, owner=self._root, info_callback=lambda msg: logger.info(msg))

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
        self._tree_nanobanana = ttk.Treeview(
            nbtree_fr, columns=nbcols, show="headings", height=self._tree_rows_small, selectmode="browse"
        )
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
        install_treeview_shortcuts(self._tree_nanobanana, owner=self._root, info_callback=lambda msg: logger.info(msg))

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
        self._root.bind("<<OpenScheduleJobsTab>>", self._on_open_schedule_jobs_tab_event, add="+")

        tab_ve = ttk.Frame(nb, padding=4)
        nb.add(tab_ve, text="  5. Video Editor  ")
        tab_ve.columnconfigure(0, weight=1)
        tab_ve.rowconfigure(0, weight=1)
        ve_host = ttk.Frame(tab_ve)
        ve_host.grid(row=0, column=0, sticky="nsew")
        ve_host.columnconfigure(0, weight=1)
        ve_host.rowconfigure(0, weight=1)
        (
            self._schedule_ve_background_fill,
            self._shutdown_video_editor_tab,
            self._refresh_ve_download_jobs,
        ) = build_video_editor_tab(ve_host, self._root)
        self._tab_ve_notebook_child = tab_ve

        tab_dl = ttk.Frame(nb, padding=4)
        nb.add(tab_dl, text="  6. Tải Video  ")
        tab_dl.columnconfigure(0, weight=1)
        tab_dl.rowconfigure(0, weight=1)
        self._tab_dl_notebook_child = tab_dl
        try:
            from src.gui.ai_video_dialog import AIVideoDialog

            self._embedded_download_panel = AIVideoDialog(
                self._root,
                project_spec={},
                start_tab="download",
                embedded_download_host=tab_dl,
            )
        except Exception as exc:  # noqa: BLE001
            self._embedded_download_panel = None
            ttk.Label(tab_dl, text="Không khởi tạo được module tải video.", foreground="#b00020").grid(
                row=0, column=0, sticky="w"
            )
            ttk.Label(tab_dl, text=str(exc), foreground="#555", wraplength=620, justify="left").grid(
                row=1, column=0, sticky="w", pady=(6, 0)
            )

        tab_ve_pending = ttk.Frame(nb, padding=4)
        nb.add(tab_ve_pending, text="  7.Job chờ đăng từ Video Editor  ")
        tab_ve_pending.columnconfigure(0, weight=1)
        tab_ve_pending.rowconfigure(3, weight=1)
        ttk.Label(
            tab_ve_pending,
            text="Job chờ đăng từ Video Editor (file video_editor_schedule_jobs.json)",
            font=("Segoe UI", 10, "bold"),
        ).grid(row=0, column=0, sticky="w", pady=(0, 4))
        ve_top_pe = ttk.Frame(tab_ve_pending)
        ve_top_pe.grid(row=1, column=0, sticky="ew", pady=(0, 4))
        ve_top_pe.columnconfigure(0, weight=1)
        ttk.Label(
            ve_top_pe,
            text=(
                "Chọn job trong ô xổ bên dưới — phần «Chi tiết job» hiện metadata; bảng cuối liệt kê từng video "
                "(tiêu đề, nội dung, hashtag, đường dẫn file…). Double‑click bảng video để mở «Nạp từ Export…»."
            ),
            foreground="#555",
            font=("Segoe UI", 8),
            wraplength=720,
            justify="left",
        ).grid(row=0, column=0, sticky="w")
        ve_pe_btns = ttk.Frame(ve_top_pe)
        ve_pe_btns.grid(row=0, column=1, sticky="e", padx=(8, 0))
        ttk.Button(ve_pe_btns, text="Làm mới", command=self._fill_ve_pending_export_jobs_tree).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(ve_pe_btns, text="Nạp từ Export…", command=self._on_import_saved_export_job).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(
            ve_pe_btns,
            text="Mở folder chứa file job chờ…",
            command=self._on_open_ve_pending_export_jobs_folder,
        ).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(ve_pe_btns, text="Xóa job chọn", command=self._on_delete_ve_pending_export_jobs).pack(
            side=tk.LEFT, padx=(6, 0)
        )

        ve_filter_pe = ttk.Frame(tab_ve_pending)
        ve_filter_pe.grid(row=2, column=0, sticky="ew", pady=(0, 4))
        ve_filter_pe.columnconfigure(1, weight=1)
        ttk.Label(ve_filter_pe, text="Tìm job").grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Entry(ve_filter_pe, textvariable=self._var_ve_pending_search, width=36).grid(
            row=0, column=1, sticky="ew", padx=(0, 8)
        )
        ttk.Button(ve_filter_pe, text="Xóa lọc", command=lambda: self._var_ve_pending_search.set("")).grid(
            row=0, column=2, sticky="w", padx=(0, 10)
        )
        ttk.Label(ve_filter_pe, text="Sắp xếp job").grid(row=0, column=3, sticky="w", padx=(0, 6))
        self._cb_ve_pending_sort_field = ttk.Combobox(
            ve_filter_pe,
            textvariable=self._var_ve_pending_sort_ui,
            values=("Tạo lúc", "Tên job", "id", "Đích gợi ý", "Số video", "Trạng thái"),
            state="readonly",
            width=14,
        )
        self._cb_ve_pending_sort_field.grid(row=0, column=4, sticky="w", padx=(0, 6))
        self._cb_ve_pending_sort_field.bind("<<ComboboxSelected>>", self._on_ve_pending_sort_field_changed)
        ttk.Button(ve_filter_pe, text="Đảo chiều ↑/↓", command=self._on_ve_pending_sort_dir_toggle, width=12).grid(
            row=0, column=5, sticky="w"
        )
        ttk.Label(
            ve_filter_pe,
            text="Lọc theo id / tên / đích / số video / thời gian / trạng thái — danh sách trong ô «Chọn job».",
            foreground="#666",
            font=("Segoe UI", 8),
        ).grid(row=1, column=0, columnspan=6, sticky="w", pady=(4, 0))

        lf_job = ttk.LabelFrame(tab_ve_pending, text="Chi tiết job đang chọn", padding=(8, 6))
        lf_job.grid(row=3, column=0, sticky="nsew", pady=(4, 0))
        lf_job.columnconfigure(1, weight=1)
        lf_job.rowconfigure(1, weight=0)
        lf_job.rowconfigure(3, weight=1)
        ttk.Label(lf_job, text="Chọn job").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 6))
        self._cb_ve_pending_jobs = ttk.Combobox(
            lf_job,
            textvariable=self._var_ve_pending_job,
            state="readonly",
            width=72,
        )
        self._cb_ve_pending_jobs.grid(row=0, column=1, columnspan=2, sticky="ew", pady=(0, 6))
        self._cb_ve_pending_jobs.bind("<<ComboboxSelected>>", self._on_ve_pending_job_combo_selected)
        self._frm_ve_pending_job_meta = ttk.Frame(lf_job)
        self._frm_ve_pending_job_meta.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(0, 8))
        self._frm_ve_pending_job_meta.columnconfigure(0, weight=1)
        ttk.Label(lf_job, text="Từng video trong job", font=("Segoe UI", 9, "bold")).grid(
            row=2, column=0, columnspan=3, sticky="w", pady=(0, 4)
        )
        vf = ttk.Frame(lf_job)
        vf.grid(row=3, column=0, columnspan=3, sticky="nsew")
        vf.columnconfigure(0, weight=1)
        vf.rowconfigure(1, weight=1)
        ve_tb = ttk.Frame(vf)
        ve_tb.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        ttk.Button(ve_tb, text="Thêm video (chọn nhiều)…", command=self._on_ve_pending_add_video_files).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(ve_tb, text="Gán đích đăng cho dòng đã chọn…", command=self._on_ve_pending_assign_dest_for_selection).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Label(
            ve_tb,
            text="Ctrl/Shift + click để chọn nhiều dòng. Double-click «Đích clip» để sửa. Kéo viền tiêu đề cột để rộng/hẹp cột.",
            foreground="#666",
            font=("Segoe UI", 8),
        ).pack(side=tk.LEFT, padx=(8, 0))
        vf_inner = ttk.Frame(vf)
        vf_inner.grid(row=1, column=0, sticky="nsew")
        vf_inner.columnconfigure(0, weight=1)
        vf_inner.rowconfigure(0, weight=1)
        cols_v = ("stt", "title", "content", "hashtags", "path", "src_id", "clip_status", "sched_st", "dest")
        self._tree_ve_pending_videos = ttk.Treeview(
            vf_inner,
            columns=cols_v,
            show="headings",
            height=max(6, self._tree_rows_jobs - 2),
            selectmode="extended",
        )
        heads_v = {
            "stt": "#",
            "title": "Tiêu đề",
            "content": "Nội dung",
            "hashtags": "Hashtag",
            "path": "File video",
            "src_id": "ID nguồn tải",
            "clip_status": "Trạng thái file",
            "sched_st": "Trạng thái lịch",
            "dest": "Đích clip",
        }
        widths_mw_stretch_anchor = (
            ("stt", 36, 28, False, "center"),
            ("title", 120, 48, True, "w"),
            ("content", 180, 48, True, "w"),
            ("hashtags", 100, 40, True, "w"),
            ("path", 160, 80, True, "w"),
            ("src_id", 90, 48, False, "w"),
            ("clip_status", 90, 48, False, "center"),
            ("sched_st", 130, 72, True, "w"),
            ("dest", 320, 120, True, "w"),
        )
        for c, w, mw, st, anc in widths_mw_stretch_anchor:
            self._tree_ve_pending_videos.heading(c, text=heads_v[c])
            self._tree_ve_pending_videos.column(c, width=w, minwidth=mw, stretch=st, anchor=anc)
        sy_v = ttk.Scrollbar(vf_inner, orient=tk.VERTICAL, command=self._tree_ve_pending_videos.yview)
        sx_v = ttk.Scrollbar(vf_inner, orient=tk.HORIZONTAL, command=self._tree_ve_pending_videos.xview)
        self._tree_ve_pending_videos.configure(
            yscrollcommand=sy_v.set,
            xscrollcommand=sx_v.set,
        )
        self._tree_ve_pending_videos.grid(row=0, column=0, sticky="nsew")
        sy_v.grid(row=0, column=1, sticky="ns")
        sx_v.grid(row=1, column=0, columnspan=2, sticky="ew")
        self._tree_ve_pending_videos.bind("<Double-1>", self._on_ve_pending_videos_double_click)
        install_treeview_shortcuts(
            self._tree_ve_pending_videos, owner=self._root, info_callback=lambda msg: logger.info(msg)
        )
        self._var_ve_pending_search.trace_add("write", lambda *_: self._fill_ve_pending_export_jobs_tree())

        tab_tt = ttk.Frame(nb, padding=4)
        nb.add(tab_tt, text="  8. TikTok Manager  ")
        tab_tt.columnconfigure(0, weight=1)
        tab_tt.rowconfigure(0, weight=1)
        tt_host = ttk.Frame(tab_tt)
        tt_host.grid(row=0, column=0, sticky="nsew")
        tt_host.columnconfigure(0, weight=1)
        # TikTok Manager: hàng 0 = nhãn mô tả (chiều cao tự nhiên), hàng 1 = Notebook — phải co giãn ở hàng 1.
        tt_host.rowconfigure(0, weight=0)
        tt_host.rowconfigure(1, weight=1)
        build_tiktok_manager_tab(tt_host, self._root)

        tab_human = ttk.Frame(nb, padding=4)
        nb.add(tab_human, text="  9. Tương tác người dùng  ")
        tab_human.columnconfigure(0, weight=1)
        tab_human.rowconfigure(0, weight=1)
        build_human_interaction_tab(tab_human, self._root)

        # --- Platform view (Facebook vs TikTok) ---
        self._tab_facebook_accounts = tab_acc
        self._tab_facebook_pages = tab_pg
        self._tab_facebook_jobs = tab_jobs
        self._tab_ve_pending_export = tab_ve_pending
        self._tab_ve_pending_notebook_child = tab_ve_pending
        self._tab_tiktok_manager = tab_tt
        self._tab_human_interaction = tab_human
        self._apply_platform_view(self._platform_view_var.get())

        self._nb.bind("<<NotebookTabChanged>>", self._on_manager_notebook_tab_changed, add="+")
        self._root.after_idle(self._on_manager_notebook_tab_changed)

        body.add(nb_host, weight=5)

        log_fr = ttk.Frame(body, padding=4)
        log_bar = ttk.Frame(log_fr)
        log_bar.pack(fill=tk.X, anchor="w")
        ttk.Label(log_bar, text="Nhật ký (INFO)").pack(side=tk.LEFT, anchor="w")
        ttk.Button(log_bar, text="Clear", command=self._on_clear_log_text, width=8).pack(side=tk.RIGHT)
        self._log_text = tk.Text(log_fr, height=self._log_rows, state="disabled", wrap="word", font=("Consolas", 9))
        ly = ttk.Scrollbar(log_fr, orient=tk.VERTICAL, command=self._log_text.yview)
        self._log_text.configure(yscrollcommand=ly.set)
        self._log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ly.pack(side=tk.RIGHT, fill=tk.Y)
        body.add(log_fr, weight=2)
        try:
            body.paneconfigure(nb_host, minsize=340)
            body.paneconfigure(log_fr, minsize=150)
        except tk.TclError:
            pass

        self._root.minsize(860 if self._compact_ui else 960, 560 if self._compact_ui else 620)

        self._attach_log_sink()
        self._root.after_idle(self._defer_startup_data_load)
        self._refresh_openai_tab()
        self._refresh_gemini_tab()
        self._refresh_nanobanana_tab()
        self._capture_ai_provider_widgets()
        self._apply_ai_provider_view()
        self._sync_ai_tab_scrollregion()
        self._start_ui_watchdog()
        self._start_multitask_reconcile_timer()
        self._root.after(900, self._schedule_startup_git_sync)
        logger.info(
            "Đã mở giao diện quản lý — tab Tài khoản / Page / Job lịch / Cài đặt AI; «Bắt đầu lịch» chạy scheduler nền."
        )

    def run(self) -> None:
        """
        Chạy vòng lặp sự kiện Tk cho tới khi đóng cửa sổ.
        """
        self._root.mainloop()

    def _on_manager_notebook_tab_changed(self, _event: tk.Event | None = None) -> None:
        """Trì hoãn tác vụ nền (Video Editor / tab Tải) tới lần đầu người dùng mở đúng tab."""
        try:
            sel = self._nb.select()
        except tk.TclError:
            return
        ve_tab = self._tab_ve_notebook_child
        if ve_tab is not None and str(sel) == str(ve_tab):
            fn = self._schedule_ve_background_fill
            if fn is not None:
                try:
                    fn()
                except Exception:
                    pass
            refresh_dl = getattr(self, "_refresh_ve_download_jobs", None)
            if refresh_dl is not None:
                try:
                    refresh_dl()
                except Exception:
                    pass
        dl_tab = self._tab_dl_notebook_child
        panel = self._embedded_download_panel
        if panel is not None and dl_tab is not None and str(sel) == str(dl_tab):
            try:
                panel.warm_embedded_download_panel()
            except Exception:
                pass
        ve_p_tab = getattr(self, "_tab_ve_pending_notebook_child", None)
        if ve_p_tab is not None and str(sel) == str(ve_p_tab):
            try:
                self._fill_ve_pending_export_jobs_tree()
            except Exception:
                pass

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

    def _start_multitask_reconcile_timer(self) -> None:
        """Định kỳ tự điều chỉnh browser/FFmpeg khi nhiều chức năng chạy song song."""
        if self._multitask_reconcile_after_id is not None:
            return

        def _tick() -> None:
            try:
                from src.scheduler import get_default_browser_pool
                from src.utils.concurrency_runtime import reconcile_multi_task_limits

                pool = get_default_browser_pool()
                in_use = int(getattr(pool, "_in_use", 0))
                reconcile_multi_task_limits(browser_slots_in_use=in_use)
                self._update_multitask_status_label(browser_slots_in_use=in_use)
            except Exception:
                pass
            self._multitask_reconcile_after_id = self._root.after(8_000, _tick)

        self._multitask_reconcile_after_id = self._root.after(8_000, _tick)

    def _stop_multitask_reconcile_timer(self) -> None:
        if self._multitask_reconcile_after_id is None:
            return
        try:
            self._root.after_cancel(self._multitask_reconcile_after_id)
        except Exception:
            pass
        self._multitask_reconcile_after_id = None

    def _refresh_setup_banner(self) -> None:
        """Hiện banner hướng dẫn khi máy mới clone repo chưa có TK / API key."""
        if not hasattr(self, "_setup_banner"):
            return
        try:
            from src.utils.first_run_bootstrap import setup_status

            st = setup_status()
            parts: list[str] = []
            if st.get("needs_account"):
                parts.append("① Thêm tài khoản (tab Tài khoản)")
            if st.get("needs_secrets"):
                parts.append("② Cấu hình Gemini API (tab Cài đặt AI)")
            if not parts:
                self._setup_banner.pack_forget()
                return
            self._lbl_setup_hint.configure(
                text="Máy mới / chưa cấu hình: " + " · ".join(parts) + " — bấm «Hướng dẫn» để xem chi tiết."
            )
            if not self._setup_banner.winfo_ismapped():
                self._setup_banner.pack(fill=tk.X)
        except Exception:
            pass

    def _update_multitask_status_label(self, *, browser_slots_in_use: int | None = None) -> None:
        if not hasattr(self, "_lbl_multitask"):
            return
        try:
            import os

            from src.utils.concurrency_runtime import get_last_applied_limits, workload_snapshot

            if browser_slots_in_use is None:
                try:
                    from src.scheduler import get_default_browser_pool

                    browser_slots_in_use = int(getattr(get_default_browser_pool(), "_in_use", 0))
                except Exception:
                    browser_slots_in_use = 0
            snap = workload_snapshot(browser_slots_in_use=browser_slots_in_use)
            active = [k for k, v in snap.items() if v]
            lim = get_last_applied_limits()
            br = lim.get("BROWSER_CONCURRENCY") or os.environ.get("BROWSER_CONCURRENCY", "?")
            ff = lim.get("TOOLFB_FFMPEG_CONCURRENCY") or os.environ.get("TOOLFB_FFMPEG_CONCURRENCY", "?")
            if active:
                self._lbl_multitask.configure(
                    text=f"Đa tác vụ: {', '.join(active)} | browser≤{br} ffmpeg≤{ff}"
                )
            else:
                self._lbl_multitask.configure(text=f"Sẵn sàng đa tác vụ | browser≤{br} ffmpeg≤{ff}")
        except Exception:
            pass

    def _on_setup_guide(self) -> None:
        """Checklist thiết lập cho máy clone từ GitHub."""
        from src.utils.first_run_bootstrap import setup_status
        from src.utils.paths import project_root

        st = setup_status()
        root = project_root()
        body = (
            "Thiết lập ToolFB (máy mới)\n"
            "────────────────────────\n\n"
            "1. Cài đặt (một lần)\n"
            "   scripts\\setup_windows.bat\n"
            "   hoặc: pip install -r requirements.txt\n"
            "         python -m playwright install firefox\n\n"
            "2. Chạy app\n"
            "   Start_ToolFB_GUI.bat  hoặc  python main.py --gui\n\n"
            "3. Trong app\n"
            "   • Tab Tài khoản → Thêm (profile + cookie)\n"
            "   • Tab Page/Group → thêm Page, gắn account_id\n"
            "   • Tab Cài đặt AI → Gemini API key\n"
            "   • Tab Job lịch → tạo job → Bắt đầu lịch\n\n"
            "4. Tùy chọn\n"
            "   • TOTP đăng nhập: Sửa tài khoản → Facebook login\n"
            "   • Tải video / Video Editor: tab trong notebook\n"
            "   • Nhiều cửa sổ: --multi-instance --data-dir <path>\n\n"
            f"Thư mục dự án: {root}\n"
            f"Tài khoản: {st.get('n_accounts', 0)} | API secrets: {'có' if st.get('has_secrets') else 'chưa'}\n\n"
            "Chi tiết: README.md trong repo."
        )
        top = tk.Toplevel(self._root)
        top.title("Hướng dẫn thiết lập")
        top.transient(self._root)
        top.geometry("520x480")
        top.minsize(420, 360)
        txt = tk.Text(top, wrap="word", font=("Segoe UI", 10), padx=8, pady=8)
        txt.pack(fill=tk.BOTH, expand=True)
        txt.insert("1.0", body)
        txt.configure(state=tk.DISABLED)
        ttk.Button(top, text="Đóng", command=top.destroy).pack(pady=6)

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

    def _account_tree_insert_specs(self, rows: list[AccountRecord]) -> list[dict[str, Any]]:
        specs: list[dict[str, Any]] = []
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
            specs.append(
                {
                    "iid": aid,
                    "values": (
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
                }
            )
        return specs

    def _fill_tree(self, rows: list[AccountRecord]) -> None:
        """Điền bảng tài khoản — insert từng lô nếu danh sách dài."""
        prev_sel = set(self._selected_account_ids())
        specs = self._account_tree_insert_specs(rows)
        self._accounts_tree_gen += 1
        gen = self._accounts_tree_gen
        tree_delete_all(self._tree_accounts)

        def _after_insert() -> None:
            if gen != self._accounts_tree_gen:
                return
            kids = list(self._tree_accounts.get_children())
            self._account_tick = {str(k): self._account_tick.get(str(k), False) for k in kids}
            restore = [i for i in kids if i in prev_sel]
            if restore:
                self._tree_accounts.selection_set(restore)

        if len(specs) < ASYNC_PREP_MIN_ROWS:
            for spec in specs:
                self._tree_accounts.insert("", tk.END, **spec)
            _after_insert()
            return

        tree_insert_chunked(
            self._root,
            self._tree_accounts,
            specs,
            generation=gen,
            is_current=lambda g: g == self._accounts_tree_gen,
            on_complete=_after_insert,
            chunk=DEFAULT_TREE_CHUNK,
        )

    def _refresh_tree(self) -> None:
        """Đọc ``accounts.json`` nền rồi render bảng (không block UI)."""
        if self._accounts_load_busy:
            return
        self._accounts_load_busy = True
        self._set_ui_busy("Đọc accounts.json")

        def _worker() -> list[AccountRecord] | None:
            return self._accounts.reload_from_disk()

        def _on_main(rows: list[AccountRecord] | None) -> None:
            self._accounts_load_busy = False
            self._clear_ui_busy()
            if rows is None:
                return
            try:
                self._fill_tree(rows)
                logger.info("Đã làm mới danh sách từ đĩa: {} tài khoản.", len(rows))
            except Exception as exc:  # noqa: BLE001
                logger.exception("Làm mới danh sách thất bại: {}", exc)
                messagebox.showerror("Lỗi", f"Không hiển thị được bảng tài khoản:\n{exc}")

        def _on_err(exc: BaseException) -> None:
            self._accounts_load_busy = False
            self._clear_ui_busy()
            logger.exception("Làm mới danh sách thất bại: {}", exc)
            messagebox.showerror("Lỗi", f"Không đọc được accounts.json:\n{exc}")

        run_background_then_main(self._root, _worker, _on_main, on_error=_on_err)

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
        def _copy_rows() -> None:
            sel = list(self._tree_accounts.selection())
            if not sel:
                return
            lines: list[str] = []
            for iid in sel:
                vals = [str(v) for v in (self._tree_accounts.item(iid, "values") or ())]
                if vals:
                    lines.append("\t".join(vals))
            if not lines:
                return
            self._root.clipboard_clear()
            self._root.clipboard_append("\n".join(lines))
            logger.info("Đã copy {} dòng tài khoản.", len(lines))

        def _copy_links() -> None:
            sel = list(self._tree_accounts.selection())
            if not sel:
                return
            links: list[str] = []
            for iid in sel:
                vals = [str(v) for v in (self._tree_accounts.item(iid, "values") or ())]
                for v in vals:
                    s = v.strip()
                    if not s:
                        continue
                    low = s.lower()
                    if "http://" in low or "https://" in low:
                        links.append(s)
            if not links:
                logger.info("Không có link trong dòng đã chọn.")
                return
            uniq = list(dict.fromkeys(links))
            self._root.clipboard_clear()
            self._root.clipboard_append("\n".join(uniq))
            logger.info("Đã copy {} link.", len(uniq))

        n_sel = len(self._tree_accounts.selection())
        tick_lbl = "Tick ☑ các dòng đang chọn" if n_sel > 1 else "Tick ☑ dòng này"
        untick_lbl = "Bỏ tick ☐ các dòng đang chọn" if n_sel > 1 else "Bỏ tick ☐ dòng này"
        menu.add_command(label=tick_lbl, command=self._on_menu_tick_selection)
        menu.add_command(label=untick_lbl, command=self._on_menu_untick_selection)
        menu.add_command(label="Chọn hết (Ctrl+A)", command=self._on_accounts_select_all)
        menu.add_separator()
        menu.add_command(label="Copy dòng đã chọn", command=_copy_rows)
        menu.add_command(label="Copy link trong dòng đã chọn", command=_copy_links)
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
        """Đọc ``pages.json`` nền rồi filter/sort/render."""
        if self._pages_load_busy:
            return
        self._pages_load_busy = True
        self._set_ui_busy("Đọc pages.json")

        def _worker() -> list[dict[str, Any]] | None:
            rows = self._pages.load_all()
            return [dict(r) for r in rows]

        def _on_main(pages: list[dict[str, Any]] | None) -> None:
            self._pages_load_busy = False
            self._clear_ui_busy()
            if pages is None:
                return
            self._all_pages = pages
            self._refresh_pages_filter_choices()
            self._render_pages_tree()

        def _on_err(exc: BaseException) -> None:
            self._pages_load_busy = False
            self._clear_ui_busy()
            logger.exception("Đọc pages.json: {}", exc)

        run_background_then_main(self._root, _worker, _on_main, on_error=_on_err)

    def _pages_filtered_sorted_rows(self) -> list[dict[str, Any]]:
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
        elif sk in ("followers", "views", "stats_at"):
            period = self._pages_insights_period_key()

            def _metric_val(r: dict[str, Any], field: str) -> int | str:
                snap = self._page_insights.get_snapshot(str(r.get("id", "")), period)
                if not snap:
                    return -1 if field != "stats_at" else ""
                if field == "stats_at":
                    return str(snap.get("fetched_at", "") or "")
                v = snap.get(field)
                try:
                    return int(v) if v is not None else -1
                except (TypeError, ValueError):
                    return -1

            if sk == "stats_at":
                rows.sort(key=lambda r: _metric_val(r, "stats_at"), reverse=rev)
            else:
                rows.sort(key=lambda r: _metric_val(r, sk), reverse=rev)
        return rows

    def _pages_tree_insert_specs(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        specs: list[dict[str, Any]] = []
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
            fol, views, stats_at = self._page_insights_display(str(p.get("id", "")))
            row_tag = (
                "pg_failed"
                if st_disp == "failed"
                else "pg_success"
                if st_disp == "success"
                else "pg_pending"
            )
            specs.append(
                {
                    "values": (
                        p.get("id", ""),
                        p.get("account_id", ""),
                        p.get("page_kind", "") or "—",
                        p.get("page_name", ""),
                        fol,
                        views,
                        stats_at,
                        top or "—",
                        p.get("post_style", ""),
                        p.get("schedule_time", "") or "—",
                        st_disp,
                        last_post or "—",
                        fb_pid or "—",
                        url,
                    ),
                    "tags": (row_tag,),
                }
            )
        return specs

    def _pages_tree_finish_render(self, rows: list[dict[str, Any]]) -> None:
        self._tree_pages.tag_configure("pg_pending", foreground="#6b6b6b")
        self._tree_pages.tag_configure("pg_success", foreground="#0a7a2e")
        self._tree_pages.tag_configure("pg_failed", foreground="#b00020")
        self._update_pages_heading_sort_indicator()
        if hasattr(self, "_lbl_pages_stats"):
            try:
                self._lbl_pages_stats.configure(
                    text=f"Hiển thị {len(rows)} / {len(getattr(self, '_all_pages', []))} Page"
                )
            except Exception:
                pass

    def _render_pages_tree(self) -> None:
        self._pages_tree_gen += 1
        gen = self._pages_tree_gen
        rows = self._pages_filtered_sorted_rows()
        tree_delete_all(self._tree_pages)
        if not rows:
            self._pages_tree_finish_render(rows)
            return

        if len(rows) >= ASYNC_PREP_MIN_ROWS:
            self._set_ui_busy("Làm mới bảng Page")

            def _worker() -> list[dict[str, Any]]:
                return self._pages_tree_insert_specs(rows)

            def _on_main(specs: list[dict[str, Any]]) -> None:
                self._clear_ui_busy()

                def _done() -> None:
                    if gen != self._pages_tree_gen:
                        return
                    self._pages_tree_finish_render(rows)

                tree_insert_chunked(
                    self._root,
                    self._tree_pages,
                    specs,
                    generation=gen,
                    is_current=lambda g: g == self._pages_tree_gen,
                    on_complete=_done,
                )

            run_background_then_main(self._root, _worker, _on_main)
            return

        specs = self._pages_tree_insert_specs(rows)
        for spec in specs:
            self._tree_pages.insert("", tk.END, **spec)
        self._pages_tree_finish_render(rows)

    def _pages_insights_period_key(self) -> PageInsightsPeriod:
        label = (
            self._var_pages_insights_period.get()
            if hasattr(self, "_var_pages_insights_period")
            else "7 ngày"
        )
        if "28" in str(label):
            return "28d"
        return "7d"

    def _page_insights_display(self, page_id: str) -> tuple[str, str, str]:
        period = self._pages_insights_period_key()
        snap = self._page_insights.get_snapshot(page_id, period)
        if not snap:
            return "—", "—", "—"
        fol = format_metric(snap.get("followers") if snap.get("followers") is not None else None)
        views = format_metric(snap.get("views") if snap.get("views") is not None else None)
        err = str(snap.get("error", "") or "").strip()
        if err and fol == "—" and views == "—":
            views = "!"
        fetched = str(snap.get("fetched_at", "") or "").strip()
        if fetched:
            fetched = fetched.replace("T", " ")[:16]
        return fol, views, fetched or "—"

    def _pages_for_insights_fetch(self, *, selected_only: bool) -> list[dict[str, Any]]:
        if selected_only:
            ids = self._selected_page_ids()
            if not ids:
                return []
            out: list[dict[str, Any]] = []
            for pid in ids:
                rec = self._record_page_by_id(pid)
                if rec is not None:
                    out.append(dict(rec))
            return out
        return self._pages_filtered_sorted_rows()

    def _on_fetch_page_insights_selected(self) -> None:
        self._run_page_insights_fetch(selected_only=True)

    def _on_fetch_page_insights_filtered(self) -> None:
        self._run_page_insights_fetch(selected_only=False)

    def _run_page_insights_fetch(self, *, selected_only: bool) -> None:
        pages = self._pages_for_insights_fetch(selected_only=selected_only)
        if not pages:
            messagebox.showwarning(
                "Chưa có Page",
                "Chọn ít nhất một Page trong bảng, hoặc bỏ lọc để quét theo danh sách hiện tại.",
                parent=self._root,
            )
            return
        missing_id = [p for p in pages if not str(p.get("fb_page_id", "") or "").strip()]
        if missing_id and not messagebox.askyesno(
            "Thiếu Meta Page ID",
            f"{len(missing_id)} Page không có fb_page_id — Insights có thể thất bại.\nTiếp tục?",
            parent=self._root,
        ):
            return
        period = self._pages_insights_period_key()
        period_label = "7 ngày" if period == "7d" else "28 ngày"
        force = bool(getattr(self, "_var_pages_insights_force", tk.BooleanVar(value=False)).get())
        policy = PageInsightsPolicy.from_env()
        plan = plan_page_insights_fetch(
            pages,
            period=period,
            store=self._page_insights,
            policy=policy,
            force_refresh=force,
        )
        lines = [
            f"Kỳ: {period_label}",
            f"• Bỏ qua (cache còn mới, ≥{policy.min_interval_hours:.0f}h): {len(plan.skipped)}",
            f"• Sẽ quét ngay (tối đa {policy.max_pages_per_run} Page/lần): {len(plan.to_fetch)}",
        ]
        if plan.deferred_over_limit:
            lines.append(f"• Hoãn (vượt giới hạn / cooldown tài khoản): {len(plan.deferred_over_limit)}")
        if plan.account_blocked:
            for aid, msg in list(plan.account_blocked.items())[:3]:
                lines.append(f"  — {aid}: {msg}")
        lines.append(
            f"\nNghỉ ~{policy.page_delay_min_sec:.0f}–{policy.page_delay_max_sec:.0f}s giữa mỗi Page để giảm checkpoint."
        )
        if not plan.to_fetch:
            messagebox.showinfo(
                "Thống kê Page",
                "\n".join(lines) + "\n\nKhông có Page nào cần quét. Bật «Bỏ qua cache» nếu muốn cập nhật lại.",
                parent=self._root,
            )
            return
        if not messagebox.askyesno("Xác nhận lấy thống kê", "\n".join(lines) + "\n\nTiếp tục?", parent=self._root):
            return
        top = tk.Toplevel(self._root)
        top.title("Lấy thống kê Page")
        top.transient(self._root)
        top.grab_set()
        top.geometry("460x150")
        status_var = tk.StringVar(
            value=f"Chuẩn bị… ({period_label}, quét {len(plan.to_fetch)} Page)"
        )
        ttk.Label(top, textvariable=status_var, wraplength=420).pack(fill=tk.X, padx=12, pady=12)
        pbar = ttk.Progressbar(top, mode="indeterminate", length=400)
        pbar.pack(fill=tk.X, padx=12, pady=(0, 12))
        pbar.start(12)
        done_evt = threading.Event()
        err_holder: list[str] = []
        ok_count = {"n": 0}

        def worker() -> None:
            from src.services.page_insights_scraper import fetch_insights_for_pages

            by_owner: dict[str, list[dict[str, Any]]] = {}
            for row in plan.to_fetch:
                aid = str(row.get("account_id", "")).strip()
                by_owner.setdefault(aid, []).append(row)
            factory: BrowserFactory | None = None
            try:
                factory = BrowserFactory(accounts=self._accounts, headless=True)
                for aid, group in by_owner.items():
                    if not aid:
                        err_holder.append("Có Page thiếu account_id (owner).")
                        continue
                    acc = self._accounts.get_by_id(aid)
                    if acc is None:
                        err_holder.append(f"Không tìm thấy tài khoản owner {aid!r}.")
                        continue

                    def _status(msg: str) -> None:
                        try:
                            self._root.after(0, lambda m=msg: status_var.set(m))
                        except Exception:
                            pass

                    self._page_insights.touch_account_session(aid)
                    ctx = factory.get_browser_context(aid, headless=True)
                    try:
                        batch = fetch_insights_for_pages(
                            ctx,
                            account=dict(acc),
                            pages=group,
                            period=period,
                            status_cb=_status,
                            policy=policy,
                        )
                        if batch.stopped_early and batch.stop_reason:
                            err_holder.append(batch.stop_reason)
                        for pid, snap in batch.results:
                            self._page_insights.save_snapshot(
                                pid,
                                period,
                                followers=snap.get("followers"),
                                views=snap.get("views"),
                                source_url=str(snap.get("source_url", "")),
                                error=str(snap.get("error", "")),
                            )
                            if snap.get("followers") is not None or snap.get("views") is not None:
                                ok_count["n"] += 1
                    finally:
                        sync_close_persistent_context(ctx, log_label=f"page_insights:{aid}")
            except Exception as exc:  # noqa: BLE001
                err_holder.append(str(exc))
                logger.exception("Lấy thống kê Page lỗi")
            finally:
                if factory is not None:
                    try:
                        factory.close()
                    except Exception:
                        pass
                done_evt.set()

        threading.Thread(target=worker, name="page_insights_fetch", daemon=True).start()

        def poll() -> None:
            if not done_evt.is_set():
                top.after(200, poll)
                return
            try:
                pbar.stop()
            except Exception:
                pass
            try:
                top.grab_release()
                top.destroy()
            except Exception:
                pass
            self._render_pages_tree()
            if err_holder:
                messagebox.showwarning(
                    "Thống kê Page",
                    f"Đã lưu một phần ({ok_count['n']} Page có số liệu).\n\n" + "\n".join(err_holder[:5]),
                    parent=self._root,
                )
            else:
                extra = ""
                if plan.skipped:
                    extra = f"\nĐã bỏ qua {len(plan.skipped)} Page (cache còn mới)."
                if plan.deferred_over_limit:
                    extra += f"\nHoãn {len(plan.deferred_over_limit)} Page — chạy lại sau hoặc tăng TOOLFB_PAGE_INSIGHTS_MAX_PER_RUN."
                messagebox.showinfo(
                    "Thống kê Page",
                    f"Hoàn tất ({period_label}): {ok_count['n']}/{len(plan.to_fetch)} Page quét có số liệu.{extra}",
                    parent=self._root,
                )

        top.after(200, poll)

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
            "followers": "followers",
            "views": "views",
            "stats_at": "stats_at",
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


    def _defer_startup_data_load(self) -> None:
        """Nạp tab Accounts / Pages / Jobs sau khi cửa sổ hiện — tránh Not Responding lúc mở app."""
        self._refresh_tree()
        self._fill_pages_tree()
        self._fill_schedule_jobs_tree()
        self._refresh_setup_banner()
        self._update_multitask_status_label()

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
        old_d, new_d = str(old_dir), str(new_dir)
        script_s = str(script)
        self._set_ui_busy("migrate_user_data")
        self._root.configure(cursor="watch")

        def _migrate_worker() -> None:
            result: dict[str, Any] = {"cp": None, "exc": None}
            try:
                result["cp"] = subprocess.run(
                    [sys.executable, script_s, "--from", old_d, "--to", new_d],
                    capture_output=True,
                    text=True,
                    timeout=180,
                    check=False,
                )
            except Exception as exc:  # noqa: BLE001
                result["exc"] = exc

            def _migrate_done() -> None:
                self._clear_ui_busy()
                self._root.configure(cursor="")
                if result["exc"] is not None:
                    messagebox.showerror(
                        "Migrate",
                        f"Chạy migrate thất bại:\n{result['exc']}",
                        parent=self._root,
                    )
                    return
                cp = result["cp"]
                if cp is None:
                    messagebox.showerror("Migrate", "Không có kết quả subprocess.", parent=self._root)
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
                    f"Đã migrate dữ liệu từ:\n{old_d}\n\nSang:\n{new_d}\n\n{preview}",
                    parent=self._root,
                )

            self._root.after(0, _migrate_done)

        threading.Thread(target=_migrate_worker, name="migrate_user_data", daemon=True).start()

    def _fill_schedule_jobs_tree(self) -> None:
        """Đọc dữ liệu gốc vào ``self._all_jobs`` rồi áp filter/sort và render (JSON ngoài main thread)."""
        if self._schedule_jobs_load_busy:
            return
        self._schedule_jobs_load_busy = True
        self._set_ui_busy("Đọc schedule_posts.json")

        def _worker() -> None:
            err: str | None = None
            jobs: list[dict[str, Any]] | None = None
            try:
                rows = self._schedule_posts.load_all()
                jobs = [dict(r) for r in rows]
            except Exception as exc:  # noqa: BLE001
                err = str(exc)
                logger.warning("Không đọc schedule_posts: {}", exc)

            def _done() -> None:
                self._schedule_jobs_load_busy = False
                self._clear_ui_busy()
                if err is not None or jobs is None:
                    try:
                        self._root.after(50, self._fill_ve_pending_export_jobs_tree)
                    except Exception:
                        pass
                    return
                clear_file_exists_cache()
                self._all_jobs = jobs
                self._refresh_job_page_name_map()
                self._refresh_job_account_name_map()
                self._refresh_job_filter_choices()
                self._render_schedule_jobs_tree()
                self._root.after(50, self._fill_ve_pending_export_jobs_tree)

            try:
                self._root.after(0, _done)
            except tk.TclError:
                self._schedule_jobs_load_busy = False

        threading.Thread(target=_worker, name="load_schedule_posts", daemon=True).start()

    def _apply_ve_pending_export_jobs_ui(self, rows_out: list[dict[str, Any]]) -> None:
        """Cập nhật combobox job chờ đăng (đã lọc/sort trên worker)."""
        if not hasattr(self, "_cb_ve_pending_jobs"):
            return
        col = str(getattr(self, "_ve_pending_sort_col", "created") or "created")
        asc = bool(getattr(self, "_ve_pending_sort_asc", False))

        def _sort_key(it: dict[str, Any]) -> Any:
            if col == "n":
                try:
                    return int(it.get("n", 0))
                except (TypeError, ValueError):
                    return 0
            if col == "id":
                return str(it.get("jid", "")).lower()
            if col == "job_name":
                return str(it.get("jn", "")).lower()
            if col == "target":
                return str(it.get("tgt_disp", "")).lower()
            if col == "created":
                return str(it.get("created", ""))
            if col == "status":
                return str(it.get("st_disp", "")).lower()
            return str(it.get("created", ""))

        rows_out.sort(key=_sort_key, reverse=not asc)

        prev_id = str(getattr(self, "_ve_pending_selected_id", "") or "").strip()
        by_label: dict[str, dict[str, Any]] = {}
        labels: list[str] = []
        for it in rows_out:
            r = it["row"]
            jid = it["jid"]
            jn = it["jn"]
            n = it["n"]
            created = it["created"]
            base_lbl = f"{jn} ({n} video)  {created}  [{jid}]"
            lbl = base_lbl
            dup = 0
            while lbl in by_label:
                dup += 1
                lbl = f"{base_lbl}  ({dup})"
            by_label[lbl] = r
            labels.append(lbl)

        self._ve_pending_job_by_label = by_label
        self._suppress_ve_pending_job_cb["v"] = True
        try:
            self._cb_ve_pending_jobs.configure(values=labels)
            picked = ""
            if labels:
                if prev_id:
                    picked = next(
                        (lb for lb in labels if str(by_label[lb].get("id") or "").strip() == prev_id),
                        "",
                    )
                if not picked:
                    picked = labels[0]
                self._var_ve_pending_job.set(picked)
            else:
                self._var_ve_pending_job.set("")
        finally:
            self._suppress_ve_pending_job_cb["v"] = False

        self._sync_ve_pending_selected_id_from_combo()
        self._root.after(0, self._refresh_ve_pending_job_detail)

    def _fill_ve_pending_export_jobs_tree(self) -> None:
        """Nạp danh sách job chờ vào combobox (đọc JSON ngoài main thread)."""
        if not hasattr(self, "_cb_ve_pending_jobs"):
            return
        needle = str(self._var_ve_pending_search.get() or "").strip().casefold()
        tgt_labels = {
            "facebook": "Facebook + Page",
            "tiktok": "TikTok",
            "unspecified": "Chờ chọn",
            "": "—",
        }

        def _worker() -> None:
            rows_out: list[dict[str, Any]] = []
            for r in self._load_saved_export_schedule_jobs():
                if not isinstance(r, dict):
                    continue
                st = str(r.get("status", "")).strip().lower()
                if st not in {"", "saved", "pending"}:
                    continue
                jid = str(r.get("id") or "").strip()
                if not jid:
                    continue
                jn = str(r.get("job_name") or jid).strip()
                tgt = str(r.get("publish_target") or "").strip().lower()
                tgt_disp = tgt_labels.get(tgt, tgt or "—")
                try:
                    n = len(r.get("items") or [])
                except TypeError:
                    n = 0
                created = str(r.get("created_at") or "").strip()
                st_disp = str(r.get("status") or "").strip() or "saved"
                hay = f"{jid} {jn} {tgt_disp} {n} {created} {st_disp}".casefold()
                if needle and needle not in hay:
                    continue
                rows_out.append(
                    {
                        "row": r,
                        "jid": jid,
                        "jn": jn,
                        "tgt_disp": tgt_disp,
                        "n": n,
                        "created": created,
                        "st_disp": st_disp,
                    }
                )

            def _done() -> None:
                self._apply_ve_pending_export_jobs_ui(rows_out)

            try:
                self._root.after(0, _done)
            except tk.TclError:
                pass

        threading.Thread(target=_worker, name="ve_pending_jobs_load", daemon=True).start()

    def _sync_ve_pending_sort_ui_from_col(self) -> None:
        lab = _VE_PENDING_COL_TO_SORT_LABEL.get(str(getattr(self, "_ve_pending_sort_col", "") or ""), "Tạo lúc")
        self._suppress_ve_pending_sort_ui["v"] = True
        try:
            self._var_ve_pending_sort_ui.set(lab)
        finally:
            self._suppress_ve_pending_sort_ui["v"] = False

    def _on_ve_pending_sort_field_changed(self, _event: tk.Event | None = None) -> None:
        if self._suppress_ve_pending_sort_ui["v"]:
            return
        lab = str(self._var_ve_pending_sort_ui.get() or "").strip()
        c = _VE_PENDING_SORT_LABEL_TO_COL.get(lab, "created")
        if getattr(self, "_ve_pending_sort_col", "") == c:
            return
        self._ve_pending_sort_col = c
        self._ve_pending_sort_asc = True
        self._fill_ve_pending_export_jobs_tree()

    def _on_ve_pending_sort_dir_toggle(self) -> None:
        self._ve_pending_sort_asc = not bool(getattr(self, "_ve_pending_sort_asc", True))
        self._fill_ve_pending_export_jobs_tree()

    def _sync_ve_pending_selected_id_from_combo(self) -> None:
        lbl = str(self._var_ve_pending_job.get() or "").strip()
        r = self._ve_pending_job_by_label.get(lbl) if lbl else None
        if isinstance(r, dict):
            self._ve_pending_selected_id = str(r.get("id") or "").strip()
        else:
            self._ve_pending_selected_id = ""

    def _on_ve_pending_job_combo_selected(self, _event: tk.Event | None = None) -> None:
        if self._suppress_ve_pending_job_cb["v"]:
            return
        self._sync_ve_pending_selected_id_from_combo()
        self._refresh_ve_pending_job_detail()

    @staticmethod
    def _ve_pending_truncate(s: str, max_len: int) -> str:
        t = str(s or "").replace("\r\n", "\n").replace("\r", "\n").strip()
        if len(t) <= max_len:
            return t
        return t[: max(0, max_len - 1)] + "…"

    @staticmethod
    def _ve_resolve_item_publish(it: dict[str, Any], rec: dict[str, Any], dialog_plat: str) -> str:
        t = str(it.get("item_publish_target") or "").strip().lower()
        if t in {"", "inherit"}:
            jt = str(rec.get("publish_target") or "").strip().lower()
            if jt == "facebook":
                return "facebook"
            if jt == "tiktok":
                return "tiktok"
            return "tiktok" if str(dialog_plat or "").strip().lower() == "tiktok" else "facebook"
        if t == "unspecified":
            return "unspecified"
        if t == "tiktok":
            return "tiktok"
        return "facebook"

    @staticmethod
    def _ve_resolve_item_fb_ids(
        it: dict[str, Any],
        rec: dict[str, Any],
        dialog_acc_lbl: str,
        dialog_page_lbl: str,
        fb_account_map: dict[str, str],
        page_map: dict[str, str],
    ) -> tuple[str, str]:
        ja = str(rec.get("preset_fb_account_id") or "").strip()
        jp = str(rec.get("preset_fb_page_id") or "").strip()
        ia = str(it.get("item_preset_fb_account_id") or "").strip()
        ip = str(it.get("item_preset_fb_page_id") or "").strip()
        acc_id = ia or ja
        page_id = ip or jp
        if not acc_id:
            acc_id = fb_account_map.get(str(dialog_acc_lbl or "").strip(), "").strip()
        if not page_id:
            page_id = page_map.get(str(dialog_page_lbl or "").strip(), "").strip()
        return acc_id, page_id

    @staticmethod
    def _ve_resolve_item_tt_account(
        it: dict[str, Any],
        rec: dict[str, Any],
        dialog_tt_lbl: str,
        tt_account_map: dict[str, str],
    ) -> str:
        tid = str(it.get("item_preset_tiktok_account_id") or "").strip()
        if tid:
            return tid
        jr = str(rec.get("preset_tiktok_account_id") or "").strip()
        if jr:
            return jr
        return tt_account_map.get(str(dialog_tt_lbl or "").strip(), "").strip()

    def _ve_pending_format_item_dest_disp(
        self,
        it: dict[str, Any],
        rec: dict[str, Any],
        *,
        fb_acc_lbl: dict[str, str],
        fb_page_lbl: dict[str, str],
        tt_lbl: dict[str, str],
    ) -> str:
        def _lbl(m: dict[str, str], key: str, kind: str) -> str:
            k = str(key or "").strip()
            if not k:
                return ""
            return str(m.get(k) or "").strip() or f"({kind} id: {self._ve_pending_truncate(k, 14)})"

        t = str(it.get("item_publish_target") or "").strip().lower()
        if t in {"", "inherit"}:
            jt = str(rec.get("publish_target") or "").strip().lower()
            if not jt or jt == "unspecified":
                return "Theo dialog khi nạp"
            if jt == "facebook":
                ja = str(rec.get("preset_fb_account_id") or "").strip()
                jp = str(rec.get("preset_fb_page_id") or "").strip()
                if ja or jp:
                    pn = _lbl(fb_page_lbl, jp, "Page")
                    an = _lbl(fb_acc_lbl, ja, "TK")
                    return f"Theo job · FB: {pn} · {an}"
                return "Theo job · Facebook"
            if jt == "tiktok":
                jr = str(rec.get("preset_tiktok_account_id") or "").strip()
                if jr:
                    tn = _lbl(tt_lbl, jr, "TT")
                    return f"Theo job · TT: {tn}"
                return "Theo job · TikTok"
            return f"Theo job · {jt}"
        if t == "unspecified":
            return "Chờ chọn (clip)"
        if t == "tiktok":
            tid = str(it.get("item_preset_tiktok_account_id") or "").strip()
            if not tid:
                return "TikTok · thiếu TK"
            tn = _lbl(tt_lbl, tid, "TT")
            return f"TT · {tn}"
        ia = str(it.get("item_preset_fb_account_id") or "").strip()
        ip = str(it.get("item_preset_fb_page_id") or "").strip()
        if not ia and not ip:
            return "Facebook · thiếu Page/TK"
        pn = _lbl(fb_page_lbl, ip, "Page")
        an = _lbl(fb_acc_lbl, ia, "TK")
        return f"FB · {pn} · {an}"

    def _ve_pending_sync_item_schedule_ref(self, it: dict[str, Any], *, tt_store: TikTokJobStore) -> bool:
        """Xóa liên kết lịch trên clip nếu job FB/TT không còn — trả về True nếu đã sửa dict."""
        jid = str(it.get("item_scheduled_job_id") or "").strip()
        if not jid:
            return False
        plat = str(it.get("item_scheduled_platform") or "").strip().lower()
        if not plat:
            if "tt_job" in jid or jid.startswith("tt_"):
                plat = "tiktok"
            elif jid.startswith("sched_"):
                plat = "facebook"
        exists = False
        if plat == "tiktok":
            exists = tt_store.get_by_id(jid) is not None
        elif plat == "facebook":
            exists = self._schedule_posts.get_by_id(jid) is not None
        else:
            exists = self._schedule_posts.get_by_id(jid) is not None or tt_store.get_by_id(jid) is not None
        if exists:
            return False
        for k in ("item_scheduled_platform", "item_scheduled_job_id", "item_scheduled_at"):
            it.pop(k, None)
        return True

    def _ve_pending_item_schedule_status_disp(self, it: dict[str, Any], *, tt_store: TikTokJobStore) -> str:
        jid = str(it.get("item_scheduled_job_id") or "").strip()
        if not jid:
            return "Chưa nạp lịch"
        plat = str(it.get("item_scheduled_platform") or "").strip().lower()
        if not plat:
            if "tt_job" in jid or jid.startswith("tt_"):
                plat = "tiktok"
            elif jid.startswith("sched_"):
                plat = "facebook"
        if plat == "tiktok":
            r = tt_store.get_by_id(jid)
            if not r:
                return "TT · không còn trên lịch"
            st = str(r.get("status") or "").strip() or "—"
            return f"TT · {st}"
        if plat == "facebook":
            r = self._schedule_posts.get_by_id(jid)
            if r is None:
                return "FB · không còn trên lịch"
            st = str(r.get("status") or "").strip() or "—"
            return f"FB · {st}"
        return f"Đã nạp · {self._ve_pending_truncate(jid, 28)}"

    def _ve_pending_persist_current_selection(self) -> bool:
        jid = str(getattr(self, "_ve_pending_selected_id", "") or "").strip()
        lbl = str(self._var_ve_pending_job.get() or "").strip()
        cur = self._ve_pending_job_by_label.get(lbl) if lbl else None
        if not jid or not isinstance(cur, dict):
            messagebox.showwarning(
                "Chưa chọn job",
                "Chọn một job trong ô «Chọn job» trước.",
                parent=self._root,
            )
            return False
        rows = self._load_saved_export_schedule_jobs()
        idx: int | None = None
        for i, row in enumerate(rows):
            if isinstance(row, dict) and str(row.get("id") or "").strip() == jid:
                idx = i
                break
        if idx is None:
            messagebox.showerror(
                "Lỗi lưu",
                "Không tìm thấy job trong file (id có thể đã đổi). Hãy làm mới danh sách.",
                parent=self._root,
            )
            return False
        rows[idx] = copy.deepcopy(cur)
        try:
            self._save_saved_export_schedule_jobs(rows)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi lưu file", str(exc), parent=self._root)
            return False
        self._fill_ve_pending_export_jobs_tree()
        return True

    def _on_ve_pending_add_video_files(self) -> None:
        from src.gui.file_dialog_defaults import pick_video_media_files

        paths = pick_video_media_files(
            self._root,
            title="Chọn video từ thư mục (renders hoặc nơi khác)",
            multiple=True,
        )
        if not paths:
            return
        lbl = str(self._var_ve_pending_job.get() or "").strip()
        cur = self._ve_pending_job_by_label.get(lbl)
        if not isinstance(cur, dict):
            messagebox.showwarning(
                "Chưa chọn job",
                "Chọn một job trong ô «Chọn job» trước khi thêm video.",
                parent=self._root,
            )
            return
        items = list(cur.get("items") or [])
        for p in paths:
            pth = str(Path(p).expanduser().resolve())
            stem = Path(pth).stem
            items.append(
                {
                    "video_path": pth,
                    "title": stem,
                    "content": stem,
                    "hashtags": [],
                    "source_download_video_id": "",
                }
            )
        cur["items"] = items
        self._ve_pending_persist_current_selection()

    def _on_ve_pending_assign_dest_for_selection(self) -> None:
        sel = self._tree_ve_pending_videos.selection()
        if not sel:
            messagebox.showinfo(
                "Chưa chọn dòng",
                "Chọn một hoặc nhiều dòng trong bảng video (Ctrl/Shift + click), rồi bấm lại.",
                parent=self._root,
            )
            return
        positions: list[int] = []
        for iid in sel:
            try:
                positions.append(int(str(iid)))
            except ValueError:
                continue
        positions = sorted(set(positions))
        if not positions:
            return
        lbl = str(self._var_ve_pending_job.get() or "").strip()
        cur = self._ve_pending_job_by_label.get(lbl)
        if not isinstance(cur, dict):
            messagebox.showwarning("Chưa chọn job", "Chọn job trong ô «Chọn job».", parent=self._root)
            return
        items = cur.get("items") or []
        if not isinstance(items, list):
            messagebox.showerror("Lỗi dữ liệu", "Job không có danh sách items hợp lệ.", parent=self._root)
            return
        for pos in positions:
            if pos < 0 or pos >= len(items) or not isinstance(items[pos], dict):
                messagebox.showerror("Lỗi", f"Dòng không hợp lệ (index {pos}).", parent=self._root)
                return
        self._ve_pending_open_assign_dest_dialog(positions, cur)

    def _ve_pending_open_assign_dest_dialog(self, positions: list[int], job: dict[str, Any]) -> None:
        fb_account_map: dict[str, str] = {}
        try:
            for a in self._accounts.load_all():
                if not isinstance(a, dict):
                    continue
                sk, lb, aid = self._ve_import_fb_account_row(a)
                if aid:
                    fb_account_map[lb] = aid
        except Exception:
            pass
        tt_account_map: dict[str, str] = {}
        try:
            tt_rows: list[tuple[str, str, str]] = []
            for a in TikTokAccountStore().load_all():
                if not isinstance(a, dict):
                    continue
                sk, lb, aid = self._ve_import_tt_account_row(a)
                if aid:
                    tt_rows.append((sk, lb, aid))
            tt_rows.sort(key=lambda x: x[0])
            for _, lb, aid in tt_rows:
                tt_account_map[lb] = aid
        except Exception:
            pass

        top = tk.Toplevel(self._root)
        top.title("Gán đích đăng cho clip")
        top.transient(self._root)
        top.geometry("560x640")
        top.minsize(480, 320)
        frm = ttk.Frame(top, padding=10)
        frm.pack(fill=tk.BOTH, expand=True)
        frm.columnconfigure(0, weight=1)
        frm.rowconfigure(0, weight=1)

        body = ttk.Frame(frm)
        body.grid(row=0, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)

        n = len(positions)
        ttk.Label(body, text=f"Áp dụng cho {n} clip đã chọn (index: {positions[0]}{'…' if n > 1 else ''}).").pack(
            anchor="w", pady=(0, 8)
        )
        var_mode = tk.StringVar(value="inherit")
        ttk.Label(body, text="Chế độ").pack(anchor="w")
        for val, cap in (
            ("inherit", "Theo job / theo dialog khi nạp (xóa gán riêng clip)"),
            ("facebook", "Facebook — tài khoản + Page cố định cho clip"),
            ("tiktok", "TikTok — tài khoản cố định cho clip"),
            ("unspecified", "Chờ chọn (clip) — bắt buộc gán trước khi nạp lịch"),
        ):
            ttk.Radiobutton(body, text=cap, value=val, variable=var_mode).pack(anchor="w", pady=1)

        box_fb = ttk.LabelFrame(body, text="Facebook", padding=6)
        var_acc = tk.StringVar(value="")
        var_page = tk.StringVar(value="")
        page_map: dict[str, str] = {}

        def _refresh_pages_local(*_a: Any) -> None:
            page_map.clear()
            aid = fb_account_map.get(str(var_acc.get() or "").strip(), "")
            rows_pg: list[tuple[str, str, str]] = []
            if aid:
                for p in self._pages.load_all():
                    if not isinstance(p, dict):
                        continue
                    if str(p.get("account_id") or "").strip() != aid:
                        continue
                    sk, lb, pid = self._ve_import_fb_page_row(p)
                    if pid:
                        rows_pg.append((sk, lb, pid))
            rows_pg.sort(key=lambda x: x[0])
            vals = [x[1] for x in rows_pg]
            for _, lb, pid in rows_pg:
                page_map[lb] = pid
            cb_page.configure(values=vals)
            if vals and str(var_page.get() or "").strip() not in vals:
                var_page.set(vals[0])
            elif not vals:
                var_page.set("")

        acc_vals = sorted(fb_account_map.keys())
        ttk.Label(box_fb, text="Tài khoản").grid(row=0, column=0, sticky="w", padx=(0, 6))
        cb_acc = ttk.Combobox(box_fb, textvariable=var_acc, values=acc_vals, state="readonly", width=52)
        cb_acc.grid(row=0, column=1, sticky="ew", pady=2)
        ttk.Label(box_fb, text="Page").grid(row=1, column=0, sticky="w", padx=(0, 6))
        cb_page = ttk.Combobox(box_fb, textvariable=var_page, state="readonly", width=52)
        cb_page.grid(row=1, column=1, sticky="ew", pady=2)
        box_fb.columnconfigure(1, weight=1)

        box_tt = ttk.LabelFrame(body, text="TikTok", padding=6)
        var_tt = tk.StringVar(value="")
        tt_vals = sorted(tt_account_map.keys())
        ttk.Label(box_tt, text="Tài khoản").grid(row=0, column=0, sticky="w", padx=(0, 6))
        cb_tt = ttk.Combobox(box_tt, textvariable=var_tt, values=tt_vals, state="readonly", width=52)
        cb_tt.grid(row=0, column=1, sticky="ew")
        box_tt.columnconfigure(1, weight=1)

        def _sync_mode_ui(*_a: Any) -> None:
            m = str(var_mode.get() or "").strip()
            box_fb.pack_forget()
            box_tt.pack_forget()
            if m == "facebook":
                box_fb.pack(fill=tk.X, pady=(8, 0))
                if acc_vals and not str(var_acc.get() or "").strip():
                    var_acc.set(acc_vals[0])
                _refresh_pages_local()
            elif m == "tiktok":
                box_tt.pack(fill=tk.X, pady=(8, 0))
                if tt_vals and not str(var_tt.get() or "").strip():
                    var_tt.set(tt_vals[0])

        var_mode.trace_add("write", _sync_mode_ui)
        var_acc.trace_add("write", _refresh_pages_local)
        preset_done = False
        if len(positions) == 1:
            items0 = job.get("items") or []
            p0 = positions[0]
            if isinstance(items0, list) and 0 <= p0 < len(items0) and isinstance(items0[p0], dict):
                it0 = items0[p0]
                tm = str(it0.get("item_publish_target") or "").strip().lower()
                if tm == "unspecified":
                    var_mode.set("unspecified")
                    preset_done = True
                elif tm == "facebook":
                    ia = str(it0.get("item_preset_fb_account_id") or "").strip()
                    ip = str(it0.get("item_preset_fb_page_id") or "").strip()
                    if ia or ip:
                        var_mode.set("facebook")
                        al = next((lb for lb, xid in fb_account_map.items() if xid == ia), "")
                        if al:
                            var_acc.set(al)
                        _refresh_pages_local()
                        pl = next((lb for lb, xid in page_map.items() if xid == ip), "")
                        if pl:
                            var_page.set(pl)
                        preset_done = True
                elif tm == "tiktok":
                    tid = str(it0.get("item_preset_tiktok_account_id") or "").strip()
                    if tid:
                        var_mode.set("tiktok")
                        tl = next((lb for lb, xid in tt_account_map.items() if xid == tid), "")
                        if tl:
                            var_tt.set(tl)
                        preset_done = True
        if not preset_done:
            if acc_vals:
                var_acc.set(acc_vals[0])
                _refresh_pages_local()
            if tt_vals:
                var_tt.set(tt_vals[0])
        _sync_mode_ui()

        try:
            _tz_assign = str(getattr(scheduler_tz(), "key", "") or "").strip() or "Asia/Ho_Chi_Minh"
        except Exception:
            _tz_assign = "Asia/Ho_Chi_Minh"

        sch_fr = ttk.LabelFrame(body, text="Lịch đăng", padding=8)
        sch_fr.pack(fill=tk.X, pady=(10, 0))
        sch_fr.columnconfigure(1, weight=1)
        _now_a = datetime.now()
        var_sched_rule_a = tk.StringVar(value="Một lần")
        var_sch_step = tk.StringVar(value="30")
        sra = 0
        ttk.Label(sch_fr, text="Kiểu lịch").grid(row=sra, column=0, sticky="nw", padx=(0, 8), pady=2)
        cb_sched_rule_a = ttk.Combobox(
            sch_fr,
            textvariable=var_sched_rule_a,
            values=("Đăng ngay", "Một lần", "Theo khung giờ mỗi ngày"),
            state="readonly",
            width=36,
        )
        cb_sched_rule_a.grid(row=sra, column=1, sticky="w", pady=2)
        sra += 1
        lbl_start_date_a = ttk.Label(sch_fr, text="Ngày bắt đầu (YYYY-MM-DD)")
        lbl_start_date_a.grid(row=sra, column=0, sticky="nw", padx=(0, 8), pady=4)
        e_start_date_a = ttk.Entry(sch_fr, width=14)
        e_start_date_a.insert(0, _now_a.strftime("%Y-%m-%d"))
        e_start_date_a.grid(row=sra, column=1, sticky="w", pady=4)
        sra += 1
        lbl_once_time_a = ttk.Label(sch_fr, text="Giờ/phút (cho kiểu Một lần)")
        lbl_once_time_a.grid(row=sra, column=0, sticky="nw", padx=(0, 8), pady=4)
        sched_once_fr_a = ttk.Frame(sch_fr)
        sched_once_fr_a.grid(row=sra, column=1, sticky="w", pady=4)
        ttk.Label(sched_once_fr_a, text="Giờ:").pack(side=tk.LEFT)
        sp_hour_a = ttk.Spinbox(sched_once_fr_a, from_=0, to=23, width=4, format="%.0f")
        sp_hour_a.set(str(_now_a.hour))
        sp_hour_a.pack(side=tk.LEFT, padx=4)
        ttk.Label(sched_once_fr_a, text="Phút:").pack(side=tk.LEFT)
        sp_min_a = ttk.Spinbox(sched_once_fr_a, from_=0, to=59, width=4, format="%.0f")
        sp_min_a.set(str(_now_a.minute))
        sp_min_a.pack(side=tk.LEFT, padx=4)
        sra += 1
        lbl_daily_slots_a = ttk.Label(sch_fr, text="Khung giờ/ngày (HH:MM, phẩy)")
        lbl_daily_slots_a.grid(row=sra, column=0, sticky="nw", padx=(0, 8), pady=4)
        e_daily_slots_a = ttk.Entry(sch_fr, width=34)
        e_daily_slots_a.insert(0, "04:30,10:15,22:30")
        e_daily_slots_a.grid(row=sra, column=1, sticky="ew", pady=4)
        sra += 1
        lbl_delay_min_a = ttk.Label(sch_fr, text="Delay tối thiểu (phút)")
        lbl_delay_min_a.grid(row=sra, column=0, sticky="nw", padx=(0, 8), pady=4)
        delay_min_a = ttk.Spinbox(sch_fr, from_=0, to=180, width=6)
        delay_min_a.insert(0, "1")
        delay_min_a.grid(row=sra, column=1, sticky="w", pady=4)
        sra += 1
        lbl_delay_max_a = ttk.Label(sch_fr, text="Delay tối đa (phút)")
        lbl_delay_max_a.grid(row=sra, column=0, sticky="nw", padx=(0, 8), pady=4)
        delay_max_a = ttk.Spinbox(sch_fr, from_=0, to=180, width=6)
        delay_max_a.insert(0, "5")
        delay_max_a.grid(row=sra, column=1, sticky="w", pady=4)
        sra += 1
        lbl_timezone_a = ttk.Label(sch_fr, text="Múi giờ")
        lbl_timezone_a.grid(row=sra, column=0, sticky="nw", padx=(0, 8), pady=4)
        e_timezone_a = ttk.Entry(sch_fr, width=30)
        e_timezone_a.insert(0, _tz_assign)
        e_timezone_a.grid(row=sra, column=1, sticky="ew", pady=4)
        sra += 1
        lbl_step_gap_a = ttk.Label(sch_fr, text="Cách nhau (phút, nhiều video)")
        lbl_step_gap_a.grid(row=sra, column=0, sticky="nw", padx=(0, 8), pady=4)
        ent_step_gap_a = ttk.Entry(sch_fr, textvariable=var_sch_step, width=8)
        ent_step_gap_a.grid(row=sra, column=1, sticky="w", pady=4)
        sra += 1
        _default_lbl_fg_a = lbl_daily_slots_a.cget("foreground")
        lbl_sch_hint_a = ttk.Label(
            sch_fr,
            text=(
                "Giống job lịch / «Thêm batch job»: Đăng ngay / Một lần / Theo khung giờ mỗi ngày. "
                "Nhiều clip: «Theo khung giờ» xếp theo slot; «Đăng ngay» / «Một lần» dùng «Cách nhau (phút)»."
            ),
            foreground="gray",
            font=("Segoe UI", 8),
            wraplength=500,
        )
        lbl_sch_hint_a.grid(row=sra, column=0, columnspan=2, sticky="w", pady=(4, 0))
        sra += 1

        def _assign_sched_rule_key() -> str:
            s = str(var_sched_rule_a.get() or "")
            if "Đăng ngay" in s:
                return "immediate"
            if "Theo khung giờ" in s:
                return "daily_slots"
            return "once"

        def _on_assign_sched_rule_changed(*_a: Any) -> None:
            rule = _assign_sched_rule_key()
            if rule == "immediate":
                lbl_start_date_a.grid_remove()
                e_start_date_a.grid_remove()
                lbl_once_time_a.grid_remove()
                sched_once_fr_a.grid_remove()
                lbl_daily_slots_a.grid_remove()
                e_daily_slots_a.grid_remove()
                lbl_delay_min_a.grid_remove()
                delay_min_a.grid_remove()
                lbl_delay_max_a.grid_remove()
                delay_max_a.grid_remove()
                lbl_timezone_a.grid_remove()
                e_timezone_a.grid_remove()
                lbl_step_gap_a.grid()
                ent_step_gap_a.grid()
            elif rule == "once":
                lbl_start_date_a.grid()
                e_start_date_a.grid()
                lbl_once_time_a.grid()
                sched_once_fr_a.grid()
                lbl_daily_slots_a.grid_remove()
                e_daily_slots_a.grid_remove()
                lbl_delay_min_a.grid_remove()
                delay_min_a.grid_remove()
                lbl_delay_max_a.grid_remove()
                delay_max_a.grid_remove()
                lbl_timezone_a.grid_remove()
                e_timezone_a.grid_remove()
                lbl_step_gap_a.grid()
                ent_step_gap_a.grid()
            else:
                lbl_start_date_a.grid()
                e_start_date_a.grid()
                lbl_once_time_a.grid_remove()
                sched_once_fr_a.grid_remove()
                lbl_daily_slots_a.grid()
                e_daily_slots_a.grid()
                lbl_delay_min_a.grid()
                delay_min_a.grid()
                lbl_delay_max_a.grid()
                delay_max_a.grid()
                lbl_timezone_a.grid()
                e_timezone_a.grid()
                lbl_step_gap_a.grid_remove()
                ent_step_gap_a.grid_remove()
            try:
                top.event_generate("<Configure>")
            except tk.TclError:
                pass

        cb_sched_rule_a.bind("<<ComboboxSelected>>", _on_assign_sched_rule_changed)
        _on_assign_sched_rule_changed()

        def _assign_dlg_wrap(_e: Any = None) -> None:
            try:
                w = max(320, int(top.winfo_width()) - 40)
                lbl_sch_hint_a.configure(wraplength=max(280, w - 24))
            except tk.TclError:
                pass

        top.bind("<Configure>", _assign_dlg_wrap, add="+")

        def _clear_schedule_assign_marks() -> None:
            for w in (lbl_daily_slots_a, lbl_delay_min_a, lbl_delay_max_a, lbl_timezone_a):
                try:
                    w.configure(foreground=_default_lbl_fg_a)
                except tk.TclError:
                    pass

        def _parse_daily_slot_strings_assign() -> list[str]:
            raw = e_daily_slots_a.get().strip()
            if not raw:
                lbl_daily_slots_a.configure(foreground="red")
                raise ValueError("Khung giờ/ngày không được để trống.")
            out: list[str] = []
            for token in raw.split(","):
                s = token.strip()
                if not s:
                    continue
                parts = s.split(":")
                if len(parts) != 2:
                    lbl_daily_slots_a.configure(foreground="red")
                    raise ValueError(f"Khung giờ không hợp lệ: {s!r}. Dùng HH:MM.")
                h, mi = int(parts[0]), int(parts[1])
                if not (0 <= h <= 23 and 0 <= mi <= 59):
                    lbl_daily_slots_a.configure(foreground="red")
                    raise ValueError(f"Khung giờ không hợp lệ: {s!r}.")
                out.append(f"{h:02d}:{mi:02d}")
            return sorted(set(out))

        def _resolved_tz_name_assign() -> str:
            name = (e_timezone_a.get() or "").strip() or "Asia/Ho_Chi_Minh"
            try:
                ZoneInfo(name)
                return name
            except Exception:
                lbl_timezone_a.configure(foreground="red")
                return "Asia/Ho_Chi_Minh"

        def _iso_to_local_wall_assign(iso_s: str, tz: ZoneInfo) -> str:
            s2 = str(iso_s or "").strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(s2)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M")

        def _build_assign_plans(n_valid: int) -> tuple[list[dict[str, Any]], str] | None:
            if n_valid < 1:
                return None
            _clear_schedule_assign_marks()
            rule = _assign_sched_rule_key()
            tz_sched = scheduler_tz()
            tz_row_fb = str(getattr(tz_sched, "key", "") or "").strip() or _tz_assign

            if rule == "immediate":
                try:
                    gap = max(0, int(str(var_sch_step.get()).strip() or "0"))
                except ValueError:
                    messagebox.showwarning("Lịch", "«Cách nhau (phút)» phải là số nguyên ≥ 0.", parent=top)
                    return None
                base = datetime.now(tz_sched).replace(second=0, microsecond=0)
                plans_i: list[dict[str, Any]] = []
                for i in range(n_valid):
                    dt = base + timedelta(minutes=i * gap)
                    iso = dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
                    plans_i.append(
                        {
                            "scheduled_at": iso,
                            "wall": dt.strftime("%Y-%m-%d %H:%M"),
                            "schedule_recurrence": "",
                            "schedule_slot": "",
                        }
                    )
                return plans_i, tz_row_fb

            if rule == "once":
                try:
                    gap = max(0, int(str(var_sch_step.get()).strip() or "0"))
                except ValueError:
                    messagebox.showwarning("Lịch", "«Cách nhau (phút)» phải là số nguyên ≥ 0.", parent=top)
                    return None
                try:
                    d_only = parse_date_only_yyyy_mm_dd(str(e_start_date_a.get() or "").strip())
                except Exception:
                    messagebox.showwarning("Sai định dạng", "Ngày bắt đầu phải là YYYY-MM-DD.", parent=top)
                    return None
                try:
                    h0 = int(str(sp_hour_a.get()).strip())
                    m0 = int(str(sp_min_a.get()).strip())
                except ValueError:
                    messagebox.showwarning("Sai giờ", "Giờ và phút phải là số nguyên.", parent=top)
                    return None
                if not (0 <= h0 <= 23 and 0 <= m0 <= 59):
                    messagebox.showwarning("Sai giờ", "Giờ 0–23, phút 0–59.", parent=top)
                    return None
                cur = datetime(d_only.year, d_only.month, d_only.day, h0, m0, 0, tzinfo=tz_sched)
                slot = build_schedule_slot_hhmm(h0, m0)
                plans_once: list[dict[str, Any]] = []
                for i in range(n_valid):
                    dt = cur + timedelta(minutes=i * gap)
                    iso = dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
                    plans_once.append(
                        {
                            "scheduled_at": iso,
                            "wall": dt.strftime("%Y-%m-%d %H:%M"),
                            "schedule_recurrence": "once",
                            "schedule_slot": slot,
                        }
                    )
                return plans_once, tz_row_fb

            tz_out = _resolved_tz_name_assign()
            try:
                d_only_d = parse_date_only_yyyy_mm_dd(str(e_start_date_a.get() or "").strip())
            except Exception:
                messagebox.showwarning("Sai định dạng", "Ngày bắt đầu phải là YYYY-MM-DD.", parent=top)
                return None
            try:
                slots_list = _parse_daily_slot_strings_assign()
            except ValueError as exc:
                messagebox.showerror("Lịch", str(exc), parent=top)
                return None
            try:
                dmin = int(str(delay_min_a.get() or "0").strip())
                dmax = int(str(delay_max_a.get() or "0").strip())
            except ValueError:
                messagebox.showwarning("Lịch", "Delay phải là số nguyên.", parent=top)
                return None
            if dmin < 0 or dmax < 0 or dmin > dmax:
                lbl_delay_min_a.configure(foreground="red")
                lbl_delay_max_a.configure(foreground="red")
                messagebox.showwarning("Lịch", "Delay tối thiểu ≤ delay tối đa (cùng ≥ 0).", parent=top)
                return None
            try:
                tz_z = ZoneInfo(tz_out)
            except Exception:
                tz_z = scheduler_tz()
                tz_out = tz_row_fb
            try:
                raw_plans = build_schedule_by_daily_slots(
                    start_date=d_only_d,
                    time_slots=slots_list,
                    job_count=n_valid,
                    delay_min_minutes=dmin,
                    delay_max_minutes=dmax,
                    timezone_name=tz_out,
                )
            except ValueError as exc:
                messagebox.showerror("Lịch", str(exc), parent=top)
                return None
            if len(slots_list) == 1:
                rec_rr, slot0 = "daily", slots_list[0]
            else:
                rec_rr, slot0 = "", ""
            plans_d: list[dict[str, Any]] = []
            ssd = d_only_d.strftime("%Y-%m-%d")
            slots_csv = ",".join(slots_list)
            for p in raw_plans:
                iso = str(p.get("scheduled_at", "")).strip()
                wall = _iso_to_local_wall_assign(iso, tz_z)
                slot_base = str(p.get("slot_base_local", "")).strip()
                try:
                    dam = int(p.get("delay_applied_min", 0))
                except (TypeError, ValueError):
                    dam = 0
                plans_d.append(
                    {
                        "scheduled_at": iso,
                        "wall": wall,
                        "schedule_recurrence": rec_rr,
                        "schedule_slot": slot0,
                        "schedule_daily_slots": slots_csv,
                        "schedule_delay_min": dmin,
                        "schedule_delay_max": dmax,
                        "schedule_start_date": ssd,
                        "slot_base_local": slot_base,
                        "delay_applied_min": dam,
                    }
                )
            return plans_d, tz_out

        def _write_dest_from_ui_to_items() -> bool:
            items = job.get("items") or []
            if not isinstance(items, list):
                return False
            m = str(var_mode.get() or "").strip()
            for pos in positions:
                if pos < 0 or pos >= len(items):
                    continue
                it = items[pos]
                if not isinstance(it, dict):
                    continue
                for k in (
                    "item_publish_target",
                    "item_preset_fb_account_id",
                    "item_preset_fb_page_id",
                    "item_preset_tiktok_account_id",
                ):
                    it.pop(k, None)
                if m == "inherit":
                    pass
                elif m == "unspecified":
                    it["item_publish_target"] = "unspecified"
                elif m == "facebook":
                    aid = fb_account_map.get(str(var_acc.get() or "").strip(), "").strip()
                    pid = page_map.get(str(var_page.get() or "").strip(), "").strip()
                    if not aid or not pid:
                        messagebox.showwarning(
                            "Thiếu dữ liệu",
                            "Chọn đủ tài khoản và Page Facebook.",
                            parent=top,
                        )
                        return False
                    it["item_publish_target"] = "facebook"
                    it["item_preset_fb_account_id"] = aid
                    it["item_preset_fb_page_id"] = pid
                elif m == "tiktok":
                    tid = tt_account_map.get(str(var_tt.get() or "").strip(), "").strip()
                    if not tid:
                        messagebox.showwarning("Thiếu dữ liệu", "Chọn tài khoản TikTok.", parent=top)
                        return False
                    it["item_publish_target"] = "tiktok"
                    it["item_preset_tiktok_account_id"] = tid
            return True

        def _apply() -> None:
            if not _write_dest_from_ui_to_items():
                return
            top.destroy()
            if self._ve_pending_persist_current_selection():
                messagebox.showinfo("Đã lưu", "Đã cập nhật đích clip vào file job.", parent=self._root)

        def _import_selected_to_schedule() -> None:
            if not _write_dest_from_ui_to_items():
                return
            rec = job
            items = rec.get("items") or []
            if not isinstance(items, list):
                return
            dlg_acc = str(var_acc.get() or "")
            dlg_page = str(var_page.get() or "")
            mode_ui = str(var_mode.get() or "").strip()
            dialog_plat = "TikTok" if mode_ui == "tiktok" else "Facebook"
            ordered = sorted(set(int(p) for p in positions))
            clip_pairs: list[tuple[int, dict[str, Any]]] = []
            for pos in ordered:
                if pos < 0 or pos >= len(items):
                    continue
                it = items[pos]
                if not isinstance(it, dict):
                    continue
                if not str(it.get("video_path") or "").strip():
                    messagebox.showwarning(
                        "Thiếu file",
                        f"Dòng index {pos} chưa có đường dẫn video — thêm file hoặc bỏ chọn dòng đó.",
                        parent=top,
                    )
                    return
                clip_pairs.append((pos, it))
            if not clip_pairs:
                messagebox.showwarning("Trống", "Không có clip hợp lệ để nạp.", parent=top)
                return
            built = _build_assign_plans(len(clip_pairs))
            if built is None:
                return
            plans, tz_row = built
            errs: list[str] = []
            for idx, (_p, it) in enumerate(clip_pairs):
                plat_i = self._ve_resolve_item_publish(it, rec, dialog_plat)
                if plat_i == "unspecified":
                    errs.append(f"Clip {idx + 1}: đích «chờ chọn (clip)» — chọn Facebook/TikTok ở trên.")
                    continue
                if plat_i == "tiktok":
                    aid_tt = self._ve_resolve_item_tt_account(it, rec, dlg_acc, tt_account_map)
                    if not aid_tt:
                        errs.append(f"Clip {idx + 1}: TikTok thiếu tài khoản.")
                else:
                    aid_fb, pid_fb = self._ve_resolve_item_fb_ids(
                        it, rec, dlg_acc, dlg_page, fb_account_map, page_map
                    )
                    if not aid_fb or not pid_fb:
                        errs.append(f"Clip {idx + 1}: Facebook thiếu tài khoản hoặc Page.")
            if errs:
                messagebox.showerror(
                    "Không nạp được",
                    "\n".join(errs[:18]) + (f"\n… (+{len(errs) - 18} lỗi)" if len(errs) > 18 else ""),
                    parent=top,
                )
                return
            job_store = TikTokJobStore()
            pps = page_post_style_for_post_type("video")
            created_fb = 0
            created_tt = 0
            last_fb_aid = ""
            last_fb_pid = ""
            last_tt_aid = ""
            _ts = datetime.now().isoformat(timespec="seconds")
            for idx, (_p, it) in enumerate(clip_pairs):
                vp = str(it.get("video_path") or "").strip()
                plan = plans[idx]
                plat_i = self._ve_resolve_item_publish(it, rec, dialog_plat)
                if plat_i == "tiktok":
                    aid_tt = self._ve_resolve_item_tt_account(it, rec, dlg_acc, tt_account_map)
                    raw_c = str(it.get("content") or "").strip()
                    line = internal_post_title_from_body(raw_c, fallback="")
                    if not line:
                        line = internal_post_title_from_body(
                            str(it.get("title") or Path(vp).stem).strip(), fallback=Path(vp).stem
                        )
                    tags = [str(x).strip() for x in (it.get("hashtags") or []) if str(x).strip()]
                    job_tt = default_job_dict(account_id=aid_tt, video_path=vp, caption=line, hashtags=tags)
                    job_tt["schedule_enabled"] = True
                    job_tt["scheduled_at"] = plan["scheduled_at"]
                    job_tt["schedule_time"] = plan["wall"]
                    job_tt["created_by"] = "video_editor_saved_job_import"
                    job_tt["source_project_id"] = str(rec.get("source_project_id") or "")
                    job_tt["source_download_job_id"] = str(rec.get("source_download_job_id") or "")
                    job_tt["source_download_job_label"] = str(rec.get("source_download_job_label") or "")
                    job_tt["source_download_video_id"] = str(it.get("source_download_video_id") or "")
                    job_store.upsert(job_tt)
                    it["item_scheduled_platform"] = "tiktok"
                    it["item_scheduled_job_id"] = str(job_tt.get("id") or "")
                    it["item_scheduled_at"] = _ts
                    created_tt += 1
                    last_tt_aid = aid_tt or last_tt_aid
                else:
                    aid_fb, pid_fb = self._ve_resolve_item_fb_ids(
                        it, rec, dlg_acc, dlg_page, fb_account_map, page_map
                    )
                    raw_fb = str(it.get("content") or "").strip()
                    line_fb = internal_post_title_from_body(raw_fb, fallback="")
                    if not line_fb:
                        line_fb = internal_post_title_from_body(
                            str(it.get("title") or Path(vp).stem).strip(), fallback=Path(vp).stem
                        )
                    row_fb: dict[str, Any] = {
                        "id": f"sched_{uuid.uuid4().hex[:10]}",
                        "page_id": pid_fb,
                        "account_id": aid_fb,
                        "post_type": "video",
                        "page_post_style": pps,
                        "title": line_fb,
                        "content": line_fb,
                        "hashtags": [str(x).strip() for x in (it.get("hashtags") or []) if str(x).strip()],
                        "media_files": [vp],
                        "video_path": vp,
                        "scheduled_at": plan["scheduled_at"],
                        "timezone": tz_row,
                        "schedule_recurrence": str(plan.get("schedule_recurrence") or ""),
                        "schedule_slot": str(plan.get("schedule_slot") or ""),
                        "status": "pending",
                        "retry_count": 0,
                        "max_retry": 3,
                        "created_by": "video_editor_saved_job_import",
                        "created_at": datetime.now().isoformat(timespec="seconds"),
                        "source_project_id": str(rec.get("source_project_id") or ""),
                        "source_download_job_id": str(rec.get("source_download_job_id") or ""),
                        "source_download_job_label": str(rec.get("source_download_job_label") or ""),
                        "source_download_video_id": str(it.get("source_download_video_id") or ""),
                    }
                    if plan.get("schedule_daily_slots"):
                        row_fb["schedule_daily_slots"] = str(plan["schedule_daily_slots"])
                        row_fb["schedule_delay_min"] = int(plan["schedule_delay_min"])
                        row_fb["schedule_delay_max"] = int(plan["schedule_delay_max"])
                        row_fb["schedule_start_date"] = str(plan["schedule_start_date"])
                    sbl = str(plan.get("slot_base_local") or "").strip()
                    if sbl:
                        row_fb["slot_base_local"] = sbl[:80]
                    if "delay_applied_min" in plan:
                        row_fb["schedule_delay_applied_min"] = max(0, min(180, int(plan["delay_applied_min"])))
                    self._schedule_posts.upsert(row_fb)  # type: ignore[arg-type]
                    it["item_scheduled_platform"] = "facebook"
                    it["item_scheduled_job_id"] = str(row_fb["id"])
                    it["item_scheduled_at"] = _ts
                    created_fb += 1
                    last_fb_aid, last_fb_pid = aid_fb, pid_fb
            if not self._ve_pending_persist_current_selection():
                messagebox.showerror(
                    "Lỗi lưu",
                    "Đã tạo job lịch nhưng không ghi được file job chờ (trạng thái clip). Kiểm tra quyền ghi file.",
                    parent=self._root,
                )
                return
            src_hint = str(rec.get("source_download_job_label") or rec.get("source_download_job_id") or "").strip()
            if created_fb:
                if hasattr(self, "_var_jobs_filter_status"):
                    self._var_jobs_filter_status.set("pending")
                if hasattr(self, "_var_jobs_filter_account"):
                    self._var_jobs_filter_account.set(str(var_acc.get() or "Tất cả account"))
                if hasattr(self, "_var_jobs_filter_page"):
                    self._var_jobs_filter_page.set(str(var_page.get() or "Tất cả page"))
                if hasattr(self, "_var_jobs_filter_retry"):
                    self._var_jobs_filter_retry.set("Retry: tất cả")
                if hasattr(self, "_var_jobs_search"):
                    self._var_jobs_search.set(src_hint or "video_editor_saved_job_import")
            self._fill_schedule_jobs_tree()
            msg_done = (
                f"Facebook: {created_fb} job lịch; TikTok: {created_tt} job (có lịch)."
                if (created_fb and created_tt)
                else (
                    f"Đã nạp {created_tt} job TikTok (có lịch)."
                    if created_tt
                    else f"Đã nạp {created_fb} job lịch Facebook."
                )
            )
            top.destroy()
            messagebox.showinfo("Hoàn tất", msg_done, parent=self._root)

        bf = ttk.Frame(frm)
        bf.grid(row=1, column=0, sticky="sew", pady=(12, 0))
        ttk.Button(bf, text="Hủy", command=top.destroy).pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Button(bf, text="Áp dụng và lưu", command=_apply).pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Button(bf, text="Nạp vào job lịch", command=_import_selected_to_schedule).pack(side=tk.RIGHT, padx=(6, 0))

        top.grab_set()
        top.wait_window()

    def _ve_pending_get_meta_section_open(self, title: str) -> bool:
        key = str(title or "").strip()
        if key not in self._ve_pending_meta_section_open:
            defaults = {"Thông tin chung": True, "Nguồn & liên kết": False, "Đăng & gợi ý đích": False}
            self._ve_pending_meta_section_open[key] = bool(defaults.get(key, True))
        return bool(self._ve_pending_meta_section_open[key])

    def _ve_pending_build_meta_collapsible_block(
        self,
        parent: ttk.Frame,
        title: str,
        pairs: list[tuple[str, str]],
        *,
        wrap_val: int = 560,
    ) -> None:
        outer = ttk.Frame(parent)
        outer.pack(fill=tk.X, pady=(0, 6))
        hdr = ttk.Frame(outer)
        hdr.pack(fill=tk.X)
        expanded = self._ve_pending_get_meta_section_open(title)
        head_lbl = ttk.Label(
            hdr,
            text=("▼ " if expanded else "▶ ") + title,
            font=("Segoe UI", 9, "bold"),
            cursor="hand2",
        )
        head_lbl.pack(side=tk.LEFT, anchor="w")
        hint = ttk.Label(hdr, text="  (bấm để mở / thu)", foreground="#888", font=("Segoe UI", 8))
        hint.pack(side=tk.LEFT, anchor="w")

        body = ttk.Frame(outer, padding=(4, 2, 8, 6))
        body.columnconfigure(1, weight=1)
        for i, (key, val) in enumerate(pairs):
            v = (val or "").strip() or "—"
            ttk.Label(body, text=key, foreground="#555", font=("Segoe UI", 9)).grid(
                row=i, column=0, sticky="ne", padx=(0, 12), pady=3
            )
            ttk.Label(
                body,
                text=v,
                wraplength=wrap_val,
                justify="left",
                font=("Segoe UI", 9),
            ).grid(row=i, column=1, sticky="ew", pady=3)

        def _sync_header(is_open: bool) -> None:
            head_lbl.configure(text=("▼ " if is_open else "▶ ") + title)

        def _toggle(_event: tk.Event | None = None) -> None:
            cur = not self._ve_pending_get_meta_section_open(title)
            self._ve_pending_meta_section_open[str(title).strip()] = cur
            if cur:
                body.pack(fill=tk.X, pady=(2, 0))
            else:
                body.pack_forget()
            _sync_header(cur)

        head_lbl.bind("<Button-1>", _toggle)
        hint.bind("<Button-1>", _toggle)

        _sync_header(expanded)
        if expanded:
            body.pack(fill=tk.X, pady=(2, 0))

    def _refresh_ve_pending_job_detail(self) -> None:
        if not hasattr(self, "_frm_ve_pending_job_meta"):
            return
        lbl = str(self._var_ve_pending_job.get() or "").strip()
        r = self._ve_pending_job_by_label.get(lbl) if lbl else None

        for i in self._tree_ve_pending_videos.get_children():
            self._tree_ve_pending_videos.delete(i)

        for w in self._frm_ve_pending_job_meta.winfo_children():
            w.destroy()

        if not isinstance(r, dict):
            ttk.Label(
                self._frm_ve_pending_job_meta,
                text="Chưa có job nào hoặc chưa chọn job trong ô «Chọn job».",
                foreground="#666",
                font=("Segoe UI", 9),
            ).pack(anchor="w", pady=(4, 0))
            return

        tgt_raw = str(r.get("publish_target") or "").strip().lower()
        tgt_disp = {"facebook": "Facebook", "tiktok": "TikTok", "unspecified": "Chờ chọn"}.get(tgt_raw, tgt_raw or "—")

        try:
            n_items = len(r.get("items") or [])
        except TypeError:
            n_items = 0

        self._ve_pending_build_meta_collapsible_block(
            self._frm_ve_pending_job_meta,
            "Thông tin chung",
            [
                ("ID job", str(r.get("id") or "")),
                ("Tên job", str(r.get("job_name") or "")),
                ("Trạng thái", str(r.get("status") or "").strip() or "saved"),
                ("Tạo lúc", str(r.get("created_at") or "").strip()),
                ("Đích đăng (publish_target)", tgt_disp),
                ("Số video trong job", str(n_items)),
                (
                    "Lịch & xóa clip",
                    "Cột «Trạng thái lịch» đồng bộ với job Facebook/TikTok đã nạp. Xóa dòng clip ở đây chỉ xóa trong file job chờ — không xóa job lịch đã tạo. Nếu job lịch bị xóa ở tab lịch, mở lại job này sẽ tự dọn liên kết trên clip.",
                ),
            ],
        )
        self._ve_pending_build_meta_collapsible_block(
            self._frm_ve_pending_job_meta,
            "Nguồn & liên kết",
            [
                ("Loại nguồn (source_type)", str(r.get("source_type") or "").strip()),
                ("Dự án Video Editor", str(r.get("source_project_id") or "").strip()),
                ("Download job (id)", str(r.get("source_download_job_id") or "").strip()),
                ("Download job (nhãn)", str(r.get("source_download_job_label") or "").strip()),
            ],
        )
        self._ve_pending_build_meta_collapsible_block(
            self._frm_ve_pending_job_meta,
            "Đăng & gợi ý đích",
            [
                ("Đã import lúc", str(r.get("imported_at") or "").strip()),
                ("Nền tảng đã import", str(r.get("imported_to_platform") or "").strip()),
                ("Account đã import", str(r.get("imported_to_account_id") or "").strip()),
                ("Page đã import", str(r.get("imported_to_page_id") or "").strip()),
                ("Preset — FB account id", str(r.get("preset_fb_account_id") or "").strip()),
                ("Preset — FB page id", str(r.get("preset_fb_page_id") or "").strip()),
                ("Preset — TikTok account id", str(r.get("preset_tiktok_account_id") or "").strip()),
            ],
        )

        fb_acc_lbl: dict[str, str] = {}
        fb_page_lbl: dict[str, str] = {}
        tt_lbl: dict[str, str] = {}
        try:
            for a in self._accounts.load_all():
                if isinstance(a, dict):
                    _, lb, aid = self._ve_import_fb_account_row(a)
                    if aid:
                        fb_acc_lbl[aid] = lb
            for p in self._pages.load_all():
                if isinstance(p, dict):
                    _, lb, pid = self._ve_import_fb_page_row(p)
                    if pid:
                        fb_page_lbl[pid] = lb
            for a in TikTokAccountStore().load_all():
                if isinstance(a, dict):
                    _, lb, aid = self._ve_import_tt_account_row(a)
                    if aid:
                        tt_lbl[aid] = lb
        except Exception:
            pass

        items = r.get("items") or []
        if not isinstance(items, list):
            items = []
        tt_sched_store = TikTokJobStore()
        sched_cleared_any = False
        for _it in items:
            if isinstance(_it, dict) and self._ve_pending_sync_item_schedule_ref(_it, tt_store=tt_sched_store):
                sched_cleared_any = True
        if sched_cleared_any:
            self._ve_pending_persist_current_selection()

        for pos, it in enumerate(items):
            if not isinstance(it, dict):
                continue
            idx = pos + 1
            title = self._ve_pending_truncate(str(it.get("title") or ""), 200)
            content = self._ve_pending_truncate(str(it.get("content") or ""), 400)
            tags_raw = it.get("hashtags")
            if isinstance(tags_raw, list):
                ht = " ".join(f"#{str(x).strip().lstrip('#')}" for x in tags_raw if str(x).strip())
            else:
                ht = str(tags_raw or "").strip()
            ht = self._ve_pending_truncate(ht, 160)
            vp = str(it.get("video_path") or "").strip()
            if not vp:
                path_disp = "—"
            else:
                try:
                    path_disp = str(Path(vp).name)
                except Exception:
                    path_disp = self._ve_pending_truncate(vp, 120)
            src_id = str(it.get("source_download_video_id") or "").strip() or "—"
            clip_st = "—"
            if vp:
                try:
                    clip_st = "Có file" if Path(vp).is_file() else "Thiếu file"
                except OSError:
                    clip_st = "?"
            else:
                clip_st = "Chưa có đường dẫn"
            dest_disp = self._ve_pending_format_item_dest_disp(
                it, r, fb_acc_lbl=fb_acc_lbl, fb_page_lbl=fb_page_lbl, tt_lbl=tt_lbl
            )
            sched_disp = self._ve_pending_item_schedule_status_disp(it, tt_store=tt_sched_store)

            self._tree_ve_pending_videos.insert(
                "",
                tk.END,
                iid=str(pos),
                values=(idx, title, content, ht, path_disp, src_id, clip_st, sched_disp, dest_disp),
            )

    def _on_delete_ve_pending_export_jobs(self) -> None:
        jid = str(getattr(self, "_ve_pending_selected_id", "") or "").strip()
        if not jid:
            messagebox.showwarning(
                "Chưa chọn",
                "Chọn một job trong ô «Chọn job» rồi bấm «Xóa job chọn».",
                parent=self._root,
            )
            return
        lbl = str(self._var_ve_pending_job.get() or "").strip()
        jn = ""
        r = self._ve_pending_job_by_label.get(lbl)
        if isinstance(r, dict):
            jn = str(r.get("job_name") or "").strip()
        preview = jn or jid
        if not messagebox.askyesno(
            "Xóa job chờ",
            f"Xóa job «{preview}» ({jid}) khỏi file video_editor_schedule_jobs.json?\n"
            "Thao tác này không thể hoàn tác.",
            parent=self._root,
        ):
            return
        rows = self._load_saved_export_schedule_jobs()
        new_rows = [row for row in rows if not (isinstance(row, dict) and str(row.get("id") or "").strip() == jid)]
        if len(new_rows) == len(rows):
            messagebox.showinfo(
                "Không xóa",
                "Không tìm thấy job trong file (có thể đã bị xóa hoặc id không khớp).",
                parent=self._root,
            )
            self._fill_ve_pending_export_jobs_tree()
            return
        try:
            self._save_saved_export_schedule_jobs(new_rows)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lỗi lưu file", str(exc), parent=self._root)
            return
        self._ve_pending_selected_id = ""
        self._fill_ve_pending_export_jobs_tree()
        logger.info("Đã xóa job chờ export id={}", jid)

    def _on_ve_pending_videos_double_click(self, event: tk.Event) -> None:
        tv = self._tree_ve_pending_videos
        if str(tv.identify_region(event.x, event.y) or "") != "cell":
            self._on_ve_pending_export_double_click()
            return
        cid_raw = str(tv.identify_column(event.x) or "").lstrip("#")
        try:
            cix = int(cid_raw) - 1
        except ValueError:
            cix = -1
        cols = tv.cget("columns")
        if isinstance(cols, str):
            col_names = tuple(str(cols).split())
        else:
            col_names = tuple(cols)
        cname = col_names[cix] if 0 <= cix < len(col_names) else ""
        if cname == "dest":
            row = tv.identify_row(event.y)
            if not row:
                return
            try:
                pos = int(str(row))
            except ValueError:
                return
            lbl = str(self._var_ve_pending_job.get() or "").strip()
            cur = self._ve_pending_job_by_label.get(lbl)
            if isinstance(cur, dict):
                self._ve_pending_open_assign_dest_dialog([pos], cur)
            return
        self._on_ve_pending_export_double_click()

    def _on_ve_pending_export_double_click(self, _event: tk.Event | None = None) -> None:
        jid = str(getattr(self, "_ve_pending_selected_id", "") or "").strip()
        if not jid:
            return
        try:
            setattr(self._root, "_ve_saved_export_job_id", jid)
        except Exception:
            pass
        self._on_import_saved_export_job()

    def _on_open_ve_pending_export_jobs_folder(self) -> None:
        """Mở Explorer/Finder tới thư mục chứa đúng file ``video_editor_schedule_jobs.json`` đang dùng."""
        p = video_editor_schedule_jobs_json_path()
        d = p.parent
        try:
            d.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            messagebox.showerror("Folder", str(exc), parent=self._root)
            return
        try:
            if os.name == "nt":
                os.startfile(str(d))  # type: ignore[attr-defined]
            else:
                subprocess.Popen(["xdg-open", str(d)])
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Folder", str(exc), parent=self._root)

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

    def _refresh_job_account_name_map(self) -> None:
        """Nạp map ``account_id -> name`` cho combobox lọc và cột account."""
        mp: dict[str, str] = {}
        try:
            for a in self._accounts.load_all():
                aid = str(a.get("id", "")).strip()
                if not aid:
                    continue
                name = str(a.get("name", "") or "").strip()
                mp[aid] = name or aid
        except Exception as exc:  # noqa: BLE001
            logger.debug("Không nạp được map tên account cho tab jobs: {}", exc)
        self._job_account_name_by_id = mp

    def _job_page_display(self, page_id: str) -> str:
        pid = str(page_id or "").strip()
        if not pid:
            return ""
        return format_page_filter_label(pid, self._job_page_name_by_id.get(pid, ""))

    def _job_account_display(self, account_id: str) -> str:
        aid = str(account_id or "").strip()
        if not aid:
            return ""
        return format_account_filter_label(aid, self._job_account_name_by_id.get(aid, ""))

    def _resolve_jobs_filter_account_id(self, display: str) -> str:
        disp = str(display or "").strip()
        if not disp or disp == "Tất cả account":
            return ""
        return self._jobs_filter_account_label_to_id.get(disp, disp)

    def _resolve_jobs_filter_page_id(self, display: str) -> str:
        disp = str(display or "").strip()
        if not disp or disp == "Tất cả page":
            return ""
        return self._jobs_filter_page_label_to_id.get(disp, disp)

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
        self._cb_jobs_filter_account = ttk.Combobox(fr, textvariable=self._var_jobs_filter_account, state="readonly", width=32)
        self._cb_jobs_filter_account.grid(row=0, column=2, padx=(0, 4))
        self._cb_jobs_filter_account.bind("<<ComboboxSelected>>", lambda _e: self._render_schedule_jobs_tree())

        self._var_jobs_filter_page = tk.StringVar(value="Tất cả page")
        self._cb_jobs_filter_page = ttk.Combobox(fr, textvariable=self._var_jobs_filter_page, state="readonly", width=32)
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
                "ready_queue",
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
        """Cập nhật options Account/Page combobox theo dữ liệu đang có (tên + id)."""
        if not hasattr(self, "_cb_jobs_filter_account"):
            return
        accs = sorted({str(j.get("account_id", "")).strip() for j in self._all_jobs if j.get("account_id")})
        pages = sorted({str(j.get("page_id", "")).strip() for j in self._all_jobs if j.get("page_id")})

        acc_label_to_id: dict[str, str] = {}
        acc_labels = ["Tất cả account"]
        for aid in accs:
            lbl = format_account_filter_label(aid, self._job_account_name_by_id.get(aid, ""))
            acc_label_to_id[lbl] = aid
            acc_labels.append(lbl)
        self._jobs_filter_account_label_to_id = acc_label_to_id

        pg_label_to_id: dict[str, str] = {}
        pg_labels = ["Tất cả page"]
        for pid in pages:
            lbl = format_page_filter_label(pid, self._job_page_name_by_id.get(pid, ""))
            pg_label_to_id[lbl] = pid
            pg_labels.append(lbl)
        self._jobs_filter_page_label_to_id = pg_label_to_id

        cur_acc_id = self._resolve_jobs_filter_account_id(self._var_jobs_filter_account.get())
        self._cb_jobs_filter_account.configure(values=tuple(acc_labels))
        if cur_acc_id and cur_acc_id in accs:
            self._var_jobs_filter_account.set(
                format_account_filter_label(cur_acc_id, self._job_account_name_by_id.get(cur_acc_id, ""))
            )
        elif self._var_jobs_filter_account.get() not in acc_labels:
            self._var_jobs_filter_account.set("Tất cả account")

        cur_pg_id = self._resolve_jobs_filter_page_id(self._var_jobs_filter_page.get())
        self._cb_jobs_filter_page.configure(values=tuple(pg_labels))
        if cur_pg_id and cur_pg_id in pages:
            self._var_jobs_filter_page.set(
                format_page_filter_label(cur_pg_id, self._job_page_name_by_id.get(cur_pg_id, ""))
            )
        elif self._var_jobs_filter_page.get() not in pg_labels:
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
            "account": self._resolve_jobs_filter_account_id(self._var_jobs_filter_account.get()),
            "page_id": self._resolve_jobs_filter_page_id(self._var_jobs_filter_page.get()),
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

    _JOBS_TREE_INSERT_CHUNK = 45
    _JOBS_TREE_ASYNC_MIN = 35

    def _job_tree_row_values(self, j: dict[str, Any]) -> tuple[Any, ...]:
        tit = str(j.get("title", "") or "")
        if len(tit) > 40:
            tit = tit[:37] + "…"
        img_prompt = str(j.get("image_prompt", "") or "")
        if len(img_prompt) > 90:
            img_prompt = img_prompt[:87] + "…"
        miss_txt = format_missing_fields_for_display(j.get("_missing_fields") or [])
        return (
            j.get("id", ""),
            self._job_page_display(str(j.get("page_id", "") or "")),
            self._job_account_display(str(j.get("account_id", "") or "")),
            j.get("post_type", ""),
            j.get("ai_language", ""),
            tit,
            img_prompt,
            j.get("_display_scheduled_local", "—"),
            self._job_status_with_retry(j),
            self._schedule_job_error_display(j),
            j.get("retry_count", 0),
            miss_txt,
        )

    def _jobs_tree_missing_filter_preset(self) -> dict[str, Any]:
        if not hasattr(self, "_var_jobs_filter_missing"):
            return preset_by_label("")
        return preset_by_label(self._var_jobs_filter_missing.get())

    def _prepare_jobs_tree_render_pack(
        self,
        jobs: list[dict[str, Any]],
        *,
        filters: dict[str, Any],
        sort_key: str,
        sort_asc: bool,
        missing_preset: dict[str, Any],
    ) -> tuple[list[tuple[Any, ...]], list[dict[str, Any]]]:
        for j in jobs:
            j["_display_scheduled_local"] = self._format_scheduled_for_ui(j)
            j["_missing_fields"] = get_missing_fields(j)
        filtered = apply_job_filters(jobs, **filters)
        if missing_preset.get("match_mode") != "none" and missing_preset.get("fields"):
            filtered = filter_jobs_by_missing_fields(
                filtered,
                missing_preset["fields"],
                match_mode=missing_preset.get("match_mode", "any"),
            )
        filtered = sort_jobs(filtered, sort_key=sort_key, ascending=sort_asc)
        rows = [self._job_tree_row_values(j) for j in filtered]
        return rows, filtered

    def _insert_job_tree_rows_chunked(
        self,
        rows: list[tuple[Any, ...]],
        *,
        token: int,
        start: int = 0,
    ) -> None:
        if token != self._jobs_tree_render_gen:
            return
        chunk = self._JOBS_TREE_INSERT_CHUNK
        end = min(start + chunk, len(rows))
        for i in range(start, end):
            self._tree_jobs.insert("", tk.END, values=rows[i])
        if end < len(rows):
            self._root.after(1, lambda t=token, n=end: self._insert_job_tree_rows_chunked(rows, token=t, start=n))
            return
        self._update_schedule_jobs_sort_indicator()
        self._update_schedule_jobs_stats_label()
        self._clear_ui_busy()

    def _apply_jobs_tree_render_pack(
        self,
        *,
        token: int,
        rows: list[tuple[Any, ...]] | None,
        filtered: list[dict[str, Any]] | None,
        err: str | None = None,
    ) -> None:
        if token != self._jobs_tree_render_gen:
            self._clear_ui_busy()
            return
        if err or rows is None or filtered is None:
            logger.warning("Render job tree lỗi: {}", err or "unknown")
            self._clear_ui_busy()
            return
        self._filtered_jobs = filtered
        try:
            children = self._tree_jobs.get_children()
            if children:
                self._tree_jobs.delete(*children)
        except tk.TclError:
            pass
        if not rows:
            self._update_schedule_jobs_sort_indicator()
            self._update_schedule_jobs_stats_label()
            self._clear_ui_busy()
            return
        self._insert_job_tree_rows_chunked(rows, token=token, start=0)

    def _render_schedule_jobs_tree(self) -> None:
        """Filter/sort + rebuild Treeview; tác vụ nặng chạy nền, insert từng lô trên main thread."""
        self._jobs_tree_render_gen += 1
        token = self._jobs_tree_render_gen
        jobs = list(self._all_jobs)
        if not jobs:
            try:
                children = self._tree_jobs.get_children()
                if children:
                    self._tree_jobs.delete(*children)
            except tk.TclError:
                pass
            self._filtered_jobs = []
            self._update_schedule_jobs_sort_indicator()
            self._update_schedule_jobs_stats_label()
            return

        filters = self._current_jobs_filters()
        sort_key = self._jobs_sort_key
        sort_asc = self._jobs_sort_asc
        missing_preset = self._jobs_tree_missing_filter_preset()

        if len(jobs) < self._JOBS_TREE_ASYNC_MIN:
            try:
                rows, filtered = self._prepare_jobs_tree_render_pack(
                    jobs,
                    filters=filters,
                    sort_key=sort_key,
                    sort_asc=sort_asc,
                    missing_preset=missing_preset,
                )
                self._apply_jobs_tree_render_pack(token=token, rows=rows, filtered=filtered)
            except Exception as exc:  # noqa: BLE001
                self._apply_jobs_tree_render_pack(token=token, rows=None, filtered=None, err=str(exc))
            return

        self._set_ui_busy("Làm mới bảng job")

        def _worker() -> None:
            err: str | None = None
            rows: list[tuple[Any, ...]] | None = None
            filtered: list[dict[str, Any]] | None = None
            try:
                rows, filtered = self._prepare_jobs_tree_render_pack(
                    jobs,
                    filters=filters,
                    sort_key=sort_key,
                    sort_asc=sort_asc,
                    missing_preset=missing_preset,
                )
            except Exception as exc:  # noqa: BLE001
                err = str(exc)

            def _done() -> None:
                self._apply_jobs_tree_render_pack(
                    token=token, rows=rows, filtered=filtered, err=err
                )

            try:
                self._root.after(0, _done)
            except tk.TclError:
                pass

        threading.Thread(target=_worker, name="jobs_tree_render", daemon=True).start()

    def _schedule_job_error_display(self, job: dict[str, Any], *, max_len: int = 120) -> str:
        """Rút gọn ``error_note`` để hiện trên cây job."""
        note = str(job.get("error_note") or "").strip()
        if not note:
            st = str(job.get("status") or "").strip().lower()
            if st in {"failed", "need_manual_check"}:
                return "(chưa ghi chi tiết — xem failed_accounts.log)"
            return ""
        if len(note) <= max_len:
            return note
        return note[: max_len - 1] + "…"

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
        ready = sum(1 for j in self._all_jobs if str(j.get("status", "")).lower() == "ready_queue")
        running = sum(1 for j in self._all_jobs if str(j.get("status", "")).lower() == "running")
        failed = sum(1 for j in self._all_jobs if str(j.get("status", "")).lower() == "failed")
        success = sum(1 for j in self._all_jobs if str(j.get("status", "")).lower() == "success")
        self._lbl_jobs_stats.configure(
            text=(
                f"Tổng: {total}  |  Đang hiển thị: {shown}  |  Đang chọn: {sel}  "
                f"|  pending: {pending}  ready: {ready}  running: {running}  "
                f"failed: {failed}  success: {success}"
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
        want_ids = {
            str(j.get("id", "")).strip()
            for j in self._filtered_jobs
            if str(j.get("status", "")).strip().lower() == target
        }
        sel: list[str] = []
        for iid in self._tree_jobs.get_children():
            vals = self._tree_jobs.item(iid, "values")
            if vals and str(vals[0]).strip() in want_ids:
                sel.append(iid)
        if sel:
            self._tree_jobs.selection_set(sel)
        else:
            cur = self._tree_jobs.selection()
            if cur:
                self._tree_jobs.selection_remove(*cur)
        self._update_schedule_jobs_stats_label()

    def _selected_schedule_job_ids(self) -> list[str]:
        """Lấy ``id`` job từ các dòng Treeview đang chọn."""
        out: list[str] = []
        for iid in self._tree_jobs.selection():
            vals = self._tree_jobs.item(iid, "values")
            if not vals:
                continue
            jid = str(vals[0]).strip()
            if jid:
                out.append(jid)
        return out

    def _on_jobs_bulk_edit_content(self) -> None:
        """
        Sửa hàng loạt ``title`` + ``content`` (chung) và ``hashtags`` cho job đã chọn.

        Mặc định chỉ cập nhật job ``pending`` trong tập chọn.
        """
        job_ids = self._selected_schedule_job_ids()
        if not job_ids:
            messagebox.showwarning(
                "Chưa chọn job",
                "Chọn một hoặc nhiều job trong bảng.\n"
                "Gợi ý: lọc Page + trạng thái «pending» → «Chọn pending» → «Sửa nội dung hàng loạt…».",
                parent=self._root,
            )
            return

        jobs_by_id: dict[str, dict[str, Any]] = {}
        for jid in job_ids:
            row = self._schedule_posts.get_by_id(jid)
            if row:
                jobs_by_id[jid] = dict(row)

        if not jobs_by_id:
            messagebox.showwarning("Không có dữ liệu", "Không đọc được job đã chọn.", parent=self._root)
            return

        pending_ids = [jid for jid, j in jobs_by_id.items() if str(j.get("status", "")).lower() == "pending"]
        page_filter = self._resolve_jobs_filter_page_id(self._var_jobs_filter_page.get())
        page_name = self._job_page_name_by_id.get(page_filter, "") if page_filter else ""

        top = tk.Toplevel(self._root)
        top.title("Sửa nội dung hàng loạt")
        top.transient(self._root)
        top.grab_set()
        top.columnconfigure(0, weight=1)

        hdr = (
            f"Đã chọn {len(job_ids)} job"
            + (f" · Page: {page_name or page_filter}" if page_filter else "")
            + f" · {len(pending_ids)} pending"
        )
        ttk.Label(top, text=hdr, wraplength=520).grid(row=0, column=0, sticky="w", padx=12, pady=(12, 6))

        var_pending_only = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            top,
            text=f"Chỉ cập nhật job pending ({len(pending_ids)}/{len(job_ids)})",
            variable=var_pending_only,
        ).grid(row=1, column=0, sticky="w", padx=12)

        body = ttk.Frame(top, padding=(12, 4))
        body.grid(row=2, column=0, sticky="nsew")
        body.columnconfigure(1, weight=1)
        top.rowconfigure(2, weight=1)

        var_apply_title = tk.BooleanVar(value=True)
        ttk.Checkbutton(body, text="Tiêu đề + Nội dung (dùng chung)", variable=var_apply_title).grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 4)
        )
        txt_title = tk.Text(body, height=4, width=58, wrap=tk.WORD)
        txt_title.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        sample = next(iter(jobs_by_id.values()), {})
        init_body = str(sample.get("content") or sample.get("title") or "").strip()
        if init_body:
            txt_title.insert("1.0", init_body)

        var_apply_ht = tk.BooleanVar(value=True)
        ttk.Checkbutton(body, text="Hashtags (phẩy hoặc xuống dòng)", variable=var_apply_ht).grid(
            row=2, column=0, columnspan=2, sticky="w", pady=(0, 4)
        )
        ent_ht = ttk.Entry(body, width=60)
        ent_ht.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        ht0 = sample.get("hashtags") or []
        if isinstance(ht0, list) and ht0:
            ent_ht.insert(0, ", ".join(str(x) for x in ht0))

        ttk.Label(
            body,
            text="Chỉ các trường được tick: thay mới hoàn toàn (bỏ cũ), không nối/ghép. "
            "Trường không tick — giữ nguyên từng job.",
            foreground="gray",
            wraplength=520,
        ).grid(row=4, column=0, columnspan=2, sticky="w")

        lbl_progress = ttk.Label(top, text="", foreground="gray")
        lbl_progress.grid(row=3, column=0, sticky="w", padx=12, pady=(4, 0))

        btn_fr = ttk.Frame(top)
        btn_fr.grid(row=4, column=0, sticky="e", padx=12, pady=(8, 12))

        def _set_progress(done: int, total: int, jid: str = "") -> None:
            short = jid[:14] + "…" if len(jid) > 14 else jid
            lbl_progress.configure(text=f"Đang lưu {done}/{total}… {short}")
            if hasattr(self, "_lbl_jobs_regen_status"):
                self._lbl_jobs_regen_status.configure(text=f"Sửa hàng loạt {done}/{total}")

        def _apply() -> None:
            targets = list(pending_ids) if var_pending_only.get() else list(jobs_by_id.keys())
            if not targets:
                messagebox.showwarning(
                    "Không có job pending",
                    "Không có job pending trong tập chọn.\nBỏ tick «Chỉ pending» để sửa mọi trạng thái.",
                    parent=top,
                )
                return
            patch: dict[str, Any] = {}
            if var_apply_title.get():
                line = txt_title.get("1.0", "end").strip()
                if line:
                    patch["title"] = line
                    patch["content"] = line
            if var_apply_ht.get():
                # Ghi đè toàn bộ mảng hashtag (ô trống = xóa hashtag cũ).
                patch["hashtags"] = split_hashtags_csv(ent_ht.get())
            if not patch:
                messagebox.showwarning(
                    "Chưa chọn trường",
                    "Tick ít nhất một trường cần thay:\n"
                    "• Tiêu đề + Nội dung — nhập chữ mới\n"
                    "• Hashtags — nhập danh sách mới (ô trống = xóa hashtag cũ)",
                    parent=top,
                )
                return
            if var_apply_title.get() and not patch.get("title"):
                messagebox.showwarning(
                    "Thiếu tiêu đề/nội dung",
                    "Đã tick «Tiêu đề + Nội dung» nhưng ô trống — nhập nội dung hoặc bỏ tick.",
                    parent=top,
                )
                return
            replace_note = []
            if "title" in patch:
                replace_note.append("title + content (thay mới)")
            if "hashtags" in patch:
                n_ht = len(patch["hashtags"])
                replace_note.append(f"hashtags ({n_ht} tag, thay mới)" if n_ht else "hashtags (xóa hết)")
            if not messagebox.askyesno(
                "Xác nhận ghi đè",
                f"Cập nhật {len(targets)} job?\n"
                f"Thay thế: {', '.join(replace_note)}\n"
                "Các trường khác (lịch, video, AI, trạng thái…) giữ nguyên.\n\n"
                "Chạy lần lượt trên nền — cửa sổ không bị treo.",
                parent=top,
            ):
                return

            btn_apply.configure(state=tk.DISABLED)
            btn_cancel.configure(state=tk.DISABLED)
            top.grab_release()
            self._set_ui_busy("Sửa hàng loạt job")
            _set_progress(0, len(targets))
            target_list = list(targets)
            target_set = set(targets)
            mgr = self._schedule_posts

            def _on_prog(done: int, total: int, jid: str) -> None:
                try:
                    self._root.after(0, lambda d=done, t=total, j=jid: _set_progress(d, t, j))
                except tk.TclError:
                    pass

            def _worker() -> tuple[int, dict[str, Any]]:
                ok_n = mgr.update_jobs_fields_sequential(
                    target_list,
                    fields=patch,
                    on_progress=_on_prog,
                    step_delay_sec=0.03,
                )
                return ok_n, patch

            def _on_done(result: tuple[int, dict[str, Any]]) -> None:
                ok_n, applied_patch = result
                for j in self._all_jobs:
                    if str(j.get("id", "")).strip() in target_set:
                        j.update(applied_patch)
                self._clear_ui_busy()
                if hasattr(self, "_lbl_jobs_regen_status"):
                    self._lbl_jobs_regen_status.configure(text=f"Đã sửa {ok_n}/{len(target_list)} job")
                try:
                    top.destroy()
                except tk.TclError:
                    pass
                self._render_schedule_jobs_tree()
                messagebox.showinfo(
                    "Đã lưu",
                    f"Đã cập nhật {ok_n}/{len(target_list)} job (lần lượt, một lần ghi file).",
                    parent=self._root,
                )

            def _on_err(exc: BaseException) -> None:
                self._clear_ui_busy()
                logger.warning("Bulk edit jobs: {}", exc)
                try:
                    btn_apply.configure(state=tk.NORMAL)
                    btn_cancel.configure(state=tk.NORMAL)
                    top.grab_set()
                except tk.TclError:
                    pass
                messagebox.showerror("Lỗi sửa hàng loạt", str(exc), parent=top)

            run_background_then_main(self._root, _worker, _on_done, on_error=_on_err)

        btn_cancel = ttk.Button(btn_fr, text="Hủy", command=top.destroy)
        btn_cancel.pack(side=tk.RIGHT, padx=(6, 0))
        btn_apply = ttk.Button(btn_fr, text="Áp dụng", command=_apply)
        btn_apply.pack(side=tk.RIGHT)

        try:
            top.geometry("+%d+%d" % (self._root.winfo_rootx() + 60, self._root.winfo_rooty() + 80))
        except tk.TclError:
            pass

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
        top.geometry("760x420")
        top.minsize(560, 320)
        host = ttk.Frame(top, padding=8)
        host.pack(fill=tk.BOTH, expand=True)
        host.columnconfigure(0, weight=1)
        host.rowconfigure(0, weight=1)
        txt = tk.Text(host, width=80, height=min(24, max(8, len(jobs) + 2)))
        sy = ttk.Scrollbar(host, orient=tk.VERTICAL, command=txt.yview)
        txt.configure(yscrollcommand=sy.set)
        txt.grid(row=0, column=0, sticky="nsew")
        sy.grid(row=0, column=1, sticky="ns")
        for j in jobs:
            missing = get_missing_fields(j)
            jid = str(j.get("id", ""))[:12]
            line = f"[{jid}] post_type={j.get('post_type', '')}  → "
            line += (", ".join(missing) if missing else "(đủ field)") + "\n"
            txt.insert(tk.END, line)
        txt.configure(state=tk.DISABLED)
        ttk.Button(host, text="Đóng", command=top.destroy).grid(row=1, column=0, sticky="e", pady=(8, 0))

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
        dlg.geometry("520x520")
        dlg.minsize(420, 340)
        ttk.Label(
            dlg,
            text="Chỉ tái tạo những field được tick dưới đây và ĐANG THIẾU trên job.\n"
            "Các field đã có dữ liệu hợp lệ sẽ được giữ nguyên.",
            justify="left",
        ).pack(anchor="w", padx=10, pady=(10, 6))
        vars_: dict[str, tk.BooleanVar] = {}
        body = ttk.Frame(dlg)
        body.pack(fill=tk.BOTH, expand=True, padx=10)
        body.columnconfigure(0, weight=1)
        body.rowconfigure(0, weight=1)
        cvs = tk.Canvas(body, highlightthickness=0)
        sy = ttk.Scrollbar(body, orient=tk.VERTICAL, command=cvs.yview)
        fr = ttk.Frame(cvs)
        fr.bind("<Configure>", lambda _e: cvs.configure(scrollregion=cvs.bbox("all")))
        win = cvs.create_window((0, 0), window=fr, anchor="nw")
        cvs.configure(yscrollcommand=sy.set)
        cvs.grid(row=0, column=0, sticky="nsew")
        sy.grid(row=0, column=1, sticky="ns")
        cvs.bind("<Configure>", lambda e: cvs.itemconfigure(win, width=max(1, int(getattr(e, "width", cvs.winfo_width())))))
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
            fail_notes: list[str] = []
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
                        row_after = self._schedule_posts.get_by_id(jid)
                        note = self._schedule_job_error_display(
                            dict(row_after or {}), max_len=500
                        )
                        if not note:
                            note = "Đăng thất bại (không có error_note — xem logs/failed_accounts.log)"
                        fail_notes.append(f"- {jid}:\n  {note}")
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Đăng ngay job {} lỗi: {}", jid, exc)
                    fail += 1
                    fail_notes.append(f"- {jid}: lỗi ngoại lệ (GUI/worker): {exc}")
            self._root.after(
                0,
                lambda: self._finish_run_selected_jobs_now(
                    ok, fail, skipped_notes, fail_notes
                ),
            )

        threading.Thread(target=worker, name="run_selected_jobs_now", daemon=True).start()

    def _finish_run_selected_jobs_now(
        self,
        ok: int,
        fail: int,
        skipped_notes: list[str] | None = None,
        fail_notes: list[str] | None = None,
    ) -> None:
        self._root.configure(cursor="")
        self._fill_schedule_jobs_tree()
        skipped_notes = skipped_notes or []
        fail_notes = fail_notes or []
        skip_count = len(skipped_notes)
        detail = ""
        if fail_notes:
            shown_f = fail_notes[:5]
            detail += "\n\nThất bại (chi tiết):\n" + "\n".join(shown_f)
            if len(fail_notes) > len(shown_f):
                detail += f"\n... và {len(fail_notes) - len(shown_f)} job lỗi khác (xem cột «Lỗi gần nhất»)"
        if skipped_notes:
            shown = skipped_notes[:8]
            detail += "\n\nBỏ qua:\n" + "\n".join(shown)
            if skip_count > len(shown):
                detail += f"\n... và {skip_count - len(shown)} mục khác"
        title = "Kết quả đăng ngay"
        body = f"Thành công: {ok}\nThất bại: {fail}\nBỏ qua: {skip_count}{detail}"
        if fail > 0:
            messagebox.showwarning(title, body, parent=self._root)
        else:
            messagebox.showinfo(title, body, parent=self._root)

    def _open_posting_visual_monitor(self) -> None:
        """
        Cửa sổ 450×400: bước FSM từ ``data/runtime/job_run_monitor.json`` + screenshot mới nhất.
        """
        top = tk.Toplevel(self._root)
        top.title("Màn hình trực quan đăng bài")
        top.geometry("760x520")
        top.minsize(520, 380)
        frm = ttk.Frame(top, padding=6)
        frm.pack(fill=tk.BOTH, expand=True)
        step_var = tk.StringVar(value="Đang đọc tiến trình job…")
        lbl_step = ttk.Label(frm, textvariable=step_var, font=("Segoe UI", 8), wraplength=720, justify=tk.LEFT)
        lbl_step.pack(
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
        top._visual_max_w = 720  # type: ignore[attr-defined]
        top._visual_max_h = 300  # type: ignore[attr-defined]

        def _on_resize(_event: tk.Event | None = None) -> None:
            try:
                w = max(360, int(top.winfo_width()))
                h = max(300, int(top.winfo_height()))
                lbl_step.configure(wraplength=max(300, w - 40))
                top._visual_max_w = max(300, w - 40)  # type: ignore[attr-defined]
                top._visual_max_h = max(180, h - 220)  # type: ignore[attr-defined]
            except Exception:
                pass

        top.bind("<Configure>", _on_resize, add="+")
        _on_resize()

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
                            max_w = int(getattr(top, "_visual_max_w", 420) or 420)
                            max_h = int(getattr(top, "_visual_max_h", 220) or 220)
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

    @staticmethod
    def _ve_import_short(s: str, max_len: int = 80) -> str:
        t = str(s or "").strip()
        if len(t) <= max_len:
            return t
        return t[: max_len - 1] + "…"

    @staticmethod
    def _ve_import_fb_account_row(a: dict[str, Any]) -> tuple[str, str, str]:
        """sort_key, label, account_id."""
        aid = str(a.get("id") or "").strip()
        if not aid:
            return "", "", ""
        name = str(a.get("name") or "").strip()
        raw_notes = str(a.get("notes") or "").strip()
        notes_line = _ManagerWindow._ve_import_short(raw_notes.splitlines()[0], 72) if raw_notes else ""
        topic = _ManagerWindow._ve_import_short(str(a.get("topic") or "").strip(), 48)
        if name and name != aid:
            core = name
        elif notes_line:
            core = notes_line
        elif topic:
            core = topic
        else:
            core = aid
        label = f"{core}  [{aid}]"
        return core.lower(), label, aid

    @staticmethod
    def _ve_import_fb_page_row(p: dict[str, Any]) -> tuple[str, str, str]:
        """sort_key, label, page_id."""
        pid = str(p.get("id") or "").strip()
        if not pid:
            return "", "", ""
        pname = str(p.get("page_name") or "").strip()
        kind = str(p.get("page_kind") or "").strip()
        bus = str(p.get("business_name") or "").strip()
        url = str(p.get("page_url") or "").strip()
        host_hint = ""
        if url and (not pname or pname == pid):
            try:
                from urllib.parse import urlparse

                host_hint = _ManagerWindow._ve_import_short(urlparse(url).netloc, 40)
            except Exception:
                pass
        parts: list[str] = []
        if pname and pname != pid:
            parts.append(pname)
        elif bus:
            parts.append(bus)
        elif host_hint:
            parts.append(host_hint)
        if kind:
            parts.append(kind)
        core = " · ".join(parts) if parts else (pname or bus or pid)
        label = f"{core}  [{pid}]"
        return core.lower(), label, pid

    @staticmethod
    def _ve_import_tt_account_row(a: dict[str, Any]) -> tuple[str, str, str]:
        aid = str(a.get("id") or "").strip()
        if not aid:
            return "", "", ""
        name = str(a.get("name") or "").strip()
        un = str(a.get("username") or "").strip()
        raw_notes = str(a.get("notes") or "").strip()
        notes_line = _ManagerWindow._ve_import_short(raw_notes.splitlines()[0], 64) if raw_notes else ""
        if name and name != aid:
            core = name
        elif un:
            core = f"@{un}"
        elif notes_line:
            core = notes_line
        else:
            core = aid
        extra = f" @{un}" if un and not core.startswith("@") else ""
        label = f"{core}{extra}  [{aid}]"
        return core.lower(), label, aid

    def _ve_import_prepare_account_maps(self) -> tuple[dict[str, str], dict[str, str], str | None]:
        """Đọc accounts Facebook/TikTok cho dialog nạp job Export (gọi từ thread nền)."""
        fb_account_map: dict[str, str] = {}
        try:
            acc_rows: list[tuple[str, str, str]] = []
            for a in self._accounts.load_all():
                if not isinstance(a, dict):
                    continue
                sk, lb, aid = self._ve_import_fb_account_row(a)
                if aid:
                    acc_rows.append((sk, lb, aid))
            acc_rows.sort(key=lambda x: x[0])
            for _, lb, aid in acc_rows:
                fb_account_map[lb] = aid
        except Exception as exc:  # noqa: BLE001
            logger.exception("Nạp job chờ đăng: đọc accounts.json")
            return {}, {}, f"Không đọc được danh sách Facebook (accounts.json):\n{exc}"
        tt_account_map: dict[str, str] = {}
        try:
            tt_rows: list[tuple[str, str, str]] = []
            for a in TikTokAccountStore().load_all():
                if not isinstance(a, dict):
                    continue
                sk, lb, aid = self._ve_import_tt_account_row(a)
                if aid:
                    tt_rows.append((sk, lb, aid))
            tt_rows.sort(key=lambda x: x[0])
            for _, lb, aid in tt_rows:
                tt_account_map[lb] = aid
        except Exception:
            pass
        return fb_account_map, tt_account_map, None

    @staticmethod
    def _load_saved_export_schedule_jobs() -> list[dict[str, Any]]:
        p = video_editor_schedule_jobs_json_path()
        if not p.is_file():
            return []
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            return [dict(x) for x in raw] if isinstance(raw, list) else []
        except Exception:
            return []

    @staticmethod
    def _save_saved_export_schedule_jobs(rows: list[dict[str, Any]]) -> None:
        p = video_editor_schedule_jobs_json_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _on_import_saved_export_job(self) -> None:
        def _worker() -> tuple[list[dict[str, Any]], dict[str, str], dict[str, str]] | None:
            rows = self._load_saved_export_schedule_jobs()
            if not rows:
                return None
            fb_map, tt_map, err = self._ve_import_prepare_account_maps()
            if err:
                raise RuntimeError(err)
            return rows, fb_map, tt_map

        def _on_main(data: tuple[list[dict[str, Any]], dict[str, str], dict[str, str]] | None) -> None:
            if data is None:
                messagebox.showinfo(
                    "Job chờ đăng",
                    "Chưa có job chờ đăng từ Video Editor.",
                    parent=self._root,
                )
                return
            rows, fb_account_map, tt_account_map = data
            if not fb_account_map and not tt_account_map:
                messagebox.showwarning(
                    "Chưa có tài khoản",
                    "Chưa có tài khoản Facebook hoặc TikTok — không thể «Nạp» vào lịch cho đến khi thêm tài khoản (tab 1 / TikTok Manager).\n"
                    "Bạn vẫn có thể xem job chờ trong tab «7.Job chờ đăng từ Video Editor».",
                    parent=self._root,
                )
            self._show_ve_import_saved_export_dialog(rows, fb_account_map, tt_account_map)

        def _on_err(exc: BaseException) -> None:
            messagebox.showerror("Lỗi tài khoản", str(exc), parent=self._root)

        run_background_then_main(self._root, _worker, _on_main, on_error=_on_err)

    def _show_ve_import_saved_export_dialog(
        self,
        rows: list[dict[str, Any]],
        fb_account_map: dict[str, str],
        tt_account_map: dict[str, str],
    ) -> None:
        top = tk.Toplevel(self._root)
        top.title("Nạp job chờ đăng từ Export")
        top.transient(self._root)
        top.geometry("820x700")
        top.minsize(680, 560)
        frm = ttk.Frame(top, padding=10)
        frm.columnconfigure(1, weight=1)

        job_map: dict[str, dict[str, Any]] = {}
        preferred_saved_job_id = str(getattr(self._root, "_ve_saved_export_job_id", "") or "").strip()
        var_show_imported = tk.BooleanVar(value=False)
        var_allow_reimport = tk.BooleanVar(value=False)
        var_job = tk.StringVar(value="")

        var_platform = tk.StringVar(
            value=(
                "TikTok"
                if not fb_account_map and tt_account_map
                else "Facebook"
                if fb_account_map and not tt_account_map
                else "Facebook"
            )
        )
        var_acc = tk.StringVar(value="")
        var_page = tk.StringVar(value="")
        var_step = tk.StringVar(value="30")
        page_map: dict[str, str] = {}
        try:
            _tz_default = str(getattr(scheduler_tz(), "key", "") or "").strip() or "Asia/Ho_Chi_Minh"
        except Exception:
            _tz_default = "Asia/Ho_Chi_Minh"

        _kieu_wait = "Chờ chọn — chưa gán Facebook/TikTok (chỉ cập nhật file job)"
        _kieu_fb = "Facebook → nạp vào lịch schedule_posts"
        _kieu_tt = "TikTok → nạp vào TikTok Manager"
        var_kieu_dich = tk.StringVar(value=_kieu_wait)
        import_btn_ref: dict[str, Any] = {}

        ttk.Label(frm, text="Job chờ đăng").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 6))
        cb_job = ttk.Combobox(frm, textvariable=var_job, values=[], state="readonly", width=56)
        cb_job.grid(row=0, column=1, sticky="ew", pady=(0, 6))
        ttk.Checkbutton(
            frm,
            text="Hiện cả job đã import",
            variable=var_show_imported,
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 6))

        lf_dest = ttk.LabelFrame(frm, text="Đích đăng", padding=(8, 6))
        lf_dest.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        lf_dest.columnconfigure(0, weight=1)
        dest_inner = ttk.Frame(lf_dest)
        dest_inner.grid(row=0, column=0, sticky="nsew")
        dest_inner.columnconfigure(1, weight=1)
        ttk.Label(dest_inner, text="Kiểu đích").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 4))
        cb_kieu = ttk.Combobox(
            dest_inner,
            textvariable=var_kieu_dich,
            values=(_kieu_wait, _kieu_fb, _kieu_tt),
            state="readonly",
            width=56,
        )
        cb_kieu.grid(row=0, column=1, sticky="w", pady=(0, 4))
        ttk.Button(dest_inner, text="Làm mới danh sách", command=lambda: _reload_ve_import_maps()).grid(
            row=0, column=2, sticky="e", padx=(8, 0), pady=(0, 4)
        )
        lbl_acc = ttk.Label(dest_inner, text="Tài khoản")
        lbl_acc.grid(row=1, column=0, sticky="nw", padx=(0, 8), pady=(4, 4))
        cb_acc = ttk.Combobox(dest_inner, textvariable=var_acc, values=[], state="readonly", width=72)
        cb_acc.grid(row=1, column=1, sticky="ew", pady=(4, 4))
        lbl_page = ttk.Label(dest_inner, text="Page Facebook")
        lbl_page.grid(row=2, column=0, sticky="nw", padx=(0, 8), pady=(0, 4))
        cb_page = ttk.Combobox(dest_inner, textvariable=var_page, state="readonly", width=72)
        cb_page.grid(row=2, column=1, sticky="ew", pady=(0, 4))
        lbl_kieu_wait = ttk.Label(
            dest_inner,
            text=(
                "«Chờ chọn»: job vẫn có đủ video + caption; phần đăng để trống. "
                "Bấm «Lưu gợi ý đích vào file job» để ghi lại. Khi muốn đăng, đổi Kiểu đích sang Facebook hoặc TikTok, chọn tài khoản/Page, rồi «Nạp vào job lịch»."
            ),
            foreground="#555",
            font=("Segoe UI", 8),
            wraplength=760,
            justify="left",
        )
        lbl_kieu_wait.grid(row=3, column=0, columnspan=3, sticky="w", pady=(4, 0))
        lbl_dest_help = ttk.Label(
            dest_inner,
            text="Facebook: chọn Page thuộc tài khoản. TikTok: không cần Page. Tên hiển thị trước, id trong […].",
            foreground="#666",
            font=("Segoe UI", 8),
            wraplength=760,
            justify="left",
        )
        lbl_dest_help.grid(row=4, column=0, columnspan=3, sticky="w", pady=(2, 0))

        sch_fr = ttk.LabelFrame(frm, text="Lịch đăng", padding=8)
        sch_fr.grid(row=4, column=0, columnspan=2, sticky="nsew", pady=(2, 4))
        frm.rowconfigure(4, weight=1)
        sch_fr.columnconfigure(1, weight=1)
        _now = datetime.now()
        var_sched_rule = tk.StringVar(value="Một lần")
        sr = 0
        ttk.Label(sch_fr, text="Kiểu lịch").grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=2)
        cb_sched_rule = ttk.Combobox(
            sch_fr,
            textvariable=var_sched_rule,
            values=("Đăng ngay", "Một lần", "Theo khung giờ mỗi ngày"),
            state="readonly",
            width=38,
        )
        cb_sched_rule.grid(row=sr, column=1, sticky="w", pady=2)
        sr += 1
        lbl_start_date = ttk.Label(sch_fr, text="Ngày bắt đầu (YYYY-MM-DD)")
        lbl_start_date.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        e_start_date = ttk.Entry(sch_fr, width=14)
        e_start_date.insert(0, _now.strftime("%Y-%m-%d"))
        e_start_date.grid(row=sr, column=1, sticky="w", pady=4)
        sr += 1
        lbl_once_time = ttk.Label(sch_fr, text="Giờ/phút (cho kiểu Một lần)")
        lbl_once_time.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        sched_once_fr = ttk.Frame(sch_fr)
        sched_once_fr.grid(row=sr, column=1, sticky="w", pady=4)
        ttk.Label(sched_once_fr, text="Giờ:").pack(side=tk.LEFT)
        sp_hour = ttk.Spinbox(sched_once_fr, from_=0, to=23, width=4, format="%.0f")
        sp_hour.set(str(_now.hour))
        sp_hour.pack(side=tk.LEFT, padx=4)
        ttk.Label(sched_once_fr, text="Phút:").pack(side=tk.LEFT)
        sp_min = ttk.Spinbox(sched_once_fr, from_=0, to=59, width=4, format="%.0f")
        sp_min.set(str(_now.minute))
        sp_min.pack(side=tk.LEFT, padx=4)
        sr += 1
        lbl_daily_slots = ttk.Label(sch_fr, text="Khung giờ/ngày (HH:MM, phẩy)")
        lbl_daily_slots.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        e_daily_slots = ttk.Entry(sch_fr, width=36)
        e_daily_slots.insert(0, "04:30,10:15,22:30")
        e_daily_slots.grid(row=sr, column=1, sticky="ew", pady=4)
        sr += 1
        lbl_delay_min = ttk.Label(sch_fr, text="Delay tối thiểu (phút)")
        lbl_delay_min.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        delay_min_w = ttk.Spinbox(sch_fr, from_=0, to=180, width=6)
        delay_min_w.insert(0, "1")
        delay_min_w.grid(row=sr, column=1, sticky="w", pady=4)
        sr += 1
        lbl_delay_max = ttk.Label(sch_fr, text="Delay tối đa (phút)")
        lbl_delay_max.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        delay_max_w = ttk.Spinbox(sch_fr, from_=0, to=180, width=6)
        delay_max_w.insert(0, "5")
        delay_max_w.grid(row=sr, column=1, sticky="w", pady=4)
        sr += 1
        lbl_timezone = ttk.Label(sch_fr, text="Múi giờ")
        lbl_timezone.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        e_timezone = ttk.Entry(sch_fr, width=32)
        e_timezone.insert(0, _tz_default)
        e_timezone.grid(row=sr, column=1, sticky="ew", pady=4)
        sr += 1
        lbl_step_gap = ttk.Label(sch_fr, text="Cách nhau (phút, nhiều video)")
        lbl_step_gap.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        ent_step_gap = ttk.Entry(sch_fr, textvariable=var_step, width=8)
        ent_step_gap.grid(row=sr, column=1, sticky="w", pady=4)
        sr += 1
        _default_lbl_fg = lbl_daily_slots.cget("foreground")
        lbl_sch_hint = ttk.Label(
            sch_fr,
            text=(
                "Giống job lịch / «Thêm batch job»: Đăng ngay / Một lần / Theo khung giờ mỗi ngày. "
                "Nhiều video: kiểu «Theo khung giờ» xếp lần lượt theo các slot; "
                "«Đăng ngay» / «Một lần» dùng «Cách nhau (phút)»."
            ),
            foreground="gray",
            font=("Segoe UI", 8),
            wraplength=640,
        )
        lbl_sch_hint.grid(row=sr, column=0, columnspan=2, sticky="w", pady=(4, 0))
        sr += 1

        def _import_sched_rule_key() -> str:
            s = str(var_sched_rule.get() or "")
            if "Đăng ngay" in s:
                return "immediate"
            if "Theo khung giờ" in s:
                return "daily_slots"
            return "once"

        def _on_import_sched_rule_changed(*_a: Any) -> None:
            rule = _import_sched_rule_key()
            if rule == "immediate":
                lbl_start_date.grid_remove()
                e_start_date.grid_remove()
                lbl_once_time.grid_remove()
                sched_once_fr.grid_remove()
                lbl_daily_slots.grid_remove()
                e_daily_slots.grid_remove()
                lbl_delay_min.grid_remove()
                delay_min_w.grid_remove()
                lbl_delay_max.grid_remove()
                delay_max_w.grid_remove()
                lbl_timezone.grid_remove()
                e_timezone.grid_remove()
                lbl_step_gap.grid()
                ent_step_gap.grid()
            elif rule == "once":
                lbl_start_date.grid()
                e_start_date.grid()
                lbl_once_time.grid()
                sched_once_fr.grid()
                lbl_daily_slots.grid_remove()
                e_daily_slots.grid_remove()
                lbl_delay_min.grid_remove()
                delay_min_w.grid_remove()
                lbl_delay_max.grid_remove()
                delay_max_w.grid_remove()
                lbl_timezone.grid_remove()
                e_timezone.grid_remove()
                lbl_step_gap.grid()
                ent_step_gap.grid()
            else:
                lbl_start_date.grid()
                e_start_date.grid()
                lbl_once_time.grid_remove()
                sched_once_fr.grid_remove()
                lbl_daily_slots.grid()
                e_daily_slots.grid()
                lbl_delay_min.grid()
                delay_min_w.grid()
                lbl_delay_max.grid()
                delay_max_w.grid()
                lbl_timezone.grid()
                e_timezone.grid()
                lbl_step_gap.grid_remove()
                ent_step_gap.grid_remove()
            try:
                top.event_generate("<Configure>")
            except tk.TclError:
                pass

        cb_sched_rule.bind("<<ComboboxSelected>>", _on_import_sched_rule_changed)
        _on_import_sched_rule_changed()

        ttk.Checkbutton(
            frm,
            text="Cho phép import lại job đã import",
            variable=var_allow_reimport,
        ).grid(row=5, column=1, sticky="w", pady=(4, 0))
        ttk.Label(
            frm,
            text="(Dùng khi muốn tạo lại toàn bộ lịch từ cùng một job đã import)",
            foreground="#666",
            font=("Segoe UI", 8),
        ).grid(row=6, column=1, sticky="w")

        var_preview = tk.StringVar(value="")
        lbl_preview = ttk.Label(frm, textvariable=var_preview, foreground="#666", wraplength=680, justify="left")
        lbl_preview.grid(
            row=7, column=0, columnspan=2, sticky="w", pady=(8, 0)
        )
        def _import_dlg_wrap(_e: Any = None) -> None:
            w = max(320, int(top.winfo_width()) - 80)
            try:
                lbl_preview.configure(wraplength=w)
                lbl_sch_hint.configure(wraplength=max(340, w - 40))
            except tk.TclError:
                pass

        top.bind("<Configure>", _import_dlg_wrap, add="+")

        _suppress_ve_import_job_trace: dict[str, bool] = {"v": False}

        def _refresh_job_choices(*_args: Any) -> None:
            job_map.clear()
            include_imported = bool(var_show_imported.get())
            job_pairs: list[tuple[str, str, dict[str, Any]]] = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                st = str(r.get("status", "")).strip().lower()
                if not include_imported and st not in {"", "saved", "pending"}:
                    continue
                jid = str(r.get("id") or "").strip()
                jn = str(r.get("job_name") or jid).strip()
                created = str(r.get("created_at") or "").strip()
                cnt = len(r.get("items") or [])
                suffix = " [ĐÃ IMPORT]" if st == "imported" else ""
                label = f"{jn} ({cnt} video)  {created}{suffix}  [{jid}]"
                job_pairs.append((jn.lower(), label, r))
            job_pairs.sort(key=lambda x: x[0])
            vals: list[str] = []
            for _, label, r in job_pairs:
                job_map[label] = r
                vals.append(label)
            cb_job.configure(values=vals)
            if vals:
                current = str(var_job.get() or "").strip()
                if current not in job_map:
                    picked = ""
                    if preferred_saved_job_id:
                        needle = f"[{preferred_saved_job_id}]"
                        picked = next((v for v in vals if needle in v), "")
                    _suppress_ve_import_job_trace["v"] = True
                    try:
                        var_job.set(picked or vals[0])
                    finally:
                        _suppress_ve_import_job_trace["v"] = False
                    _apply_saved_job_publish_presets()
                    _sync_kieu_dich_ui()
                    _refresh_preview()
            else:
                _suppress_ve_import_job_trace["v"] = True
                try:
                    var_job.set("")
                finally:
                    _suppress_ve_import_job_trace["v"] = False
                _sync_kieu_dich_ui()
                _refresh_preview()

        def _active_account_map() -> dict[str, str]:
            return tt_account_map if str(var_platform.get() or "").strip() == "TikTok" else fb_account_map

        def _sync_account_combo(*_args: Any) -> None:
            m = _active_account_map()
            vals = list(m.keys())
            cb_acc.configure(values=vals)
            cur = str(var_acc.get() or "").strip()
            if vals:
                if cur not in m:
                    var_acc.set(vals[0])
            else:
                var_acc.set("")
            plat = str(var_platform.get() or "").strip()
            if plat == "TikTok":
                lbl_page.grid_remove()
                cb_page.grid_remove()
                page_map.clear()
                var_page.set("")
            else:
                lbl_page.grid()
                cb_page.grid()
            _refresh_pages()

        def _refresh_pages(*_args: Any) -> None:
            page_map.clear()
            if str(var_platform.get() or "").strip() == "TikTok":
                cb_page.configure(values=[])
                var_page.set("")
                return
            aid = fb_account_map.get(str(var_acc.get() or "").strip(), "")
            rows_pg: list[tuple[str, str, str]] = []
            if aid:
                pages_src = list(getattr(self, "_all_pages", []) or [])
                if not pages_src:
                    try:
                        pages_src = self._pages.load_all()
                    except Exception:
                        pages_src = []
                for p in pages_src:
                    if not isinstance(p, dict):
                        continue
                    if str(p.get("account_id") or "").strip() != aid:
                        continue
                    sk, lb, pid = self._ve_import_fb_page_row(p)
                    if pid:
                        rows_pg.append((sk, lb, pid))
            rows_pg.sort(key=lambda x: x[0])
            vals = [x[1] for x in rows_pg]
            for _, lb, pid in rows_pg:
                page_map[lb] = pid
            cb_page.configure(values=vals)
            var_page.set(vals[0] if vals else "")

        def _kieu_key() -> str:
            v = str(var_kieu_dich.get() or "").strip()
            if v == _kieu_wait:
                return "wait"
            if v == _kieu_fb:
                return "fb"
            if v == _kieu_tt:
                return "tt"
            return "wait"

        def _reload_ve_import_maps() -> None:
            def _worker() -> tuple[dict[str, str], dict[str, str], str | None]:
                return self._ve_import_prepare_account_maps()

            def _on_main(maps: tuple[dict[str, str], dict[str, str], str | None]) -> None:
                fb_m, tt_m, err = maps
                if err:
                    messagebox.showerror("Lỗi tài khoản", err, parent=top)
                    return
                fb_account_map.clear()
                fb_account_map.update(fb_m)
                tt_account_map.clear()
                tt_account_map.update(tt_m)
                _sync_kieu_dich_ui()
                _refresh_preview()

            run_background_then_main(top, _worker, _on_main)

        def _sync_kieu_dich_ui(*_a: Any) -> None:
            kv = _kieu_key()
            if kv == "wait":
                var_platform.set("Facebook")
                lbl_acc.grid_remove()
                cb_acc.grid_remove()
                lbl_page.grid_remove()
                cb_page.grid_remove()
                lbl_kieu_wait.grid()
                lbl_dest_help.grid_remove()
            elif kv == "fb":
                lbl_kieu_wait.grid_remove()
                lbl_dest_help.grid()
                var_platform.set("Facebook")
                lbl_acc.grid(row=1, column=0, sticky="nw", padx=(0, 8), pady=(4, 4))
                cb_acc.grid(row=1, column=1, sticky="ew", pady=(4, 4))
                lbl_page.grid(row=2, column=0, sticky="nw", padx=(0, 8), pady=(0, 4))
                cb_page.grid(row=2, column=1, sticky="ew", pady=(0, 4))
                _sync_account_combo()
            else:
                lbl_kieu_wait.grid_remove()
                lbl_dest_help.grid()
                var_platform.set("TikTok")
                lbl_acc.grid(row=1, column=0, sticky="nw", padx=(0, 8), pady=(4, 4))
                cb_acc.grid(row=1, column=1, sticky="ew", pady=(4, 4))
                lbl_page.grid_remove()
                cb_page.grid_remove()
                _sync_account_combo()
            ib = import_btn_ref.get("b")
            if ib is not None:
                try:
                    ib.configure(state=("disabled" if kv == "wait" else "normal"))
                except tk.TclError:
                    pass

        def _refresh_preview(*_args: Any) -> None:
            rec = job_map.get(str(var_job.get() or "").strip(), {})
            cnt = len(rec.get("items") or [])
            nm = str(rec.get("job_name") or rec.get("id") or "").strip()
            if _kieu_key() == "wait":
                var_preview.set(
                    f"«{nm}»: {cnt} video — Kiểu đích «Chờ chọn». "
                    "Có thể «Lưu gợi ý đích vào file job» để ghi unspecified; khi muốn đăng, chọn Facebook hoặc TikTok và «Nạp vào job lịch»."
                )
                return
            plat = str(var_platform.get() or "").strip()
            if plat == "TikTok":
                var_preview.set(
                    f"Sẽ tạo {cnt} job TikTok (lịch) từ «{nm}». Xem và sửa ở tab TikTok Manager → Job & lịch đăng."
                )
            else:
                var_preview.set(f"Sẽ tạo {cnt} job lịch Facebook từ «{nm}». Bạn có thể chỉnh sửa từng job sau khi nạp.")

        def _apply_saved_job_publish_presets(*_a: Any) -> None:
            rec = job_map.get(str(var_job.get() or "").strip(), {})
            if not rec:
                return
            tgt = str(rec.get("publish_target") or "").strip().lower()
            if not tgt or tgt == "unspecified":
                var_kieu_dich.set(_kieu_wait)
                return
            try:
                if tgt == "facebook" and fb_account_map:
                    var_kieu_dich.set(_kieu_fb)
                    var_platform.set("Facebook")
                    aid0 = str(rec.get("preset_fb_account_id") or "").strip()
                    if aid0:
                        acc_lbl = next((k for k, v in fb_account_map.items() if v == aid0), "")
                        if acc_lbl:
                            var_acc.set(acc_lbl)
                    _refresh_pages()
                    pid0 = str(rec.get("preset_fb_page_id") or "").strip()
                    if pid0:
                        pg_lbl = next((k for k, v in page_map.items() if v == pid0), "")
                        if pg_lbl:
                            var_page.set(pg_lbl)
                elif tgt == "tiktok" and tt_account_map:
                    var_kieu_dich.set(_kieu_tt)
                    var_platform.set("TikTok")
                    tid0 = str(rec.get("preset_tiktok_account_id") or "").strip()
                    if tid0:
                        tt_lbl = next((k for k, v in tt_account_map.items() if v == tid0), "")
                        if tt_lbl:
                            var_acc.set(tt_lbl)
            except Exception:
                pass

        def _on_import_job_label_changed(*_a: Any) -> None:
            if _suppress_ve_import_job_trace["v"]:
                return
            _apply_saved_job_publish_presets()
            _sync_kieu_dich_ui()
            _refresh_preview()

        def _on_acc_changed(*_a: Any) -> None:
            _refresh_pages()
            _refresh_preview()

        var_job.trace_add("write", _on_import_job_label_changed)
        var_show_imported.trace_add("write", _refresh_job_choices)
        cb_kieu.bind("<<ComboboxSelected>>", lambda _e=None: (_sync_kieu_dich_ui(), _refresh_preview()))
        var_acc.trace_add("write", _on_acc_changed)
        var_page.trace_add("write", _refresh_preview)
        _refresh_job_choices()
        _apply_saved_job_publish_presets()
        _sync_kieu_dich_ui()
        _refresh_preview()

        def _clear_schedule_import_marks() -> None:
            for w in (lbl_daily_slots, lbl_delay_min, lbl_delay_max, lbl_timezone):
                try:
                    w.configure(foreground=_default_lbl_fg)
                except tk.TclError:
                    pass

        def _parse_daily_slot_strings_import() -> list[str]:
            raw = e_daily_slots.get().strip()
            if not raw:
                lbl_daily_slots.configure(foreground="red")
                raise ValueError("Khung giờ/ngày không được để trống.")
            out: list[str] = []
            for token in raw.split(","):
                s = token.strip()
                if not s:
                    continue
                parts = s.split(":")
                if len(parts) != 2:
                    lbl_daily_slots.configure(foreground="red")
                    raise ValueError(f"Khung giờ không hợp lệ: {s!r}. Dùng HH:MM.")
                h, mi = int(parts[0]), int(parts[1])
                if not (0 <= h <= 23 and 0 <= mi <= 59):
                    lbl_daily_slots.configure(foreground="red")
                    raise ValueError(f"Khung giờ không hợp lệ: {s!r}.")
                out.append(f"{h:02d}:{mi:02d}")
            return sorted(set(out))

        def _resolved_tz_name_import() -> str:
            name = (e_timezone.get() or "").strip() or "Asia/Ho_Chi_Minh"
            try:
                ZoneInfo(name)
                return name
            except Exception:
                lbl_timezone.configure(foreground="red")
                return "Asia/Ho_Chi_Minh"

        def _iso_to_local_wall(iso_s: str, tz: ZoneInfo) -> str:
            s2 = str(iso_s or "").strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(s2)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M")

        def _build_import_plans(n_valid: int) -> tuple[list[dict[str, Any]], str] | None:
            if n_valid < 1:
                return None
            _clear_schedule_import_marks()
            rule = _import_sched_rule_key()
            tz_sched = scheduler_tz()
            tz_row_fb = str(getattr(tz_sched, "key", "") or "").strip() or _tz_default

            if rule == "immediate":
                try:
                    gap = max(0, int(str(var_step.get()).strip() or "0"))
                except ValueError:
                    messagebox.showwarning("Lịch", "«Cách nhau (phút)» phải là số nguyên ≥ 0.", parent=top)
                    return None
                base = datetime.now(tz_sched).replace(second=0, microsecond=0)
                plans: list[dict[str, Any]] = []
                for i in range(n_valid):
                    dt = base + timedelta(minutes=i * gap)
                    iso = dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
                    plans.append(
                        {
                            "scheduled_at": iso,
                            "wall": dt.strftime("%Y-%m-%d %H:%M"),
                            "schedule_recurrence": "",
                            "schedule_slot": "",
                        }
                    )
                return plans, tz_row_fb

            if rule == "once":
                try:
                    gap = max(0, int(str(var_step.get()).strip() or "0"))
                except ValueError:
                    messagebox.showwarning("Lịch", "«Cách nhau (phút)» phải là số nguyên ≥ 0.", parent=top)
                    return None
                try:
                    d_only = parse_date_only_yyyy_mm_dd(str(e_start_date.get() or "").strip())
                except Exception:
                    messagebox.showwarning("Sai định dạng", "Ngày bắt đầu phải là YYYY-MM-DD.", parent=top)
                    return None
                try:
                    h0 = int(str(sp_hour.get()).strip())
                    m0 = int(str(sp_min.get()).strip())
                except ValueError:
                    messagebox.showwarning("Sai giờ", "Giờ và phút phải là số nguyên.", parent=top)
                    return None
                if not (0 <= h0 <= 23 and 0 <= m0 <= 59):
                    messagebox.showwarning("Sai giờ", "Giờ 0–23, phút 0–59.", parent=top)
                    return None
                cur = datetime(d_only.year, d_only.month, d_only.day, h0, m0, 0, tzinfo=tz_sched)
                slot = build_schedule_slot_hhmm(h0, m0)
                plans_once: list[dict[str, Any]] = []
                for i in range(n_valid):
                    dt = cur + timedelta(minutes=i * gap)
                    iso = dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
                    plans_once.append(
                        {
                            "scheduled_at": iso,
                            "wall": dt.strftime("%Y-%m-%d %H:%M"),
                            "schedule_recurrence": "once",
                            "schedule_slot": slot,
                        }
                    )
                return plans_once, tz_row_fb

            tz_out = _resolved_tz_name_import()
            try:
                d_only_d = parse_date_only_yyyy_mm_dd(str(e_start_date.get() or "").strip())
            except Exception:
                messagebox.showwarning("Sai định dạng", "Ngày bắt đầu phải là YYYY-MM-DD.", parent=top)
                return None
            try:
                slots_list = _parse_daily_slot_strings_import()
            except ValueError as exc:
                messagebox.showerror("Lịch", str(exc), parent=top)
                return None
            try:
                dmin = int(str(delay_min_w.get() or "0").strip())
                dmax = int(str(delay_max_w.get() or "0").strip())
            except ValueError:
                messagebox.showwarning("Lịch", "Delay phải là số nguyên.", parent=top)
                return None
            if dmin < 0 or dmax < 0 or dmin > dmax:
                lbl_delay_min.configure(foreground="red")
                lbl_delay_max.configure(foreground="red")
                messagebox.showwarning("Lịch", "Delay tối thiểu ≤ delay tối đa (cùng ≥ 0).", parent=top)
                return None
            try:
                tz_z = ZoneInfo(tz_out)
            except Exception:
                tz_z = scheduler_tz()
                tz_out = tz_row_fb
            try:
                raw_plans = build_schedule_by_daily_slots(
                    start_date=d_only_d,
                    time_slots=slots_list,
                    job_count=n_valid,
                    delay_min_minutes=dmin,
                    delay_max_minutes=dmax,
                    timezone_name=tz_out,
                )
            except ValueError as exc:
                messagebox.showerror("Lịch", str(exc), parent=top)
                return None
            if len(slots_list) == 1:
                rec, slot0 = "daily", slots_list[0]
            else:
                rec, slot0 = "", ""
            plans_d: list[dict[str, Any]] = []
            ssd = d_only_d.strftime("%Y-%m-%d")
            slots_csv = ",".join(slots_list)
            for p in raw_plans:
                iso = str(p.get("scheduled_at", "")).strip()
                wall = _iso_to_local_wall(iso, tz_z)
                slot_base = str(p.get("slot_base_local", "")).strip()
                try:
                    dam = int(p.get("delay_applied_min", 0))
                except (TypeError, ValueError):
                    dam = 0
                plans_d.append(
                    {
                        "scheduled_at": iso,
                        "wall": wall,
                        "schedule_recurrence": rec,
                        "schedule_slot": slot0,
                        "schedule_daily_slots": slots_csv,
                        "schedule_delay_min": dmin,
                        "schedule_delay_max": dmax,
                        "schedule_start_date": ssd,
                        "slot_base_local": slot_base,
                        "delay_applied_min": dam,
                    }
                )
            return plans_d, tz_out

        def _save_dest_hint_to_job() -> None:
            label = str(var_job.get() or "").strip()
            rec = job_map.get(label, {})
            if not rec:
                messagebox.showwarning("Thiếu dữ liệu", "Chọn job chờ đăng.", parent=top)
                return
            kv = _kieu_key()
            if kv == "wait":
                rec["publish_target"] = "unspecified"
                rec["preset_fb_account_id"] = ""
                rec["preset_fb_page_id"] = ""
                rec["preset_tiktok_account_id"] = ""
            elif kv == "fb":
                aid = fb_account_map.get(str(var_acc.get() or "").strip(), "").strip()
                pid = page_map.get(str(var_page.get() or "").strip(), "").strip()
                if not aid or not pid:
                    messagebox.showwarning(
                        "Thiếu dữ liệu",
                        "Chọn đủ tài khoản và Page Facebook trước khi lưu gợi ý.",
                        parent=top,
                    )
                    return
                rec["publish_target"] = "facebook"
                rec["preset_fb_account_id"] = aid
                rec["preset_fb_page_id"] = pid
                rec["preset_tiktok_account_id"] = ""
            else:
                tid = tt_account_map.get(str(var_acc.get() or "").strip(), "").strip()
                if not tid:
                    messagebox.showwarning("Thiếu dữ liệu", "Chọn tài khoản TikTok.", parent=top)
                    return
                rec["publish_target"] = "tiktok"
                rec["preset_fb_account_id"] = ""
                rec["preset_fb_page_id"] = ""
                rec["preset_tiktok_account_id"] = tid
            try:
                self._save_saved_export_schedule_jobs(rows)
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("Lỗi lưu", str(exc), parent=top)
                return
            self._fill_ve_pending_export_jobs_tree()
            messagebox.showinfo("Đã lưu", "Đã cập nhật gợi ý đích vào file job.", parent=top)

        def _ok() -> None:
            rec = job_map.get(str(var_job.get() or "").strip(), {})
            if not rec:
                messagebox.showwarning("Thiếu dữ liệu", "Chọn job chờ đăng.", parent=top)
                return
            if _kieu_key() == "wait":
                messagebox.showinfo(
                    "Chờ chọn",
                    "Kiểu đích đang «Chờ chọn» — không tạo lịch. Đổi sang Facebook hoặc TikTok, chọn tài khoản (và Page nếu Facebook), rồi «Nạp vào job lịch».",
                    parent=top,
                )
                return
            if str(rec.get("status", "")).strip().lower() == "imported" and not bool(var_allow_reimport.get()):
                messagebox.showwarning(
                    "Đã import",
                    "Job này đã import rồi. Bật «Cho phép import lại» nếu bạn muốn tạo lại lịch.",
                    parent=top,
                )
                return
            dialog_plat = str(var_platform.get() or "").strip()
            dlg_acc = str(var_acc.get() or "")
            dlg_page = str(var_page.get() or "")
            raw_items = rec.get("items") or []
            if not isinstance(raw_items, list):
                messagebox.showwarning("Trống", "Job chờ đăng không có video.", parent=top)
                return
            valid_entries: list[tuple[int, dict[str, Any]]] = []
            for orig_i, it in enumerate(raw_items):
                if isinstance(it, dict) and str(it.get("video_path") or "").strip():
                    valid_entries.append((orig_i, it))
            if not valid_entries:
                messagebox.showwarning("Trống", "Không có video_path hợp lệ trong job.", parent=top)
                return
            built = _build_import_plans(len(valid_entries))
            if built is None:
                return
            plans, tz_row = built

            errs: list[str] = []
            for idx, (_orig_i, it) in enumerate(valid_entries):
                plat_i = self._ve_resolve_item_publish(it, rec, dialog_plat)
                if plat_i == "unspecified":
                    errs.append(f"Clip {idx + 1}: đích «chờ chọn (clip)» — gán ở tab «7.Job chờ đăng» (cột Đích clip).")
                    continue
                if plat_i == "tiktok":
                    aid_tt = self._ve_resolve_item_tt_account(it, rec, dlg_acc, tt_account_map)
                    if not aid_tt:
                        errs.append(
                            f"Clip {idx + 1}: TikTok thiếu tài khoản — chọn TK ở dialog hoặc gán riêng cho clip."
                        )
                else:
                    aid_fb, pid_fb = self._ve_resolve_item_fb_ids(
                        it, rec, dlg_acc, dlg_page, fb_account_map, page_map
                    )
                    if not aid_fb or not pid_fb:
                        errs.append(
                            f"Clip {idx + 1}: Facebook thiếu tài khoản hoặc Page — chọn ở dialog hoặc gán riêng cho clip."
                        )
            if errs:
                messagebox.showerror(
                    "Không nạp được",
                    "Một hoặc nhiều clip chưa đủ đích/tài khoản:\n\n"
                    + "\n".join(errs[:18])
                    + (f"\n… (+{len(errs) - 18} lỗi)" if len(errs) > 18 else ""),
                    parent=top,
                )
                return

            imp_btn = import_btn_ref.get("b")
            if imp_btn is not None:
                try:
                    imp_btn.configure(state="disabled")
                except tk.TclError:
                    pass
            n_clips = len(valid_entries)
            var_preview.set(f"Đang nạp {n_clips} clip vào lịch…")

            def _import_worker() -> dict[str, Any]:
                job_store = TikTokJobStore()
                pps = page_post_style_for_post_type("video")
                fb_batch: list[dict[str, Any]] = []
                tt_batch: list[dict[str, Any]] = []
                created_fb = 0
                created_tt = 0
                last_fb_aid = ""
                last_fb_pid = ""
                last_tt_aid = ""
                _ts_imp = datetime.now().isoformat(timespec="seconds")
                for idx, (_orig_i, it) in enumerate(valid_entries):
                    vp = str(it.get("video_path") or "").strip()
                    plan = plans[idx]
                    plat_i = self._ve_resolve_item_publish(it, rec, dialog_plat)
                    if plat_i == "tiktok":
                        aid_tt = self._ve_resolve_item_tt_account(it, rec, dlg_acc, tt_account_map)
                        raw_c = str(it.get("content") or "").strip()
                        line = internal_post_title_from_body(raw_c, fallback="")
                        if not line:
                            line = internal_post_title_from_body(
                                str(it.get("title") or Path(vp).stem).strip(), fallback=Path(vp).stem
                            )
                        caption = line
                        tags = [str(x).strip() for x in (it.get("hashtags") or []) if str(x).strip()]
                        job_tt = default_job_dict(account_id=aid_tt, video_path=vp, caption=caption, hashtags=tags)
                        job_tt["schedule_enabled"] = True
                        job_tt["scheduled_at"] = plan["scheduled_at"]
                        job_tt["schedule_time"] = plan["wall"]
                        job_tt["created_by"] = "video_editor_saved_job_import"
                        job_tt["source_project_id"] = str(rec.get("source_project_id") or "")
                        job_tt["source_download_job_id"] = str(rec.get("source_download_job_id") or "")
                        job_tt["source_download_job_label"] = str(rec.get("source_download_job_label") or "")
                        job_tt["source_download_video_id"] = str(it.get("source_download_video_id") or "")
                        tt_batch.append(job_tt)
                        it["item_scheduled_platform"] = "tiktok"
                        it["item_scheduled_job_id"] = str(job_tt.get("id") or "")
                        it["item_scheduled_at"] = _ts_imp
                        created_tt += 1
                        last_tt_aid = aid_tt
                    else:
                        aid_fb, pid_fb = self._ve_resolve_item_fb_ids(
                            it, rec, dlg_acc, dlg_page, fb_account_map, page_map
                        )
                        raw_fb = str(it.get("content") or "").strip()
                        line_fb = internal_post_title_from_body(raw_fb, fallback="")
                        if not line_fb:
                            line_fb = internal_post_title_from_body(
                                str(it.get("title") or Path(vp).stem).strip(), fallback=Path(vp).stem
                            )
                        row_fb: dict[str, Any] = {
                            "id": f"sched_{uuid.uuid4().hex[:10]}",
                            "page_id": pid_fb,
                            "account_id": aid_fb,
                            "post_type": "video",
                            "page_post_style": pps,
                            "title": line_fb,
                            "content": line_fb,
                            "hashtags": [str(x).strip() for x in (it.get("hashtags") or []) if str(x).strip()],
                            "media_files": [vp],
                            "video_path": vp,
                            "scheduled_at": plan["scheduled_at"],
                            "timezone": tz_row,
                            "schedule_recurrence": str(plan.get("schedule_recurrence") or ""),
                            "schedule_slot": str(plan.get("schedule_slot") or ""),
                            "status": "pending",
                            "retry_count": 0,
                            "max_retry": 3,
                            "created_by": "video_editor_saved_job_import",
                            "created_at": datetime.now().isoformat(timespec="seconds"),
                            "source_project_id": str(rec.get("source_project_id") or ""),
                            "source_download_job_id": str(rec.get("source_download_job_id") or ""),
                            "source_download_job_label": str(rec.get("source_download_job_label") or ""),
                            "source_download_video_id": str(it.get("source_download_video_id") or ""),
                        }
                        if plan.get("schedule_daily_slots"):
                            row_fb["schedule_daily_slots"] = str(plan["schedule_daily_slots"])
                            row_fb["schedule_delay_min"] = int(plan["schedule_delay_min"])
                            row_fb["schedule_delay_max"] = int(plan["schedule_delay_max"])
                            row_fb["schedule_start_date"] = str(plan["schedule_start_date"])
                        sbl = str(plan.get("slot_base_local") or "").strip()
                        if sbl:
                            row_fb["slot_base_local"] = sbl[:80]
                        if "delay_applied_min" in plan:
                            row_fb["schedule_delay_applied_min"] = max(0, min(180, int(plan["delay_applied_min"])))
                        fb_batch.append(row_fb)
                        it["item_scheduled_platform"] = "facebook"
                        it["item_scheduled_job_id"] = str(row_fb["id"])
                        it["item_scheduled_at"] = _ts_imp
                        created_fb += 1
                        last_fb_aid, last_fb_pid = aid_fb, pid_fb

                if fb_batch:
                    self._schedule_posts.upsert_many(fb_batch)  # type: ignore[arg-type]
                if tt_batch:
                    job_store.upsert_many(tt_batch)

                rec["status"] = "imported"
                rec["imported_at"] = datetime.now().isoformat(timespec="seconds")
                if created_fb and created_tt:
                    rec["imported_to_platform"] = "mixed"
                    rec["imported_to_account_id"] = f"fb:{last_fb_aid};tt:{last_tt_aid}"[:480]
                    rec["imported_to_page_id"] = last_fb_pid
                elif created_tt:
                    rec["imported_to_platform"] = "tiktok"
                    rec["imported_to_account_id"] = last_tt_aid
                    rec["imported_to_page_id"] = ""
                else:
                    rec["imported_to_platform"] = "facebook"
                    rec["imported_to_account_id"] = last_fb_aid
                    rec["imported_to_page_id"] = last_fb_pid
                self._save_saved_export_schedule_jobs(rows)
                src_hint = str(rec.get("source_download_job_label") or rec.get("source_download_job_id") or "").strip()
                return {
                    "created_fb": created_fb,
                    "created_tt": created_tt,
                    "src_hint": src_hint,
                    "filter_acc": str(var_acc.get() or "Tất cả account"),
                    "filter_page": str(var_page.get() or "Tất cả page"),
                }

            def _import_on_main(res: dict[str, Any]) -> None:
                try:
                    top.grab_release()
                except tk.TclError:
                    pass
                top.destroy()
                created_fb = int(res["created_fb"])
                created_tt = int(res["created_tt"])
                src_hint = str(res.get("src_hint") or "")
                if created_fb:
                    if hasattr(self, "_var_jobs_filter_status"):
                        self._var_jobs_filter_status.set("pending")
                    if hasattr(self, "_var_jobs_filter_account"):
                        self._var_jobs_filter_account.set(str(res.get("filter_acc") or "Tất cả account"))
                    if hasattr(self, "_var_jobs_filter_page"):
                        self._var_jobs_filter_page.set(str(res.get("filter_page") or "Tất cả page"))
                    if hasattr(self, "_var_jobs_filter_retry"):
                        self._var_jobs_filter_retry.set("Retry: tất cả")
                    if hasattr(self, "_var_jobs_search"):
                        self._var_jobs_search.set(src_hint or "video_editor_saved_job_import")
                self._fill_schedule_jobs_tree()
                msg_done = (
                    f"Facebook: {created_fb} job lịch; TikTok: {created_tt} job (có lịch)."
                    if (created_fb and created_tt)
                    else (
                        f"Đã nạp {created_tt} job TikTok (có lịch) vào TikTok Manager."
                        if created_tt
                        else f"Đã nạp {created_fb} job lịch Facebook từ job chờ đăng."
                    )
                )
                messagebox.showinfo("Hoàn tất", msg_done, parent=self._root)

            def _import_on_err(exc: BaseException) -> None:
                if imp_btn is not None:
                    try:
                        imp_btn.configure(state="normal")
                    except tk.TclError:
                        pass
                var_preview.set("Lỗi khi nạp — thử lại.")
                messagebox.showerror("Lỗi nạp", str(exc), parent=top)

            run_background_then_main(top, _import_worker, _import_on_main, on_error=_import_on_err)

        btns = ttk.Frame(frm)
        btns.grid(row=8, column=0, columnspan=2, sticky="e", pady=(10, 0))
        ttk.Button(btns, text="Hủy", command=top.destroy).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btns, text="Lưu gợi ý đích vào file job", command=_save_dest_hint_to_job).pack(side=tk.LEFT, padx=(0, 6))
        btn_import_ve = ttk.Button(btns, text="Nạp vào job lịch", command=_ok)
        btn_import_ve.pack(side=tk.LEFT)
        import_btn_ref["b"] = btn_import_ve
        _sync_kieu_dich_ui()
        frm.pack(fill=tk.BOTH, expand=True)
        try:
            top.update_idletasks()
        except tk.TclError:
            pass
        top.grab_set()
        top.wait_window()

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
        top.geometry("400x124")
        top.minsize(360, 124)
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

    def _pages_rows_for_csv_export(self, mode: str, owner_id: str) -> list[dict[str, Any]]:
        mode = str(mode or "").strip().lower()
        if mode == "selected":
            ids = self._selected_page_ids()
            if not ids:
                raise ValueError("Chọn ít nhất một Page trong bảng.")
            out: list[dict[str, Any]] = []
            for pid in ids:
                rec = self._record_page_by_id(pid)
                if rec is not None:
                    out.append(dict(rec))
            return out
        if mode == "account":
            aid = str(owner_id or "").strip()
            if not aid:
                raise ValueError("Chọn tài khoản (owner) để xuất.")
            rows = list(getattr(self, "_all_pages", []) or [])
            if not rows:
                rows = [dict(r) for r in self._pages.load_all()]
            return [dict(r) for r in rows if str(r.get("account_id", "")).strip() == aid]
        if mode == "filtered":
            return [dict(r) for r in self._pages_filtered_sorted_rows()]
        raise ValueError(f"Phạm vi xuất không hợp lệ: {mode!r}")

    def _on_export_pages_csv(self) -> None:
        owner_ids = sorted(
            {
                str(r.get("account_id", "")).strip()
                for r in (getattr(self, "_all_pages", []) or [])
                if str(r.get("account_id", "")).strip()
            }
        )
        selected = len(self._selected_page_ids())
        filtered = len(self._pages_filtered_sorted_rows())
        dlg = PagesExportDialog(
            self._root,
            owner_account_ids=owner_ids,
            selected_count=selected,
            filtered_count=filtered,
            resolve_rows=self._pages_rows_for_csv_export,
        )
        if dlg.exported_path:
            logger.info("Đã xuất Page CSV: {}", dlg.exported_path)

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
            insights=self._page_insights.all_for_page(pid),
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

    def _notebook_has_tab(self, child: tk.Widget) -> bool:
        try:
            self._nb.tab(child)
            return True
        except tk.TclError:
            return False

    def _apply_platform_view(self, platform_label: str) -> None:
        """
        Chỉ hiển thị nhóm tab theo nền tảng để giao diện gọn hơn:
        - Facebook: tab accounts / page / job lịch
        - TikTok: tab TikTok Manager + tab job chờ đăng từ Video Editor (nạp TikTok từ Export)
        """
        view = (platform_label or "").strip().lower()
        want_tiktok = view.startswith("tiktok")
        if getattr(self, "_platform_view_is_tiktok", None) == want_tiktok:
            return
        self._platform_view_is_tiktok = want_tiktok

        nb = self._nb

        fb_accounts_text = "  1. Tài khoản (accounts.json)  "
        fb_pages_text = "  2. Page / Group (pages.json)  "
        fb_jobs_text = "  3. Job lịch đăng (schedule_posts.json)  "
        ve_pending_text = "  7.Job chờ đăng từ Video Editor  "
        tt_text = "  8. TikTok Manager  "
        human_text = "  9. Tương tác người dùng  "

        if want_tiktok:
            # Ẩn tab Facebook
            for child in (self._tab_facebook_jobs, self._tab_facebook_pages, self._tab_facebook_accounts):
                if child is not None and self._notebook_has_tab(child):
                    nb.forget(child)
            # Ẩn tab tương tác Facebook khi đang ở view TikTok.
            if self._tab_human_interaction is not None and self._notebook_has_tab(self._tab_human_interaction):
                nb.forget(self._tab_human_interaction)
            if self._tab_ve_pending_export is not None and not self._notebook_has_tab(self._tab_ve_pending_export):
                nb.add(self._tab_ve_pending_export, text=ve_pending_text)
            # Hiện TikTok
            if self._tab_tiktok_manager is not None and not self._notebook_has_tab(self._tab_tiktok_manager):
                nb.add(self._tab_tiktok_manager, text=tt_text)
            if self._tab_tiktok_manager is not None and self._notebook_has_tab(self._tab_tiktok_manager):
                nb.select(self._tab_tiktok_manager)
            self._jobs_tab_index = 0
            return

        # want facebook
        # Ẩn TikTok
        if self._tab_tiktok_manager is not None and self._notebook_has_tab(self._tab_tiktok_manager):
            nb.forget(self._tab_tiktok_manager)

        # Hiện tab Facebook (nếu bị quên do toggle trước đó)
        if self._tab_facebook_accounts is not None and not self._notebook_has_tab(self._tab_facebook_accounts):
            nb.add(self._tab_facebook_accounts, text=fb_accounts_text)
        if self._tab_facebook_pages is not None and not self._notebook_has_tab(self._tab_facebook_pages):
            nb.add(self._tab_facebook_pages, text=fb_pages_text)
        if self._tab_facebook_jobs is not None and not self._notebook_has_tab(self._tab_facebook_jobs):
            nb.add(self._tab_facebook_jobs, text=fb_jobs_text)
        if self._tab_human_interaction is not None and not self._notebook_has_tab(self._tab_human_interaction):
            nb.add(self._tab_human_interaction, text=human_text)

        # Cập nhật index tab jobs để nút điều hướng hoạt động đúng.
        if self._tab_facebook_jobs is not None and self._notebook_has_tab(self._tab_facebook_jobs):
            self._jobs_tab_index = nb.index(self._tab_facebook_jobs)
        else:
            self._jobs_tab_index = 0

        # Chuyển tới tab Page để người dùng thấy “chức năng Facebook” ngay.
        if self._tab_facebook_pages is not None and self._notebook_has_tab(self._tab_facebook_pages):
            nb.select(self._tab_facebook_pages)
        elif self._tab_facebook_accounts is not None and self._notebook_has_tab(self._tab_facebook_accounts):
            nb.select(self._tab_facebook_accounts)

    def _on_goto_jobs_for_page(self) -> None:
        """Chuyển sang tab Job; gợi ý tạo job cho Page đang chọn."""
        if getattr(self, "_platform_view_is_tiktok", None):
            # User đang xem TikTok -> tự chuyển sang Facebook để tab job tồn tại.
            self._platform_view_var.set("Facebook")
            self._apply_platform_view("Facebook")
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

    def _on_open_schedule_jobs_tab_event(self, _event: tk.Event | None = None) -> None:
        """Nhận event từ Video Editor: mở tab «7.Job chờ đăng từ Video Editor» (và popup nạp nếu được)."""
        t_vp = getattr(self, "_tab_ve_pending_export", None)
        if t_vp is not None and self._notebook_has_tab(t_vp):
            try:
                self._nb.select(self._nb.index(t_vp))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Không chuyển tab job chờ đăng từ event: {}", exc)
                return
            try:
                self._on_import_saved_export_job()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Không mở popup nạp job chờ đăng tự động: {}", exc)
            return
        if getattr(self, "_platform_view_is_tiktok", None):
            self._platform_view_var.set("Facebook")
            self._apply_platform_view("Facebook")
        try:
            self._nb.select(self._jobs_tab_index)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không chuyển tab Job lịch đăng từ event: {}", exc)
            return
        try:
            self._on_import_saved_export_job()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không mở popup nạp job chờ đăng tự động: {}", exc)

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

        self._set_ui_busy("open_profile_browser")
        self._root.configure(cursor="watch")
        th = threading.Thread(target=_open_worker, name="open_profile_playwright", daemon=True)
        th.start()
        t0 = time.monotonic()
        timeout_sec = 120.0

        def _poll_open_profile() -> None:
            if state["ready"].is_set():
                self._root.configure(cursor="")
                self._clear_ui_busy()
                self._manual_profile_sessions.append({"account_id": aid, "thread": th, "shutdown": shutdown_evt})
                messagebox.showinfo(
                    "Đã mở profile",
                    f"Đã mở profile browser cho tài khoản {aid}.\nBạn có thể thao tác/login trực tiếp trên cửa sổ này.",
                    parent=self._root,
                )
                return
            if state.get("err") is not None:
                self._root.configure(cursor="")
                self._clear_ui_busy()
                shutdown_evt.set()
                th.join(timeout=15.0)
                messagebox.showerror("Mở profile thất bại", str(state["err"]), parent=self._root)
                return
            if not th.is_alive() and not state["ready"].is_set():
                self._root.configure(cursor="")
                self._clear_ui_busy()
                messagebox.showerror(
                    "Mở profile thất bại",
                    "Luồng mở trình duyệt đã kết thúc sớm.",
                    parent=self._root,
                )
                return
            if time.monotonic() - t0 >= timeout_sec:
                self._root.configure(cursor="")
                self._clear_ui_busy()
                shutdown_evt.set()
                th.join(timeout=15.0)
                messagebox.showerror(
                    "Mở profile",
                    "Hết thời gian chờ (120s) — luồng mở trình duyệt chưa xong.",
                    parent=self._root,
                )
                return
            self._root.after(100, _poll_open_profile)

        self._root.after(100, _poll_open_profile)

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
        ids_copy = list(ids)
        self._set_ui_busy("verify_profile")
        self._root.configure(cursor="watch")

        def _verify_worker() -> None:
            lines: list[str] = []
            n_ok = 0
            n_fail = 0
            for aid in ids_copy:
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

            def _verify_done() -> None:
                self._clear_ui_busy()
                self._root.configure(cursor="")
                body = "\n".join(lines[:80])
                if len(lines) > 80:
                    body += f"\n… (+{len(lines) - 80} tài khoản)"
                title = f"Verify Profile ({len(ids_copy)} tài khoản — OK {n_ok}, lỗi {n_fail})"
                if n_fail == 0:
                    messagebox.showinfo(title, body, parent=self._root)
                elif n_ok == 0:
                    messagebox.showerror(title, body, parent=self._root)
                else:
                    messagebox.showwarning(title, body, parent=self._root)

            self._root.after(0, _verify_done)

        threading.Thread(target=_verify_worker, name="verify_profile", daemon=True).start()

    def _on_check_proxy(self) -> None:
        ids = self._profile_ids_for_bulk()
        if not ids:
            messagebox.showwarning(
                "Chưa chọn",
                "Tick ít nhất một ô «Chọn» (☑), hoặc chọn dòng trong bảng (Ctrl/Shift / kéo chuột).",
                parent=self._root,
            )
            return
        ids_copy = list(ids)
        self._set_ui_busy("check_proxy")
        self._root.configure(cursor="watch")

        def _proxy_worker() -> None:
            lines: list[str] = []
            n_live = 0
            n_die = 0
            n_skip = 0
            for aid in ids_copy:
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
                ok, msg, scheme = check_proxy(
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
                    kind = scheme.upper() if scheme != "none" else "?"
                    lines.append(f"• {aid}: LIVE ({kind}) — {ip}")
                else:
                    n_die += 1
                    err = (msg or "").replace("\n", " ")
                    if len(err) > 120:
                        err = err[:117] + "…"
                    lines.append(f"• {aid}: DIE / lỗi — {err}")

            def _proxy_done() -> None:
                self._clear_ui_busy()
                self._root.configure(cursor="")
                body = "\n".join(lines[:80])
                if len(lines) > 80:
                    body += f"\n… (+{len(lines) - 80} tài khoản)"
                title = (
                    f"Kiểm tra proxy ({len(ids_copy)} dòng — LIVE {n_live}, DIE/lỗi {n_die}, bỏ qua {n_skip})"
                )
                if n_die == 0:
                    messagebox.showinfo(title, body, parent=self._root)
                elif n_live == 0:
                    messagebox.showerror(title, body, parent=self._root)
                else:
                    messagebox.showwarning(title, body, parent=self._root)

            self._root.after(0, _proxy_done)

        threading.Thread(target=_proxy_worker, name="check_proxy", daemon=True).start()

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
            q = (
                f"Xóa vĩnh viễn tài khoản {ids[0]!r}?\n\n"
                "Sẽ xóa luôn profile Firefox/Playwright riêng, file cookie và mật khẩu trong vault."
            )
        else:
            preview = ", ".join(ids[:15])
            if len(ids) > 15:
                preview += f", … (+{len(ids) - 15})"
            q = (
                f"Xóa vĩnh viễn {len(ids)} tài khoản?\n\n{preview}\n\n"
                "Sẽ xóa profile Playwright, cookie và vault của từng tài khoản."
            )
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
        base["import_type"] = "duplicate"
        from src.utils.account_browser_profile import default_cookie_path, default_portable_path

        bt = str(base.get("browser_type") or "firefox")
        base["portable_path"] = default_portable_path(new_id, bt)
        base["profile_path"] = base["portable_path"]
        base["cookie_path"] = default_cookie_path(new_id)
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
                from src.utils.account_browser_profile import relativize_account_storage_path

                rel_pp = relativize_account_storage_path(str(resolved))
                acc["portable_path"] = rel_pp
                acc["profile_path"] = rel_pp
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
            text=(
                "URL manifest (khuyên dùng raw GitHub: …/main/release/update/latest.json — "
                "không cần tạo Release chỉ để có latest.json):"
            ),
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
        elif "raw.githubusercontent.com/" in gh_url and "/release/update/latest.json" in gh_url:
            try:
                mid = gh_url.split("raw.githubusercontent.com/", 1)[1]
                parts = mid.split("/", 2)
                if len(parts) >= 2:
                    var_repo.set(f"{parts[0]}/{parts[1]}")
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
            """Điền owner/repo + URL manifest từ ``git remote origin``; nếu không có .git thì dùng repo công khai ToolFB."""
            rid = github_owner_repo_from_git(project_root())
            if not rid:
                var_repo.set(TOOLFB_PUBLIC_REPO)
                try:
                    var_manifest.set(github_latest_manifest_url(TOOLFB_PUBLIC_REPO))
                except Exception as exc:
                    messagebox.showerror("Kênh cập nhật", str(exc), parent=top)
                    return
                messagebox.showinfo(
                    "Kênh cập nhật",
                    (
                        "Không đọc được ``git remote origin`` (bản copy không có .git, hoặc chạy .exe).\n"
                        "Đã điền manifest mặc định của ToolFB (nhánh main).\n\n"
                        "Nếu bạn dùng fork GitHub — sửa ô owner/repo rồi bấm «Tạo URL GitHub»."
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
            "Manifest mặc định đọc từ nhánh main (raw GitHub: release/update/latest.json), không phụ thuộc file Release cũ.\n"
            "«Tự động từ Git remote»: có clone thì lấy origin; không có .git thì điền sẵn repo công khai ToolFB.\n"
            "Nếu chạy từ thư mục git clone: nút «Cập nhật» ưu tiên git pull.\n"
            "Biến môi trường TOOLFB_UPDATE_MANIFEST_URL (nếu có) vẫn được ưu tiên."
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
            if url:
                url = prefer_repo_raw_manifest_url(url)
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
                "Đã lưu config/update_channel.json.\n"
                "Vài giây sau app sẽ kiểm tra nền — nếu có bản mới sẽ hiện nút «Cập nhật».\n"
                "Hoặc bấm «Chỉ kiểm tra» để xem ngay; để tải/cài bản zip hãy bấm «Cập nhật».",
                parent=self._root,
            )
            self._root.after(400, self._schedule_probe_update_button_visibility)
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
        self._set_ui_busy("reset_veo3_profile")
        self._root.configure(cursor="watch")

        def _reset_veo3_worker() -> None:
            base = root / "data" / "nanobanana"
            main_profile = base / "browser_profile"
            recovery_profile = base / "browser_profile_recovery"
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_root = root / "data" / "backups" / f"veo3_profile_reset_{ts}"
            moved: list[str] = []
            errors: list[str] = []

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

            try:
                backup_root.mkdir(parents=True, exist_ok=True)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Tạo thư mục backup: {exc}")

            if not errors:
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

                try:
                    main_profile.mkdir(parents=True, exist_ok=True)
                    recovery_profile.mkdir(parents=True, exist_ok=True)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"Tạo profile mới: {exc}")

            def _reset_done() -> None:
                self._clear_ui_busy()
                self._root.configure(cursor="")
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

            self._root.after(0, _reset_done)

        threading.Thread(target=_reset_veo3_worker, name="reset_veo3_profile", daemon=True).start()

    def _show_apply_update_button(self) -> None:
        """Hiện nút «Cập nhật» (đặt bên phải «Chỉ kiểm tra») khi có bản mới trên remote."""
        try:
            if self._btn_apply_update.winfo_manager():
                return
            self._btn_apply_update.pack(side=tk.LEFT, padx=(0, 4), after=self._apply_update_pack_after)
        except tk.TclError:
            pass

    def _hide_apply_update_button(self) -> None:
        """Ẩn nút «Cập nhật» khi không có bản mới hoặc sau khi cập nhật xong."""
        try:
            if self._btn_apply_update.winfo_manager():
                self._btn_apply_update.pack_forget()
            self._btn_apply_update.configure(state=tk.NORMAL)
        except tk.TclError:
            pass

    def _schedule_startup_git_sync(self) -> None:
        """
        Sau khi mở GUI: với bản git clone — tự ``git pull`` nếu bật (``config/auto_update`` / env);
        luôn kiểm tra để hiện nút «Cập nhật» khi còn commit mới (dirty tree / lỗi pull).
        """

        def worker() -> None:
            pulled = False
            commits_pulled = 0
            has_new = False
            root_p = project_root()
            try:
                if should_use_git_updates(root_p):
                    outcome = maybe_auto_git_pull_on_startup(root_p, timeout_fetch=120)
                    pulled = outcome.pulled
                    info = outcome.check_result
                    if pulled and info is not None:
                        commits_pulled = max(0, info.commits_behind)
                    if pulled:
                        has_new = False
                    elif info is not None:
                        has_new = bool(info.ok and info.has_new_commits)
                else:
                    mu = resolve_manifest_url(root_p)
                    if mu:
                        lv = read_local_version(root_p)
                        mf = read_manifest_from_url(mu, timeout_sec=18)
                        has_new = is_newer_version(mf.version, lv)
            except Exception:
                has_new = False
                pulled = False

            def on_main() -> None:
                if pulled:
                    self._git_update_result = None
                    self._app_version_str = read_local_version(root_p)
                    self._root.title(
                        f"Facebook Automation — Bảng điều khiển (v{self._app_version_str})"
                    )
                    self._lbl_app_version.configure(text=f"Phiên bản {self._app_version_str}")
                    tip = (
                        f"Đã tự cập nhật {commits_pulled} commit từ GitHub."
                        if commits_pulled
                        else "Đã tự cập nhật code từ GitHub."
                    )
                    self._lbl_state.configure(text=tip)
                    self._hide_apply_update_button()
                    self._show_update_success_restart_dialog(
                        version=self._app_version_str,
                        backup_dir=None,
                    )
                elif has_new:
                    self._show_apply_update_button()
                else:
                    self._hide_apply_update_button()

            try:
                self._root.after(0, on_main)
            except Exception:
                pass

        threading.Thread(target=worker, name="startup_git_sync", daemon=True).start()

    def _git_run_pull_thread(self, root: Path, info: GitUpdateCheckResult) -> None:
        """
        Chạy ``git pull --ff-only`` trên thread nền; gọi từ main thread khi đã bật busy và tắt nút.

        ``self._lbl_state`` nên đã ghi «đang pull» trước khi gọi (tránh gọi Tk từ worker).
        """

        def worker_pull() -> None:
            try:
                ok, msg = apply_git_pull_ff(root, result=info)

                def done_pull() -> None:
                    if ok:
                        self._git_update_result = None
                        self._app_version_str = read_local_version(root)
                        self._root.title(
                            f"Facebook Automation — Bảng điều khiển (v{self._app_version_str})"
                        )
                        self._lbl_app_version.configure(text=f"Phiên bản {self._app_version_str}")
                        self._lbl_state.configure(text="Update (git): hoàn tất — khởi động lại")
                        self._clear_ui_busy()
                        self._btn_check_updates.configure(state=tk.NORMAL)
                        self._btn_apply_update.configure(state=tk.NORMAL)
                        self._show_update_success_restart_dialog(
                            version=self._app_version_str,
                            backup_dir=None,
                        )
                    else:
                        self._btn_check_updates.configure(state=tk.NORMAL)
                        self._btn_apply_update.configure(state=tk.NORMAL)
                        self._lbl_state.configure(text="Update (git): lỗi")
                        self._clear_ui_busy()
                        messagebox.showerror(
                            "Cập nhật (git)",
                            msg[:8000] or "git pull thất bại.",
                            parent=self._root,
                        )

                self._root.after(0, done_pull)
            except Exception as exc:
                err_text = str(exc)

                def done_err() -> None:
                    self._btn_check_updates.configure(state=tk.NORMAL)
                    self._btn_apply_update.configure(state=tk.NORMAL)
                    self._lbl_state.configure(text="Update (git): lỗi")
                    self._clear_ui_busy()
                    messagebox.showerror("Cập nhật (git)", err_text, parent=self._root)

                self._root.after(0, done_err)

        threading.Thread(target=worker_pull, name="apply_git_pull", daemon=True).start()

    def _run_manifest_download_apply_thread(self, mf: UpdateManifest) -> None:
        """
        Tải + áp dụng gói manifest trên thread nền; gọi từ main thread khi đã busy.

        Cập nhật nhãn trạng thái «đang tải…» trước khi bắt đầu tải.
        """
        has_patch = bool((mf.patch_download_url or "").strip())
        self._lbl_state.configure(
            text="Update: đang tải bản vá (nhẹ)…" if has_patch else "Update: đang tải & áp dụng…"
        )

        def worker_download() -> None:
            try:
                backup_dir = apply_update_package(project_root=project_root(), manifest=mf)

                def done_ok() -> None:
                    self._lbl_state.configure(text="Update: hoàn tất — khởi động lại để dùng bản mới")
                    self._clear_ui_busy()
                    self._btn_check_updates.configure(state=tk.NORMAL)
                    self._btn_apply_update.configure(state=tk.NORMAL)
                    self._show_update_success_restart_dialog(version=str(mf.version), backup_dir=backup_dir)

                self._root.after(0, done_ok)
            except Exception as exc:  # noqa: BLE001
                err_text = str(exc)

                def done_err() -> None:
                    self._btn_check_updates.configure(state=tk.NORMAL)
                    self._btn_apply_update.configure(state=tk.NORMAL)
                    self._lbl_state.configure(text="Update: lỗi")
                    self._clear_ui_busy()
                    messagebox.showerror("Cập nhật", f"Cập nhật thất bại:\n{err_text}", parent=self._root)

                self._root.after(0, done_err)

        threading.Thread(target=worker_download, name="apply_update_download", daemon=True).start()

    def _on_check_updates(self) -> None:
        """«Chỉ kiểm tra»: git fetch / đọc manifest; nếu có bản mới thì hiện nút «Cập nhật» (zip: không tự tải — bấm «Cập nhật»)."""
        root = project_root()
        if should_use_git_updates(root):

            def worker_git() -> None:
                try:
                    info = check_git_updates(root)

                    def done_git() -> None:
                        self._git_update_result = info if info.ok else None
                        self._latest_update_manifest = None
                        if not info.ok:
                            self._hide_apply_update_button()
                            self._btn_check_updates.configure(state=tk.NORMAL)
                            self._btn_apply_update.configure(state=tk.NORMAL)
                            self._clear_ui_busy()
                            messagebox.showerror(
                                "Cập nhật (git)",
                                info.error or "Không kiểm tra được qua git.",
                                parent=self._root,
                            )
                            return
                        if info.has_new_commits:
                            self._show_apply_update_button()
                            self._btn_check_updates.configure(state=tk.NORMAL)
                            self._btn_apply_update.configure(state=tk.NORMAL)
                            self._clear_ui_busy()
                            messagebox.showinfo(
                                "Cập nhật (git)",
                                (
                                    f"Có thay đổi trên {info.remote_ref} mà bạn chưa kéo về "
                                    f"({info.commits_behind} commit, nhánh «{info.branch}»).\n\n"
                                    "Bấm «Cập nhật» để chạy git pull — «Chỉ kiểm tra» không tự pull."
                                ),
                                parent=self._root,
                            )
                            return
                        self._hide_apply_update_button()
                        self._btn_check_updates.configure(state=tk.NORMAL)
                        self._btn_apply_update.configure(state=tk.NORMAL)
                        self._clear_ui_busy()
                        lv = read_local_version(root)
                        messagebox.showinfo(
                            "Cập nhật (git)",
                            (
                                f"Đã đồng bộ với {info.remote_ref} (nhánh «{info.branch}»).\n"
                                f"Commit: {info.local_sha_short} — version.json: {lv}"
                            ),
                            parent=self._root,
                        )

                    self._root.after(0, done_git)
                except Exception as exc:  # noqa: BLE001
                    err_text = str(exc)

                    def done_err() -> None:
                        self._hide_apply_update_button()
                        self._btn_check_updates.configure(state=tk.NORMAL)
                        self._btn_apply_update.configure(state=tk.NORMAL)
                        self._clear_ui_busy()
                        messagebox.showerror("Cập nhật (git)", f"Lỗi:\n{err_text}", parent=self._root)

                    self._root.after(0, done_err)

            self._set_ui_busy("check_updates")
            self._btn_check_updates.configure(state=tk.DISABLED)
            self._btn_apply_update.configure(state=tk.DISABLED)
            threading.Thread(target=worker_git, name="check_updates_git", daemon=True).start()
            return

        manifest_url = resolve_manifest_url(root)
        if not manifest_url:
            self._hide_apply_update_button()
            if messagebox.askyesno(
                "Cập nhật",
                (
                    "Không phát hiện thư mục git (.git) hoặc không có lệnh git — cần kênh manifest (zip).\n\n"
                    "Mở «Cấu hình kênh cập nhật» để nhập URL GitHub Release (latest.json)?\n\n"
                    "(Clone repo rồi chạy trong thư mục đó để cập nhật bằng git pull; "
                    "hoặc TOOLFB_UPDATE_MANIFEST_URL / dist/latest.json khi dev.)"
                ),
                parent=self._root,
            ):
                self._on_configure_update_channel()
            return
        self._git_update_result = None
        self._set_ui_busy("check_updates")
        self._btn_check_updates.configure(state=tk.DISABLED)
        self._btn_apply_update.configure(state=tk.DISABLED)

        def worker() -> None:
            try:
                local_v = read_local_version(project_root())
                mf = read_manifest_from_url(manifest_url)
                has_new = is_newer_version(mf.version, local_v)

                def done_ok() -> None:
                    self._latest_update_manifest = mf if has_new else None
                    if has_new:
                        self._show_apply_update_button()
                        self._btn_check_updates.configure(state=tk.NORMAL)
                        self._btn_apply_update.configure(state=tk.NORMAL)
                        self._clear_ui_busy()
                        messagebox.showinfo(
                            "Cập nhật",
                            (
                                f"Có phiên bản mới trên kênh: {mf.version} (đang dùng {local_v}).\n\n"
                                "Bấm «Cập nhật» để tải và cài — «Chỉ kiểm tra» không tự cài bản zip."
                            ),
                            parent=self._root,
                        )
                        return
                    self._hide_apply_update_button()
                    self._btn_check_updates.configure(state=tk.NORMAL)
                    self._btn_apply_update.configure(state=tk.NORMAL)
                    self._clear_ui_busy()
                    root_p = project_root()
                    rid = resolve_github_owner_repo_for_version_check(root_p)
                    raw_v, raw_br = read_remote_version_from_github_raw(rid) if rid else (None, "")
                    if (
                        raw_v
                        and is_newer_version(raw_v, local_v)
                        and is_newer_version(raw_v, mf.version)
                    ):
                        messagebox.showwarning(
                            "Cập nhật — manifest chưa kịp theo GitHub",
                            (
                                f"Manifest kênh zip hiện báo bản {mf.version}; trên nhánh «{raw_br}» "
                                f"(raw) có version.json = {raw_v} — có thể chưa đóng gói Release.\n\n"
                                "• Bản zip/.exe: không cần thư mục .git hay đăng nhập GitHub; khi maintainer đã "
                                "đăng latest.json + zip lên Release «Latest», bấm «Chỉ kiểm tra» rồi «Cập nhật».\n"
                                "• Máy clone: có thể dùng git pull nếu đã cài Git và mở app trong thư mục có .git "
                                r"(hoặc TOOLFB_GIT trỏ tới git.exe)."
                            ),
                            parent=self._root,
                        )
                    else:
                        messagebox.showinfo(
                            "Cập nhật",
                            f"Bạn đang dùng bản mới nhất ({local_v}).",
                            parent=self._root,
                        )

                self._root.after(0, done_ok)
            except Exception as exc:  # noqa: BLE001
                err_text = str(exc)

                def done_err() -> None:
                    self._hide_apply_update_button()
                    self._btn_check_updates.configure(state=tk.NORMAL)
                    self._btn_apply_update.configure(state=tk.NORMAL)
                    self._clear_ui_busy()
                    messagebox.showerror("Cập nhật", f"Kiểm tra bản mới thất bại:\n{err_text}", parent=self._root)

                self._root.after(0, done_err)

        threading.Thread(target=worker, name="check_updates", daemon=True).start()

    def _show_update_success_restart_dialog(self, *, version: str, backup_dir: Path | None) -> None:
        """
        Sau cập nhật thành công: nút mở lại chương trình ngay (khuyến nghị) + để sau.
        """
        self._hide_apply_update_button()
        top = tk.Toplevel(self._root)
        top.title("Cập nhật xong — mở lại chương trình")
        top.transient(self._root)
        top.geometry("680x300")
        top.minsize(520, 240)
        try:
            top.grab_set()
        except Exception:
            pass
        fr = ttk.Frame(top, padding=16)
        fr.pack(fill=tk.BOTH, expand=True)
        extra = ""
        if getattr(sys, "frozen", False) and (project_root() / "data" / "updates" / DEFERRED_GUI_BAT_NAME).is_file():
            extra = (
                "\n\n(Windows) Bản .exe và thư mục _internal sẽ được thay sau khi bạn bấm mở lại "
                "(có thể thấy cửa sổ lệnh tối thiểu vài giây — bình thường)."
            )
        backup_line = (
            str(backup_dir)
            if backup_dir is not None
            else "(Cập nhật qua git — lịch sử trong .git; hoàn tác: git revert / git checkout nếu cần.)"
        )
        msg = (
            f"Đã cập nhật lên phiên bản {version}.\n\n"
            f"Backup / ghi chú:\n{backup_line}\n\n"
            "Nên bấm «Mở lại chương trình ngay» để dùng bản mới (cửa sổ hiện tại sẽ đóng và app mở lại).\n"
            "Phím Enter = mở lại ngay. Esc = để sau."
            f"{extra}"
        )
        lbl_msg = ttk.Label(fr, text=msg, wraplength=620, justify=tk.LEFT)
        lbl_msg.pack(anchor="w", pady=(0, 14))
        top.bind(
            "<Configure>",
            lambda _e: lbl_msg.configure(wraplength=max(360, int(top.winfo_width()) - 60)),
            add="+",
        )
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
        """«Cập nhật»: git pull hoặc tải zip manifest; nút chỉ hiện khi đã phát hiện bản mới (hoặc trong lúc chạy)."""
        root = project_root()
        if should_use_git_updates(root):
            self._set_ui_busy("apply_update")
            self._btn_check_updates.configure(state=tk.DISABLED)
            self._btn_apply_update.configure(state=tk.DISABLED)
            self._lbl_state.configure(text="Update (git): đang kiểm tra…")

            def worker_git_apply() -> None:
                try:
                    info = check_git_updates(root)
                    if not info.ok:

                        def on_bad() -> None:
                            self._hide_apply_update_button()
                            self._clear_ui_busy()
                            self._btn_check_updates.configure(state=tk.NORMAL)
                            self._btn_apply_update.configure(state=tk.NORMAL)
                            self._lbl_state.configure(text="")
                            messagebox.showerror(
                                "Cập nhật (git)",
                                info.error or "Không kiểm tra được qua git.",
                                parent=self._root,
                            )

                        self._root.after(0, on_bad)
                        return
                    if not info.has_new_commits:

                        def on_uptodate() -> None:
                            self._hide_apply_update_button()
                            self._clear_ui_busy()
                            self._btn_check_updates.configure(state=tk.NORMAL)
                            self._btn_apply_update.configure(state=tk.NORMAL)
                            self._lbl_state.configure(text="")
                            messagebox.showinfo(
                                "Cập nhật (git)",
                                (
                                    f"Đã đồng bộ — không có commit mới trên {info.remote_ref}.\n"
                                    f"Local {info.local_sha_short} (nhánh «{info.branch}»)."
                                ),
                                parent=self._root,
                            )

                        self._root.after(0, on_uptodate)
                        return

                    ui_evt = threading.Event()

                    def on_ui_pulling() -> None:
                        self._show_apply_update_button()
                        self._lbl_state.configure(text="Update (git): đang pull…")
                        ui_evt.set()

                    self._root.after(0, on_ui_pulling)
                    ui_evt.wait(timeout=60)
                    ok, msg = apply_git_pull_ff(root, result=info)

                    def done_pull() -> None:
                        if ok:
                            self._git_update_result = None
                            self._app_version_str = read_local_version(root)
                            self._root.title(
                                f"Facebook Automation — Bảng điều khiển (v{self._app_version_str})"
                            )
                            self._lbl_app_version.configure(text=f"Phiên bản {self._app_version_str}")
                            self._lbl_state.configure(text="Update (git): hoàn tất — khởi động lại")
                            self._clear_ui_busy()
                            self._btn_check_updates.configure(state=tk.NORMAL)
                            self._btn_apply_update.configure(state=tk.NORMAL)
                            self._show_update_success_restart_dialog(
                                version=self._app_version_str,
                                backup_dir=None,
                            )
                        else:
                            self._btn_check_updates.configure(state=tk.NORMAL)
                            self._btn_apply_update.configure(state=tk.NORMAL)
                            self._lbl_state.configure(text="Update (git): lỗi")
                            self._clear_ui_busy()
                            messagebox.showerror(
                                "Cập nhật (git)",
                                msg[:8000] or "git pull thất bại.",
                                parent=self._root,
                            )

                    self._root.after(0, done_pull)
                except Exception as exc:  # noqa: BLE001
                    err_text = str(exc)

                    def done_err() -> None:
                        self._hide_apply_update_button()
                        self._btn_check_updates.configure(state=tk.NORMAL)
                        self._btn_apply_update.configure(state=tk.NORMAL)
                        self._lbl_state.configure(text="")
                        self._clear_ui_busy()
                        messagebox.showerror("Cập nhật (git)", err_text, parent=self._root)

                    self._root.after(0, done_err)

            threading.Thread(target=worker_git_apply, name="apply_update_git", daemon=True).start()
            return

        manifest_url = resolve_manifest_url(root)
        if not manifest_url:
            self._hide_apply_update_button()
            if messagebox.askyesno(
                "Cập nhật",
                (
                    "Không có .git / git — cần URL manifest (latest.json) để tải zip.\n\n"
                    "Mở «Cấu hình kênh cập nhật»?\n\n"
                    "(Clone repo và chạy trong thư mục đó để dùng git pull.)"
                ),
                parent=self._root,
            ):
                self._on_configure_update_channel()
            return

        self._git_update_result = None
        self._set_ui_busy("apply_update")
        self._btn_check_updates.configure(state=tk.DISABLED)
        self._btn_apply_update.configure(state=tk.DISABLED)
        self._lbl_state.configure(text="Update: đang kiểm tra manifest…")

        def worker_manifest_apply() -> None:
            try:
                local_v = read_local_version(project_root())
                mf = read_manifest_from_url(manifest_url)
                has_new = is_newer_version(mf.version, local_v)

                if not has_new:

                    def on_no() -> None:
                        self._hide_apply_update_button()
                        self._latest_update_manifest = None
                        self._clear_ui_busy()
                        self._btn_check_updates.configure(state=tk.NORMAL)
                        self._btn_apply_update.configure(state=tk.NORMAL)
                        self._lbl_state.configure(text="")
                        messagebox.showinfo(
                            "Cập nhật",
                            f"Bạn đang dùng bản mới nhất ({local_v}). Không cần cập nhật.",
                            parent=self._root,
                        )

                    self._root.after(0, on_no)
                    return

                ui_evt = threading.Event()

                def on_ui_start() -> None:
                    self._show_apply_update_button()
                    self._latest_update_manifest = mf
                    has_patch = bool((mf.patch_download_url or "").strip())
                    self._lbl_state.configure(
                        text="Update: đang tải bản vá (nhẹ)…" if has_patch else "Update: đang tải & áp dụng…"
                    )
                    ui_evt.set()

                self._root.after(0, on_ui_start)
                ui_evt.wait(timeout=60)
                backup_dir = apply_update_package(project_root=project_root(), manifest=mf)

                def on_done() -> None:
                    self._lbl_state.configure(text="Update: hoàn tất — khởi động lại để dùng bản mới")
                    self._clear_ui_busy()
                    self._btn_check_updates.configure(state=tk.NORMAL)
                    self._btn_apply_update.configure(state=tk.NORMAL)
                    self._show_update_success_restart_dialog(version=str(mf.version), backup_dir=backup_dir)

                self._root.after(0, on_done)
            except Exception as exc:  # noqa: BLE001
                err_text = str(exc)

                def on_err() -> None:
                    self._btn_check_updates.configure(state=tk.NORMAL)
                    self._btn_apply_update.configure(state=tk.NORMAL)
                    self._lbl_state.configure(text="Update: lỗi")
                    self._clear_ui_busy()
                    messagebox.showerror("Cập nhật", f"Cập nhật thất bại:\n{err_text}", parent=self._root)

                self._root.after(0, on_err)

        threading.Thread(target=worker_manifest_apply, name="apply_update_manifest", daemon=True).start()

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

        from src.utils.concurrency_runtime import workload_begin

        workload_begin("scheduler")
        self._worker = threading.Thread(target=runner, name="fb_scheduler", daemon=True)
        self._worker.start()
        self._btn_start.configure(state=tk.DISABLED)
        self._btn_stop.configure(state=tk.NORMAL)
        self._lbl_state.configure(text="Lịch: đang chạy")
        self._start_multitask_reconcile_timer()
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
        from src.utils.concurrency_runtime import workload_end

        self._set_ui_busy("stop_scheduler")
        workload_end("scheduler")
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
        self._stop_multitask_reconcile_timer()
        try:
            from src.utils.concurrency_runtime import workload_end

            workload_end("scheduler")
        except Exception:
            pass
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
        try:
            shutdown_ve = getattr(self, "_shutdown_video_editor_tab", None)
            if callable(shutdown_ve):
                shutdown_ve()
        except Exception:  # noqa: BLE001
            pass
        self._stop_ui_watchdog()
        self._detach_log_sink()
        try:
            self._root.after(0, self._root.destroy)
        except Exception:  # noqa: BLE001
            self._root.destroy()
