from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
import tkinter as tk
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from loguru import logger
from tkinter import filedialog, messagebox, ttk

from src.services.ai_video_generation_service import AIVideoGenerationService
from src.services.reverse_video_prompt_engine import VideoReversePromptEngine, ensure_reverse_video_layout
from src.services.facebook_reels_catalog import (
    normalize_facebook_reels_tab_url,
    scan_facebook_profile_reels_page,
)
from src.services.instagram_reels_catalog import (
    normalize_instagram_reels_tab_url,
    scan_instagram_profile_reels_page,
)
from src.services.universal_video_downloader import (
    DOWNLOAD_JOB_FINISHED_TK_EVENT,
    UV_DOWNLOAD_SEQUENTIAL_THRESHOLD,
    UV_MAX_PLAYLIST_ENTRIES,
    UniversalVideoDownloader,
    _extract_hashtags_from_text,
    arm_auto_import_download_job,
    classify_url_type,
    detect_platform,
    ensure_downloader_layout,
    extract_failed_download_pairs,
    load_universal_video_downloader_config,
    persist_facebook_reels_settings,
    persist_instagram_reels_settings,
    set_root_pending_download_job,
    write_failed_download_urls_log,
    write_pending_video_editor_job,
)
from src.gui.treeview_shortcuts import install_treeview_shortcuts
from src.gui.ui_responsiveness import (
    DEFAULT_TREE_APPEND_CHUNK,
    DEFAULT_TREE_CHUNK,
    DEFAULT_TREE_SELECT_CHUNK,
    run_background_then_main,
    tree_delete_all,
    tree_insert_chunked,
    tree_select_all_chunked,
)
from src.utils.app_secrets import get_nanobanana_runtime_config
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.paths import project_root

_INTERNAL_TOOL_DIR = project_root() / "tools" / "Veo3Studio"
_INTERNAL_TOOL_EXE = _INTERNAL_TOOL_DIR / "Veo3Studio.exe"
_EXTERNAL_TOOL_DIR = Path(r"C:\Users\Hello\Desktop\Tool")
_EXTERNAL_TOOL_EXE = _EXTERNAL_TOOL_DIR / "Veo3Studio.exe"
UV_LIBRARY_UI_MAX_ROWS = 1200
UV_CHANNEL_LIST_MAX = UV_MAX_PLAYLIST_ENTRIES
UV_FB_MAX_COLLECT = UV_MAX_PLAYLIST_ENTRIES
# Trên ngưỡng này «Chọn hết» dùng chọn logic (không highlight từng dòng) để GUI không treo.
UV_LOGICAL_SELECT_ALL_THRESHOLD = 280


class _UvCollapsibleSection:
    """Khối có nút ▼/▶ thu gọn (dùng cho Bước 2 Facebook / YouTube / TikTok trên tab Tải video)."""

    def __init__(
        self,
        parent: tk.Misc,
        *,
        title: str,
        start_open: bool = True,
        on_toggle: Callable[[], None] | None = None,
    ) -> None:
        self.outer = ttk.Frame(parent)
        self._on_toggle = on_toggle
        self._open = tk.BooleanVar(value=bool(start_open))
        hdr = ttk.Frame(self.outer)
        hdr.pack(fill=tk.X)
        self._btn = ttk.Button(hdr, width=3, command=self._toggle)
        self._btn.pack(side=tk.LEFT)
        ttk.Label(hdr, text=title, font=("Segoe UI", 9, "bold")).pack(side=tk.LEFT, padx=(6, 0))
        self.body = ttk.Frame(self.outer)
        self._apply_visibility()

    def _toggle(self) -> None:
        self._open.set(not self._open.get())
        self._apply_visibility()
        if self._on_toggle is not None:
            self._on_toggle()

    def _apply_visibility(self) -> None:
        if self._open.get():
            self._btn.configure(text="▼")
            self.body.pack(fill=tk.BOTH, expand=True, pady=(6, 0))
        else:
            self._btn.configure(text="▶")
            self.body.pack_forget()


def ai_video_project_gate_dialog(parent: tk.Misc) -> dict[str, Any] | None:
    """
    Cổng vào tối giản cho module AI Video sạch.
    Trả về spec để tương thích với luồng gọi hiện tại trong manager_app.
    """
    ok = messagebox.askyesno(
        "AI Video Gemini/Veo",
        "Module AI Video Gemini/Veo đã được làm sạch để tích hợp tool mới.\n\n"
        "Bấm Yes để mở màn hình trống (placeholder).",
        parent=parent,
    )
    if not ok:
        return None
    return {
        "action": "open_clean_module",
        "created_at": datetime.now().replace(microsecond=0).isoformat(),
    }


def _nearest_existing_parent(path_like: Path) -> Path | None:
    p = Path(path_like)
    for cand in (p, *p.parents):
        if cand.exists():
            return cand
    return None


def _normalize_user_path(raw: str) -> Path:
    s = str(raw or "").strip().strip('"').strip("'")
    if not s:
        return Path()
    return Path(os.path.expandvars(os.path.expanduser(s)))


class AIVideoDialog:
    """
    Placeholder trống cho AI Video Gemini/Veo.
    Dùng làm nền tích hợp tool mới do người dùng cung cấp.
    """

    def __init__(
        self,
        parent: tk.Misc,
        *,
        project_spec: dict[str, Any] | None = None,
        start_tab: str = "reverse",
        embedded_download_host: ttk.Frame | None = None,
    ) -> None:
        self._parent = parent
        self._project_spec = dict(project_spec or {})
        self._suspend_reverse_source_reset = True
        self._reverse_source_change_after: str | None = None
        self._last_reverse_source_signature = ""
        default_exe = _INTERNAL_TOOL_EXE if _INTERNAL_TOOL_EXE.is_file() else _EXTERNAL_TOOL_EXE
        self._tool_exe = _normalize_user_path(str(self._project_spec.get("tool_exe") or default_exe))
        self._embedded_download_host = embedded_download_host
        if self._embedded_download_host is None:
            self._top = tk.Toplevel(parent)
            self._top.title("AI Video Gemini/Veo — External Tool Bridge")
            self._top.geometry("980x700")
            self._top.minsize(760, 520)
        else:
            self._top = self._embedded_download_host.winfo_toplevel()
        self._reverse_engine = VideoReversePromptEngine(log=self._append_reverse_log)
        self._reverse_paths = ensure_reverse_video_layout()
        self._ai_video_service = AIVideoGenerationService()
        self._uv_downloader: UniversalVideoDownloader | None = None
        self._uv_downloader_init_error: str | None = None
        self._last_download_job_id: str | None = None
        self._notebook: ttk.Notebook | None = None
        self._txt_uv_log: tk.Text | None = None
        self._tree_uv: ttk.Treeview | None = None
        self._var_uv_ytdlp_status = tk.StringVar(value="yt-dlp: đang kiểm tra…")
        self._var_uv_operation_status = tk.StringVar(value="")
        self._uv_progress: ttk.Progressbar | None = None
        self._uv_busy_disable_widgets: list[tk.Widget] = []
        self._tree_fb_reels: ttk.Treeview | None = None
        self._uv_fb_reel_urls: list[str] = []
        self._tree_yt_channel: ttk.Treeview | None = None
        self._uv_yt_entry_rows: list[dict[str, str]] = []
        self._tree_tt_channel: ttk.Treeview | None = None
        self._uv_tt_entry_rows: list[dict[str, str]] = []
        self._tree_ig_channel: ttk.Treeview | None = None
        self._uv_ig_entry_rows: list[dict[str, str]] = []
        self._var_uv_yt_list_max = tk.StringVar(value="100")
        self._var_uv_yt_scan_status = tk.StringVar(value="")
        self._var_uv_tt_list_max = tk.StringVar(value="100")
        self._var_uv_tt_scan_status = tk.StringVar(value="")
        self._var_uv_ig_list_max = tk.StringVar(value="100")
        self._var_uv_ig_scan_status = tk.StringVar(value="")
        self._var_uv_ig_cookie = tk.StringVar(value="")
        self._var_uv_ig_show_browser = tk.BooleanVar(value=False)
        self._var_uv_ig_max_scroll = tk.StringVar(value="60")
        self._var_uv_ig_scan_minutes = tk.StringVar(value="15")
        self._var_uv_ig_scroll_until_end = tk.BooleanVar(value=True)
        self._var_uv_job_name = tk.StringVar(value="")
        self._uv_last_saved_job_name: str = ""
        self._var_uv_lib_job_filter = tk.StringVar(value="Tất cả job")
        self._var_uv_lib_total_ok = tk.StringVar(value="Tổng thành công: 0 video")
        self._cb_uv_lib_job_filter: ttk.Combobox | None = None
        self._uv_lib_job_ids_by_filter_label: dict[str, set[str]] = {}
        self._var_uv_fb_cookie = tk.StringVar(value="")
        self._var_uv_fb_scan_status = tk.StringVar(value="")
        self._var_uv_fb_max_collect = tk.StringVar(value="200")
        self._var_uv_fb_max_scroll = tk.StringVar(value="100")
        self._var_uv_fb_scan_minutes = tk.StringVar(value="30")
        self._var_uv_fb_scroll_until_end = tk.BooleanVar(value=True)
        self._var_uv_fb_profile_pick = tk.StringVar(value="")
        self._var_uv_fb_show_browser = tk.BooleanVar(value=False)
        self._uv_fb_profile_urls: list[str] = []
        self._cb_uv_fb_profile: ttk.Combobox | None = None
        self._uv_fb_accounts_by_id: dict[str, dict[str, Any]] = {}
        self._uv_fb_selected_account_id: str = ""
        self._uv_download_scroll_canvas: tk.Canvas | None = None
        self._uv_log_buffer: list[str] = []
        self._uv_log_flush_after_id: str | None = None
        self._uv_last_partial_ui_ts: float = 0.0
        self._uv_fb_tree_gen = 0
        self._uv_yt_tree_gen = 0
        self._uv_tt_tree_gen = 0
        self._uv_ig_tree_gen = 0
        self._uv_fb_logical_select_all = False
        self._uv_yt_logical_select_all = False
        self._uv_ig_logical_select_all = False
        self._uv_tt_logical_select_all = False
        self._uv_lib_tree_gen = 0
        self._uv_lib_refresh_gen = 0
        self._uv_embedded_warm_done: bool = False
        self._start_tab = str(start_tab or "reverse").strip().lower()
        try:
            self._uv_downloader = UniversalVideoDownloader(log=self._append_uv_log)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Không khởi tạo UniversalVideoDownloader")
            self._uv_downloader = None
            self._uv_downloader_init_error = str(exc)
        if self._embedded_download_host is None:
            self._build_ui()
        else:
            self._build_download_only_ui(self._embedded_download_host)
        if self._uv_downloader_init_error:
            self._var_uv_ytdlp_status.set(f"Không khởi tạo module tải: {self._uv_downloader_init_error}")
        elif self._embedded_download_host is None:
            self._refresh_uv_ytdlp_status()
        if self._embedded_download_host is None:
            self._load_reverse_session_state()
            self._last_reverse_source_signature = self._current_reverse_source_signature()
            self._suspend_reverse_source_reset = False
            self._top.protocol("WM_DELETE_WINDOW", self._on_close_dialog)

    def _build_ui(self) -> None:
        root = ttk.Frame(self._top, padding=14)
        root.pack(fill=tk.BOTH, expand=True)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(1, weight=1)

        ttk.Label(
            root,
            text="AI Video Gemini/Veo (Tích hợp Tool ngoài)",
            font=("Segoe UI", 14, "bold"),
        ).grid(row=0, column=0, sticky="w")

        tabs = ttk.Notebook(root)
        tabs.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        self._notebook = tabs

        bridge_tab = ttk.Frame(tabs, padding=10)
        reverse_tab_host = ttk.Frame(tabs)
        tabs.add(bridge_tab, text="Bridge Launcher")
        tabs.add(reverse_tab_host, text="Reverse Video Prompt")

        self._build_bridge_tab(bridge_tab)
        reverse_inner, _ = self._make_scrollable_tab(reverse_tab_host)
        self._build_reverse_tab(reverse_inner)
        if self._start_tab == "bridge":
            tabs.select(bridge_tab)
        else:
            tabs.select(reverse_tab_host)

    def _build_download_only_ui(self, host: ttk.Frame) -> None:
        host.columnconfigure(0, weight=1)
        host.rowconfigure(0, weight=1)
        dl_inner, self._uv_download_scroll_canvas = self._make_scrollable_tab(host, grid_row=0)
        self._build_download_tab(dl_inner)

    def _on_notebook_tab_changed(self, _event: tk.Event | None = None) -> None:
        try:
            nb = self._notebook
            if nb is None:
                return
            if nb.index(nb.select()) == 1:
                self._refresh_uv_ytdlp_status()
        except Exception:
            pass

    def _make_scrollable_tab(self, host: ttk.Frame, *, grid_row: int = 0) -> tuple[ttk.Frame, tk.Canvas]:
        host.columnconfigure(0, weight=1)
        host.rowconfigure(grid_row, weight=1)
        canvas = tk.Canvas(host, highlightthickness=0)
        vbar = ttk.Scrollbar(host, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vbar.set)
        canvas.grid(row=grid_row, column=0, sticky="nsew")
        vbar.grid(row=grid_row, column=1, sticky="ns")

        inner = ttk.Frame(canvas, padding=10)
        win = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _on_inner_configure(_event: tk.Event) -> None:
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _on_canvas_configure(event: tk.Event) -> None:
            canvas.itemconfigure(win, width=event.width)

        inner.bind("<Configure>", _on_inner_configure)
        canvas.bind("<Configure>", _on_canvas_configure)

        def _on_mousewheel(event: tk.Event) -> None:
            delta = int(-1 * (event.delta / 120)) if getattr(event, "delta", 0) else 0
            if delta:
                try:
                    canvas.yview_scroll(delta, "units")
                except tk.TclError:
                    pass

        def _scroll_units(step: int) -> None:
            try:
                canvas.yview_scroll(step, "units")
            except tk.TclError:
                pass

        canvas.bind("<MouseWheel>", _on_mousewheel)
        inner.bind("<MouseWheel>", _on_mousewheel)
        host.bind("<MouseWheel>", _on_mousewheel)
        canvas.bind("<Button-4>", lambda _e: _scroll_units(-1))
        canvas.bind("<Button-5>", lambda _e: _scroll_units(1))
        inner.bind("<Button-4>", lambda _e: _scroll_units(-1))
        inner.bind("<Button-5>", lambda _e: _scroll_units(1))
        return inner, canvas

    def _sync_uv_download_scrollregion(self, *, scroll_to_content: bool = False) -> None:
        """Canvas tab Tải video: cập nhật vùng cuộn sau khi Treeview/bảng đổi kích thước."""
        c = self._uv_download_scroll_canvas
        if c is None:
            return
        c.update_idletasks()
        br = c.bbox("all")
        if br:
            c.configure(scrollregion=br)
        if scroll_to_content:
            try:
                c.yview_moveto(0.11)
            except tk.TclError:
                pass

    def _append_uv_log(self, msg: str) -> None:
        text = f"{msg}\n"
        if threading.current_thread() is not threading.main_thread():
            self._top.after(0, lambda m=msg: self._append_uv_log(m))
            return
        self._uv_log_buffer.append(text)
        if self._uv_log_flush_after_id is not None:
            return

        def _flush() -> None:
            self._uv_log_flush_after_id = None
            w = self._txt_uv_log
            if w is None or not self._uv_log_buffer:
                self._uv_log_buffer.clear()
                return
            chunk = "".join(self._uv_log_buffer)
            self._uv_log_buffer.clear()
            try:
                w.configure(state="normal")
                w.insert("end", chunk)
                try:
                    # Giữ phần cuối để tránh widget quá nặng gây "Not Responding".
                    tail = w.get("end-8000l", "end")
                    w.delete("1.0", "end")
                    w.insert("1.0", tail)
                except tk.TclError:
                    pass
                w.see("end")
                w.configure(state="disabled")
            except tk.TclError:
                pass

        self._uv_log_flush_after_id = self._top.after(120, _flush)

    def _uv_require_downloader(self, *, fail_title: str) -> UniversalVideoDownloader | None:
        if self._uv_downloader is not None:
            return self._uv_downloader
        detail = (self._uv_downloader_init_error or "").strip()
        lines = [
            "Module tải video chưa khởi tạo được.",
            "",
        ]
        if detail:
            lines.append(f"Chi tiết: {detail}")
            lines.append("")
        lines.append(
            "Gợi ý: pip install yt-dlp (đúng Python đang chạy app); kiểm tra quyền ghi thư mục data/; "
            "bấm «Kiểm tra yt-dlp» trên tab này nếu module đã nạp."
        )
        messagebox.showerror(fail_title, "\n".join(lines), parent=self._top)
        return None

    def _set_uv_status(self, msg: str) -> None:
        """Thông báo ngắn từ treeview shortcuts (copy / chọn nhanh)."""
        self._var_uv_operation_status.set(str(msg or "").strip())

    def _uv_download_progress_hook(self) -> Callable[[dict[str, Any]], None]:
        """Gọi từ worker thread (yt-dlp); marshal lên UI — số file xong + dòng stderr gọn."""

        def _hook(d: dict[str, Any]) -> None:
            ev = str(d.get("event") or "")
            if ev == "start":
                bt = int(d.get("batch_total") or 0)
                uv_type = str(d.get("url_type") or "")
                mv = int(d.get("max_videos") or 0)
                if bt > 0:
                    msg = f"Đang tải batch: 0/{bt} video (đang chạy yt-dlp)…"
                else:
                    hint = f", tối đa ~{mv} mục" if mv > 1 else ""
                    msg = f"Đang tải — loại: {uv_type or '?'}{hint}…"
                self._top.after(0, lambda m=msg: self._var_uv_operation_status.set(m))
                return
            if ev == "file_complete":
                c = int(d.get("completed") or 0)
                t = int(d.get("total") or 0)
                path = str(d.get("path") or "")
                try:
                    short = Path(path).name if path else ""
                except Exception:
                    short = (path or "")[-72:]
                if t > 0:
                    msg = f"Tải batch: {c}/{t} video xong — {short}"
                    log_msg = f"[Tiến độ] Đã xong {c}/{t} — {short}"
                else:
                    msg = f"Đã nhận file {c} — {short}"
                    log_msg = f"[Tiến độ] File {c}: {short}"
                self._top.after(0, lambda m=msg: self._var_uv_operation_status.set(m))
                self._top.after(0, lambda lm=log_msg: self._append_uv_log(lm))
                return
            if ev == "stderr_activity":
                ln = str(d.get("line") or "").replace("\n", " ").strip()
                clip = ln[:150] + ("…" if len(ln) > 150 else "")
                msg = f"Đang tải… {clip}"
                self._top.after(0, lambda m=msg: self._var_uv_operation_status.set(m))
                return
            if ev == "error_line":
                ln = str(d.get("line") or "").replace("\n", " ").strip()
                clip = ln[:180] + ("…" if len(ln) > 180 else "")
                msg = f"Lỗi: {clip}"
                self._top.after(0, lambda m=msg: self._var_uv_operation_status.set(m))
                self._top.after(0, lambda lm=clip: self._append_uv_log(f"[yt-dlp] {lm}"))

        return _hook

    def _uv_set_busy(self, busy: bool, message: str = "") -> None:
        """Chạy trên luồng UI: thanh tiến trình + khóa nút để tránh Not Responding / double-click."""
        if threading.current_thread() is not threading.main_thread():
            self._top.after(0, lambda b=busy, m=message: self._uv_set_busy(b, m))
            return
        if message:
            self._var_uv_operation_status.set(message)
        elif not busy:
            self._var_uv_operation_status.set("Sẵn sàng — có thể thao tác.")
        pr = self._uv_progress
        if pr is not None:
            if busy:
                pr.start(12)
            else:
                pr.stop()
        state = "disabled" if busy else "normal"
        for w in self._uv_busy_disable_widgets:
            try:
                w.configure(state=state)
            except tk.TclError:
                pass

    def _apply_ytdlp_status_to_var(self, st: dict[str, Any]) -> None:
        if st.get("ok"):
            js = str(st.get("js_runtimes_resolved") or "").strip()
            tail = ""
            if not js:
                tail = " | YouTube: chưa có JS runtime (cài Node.js PATH hoặc yt_dlp.js_runtimes)."
            self._var_uv_ytdlp_status.set(
                f"Sẵn sàng — {st.get('version', '')}. {st.get('label', '')}{tail}"
            )
        else:
            tail = f" ({st.get('label')})" if st.get("label") else ""
            self._var_uv_ytdlp_status.set(f"Chưa dùng được: {st.get('message', 'Lỗi không rõ')}{tail}")

    def _refresh_uv_ytdlp_status(self) -> None:
        """Chạy yt-dlp --version trong nền, cập nhật nhãn (không đơ UI)."""
        down = self._uv_downloader

        def _work() -> None:
            if not down:
                err = (self._uv_downloader_init_error or "").strip()
                msg = f"Không khởi tạo module tải: {err}" if err else "yt-dlp: module tải chưa sẵn sàng."
                self._top.after(0, lambda m=msg: self._var_uv_ytdlp_status.set(m))
                return
            st = down.get_ytdlp_status()
            self._top.after(0, lambda s=st: self._apply_ytdlp_status_to_var(s))

        threading.Thread(target=_work, daemon=True, name="ytdlp_status_check").start()

    def _on_uv_verify_ytdlp(self) -> None:
        if self._uv_require_downloader(fail_title="yt-dlp") is None:
            return
        self._uv_set_busy(True, "Đang kiểm tra yt-dlp (chạy --version)…")

        def _work() -> None:
            st = self._uv_downloader.get_ytdlp_status() if self._uv_downloader else {"ok": False, "message": "no downloader"}

            def _done() -> None:
                self._uv_set_busy(False)
                self._apply_ytdlp_status_to_var(st)
                if st.get("ok"):
                    messagebox.showinfo(
                        "yt-dlp",
                        f"{st.get('version', '')}\n\n{st.get('label', '')}\n\nCó thể dùng tab Tải video, không cần cài thêm nếu bạn đã có pip/yt-dlp hoặc file exe trong config.",
                        parent=self._top,
                    )
                else:
                    messagebox.showerror(
                        "yt-dlp",
                        str(st.get("message") or "Không chạy được yt-dlp.")
                        + "\n\nGợi ý: chạy pip install yt-dlp (cùng Python đang mở app), hoặc đặt yt-dlp.exe và bật use_exe trong config.",
                        parent=self._top,
                    )

            self._top.after(0, _done)

        threading.Thread(target=_work, daemon=True, name="uv_verify_ytdlp").start()

    def _on_uv_ytdlp_check_and_update(self) -> None:
        """Tra cứu PyPI; nếu cũ hơn (hoặc chưa cài) thì đề xuất ``pip install -U yt-dlp``."""
        if self._uv_require_downloader(fail_title="yt-dlp") is None:
            return
        down = self._uv_downloader
        self._uv_set_busy(True, "Đang kiểm tra bản yt-dlp trên PyPI…")

        def _pip_then_refresh(success_title: str) -> None:
            self._uv_set_busy(True, "Đang cập nhật yt-dlp (pip install -U yt-dlp)…")

            def _pip_work() -> None:
                up = down.upgrade_ytdlp_via_pip()

                def _pip_ui() -> None:
                    self._uv_set_busy(False)
                    self._refresh_uv_ytdlp_status()
                    tail = str(up.get("message") or "").strip()
                    if len(tail) > 900:
                        tail = tail[-900:]
                    if up.get("ok"):
                        messagebox.showinfo(
                            "yt-dlp",
                            f"{success_title}\n\n{tail}" if tail else success_title,
                            parent=self._top,
                        )
                    else:
                        messagebox.showerror("yt-dlp", tail or "pip thất bại.", parent=self._top)

                self._top.after(0, _pip_ui)

            threading.Thread(target=_pip_work, daemon=True, name="uv_ytdlp_pip_upgrade").start()

        def _work() -> None:
            rep = down.get_ytdlp_update_check()

            def _phase1() -> None:
                self._uv_set_busy(False)
                if not rep.get("pypi_ok"):
                    messagebox.showerror(
                        "yt-dlp — PyPI",
                        str(rep.get("pypi_error") or "Không đọc được PyPI."),
                        parent=self._top,
                    )
                    return
                local = rep.get("local_version_line") or "(chưa chạy được yt-dlp)"
                remote = str(rep.get("pypi_version") or "")
                kind = str(rep.get("install_kind") or "unknown")
                kind_note = ""
                if kind == "standalone":
                    kind_note = (
                        "\n\nLưu ý: App đang ưu tiên yt-dlp dạng file/PATH. "
                        "pip chỉ cập nhật gói trong Python này; "
                        "để dùng bản pip có thể cần tắt use_exe trong config hoặc đổi PATH."
                    )
                pip_hint = f"\n\nLệnh tương đương: {sys.executable} -m pip install -U yt-dlp"
                if rep.get("needs_upgrade"):
                    if not messagebox.askyesno(
                        "yt-dlp — Có bản mới trên PyPI",
                        f"Cục bộ: {local}\nPyPI: {remote}{kind_note}{pip_hint}\n\nChạy cập nhật pip ngay?",
                        parent=self._top,
                    ):
                        self._refresh_uv_ytdlp_status()
                        return
                    _pip_then_refresh("Đã chạy pip cập nhật yt-dlp.")
                    return
                if rep.get("offer_optional_pip"):
                    if not messagebox.askyesno(
                        "yt-dlp — Đồng bộ pip (tùy chọn)",
                        f"Không so sánh được số phiên bản cục bộ.\nCục bộ: {local}\nPyPI: {remote}{kind_note}{pip_hint}\n\nVẫn chạy pip install -U yt-dlp?",
                        parent=self._top,
                    ):
                        self._refresh_uv_ytdlp_status()
                        return
                    _pip_then_refresh("Đã chạy pip (đồng bộ gói yt-dlp).")
                    return
                messagebox.showinfo(
                    "yt-dlp — PyPI",
                    f"Bản trên máy: {local}\nMới nhất trên PyPI: {remote}\n\nKhông cần nâng cấp (đã đủ mới).{kind_note}",
                    parent=self._top,
                )
                self._refresh_uv_ytdlp_status()

            self._top.after(0, _phase1)

        threading.Thread(target=_work, daemon=True, name="uv_ytdlp_pypi_check").start()

    def _build_download_tab(self, host: ttk.Frame) -> None:
        host.columnconfigure(0, weight=1)
        ucfg = load_universal_video_downloader_config().get("universal_video_downloader") or {}
        dl_cfg = ucfg.get("download") or {}
        yt_cfg = ucfg.get("yt_dlp") or {}
        default_dir = str(dl_cfg.get("last_output_dir") or dl_cfg.get("default_output_dir") or "").strip()
        if not default_dir:
            default_dir = str(project_root() / "data" / "downloads")
        ttk.Label(
            host,
            text=(
                "Luồng: (1) nhập URL + thư mục lưu  (2) quét Reels / YouTube / TikTok / Instagram  "
                "(3) «Chọn hết» nếu cần → «Tải … đã chọn»  (4) xem «Video đã tải».\n"
                "Nhiều video đã chọn được tải trong một lần chạy yt-dlp (batch), nhanh hơn gọi tuần tự từng URL.\n"
                "Mỗi khối Bước 2 (Facebook / YouTube / TikTok / Instagram) có nút ▼/▶ để thu gọn khi cần chỗ màn hình."
            ),
            wraplength=840,
            justify=tk.LEFT,
            foreground="#555",
            font=("Segoe UI", 9),
        ).grid(row=0, column=0, sticky="w")

        st_fr = ttk.LabelFrame(host, text="yt-dlp — trạng thái (kiểm tra khi mở tab «Tải Video» lần đầu)", padding=8)
        st_fr.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        ttk.Label(
            st_fr,
            textvariable=self._var_uv_ytdlp_status,
            wraplength=880,
            justify=tk.LEFT,
        ).pack(anchor="w")
        ttk.Label(
            st_fr,
            text=(
                "Thứ tự tìm yt-dlp: PATH hệ thống → file trong tools/yt-dlp/ (bản .exe đóng gói) "
                "→ python -m yt_dlp (chỉ khi chạy từ mã nguồn / pip). "
                "Facebook Reels / Instagram profile: dùng cookie JSON khi cần đăng nhập (config universal_video_downloader.json)."
            ),
            wraplength=820,
            justify=tk.LEFT,
            font=("Segoe UI", 9),
            foreground="#555",
        ).pack(anchor="w", pady=(4, 0))

        form = ttk.LabelFrame(host, text="Bước 1 — URL, thư mục lưu và tùy chọn", padding=8)
        form.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        form.columnconfigure(1, weight=1)
        self._var_uv_url = tk.StringVar()
        self._var_uv_platform = tk.StringVar(value="Tự nhận diện")
        self._var_uv_url_type = tk.StringVar(value="Tự nhận diện")
        _jn = str(dl_cfg.get("last_job_name") or "").strip()[:120]
        self._var_uv_job_name.set(_jn)
        self._uv_last_saved_job_name = _jn
        self._var_uv_max_videos = tk.StringVar(value=str(yt_cfg.get("max_videos_default") or 50))
        self._var_uv_out_dir = tk.StringVar(value=default_dir)
        self._var_uv_org_platform = tk.BooleanVar(value=bool(dl_cfg.get("organize_by_platform", True)))
        self._var_uv_org_uploader = tk.BooleanVar(value=bool(dl_cfg.get("organize_by_uploader", True)))
        self._var_uv_skip_existing = tk.BooleanVar(value=bool(dl_cfg.get("skip_existing", True)))
        self._var_uv_info_json = tk.BooleanVar(value=bool(yt_cfg.get("write_info_json", True)))
        self._var_uv_thumbnail = tk.BooleanVar(value=bool(yt_cfg.get("write_thumbnail", False)))
        var_detect_hint = tk.StringVar(value="")
        var_quick_guide = tk.StringVar(value="Mẹo nhanh: dán URL để tự nhận diện YouTube / Facebook / TikTok / Instagram.")
        var_platform_badge = tk.StringVar(value="AUTO")
        section_state: dict[str, Any] = {"fb_fr": None, "yt_fr": None, "tt_fr": None, "ig_fr": None, "view": None}

        job_row = ttk.Frame(form)
        job_row.grid(row=0, column=0, columnspan=2, sticky="ew")
        job_row.columnconfigure(1, weight=1)
        ttk.Label(job_row, text="Tên job:").grid(row=0, column=0, sticky="w")
        ent_uv_job = ttk.Entry(job_row, textvariable=self._var_uv_job_name)
        ent_uv_job.grid(row=0, column=1, sticky="ew", padx=(8, 8))
        ent_uv_job.bind("<FocusOut>", lambda _e: self._persist_uv_last_job_name(interactive=False))
        ent_uv_job.bind("<Return>", lambda _e: self._persist_uv_last_job_name(interactive=False))
        ttk.Button(job_row, text="Lưu tên job", command=self._on_uv_save_job_name).grid(row=0, column=2, sticky="w")
        ttk.Label(form, text="URL:").grid(row=1, column=0, sticky="w", pady=(4, 0))
        ent_url = ttk.Entry(form, textvariable=self._var_uv_url)
        ent_url.grid(row=1, column=1, sticky="ew", padx=(8, 0), pady=(4, 0))
        ttk.Label(form, textvariable=var_detect_hint, foreground="#1a4480", font=("Segoe UI", 8)).grid(
            row=2, column=1, sticky="w", padx=(8, 0), pady=(2, 0)
        )
        ttk.Label(form, textvariable=var_quick_guide, foreground="#666", font=("Segoe UI", 8)).grid(
            row=3, column=1, sticky="w", padx=(8, 0), pady=(1, 0)
        )
        lbl_platform_badge = tk.Label(
            form,
            textvariable=var_platform_badge,
            bg="#6b7280",
            fg="#ffffff",
            padx=8,
            pady=2,
            font=("Segoe UI", 8, "bold"),
        )
        lbl_platform_badge.grid(row=3, column=0, sticky="w", pady=(1, 0))

        quick = ttk.Frame(form)
        quick.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        quick.columnconfigure(1, weight=1)
        quick.columnconfigure(3, weight=1)
        ttk.Label(quick, text="Nền tảng").grid(row=0, column=0, sticky="w")
        cb_platform = ttk.Combobox(
            quick,
            textvariable=self._var_uv_platform,
            values=["Tự nhận diện", "youtube", "tiktok", "instagram", "facebook", "unknown"],
            state="readonly",
            width=20,
        )
        cb_platform.grid(row=0, column=1, sticky="ew", padx=(6, 12))
        ttk.Label(quick, text="Loại URL").grid(row=0, column=2, sticky="w")
        cb_url_type = ttk.Combobox(
            quick,
            textvariable=self._var_uv_url_type,
            values=[
                "Tự nhận diện",
                "Video đơn",
                "Danh sách (playlist/profile)",
                "Playlist",
                "Kênh",
                "Profile",
                "Không rõ",
            ],
            state="readonly",
            width=22,
        )
        cb_url_type.grid(row=0, column=3, sticky="ew", padx=(6, 12))
        ttk.Label(quick, text="Tối đa").grid(row=0, column=4, sticky="w")
        ttk.Entry(quick, textvariable=self._var_uv_max_videos, width=10).grid(row=0, column=5, sticky="w", padx=(6, 0))

        def _refresh_detect_hint(_e: Any = None) -> None:
            url = self._var_uv_url.get().strip()
            if not url:
                var_detect_hint.set("Tự nhận diện: chờ nhập URL.")
                var_quick_guide.set(
                    "Ví dụ: YouTube /@kênh/shorts • Facebook /profile/reels • TikTok /@user • Instagram /user/reels/ hoặc /reel/…"
                )
                _refresh_platform_actions("unknown", "unknown")
                return
            auto_platform = detect_platform(url)
            auto_type = classify_url_type(url)
            picked_platform = self._normalize_uv_platform_choice(self._var_uv_platform.get().strip() or "Tự nhận diện")
            picked_type = self._normalize_uv_url_type_choice(self._var_uv_url_type.get().strip() or "Tự nhận diện")
            use_platform = auto_platform if picked_platform in ("auto", "") else picked_platform
            use_type = auto_type if picked_type in ("auto", "") else picked_type
            var_detect_hint.set(
                f"Tự nhận diện: {auto_platform}/{auto_type} • Sẽ tải theo: {use_platform}/{use_type}"
            )
            if auto_platform == "youtube":
                var_quick_guide.set(
                    "YouTube: link video đơn, playlist (?list=...), hoặc kênh/tab Shorts để quét danh sách."
                )
            elif auto_platform == "facebook":
                var_quick_guide.set(
                    "Facebook: nên dùng link tab Reels để quét trước, rồi «Chọn hết» và tải reel đã chọn."
                )
            elif auto_platform == "tiktok":
                var_quick_guide.set(
                    "TikTok: dán link profile/video và bấm «Tải TikTok URL» để chạy nhanh."
                )
            elif auto_platform == "instagram":
                var_quick_guide.set(
                    "Instagram: reel/post đơn → «Tải URL hiện tại»; profile/tab Reels → «Quét Instagram» (có thể cần cookie)."
                )
            else:
                var_quick_guide.set(
                    "Không nhận diện chắc nền tảng: vẫn có thể bấm «Tải ngay URL hiện tại»."
                )
            _refresh_platform_actions(auto_platform, use_platform)

        ttk.Label(form, text="Thư mục lưu:").grid(row=5, column=0, sticky="w", pady=(6, 0))
        od_frame = ttk.Frame(form)
        od_frame.grid(row=5, column=1, sticky="ew", padx=(8, 0), pady=(6, 0))
        od_frame.columnconfigure(0, weight=1)
        ttk.Entry(od_frame, textvariable=self._var_uv_out_dir).grid(row=0, column=0, sticky="ew")
        ttk.Button(od_frame, text="Chọn folder", command=self._on_uv_pick_folder).grid(row=0, column=1, padx=(8, 0))

        var_show_adv = tk.BooleanVar(value=False)
        adv_toggle_fr = ttk.Frame(form)
        adv_toggle_fr.grid(row=6, column=0, columnspan=2, sticky="w", pady=(8, 0))
        btn_adv = ttk.Button(adv_toggle_fr, text="Hiện nâng cao ▾")
        btn_adv.pack(side=tk.LEFT)
        ttk.Label(
            adv_toggle_fr,
            text="(tuỳ chọn tổ chức file, metadata)",
            foreground="#666",
            font=("Segoe UI", 8),
        ).pack(side=tk.LEFT, padx=(8, 0))

        adv_opts = ttk.Frame(form)
        adv_opts.grid(row=7, column=0, columnspan=2, sticky="w", pady=(4, 0))
        adv_opts.grid_remove()

        opt = ttk.Frame(adv_opts)
        opt.grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(opt, text="Tổ chức theo nền tảng", variable=self._var_uv_org_platform).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(opt, text="Tổ chức theo uploader", variable=self._var_uv_org_uploader).grid(row=0, column=1, sticky="w", padx=(12, 0))
        ttk.Checkbutton(opt, text="Không tải trùng", variable=self._var_uv_skip_existing).grid(row=0, column=2, sticky="w", padx=(12, 0))
        ttk.Checkbutton(opt, text="Lưu metadata JSON", variable=self._var_uv_info_json).grid(row=1, column=0, sticky="w", pady=(4, 0))
        ttk.Checkbutton(opt, text="Lưu thumbnail", variable=self._var_uv_thumbnail).grid(row=1, column=1, sticky="w", padx=(12, 0), pady=(4, 0))

        def _toggle_advanced() -> None:
            show = not bool(var_show_adv.get())
            var_show_adv.set(show)
            if show:
                adv_opts.grid()
                btn_adv.configure(text="Ẩn nâng cao ▴")
            else:
                adv_opts.grid_remove()
                btn_adv.configure(text="Hiện nâng cao ▾")
            self._sync_uv_download_scrollregion(scroll_to_content=False)

        btn_adv.configure(command=_toggle_advanced)

        fb_prep = ttk.LabelFrame(form, text="Chuẩn bị Facebook (nếu quét Reels)", padding=8)
        fb_prep.grid(row=8, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        fb_prep.columnconfigure(0, weight=1)
        ttk.Label(
            fb_prep,
            text="Chọn tài khoản trước khi bấm quét Reels ở Bước 1.",
            foreground="#555",
            wraplength=860,
            justify=tk.LEFT,
        ).grid(row=0, column=0, sticky="w")
        self._uv_fb_profile_urls = self._load_uv_fb_accounts()
        if self._uv_fb_profile_urls:
            self._var_uv_fb_profile_pick.set(self._uv_fb_profile_urls[0])
        fb_prep_row = ttk.Frame(fb_prep)
        fb_prep_row.grid(row=1, column=0, sticky="ew", pady=(6, 0))
        fb_prep_row.columnconfigure(1, weight=1)
        ttk.Label(fb_prep_row, text="Tài khoản").grid(row=0, column=0, sticky="w")
        self._cb_uv_fb_profile = ttk.Combobox(
            fb_prep_row,
            textvariable=self._var_uv_fb_profile_pick,
            values=self._uv_fb_profile_urls,
            width=46,
        )
        self._cb_uv_fb_profile.grid(row=0, column=1, sticky="ew", padx=(6, 8))
        ttk.Button(fb_prep_row, text="Nạp lại", command=self._on_uv_reload_fb_profiles).grid(row=0, column=2, sticky="w", padx=(0, 8))
        ttk.Button(fb_prep_row, text="Chọn tài khoản", command=self._on_uv_apply_fb_profile_url).grid(row=0, column=3, sticky="w")
        ttk.Checkbutton(
            fb_prep_row,
            text="Hiện browser khi quét",
            variable=self._var_uv_fb_show_browser,
            command=self._on_uv_fb_browser_toggle_notice,
        ).grid(row=0, column=4, sticky="w", padx=(10, 0))
        ttk.Label(fb_prep_row, text="(Bỏ chọn = chạy ẩn/headless)", foreground="#666", font=("Segoe UI", 8)).grid(
            row=0, column=5, sticky="w", padx=(8, 0)
        )

        ig_prep = ttk.LabelFrame(form, text="Chuẩn bị Instagram (nếu quét profile/Reels)", padding=8)
        ig_prep.grid(row=9, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        ig_prep.columnconfigure(1, weight=1)
        ttk.Label(
            ig_prep,
            text=(
                "Quét tab Reels dùng Playwright (yt-dlp chưa hỗ trợ …/username/reels/). "
                "Profile riêng tư: chọn file cookie Playwright (storage_state JSON)."
            ),
            foreground="#555",
            wraplength=860,
            justify=tk.LEFT,
        ).grid(row=0, column=0, columnspan=4, sticky="w")
        ig_prep_row = ttk.Frame(ig_prep)
        ig_prep_row.grid(row=1, column=0, columnspan=4, sticky="ew", pady=(6, 0))
        ig_prep_row.columnconfigure(1, weight=1)
        ttk.Label(ig_prep_row, text="Cookie IG").grid(row=0, column=0, sticky="w")
        ttk.Entry(ig_prep_row, textvariable=self._var_uv_ig_cookie, width=52).grid(
            row=0, column=1, sticky="ew", padx=(6, 8)
        )
        ttk.Button(ig_prep_row, text="Chọn file…", command=self._on_uv_pick_ig_cookie).grid(row=0, column=2, sticky="w")
        ttk.Checkbutton(
            ig_prep_row,
            text="Hiện browser khi quét",
            variable=self._var_uv_ig_show_browser,
        ).grid(row=0, column=3, sticky="w", padx=(10, 0))

        platform_ops = ttk.LabelFrame(form, text="Quét danh sách (theo URL / nền tảng ở trên)", padding=8)
        platform_ops.grid(row=10, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        var_platform_ops = tk.StringVar(value="Dán URL để hệ thống tự chọn đúng luồng thao tác.")
        ttk.Label(platform_ops, textvariable=var_platform_ops, wraplength=860, justify=tk.LEFT).pack(anchor="w")
        platform_scan_opts = ttk.Frame(platform_ops)
        platform_scan_opts.pack(anchor="w", pady=(6, 0))
        fb_scan_opts = ttk.Frame(platform_scan_opts)
        ttk.Label(fb_scan_opts, text="Reel").grid(row=0, column=0, sticky="w")
        ttk.Entry(fb_scan_opts, textvariable=self._var_uv_fb_max_collect, width=6).grid(row=0, column=1, sticky="w", padx=(6, 12))
        ttk.Label(fb_scan_opts, text="Vòng").grid(row=0, column=2, sticky="w")
        ttk.Entry(fb_scan_opts, textvariable=self._var_uv_fb_max_scroll, width=6).grid(row=0, column=3, sticky="w", padx=(6, 12))
        ttk.Label(fb_scan_opts, text="Phút").grid(row=0, column=4, sticky="w")
        ttk.Entry(fb_scan_opts, textvariable=self._var_uv_fb_scan_minutes, width=6).grid(row=0, column=5, sticky="w", padx=(6, 12))
        ttk.Checkbutton(fb_scan_opts, text="Cuộn tới hết trang", variable=self._var_uv_fb_scroll_until_end).grid(
            row=0, column=6, sticky="w", padx=(6, 12)
        )
        ttk.Button(fb_scan_opts, text="Lưu giới hạn", command=self._on_uv_save_fb_reel_limits).grid(row=0, column=7, sticky="w")
        yt_scan_opts = ttk.Frame(platform_scan_opts)
        ttk.Label(yt_scan_opts, text="Tối đa entry").grid(row=0, column=0, sticky="w")
        ttk.Entry(yt_scan_opts, textvariable=self._var_uv_yt_list_max, width=6).grid(row=0, column=1, sticky="w", padx=(6, 12))
        ttk.Label(
            yt_scan_opts,
            text=f"(1–{UV_CHANNEL_LIST_MAX}, quét theo lô để tránh treo)",
            foreground="#666",
            font=("Segoe UI", 8),
        ).grid(row=0, column=2, sticky="w")
        tt_scan_opts = ttk.Frame(platform_scan_opts)
        ttk.Label(tt_scan_opts, text="Tối đa entry").grid(row=0, column=0, sticky="w")
        ttk.Entry(tt_scan_opts, textvariable=self._var_uv_tt_list_max, width=6).grid(row=0, column=1, sticky="w", padx=(6, 12))
        ttk.Label(tt_scan_opts, text=f"(1–{UV_CHANNEL_LIST_MAX})", foreground="#666", font=("Segoe UI", 8)).grid(
            row=0, column=2, sticky="w"
        )
        ig_scan_opts = ttk.Frame(platform_scan_opts)
        ttk.Label(ig_scan_opts, text="Reel").grid(row=0, column=0, sticky="w")
        ttk.Entry(ig_scan_opts, textvariable=self._var_uv_ig_list_max, width=6).grid(row=0, column=1, sticky="w", padx=(6, 12))
        ttk.Label(ig_scan_opts, text="Vòng").grid(row=0, column=2, sticky="w")
        ttk.Entry(ig_scan_opts, textvariable=self._var_uv_ig_max_scroll, width=6).grid(row=0, column=3, sticky="w", padx=(6, 12))
        ttk.Label(ig_scan_opts, text="Phút").grid(row=0, column=4, sticky="w")
        ttk.Entry(ig_scan_opts, textvariable=self._var_uv_ig_scan_minutes, width=6).grid(row=0, column=5, sticky="w", padx=(6, 12))
        ttk.Checkbutton(ig_scan_opts, text="Cuộn tới hết trang", variable=self._var_uv_ig_scroll_until_end).grid(
            row=0, column=6, sticky="w", padx=(6, 12)
        )
        ttk.Button(ig_scan_opts, text="Lưu giới hạn", command=self._on_uv_save_ig_reel_limits).grid(row=0, column=7, sticky="w")
        platform_ops_btns = ttk.Frame(platform_ops)
        platform_ops_btns.pack(anchor="w", pady=(6, 0))
        btn_ops_generic_dl = ttk.Button(platform_ops_btns, text="Tải URL hiện tại", command=self._on_uv_download)
        btn_ops_fb_scan = ttk.Button(platform_ops_btns, text="1) Quét Reels Facebook", command=self._on_uv_scan_fb_reels)
        btn_ops_yt_scan = ttk.Button(platform_ops_btns, text="1) Quét danh sách YouTube", command=self._on_uv_scan_yt_channel)
        btn_ops_tt_scan = ttk.Button(platform_ops_btns, text="1) Quét kênh TikTok", command=self._on_uv_scan_tt_channel)
        btn_ops_ig_scan = ttk.Button(platform_ops_btns, text="1) Quét Instagram", command=self._on_uv_scan_ig_channel)
        for b in (
            btn_ops_generic_dl,
            btn_ops_fb_scan,
            btn_ops_yt_scan,
            btn_ops_tt_scan,
            btn_ops_ig_scan,
        ):
            self._uv_busy_disable_widgets.append(b)

        def _show_platform_buttons(buttons: list[ttk.Button]) -> None:
            for b in (
                btn_ops_generic_dl,
                btn_ops_fb_scan,
                btn_ops_yt_scan,
                btn_ops_tt_scan,
                btn_ops_ig_scan,
            ):
                b.pack_forget()
            for b in buttons:
                b.pack(side=tk.LEFT, padx=(0, 8))

        def _refresh_platform_actions(auto_platform: str, use_platform: str) -> None:
            p = str(use_platform or auto_platform or "unknown").strip().lower()
            fb_prep_fr = fb_prep
            fb_fr = section_state.get("fb_fr")
            yt_fr = section_state.get("yt_fr")
            tt_fr = section_state.get("tt_fr")
            ig_fr = section_state.get("ig_fr")

            def _set_badge(text: str, bg: str) -> None:
                var_platform_badge.set(text)
                try:
                    lbl_platform_badge.configure(bg=bg)
                except Exception:
                    pass

            def _show_sections(
                show_fb: bool, show_yt: bool, show_tt: bool, show_ig: bool, view_key: str
            ) -> None:
                if section_state.get("view") == view_key:
                    return
                section_state["view"] = view_key
                for f in (fb_scan_opts, yt_scan_opts, tt_scan_opts, ig_scan_opts):
                    f.pack_forget()
                if show_fb:
                    fb_scan_opts.pack(anchor="w")
                elif show_yt:
                    yt_scan_opts.pack(anchor="w")
                elif show_tt:
                    tt_scan_opts.pack(anchor="w")
                elif show_ig:
                    ig_scan_opts.pack(anchor="w")
                if fb_prep_fr is not None:
                    if show_fb:
                        fb_prep_fr.grid()
                    else:
                        fb_prep_fr.grid_remove()
                if fb_fr is not None:
                    if show_fb:
                        fb_fr.grid()
                    else:
                        fb_fr.grid_remove()
                if yt_fr is not None:
                    if show_yt:
                        yt_fr.grid()
                    else:
                        yt_fr.grid_remove()
                if tt_fr is not None:
                    if show_tt:
                        tt_fr.grid()
                    else:
                        tt_fr.grid_remove()
                if ig_fr is not None:
                    if show_ig:
                        ig_fr.grid()
                    else:
                        ig_fr.grid_remove()
                self._sync_uv_download_scrollregion(scroll_to_content=False)

            if p == "youtube":
                _set_badge("▶ YouTube", "#cc0000")
                var_platform_ops.set("YouTube: quét danh sách (Bước 1). Chọn dòng → «Tải video đã chọn» (một lần yt-dlp nếu chọn nhiều).")
                _show_platform_buttons([btn_ops_yt_scan])
                _show_sections(show_fb=False, show_yt=True, show_tt=False, show_ig=False, view_key="youtube")
                return
            if p == "facebook":
                _set_badge("f Facebook", "#1877F2")
                var_platform_ops.set(
                    "Facebook: quét Reels (Bước 1) → «Tải hết danh sách» hoặc chọn dòng → «Tải reel đã chọn»."
                )
                _show_platform_buttons([btn_ops_fb_scan])
                _show_sections(show_fb=True, show_yt=False, show_tt=False, show_ig=False, view_key="facebook")
                return
            if p == "tiktok":
                _set_badge("♪ TikTok", "#111111")
                var_platform_ops.set("TikTok: quét kênh (Bước 1). Chọn dòng → «Tải TikTok đã chọn» (một lần yt-dlp nếu nhiều).")
                _show_platform_buttons([btn_ops_tt_scan])
                _show_sections(show_fb=False, show_yt=False, show_tt=True, show_ig=False, view_key="tiktok")
                return
            if p == "instagram":
                _set_badge("◎ IG", "#E1306C")
                var_platform_ops.set(
                    "Instagram: quét tab Reels bằng Playwright (Bước 1). Reel/post đơn → «Tải URL hiện tại». "
                    "Profile riêng tư: cookie Playwright ở khối Chuẩn bị Instagram."
                )
                _show_platform_buttons([btn_ops_ig_scan, btn_ops_generic_dl])
                _show_sections(show_fb=False, show_yt=False, show_tt=False, show_ig=True, view_key="instagram")
                return
            _set_badge("◎ AUTO", "#6b7280")
            var_platform_ops.set("Chưa nhận diện nền tảng: có thể tải nhanh URL hiện tại hoặc nhập lại URL rõ hơn.")
            _show_platform_buttons([btn_ops_generic_dl])
            _show_sections(show_fb=False, show_yt=False, show_tt=False, show_ig=False, view_key="unknown")

        ent_url.bind("<KeyRelease>", _refresh_detect_hint)
        cb_platform.bind("<<ComboboxSelected>>", _refresh_detect_hint)
        cb_url_type.bind("<<ComboboxSelected>>", _refresh_detect_hint)

        fb_cfg = ucfg.get("facebook_reels") or {}
        ig_cfg = ucfg.get("instagram_reels") or {}
        self._var_uv_fb_cookie.set(str(fb_cfg.get("cookie_path") or "").strip())
        self._var_uv_ig_cookie.set(str(ig_cfg.get("cookie_path") or "").strip())
        self._var_uv_ig_list_max.set(str(int(ig_cfg.get("max_collect") or 120)))
        self._var_uv_ig_max_scroll.set(str(int(ig_cfg.get("max_scroll_rounds") or 60)))
        self._var_uv_ig_scan_minutes.set(str(int(ig_cfg.get("max_scan_minutes") or 15)))
        self._var_uv_ig_scroll_until_end.set(bool(ig_cfg.get("scroll_until_end", True)))
        self._var_uv_ig_show_browser.set(bool(ig_cfg.get("show_browser", False)))
        self._var_uv_fb_max_collect.set(str(int(fb_cfg.get("max_collect") or 300)))
        self._var_uv_fb_max_scroll.set(str(int(fb_cfg.get("max_scroll_rounds") or 100)))
        self._var_uv_fb_scan_minutes.set(str(int(fb_cfg.get("max_scan_minutes") or 30)))
        self._var_uv_fb_scroll_until_end.set(bool(fb_cfg.get("scroll_until_end", True)))
        self._var_uv_fb_scan_status.set("Chưa quét. Dán URL tab Reels (hoặc profile) ở ô URL phía trên.")

        def _uv_step2_scroll_sync() -> None:
            self._sync_uv_download_scrollregion(scroll_to_content=False)

        fb_coll = _UvCollapsibleSection(
            host,
            title="Bước 2 (Facebook) — Quét danh sách Reels bằng browser tài khoản đã chọn",
            start_open=True,
            on_toggle=_uv_step2_scroll_sync,
        )
        fb_coll.outer.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        fb_coll.outer.columnconfigure(0, weight=1)
        fb_fr = ttk.Frame(fb_coll.body, padding=8)
        fb_fr.pack(fill=tk.BOTH, expand=True)
        fb_fr.columnconfigure(1, weight=1)
        ttk.Label(
            fb_fr,
            text=(
                "Có thể chọn tài khoản Facebook sẵn có để nạp URL nhanh. "
                "Bật «Hiện browser khi quét» nếu cần theo dõi Playwright trực tiếp."
            ),
            wraplength=820,
            justify=tk.LEFT,
            foreground="#555",
        ).grid(row=0, column=0, columnspan=3, sticky="w")
        ttk.Label(fb_fr, textvariable=self._var_uv_fb_scan_status, wraplength=860, justify=tk.LEFT).grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(6, 0)
        )
        fb_act = ttk.Frame(fb_fr)
        fb_act.grid(row=2, column=0, columnspan=3, sticky="w", pady=(6, 0))
        btn_fb_select = ttk.Button(fb_act, text="2) Chọn hết", command=self._on_uv_fb_select_all)
        btn_fb_select.pack(side=tk.LEFT, padx=(0, 8))
        btn_fb_dl = ttk.Button(fb_act, text="3) Tải reel đã chọn", command=self._on_uv_download_fb_reels_selected)
        btn_fb_dl.pack(side=tk.LEFT, padx=(0, 8))
        btn_fb_dl_all = ttk.Button(fb_act, text="Tải hết danh sách", command=self._on_uv_download_fb_reels_all)
        btn_fb_dl_all.pack(side=tk.LEFT, padx=(0, 8))
        self._uv_busy_disable_widgets.extend((btn_fb_select, btn_fb_dl, btn_fb_dl_all))
        fb_tree_fr = ttk.Frame(fb_fr)
        fb_tree_fr.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(6, 0))
        fb_tree_fr.columnconfigure(0, weight=1)
        self._tree_fb_reels = ttk.Treeview(
            fb_tree_fr,
            columns=("idx", "url"),
            show="headings",
            height=8,
            selectmode="extended",
        )
        self._tree_fb_reels.heading("idx", text="#")
        self._tree_fb_reels.heading("url", text="URL reel")
        self._tree_fb_reels.column("idx", width=44, stretch=False)
        self._tree_fb_reels.column("url", width=900, stretch=True)
        syf = ttk.Scrollbar(fb_tree_fr, orient="vertical", command=self._tree_fb_reels.yview)
        sxf = ttk.Scrollbar(fb_tree_fr, orient="horizontal", command=self._tree_fb_reels.xview)
        self._tree_fb_reels.configure(yscrollcommand=syf.set, xscrollcommand=sxf.set)
        self._tree_fb_reels.grid(row=0, column=0, sticky="ew")
        syf.grid(row=0, column=1, sticky="ns")
        sxf.grid(row=1, column=0, sticky="ew")
        install_treeview_shortcuts(self._tree_fb_reels, owner=self._top, info_callback=self._set_uv_status)

        self._var_uv_yt_scan_status.set(
            "Chưa quét. Dán URL kênh / tab Shorts / playlist YouTube ở ô URL phía trên, rồi dùng nút ở Bước 1."
        )
        yt_coll = _UvCollapsibleSection(
            host,
            title="Bước 2 (YouTube) — Quét danh sách video để chọn tải",
            start_open=True,
            on_toggle=_uv_step2_scroll_sync,
        )
        yt_coll.outer.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        yt_coll.outer.columnconfigure(0, weight=1)
        yt_fr = ttk.Frame(yt_coll.body, padding=8)
        yt_fr.pack(fill=tk.BOTH, expand=True)
        yt_fr.columnconfigure(1, weight=1)
        ttk.Label(
            yt_fr,
            text=(
                "Dùng cùng ô URL với Bước 1. Chỉ áp dụng khi Tự nhận diện là youtube + channel hoặc playlist "
                "(ví dụ /@kênh/shorts, playlist ?list=…)."
            ),
            wraplength=820,
            justify=tk.LEFT,
            foreground="#555",
        ).grid(row=0, column=0, columnspan=3, sticky="w")
        ttk.Label(yt_fr, textvariable=self._var_uv_yt_scan_status, wraplength=860, justify=tk.LEFT).grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(6, 0)
        )
        yt_act = ttk.Frame(yt_fr)
        yt_act.grid(row=2, column=0, columnspan=3, sticky="w", pady=(6, 0))
        btn_yt_select = ttk.Button(yt_act, text="2) Chọn hết", command=self._on_uv_yt_select_all)
        btn_yt_select.pack(side=tk.LEFT, padx=(0, 8))
        btn_yt_dl = ttk.Button(yt_act, text="3) Tải video đã chọn", command=self._on_uv_download_yt_selected)
        btn_yt_dl.pack(side=tk.LEFT, padx=(0, 8))
        btn_yt_dl_all = ttk.Button(yt_act, text="Tải hết danh sách", command=self._on_uv_download_yt_all)
        btn_yt_dl_all.pack(side=tk.LEFT, padx=(0, 8))
        self._uv_busy_disable_widgets.extend((btn_yt_select, btn_yt_dl, btn_yt_dl_all))
        yt_tree_fr = ttk.Frame(yt_fr)
        yt_tree_fr.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(6, 0))
        yt_tree_fr.columnconfigure(0, weight=1)
        self._tree_yt_channel = ttk.Treeview(
            yt_tree_fr,
            columns=("idx", "title", "url"),
            show="headings",
            height=8,
            selectmode="extended",
        )
        self._tree_yt_channel.heading("idx", text="#")
        self._tree_yt_channel.heading("title", text="Tiêu đề")
        self._tree_yt_channel.heading("url", text="URL")
        self._tree_yt_channel.column("idx", width=40, stretch=False)
        self._tree_yt_channel.column("title", width=260, stretch=True)
        self._tree_yt_channel.column("url", width=520, stretch=True)
        sy_yt = ttk.Scrollbar(yt_tree_fr, orient="vertical", command=self._tree_yt_channel.yview)
        sx_yt = ttk.Scrollbar(yt_tree_fr, orient="horizontal", command=self._tree_yt_channel.xview)
        self._tree_yt_channel.configure(yscrollcommand=sy_yt.set, xscrollcommand=sx_yt.set)
        self._tree_yt_channel.grid(row=0, column=0, sticky="ew")
        sy_yt.grid(row=0, column=1, sticky="ns")
        sx_yt.grid(row=1, column=0, sticky="ew")
        install_treeview_shortcuts(self._tree_yt_channel, owner=self._top, info_callback=self._set_uv_status)

        self._var_uv_tt_scan_status.set(
            "Chưa quét. Dán URL profile TikTok ở ô URL phía trên, rồi dùng nút ở Bước 1."
        )
        tt_coll = _UvCollapsibleSection(
            host,
            title="Bước 2 (TikTok) — Quét danh sách video kênh để chọn tải",
            start_open=True,
            on_toggle=_uv_step2_scroll_sync,
        )
        tt_coll.outer.grid(row=5, column=0, sticky="ew", pady=(8, 0))
        tt_coll.outer.columnconfigure(0, weight=1)
        tt_fr = ttk.Frame(tt_coll.body, padding=8)
        tt_fr.pack(fill=tk.BOTH, expand=True)
        tt_fr.columnconfigure(1, weight=1)
        ttk.Label(
            tt_fr,
            text="Nhập URL dạng https://www.tiktok.com/@username để lấy danh sách video trong kênh.",
            wraplength=820,
            justify=tk.LEFT,
            foreground="#555",
        ).grid(row=0, column=0, columnspan=3, sticky="w")
        ttk.Label(tt_fr, textvariable=self._var_uv_tt_scan_status, wraplength=860, justify=tk.LEFT).grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(6, 0)
        )
        tt_act = ttk.Frame(tt_fr)
        tt_act.grid(row=2, column=0, columnspan=3, sticky="w", pady=(6, 0))
        btn_tt_select = ttk.Button(tt_act, text="2) Chọn hết", command=self._on_uv_tt_select_all)
        btn_tt_select.pack(side=tk.LEFT, padx=(0, 8))
        btn_tt_dl = ttk.Button(tt_act, text="3) Tải TikTok đã chọn", command=self._on_uv_download_tt_selected)
        btn_tt_dl.pack(side=tk.LEFT, padx=(0, 8))
        btn_tt_dl_all = ttk.Button(tt_act, text="Tải hết danh sách", command=self._on_uv_download_tt_all)
        btn_tt_dl_all.pack(side=tk.LEFT, padx=(0, 8))
        self._uv_busy_disable_widgets.extend((btn_tt_select, btn_tt_dl, btn_tt_dl_all))
        tt_tree_fr = ttk.Frame(tt_fr)
        tt_tree_fr.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(6, 0))
        tt_tree_fr.columnconfigure(0, weight=1)
        self._tree_tt_channel = ttk.Treeview(
            tt_tree_fr,
            columns=("idx", "title", "url"),
            show="headings",
            height=8,
            selectmode="extended",
        )
        self._tree_tt_channel.heading("idx", text="#")
        self._tree_tt_channel.heading("title", text="Tiêu đề")
        self._tree_tt_channel.heading("url", text="URL")
        self._tree_tt_channel.column("idx", width=40, stretch=False)
        self._tree_tt_channel.column("title", width=260, stretch=True)
        self._tree_tt_channel.column("url", width=520, stretch=True)
        sy_tt = ttk.Scrollbar(tt_tree_fr, orient="vertical", command=self._tree_tt_channel.yview)
        sx_tt = ttk.Scrollbar(tt_tree_fr, orient="horizontal", command=self._tree_tt_channel.xview)
        self._tree_tt_channel.configure(yscrollcommand=sy_tt.set, xscrollcommand=sx_tt.set)
        self._tree_tt_channel.grid(row=0, column=0, sticky="ew")
        sy_tt.grid(row=0, column=1, sticky="ns")
        sx_tt.grid(row=1, column=0, sticky="ew")
        install_treeview_shortcuts(self._tree_tt_channel, owner=self._top, info_callback=self._set_uv_status)

        self._var_uv_ig_scan_status.set(
            "Chưa quét. Dán URL profile Instagram hoặc …/username/reels/ ở ô URL phía trên (Playwright)."
        )
        ig_coll = _UvCollapsibleSection(
            host,
            title="Bước 2 (Instagram) — Quét tab Reels profile để chọn tải",
            start_open=True,
            on_toggle=_uv_step2_scroll_sync,
        )
        ig_coll.outer.grid(row=6, column=0, sticky="ew", pady=(8, 0))
        ig_coll.outer.columnconfigure(0, weight=1)
        ig_fr = ttk.Frame(ig_coll.body, padding=8)
        ig_fr.pack(fill=tk.BOTH, expand=True)
        ig_fr.columnconfigure(1, weight=1)
        ttk.Label(
            ig_fr,
            text=(
                "URL profile: https://www.instagram.com/username/ hoặc tab Reels …/username/reels/. "
                "Reel/post đơn: dùng «Tải URL hiện tại» ở Bước 1."
            ),
            wraplength=820,
            justify=tk.LEFT,
            foreground="#555",
        ).grid(row=0, column=0, columnspan=3, sticky="w")
        ttk.Label(ig_fr, textvariable=self._var_uv_ig_scan_status, wraplength=860, justify=tk.LEFT).grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(6, 0)
        )
        ig_act = ttk.Frame(ig_fr)
        ig_act.grid(row=2, column=0, columnspan=3, sticky="w", pady=(6, 0))
        btn_ig_select = ttk.Button(ig_act, text="2) Chọn hết", command=self._on_uv_ig_select_all)
        btn_ig_select.pack(side=tk.LEFT, padx=(0, 8))
        btn_ig_dl = ttk.Button(ig_act, text="3) Tải Instagram đã chọn", command=self._on_uv_download_ig_selected)
        btn_ig_dl.pack(side=tk.LEFT, padx=(0, 8))
        btn_ig_dl_all = ttk.Button(ig_act, text="Tải hết danh sách", command=self._on_uv_download_ig_all)
        btn_ig_dl_all.pack(side=tk.LEFT, padx=(0, 8))
        self._uv_busy_disable_widgets.extend((btn_ig_select, btn_ig_dl, btn_ig_dl_all))
        ig_tree_fr = ttk.Frame(ig_fr)
        ig_tree_fr.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(6, 0))
        ig_tree_fr.columnconfigure(0, weight=1)
        self._tree_ig_channel = ttk.Treeview(
            ig_tree_fr,
            columns=("idx", "title", "url"),
            show="headings",
            height=8,
            selectmode="extended",
        )
        self._tree_ig_channel.heading("idx", text="#")
        self._tree_ig_channel.heading("title", text="Tiêu đề")
        self._tree_ig_channel.heading("url", text="URL")
        self._tree_ig_channel.column("idx", width=40, stretch=False)
        self._tree_ig_channel.column("title", width=260, stretch=True)
        self._tree_ig_channel.column("url", width=520, stretch=True)
        sy_ig = ttk.Scrollbar(ig_tree_fr, orient="vertical", command=self._tree_ig_channel.yview)
        sx_ig = ttk.Scrollbar(ig_tree_fr, orient="horizontal", command=self._tree_ig_channel.xview)
        self._tree_ig_channel.configure(yscrollcommand=sy_ig.set, xscrollcommand=sx_ig.set)
        self._tree_ig_channel.grid(row=0, column=0, sticky="ew")
        sy_ig.grid(row=0, column=1, sticky="ns")
        sx_ig.grid(row=1, column=0, sticky="ew")
        install_treeview_shortcuts(self._tree_ig_channel, owner=self._top, info_callback=self._set_uv_status)
        section_state["fb_fr"] = fb_coll.outer
        section_state["yt_fr"] = yt_coll.outer
        section_state["tt_fr"] = tt_coll.outer
        section_state["ig_fr"] = ig_coll.outer

        prog_fr = ttk.LabelFrame(host, text="Bước 3 — Theo dõi tiến trình", padding=8)
        prog_fr.grid(row=7, column=0, sticky="ew", pady=(6, 0))
        prog_fr.columnconfigure(0, weight=1)
        self._var_uv_operation_status.set("Sẵn sàng — có thể thao tác.")
        ttk.Label(prog_fr, textvariable=self._var_uv_operation_status, wraplength=860, justify=tk.LEFT).grid(
            row=0, column=0, sticky="w"
        )
        self._uv_progress = ttk.Progressbar(prog_fr, mode="indeterminate", length=420)
        self._uv_progress.grid(row=1, column=0, sticky="ew", pady=(6, 0))

        act = ttk.Frame(host)
        act.grid(row=8, column=0, sticky="w", pady=(8, 0))

        row_main = ttk.Frame(act)
        row_main.pack(anchor="w")
        for text, cmd in (
            ("Tiếp tục job cuối", self._on_uv_resume),
            ("Tạm dừng / Hủy", self._on_uv_pause),
            ("Mở thư mục lưu video", self._on_uv_open_out_dir),
            ("Mở Video Editor", self._on_uv_open_video_editor_with_last_job),
        ):
            b = ttk.Button(row_main, text=text, command=cmd)
            b.pack(side=tk.LEFT, padx=(0, 8))
            if text == "Tiếp tục job cuối":
                self._uv_busy_disable_widgets.append(b)
        row_b = ttk.Frame(act)
        row_b.pack(anchor="w", pady=(4, 0))
        b_refresh = ttk.Button(row_b, text="Làm mới danh sách", command=self._refresh_uv_library)
        b_refresh.pack(side=tk.LEFT, padx=(0, 8))
        self._uv_busy_disable_widgets.append(b_refresh)
        var_show_tools = tk.BooleanVar(value=False)
        btn_tools = ttk.Button(row_b, text="Công cụ nâng cao ▾")
        btn_tools.pack(side=tk.LEFT, padx=(0, 8))
        tools_fr = ttk.Frame(act)
        tools_fr.pack(anchor="w", pady=(4, 0))
        tools_fr.pack_forget()

        def _toggle_tools() -> None:
            show = not bool(var_show_tools.get())
            var_show_tools.set(show)
            if show:
                tools_fr.pack(anchor="w", pady=(4, 0))
                btn_tools.configure(text="Ẩn công cụ nâng cao ▴")
            else:
                tools_fr.pack_forget()
                btn_tools.configure(text="Công cụ nâng cao ▾")
            self._sync_uv_download_scrollregion(scroll_to_content=False)

        btn_tools.configure(command=_toggle_tools)
        for text, cmd in (
            ("Kiểm tra URL", self._on_uv_check_url),
            ("Kiểm tra yt-dlp", self._on_uv_verify_ytdlp),
            ("Cập nhật yt-dlp", self._on_uv_ytdlp_check_and_update),
        ):
            b = ttk.Button(tools_fr, text=text, command=cmd)
            b.pack(side=tk.LEFT, padx=(0, 8))
            self._uv_busy_disable_widgets.append(b)

        lib = ttk.LabelFrame(host, text="Bước 4 — Video đã tải", padding=8)
        lib.grid(row=9, column=0, sticky="nsew", pady=(8, 0))
        lib.columnconfigure(0, weight=1)
        lib.rowconfigure(1, weight=1)
        lib_filter = ttk.Frame(lib)
        lib_filter.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        lib_filter.columnconfigure(1, weight=1)
        ttk.Label(lib_filter, text="Lọc theo job").grid(row=0, column=0, sticky="w")
        self._cb_uv_lib_job_filter = ttk.Combobox(
            lib_filter,
            textvariable=self._var_uv_lib_job_filter,
            values=["Tất cả job"],
            state="readonly",
            width=44,
        )
        self._cb_uv_lib_job_filter.grid(row=0, column=1, sticky="ew", padx=(8, 8))
        self._cb_uv_lib_job_filter.bind("<<ComboboxSelected>>", lambda _e: self._on_uv_library_job_filter_changed())
        ttk.Button(lib_filter, text="Bỏ lọc", command=self._on_uv_library_clear_job_filter).grid(
            row=0, column=2, sticky="w"
        )
        ttk.Button(lib_filter, text="Xóa job lọc", command=self._on_uv_delete_filtered_job).grid(
            row=0, column=3, sticky="w", padx=(8, 0)
        )
        lib_filter.columnconfigure(4, weight=1)
        ttk.Label(
            lib_filter,
            textvariable=self._var_uv_lib_total_ok,
            foreground="#1f4d8f",
            font=("Segoe UI", 9, "bold"),
        ).grid(row=0, column=4, sticky="e", padx=(12, 0))
        cols = ("job", "platform", "title", "hashtags", "duration", "uploader", "status", "path")
        self._tree_uv = ttk.Treeview(lib, columns=cols, show="headings", height=8, selectmode="extended")
        heads = {
            "job": "Job",
            "platform": "Nền tảng",
            "title": "Tiêu đề",
            "hashtags": "Hashtag",
            "duration": "Thời lượng",
            "uploader": "Kênh/Tác giả",
            "status": "Trạng thái",
            "path": "Đường dẫn file",
        }
        widths = {
            "job": 140,
            "platform": 86,
            "title": 160,
            "hashtags": 150,
            "duration": 70,
            "uploader": 110,
            "status": 86,
            "path": 320,
        }
        for c in cols:
            self._tree_uv.heading(c, text=heads[c])
            self._tree_uv.column(c, width=widths[c], stretch=True if c == "path" else False)
        sy = ttk.Scrollbar(lib, orient="vertical", command=self._tree_uv.yview)
        sx = ttk.Scrollbar(lib, orient="horizontal", command=self._tree_uv.xview)
        self._tree_uv.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)
        self._tree_uv.grid(row=1, column=0, sticky="nsew")
        sy.grid(row=1, column=1, sticky="ns")
        sx.grid(row=2, column=0, sticky="ew")
        install_treeview_shortcuts(self._tree_uv, owner=self._top, info_callback=self._set_uv_status)

        ab = ttk.Frame(lib)
        ab.grid(row=3, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Label(
            ab,
            text="Chọn 1 hoặc kéo nhiều dòng. Xem/Mở/Reverse/Dùng lấy dòng đầu; Xóa hỗ trợ nhiều dòng.",
            foreground="#666",
            font=("Segoe UI", 8),
        ).pack(side=tk.LEFT, padx=(0, 12))
        ttk.Button(ab, text="Xem nhanh", command=self._on_uv_preview_selected).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(ab, text="Mở thư mục file", command=self._on_uv_open_folder_selected).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(ab, text="Phân tích Reverse", command=self._on_uv_analyze_reverse).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(ab, text="Dùng cho AI Video", command=self._on_uv_use_ai_video).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(ab, text="Xóa mục chọn", command=self._on_uv_delete_selected).pack(side=tk.LEFT)

        logf = ttk.LabelFrame(host, text="Log tải (chi tiết)", padding=6)
        logf.grid(row=10, column=0, sticky="nsew", pady=(8, 0))
        logf.columnconfigure(0, weight=1)
        logf.rowconfigure(0, weight=1)
        self._txt_uv_log = tk.Text(logf, wrap="word", height=6)
        sl = ttk.Scrollbar(logf, orient="vertical", command=self._txt_uv_log.yview)
        self._txt_uv_log.configure(yscrollcommand=sl.set)
        self._txt_uv_log.grid(row=0, column=0, sticky="nsew")
        sl.grid(row=0, column=1, sticky="ns")
        self._txt_uv_log.insert("1.0", "Log chi tiết khi tải URL sẽ hiện ở đây.\n")
        self._txt_uv_log.configure(state="disabled")

        host.rowconfigure(9, weight=1)
        _resize_job: dict[str, str | None] = {"after_id": None}

        def _visit_widgets(w: tk.Misc, fn: Callable[[tk.Misc], None]) -> None:
            fn(w)
            for ch in w.winfo_children():
                _visit_widgets(ch, fn)

        def _fit_tree_columns() -> None:
            if self._tree_fb_reels is not None:
                w = max(260, int(self._tree_fb_reels.winfo_width()))
                self._tree_fb_reels.column("idx", width=44, stretch=False)
                self._tree_fb_reels.column("url", width=max(220, w - 52), stretch=True)
            if self._tree_yt_channel is not None:
                w = max(260, int(self._tree_yt_channel.winfo_width()))
                self._tree_yt_channel.column("idx", width=44, stretch=False)
                self._tree_yt_channel.column("title", width=max(120, int(w * 0.32)), stretch=True)
                self._tree_yt_channel.column("url", width=max(180, int(w * 0.60)), stretch=True)
            if self._tree_tt_channel is not None:
                w = max(260, int(self._tree_tt_channel.winfo_width()))
                self._tree_tt_channel.column("idx", width=44, stretch=False)
                self._tree_tt_channel.column("title", width=max(120, int(w * 0.32)), stretch=True)
                self._tree_tt_channel.column("url", width=max(180, int(w * 0.60)), stretch=True)
            if self._tree_ig_channel is not None:
                w = max(260, int(self._tree_ig_channel.winfo_width()))
                self._tree_ig_channel.column("idx", width=44, stretch=False)
                self._tree_ig_channel.column("title", width=max(120, int(w * 0.32)), stretch=True)
                self._tree_ig_channel.column("url", width=max(180, int(w * 0.60)), stretch=True)
            if self._tree_uv is not None:
                w = max(260, int(self._tree_uv.winfo_width()))
                self._tree_uv.column("job", width=max(100, int(w * 0.12)), stretch=True)
                self._tree_uv.column("platform", width=max(84, int(w * 0.08)), stretch=False)
                self._tree_uv.column("title", width=max(120, int(w * 0.16)), stretch=True)
                self._tree_uv.column("hashtags", width=max(100, int(w * 0.12)), stretch=True)
                self._tree_uv.column("duration", width=max(70, int(w * 0.06)), stretch=False)
                self._tree_uv.column("uploader", width=max(120, int(w * 0.12)), stretch=True)
                self._tree_uv.column("status", width=max(90, int(w * 0.08)), stretch=False)
                self._tree_uv.column("path", width=max(180, int(w * 0.26)), stretch=True)

        def _reflow_download_tab() -> None:
            # Cho cửa sổ hẹp / DPI cao: vẫn co wrap + cột tree, tránh nhãn cắt chữ và thanh ngang vô dụng.
            host_w = max(280, int(host.winfo_width()))
            wrap = max(220, host_w - 48)

            def _apply_wrap(widget: tk.Misc) -> None:
                if not isinstance(widget, (ttk.Label, tk.Label)):
                    return
                try:
                    raw = str(widget.cget("wraplength") or "").strip()
                except Exception:
                    return
                if not raw:
                    return
                try:
                    cur = int(float(raw))
                except Exception:
                    return
                if cur > 0:
                    try:
                        widget.configure(wraplength=wrap)
                    except Exception:
                        pass

            _visit_widgets(host, _apply_wrap)
            _fit_tree_columns()
            self._sync_uv_download_scrollregion(scroll_to_content=False)

        def _schedule_reflow(_event: tk.Event | None = None) -> None:
            prev = _resize_job.get("after_id")
            if prev:
                try:
                    self._top.after_cancel(prev)
                except Exception:
                    pass
            _resize_job["after_id"] = self._top.after(80, _reflow_download_tab)

        host.bind("<Configure>", _schedule_reflow, add="+")
        if self._embedded_download_host is None:
            self._top.after(100, self._refresh_uv_library)
            self._top.after(150, lambda: self._sync_uv_download_scrollregion(scroll_to_content=False))
            self._top.after(220, _reflow_download_tab)
        else:
            self._top.after(120, _reflow_download_tab)
        _refresh_detect_hint()


    def _uv_options_dict(self) -> dict[str, Any]:
        plat = self._normalize_uv_platform_choice(self._var_uv_platform.get().strip())
        if plat in ("auto", ""):
            plat = detect_platform(self._var_uv_url.get().strip())
        plat_low = plat.strip().lower()
        ut = self._normalize_uv_url_type_choice(self._var_uv_url_type.get().strip())
        if ut == "playlist_or_profile":
            if plat_low == "youtube":
                ut = "playlist"
            elif plat_low in ("facebook", "tiktok", "instagram"):
                ut = "profile"
            else:
                ut = "playlist"
        if ut in ("auto", ""):
            ut = classify_url_type(self._var_uv_url.get().strip())
        return {
            "platform": plat,
            "url_type": ut,
            "job_name": self._var_uv_job_name.get().strip(),
            "max_videos": int(self._var_uv_max_videos.get().strip() or "50"),
            "output_dir": self._var_uv_out_dir.get().strip(),
            "organize_by_platform": bool(self._var_uv_org_platform.get()),
            "organize_by_uploader": bool(self._var_uv_org_uploader.get()),
            "skip_existing": bool(self._var_uv_skip_existing.get()),
            "write_info_json": bool(self._var_uv_info_json.get()),
            "write_thumbnail": bool(self._var_uv_thumbnail.get()),
        }

    @staticmethod
    def _normalize_uv_platform_choice(choice: str) -> str:
        c = str(choice or "").strip().lower()
        mapping = {
            "tự nhận diện": "auto",
            "auto detect": "auto",
            "auto": "auto",
            "youtube": "youtube",
            "facebook": "facebook",
            "tiktok": "tiktok",
            "instagram": "instagram",
            "ig": "instagram",
            "unknown": "unknown",
            "không rõ": "unknown",
        }
        return mapping.get(c, c)

    @staticmethod
    def _normalize_uv_url_type_choice(choice: str) -> str:
        c = str(choice or "").strip().lower()
        mapping = {
            "tự nhận diện": "auto",
            "auto detect": "auto",
            "auto": "auto",
            "video đơn": "single_video",
            "single_video": "single_video",
            "danh sách (playlist/profile)": "playlist_or_profile",
            "playlist_or_profile": "playlist_or_profile",
            "playlist": "playlist",
            "kênh": "channel",
            "channel": "channel",
            "profile": "profile",
            "không rõ": "unknown",
            "unknown": "unknown",
        }
        return mapping.get(c, c)

    def _uv_batch_job_source_url(self, urls: list[str]) -> str:
        """URL ghi trên job khi tải batch (ưu tiên ô URL kênh/tab; fallback URL đầu danh sách)."""
        root = str(self._var_uv_url.get() or "").strip()
        if root:
            return root
        for u in urls:
            s = str(u or "").strip()
            if s:
                return s
        return ""

    @staticmethod
    def _uv_unique_nonempty_urls(urls: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for raw in urls:
            u = str(raw or "").strip()
            if not u:
                continue
            key = u.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(u)
        return out

    def _on_uv_pick_folder(self) -> None:
        d = filedialog.askdirectory(parent=self._top, title="Chọn thư mục lưu video")
        if d:
            self._var_uv_out_dir.set(d)

    def _persist_uv_last_job_name(self, *, interactive: bool) -> bool:
        name = str(self._var_uv_job_name.get() or "").strip()[:120]
        self._var_uv_job_name.set(name)
        if name == self._uv_last_saved_job_name:
            if interactive:
                messagebox.showinfo(
                    "Tải video",
                    "Tên job trùng với bản đã lưu — không cần ghi lại.",
                    parent=self._top,
                )
            return True
        cfg_path = project_root() / "config" / "universal_video_downloader.json"
        try:
            raw: dict[str, Any] = {}
            if cfg_path.is_file():
                loaded = json.loads(cfg_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    raw = loaded
            uvd = dict(raw.get("universal_video_downloader") or {})
            dl = dict(uvd.get("download") or {})
            dl["last_job_name"] = name
            uvd["download"] = dl
            raw["universal_video_downloader"] = uvd
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            cfg_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            self._uv_last_saved_job_name = name
            if interactive:
                messagebox.showinfo("Tải video", "Đã lưu tên job mặc định.", parent=self._top)
            return True
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Tải video", f"Không lưu được tên job: {exc}", parent=self._top)
            return False

    def _on_uv_save_job_name(self) -> None:
        self._persist_uv_last_job_name(interactive=True)

    def _fb_reel_download_opts(self) -> dict[str, Any]:
        o = self._uv_options_dict()
        o["platform"] = "facebook"
        o["url_type"] = "single_video"
        o["max_videos"] = 1
        # Giữ theo checkbox «Ghi info JSON» / «Lưu thumbnail» — tắt sẽ tải batch nhanh hơn rõ rệt.
        return o

    def _parse_fb_reel_limits(self) -> tuple[int, int, int, bool]:
        try:
            mc = int(self._var_uv_fb_max_collect.get().strip())
        except ValueError:
            mc = 300
        try:
            ms = int(self._var_uv_fb_max_scroll.get().strip())
        except ValueError:
            ms = 100
        try:
            mins = int(self._var_uv_fb_scan_minutes.get().strip())
        except ValueError:
            mins = 30
        mc = max(10, min(UV_FB_MAX_COLLECT, mc))
        ms = max(5, min(280, ms))
        mins = max(1, min(180, mins))
        till_end = bool(self._var_uv_fb_scroll_until_end.get())
        return mc, ms, mins, till_end

    def _on_uv_save_fb_reel_limits(self) -> None:
        mc, ms, mins, till_end = self._parse_fb_reel_limits()
        self._var_uv_fb_max_collect.set(str(mc))
        self._var_uv_fb_max_scroll.set(str(ms))
        self._var_uv_fb_scan_minutes.set(str(mins))
        self._var_uv_fb_scroll_until_end.set(till_end)
        try:
            persist_facebook_reels_settings(
                max_collect=mc,
                max_scroll_rounds=ms,
                max_scan_minutes=mins,
                scroll_until_end=till_end,
            )
        except OSError as exc:
            messagebox.showerror("Cấu hình", str(exc), parent=self._top)
            return
        messagebox.showinfo("Cấu hình", "Đã lưu giới hạn quét Reels vào config.", parent=self._top)

    def _on_uv_pick_fb_cookie(self) -> None:
        path = filedialog.askopenfilename(
            parent=self._top,
            title="Chọn file cookie Playwright (JSON)",
            filetypes=[("JSON", "*.json"), ("All", "*.*")],
        )
        if path:
            self._var_uv_fb_cookie.set(path)
            try:
                persist_facebook_reels_settings(cookie_path=path)
            except OSError as exc:
                messagebox.showwarning("Cookie", f"Đã chọn file nhưng không ghi được config: {exc}", parent=self._top)

    def _parse_ig_reel_limits(self) -> tuple[int, int, int, bool]:
        try:
            mc = int(self._var_uv_ig_list_max.get().strip())
        except ValueError:
            mc = 120
        try:
            ms = int(self._var_uv_ig_max_scroll.get().strip())
        except ValueError:
            ms = 60
        try:
            mins = int(self._var_uv_ig_scan_minutes.get().strip())
        except ValueError:
            mins = 15
        mc = max(10, min(UV_FB_MAX_COLLECT, mc))
        ms = max(5, min(280, ms))
        mins = max(1, min(180, mins))
        till_end = bool(self._var_uv_ig_scroll_until_end.get())
        return mc, ms, mins, till_end

    def _on_uv_save_ig_reel_limits(self) -> None:
        mc, ms, mins, till_end = self._parse_ig_reel_limits()
        self._var_uv_ig_list_max.set(str(mc))
        self._var_uv_ig_max_scroll.set(str(ms))
        self._var_uv_ig_scan_minutes.set(str(mins))
        self._var_uv_ig_scroll_until_end.set(till_end)
        try:
            persist_instagram_reels_settings(
                max_collect=mc,
                max_scroll_rounds=ms,
                max_scan_minutes=mins,
                scroll_until_end=till_end,
            )
        except OSError as exc:
            messagebox.showerror("Cấu hình", str(exc), parent=self._top)
            return
        messagebox.showinfo("Cấu hình", "Đã lưu giới hạn quét Instagram vào config.", parent=self._top)

    def _on_uv_pick_ig_cookie(self) -> None:
        path = filedialog.askopenfilename(
            parent=self._top,
            title="Chọn cookie Instagram (Playwright storage_state JSON)",
            filetypes=[("JSON", "*.json"), ("All", "*.*")],
        )
        if path:
            self._var_uv_ig_cookie.set(path)
            try:
                persist_instagram_reels_settings(cookie_path=path)
            except OSError as exc:
                messagebox.showwarning("Cookie", f"Đã chọn file nhưng không ghi được config: {exc}", parent=self._top)

    def _resolve_ig_cookie_path(self) -> str | None:
        raw = str(self._var_uv_ig_cookie.get() or "").strip()
        if not raw:
            return None
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = project_root() / path
        if path.is_file():
            return str(path.resolve())
        return None

    def _load_uv_fb_accounts(self) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        try:
            acc_raw = AccountsDatabaseManager().load_all()
        except Exception:
            acc_raw = []
        self._uv_fb_accounts_by_id = {}
        if isinstance(acc_raw, list):
            for row in acc_raw:
                if not isinstance(row, dict):
                    continue
                aid = str(row.get("id") or "").strip()
                aname = str(row.get("name") or "").strip() or aid
                if not aid:
                    continue
                self._uv_fb_accounts_by_id[aid] = dict(row)
                label = f"[{aid}] {aname}"
                if label in seen:
                    continue
                seen.add(label)
                out.append(label)
        return out

    def _on_uv_reload_fb_profiles(self) -> None:
        self._uv_fb_profile_urls = self._load_uv_fb_accounts()
        if self._cb_uv_fb_profile is not None:
            self._cb_uv_fb_profile.configure(values=self._uv_fb_profile_urls)
        if self._uv_fb_profile_urls and not str(self._var_uv_fb_profile_pick.get() or "").strip():
            self._var_uv_fb_profile_pick.set(self._uv_fb_profile_urls[0])

    def _on_uv_apply_fb_profile_url(self) -> None:
        picked = str(self._var_uv_fb_profile_pick.get() or "").strip()
        if not picked:
            messagebox.showwarning("Tài khoản Facebook", "Chưa chọn tài khoản.", parent=self._top)
            return
        aid = ""
        if picked.startswith("[") and "]" in picked:
            aid = picked[1 : picked.find("]")].strip()
        if not aid:
            messagebox.showwarning("Tài khoản Facebook", "Không đọc được account id từ lựa chọn.", parent=self._top)
            return
        self._var_uv_platform.set("facebook")
        self._uv_fb_selected_account_id = aid
        self._var_uv_fb_scan_status.set(
            f"Đã chọn tài khoản {picked}. Quét Reels sẽ dùng đúng profile browser của tài khoản này."
        )

    @staticmethod
    def _extract_fb_account_id_from_label(label: str) -> str:
        s = str(label or "").strip()
        if s.startswith("[") and "]" in s:
            return s[1 : s.find("]")].strip()
        return ""

    def _on_uv_fb_browser_toggle_notice(self) -> None:
        show = bool(self._var_uv_fb_show_browser.get())
        if show:
            msg = "Đã bật HIỆN browser khi quét Reels (Playwright sẽ mở cửa sổ trình duyệt)."
        else:
            msg = "Đã bật chế độ CHẠY ẨN browser khi quét Reels (headless)."
        self._var_uv_fb_scan_status.set(msg)
        messagebox.showinfo("Quét Reels", msg, parent=self._top)

    def _uv_tree_gen_bump(self, attr: str) -> int:
        g = int(getattr(self, attr, 0)) + 1
        setattr(self, attr, g)
        return g

    def _uv_tree_gen_is_current(self, attr: str, generation: int) -> bool:
        return int(getattr(self, attr, 0)) == int(generation)

    def _uv_finish_scan_tree_refresh(
        self,
        *,
        rows_or_urls: list[Any],
        refresh_fn: Callable[..., None],
        backing_count: Callable[[], int],
        status_setter: Callable[[str], None],
        status_text: str,
        sync_backing: Callable[[list[Any]], None] | None = None,
    ) -> None:
        """Sau quét xong: chỉ vẽ lại tree nếu dữ liệu thay đổi so với lần partial cuối."""
        current_n = backing_count()
        if len(rows_or_urls) == current_n and current_n > 0:
            if sync_backing is not None:
                sync_backing(rows_or_urls)
            status_setter(status_text)
            return
        refresh_fn(rows_or_urls)
        status_setter(status_text)

    def _uv_tree_append_specs_chunked(
        self,
        tree: ttk.Treeview,
        specs: list[dict[str, Any]],
        *,
        gen_attr: str,
    ) -> None:
        """Append nhiều dòng Treeview theo lô — dùng khi partial quét thêm hàng trăm dòng."""
        if not specs:
            return
        gen = self._uv_tree_gen_bump(gen_attr)

        def _done() -> None:
            if self._uv_tree_gen_is_current(gen_attr, gen):
                self._sync_uv_download_scrollregion(scroll_to_content=False)

        tree_insert_chunked(
            self._top,
            tree,
            specs,
            generation=gen,
            is_current=lambda g: self._uv_tree_gen_is_current(gen_attr, g),
            on_complete=_done,
            chunk=DEFAULT_TREE_APPEND_CHUNK,
        )

    def _refresh_fb_reel_tree(self, urls: list[str], *, append_from: int = 0) -> None:
        """Làm mới hoặc chỉ append dòng mới (``append_from`` > 0) để quét dài không block UI."""
        self._uv_fb_reel_urls = list(urls)
        tr = self._tree_fb_reels
        if tr is None:
            return
        if append_from > 0 and append_from <= len(urls):
            specs = [{"iid": str(i), "values": (str(i + 1), urls[i])} for i in range(append_from, len(urls))]
            if specs:
                self._uv_tree_append_specs_chunked(tr, specs, gen_attr="_uv_fb_tree_gen")
            return
        gen = self._uv_tree_gen_bump("_uv_fb_tree_gen")
        tree_delete_all(tr)
        if not urls:
            self._sync_uv_download_scrollregion(scroll_to_content=False)
            return
        specs = [{"iid": str(i), "values": (str(i + 1), u)} for i, u in enumerate(urls)]

        def _done() -> None:
            if self._uv_tree_gen_is_current("_uv_fb_tree_gen", gen):
                self._sync_uv_download_scrollregion(scroll_to_content=True)

        tree_insert_chunked(
            self._top,
            tr,
            specs,
            generation=gen,
            is_current=lambda g: self._uv_tree_gen_is_current("_uv_fb_tree_gen", g),
            on_complete=_done,
            chunk=DEFAULT_TREE_CHUNK,
        )

    def _on_uv_fb_select_all(self) -> None:
        tr = self._tree_fb_reels
        if not tr:
            return
        n = len(self._uv_fb_reel_urls)
        if n > UV_LOGICAL_SELECT_ALL_THRESHOLD:
            self._uv_fb_logical_select_all = True
            tr.selection_remove(tr.selection())
            self._var_uv_fb_scan_status.set(f"Đã chọn logic {n} reel — bấm «Tải reel đã chọn».")
            return
        self._uv_fb_logical_select_all = False
        children = tr.get_children()
        if len(children) > DEFAULT_TREE_SELECT_CHUNK:
            tree_select_all_chunked(self._top, tr, chunk=DEFAULT_TREE_SELECT_CHUNK)
        else:
            tr.selection_set(children)

    def _on_uv_fb_select_none(self) -> None:
        tr = self._tree_fb_reels
        if not tr:
            return
        self._uv_fb_logical_select_all = False
        tr.selection_remove(tr.selection())

    def _on_uv_scan_fb_reels(self) -> None:
        raw = self._var_uv_url.get().strip()
        if not raw:
            messagebox.showwarning("Quét Reels", "Nhập URL profile hoặc tab Reels ở ô URL phía trên.", parent=self._top)
            return
        if detect_platform(raw) != "facebook":
            messagebox.showwarning(
                "Quét Reels",
                "URL hiện tại không phải Facebook.\nVí dụ:\n"
                "- https://www.facebook.com/<profile>\n"
                "- https://www.facebook.com/<profile>/reels\n"
                "- https://www.facebook.com/profile.php?id=<id>",
                parent=self._top,
            )
            return
        page_url = normalize_facebook_reels_tab_url(raw)
        self._var_uv_url.set(page_url)
        max_reels, max_scroll, max_minutes, till_end = self._parse_fb_reel_limits()
        self._var_uv_fb_max_collect.set(str(max_reels))
        self._var_uv_fb_max_scroll.set(str(max_scroll))
        self._var_uv_fb_scan_minutes.set(str(max_minutes))
        self._var_uv_fb_scroll_until_end.set(till_end)
        try:
            persist_facebook_reels_settings(
                max_collect=max_reels,
                max_scroll_rounds=max_scroll,
                max_scan_minutes=max_minutes,
                scroll_until_end=till_end,
            )
        except OSError:
            pass
        picked_label = str(self._var_uv_fb_profile_pick.get() or "").strip()
        aid = str(self._uv_fb_selected_account_id or "").strip() or self._extract_fb_account_id_from_label(picked_label)
        if not aid:
            messagebox.showwarning(
                "Quét Reels",
                "Chưa chọn tài khoản Facebook. Hãy chọn tài khoản trước khi quét.",
                parent=self._top,
            )
            return
        self._uv_fb_selected_account_id = aid
        mode_txt = "cuộn tới hết trang" if till_end else "dừng theo vòng cuộn"
        self._uv_set_busy(
            True,
            f"Đang mở Playwright và quét tab Reels ({mode_txt}, tối đa {max_minutes} phút)…",
        )
        self._uv_fb_logical_select_all = False
        self._refresh_fb_reel_tree([])
        self._var_uv_fb_scan_status.set("Đang quét — bảng «URL reel» sẽ hiện dần…")

        def _status(msg: str) -> None:
            self._top.after(0, lambda m=msg: self._var_uv_fb_scan_status.set(m))

        last_fb_count = {"n": 0}

        def _partial(urls: list[str]) -> None:
            snap = list(urls)
            now = time.monotonic()
            grow = len(snap) - int(last_fb_count["n"])
            if grow < 2 and now - self._uv_last_partial_ui_ts < 0.35:
                return
            self._uv_last_partial_ui_ts = now
            prev = int(last_fb_count["n"])
            last_fb_count["n"] = len(snap)

            def _apply() -> None:
                if prev <= 0:
                    self._refresh_fb_reel_tree(snap)
                else:
                    self._refresh_fb_reel_tree(snap, append_from=prev)
                if snap:
                    self._var_uv_fb_scan_status.set(f"Đang quét… đã thấy {len(snap)} reel (cập nhật trực tiếp trong bảng).")

            self._top.after(0, _apply)

        def _work() -> None:
            show_browser = bool(self._var_uv_fb_show_browser.get())
            acc_id = str(self._uv_fb_selected_account_id or "").strip()
            account: dict[str, Any] | None = None
            if acc_id:
                account = self._uv_fb_accounts_by_id.get(acc_id)
                if not account:
                    try:
                        account = AccountsDatabaseManager().get_by_id(acc_id)
                    except Exception:
                        account = None
            if not isinstance(account, dict):
                self._top.after(
                    0,
                    lambda: messagebox.showerror(
                        "Quét Reels",
                        "Không đọc được hồ sơ tài khoản đã chọn. Dừng quét (không mở browser mới).",
                        parent=self._top,
                    ),
                )
                self._top.after(0, lambda: self._uv_set_busy(False))
                return
            res = scan_facebook_profile_reels_page(
                page_url=page_url,
                account=account,
                max_reels=max_reels,
                max_scroll_rounds=max_scroll,
                max_scan_minutes=max_minutes,
                scroll_until_end=till_end,
                headless=not show_browser,
                status=_status,
                on_partial=_partial,
            )

            def _ui() -> None:
                self._uv_set_busy(False)
                if res.get("ok"):
                    items = res.get("items") or []
                    urls = [str(x.get("url") or "") for x in items if isinstance(x, dict)]
                    self._uv_finish_scan_tree_refresh(
                        rows_or_urls=urls,
                        refresh_fn=self._refresh_fb_reel_tree,
                        backing_count=lambda: len(self._uv_fb_reel_urls),
                        status_setter=self._var_uv_fb_scan_status.set,
                        status_text=res.get("message") or f"{len(urls)} reel.",
                        sync_backing=lambda u: setattr(self, "_uv_fb_reel_urls", list(u)),
                    )
                    messagebox.showinfo(
                        "Quét Reels",
                        f"{res.get('message', '')}\n\n«Tải hết danh sách» hoặc chọn dòng → «Tải reel đã chọn».",
                        parent=self._top,
                    )
                else:
                    self._var_uv_fb_scan_status.set(str(res.get("message") or "Lỗi"))
                    messagebox.showerror("Quét Reels", str(res.get("message") or "Thất bại."), parent=self._top)

            self._top.after(0, _ui)

        threading.Thread(target=_work, daemon=True, name="uv_scan_fb_reels").start()

    def _on_uv_download_fb_reels_all(self) -> None:
        n = len(self._uv_fb_reel_urls)
        if not self._uv_confirm_download_all("Tải hết reel", n, item_label="reel"):
            return
        self._uv_fb_logical_select_all = True
        self._run_uv_fb_reel_download_batch(list(self._uv_fb_reel_urls))

    def _on_uv_download_fb_reels_selected(self) -> None:
        tr = self._tree_fb_reels
        if not tr or not self._uv_fb_reel_urls:
            messagebox.showwarning("Tải reel", "Chưa có danh sách — hãy «Quét Reels» trước.", parent=self._top)
            return
        if self._uv_fb_logical_select_all and self._uv_fb_reel_urls:
            urls = list(self._uv_fb_reel_urls)
        else:
            sel = tr.selection()
            if not sel:
                messagebox.showwarning("Tải reel", "Chọn ít nhất một dòng trong bảng reel.", parent=self._top)
                return
            idxs = sorted({int(i) for i in sel if str(i).isdigit()})
            urls = [self._uv_fb_reel_urls[i] for i in idxs if 0 <= i < len(self._uv_fb_reel_urls)]
        if not urls:
            messagebox.showwarning("Tải reel", "Không lấy được URL từ lựa chọn.", parent=self._top)
            return
        self._run_uv_fb_reel_download_batch(urls)

    def _run_uv_fb_reel_download_batch(self, urls: list[str]) -> None:
        down = self._uv_require_downloader(fail_title="Tải reel")
        if down is None:
            return
        clean_urls: list[str] = []
        seen_fb_ids: set[str] = set()
        for raw in urls:
            u = str(raw or "").strip()
            m = re.search(r"facebook\.com/reel/(\d+)", u, re.I)
            if not m:
                continue
            rid = m.group(1)
            if rid in seen_fb_ids:
                continue
            seen_fb_ids.add(rid)
            clean_urls.append(f"https://www.facebook.com/reel/{rid}")
        urls = clean_urls
        if not urls:
            messagebox.showwarning(
                "Tải reel",
                "Danh sách không có URL reel hợp lệ (dạng /reel/<id>).",
                parent=self._top,
            )
            return
        self._persist_uv_last_job_name(interactive=False)
        try:
            opts = self._fb_reel_download_opts()
        except (ValueError, tk.TclError, TypeError) as exc:  # noqa: BLE001
            messagebox.showerror("Tải reel", f"Tùy chọn không hợp lệ: {exc}", parent=self._top)
            return
        n = len(urls)
        self._uv_set_busy(True, f"Chuẩn bị tải {n} reel bằng yt-dlp…")

        def _batch() -> None:
            jid = ""
            try:
                st = down.get_ytdlp_status()
                if not st.get("ok"):

                    def _bad() -> None:
                        self._uv_set_busy(False)
                        self._apply_ytdlp_status_to_var(st)
                        messagebox.showerror(
                            "Tải reel",
                            f"yt-dlp chưa chạy được: {st.get('message', '')}",
                            parent=self._top,
                        )

                    self._top.after(0, _bad)
                    return
                down.clear_cancel()
                root = self._uv_batch_job_source_url(urls)
                if not root:

                    def _no_root() -> None:
                        self._uv_set_busy(False)
                        messagebox.showwarning(
                            "Tải reel",
                            "Thiếu URL nguồn — nhập URL tab Reels / trang ở ô URL phía trên.",
                            parent=self._top,
                        )

                    self._top.after(0, _no_root)
                    return
                job = down.create_download_job(root, opts)
                jid = str(job.get("id") or "")
                self._top.after(0, lambda j=jid: setattr(self, "_last_download_job_id", j))
                failed_urls: list[str] = []
                cancelled = False
                if down.is_cancel_requested():
                    cancelled = True
                else:
                    use_seq = n > UV_DOWNLOAD_SEQUENTIAL_THRESHOLD

                    def _pulse() -> None:
                        if use_seq:
                            self._var_uv_operation_status.set(f"Đang tải tuần tự {n} reel (1/{n})…")
                            self._append_uv_log(f"[INFO] Bắt đầu tải tuần tự {n} reel…")
                        else:
                            self._var_uv_operation_status.set(f"Đang tải batch {n} reel (yt-dlp -a)…")
                            self._append_uv_log(f"[INFO] Bắt đầu batch {n} reel (1× yt-dlp -a …)")

                    self._top.after(0, _pulse)
                    ph = self._uv_download_progress_hook()

                    def _item_done(idx: int, total: int, _url: str) -> None:
                        if idx == 1 or idx % 8 == 0 or idx == total:
                            self._top.after(
                                0,
                                lambda i=idx, t=total: self._var_uv_operation_status.set(f"Đang tải reel {i}/{t}…"),
                            )

                    try:
                        if use_seq:
                            jcur = down.run_download_urls_sequential_for_job(
                                jid, urls, on_progress=ph, on_item_done=_item_done
                            ) or {}
                        else:
                            jcur = down.run_download_urls_batch_for_job(jid, urls, on_progress=ph) or {}
                        for it in jcur.get("failed_items") or []:
                            uu = str(it.get("url") or "").strip()
                            ee = str(it.get("error") or "")
                            if uu:
                                failed_urls.append(uu)
                                self._top.after(0, lambda a=uu, b=ee: self._append_uv_log(f"[FAILED] {a} | {b}"))
                    except Exception as exc:  # noqa: BLE001
                        self._top.after(0, lambda e=exc: self._append_uv_log(f"[ERROR] {e}"))
                jdone = down.finalize_batch_download_job(jid) if jid else {}
                n_ok = len(jdone.get("downloaded_files") or [])
                n_fail = len(jdone.get("failed_items") or [])
                self._top.after(
                    0,
                    lambda ok=n_ok, ff=n_fail: self._append_uv_log(
                        f"[INFO] Tổng kết job: thành công {ok} video, lỗi {ff}."
                    ),
                )
                self._top.after(
                    0,
                    lambda ok=n_ok, ff=n_fail, tot=n: self._var_uv_operation_status.set(
                        f"Hoàn tất tải: {ok}/{tot} video OK, {ff} lỗi."
                    ),
                )

                def _done(cancelled_run: bool, ok_count: int, fail_count: int, job_done: dict[str, Any]) -> None:
                    self._uv_set_busy(False)
                    self._refresh_uv_library()
                    self._show_uv_list_download_done(
                        title="Tải reel",
                        platform_key="facebook_reels",
                        job_id=jid,
                        total=n,
                        ok_count=ok_count,
                        fail_count=fail_count,
                        jdone=job_done,
                        cancelled=cancelled_run,
                        item_label="reel",
                    )

                self._top.after(0, lambda c=cancelled, ok=n_ok, ff=n_fail, jd=jdone: _done(c, ok, ff, jd))
            except Exception as exc:  # noqa: BLE001
                if jid:
                    try:
                        down.finalize_batch_download_job(jid)
                    except Exception:
                        pass
                self._top.after(0, self._uv_set_busy, False)
                self._top.after(0, self._refresh_uv_library)
                self._top.after(0, lambda e=exc: messagebox.showerror("Tải reel", str(e), parent=self._top))

        threading.Thread(target=_batch, daemon=True, name="uv_fb_reel_batch").start()

    def _yt_channel_download_opts(self) -> dict[str, Any]:
        o = self._uv_options_dict()
        o["platform"] = "youtube"
        o["url_type"] = "single_video"
        o["max_videos"] = 1
        return o

    def _parse_yt_list_max(self) -> int:
        try:
            lim = int(self._var_uv_yt_list_max.get().strip())
        except ValueError:
            lim = 100
        return max(1, min(UV_CHANNEL_LIST_MAX, lim))

    def _refresh_yt_channel_tree(self, rows: list[dict[str, str]], *, append_from: int = 0) -> None:
        self._uv_yt_entry_rows = list(rows)
        tr = self._tree_yt_channel
        if tr is None:
            return
        if append_from > 0 and append_from <= len(rows):
            specs = [
                {
                    "iid": str(i),
                    "values": (str(i + 1), str(rows[i].get("title") or ""), str(rows[i].get("url") or "")),
                }
                for i in range(append_from, len(rows))
            ]
            if specs:
                self._uv_tree_append_specs_chunked(tr, specs, gen_attr="_uv_yt_tree_gen")
            return
        gen = self._uv_tree_gen_bump("_uv_yt_tree_gen")
        tree_delete_all(tr)
        if not rows:
            self._sync_uv_download_scrollregion(scroll_to_content=False)
            return
        specs = [
            {
                "iid": str(i),
                "values": (str(i + 1), str(r.get("title") or ""), str(r.get("url") or "")),
            }
            for i, r in enumerate(rows)
        ]

        def _done() -> None:
            if self._uv_tree_gen_is_current("_uv_yt_tree_gen", gen):
                self._sync_uv_download_scrollregion(scroll_to_content=True)

        tree_insert_chunked(
            self._top,
            tr,
            specs,
            generation=gen,
            is_current=lambda g: self._uv_tree_gen_is_current("_uv_yt_tree_gen", g),
            on_complete=_done,
            chunk=DEFAULT_TREE_CHUNK,
        )

    def _on_uv_yt_select_all(self) -> None:
        tr = self._tree_yt_channel
        if not tr:
            return
        n = len(self._uv_yt_entry_rows)
        if n > UV_LOGICAL_SELECT_ALL_THRESHOLD:
            self._uv_yt_logical_select_all = True
            tr.selection_remove(tr.selection())
            self._var_uv_yt_scan_status.set(f"Đã chọn logic {n} video — bấm «Tải video đã chọn».")
            return
        self._uv_yt_logical_select_all = False
        children = tr.get_children()
        if len(children) > DEFAULT_TREE_SELECT_CHUNK:
            tree_select_all_chunked(self._top, tr, chunk=DEFAULT_TREE_SELECT_CHUNK)
        else:
            tr.selection_set(children)

    def _on_uv_yt_select_none(self) -> None:
        tr = self._tree_yt_channel
        if not tr:
            return
        self._uv_yt_logical_select_all = False
        tr.selection_remove(tr.selection())

    def _on_uv_scan_yt_channel(self) -> None:
        raw = self._var_uv_url.get().strip()
        if not raw:
            messagebox.showwarning("Quét YouTube", "Nhập URL kênh hoặc playlist ở ô URL phía trên.", parent=self._top)
            return
        if detect_platform(raw) != "youtube":
            messagebox.showwarning(
                "Quét YouTube",
                "Cần URL YouTube (kênh, tab Shorts hoặc playlist).",
                parent=self._top,
            )
            return
        ut = classify_url_type(raw)
        picked = self._normalize_uv_url_type_choice(self._var_uv_url_type.get().strip())
        if picked not in ("auto", ""):
            if picked == "playlist_or_profile":
                picked = "playlist"
            elif picked == "profile":
                picked = "channel"
            ut = picked
        if ut not in ("channel", "playlist"):
            messagebox.showwarning(
                "Quét YouTube",
                "URL hiện tại không phải kênh/playlist.\n"
                "Ví dụ: https://www.youtube.com/@tên/shorts hoặc …/playlist?list=…",
                parent=self._top,
            )
            return
        lim = self._parse_yt_list_max()
        self._var_uv_yt_list_max.set(str(lim))
        down = self._uv_require_downloader(fail_title="Quét YouTube")
        if down is None:
            return
        self._uv_yt_logical_select_all = False
        self._uv_set_busy(True, f"Đang quét danh sách YouTube (tối đa {lim} video, yt-dlp)…")
        self._refresh_yt_channel_tree([])
        self._var_uv_yt_scan_status.set("Đang gọi yt-dlp --flat-playlist…")
        last_ui_count = {"n": 0}

        def _partial(rows: list[dict[str, str]]) -> None:
            snap = list(rows)
            now = time.monotonic()
            grow = len(snap) - int(last_ui_count["n"])
            if grow < 3 and now - self._uv_last_partial_ui_ts < 0.55:
                return
            self._uv_last_partial_ui_ts = now
            prev = int(last_ui_count["n"])
            last_ui_count["n"] = len(snap)

            def _apply() -> None:
                if prev <= 0:
                    self._refresh_yt_channel_tree(snap)
                else:
                    self._refresh_yt_channel_tree(snap, append_from=prev)
                self._var_uv_yt_scan_status.set(f"Đang quét… đã thấy {len(snap)} video.")

            self._top.after(0, _apply)

        def _work() -> None:
            res = down.list_flat_playlist_entries(raw, max_entries=lim, on_partial=_partial)

            def _ui() -> None:
                self._uv_set_busy(False)
                if res.get("success"):
                    entries = res.get("entries") or []
                    rows = [e for e in entries if isinstance(e, dict) and str(e.get("url") or "").strip()]
                    ptitle = str(res.get("playlist_title") or "").strip()
                    warn = str(res.get("warning") or "").strip()
                    partial = bool(res.get("partial"))
                    status = f"Đã quét {len(rows)} video." + (f" — {ptitle}" if ptitle else "")
                    if partial and warn:
                        status += f" ({warn})"
                    self._uv_finish_scan_tree_refresh(
                        rows_or_urls=rows,
                        refresh_fn=self._refresh_yt_channel_tree,
                        backing_count=lambda: len(self._uv_yt_entry_rows),
                        status_setter=self._var_uv_yt_scan_status.set,
                        status_text=status,
                        sync_backing=lambda r: setattr(self, "_uv_yt_entry_rows", list(r)),
                    )
                    messagebox.showinfo(
                        "Quét YouTube",
                        (
                            f"{len(rows)} video trong danh sách.\n"
                            + ("(Quét một phần do mạng chậm, bạn vẫn có thể tải các video đã hiện.)\n" if partial else "")
                            + "«Tải hết danh sách» hoặc chọn dòng → «Tải video đã chọn»."
                        ),
                        parent=self._top,
                    )
                else:
                    self._var_uv_yt_scan_status.set(str(res.get("error") or "Lỗi"))
                    messagebox.showerror("Quét YouTube", str(res.get("error") or "Thất bại."), parent=self._top)

            self._top.after(0, _ui)

        threading.Thread(target=_work, daemon=True, name="uv_scan_yt_channel").start()

    def _on_uv_download_yt_all(self) -> None:
        n = len(self._uv_yt_entry_rows)
        if not self._uv_confirm_download_all("Tải hết YouTube", n, item_label="video"):
            return
        self._uv_yt_logical_select_all = True
        urls = [str(r.get("url") or "") for r in self._uv_yt_entry_rows]
        urls = [u for u in urls if u]
        if not urls:
            messagebox.showwarning("Tải hết YouTube", "Danh sách không có URL hợp lệ.", parent=self._top)
            return
        self._run_uv_yt_channel_download_batch(urls)

    def _on_uv_download_yt_selected(self) -> None:
        tr = self._tree_yt_channel
        if not tr or not self._uv_yt_entry_rows:
            messagebox.showwarning("Tải YouTube", "Chưa có danh sách — hãy «Quét kênh (yt-dlp)» trước.", parent=self._top)
            return
        if self._uv_yt_logical_select_all and self._uv_yt_entry_rows:
            urls = [str(r.get("url") or "") for r in self._uv_yt_entry_rows]
            urls = [u for u in urls if u]
        else:
            sel = tr.selection()
            if not sel:
                messagebox.showwarning("Tải YouTube", "Chọn ít nhất một dòng trong bảng.", parent=self._top)
                return
            idxs = sorted({int(i) for i in sel if str(i).isdigit()})
            urls = [str(self._uv_yt_entry_rows[i].get("url") or "") for i in idxs if 0 <= i < len(self._uv_yt_entry_rows)]
            urls = [u for u in urls if u]
        if not urls:
            messagebox.showwarning("Tải YouTube", "Không lấy được URL từ lựa chọn.", parent=self._top)
            return
        self._run_uv_yt_channel_download_batch(urls)

    def _run_uv_yt_channel_download_batch(self, urls: list[str]) -> None:
        down = self._uv_require_downloader(fail_title="Tải YouTube")
        if down is None:
            return
        urls = self._uv_unique_nonempty_urls(urls)
        if not urls:
            messagebox.showwarning("Tải YouTube", "Danh sách URL YouTube rỗng hoặc trùng lặp.", parent=self._top)
            return
        self._persist_uv_last_job_name(interactive=False)
        try:
            opts = self._yt_channel_download_opts()
        except (ValueError, tk.TclError, TypeError) as exc:  # noqa: BLE001
            messagebox.showerror("Tải YouTube", f"Tùy chọn không hợp lệ: {exc}", parent=self._top)
            return
        n = len(urls)
        self._uv_set_busy(True, f"Chuẩn bị tải {n} video YouTube bằng yt-dlp…")

        def _batch() -> None:
            jid = ""
            try:
                st = down.get_ytdlp_status()
                if not st.get("ok"):

                    def _bad() -> None:
                        self._uv_set_busy(False)
                        self._apply_ytdlp_status_to_var(st)
                        messagebox.showerror(
                            "Tải YouTube",
                            f"yt-dlp chưa chạy được: {st.get('message', '')}",
                            parent=self._top,
                        )

                    self._top.after(0, _bad)
                    return
                down.clear_cancel()
                root = self._uv_batch_job_source_url(urls)
                if not root:

                    def _no_root() -> None:
                        self._uv_set_busy(False)
                        messagebox.showwarning(
                            "Tải YouTube",
                            "Thiếu URL nguồn — dán URL kênh / tab Shorts / playlist ở ô URL phía trên.",
                            parent=self._top,
                        )

                    self._top.after(0, _no_root)
                    return
                job = down.create_download_job(root, opts)
                jid = str(job.get("id") or "")
                self._top.after(0, lambda j=jid: setattr(self, "_last_download_job_id", j))
                failed_urls: list[str] = []
                cancelled = False
                if down.is_cancel_requested():
                    cancelled = True
                else:

                    use_seq = n > UV_DOWNLOAD_SEQUENTIAL_THRESHOLD

                    def _pulse_yt() -> None:
                        if use_seq:
                            self._var_uv_operation_status.set(f"Đang tải tuần tự {n} video YouTube (1/{n})…")
                            self._append_uv_log(f"[INFO] Bắt đầu tải tuần tự {n} URL YouTube…")
                        else:
                            self._var_uv_operation_status.set(f"Đang tải batch {n} video YouTube (yt-dlp -a)…")
                            self._append_uv_log(f"[INFO] Bắt đầu batch {n} URL YouTube (1× yt-dlp -a …)")

                    self._top.after(0, _pulse_yt)
                    ph = self._uv_download_progress_hook()

                    def _item_done_yt(idx: int, total: int, _url: str) -> None:
                        if idx == 1 or idx % 8 == 0 or idx == total:
                            self._top.after(
                                0,
                                lambda i=idx, t=total: self._var_uv_operation_status.set(
                                    f"Đang tải YouTube {i}/{t}…"
                                ),
                            )

                    try:
                        if use_seq:
                            jcur = down.run_download_urls_sequential_for_job(
                                jid, urls, on_progress=ph, on_item_done=_item_done_yt
                            ) or {}
                        else:
                            jcur = down.run_download_urls_batch_for_job(jid, urls, on_progress=ph) or {}
                        for it in jcur.get("failed_items") or []:
                            uu = str(it.get("url") or "").strip()
                            ee = str(it.get("error") or "")
                            if uu:
                                failed_urls.append(uu)
                                self._top.after(0, lambda a=uu, b=ee: self._append_uv_log(f"[FAILED] {a} | {b}"))
                    except Exception as exc:  # noqa: BLE001
                        self._top.after(0, lambda e=exc: self._append_uv_log(f"[ERROR] {e}"))
                jdone = down.finalize_batch_download_job(jid) if jid else {}
                n_ok = len(jdone.get("downloaded_files") or [])
                n_fail = len(jdone.get("failed_items") or [])
                self._top.after(
                    0,
                    lambda ok=n_ok, ff=n_fail: self._append_uv_log(
                        f"[INFO] Tổng kết job: thành công {ok} video, lỗi {ff}."
                    ),
                )
                self._top.after(
                    0,
                    lambda ok=n_ok, ff=n_fail, tot=n: self._var_uv_operation_status.set(
                        f"Hoàn tất tải: {ok}/{tot} video OK, {ff} lỗi."
                    ),
                )

                def _done(cancelled_run: bool, ok_count: int, fail_count: int, job_done: dict[str, Any]) -> None:
                    self._uv_set_busy(False)
                    self._refresh_uv_library()
                    self._show_uv_list_download_done(
                        title="Tải YouTube",
                        platform_key="youtube",
                        job_id=jid,
                        total=n,
                        ok_count=ok_count,
                        fail_count=fail_count,
                        jdone=job_done,
                        cancelled=cancelled_run,
                        item_label="video",
                    )

                self._top.after(0, lambda c=cancelled, ok=n_ok, ff=n_fail, jd=jdone: _done(c, ok, ff, jd))
            except Exception as exc:  # noqa: BLE001
                if jid:
                    try:
                        down.finalize_batch_download_job(jid)
                    except Exception:
                        pass
                self._top.after(0, self._uv_set_busy, False)
                self._top.after(0, self._refresh_uv_library)
                self._top.after(0, lambda e=exc: messagebox.showerror("Tải YouTube", str(e), parent=self._top))

        threading.Thread(target=_batch, daemon=True, name="uv_yt_channel_batch").start()

    def _parse_tt_list_max(self) -> int:
        try:
            lim = int(self._var_uv_tt_list_max.get().strip())
        except ValueError:
            lim = 100
        return max(1, min(UV_CHANNEL_LIST_MAX, lim))

    def _refresh_tt_channel_tree(self, rows: list[dict[str, str]], *, append_from: int = 0) -> None:
        self._uv_tt_entry_rows = list(rows)
        tr = self._tree_tt_channel
        if tr is None:
            return
        if append_from > 0 and append_from <= len(rows):
            specs = [
                {
                    "iid": str(i),
                    "values": (str(i + 1), str(rows[i].get("title") or ""), str(rows[i].get("url") or "")),
                }
                for i in range(append_from, len(rows))
            ]
            if specs:
                self._uv_tree_append_specs_chunked(tr, specs, gen_attr="_uv_tt_tree_gen")
            return
        gen = self._uv_tree_gen_bump("_uv_tt_tree_gen")
        tree_delete_all(tr)
        if not rows:
            self._sync_uv_download_scrollregion(scroll_to_content=False)
            return
        specs = [
            {
                "iid": str(i),
                "values": (str(i + 1), str(r.get("title") or ""), str(r.get("url") or "")),
            }
            for i, r in enumerate(rows)
        ]

        def _done() -> None:
            if self._uv_tree_gen_is_current("_uv_tt_tree_gen", gen):
                self._sync_uv_download_scrollregion(scroll_to_content=True)

        tree_insert_chunked(
            self._top,
            tr,
            specs,
            generation=gen,
            is_current=lambda g: self._uv_tree_gen_is_current("_uv_tt_tree_gen", g),
            on_complete=_done,
            chunk=DEFAULT_TREE_CHUNK,
        )

    def _on_uv_tt_select_all(self) -> None:
        tr = self._tree_tt_channel
        if not tr:
            return
        n = len(self._uv_tt_entry_rows)
        if n > UV_LOGICAL_SELECT_ALL_THRESHOLD:
            self._uv_tt_logical_select_all = True
            tr.selection_remove(tr.selection())
            self._var_uv_tt_scan_status.set(f"Đã chọn logic {n} video — bấm «Tải TikTok đã chọn».")
            return
        self._uv_tt_logical_select_all = False
        children = tr.get_children()
        if len(children) > DEFAULT_TREE_SELECT_CHUNK:
            tree_select_all_chunked(self._top, tr, chunk=DEFAULT_TREE_SELECT_CHUNK)
        else:
            tr.selection_set(children)

    def _on_uv_tt_select_none(self) -> None:
        tr = self._tree_tt_channel
        if not tr:
            return
        self._uv_tt_logical_select_all = False
        tr.selection_remove(tr.selection())

    def _on_uv_scan_tt_channel(self) -> None:
        raw = self._var_uv_url.get().strip()
        if not raw:
            messagebox.showwarning("Quét TikTok", "Nhập URL profile TikTok ở ô URL phía trên.", parent=self._top)
            return
        if detect_platform(raw) != "tiktok":
            messagebox.showwarning(
                "Quét TikTok",
                "Cần URL TikTok profile, ví dụ: https://www.tiktok.com/@username",
                parent=self._top,
            )
            return
        ut = classify_url_type(raw)
        if ut != "profile":
            messagebox.showwarning(
                "Quét TikTok",
                "URL hiện tại không phải profile TikTok.\nVí dụ: https://www.tiktok.com/@username",
                parent=self._top,
            )
            return
        lim = self._parse_tt_list_max()
        self._var_uv_tt_list_max.set(str(lim))
        down = self._uv_require_downloader(fail_title="Quét TikTok")
        if down is None:
            return
        self._uv_tt_logical_select_all = False
        self._uv_set_busy(True, f"Đang quét danh sách TikTok (tối đa {lim} video, yt-dlp)…")
        self._refresh_tt_channel_tree([])
        self._var_uv_tt_scan_status.set("Đang gọi yt-dlp --flat-playlist…")
        last_ui_count = {"n": 0}

        def _partial(rows: list[dict[str, str]]) -> None:
            snap = list(rows)
            now = time.monotonic()
            grow = len(snap) - int(last_ui_count["n"])
            if grow < 3 and now - self._uv_last_partial_ui_ts < 0.55:
                return
            self._uv_last_partial_ui_ts = now
            prev = int(last_ui_count["n"])
            last_ui_count["n"] = len(snap)

            def _apply() -> None:
                if prev <= 0:
                    self._refresh_tt_channel_tree(snap)
                else:
                    self._refresh_tt_channel_tree(snap, append_from=prev)
                self._var_uv_tt_scan_status.set(f"Đang quét… đã thấy {len(snap)} video TikTok.")

            self._top.after(0, _apply)

        def _work() -> None:
            res = down.list_flat_playlist_entries(raw, max_entries=lim, on_partial=_partial)

            def _ui() -> None:
                self._uv_set_busy(False)
                if res.get("success"):
                    entries = res.get("entries") or []
                    rows = [e for e in entries if isinstance(e, dict) and str(e.get("url") or "").strip()]
                    warn = str(res.get("warning") or "").strip()
                    partial = bool(res.get("partial"))
                    status = f"Đã quét {len(rows)} video TikTok."
                    if partial and warn:
                        status += f" ({warn})"
                    self._uv_finish_scan_tree_refresh(
                        rows_or_urls=rows,
                        refresh_fn=self._refresh_tt_channel_tree,
                        backing_count=lambda: len(self._uv_tt_entry_rows),
                        status_setter=self._var_uv_tt_scan_status.set,
                        status_text=status,
                        sync_backing=lambda r: setattr(self, "_uv_tt_entry_rows", list(r)),
                    )
                    messagebox.showinfo(
                        "Quét TikTok",
                        (
                            f"{len(rows)} video trong profile.\n"
                            + ("(Quét một phần do mạng chậm, bạn vẫn có thể tải các video đã hiện.)\n" if partial else "")
                            + "«Tải hết danh sách» hoặc chọn dòng → «Tải TikTok đã chọn»."
                        ),
                        parent=self._top,
                    )
                else:
                    self._var_uv_tt_scan_status.set(str(res.get("error") or "Lỗi"))
                    messagebox.showerror("Quét TikTok", str(res.get("error") or "Thất bại."), parent=self._top)

            self._top.after(0, _ui)

        threading.Thread(target=_work, daemon=True, name="uv_scan_tt_channel").start()

    def _on_uv_download_tt_all(self) -> None:
        n = len(self._uv_tt_entry_rows)
        if not self._uv_confirm_download_all("Tải hết TikTok", n, item_label="video"):
            return
        self._uv_tt_logical_select_all = True
        urls = [str(r.get("url") or "") for r in self._uv_tt_entry_rows]
        urls = [u for u in urls if u]
        if not urls:
            messagebox.showwarning("Tải hết TikTok", "Danh sách không có URL hợp lệ.", parent=self._top)
            return
        self._run_uv_tt_channel_download_batch(urls)

    def _on_uv_download_tt_selected(self) -> None:
        tr = self._tree_tt_channel
        if not tr or not self._uv_tt_entry_rows:
            messagebox.showwarning("Tải TikTok", "Chưa có danh sách — hãy «Quét kênh TikTok (yt-dlp)» trước.", parent=self._top)
            return
        if self._uv_tt_logical_select_all and self._uv_tt_entry_rows:
            urls = [str(r.get("url") or "") for r in self._uv_tt_entry_rows]
            urls = [u for u in urls if u]
        else:
            sel = tr.selection()
            if not sel:
                messagebox.showwarning("Tải TikTok", "Chọn ít nhất một dòng trong bảng.", parent=self._top)
                return
            idxs = sorted({int(i) for i in sel if str(i).isdigit()})
            urls = [str(self._uv_tt_entry_rows[i].get("url") or "") for i in idxs if 0 <= i < len(self._uv_tt_entry_rows)]
            urls = [u for u in urls if u]
        if not urls:
            messagebox.showwarning("Tải TikTok", "Không lấy được URL từ lựa chọn.", parent=self._top)
            return
        self._run_uv_tt_channel_download_batch(urls)

    def _run_uv_tt_channel_download_batch(self, urls: list[str]) -> None:
        down = self._uv_require_downloader(fail_title="Tải TikTok")
        if down is None:
            return
        urls = self._uv_unique_nonempty_urls(urls)
        if not urls:
            messagebox.showwarning("Tải TikTok", "Danh sách URL TikTok rỗng hoặc trùng lặp.", parent=self._top)
            return
        self._persist_uv_last_job_name(interactive=False)
        try:
            opts = self._uv_options_dict()
        except (ValueError, tk.TclError, TypeError) as exc:  # noqa: BLE001
            messagebox.showerror("Tải TikTok", f"Tùy chọn không hợp lệ: {exc}", parent=self._top)
            return
        opts["platform"] = "tiktok"
        opts["url_type"] = "single_video"
        opts["max_videos"] = 1
        n = len(urls)
        self._uv_set_busy(True, f"Chuẩn bị tải {n} video TikTok bằng yt-dlp…")

        def _batch() -> None:
            jid = ""
            try:
                st = down.get_ytdlp_status()
                if not st.get("ok"):

                    def _bad() -> None:
                        self._uv_set_busy(False)
                        self._apply_ytdlp_status_to_var(st)
                        messagebox.showerror("Tải TikTok", f"yt-dlp chưa chạy được: {st.get('message', '')}", parent=self._top)

                    self._top.after(0, _bad)
                    return
                down.clear_cancel()
                root = self._uv_batch_job_source_url(urls)
                if not root:

                    def _no_root() -> None:
                        self._uv_set_busy(False)
                        messagebox.showwarning(
                            "Tải TikTok",
                            "Thiếu URL nguồn — dán URL profile TikTok ở ô URL phía trên.",
                            parent=self._top,
                        )

                    self._top.after(0, _no_root)
                    return
                job = down.create_download_job(root, opts)
                jid = str(job.get("id") or "")
                self._top.after(0, lambda j=jid: setattr(self, "_last_download_job_id", j))
                failed_urls: list[str] = []
                cancelled = False
                if down.is_cancel_requested():
                    cancelled = True
                else:

                    use_seq = n > UV_DOWNLOAD_SEQUENTIAL_THRESHOLD

                    def _pulse_tt() -> None:
                        if use_seq:
                            self._var_uv_operation_status.set(f"Đang tải tuần tự {n} video TikTok (1/{n})…")
                            self._append_uv_log(f"[INFO] Bắt đầu tải tuần tự {n} URL TikTok…")
                        else:
                            self._var_uv_operation_status.set(f"Đang tải batch {n} video TikTok (yt-dlp -a)…")
                            self._append_uv_log(f"[INFO] Bắt đầu batch {n} URL TikTok (1× yt-dlp -a …)")

                    self._top.after(0, _pulse_tt)
                    ph = self._uv_download_progress_hook()

                    def _item_done_tt(idx: int, total: int, _url: str) -> None:
                        if idx == 1 or idx % 8 == 0 or idx == total:
                            self._top.after(
                                0,
                                lambda i=idx, t=total: self._var_uv_operation_status.set(
                                    f"Đang tải TikTok {i}/{t}…"
                                ),
                            )

                    try:
                        if use_seq:
                            jcur = down.run_download_urls_sequential_for_job(
                                jid, urls, on_progress=ph, on_item_done=_item_done_tt
                            ) or {}
                        else:
                            jcur = down.run_download_urls_batch_for_job(jid, urls, on_progress=ph) or {}
                        for it in jcur.get("failed_items") or []:
                            uu = str(it.get("url") or "").strip()
                            ee = str(it.get("error") or "")
                            if uu:
                                failed_urls.append(uu)
                                self._top.after(0, lambda a=uu, b=ee: self._append_uv_log(f"[FAILED] {a} | {b}"))
                    except Exception as exc:  # noqa: BLE001
                        self._top.after(0, lambda e=exc: self._append_uv_log(f"[ERROR] {e}"))
                jdone = down.finalize_batch_download_job(jid) if jid else {}
                n_ok = len(jdone.get("downloaded_files") or [])
                n_fail = len(jdone.get("failed_items") or [])
                self._top.after(
                    0,
                    lambda ok=n_ok, ff=n_fail: self._append_uv_log(
                        f"[INFO] Tổng kết job: thành công {ok} video, lỗi {ff}."
                    ),
                )
                self._top.after(
                    0,
                    lambda ok=n_ok, ff=n_fail, tot=n: self._var_uv_operation_status.set(
                        f"Hoàn tất tải: {ok}/{tot} video OK, {ff} lỗi."
                    ),
                )

                def _done(cancelled_run: bool, ok_count: int, fail_count: int, job_done: dict[str, Any]) -> None:
                    self._uv_set_busy(False)
                    self._refresh_uv_library()
                    self._show_uv_list_download_done(
                        title="Tải TikTok",
                        platform_key="tiktok",
                        job_id=jid,
                        total=n,
                        ok_count=ok_count,
                        fail_count=fail_count,
                        jdone=job_done,
                        cancelled=cancelled_run,
                        item_label="video",
                    )

                self._top.after(0, lambda c=cancelled, ok=n_ok, ff=n_fail, jd=jdone: _done(c, ok, ff, jd))
            except Exception as exc:  # noqa: BLE001
                if jid:
                    try:
                        down.finalize_batch_download_job(jid)
                    except Exception:
                        pass
                self._top.after(0, self._uv_set_busy, False)
                self._top.after(0, self._refresh_uv_library)
                self._top.after(0, lambda e=exc: messagebox.showerror("Tải TikTok", str(e), parent=self._top))

        threading.Thread(target=_batch, daemon=True, name="uv_tt_channel_batch").start()

    def _parse_ig_list_max(self) -> int:
        try:
            lim = int(self._var_uv_ig_list_max.get().strip())
        except ValueError:
            lim = 100
        return max(1, min(UV_CHANNEL_LIST_MAX, lim))

    def _refresh_ig_channel_tree(self, rows: list[dict[str, str]], *, append_from: int = 0) -> None:
        self._uv_ig_entry_rows = list(rows)
        tr = self._tree_ig_channel
        if tr is None:
            return
        if append_from > 0 and append_from <= len(rows):
            specs = [
                {
                    "iid": str(i),
                    "values": (str(i + 1), str(rows[i].get("title") or ""), str(rows[i].get("url") or "")),
                }
                for i in range(append_from, len(rows))
            ]
            if specs:
                self._uv_tree_append_specs_chunked(tr, specs, gen_attr="_uv_ig_tree_gen")
            return
        gen = self._uv_tree_gen_bump("_uv_ig_tree_gen")
        tree_delete_all(tr)
        if not rows:
            self._sync_uv_download_scrollregion(scroll_to_content=False)
            return
        specs = [
            {
                "iid": str(i),
                "values": (str(i + 1), str(r.get("title") or ""), str(r.get("url") or "")),
            }
            for i, r in enumerate(rows)
        ]

        def _done() -> None:
            if self._uv_tree_gen_is_current("_uv_ig_tree_gen", gen):
                self._sync_uv_download_scrollregion(scroll_to_content=True)

        tree_insert_chunked(
            self._top,
            tr,
            specs,
            generation=gen,
            is_current=lambda g: self._uv_tree_gen_is_current("_uv_ig_tree_gen", g),
            on_complete=_done,
            chunk=DEFAULT_TREE_CHUNK,
        )

    def _on_uv_ig_select_all(self) -> None:
        tr = self._tree_ig_channel
        if not tr:
            return
        n = len(self._uv_ig_entry_rows)
        if n > UV_LOGICAL_SELECT_ALL_THRESHOLD:
            self._uv_ig_logical_select_all = True
            tr.selection_remove(tr.selection())
            self._var_uv_ig_scan_status.set(f"Đã chọn logic {n} video — bấm «Tải Instagram đã chọn».")
            return
        self._uv_ig_logical_select_all = False
        children = tr.get_children()
        if len(children) > DEFAULT_TREE_SELECT_CHUNK:
            tree_select_all_chunked(self._top, tr, chunk=DEFAULT_TREE_SELECT_CHUNK)
        else:
            tr.selection_set(children)

    def _ig_urls_to_rows(self, urls: list[str]) -> list[dict[str, str]]:
        return [{"title": f"Reel {i + 1}", "url": u} for i, u in enumerate(urls) if str(u or "").strip()]

    def _on_uv_scan_ig_channel(self) -> None:
        raw = self._var_uv_url.get().strip()
        if not raw:
            messagebox.showwarning("Quét Instagram", "Nhập URL profile Instagram ở ô URL phía trên.", parent=self._top)
            return
        if detect_platform(raw) != "instagram":
            messagebox.showwarning(
                "Quét Instagram",
                "Cần URL Instagram profile hoặc tab Reels.\n"
                "Ví dụ: https://www.instagram.com/username/ hoặc …/username/reels/",
                parent=self._top,
            )
            return
        ut = classify_url_type(raw)
        if ut != "profile":
            messagebox.showwarning(
                "Quét Instagram",
                "URL hiện tại không phải profile/tab Reels.\n"
                "Reel/post đơn: dùng «Tải URL hiện tại».\n"
                "Profile: https://www.instagram.com/username/",
                parent=self._top,
            )
            return
        page_url = normalize_instagram_reels_tab_url(raw)
        self._var_uv_url.set(page_url)
        max_reels, max_scroll, max_minutes, till_end = self._parse_ig_reel_limits()
        self._var_uv_ig_list_max.set(str(max_reels))
        self._var_uv_ig_max_scroll.set(str(max_scroll))
        self._var_uv_ig_scan_minutes.set(str(max_minutes))
        self._var_uv_ig_scroll_until_end.set(till_end)
        try:
            persist_instagram_reels_settings(
                max_collect=max_reels,
                max_scroll_rounds=max_scroll,
                max_scan_minutes=max_minutes,
                scroll_until_end=till_end,
            )
        except OSError:
            pass
        mode_txt = "cuộn tới hết trang" if till_end else "dừng theo vòng cuộn"
        self._uv_ig_logical_select_all = False
        self._uv_set_busy(
            True,
            f"Đang mở Playwright quét tab Reels Instagram ({mode_txt}, tối đa {max_minutes} phút)…",
        )
        self._refresh_ig_channel_tree([])
        self._var_uv_ig_scan_status.set("Đang quét — bảng «URL reel» sẽ hiện dần…")
        last_ui_count = {"n": 0}

        def _status(msg: str) -> None:
            self._top.after(0, lambda m=msg: self._var_uv_ig_scan_status.set(m))

        def _partial(urls: list[str]) -> None:
            snap = self._ig_urls_to_rows(urls)
            now = time.monotonic()
            grow = len(snap) - int(last_ui_count["n"])
            if grow < 2 and now - self._uv_last_partial_ui_ts < 0.35:
                return
            self._uv_last_partial_ui_ts = now
            prev = int(last_ui_count["n"])
            last_ui_count["n"] = len(snap)

            def _apply() -> None:
                if prev <= 0:
                    self._refresh_ig_channel_tree(snap)
                else:
                    self._refresh_ig_channel_tree(snap, append_from=prev)
                if snap:
                    self._var_uv_ig_scan_status.set(
                        f"Đang quét… đã thấy {len(snap)} reel (cập nhật trực tiếp trong bảng)."
                    )

            self._top.after(0, _apply)

        def _work() -> None:
            show_browser = bool(self._var_uv_ig_show_browser.get())
            cookie_path = self._resolve_ig_cookie_path()
            res = scan_instagram_profile_reels_page(
                page_url=page_url,
                cookie_path=cookie_path,
                max_reels=max_reels,
                max_scroll_rounds=max_scroll,
                max_scan_minutes=max_minutes,
                scroll_until_end=till_end,
                headless=not show_browser,
                status=_status,
                on_partial=_partial,
            )

            def _ui() -> None:
                self._uv_set_busy(False)
                if res.get("ok"):
                    items = res.get("items") or []
                    urls = [str(x.get("url") or "") for x in items if isinstance(x, dict)]
                    rows = self._ig_urls_to_rows(urls)
                    self._uv_finish_scan_tree_refresh(
                        rows_or_urls=rows,
                        refresh_fn=self._refresh_ig_channel_tree,
                        backing_count=lambda: len(self._uv_ig_entry_rows),
                        status_setter=self._var_uv_ig_scan_status.set,
                        status_text=res.get("message") or f"{len(rows)} reel.",
                        sync_backing=lambda r: setattr(self, "_uv_ig_entry_rows", list(r)),
                    )
                    messagebox.showinfo(
                        "Quét Instagram",
                        f"{res.get('message', '')}\n\n«Tải hết danh sách» hoặc chọn dòng → «Tải Instagram đã chọn».",
                        parent=self._top,
                    )
                else:
                    self._var_uv_ig_scan_status.set(str(res.get("message") or "Lỗi"))
                    messagebox.showerror("Quét Instagram", str(res.get("message") or "Thất bại."), parent=self._top)

            self._top.after(0, _ui)

        threading.Thread(target=_work, daemon=True, name="uv_scan_ig_channel").start()

    def _on_uv_download_ig_all(self) -> None:
        n = len(self._uv_ig_entry_rows)
        if not self._uv_confirm_download_all("Tải hết Instagram", n, item_label="video"):
            return
        self._uv_ig_logical_select_all = True
        urls = [str(r.get("url") or "") for r in self._uv_ig_entry_rows]
        urls = [u for u in urls if u]
        if not urls:
            messagebox.showwarning("Tải hết Instagram", "Danh sách không có URL hợp lệ.", parent=self._top)
            return
        self._run_uv_ig_channel_download_batch(urls)

    def _on_uv_download_ig_selected(self) -> None:
        tr = self._tree_ig_channel
        if not tr or not self._uv_ig_entry_rows:
            messagebox.showwarning(
                "Tải Instagram",
                "Chưa có danh sách — hãy «Quét Instagram» trước.",
                parent=self._top,
            )
            return
        if self._uv_ig_logical_select_all and self._uv_ig_entry_rows:
            urls = [str(r.get("url") or "") for r in self._uv_ig_entry_rows]
            urls = [u for u in urls if u]
        else:
            sel = tr.selection()
            if not sel:
                messagebox.showwarning("Tải Instagram", "Chọn ít nhất một dòng trong bảng.", parent=self._top)
                return
            idxs = sorted({int(i) for i in sel if str(i).isdigit()})
            urls = [str(self._uv_ig_entry_rows[i].get("url") or "") for i in idxs if 0 <= i < len(self._uv_ig_entry_rows)]
            urls = [u for u in urls if u]
        if not urls:
            messagebox.showwarning("Tải Instagram", "Không lấy được URL từ lựa chọn.", parent=self._top)
            return
        self._run_uv_ig_channel_download_batch(urls)

    def _run_uv_ig_channel_download_batch(self, urls: list[str]) -> None:
        down = self._uv_require_downloader(fail_title="Tải Instagram")
        if down is None:
            return
        urls = self._uv_unique_nonempty_urls(urls)
        if not urls:
            messagebox.showwarning("Tải Instagram", "Danh sách URL rỗng hoặc trùng lặp.", parent=self._top)
            return
        self._persist_uv_last_job_name(interactive=False)
        try:
            opts = self._uv_options_dict()
        except (ValueError, tk.TclError, TypeError) as exc:  # noqa: BLE001
            messagebox.showerror("Tải Instagram", f"Tùy chọn không hợp lệ: {exc}", parent=self._top)
            return
        opts["platform"] = "instagram"
        opts["url_type"] = "single_video"
        opts["max_videos"] = 1
        n = len(urls)
        self._uv_set_busy(True, f"Chuẩn bị tải {n} video Instagram bằng yt-dlp…")

        def _batch() -> None:
            jid = ""
            try:
                st = down.get_ytdlp_status()
                if not st.get("ok"):

                    def _bad() -> None:
                        self._uv_set_busy(False)
                        self._apply_ytdlp_status_to_var(st)
                        messagebox.showerror(
                            "Tải Instagram",
                            f"yt-dlp chưa chạy được: {st.get('message', '')}",
                            parent=self._top,
                        )

                    self._top.after(0, _bad)
                    return
                down.clear_cancel()
                root = self._uv_batch_job_source_url(urls)
                if not root:

                    def _no_root() -> None:
                        self._uv_set_busy(False)
                        messagebox.showwarning(
                            "Tải Instagram",
                            "Thiếu URL nguồn — dán URL profile Instagram ở ô URL phía trên.",
                            parent=self._top,
                        )

                    self._top.after(0, _no_root)
                    return
                job = down.create_download_job(root, opts)
                jid = str(job.get("id") or "")
                self._top.after(0, lambda j=jid: setattr(self, "_last_download_job_id", j))
                cancelled = False
                if down.is_cancel_requested():
                    cancelled = True
                else:
                    use_seq = n > UV_DOWNLOAD_SEQUENTIAL_THRESHOLD

                    def _pulse_ig() -> None:
                        if use_seq:
                            self._var_uv_operation_status.set(f"Đang tải tuần tự {n} video Instagram (1/{n})…")
                            self._append_uv_log(f"[INFO] Bắt đầu tải tuần tự {n} URL Instagram…")
                        else:
                            self._var_uv_operation_status.set(f"Đang tải batch {n} video Instagram (yt-dlp -a)…")
                            self._append_uv_log(f"[INFO] Bắt đầu batch {n} URL Instagram (1× yt-dlp -a …)")

                    self._top.after(0, _pulse_ig)
                    ph = self._uv_download_progress_hook()

                    def _item_done_ig(idx: int, total: int, _url: str) -> None:
                        if idx == 1 or idx % 8 == 0 or idx == total:
                            self._top.after(
                                0,
                                lambda i=idx, t=total: self._var_uv_operation_status.set(
                                    f"Đang tải Instagram {i}/{t}…"
                                ),
                            )

                    try:
                        if use_seq:
                            jcur = down.run_download_urls_sequential_for_job(
                                jid, urls, on_progress=ph, on_item_done=_item_done_ig
                            ) or {}
                        else:
                            jcur = down.run_download_urls_batch_for_job(jid, urls, on_progress=ph) or {}
                        for it in jcur.get("failed_items") or []:
                            uu = str(it.get("url") or "").strip()
                            ee = str(it.get("error") or "")
                            if uu:
                                self._top.after(0, lambda a=uu, b=ee: self._append_uv_log(f"[FAILED] {a} | {b}"))
                    except Exception as exc:  # noqa: BLE001
                        self._top.after(0, lambda e=exc: self._append_uv_log(f"[ERROR] {e}"))
                jdone = down.finalize_batch_download_job(jid) if jid else {}
                n_ok = len(jdone.get("downloaded_files") or [])
                n_fail = len(jdone.get("failed_items") or [])
                self._top.after(
                    0,
                    lambda ok=n_ok, ff=n_fail: self._append_uv_log(
                        f"[INFO] Tổng kết job: thành công {ok} video, lỗi {ff}."
                    ),
                )
                self._top.after(
                    0,
                    lambda ok=n_ok, ff=n_fail, tot=n: self._var_uv_operation_status.set(
                        f"Hoàn tất tải: {ok}/{tot} video OK, {ff} lỗi."
                    ),
                )

                def _done(cancelled_run: bool, ok_count: int, fail_count: int, job_done: dict[str, Any]) -> None:
                    self._uv_set_busy(False)
                    self._refresh_uv_library()
                    self._show_uv_list_download_done(
                        title="Tải Instagram",
                        platform_key="instagram",
                        job_id=jid,
                        total=n,
                        ok_count=ok_count,
                        fail_count=fail_count,
                        jdone=job_done,
                        cancelled=cancelled_run,
                        item_label="video",
                    )

                self._top.after(0, lambda c=cancelled, ok=n_ok, ff=n_fail, jd=jdone: _done(c, ok, ff, jd))
            except Exception as exc:  # noqa: BLE001
                if jid:
                    try:
                        down.finalize_batch_download_job(jid)
                    except Exception:
                        pass
                self._top.after(0, self._uv_set_busy, False)
                self._top.after(0, self._refresh_uv_library)
                self._top.after(0, lambda e=exc: messagebox.showerror("Tải Instagram", str(e), parent=self._top))

        threading.Thread(target=_batch, daemon=True, name="uv_ig_channel_batch").start()

    def _on_uv_check_url(self) -> None:
        url = self._var_uv_url.get().strip()
        if not url:
            messagebox.showwarning("Tải video", "Nhập URL trước.", parent=self._top)
            return
        down = self._uv_require_downloader(fail_title="Tải video")
        if down is None:
            return
        self._uv_set_busy(
            True,
            "Đang quét URL bằng yt-dlp (playlist/kênh có thể mất vài chục giây — cửa sổ vẫn phản hồi)…",
        )

        def _job() -> None:
            st = down.get_ytdlp_status()
            if not st.get("ok"):

                def _bad() -> None:
                    self._uv_set_busy(False)
                    self._apply_ytdlp_status_to_var(st)
                    messagebox.showerror(
                        "Tải video",
                        f"yt-dlp chưa chạy được: {st.get('message', '')}\n\nBấm «Kiểm tra yt-dlp» hoặc xem dòng trạng thái phía trên.",
                        parent=self._top,
                    )

                self._top.after(0, _bad)
                return
            info = down.check_url(url)

            def _ui() -> None:
                self._uv_set_busy(False)
                if info.get("success"):
                    messagebox.showinfo(
                        "Kiểm tra URL",
                        (
                            f"Tiêu đề: {info.get('title') or '-'}\n"
                            f"Extractor: {info.get('extractor')}\n"
                            f"Uploader: {info.get('uploader')}\n"
                            f"Số entry (ước lượng): {info.get('entry_count')}\n"
                            f"Loại URL (auto): {info.get('url_type')}"
                        ),
                        parent=self._top,
                    )
                else:
                    messagebox.showerror("Kiểm tra URL", str(info.get("error") or "unknown"), parent=self._top)

            self._top.after(0, _ui)

        threading.Thread(target=_job, daemon=True, name="uv_check_url").start()

    def _on_uv_download(self) -> None:
        down = self._uv_require_downloader(fail_title="Tải video")
        if down is None:
            return
        url = self._var_uv_url.get().strip()
        if not url:
            messagebox.showwarning("Tải video", "Nhập URL.", parent=self._top)
            return
        self._persist_uv_last_job_name(interactive=False)
        try:
            opts = self._uv_options_dict()
        except (ValueError, tk.TclError, TypeError) as exc:  # noqa: BLE001
            messagebox.showerror("Tải video", f"Tùy chọn không hợp lệ: {exc}", parent=self._top)
            return
        self._uv_set_busy(True, "Đang kiểm tra yt-dlp trước khi tạo job…")

        def _prepare_and_run() -> None:
            st = down.get_ytdlp_status()
            if not st.get("ok"):

                def _bad() -> None:
                    self._uv_set_busy(False)
                    self._apply_ytdlp_status_to_var(st)
                    messagebox.showerror(
                        "Tải video",
                        f"yt-dlp chưa chạy được: {st.get('message', '')}",
                        parent=self._top,
                    )

                self._top.after(0, _bad)
                return
            try:
                job = down.create_download_job(url, opts)
            except Exception as exc:  # noqa: BLE001

                def _err_create(err: Exception = exc) -> None:
                    self._uv_set_busy(False)
                    messagebox.showerror("Tải video", str(err), parent=self._top)

                self._top.after(0, _err_create)
                return

            jid = job["id"]

            def _start_bar() -> None:
                self._last_download_job_id = jid
                self._var_uv_operation_status.set(
                    f"Đang tải job {jid} — xem log bên dưới; có thể lâu nếu nhiều video…"
                )
                self._append_uv_log(f"[INFO] Bắt đầu job {jid} …")

            self._top.after(0, _start_bar)

            try:
                down.clear_cancel()
                ph = self._uv_download_progress_hook()
                done_job = down.run_download_job(jid, on_progress=ph)
                ok_count = len((done_job or {}).get("downloaded_files") or [])
                fail_count = len((done_job or {}).get("failed_items") or [])
                self._top.after(
                    0,
                    lambda j=jid, ok=ok_count, ff=fail_count: self._var_uv_operation_status.set(
                        f"Hoàn tất job {j}: {ok} video OK, {ff} lỗi."
                    ),
                )
                self._top.after(
                    0,
                    lambda j=jid, ok=ok_count, ff=fail_count: self._show_uv_done_with_open_folder(
                        "Tải video",
                        f"Hoàn tất job {j}.\nThành công: {ok} video | Lỗi: {ff}.",
                        job_id=j,
                        ok_count=ok,
                    ),
                )
            except Exception as e:  # noqa: BLE001
                self._top.after(0, lambda err=e: messagebox.showerror("Tải video", str(err), parent=self._top))
            finally:
                self._top.after(0, self._uv_set_busy, False)
                self._top.after(0, self._refresh_uv_library)

        threading.Thread(target=_prepare_and_run, daemon=True, name="uv_download").start()

    def _on_uv_download_tiktok(self) -> None:
        """Shortcut rõ ràng cho người dùng muốn tải TikTok."""
        raw = self._var_uv_url.get().strip()
        if not raw:
            messagebox.showwarning("Tải TikTok", "Nhập URL TikTok trước.", parent=self._top)
            return
        if detect_platform(raw) != "tiktok":
            messagebox.showwarning(
                "Tải TikTok",
                "URL hiện tại không phải TikTok.\nVí dụ: https://www.tiktok.com/@user hoặc .../@user/video/<id>",
                parent=self._top,
            )
            return
        self._var_uv_platform.set("tiktok")
        auto_type = classify_url_type(raw)
        if auto_type == "single_video":
            self._var_uv_url_type.set("Video đơn")
        elif auto_type in ("profile", "playlist"):
            self._var_uv_url_type.set("Danh sách (playlist/profile)")
        else:
            self._var_uv_url_type.set("Tự nhận diện")
        self._on_uv_download()

    def _on_uv_pause(self) -> None:
        if self._uv_downloader:
            self._uv_downloader.cancel_current()
            self._append_uv_log("[INFO] Đã gửi yêu cầu dừng (terminate process nếu đang chạy).")

    def _on_uv_resume(self) -> None:
        if not self._last_download_job_id or not self._uv_downloader:
            messagebox.showwarning("Tải video", "Chưa có job gần nhất. Bấm «Tải video» trước.", parent=self._top)
            return
        jid = self._last_download_job_id
        self._uv_set_busy(True, f"Đang chạy lại job {jid}…")

        def _run() -> None:
            assert self._uv_downloader is not None
            try:
                self._uv_downloader.clear_cancel()
                ph = self._uv_download_progress_hook()
                done = self._uv_downloader.run_download_job(jid, on_progress=ph)
                n_ok = len((done or {}).get("downloaded_files") or [])
                n_ff = len((done or {}).get("failed_items") or [])
                self._top.after(
                    0,
                    lambda j=jid, ok=n_ok, ff=n_ff: messagebox.showinfo(
                        "Tải video",
                        f"Chạy lại job {j} xong — {ok} file trong job, {ff} mục lỗi (trùng archive có thể đã bỏ qua).",
                        parent=self._top,
                    ),
                )
            except Exception as e:  # noqa: BLE001
                self._top.after(0, lambda err=e: messagebox.showerror("Tải video", str(err), parent=self._top))
            finally:
                self._top.after(0, self._uv_set_busy, False)
                self._top.after(0, self._refresh_uv_library)

        threading.Thread(target=_run, daemon=True, name="uv_resume").start()

    def _on_uv_open_out_dir(self) -> None:
        d = self._resolve_uv_target_open_dir()
        try:
            d.mkdir(parents=True, exist_ok=True)
            os.startfile(str(d.resolve()))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Tải video", str(exc), parent=self._top)

    def _on_uv_open_video_editor_with_last_job(self) -> None:
        jid = str(self._last_download_job_id or "").strip()
        if not jid:
            if not messagebox.askyesno(
                "Tải video",
                "Chưa có job gần nhất. Vẫn mở tab Video Editor?",
                parent=self._top,
            ):
                return
        else:
            self._store_pending_job_for_video_editor(jid)
            arm_auto_import_download_job(self._top)
            self._notify_download_job_finished(jid, ok_count=1)
        if self._open_video_editor_tab_if_embedded():
            if jid:
                notify_msg = f"Đã mở Video Editor — job {jid} sẽ tự import vào Media."
                messagebox.showinfo("Tải video", notify_msg, parent=self._top)
        else:
            msg = "Không tự chuyển tab được trong chế độ cửa sổ riêng."
            if jid:
                msg += f"\nĐã lưu job {jid}, vào tab Video Editor bấm «Nạp job tải» để nhận."
            messagebox.showinfo("Tải video", msg, parent=self._top)

    @staticmethod
    def _store_pending_job_for_video_editor(job_id: str) -> None:
        jid = str(job_id or "").strip()
        if not jid:
            return
        write_pending_video_editor_job(jid)

    def _notify_download_job_finished(self, job_id: str, *, ok_count: int = 0) -> None:
        """Đồng bộ tab Video Editor: pending job + làm mới combobox «Job tải»."""
        jid = str(job_id or "").strip()
        if not jid:
            return
        self._last_download_job_id = jid
        if int(ok_count) > 0:
            self._store_pending_job_for_video_editor(jid)
            set_root_pending_download_job(self._top, jid)
        try:
            self._top.event_generate(DOWNLOAD_JOB_FINISHED_TK_EVENT, when="tail")
        except Exception:
            pass

    def _open_video_editor_tab_if_embedded(self) -> bool:
        if self._embedded_download_host is None:
            return False
        cur: tk.Misc | None = self._embedded_download_host
        while cur is not None and not isinstance(cur, ttk.Notebook):
            cur = cur.master
        if not isinstance(cur, ttk.Notebook):
            return False
        try:
            for tab_id in cur.tabs():
                text = str(cur.tab(tab_id, "text") or "")
                if "Video Editor" in text:
                    cur.select(tab_id)
                    return True
        except Exception:
            return False
        return False

    @staticmethod
    def _normalize_uv_source_url(url: str) -> str:
        u = str(url or "").strip()
        if not u:
            return ""
        return u.rstrip("/").lower()

    def _resolve_uv_target_open_dir(self) -> Path:
        # 1) Ưu tiên dòng đang chọn ở thư viện "Video đã tải"
        vid = self._uv_selected_id()
        if vid and self._uv_downloader:
            rec = self._uv_downloader.get_downloaded_video(vid)
            if isinstance(rec, dict):
                p = Path(str(rec.get("video_path") or "")).expanduser()
                if p.is_file():
                    return p.parent
                if p.is_dir():
                    return p

        # 2) Ưu tiên URL đang chọn ở bảng quét theo nền tảng
        candidates: list[str] = []
        tr_fb = self._tree_fb_reels
        if tr_fb:
            sels = tr_fb.selection()
            for iid in sels:
                vals = tr_fb.item(iid, "values") or ()
                if len(vals) >= 2:
                    u = str(vals[1] or "").strip()
                    if u:
                        candidates.append(u)
        tr_yt = self._tree_yt_channel
        if tr_yt and self._uv_yt_entry_rows:
            idxs = [int(i) for i in tr_yt.selection() if str(i).isdigit()]
            for i in idxs:
                if 0 <= i < len(self._uv_yt_entry_rows):
                    u = str(self._uv_yt_entry_rows[i].get("url") or "").strip()
                    if u:
                        candidates.append(u)
        tr_tt = self._tree_tt_channel
        if tr_tt and self._uv_tt_entry_rows:
            idxs = [int(i) for i in tr_tt.selection() if str(i).isdigit()]
            for i in idxs:
                if 0 <= i < len(self._uv_tt_entry_rows):
                    u = str(self._uv_tt_entry_rows[i].get("url") or "").strip()
                    if u:
                        candidates.append(u)

        raw_url = self._var_uv_url.get().strip()
        if raw_url:
            candidates.append(raw_url)

        if self._uv_downloader and candidates:
            rows = self._uv_downloader.list_downloaded_videos()
            norm_cands = [self._normalize_uv_source_url(u) for u in candidates if self._normalize_uv_source_url(u)]
            if norm_cands:
                for r in rows:
                    src = self._normalize_uv_source_url(str(r.get("source_url") or ""))
                    if not src:
                        continue
                    if any(src == cu or src.startswith(cu) or cu.startswith(src) for cu in norm_cands):
                        vp = Path(str(r.get("video_path") or "")).expanduser()
                        if vp.is_file():
                            return vp.parent
                        if vp.is_dir():
                            return vp

        # 3) Fallback: thư mục output mặc định hiện tại
        return Path(self._var_uv_out_dir.get().strip() or ".")

    def _show_uv_done_with_open_folder(
        self,
        title: str,
        summary: str,
        *,
        job_id: str = "",
        ok_count: int = 0,
    ) -> None:
        jid = str(job_id or "").strip()
        if jid and int(ok_count) > 0:
            self._notify_download_job_finished(jid, ok_count=ok_count)
        if self._embedded_download_host is not None and jid and int(ok_count) > 0:
            choice = messagebox.askyesnocancel(
                title,
                f"{summary}\n\nĐã cập nhật bảng «Video đã tải».\n\n"
                "• Có = Mở Video Editor (tự import job vào Media)\n"
                "• Không = Mở thư mục chứa file video\n"
                "• Hủy = Đóng (job vẫn có trong combobox Editor khi bạn mở tab 5)",
                parent=self._top,
            )
            if choice is True:
                arm_auto_import_download_job(self._top)
                self._open_video_editor_tab_if_embedded()
                return
            if choice is False:
                self._on_uv_open_out_dir()
                return
            return
        open_now = messagebox.askyesno(
            title,
            f"{summary}\n\nĐã cập nhật bảng «Video đã tải».\nMở thư mục lưu video ngay?",
            parent=self._top,
        )
        if open_now:
            self._on_uv_open_out_dir()

    def _uv_confirm_download_all(self, title: str, count: int, *, item_label: str = "video") -> bool:
        """Xác nhận tải hết danh sách đã quét (tránh bấm nhầm với list lớn)."""
        if count <= 0:
            messagebox.showwarning(title, "Danh sách trống — hãy «Quét» trước.", parent=self._top)
            return False
        if count > UV_LOGICAL_SELECT_ALL_THRESHOLD:
            return bool(
                messagebox.askyesno(
                    title,
                    f"Tải hết {count} {item_label} trong danh sách?\n\n"
                    "Quá trình có thể mất nhiều giờ. Nên bật «Bỏ qua file đã có» để resume an toàn.\n"
                    "Bạn có thể dùng «Tạm dừng / Hủy» giữa chừng.\n\nTiếp tục?",
                    parent=self._top,
                )
            )
        if count > 30:
            return bool(
                messagebox.askyesno(
                    title,
                    f"Tải hết {count} {item_label} trong danh sách?",
                    parent=self._top,
                )
            )
        return True

    def _uv_failed_log_hint(self, job: dict[str, Any], *, platform_key: str, job_id: str) -> str:
        """Ghi file URL lỗi và trả về dòng gợi ý hiển thị cho user."""
        pairs = extract_failed_download_pairs(job)
        path = write_failed_download_urls_log(
            platform=platform_key,
            job_id=job_id,
            failed_pairs=pairs,
            log_fn=self._append_uv_log,
        )
        if path is None:
            return ""
        return f"\nFile URL lỗi: {path}"

    def _show_uv_list_download_done(
        self,
        *,
        title: str,
        platform_key: str,
        job_id: str,
        total: int,
        ok_count: int,
        fail_count: int,
        jdone: dict[str, Any],
        cancelled: bool,
        item_label: str = "video",
    ) -> None:
        """Thông báo hoàn tất batch quét-list + ghi file .txt nếu có URL lỗi."""
        log_hint = self._uv_failed_log_hint(jdone, platform_key=platform_key, job_id=job_id)
        if cancelled:
            if ok_count > 0:
                self._notify_download_job_finished(job_id, ok_count=ok_count)
            messagebox.showinfo(
                title,
                f"Đã dừng theo «Tạm dừng / Hủy».\n"
                f"Đã tải thành công {ok_count}/{total} {item_label} (lỗi: {fail_count})."
                + log_hint,
                parent=self._top,
            )
            return
        self._show_uv_done_with_open_folder(
            title,
            f"Hoàn tất lệnh tải {total} {item_label} — job ({job_id}).\n"
            f"Thành công: {ok_count} | Lỗi: {fail_count}."
            + log_hint,
            job_id=job_id,
            ok_count=ok_count,
        )

    def warm_embedded_download_panel(self) -> None:
        """Tab Tải Video nhúng trong cửa sổ chính: chạy kiểm tra yt-dlp + làm mới thư viện khi user mở tab."""
        if self._embedded_download_host is None or self._uv_embedded_warm_done:
            return
        self._uv_embedded_warm_done = True
        self._refresh_uv_ytdlp_status()
        self._top.after(80, self._refresh_uv_library)
        self._top.after(120, lambda: self._sync_uv_download_scrollregion(scroll_to_content=False))

    def _refresh_uv_library(self) -> None:
        if self._tree_uv is None or not self._uv_downloader:
            return
        down = self._uv_downloader
        self._uv_lib_refresh_gen = int(getattr(self, "_uv_lib_refresh_gen", 0)) + 1
        refresh_gen = self._uv_lib_refresh_gen

        def _worker() -> list[dict[str, Any]]:
            return down.list_downloaded_videos()

        def _apply(rows: list[dict[str, Any]]) -> None:
            if refresh_gen != self._uv_lib_refresh_gen:
                return
            self._apply_uv_library_rows(rows)

        run_background_then_main(self._top, _worker, _apply)

    def _apply_uv_library_rows(self, rows: list[dict[str, Any]]) -> None:
        if self._tree_uv is None:
            return
        tree_delete_all(self._tree_uv)
        total_ok = len([r for r in rows if str(r.get("video_path") or "").strip()])
        self._refresh_uv_library_job_filter(rows)
        selected_job_filter = str(self._var_uv_lib_job_filter.get() or "").strip()
        shown_ok = 0
        specs: list[dict[str, Any]] = []
        for r in rows:
            vid = str(r.get("id") or "")
            if not vid:
                continue
            display_job = self._uv_display_job_label(r)
            if selected_job_filter and selected_job_filter != "Tất cả job" and display_job != selected_job_filter:
                continue
            shown_ok += 1
            if len(specs) >= UV_LIBRARY_UI_MAX_ROWS:
                continue
            dur = r.get("duration") or 0
            try:
                ds = f"{float(dur):.1f}s"
            except (TypeError, ValueError):
                ds = str(dur)
            specs.append(
                {
                    "iid": vid,
                    "values": (
                        display_job,
                        str(r.get("platform") or ""),
                        str(r.get("title") or "")[:120],
                        self._uv_format_library_hashtags_cell(r),
                        ds,
                        str(r.get("uploader") or "")[:40],
                        str(r.get("status") or ""),
                        str(r.get("video_path") or ""),
                    ),
                }
            )
        inserted = len(specs)
        if specs:
            gen = self._uv_tree_gen_bump("_uv_lib_tree_gen")
            tree_insert_chunked(
                self._top,
                self._tree_uv,
                specs,
                generation=gen,
                is_current=lambda g: self._uv_tree_gen_is_current("_uv_lib_tree_gen", g),
                chunk=DEFAULT_TREE_CHUNK,
            )
        if selected_job_filter and selected_job_filter != "Tất cả job":
            tail = f", đang hiển thị: {inserted}" if shown_ok > inserted else ""
            self._var_uv_lib_total_ok.set(f"Tổng thành công: {total_ok} video (đang lọc: {shown_ok}{tail})")
        else:
            tail = f" (đang hiển thị {inserted}/{shown_ok})" if shown_ok > inserted else ""
            self._var_uv_lib_total_ok.set(f"Tổng thành công: {total_ok} video{tail}")

    def _refresh_uv_library_job_filter(self, rows: list[dict[str, Any]]) -> None:
        if self._cb_uv_lib_job_filter is None:
            return
        vals = ["Tất cả job"]
        seen: set[str] = {"Tất cả job"}
        self._uv_lib_job_ids_by_filter_label = {"Tất cả job": set()}
        for r in rows:
            name = self._uv_display_job_label(r)
            jid = str(r.get("download_job_id") or "").strip()
            if not name:
                continue
            self._uv_lib_job_ids_by_filter_label.setdefault(name, set())
            if jid:
                self._uv_lib_job_ids_by_filter_label[name].add(jid)
            if name in seen:
                continue
            seen.add(name)
            vals.append(name)
        self._cb_uv_lib_job_filter.configure(values=vals)
        cur = str(self._var_uv_lib_job_filter.get() or "").strip()
        if cur not in seen:
            self._var_uv_lib_job_filter.set("Tất cả job")

    @staticmethod
    def _uv_display_job_label(row: dict[str, Any]) -> str:
        jname = str(row.get("download_job_name") or "").strip()
        jid = str(row.get("download_job_id") or "").strip()
        if jname and jid and jname != jid:
            return f"{jname} | {jid}"
        return jname or jid

    @staticmethod
    def _uv_format_library_hashtags_cell(row: dict[str, Any]) -> str:
        parts: list[str] = []
        seen: set[str] = set()

        def push(tag: str) -> None:
            t = str(tag).strip()
            if not t:
                return
            if not t.startswith("#"):
                t = "#" + t.lstrip("#")
            key = t.casefold()
            if key in seen:
                return
            seen.add(key)
            parts.append(t)

        raw = row.get("hashtags")
        if isinstance(raw, str) and raw.strip():
            for piece in re.split(r"[\s,;]+", raw.strip()):
                push(piece)
        elif isinstance(raw, list):
            for x in raw:
                push(str(x))
        for h in _extract_hashtags_from_text(str(row.get("title") or "")):
            push(h)
        joined = " ".join(parts)
        return joined[:240] if len(joined) > 240 else joined

    def _on_uv_library_job_filter_changed(self) -> None:
        self._refresh_uv_library()

    def _on_uv_library_clear_job_filter(self) -> None:
        self._var_uv_lib_job_filter.set("Tất cả job")
        self._refresh_uv_library()

    def _on_uv_delete_filtered_job(self) -> None:
        if self._uv_require_downloader(fail_title="Tải video") is None:
            return
        flt = str(self._var_uv_lib_job_filter.get() or "").strip()
        if not flt or flt == "Tất cả job":
            messagebox.showwarning("Tải video", "Hãy chọn 1 job cụ thể ở bộ lọc trước khi xóa.", parent=self._top)
            return
        job_ids = sorted(self._uv_lib_job_ids_by_filter_label.get(flt) or [])
        if not job_ids:
            messagebox.showwarning("Tải video", "Không tìm được job_id để xóa từ bộ lọc hiện tại.", parent=self._top)
            return
        if len(job_ids) > 1:
            messagebox.showwarning(
                "Tải video",
                "Bộ lọc này đang trùng nhiều job khác nhau. Hãy chọn bộ lọc có job_id cụ thể (dạng «Tên | job_id»).",
                parent=self._top,
            )
            return
        jid = job_ids[0]
        if not messagebox.askyesno(
            "Tải video",
            f"Xóa toàn bộ metadata của job {jid} và danh sách video thuộc job này?",
            parent=self._top,
        ):
            return
        remove_files = messagebox.askyesnocancel(
            "Tải video",
            "Bạn muốn xóa luôn file video/thumbnail/info trên đĩa không?\n\n"
            "Có = xóa metadata + xóa file + dọn folder rỗng\n"
            "Không = chỉ xóa metadata, giữ nguyên file\n"
            "Hủy = không thực hiện",
            parent=self._top,
        )
        if remove_files is None:
            return
        ret = self._uv_downloader.delete_download_job(
            jid,
            delete_files=bool(remove_files),
            prune_empty_dirs=bool(remove_files),
        )
        self._refresh_uv_library()
        self._var_uv_lib_job_filter.set("Tất cả job")
        messagebox.showinfo(
            "Tải video",
            f"Đã xóa job: {'OK' if ret.get('deleted_job') else 'không tìm thấy'}\n"
            f"Số video đã xóa khỏi thư viện: {int(ret.get('deleted_videos') or 0)}\n"
            f"Chế độ xóa file: {'Có' if remove_files else 'Không'}",
            parent=self._top,
        )

    def _uv_selected_id(self) -> str | None:
        if not self._tree_uv:
            return None
        sel = self._tree_uv.selection()
        return str(sel[0]) if sel else None

    def _uv_selected_ids(self) -> list[str]:
        if not self._tree_uv:
            return []
        return [str(x) for x in self._tree_uv.selection() if str(x).strip()]

    def _on_uv_preview_selected(self) -> None:
        vid = self._uv_selected_id()
        if not vid or not self._uv_downloader:
            messagebox.showwarning("Tải video", "Chọn một dòng trong bảng.", parent=self._top)
            return
        rec = self._uv_downloader.get_downloaded_video(vid)
        if not rec:
            messagebox.showerror("Tải video", "Không tìm thấy bản ghi.", parent=self._top)
            return
        p = Path(str(rec.get("video_path") or ""))
        if not p.is_file():
            messagebox.showerror("Tải video", "File không tồn tại.", parent=self._top)
            return
        try:
            os.startfile(str(p))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Tải video", str(exc), parent=self._top)

    def _on_uv_open_folder_selected(self) -> None:
        vid = self._uv_selected_id()
        if not vid or not self._uv_downloader:
            messagebox.showwarning("Tải video", "Chọn một dòng.", parent=self._top)
            return
        rec = self._uv_downloader.get_downloaded_video(vid)
        if not rec:
            return
        p = Path(str(rec.get("video_path") or ""))
        try:
            os.startfile(str(p.parent if p.is_file() else p))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Tải video", str(exc), parent=self._top)

    def _on_uv_analyze_reverse(self) -> None:
        vid = self._uv_selected_id()
        if not vid or not self._uv_downloader or not self._notebook:
            messagebox.showwarning("Tải video", "Chọn một video.", parent=self._top)
            return
        try:
            bridge = self._uv_downloader.send_to_reverse_prompt_engine(vid)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Tải video", str(exc), parent=self._top)
            return
        base = self._collect_reverse_payload()
        base["id"] = self._new_reverse_job_id()
        base["source_url"] = str(bridge.get("source_url") or "")
        payload = self._reverse_engine.create_job_from_local_video(
            local_video_path=str(bridge["local_video_path"]),
            video_id=str(bridge.get("video_id") or ""),
            job_id=str(base["id"]),
            base_payload=base,
        )
        self._suspend_reverse_source_reset = True
        try:
            self._var_job_id.set(str(payload.get("id") or ""))
            self._var_local_video.set(str(payload.get("local_video_path") or ""))
            self._var_source_type.set("local")
        finally:
            self._suspend_reverse_source_reset = False
        self._last_reverse_source_signature = self._current_reverse_source_signature()
        self._save_reverse_session_state()
        if self._notebook is not None:
            tabs = self._notebook.tabs()
            if len(tabs) >= 2:
                self._notebook.select(1)
        messagebox.showinfo(
            "Reverse Video",
            "Đã nạp video vào tab Reverse Video Prompt.\nChạy B1 (Import + Tách keyframes) rồi B2.",
            parent=self._top,
        )

    def _on_uv_use_ai_video(self) -> None:
        vid = self._uv_selected_id()
        if not vid or not self._uv_downloader:
            messagebox.showwarning("Tải video", "Chọn một video.", parent=self._top)
            return
        try:
            out = self._uv_downloader.send_to_ai_video_library(vid)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Tải video", str(exc), parent=self._top)
            return
        messagebox.showinfo(
            "AI Video",
            f"Đã ghi manifest nguồn video (tham chiếu file local, không tạo video mới):\n{out.get('manifest_path')}",
            parent=self._top,
        )

    def _on_uv_delete_selected(self) -> None:
        vids = self._uv_selected_ids()
        if not vids or not self._uv_downloader:
            return
        if not messagebox.askyesno(
            "Tải video",
            f"Xóa {len(vids)} video khỏi thư viện? (có thể xóa cả file trên đĩa)",
            parent=self._top,
        ):
            return
        delete_file = messagebox.askyesnocancel(
            "Tải video",
            "Xóa luôn file video/thumbnail trên đĩa và dọn folder rỗng?",
            parent=self._top,
        )
        if delete_file is None:
            return
        for vid in vids:
            self._uv_downloader.delete_downloaded_video(
                vid,
                delete_file=bool(delete_file),
                prune_empty_dirs=bool(delete_file),
            )
        self._refresh_uv_library()

    def _build_bridge_tab(self, host: ttk.Frame) -> None:
        host.columnconfigure(0, weight=1)
        ttk.Label(
            host,
            text=(
                "Đã nối module AI Video Gemini/Veo với tool ngoài Veo3Studio.\n"
                "Bạn có thể mở tool trực tiếp từ đây để vận hành quy trình mới."
            ),
            justify=tk.LEFT,
            wraplength=840,
        ).grid(row=0, column=0, sticky="w", pady=(0, 8))

        launcher = ttk.LabelFrame(host, text="Bridge Launcher", padding=10)
        launcher.grid(row=1, column=0, sticky="ew")
        launcher.columnconfigure(1, weight=1)
        ttk.Label(launcher, text="Tool exe").grid(row=0, column=0, sticky="w")
        self._var_tool_exe = tk.StringVar(value=str(self._tool_exe))
        ent = ttk.Entry(launcher, textvariable=self._var_tool_exe)
        ent.grid(row=0, column=1, sticky="ew", padx=(8, 0))

        acts = ttk.Frame(launcher)
        acts.grid(row=1, column=0, columnspan=2, sticky="w", pady=(10, 0))
        ttk.Button(acts, text="Mở Veo3Studio.exe", command=self._on_launch_tool).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(acts, text="Mở thư mục Tool", command=self._on_open_tool_folder).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(acts, text="Kiểm tra đường dẫn", command=self._on_validate_tool_path).pack(side=tk.LEFT)

        box = ttk.LabelFrame(host, text="Thông tin phiên tích hợp", padding=10)
        box.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
        box.columnconfigure(0, weight=1)
        host.rowconfigure(2, weight=1)

        spec_txt = "\n".join(
            [
                f"- action: {self._project_spec.get('action', 'open_clean_module')}",
                f"- created_at: {self._project_spec.get('created_at', '-')}",
                f"- tool_exe: {self._var_tool_exe.get()}",
                "- trạng thái: ready_for_external_tool_launch",
            ]
        )
        txt = tk.Text(box, wrap="word", height=12)
        txt.grid(row=0, column=0, sticky="nsew")
        txt.insert("1.0", spec_txt)
        txt.configure(state="disabled")

        btns = ttk.Frame(host)
        btns.grid(row=3, column=0, sticky="e", pady=(10, 0))
        ttk.Button(btns, text="Đóng", command=self._top.destroy).pack(side=tk.RIGHT)

    def _build_reverse_tab(self, host: ttk.Frame) -> None:
        host.columnconfigure(0, weight=1)
        host.rowconfigure(4, weight=1)

        guide = ttk.LabelFrame(host, text="Huong dan nhanh", padding=10)
        guide.grid(row=0, column=0, sticky="ew")
        ttk.Label(
            guide,
            text=(
                "B1. Chon file video LOCAL (tai hang loat o tab «Tải video») -> B2. Tach keyframes -> B3. Phan tich Gemini + build prompt\n"
                "B4. Xuat prompt sang Bridge Launcher va mo Veo3Studio. Tai URL bang yt-dlp chi o tab «Tải video», khong tai truc tiep o day."
            ),
            justify=tk.LEFT,
            wraplength=900,
        ).grid(row=0, column=0, sticky="w")

        src_box = ttk.LabelFrame(host, text="B1 - Nguon video (chi file local)", padding=10)
        src_box.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        src_box.columnconfigure(1, weight=1)
        src_box.columnconfigure(3, weight=1)
        self._var_job_id = tk.StringVar(value=f"reverse_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        self._var_source_type = tk.StringVar(value="local")
        self._var_local_video = tk.StringVar()
        self._var_source_type.trace_add("write", lambda *_: self._schedule_reverse_source_reset())
        self._var_local_video.trace_add("write", lambda *_: self._schedule_reverse_source_reset())
        ttk.Label(src_box, text="Ma job").grid(row=0, column=0, sticky="w")
        self._ent_job_id = ttk.Entry(src_box, textvariable=self._var_job_id, width=28)
        self._ent_job_id.grid(row=0, column=1, sticky="ew", padx=(8, 12))
        ttk.Label(src_box, text="Loai nguon").grid(row=0, column=2, sticky="w")
        ttk.Combobox(src_box, textvariable=self._var_source_type, values=["local"], width=12, state="readonly").grid(
            row=0, column=3, sticky="w"
        )
        ttk.Label(src_box, text="Duong dan video local").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(src_box, textvariable=self._var_local_video).grid(row=1, column=1, columnspan=2, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Button(src_box, text="Chon file", command=self._on_pick_local_video).grid(row=1, column=3, sticky="e", pady=(8, 0))
        ttk.Label(
            src_box,
            text="Tip: Tab «Tải video» -> Tải xong -> «Phân tích Reverse» để đưa file vào đây.",
            justify=tk.LEFT,
            wraplength=820,
        ).grid(row=2, column=0, columnspan=4, sticky="w", pady=(8, 0))

        cfg_box = ttk.LabelFrame(host, text="B2 - Cai dat keyframe va Gemini", padding=10)
        cfg_box.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        for c in range(6):
            cfg_box.columnconfigure(c, weight=1 if c in {1, 3, 5} else 0)
        self._var_keyframe_mode = tk.StringVar(value="auto")
        self._var_max_frames = tk.StringVar(value="20")
        self._var_output_language = tk.StringVar(value="Tiếng Việt")
        self._var_duration_sec = tk.StringVar(value="8")
        self._var_aspect_ratio = tk.StringVar(value="9:16")
        self._var_show_browser = tk.BooleanVar(value=False)
        self._var_upload_mode = tk.StringVar(value="auto_optimal")
        self._var_keyframe_help = tk.StringVar(value="")
        self._var_gemini_cfg_info = tk.StringVar(value="")
        self._var_frame_stats = tk.StringVar(value="Frames extracted: 0 | Frames selected for Gemini: 0 | Gemini limit: 10 files")
        language_options = [
            "Tiếng Việt",
            "English",
            "中文 (Chinese)",
            "Español",
            "Português",
            "हिन्दी (Hindi)",
            "日本語",
            "한국어",
            "Français",
            "Deutsch",
            "Русский",
            "Bahasa Indonesia",
            "ไทย",
            "العربية",
        ]
        aspect_options = ["9:16", "16:9", "1:1", "4:5", "3:4", "21:9"]
        ttk.Label(cfg_box, text="Che do keyframe").grid(row=0, column=0, sticky="w")
        self._cmb_keyframe_mode = ttk.Combobox(
            cfg_box,
            textvariable=self._var_keyframe_mode,
            values=["auto", "hybrid", "fixed_interval", "scene_detection", "thumbnail"],
            state="readonly",
        )
        self._cmb_keyframe_mode.grid(
            row=0, column=1, sticky="ew", padx=(8, 12)
        )
        ttk.Label(cfg_box, text="So frame toi da").grid(row=0, column=2, sticky="w")
        ttk.Entry(cfg_box, textvariable=self._var_max_frames, width=8).grid(row=0, column=3, sticky="w", padx=(8, 12))
        ttk.Label(cfg_box, text="Ngon ngu output").grid(row=0, column=4, sticky="w")
        ttk.Combobox(cfg_box, textvariable=self._var_output_language, values=language_options, state="readonly").grid(
            row=0, column=5, sticky="ew", padx=(8, 0)
        )
        ttk.Label(cfg_box, text="Thoi luong (s)").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(cfg_box, textvariable=self._var_duration_sec, width=8).grid(row=1, column=1, sticky="w", padx=(8, 12), pady=(8, 0))
        ttk.Label(cfg_box, text="Ti le khung").grid(row=1, column=2, sticky="w", pady=(8, 0))
        ttk.Combobox(cfg_box, textvariable=self._var_aspect_ratio, values=aspect_options, state="readonly", width=10).grid(
            row=1, column=3, sticky="w", padx=(8, 12), pady=(8, 0)
        )
        ttk.Checkbutton(cfg_box, text="Hien Gemini Browser (debug)", variable=self._var_show_browser).grid(row=1, column=4, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Label(cfg_box, text="Che do upload Gemini").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Combobox(
            cfg_box,
            textvariable=self._var_upload_mode,
            values=["auto_optimal", "best_10", "detailed_chunks"],
            state="readonly",
            width=18,
        ).grid(row=2, column=1, sticky="w", padx=(8, 12), pady=(8, 0))
        ttk.Label(cfg_box, textvariable=self._var_frame_stats, justify=tk.LEFT, wraplength=700).grid(
            row=2, column=2, columnspan=4, sticky="w", padx=(8, 0), pady=(8, 0)
        )
        ttk.Label(cfg_box, text="Giai thich keyframe").grid(row=3, column=0, sticky="nw", pady=(8, 0))
        ttk.Label(cfg_box, textvariable=self._var_keyframe_help, justify=tk.LEFT, wraplength=760).grid(
            row=3, column=1, columnspan=5, sticky="w", padx=(8, 0), pady=(8, 0)
        )
        ttk.Label(cfg_box, text="Gemini/Veo3 (lay tu Cai dat AI Providers)").grid(row=4, column=0, sticky="nw", pady=(8, 0))
        ttk.Label(cfg_box, textvariable=self._var_gemini_cfg_info, justify=tk.LEFT, wraplength=760).grid(
            row=4, column=1, columnspan=5, sticky="w", padx=(8, 0), pady=(8, 0)
        )

        repl_box = ttk.LabelFrame(host, text="B3 - Thay nhan vat va prompt series", padding=10)
        repl_box.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        for c in range(6):
            repl_box.columnconfigure(c, weight=1 if c in {1, 3, 5} else 0)
        self._var_repl_enabled = tk.BooleanVar(value=False)
        self._var_old_subject_id = tk.StringVar(value="subject_001")
        self._var_new_subject = tk.StringVar()
        self._var_keep_story = tk.BooleanVar(value=True)
        self._var_keep_style = tk.BooleanVar(value=True)
        self._var_keep_camera = tk.BooleanVar(value=True)
        self._var_keep_lighting = tk.BooleanVar(value=True)
        self._var_keep_motion = tk.BooleanVar(value=True)
        self._var_series_enabled = tk.BooleanVar(value=False)
        self._var_series_parts = tk.StringVar(value="12")
        self._var_export_separate_jobs = tk.BooleanVar(value=True)
        ttk.Checkbutton(repl_box, text="Bat thay nhan vat", variable=self._var_repl_enabled).grid(row=0, column=0, sticky="w")
        ttk.Label(repl_box, text="Subject ID cu").grid(row=0, column=1, sticky="e")
        ttk.Entry(repl_box, textvariable=self._var_old_subject_id, width=16).grid(row=0, column=2, sticky="w", padx=(8, 12))
        ttk.Label(repl_box, text="Subject moi").grid(row=0, column=3, sticky="e")
        ttk.Entry(repl_box, textvariable=self._var_new_subject).grid(row=0, column=4, columnspan=2, sticky="ew", padx=(8, 0))
        ttk.Checkbutton(repl_box, text="Giu cot truyện", variable=self._var_keep_story).grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Checkbutton(repl_box, text="Giu style", variable=self._var_keep_style).grid(row=1, column=1, sticky="w", pady=(8, 0))
        ttk.Checkbutton(repl_box, text="Giu camera", variable=self._var_keep_camera).grid(row=1, column=2, sticky="w", pady=(8, 0))
        ttk.Checkbutton(repl_box, text="Giu anh sang", variable=self._var_keep_lighting).grid(row=1, column=3, sticky="w", pady=(8, 0))
        ttk.Checkbutton(repl_box, text="Giu motion", variable=self._var_keep_motion).grid(row=1, column=4, sticky="w", pady=(8, 0))
        ttk.Checkbutton(repl_box, text="Tao series noi tiep", variable=self._var_series_enabled).grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Label(repl_box, text="So phan series").grid(row=2, column=1, sticky="e", pady=(8, 0))
        ttk.Entry(repl_box, textvariable=self._var_series_parts, width=8).grid(row=2, column=2, sticky="w", padx=(8, 12), pady=(8, 0))
        ttk.Checkbutton(
            repl_box,
            text="Xuat moi prompt thanh job rieng (khuyen nghi 12 job)",
            variable=self._var_export_separate_jobs,
        ).grid(row=2, column=3, columnspan=3, sticky="w", pady=(8, 0))

        run_box = ttk.LabelFrame(host, text="B4 - Thuc thi", padding=10)
        run_box.grid(row=4, column=0, sticky="nsew", pady=(8, 0))
        run_box.columnconfigure(0, weight=1)
        run_box.rowconfigure(1, weight=1)
        act = ttk.Frame(run_box)
        act.grid(row=0, column=0, sticky="w")
        self._btn_step1 = ttk.Button(act, text="1) Import + Tach keyframes", command=self._on_reverse_import_extract)
        self._btn_step1.pack(side=tk.LEFT, padx=(0, 8))
        self._btn_step2 = ttk.Button(act, text="2) Phan tich Gemini + Build prompt", command=self._on_reverse_analyze_build)
        self._btn_step2.pack(side=tk.LEFT, padx=(0, 8))
        self._btn_full = ttk.Button(act, text="Chay full pipeline", command=self._on_reverse_full_pipeline)
        self._btn_full.pack(side=tk.LEFT, padx=(0, 8))
        self._btn_step3 = ttk.Button(
            act, text="3) Xuat sang Bridge Launcher + Mo Tool", command=self._on_push_to_ai_video_engine
        )
        self._btn_step3.pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(act, text="Mo thu muc reverse_video", command=self._on_open_reverse_folder).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(act, text="Nap checkpoint theo Ma job", command=self._on_load_checkpoint_for_current_job).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(act, text="Reset tien trinh", command=self._on_reset_reverse_wizard).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(act, text="Xoa log", command=self._on_clear_reverse_log).pack(side=tk.LEFT, padx=(0, 8))
        prompt_box = ttk.LabelFrame(run_box, text="Prompt da tao - copy dua vao Tool / Bridge Launcher", padding=8)
        prompt_box.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        prompt_box.columnconfigure(1, weight=1)
        prompt_box.rowconfigure(0, weight=1)
        self._reverse_prompt_rows: list[dict[str, Any]] = []
        self._lst_reverse_prompts = tk.Listbox(prompt_box, height=7, exportselection=False)
        self._lst_reverse_prompts.grid(row=0, column=0, sticky="nsw", padx=(0, 8))
        self._txt_reverse_prompt_preview = tk.Text(prompt_box, wrap="word", height=7)
        self._txt_reverse_prompt_preview.grid(row=0, column=1, sticky="nsew")
        self._txt_reverse_prompt_preview.insert("1.0", "Chua co prompt. Chay B2 de build prompt truoc.\n")
        self._txt_reverse_prompt_preview.configure(state="disabled")
        prompt_btns = ttk.Frame(prompt_box)
        prompt_btns.grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Button(prompt_btns, text="Copy prompt dang chon", command=self._on_copy_selected_reverse_prompt).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(prompt_btns, text="Copy tat ca prompt", command=self._on_copy_all_reverse_prompts).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(prompt_btns, text="Mo file prompts.txt", command=self._on_open_bridge_prompts_file).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(prompt_btns, text="Nap prompt tu analysis", command=self._load_prompt_preview_for_current_job).pack(side=tk.LEFT, padx=(0, 8))
        self._lst_reverse_prompts.bind("<<ListboxSelect>>", lambda _e: self._on_reverse_prompt_selected())

        log_frame = ttk.Frame(run_box)
        log_frame.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self._txt_reverse_log = tk.Text(log_frame, wrap="word", height=10)
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self._txt_reverse_log.yview)
        self._txt_reverse_log.configure(yscrollcommand=log_scroll.set)
        self._txt_reverse_log.grid(row=0, column=0, sticky="nsew")
        log_scroll.grid(row=0, column=1, sticky="ns")
        self._txt_reverse_log.insert("1.0", "San sang.\n")
        self._txt_reverse_log.configure(state="disabled")
        self._set_reverse_wizard_state(step1=True, step2=False, step3=False, full=True)
        self._cmb_keyframe_mode.bind("<<ComboboxSelected>>", lambda _e: self._refresh_keyframe_help())
        self._ent_job_id.bind("<FocusOut>", lambda _e: self._sync_wizard_from_checkpoints())
        self._refresh_keyframe_help()
        self._refresh_gemini_provider_info()
        self._sync_wizard_from_checkpoints()

    def _append_reverse_log(self, msg: str) -> None:
        text = f"{msg}\n"
        if threading.current_thread() is not threading.main_thread():
            self._top.after(0, lambda: self._append_reverse_log(msg))
            return
        self._txt_reverse_log.configure(state="normal")
        self._txt_reverse_log.insert("end", text)
        self._txt_reverse_log.see("end")
        self._txt_reverse_log.configure(state="disabled")

    def _clear_reverse_log_text(self, text: str = "San sang.\n") -> None:
        self._txt_reverse_log.configure(state="normal")
        self._txt_reverse_log.delete("1.0", "end")
        self._txt_reverse_log.insert("1.0", text)
        self._txt_reverse_log.configure(state="disabled")

    def _set_prompt_preview_rows(self, rows: list[dict[str, Any]]) -> None:
        self._reverse_prompt_rows = rows
        self._lst_reverse_prompts.delete(0, tk.END)
        for idx, row in enumerate(rows, start=1):
            part = row.get("part")
            title = str(row.get("title") or "Prompt")
            label = f"{idx}. {title}" + (f" (Part {part})" if part is not None else "")
            if row.get("job_id"):
                label += f" - {row.get('job_id')}"
            self._lst_reverse_prompts.insert(tk.END, label)
        if rows:
            self._lst_reverse_prompts.selection_set(0)
            self._show_prompt_preview(0)
        else:
            self._txt_reverse_prompt_preview.configure(state="normal")
            self._txt_reverse_prompt_preview.delete("1.0", "end")
            self._txt_reverse_prompt_preview.insert("1.0", "Chua co prompt. Chay B2 de build prompt truoc.\n")
            self._txt_reverse_prompt_preview.configure(state="disabled")

    def _show_prompt_preview(self, idx: int) -> None:
        text = ""
        if 0 <= idx < len(self._reverse_prompt_rows):
            row = self._reverse_prompt_rows[idx]
            part = row.get("part")
            title = str(row.get("title") or "Prompt")
            text = f"{title}" + (f" | Part {part}" if part is not None else "")
            if row.get("job_id"):
                text += f"\nJob rieng: {row.get('job_id')}"
            text += "\n\n" + str(row.get("prompt") or "")
        self._txt_reverse_prompt_preview.configure(state="normal")
        self._txt_reverse_prompt_preview.delete("1.0", "end")
        self._txt_reverse_prompt_preview.insert("1.0", text or "Chua co prompt.")
        self._txt_reverse_prompt_preview.configure(state="disabled")

    def _on_reverse_prompt_selected(self) -> None:
        sel = self._lst_reverse_prompts.curselection()
        if not sel:
            return
        self._show_prompt_preview(int(sel[0]))

    def _new_reverse_job_id(self) -> str:
        return f"reverse_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def _current_reverse_source_signature(self) -> str:
        source_type = self._var_source_type.get().strip().lower() or "local"
        source = self._var_local_video.get().strip()
        return f"{source_type}|{source}"

    def _schedule_reverse_source_reset(self) -> None:
        if self._suspend_reverse_source_reset:
            return
        if self._reverse_source_change_after is not None:
            try:
                self._top.after_cancel(self._reverse_source_change_after)
            except Exception:
                pass
        self._reverse_source_change_after = self._top.after(700, self._reset_reverse_for_new_source_if_needed)

    def _reset_reverse_for_new_source_if_needed(self) -> None:
        self._reverse_source_change_after = None
        if self._suspend_reverse_source_reset:
            return
        sig = self._current_reverse_source_signature()
        if not sig.split("|", 1)[1].strip():
            return
        if sig == self._last_reverse_source_signature:
            return
        old_job = self._var_job_id.get().strip()
        self._last_reverse_source_signature = sig
        self._suspend_reverse_source_reset = True
        try:
            self._var_job_id.set(self._new_reverse_job_id())
            self._update_frame_stats(extracted=0, selected=0)
            self._set_prompt_preview_rows([])
            self._set_reverse_wizard_state(step1=True, step2=False, step3=False, full=True)
            self._clear_reverse_log_text("Nguon video moi - da tao phien Reverse moi, da xoa prompt/checkpoint cu khoi man hinh.\n")
            if old_job:
                self._append_reverse_log(f"[INFO] Da tach khoi job cu: {old_job}")
            self._append_reverse_log(f"[INFO] Ma job moi: {self._var_job_id.get().strip()}")
            self._save_reverse_session_state()
        finally:
            self._suspend_reverse_source_reset = False

    def _rows_from_analysis_payload(self, raw: dict[str, Any]) -> list[dict[str, Any]]:
        final_prompt = str(raw.get("final_prompt") or "").strip()
        rows: list[dict[str, Any]] = []
        series = raw.get("continuous_prompts")
        if isinstance(series, list) and series:
            for row in series:
                if not isinstance(row, dict):
                    continue
                ptxt = str(row.get("prompt") or "").strip()
                if not ptxt:
                    continue
                rows.append(
                    {
                        "title": str(row.get("title") or f"Part {row.get('part') or len(rows) + 1}"),
                        "part": row.get("part"),
                        "prompt": ptxt,
                    }
                )
        if not rows and final_prompt:
            rows.append({"title": "Final prompt", "part": None, "prompt": final_prompt})
        return rows

    def _load_prompt_preview_for_current_job(self) -> None:
        job_id = self._var_job_id.get().strip()
        if not job_id:
            return
        analysis_path = self._reverse_paths["analysis"] / f"{job_id}.json"
        if not analysis_path.is_file():
            self._set_prompt_preview_rows([])
            return
        try:
            raw = json.loads(analysis_path.read_text(encoding="utf-8"))
            rows = self._rows_from_analysis_payload(raw if isinstance(raw, dict) else {})
            self._set_prompt_preview_rows(rows)
            self._append_reverse_log(f"[INFO] Da nap {len(rows)} prompt vao bang copy.")
        except Exception as exc:  # noqa: BLE001
            self._append_reverse_log(f"[WARNING] Khong nap duoc prompt preview: {exc}")

    def _copy_text_to_clipboard(self, text: str) -> None:
        self._top.clipboard_clear()
        self._top.clipboard_append(text)
        self._top.update_idletasks()

    def _on_copy_selected_reverse_prompt(self) -> None:
        sel = self._lst_reverse_prompts.curselection()
        if not sel or not self._reverse_prompt_rows:
            messagebox.showwarning("Reverse Video", "Chua chon prompt de copy.", parent=self._top)
            return
        row = self._reverse_prompt_rows[int(sel[0])]
        text = str(row.get("prompt") or "")
        self._copy_text_to_clipboard(text)
        self._append_reverse_log("[SUCCESS] Da copy prompt dang chon vao clipboard.")

    def _on_copy_all_reverse_prompts(self) -> None:
        if not self._reverse_prompt_rows:
            messagebox.showwarning("Reverse Video", "Chua co prompt de copy.", parent=self._top)
            return
        text = "\n\n".join(
            [
                f"=== {row.get('title') or 'Prompt'}"
                + (f" | Part {row.get('part')}" if row.get("part") is not None else "")
                + (f" | Job {row.get('job_id')}" if row.get("job_id") else "")
                + f" ===\n{row.get('prompt') or ''}"
                for row in self._reverse_prompt_rows
            ]
        )
        self._copy_text_to_clipboard(text)
        self._append_reverse_log(f"[SUCCESS] Da copy tat ca {len(self._reverse_prompt_rows)} prompt vao clipboard.")

    def _on_open_bridge_prompts_file(self) -> None:
        job_id = self._var_job_id.get().strip()
        if not job_id:
            return
        exe = Path(self._var_tool_exe.get().strip() or str(self._tool_exe))
        tool_dir = exe.parent if exe.parent.exists() else _INTERNAL_TOOL_DIR
        prompts_file = tool_dir / "data" / "reverse_bridge" / f"{job_id}_prompts.txt"
        if not prompts_file.is_file():
            messagebox.showwarning("Reverse Video", "Chua co prompts.txt. Hay bam B3 xuat sang Bridge Launcher truoc.", parent=self._top)
            return
        try:
            os.startfile(str(prompts_file))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Reverse Video", f"Khong mo duoc file prompts:\n{exc}", parent=self._top)

    def _collect_reverse_payload(self) -> dict[str, Any]:
        return {
            "id": self._var_job_id.get().strip(),
            "source_type": "local",
            "source_url": "",
            "local_video_path": self._var_local_video.get().strip(),
            "target_platform": "Facebook Reels",
            "output_language": self._var_output_language.get().strip(),
            "duration_sec": int(self._var_duration_sec.get().strip() or "8"),
            "aspect_ratio": self._var_aspect_ratio.get().strip() or "9:16",
            "analysis_mode": "gemini_browser",
            "keyframe_mode": self._var_keyframe_mode.get().strip(),
            "max_frames": int(self._var_max_frames.get().strip() or "20"),
            "replacement": {
                "enabled": bool(self._var_repl_enabled.get()),
                "replace_type": "character",
                "old_subject_id": self._var_old_subject_id.get().strip(),
                "new_subject": self._var_new_subject.get().strip(),
                "keep_story": bool(self._var_keep_story.get()),
                "keep_style": bool(self._var_keep_style.get()),
                "keep_camera": bool(self._var_keep_camera.get()),
                "keep_lighting": bool(self._var_keep_lighting.get()),
                "keep_motion": bool(self._var_keep_motion.get()),
            },
            "continuous_series": {
                "enabled": bool(self._var_series_enabled.get()),
                "total_parts": int(self._var_series_parts.get().strip() or "5"),
                "continue_action_between_prompts": True,
                "export_separate_jobs": bool(self._var_export_separate_jobs.get()),
            },
            "gemini_browser": {
                "show_browser": bool(self._var_show_browser.get()),
                "upload_mode": self._var_upload_mode.get().strip(),
            },
        }

    def _reverse_session_file(self) -> Path:
        return self._reverse_paths["analysis"] / "reverse_session_state.json"

    def _save_reverse_session_state(self) -> None:
        try:
            payload = self._collect_reverse_payload()
            state = {
                "saved_at": datetime.now().replace(microsecond=0).isoformat(),
                "tool_exe": str(_normalize_user_path(self._var_tool_exe.get() or str(self._tool_exe))),
                "payload": payload,
            }
            self._reverse_session_file().write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        except Exception:
            pass

    def _load_reverse_session_state(self) -> None:
        p = self._reverse_session_file()
        if not p.is_file():
            return
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return
        data = dict(raw.get("payload") or {})
        saved_tool = _normalize_user_path(str(raw.get("tool_exe") or ""))
        if saved_tool:
            self._set_tool_exe_path(saved_tool)
        if not data:
            return
        self._var_job_id.set(str(data.get("id") or self._var_job_id.get()))
        self._var_source_type.set("local")
        self._var_local_video.set(str(data.get("local_video_path") or ""))
        self._var_output_language.set(str(data.get("output_language") or self._var_output_language.get()))
        self._var_duration_sec.set(str(data.get("duration_sec") or self._var_duration_sec.get()))
        self._var_aspect_ratio.set(str(data.get("aspect_ratio") or self._var_aspect_ratio.get()))
        self._var_keyframe_mode.set(str(data.get("keyframe_mode") or self._var_keyframe_mode.get()))
        self._var_max_frames.set(str(data.get("max_frames") or self._var_max_frames.get()))
        repl = dict(data.get("replacement") or {})
        self._var_repl_enabled.set(bool(repl.get("enabled", False)))
        self._var_old_subject_id.set(str(repl.get("old_subject_id") or self._var_old_subject_id.get()))
        self._var_new_subject.set(str(repl.get("new_subject") or ""))
        self._var_keep_story.set(bool(repl.get("keep_story", True)))
        self._var_keep_style.set(bool(repl.get("keep_style", True)))
        self._var_keep_camera.set(bool(repl.get("keep_camera", True)))
        self._var_keep_lighting.set(bool(repl.get("keep_lighting", True)))
        self._var_keep_motion.set(bool(repl.get("keep_motion", True)))
        series = dict(data.get("continuous_series") or {})
        self._var_series_enabled.set(bool(series.get("enabled", False)))
        self._var_series_parts.set(str(series.get("total_parts") or self._var_series_parts.get()))
        self._var_export_separate_jobs.set(bool(series.get("export_separate_jobs", True)))
        gb = dict(data.get("gemini_browser") or {})
        # Mặc định luôn ẩn browser khi mở lại dialog; bật checkbox chỉ dùng cho phiên debug hiện tại.
        self._var_show_browser.set(False)
        self._var_upload_mode.set(str(gb.get("upload_mode") or "auto_optimal"))
        self._append_reverse_log(f"[INFO] Da khoi phuc phien Reverse theo Ma job: {self._var_job_id.get().strip()}")
        self._sync_wizard_from_checkpoints()

    def _set_tool_exe_path(self, exe_path: Path | str) -> None:
        exe = _normalize_user_path(str(exe_path))
        if not exe:
            return
        self._tool_exe = exe
        self._project_spec["tool_exe"] = str(exe)
        if hasattr(self, "_var_tool_exe") and self._var_tool_exe is not None:
            try:
                self._var_tool_exe.set(str(exe))
            except Exception:
                pass

    def _on_pick_local_video(self) -> None:
        path = filedialog.askopenfilename(
            parent=self._top,
            title="Chon file video local",
            filetypes=[("Video files", "*.mp4 *.mov *.webm *.mkv"), ("All files", "*.*")],
        )
        if not path:
            return
        self._var_local_video.set(path)
        self._var_source_type.set("local")
        self._reset_reverse_for_new_source_if_needed()

    def _refresh_keyframe_help(self) -> None:
        mode = self._var_keyframe_mode.get().strip().lower()
        mapping = {
            "auto": "Auto: tu dong chon theo do dai video (ngan => fixed interval, vua => hybrid, dai => scene detection).",
            "hybrid": "Hybrid (khuyen nghi): lay dau/giua/cuoi + interval/scene, sau do loc trung va gioi han so frame.",
            "fixed_interval": "Fixed interval: cat frame theo chu ky co dinh (vi du 1 frame/giay), hop video ngan.",
            "scene_detection": "Scene detection: cat khi canh thay doi manh, hop video dai/co nhieu chuyen canh.",
            "thumbnail": "Thumbnail: lay frame dai dien theo cum, nhanh nhe khi can bo frame dai dien.",
        }
        self._var_keyframe_help.set(mapping.get(mode, "Chon che do keyframe phu hop noi dung video."))

    def _update_frame_stats(self, *, extracted: int, selected: int) -> None:
        self._var_frame_stats.set(
            f"Frames extracted: {int(extracted)} | Frames selected for Gemini: {int(selected)} | Gemini limit: 10 files"
        )

    def _refresh_gemini_provider_info(self) -> None:
        cfg = get_nanobanana_runtime_config()
        web_url = os.environ.get("NANOBANANA_WEB_URL", "").strip() or os.environ.get("VEO3_WEB_URL", "").strip() or str(cfg.get("web_url") or "").strip()
        profile = (
            os.environ.get("NANOBANANA_BROWSER_PROFILE_DIR", "").strip()
            or os.environ.get("VEO3_BROWSER_PROFILE_DIR", "").strip()
            or str(project_root() / "data" / "nanobanana" / "browser_profile")
        )
        info = (
            f"- URL: {web_url or '(chua cau hinh, mac dinh Gemini)'}\n"
            f"- Profile: {profile}\n"
            "- Cau hinh nay duoc quan ly tai tab: Cai dat AI Providers -> Dang nhap Gemini/Veo3 (Browser)."
        )
        self._var_gemini_cfg_info.set(info)

    def _on_clear_reverse_log(self) -> None:
        self._clear_reverse_log_text("Da xoa log.\n")
        self._save_reverse_session_state()

    def _on_reset_reverse_wizard(self) -> None:
        self._set_reverse_wizard_state(step1=True, step2=False, step3=False, full=True)
        self._append_reverse_log("[INFO] Da reset wizard: B1 mo, B2/B3 khoa.")

    def _set_reverse_wizard_state(self, *, step1: bool, step2: bool, step3: bool, full: bool) -> None:
        self._btn_step1.configure(state=("normal" if step1 else "disabled"))
        self._btn_step2.configure(state=("normal" if step2 else "disabled"))
        self._btn_step3.configure(state=("normal" if step3 else "disabled"))
        self._btn_full.configure(state=("normal" if full else "disabled"))

    def _on_open_reverse_folder(self) -> None:
        try:
            os.startfile(str(self._reverse_paths["root"]))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Reverse Video", f"Không mở được thư mục:\n{exc}", parent=self._top)

    def _checkpoint_paths(self, job_id: str) -> tuple[Path, Path, Path]:
        jid = str(job_id or "").strip()
        pre = self._reverse_paths["analysis"] / f"{jid}_pre_gemini.json"
        final = self._reverse_paths["analysis"] / f"{jid}.json"
        pushed = self._reverse_paths["analysis"] / f"{jid}_pushed_ai_video.json"
        return pre, final, pushed

    def _sync_wizard_from_checkpoints(self) -> None:
        job_id = self._var_job_id.get().strip()
        if not job_id:
            self._set_reverse_wizard_state(step1=True, step2=False, step3=False, full=True)
            return
        pre, final, pushed = self._checkpoint_paths(job_id)
        stats_src = final if final.is_file() else pre
        if stats_src.is_file():
            try:
                raw = json.loads(stats_src.read_text(encoding="utf-8"))
                frames = raw.get("frames") if isinstance(raw, dict) and isinstance(raw.get("frames"), list) else []
                self._update_frame_stats(extracted=len(frames), selected=min(10, len(frames)))
            except Exception:
                pass
        has_pre = pre.is_file()
        has_final = final.is_file()
        _has_pushed = pushed.is_file()
        if has_final:
            self._set_reverse_wizard_state(step1=True, step2=True, step3=True, full=True)
            self._append_reverse_log(f"[INFO] Da nap checkpoint B2 (analysis): {final.name}")
            self._load_prompt_preview_for_current_job()
            return
        if has_pre:
            self._set_reverse_wizard_state(step1=True, step2=True, step3=False, full=True)
            self._append_reverse_log(f"[INFO] Da nap checkpoint B1 (pre_gemini): {pre.name}")
            return
        self._set_reverse_wizard_state(step1=True, step2=False, step3=False, full=True)

    def _on_load_checkpoint_for_current_job(self) -> None:
        self._sync_wizard_from_checkpoints()
        job_id = self._var_job_id.get().strip()
        if not job_id:
            messagebox.showwarning("Reverse Video", "Vui long nhap Ma job truoc.", parent=self._top)
            return
        pre, final, pushed = self._checkpoint_paths(job_id)
        msg = (
            f"Checkpoint cua {job_id}:\n"
            f"- B1 pre_gemini: {'co' if pre.is_file() else 'chua'}\n"
            f"- B2 analysis: {'co' if final.is_file() else 'chua'}\n"
            f"- B3 pushed: {'co' if pushed.is_file() else 'chua'}"
        )
        messagebox.showinfo("Reverse Video", msg, parent=self._top)

    def _run_bg(self, title: str, fn: Callable[[], None]) -> None:
        def _worker() -> None:
            try:
                fn()
            except Exception as exc:  # noqa: BLE001
                self._append_reverse_log(f"[ERROR] {exc}")
                self._top.after(0, lambda: self._set_reverse_wizard_state(step1=True, step2=True, step3=True, full=True))
                self._top.after(0, lambda err=exc: messagebox.showerror("Reverse Video", f"{title} lỗi:\n{err}", parent=self._top))

        threading.Thread(target=_worker, daemon=True).start()

    def _on_reverse_import_extract(self) -> None:
        payload = self._collect_reverse_payload()
        self._save_reverse_session_state()
        self._set_reverse_wizard_state(step1=False, step2=False, step3=False, full=False)

        def _job() -> None:
            job = self._reverse_engine.build_job_from_input(payload)
            if not self._reverse_engine.ff.check_ffmpeg_available():
                raise RuntimeError("FFmpeg/ffprobe chưa sẵn sàng")
            video_path = self._reverse_engine.importer.import_video(job)
            meta = self._reverse_engine.ff.read_metadata(video_path)
            self._append_reverse_log(f"[INFO] Đã đọc metadata video: {meta.get('resolution')}, {meta.get('duration')}s")
            frames = self._reverse_engine.extractor.extract(
                job_id=job.id,
                video_path=video_path,
                mode=job.keyframe_mode,
                max_frames=max(1, min(job.max_frames, 40)),
                duration=float(meta.get("duration") or 0.0),
            )
            out = {"id": job.id, "video_path": str(video_path), "video_metadata": meta, "frames": frames, "status": "ready_for_gemini"}
            path = self._reverse_paths["analysis"] / f"{job.id}_pre_gemini.json"
            path.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            self._append_reverse_log(f"[SUCCESS] Đã xong import + keyframes. File: {path}")
            self._top.after(0, lambda: self._update_frame_stats(extracted=len(frames), selected=min(10, len(frames))))
            self._top.after(0, self._sync_wizard_from_checkpoints)
            self._top.after(0, self._save_reverse_session_state)

        self._run_bg("Import + extract", _job)

    def _on_reverse_analyze_build(self) -> None:
        payload = self._collect_reverse_payload()
        self._save_reverse_session_state()
        self._set_reverse_wizard_state(step1=False, step2=False, step3=False, full=False)

        def _job() -> None:
            job = self._reverse_engine.build_job_from_input(payload)
            pre = self._reverse_paths["analysis"] / f"{job.id}_pre_gemini.json"
            if not pre.is_file():
                raise RuntimeError("Chưa có pre_gemini. Hãy bấm Import + Tách keyframes trước.")
            raw = json.loads(pre.read_text(encoding="utf-8"))
            frames = raw.get("frames") if isinstance(raw.get("frames"), list) else []
            frame_paths = [str(x.get("path") or "") for x in frames if isinstance(x, dict)]
            video_path = str(raw.get("video_path") or "")
            raw_txt = self._reverse_engine.gemini.analyze(job=job, frame_paths=frame_paths, video_path=video_path)
            parsed = self._reverse_engine.parser.extract_json(raw_txt)
            parsed, repl = self._reverse_engine.replacement_engine.apply(parsed=parsed, replacement=job.replacement)
            final_prompt = self._reverse_engine.prompt_builder.build(parsed=parsed, job=job)
            scenes = self._reverse_engine.scene_builder.build(parsed)
            series: list[dict[str, Any]] = []
            if job.continuous_series and job.continuous_series.get("enabled"):
                series = self._reverse_engine.series_engine.build(
                    final_prompt=final_prompt,
                    scenes=scenes,
                    total_parts=int(job.continuous_series.get("total_parts") or 5),
                    parsed=parsed,
                )
            output = {
                "id": job.id,
                "source_url": job.source_url,
                "video_path": video_path,
                "video_metadata": raw.get("video_metadata") or {},
                "frames": frames,
                "frame_zip_path": "",
                "gemini_raw_output": raw_txt,
                "visual_analysis": parsed,
                "subjects": self._reverse_engine.subject_builder.build(parsed),
                "environments": self._reverse_engine.env_builder.build(parsed),
                "scenes": scenes,
                "style_analysis": self._reverse_engine.style_builder.build(parsed),
                "story_map": self._reverse_engine.story_builder.build(parsed),
                "replacement_map": repl,
                "final_prompt": final_prompt,
                "continuous_prompts": series,
                "status": "completed",
                "error_message": "",
            }
            self._reverse_engine.exporter.export_prompt_package(job_id=job.id, payload=output)
            path = self._reverse_paths["analysis"] / f"{job.id}.json"
            path.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            self._append_reverse_log(f"[SUCCESS] Build prompt thành công. File: {path}")
            total_prompts = len(series) if series else 1
            self._top.after(0, self._load_prompt_preview_for_current_job)
            self._append_reverse_log(
                f"[INFO] Đã sẵn sàng sang B3 Bridge Launcher. Số prompt tạo video: {total_prompts} (series {'bật' if bool(series) else 'tắt'})."
            )
            self._top.after(0, self._sync_wizard_from_checkpoints)
            self._top.after(0, self._save_reverse_session_state)
            self._top.after(
                0,
                lambda: messagebox.showinfo(
                    "Reverse Video",
                    f"B2 hoàn tất.\nSẵn sàng sang B3 để xuất sang Bridge Launcher/Veo3Studio.\n\n"
                    f"Số prompt sẽ xuất: {total_prompts}.",
                    parent=self._top,
                ),
            )

        self._run_bg("Analyze + build", _job)

    def _on_reverse_full_pipeline(self) -> None:
        payload = self._collect_reverse_payload()
        self._save_reverse_session_state()
        self._set_reverse_wizard_state(step1=False, step2=False, step3=False, full=False)

        def _job() -> None:
            out = self._reverse_engine.run_pipeline(payload)
            self._append_reverse_log(f"[SUCCESS] Full pipeline done: {out.get('id')}")
            self._top.after(0, self._sync_wizard_from_checkpoints)
            self._top.after(0, self._save_reverse_session_state)
            self._top.after(
                0,
                lambda: messagebox.showinfo("Reverse Video", f"Hoàn tất reverse prompt: {out.get('id')}", parent=self._top),
            )

        self._run_bg("Full pipeline", _job)

    def _on_push_to_ai_video_engine(self) -> None:
        payload = self._collect_reverse_payload()
        self._save_reverse_session_state()
        self._set_reverse_wizard_state(step1=False, step2=False, step3=False, full=False)

        def _job() -> None:
            job_id = str(payload.get("id") or "").strip()
            if not job_id:
                raise RuntimeError("Thiếu job_id")
            analysis_path = self._reverse_paths["analysis"] / f"{job_id}.json"
            if not analysis_path.is_file():
                raise RuntimeError("Chưa có output reverse hoàn chỉnh. Hãy chạy 'Phân tích Gemini + Build prompt' hoặc 'Full pipeline'.")
            raw = json.loads(analysis_path.read_text(encoding="utf-8"))
            final_prompt = str(raw.get("final_prompt") or "").strip()
            if not final_prompt:
                raise RuntimeError("Output reverse thiếu final_prompt.")
            series = raw.get("continuous_prompts")
            prompt_rows: list[dict[str, Any]] = [{"prompt": final_prompt, "part": None, "title": "Final prompt"}]
            if isinstance(series, list) and series:
                prompt_rows = []
                for row in series:
                    if not isinstance(row, dict):
                        continue
                    ptxt = str(row.get("prompt") or "").strip()
                    if not ptxt:
                        continue
                    try:
                        part_no = int(row.get("part"))
                    except Exception:
                        part_no = None
                    prompt_rows.append(
                        {
                            "prompt": ptxt,
                            "part": part_no,
                            "title": str(row.get("title") or (f"Part {part_no}" if part_no else "Series part")),
                            "state_in": dict(row.get("state_in") or {}),
                            "state_out": dict(row.get("state_out") or {}),
                        }
                    )
                if not prompt_rows:
                    prompt_rows = [{"prompt": final_prompt, "part": None, "title": "Final prompt"}]
            batch_id = f"{job_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            for idx, row in enumerate(prompt_rows, start=1):
                part_no = row.get("part") if row.get("part") is not None else idx
                try:
                    part_int = int(part_no)
                except Exception:
                    part_int = idx
                row["job_id"] = f"{batch_id}_part_{part_int:02d}"
                row["batch_id"] = batch_id

            bridge_payload = {
                "schema": "toolfb.reverse_video.bridge.v1",
                "job_id": job_id,
                "batch_id": batch_id,
                "created_at": datetime.now().replace(microsecond=0).isoformat(),
                "source": {
                    "source_url": raw.get("source_url") or "",
                    "video_path": raw.get("video_path") or "",
                    "analysis_path": str(analysis_path),
                },
                "target_tool": "Veo3Studio",
                "target_module": "Bridge Launcher / Gen Normal",
                "project": {
                    "name": f"Reverse {job_id}",
                    "aspect_ratio": str(payload.get("aspect_ratio") or "9:16"),
                    "duration_sec": int(payload.get("duration_sec") or 8),
                    "language": str(payload.get("output_language") or "Vietnamese"),
                    "mode": "TEXT_TO_VIDEO",
                },
                "prompts": prompt_rows,
                "context": {
                    "style_analysis": raw.get("style_analysis") or {},
                    "subjects": raw.get("subjects") or [],
                    "environments": raw.get("environments") or [],
                    "scenes": raw.get("scenes") or [],
                    "story_map": raw.get("story_map") or {},
                },
            }

            exe = Path(self._var_tool_exe.get().strip() or str(self._tool_exe))
            tool_dir = exe.parent if exe.parent.exists() else _INTERNAL_TOOL_DIR
            bridge_dir = tool_dir / "data" / "reverse_bridge"
            bridge_dir.mkdir(parents=True, exist_ok=True)
            bridge_json = bridge_dir / f"{job_id}_bridge_payload.json"
            bridge_txt = bridge_dir / f"{job_id}_prompts.txt"
            bridge_json.write_text(json.dumps(bridge_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            bridge_txt.write_text(
                "\n\n".join(
                    [
                        f"=== {row.get('title') or 'Prompt'}"
                        + (f" | Part {row.get('part')}" if row.get("part") is not None else "")
                        + (f" | Job {row.get('job_id')}" if row.get("job_id") else "")
                        + f" ===\n{row.get('prompt') or ''}"
                        for row in prompt_rows
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            separate_job_files: list[str] = []
            if bool(dict(payload.get("continuous_series") or {}).get("export_separate_jobs", True)):
                batch_dir = bridge_dir / batch_id
                batch_dir.mkdir(parents=True, exist_ok=True)
                for row in prompt_rows:
                    child_job_id = str(row.get("job_id") or "").strip()
                    if not child_job_id:
                        continue
                    child_payload = {
                        "schema": "toolfb.reverse_video.bridge.job.v1",
                        "job_id": child_job_id,
                        "parent_job_id": job_id,
                        "batch_id": batch_id,
                        "created_at": bridge_payload["created_at"],
                        "source": bridge_payload["source"],
                        "target_tool": bridge_payload["target_tool"],
                        "target_module": bridge_payload["target_module"],
                        "project": {
                            **bridge_payload["project"],
                            "name": f"Reverse {child_job_id}",
                        },
                        "prompts": [row],
                        "context": bridge_payload["context"],
                    }
                    child_json = bridge_dir / f"{child_job_id}_bridge_payload.json"
                    child_txt = bridge_dir / f"{child_job_id}_prompt.txt"
                    child_json.write_text(json.dumps(child_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                    child_txt.write_text(str(row.get("prompt") or "").strip() + "\n", encoding="utf-8")
                    (batch_dir / child_json.name).write_text(json.dumps(child_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                    (batch_dir / child_txt.name).write_text(str(row.get("prompt") or "").strip() + "\n", encoding="utf-8")
                    separate_job_files.append(str(child_json))
                manifest = {
                    "schema": "toolfb.reverse_video.bridge.batch_manifest.v1",
                    "parent_job_id": job_id,
                    "batch_id": batch_id,
                    "job_count": len(separate_job_files),
                    "jobs": separate_job_files,
                }
                (batch_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            self._top.after(0, lambda rows=prompt_rows: self._set_prompt_preview_rows(rows))

            pushed_path = self._reverse_paths["analysis"] / f"{job_id}_pushed_ai_video.json"
            pushed_path.write_text(
                json.dumps(
                    {
                        "job_id": job_id,
                        "bridge_payload_path": str(bridge_json),
                        "bridge_prompts_path": str(bridge_txt),
                        "batch_id": batch_id,
                        "separate_job_files": separate_job_files,
                        "prompt_count": len(prompt_rows),
                        "pushed_at": datetime.now().replace(microsecond=0).isoformat(),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            self._append_reverse_log(f"[SUCCESS] Đã xuất {len(prompt_rows)} prompt sang Bridge Launcher: {bridge_json}")
            if separate_job_files:
                self._append_reverse_log(f"[SUCCESS] Đã tạo {len(separate_job_files)} job riêng trong reverse_bridge (batch: {batch_id}).")
            if exe.is_file():
                subprocess.Popen([str(exe)], cwd=str(exe.parent))
                self._append_reverse_log(f"[INFO] Đã mở Veo3Studio qua Bridge Launcher: {exe}")
            else:
                self._append_reverse_log(f"[WARNING] Không tìm thấy Veo3Studio.exe để mở tự động: {exe}")
            self._top.after(0, self._sync_wizard_from_checkpoints)
            self._top.after(0, self._save_reverse_session_state)
            self._top.after(
                0,
                lambda: messagebox.showinfo(
                    "Reverse Video",
                    f"Đã xuất {len(prompt_rows)} prompt sang Bridge Launcher.\n\n{bridge_json}",
                    parent=self._top,
                ),
            )

        self._run_bg("Export to Bridge Launcher", _job)

    def _on_close_dialog(self) -> None:
        self._save_reverse_session_state()
        self._top.destroy()

    def _on_validate_tool_path(self) -> None:
        exe = Path(self._var_tool_exe.get().strip())
        if exe.is_file():
            messagebox.showinfo("AI Video", f"Đường dẫn hợp lệ:\n{exe}", parent=self._top)
            return
        messagebox.showwarning("AI Video", f"Không tìm thấy exe:\n{exe}", parent=self._top)

    def _on_pick_tool_exe(self) -> None:
        cur = _normalize_user_path(self._var_tool_exe.get())
        initial_dir = str((_nearest_existing_parent(cur.parent) or _nearest_existing_parent(self._tool_exe.parent) or project_root()))
        picked = filedialog.askopenfilename(
            parent=self._top,
            title="Chọn Veo3Studio.exe",
            initialdir=initial_dir,
            filetypes=[("Executable", "*.exe"), ("All files", "*.*")],
        )
        if not picked:
            return
        exe = _normalize_user_path(picked)
        self._set_tool_exe_path(exe)
        self._save_reverse_session_state()
        if exe.is_file():
            messagebox.showinfo("AI Video", f"Đã chọn tool:\n{exe}", parent=self._top)
        else:
            messagebox.showwarning("AI Video", f"Đường dẫn đã chọn không tồn tại:\n{exe}", parent=self._top)
    def _on_open_tool_folder(self) -> None:
        exe = Path(self._var_tool_exe.get().strip())
        folder = exe.parent if exe.parent.exists() else _EXTERNAL_TOOL_DIR
        try:
            os.startfile(str(folder))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", f"Không mở được thư mục tool:\n{exc}", parent=self._top)

    def _on_launch_tool(self) -> None:
        raw_exe = _normalize_user_path(self._var_tool_exe.get())
        base_dir = (
            _nearest_existing_parent(raw_exe.parent)
            or _nearest_existing_parent(self._tool_exe.parent)
            or _nearest_existing_parent(_EXTERNAL_TOOL_DIR)
            or _nearest_existing_parent(_INTERNAL_TOOL_DIR)
            or project_root()
        )
        dev_launcher = Path(base_dir) / "run_dev_auto_login.bat"

        candidates: list[Path] = []
        if raw_exe:
            candidates.append(raw_exe)
        candidates.extend(
            [
                Path(base_dir) / "Veo3Studio.exe",
                Path(base_dir) / "Veo3StudioLite.exe",
                _EXTERNAL_TOOL_EXE,
                _INTERNAL_TOOL_EXE,
            ]
        )
        exe: Path | None = None
        for cand in candidates:
            try:
                p = _normalize_user_path(str(cand))
            except Exception:
                continue
            if p.is_file():
                exe = p
                break
        try:
            if dev_launcher.is_file():
                cmd = ["cmd", "/c", str(dev_launcher)]
                cwd = str(Path(base_dir))
                if exe is not None:
                    cmd.append(str(exe))
                    cwd = str(exe.parent)
                subprocess.Popen(cmd, cwd=cwd, shell=False)
                messagebox.showinfo(
                    "AI Video",
                    f"Đã mở tool qua DEV launcher:\n{dev_launcher}",
                    parent=self._top,
                )
                return
            if exe is None:
                messagebox.showwarning(
                    "AI Video",
                    "Không tìm thấy file chạy Veo3Studio.\n"
                    f"Đường dẫn đang nhập: {raw_exe}\n"
                    "Hãy bấm «Chọn file .exe...» và chọn lại đúng file.",
                    parent=self._top,
                )
                return
            subprocess.Popen([str(exe)], cwd=str(exe.parent))
            self._set_tool_exe_path(exe)
            self._save_reverse_session_state()
            messagebox.showinfo("AI Video", f"Đã mở tool:\n{exe}", parent=self._top)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", f"Mở tool thất bại:\n{exc}", parent=self._top)
