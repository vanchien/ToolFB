from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from tkinter import filedialog, messagebox, ttk

from src.services.ai_video_generation_service import AIVideoGenerationService
from src.services.reverse_video_prompt_engine import VideoReversePromptEngine, ensure_reverse_video_layout
from src.services.facebook_reels_catalog import (
    normalize_facebook_reels_tab_url,
    scan_facebook_profile_reels_page,
)
from src.services.universal_video_downloader import (
    UniversalVideoDownloader,
    classify_url_type,
    detect_platform,
    load_universal_video_downloader_config,
    persist_facebook_reels_settings,
)
from src.utils.app_secrets import get_nanobanana_runtime_config
from src.utils.paths import project_root

_INTERNAL_TOOL_DIR = project_root() / "tools" / "Veo3Studio"
_INTERNAL_TOOL_EXE = _INTERNAL_TOOL_DIR / "Veo3Studio.exe"
_EXTERNAL_TOOL_DIR = Path(r"C:\Users\Hello\Desktop\Tool")
_EXTERNAL_TOOL_EXE = _EXTERNAL_TOOL_DIR / "Veo3Studio.exe"


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
        self._tool_exe = Path(self._project_spec.get("tool_exe") or default_exe)
        self._embedded_download_host = embedded_download_host
        if self._embedded_download_host is None:
            self._top = tk.Toplevel(parent)
            self._top.title("AI Video Gemini/Veo — External Tool Bridge")
            self._top.geometry("980x700")
            self._top.minsize(900, 620)
        else:
            self._top = self._embedded_download_host.winfo_toplevel()
        self._reverse_engine = VideoReversePromptEngine(log=self._append_reverse_log)
        self._reverse_paths = ensure_reverse_video_layout()
        self._ai_video_service = AIVideoGenerationService()
        self._uv_downloader: UniversalVideoDownloader | None = None
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
        self._var_uv_yt_list_max = tk.StringVar(value="500")
        self._var_uv_yt_scan_status = tk.StringVar(value="")
        self._var_uv_fb_cookie = tk.StringVar(value="")
        self._var_uv_fb_scan_status = tk.StringVar(value="")
        self._var_uv_fb_max_collect = tk.StringVar(value="200")
        self._var_uv_fb_max_scroll = tk.StringVar(value="100")
        self._var_uv_fb_scan_minutes = tk.StringVar(value="30")
        self._var_uv_fb_scroll_until_end = tk.BooleanVar(value=True)
        self._uv_download_scroll_canvas: tk.Canvas | None = None
        self._uv_log_buffer: list[str] = []
        self._uv_log_flush_after_id: str | None = None
        self._uv_last_partial_ui_ts: float = 0.0
        self._start_tab = str(start_tab or "reverse").strip().lower()
        if self._embedded_download_host is None:
            self._build_ui()
        else:
            self._build_download_only_ui(self._embedded_download_host)
        self._uv_downloader = UniversalVideoDownloader(log=self._append_uv_log)
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
            self._var_uv_ytdlp_status.set(
                f"Sẵn sàng — {st.get('version', '')}. {st.get('label', '')}"
            )
        else:
            tail = f" ({st.get('label')})" if st.get("label") else ""
            self._var_uv_ytdlp_status.set(f"Chưa dùng được: {st.get('message', 'Lỗi không rõ')}{tail}")

    def _refresh_uv_ytdlp_status(self) -> None:
        """Chạy yt-dlp --version trong nền, cập nhật nhãn (không đơ UI)."""
        down = self._uv_downloader

        def _work() -> None:
            if not down:
                self._top.after(
                    0,
                    lambda: self._var_uv_ytdlp_status.set("yt-dlp: module tải chưa sẵn sàng."),
                )
                return
            st = down.get_ytdlp_status()
            self._top.after(0, lambda s=st: self._apply_ytdlp_status_to_var(s))

        threading.Thread(target=_work, daemon=True, name="ytdlp_status_check").start()

    def _on_uv_verify_ytdlp(self) -> None:
        if not self._uv_downloader:
            messagebox.showerror("yt-dlp", "Module tải chưa khởi tạo.", parent=self._top)
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
        if not self._uv_downloader:
            messagebox.showerror("yt-dlp", "Module tải chưa khởi tạo.", parent=self._top)
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
                "Luồng nhanh: (1) nhập URL + thư mục lưu  (2) quét Reels hoặc quét YouTube  "
                "(3) tải đã chọn / tất cả  (4) xem thư viện đã tải."
            ),
            wraplength=840,
            justify=tk.LEFT,
            foreground="#555",
            font=("Segoe UI", 9),
        ).grid(row=0, column=0, sticky="w")

        st_fr = ttk.LabelFrame(host, text="yt-dlp — trạng thái (tự kiểm tra khi mở tab)", padding=8)
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
                "Tool tự tìm: PATH -> config -> python -m yt_dlp (pip). "
                "Reels: dùng cookie JSON khi cần đăng nhập."
            ),
            wraplength=820,
            justify=tk.LEFT,
            font=("Segoe UI", 9),
            foreground="#555",
        ).pack(anchor="w", pady=(4, 0))

        form = ttk.LabelFrame(host, text="Bước 1 — Nguồn & thư mục lưu", padding=8)
        form.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        form.columnconfigure(1, weight=1)
        self._var_uv_url = tk.StringVar()
        self._var_uv_platform = tk.StringVar(value="Auto detect")
        self._var_uv_url_type = tk.StringVar(value="Auto detect")
        self._var_uv_max_videos = tk.StringVar(value=str(yt_cfg.get("max_videos_default") or 50))
        self._var_uv_out_dir = tk.StringVar(value=default_dir)
        self._var_uv_org_platform = tk.BooleanVar(value=bool(dl_cfg.get("organize_by_platform", True)))
        self._var_uv_org_uploader = tk.BooleanVar(value=bool(dl_cfg.get("organize_by_uploader", True)))
        self._var_uv_skip_existing = tk.BooleanVar(value=bool(dl_cfg.get("skip_existing", True)))
        self._var_uv_info_json = tk.BooleanVar(value=bool(yt_cfg.get("write_info_json", True)))
        self._var_uv_thumbnail = tk.BooleanVar(value=bool(yt_cfg.get("write_thumbnail", True)))
        var_detect_hint = tk.StringVar(value="")

        ttk.Label(form, text="URL:").grid(row=0, column=0, sticky="w")
        ent_url = ttk.Entry(form, textvariable=self._var_uv_url)
        ent_url.grid(row=0, column=1, sticky="ew", padx=(8, 0))
        ttk.Label(form, textvariable=var_detect_hint, foreground="#1a4480", font=("Segoe UI", 8)).grid(
            row=1, column=1, sticky="w", padx=(8, 0), pady=(2, 0)
        )

        quick = ttk.Frame(form)
        quick.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        ttk.Label(quick, text="Nền tảng").grid(row=0, column=0, sticky="w")
        cb_platform = ttk.Combobox(
            quick,
            textvariable=self._var_uv_platform,
            values=["Auto detect", "youtube", "tiktok", "facebook", "unknown"],
            state="readonly",
            width=16,
        )
        cb_platform.grid(row=0, column=1, sticky="w", padx=(6, 12))
        ttk.Label(quick, text="Loại URL").grid(row=0, column=2, sticky="w")
        cb_url_type = ttk.Combobox(
            quick,
            textvariable=self._var_uv_url_type,
            values=[
                "Auto detect",
                "single_video",
                "playlist",
                "channel",
                "profile",
                "unknown",
            ],
            state="readonly",
            width=16,
        )
        cb_url_type.grid(row=0, column=3, sticky="w", padx=(6, 12))
        ttk.Label(quick, text="Tối đa").grid(row=0, column=4, sticky="w")
        ttk.Entry(quick, textvariable=self._var_uv_max_videos, width=8).grid(row=0, column=5, sticky="w", padx=(6, 0))

        def _refresh_detect_hint(_e: Any = None) -> None:
            url = self._var_uv_url.get().strip()
            if not url:
                var_detect_hint.set("Auto detect: chờ nhập URL.")
                return
            auto_platform = detect_platform(url)
            auto_type = classify_url_type(url)
            picked_platform = self._var_uv_platform.get().strip() or "Auto detect"
            picked_type = self._var_uv_url_type.get().strip() or "Auto detect"
            use_platform = auto_platform if picked_platform.lower() in ("auto detect", "auto", "") else picked_platform
            use_type = auto_type if picked_type.lower() in ("auto detect", "auto", "") else picked_type
            var_detect_hint.set(
                f"Auto detect: {auto_platform}/{auto_type} • Sẽ tải theo: {use_platform}/{use_type}"
            )

        ent_url.bind("<KeyRelease>", _refresh_detect_hint)
        cb_platform.bind("<<ComboboxSelected>>", _refresh_detect_hint)
        cb_url_type.bind("<<ComboboxSelected>>", _refresh_detect_hint)
        _refresh_detect_hint()

        ttk.Label(form, text="Thư mục lưu:").grid(row=3, column=0, sticky="w", pady=(6, 0))
        od_frame = ttk.Frame(form)
        od_frame.grid(row=3, column=1, sticky="ew", padx=(8, 0), pady=(6, 0))
        od_frame.columnconfigure(0, weight=1)
        ttk.Entry(od_frame, textvariable=self._var_uv_out_dir).grid(row=0, column=0, sticky="ew")
        ttk.Button(od_frame, text="Chọn folder", command=self._on_uv_pick_folder).grid(row=0, column=1, padx=(8, 0))

        var_show_adv = tk.BooleanVar(value=False)
        adv_toggle_fr = ttk.Frame(form)
        adv_toggle_fr.grid(row=4, column=0, columnspan=2, sticky="w", pady=(8, 0))
        btn_adv = ttk.Button(adv_toggle_fr, text="Hiện nâng cao ▾")
        btn_adv.pack(side=tk.LEFT)
        ttk.Label(
            adv_toggle_fr,
            text="(tuỳ chọn tổ chức file, metadata)",
            foreground="#666",
            font=("Segoe UI", 8),
        ).pack(side=tk.LEFT, padx=(8, 0))

        adv_opts = ttk.Frame(form)
        adv_opts.grid(row=5, column=0, columnspan=2, sticky="w", pady=(4, 0))
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

        fb_cfg = ucfg.get("facebook_reels") or {}
        self._var_uv_fb_cookie.set(str(fb_cfg.get("cookie_path") or "").strip())
        self._var_uv_fb_max_collect.set(str(int(fb_cfg.get("max_collect") or 300)))
        self._var_uv_fb_max_scroll.set(str(int(fb_cfg.get("max_scroll_rounds") or 100)))
        self._var_uv_fb_scan_minutes.set(str(int(fb_cfg.get("max_scan_minutes") or 30)))
        self._var_uv_fb_scroll_until_end.set(bool(fb_cfg.get("scroll_until_end", True)))
        self._var_uv_fb_scan_status.set("Chưa quét. Dán URL tab Reels (hoặc profile) ở ô URL phía trên.")

        fb_fr = ttk.LabelFrame(
            host,
            text="Bước 2 — Facebook Reels: quét danh sách (Playwright) → bảng bên dưới cập nhật dần khi cuộn trang",
            padding=8,
        )
        fb_fr.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        fb_fr.columnconfigure(1, weight=1)
        ttk.Label(
            fb_fr,
            text=(
                "Quét public-only (không dùng cookies/session). "
                "Có thể tăng phút quét + bật «Cuộn tới hết trang» để quét sâu."
            ),
            wraplength=820,
            justify=tk.LEFT,
            foreground="#555",
        ).grid(row=0, column=0, columnspan=3, sticky="w")
        lim = ttk.Frame(fb_fr)
        lim.grid(row=1, column=0, columnspan=3, sticky="w", pady=(6, 0))
        ttk.Label(lim, text="Reel").grid(row=0, column=0, sticky="w")
        ttk.Entry(lim, textvariable=self._var_uv_fb_max_collect, width=5).grid(row=0, column=1, sticky="w", padx=(4, 10))
        ttk.Label(lim, text="Vòng").grid(row=0, column=2, sticky="w")
        ttk.Entry(lim, textvariable=self._var_uv_fb_max_scroll, width=5).grid(row=0, column=3, sticky="w", padx=(4, 10))
        ttk.Label(lim, text="Phút").grid(row=0, column=4, sticky="w")
        ttk.Entry(lim, textvariable=self._var_uv_fb_scan_minutes, width=5).grid(row=0, column=5, sticky="w", padx=(4, 10))
        ttk.Checkbutton(lim, text="Cuộn tới hết trang", variable=self._var_uv_fb_scroll_until_end).grid(
            row=0, column=6, sticky="w", padx=(4, 10)
        )
        ttk.Button(lim, text="Lưu giới hạn", command=self._on_uv_save_fb_reel_limits).grid(row=0, column=7, sticky="w")
        ttk.Label(fb_fr, textvariable=self._var_uv_fb_scan_status, wraplength=860, justify=tk.LEFT).grid(
            row=2, column=0, columnspan=3, sticky="w", pady=(6, 0)
        )
        fb_act = ttk.Frame(fb_fr)
        fb_act.grid(row=3, column=0, columnspan=3, sticky="w", pady=(6, 0))

        def _fb_busy_btn(text: str, cmd: Callable[[], None]) -> ttk.Button:
            b = ttk.Button(fb_act, text=text, command=cmd)
            b.pack(side=tk.LEFT, padx=(0, 8))
            self._uv_busy_disable_widgets.append(b)
            return b

        _fb_busy_btn("Quét Reels (Playwright)", self._on_uv_scan_fb_reels)
        ttk.Button(fb_act, text="Chọn hết", command=self._on_uv_fb_select_all).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(fb_act, text="Bỏ chọn", command=self._on_uv_fb_select_none).pack(side=tk.LEFT, padx=(0, 8))
        _fb_busy_btn("Tải reel đã chọn", self._on_uv_download_fb_reels_selected)
        _fb_busy_btn("Tải tất cả reel", self._on_uv_download_fb_reels_all)

        fb_tree_fr = ttk.Frame(fb_fr)
        fb_tree_fr.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(6, 0))
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
        self._tree_fb_reels.column("url", width=900, stretch=False)
        syf = ttk.Scrollbar(fb_tree_fr, orient="vertical", command=self._tree_fb_reels.yview)
        sxf = ttk.Scrollbar(fb_tree_fr, orient="horizontal", command=self._tree_fb_reels.xview)
        self._tree_fb_reels.configure(yscrollcommand=syf.set, xscrollcommand=sxf.set)
        self._tree_fb_reels.grid(row=0, column=0, sticky="ew")
        syf.grid(row=0, column=1, sticky="ns")
        sxf.grid(row=1, column=0, sticky="ew")

        self._var_uv_yt_scan_status.set(
            "Chưa quét. Dán URL kênh / tab Shorts / playlist YouTube ở ô URL phía trên rồi «Quét kênh (yt-dlp)»."
        )
        yt_fr = ttk.LabelFrame(
            host,
            text="Bước 3 — YouTube: quét danh sách video (yt-dlp flat-playlist) → chọn từng video hoặc tải hết",
            padding=8,
        )
        yt_fr.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        yt_fr.columnconfigure(1, weight=1)
        ttk.Label(
            yt_fr,
            text=(
                "Dùng cùng ô URL với Bước 1. Chỉ áp dụng khi Auto detect là youtube + channel hoặc playlist "
                "(ví dụ /@kênh/shorts, playlist ?list=…)."
            ),
            wraplength=820,
            justify=tk.LEFT,
            foreground="#555",
        ).grid(row=0, column=0, columnspan=3, sticky="w")
        yt_lim = ttk.Frame(yt_fr)
        yt_lim.grid(row=1, column=0, columnspan=3, sticky="w", pady=(6, 0))
        ttk.Label(yt_lim, text="Tối đa entry").grid(row=0, column=0, sticky="w")
        ttk.Entry(yt_lim, textvariable=self._var_uv_yt_list_max, width=6).grid(row=0, column=1, sticky="w", padx=(4, 12))
        ttk.Label(yt_lim, text="(1–2000, giới hạn tốc độ quét)", foreground="#666", font=("Segoe UI", 8)).grid(
            row=0, column=2, sticky="w"
        )
        ttk.Label(yt_fr, textvariable=self._var_uv_yt_scan_status, wraplength=860, justify=tk.LEFT).grid(
            row=2, column=0, columnspan=3, sticky="w", pady=(6, 0)
        )
        yt_act = ttk.Frame(yt_fr)
        yt_act.grid(row=3, column=0, columnspan=3, sticky="w", pady=(6, 0))

        def _yt_busy_btn(text: str, cmd: Callable[[], None]) -> ttk.Button:
            b = ttk.Button(yt_act, text=text, command=cmd)
            b.pack(side=tk.LEFT, padx=(0, 8))
            self._uv_busy_disable_widgets.append(b)
            return b

        _yt_busy_btn("Quét kênh (yt-dlp)", self._on_uv_scan_yt_channel)
        ttk.Button(yt_act, text="Chọn hết", command=self._on_uv_yt_select_all).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(yt_act, text="Bỏ chọn", command=self._on_uv_yt_select_none).pack(side=tk.LEFT, padx=(0, 8))
        _yt_busy_btn("Tải video đã chọn", self._on_uv_download_yt_selected)
        _yt_busy_btn("Tải tất cả video", self._on_uv_download_yt_all)

        yt_tree_fr = ttk.Frame(yt_fr)
        yt_tree_fr.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(6, 0))
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
        self._tree_yt_channel.column("title", width=260, stretch=False)
        self._tree_yt_channel.column("url", width=520, stretch=False)
        sy_yt = ttk.Scrollbar(yt_tree_fr, orient="vertical", command=self._tree_yt_channel.yview)
        sx_yt = ttk.Scrollbar(yt_tree_fr, orient="horizontal", command=self._tree_yt_channel.xview)
        self._tree_yt_channel.configure(yscrollcommand=sy_yt.set, xscrollcommand=sx_yt.set)
        self._tree_yt_channel.grid(row=0, column=0, sticky="ew")
        sy_yt.grid(row=0, column=1, sticky="ns")
        sx_yt.grid(row=1, column=0, sticky="ew")

        prog_fr = ttk.LabelFrame(host, text="Tiến trình (tránh treo cửa sổ)", padding=8)
        prog_fr.grid(row=5, column=0, sticky="ew", pady=(6, 0))
        prog_fr.columnconfigure(0, weight=1)
        self._var_uv_operation_status.set("Sẵn sàng — có thể thao tác.")
        ttk.Label(prog_fr, textvariable=self._var_uv_operation_status, wraplength=860, justify=tk.LEFT).grid(
            row=0, column=0, sticky="w"
        )
        self._uv_progress = ttk.Progressbar(prog_fr, mode="indeterminate", length=420)
        self._uv_progress.grid(row=1, column=0, sticky="ew", pady=(6, 0))

        act = ttk.Frame(host)
        act.grid(row=6, column=0, sticky="w", pady=(8, 0))

        row_a = ttk.Frame(act)
        row_a.pack(anchor="w")
        row_b = ttk.Frame(act)
        row_b.pack(anchor="w", pady=(4, 0))
        for text, cmd in (
            ("Kiểm tra URL", self._on_uv_check_url),
            ("Tải video", self._on_uv_download),
            ("Tiếp tục job cuối", self._on_uv_resume),
        ):
            b = ttk.Button(row_a, text=text, command=cmd)
            b.pack(side=tk.LEFT, padx=(0, 8))
            self._uv_busy_disable_widgets.append(b)
        row_a_adv = ttk.Frame(act)
        row_a_adv.pack(anchor="w", pady=(4, 0))
        for text, cmd in (
            ("Kiểm tra yt-dlp", self._on_uv_verify_ytdlp),
            ("Cập nhật yt-dlp", self._on_uv_ytdlp_check_and_update),
        ):
            b = ttk.Button(row_a_adv, text=text, command=cmd)
            b.pack(side=tk.LEFT, padx=(0, 8))
            self._uv_busy_disable_widgets.append(b)
        ttk.Button(row_b, text="Tạm dừng / Hủy", command=self._on_uv_pause).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(row_b, text="Mở thư mục lưu", command=self._on_uv_open_out_dir).pack(side=tk.LEFT, padx=(0, 8))
        b_refresh = ttk.Button(row_b, text="Làm mới danh sách", command=self._refresh_uv_library)
        b_refresh.pack(side=tk.LEFT, padx=(0, 8))
        self._uv_busy_disable_widgets.append(b_refresh)

        lib = ttk.LabelFrame(host, text="Bước 4 — Video đã tải (thư viện yt-dlp)", padding=8)
        lib.grid(row=7, column=0, sticky="nsew", pady=(8, 0))
        lib.columnconfigure(0, weight=1)
        lib.rowconfigure(0, weight=1)
        cols = ("platform", "title", "duration", "uploader", "status", "path")
        self._tree_uv = ttk.Treeview(lib, columns=cols, show="headings", height=8, selectmode="browse")
        heads = {
            "platform": "Platform",
            "title": "Title",
            "duration": "Duration",
            "uploader": "Uploader",
            "status": "Status",
            "path": "File path",
        }
        widths = {"platform": 86, "title": 180, "duration": 70, "uploader": 110, "status": 86, "path": 420}
        for c in cols:
            self._tree_uv.heading(c, text=heads[c])
            self._tree_uv.column(c, width=widths[c], stretch=True if c == "path" else False)
        sy = ttk.Scrollbar(lib, orient="vertical", command=self._tree_uv.yview)
        sx = ttk.Scrollbar(lib, orient="horizontal", command=self._tree_uv.xview)
        self._tree_uv.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)
        self._tree_uv.grid(row=0, column=0, sticky="nsew")
        sy.grid(row=0, column=1, sticky="ns")
        sx.grid(row=1, column=0, sticky="ew")

        ab = ttk.Frame(lib)
        ab.grid(row=2, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Button(ab, text="Preview", command=self._on_uv_preview_selected).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(ab, text="Open folder", command=self._on_uv_open_folder_selected).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(ab, text="Phân tích Reverse", command=self._on_uv_analyze_reverse).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(ab, text="Use in AI Video", command=self._on_uv_use_ai_video).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(ab, text="Xóa khỏi danh sách", command=self._on_uv_delete_selected).pack(side=tk.LEFT)

        logf = ttk.LabelFrame(host, text="Log tải (chi tiết)", padding=6)
        logf.grid(row=8, column=0, sticky="nsew", pady=(8, 0))
        logf.columnconfigure(0, weight=1)
        logf.rowconfigure(0, weight=1)
        self._txt_uv_log = tk.Text(logf, wrap="word", height=6)
        sl = ttk.Scrollbar(logf, orient="vertical", command=self._txt_uv_log.yview)
        self._txt_uv_log.configure(yscrollcommand=sl.set)
        self._txt_uv_log.grid(row=0, column=0, sticky="nsew")
        sl.grid(row=0, column=1, sticky="ns")
        self._txt_uv_log.insert("1.0", "Log chi tiết khi tải URL sẽ hiện ở đây.\n")
        self._txt_uv_log.configure(state="disabled")

        host.rowconfigure(7, weight=1)
        self._top.after(100, self._refresh_uv_library)
        self._top.after(150, lambda: self._sync_uv_download_scrollregion(scroll_to_content=False))


    def _uv_options_dict(self) -> dict[str, Any]:
        plat = self._var_uv_platform.get().strip()
        if plat.lower() in ("auto detect", ""):
            plat = detect_platform(self._var_uv_url.get().strip())
        ut = self._var_uv_url_type.get().strip()
        if ut.lower() in ("auto detect", ""):
            ut = classify_url_type(self._var_uv_url.get().strip())
        return {
            "platform": plat,
            "url_type": ut,
            "max_videos": int(self._var_uv_max_videos.get().strip() or "50"),
            "output_dir": self._var_uv_out_dir.get().strip(),
            "organize_by_platform": bool(self._var_uv_org_platform.get()),
            "organize_by_uploader": bool(self._var_uv_org_uploader.get()),
            "skip_existing": bool(self._var_uv_skip_existing.get()),
            "write_info_json": bool(self._var_uv_info_json.get()),
            "write_thumbnail": bool(self._var_uv_thumbnail.get()),
        }

    def _on_uv_pick_folder(self) -> None:
        d = filedialog.askdirectory(parent=self._top, title="Chọn thư mục lưu video")
        if d:
            self._var_uv_out_dir.set(d)
            if self._uv_downloader:
                self._uv_downloader.remember_output_dir(d)

    def _fb_reel_download_opts(self) -> dict[str, Any]:
        o = self._uv_options_dict()
        o["platform"] = "facebook"
        o["url_type"] = "single_video"
        o["max_videos"] = 1
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
        mc = max(10, min(500, mc))
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

    def _refresh_fb_reel_tree(self, urls: list[str]) -> None:
        self._uv_fb_reel_urls = list(urls)
        tr = self._tree_fb_reels
        if tr is None:
            return
        for x in tr.get_children():
            tr.delete(x)
        for i, u in enumerate(urls):
            tr.insert("", "end", iid=str(i), values=(str(i + 1), u))
        self._sync_uv_download_scrollregion(scroll_to_content=bool(urls))

    def _on_uv_fb_select_all(self) -> None:
        tr = self._tree_fb_reels
        if not tr:
            return
        tr.selection_set(tr.get_children())

    def _on_uv_fb_select_none(self) -> None:
        tr = self._tree_fb_reels
        if not tr:
            return
        tr.selection_remove(tr.selection())

    def _on_uv_scan_fb_reels(self) -> None:
        raw = self._var_uv_url.get().strip()
        if not raw:
            messagebox.showwarning("Quét Reels", "Nhập URL profile hoặc tab Reels ở ô URL phía trên.", parent=self._top)
            return
        raw_low = raw.lower().strip()
        ok_reels_path = bool(re.search(r"^https?://(?:[\w-]+\.)?facebook\.com/[^/]+/reels/?(?:[?#].*)?$", raw_low))
        ok_profile_reels_tab = bool(
            re.search(r"^https?://(?:[\w-]+\.)?facebook\.com/profile\.php\?[^#]*\bid=\d+", raw_low)
            and "sk=reels_tab" in raw_low
        )
        if not (ok_reels_path or ok_profile_reels_tab):
            messagebox.showwarning(
                "Quét Reels",
                "Nhập đúng URL Reels, ví dụ:\n"
                "- https://www.facebook.com/<profile>/reels\n"
                "- https://www.facebook.com/profile.php?id=<id>&sk=reels_tab",
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
        mode_txt = "cuộn tới hết trang" if till_end else "dừng theo vòng cuộn"
        self._uv_set_busy(
            True,
            f"Đang mở Playwright và quét tab Reels ({mode_txt}, tối đa {max_minutes} phút)…",
        )
        self._refresh_fb_reel_tree([])
        self._var_uv_fb_scan_status.set("Đang quét — bảng «URL reel» sẽ hiện dần…")

        def _status(msg: str) -> None:
            self._top.after(0, lambda m=msg: self._var_uv_fb_scan_status.set(m))

        def _partial(urls: list[str]) -> None:
            snap = list(urls)
            now = time.monotonic()
            if now - self._uv_last_partial_ui_ts < 0.45:
                return
            self._uv_last_partial_ui_ts = now

            def _apply() -> None:
                self._refresh_fb_reel_tree(snap)
                if snap:
                    self._var_uv_fb_scan_status.set(f"Đang quét… đã thấy {len(snap)} reel (cập nhật trực tiếp trong bảng).")

            self._top.after(0, _apply)

        def _work() -> None:
            res = scan_facebook_profile_reels_page(
                page_url=page_url,
                max_reels=max_reels,
                max_scroll_rounds=max_scroll,
                max_scan_minutes=max_minutes,
                scroll_until_end=till_end,
                status=_status,
                on_partial=_partial,
            )

            def _ui() -> None:
                self._uv_set_busy(False)
                if res.get("ok"):
                    items = res.get("items") or []
                    urls = [str(x.get("url") or "") for x in items if isinstance(x, dict)]
                    self._refresh_fb_reel_tree(urls)
                    self._var_uv_fb_scan_status.set(res.get("message") or f"{len(urls)} reel.")
                    messagebox.showinfo(
                        "Quét Reels",
                        f"{res.get('message', '')}\n\nChọn dòng trong bảng rồi «Tải reel đã chọn», hoặc «Tải tất cả reel».",
                        parent=self._top,
                    )
                else:
                    self._var_uv_fb_scan_status.set(str(res.get("message") or "Lỗi"))
                    messagebox.showerror("Quét Reels", str(res.get("message") or "Thất bại."), parent=self._top)

            self._top.after(0, _ui)

        threading.Thread(target=_work, daemon=True, name="uv_scan_fb_reels").start()

    def _on_uv_download_fb_reels_selected(self) -> None:
        tr = self._tree_fb_reels
        if not tr or not self._uv_fb_reel_urls:
            messagebox.showwarning("Tải reel", "Chưa có danh sách — hãy «Quét Reels» trước.", parent=self._top)
            return
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

    def _on_uv_download_fb_reels_all(self) -> None:
        if not self._uv_fb_reel_urls:
            messagebox.showwarning("Tải reel", "Chưa có danh sách — hãy «Quét Reels» trước.", parent=self._top)
            return
        self._run_uv_fb_reel_download_batch(list(self._uv_fb_reel_urls))

    def _run_uv_fb_reel_download_batch(self, urls: list[str]) -> None:
        if not self._uv_downloader:
            messagebox.showerror("Tải reel", "Module tải chưa sẵn sàng.", parent=self._top)
            return
        try:
            opts = self._fb_reel_download_opts()
        except (ValueError, tk.TclError, TypeError) as exc:  # noqa: BLE001
            messagebox.showerror("Tải reel", f"Tùy chọn không hợp lệ: {exc}", parent=self._top)
            return
        down = self._uv_downloader
        n = len(urls)
        self._uv_set_busy(True, f"Chuẩn bị tải {n} reel bằng yt-dlp…")

        def _batch() -> None:
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
            failed = 0
            failed_urls: list[str] = []
            for i, u in enumerate(urls, start=1):
                if down.is_cancel_requested():

                    def _stopped() -> None:
                        self._uv_set_busy(False)
                        self._refresh_uv_library()
                        messagebox.showinfo("Tải reel", "Đã dừng theo «Tạm dừng / Hủy».", parent=self._top)

                    self._top.after(0, _stopped)
                    return
                msg = f"Đang tải reel {i}/{n}…"
                self._top.after(0, lambda m=msg: self._var_uv_operation_status.set(m))
                self._top.after(0, lambda ii=i, uu=u: self._append_uv_log(f"[INFO] Reel {ii}/{n}: {uu}"))
                try:
                    job = down.create_download_job(u, opts)
                    jid = job["id"]
                    self._top.after(0, lambda j=jid: setattr(self, "_last_download_job_id", j))
                    done = down.run_download_job(jid)
                    if str(done.get("status") or "") != "completed":
                        failed += 1
                        failed_urls.append(u)
                        em = str(done.get("error_message") or "yt-dlp failed")
                        self._top.after(0, lambda uu=u, ee=em: self._append_uv_log(f"[FAILED] {uu} | {ee}"))
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    failed_urls.append(u)
                    self._top.after(0, lambda e=exc: self._append_uv_log(f"[ERROR] {e}"))

            def _done() -> None:
                self._uv_set_busy(False)
                self._refresh_uv_library()
                messagebox.showinfo(
                    "Tải reel",
                    f"Hoàn tất lệnh tải {n} reel (lỗi: {failed})."
                    + (f"\nReel lỗi: {len(failed_urls)} (đã ghi log)." if failed_urls else "")
                    + "\nXem log và bảng «Video đã tải».",
                    parent=self._top,
                )

            self._top.after(0, _done)

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
            lim = 500
        return max(1, min(2000, lim))

    def _refresh_yt_channel_tree(self, rows: list[dict[str, str]]) -> None:
        self._uv_yt_entry_rows = list(rows)
        tr = self._tree_yt_channel
        if tr is None:
            return
        for x in tr.get_children():
            tr.delete(x)
        for i, r in enumerate(rows):
            title = str(r.get("title") or "")
            url = str(r.get("url") or "")
            tr.insert("", "end", iid=str(i), values=(str(i + 1), title, url))
        self._sync_uv_download_scrollregion(scroll_to_content=bool(rows))

    def _on_uv_yt_select_all(self) -> None:
        tr = self._tree_yt_channel
        if not tr:
            return
        tr.selection_set(tr.get_children())

    def _on_uv_yt_select_none(self) -> None:
        tr = self._tree_yt_channel
        if not tr:
            return
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
        picked = self._var_uv_url_type.get().strip().lower()
        if picked not in ("auto detect", "auto", ""):
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
        if not self._uv_downloader:
            messagebox.showerror("Quét YouTube", "Module tải chưa sẵn sàng.", parent=self._top)
            return
        self._uv_set_busy(True, f"Đang quét danh sách YouTube (tối đa {lim} video, yt-dlp)…")
        self._refresh_yt_channel_tree([])
        self._var_uv_yt_scan_status.set("Đang gọi yt-dlp --flat-playlist…")

        down = self._uv_downloader

        def _work() -> None:
            res = down.list_flat_playlist_entries(raw, max_entries=lim)

            def _ui() -> None:
                self._uv_set_busy(False)
                if res.get("success"):
                    entries = res.get("entries") or []
                    rows = [e for e in entries if isinstance(e, dict) and str(e.get("url") or "").strip()]
                    self._refresh_yt_channel_tree(rows)
                    ptitle = str(res.get("playlist_title") or "").strip()
                    self._var_uv_yt_scan_status.set(
                        f"Đã quét {len(rows)} video."
                        + (f" — {ptitle}" if ptitle else "")
                    )
                    messagebox.showinfo(
                        "Quét YouTube",
                        f"{len(rows)} video trong danh sách.\n"
                        "Chọn dòng rồi «Tải video đã chọn», hoặc «Tải tất cả video».",
                        parent=self._top,
                    )
                else:
                    self._var_uv_yt_scan_status.set(str(res.get("error") or "Lỗi"))
                    messagebox.showerror("Quét YouTube", str(res.get("error") or "Thất bại."), parent=self._top)

            self._top.after(0, _ui)

        threading.Thread(target=_work, daemon=True, name="uv_scan_yt_channel").start()

    def _on_uv_download_yt_selected(self) -> None:
        tr = self._tree_yt_channel
        if not tr or not self._uv_yt_entry_rows:
            messagebox.showwarning("Tải YouTube", "Chưa có danh sách — hãy «Quét kênh (yt-dlp)» trước.", parent=self._top)
            return
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

    def _on_uv_download_yt_all(self) -> None:
        if not self._uv_yt_entry_rows:
            messagebox.showwarning("Tải YouTube", "Chưa có danh sách — hãy «Quét kênh (yt-dlp)» trước.", parent=self._top)
            return
        urls = [str(r.get("url") or "") for r in self._uv_yt_entry_rows]
        urls = [u for u in urls if u]
        self._run_uv_yt_channel_download_batch(urls)

    def _run_uv_yt_channel_download_batch(self, urls: list[str]) -> None:
        if not self._uv_downloader:
            messagebox.showerror("Tải YouTube", "Module tải chưa sẵn sàng.", parent=self._top)
            return
        try:
            opts = self._yt_channel_download_opts()
        except (ValueError, tk.TclError, TypeError) as exc:  # noqa: BLE001
            messagebox.showerror("Tải YouTube", f"Tùy chọn không hợp lệ: {exc}", parent=self._top)
            return
        down = self._uv_downloader
        n = len(urls)
        self._uv_set_busy(True, f"Chuẩn bị tải {n} video YouTube bằng yt-dlp…")

        def _batch() -> None:
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
            failed = 0
            failed_urls: list[str] = []
            for i, u in enumerate(urls, start=1):
                if down.is_cancel_requested():

                    def _stopped() -> None:
                        self._uv_set_busy(False)
                        self._refresh_uv_library()
                        messagebox.showinfo("Tải YouTube", "Đã dừng theo «Tạm dừng / Hủy».", parent=self._top)

                    self._top.after(0, _stopped)
                    return
                msg = f"Đang tải YouTube {i}/{n}…"
                self._top.after(0, lambda m=msg: self._var_uv_operation_status.set(m))
                self._top.after(0, lambda ii=i, uu=u: self._append_uv_log(f"[INFO] YouTube {ii}/{n}: {uu}"))
                try:
                    job = down.create_download_job(u, opts)
                    jid = job["id"]
                    self._top.after(0, lambda j=jid: setattr(self, "_last_download_job_id", j))
                    done = down.run_download_job(jid)
                    if str(done.get("status") or "") != "completed":
                        failed += 1
                        failed_urls.append(u)
                        em = str(done.get("error_message") or "yt-dlp failed")
                        self._top.after(0, lambda uu=u, ee=em: self._append_uv_log(f"[FAILED] {uu} | {ee}"))
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    failed_urls.append(u)
                    self._top.after(0, lambda e=exc: self._append_uv_log(f"[ERROR] {e}"))

            def _done() -> None:
                self._uv_set_busy(False)
                self._refresh_uv_library()
                messagebox.showinfo(
                    "Tải YouTube",
                    f"Hoàn tất lệnh tải {n} video (lỗi: {failed})."
                    + (f"\nURL lỗi: {len(failed_urls)} (đã ghi log)." if failed_urls else "")
                    + "\nXem log và bảng «Video đã tải».",
                    parent=self._top,
                )

            self._top.after(0, _done)

        threading.Thread(target=_batch, daemon=True, name="uv_yt_channel_batch").start()

    def _on_uv_check_url(self) -> None:
        url = self._var_uv_url.get().strip()
        if not url:
            messagebox.showwarning("Tải video", "Nhập URL trước.", parent=self._top)
            return
        if not self._uv_downloader:
            messagebox.showerror("Tải video", "Module tải chưa sẵn sàng.", parent=self._top)
            return
        self._uv_set_busy(
            True,
            "Đang quét URL bằng yt-dlp (playlist/kênh có thể mất vài chục giây — cửa sổ vẫn phản hồi)…",
        )
        down = self._uv_downloader

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
        if not self._uv_downloader:
            messagebox.showerror("Tải video", "Module tải chưa sẵn sàng.", parent=self._top)
            return
        url = self._var_uv_url.get().strip()
        if not url:
            messagebox.showwarning("Tải video", "Nhập URL.", parent=self._top)
            return
        try:
            opts = self._uv_options_dict()
        except (ValueError, tk.TclError, TypeError) as exc:  # noqa: BLE001
            messagebox.showerror("Tải video", f"Tùy chọn không hợp lệ: {exc}", parent=self._top)
            return
        self._uv_set_busy(True, "Đang kiểm tra yt-dlp trước khi tạo job…")
        down = self._uv_downloader

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

                def _err_create() -> None:
                    self._uv_set_busy(False)
                    messagebox.showerror("Tải video", str(exc), parent=self._top)

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
                down.run_download_job(jid)
                self._top.after(
                    0,
                    lambda: messagebox.showinfo(
                        "Tải video",
                        f"Hoàn tất job {jid}. Xem bảng Video đã tải.",
                        parent=self._top,
                    ),
                )
            except Exception as e:  # noqa: BLE001
                self._top.after(0, lambda err=e: messagebox.showerror("Tải video", str(err), parent=self._top))
            finally:
                self._top.after(0, self._uv_set_busy, False)
                self._top.after(0, self._refresh_uv_library)

        threading.Thread(target=_prepare_and_run, daemon=True, name="uv_download").start()

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
                self._uv_downloader.run_download_job(jid)
                self._top.after(
                    0,
                    lambda: messagebox.showinfo(
                        "Tải video",
                        f"Chạy lại job {jid} xong (file trùng có thể đã skip).",
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
        d = Path(self._var_uv_out_dir.get().strip() or ".")
        try:
            d.mkdir(parents=True, exist_ok=True)
            os.startfile(str(d.resolve()))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Tải video", str(exc), parent=self._top)

    def _refresh_uv_library(self) -> None:
        if self._tree_uv is None or not self._uv_downloader:
            return
        for x in self._tree_uv.get_children():
            self._tree_uv.delete(x)
        rows = self._uv_downloader.list_downloaded_videos()
        for i, r in enumerate(rows, start=1):
            vid = str(r.get("id") or "")
            if not vid:
                continue
            dur = r.get("duration") or 0
            try:
                ds = f"{float(dur):.1f}s"
            except (TypeError, ValueError):
                ds = str(dur)
            self._tree_uv.insert(
                "",
                "end",
                iid=vid,
                values=(
                    str(r.get("platform") or ""),
                    str(r.get("title") or "")[:120],
                    ds,
                    str(r.get("uploader") or "")[:40],
                    str(r.get("status") or ""),
                    str(r.get("video_path") or ""),
                ),
            )

    def _uv_selected_id(self) -> str | None:
        if not self._tree_uv:
            return None
        sel = self._tree_uv.selection()
        return str(sel[0]) if sel else None

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
        self._notebook.select(2)
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
        vid = self._uv_selected_id()
        if not vid or not self._uv_downloader:
            return
        if not messagebox.askyesno("Tải video", "Xóa khỏi thư viện? (có thể xóa cả file trên đĩa)", parent=self._top):
            return
        delete_file = messagebox.askyesno("Tải video", "Xóa luôn file video/thumbnail trên đĩa?", parent=self._top)
        self._uv_downloader.delete_downloaded_video(vid, delete_file=delete_file)
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
        if self._reverse_source_change_after:
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

    def _on_open_tool_folder(self) -> None:
        exe = Path(self._var_tool_exe.get().strip())
        folder = exe.parent if exe.parent.exists() else _EXTERNAL_TOOL_DIR
        try:
            os.startfile(str(folder))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", f"Không mở được thư mục tool:\n{exc}", parent=self._top)

    def _on_launch_tool(self) -> None:
        exe = Path(self._var_tool_exe.get().strip())
        if not exe.is_file():
            messagebox.showwarning("AI Video", f"Không tìm thấy Veo3Studio.exe:\n{exe}", parent=self._top)
            return
        try:
            subprocess.Popen([str(exe)], cwd=str(exe.parent))
            messagebox.showinfo("AI Video", f"Đã mở tool:\n{exe}", parent=self._top)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", f"Mở tool thất bại:\n{exc}", parent=self._top)
