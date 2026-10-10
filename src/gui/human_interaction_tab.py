"""
Tab GUI: Tương tác giống người dùng — hai trang «Đăng nhập» và «Tương tác» tách riêng.
"""

from __future__ import annotations

import os
import queue
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable

from loguru import logger

from src.gui.cookie_capture import run_fb_cookie_capture_dialog, run_fb_profile_browser_dialog
from src.gui.treeview_shortcuts import install_treeview_shortcuts
from src.gui.ui_responsiveness import run_background_then_main, schedule_on_main_thread
from src.models.mapped_account import MappedAccount
from src.services.human_interaction_profile import PROFILES, resolve_profile
from src.services.human_interaction_pool import (
    HumanInteractionPool,
    compute_pool_join_timeout_sec,
    validate_pool_start,
)
from src.utils.account_proxy_mapper import (
    AccountProxyMappingError,
    apply_mapped_secrets_to_vault,
    count_unique_proxy_servers,
    assert_proxy_exclusive_among_accounts,
    ensure_mapped_proxy_live,
    export_mapped_accounts_to_registry,
    load_registry_proxy_index,
    classify_account_live,
    live_result_rank,
    filter_lines_by_live_proxy,
    account_line_format_label,
    apply_imported_cookies_to_accounts,
    map_accounts_with_proxies,
    map_pasted_accounts_for_live_check,
    parse_proxy_line_to_network,
    proxy_identity_key_for_account,
    proxy_identity_key_for_network,
    _proxy_blocked_for_candidate,
    normalize_account_line_format,
    ACCOUNT_LINE_FORMAT_LABELS,
    attach_imported_cookie,
    mapped_account_to_account_dict,
    proxy_dict_to_network,
    persist_mapped_proxy_to_accounts_json,
    read_lines_file,
    reassign_proxies_from_pool,
    refresh_mapped_accounts_storage,
)
from src.utils.account_browser_profile import (
    default_cookie_path,
    delete_account_browser_bundle,
)
from src.utils.grid_layout_manager import GridWindowSlot, compute_grid_layout, get_screen_resolution
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.human_interaction_settings import (
    load_human_interaction_settings,
    load_interaction_queue_from_settings,
    load_login_queue_from_settings,
    save_human_interaction_settings,
)

_STATUS_VI = {
    "pending": "Đang chờ",
    "waiting": "Đang chờ slot",
    "running": "Đang chạy",
    "proxy_error": "Lỗi Proxy",
    "proxy_busy": "Proxy bận",
    "login_failed": "Lỗi đăng nhập",
    "login_ok": "Đăng nhập OK",
    "success": "Thành công",
    "error": "Lỗi",
}

_FONT_UI = ("Segoe UI", 9)
_FONT_MONO = ("Consolas", 9)
_FONT_HINT = ("Segoe UI", 8)
_FONT_STATUS = ("Segoe UI", 10)

# Thanh trạng thái — một màu ổn định, không nhấp nền.
_C_IDLE_BG = "#f8fafc"
_C_IDLE_FG = "#334155"
_C_IDLE_LED = "#94a3b8"
_C_RUN_BG = "#ecfdf5"
_C_RUN_FG = "#065f46"
_C_RUN_LED = "#059669"
_C_STOPPING_BG = "#fffbeb"
_C_STOPPING_FG = "#92400e"
_C_STOPPING_LED = "#d97706"
_C_LOGIN_HINT_BG = "#ecfdf5"
_C_LOGIN_HINT_FG = "#047857"
_C_INTER_HINT_BG = "#eff6ff"
_C_INTER_HINT_FG = "#1d4ed8"
_C_HEALTH_BG = "#eef2ff"
_C_HEALTH_FG = "#4338ca"
_C_BTN_MERGE = "#7c3aed"
_C_BTN_MERGE_H = "#6d28d9"
_C_BTN_PROXY = "#d97706"
_C_BTN_PROXY_H = "#b45309"
_C_BTN_SAVE = "#64748b"
_C_BTN_SAVE_H = "#475569"
_C_BTN_SECONDARY = "#0ea5e9"
_C_BTN_SECONDARY_H = "#0284c7"
_C_BTN_DANGER = "#dc2626"
_C_BTN_DANGER_H = "#b91c1c"

_TREE_STATUS_TAGS: dict[str, tuple[str, str, str]] = {
    "pending": ("status_pending", "#f8fafc", "#64748b"),
    "waiting": ("status_waiting", "#fef9c3", "#854d0e"),
    "running": ("status_running", "#dbeafe", "#1e40af"),
    "proxy_error": ("status_proxy", "#fee2e2", "#991b1b"),
    "proxy_busy": ("status_proxy", "#fee2e2", "#991b1b"),
    "login_failed": ("status_fail", "#ffe4e6", "#9f1239"),
    "login_ok": ("status_ok", "#d1fae5", "#065f46"),
    "success": ("status_ok", "#d1fae5", "#065f46"),
    "error": ("status_fail", "#ffe4e6", "#9f1239"),
    "cancelled": ("status_muted", "#f1f5f9", "#94a3b8"),
}


def _flat_btn(
    master: tk.Misc,
    *,
    text: str,
    command: Any,
    bg: str,
    active_bg: str,
    fg: str = "white",
    state: str = tk.NORMAL,
) -> tk.Button:
    """Nút phẳng cùng cỡ — chiều cao và chữ giống nhau trên mọi hàng."""
    return tk.Button(
        master,
        text=text,
        command=command,
        font=("Segoe UI", 9),
        bg=bg,
        fg=fg,
        activebackground=active_bg,
        activeforeground=fg,
        relief=tk.FLAT,
        padx=10,
        pady=3,
        cursor="hand2",
        state=state,
        highlightthickness=0,
        bd=0,
    )


class _FlowBar(ttk.Frame):
    """Hàng nút cùng khoảng cách, tự xuống dòng khi khung hẹp."""

    def __init__(self, master: tk.Misc) -> None:
        super().__init__(master)
        self._items: list[tk.Widget] = []
        self._width = 0
        self.bind("<Configure>", self._on_configure)

    def add(self, widget: tk.Widget) -> tk.Widget:
        """Thêm nút vào hàng và giữ khoảng cách đều."""
        self._items.append(widget)
        widget.grid(row=0, column=len(self._items) - 1, padx=(0, 6), pady=2, sticky="w")
        return widget

    def _on_configure(self, event: tk.Event) -> None:
        if event.widget is not self or event.width < 80:
            return
        if abs(event.width - self._width) < 12:
            return
        self._width = int(event.width)
        x = 0
        row = 0
        col = 0
        for widget in self._items:
            need = int(widget.winfo_reqwidth()) + 6
            if col and x + need > event.width:
                row += 1
                col = 0
                x = 0
            widget.grid_configure(row=row, column=col)
            col += 1
            x += need


def _apply_human_ttk_styles(root: tk.Tk) -> ttk.Style:
    """Style ttk riêng cho tab Human (notebook, bảng, nhãn)."""
    style = ttk.Style(root)

    def _clone_layout(dst: str, src: str) -> None:
        try:
            style.layout(dst, style.layout(src))
        except tk.TclError:
            pass

    _clone_layout("Human.TNotebook", "TNotebook")
    _clone_layout("Human.TLabelframe", "TLabelframe")
    _clone_layout("Human.Treeview", "Treeview")
    _clone_layout("Human.Treeview.Item", "Treeview.Item")
    try:
        style.configure(
            "Human.TNotebook.Tab",
            padding=[14, 7],
            font=("Segoe UI", 9, "bold"),
        )
        style.map("Human.TNotebook.Tab", background=[("selected", "#dbeafe")])
        style.configure("Human.TLabelframe.Label", font=("Segoe UI", 9, "bold"), foreground="#334155")
        style.configure("Human.Treeview", rowheight=24, font=("Segoe UI", 9))
        style.configure(
            "Human.Treeview.Heading",
            font=("Segoe UI", 9, "bold"),
            background="#e2e8f0",
            foreground="#1e293b",
        )
    except tk.TclError:
        pass
    return style


def _tree_tag_for_status(status: str) -> str:
    key = str(status or "pending").strip() or "pending"
    return _TREE_STATUS_TAGS.get(key, _TREE_STATUS_TAGS["pending"])[0]


def _configure_tree_status_tags(tr: ttk.Treeview) -> None:
    seen: set[str] = set()
    for _st, (tag, bg, fg) in _TREE_STATUS_TAGS.items():
        if tag in seen:
            continue
        seen.add(tag)
        try:
            tr.tag_configure(tag, background=bg, foreground=fg)
        except tk.TclError:
            pass


def _human_vertical_scroll(host: ttk.Frame) -> ttk.Frame:
    """Canvas + thanh cuộn dọc; trả về frame bên trong để đặt nội dung dài."""
    host.columnconfigure(0, weight=1)
    host.rowconfigure(0, weight=1)
    canvas = tk.Canvas(host, highlightthickness=0, borderwidth=0)
    vsb = ttk.Scrollbar(host, orient=tk.VERTICAL, command=canvas.yview)
    canvas.configure(yscrollcommand=vsb.set)
    inner = ttk.Frame(canvas)
    inner.columnconfigure(0, weight=1)
    win_id = canvas.create_window((0, 0), window=inner, anchor="nw")

    def _sync_region(_event: tk.Event | None = None) -> None:
        bbox = canvas.bbox("all")
        if bbox:
            canvas.configure(scrollregion=bbox)

    def _on_canvas_configure(event: tk.Event) -> None:
        if event.widget is not canvas:
            return
        cw = int(event.width)
        if cw > 1:
            canvas.itemconfigure(win_id, width=cw)
        _sync_region()

    def _on_wheel(event: tk.Event) -> None:
        delta = getattr(event, "delta", 0) or 0
        if delta:
            canvas.yview_scroll(int(-delta / 120), "units")
            return
        num = getattr(event, "num", None)
        if num == 4:
            canvas.yview_scroll(-3, "units")
        elif num == 5:
            canvas.yview_scroll(3, "units")

    inner.bind("<Configure>", _sync_region)
    canvas.bind("<Configure>", _on_canvas_configure)
    for widget in (canvas, inner):
        widget.bind("<MouseWheel>", _on_wheel)
        widget.bind("<Button-4>", _on_wheel)
        widget.bind("<Button-5>", _on_wheel)

    canvas.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    return inner


def build_human_interaction_tab(
    parent: ttk.Frame,
    root: tk.Tk,
    *,
    on_accounts_registry_changed: Callable[[list[str]], None] | None = None,
) -> None:
    """Gắn tab «Tương tác người dùng» — nhập liệu, ghép, chạy từng dòng hoặc cả danh sách."""
    persisted = load_human_interaction_settings()
    parent.columnconfigure(0, weight=1)
    parent.rowconfigure(0, weight=0)
    parent.rowconfigure(1, weight=1)

    state: dict[str, Any] = {
        "mapped_login": [],
        "mapped_interaction": [],
        "pool": None,
        "pool_stopping": False,
        "stop_wait_tip": None,
        "capture_sessions": set(),
        "capture_slot_by_account": {},
        "profile_browser_sessions": {},
        "accounts_path": "",
        "proxies_path": "",
        "_save_debounce_id": None,
        "pool_login_only": False,
        "pool_generation": 0,
        "pool_auto_continue_round": 0,
        "btn_stop_all": [],
        "_pulse_after": None,
        "_pulse_phase": 0,
    }

    _apply_human_ttk_styles(root)

    _threads_raw = persisted.get("threads")
    try:
        _threads_default = max(1, int(_threads_raw))
    except (TypeError, ValueError):
        _threads_default = 4
    _cols_raw = persisted.get("grid_cols")
    try:
        _grid_cols_default = max(1, min(8, int(_cols_raw)))
    except (TypeError, ValueError):
        _grid_cols_default = 4
    _rows_raw = persisted.get("grid_rows")
    try:
        _grid_rows_default = max(0, min(4, int(_rows_raw)))
    except (TypeError, ValueError):
        _grid_rows_default = 0

    var_acc = tk.StringVar(value=str(persisted.get("accounts_path") or ""))
    var_px = tk.StringVar(value=str(persisted.get("proxies_path") or ""))
    var_threads = tk.IntVar(value=_threads_default)
    var_grid_cols = tk.IntVar(value=_grid_cols_default)
    var_grid_rows = tk.IntVar(value=_grid_rows_default)
    var_headless = tk.BooleanVar(value=bool(persisted.get("headless", False)))
    var_profile = tk.StringVar(value=str(persisted.get("profile") or "normal"))
    try:
        _like_pct_default = max(0, min(100, int(persisted.get("like_rate_pct", 30))))
    except (TypeError, ValueError):
        _like_pct_default = 30
    try:
        _comment_pct_default = max(0, min(100, int(persisted.get("comment_rate_pct", 10))))
    except (TypeError, ValueError):
        _comment_pct_default = 10
    var_like_pct = tk.IntVar(value=_like_pct_default)
    var_comment_pct = tk.IntVar(value=_comment_pct_default)
    var_virtual_cursor = tk.BooleanVar(value=bool(persisted.get("virtual_cursor", True)))
    var_ai_comments = tk.BooleanVar(value=bool(persisted.get("ai_comments", True)))
    var_summary = tk.StringVar(value="Chọn tab bên dưới: Đăng nhập hoặc Tương tác")
    var_run_status = tk.StringVar(value="● Sẵn sàng — chưa có tiến trình")

    run_banner = tk.Frame(parent, bg=_C_IDLE_BG, padx=8, pady=4, highlightthickness=1, highlightbackground="#e2e8f0")
    run_banner.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 0))
    lbl_run_led = tk.Label(
        run_banner,
        text="●",
        font=("Segoe UI", 12),
        bg=_C_IDLE_BG,
        fg=_C_IDLE_LED,
        width=2,
    )
    lbl_run_led.pack(side=tk.LEFT, padx=(0, 6))
    lbl_run_status = tk.Label(
        run_banner,
        textvariable=var_run_status,
        font=_FONT_STATUS,
        bg=_C_IDLE_BG,
        fg=_C_IDLE_FG,
        anchor="w",
    )
    lbl_run_status.pack(side=tk.LEFT, fill=tk.X, expand=True)
    btn_stop_global = _flat_btn(
        run_banner,
        text="Dừng",
        command=lambda: None,
        bg=_C_BTN_DANGER,
        active_bg=_C_BTN_DANGER_H,
        state=tk.DISABLED,
    )
    btn_stop_global.pack(side=tk.RIGHT, padx=(8, 0))

    # --- Hai TRANG chính (không gộp chung một màn hình) ---
    nb_main = ttk.Notebook(parent, padding=2, style="Human.TNotebook")
    nb_main.grid(row=1, column=0, sticky="nsew", padx=4, pady=4)

    page_login = ttk.Frame(nb_main, padding=6)
    page_interaction = ttk.Frame(nb_main, padding=6)
    nb_main.add(page_login, text="  🔐  ĐĂNG NHẬP  ")
    nb_main.add(page_interaction, text="  👤  TƯƠNG TÁC  ")
    page_login.columnconfigure(0, weight=1)
    page_login.rowconfigure(0, weight=1)
    page_interaction.columnconfigure(0, weight=1)
    page_interaction.rowconfigure(0, weight=1)

    login_paned = ttk.Panedwindow(page_login, orient=tk.VERTICAL)
    login_paned.grid(row=0, column=0, sticky="nsew")
    login_scroll_host = ttk.Frame(login_paned)
    login_paned.add(login_scroll_host, weight=1)
    login_inner = _human_vertical_scroll(login_scroll_host)
    login_inner.columnconfigure(0, weight=1)

    interaction_paned = ttk.Panedwindow(page_interaction, orient=tk.VERTICAL)
    interaction_paned.grid(row=0, column=0, sticky="nsew")
    interaction_scroll_host = ttk.Frame(interaction_paned)
    interaction_paned.add(interaction_scroll_host, weight=1)
    interaction_inner = _human_vertical_scroll(interaction_scroll_host)
    interaction_inner.columnconfigure(0, weight=1)
    interaction_bottom = ttk.Frame(interaction_paned)
    interaction_paned.add(interaction_bottom, weight=3)
    interaction_bottom.columnconfigure(0, weight=1)
    interaction_bottom.rowconfigure(0, weight=1)

    hint_login = tk.Frame(login_inner, bg=_C_LOGIN_HINT_BG, highlightthickness=0)
    hint_login.grid(row=0, column=0, sticky="ew", pady=(0, 8))
    lbl_hint_login = tk.Label(
        hint_login,
        text="🔐  Bước 1 — Dán TK + Proxy → Ghép → Đăng nhập. Tài khoản OK ở lại tab này cho đến khi bạn mở tab «Tương tác».",
        font=("Segoe UI", 9),
        bg=_C_LOGIN_HINT_BG,
        fg=_C_LOGIN_HINT_FG,
        wraplength=640,
        justify=tk.LEFT,
        padx=10,
        pady=8,
    )
    lbl_hint_login.pack(anchor="w", fill=tk.X)

    hint_interaction = tk.Frame(interaction_inner, bg=_C_INTER_HINT_BG, highlightthickness=0)
    hint_interaction.grid(row=0, column=0, sticky="ew", pady=(0, 8))
    lbl_hint_interaction = tk.Label(
        hint_interaction,
        text="👤  Bước 2 — Mở tab này để nhận tài khoản Đăng nhập OK. Chạy tương tác tại đây.",
        font=("Segoe UI", 9),
        bg=_C_INTER_HINT_BG,
        fg=_C_INTER_HINT_FG,
        wraplength=640,
        justify=tk.LEFT,
        padx=10,
        pady=8,
    )
    lbl_hint_interaction.pack(anchor="w", fill=tk.X)

    # --- Trang Đăng nhập: nguồn dữ liệu (gọn) ---
    step1 = ttk.LabelFrame(
        login_inner, text="Nguồn dữ liệu — account ↔ proxy (dòng 1↔1)", padding=4, style="Human.TLabelframe"
    )
    step1.grid(row=1, column=0, sticky="ew", pady=(0, 6))
    step1.columnconfigure(0, weight=1)
    step1.rowconfigure(0, weight=0)

    fmt_row = ttk.Frame(step1)
    fmt_row.grid(row=0, column=0, sticky="ew", pady=(0, 4))
    ttk.Label(fmt_row, text="Định dạng nick", font=_FONT_UI).pack(side=tk.LEFT, padx=(0, 6))
    _fmt_labels = [label for _fid, label in ACCOUNT_LINE_FORMAT_LABELS]
    _fmt_by_label = {label: fid for fid, label in ACCOUNT_LINE_FORMAT_LABELS}
    _saved_fmt = normalize_account_line_format(str(persisted.get("account_line_format") or "mail"))
    var_account_format = tk.StringVar(value=account_line_format_label(_saved_fmt))
    cmb_fmt = ttk.Combobox(
        fmt_row,
        textvariable=var_account_format,
        values=_fmt_labels,
        state="readonly",
        width=36,
    )
    cmb_fmt.pack(side=tk.LEFT, fill=tk.X, expand=True)

    def _current_account_format() -> str:
        """Id định dạng đang chọn — dùng khi ghép dòng và đăng nhập."""
        return _fmt_by_label.get(var_account_format.get().strip(), "mail")

    nb_in = ttk.Notebook(step1)
    nb_in.grid(row=1, column=0, sticky="nsew")

    tab_paste = ttk.Frame(nb_in, padding=4)
    tab_file = ttk.Frame(nb_in, padding=4)
    nb_in.add(tab_paste, text="  Dán trực tiếp  ")
    nb_in.add(tab_file, text="  Chọn file  ")

    tab_paste.columnconfigure(0, weight=1)
    tab_paste.rowconfigure(2, weight=1, minsize=150)

    lbl_paste_help = ttk.Label(
        tab_paste,
        text="",
        font=_FONT_HINT,
        wraplength=640,
    )
    lbl_paste_help.grid(row=0, column=0, sticky="ew", pady=(0, 4))

    def _apply_format_hint(*_args: object) -> None:
        """Đổi dòng hướng dẫn theo định dạng nick đang chọn."""
        fid = _current_account_format()
        if fid == "cookie":
            order = "UID | mật khẩu | 2FA | cookie | mail khôi phục | mật khẩu mail"
            extra = " Cookie (c_user/xs hoặc JSON) được nạp trước khi đăng nhập."
        elif fid == "auto":
            order = "Tự nhận: trường 4 là email → định dạng mail; còn lại → cookie"
            extra = ""
        else:
            order = "uid | pass | 2FA | mail | pass mail | mail khôi phục"
            extra = ""
        lbl_paste_help.configure(
            text=(
                f"Tài khoản — {order}. Phân tách | hoặc Tab từ Excel.{extra} "
                "Proxy: host:port:user:pass | socks5://ip:port:user:pass | http://user:pass@host:port"
            )
        )

    var_account_format.trace_add("write", _apply_format_hint)
    _apply_format_hint()

    editors = ttk.Frame(tab_paste)
    editors.grid(row=2, column=0, sticky="nsew", pady=(2, 0))
    editors.columnconfigure(0, weight=3, minsize=220)
    editors.columnconfigure(1, weight=2, minsize=220)
    editors.rowconfigure(0, weight=1)

    acc_wrap = ttk.LabelFrame(editors, text="Tài khoản", padding=4)
    px_wrap = ttk.LabelFrame(editors, text="Proxy", padding=4)
    acc_wrap.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
    px_wrap.grid(row=0, column=1, sticky="nsew")

    acc_box = ttk.Frame(acc_wrap)
    px_box = ttk.Frame(px_wrap)
    acc_box.pack(fill=tk.BOTH, expand=True)
    px_box.pack(fill=tk.BOTH, expand=True)
    acc_box.columnconfigure(0, weight=1)
    acc_box.rowconfigure(0, weight=1)
    px_box.columnconfigure(0, weight=1)
    px_box.rowconfigure(0, weight=1)

    txt_acc = tk.Text(acc_box, height=6, wrap="none", font=_FONT_MONO)
    txt_px = tk.Text(px_box, height=6, wrap="none", font=_FONT_MONO)
    sy_acc = ttk.Scrollbar(acc_box, orient=tk.VERTICAL, command=txt_acc.yview)
    sy_px = ttk.Scrollbar(px_box, orient=tk.VERTICAL, command=txt_px.yview)
    txt_acc.configure(yscrollcommand=sy_acc.set)
    txt_px.configure(yscrollcommand=sy_px.set)
    txt_acc.grid(row=0, column=0, sticky="nsew")
    sy_acc.grid(row=0, column=1, sticky="ns")
    txt_px.grid(row=0, column=0, sticky="nsew")
    sy_px.grid(row=0, column=1, sticky="ns")

    var_input_counts = tk.StringVar(value="Tài khoản: 0  |  Proxy: 0  |  Proxy LIVE: chưa kiểm tra")
    lbl_input_counts = ttk.Label(
        tab_paste,
        textvariable=var_input_counts,
        font=("Segoe UI", 9, "bold"),
        foreground="#0f172a",
    )
    lbl_input_counts.grid(row=3, column=0, sticky="w", pady=(6, 0))

    paste_btns = _FlowBar(tab_paste)
    paste_btns.grid(row=4, column=0, sticky="ew", pady=(4, 0))

    tab_file.columnconfigure(1, weight=1)
    ttk.Label(tab_file, text="File tài khoản (.txt)").grid(row=0, column=0, sticky="w", pady=4)
    ttk.Entry(tab_file, textvariable=var_acc).grid(row=0, column=1, sticky="ew", padx=6)
    ttk.Label(tab_file, text="File proxy (.txt)").grid(row=1, column=0, sticky="w", pady=4)
    ttk.Entry(tab_file, textvariable=var_px).grid(row=1, column=1, sticky="ew", padx=6)
    var_file_counts = tk.StringVar(value="File: chưa đếm")
    ttk.Label(
        tab_file,
        textvariable=var_file_counts,
        font=("Segoe UI", 9, "bold"),
    ).grid(row=3, column=0, columnspan=4, sticky="w", pady=(6, 0))
    ttk.Label(
        tab_file,
        text="Dùng tab này khi đã có sẵn file trên máy. Tab «Dán trực tiếp» được ưu tiên nếu cả hai đều có nội dung.",
        font=_FONT_HINT,
        wraplength=640,
    ).grid(row=4, column=0, columnspan=4, sticky="w", pady=(4, 0))

    if persisted.get("accounts_text"):
        txt_acc.insert("1.0", str(persisted.get("accounts_text")))
    if persisted.get("proxies_text"):
        txt_px.insert("1.0", str(persisted.get("proxies_text")))

    # Cấu hình lưới cửa sổ (chỉ đăng nhập đa luồng)
    cfg_login = ttk.LabelFrame(
        login_inner, text="Cấu hình lưới trình duyệt (đăng nhập song song)", padding=4, style="Human.TLabelframe"
    )
    cfg_login.grid(row=2, column=0, sticky="ew", pady=(0, 6))
    ttk.Label(cfg_login, text="Luồng", font=_FONT_UI).grid(row=0, column=0, sticky="w", padx=(0, 4))
    ttk.Spinbox(cfg_login, from_=1, to=24, textvariable=var_threads, width=5).grid(row=0, column=1, sticky="w")
    ttk.Label(cfg_login, text="Cột", font=_FONT_UI).grid(row=0, column=2, sticky="w", padx=(12, 4))
    ttk.Spinbox(cfg_login, from_=1, to=8, textvariable=var_grid_cols, width=4).grid(row=0, column=3, sticky="w")
    ttk.Label(cfg_login, text="Hàng", font=_FONT_UI).grid(row=0, column=4, sticky="w", padx=(12, 4))
    ttk.Spinbox(cfg_login, from_=0, to=4, textvariable=var_grid_rows, width=4).grid(row=0, column=5, sticky="w")
    lbl_grid_hint = ttk.Label(cfg_login, text="", font=_FONT_HINT, foreground="#555", wraplength=720)
    lbl_grid_hint.grid(row=1, column=0, columnspan=6, sticky="w", pady=(4, 0))

    login_toolbar = ttk.LabelFrame(login_inner, text="Thao tác đăng nhập", padding=6, style="Human.TLabelframe")
    login_toolbar.grid(row=3, column=0, sticky="ew", pady=(0, 4))
    data_btns = _FlowBar(login_toolbar)
    data_btns.pack(fill=tk.X)
    login_btns = _FlowBar(login_toolbar)
    login_btns.pack(fill=tk.X, pady=(4, 0))
    login_row_btns = _FlowBar(login_toolbar)
    login_row_btns.pack(fill=tk.X, pady=(4, 0))
    lbl_login_summary = ttk.Label(login_toolbar, textvariable=var_summary, font=_FONT_HINT)
    lbl_login_summary.pack(anchor="e", pady=(4, 0))

    login_table_fr = ttk.LabelFrame(
        login_paned, text="Danh sách chờ đăng nhập", padding=4, style="Human.TLabelframe"
    )
    login_paned.add(login_table_fr, weight=4)
    login_table_fr.columnconfigure(0, weight=1)
    login_table_fr.rowconfigure(0, weight=1)

    # --- Trang Tương tác ---
    cfg_interaction = ttk.LabelFrame(
        interaction_inner, text="Cấu hình tương tác", padding=4, style="Human.TLabelframe"
    )
    cfg_interaction.grid(row=1, column=0, sticky="ew", pady=(0, 6))
    ttk.Label(cfg_interaction, text="Profile", font=_FONT_UI).grid(row=0, column=0, sticky="w", padx=(0, 4))
    ttk.Combobox(
        cfg_interaction,
        textvariable=var_profile,
        values=["auto", *list(PROFILES.keys())],
        state="readonly",
        width=10,
    ).grid(row=0, column=1, sticky="w")
    ttk.Checkbutton(cfg_interaction, text="Headless", variable=var_headless).grid(
        row=0, column=2, sticky="w", padx=(12, 0)
    )
    ttk.Label(cfg_interaction, text="Luồng", font=_FONT_UI).grid(row=0, column=3, sticky="w", padx=(12, 4))
    ttk.Spinbox(cfg_interaction, from_=1, to=24, textvariable=var_threads, width=5).grid(row=0, column=4, sticky="w")
    ttk.Label(cfg_interaction, text="Cột", font=_FONT_UI).grid(row=0, column=5, sticky="w", padx=(12, 4))
    ttk.Spinbox(cfg_interaction, from_=1, to=8, textvariable=var_grid_cols, width=4).grid(row=0, column=6, sticky="w")
    ttk.Label(cfg_interaction, text="Hàng", font=_FONT_UI).grid(row=0, column=7, sticky="w", padx=(8, 4))
    ttk.Spinbox(cfg_interaction, from_=0, to=4, textvariable=var_grid_rows, width=4).grid(row=0, column=8, sticky="w")
    ttk.Label(cfg_interaction, text="Like %", font=_FONT_UI).grid(row=1, column=0, sticky="w", pady=(6, 0))
    ttk.Spinbox(cfg_interaction, from_=0, to=100, textvariable=var_like_pct, width=5).grid(
        row=1, column=1, sticky="w", pady=(6, 0)
    )
    ttk.Label(cfg_interaction, text="Comment %", font=_FONT_UI).grid(row=1, column=2, sticky="w", padx=(12, 4), pady=(6, 0))
    ttk.Spinbox(cfg_interaction, from_=0, to=100, textvariable=var_comment_pct, width=5).grid(
        row=1, column=3, sticky="w", pady=(6, 0)
    )
    ttk.Checkbutton(cfg_interaction, text="Con trỏ ảo", variable=var_virtual_cursor).grid(
        row=1, column=4, sticky="w", padx=(12, 0), pady=(6, 0)
    )
    ttk.Checkbutton(cfg_interaction, text="Comment AI", variable=var_ai_comments).grid(
        row=1, column=5, sticky="w", padx=(8, 0), pady=(6, 0)
    )
    ttk.Label(
        cfg_interaction,
        text=(
            "Like/Comment = tỷ lệ trên số lần cuộn bảng tin. "
            "Cột 6 và Hàng 3 = 18 cửa sổ nhỏ chạy cùng lúc. Hàng 0 thì tự chia. "
            "«Luồng» theo cột × hàng; tài khoản còn lại vào hàng đợi."
        ),
        font=_FONT_HINT,
        foreground="#64748b",
        wraplength=720,
        ).grid(row=2, column=0, columnspan=9, sticky="w", pady=(4, 0))

    interaction_toolbar = ttk.LabelFrame(
        interaction_inner, text="Thao tác tương tác", padding=6, style="Human.TLabelframe"
    )
    interaction_toolbar.grid(row=2, column=0, sticky="ew", pady=(0, 4))
    run_btns = _FlowBar(interaction_toolbar)
    run_btns.pack(fill=tk.X)
    profile_btns = _FlowBar(interaction_toolbar)
    profile_btns.pack(fill=tk.X, pady=(4, 0))
    interaction_row_btns = _FlowBar(interaction_toolbar)
    interaction_row_btns.pack(fill=tk.X, pady=(4, 0))
    lbl_interaction_summary = ttk.Label(interaction_toolbar, textvariable=var_summary, font=_FONT_HINT)
    lbl_interaction_summary.pack(anchor="e", pady=(4, 0))

    interaction_table_fr = ttk.LabelFrame(
        interaction_bottom,
        text="Tài khoản đã đăng nhập — sẵn sàng tương tác",
        padding=4,
        style="Human.TLabelframe",
    )
    interaction_table_fr.grid(row=0, column=0, sticky="nsew")
    interaction_table_fr.columnconfigure(0, weight=1)
    interaction_table_fr.rowconfigure(0, weight=1)

    health_fr = tk.Frame(interaction_bottom, bg=_C_HEALTH_BG, padx=10, pady=6)
    health_fr.grid(row=1, column=0, sticky="ew", pady=(6, 0))
    tk.Label(health_fr, text="📊 Pool", font=("Segoe UI", 8, "bold"), bg=_C_HEALTH_BG, fg=_C_HEALTH_FG).pack(
        side=tk.LEFT
    )
    lbl_health = tk.Label(
        health_fr, text="Health: idle", font=_FONT_MONO, bg=_C_HEALTH_BG, fg=_C_HEALTH_FG, anchor="e"
    )
    lbl_health.pack(side=tk.RIGHT, fill=tk.X, expand=True)

    cols = ("stt", "uid", "username", "password", "email", "twofa", "proxy", "status", "detail")
    table_toolbars: list[ttk.Frame] = []

    def _build_mapped_tree(parent_fr: ttk.Frame) -> ttk.Treeview:
        tr = ttk.Treeview(
            parent_fr,
            columns=cols,
            show="headings",
            height=8,
            selectmode="extended",
            style="Human.Treeview",
        )
        tr.heading("stt", text="STT")
        tr.heading("uid", text="UID")
        tr.heading("username", text="Tên người dùng")
        tr.heading("password", text="Pass")
        tr.heading("email", text="Mail")
        tr.heading("twofa", text="2FA")
        tr.heading("proxy", text="Proxy")
        tr.heading("status", text="Trạng thái")
        tr.heading("detail", text="Chi tiết")
        tr.column("stt", width=52, minwidth=44, stretch=False, anchor="center")
        tr.column("uid", width=120, minwidth=90, stretch=False)
        tr.column("username", width=140, minwidth=90, stretch=False)
        tr.column("password", width=120, minwidth=80, stretch=False)
        tr.column("email", width=180, minwidth=100, stretch=False)
        tr.column("twofa", width=52, minwidth=44, stretch=False)
        tr.column("proxy", width=220, minwidth=120, stretch=False)
        tr.column("status", width=110, minwidth=80, stretch=False)
        tr.column("detail", width=280, minwidth=140, stretch=True)
        tools = ttk.Frame(parent_fr)
        tools.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        table_toolbars.append(tools)
        sy = ttk.Scrollbar(parent_fr, orient=tk.VERTICAL, command=tr.yview)
        sx = ttk.Scrollbar(parent_fr, orient=tk.HORIZONTAL, command=tr.xview)
        tr.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)
        tr.grid(row=1, column=0, sticky="nsew")
        sy.grid(row=1, column=1, sticky="ns")
        sx.grid(row=2, column=0, sticky="ew")
        parent_fr.rowconfigure(0, weight=0)
        parent_fr.rowconfigure(1, weight=1)
        parent_fr.columnconfigure(0, weight=1)
        _configure_tree_status_tags(tr)
        tr.heading("status", command=lambda tree=tr: _sort_tree_by_status(tree))
        return tr

    tree_login = _build_mapped_tree(login_table_fr)
    tree_interaction = _build_mapped_tree(interaction_table_fr)
    state["nb_main"] = nb_main
    state["page_login"] = page_login
    state["page_interaction"] = page_interaction
    state["tree_login"] = tree_login
    state["tree_interaction"] = tree_interaction

    # --- Logic ---
    def _clipboard_text() -> str:
        try:
            return str(root.clipboard_get() or "")
        except tk.TclError:
            return ""

    def _paste_clipboard_into(widget: tk.Text, *, label: str) -> None:
        clip = _clipboard_text().strip()
        if not clip:
            messagebox.showwarning("Clipboard trống", "Không có văn bản trong clipboard.", parent=parent)
            return
        widget.delete("1.0", tk.END)
        widget.insert("1.0", clip)
        logger.info("[Human GUI] Đã dán clipboard → {}", label)

    def _clear_both_text_areas() -> None:
        if not txt_acc.get("1.0", tk.END).strip() and not txt_px.get("1.0", tk.END).strip():
            return
        if not messagebox.askyesno("Xóa trắng", "Xóa toàn bộ 2 ô dán trực tiếp?", parent=parent):
            return
        txt_acc.delete("1.0", tk.END)
        txt_px.delete("1.0", tk.END)
        state["proxy_live"] = None
        _refresh_input_counts()

    def _set_text_lines(widget: tk.Text, lines: list[str]) -> None:
        widget.delete("1.0", tk.END)
        if lines:
            widget.insert("1.0", "\n".join(lines) + "\n")

    def _on_check_proxy_live(*, from_file_tab: bool = False) -> None:
        """Kiểm tra LIVE từng dòng proxy — xóa dòng die, giữ cặp TK↔proxy theo index."""

        def work() -> tuple[list[str], list[str], list[dict[str, Any]], int, int, dict[str, int]]:
            acc_lines = _non_empty_lines(txt_acc.get("1.0", tk.END))
            px_lines = _non_empty_lines(txt_px.get("1.0", tk.END))
            if not px_lines and from_file_tab:
                pp = var_px.get().strip()
                ap = var_acc.get().strip()
                if pp:
                    px_lines = read_lines_file(pp)
                if ap:
                    acc_lines = read_lines_file(ap)
            if not px_lines:
                raise AccountProxyMappingError(
                    "Chưa có dòng proxy — dán vào ô Proxy hoặc chọn file .txt."
                )
            n_before = len(px_lines)
            live_acc, live_px, dead, scheme_counts = filter_lines_by_live_proxy(acc_lines, px_lines)
            return live_acc, live_px, dead, n_before, len(live_px), scheme_counts

        def ok(
            payload: tuple[list[str], list[str], list[dict[str, Any]], int, int, dict[str, int]],
        ) -> None:
            live_acc, live_px, dead, n_before, n_live, scheme_counts = payload
            state["_suppress_live_invalidate"] = True
            try:
                _set_text_lines(txt_px, live_px)
                if live_acc or _non_empty_lines(txt_acc.get("1.0", tk.END)):
                    _set_text_lines(txt_acc, live_acc)
            finally:
                state["_suppress_live_invalidate"] = False
            state["proxy_live"] = {"live": n_live, "checked": n_before}
            _refresh_input_counts()
            _save_settings()
            removed = n_before - n_live
            lines = [
                f"Proxy LIVE: {n_live}/{n_before} (HTTP/HTTPS/SOCKS4/SOCKS5)",
                f"Đã bỏ {removed} dòng proxy lỗi/chết.",
            ]
            if scheme_counts:
                parts = [f"{k.upper()}: {v}" for k, v in sorted(scheme_counts.items()) if v]
                lines.append("Loại đã giữ: " + ", ".join(parts))
                lines.append("Dòng LIVE đã ghi scheme (vd. socks5://…) — trình duyệt dùng đúng loại.")
            if live_acc:
                lines.append(f"Tài khoản còn {len(live_acc)} dòng (khớp proxy LIVE).")
            if dead:
                lines.append("\nVí dụ proxy die:")
                for row in dead[:8]:
                    lines.append(
                        f"  Dòng {row['line_no']}: {row['proxy_line'][:48]}… — {row['error'][:80]}"
                    )
                if len(dead) > 8:
                    lines.append(f"  … và {len(dead) - 8} dòng khác.")
            messagebox.showinfo("Check Proxy LIVE", "\n".join(lines), parent=parent)
            logger.info("[Human GUI] Check proxy LIVE: {} live, {} removed", n_live, removed)

        def err(exc: BaseException) -> None:
            messagebox.showerror("Check Proxy LIVE", str(exc), parent=parent)

        if not messagebox.askyesno(
            "Check Proxy LIVE",
            "Kiểm tra từng dòng proxy (HTTP, HTTPS, SOCKS4, SOCKS5) và xóa dòng không LIVE?\n"
            "Proxy LIVE được ghi lại kèm scheme để trình duyệt áp dụng đúng.\n"
            "Tài khoản cùng dòng với proxy die cũng bị bỏ (ghép 1↔1).",
            parent=parent,
        ):
            return
        run_background_then_main(root, work, ok, on_error=err)

    def _pick_file(var: tk.StringVar, kind: str) -> None:
        path = filedialog.askopenfilename(
            parent=root,
            title=f"Chọn file {kind}",
            filetypes=[("Text", "*.txt"), ("All", "*.*")],
        )
        if path:
            var.set(path)
            if kind == "accounts":
                state["accounts_path"] = path
            else:
                state["proxies_path"] = path
            nb_in.select(tab_file)
            _refresh_file_counts()

    def _mapped_snapshot_for_save() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Chụp bảng đang có — không đọc đĩa trên luồng giao diện."""
        login = [ma.to_dict() for ma in (state.get("mapped_login") or [])]
        interaction = [ma.to_dict() for ma in (state.get("mapped_interaction") or [])]
        return login, interaction

    def _save_settings() -> None:
        login_snap, interaction_snap = _mapped_snapshot_for_save()
        payload = {
            "accounts_path": var_acc.get().strip(),
            "proxies_path": var_px.get().strip(),
            "accounts_text": txt_acc.get("1.0", tk.END).strip(),
            "proxies_text": txt_px.get("1.0", tk.END).strip(),
            "threads": max(1, int(var_threads.get())),
            "grid_cols": max(1, min(8, int(var_grid_cols.get()))),
            "grid_rows": max(0, min(4, int(var_grid_rows.get()))),
            "headless": bool(var_headless.get()),
            "profile": var_profile.get().strip().lower() or "normal",
            "like_rate_pct": max(0, min(100, int(var_like_pct.get()))),
            "comment_rate_pct": max(0, min(100, int(var_comment_pct.get()))),
            "virtual_cursor": bool(var_virtual_cursor.get()),
            "ai_comments": bool(var_ai_comments.get()),
            "account_line_format": _current_account_format(),
            "mapped_accounts_login": login_snap,
            "mapped_accounts_interaction": interaction_snap,
        }

        def _write() -> None:
            try:
                save_human_interaction_settings(payload)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[Human GUI] Ghi cài đặt thất bại: {}", exc)

        threading.Thread(target=_write, name="human_settings_save", daemon=True).start()

    def _schedule_save_settings() -> None:
        """Ghi cài đặt + bảng đã ghép (debounce khi cập nhật trạng thái liên tục)."""
        prev = state.get("_save_debounce_id")
        if prev is not None:
            try:
                root.after_cancel(prev)
            except tk.TclError:
                pass
        delay_ms = 2800 if state.get("pool") else 600
        state["_save_debounce_id"] = root.after(delay_ms, _save_settings)

    def _restore_mapped_session() -> None:
        """Khôi phục hai hàng đợi — hiện bảng ngay, đồng bộ profile/cookie ở luồng nền."""
        login_raw = load_login_queue_from_settings(persisted)
        interaction_raw = load_interaction_queue_from_settings(persisted)
        login_restored: list[MappedAccount] = []
        interaction_restored: list[MappedAccount] = []
        for item in login_raw:
            try:
                login_restored.append(MappedAccount.from_dict(item))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Bỏ qua dòng login lưu lỗi: {}", exc)
        for item in interaction_raw:
            try:
                interaction_restored.append(MappedAccount.from_dict(item))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Bỏ qua dòng tương tác lưu lỗi: {}", exc)
        if not login_restored and not interaction_restored:
            return
        state["mapped_login"] = login_restored
        state["mapped_interaction"] = interaction_restored
        _refresh_trees()
        logger.info(
            "[Human GUI] Khôi phục phiên: {} chờ login, {} tương tác.",
            len(login_restored),
            len(interaction_restored),
        )
        batch = login_restored + interaction_restored

        def _sync_storage() -> None:
            refresh_mapped_accounts_storage(batch)

        def _sync_done(_result: object) -> None:
            _refresh_trees()

        run_background_then_main(root, _sync_storage, _sync_done)

    def _select_main_page(page: ttk.Frame) -> None:
        try:
            nb_main.select(page)
        except tk.TclError:
            pass

    def _is_interaction_tab_active() -> bool:
        try:
            return int(nb_main.index(nb_main.select())) == 1
        except tk.TclError:
            return False

    def _active_tree() -> ttk.Treeview:
        return tree_interaction if _is_interaction_tab_active() else tree_login

    def _all_mapped_accounts() -> list[MappedAccount]:
        """Toàn bộ tài khoản tab Đăng nhập + Tương tác — kiểm tra proxy 1:1."""
        return list(state.get("mapped_login") or []) + list(state.get("mapped_interaction") or [])

    def _queue_proxy_owners() -> dict[str, str]:
        """Proxy đang gắn trên tab Đăng nhập / Tương tác — để ghép bỏ qua."""
        owners: dict[str, str] = {}
        for ma in _all_mapped_accounts():
            key = proxy_identity_key_for_account(ma)
            if key:
                owners.setdefault(key, ma.account_id)
        return owners

    def _merge_into_login_queue(
        new_rows: list[MappedAccount],
        proxy_lines: list[str] | None = None,
    ) -> tuple[int, int, int]:
        """Ghép vào hàng đợi login. Proxy đã gắn thì đổi proxy khác, không hủy cả lô."""
        interaction_ids = {m.account_id for m in state.get("mapped_interaction") or []}
        by_id = {m.account_id: m for m in state.get("mapped_login") or []}
        registry_index = load_registry_proxy_index()
        seen_proxy_keys = _queue_proxy_owners()
        for key, owner in registry_index.items():
            seen_proxy_keys.setdefault(key, owner)
        pool: list[tuple[Any, str]] = []
        for line in proxy_lines or []:
            try:
                network = parse_proxy_line_to_network(line)
            except ValueError:
                continue
            pool.append((network, proxy_identity_key_for_network(network)))
        added = updated = skipped = 0
        for ma in new_rows:
            if ma.account_id in interaction_ids:
                skipped += 1
                continue
            key = proxy_identity_key_for_account(ma)
            blocker = _proxy_blocked_for_candidate(
                key,
                ma.account_id,
                username=ma.auth.username,
                seen_proxy_keys=seen_proxy_keys,
                registry_index=registry_index,
            )
            if blocker:
                replacement = None
                for network, pkey in pool:
                    if _proxy_blocked_for_candidate(
                        pkey,
                        ma.account_id,
                        username=ma.auth.username,
                        seen_proxy_keys=seen_proxy_keys,
                        registry_index=registry_index,
                    ):
                        continue
                    replacement = (network, pkey)
                    break
                if replacement is None:
                    skipped += 1
                    logger.warning(
                        "[Human GUI] Bỏ {} — proxy đã gắn «{}», không còn proxy trống.",
                        ma.account_id,
                        blocker,
                    )
                    continue
                ma.network = replacement[0]  # type: ignore[assignment]
                key = replacement[1]
                ma.status_detail = "Đổi sang proxy khác (proxy trước đã gắn tài khoản khác)"
                logger.info(
                    "[Human GUI] {} đổi proxy vì «{}» đang giữ proxy cũ.",
                    ma.account_id,
                    blocker,
                )
            if key:
                seen_proxy_keys[key] = ma.account_id
            ma.status = "pending"
            if ma.account_id in by_id:
                prev = by_id[ma.account_id]
                prev.auth = ma.auth
                prev.network = ma.network
                prev.storage = ma.storage
                prev.cookie_path = ma.cookie_path
                prev.use_proxy = ma.use_proxy
                prev.login_via_cookie = ma.login_via_cookie
                prev.status = "pending"
                prev.status_detail = ma.status_detail or ""
                updated += 1
            else:
                by_id[ma.account_id] = ma
                added += 1
        state["mapped_login"] = list(by_id.values())
        return added, updated, skipped

    def _promote_to_interaction(ma: MappedAccount) -> None:
        """Chuyển tài khoản login OK sang tab tương tác — chỉ khi file cookie có c_user."""
        from src.services.facebook_session_persist import cookie_file_has_session
        from src.utils.account_proxy_mapper import sync_mapped_account_storage_from_registry

        sync_mapped_account_storage_from_registry(ma)
        if not ma.cookie_path:
            ma.cookie_path = default_cookie_path(ma.account_id)
        if not cookie_file_has_session(ma.cookie_path):
            ma.status = "login_failed"
            ma.status_detail = "Chưa có file cookie phiên — đăng nhập lại và lưu cookie"
            logger.warning(
                "[Human GUI] Không chuyển tab Tương tác — thiếu cookie account={}",
                ma.account_id,
            )
            _refresh_trees()
            return
        state["mapped_login"] = [
            m for m in (state.get("mapped_login") or []) if m.account_id != ma.account_id
        ]
        interaction = state.setdefault("mapped_interaction", [])
        if not any(m.account_id == ma.account_id for m in interaction):
            ma.status = "login_ok"
            if not str(ma.status_detail or "").strip():
                ma.status_detail = "Sẵn sàng tương tác (cookie đã lưu)"
            interaction.append(ma)
        logger.info(
            "[Human GUI] Đã chuyển {} sang tab Tương tác (cookie={}).",
            ma.account_id,
            ma.cookie_path,
        )

    def _promote_ready_logins_to_interaction() -> int:
        """Chuyển dòng Đăng nhập OK sang tab Tương tác — chỉ khi người dùng mở tab đó."""
        ready = [
            m
            for m in (state.get("mapped_login") or [])
            if m.status in ("login_ok", "success")
        ]
        moved = 0
        for ma in ready:
            _promote_to_interaction(ma)
            still_login = any(
                m.account_id == ma.account_id for m in (state.get("mapped_login") or [])
            )
            if not still_login:
                moved += 1
        if moved:
            logger.info("[Human GUI] Mở tab Tương tác — chuyển {} tài khoản.", moved)
        return moved

    def _ensure_login_queue_loaded(*, persist_secrets: bool = True) -> list[MappedAccount] | None:
        """Ghép ô dán vào hàng đợi login nếu tab login đang trống."""
        if state.get("mapped_login"):
            return list(state["mapped_login"])
        try:
            mapped = _load_mapped(persist_secrets=persist_secrets)
        except AccountProxyMappingError as exc:
            messagebox.showerror("Chưa ghép được", str(exc), parent=parent)
            return None
        _merge_into_login_queue(mapped)
        _refresh_trees()
        _save_settings()
        logger.info("[Human GUI] Tự ghép {} dòng vào hàng đợi login.", len(mapped))
        return list(state.get("mapped_login") or [])

    def _grid_cols_value() -> int:
        try:
            return max(1, min(8, int(var_grid_cols.get())))
        except (TypeError, ValueError, tk.TclError):
            return 4

    def _grid_rows_value() -> int:
        """0 = tự chia hàng. 3 với 6 cột thì thành 3 hàng, 18 cửa sổ."""
        try:
            return max(0, min(4, int(var_grid_rows.get())))
        except (TypeError, ValueError, tk.TclError):
            return 0

    def _max_capture_windows() -> int:
        return max(1, int(var_threads.get()))

    def _active_capture_count() -> int:
        return len(state.get("capture_sessions") or set())

    def _allocate_capture_slots(need: int) -> list[GridWindowSlot]:
        """Cấp ``need`` ô lưới trống cho mở trình duyệt thủ công."""
        mc = _max_capture_windows()
        slots = compute_grid_layout(mc, max_cols=_grid_cols_value(), rows=_grid_rows_value() or None)
        used = set((state.get("capture_slot_by_account") or {}).values())
        free = [s for s in slots if s.index not in used]
        return free[: max(0, int(need))]

    def _non_empty_lines(text: str) -> list[str]:
        return [ln.strip() for ln in str(text or "").splitlines() if ln.strip() and not ln.strip().startswith("#")]

    def _refresh_input_counts(*, invalidate_live: bool = False) -> None:
        """Hiện số tài khoản, số proxy và kết quả proxy LIVE ngay trên ô dán."""
        if invalidate_live and not state.get("_suppress_live_invalidate"):
            state["proxy_live"] = None
        n_acc = len(_non_empty_lines(txt_acc.get("1.0", tk.END)))
        n_px = len(_non_empty_lines(txt_px.get("1.0", tk.END)))
        live = state.get("proxy_live")
        if isinstance(live, dict) and int(live.get("checked") or 0) > 0:
            live_txt = f"{int(live.get('live') or 0)}/{int(live.get('checked') or 0)}"
        else:
            live_txt = "chưa kiểm tra"
        gap = ""
        if n_acc and n_px and n_acc != n_px:
            gap = f"  |  lệch {abs(n_acc - n_px)} dòng (ghép 1↔1)"
        var_input_counts.set(f"Tài khoản: {n_acc}  |  Proxy: {n_px}  |  Proxy LIVE: {live_txt}{gap}")
        acc_wrap.configure(text=f"Tài khoản ({n_acc})")
        if isinstance(live, dict) and int(live.get("checked") or 0) > 0:
            px_wrap.configure(text=f"Proxy ({n_px}) · LIVE {int(live.get('live') or 0)}")
        else:
            px_wrap.configure(text=f"Proxy ({n_px})")

    def _on_paste_modified(event: tk.Event) -> None:
        widget = event.widget
        try:
            if widget.edit_modified():
                widget.edit_modified(False)
        except tk.TclError:
            return
        if state.get("_suppress_live_invalidate"):
            return
        _refresh_input_counts(invalidate_live=True)

    def _refresh_file_counts() -> None:
        """Đếm dòng trong hai file đã chọn."""
        parts: list[str] = []
        for label, path in (("Tài khoản", var_acc.get().strip()), ("Proxy", var_px.get().strip())):
            if not path:
                parts.append(f"{label}: chưa chọn file")
                continue
            try:
                n = len(read_lines_file(path))
                parts.append(f"{label}: {n}")
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human GUI] Không đếm file {}: {}", label, exc)
                parts.append(f"{label}: không đọc được file")
        var_file_counts.set("  |  ".join(parts))

    txt_acc.bind("<<Modified>>", _on_paste_modified)
    txt_px.bind("<<Modified>>", _on_paste_modified)
    txt_acc.edit_modified(False)
    txt_px.edit_modified(False)
    _refresh_input_counts()
    _refresh_file_counts()

    def _resolve_input_lines() -> tuple[list[str], list[str]]:
        acc_text_lines = _non_empty_lines(txt_acc.get("1.0", tk.END))
        px_text_lines = _non_empty_lines(txt_px.get("1.0", tk.END))
        if acc_text_lines and px_text_lines:
            return acc_text_lines, px_text_lines
        ap = var_acc.get().strip()
        pp = var_px.get().strip()
        if not ap or not pp:
            raise AccountProxyMappingError(
                "Tab «Dán trực tiếp»: nhập đủ 2 ô — hoặc tab «Chọn file»: chọn đủ 2 file."
            )
        return read_lines_file(ap), read_lines_file(pp)

    def _resolve_proxy_pool_lines() -> list[str]:
        """Gộp proxy ô dán + file tab Đăng nhập (dòng mới thêm vẫn được tính)."""
        merged: list[str] = []
        seen: set[str] = set()
        for ln in _non_empty_lines(txt_px.get("1.0", tk.END)):
            if ln not in seen:
                seen.add(ln)
                merged.append(ln)
        pp = var_px.get().strip()
        if pp:
            try:
                file_lines = read_lines_file(pp)
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human GUI] Không đọc file proxy: {}", exc)
                file_lines = []
            for ln in file_lines:
                s = str(ln or "").strip()
                if s and not s.startswith("#") and s not in seen:
                    seen.add(s)
                    merged.append(s)
        if not merged:
            raise AccountProxyMappingError(
                "Chưa có danh sách proxy — nhập ở tab «Đăng nhập» (ô Proxy hoặc chọn file)."
            )
        return merged

    def _short(text: str, limit: int = 36) -> str:
        s = str(text or "").strip() or "—"
        return s if len(s) <= limit else s[: limit - 3] + "..."

    def _display_username(ma: MappedAccount) -> str:
        """Tên đăng nhập gốc — khác UID khi dòng import không phải số Facebook."""
        raw = str(ma.auth.username or "").strip()
        if raw.upper().startswith("UID_"):
            raw = raw[4:]
        return raw or "—"

    def _tree_values(ma: MappedAccount, *, status: str | None = None, detail: str | None = None) -> tuple[str, ...]:
        st = _STATUS_VI.get(status or ma.status, status or ma.status)
        twofa = "Có" if ma.auth.two_fa_secret else "—"
        n = int(getattr(ma, "queue_no", 0) or 0)
        return (
            f"{n:02d}" if n else "—",
            ma.display_uid(),
            _short(_display_username(ma), 28),
            ma.auth.password or "—",
            _short(ma.auth.email or "—", 28),
            twofa,
            _short(ma.network.proxy_server or "—", 32),
            st,
            detail if detail is not None else (ma.status_detail or ""),
        )

    def _full_proxy_line(ma: MappedAccount) -> str:
        """Proxy đủ user/pass để dán lại — không cắt như cột bảng."""
        server = str(ma.network.proxy_server or "").strip()
        user = str(ma.network.proxy_username or "").strip()
        password = str(ma.network.proxy_password or "").strip()
        if not server:
            return ""
        if user and "@" not in server:
            if "://" in server:
                scheme, rest = server.split("://", 1)
                return f"{scheme}://{user}:{password}@{rest}"
            return f"{server}:{user}:{password}"
        return server

    def _account_copy_line(ma: MappedAccount, *, full: bool) -> str:
        uid = ma.display_uid()
        username = _display_username(ma)
        if username == "—":
            username = ""
        pw = ma.auth.password or ""
        mail = ma.auth.email or ""
        proxy = _full_proxy_line(ma)
        if not full:
            return "|".join((uid, username, pw, mail, proxy))
        return "|".join(
            (
                uid,
                username,
                pw,
                ma.auth.two_fa_secret or "",
                mail,
                ma.auth.email_password or "",
                ma.auth.recovery_email or "",
                proxy,
            )
        )

    def _copy_selected_accounts(*, full: bool = False) -> None:
        rows = _selected_mapped()
        if not rows:
            messagebox.showinfo("Chưa chọn", "Chọn một hoặc nhiều dòng trong bảng.", parent=parent)
            return
        text = "\n".join(_account_copy_line(ma, full=full) for ma in rows)
        try:
            root.clipboard_clear()
            root.clipboard_append(text)
        except tk.TclError as exc:
            messagebox.showerror("Copy", str(exc), parent=parent)
            return
        kind = "đầy đủ" if full else "uid|tên|pass|mail|proxy"
        var_summary.set(f"Đã copy {len(rows)} tài khoản ({kind})")
        logger.info("[Human GUI] Copy {} tài khoản ({})", len(rows), kind)

    def _cookie_header_for_mapped(ma: MappedAccount) -> tuple[str, str]:
        """Cookie thật và email khôi phục — không lấy email làm cookie."""
        from src.utils.account_proxy_mapper import (
            _looks_like_cookie,
            _looks_like_email,
            _separate_cookie_and_recovery,
            cookie_header_from_file,
            parse_account_line,
        )

        cookie = str(ma.auth.imported_cookie or "").strip() or cookie_header_from_file(ma.cookie_path)
        recovery = str(ma.auth.recovery_email or "").strip()
        uid = ma.display_uid()
        try:
            lines = _non_empty_lines(txt_acc.get("1.0", tk.END))
        except tk.TclError:
            lines = []
        for line in lines:
            try:
                auth = parse_account_line(line, account_format=_current_account_format())
            except ValueError:
                continue
            if auth.username != uid and auth.username != ma.auth.username:
                continue
            if auth.imported_cookie:
                cookie = auth.imported_cookie
            if auth.recovery_email:
                recovery = auth.recovery_email
            break
        cookie, recovery = _separate_cookie_and_recovery(cookie, recovery)
        if _looks_like_email(cookie) and not _looks_like_cookie(cookie):
            cookie = ""
        return cookie, recovery

    def _form_record_for_mapped(ma: MappedAccount) -> dict[str, Any]:
        """Đưa dòng tab Tương tác sang form «Sửa tài khoản»."""
        try:
            apply_mapped_secrets_to_vault(ma)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[Human GUI] Chưa ghi vault trước khi mở form: {}", exc)
        rec = mapped_account_to_account_dict(ma)
        shown = _display_username(ma)
        uid = ma.display_uid()
        cookie, recovery = _cookie_header_for_mapped(ma)
        facebook_email = str(ma.auth.email or "").strip()
        if facebook_email and facebook_email == recovery:
            facebook_email = ""
        rec["name"] = shown if shown != "—" else uid
        rec["facebook_uid"] = uid if str(uid).isdigit() else ""
        rec["email"] = facebook_email
        rec["recovery_email"] = recovery
        rec["cookie_header"] = cookie
        rec["totp_enabled"] = bool(ma.auth.two_fa_secret)
        rec["notes"] = ""
        return rec

    def _apply_account_form_to_mapped(ma: MappedAccount, rec: dict[str, Any]) -> None:
        """Ghi form sửa tài khoản ngược lại dòng trong bảng."""
        from src.utils.account_credentials import (
            get_account_password,
            get_account_recovery_email,
            get_account_totp_secret,
        )

        aid = str(rec.get("id") or ma.account_id).strip() or ma.account_id
        px = rec.get("proxy") if isinstance(rec.get("proxy"), dict) else {}
        ma.use_proxy = bool(rec.get("use_proxy"))
        ma.network = proxy_dict_to_network(px)
        browser = str(rec.get("browser_type") or "").strip()
        if browser:
            ma.browser_type = browser
        portable = str(rec.get("portable_path") or rec.get("profile_path") or "").strip()
        if portable:
            ma.storage.profile_path = portable
        cookie = str(rec.get("cookie_path") or "").strip()
        if cookie:
            ma.cookie_path = cookie
        uid = str(rec.get("facebook_uid") or "").strip()
        if uid:
            ma.auth.username = uid
        ma.auth.email = str(rec.get("email") or "").strip()
        pwd_ref = str(rec.get("password_ref") or "").strip() or None
        totp_ref = str(rec.get("totp_secret_ref") or "").strip() or None
        password = get_account_password(aid, pwd_ref)
        if password:
            ma.auth.password = password
        if rec.get("totp_enabled"):
            secret = get_account_totp_secret(aid, totp_ref)
            if secret:
                ma.auth.two_fa_secret = secret
        else:
            ma.auth.two_fa_secret = ""
        recovery = get_account_recovery_email(aid, pwd_ref) or str(rec.get("recovery_email") or "").strip()
        header = str(rec.get("cookie_header") or "").strip()
        from src.utils.account_proxy_mapper import _separate_cookie_and_recovery

        header, recovery = _separate_cookie_and_recovery(header, recovery)
        ma.auth.recovery_email = recovery
        if ma.auth.email and ma.auth.email == recovery:
            ma.auth.email = str(rec.get("email") or "").strip()
        else:
            ma.auth.email = str(rec.get("email") or ma.auth.email or "").strip()
        if header:
            ma.auth.imported_cookie = header
            if not ma.cookie_path:
                ma.cookie_path = default_cookie_path(ma.account_id)
            attach_imported_cookie(ma)

    def _open_attribute_dialog() -> None:
        """Mở form sửa tài khoản (giống tab Tài khoản) để sửa hoặc copy từng ô."""
        selected = list(_selected_mapped())
        if not selected:
            messagebox.showinfo(
                "Chưa chọn",
                "Chọn một dòng trong bảng rồi bấm «Sửa / copy», hoặc chuột phải trên dòng.",
                parent=parent,
            )
            return
        focus_id = ""
        try:
            focus_id = str(_active_tree().focus() or "")
        except tk.TclError:
            focus_id = ""
        ma = _mapped_by_id(focus_id) if focus_id else None
        if ma is None or ma.account_id not in {item.account_id for item in selected}:
            ma = selected[0]
        from src.gui.account_workbench import AccountFormDialog

        dlg = AccountFormDialog(
            parent,
            AccountsDatabaseManager(),
            title=f"Sửa tài khoản — {ma.display_uid()}",
            initial=_form_record_for_mapped(ma),
            id_readonly=True,
        )
        result = dlg.result
        if not isinstance(result, dict):
            return
        _apply_account_form_to_mapped(ma, result)
        _refresh_trees()
        _save_settings()
        _update_summary()
        logger.info("[Human GUI] Đã cập nhật dòng {} từ form sửa tài khoản", ma.account_id)

    def _on_remove_from_list() -> None:
        """Gỡ dòng khỏi bảng. Không xóa profile hay accounts.json."""
        selected = _selected_mapped()
        if not selected:
            messagebox.showinfo(
                "Chưa chọn",
                "Chọn dòng trong bảng rồi bấm «Xóa khỏi danh sách».",
                parent=parent,
            )
            return
        if state.get("pool"):
            busy = [ma for ma in selected if ma.status in ("running", "waiting", "proxy_busy")]
            if busy:
                messagebox.showwarning(
                    "Đang chạy",
                    "Bấm «Dừng» và chờ xong trước khi xóa dòng đang chạy.",
                    parent=parent,
                )
                return
        n = len(selected)
        preview = ", ".join(ma.display_uid() for ma in selected[:8])
        if n > 8:
            preview += f" … (+{n - 8})"
        if not messagebox.askyesno(
            "Xóa khỏi danh sách",
            f"Xóa {n} dòng khỏi bảng?\n{preview}\n\nChỉ gỡ khỏi danh sách này. Profile và cookie trên máy vẫn giữ.",
            parent=parent,
        ):
            return
        sel_ids = {ma.account_id for ma in selected}
        if _is_interaction_tab_active():
            state["mapped_interaction"] = [
                m for m in (state.get("mapped_interaction") or []) if m.account_id not in sel_ids
            ]
        else:
            state["mapped_login"] = [
                m for m in (state.get("mapped_login") or []) if m.account_id not in sel_ids
            ]
        _refresh_trees()
        _save_settings()
        _update_summary()
        logger.info("[Human GUI] Đã gỡ {} dòng khỏi danh sách", n)

    def _account_context_menu(menu: tk.Menu) -> None:
        menu.add_command(label="Sửa / copy (form tài khoản)", command=_open_attribute_dialog)
        menu.add_command(label="Xóa khỏi danh sách", command=_on_remove_from_list)
        menu.add_separator()
        menu.add_command(
            label="Copy UID | tên | pass | mail | proxy",
            command=lambda: _copy_selected_accounts(full=False),
        )
        menu.add_command(
            label="Copy đầy đủ (uid|tên|pass|2FA|mail|pass mail|mail khôi phục|proxy)",
            command=lambda: _copy_selected_accounts(full=True),
        )

    for _tr in (tree_login, tree_interaction):
        install_treeview_shortcuts(
            _tr,
            owner=root,
            enable_drag_select=True,
            info_callback=lambda msg: logger.info(msg),
            extra_context_items=_account_context_menu,
        )
        _tr.bind("<Delete>", lambda _e: _on_remove_from_list())

    for _bar in table_toolbars:
        ttk.Button(_bar, text="Xóa khỏi danh sách", command=_on_remove_from_list).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(_bar, text="Sửa / copy", command=_open_attribute_dialog).pack(side=tk.LEFT)

    def _pool_busy() -> bool:
        return bool(state.get("pool")) or bool(state.get("pool_stopping"))

    def _configure_stop_buttons(*, enabled: bool) -> None:
        """Bật/tắt nút Dừng nhưng giữ nguyên chữ để nút không đổi kích thước."""
        st = tk.NORMAL if enabled else tk.DISABLED
        for bs in state.get("btn_stop_all") or []:
            try:
                bs.configure(text="Dừng", state=st)
            except tk.TclError:
                pass

    def _stop_run_pulse() -> None:
        aid = state.get("_pulse_after")
        if aid is not None:
            try:
                root.after_cancel(aid)
            except tk.TclError:
                pass
        state["_pulse_after"] = None

    def _apply_banner_colors(*, bg: str, fg: str, led: str) -> None:
        """Đổi màu thanh trạng thái — giữ nguyên chiều cao và cỡ chữ."""
        run_banner.configure(bg=bg, highlightbackground="#e2e8f0")
        lbl_run_status.configure(bg=bg, fg=fg)
        lbl_run_led.configure(bg=bg, fg=led, text="●")

    def _tick_run_pulse() -> None:
        """Chỉ nhấp chấm trạng thái — nền không đổi màu."""
        if not state.get("pool") or state.get("pool_stopping"):
            _stop_run_pulse()
            return
        phase = int(state.get("_pulse_phase") or 0)
        try:
            lbl_run_led.configure(text="●" if phase % 2 == 0 else "○")
        except tk.TclError:
            _stop_run_pulse()
            return
        state["_pulse_phase"] = phase + 1
        state["_pulse_after"] = root.after(800, _tick_run_pulse)

    def _sync_run_banner() -> None:
        """Thanh trạng thái + nút Dừng toàn tab — luôn thấy dù đang ở trang Đăng nhập hay Tương tác."""
        pool = state.get("pool")
        workers_alive = False
        if pool is not None:
            try:
                workers_alive = bool(pool.has_live_workers())
            except Exception:
                workers_alive = True
        if state.get("pool_stopping") and workers_alive:
            _stop_run_pulse()
            var_run_status.set("Đang dừng — chờ luồng hiện tại kết thúc")
            _apply_banner_colors(bg=_C_STOPPING_BG, fg=_C_STOPPING_FG, led=_C_STOPPING_LED)
            _configure_stop_buttons(enabled=False)
            return
        if state.get("pool_stopping") and not workers_alive:
            _dismiss_stop_dialog()
            _stop_run_pulse()
            var_run_status.set("Sẵn sàng")
            _apply_banner_colors(bg=_C_IDLE_BG, fg=_C_IDLE_FG, led=_C_IDLE_LED)
            _configure_stop_buttons(enabled=False)
            return
        if pool:
            mode = "Đăng nhập" if state.get("pool_login_only") else "Tương tác"
            try:
                snap = pool.health_snapshot()
                run_n = snap.get("running", 0)
                lim = snap.get("dynamic_limit", 0)
                done_n = int(snap.get("completed_accounts") or 0)
                tot_n = int(snap.get("total_accounts") or 0)
                pend_n = int(snap.get("pending_accounts") or 0)
                extra = f"  ·  {run_n}/{lim} luồng  ·  {done_n}/{tot_n} xong"
                if pend_n > 0:
                    extra += f"  ·  còn {pend_n}"
            except Exception:
                extra = ""
            var_run_status.set(f"Đang chạy  ·  {mode}{extra}")
            _apply_banner_colors(bg=_C_RUN_BG, fg=_C_RUN_FG, led=_C_RUN_LED)
            _configure_stop_buttons(enabled=True)
            if state.get("_pulse_after") is None:
                _tick_run_pulse()
            return
        _stop_run_pulse()
        var_run_status.set("Sẵn sàng")
        _apply_banner_colors(bg=_C_IDLE_BG, fg=_C_IDLE_FG, led=_C_IDLE_LED)
        _configure_stop_buttons(enabled=False)

    def _finish_pool_cleanup(
        *,
        pool_ref: HumanInteractionPool | None = None,
        generation: int | None = None,
    ) -> None:
        if generation is not None and state.get("pool_generation") != generation:
            logger.info(
                "[Human GUI] Bỏ cleanup pool gen={} (hiện tại={})",
                generation,
                state.get("pool_generation"),
            )
            return
        current_pool = state.get("pool")
        if pool_ref is not None and current_pool is not None and current_pool is not pool_ref:
            logger.info("[Human GUI] Bỏ cleanup — pool instance đã thay thế")
            return
        _stop_run_pulse()
        state["pool"] = None
        state["pool_stopping"] = False
        state["pool_login_only"] = False
        state.pop("manual_captcha_shown", None)
        tip = state.get("stop_wait_tip")
        if tip is not None:
            try:
                if tip.winfo_exists():
                    tip.destroy()
            except tk.TclError:
                pass
            state["stop_wait_tip"] = None
        _configure_stop_buttons(enabled=False)
        _sync_run_banner()
        _update_summary()

    def _pool_auto_continue_enabled() -> bool:
        """Mặc định bật — tự chạy lượt tiếp khi pool timeout / TK chưa xong."""
        return os.environ.get("FB_POOL_AUTO_CONTINUE", "1").strip().lower() not in (
            "0",
            "false",
            "no",
            "off",
        )

    def _pool_auto_continue_max_rounds() -> int:
        return max(1, int(os.environ.get("FB_POOL_AUTO_CONTINUE_MAX", "10")))

    def _pool_auto_continue_delay_ms() -> int:
        return max(1200, int(os.environ.get("FB_POOL_AUTO_CONTINUE_DELAY_MS", "2500")))

    def _accounts_for_auto_continue(
        accounts: list[MappedAccount],
        *,
        login_only: bool,
    ) -> list[MappedAccount]:
        """TK cần chạy thêm — loại trừ thành công / hủy / lỗi proxy cứng."""
        terminal_ok = frozenset({"login_ok"} if login_only else {"success"})
        out: list[MappedAccount] = []
        for ma in accounts:
            st = str(ma.status or "").strip()
            if st in terminal_ok:
                continue
            if st == "cancelled":
                continue
            if st in ("login_failed", "proxy_error"):
                continue
            detail = str(ma.status_detail or "").lower()
            if st == "error" and "đóng bất ngờ" not in detail and "thử lại" not in detail:
                continue
            out.append(ma)
        return out

    def _reconcile_accounts_after_pool(
        accounts: list[MappedAccount],
        *,
        join_ok: bool,
        workers_alive: bool,
        user_cancelled: bool = False,
    ) -> None:
        """Cập nhật dòng còn «Đang chạy»/«Chờ» khi pool đã join — tránh UI lệch banner."""
        terminal = frozenset(
            {"success", "login_ok", "login_failed", "proxy_error", "error", "cancelled"}
        )
        # Đồng bộ cả hàng đợi tab (không chỉ batch vừa chạy) — tránh TK «Đang chạy» treo.
        batch_ids = {ma.account_id for ma in accounts}
        targets: list[MappedAccount] = list(accounts)
        for ma in (state.get("mapped_interaction") or []) + (state.get("mapped_login") or []):
            if ma.account_id in batch_ids:
                continue
            if str(ma.status or "").strip() in ("running", "waiting"):
                targets.append(ma)
        n_fixed = 0
        for ma in targets:
            st = str(ma.status or "").strip()
            if st in terminal:
                continue
            detail = str(ma.status_detail or "")
            if st == "pending" and (
                "Chưa tới lượt" in detail
                or "Chưa kịp chạy" in detail
                or "Chưa chạy — pool" in detail
            ):
                continue
            if st in ("running", "waiting", "pending", ""):
                n_fixed += 1
                if user_cancelled:
                    ma.status = "cancelled"
                    ma.status_detail = "Đã hủy — người dùng bấm Dừng"
                elif st == "running":
                    ma.status = "pending"
                    ma.status_detail = (
                        "Dừng giữa module — tự động chạy tiếp"
                        if _pool_auto_continue_enabled()
                        else "Dừng giữa module — bấm «Chạy» lại (cookie/profile giữ nguyên)"
                    )
                elif st == "waiting" and "Module:" in detail:
                    ma.status = "pending"
                    ma.status_detail = (
                        "Dừng giữa module — tự động chạy tiếp"
                        if _pool_auto_continue_enabled()
                        else "Dừng giữa module — bấm «Chạy» lại (cookie/profile giữ nguyên)"
                    )
                elif not join_ok or workers_alive:
                    ma.status = "pending"
                    ma.status_detail = (
                        "Chưa chạy xong — tự động chạy tiếp"
                        if _pool_auto_continue_enabled()
                        else "Chưa chạy xong — pool hết thời gian chờ, bấm «Chạy» để tiếp tục"
                    )
                else:
                    ma.status = "error"
                    ma.status_detail = "Pool kết thúc nhưng trạng thái chưa cập nhật — kiểm tra Firefox"
        if n_fixed:
            _refresh_trees()
            _save_settings()
            logger.warning(
                "[Human GUI] Đã đồng bộ {} dòng trạng thái treo (join_ok={} workers_alive={})",
                n_fixed,
                join_ok,
                workers_alive,
            )

    def _set_stop_button_stopping() -> None:
        state["pool_stopping"] = True
        _configure_stop_buttons(enabled=False)
        _sync_run_banner()

    def _dismiss_stop_dialog() -> None:
        """Đóng hộp «Đang dừng» khi không còn luồng chạy."""
        tip = state.get("stop_wait_tip")
        if tip is None:
            return
        try:
            if tip.winfo_exists():
                tip.destroy()
        except tk.TclError:
            pass
        state["stop_wait_tip"] = None

    def _show_stopping_dialog() -> None:
        tip = state.get("stop_wait_tip")
        if tip is not None:
            try:
                if tip.winfo_exists():
                    return
            except tk.TclError:
                pass
        dlg = tk.Toplevel(parent)
        dlg.title("Đang dừng")
        dlg.transient(parent)
        dlg.resizable(False, False)
        ttk.Label(
            dlg,
            text="Đang dừng tiến trình…\nCác luồng sẽ kết thúc sau khi hoàn tất bước hiện tại.",
            justify=tk.CENTER,
        ).pack(padx=20, pady=(16, 8))
        ttk.Button(dlg, text="Ẩn", command=dlg.destroy, width=10).pack(pady=(0, 12))
        state["stop_wait_tip"] = dlg

    def _force_pool_stale_cleanup(*, accounts: list[MappedAccount] | None = None) -> None:
        """Giải phóng pool kẹt sau timeout dừng — cho phép chạy lượt mới."""
        pool = state.get("pool")
        if pool is not None:
            try:
                pool.join(timeout=8.0)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[Human GUI] force cleanup join: {}", exc)
        if accounts:
            _reconcile_accounts_after_pool(accounts, join_ok=False, workers_alive=True, user_cancelled=False)
        _finish_pool_cleanup()

    def _wait_pool_idle_then(
        on_ready: Any,
        *,
        timeout_ms: int = 120_000,
        started_at: float | None = None,
    ) -> None:
        """Poll đến khi pool kết thúc (sau pool.stop), rồi gọi ``on_ready`` trên main thread."""
        import time as _time

        t0 = started_at if started_at is not None else _time.monotonic()
        if not state.get("pool"):
            state["pool_stopping"] = False
            tip = state.get("stop_wait_tip")
            if tip is not None:
                try:
                    if tip.winfo_exists():
                        tip.destroy()
                except tk.TclError:
                    pass
                state["stop_wait_tip"] = None
            _configure_stop_buttons(enabled=False)
            _sync_run_banner()
            on_ready()
            return
        if (_time.monotonic() - t0) * 1000.0 >= timeout_ms:
            messagebox.showerror(
                "Dừng tiến trình",
                "Hết thời gian chờ dừng — đã ép đóng pool.\n"
                "Có thể chạy lại ngay; đóng Firefox thủ công nếu còn cửa sổ.",
                parent=parent,
            )
            _force_pool_stale_cleanup()
            on_ready()
            return
        root.after(250, lambda: _wait_pool_idle_then(on_ready, timeout_ms=timeout_ms, started_at=t0))

    def _request_pool_stop(*, show_dialog: bool = True) -> None:
        """Gửi tín hiệu dừng pool (không join — thread watch sẽ join)."""
        pool = state.get("pool")
        if not pool:
            return
        if state.get("pool_stopping"):
            return
        pool.stop()
        _set_stop_button_stopping()
        if show_dialog:
            _show_stopping_dialog()

    def _ensure_pool_stopped_or_ask(action_desc: str, on_ready: Any) -> None:
        """Nếu đang chạy pool: hỏi «Có» = dừng rồi làm ``action_desc``."""
        if not _pool_busy():
            on_ready()
            return
        if not messagebox.askyesno(
            "Đang chạy",
            (
                "Tiến trình tự động đang chạy.\n\n"
                f"• Có — Dừng tiến trình và {action_desc}\n"
                "• Không — Giữ nguyên, không làm gì thêm"
            ),
            parent=parent,
            icon="warning",
        ):
            return
        _request_pool_stop(show_dialog=True)
        _wait_pool_idle_then(on_ready)

    def _update_summary() -> None:
        login_list = state.get("mapped_login") or []
        interaction_list = state.get("mapped_interaction") or []
        tr = _active_tree()
        sel = tr.selection()
        pending_login = sum(1 for m in login_list if m.status in ("pending", "proxy_busy", ""))
        n_ok = sum(1 for m in login_list if m.status in ("login_ok", "success"))
        n_fail = sum(1 for m in login_list if m.status in ("login_failed", "error", "proxy_error"))
        stopping_ui = bool(state.get("pool_stopping"))
        if stopping_ui and state.get("pool") is not None:
            try:
                if not state["pool"].has_live_workers():
                    stopping_ui = False
            except Exception:
                pass
        if stopping_ui:
            run_lbl = "ĐANG DỪNG"
        elif state.get("pool") and not state.get("pool_stopping"):
            run_lbl = "ĐANG CHẠY"
        else:
            run_lbl = "Sẵn sàng"
        try:
            nb_main.tab(0, text=f"  🔐  ĐĂNG NHẬP ({len(login_list)})  ")
            nb_main.tab(1, text=f"  👤  TƯƠNG TÁC ({len(interaction_list)})  ")
        except tk.TclError:
            pass
        _sync_run_banner()
        pool_prog = ""
        pool = state.get("pool")
        if pool:
            try:
                snap = pool.health_snapshot()
                pool_prog = (
                    f" | {int(snap.get('completed_accounts') or 0)}/"
                    f"{int(snap.get('total_accounts') or 0)} xong"
                )
            except Exception:
                pool_prog = ""
        login_bits = [f"{len(login_list)} tài khoản"]
        if n_ok:
            login_bits.append(f"{n_ok} OK")
        if pending_login:
            login_bits.append(f"{pending_login} chờ")
        if n_fail:
            login_bits.append(f"{n_fail} lỗi")
        try:
            login_table_fr.configure(text="Danh sách chờ đăng nhập — " + " · ".join(login_bits))
            interaction_table_fr.configure(
                text=f"Tài khoản đã đăng nhập — {len(interaction_list)} tài khoản"
            )
        except tk.TclError:
            pass
        if _is_interaction_tab_active():
            var_summary.set(
                f"Tương tác: {len(interaction_list)} TK | chọn {len(sel)} | {run_lbl}{pool_prog}"
            )
        else:
            stay = f" · {n_ok} OK giữ ở tab này" if n_ok else ""
            var_summary.set(
                f"Đăng nhập: {len(login_list)} TK (chờ {pending_login}{stay}) | "
                f"chọn {len(sel)} | {run_lbl} — Tương tác ({len(interaction_list)})"
            )

    def _update_tree_row(ma: MappedAccount) -> None:
        """Cập nhật một dòng — tránh xóa/vẽ lại cả bảng mỗi lần đổi trạng thái."""
        tag = _tree_tag_for_status(ma.status)
        vals = _tree_values(ma)
        for tr in (tree_login, tree_interaction):
            if tr.exists(ma.account_id):
                try:
                    tr.item(ma.account_id, values=vals, tags=(tag,))
                except tk.TclError:
                    pass

    def _ensure_queue_numbers() -> None:
        """Gán STT ổn định — số trên bảng trùng số trên cửa sổ trình duyệt."""
        rows = list(state.get("mapped_login") or []) + list(state.get("mapped_interaction") or [])
        used = {int(getattr(m, "queue_no", 0) or 0) for m in rows}
        used.discard(0)
        nxt = (max(used) if used else 0) + 1
        for ma in rows:
            if int(getattr(ma, "queue_no", 0) or 0) <= 0:
                ma.queue_no = nxt
                nxt += 1

    def _status_sort_key(ma: MappedAccount, mode: int) -> tuple[int, int]:
        st = str(ma.status or "")
        if st in ("login_failed", "error", "proxy_error"):
            rank = 0
        elif st in ("running", "waiting", "proxy_busy"):
            rank = 1
        elif st in ("pending", ""):
            rank = 2
        elif st in ("login_ok", "success"):
            rank = 3
        else:
            rank = 4
        if mode == 2:
            rank = -rank
        return (rank if mode else 0, int(getattr(ma, "queue_no", 0) or 0))

    def _status_heading_label(mode: int) -> str:
        if mode == 1:
            return "Trạng thái · lỗi trước"
        if mode == 2:
            return "Trạng thái · OK trước"
        return "Trạng thái"

    def _sort_tree_by_status(tr: ttk.Treeview) -> None:
        """Bấm cột Trạng thái: thứ tự gốc → lỗi lên trên → OK lên trên."""
        key = "login" if tr is tree_login else "interaction"
        modes = state.setdefault("_status_sort_mode", {})
        modes[key] = (int(modes.get(key) or 0) + 1) % 3
        tr.heading("status", text=_status_heading_label(modes[key]))
        _refresh_trees()

    def _rows_for_tree(rows: list[MappedAccount], tr: ttk.Treeview) -> list[MappedAccount]:
        key = "login" if tr is tree_login else "interaction"
        mode = int((state.get("_status_sort_mode") or {}).get(key) or 0)
        if mode == 0:
            return sorted(rows, key=lambda m: int(getattr(m, "queue_no", 0) or 0))
        return sorted(rows, key=lambda m: _status_sort_key(m, mode))

    def _refresh_one_tree(tr: ttk.Treeview, rows: list[MappedAccount], *, generation: int) -> None:
        """Xóa rồi chèn từng lô — bảng dài không khóa luồng giao diện."""
        selected = list(tr.selection())
        children = tr.get_children()
        if children:
            tr.delete(*children)

        def _insert(start: int = 0) -> None:
            if generation != int(state.get("_tree_gen") or 0):
                return
            end = min(start + 40, len(rows))
            for ma in rows[start:end]:
                try:
                    tr.insert(
                        "",
                        tk.END,
                        iid=ma.account_id,
                        values=_tree_values(ma),
                        tags=(_tree_tag_for_status(ma.status),),
                    )
                except tk.TclError as exc:
                    logger.warning("[Human GUI] Không chèn dòng {}: {}", ma.account_id, exc)
            if end < len(rows):
                root.after(1, lambda s=end: _insert(s))
                return
            if selected:
                keep = [iid for iid in selected if tr.exists(iid)]
                if keep:
                    try:
                        tr.selection_set(keep)
                    except tk.TclError:
                        pass

        _insert()

    def _refresh_trees() -> None:
        state["_tree_gen"] = int(state.get("_tree_gen") or 0) + 1
        gen = int(state["_tree_gen"])
        _ensure_queue_numbers()
        modes = state.get("_status_sort_mode") or {}
        try:
            tree_login.heading("status", text=_status_heading_label(int(modes.get("login") or 0)))
            tree_interaction.heading("status", text=_status_heading_label(int(modes.get("interaction") or 0)))
        except tk.TclError:
            pass
        _refresh_one_tree(
            tree_login,
            _rows_for_tree(list(state.get("mapped_login") or []), tree_login),
            generation=gen,
        )
        _refresh_one_tree(
            tree_interaction,
            _rows_for_tree(list(state.get("mapped_interaction") or []), tree_interaction),
            generation=gen,
        )
        _update_summary()

    def _flush_status_ui_batch() -> None:
        """Gộp nhiều callback trạng thái worker → một lần vẽ UI."""
        state["_status_flush_id"] = None
        pending: dict[str, MappedAccount] = dict(state.pop("_status_pending", {}) or {})
        if not pending:
            return
        need_full_refresh = False
        # Chỉ rời tab Đăng nhập khi người dùng đang xem tab Tương tác.
        if _is_interaction_tab_active():
            login_ids = {m.account_id for m in state.get("mapped_login") or []}
            for ma in pending.values():
                if ma.status in ("login_ok", "success") and ma.account_id in login_ids:
                    _promote_to_interaction(ma)
                    need_full_refresh = True
        if need_full_refresh:
            _refresh_trees()
        else:
            for ma in pending.values():
                _update_tree_row(ma)
            _update_summary()
        _schedule_save_settings()

    def _refresh_tree() -> None:
        """Alias — làm mới cả hai bảng."""
        _refresh_trees()

    def _mapped_by_id(account_id: str) -> MappedAccount | None:
        for ma in (state.get("mapped_interaction") or []) + (state.get("mapped_login") or []):
            if ma.account_id == account_id:
                return ma
        return None

    def _selected_mapped() -> list[MappedAccount]:
        out: list[MappedAccount] = []
        tr = _active_tree()
        for iid in tr.selection():
            ma = _mapped_by_id(str(iid))
            if ma is not None:
                out.append(ma)
        return out

    def _show_manual_captcha_guide(ma: MappedAccount) -> None:
        """Hướng dẫn Cách 1 (tick tay) / Cách 2 (proxy User:Pass) — một lần mỗi account mỗi lượt pool."""
        dlg = tk.Toplevel(parent)
        dlg.title("Captcha — cần thao tác tay")
        dlg.transient(parent)
        dlg.resizable(True, True)
        dlg.configure(bg="#fffbeb")
        uid = ma.display_uid()
        tk.Label(
            dlg,
            text=f"Tài khoản {uid} — CapSolver không tự giải được",
            font=("Segoe UI", 11, "bold"),
            bg="#fffbeb",
            fg="#92400e",
            wraplength=520,
            justify=tk.LEFT,
        ).pack(anchor="w", padx=16, pady=(14, 8))
        body = (
            "CÁCH 1 (nhanh nhất — một lần duy nhất):\n"
            "• Cửa sổ Firefox do tool mở đang chờ tối đa 180 giây.\n"
            "• Dùng chuột tích captcha (chọn hình nếu có) → bấm Tiếp tục.\n"
            "• Tool tự đăng nhập và lưu cookie — lần sau không cần làm lại.\n\n"
            "CÁCH 2 (nhiều TK, tự động 100%):\n"
            "• Trang quản lý proxy: tắt Whitelist IP → bật User:Pass.\n"
            "• Ghi user/pass vào proxy trong accounts.json hoặc ô Proxy tab Đăng nhập.\n"
            "• CapSolver hết CONNECT_REFUSED → tự giải Enterprise."
        )
        tk.Label(
            dlg,
            text=body,
            font=("Segoe UI", 9),
            bg="#fffbeb",
            fg="#78350f",
            justify=tk.LEFT,
            wraplength=520,
        ).pack(anchor="w", padx=16, pady=(0, 12))
        tk.Button(
            dlg,
            text="Đã hiểu — tôi sẽ tick captcha trên Firefox",
            command=dlg.destroy,
            font=("Segoe UI", 9, "bold"),
            bg="#059669",
            fg="white",
            activebackground="#047857",
            relief=tk.FLAT,
            padx=12,
            pady=6,
            cursor="hand2",
        ).pack(pady=(0, 14))
        try:
            dlg.geometry("+%d+%d" % (parent.winfo_rootx() + 40, parent.winfo_rooty() + 80))
        except tk.TclError:
            pass

    def _on_status(ma: MappedAccount, status: str, detail: str) -> None:
        def _ui() -> None:
            # Pool đã dọn — bỏ callback trễ (tránh «Đang chạy» treo sau reconcile).
            if state.get("pool") is None:
                terminal = frozenset(
                    {"success", "login_ok", "login_failed", "proxy_error", "error", "cancelled"}
                )
                if str(status or "").strip() not in terminal:
                    return
            ma.status = status
            ma.status_detail = detail
            if status in ("login_ok", "success") and ma.cookie_path:
                if "cookie" not in detail.lower():
                    ma.status_detail = f"{detail} | Cookie: {ma.cookie_path}"[:220]
            low = detail.lower()
            if "captcha thủ công" in low or "tick captcha" in low or "🖐" in detail:
                shown: set[str] = state.setdefault("manual_captcha_shown", set())
                if ma.account_id not in shown:
                    shown.add(ma.account_id)
                    _show_manual_captcha_guide(ma)
            pending: dict[str, MappedAccount] = state.setdefault("_status_pending", {})
            pending[ma.account_id] = ma
            if state.get("_status_flush_id") is None:
                state["_status_flush_id"] = root.after(260, _flush_status_ui_batch)

        schedule_on_main_thread(root, _ui)

    def _load_mapped(*, persist_secrets: bool) -> list[MappedAccount]:
        acc_lines, px_lines = _resolve_input_lines()
        mc = max(1, int(var_threads.get()))
        return map_accounts_with_proxies(
            acc_lines,
            px_lines,
            max_concurrent=mc,
            persist_secrets=persist_secrets,
            account_format=_current_account_format(),
        )

    def _refresh_grid_hint() -> None:
        mc = max(1, int(var_threads.get()))
        gc = _grid_cols_value()
        gr = _grid_rows_value()
        sw, sh = get_screen_resolution()
        slots = compute_grid_layout(mc, max_cols=gc, rows=gr or None)
        if slots:
            s0 = slots[0]
            cols = s0.col + 1 if slots else gc
            for s in slots:
                cols = max(cols, s.col + 1)
            active = _active_capture_count()
            lbl_grid_hint.configure(
                text=(
                    f"Lưới góc trên-trái màn {sw}×{sh}: tối đa {len(slots)} cửa sổ "
                    f"({cols} cột × {max(s.row for s in slots) + 1} hàng), "
                    f"~{s0.width}×{s0.height}px/ô — ô 1 @ ({s0.x},{s0.y}). "
                    f"Mở thủ công: {active}/{mc}. "
                    f"Cần {mc} proxy khác nhau cho {mc} luồng."
                )
            )

    def _filter_accounts_for_interaction(
        accounts: list[MappedAccount],
    ) -> tuple[list[MappedAccount], list[MappedAccount], list[str]]:
        """Tách dòng sẵn sàng tương tác vs dòng chưa đủ điều kiện."""
        from src.services.facebook_session_persist import mapped_account_ready_for_interaction

        ready: list[MappedAccount] = []
        blocked: list[MappedAccount] = []
        reasons: list[str] = []
        for ma in accounts:
            ok, msg = mapped_account_ready_for_interaction(ma)
            if ok:
                ready.append(ma)
            else:
                blocked.append(ma)
                if msg:
                    reasons.append(msg)
        return ready, blocked, reasons

    def _mark_soft_login(accounts: list[MappedAccount], *, enabled: bool) -> None:
        """Bật/tắt đăng nhập nhẹ (chỉ khi mở browser mà chưa có phiên)."""
        for ma in accounts:
            ma.soft_login_if_needed = bool(enabled)

    def _ask_smart_interaction_run(
        ready: list[MappedAccount],
        blocked: list[MappedAccount],
        reasons: list[str],
    ) -> list[MappedAccount] | None:
        """
        Hỏi cách chạy khi có cả TK sẵn sàng và TK chưa chắc phiên.

        Returns:
            Danh sách tài khoản sẽ chạy, hoặc ``None`` nếu user hủy.
        """
        preview = "\n".join(reasons[:6])
        if len(reasons) > 6:
            preview += f"\n… (+{len(reasons) - 6} dòng)"
        if messagebox.askyesno(
            "Chạy tương tác thông minh",
            f"✓ {len(ready)} tài khoản có cookie — vào tương tác ngay (không đăng nhập lại).\n"
            f"? {len(blocked)} tài khoản chưa chắc phiên — thử profile/cookie trước;\n"
            f"  chỉ đăng nhập khi mở browser mà thực sự chưa vào được (không xóa cookie).\n\n"
            f"{preview}\n\n"
            "• Có — chạy TẤT CẢ theo cách trên\n"
            f"• Không — chỉ chạy {len(ready)} tài khoản đã có cookie",
            parent=parent,
        ):
            logger.info(
                "[Human GUI] Chạy thông minh: {} có cookie + {} thử phiên/login nhẹ",
                len(ready),
                len(blocked),
            )
            _mark_soft_login(ready, enabled=False)
            _mark_soft_login(blocked, enabled=True)
            return list(ready) + list(blocked)
        if ready:
            logger.info(
                "[Human GUI] Chỉ chạy {} TK có cookie — bỏ qua {} TK chưa đủ phiên",
                len(ready),
                len(blocked),
            )
            _mark_soft_login(ready, enabled=False)
            return list(ready)
        logger.info("[Human GUI] User hủy — không chạy tương tác")
        return None

    def _ask_soft_login_for_blocked_only(
        blocked: list[MappedAccount],
        reasons: list[str],
    ) -> list[MappedAccount] | None:
        """
        Không có TK nào có cookie — hỏi có thử mở profile/cookie + login nhẹ không.

        Returns:
            Danh sách chạy hoặc ``None`` nếu hủy.
        """
        preview = "\n".join(reasons[:8])
        if len(reasons) > 8:
            preview += f"\n… (+{len(reasons) - 8} dòng)"
        if not messagebox.askyesno(
            "Chưa có cookie phiên",
            f"{len(blocked)} tài khoản chưa có file cookie hợp lệ (c_user).\n\n"
            f"{preview}\n\n"
            "Thử mở profile/cookie trước — chỉ đăng nhập form khi thực sự chưa vào được.\n"
            "(Không xóa cookie, không đăng nhập lại từ đầu nếu phiên còn sống.)\n\n"
            "• Có — chạy thông minh (thử phiên → login nhẹ nếu cần → tương tác)\n"
            "• Không — dừng",
            parent=parent,
        ):
            logger.info("[Human GUI] User từ chối — không chạy TK chưa có cookie")
            return None
        missing_pw = [m for m in blocked if not m.auth.password]
        if missing_pw:
            uids = ", ".join(m.display_uid() for m in missing_pw[:5])
            extra = f" (+{len(missing_pw) - 5})" if len(missing_pw) > 5 else ""
            messagebox.showwarning(
                "Thiếu mật khẩu",
                f"{len(missing_pw)} tài khoản cần pass nếu phải đăng nhập form:\n{uids}{extra}\n"
                "Bổ sung pass trong dòng import.",
                parent=parent,
            )
            return None
        logger.info("[Human GUI] Chạy thông minh {} TK (soft login nếu thiếu phiên)", len(blocked))
        _mark_soft_login(blocked, enabled=True)
        return list(blocked)

    def _start_pool(
        accounts: list[MappedAccount],
        *,
        login_only: bool = False,
        on_pool_finished: Callable[[bool, list[MappedAccount]], None] | None = None,
        is_auto_continue: bool = False,
        relogin: bool = False,
    ) -> None:
        if state.get("pool") or state.get("pool_stopping"):
            if is_auto_continue:
                logger.warning("[Human GUI] Auto-continue trễ — pool vẫn đang chạy/dừng")
                root.after(
                    _pool_auto_continue_delay_ms(),
                    lambda: _start_pool(
                        accounts,
                        login_only=login_only,
                        on_pool_finished=on_pool_finished,
                        is_auto_continue=True,
                    ),
                )
            else:
                messagebox.showwarning(
                    "Đang chạy",
                    "Tiến trình đang chạy hoặc đang dừng — chờ xong rồi mới chạy lượt mới.",
                    parent=parent,
                )
            return
        if not accounts:
            if not is_auto_continue:
                messagebox.showwarning("Chưa chọn", "Chọn ít nhất một dòng trong bảng.", parent=parent)
            return
        _ensure_queue_numbers()
        _refresh_trees()

        if not is_auto_continue:
            state["pool_auto_continue_round"] = 0

        if not login_only:
            from src.utils.account_proxy_mapper import prepare_mapped_account_for_browser_run

            for ma in accounts:
                try:
                    prepare_mapped_account_for_browser_run(ma)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("[Human GUI] Chuẩn bị TK {}: {}", ma.account_id, exc)

        if not login_only and not is_auto_continue:
            ready, blocked, blocked_reasons = _filter_accounts_for_interaction(accounts)
            if ready:
                _refresh_trees()
            if ready and blocked:
                picked = _ask_smart_interaction_run(ready, blocked, blocked_reasons)
                if not picked:
                    return
                accounts = picked
            elif blocked and not ready:
                picked = _ask_soft_login_for_blocked_only(blocked, blocked_reasons)
                if not picked:
                    return
                accounts = picked
            else:
                _mark_soft_login(ready, enabled=False)
                accounts = ready
            if not accounts:
                messagebox.showwarning(
                    "Chưa đủ điều kiện tương tác",
                    "Không có tài khoản nào đủ điều kiện (Đăng nhập OK + cookie).",
                    parent=parent,
                )
                return

        mc = max(1, int(var_threads.get()))
        validate_pool_start(
            len(accounts),
            len(accounts),
            mc,
            unique_proxy_count=count_unique_proxy_servers(accounts),
            accounts=accounts,
        )

        use_headless = bool(var_headless.get())
        if login_only and use_headless:
            messagebox.showinfo(
                "Chế độ có giao diện",
                "Đăng nhập đồng thời cần cửa sổ trình duyệt (không Headless) để chia lưới và dùng chuột.\n"
                "Đã tự tắt Headless cho lượt này.",
                parent=parent,
            )
            var_headless.set(False)
            use_headless = False
        if login_only and mc > 1 and not use_headless:
            _refresh_grid_hint()

        from src.services.facebook_session_persist import cookie_file_has_session

        for ma in accounts:
            if not login_only:
                try:
                    from src.utils.account_proxy_mapper import prepare_mapped_account_for_browser_run

                    prepare_mapped_account_for_browser_run(ma)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("[Human GUI] Chuẩn bị trước pool {}: {}", ma.account_id, exc)
            ck = str(ma.cookie_path or "").strip()
            prof = str(getattr(ma.storage, "profile_path", "") or "").strip()
            prev_st = str(ma.status or "").strip()
            prev_det = str(ma.status_detail or "")
            has_pw = bool(str(getattr(ma.auth, "password", "") or "").strip())
            rerunnable = prev_st in ("cancelled", "login_failed", "error", "pending") or (
                "Chưa chạy" in prev_det
                or "Dừng giữa chừng" in prev_det
                or "nạp cookie" in prev_det
            )
            if not login_only and cookie_file_has_session(ck):
                ma.status = "login_ok"
                ma.status_detail = "Tái sử dụng cookie phiên đã lưu"
                ma.soft_login_if_needed = False
            elif not login_only and prev_st in ("login_ok", "success") and prof:
                ma.soft_login_if_needed = False
                if not prev_det.strip():
                    ma.status_detail = "Tái sử dụng phiên profile portable"
            elif not login_only and rerunnable:
                ma.status = "pending"
                ma.status_detail = "Sẵn sàng chạy lại"
                ma.soft_login_if_needed = bool(has_pw and not cookie_file_has_session(ck))
            else:
                ma.status = "pending"
                if relogin and cookie_file_has_session(ck):
                    ma.status_detail = "Đăng nhập lại — ưu tiên cookie"
                elif login_only and (ma.login_via_cookie or cookie_file_has_session(ck)):
                    ma.status_detail = "Đăng nhập bằng cookie"
                else:
                    ma.status_detail = ""
                if not login_only and has_pw:
                    ma.soft_login_if_needed = False
        _refresh_tree()
        _save_settings()

        profile_name = var_profile.get().strip().lower()
        auto_profile = profile_name == "auto"
        behavior_settings = {
            "like_rate_pct": var_like_pct.get(),
            "comment_rate_pct": var_comment_pct.get(),
            "virtual_cursor": var_virtual_cursor.get(),
            "ai_comments": var_ai_comments.get(),
        }
        profile = resolve_profile(profile_name, settings=behavior_settings)

        pool = HumanInteractionPool(
            accounts,
            max_concurrent=min(mc, len(accounts)),
            headless=use_headless,
            profile=profile,
            auto_profile=auto_profile and not login_only,
            login_only=login_only,
            max_cols=_grid_cols_value(),
            max_rows=_grid_rows_value(),
            on_status=_on_status,
        )
        state["pool_generation"] = int(state.get("pool_generation") or 0) + 1
        pool_generation = int(state["pool_generation"])
        state["pool"] = pool
        state["pool_login_only"] = login_only
        state["pool_stopping"] = False
        _sync_run_banner()
        if is_auto_continue:
            logger.info(
                "[Human GUI] Auto-continue lượt {} — {} TK",
                state.get("pool_auto_continue_round"),
                len(accounts),
            )
        pool.start()
        ids_preview = ", ".join(m.display_uid() for m in accounts[:6])
        if len(accounts) > 6:
            ids_preview += f", … (+{len(accounts) - 6})"
        logger.info(
            "[Human GUI] Pool queue: {} tài khoản [{}]",
            len(accounts),
            ids_preview,
        )
        _schedule_health_refresh()
        _update_summary()

        def _after_pool_join(*, join_ok: bool) -> None:
            """Chạy trên main thread — dừng banner, cập nhật UI, thông báo."""
            # Ngắt callback trạng thái trước reconcile — worker có thể vẫn sống vài giây.
            state["pool"] = None
            workers_alive = bool(getattr(pool, "_join_workers_alive", False))
            user_cancelled = bool(getattr(pool, "is_stopped", lambda: False)())
            _reconcile_accounts_after_pool(
                accounts,
                join_ok=join_ok,
                workers_alive=workers_alive,
                user_cancelled=user_cancelled,
            )
            refresh_mapped_accounts_storage(
                list(state.get("mapped_login") or [])
                + list(state.get("mapped_interaction") or [])
            )
            chain_cb = on_pool_finished
            if chain_cb is not None:
                try:
                    chain_cb(join_ok, accounts)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("[Human GUI] on_pool_finished lỗi: {}", exc)
            _finish_pool_cleanup(pool_ref=pool, generation=pool_generation)
            _save_settings()
            logger.info(
                "[Human GUI] Pool kết thúc (join_ok={} workers_alive={} gen={}).",
                join_ok,
                workers_alive,
                pool_generation,
            )

            pending_continue = _accounts_for_auto_continue(accounts, login_only=login_only)
            if not pending_continue:
                state["pool_auto_continue_round"] = 0
            elif not user_cancelled and _pool_auto_continue_enabled() and chain_cb is None:
                rnd = int(state.get("pool_auto_continue_round") or 0) + 1
                max_r = _pool_auto_continue_max_rounds()
                if rnd <= max_r:
                    state["pool_auto_continue_round"] = rnd
                    for ma in pending_continue:
                        ma.status = "waiting"
                        ma.status_detail = (
                            f"Tự động chạy tiếp — lượt {rnd}/{max_r} "
                            f"({len(pending_continue)} TK còn lại)"
                        )
                    _refresh_trees()
                    _save_settings()
                    logger.info(
                        "[Human GUI] Auto-continue lượt {}/{} | {} TK | join_ok={}",
                        rnd,
                        max_r,
                        len(pending_continue),
                        join_ok,
                    )
                    root.after(
                        _pool_auto_continue_delay_ms(),
                        lambda pending=list(pending_continue): _start_pool(
                            pending,
                            login_only=login_only,
                            is_auto_continue=True,
                        ),
                    )
                    return
                state["pool_auto_continue_round"] = 0
                logger.warning("[Human GUI] Đã hết {} lượt auto-continue", max_r)

            if chain_cb is not None:
                return
            if login_only:
                ok_n = sum(1 for m in accounts if m.status == "login_ok")
                fail_n = sum(1 for m in accounts if m.status == "login_failed")
                cancelled_n = sum(1 for m in accounts if m.status == "cancelled")
                other = len(accounts) - ok_n - fail_n - cancelled_n
                lines = [f"Đã xác nhận vào tài khoản: {ok_n}/{len(accounts)}"]
                if fail_n:
                    lines.append(f"Thất bại / chưa vào được: {fail_n}")
                if cancelled_n:
                    lines.append(f"Đã hủy: {cancelled_n}")
                if other:
                    lines.append(f"Khác (proxy/lỗi): {other}")
                if not join_ok:
                    lines.append(
                        "\nMột số luồng chưa thoát hết — nếu Firefox còn mở, đóng tay hoặc bấm «Dừng»."
                    )
                messagebox.showinfo("Hoàn tất đăng nhập", "\n".join(lines), parent=parent)
            else:
                ok_n = sum(1 for m in accounts if m.status == "success")
                pending_n = len(_accounts_for_auto_continue(accounts, login_only=False))
                msg = f"Đã xử lý xong {len(accounts)} tài khoản (thành công {ok_n})."
                if pending_n and not _pool_auto_continue_enabled():
                    msg += (
                        f"\n\nCòn {pending_n} tài khoản chưa xong — bấm «Chạy» lại."
                    )
                elif pending_n:
                    msg += f"\n\nCòn {pending_n} tài khoản — đã hết lượt tự động, bấm «Chạy» nếu cần."
                messagebox.showinfo("Hoàn tất", msg, parent=parent)

        def _watch() -> None:
            join_timeout = compute_pool_join_timeout_sec(
                len(accounts),
                max_concurrent=min(mc, len(accounts)),
                login_only=login_only,
            )
            logger.info(
                "[Human GUI] Pool join timeout {:.0f}s | {} TK | {} luồng",
                join_timeout,
                len(accounts),
                min(mc, len(accounts)),
            )
            join_ok = False
            try:
                join_ok = bool(pool.join(timeout=join_timeout))
            except Exception as exc:  # noqa: BLE001
                logger.exception("[Human GUI] pool.join lỗi: {}", exc)
            finally:
                gen = pool_generation
                schedule_on_main_thread(
                    root, lambda ok=join_ok, g=gen: _after_pool_join(join_ok=ok)
                )

        threading.Thread(
            target=_watch,
            name=f"human-pool-watch-{pool_generation}",
            daemon=True,
        ).start()

    def _start_pool_when_idle(
        accounts: list[MappedAccount],
        *,
        login_only: bool = False,
        on_pool_finished: Callable[[bool, list[MappedAccount]], None] | None = None,
        relogin: bool = False,
    ) -> None:
        action = "đăng nhập lại" if relogin else ("đăng nhập lượt mới" if login_only else "chạy lượt mới")
        _ensure_pool_stopped_or_ask(
            action,
            lambda: _start_pool(
                accounts,
                login_only=login_only,
                on_pool_finished=on_pool_finished,
                relogin=relogin,
            ),
        )

    def _on_save_cookie_only() -> None:
        """
        Chỉ lưu cookie — không tự chạy tương tác.

        • Pool đang chạy → không dừng, luồng hiện tại tự lưu cookie.
        • Pool rảnh → đăng nhập + lưu cookie rồi dừng (không chạy module).
        """
        from src.services.facebook_session_persist import cookie_file_has_session

        if not state.get("mapped_interaction"):
            messagebox.showwarning(
                "Chưa có tài khoản tương tác",
                "Tab «Tương tác» đang trống — đăng nhập OK trước hoặc chuyển TK từ tab Đăng nhập.",
                parent=parent,
            )
            return
        _select_main_page(page_interaction)
        selected = _selected_mapped()
        if not selected:
            messagebox.showwarning(
                "Chưa chọn dòng",
                "Chọn một hoặc nhiều dòng trong tab «Tương tác» (Ctrl+A = chọn hết).",
                parent=parent,
            )
            return

        if _pool_busy():
            var_run_status.set(
                f"▶  Đang chạy — cookie tự lưu trong luồng ({len(selected)} dòng chọn)"
            )
            logger.info(
                "[Human GUI] Lưu cookie — pool đang chạy, không dừng ({} TK chọn)",
                len(selected),
            )
            return

        need_save: list[MappedAccount] = []
        for ma in selected:
            sync_mapped_account_storage_from_registry(ma)
            ck = str(ma.cookie_path or "").strip()
            if cookie_file_has_session(ck):
                ma.status = "login_ok"
                ma.status_detail = "Cookie đã có — không cần mở browser"
            else:
                need_save.append(ma)

        _refresh_trees()
        _save_settings()

        if not need_save:
            n = len(selected)
            var_run_status.set(f"⏸  {n} TK đã có cookie — không mở browser")
            logger.info("[Human GUI] Lưu cookie — {} TK đã có cookie sẵn", n)
            return

        missing_pw = [m for m in need_save if not m.auth.password]
        if missing_pw:
            uids = ", ".join(m.display_uid() for m in missing_pw[:5])
            extra = f" (+{len(missing_pw) - 5})" if len(missing_pw) > 5 else ""
            messagebox.showwarning(
                "Thiếu mật khẩu",
                f"{len(missing_pw)} dòng chưa có cookie và thiếu pass:\n{uids}{extra}",
                parent=parent,
            )
            return

        for ma in need_save:
            ma.status = "pending"
            ma.status_detail = "Lưu cookie…"
        _refresh_trees()
        var_run_status.set(f"▶  Lưu cookie {len(need_save)} TK — xong sẽ dừng")
        logger.info("[Human GUI] Lưu cookie: đăng nhập {} TK (không chạy tương tác)", len(need_save))
        _start_pool(list(need_save), login_only=True)

    def _refresh_cookie_login(selected: list[MappedAccount]) -> int:
        """Nếu đang chọn định dạng cookie, ghi cookie từ ô dán vào đúng UID trước khi mở browser."""
        fmt = _current_account_format()
        if fmt == "mail":
            return 0
        acc_text = _non_empty_lines(txt_acc.get("1.0", tk.END))
        if not acc_text:
            ap = var_acc.get().strip()
            if ap:
                try:
                    acc_text = read_lines_file(ap)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("[Human GUI] Không đọc file tài khoản để nạp cookie: {}", exc)
                    acc_text = []
        if not acc_text:
            return 0
        return apply_imported_cookies_to_accounts(selected, acc_text, account_format=fmt)

    def _proceed_login_selected(selected: list[MappedAccount]) -> None:
        if not selected:
            messagebox.showwarning(
                "Chưa chọn dòng",
                "Chọn một hoặc nhiều dòng trong bảng (Ctrl+A = chọn hết).",
                parent=parent,
            )
            return
        cookie_ready = _refresh_cookie_login(selected)
        if cookie_ready:
            logger.info("[Human GUI] {} tài khoản đăng nhập bằng cookie", cookie_ready)
        for ma in selected:
            has_cookie_login = bool(ma.login_via_cookie)
            if not ma.auth.password and not has_cookie_login:
                messagebox.showwarning(
                    "Thiếu mật khẩu",
                    f"Dòng UID {ma.display_uid()} chưa có mật khẩu và chưa có cookie.\n"
                    f"Định dạng đang chọn: {var_account_format.get().strip()}",
                    parent=parent,
                )
                return
        n = len(selected)
        via_cookie = sum(1 for ma in selected if ma.login_via_cookie)
        if via_cookie == n:
            prompt = (
                f"Đăng nhập bằng cookie cho {n} tài khoản?\n"
                "Không mở form mật khẩu. Cookie hết hạn thì dòng báo lỗi."
            )
        elif via_cookie:
            prompt = (
                f"Đăng nhập {n} tài khoản?\n"
                f"{via_cookie} dòng dùng cookie, {n - via_cookie} dòng dùng mật khẩu."
            )
        else:
            prompt = (
                f"Tự động đăng nhập Facebook cho {n} tài khoản đã chọn?\n"
                "(Mở browser + profile riêng, lưu cookie sau khi thành công.)"
            )
        if not messagebox.askyesno("Đăng nhập đã chọn", prompt, parent=parent):
            return
        _save_settings()
        _start_pool_when_idle(selected, login_only=True)

    def _on_relogin_selected() -> None:
        """Đăng nhập lại dòng đang chọn — cookie đã lưu được thử trước form mật khẩu."""
        if _pool_busy():
            messagebox.showwarning(
                "Đang chạy",
                "Dừng tiến trình hiện tại trước khi đăng nhập lại.",
                parent=parent,
            )
            return
        selected = _selected_mapped()
        if not selected:
            messagebox.showwarning(
                "Chưa chọn dòng",
                "Chọn một hoặc nhiều tài khoản rồi bấm «Đăng nhập lại».",
                parent=parent,
            )
            return
        from src.services.facebook_session_persist import cookie_file_has_session

        n = len(selected)
        n_ck = sum(
            1
            for ma in selected
            if ma.login_via_cookie or cookie_file_has_session(ma.cookie_path)
        )
        if not messagebox.askyesno(
            "Đăng nhập lại",
            f"Đăng nhập lại {n} tài khoản đã chọn?\n"
            f"{n_ck} dòng có cookie — nạp cookie trước.\n"
            "Cookie vào được thì không mở form mật khẩu. Cookie hết hạn và có mật khẩu thì mới nhập form.",
            parent=parent,
        ):
            return
        _save_settings()
        _start_pool_when_idle(selected, login_only=True, relogin=True)

    def _on_login_selected() -> None:
        if state.get("mapped_login"):
            _select_main_page(page_login)
            _proceed_login_selected(_selected_mapped())
            return

        def work() -> list[MappedAccount]:
            acc_lines, px_lines = _resolve_input_lines()
            mc = max(1, int(var_threads.get()))
            return map_accounts_with_proxies(
                acc_lines,
                px_lines,
                max_concurrent=mc,
                persist_secrets=True,
                account_format=_current_account_format(),
            )

        def ok(mapped: list[MappedAccount]) -> None:
            _merge_into_login_queue(mapped)
            _refresh_trees()
            _save_settings()
            _select_main_page(page_login)
            sel = _selected_mapped()
            if not sel:
                login_rows = state.get("mapped_login") or []
                if login_rows:
                    tree_login.selection_set(login_rows[0].account_id)
                    sel = [login_rows[0]]
            if not sel:
                messagebox.showwarning(
                    "Chưa chọn dòng",
                    "Đã ghép xong — chọn dòng tab «Đăng nhập» rồi bấm «Đăng nhập đã chọn».",
                    parent=parent,
                )
                return
            _proceed_login_selected(sel)

        def err(exc: BaseException) -> None:
            messagebox.showerror("Không thể đăng nhập", str(exc), parent=parent)

        run_background_then_main(root, work, ok, on_error=err)

    def _on_login_all() -> None:
        login_rows = state.get("mapped_login") or []
        if not login_rows:
            if not _ensure_login_queue_loaded(persist_secrets=True):
                return
            login_rows = state.get("mapped_login") or []

        def work() -> list[MappedAccount]:
            mapped = list(login_rows)
            mc = max(1, int(var_threads.get()))
            validate_pool_start(
                len(mapped),
                len(mapped),
                mc,
                unique_proxy_count=count_unique_proxy_servers(mapped),
                accounts=mapped,
            )
            return mapped

        def ok(mapped: list[MappedAccount]) -> None:
            if not mapped:
                messagebox.showwarning("Trống", "Không có tài khoản để đăng nhập.", parent=parent)
                return
            _select_main_page(page_login)
            _proceed_login_selected(list(mapped))

        def err(exc: BaseException) -> None:
            messagebox.showerror("Không thể đăng nhập", str(exc), parent=parent)

        run_background_then_main(root, work, ok, on_error=err)

    def _launch_profile_browser(
        ma: MappedAccount,
        slot: GridWindowSlot,
        acc: dict[str, Any],
        ck_rel: str,
        *,
        for_interaction: bool = False,
        browse_only: bool = False,
    ) -> None:
        """Mở Firefox persistent theo profile tại ô lưới ``slot``."""
        sessions: set[str] = state.setdefault("capture_sessions", set())
        slot_map: dict[str, int] = state.setdefault("capture_slot_by_account", {})
        profile_sessions: dict[str, Any] = state.setdefault("profile_browser_sessions", {})
        sessions.add(ma.account_id)
        slot_map[ma.account_id] = slot.index
        ma.grid_slot_index = slot.index
        ma.status = "running"
        if not str(ma.status_detail or "").strip():
            ma.status_detail = (
                f"Lướt nhận thiết bị — ô {slot.index + 1}"
                if browse_only
                else (
                    f"Mở profile — ô {slot.index + 1}"
                    if for_interaction
                    else f"Mở thủ công — ô {slot.index + 1} (tick captcha/2FA trên Firefox)"
                )
            )
        _refresh_trees()
        _refresh_grid_hint()

        def _on_launch_failed(msg: str) -> None:
            ma.status = "error"
            ma.status_detail = msg[:220]
            _refresh_trees()

        def _on_block(detail: str) -> None:
            """Ghi đúng câu Facebook đang hiện vào dòng tài khoản này."""
            text = str(detail or "").strip()
            if not text or text == str(ma.status_detail or "").strip():
                return
            ma.status = "error"
            ma.status_detail = text[:220]
            _refresh_trees()
            _save_settings()

        def _on_browse_note(detail: str) -> None:
            if ma.status == "error":
                return
            ma.status = "running"
            ma.status_detail = str(detail or "Đang lướt bảng tin — 60 giây")[:220]
            _refresh_trees()

        def _on_browse_done(detail: str) -> None:
            if ma.status == "error":
                return
            ma.status = "login_ok"
            ma.status_detail = str(detail or "Đã lướt 60 giây")[:220]
            _refresh_trees()
            _save_settings()

        def _release_capture() -> None:
            sessions.discard(ma.account_id)
            slot_map.pop(ma.account_id, None)
            profile_sessions.pop(ma.account_id, None)
            if ma.status == "running":
                ma.status = "pending"
                ma.status_detail = "Đã đóng profile — có thể «Mở profile» lại"
            _refresh_trees()
            _refresh_grid_hint()
            if browse_only and state.get("device_browse_active"):
                root.after(50, _start_next_device_browse)

        def _after_save() -> None:
            from src.services.facebook_session_persist import cookie_file_has_session

            ma.cookie_path = ck_rel
            ma.storage.profile_path = str(acc.get("portable_path") or ma.storage.profile_path or "")
            if not cookie_file_has_session(ck_rel):
                ma.status = "login_failed"
                ma.status_detail = "Lưu cookie thất bại — vào www.facebook.com rồi bấm Lưu lại"
                _refresh_trees()
                _save_settings()
                return
            ma.status = "login_ok"
            ma.status_detail = f"Đã lưu cookie (profile, ô {slot.index + 1})"
            if for_interaction:
                _promote_to_interaction(ma)
            logger.info("[Human GUI] Profile — đã lưu cookie account={}", ma.account_id)
            _refresh_trees()
            _save_settings()
            if for_interaction:
                try:
                    _select_main_page(page_interaction)
                except tk.TclError:
                    pass

        _ensure_queue_numbers()
        from src.utils.browser_identity import format_account_window_label

        browser_label = format_account_window_label(int(ma.queue_no or 0), ma.display_uid())
        tip_extra = (
            f"{browser_label} — ô lưới {slot.index + 1}: {slot.width}×{slot.height} @ ({slot.x}, {slot.y}). "
            "Số và UID cũng hiện trên tab Firefox."
        )
        dialog_kw = dict(
            parent=root,
            manager=AccountsDatabaseManager(),
            acc_preview=acc,
            ck_rel=ck_rel,
            log_label=ma.account_id,
            tip_extra=tip_extra,
            on_after_save=_after_save,
            on_dialog_done=_release_capture,
            on_launch_failed=_on_launch_failed,
            grid_viewport=(slot.width, slot.height),
            window_position=(slot.x, slot.y),
            browser_label=browser_label,
            session_registry=profile_sessions,
            on_block=_on_block,
            on_browse_note=_on_browse_note if browse_only else None,
            on_browse_done=_on_browse_done if browse_only else None,
        )
        if for_interaction:
            run_fb_profile_browser_dialog(
                **dialog_kw,
                dialog_title=(
                    f"Lướt nhận thiết bị — {browser_label} (ô {slot.index + 1})"
                    if browse_only
                    else f"Profile — {browser_label} (ô {slot.index + 1})"
                ),
                browse_only=browse_only,
            )
        else:
            run_fb_cookie_capture_dialog(
                **dialog_kw,
                dialog_title=f"Đăng nhập — {browser_label} (ô {slot.index + 1})",
            )

    def _cancel_device_browse() -> None:
        """Dừng hàng lướt: không mở tài khoản tiếp theo và đóng cửa sổ đang lướt."""
        state["device_browse_active"] = False
        waiting_ids = list(state.get("device_browse_queue") or [])
        state["device_browse_queue"] = []
        by_id = {item.account_id: item for item in (state.get("mapped_interaction") or [])}
        for account_id in waiting_ids:
            waiting = by_id.get(account_id)
            if waiting is not None and waiting.status == "waiting":
                waiting.status = "pending"
                waiting.status_detail = "Đã dừng hàng lướt"
        for sess in list((state.get("profile_browser_sessions") or {}).values()):
            try:
                sess["cmd_q"].put("close")
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human GUI] Dừng lướt: {}", exc)
        _refresh_trees()
        var_run_status.set("Đã dừng lướt nhận thiết bị")

    def _start_next_device_browse() -> None:
        """Mở tài khoản kế tiếp trong hàng lướt khi còn ô trống."""
        if not state.get("device_browse_active"):
            return
        queue_ids = list(state.get("device_browse_queue") or [])
        if not queue_ids:
            if _active_capture_count() == 0:
                state["device_browse_active"] = False
                var_run_status.set("Đã lướt xong các tài khoản trong hàng")
            return
        free_slots = _allocate_capture_slots(1)
        if not free_slots:
            return
        account_id = queue_ids.pop(0)
        state["device_browse_queue"] = queue_ids
        ma = next(
            (item for item in (state.get("mapped_interaction") or []) if item.account_id == account_id),
            None,
        )
        if ma is None:
            root.after(0, _start_next_device_browse)
            return
        slot = free_slots[0]
        sessions: set[str] = state.setdefault("capture_sessions", set())
        slot_map: dict[str, int] = state.setdefault("capture_slot_by_account", {})
        sessions.add(ma.account_id)
        slot_map[ma.account_id] = slot.index
        left = len(queue_ids)
        ma.status = "running"
        ma.status_detail = f"Đang tới lượt lướt — còn {left} tài khoản"
        _refresh_trees()
        _refresh_grid_hint()
        var_run_status.set(f"Đang lướt nhận thiết bị — còn {left} tài khoản")

        def work() -> tuple[MappedAccount, GridWindowSlot, dict[str, Any], str]:
            from src.utils.account_proxy_mapper import prepare_mapped_account_for_browser_run

            apply_mapped_secrets_to_vault(ma)
            ok_px, px_msg = ensure_mapped_proxy_live(ma)
            if not ok_px:
                raise ValueError(f"proxy chưa LIVE — {px_msg}")
            acc = prepare_mapped_account_for_browser_run(ma)
            ck_rel = str(acc.get("cookie_path") or ma.cookie_path or "").strip()
            return ma, slot, acc, ck_rel

        def ok(row: tuple[MappedAccount, GridWindowSlot, dict[str, Any], str]) -> None:
            if not state.get("device_browse_active"):
                sessions.discard(ma.account_id)
                slot_map.pop(ma.account_id, None)
                if ma.status == "running":
                    ma.status = "pending"
                    ma.status_detail = "Đã dừng hàng lướt"
                _refresh_trees()
                _refresh_grid_hint()
                return
            got_ma, got_slot, acc, ck_rel = row
            _launch_profile_browser(
                got_ma,
                got_slot,
                acc,
                ck_rel,
                for_interaction=True,
                browse_only=True,
            )

        def err(exc: BaseException) -> None:
            sessions.discard(ma.account_id)
            slot_map.pop(ma.account_id, None)
            ma.status = "proxy_error"
            ma.status_detail = str(exc)[:220]
            _refresh_trees()
            _refresh_grid_hint()
            _start_next_device_browse()

        run_background_then_main(root, work, ok, on_error=err)

    def _launch_manual_browser_capture(ma: MappedAccount, slot: GridWindowSlot, acc: dict[str, Any], ck_rel: str) -> None:
        """Alias tab Đăng nhập."""
        _launch_profile_browser(ma, slot, acc, ck_rel, for_interaction=False)

    def _open_profile_browsers_impl(*, for_interaction: bool = False, browse_only: bool = False) -> None:
        if for_interaction:
            if not state.get("mapped_interaction"):
                messagebox.showwarning(
                    "Chưa có tài khoản",
                    "Tab «Tương tác» đang trống.",
                    parent=parent,
                )
                return
            _select_main_page(page_interaction)
        else:
            if not state.get("mapped_login") and not _ensure_login_queue_loaded(persist_secrets=True):
                return
            _select_main_page(page_login)

        selected = _selected_mapped()
        if not selected:
            rows = state.get("mapped_interaction" if for_interaction else "mapped_login") or []
            if len(rows) == 1:
                tr = tree_interaction if for_interaction else tree_login
                tr.selection_set(rows[0].account_id)
                selected = rows
        if not selected:
            messagebox.showwarning(
                "Chưa chọn dòng",
                "Chọn một hoặc nhiều dòng (Ctrl+A = chọn hết).",
                parent=parent,
            )
            return

        mc = _max_capture_windows()
        already = {aid for aid in (state.get("capture_sessions") or set())}
        pending = [ma for ma in selected if ma.account_id not in already]
        if not pending:
            messagebox.showinfo(
                "Đã mở",
                "Các dòng đã chọn đang có cửa sổ mở — bấm «Đóng profile» hoặc «Đóng» trên hộp thoại.",
                parent=parent,
            )
            return

        free_slots = _allocate_capture_slots(len(pending))
        if not free_slots:
            messagebox.showwarning(
                "Đủ cửa sổ",
                f"Đang mở {_active_capture_count()}/{mc} cửa sổ.\n"
                "Đóng bớt hoặc tăng «Số luồng» trước khi mở thêm.",
                parent=parent,
            )
            return

        if browse_only:
            state["device_browse_active"] = True
            state["device_browse_queue"] = [ma.account_id for ma in pending]
            total = len(pending)
            for index, ma in enumerate(pending, start=1):
                ma.status = "waiting"
                ma.status_detail = f"Đang chờ lượt lướt ({index}/{total})"
            _refresh_trees()
            var_run_status.set(f"Lướt lần lượt {total} tài khoản — mỗi tài khoản 60 giây")
            starters = min(total, len(free_slots), _max_capture_windows())
            for _ in range(starters):
                _start_next_device_browse()
            return

        can_open = min(len(pending), len(free_slots))
        if len(pending) > can_open:
            if not messagebox.askyesno(
                "Giới hạn lưới",
                f"Chỉ mở thêm được {can_open} cửa sổ (tối đa {mc} cùng lúc).\n"
                f"Mở {can_open} dòng đầu trong phần chọn?",
                parent=parent,
            ):
                return
            pending = pending[:can_open]
            free_slots = free_slots[:can_open]

        pairs = list(zip(pending, free_slots, strict=True))

        def work() -> list[tuple[MappedAccount, GridWindowSlot, dict[str, Any], str]]:
            from src.utils.account_proxy_mapper import prepare_mapped_account_for_browser_run

            pending_only = [ma for ma, _ in pairs]
            assert_proxy_exclusive_among_accounts(
                _all_mapped_accounts() + pending_only,
                registry_index=load_registry_proxy_index(),
                context="mở profile trình duyệt",
            )
            out: list[tuple[MappedAccount, GridWindowSlot, dict[str, Any], str]] = []
            for ma, slot in pairs:
                apply_mapped_secrets_to_vault(ma)
                ok_px, px_msg = ensure_mapped_proxy_live(ma)
                if not ok_px:
                    raise ValueError(f"{ma.display_uid()}: proxy chưa LIVE — {px_msg}")
                acc = prepare_mapped_account_for_browser_run(ma)
                ck_rel = str(acc.get("cookie_path") or ma.cookie_path or "").strip()
                out.append((ma, slot, acc, ck_rel))
            return out

        def ok(rows: list[tuple[MappedAccount, GridWindowSlot, dict[str, Any], str]]) -> None:
            _save_settings()
            for ma, slot, acc, ck_rel in rows:
                ma.status = "running"
                ma.status_detail = (
                    "Đang lướt — 60 giây"
                    if browse_only
                    else "Proxy LIVE — đang mở Firefox…"
                )
            _refresh_trees()
            for ma, slot, acc, ck_rel in rows:
                logger.info(
                    "[Human GUI] Mở profile account={} ô={} profile={}",
                    ma.account_id,
                    slot.index + 1,
                    acc.get("portable_path") or "",
                )
                _launch_profile_browser(
                    ma,
                    slot,
                    acc,
                    ck_rel,
                    for_interaction=for_interaction,
                    browse_only=browse_only,
                )

        def err(exc: BaseException) -> None:
            messagebox.showerror("Không mở được trình duyệt", str(exc), parent=parent)

        run_background_then_main(root, work, ok, on_error=err)

    def _on_open_browser_login() -> None:
        """Tab Đăng nhập — mở Firefox đăng nhập tay."""
        if _pool_busy():
            _ensure_pool_stopped_or_ask("mở trình duyệt đăng nhập thủ công", _on_open_browser_login)
            return
        _open_profile_browsers_impl(for_interaction=False)

    def _on_open_profile_browser() -> None:
        """Tab Tương tác — mở Firefox theo profile đã chọn."""
        if _pool_busy():
            _ensure_pool_stopped_or_ask("mở profile trình duyệt", _on_open_profile_browser)
            return
        _open_profile_browsers_impl(for_interaction=True)

    def _on_browse_for_device() -> None:
        """Mở tài khoản đã đăng nhập và chỉ lướt bảng tin."""
        if _pool_busy():
            _ensure_pool_stopped_or_ask("lướt nhận thiết bị", _on_browse_for_device)
            return
        _open_profile_browsers_impl(for_interaction=True, browse_only=True)

    def _on_close_profile_browser() -> None:
        """Đóng cửa sổ Firefox profile đang mở (dòng đã chọn)."""
        selected = _selected_mapped()
        if not selected:
            messagebox.showwarning(
                "Chưa chọn dòng",
                "Chọn dòng đang mở profile để đóng.",
                parent=parent,
            )
            return
        sessions = state.get("profile_browser_sessions") or {}
        closed = 0
        for ma in selected:
            sess = sessions.get(ma.account_id)
            if not sess:
                continue
            try:
                sess["cmd_q"].put("close")
                closed += 1
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human GUI] Đóng profile {}: {}", ma.account_id, exc)
        if closed == 0:
            messagebox.showinfo(
                "Chưa mở profile",
                "Không có cửa sổ Firefox đang mở cho dòng đã chọn.\n"
                "Dùng «Mở profile» hoặc đóng trên hộp thoại.",
                parent=parent,
            )
            return
        logger.info("[Human GUI] Yêu cầu đóng {} cửa sổ profile", closed)
        var_run_status.set(f"Đang đóng {closed} cửa sổ profile…")

    def _on_merge() -> None:
        try:
            acc_lines, px_lines = _resolve_input_lines()
        except AccountProxyMappingError as exc:
            messagebox.showerror("Lỗi ghép", str(exc), parent=parent)
            return
        blocked = _queue_proxy_owners()
        account_format = _current_account_format()
        mc = max(1, int(var_threads.get()))

        def work() -> tuple[list[MappedAccount], int, int]:
            mapped = map_accounts_with_proxies(
                acc_lines,
                px_lines,
                max_concurrent=mc,
                persist_secrets=False,
                account_format=account_format,
                extra_blocked=blocked,
            )
            return mapped, len(acc_lines), len(px_lines)

        def ok(payload: tuple[list[MappedAccount], int, int]) -> None:
            try:
                _finish_merge(payload, px_lines)
            except Exception as exc:  # noqa: BLE001
                logger.exception("[Human GUI] Ghép xong nhưng không đổ được bảng")
                messagebox.showerror("Lỗi ghép", str(exc), parent=parent)

        def err(exc: BaseException) -> None:
            messagebox.showerror("Lỗi ghép", str(exc), parent=parent)

        run_background_then_main(root, work, ok, on_error=err)

    def _finish_merge(payload: tuple[list[MappedAccount], int, int], px_lines: list[str]) -> None:
        mapped, n_acc, n_px = payload
        added, updated, skipped = _merge_into_login_queue(mapped, px_lines)
        _refresh_trees()
        _save_settings()
        _select_main_page(page_login)
        try:
            _init_login_paned_sash()
        except Exception:  # noqa: BLE001
            pass
        login_rows = state.get("mapped_login") or []
        if login_rows:
            try:
                tree_login.selection_set(login_rows[-1].account_id)
                tree_login.see(login_rows[-1].account_id)
            except tk.TclError:
                pass
        n_login = len(login_rows)
        n_int = len(state.get("mapped_interaction") or [])
        msg = (
            f"Đã ghép {len(mapped)} dòng → hàng đợi Đăng nhập: +{added} mới, cập nhật {updated}.\n"
            f"Tab Tương tác giữ nguyên {n_int} tài khoản đã login"
        )
        if skipped:
            msg += (
                f"\nBỏ qua {skipped} dòng (đã ở tab Tương tác, hoặc hết proxy trống)."
            )
        msg += f"\nĐịnh dạng nick: {var_account_format.get().strip()}"
        if len(mapped) < n_acc:
            msg += (
                f"\n\n{n_acc - len(mapped)} tài khoản chưa ghép: hết proxy trống. "
                "Proxy đã gắn tài khoản khác được bỏ qua và thay bằng proxy khác trong list khi còn."
            )
        mc = max(1, int(var_threads.get()))
        if n_px < mc:
            msg += f"\n\nLưu ý: {n_px} proxy < {mc} luồng — khi chạy tối đa {min(n_px, len(mapped))} song song."
        uniq = count_unique_proxy_servers(login_rows)
        if login_rows and uniq == len(login_rows):
            msg += f"\n\n✓ {uniq} proxy riêng — đủ cho chạy song song."
        msg += f"\n\nHiện có: {n_login} chờ login | {n_int} sẵn tương tác."
        if not login_rows:
            msg = (
                "Không đưa được dòng nào vào danh sách chờ đăng nhập.\n"
                "Tài khoản đã nằm ở tab Tương tác, hoặc mọi proxy trong list đã gắn nick khác."
            )
        messagebox.showinfo("Ghép & hiển thị", msg, parent=parent)

    def _on_run_all() -> None:
        interaction_rows = state.get("mapped_interaction") or []
        if not interaction_rows:
            messagebox.showwarning(
                "Chưa có tài khoản tương tác",
                "Mở tab «Tương tác» sau khi đăng nhập OK — tài khoản OK sẽ chuyển sang lúc đó.",
                parent=parent,
            )
            return

        def work() -> list[MappedAccount]:
            mapped = list(interaction_rows)
            mc = max(1, int(var_threads.get()))
            validate_pool_start(
                len(mapped),
                len(mapped),
                mc,
                unique_proxy_count=count_unique_proxy_servers(mapped),
                accounts=mapped,
            )
            return mapped

        def ok(mapped: list[MappedAccount]) -> None:
            _select_main_page(page_interaction)
            _start_pool_when_idle(list(mapped))

        def err(exc: BaseException) -> None:
            messagebox.showerror("Không thể chạy", str(exc), parent=parent)

        run_background_then_main(root, work, ok, on_error=err)

    def _on_run_selected() -> None:
        if not state.get("mapped_interaction"):
            messagebox.showwarning(
                "Chưa có tài khoản tương tác",
                "Mở tab «Tương tác» — tài khoản Đăng nhập OK sẽ chuyển sang khi bạn mở tab này.",
                parent=parent,
            )
            return
        _select_main_page(page_interaction)
        selected = _selected_mapped()
        if not selected:
            messagebox.showwarning(
                "Chưa chọn dòng",
                "Chọn một hoặc nhiều dòng trong tab «Tương tác».",
                parent=parent,
            )
            return
        n = len(selected)
        if not messagebox.askyesno(
            "Chạy đã chọn",
            f"Chạy tương tác cho {n} tài khoản đã chọn?",
            parent=parent,
        ):
            return
        _start_pool_when_idle(selected)

    def _on_run_one() -> None:
        if not state.get("mapped_interaction"):
            messagebox.showwarning("Chưa có tài khoản tương tác", "Đăng nhập OK trước.", parent=parent)
            return
        _select_main_page(page_interaction)
        selected = _selected_mapped()
        if len(selected) != 1:
            messagebox.showwarning(
                "Chọn một dòng",
                "Chọn đúng 1 dòng trong tab «Tương tác» (double-click để chạy).",
                parent=parent,
            )
            return
        _start_pool_when_idle(selected)

    def _on_reset_selected() -> None:
        selected = _selected_mapped()
        if not selected:
            messagebox.showwarning("Chưa chọn", "Chọn dòng cần đặt lại trạng thái.", parent=parent)
            return
        for ma in selected:
            ma.status = "pending"
            ma.status_detail = ""
        _refresh_tree()

    def _on_reassign_proxy() -> None:
        """Gán proxy mới cho TK lỗi proxy — lấy từ list tab Đăng nhập, không trùng IP:port."""
        if _pool_busy():
            messagebox.showwarning(
                "Đang chạy",
                "Dừng pool trước khi đổi proxy.",
                parent=parent,
            )
            return
        skip_status = {"running", "success", "login_ok"}
        selected = _selected_mapped()
        if selected:
            targets = [ma for ma in selected if ma.status not in skip_status]
            if not targets:
                messagebox.showinfo(
                    "Không đổi proxy",
                    "Dòng đã chọn đang chạy hoặc đã login OK — không đổi proxy.",
                    parent=parent,
                )
                return
        elif _is_interaction_tab_active():
            pool_rows = state.get("mapped_interaction") or []
            targets = [ma for ma in pool_rows if ma.status == "proxy_error"]
            if not targets:
                targets = [ma for ma in pool_rows if ma.status not in skip_status]
        else:
            pool_rows = state.get("mapped_login") or []
            targets = [ma for ma in pool_rows if ma.status == "proxy_error"]
            if not targets:
                targets = [ma for ma in pool_rows if ma.status not in skip_status]
        if not targets:
            messagebox.showinfo(
                "Không có tài khoản",
                "Không có dòng cần đổi proxy.\n"
                "Chọn dòng (kể cả «Đã hủy» / «Lỗi Proxy») hoặc dán proxy mới ở tab Đăng nhập.",
                parent=parent,
            )
            return
        try:
            px_lines = _resolve_proxy_pool_lines()
        except AccountProxyMappingError as exc:
            messagebox.showerror("Thiếu proxy", str(exc), parent=parent)
            return
        preview = ", ".join(ma.display_uid() for ma in targets[:5])
        if len(targets) > 5:
            preview += f" … (+{len(targets) - 5})"
        if not messagebox.askyesno(
            "Cập nhật proxy",
            f"Gán proxy mới cho {len(targets)} tài khoản:\n{preview}\n\n"
            f"Đọc {len(px_lines)} dòng từ ô Proxy + file ở tab Đăng nhập "
            "(dòng mới, kể cả cùng gateway khác user, được ưu tiên).",
            parent=parent,
        ):
            return
        try:
            res = reassign_proxies_from_pool(
                targets,
                all_accounts=_all_mapped_accounts(),
                proxy_lines=px_lines,
            )
        except AccountProxyMappingError as exc:
            messagebox.showerror("Lỗi proxy", str(exc), parent=parent)
            return
        persisted = 0
        for aid in res["updated"]:
            ma = _mapped_by_id(aid)
            if ma and persist_mapped_proxy_to_accounts_json(ma):
                persisted += 1
        _save_settings()
        _refresh_tree()
        lines = [f"Đã đổi proxy: {len(res['updated'])} tài khoản."]
        if persisted:
            lines.append(f"Ghi accounts.json: {persisted}.")
        if res["skipped"]:
            lines.append(f"Không đủ proxy trống: {len(res['skipped'])}.")
            for aid, reason in res["skipped"][:5]:
                ma = _mapped_by_id(aid)
                uid = ma.display_uid() if ma else aid
                lines.append(f"  • {uid}: {reason}")
        messagebox.showinfo("Cập nhật proxy", "\n".join(lines), parent=parent)
        logger.info("[Human GUI] Cập nhật proxy: {}", res)

    def _on_export_to_accounts_registry() -> None:
        """Đưa TK tab Tương tác vào accounts.json — dùng tab «Tài khoản» / lịch đăng."""
        if _pool_busy():
            messagebox.showwarning(
                "Đang chạy",
                "Dừng pool trước khi chuyển tài khoản sang tab «Tài khoản».",
                parent=parent,
            )
            return
        if not _is_interaction_tab_active():
            messagebox.showinfo(
                "Tab Tương tác",
                "Chuyển tài khoản đã đăng nhập từ tab «Tương tác (đã login OK)».\n"
                "Mở sub-tab «TƯƠNG TÁC» rồi chọn dòng cần chuyển.",
                parent=parent,
            )
            return
        selected = _selected_mapped()
        if selected:
            targets = list(selected)
        else:
            targets = list(state.get("mapped_interaction") or [])
        if not targets:
            messagebox.showinfo(
                "Chưa có tài khoản",
                "Tab «Tương tác» chưa có tài khoản nào.\n"
                "Đăng nhập xong, mở tab «Tương tác» để chuyển các TK Đăng nhập OK sang đây.",
                parent=parent,
            )
            return
        preview = ", ".join(ma.display_uid() for ma in targets[:6])
        if len(targets) > 6:
            preview += f" … (+{len(targets) - 6})"
        if not messagebox.askyesno(
            "Chuyển sang tab Tài khoản",
            f"Ghi {len(targets)} tài khoản vào accounts.json để dùng đăng lịch:\n{preview}\n\n"
            "• Giữ profile/cookie/proxy hiện có\n"
            "• Cập nhật nếu UID đã có trong tab «Tài khoản»\n"
            "• Vẫn giữ trong tab «Tương tác» (có thể xóa tay sau)",
            parent=parent,
        ):
            return
        try:
            res = export_mapped_accounts_to_registry(targets)
        except AccountProxyMappingError as exc:
            messagebox.showerror("Lỗi accounts.json", str(exc), parent=parent)
            return
        exported_ids = list(res["added"]) + list(res["updated"])
        lines = []
        if res["added"]:
            lines.append(f"Thêm mới: {len(res['added'])} — {', '.join(res['added'][:8])}")
        if res["updated"]:
            lines.append(f"Cập nhật: {len(res['updated'])} — {', '.join(res['updated'][:8])}")
        if res["skipped"]:
            lines.append(f"Bỏ qua: {len(res['skipped'])}")
            for aid, reason in res["skipped"][:5]:
                ma = _mapped_by_id(aid)
                uid = ma.display_uid() if ma else aid
                lines.append(f"  • {uid}: {reason}")
        if not exported_ids:
            messagebox.showwarning(
                "Không chuyển được",
                "\n".join(lines) or "Không có tài khoản nào đủ điều kiện (cần đăng nhập OK + cookie).",
                parent=parent,
            )
            return
        messagebox.showinfo(
            "Đã ghi accounts.json",
            "\n".join(lines)
            + "\n\nMở tab «1. Tài khoản» → chỉnh lịch/topic → tab «3. Job lịch đăng» để tạo job.",
            parent=parent,
        )
        logger.info("[Human GUI] Export registry: {}", res)
        if on_accounts_registry_changed and exported_ids:
            try:
                on_accounts_registry_changed(exported_ids)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[Human GUI] Callback tab Tài khoản: {}", exc)

    def _registry_account_id_for_mapped(ma: MappedAccount) -> str | None:
        """``id`` trong accounts.json nếu có (khớp account_id hoặc facebook_uid)."""
        aid = str(ma.account_id or "").strip()
        if not aid:
            return None
        try:
            db = AccountsDatabaseManager()
            rows = db.load_all()
        except Exception:
            return None
        for rec in rows:
            if str(rec.get("id") or "").strip() == aid:
                return aid
        uid = ma.display_uid()
        if uid.isdigit():
            for rec in rows:
                if str(rec.get("facebook_uid") or "").strip() == uid:
                    return str(rec.get("id") or "").strip() or None
        return None

    def _on_delete_selected() -> None:
        """Xóa dòng đã chọn khỏi bảng + profile Firefox/cookie/vault trên đĩa."""
        selected = _selected_mapped()
        if not selected:
            messagebox.showwarning(
                "Chưa chọn",
                "Chọn một hoặc nhiều dòng (kéo chuột / Shift / Ctrl) rồi bấm «Xóa đã chọn».",
                parent=parent,
            )
            return
        if state.get("pool"):
            busy = [
                ma.account_id
                for ma in selected
                if ma.status in ("running", "waiting", "proxy_busy")
            ]
            if busy:
                messagebox.showwarning(
                    "Đang chạy",
                    "Pool đang chạy — bấm «Dừng» và chờ kết thúc trước khi xóa profile đang hoạt động.",
                    parent=parent,
                )
                return
        ids_preview = ", ".join(ma.account_id for ma in selected[:12])
        if len(selected) > 12:
            ids_preview += f", … (+{len(selected) - 12})"
        if len(selected) == 1:
            q = (
                f"Xóa profile {selected[0].account_id!r}?\n\n"
                "• Gỡ khỏi bảng Tương tác người dùng\n"
                "• Xóa thư mục profile Firefox/Playwright + cookie + vault (nếu có)\n"
                "• Xóa bản ghi trong accounts.json nếu trùng id/UID"
            )
        else:
            q = (
                f"Xóa {len(selected)} profile đã chọn?\n\n{ids_preview}\n\n"
                "• Gỡ khỏi bảng\n"
                "• Xóa profile/cookie/vault trên đĩa\n"
                "• Xóa trong accounts.json nếu có"
            )
        if not messagebox.askyesno("Xác nhận xóa profile", q, parent=parent, icon="warning"):
            return

        db = AccountsDatabaseManager()
        removed_rows = 0
        registry_removed = 0
        failed_registry: list[str] = []

        for ma in selected:
            reg_id = _registry_account_id_for_mapped(ma)
            if reg_id:
                if db.delete_by_id(reg_id):
                    registry_removed += 1
                else:
                    failed_registry.append(reg_id)
                    delete_account_browser_bundle(mapped_account_to_account_dict(ma))
            else:
                delete_account_browser_bundle(mapped_account_to_account_dict(ma))

            state.get("capture_sessions", set()).discard(ma.account_id)
            state.get("capture_slot_by_account", {}).pop(ma.account_id, None)

        sel_ids = {ma.account_id for ma in selected}
        if _is_interaction_tab_active():
            state["mapped_interaction"] = [
                m for m in (state.get("mapped_interaction") or []) if m.account_id not in sel_ids
            ]
        else:
            state["mapped_login"] = [
                m for m in (state.get("mapped_login") or []) if m.account_id not in sel_ids
            ]
        removed_rows = len(selected)
        _refresh_trees()
        _save_settings()
        _update_summary()

        msg = f"Đã xóa {removed_rows} dòng khỏi bảng."
        if registry_removed:
            msg += f"\nĐã gỡ {registry_removed} tài khoản khỏi accounts.json."
        if failed_registry:
            msg += f"\nKhông xóa được trong JSON: {', '.join(failed_registry)}"
        messagebox.showinfo("Xóa profile", msg, parent=parent)
        logger.info(
            "[Human GUI] Đã xóa {} profile (registry={}): [{}]",
            removed_rows,
            registry_removed,
            ", ".join(sorted(sel_ids)),
        )

    def _on_reset_all() -> None:
        if _is_interaction_tab_active():
            mapped = state.get("mapped_interaction") or []
            label = "tab Tương tác"
        else:
            mapped = state.get("mapped_login") or []
            label = "tab Đăng nhập"
        if not mapped:
            return
        if not messagebox.askyesno("Đặt lại tất cả", f"Đặt mọi dòng {label} về «Đang chờ»?", parent=parent):
            return
        for ma in mapped:
            ma.status = "pending"
            ma.status_detail = ""
        _refresh_trees()

    def _on_stop() -> None:
        if state.get("device_browse_active"):
            _cancel_device_browse()
            if not state.get("pool"):
                return
        if not state.get("pool"):
            if state.get("pool_stopping"):
                messagebox.showinfo(
                    "Đang dừng",
                    "Tiến trình đang được dừng — vui lòng chờ vài giây.",
                    parent=parent,
                )
            return
        if state.get("pool_stopping"):
            return
        if not messagebox.askyesno(
            "Dừng tiến trình",
            "Dừng tất cả luồng đang chạy?\n(Các bước đang làm sẽ kết thúc trước khi dừng hẳn.)",
            parent=parent,
        ):
            return
        _request_pool_stop(show_dialog=True)
        _wait_pool_idle_then(lambda: _update_summary())

    def _on_save_inputs() -> None:
        _save_settings()
        messagebox.showinfo("Đã lưu", "Đã lưu ô nhập + cấu hình.", parent=parent)

    def _schedule_health_refresh() -> None:
        pool = state.get("pool")
        if not pool:
            if state.get("pool_stopping"):
                health_fr.configure(bg=_C_STOPPING_BG)
                lbl_health.configure(
                    text="⏳ Đang dừng pool…",
                    bg=_C_STOPPING_BG,
                    fg=_C_STOPPING_FG,
                )
            else:
                health_fr.configure(bg=_C_HEALTH_BG)
                lbl_health.configure(text="💤 Idle — chưa chạy pool", bg=_C_HEALTH_BG, fg=_C_HEALTH_FG)
            _sync_run_banner()
            _update_summary()
            return
        if state.get("pool_stopping"):
            try:
                alive = bool(pool.has_live_workers())
            except Exception:
                alive = True
            if not alive:
                health_fr.configure(bg=_C_HEALTH_BG)
                lbl_health.configure(text="💤 Idle — các luồng đã dừng", bg=_C_HEALTH_BG, fg=_C_HEALTH_FG)
                _sync_run_banner()
                _update_summary()
                root.after(1200, _schedule_health_refresh)
                return
        snap = pool.health_snapshot()
        health_fr.configure(bg="#d1fae5")
        lbl_health.configure(
            text=(
                f"▶ {snap['running']}/{snap['dynamic_limit']} luồng (max {snap['configured_limit']})  ·  "
                f"tiến độ {snap.get('completed_accounts', 0)}/{snap.get('total_accounts', 0)}  ·  "
                f"còn {snap.get('pending_accounts', 0)} TK  ·  "
                f"profile={snap.get('profile', 'normal')}{'*' if snap.get('auto_profile') else ''}  ·  "
                f"ok={snap['recent_success']}  proxy_err={snap['recent_proxy_error']}  err={snap['recent_error']}"
            ),
            bg="#d1fae5",
            fg="#065f46",
        )
        _sync_run_banner()
        _update_summary()
        root.after(1200, _schedule_health_refresh)

    def _on_tree_select(_event: Any = None) -> None:
        _update_summary()

    def _on_tree_double_click(event: Any) -> None:
        tr = event.widget
        if not isinstance(tr, ttk.Treeview):
            tr = _active_tree()
        iid = tr.identify_row(event.y)
        if not iid:
            return
        tr.selection_set(iid)
        tr.focus(iid)
        ma = _mapped_by_id(str(iid))
        if not ma:
            return
        _open_attribute_dialog()

    def _bind_tree_events(tr: ttk.Treeview) -> None:
        tr.bind("<<TreeviewSelect>>", _on_tree_select)
        tr.bind("<Double-1>", _on_tree_double_click)

    _bind_tree_events(tree_login)
    _bind_tree_events(tree_interaction)
    def _on_main_tab_changed(_event: object = None) -> None:
        """Mở tab Tương tác thì mới chuyển các tài khoản đã đăng nhập OK."""
        if _is_interaction_tab_active() and _promote_ready_logins_to_interaction():
            _refresh_trees()
            _save_settings()
            return
        _update_summary()

    nb_main.bind("<<NotebookTabChanged>>", _on_main_tab_changed)

    def _open_check_live_dialog() -> None:
        """Check live tài khoản từ list dán riêng — không ghi vào hàng đợi Đăng nhập."""
        if _pool_busy():
            messagebox.showwarning(
                "Đang chạy",
                "Dừng đăng nhập / tương tác trước khi check live.",
                parent=parent,
            )
            return
        win = tk.Toplevel(parent)
        win.title("Check Live tài khoản")
        win.geometry("760x560")
        win.transient(parent)
        ttk.Label(
            win,
            text=(
                "Chọn một tài khoản đã đăng nhập. Bấm Hiện trình duyệt để xem trang đang check, "
                "hoặc Ẩn trình duyệt khi không cần nhìn. "
                "Chỉ kết luận khi trang khóa (Die) hoặc hồ sơ đã hiện (Live). "
                "UID lấy từ số hoặc cookie c_user=1000…. Live xếp trên, Die xếp dưới."
            ),
            wraplength=720,
        ).pack(anchor="w", padx=10, pady=(8, 4))
        viewer_row = ttk.Frame(win)
        viewer_row.pack(fill=tk.X, padx=10, pady=(0, 4))
        ttk.Label(viewer_row, text="Tài khoản đã đăng nhập").pack(side=tk.LEFT)
        var_viewer = tk.StringVar()
        viewer_box = ttk.Combobox(viewer_row, textvariable=var_viewer, state="readonly", width=48)
        viewer_box.pack(side=tk.LEFT, padx=6)
        viewer_choices: dict[str, tuple[str, str]] = {}

        def _reload_viewers() -> None:
            viewer_choices.clear()
            labels: list[str] = []
            seen: set[str] = set()
            for bucket in ("mapped_interaction", "mapped_login"):
                for ma in state.get(bucket) or []:
                    if bucket == "mapped_login" and ma.status not in ("login_ok", "success"):
                        continue
                    if ma.account_id in seen:
                        continue
                    seen.add(ma.account_id)
                    label = f"{ma.display_uid()} · đã đăng nhập"
                    viewer_choices[label] = ("mapped", ma.account_id)
                    labels.append(label)
            try:
                from src.utils.db_manager import AccountsDatabaseManager

                for row in AccountsDatabaseManager().load_all():
                    aid = str(row.get("id") or "")
                    if not aid or aid in seen:
                        continue
                    seen.add(aid)
                    label = f"{row.get('name') or aid} · profile đã lưu"
                    viewer_choices[label] = ("registry", aid)
                    labels.append(label)
            except Exception:  # noqa: BLE001
                pass
            viewer_box["values"] = labels
            if var_viewer.get() not in viewer_choices and labels:
                var_viewer.set(labels[0])

        ttk.Button(viewer_row, text="Tải lại", command=_reload_viewers).pack(side=tk.LEFT)
        _reload_viewers()
        body = ttk.Panedwindow(win, orient=tk.HORIZONTAL)
        body.pack(fill=tk.BOTH, expand=True, padx=10, pady=4)
        left = ttk.Frame(body)
        right = ttk.Frame(body)
        body.add(left, weight=1)
        body.add(right, weight=1)
        ttk.Label(left, text="Tài khoản").pack(anchor="w")
        txt_live_acc = tk.Text(left, height=8, wrap="none", font=("Consolas", 9))
        txt_live_acc.pack(fill=tk.BOTH, expand=True)
        uid_bar = ttk.Frame(right)
        uid_bar.pack(fill=tk.X)
        ttk.Label(uid_bar, text="UID đã lọc").pack(side=tk.LEFT)
        txt_live_uids = tk.Text(right, height=8, wrap="none", font=("Consolas", 9))

        def _filter_uids() -> None:
            from src.utils.account_proxy_mapper import extract_uid_lines

            uids = extract_uid_lines(txt_live_acc.get("1.0", tk.END))
            txt_live_uids.delete("1.0", tk.END)
            if uids:
                txt_live_uids.insert("1.0", "\n".join(uids))
            var_live_status.set(f"Đã lọc {len(uids)} UID. Bấm Copy UID để chép.")

        def _copy_uids() -> None:
            raw = txt_live_uids.get("1.0", tk.END).strip()
            if not raw:
                _filter_uids()
                raw = txt_live_uids.get("1.0", tk.END).strip()
            if not raw:
                messagebox.showwarning("Chưa có UID", "Không thấy dãy số UID trong danh sách.", parent=win)
                return
            win.clipboard_clear()
            win.clipboard_append(raw)
            var_live_status.set(f"Đã copy {len(raw.splitlines())} UID.")

        ttk.Button(uid_bar, text="Lọc UID", command=_filter_uids).pack(side=tk.LEFT, padx=6)
        ttk.Button(uid_bar, text="Copy UID", command=_copy_uids).pack(side=tk.LEFT)
        txt_live_uids.pack(fill=tk.BOTH, expand=True, pady=(4, 0))
        opts = ttk.Frame(win)
        opts.pack(fill=tk.X, padx=10, pady=4)
        ttk.Label(opts, text="Lần lượt từng UID").pack(side=tk.LEFT)
        ttk.Label(opts, text="Lọc").pack(side=tk.LEFT, padx=(8, 2))
        var_live_filter = tk.StringVar(value="Tất cả")
        cb_live_filter = ttk.Combobox(
            opts,
            textvariable=var_live_filter,
            state="readonly",
            width=18,
            values=("Tất cả", "Còn hoạt động", "Không hoạt động", "Lỗi"),
        )
        cb_live_filter.pack(side=tk.LEFT)
        var_live_status = tk.StringVar(value="Sẵn sàng")
        ttk.Label(opts, textvariable=var_live_status).pack(side=tk.LEFT, padx=8)
        cols = ("uid", "result", "detail")
        tree = ttk.Treeview(win, columns=cols, show="headings", height=8)
        tree.heading("uid", text="UID")
        tree.heading("result", text="Kết quả")
        tree.heading("detail", text="Chi tiết")
        tree.column("uid", width=140)
        tree.column("result", width=100)
        tree.column("detail", width=480)
        tree.tag_configure("live", foreground="#047857")
        tree.tag_configure("die", foreground="#b91c1c")
        tree.tag_configure("bad", foreground="#b45309")
        tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=4)
        stop_ev = threading.Event()
        running = {"on": False}
        checked: dict[str, MappedAccount] = {}
        show_browser = {"on": True}
        browser_profile = {"path": ""}

        def _reveal_browser() -> None:
            show_browser["on"] = True
            path = browser_profile["path"]
            if not path:
                var_live_status.set("Sẽ hiện trình duyệt khi bắt đầu check.")
                return
            from src.services.facebook_live_check import set_account_window_visible

            set_account_window_visible({"portable_path": path}, True)
            var_live_status.set("Đã hiện trình duyệt.")

        def _conceal_browser() -> None:
            show_browser["on"] = False
            path = browser_profile["path"]
            if not path:
                var_live_status.set("Sẽ ẩn trình duyệt khi bắt đầu check.")
                return
            from src.services.facebook_live_check import set_account_window_visible

            set_account_window_visible({"portable_path": path}, False)
            var_live_status.set("Đã ẩn trình duyệt.")

        def _live_bucket(label: str) -> str:
            if label == "Live":
                return "Còn hoạt động"
            if label in {"Die", "Checkpoint"}:
                return "Không hoạt động"
            if label in {"Lỗi", "Lỗi proxy"}:
                return "Lỗi"
            return ""

        def _row_visible(label: str) -> bool:
            pick = str(var_live_filter.get() or "Tất cả")
            if pick == "Tất cả":
                return True
            return _live_bucket(label) == pick

        def _row_tag(label: str) -> str:
            bucket = _live_bucket(label)
            if bucket == "Còn hoạt động":
                return "live"
            if bucket == "Không hoạt động":
                return "die"
            if bucket == "Lỗi":
                return "bad"
            return ""

        def _live_summary() -> str:
            live = dead = bad = 0
            for ma in checked.values():
                bucket = _live_bucket(classify_account_live(ma.status, ma.status_detail))
                if bucket == "Còn hoạt động":
                    live += 1
                elif bucket == "Không hoạt động":
                    dead += 1
                elif bucket == "Lỗi":
                    bad += 1
            return f"Còn hoạt động: {live} · Không hoạt động: {dead} · Lỗi: {bad}"

        def _sorted_index(label: str, slot: int, skip_iid: str) -> int:
            rank = live_result_rank(label)
            index = 0
            for child in tree.get_children():
                if child == skip_iid:
                    continue
                other_rank = live_result_rank(tree.set(child, "result"))
                other = checked.get(child)
                other_slot = int(other.grid_slot_index) if other else 0
                if (other_rank, other_slot) > (rank, slot):
                    break
                index += 1
            return index

        def _fill_row(ma: MappedAccount) -> None:
            checked[ma.account_id] = ma
            label = classify_account_live(ma.status, ma.status_detail)
            iid = ma.account_id
            vals = (ma.display_uid(), label, (ma.status_detail or "")[:180])
            tag = _row_tag(label)
            if not _row_visible(label):
                if tree.exists(iid):
                    tree.delete(iid)
                return
            index = _sorted_index(label, int(ma.grid_slot_index or 0), iid)
            if tree.exists(iid):
                tree.item(iid, values=vals, tags=(tag,) if tag else ())
                tree.move(iid, "", index)
            else:
                tree.insert("", index, iid=iid, values=vals, tags=(tag,) if tag else ())

        def _apply_live_filter(_event: object = None) -> None:
            for item in tree.get_children():
                tree.delete(item)
            for ma in checked.values():
                _fill_row(ma)
            if checked and not running["on"]:
                var_live_status.set(f"Xong {len(checked)} — {_live_summary()}")

        cb_live_filter.bind("<<ComboboxSelected>>", _apply_live_filter)

        def _viewer_account_dict(kind: str, viewer_id: str) -> dict[str, Any]:
            """Profile persistent của tài khoản đã chọn. Proxy lấy từ chính tài khoản đó."""
            if kind == "mapped":
                from src.utils.account_proxy_mapper import prepare_mapped_account_for_browser_run

                for bucket in ("mapped_interaction", "mapped_login"):
                    for ma in state.get(bucket) or []:
                        if ma.account_id != viewer_id:
                            continue
                        account = prepare_mapped_account_for_browser_run(ma)
                        account.setdefault("id", viewer_id)
                        return account
                raise ValueError("Không tìm thấy tài khoản đã đăng nhập")
            from src.utils.account_proxy_mapper import prepare_account_dict_for_browser_run
            from src.utils.db_manager import AccountsDatabaseManager

            row = AccountsDatabaseManager().get_by_id(viewer_id)
            if row is None:
                raise ValueError("Không tìm thấy profile đã lưu")
            return prepare_account_dict_for_browser_run(dict(row), require_proxy_live=True)

        def _start() -> None:
            if running["on"]:
                return
            acc_lines = _non_empty_lines(txt_live_acc.get("1.0", tk.END))
            if not acc_lines:
                messagebox.showwarning("Thiếu list", "Dán ít nhất một dòng tài khoản.", parent=win)
                return
            try:
                mapped = map_pasted_accounts_for_live_check(
                    acc_lines,
                    None,
                    browser_type="firefox",
                    account_format=_current_account_format(),
                )
            except AccountProxyMappingError as exc:
                messagebox.showerror("List không hợp lệ", str(exc), parent=win)
                return
            for item in tree.get_children():
                tree.delete(item)
            checked.clear()
            for ma in mapped:
                ma.status = "pending"
                ma.status_detail = "Đang chờ"
                _fill_row(ma)
            chosen = viewer_choices.get(var_viewer.get())
            if chosen is None:
                messagebox.showwarning(
                    "Chưa chọn tài khoản",
                    "Chọn tài khoản đã đăng nhập để mở trình duyệt check.",
                    parent=win,
                )
                return
            from src.utils.account_proxy_mapper import facebook_uid_for_live_check

            by_uid: dict[str, list[MappedAccount]] = {}
            ordered_uids: list[str] = []
            for ma in mapped:
                uid = facebook_uid_for_live_check(ma)
                if not uid:
                    ma.status = "error"
                    ma.status_detail = "Thiếu UID số"
                    _fill_row(ma)
                    continue
                by_uid.setdefault(uid, []).append(ma)
                if uid not in ordered_uids:
                    ordered_uids.append(uid)
            if not ordered_uids:
                messagebox.showwarning("Thiếu UID", "Không lấy được UID số từ danh sách.", parent=win)
                return
            stop_ev.clear()
            running["on"] = True
            var_live_status.set("Đang mở trình duyệt tài khoản đã đăng nhập…")
            done_n = {"n": sum(1 for item in mapped if item.status == "error")}
            total = len(mapped)

            def _paint(ma: MappedAccount, *, finished: bool = False) -> None:
                def _ui() -> None:
                    if not win.winfo_exists():
                        return
                    _fill_row(ma)
                    if finished:
                        var_live_status.set(
                            f"{done_n['n']}/{total} — {classify_account_live(ma.status, ma.status_detail)}"
                        )
                        if done_n["n"] >= total:
                            running["on"] = False
                            var_live_status.set(f"Xong {total} — {_live_summary()}")
                    else:
                        var_live_status.set(f"Đang check {done_n['n']}/{total} — {ma.display_uid()}")

                schedule_on_main_thread(win, _ui)

            def _mark(rows: list[MappedAccount], status: str, detail: str, *, finished: bool) -> None:
                for item in rows:
                    if item.status in {"login_ok", "login_failed", "error", "cancelled"}:
                        continue
                    item.status = status
                    item.status_detail = detail
                    if finished:
                        done_n["n"] += 1
                    _paint(item, finished=finished)

            def _browser() -> None:
                from src.services.facebook_live_check import run_live_check_in_account_browser

                kind, viewer_id = chosen
                try:
                    account = _viewer_account_dict(kind, viewer_id)
                    browser_profile["path"] = str(
                        account.get("portable_path") or account.get("profile_path") or ""
                    )
                except Exception as exc:  # noqa: BLE001
                    _mark(mapped, "proxy_error" if "proxy" in str(exc).lower() else "error", str(exc)[:180], finished=True)
                    return

                def _on_begin(uid: str) -> None:
                    for item in by_uid.get(uid) or []:
                        item.status = "running"
                        item.status_detail = f"Đang xem UID {uid}…"
                        _paint(item)

                def _on_result(uid: str, status: str, detail: str) -> None:
                    rows = by_uid.get(uid) or []
                    for item in rows:
                        item.status = status
                        item.status_detail = detail
                        done_n["n"] += 1
                        _paint(item, finished=True)

                try:
                    code = run_live_check_in_account_browser(
                        account,
                        ordered_uids,
                        on_result=_on_result,
                        on_begin=_on_begin,
                        should_stop=stop_ev.is_set,
                        show_window=lambda: show_browser["on"],
                    )
                except Exception as exc:  # noqa: BLE001
                    code = "error"
                    _mark(
                        [item for rows in by_uid.values() for item in rows],
                        "error",
                        str(exc)[:180],
                        finished=True,
                    )
                    return
                if code == "cancelled":
                    _mark(
                        [item for rows in by_uid.values() for item in rows],
                        "cancelled",
                        "Đã hủy",
                        finished=True,
                    )
                elif code == "session":
                    _mark(
                        [item for rows in by_uid.values() for item in rows],
                        "error",
                        "Tài khoản đã chọn chưa đăng nhập hoặc bị đăng xuất",
                        finished=True,
                    )
                elif code == "checkpoint":
                    _mark(
                        [item for rows in by_uid.values() for item in rows],
                        "error",
                        "Tài khoản dùng để check đang bị checkpoint",
                        finished=True,
                    )
                else:
                    pending = [
                        item
                        for rows in by_uid.values()
                        for item in rows
                        if item.status not in {"login_ok", "login_failed", "error", "cancelled"}
                    ]
                    if pending:
                        _mark(pending, "error", "Chưa đọc được trang hồ sơ", finished=True)
                    else:
                        schedule_on_main_thread(
                            win,
                            lambda: var_live_status.set(f"Xong {total} — {_live_summary()}")
                            if win.winfo_exists()
                            else None,
                        )
                        running["on"] = False

            threading.Thread(target=_browser, name="live-check-browser", daemon=True).start()

        def _stop() -> None:
            stop_ev.set()
            var_live_status.set("Đang dừng…")

        bar = ttk.Frame(win)
        bar.pack(fill=tk.X, padx=10, pady=(0, 10))
        ttk.Button(bar, text="Bắt đầu check", command=_start).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="Dừng", command=_stop).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="Hiện trình duyệt", command=_reveal_browser).pack(side=tk.LEFT, padx=8)
        ttk.Button(bar, text="Ẩn trình duyệt", command=_conceal_browser).pack(side=tk.LEFT, padx=2)

    def _btn(bar: _FlowBar, **kwargs: Any) -> tk.Button:
        """Gắn nút vào hàng chảy — cùng cỡ, cùng khoảng cách."""
        return bar.add(_flat_btn(bar, **kwargs))

    # Dán / file — cùng kiểu với nút thao tác, không trộn nút hệ thống.
    _btn(
        paste_btns,
        text="Dán TK",
        command=lambda: _paste_clipboard_into(txt_acc, label="tài khoản"),
        bg=_C_BTN_SAVE,
        active_bg=_C_BTN_SAVE_H,
    )
    _btn(
        paste_btns,
        text="Dán Proxy",
        command=lambda: _paste_clipboard_into(txt_px, label="proxy"),
        bg=_C_BTN_SAVE,
        active_bg=_C_BTN_SAVE_H,
    )
    _btn(
        paste_btns,
        text="Xóa 2 ô",
        command=_clear_both_text_areas,
        bg="#cbd5e1",
        active_bg="#94a3b8",
        fg="#1e293b",
    )
    _btn(
        paste_btns,
        text="Check Proxy LIVE",
        command=_on_check_proxy_live,
        bg=_C_BTN_PROXY,
        active_bg=_C_BTN_PROXY_H,
    )

    file_btns = _FlowBar(tab_file)
    file_btns.grid(row=2, column=0, columnspan=4, sticky="ew", pady=(8, 0))
    _btn(
        file_btns,
        text="Chọn file TK",
        command=lambda: _pick_file(var_acc, "accounts"),
        bg=_C_BTN_SAVE,
        active_bg=_C_BTN_SAVE_H,
    )
    _btn(
        file_btns,
        text="Chọn file Proxy",
        command=lambda: _pick_file(var_px, "proxies"),
        bg=_C_BTN_SAVE,
        active_bg=_C_BTN_SAVE_H,
    )
    _btn(
        file_btns,
        text="Check Proxy LIVE",
        command=lambda: _on_check_proxy_live(from_file_tab=True),
        bg=_C_BTN_PROXY,
        active_bg=_C_BTN_PROXY_H,
    )

    # Chuẩn bị dữ liệu, rồi đăng nhập, rồi thao tác trên dòng đã chọn.
    _btn(data_btns, text="Ghép vào hàng", command=_on_merge, bg=_C_BTN_MERGE, active_bg=_C_BTN_MERGE_H)
    _btn(data_btns, text="Check Live TK", command=_open_check_live_dialog, bg="#0f766e", active_bg="#115e59")
    _btn(data_btns, text="Lưu nội dung", command=_on_save_inputs, bg=_C_BTN_SAVE, active_bg=_C_BTN_SAVE_H)

    _btn(login_btns, text="Đăng nhập đã chọn", command=_on_login_selected, bg="#059669", active_bg="#047857")
    _btn(login_btns, text="Đăng nhập tất cả", command=_on_login_all, bg="#10b981", active_bg="#059669")
    _btn(
        login_btns,
        text="Mở trình duyệt",
        command=_on_open_browser_login,
        bg=_C_BTN_SECONDARY,
        active_bg=_C_BTN_SECONDARY_H,
    )
    btn_stop_login = _btn(
        login_btns,
        text="Dừng",
        command=_on_stop,
        bg=_C_BTN_DANGER,
        active_bg=_C_BTN_DANGER_H,
        state=tk.DISABLED,
    )

    _btn(run_btns, text="Chạy tất cả", command=_on_run_all, bg="#2563eb", active_bg="#1d4ed8")
    _btn(run_btns, text="Chạy đã chọn", command=_on_run_selected, bg=_C_BTN_SECONDARY, active_bg=_C_BTN_SECONDARY_H)
    _btn(run_btns, text="Chạy 1 dòng", command=_on_run_one, bg="#6366f1", active_bg="#4f46e5")
    btn_stop_interaction = _btn(
        run_btns,
        text="Dừng",
        command=_on_stop,
        bg=_C_BTN_DANGER,
        active_bg=_C_BTN_DANGER_H,
        state=tk.DISABLED,
    )
    _btn(profile_btns, text="Đăng nhập lại", command=_on_relogin_selected, bg="#059669", active_bg="#047857")
    _btn(profile_btns, text="Lưu cookie", command=_on_save_cookie_only, bg="#0891b2", active_bg="#0e7490")
    _btn(profile_btns, text="Mở profile", command=_on_open_profile_browser, bg="#6366f1", active_bg="#4f46e5")
    _btn(profile_btns, text="Lướt nhận thiết bị", command=_on_browse_for_device, bg="#0f766e", active_bg="#115e59")
    _btn(profile_btns, text="Đóng profile", command=_on_close_profile_browser, bg="#64748b", active_bg="#475569")

    btn_stop_global.configure(text="Dừng", command=_on_stop)
    state["btn_stop_all"] = [btn_stop_global, btn_stop_login, btn_stop_interaction]

    def _row_actions(bar: _FlowBar, *, export: bool) -> None:
        """Cùng một hàng phụ trên cả hai tab: copy, đặt lại, proxy, xóa."""
        _btn(
            bar,
            text="Copy đã chọn",
            command=lambda: _copy_selected_accounts(full=False),
            bg="#0369a1",
            active_bg="#075985",
        )
        _btn(bar, text="Đặt lại đã chọn", command=_on_reset_selected, bg="#94a3b8", active_bg="#64748b")
        _btn(bar, text="Đặt lại tất cả", command=_on_reset_all, bg="#cbd5e1", active_bg="#94a3b8", fg="#1e293b")
        _btn(bar, text="Cập nhật proxy", command=_on_reassign_proxy, bg="#f59e0b", active_bg="#d97706")
        if export:
            _btn(
                bar,
                text="Sang tab Tài khoản",
                command=_on_export_to_accounts_registry,
                bg="#0d9488",
                active_bg="#0f766e",
            )
        _btn(bar, text="Xóa đã chọn", command=_on_delete_selected, bg="#f87171", active_bg="#ef4444")

    _row_actions(login_row_btns, export=False)
    _row_actions(interaction_row_btns, export=True)

    legend_fr = tk.Frame(interaction_inner, bg="#f8fafc", pady=2)
    legend_fr.grid(row=3, column=0, sticky="ew")
    tk.Label(legend_fr, text="Màu dòng:", font=("Segoe UI", 8), bg="#f8fafc", fg="#64748b").pack(side=tk.LEFT, padx=(0, 6))
    for label, st_key in (
        ("Chờ", "pending"),
        ("Chạy", "running"),
        ("OK", "login_ok"),
        ("Lỗi", "error"),
        ("Proxy", "proxy_error"),
    ):
        _tag, chip_bg, chip_fg = _TREE_STATUS_TAGS[st_key]
        tk.Label(
            legend_fr,
            text=f" {label} ",
            font=("Segoe UI", 8),
            bg=chip_bg,
            fg=chip_fg,
            padx=4,
        ).pack(side=tk.LEFT, padx=2)

    def _on_grid_settings_changed(*_args: object) -> None:
        _refresh_grid_hint()

    def _sync_threads_from_grid(*_args: object) -> None:
        """Cột × hàng thành số tài khoản chạy cùng lúc. Hàng 0 thì giữ số luồng đang đặt."""
        rows = _grid_rows_value()
        if rows >= 1:
            target = max(1, min(24, _grid_cols_value() * rows))
            try:
                current = int(var_threads.get())
            except (TypeError, ValueError, tk.TclError):
                current = 0
            if current != target:
                var_threads.set(target)
        _refresh_grid_hint()

    try:
        var_threads.trace_add("write", _on_grid_settings_changed)
        var_grid_cols.trace_add("write", _sync_threads_from_grid)
        var_grid_rows.trace_add("write", _sync_threads_from_grid)
    except tk.TclError:
        pass
    def _fit_wrap(label: tk.Label, width: int) -> None:
        """Chữ gợi ý xuống dòng theo bề rộng thật, không tràn ra ngoài khung."""
        if width > 40:
            label.configure(wraplength=max(240, width - 24))

    def _on_login_inner_configure(event: tk.Event) -> None:
        if event.widget is login_inner:
            _fit_wrap(lbl_hint_login, int(event.width))
            _fit_wrap(lbl_paste_help, int(event.width))

    def _on_interaction_inner_configure(event: tk.Event) -> None:
        if event.widget is interaction_inner:
            _fit_wrap(lbl_hint_interaction, int(event.width))

    login_inner.bind("<Configure>", _on_login_inner_configure, add="+")
    interaction_inner.bind("<Configure>", _on_interaction_inner_configure, add="+")

    def _place_sash(paned: ttk.Panedwindow, top: ttk.Frame, bottom: ttk.Frame, *, min_top: int, min_bottom: int) -> None:
        """Đặt thanh kéo dưới khối nhập liệu, để nút và ô văn bản không bị bảng che."""
        try:
            paned.update_idletasks()
            height = int(paned.winfo_height())
            if height < 160:
                return
            paned.pane(top, minsize=min_top)
            paned.pane(bottom, minsize=min_bottom)
            need = int(top.winfo_reqheight()) + 12
            sash = max(min_top, min(need, height - min_bottom))
            paned.sashpos(0, sash)
        except tk.TclError:
            pass

    def _init_login_paned_sash() -> None:
        _place_sash(login_paned, login_scroll_host, login_table_fr, min_top=280, min_bottom=160)

    def _init_interaction_paned_sash() -> None:
        _place_sash(
            interaction_paned,
            interaction_scroll_host,
            interaction_bottom,
            min_top=220,
            min_bottom=180,
        )

    root.after_idle(_refresh_grid_hint)
    root.after_idle(_restore_mapped_session)
    root.after_idle(_init_login_paned_sash)
    root.after_idle(_init_interaction_paned_sash)

    def shutdown_gracefully(timeout_sec: float = 60.0) -> None:
        """
        Đóng pool / cửa sổ profile an toàn trước khi thoát app — giữ lịch sử Firefox mọi TK.
        """
        try:
            _save_settings()
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Human GUI] save settings on exit: {}", exc)
        profile_sessions = state.get("profile_browser_sessions") or {}
        for aid, sess in list(profile_sessions.items()):
            try:
                cmd_q = sess.get("cmd_q")
                if cmd_q is not None:
                    cmd_q.put("close")
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human GUI] đóng profile {} on exit: {}", aid, exc)
        pool = state.get("pool")
        if pool is not None:
            try:
                pool.shutdown_gracefully(timeout=timeout_sec)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[Human GUI] graceful pool shutdown: {}", exc)
            state["pool"] = None

    setattr(root, "_toolfb_human_shutdown", shutdown_gracefully)

    _update_summary()
