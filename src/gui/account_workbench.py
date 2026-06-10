"""
Hộp thoại «Thêm / Sửa tài khoản Facebook» — một cửa sổ, ba luồng: Mới | Thư mục | Cookie.

Giữ tương thích engine: ``portable_path``, ``browser_type`` (chromium/firefox), ``proxy.user``/``pass``.
Thêm tùy chọn: ``import_type``, ``notes``, ``browser_exe_path``, đồng bộ ``profile_path`` với portable_path.
"""

from __future__ import annotations

import copy
import json
import os
import re
import subprocess
import sys
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from loguru import logger

from src.gui.cookie_capture import account_cookie_path_field, cookie_storage_dest, run_fb_cookie_capture_dialog
from src.gui.treeview_shortcuts import install_treeview_shortcuts
from src.services.totp_service import generate_totp_code
from src.utils.browser_exe_discover import find_browser_exe_in_directory as _find_browser_exe_in_directory
from src.utils.db_manager import AccountRecord, AccountsDatabaseManager
from src.utils.paths import project_root
from src.utils.proxy_check import check_proxy, parse_proxy_line, playwright_host_for_scheme


def _parse_schedule_time(value: str) -> None:
    s = str(value).strip()
    if not re.fullmatch(r"\d{1,2}:\d{2}", s):
        raise ValueError("Giờ đăng phải dạng HH:MM (ví dụ 09:00).")
    h, m = s.split(":")
    hi, mi = int(h), int(m)
    if not (0 <= hi <= 23 and 0 <= mi <= 59):
        raise ValueError("Giờ/phút ngoài phạm vi hợp lệ.")


def _start_file_with_os_default_handler(path: Path) -> tuple[bool, str]:
    """Mở / chạy file (vd. ``.exe``) bằng ứng dụng mặc định của hệ thống."""
    p = path.resolve()
    if not p.is_file():
        return False, f"Không phải file hoặc không tồn tại:\n{p}"
    try:
        if sys.platform == "win32":
            os.startfile(str(p))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(p)], check=False)  # noqa: S603
        else:
            subprocess.Popen(["xdg-open", str(p)], check=False)  # noqa: S603
        return True, ""
    except OSError as exc:
        return False, str(exc)


def _open_folder_in_os_file_manager(folder: Path) -> tuple[bool, str]:
    """
    Mở ``folder`` trong trình quản lý file hệ thống (Explorer / Finder / xdg-open).

    Returns:
        (True, "") nếu đã gọi được lệnh mở; (False, thông báo lỗi) nếu thất bại.
    """
    p = folder.resolve()
    if not p.is_dir():
        return False, f"Không phải thư mục hoặc không tồn tại:\n{p}"
    try:
        if sys.platform == "win32":
            os.startfile(str(p))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(p)], check=False)  # noqa: S603
        else:
            subprocess.Popen(["xdg-open", str(p)], check=False)  # noqa: S603
        return True, ""
    except OSError as exc:
        return False, str(exc)


def _coerce_use_proxy(raw: Any) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)
    s = str(raw).strip().lower()
    if s in ("0", "false", "no", "off"):
        return False
    return True


def _normalize_post_status(raw: Any) -> str:
    s = str(raw or "pending").strip().lower()
    if s in ("completed", "active"):
        return "success"
    if s in ("success", "failed", "pending"):
        return s
    return "pending"


_ACCOUNT_MODE_MANUAL = "Thêm Mới"
_ACCOUNT_MODE_FOLDER = "Thêm tài khoản từ thư mục"
_ACCOUNT_MODE_FILE = "Thêm bằng Cookie"

_BROWSER_LABELS = ("Chrome", "Firefox")
_BROWSER_TO_STORAGE = {"Chrome": "chromium", "Firefox": "firefox"}
_STORAGE_TO_LABEL = {"chromium": "Chrome", "firefox": "Firefox", "chrome": "Chrome", "webkit": "Chrome"}


def _browser_label_from_storage(bt: str) -> str:
    return _STORAGE_TO_LABEL.get(str(bt).strip().lower(), "Firefox")


def _browser_storage_from_label(label: str) -> str:
    return _BROWSER_TO_STORAGE.get(label, "firefox")


def _scan_profile_folder(folder: Path) -> tuple[dict[str, Any], str, str]:
    proxy: dict[str, Any] = {"host": "", "port": 0, "user": "", "pass": ""}
    cookie_rel = ""
    notes: list[str] = []
    root = folder.resolve()
    for fname in ("proxy.json", "proxy_config.json"):
        p = root / fname
        if not p.is_file():
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            notes.append(f"{fname}: không đọc được ({exc}).")
            continue
        if isinstance(data, dict):
            if any(k in data for k in ("host", "port", "user", "pass", "proxy")):
                if "proxy" in data and isinstance(data["proxy"], dict):
                    data = data["proxy"]
                proxy["host"] = str(data.get("host", "")).strip()
                try:
                    proxy["port"] = int(data.get("port", 0))
                except (TypeError, ValueError):
                    proxy["port"] = 0
                proxy["user"] = str(data.get("user", "") or data.get("username", "")).strip()
                proxy["pass"] = str(data.get("pass", "") or data.get("password", "")).strip()
                notes.append(f"Đã đọc proxy từ {fname}.")
            break
    for cname in ("cookies.json", "cookie.json", "cookies_playwright.json"):
        p = root / cname
        if p.is_file():
            cookie_rel = account_cookie_path_field(p)
            notes.append(f"Đã tìm cookie: {cname} → {cookie_rel}")
            break
    if not notes:
        notes.append("Không thấy proxy.json / cookies.json quen thuộc.")
    return proxy, cookie_rel, "\n".join(notes)


def _list_immediate_subdirs(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted([p for p in root.iterdir() if p.is_dir()], key=lambda p: p.name.lower())


def _browser_exe_for_profile_folder(sub: Path, common_exe: str) -> str:
    """Ưu tiên .exe bên trong thư mục profile; không có thì dùng ``common_exe``."""
    found = _find_browser_exe_in_directory(sub)
    if found:
        return found
    c = str(common_exe).strip()
    return c if c and Path(c).is_file() else ""


def _fb_cookie_pairs_from_json(raw: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            n = str(item.get("name", "")).strip()
            v = str(item.get("value", "")).strip()
            if n:
                out[n.lower()] = v
    elif isinstance(raw, dict):
        if "cookies" in raw and isinstance(raw["cookies"], list):
            return _fb_cookie_pairs_from_json(raw["cookies"])
        for k, v in raw.items():
            if isinstance(v, str) and k:
                out[str(k).lower()] = v
    return out


def _parse_cookie_entry_text(block: str) -> dict[str, Any]:
    block = block.strip()
    if not block:
        return {"valid": False, "err": "Rỗng", "pairs": {}, "has_c_user": False, "has_xs": False}
    try:
        raw = json.loads(block)
    except json.JSONDecodeError as exc:
        return {"valid": False, "err": f"JSON: {exc}", "pairs": {}, "has_c_user": False, "has_xs": False}
    pairs = _fb_cookie_pairs_from_json(raw)
    if not pairs:
        return {"valid": False, "err": "Không đọc được cặp name/value", "pairs": {}, "has_c_user": False, "has_xs": False}
    has_c = any(k == "c_user" for k in pairs)
    has_x = any(k == "xs" for k in pairs)
    ok = has_c and has_x
    return {
        "valid": ok,
        "err": "" if ok else "Thiếu c_user hoặc xs",
        "pairs": pairs,
        "has_c_user": has_c,
        "has_xs": has_x,
    }


def _split_cookie_blocks(text: str) -> list[str]:
    t = text.strip()
    if not t:
        return []
    parts = re.split(r"\n\s*\n+", t)
    if len(parts) == 1 and t.count("},{") > 0:
        parts = re.split(r"\}\s*\{", t)
        parts = ["{" + p.strip().strip("{}") + "}" if not p.strip().startswith("{") else p for p in parts]
    return [p.strip() for p in parts if p.strip()]


def template_new_account(suggested_id: str = "") -> AccountRecord:
    if (suggested_id or "").strip():
        sid = (suggested_id or "").strip().replace(" ", "_")
    else:
        sid = f"acc_{uuid.uuid4().hex[:10]}"
    sub = "firefox"
    return {  # type: ignore[return-value]
        "id": sid,
        "name": "Tài khoản mới",
        "browser_type": sub,
        "portable_path": f"data/profiles/{sub}/{sid}",
        "cookie_path": f"data/cookies/{sid}.json",
        "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
        "use_proxy": True,
        "import_type": "new",
        "notes": "",
        "browser_exe_path": "",
    }


class AccountFormDialog:
    """
    ``result``: một ``dict`` (một tài khoản) hoặc ``list[dict]`` (import hàng loạt), hoặc ``None``.
    """

    def __init__(
        self,
        parent: tk.Tk | tk.Toplevel,
        manager: AccountsDatabaseManager,
        *,
        title: str,
        initial: AccountRecord | None,
        id_readonly: bool,
    ) -> None:
        self._manager = manager
        self._initial = copy.deepcopy(dict(initial)) if initial else None
        self._result: dict[str, Any] | list[dict[str, Any]] | None = None
        self._id_readonly = id_readonly
        self._folder_preview: list[dict[str, Any]] = []
        self._cookie_preview: list[dict[str, Any]] = []

        self._top = tk.Toplevel(parent)
        self._top.title(title)
        self._top.transient(parent)
        self._top.grab_set()
        self._top.geometry("780x720")
        self._top.minsize(680, 560)

        btnf = ttk.Frame(self._top, padding=6)
        self._btn_save = ttk.Button(btnf, text="Lưu", command=self._on_ok)
        self._btn_save.pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Button(btnf, text="Hủy", command=self._on_cancel).pack(side=tk.RIGHT)
        # Pack thanh nút trước (BOTTOM) để không bị hàng nội dung (weight) đẩy khỏi vùng nhìn thấy.
        btnf.pack(side=tk.BOTTOM, fill=tk.X)

        outer = ttk.Frame(self._top, padding=6)
        outer.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)

        canvas = tk.Canvas(outer, highlightthickness=0)
        scroll = ttk.Scrollbar(outer, orient=tk.VERTICAL, command=canvas.yview)
        self._body = ttk.Frame(canvas, padding=4)

        def _on_body_configure(_event: tk.Event | None = None) -> None:
            bbox = canvas.bbox("all")
            if bbox:
                canvas.configure(scrollregion=bbox)

        self._body.bind("<Configure>", _on_body_configure)

        self._canvas_window_id = canvas.create_window((0, 0), window=self._body, anchor="nw")

        def _on_canvas_configure(event: tk.Event) -> None:
            if event.widget is not canvas:
                return
            cw = int(event.width)
            if cw > 1:
                canvas.itemconfigure(self._canvas_window_id, width=cw)
            _on_body_configure()

        canvas.bind("<Configure>", _on_canvas_configure)
        canvas.configure(yscrollcommand=scroll.set)
        canvas.grid(row=0, column=0, sticky="nsew")
        scroll.grid(row=0, column=1, sticky="ns")
        self._scroll_canvas = canvas

        def _on_canvas_mousewheel(event: tk.Event) -> None:
            if not canvas.bbox("all"):
                return
            if getattr(event, "delta", 0):
                canvas.yview_scroll(int(-event.delta / 120), "units")

        def _on_canvas_mousewheel_linux_up(_event: tk.Event) -> None:
            canvas.yview_scroll(-1, "units")

        def _on_canvas_mousewheel_linux_down(_event: tk.Event) -> None:
            canvas.yview_scroll(1, "units")

        canvas.bind("<MouseWheel>", _on_canvas_mousewheel)
        canvas.bind("<Button-4>", _on_canvas_mousewheel_linux_up)
        canvas.bind("<Button-5>", _on_canvas_mousewheel_linux_down)
        self._body.bind("<MouseWheel>", _on_canvas_mousewheel)
        self._body.bind("<Button-4>", _on_canvas_mousewheel_linux_up)
        self._body.bind("<Button-5>", _on_canvas_mousewheel_linux_down)

        init = self._initial or {}
        self._build_common(self._body, init)
        self._frm_modes = ttk.LabelFrame(self._body, text="Chọn cách thêm tài khoản", padding=6)
        self._frm_modes.grid(row=1, column=0, sticky="ew", pady=6)
        self._var_mode = tk.StringVar(value=_ACCOUNT_MODE_MANUAL)
        if id_readonly:
            self._frm_modes.grid_remove()
        else:
            for i, (txt, val) in enumerate(
                (
                    (_ACCOUNT_MODE_MANUAL, _ACCOUNT_MODE_MANUAL),
                    (_ACCOUNT_MODE_FOLDER, _ACCOUNT_MODE_FOLDER),
                    (_ACCOUNT_MODE_FILE, _ACCOUNT_MODE_FILE),
                )
            ):
                ttk.Radiobutton(
                    self._frm_modes,
                    text=txt,
                    variable=self._var_mode,
                    value=val,
                    command=self._on_mode_change,
                ).grid(row=i, column=0, sticky="w", pady=1)
            it = str(init.get("import_type", "new") or "").strip().lower()
            if it == "folder":
                self._var_mode.set(_ACCOUNT_MODE_FOLDER)
            elif it in ("cookie", "cookies"):
                self._var_mode.set(_ACCOUNT_MODE_FILE)

        self._dyn_host = ttk.Frame(self._body)
        # Không dùng weight=1 trên hàng này: trong Canvas, frame con sẽ bị kéo cao theo
        # viewport → scrollregion rộng, khoảng trống xám, nút Lưu/Hủy tưởng như «mất».
        self._dyn_host.grid(row=2, column=0, sticky="ew", pady=4)
        self._dyn_host.columnconfigure(0, weight=1)
        self._build_panel_new(self._dyn_host, init)
        self._build_panel_folder(self._dyn_host, init)
        self._build_panel_cookie(self._dyn_host, init)

        self._top.protocol("WM_DELETE_WINDOW", self._on_cancel)
        if id_readonly:
            self._show_panel("edit")
        else:
            self._on_mode_change()
        self._top.after_idle(self._account_dialog_scroll_top)
        self._top.wait_window()

    @property
    def result(self) -> dict[str, Any] | list[dict[str, Any]] | None:
        return self._result

    def _account_dialog_scroll_top(self) -> None:
        """Sau lần layout đầu, đưa viewport Canvas về đầu nội dung (tránh khoảng trống lệch)."""
        c = self._scroll_canvas
        try:
            c.update_idletasks()
            bbox = c.bbox("all")
            if bbox:
                c.configure(scrollregion=bbox)
            c.yview_moveto(0)
        except tk.TclError:
            pass

    def _set_status(self, msg: str) -> None:
        """``install_treeview_shortcuts`` gọi khi copy/chọn nhanh; hộp thoại không có thanh trạng thái."""
        _ = msg

    def _build_common(self, parent: ttk.Frame, init: dict[str, Any]) -> None:
        parent.columnconfigure(0, weight=1)
        lf = ttk.LabelFrame(parent, text="Cấu hình chung", padding=8)
        lf.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        lf.columnconfigure(1, weight=1)
        r = 0
        ttk.Label(lf, text="Mã tài khoản (id) *").grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=2)
        id_row = ttk.Frame(lf)
        id_row.columnconfigure(0, weight=1)
        self._e_id = ttk.Entry(id_row, width=40)
        self._e_id.insert(0, str(init.get("id", "")))
        if self._id_readonly:
            self._e_id.configure(state="readonly")
        else:
            self._e_id.bind("<KeyRelease>", lambda _e: self._refresh_profile_preview())
            ttk.Button(id_row, text="Tự sinh", command=self._on_auto_id).grid(row=0, column=1, sticky="e", padx=(4, 0))
        self._e_id.grid(row=0, column=0, sticky="ew")
        id_row.grid(row=r, column=1, sticky="ew", pady=2)
        r += 1
        ttk.Label(lf, text="Tên tài khoản *").grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=2)
        self._e_name = ttk.Entry(lf, width=48)
        self._e_name.insert(0, str(init.get("name", "")))
        self._e_name.grid(row=r, column=1, sticky="ew", pady=2)
        r += 1
        ttk.Label(lf, text="Trình duyệt *").grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=2)
        self._cb_browser_label = ttk.Combobox(
            lf, values=_BROWSER_LABELS, state="readonly", width=46
        )
        self._cb_browser_label.set(_browser_label_from_storage(str(init.get("browser_type", "firefox"))))
        self._cb_browser_label.grid(row=r, column=1, sticky="w", pady=2)
        self._cb_browser_label.bind("<<ComboboxSelected>>", lambda _e: self._refresh_profile_preview())
        r += 1
        self._var_use_proxy = tk.BooleanVar(value=_coerce_use_proxy(init.get("use_proxy", True)))
        self._chk_use_proxy = ttk.Checkbutton(
            lf,
            text="Dùng proxy khi mở trình duyệt",
            variable=self._var_use_proxy,
            command=self._on_use_proxy_toggle,
        )
        self._chk_use_proxy.grid(row=r, column=0, columnspan=2, sticky="w", pady=4)
        r += 1
        px = init.get("proxy") or {}
        if not isinstance(px, dict):
            px = {}
        self._frm_px = ttk.Frame(lf)
        self._frm_px.grid(row=r, column=0, columnspan=2, sticky="ew", pady=2)
        self._frm_px.columnconfigure(1, weight=1)
        pr = 0
        ttk.Label(self._frm_px, text="Host").grid(row=pr, column=0, sticky="w", padx=(0, 6))
        self._e_ph = ttk.Entry(self._frm_px, width=40)
        self._e_ph.insert(0, str(px.get("host", "") or px.get("username", "")))
        self._e_ph.grid(row=pr, column=1, sticky="ew")
        pr += 1
        ttk.Label(self._frm_px, text="Port").grid(row=pr, column=0, sticky="w", padx=(0, 6))
        self._e_pp = ttk.Entry(self._frm_px, width=10)
        self._e_pp.insert(0, str(px.get("port", "")))
        self._e_pp.grid(row=pr, column=1, sticky="w")
        pr += 1
        ttk.Label(self._frm_px, text="User").grid(row=pr, column=0, sticky="w", padx=(0, 6))
        self._e_pu = ttk.Entry(self._frm_px, width=40)
        self._e_pu.insert(0, str(px.get("user", "") or px.get("username", "")))
        self._e_pu.grid(row=pr, column=1, sticky="ew")
        pr += 1
        ttk.Label(self._frm_px, text="Password").grid(row=pr, column=0, sticky="w", padx=(0, 6))
        self._e_ppw = ttk.Entry(self._frm_px, width=40, show="*")
        self._e_ppw.insert(0, str(px.get("pass", "") or px.get("password", "")))
        self._e_ppw.grid(row=pr, column=1, sticky="ew")
        pr += 1
        ttk.Label(
            self._frm_px,
            text="Dán: host:port:user:pass (nhiều proxy VN là SOCKS5, tool tự nhận khi kiểm tra)",
            foreground="gray",
            font=("Segoe UI", 8),
        ).grid(row=pr, column=0, columnspan=2, sticky="w", pady=(2, 0))
        r += 1
        px_btn_row = ttk.Frame(lf)
        px_btn_row.grid(row=r, column=0, columnspan=2, sticky="w", pady=4)
        ttk.Button(px_btn_row, text="Dán chuỗi proxy", command=self._on_paste_proxy_line).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(px_btn_row, text="Kiểm tra proxy", command=self._on_check_proxy_dialog).pack(side=tk.LEFT)
        r += 1
        ttk.Label(lf, text="Ghi chú").grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=2)
        self._e_notes = tk.Text(lf, height=2, width=50, wrap="word", font=("Segoe UI", 9))
        self._e_notes.insert("1.0", str(init.get("notes", "")))
        self._e_notes.grid(row=r, column=1, sticky="ew", pady=2)
        r += 1
        ttk.Label(
            lf,
            text="SOCKS5 + user/pass: Host = socks5://IP; trình duyệt tự dùng relay HTTP local (Chromium không nhận auth SOCKS trực tiếp).",
            foreground="gray",
            font=("Segoe UI", 8),
        ).grid(row=r, column=0, columnspan=2, sticky="w")
        r += 1

        hint = ttk.Label(
            lf,
            text="Lịch đăng (HH:MM) và trạng thái đăng bài được cấu hình ở tab «Page / Group».",
            foreground="gray",
            font=("Segoe UI", 8),
        )
        hint.grid(row=r, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        r += 1

        lf_login = ttk.LabelFrame(lf, text="Facebook đăng nhập / TOTP 2FA (tùy chọn)", padding=6)
        lf_login.grid(row=r, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        lf_login.columnconfigure(1, weight=1)
        lr = 0
        init_uid, init_email = self._split_uid_email_from_init(init)
        ttk.Label(lf_login, text="UID Facebook").grid(row=lr, column=0, sticky="nw", padx=(0, 6), pady=2)
        self._e_facebook_uid = ttk.Entry(lf_login, width=48)
        self._e_facebook_uid.insert(0, init_uid)
        self._e_facebook_uid.grid(row=lr, column=1, sticky="ew", pady=2)
        lr += 1
        ttk.Label(lf_login, text="Email Facebook").grid(row=lr, column=0, sticky="nw", padx=(0, 6), pady=2)
        self._e_email = ttk.Entry(lf_login, width=48)
        self._e_email.insert(0, init_email)
        self._e_email.grid(row=lr, column=1, sticky="ew", pady=2)
        lr += 1
        ttk.Label(
            lf_login,
            text="Đăng nhập: UID + mật khẩu hoặc Email + mật khẩu (chỉ cần một trong hai).",
            foreground="gray",
            font=("Segoe UI", 8),
        ).grid(row=lr, column=0, columnspan=2, sticky="w")
        lr += 1
        ttk.Label(lf_login, text="Email khôi phục").grid(row=lr, column=0, sticky="nw", padx=(0, 6), pady=2)
        self._e_recovery_email = ttk.Entry(lf_login, width=48)
        self._e_recovery_email.insert(0, str(init.get("recovery_email", "")))
        self._e_recovery_email.grid(row=lr, column=1, sticky="ew", pady=2)
        lr += 1
        ttk.Label(
            lf_login,
            text="Dùng khi Meta hỏi email dự phòng / vượt checkpoint (lưu trong vault, không ghi accounts.json).",
            foreground="gray",
            font=("Segoe UI", 8),
        ).grid(row=lr, column=0, columnspan=2, sticky="w")
        lr += 1
        ttk.Label(lf_login, text="Mật khẩu").grid(row=lr, column=0, sticky="nw", padx=(0, 6), pady=2)
        pw_row = ttk.Frame(lf_login)
        pw_row.grid(row=lr, column=1, sticky="ew", pady=2)
        pw_row.columnconfigure(0, weight=1)
        self._e_password = ttk.Entry(pw_row, width=40, show="•")
        self._e_password.grid(row=0, column=0, sticky="ew")
        self._var_show_password = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            pw_row,
            text="Hiện",
            variable=self._var_show_password,
            command=self._toggle_password_visibility,
        ).grid(row=0, column=1, padx=(4, 0))
        lr += 1
        self._var_totp_enabled = tk.BooleanVar(value=bool(init.get("totp_enabled")))
        ttk.Checkbutton(
            lf_login,
            text="Bật TOTP (Google/Microsoft Authenticator)",
            variable=self._var_totp_enabled,
        ).grid(row=lr, column=0, columnspan=2, sticky="w", pady=2)
        lr += 1
        ttk.Label(lf_login, text="TOTP Secret (Base32)").grid(row=lr, column=0, sticky="nw", padx=(0, 6), pady=2)
        totp_row = ttk.Frame(lf_login)
        totp_row.grid(row=lr, column=1, sticky="ew", pady=2)
        totp_row.columnconfigure(0, weight=1)
        self._e_totp_secret = ttk.Entry(totp_row, width=40, show="•")
        self._e_totp_secret.grid(row=0, column=0, sticky="ew")
        self._var_show_totp = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            totp_row,
            text="Hiện",
            variable=self._var_show_totp,
            command=self._toggle_totp_visibility,
        ).grid(row=0, column=1, padx=(4, 0))
        ttk.Button(totp_row, text="Lấy mã", width=8, command=self._on_generate_totp_code).grid(
            row=0, column=2, padx=(4, 0)
        )
        lr += 1
        code_row = ttk.Frame(lf_login)
        code_row.grid(row=lr, column=1, sticky="ew", pady=(0, 2))
        ttk.Label(code_row, text="Mã 2FA:").grid(row=0, column=0, sticky="w", padx=(0, 6))
        self._e_totp_code = ttk.Entry(code_row, width=12, state="readonly", font=("Segoe UI", 11, "bold"))
        self._e_totp_code.grid(row=0, column=1, sticky="w")
        self._btn_copy_totp = ttk.Button(
            code_row,
            text="Sao chép mã",
            width=11,
            command=self._on_copy_totp_code,
            state=tk.DISABLED,
        )
        self._btn_copy_totp.grid(row=0, column=2, padx=(6, 0))
        ttk.Label(
            code_row,
            text="(dùng secret ở trên, ~30s đổi mã)",
            foreground="gray",
            font=("Segoe UI", 8),
        ).grid(row=0, column=3, sticky="w", padx=(8, 0))
        lr += 1
        self._lbl_vault_status = ttk.Label(
            lf_login,
            text="",
            foreground="gray",
            font=("Segoe UI", 8),
        )
        self._lbl_vault_status.grid(row=lr, column=0, columnspan=2, sticky="w")
        lr += 1
        ttk.Label(
            lf_login,
            text="Mật khẩu/secret/email khôi phục lưu trong config/account_credentials.json (không ghi vào accounts.json).",
            foreground="gray",
            font=("Segoe UI", 8),
        ).grid(row=lr, column=0, columnspan=2, sticky="w")
        lr += 1
        btn_row = ttk.Frame(lf_login)
        btn_row.grid(row=lr, column=0, columnspan=2, sticky="ew", pady=(4, 0))
        ttk.Button(btn_row, text="Lưu đăng nhập", command=self._on_save_login_credentials).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(btn_row, text="Tự đăng nhập", command=self._on_auto_login).pack(side=tk.LEFT, padx=(0, 8))
        self._var_test_login_fresh = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            btn_row,
            text="Đăng nhập lại từ đầu (bỏ phiên profile)",
            variable=self._var_test_login_fresh,
        ).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(btn_row, text="Test Login", command=self._on_test_login_recovery).pack(side=tk.LEFT)
        lr += 1
        ttk.Label(
            lf_login,
            text="«Lưu đăng nhập»: vault (mật khẩu/TOTP/email khôi phục). «Lưu» ở cuối cửa sổ: cả tài khoản + vault.",
            foreground="gray",
            font=("Segoe UI", 8),
        ).grid(row=lr, column=0, columnspan=2, sticky="w", pady=(4, 0))
        self._load_vault_credentials_into_form(init)

        self._on_use_proxy_toggle()

    def _refresh_profile_preview(self) -> None:
        if not hasattr(self, "_lbl_profile_preview"):
            return
        aid = self._e_id.get().strip() or "(id)"
        sub = _browser_storage_from_label(self._cb_browser_label.get())
        prev = f"data/profiles/{sub}/{aid}"
        self._lbl_profile_preview.configure(text=prev)

    def _on_use_proxy_toggle(self) -> None:
        on = bool(self._var_use_proxy.get())
        state = tk.NORMAL if on else tk.DISABLED
        for w in (self._e_ph, self._e_pp, self._e_pu, self._e_ppw):
            try:
                w.configure(state=state)
            except tk.TclError:
                pass

    def _proxy_form_host_port(self) -> tuple[str, int]:
        host = self._e_ph.get().strip()
        try:
            port = int(str(self._e_pp.get()).strip() or "0")
        except ValueError:
            port = 0
        return host, port

    def _ensure_proxy_ready_for_login(self, *, dialog_title: str) -> bool:
        """Khi bật proxy: bắt buộc host/port và kiểm tra LIVE trước đăng nhập."""
        if not bool(self._var_use_proxy.get()):
            return True
        host, port = self._proxy_form_host_port()
        if not host or port <= 0:
            messagebox.showerror(
                dialog_title,
                "Đã bật «Dùng proxy» — nhập host và port hợp lệ trước khi đăng nhập.",
                parent=self._top,
            )
            return False
        ok, msg, scheme = check_proxy(
            host,
            port,
            user=self._e_pu.get().strip(),
            password=self._e_ppw.get().strip(),
        )
        if not ok:
            messagebox.showerror(
                dialog_title,
                f"Proxy chưa kết nối được (cần LIVE trước khi đăng nhập):\n{msg}",
                parent=self._top,
            )
            return False
        if scheme == "socks5":
            self._apply_proxy_scheme_to_form(scheme)
        logger.info("[{}] Proxy LIVE ({}) — IP: {}", dialog_title, scheme, msg)
        return True

    def _apply_proxy_dict_to_form(self, px: dict[str, Any]) -> None:
        self._e_ph.delete(0, tk.END)
        self._e_ph.insert(0, str(px.get("host", "")))
        self._e_pp.delete(0, tk.END)
        self._e_pp.insert(0, str(px.get("port", "")))
        self._e_pu.delete(0, tk.END)
        self._e_pu.insert(0, str(px.get("user", "")))
        self._e_ppw.delete(0, tk.END)
        self._e_ppw.insert(0, str(px.get("pass", "")))

    def _apply_proxy_scheme_to_form(self, scheme: str) -> None:
        if scheme != "socks5":
            return
        host, port = self._proxy_form_host_port()
        if not host or port <= 0:
            return
        fixed = playwright_host_for_scheme(host, "socks5")
        if str(self._e_ph.get()).strip() != fixed:
            self._e_ph.delete(0, tk.END)
            self._e_ph.insert(0, fixed)

    def _on_paste_proxy_line(self) -> None:
        try:
            raw = self._top.clipboard_get().strip()
        except tk.TclError:
            raw = ""
        if not raw:
            messagebox.showinfo(
                "Proxy",
                "Copy chuỗi proxy (vd. 203.175.96.175:25308:user:pass) rồi bấm lại.",
                parent=self._top,
            )
            return
        try:
            px = parse_proxy_line(raw)
        except ValueError as exc:
            messagebox.showerror("Proxy", str(exc), parent=self._top)
            return
        self._var_use_proxy.set(True)
        self._on_use_proxy_toggle()
        self._apply_proxy_dict_to_form(px)
        messagebox.showinfo(
            "Proxy",
            f"Đã điền:\nHost={px['host']}\nPort={px['port']}\nUser={px.get('user') or '(trống)'}\n\n"
            "Bấm «Kiểm tra proxy» — nếu là SOCKS5, Host sẽ tự đổi thành socks5://…",
            parent=self._top,
        )

    def _on_check_proxy_dialog(self) -> None:
        if not bool(self._var_use_proxy.get()):
            messagebox.showinfo("Proxy", "Đang tắt «Dùng proxy» — không kiểm tra.", parent=self._top)
            return
        host, port = self._proxy_form_host_port()
        if not host or port <= 0:
            messagebox.showerror("Proxy", "Nhập host và port proxy.", parent=self._top)
            return
        ok, msg, scheme = check_proxy(
            host, port, user=self._e_pu.get().strip(), password=self._e_ppw.get().strip()
        )
        if ok:
            if scheme == "socks5":
                self._apply_proxy_scheme_to_form(scheme)
                host_show = self._e_ph.get().strip()
                messagebox.showinfo(
                    "Proxy",
                    f"LIVE (SOCKS5) — IP: {msg}\n\nHost đã đặt: {host_show}\n(Lưu tài khoản để giữ cấu hình.)",
                    parent=self._top,
                )
            else:
                messagebox.showinfo("Proxy", f"LIVE (HTTP) — IP: {msg}", parent=self._top)
        else:
            messagebox.showerror("Proxy", msg, parent=self._top)

    def _build_panel_new(self, parent: ttk.Frame, init: dict[str, Any]) -> None:
        self._frm_new = ttk.LabelFrame(parent, text=_ACCOUNT_MODE_MANUAL, padding=8)
        self._frm_new.columnconfigure(1, weight=1)
        ttk.Label(self._frm_new, text="cookie_path *").grid(row=0, column=0, sticky="nw", padx=(0, 6))
        self._e_cookie = ttk.Entry(self._frm_new, width=52)
        self._e_cookie.insert(0, str(init.get("cookie_path", "")))
        self._e_cookie.grid(row=0, column=1, sticky="ew")
        self._e_portable_edit: ttk.Entry | None = None
        if self._id_readonly:
            ttk.Label(self._frm_new, text="portable_path *").grid(row=1, column=0, sticky="nw", padx=(0, 6), pady=4)
            self._e_portable_edit = ttk.Entry(self._frm_new, width=52)
            self._e_portable_edit.insert(
                0, str(init.get("portable_path") or init.get("profile_path", ""))
            )
            self._e_portable_edit.grid(row=1, column=1, sticky="ew", pady=4)
            ttk.Label(self._frm_new, text="browser_exe_path (tùy chọn)").grid(
                row=2, column=0, sticky="nw", padx=(0, 6), pady=2
            )
            self._e_browser_exe = ttk.Entry(self._frm_new, width=52)
            self._e_browser_exe.insert(0, str(init.get("browser_exe_path", "")))
            self._e_browser_exe.grid(row=2, column=1, sticky="ew", pady=2)
            pr = 3
        else:
            ttk.Label(self._frm_new, text="Preview portable_path").grid(
                row=1, column=0, sticky="nw", padx=(0, 6), pady=4
            )
            self._lbl_profile_preview = ttk.Label(self._frm_new, text="", foreground="gray")
            self._lbl_profile_preview.grid(row=1, column=1, sticky="w", pady=4)
            self._refresh_profile_preview()
            ttk.Label(self._frm_new, text="browser_exe_path (tùy chọn)").grid(
                row=2, column=0, sticky="nw", padx=(0, 6), pady=2
            )
            self._e_browser_exe = ttk.Entry(self._frm_new, width=52)
            self._e_browser_exe.insert(0, str(init.get("browser_exe_path", "")))
            self._e_browser_exe.grid(row=2, column=1, sticky="ew", pady=2)
            pr = 3
        ttk.Button(self._frm_new, text="Tạo thư mục profile (mkdir)", command=self._on_mkdir_profile).grid(
            row=pr, column=1, sticky="w", pady=4
        )
        pr += 1
        ttk.Button(
            self._frm_new,
            text="Mở trình duyệt đăng nhập Facebook → lưu cookie_path",
            command=self._on_login_capture_cookie,
        ).grid(row=pr, column=0, columnspan=2, sticky="w", pady=(10, 0))

    def _on_mkdir_profile(self) -> None:
        aid = self._e_id.get().strip()
        if not aid:
            messagebox.showwarning("Thiếu id", "Nhập mã tài khoản (id) trước.", parent=self._top)
            return
        sub = _browser_storage_from_label(self._cb_browser_label.get())
        rel = Path(f"data/profiles/{sub}/{aid}")
        full = project_root() / rel
        full.mkdir(parents=True, exist_ok=True)
        messagebox.showinfo("Profile", f"Đã tạo (hoặc đã tồn tại):\n{full}", parent=self._top)

    def _on_auto_id(self) -> None:
        if self._id_readonly:
            return
        existing = {str(a.get("id", "")).strip() for a in self._manager.load_all() if str(a.get("id", "")).strip()}
        cand = ""
        for _ in range(500):
            c = f"acc_{uuid.uuid4().hex[:10]}"
            if c not in existing:
                cand = c
                break
        if not cand:
            cand = f"acc_{uuid.uuid4().hex}"
        self._e_id.configure(state=tk.NORMAL)
        self._e_id.delete(0, tk.END)
        self._e_id.insert(0, cand)
        self._refresh_profile_preview()
        self._e_cookie.delete(0, tk.END)
        self._e_cookie.insert(0, f"data/cookies/{cand}.json")

    def _portable_path_for_capture(self, aid: str) -> str:
        """Đường dẫn profile dùng khi mở trình duyệt: form sửa (tuyệt đối/tương đối) hoặc mặc định theo id."""
        if self._e_portable_edit is not None:
            raw = self._e_portable_edit.get().strip()
            if raw:
                return str(Path(raw))
        return self._build_default_portable(aid)

    def _on_login_capture_cookie(self) -> None:
        if not self._id_readonly and self._var_mode.get() != _ACCOUNT_MODE_MANUAL:
            messagebox.showinfo("Chế độ", "Chỉ dùng khi chọn «Thêm Mới».", parent=self._top)
            return
        try:
            proxy, use_px = self._proxy_block_common()
        except ValueError as exc:
            messagebox.showerror("Proxy", str(exc), parent=self._top)
            return
        aid = self._e_id.get().strip()
        if not aid:
            messagebox.showwarning(
                "Thiếu id",
                "Cần mã tài khoản (id)." if self._id_readonly else "Nhập hoặc bấm «Tự sinh» mã tài khoản trước.",
                parent=self._top,
            )
            return
        name = self._e_name.get().strip() or aid
        br = _browser_storage_from_label(self._cb_browser_label.get())
        portable = self._portable_path_for_capture(aid)
        ck_rel = self._e_cookie.get().strip() or f"data/cookies/{aid}.json"
        self._e_cookie.delete(0, tk.END)
        self._e_cookie.insert(0, ck_rel)

        acc_preview: dict[str, Any] = {
            "id": aid,
            "name": name,
            "browser_type": br,
            "portable_path": portable,
            "profile_path": portable,
            "proxy": proxy,
            "use_proxy": use_px,
            "cookie_path": ck_rel,
        }
        exe = self._e_browser_exe.get().strip() if hasattr(self, "_e_browser_exe") else ""
        if exe:
            acc_preview["browser_exe_path"] = exe
        root = project_root()
        pdir = Path(portable)
        if not pdir.is_absolute():
            pdir = root / portable
        pdir.mkdir(parents=True, exist_ok=True)

        def after_save() -> None:
            self._e_cookie.delete(0, tk.END)
            self._e_cookie.insert(0, ck_rel)

        self._start_fb_cookie_capture(
            acc_preview,
            ck_rel,
            log_label=aid,
            tip_extra="(File ghi đúng cookie_path trong form.)",
            on_after_save=after_save,
        )

    def _start_fb_cookie_capture(
        self,
        acc_preview: dict[str, Any],
        ck_rel: str,
        *,
        log_label: str,
        tip_extra: str = "(File ghi đúng cookie_path trong form.)",
        on_after_save: Callable[[], None] | None = None,
    ) -> None:
        run_fb_cookie_capture_dialog(
            self._top,
            self._manager,
            acc_preview,
            ck_rel,
            log_label=log_label,
            tip_extra=tip_extra,
            on_after_save=on_after_save,
        )

    def _folder_suggested_cookie_rel(self, row: dict[str, Any]) -> str:
        stem = re.sub(r"[^\w\-]+", "_", str(row.get("folder_name", "")))[:48] or f"row_{row.get('stt', 0)}"
        return f"data/cookies/{stem}.json"

    def _on_folder_capture_cookie_selected(self) -> None:
        """Mở profile portable của dòng chọn → đăng nhập FB → ghi Playwright ``storage_state`` (cookie JSON)."""
        row = self._get_selected_folder_preview_row()
        if not row:
            messagebox.showinfo("Cookie", "Chọn một dòng trong bảng (profile).", parent=self._top)
            return
        portable = str(row.get("portable_path", "")).strip()
        if not portable:
            messagebox.showwarning("Cookie", "Thiếu portable_path.", parent=self._top)
            return
        exe_one = str(row.get("browser_exe_path", "")).strip()
        if exe_one and not Path(exe_one).is_file():
            exe_one = ""
        if not exe_one:
            found = _find_browser_exe_in_directory(Path(portable))
            if found:
                exe_one = found
                row["browser_exe_path"] = exe_one
                self._fill_folder_tree()
                self._update_save_state()
        if not exe_one or not Path(exe_one).is_file():
            messagebox.showwarning(
                "Cookie",
                "Không có .exe hợp lệ — đã quét thư mục profile nhưng không thấy firefox.exe/chrome.exe… — "
                "dùng «Tự nhận .exe» hoặc «Exe cho dòng chọn…».",
                parent=self._top,
            )
            return
        ck_existing = str(row.get("cookie_path", "")).strip()
        ck_rel = ck_existing or self._folder_suggested_cookie_rel(row)
        row_uses = bool(row.get("use_proxy", True))
        if row_uses:
            p = self._read_proxy_dict_raw()
            proxy = {**p}
            use_px = True
        else:
            proxy = {"host": "", "port": 0, "user": "", "pass": ""}
            use_px = False
        aid = re.sub(r"[^\w\-]+", "_", str(row.get("folder_name", "")))[:48] or f"row_{row.get('stt', 0)}"
        acc_preview: dict[str, Any] = {
            "id": aid,
            "name": str(row.get("folder_name", aid)),
            "browser_type": str(row.get("browser_type", "firefox")),
            "portable_path": portable,
            "profile_path": portable,
            "proxy": proxy,
            "use_proxy": use_px,
            "cookie_path": ck_rel,
            "browser_exe_path": exe_one,
        }

        def after_save() -> None:
            dest = cookie_storage_dest(ck_rel, project_root())
            row["cookie_path"] = account_cookie_path_field(dest)
            row["has_cookie"] = True
            exe_ok = bool(str(row.get("browser_exe_path", "")).strip()) and Path(
                str(row.get("browser_exe_path", "")).strip()
            ).is_file()
            row["exe_ok"] = exe_ok
            row["valid"] = exe_ok
            self._fill_folder_tree()
            self._sync_folder_selected_profile_path()
            self._update_save_state()

        self._start_fb_cookie_capture(
            acc_preview,
            ck_rel,
            log_label=aid,
            tip_extra=f"Cookie ghi vào:\n{ck_rel}\n(sau «Lưu» tài khoản, id có thể được hậu tố nếu trùng.)",
            on_after_save=after_save,
        )

    def _build_panel_folder(self, parent: ttk.Frame, init: dict[str, Any]) -> None:
        self._frm_folder = ttk.LabelFrame(parent, text=_ACCOUNT_MODE_FOLDER, padding=8)
        self._frm_folder.columnconfigure(1, weight=1)
        ttk.Label(self._frm_folder, text="Thư mục chứa các profile (thư mục cha) *").grid(
            row=0, column=0, sticky="nw", padx=(0, 6)
        )
        fr_root = ttk.Frame(self._frm_folder)
        fr_root.columnconfigure(0, weight=1)
        self._e_folder_root = ttk.Entry(fr_root, width=42)
        self._e_folder_root.grid(row=0, column=0, sticky="ew")
        ttk.Button(fr_root, text="Chọn…", width=8, command=self._on_pick_folder_profile_root).grid(
            row=0, column=1, padx=(6, 0)
        )
        fr_root.grid(row=0, column=1, sticky="ew")
        bf = ttk.Frame(self._frm_folder)
        bf.grid(row=1, column=0, columnspan=2, sticky="w", pady=4)
        ttk.Button(bf, text="Quét thư mục", command=self._on_scan_folder).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(bf, text="Tự nhận .exe (tất cả dòng)", command=self._on_autodetect_exe_all_rows).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(bf, text="Bỏ dòng đang chọn", command=self._on_folder_remove_selected).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(bf, text="Exe cho dòng chọn…", command=self._on_folder_set_exe_for_selected).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(bf, text="Chạy .exe đã gán", command=self._on_folder_run_assigned_exe).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(bf, text="Proxy dòng chọn (có⇄không)", command=self._on_folder_toggle_proxy_selected).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(bf, text="Lấy cookie (dòng chọn)", command=self._on_folder_capture_cookie_selected).pack(side=tk.LEFT)
        cols = ("stt", "name", "browser", "profile", "exe", "proxy", "ok")
        self._tree_folder = ttk.Treeview(
            self._frm_folder, columns=cols, show="headings", height=8, selectmode="browse"
        )
        for c, h, w, stretch in (
            ("stt", "STT", 40, False),
            ("name", "Tên (thư mục)", 100, True),
            ("browser", "Trình duyệt", 68, False),
            ("profile", "Profile path", 160, True),
            ("exe", "Exe", 140, True),
            ("proxy", "Proxy", 60, False),
            ("ok", "Hợp lệ", 72, False),
        ):
            self._tree_folder.heading(c, text=h)
            self._tree_folder.column(c, width=w, minwidth=36, stretch=stretch)
        self._tree_folder.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=4)
        self._tree_folder.bind("<Double-1>", self._on_folder_tree_double)
        self._tree_folder.bind("<<TreeviewSelect>>", self._on_folder_tree_select)
        install_treeview_shortcuts(self._tree_folder, owner=self._top, info_callback=self._set_status)
        self._frm_folder.rowconfigure(2, weight=1)

        ttk.Label(self._frm_folder, text="Profile đang chọn — đường dẫn đầy đủ").grid(
            row=3, column=0, columnspan=2, sticky="w", pady=(6, 2)
        )
        sel_fr = ttk.Frame(self._frm_folder)
        sel_fr.grid(row=4, column=0, columnspan=2, sticky="ew")
        sel_fr.columnconfigure(0, weight=1)
        self._folder_sel_profile_var = tk.StringVar(value="— Chưa chọn profile —")
        self._e_folder_sel_profile = ttk.Entry(
            sel_fr, textvariable=self._folder_sel_profile_var, state="readonly"
        )
        self._e_folder_sel_profile.grid(row=0, column=0, sticky="ew")
        ttk.Button(sel_fr, text="Sao chép", width=9, command=self._on_folder_copy_selected_path).grid(
            row=0, column=1, padx=(6, 0)
        )
        ttk.Button(sel_fr, text="Mở thư mục", width=11, command=self._on_folder_open_selected_profile).grid(
            row=0, column=2, padx=(6, 0)
        )

        ttk.Label(
            self._frm_folder,
            text="1) Quét thư mục → 2) «Tự nhận .exe (tất cả dòng)» (tìm trong từng profile và thư mục cha) hoặc double-click cột Exe / «Exe cho dòng chọn» "
            "(hộp chọn file mở sẵn trong thư mục profile đang chọn, hoặc thư mục cha đã nhập). «Chạy .exe đã gán» chỉ để thử .exe — trước khi «Lấy cookie» nên đóng Chrome đó để tránh hai tiến trình cùng một profile. "
            "Lấy cookie từ đúng profile đó: chọn dòng → «Lấy cookie (dòng chọn)» — mở Playwright vào cùng portable_path và cùng .exe đã gán, đăng nhập Facebook rồi «Lưu cookie vào file» (JSON Playwright / storage_state). "
            "Hoặc đặt sẵn cookies.json / cookie.json / cookies_playwright.json trong thư mục profile để quét tự nhận. "
            "«Lưu» bật khi đã có .exe hợp lệ; thiếu file cookie trong profile vẫn có thể lưu (tạo file rỗng trong dự án). "
            "Cột Proxy: double-click hoặc «Proxy dòng chọn» (host/port ở form khi «có»).",
            foreground="gray",
            font=("Segoe UI", 8),
        ).grid(row=5, column=0, columnspan=2, sticky="ew", pady=(8, 0))

    def _build_panel_cookie(self, parent: ttk.Frame, init: dict[str, Any]) -> None:
        self._frm_cookie = ttk.LabelFrame(parent, text=_ACCOUNT_MODE_FILE, padding=8)
        self._frm_cookie.columnconfigure(0, weight=1)
        ttk.Label(self._frm_cookie, text="Prefix id (vd. acc_)").grid(row=0, column=0, sticky="w")
        self._e_cookie_prefix = ttk.Entry(self._frm_cookie, width=20)
        self._e_cookie_prefix.insert(0, "acc_")
        self._e_cookie_prefix.grid(row=1, column=0, sticky="w", pady=2)
        ttk.Label(self._frm_cookie, text="Thư mục gốc cookie (tương đối dự án)").grid(row=2, column=0, sticky="w", pady=(6, 0))
        self._e_cookie_root = ttk.Entry(self._frm_cookie, width=50)
        self._e_cookie_root.insert(0, "data/cookies")
        self._e_cookie_root.grid(row=3, column=0, sticky="ew")
        ttk.Label(self._frm_cookie, text="Danh sách cookie (mỗi block JSON Playwright / c_user+xs trên một đoạn)").grid(
            row=4, column=0, sticky="w", pady=(8, 0)
        )
        self._txt_cookies = tk.Text(self._frm_cookie, height=8, width=70, wrap="word", font=("Consolas", 9))
        self._txt_cookies.grid(row=5, column=0, sticky="nsew", pady=4)
        bf = ttk.Frame(self._frm_cookie)
        bf.grid(row=6, column=0, sticky="w")
        ttk.Button(bf, text="Chọn file .txt / .json", command=self._on_pick_cookie_file).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(bf, text="Parse cookie", command=self._on_parse_cookies).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(bf, text="Bỏ dòng đang chọn", command=self._on_cookie_remove_selected).pack(side=tk.LEFT)
        cols2 = ("stt", "name", "valid", "c_user", "xs", "path", "err")
        self._tree_cookie = ttk.Treeview(self._frm_cookie, columns=cols2, show="headings", height=7, selectmode="extended")
        for c, h, w, stretch in (
            ("stt", "STT", 36, False),
            ("name", "Tên gợi ý", 96, True),
            ("valid", "Hợp lệ", 52, False),
            ("c_user", "c_user", 44, False),
            ("xs", "xs", 32, False),
            ("path", "cookie_path", 160, True),
            ("err", "Ghi chú", 120, True),
        ):
            self._tree_cookie.heading(c, text=h)
            self._tree_cookie.column(c, width=w, minwidth=32, stretch=stretch)
        self._tree_cookie.grid(row=7, column=0, sticky="nsew", pady=4)
        install_treeview_shortcuts(self._tree_cookie, owner=self._top, info_callback=self._set_status)
        self._lbl_cookie_stats = ttk.Label(self._frm_cookie, text="Chưa parse.", foreground="gray")
        self._lbl_cookie_stats.grid(row=8, column=0, sticky="w")
        self._frm_cookie.rowconfigure(5, weight=1)
        self._frm_cookie.rowconfigure(7, weight=2)

    def _show_panel(self, which: str) -> None:
        self._frm_new.grid_remove()
        self._frm_folder.grid_remove()
        self._frm_cookie.grid_remove()
        if which == "new":
            self._frm_new.grid(row=0, column=0, sticky="ew")
        elif which == "folder":
            self._frm_folder.grid(row=0, column=0, sticky="ew")
            self._top.after_idle(self._sync_folder_selected_profile_path)
        elif which == "cookie":
            self._frm_cookie.grid(row=0, column=0, sticky="ew")
        elif which == "edit":
            self._frm_new.grid(row=0, column=0, sticky="ew")

    def _on_mode_change(self) -> None:
        if self._id_readonly:
            return
        m = self._var_mode.get()
        if m == _ACCOUNT_MODE_MANUAL:
            self._show_panel("new")
        elif m == _ACCOUNT_MODE_FOLDER:
            self._show_panel("folder")
        else:
            self._show_panel("cookie")
        self._update_save_state()

    def _update_save_state(self) -> None:
        if self._id_readonly:
            self._btn_save.configure(state=tk.NORMAL)
            return
        m = self._var_mode.get()
        if m == _ACCOUNT_MODE_FOLDER:
            ok = any(row.get("valid") for row in self._folder_preview)
            self._btn_save.configure(state=tk.NORMAL if ok else tk.DISABLED)
        elif m == _ACCOUNT_MODE_FILE:
            ok = any(row.get("valid") for row in self._cookie_preview)
            self._btn_save.configure(state=tk.NORMAL if ok else tk.DISABLED)
        else:
            self._btn_save.configure(state=tk.NORMAL)

    def _read_proxy_dict_raw(self) -> dict[str, Any]:
        try:
            port = int(str(self._e_pp.get()).strip() or "0")
        except ValueError:
            port = 0
        return {
            "host": self._e_ph.get().strip(),
            "port": port,
            "user": self._e_pu.get().strip(),
            "pass": self._e_ppw.get().strip(),
        }

    def _proxy_block_common(self) -> tuple[dict[str, Any], bool]:
        p = self._read_proxy_dict_raw()
        use_px = bool(self._var_use_proxy.get())
        if use_px and (not p["host"] or p["port"] <= 0):
            raise ValueError("Đã bật proxy — cần host và port hợp lệ.")
        return (p, use_px)

    def _collect_notes(self) -> str:
        return self._e_notes.get("1.0", "end").strip()

    def _on_pick_folder_profile_root(self) -> None:
        path = filedialog.askdirectory(parent=self._top, title="Thư mục chứa các profile (thư mục cha)")
        if not path:
            return
        self._e_folder_root.delete(0, tk.END)
        self._e_folder_root.insert(0, path)

    def _on_autodetect_exe_all_rows(self) -> None:
        """Sau khi «Quét thư mục»: tự gán .exe cho mọi dòng (trong profile + fallback thư mục cha)."""
        if not self._folder_preview:
            messagebox.showwarning("Quét thư mục", "Bấm «Quét thư mục» trước, rồi mới tự nhận .exe.", parent=self._top)
            return
        root_s = self._e_folder_root.get().strip()
        common = ""
        if root_s:
            rdir = Path(root_s)
            if rdir.is_dir():
                common = _find_browser_exe_in_directory(rdir) or ""
        n_ok = 0
        for r in self._folder_preview:
            sub = Path(str(r.get("portable_path", "")))
            new_exe = ""
            if sub.is_dir():
                new_exe = _browser_exe_for_profile_folder(sub, common)
            if not new_exe or not Path(new_exe).is_file():
                new_exe = common if common and Path(common).is_file() else ""
            r["browser_exe_path"] = new_exe
            exe_ok = bool(new_exe) and Path(new_exe).is_file()
            r["exe_ok"] = exe_ok
            has_ck = bool(str(r.get("cookie_path", "")).strip())
            r["has_cookie"] = has_ck
            r["valid"] = exe_ok
            if exe_ok:
                n_ok += 1
        self._fill_folder_tree()
        self._update_save_state()
        messagebox.showinfo(
            "Tự nhận .exe",
            f"Đã xử lý {len(self._folder_preview)} dòng — {n_ok} dòng có .exe hợp lệ.\n"
            "Dòng còn thiếu: dùng «Exe cho dòng chọn…» hoặc double-click cột Exe.",
            parent=self._top,
        )

    def _folder_tree_column_at_event_x(self, event_x: int) -> str | None:
        """Tên cột (vd. ``proxy``, ``exe``) tại tọa độ x của Treeview (``show='headings'`` → ``#1``…)."""
        cid = self._tree_folder.identify_column(event_x)
        if not cid.startswith("#"):
            return None
        try:
            xi = int(cid[1:])
        except ValueError:
            return None
        col_tuple = self._tree_folder.cget("columns")
        if isinstance(col_tuple, str):
            parts = col_tuple.split()
        else:
            parts = list(col_tuple)
        if xi < 1 or xi > len(parts):
            return None
        return str(parts[xi - 1])

    def _on_folder_tree_double(self, event: tk.Event) -> None:
        col = self._folder_tree_column_at_event_x(event.x)
        if col == "proxy":
            self._on_folder_toggle_proxy_selected()
            return
        self._on_folder_set_exe_for_selected()

    def _on_folder_toggle_proxy_selected(self) -> None:
        """Đảo ``use_proxy`` của dòng đang chọn (có / không); lưu batch dùng host/port form khi «có»)."""
        row = self._get_selected_folder_preview_row()
        if not row:
            messagebox.showinfo("Proxy", "Chọn một dòng trong bảng (profile).", parent=self._top)
            return
        row["use_proxy"] = not bool(row.get("use_proxy", True))
        self._fill_folder_tree()
        self._sync_folder_selected_profile_path()
        self._update_save_state()

    def _folder_exe_dialog_initialdir(self) -> str | None:
        """Thư mục mở sẵn khi chọn .exe: ưu tiên ``portable_path`` dòng đang chọn, sau đó thư mục cha đã nhập."""
        row = self._get_selected_folder_preview_row()
        if row:
            pp = str(row.get("portable_path", "")).strip()
            if pp:
                p = Path(pp)
                if p.is_dir():
                    return str(p.resolve())
        root_s = self._e_folder_root.get().strip()
        if root_s:
            r = Path(root_s)
            if r.is_dir():
                return str(r.resolve())
        return None

    def _on_folder_run_assigned_exe(self) -> None:
        """Chạy file ``browser_exe_path`` đã gán cho dòng đang chọn (kiểm tra nhanh Chrome portable…)."""
        row = self._get_selected_folder_preview_row()
        if not row:
            messagebox.showinfo(".exe", "Chọn một dòng trong bảng (profile).", parent=self._top)
            return
        exe_s = str(row.get("browser_exe_path", "")).strip()
        if not exe_s:
            messagebox.showwarning(".exe", "Dòng này chưa có .exe — dùng «Tự nhận .exe» hoặc «Exe cho dòng chọn…».", parent=self._top)
            return
        p = Path(exe_s)
        ok, err = _start_file_with_os_default_handler(p)
        if not ok:
            messagebox.showerror(".exe", err, parent=self._top)

    def _on_folder_set_exe_for_selected(self) -> None:
        sel = self._tree_folder.selection()
        if not sel:
            messagebox.showinfo("Exe", "Chọn một dòng trong bảng (profile).", parent=self._top)
            return
        stt = int(sel[0])
        init_dir = self._folder_exe_dialog_initialdir()
        fd_kw: dict[str, Any] = {
            "parent": self._top,
            "title": f"Chọn .exe cho profile (STT {stt})",
            "filetypes": [("Executable", "*.exe"), ("Tất cả", "*.*")],
        }
        if init_dir:
            fd_kw["initialdir"] = init_dir
        path = filedialog.askopenfilename(**fd_kw)
        if not path:
            return
        resolved = str(Path(path).resolve())
        for r in self._folder_preview:
            if r["stt"] == stt:
                r["browser_exe_path"] = resolved
                exe_ok = bool(resolved) and Path(resolved).is_file()
                r["exe_ok"] = exe_ok
                has_ck = bool(str(r.get("cookie_path", "")).strip())
                r["has_cookie"] = has_ck
                r["valid"] = exe_ok
                break
        self._fill_folder_tree()
        self._sync_folder_selected_profile_path()
        self._update_save_state()

    def _on_scan_folder(self) -> None:
        root_s = self._e_folder_root.get().strip()
        if not root_s:
            messagebox.showwarning("Thư mục", "Nhập hoặc chọn thư mục chứa profile.", parent=self._top)
            return
        root = Path(root_s)
        if not root.is_dir():
            messagebox.showerror("Thư mục", "Thư mục không tồn tại.", parent=self._top)
            return
        br_label = self._cb_browser_label.get()
        br = _browser_storage_from_label(br_label)
        use_px = bool(self._var_use_proxy.get())
        px_s = "có" if use_px else "không"
        self._folder_preview = []
        for i, sub in enumerate(_list_immediate_subdirs(root), start=1):
            px, ck, _msg = _scan_profile_folder(sub)
            portable = str(sub.resolve())
            ck_final = ck or ""
            chosen_exe = ""
            exe_ok = False
            valid = exe_ok
            self._folder_preview.append(
                {
                    "stt": i,
                    "folder_name": sub.name,
                    "browser": br_label,
                    "browser_type": br,
                    "portable_path": portable,
                    "profile_path": portable,
                    "browser_exe_path": chosen_exe,
                    "cookie_path": ck_final,
                    "proxy": px,
                    "use_proxy": use_px,
                    "valid": valid,
                    "exe_ok": exe_ok,
                    "has_cookie": bool(ck_final),
                }
            )
        self._fill_folder_tree()
        if self._folder_preview:
            first_iid = str(self._folder_preview[0]["stt"])
            self._tree_folder.focus(first_iid)
            self._tree_folder.see(first_iid)
        self._update_save_state()
        n = len(self._folder_preview)
        messagebox.showinfo(
            "Quét xong",
            f"Tìm {n} thư mục con (proxy: {px_s}).\n\n"
            "Bước tiếp: «Tự nhận .exe (tất cả dòng)» hoặc gán .exe từng dòng (double-click cột Exe / «Exe cho dòng chọn»).\n"
            "Không bắt buộc có cookies.json trong profile — thiếu thì khi «Lưu» sẽ tạo file cookie rỗng ([]) theo id tài khoản.",
            parent=self._top,
        )

    def _get_selected_folder_preview_row(self) -> dict[str, Any] | None:
        """Bản ghi ``_folder_preview`` tương ứng dòng đang chọn trong Treeview (theo ``stt`` = iid)."""
        sel = self._tree_folder.selection()
        if not sel:
            return None
        iid = sel[0]
        if iid not in self._tree_folder.get_children():
            return None
        try:
            stt = int(iid)
        except ValueError:
            return None
        for r in self._folder_preview:
            if r["stt"] == stt:
                return r
        return None

    def _sync_folder_selected_profile_path(self) -> None:
        """Hiển thị đúng ``portable_path`` của dòng đang chọn trong bảng quét thư mục."""
        if not hasattr(self, "_folder_sel_profile_var"):
            return
        r = self._get_selected_folder_preview_row()
        if not r:
            self._folder_sel_profile_var.set("— Chưa chọn profile —")
            return
        self._folder_sel_profile_var.set(str(r.get("portable_path", "")).strip())

    def _on_folder_tree_select(self, _event: tk.Event | None = None) -> None:
        self._sync_folder_selected_profile_path()

    def _on_folder_copy_selected_path(self) -> None:
        t = self._folder_sel_profile_var.get().strip()
        if not t or t.startswith("—"):
            messagebox.showinfo("Sao chép", "Chọn một dòng trong bảng trước.", parent=self._top)
            return
        self._top.clipboard_clear()
        self._top.clipboard_append(t)
        self._top.update()

    def _on_folder_open_selected_profile(self) -> None:
        """Mở Explorer / file manager tại đúng thư mục ``portable_path`` của profile đang chọn."""
        row = self._get_selected_folder_preview_row()
        if not row:
            messagebox.showinfo("Mở thư mục", "Chọn một dòng trong bảng trước.", parent=self._top)
            return
        raw = str(row.get("portable_path", "")).strip()
        if not raw:
            messagebox.showwarning("Mở thư mục", "Không có đường dẫn profile.", parent=self._top)
            return
        ok, err = _open_folder_in_os_file_manager(Path(raw))
        if not ok:
            messagebox.showerror("Mở thư mục", err, parent=self._top)

    def _fill_folder_tree(self) -> None:
        prev_sel = self._tree_folder.selection()
        prev_iid = prev_sel[0] if prev_sel else None
        for x in self._tree_folder.get_children():
            self._tree_folder.delete(x)
        for row in self._folder_preview:
            tags = ("ok",) if row["valid"] else ("bad",)
            has_ck = row.get("has_cookie")
            if has_ck is None:
                has_ck = bool(str(row.get("cookie_path", "")).strip())
            exe_ok = row.get("exe_ok")
            if exe_ok is None:
                exe_p = str(row.get("browser_exe_path", "")).strip()
                exe_ok = bool(exe_p) and Path(exe_p).is_file()
            if not exe_ok:
                st_msg = "Thiếu .exe"
            elif not has_ck:
                st_msg = "Chưa có cookie"
            else:
                st_msg = "OK"
            exe_disp = str(row.get("browser_exe_path") or "")
            if len(exe_disp) > 60:
                exe_disp = exe_disp[:57] + "…"
            row_px = bool(row.get("use_proxy", True))
            self._tree_folder.insert(
                "",
                tk.END,
                iid=str(row["stt"]),
                values=(
                    row["stt"],
                    row["folder_name"],
                    row["browser"],
                    row["portable_path"][: 80] + ("…" if len(row["portable_path"]) > 80 else ""),
                    exe_disp,
                    "có" if row_px else "không",
                    st_msg,
                ),
                tags=tags,
            )
        self._tree_folder.tag_configure("ok", background="#e8f8ec")
        self._tree_folder.tag_configure("bad", background="#fde8e8")
        children = self._tree_folder.get_children()
        if prev_iid and prev_iid in children:
            self._tree_folder.selection_set(prev_iid)
        elif children:
            self._tree_folder.selection_set(children[0])
        else:
            cur = self._tree_folder.selection()
            if cur:
                self._tree_folder.selection_remove(*cur)
        self._sync_folder_selected_profile_path()

    def _on_folder_remove_selected(self) -> None:
        sel = self._tree_folder.selection()
        if not sel:
            return
        stt = int(sel[0])
        self._folder_preview = [r for r in self._folder_preview if r["stt"] != stt]
        for i, r in enumerate(self._folder_preview, start=1):
            r["stt"] = i
        self._fill_folder_tree()
        self._update_save_state()

    def _on_pick_cookie_file(self) -> None:
        path = filedialog.askopenfilename(
            parent=self._top,
            title="Chọn file cookie",
            filetypes=[("Text / JSON", "*.txt;*.json"), ("Tất cả", "*.*")],
        )
        if path:
            self._txt_cookies.delete("1.0", tk.END)
            self._txt_cookies.insert("1.0", Path(path).read_text(encoding="utf-8", errors="replace"))

    def _on_parse_cookies(self) -> None:
        text = self._txt_cookies.get("1.0", "end")
        blocks = _split_cookie_blocks(text)
        prefix = self._e_cookie_prefix.get().strip() or "acc_"
        root_ck = self._e_cookie_root.get().strip().rstrip("/\\") or "data/cookies"
        br_label = self._cb_browser_label.get()
        br = _browser_storage_from_label(br_label)
        existing = {str(a.get("id", "")) for a in self._manager.load_all()}
        self._cookie_preview = []
        n_ok = n_bad = 0
        seq = 1
        for block in blocks:
            info = _parse_cookie_entry_text(block)
            while True:
                name_guess = f"{prefix}{seq:03d}"
                seq += 1
                if name_guess not in existing:
                    existing.add(name_guess)
                    break
            cookie_rel = f"{root_ck}/{name_guess}.json"
            row = {
                "stt": len(self._cookie_preview) + 1,
                "id": name_guess,
                "name": name_guess,
                "browser": br_label,
                "browser_type": br,
                "raw_block": block,
                "pairs": info.get("pairs", {}),
                "valid": info["valid"],
                "has_c_user": info.get("has_c_user", False),
                "has_xs": info.get("has_xs", False),
                "cookie_path": cookie_rel,
                "err": info.get("err", ""),
            }
            self._cookie_preview.append(row)
            if info["valid"]:
                n_ok += 1
            else:
                n_bad += 1
        self._fill_cookie_tree()
        self._lbl_cookie_stats.configure(
            text=f"Tổng: {len(self._cookie_preview)} | Hợp lệ: {n_ok} | Lỗi: {n_bad}",
            foreground="gray",
        )
        self._update_save_state()

    def _fill_cookie_tree(self) -> None:
        for x in self._tree_cookie.get_children():
            self._tree_cookie.delete(x)
        for row in self._cookie_preview:
            tags = ("ok",) if row["valid"] else ("bad",)
            self._tree_cookie.insert(
                "",
                tk.END,
                iid=row["id"],
                values=(
                    row["stt"],
                    row["name"],
                    "Có" if row["valid"] else "Không",
                    "Có" if row["has_c_user"] else "Không",
                    "Có" if row["has_xs"] else "Không",
                    row["cookie_path"],
                    row.get("err", ""),
                ),
                tags=tags,
            )
        self._tree_cookie.tag_configure("ok", background="#e8f8ec")
        self._tree_cookie.tag_configure("bad", background="#fde8e8")

    def _on_cookie_remove_selected(self) -> None:
        sel = self._tree_cookie.selection()
        if not sel:
            return
        rid = sel[0]
        self._cookie_preview = [r for r in self._cookie_preview if r["id"] != rid]
        for i, r in enumerate(self._cookie_preview, start=1):
            r["stt"] = i
        self._fill_cookie_tree()
        n_ok = sum(1 for r in self._cookie_preview if r["valid"])
        self._lbl_cookie_stats.configure(
            text=f"Tổng: {len(self._cookie_preview)} | Hợp lệ: {n_ok} | Lỗi: {len(self._cookie_preview) - n_ok}",
            foreground="gray",
        )
        self._update_save_state()

    def _build_account_dict_core(
        self,
        *,
        aid: str,
        name: str,
        portable: str,
        cookie: str,
        browser_type: str,
        proxy: dict[str, Any],
        use_px: bool,
        import_type: str,
        browser_exe_path: str = "",
    ) -> dict[str, Any]:
        rec: dict[str, Any] = {
            "id": aid,
            "name": name,
            "browser_type": browser_type,
            "portable_path": portable,
            "profile_path": portable,
            "cookie_path": cookie,
            "proxy": proxy,
            "use_proxy": use_px,
            "import_type": import_type,
            "notes": self._collect_notes(),
            "browser_exe_path": browser_exe_path.strip(),
            "facebook_uid": self._form_facebook_uid(),
            "email": self._form_email(),
            "totp_enabled": self._form_totp_enabled(),
            "password_ref": "",
            "totp_secret_ref": "",
            "session_status": str((self._initial or {}).get("session_status") or "active").strip() or "active",
        }
        if aid:
            from src.utils.account_credentials import default_password_ref, default_totp_secret_ref

            rec["password_ref"] = default_password_ref(aid)
            rec["totp_secret_ref"] = default_totp_secret_ref(aid)
        if self._initial and self._initial.get("last_post_at"):
            rec["last_post_at"] = self._initial["last_post_at"]
        if self._initial:
            for k in ("topic", "content_style", "post_image_path"):
                v = self._initial.get(k)
                if v is not None and str(v).strip() and k not in rec:
                    rec[k] = v
        self._manager.validate_account(rec)
        return rec

    def _toggle_password_visibility(self) -> None:
        if not hasattr(self, "_e_password"):
            return
        self._e_password.configure(show="" if self._var_show_password.get() else "•")

    def _toggle_totp_visibility(self) -> None:
        if not hasattr(self, "_e_totp_secret"):
            return
        self._e_totp_secret.configure(show="" if self._var_show_totp.get() else "•")

    def _set_totp_code_display(self, code: str) -> None:
        if not hasattr(self, "_e_totp_code"):
            return
        self._e_totp_code.configure(state=tk.NORMAL)
        self._e_totp_code.delete(0, tk.END)
        if code:
            self._e_totp_code.insert(0, code)
        self._e_totp_code.configure(state="readonly")
        if hasattr(self, "_btn_copy_totp"):
            self._btn_copy_totp.configure(state=tk.NORMAL if code else tk.DISABLED)

    def _on_generate_totp_code(self) -> None:
        secret = self._e_totp_secret.get().strip() if hasattr(self, "_e_totp_secret") else ""
        if not secret:
            messagebox.showwarning(
                "Mã 2FA",
                "Nhập TOTP Secret (Base32) trước — hoặc bật TOTP và bấm «Lưu» để nạp từ vault.",
                parent=self._top,
            )
            self._set_totp_code_display("")
            return
        code = generate_totp_code(secret)
        if not code:
            messagebox.showerror(
                "Mã 2FA",
                "Không sinh được mã — kiểm tra secret Base32 (chữ A–Z, số 2–7) hoặc cài pyotp trong .venv.",
                parent=self._top,
            )
            self._set_totp_code_display("")
            return
        self._set_totp_code_display(code)

    def _on_copy_totp_code(self) -> None:
        code = ""
        if hasattr(self, "_e_totp_code"):
            code = self._e_totp_code.get().strip()
        if not code:
            messagebox.showinfo("Sao chép", "Chưa có mã — bấm «Lấy mã» trước.", parent=self._top)
            return
        try:
            self._top.clipboard_clear()
            self._top.clipboard_append(code)
        except tk.TclError as exc:
            messagebox.showerror("Sao chép", f"Không copy được:\n{exc}", parent=self._top)
            return
        messagebox.showinfo("Sao chép", f"Đã copy mã 2FA: {code}", parent=self._top)

    def _load_vault_credentials_into_form(self, init: dict[str, Any]) -> None:
        aid = str(init.get("id", "")).strip()
        if not aid or not hasattr(self, "_e_password"):
            return
        from src.utils.account_credentials import (
            get_account_password,
            get_account_recovery_email,
            get_account_totp_secret,
        )

        pwd_ref = str(init.get("password_ref") or "").strip() or None
        totp_ref = str(init.get("totp_secret_ref") or "").strip() or None
        pw = get_account_password(aid, pwd_ref)
        totp = get_account_totp_secret(aid, totp_ref) if bool(init.get("totp_enabled")) else ""
        recovery = get_account_recovery_email(aid, pwd_ref)
        if not recovery:
            recovery = str(init.get("recovery_email") or "").strip()
        if pw:
            self._e_password.delete(0, tk.END)
            self._e_password.insert(0, pw)
        if totp:
            self._e_totp_secret.delete(0, tk.END)
            self._e_totp_secret.insert(0, totp)
        if recovery and hasattr(self, "_e_recovery_email"):
            self._e_recovery_email.delete(0, tk.END)
            self._e_recovery_email.insert(0, recovery)
        self._refresh_vault_status_label(
            has_password=bool(pw),
            has_totp=bool(totp),
            has_recovery=bool(recovery),
        )

    def _refresh_vault_status_label(
        self, *, has_password: bool, has_totp: bool, has_recovery: bool = False
    ) -> None:
        if not hasattr(self, "_lbl_vault_status"):
            return
        parts: list[str] = []
        if has_password:
            parts.append("có mật khẩu")
        if has_totp:
            parts.append("có TOTP secret")
        if has_recovery:
            parts.append("có email khôi phục")
        if parts:
            self._lbl_vault_status.configure(
                text=f"Vault hiện tại: {', '.join(parts)} — bấm «Hiện» để kiểm tra.",
                foreground="#1a7f37",
            )
        else:
            self._lbl_vault_status.configure(
                text="Vault: chưa có mật khẩu/secret/email khôi phục — nhập rồi bấm «Lưu».",
                foreground="gray",
            )

    @staticmethod
    def _looks_like_facebook_uid(value: str) -> bool:
        s = str(value or "").strip()
        return bool(s) and s.isdigit() and len(s) >= 8

    def _split_uid_email_from_init(self, init: dict[str, Any]) -> tuple[str, str]:
        uid = str(init.get("facebook_uid", "")).strip()
        email = str(init.get("email", "")).strip()
        if not uid and self._looks_like_facebook_uid(email):
            return email, ""
        return uid, email

    def _form_facebook_uid(self) -> str:
        if hasattr(self, "_e_facebook_uid"):
            return self._e_facebook_uid.get().strip()
        uid, _ = self._split_uid_email_from_init(self._initial or {})
        return uid

    def _form_email(self) -> str:
        if hasattr(self, "_e_email"):
            return self._e_email.get().strip()
        _, email = self._split_uid_email_from_init(self._initial or {})
        return email

    def _form_has_login_identity(self) -> bool:
        return bool(self._form_facebook_uid() or self._form_email())

    def _form_recovery_email(self) -> str:
        if hasattr(self, "_e_recovery_email"):
            return self._e_recovery_email.get().strip()
        return str((self._initial or {}).get("recovery_email", "")).strip()

    def _form_totp_enabled(self) -> bool:
        if hasattr(self, "_var_totp_enabled"):
            return bool(self._var_totp_enabled.get())
        return bool((self._initial or {}).get("totp_enabled"))

    def _persist_credentials_for_account(self, aid: str) -> None:
        if not aid or not hasattr(self, "_e_password"):
            return
        from src.utils.account_credentials import (
            get_account_password,
            get_account_recovery_email,
            get_account_totp_secret,
            set_account_credentials,
        )

        pw = self._e_password.get().strip()
        totp = self._e_totp_secret.get().strip() if hasattr(self, "_e_totp_secret") else ""
        recovery = self._form_recovery_email()
        pwd_ref = str((self._initial or {}).get("password_ref") or "").strip() or None
        totp_ref = str((self._initial or {}).get("totp_secret_ref") or "").strip() or None
        if not pw:
            pw = get_account_password(aid, pwd_ref)
        if self._form_totp_enabled() and not totp:
            totp = get_account_totp_secret(aid, totp_ref)
        if not recovery:
            recovery = get_account_recovery_email(aid, pwd_ref)
        if pw:
            set_account_credentials(aid, password=pw)
        if self._form_totp_enabled() and totp:
            set_account_credentials(aid, totp_secret=totp)
        elif not self._form_totp_enabled():
            set_account_credentials(aid, clear_totp=True)
        if recovery:
            set_account_credentials(aid, recovery_email=recovery)
        else:
            set_account_credentials(aid, clear_recovery_email=True)
        self._refresh_vault_status_label(
            has_password=bool(pw),
            has_totp=bool(self._form_totp_enabled() and totp),
            has_recovery=bool(recovery),
        )

    def _preview_account_for_login_test(self) -> dict[str, Any]:
        aid = self._e_id.get().strip()
        portable = ""
        if self._e_portable_edit is not None:
            portable = self._e_portable_edit.get().strip()
        elif hasattr(self, "_lbl_profile_preview"):
            portable = self._build_default_portable(aid) if aid else ""
        cookie = self._e_cookie.get().strip() if hasattr(self, "_e_cookie") else ""
        if not portable and aid:
            portable = self._build_default_portable(aid)
        from src.utils.account_credentials import default_password_ref, default_totp_secret_ref

        self._persist_credentials_for_account(aid)
        return {
            "id": aid,
            "name": self._e_name.get().strip(),
            "browser_type": _browser_storage_from_label(self._cb_browser_label.get()),
            "portable_path": portable,
            "profile_path": portable,
            "cookie_path": cookie,
            "facebook_uid": self._form_facebook_uid(),
            "email": self._form_email(),
            "totp_enabled": self._form_totp_enabled(),
            "password_ref": default_password_ref(aid) if aid else "",
            "totp_secret_ref": default_totp_secret_ref(aid) if aid else "",
            "use_proxy": bool(self._var_use_proxy.get()),
            "proxy": {
                "host": self._e_ph.get().strip(),
                "port": int(self._e_pp.get().strip() or "0"),
                "user": self._e_pu.get().strip(),
                "pass": self._e_ppw.get().strip(),
            },
        }

    def _validate_login_credentials_form(self, dialog_title: str) -> bool:
        aid = self._e_id.get().strip()
        if not aid:
            messagebox.showwarning(dialog_title, "Nhập mã tài khoản (id) trước.", parent=self._top)
            return False
        if not self._form_has_login_identity():
            messagebox.showwarning(
                dialog_title,
                "Nhập UID Facebook hoặc Email Facebook (một trong hai).",
                parent=self._top,
            )
            return False
        if not self._e_password.get().strip():
            messagebox.showwarning(
                dialog_title,
                "Nhập mật khẩu (bấm «Lưu đăng nhập» hoặc «Lưu» để ghi vault).",
                parent=self._top,
            )
            return False
        if self._form_totp_enabled() and not self._e_totp_secret.get().strip():
            messagebox.showwarning(dialog_title, "Bật TOTP nhưng chưa nhập secret.", parent=self._top)
            return False
        return True

    def _on_save_login_credentials(self) -> None:
        if not self._validate_login_credentials_form("Lưu đăng nhập"):
            return
        aid = self._e_id.get().strip()
        try:
            self._persist_credentials_for_account(aid)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror(
                "Lưu đăng nhập",
                f"Không ghi được vault:\n{exc}",
                parent=self._top,
            )
            return
        messagebox.showinfo(
            "Lưu đăng nhập",
            "Đã lưu mật khẩu / TOTP / email khôi phục vào vault.\n"
            "Bấm «Lưu» ở cuối cửa sổ để ghi UID/email vào accounts.json.",
            parent=self._top,
        )

    def _on_auto_login(self) -> None:
        if not self._validate_login_credentials_form("Tự đăng nhập"):
            return
        if not self._ensure_proxy_ready_for_login(dialog_title="Tự đăng nhập"):
            return
        self._persist_credentials_for_account(self._e_id.get().strip())
        force_fresh = bool(self._var_test_login_fresh.get())
        self._start_facebook_login_worker(
            dialog_title="Tự đăng nhập",
            force_fresh=force_fresh,
            require_proxy_check=False,
        )

    def _safe_messagebox(self, kind: str, title: str, message: str) -> None:
        """
        Hiển thị messagebox chỉ khi form còn tồn tại.

        Tránh lỗi ``bad window path name`` khi callback chạy sau lúc người dùng đã đóng Toplevel.
        """
        try:
            if not hasattr(self, "_top") or not bool(self._top.winfo_exists()):
                return
        except tk.TclError:
            return
        try:
            if kind == "error":
                messagebox.showerror(title, message, parent=self._top)
            elif kind == "warning":
                messagebox.showwarning(title, message, parent=self._top)
            else:
                messagebox.showinfo(title, message, parent=self._top)
        except tk.TclError:
            return

    def _start_facebook_login_worker(
        self,
        *,
        dialog_title: str,
        force_fresh: bool,
        require_proxy_check: bool = True,
    ) -> None:
        if require_proxy_check and not self._ensure_proxy_ready_for_login(dialog_title=dialog_title):
            return
        aid = self._e_id.get().strip()
        preview = self._preview_account_for_login_test()
        ck_rel = preview.get("cookie_path") or f"data/cookies/{aid}.json"

        def _run(*, fresh: bool = force_fresh) -> None:
            from src.automation.browser_factory import BrowserFactory, sync_close_persistent_context
            from src.automation.facebook_actions import prime_facebook_session_page
            from src.services.facebook_session_recovery import try_recover_facebook_session

            factory = None
            ctx = None
            err_msg = ""
            ok = False
            try:
                factory = BrowserFactory(accounts=self._manager, headless=False)
                ctx = factory.launch_persistent_context_from_account_dict(preview, headless=False)
                page = ctx.pages[0] if ctx.pages else ctx.new_page()
                prime_facebook_session_page(page)
                ok = try_recover_facebook_session(
                    page,
                    preview,
                    cookie_path=ck_rel,
                    force_fresh_login=fresh,
                )
            except Exception as exc:  # noqa: BLE001
                err_msg = str(exc)
            finally:
                sync_close_persistent_context(ctx, log_label="facebook_login_worker")
                if factory is not None:
                    try:
                        factory.close()
                    except Exception:
                        pass

            def _notify() -> None:
                if err_msg:
                    self._safe_messagebox("error", dialog_title, err_msg)
                elif ok:
                    detail = (
                        "Đăng nhập UID/email + mật khẩu (+ TOTP nếu bật) thành công."
                        if fresh
                        else "Profile đã có phiên hợp lệ (không cần đăng nhập lại)."
                    )
                    self._safe_messagebox(
                        "info",
                        dialog_title,
                        f"{detail}\nSession đã lưu vào cookie_path.",
                    )
                else:
                    self._safe_messagebox(
                        "warning",
                        dialog_title,
                        "Chưa xác nhận được phiên — có thể sai mật khẩu/TOTP, checkpoint hoặc cần xử lý tay.",
                    )

            try:
                if bool(self._top.winfo_exists()):
                    self._top.after(0, _notify)
            except tk.TclError:
                return

        import threading

        threading.Thread(target=_run, kwargs={"fresh": force_fresh}, daemon=True).start()
        px = " (qua proxy đã kiểm tra LIVE)" if bool(self._var_use_proxy.get()) else ""
        hint = (
            "đăng nhập lại từ form (UID hoặc email + mật khẩu + TOTP)"
            if force_fresh
            else "kiểm tra phiên profile hiện có"
        )
        messagebox.showinfo(
            dialog_title,
            f"Đang mở trình duyệt để {hint}{px}…",
            parent=self._top,
        )

    def _on_test_login_recovery(self) -> None:
        if not self._validate_login_credentials_form("Test Login"):
            return
        force_fresh = bool(self._var_test_login_fresh.get())
        self._persist_credentials_for_account(self._e_id.get().strip())
        self._start_facebook_login_worker(
            dialog_title="Test Login",
            force_fresh=force_fresh,
            require_proxy_check=True,
        )

    def _on_ok(self) -> None:
        try:
            if self._id_readonly:
                self._result = self._collect_single_edit()
            else:
                m = self._var_mode.get()
                if m == _ACCOUNT_MODE_MANUAL:
                    self._result = self._collect_single_new()
                elif m == _ACCOUNT_MODE_FOLDER:
                    self._result = self._collect_folder_batch()
                else:
                    self._result = self._collect_cookie_batch()
        except ValueError as exc:
            messagebox.showerror("Dữ liệu không hợp lệ", str(exc), parent=self._top)
            return
        if isinstance(self._result, dict):
            aid = str(self._result.get("id", "")).strip()
            try:
                self._persist_credentials_for_account(aid)
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror(
                    "Lỗi lưu vault",
                    f"Không ghi được mật khẩu/TOTP vào config/account_credentials.json:\n{exc}",
                    parent=self._top,
                )
                return
        self._top.grab_release()
        self._top.destroy()

    def _collect_single_edit(self) -> dict[str, Any]:
        proxy, use_px = self._proxy_block_common()
        aid = self._e_id.get().strip()
        portable = (
            (self._e_portable_edit.get().strip() if self._e_portable_edit else "")
            or (str(self._initial.get("portable_path", "")).strip() if self._initial else "")
        )
        cookie = self._e_cookie.get().strip()
        if not portable:
            portable = self._build_default_portable(aid)
        if not cookie:
            raise ValueError("cookie_path không được để trống.")
        br = _browser_storage_from_label(self._cb_browser_label.get())
        it = str(self._initial.get("import_type", "new") if self._initial else "new")
        exe = self._e_browser_exe.get().strip() if hasattr(self, "_e_browser_exe") else ""
        return self._build_account_dict_core(
            aid=aid,
            name=self._e_name.get().strip(),
            portable=portable,
            cookie=cookie,
            browser_type=br,
            proxy=proxy,
            use_px=use_px,
            import_type=it,
            browser_exe_path=exe,
        )

    def _build_default_portable(self, aid: str) -> str:
        sub = _browser_storage_from_label(self._cb_browser_label.get())
        return f"data/profiles/{sub}/{aid}"

    def _collect_single_new(self) -> dict[str, Any]:
        proxy, use_px = self._proxy_block_common()
        aid = self._e_id.get().strip()
        name = self._e_name.get().strip()
        if not aid:
            raise ValueError("Mã tài khoản (id) không được để trống.")
        if not name:
            raise ValueError("Tên tài khoản không được để trống.")
        if not self._id_readonly and self._manager.get_by_id(aid):
            raise ValueError(f"id {aid!r} đã tồn tại — đổi mã khác.")
        portable = self._build_default_portable(aid)
        cookie = self._e_cookie.get().strip()
        if not cookie:
            raise ValueError("cookie_path không được để trống.")
        br = _browser_storage_from_label(self._cb_browser_label.get())
        exe = self._e_browser_exe.get().strip() if hasattr(self, "_e_browser_exe") else ""
        return self._build_account_dict_core(
            aid=aid,
            name=name,
            portable=portable,
            cookie=cookie,
            browser_type=br,
            proxy=proxy,
            use_px=use_px,
            import_type="new",
            browser_exe_path=exe,
        )

    def _collect_folder_batch(self) -> list[dict[str, Any]]:
        if not self._folder_preview:
            raise ValueError("Chưa có dữ liệu — bấm «Quét thư mục».")
        rows_out = [r for r in self._folder_preview if r["valid"]]
        if not rows_out:
            raise ValueError("Không có dòng hợp lệ để lưu.")
        p_form = self._read_proxy_dict_raw()
        any_row_proxy = any(bool(r.get("use_proxy", True)) for r in rows_out)
        if any_row_proxy and (not p_form["host"] or p_form["port"] <= 0):
            raise ValueError(
                "Có profile đặt Proxy «có» — nhập host và port hợp lệ ở khối proxy (tab chung), "
                "hoặc đổi cột Proxy thành «không» cho các dòng không cần proxy."
            )
        out: list[dict[str, Any]] = []
        existing = {str(a.get("id", "")) for a in self._manager.load_all()}
        for row in rows_out:
            aid = re.sub(r"[^\w\-]+", "_", row["folder_name"])[:48] or uuid.uuid4().hex[:10]
            base = aid
            n = 0
            while aid in existing:
                n += 1
                aid = f"{base}_{n}"
            existing.add(aid)
            row_uses = bool(row.get("use_proxy", True))
            if row_uses:
                px_row = {
                    "host": p_form["host"],
                    "port": p_form["port"],
                    "user": p_form["user"],
                    "pass": p_form["pass"],
                }
            else:
                px_row = {"host": "", "port": 0, "user": "", "pass": ""}
            ck = str(row.get("cookie_path", "")).strip()
            auto_ck = False
            if not ck:
                ck = f"data/cookies/{aid}.json"
                auto_ck = True
            ck_path = Path(ck)
            ck_abs = ck_path.resolve() if ck_path.is_absolute() else (project_root() / ck_path).resolve()
            if not ck_abs.is_file() and auto_ck:
                ck_abs.parent.mkdir(parents=True, exist_ok=True)
                ck_abs.write_text("[]\n", encoding="utf-8")
            exe_one = str(row.get("browser_exe_path", "")).strip()
            if not exe_one or not Path(exe_one).is_file():
                continue
            rec = self._build_account_dict_core(
                aid=aid,
                name=row["folder_name"],
                portable=row["portable_path"],
                cookie=Path(ck).as_posix(),
                browser_type=row["browser_type"],
                proxy=px_row,
                use_px=row_uses,
                import_type="folder",
                browser_exe_path=exe_one,
            )
            out.append(rec)
        if not out:
            raise ValueError("Không tạo được bản ghi hợp lệ.")
        return out

    def _collect_cookie_batch(self) -> list[dict[str, Any]]:
        rows_ok = [r for r in self._cookie_preview if r["valid"]]
        if not rows_ok:
            raise ValueError("Không có cookie hợp lệ (cần c_user và xs trong JSON).")
        proxy, use_px = self._proxy_block_common()
        out: list[dict[str, Any]] = []
        for row in rows_ok:
            dest = project_root() / row["cookie_path"]
            dest.parent.mkdir(parents=True, exist_ok=True)
            pairs = row.get("pairs") or {}
            cookies_list = [{"name": k, "value": v} for k, v in pairs.items()]
            dest.write_text(json.dumps(cookies_list, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            rel_ck = account_cookie_path_field(dest)
            portable = self._build_default_portable(row["id"])
            rec = self._build_account_dict_core(
                aid=row["id"],
                name=row["name"],
                portable=portable,
                cookie=rel_ck,
                browser_type=row["browser_type"],
                proxy=proxy if use_px else {"host": "", "port": 0, "user": "", "pass": ""},
                use_px=use_px,
                import_type="cookie",
                browser_exe_path="",
            )
            out.append(rec)
        return out

    def _on_cancel(self) -> None:
        self._result = None
        try:
            self._top.grab_release()
        except tk.TclError:
            pass
        self._top.destroy()
