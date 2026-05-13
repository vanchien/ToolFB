"""
Tab TikTok Manager: tài khoản (profile riêng) + job upload qua Playwright.
Không dùng TikTok API; không tự đăng nhập; không hard-code credential.
"""

from __future__ import annotations

import os
import threading
import tkinter as tk
import uuid
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any

from loguru import logger

from src.services.ai_video_store import AIVideoStore
from src.services.tiktok.account_lock import try_run_for_account
from src.services.tiktok.account_manager import TikTokAccountStore, default_account_dict
from src.services.tiktok.browser_external import open_tiktok_profile_for_manual_login
from src.services.tiktok.job_manager import TikTokJobStore, default_job_dict
from src.services.tiktok.layout import ensure_tiktok_layout
from src.services.tiktok.upload_runner import run_tiktok_login_check_sync, run_tiktok_upload_job_sync
from src.gui.treeview_shortcuts import install_treeview_shortcuts
from src.utils.browser_exe_discover import find_browser_exe_in_directory
from src.utils.proxy_check import check_http_proxy
from src.utils.schedule_job_content import once_local_wall_to_utc_iso


def build_tiktok_manager_tab(parent: ttk.Frame, root: tk.Misc) -> None:
    ensure_tiktok_layout()
    acc_store = TikTokAccountStore()
    job_store = TikTokJobStore()
    ai_store = AIVideoStore()

    parent.columnconfigure(0, weight=1)
    parent.rowconfigure(0, weight=0)
    parent.rowconfigure(1, weight=1)

    ttk.Label(
        parent,
        text="TikTok Manager — mỗi tài khoản một profile browser; đăng nhập TikTok thủ công một lần. "
        "Upload qua trình duyệt (Playwright), không dùng API chính thức. "
        "Lên lịch đăng: cùng «Bắt đầu lịch» và chu kỳ quét như tab Job lịch Facebook (schedule_posts) — không thêm scheduler riêng.",
        wraplength=920,
        font=("Segoe UI", 9),
    ).grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 4))

    nb = ttk.Notebook(parent)
    nb.grid(row=1, column=0, sticky="nsew", padx=4, pady=(0, 4))
    tab_acc = ttk.Frame(nb, padding=4)
    tab_job = ttk.Frame(nb, padding=4)
    nb.add(tab_acc, text="  1. Tài khoản  ")
    nb.add(tab_job, text="  2. Job & lịch đăng  ")
    tab_acc.columnconfigure(0, weight=1)
    tab_acc.rowconfigure(0, weight=1)
    tab_job.columnconfigure(0, weight=1)
    tab_job.rowconfigure(0, weight=0)
    tab_job.rowconfigure(1, weight=2)
    tab_job.rowconfigure(2, weight=1)

    acc_fr = ttk.LabelFrame(tab_acc, text="Quản lý tài khoản TikTok", padding=6)
    acc_fr.grid(row=0, column=0, sticky="nsew")
    acc_fr.columnconfigure(0, weight=1)
    acc_fr.rowconfigure(1, weight=1)

    acc_bar = ttk.Frame(acc_fr)
    acc_bar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
    job_log_lines: list[str] = []

    def log_tt(msg: str) -> None:
        line = msg.rstrip()
        job_log_lines.append(line)
        if len(job_log_lines) > 400:
            del job_log_lines[:200]
        logger.info(line)

        def _ui() -> None:
            try:
                txt_log.configure(state="normal")
                txt_log.insert("end", line + "\n")
                txt_log.see("end")
                txt_log.configure(state="disabled")
            except tk.TclError:
                pass

        try:
            root.after(0, _ui)
        except tk.TclError:
            pass

    def refresh_accounts() -> None:
        tree_acc.delete(*tree_acc.get_children())
        for r in acc_store.load_all():
            px = r.get("proxy") if isinstance(r.get("proxy"), dict) else {}
            px_on = bool(px.get("enabled")) and str(px.get("server", "")).strip()
            tree_acc.insert(
                "",
                "end",
                iid=str(r.get("id")),
                values=(
                    str(r.get("name", "")),
                    str(r.get("username", "")),
                    str(r.get("profile_path", "")),
                    "có" if px_on else "không",
                    str(r.get("status", "")),
                ),
            )
        ids = [str(x.get("id")) for x in acc_store.load_all() if str(x.get("id", "")).strip()]
        cb_acc["values"] = ids
        if ids and var_acc.get() not in ids:
            var_acc.set(ids[0])

    def refresh_jobs() -> None:
        tree_job.delete(*tree_job.get_children())
        for r in job_store.load_all():
            vp = str(r.get("video_path", ""))[:80]
            sat = str(r.get("scheduled_at") or r.get("schedule_time") or "").strip()
            tree_job.insert(
                "",
                "end",
                iid=str(r.get("id")),
                values=(
                    str(r.get("id", "")),
                    str(r.get("account_id", "")),
                    vp,
                    (sat[:19] + "…") if len(sat) > 19 else (sat or "—"),
                    str(r.get("status", "")),
                    str(r.get("step", "")),
                    str(r.get("error_message", ""))[:120],
                ),
            )

    cols_acc = ("name", "username", "profile_path", "proxy", "status")
    tree_acc = ttk.Treeview(acc_fr, columns=cols_acc, show="headings", height=10, selectmode="extended")
    _acc_col_cfg: list[tuple[str, str, int, int, bool]] = [
        ("name", "Tên", 140, 80, True),
        ("username", "Username", 120, 72, True),
        ("profile_path", "Profile path", 260, 120, True),
        ("proxy", "Proxy", 72, 56, False),
        ("status", "Status", 88, 64, True),
    ]
    for c, title, w, mw, st in _acc_col_cfg:
        tree_acc.heading(c, text=title)
        tree_acc.column(c, width=w, minwidth=mw, stretch=st, anchor="w")
    sy1 = ttk.Scrollbar(acc_fr, orient=tk.VERTICAL, command=tree_acc.yview)
    tree_acc.configure(yscrollcommand=sy1.set)
    tree_acc.grid(row=1, column=0, sticky="nsew")
    sy1.grid(row=1, column=1, sticky="ns")
    install_treeview_shortcuts(tree_acc, owner=root, info_callback=log_tt)

    job_fr = ttk.LabelFrame(tab_job, text="Tạo / sửa thông tin job đăng", padding=6)
    job_fr.grid(row=0, column=0, sticky="nsew", pady=(0, 4))
    job_fr.columnconfigure(1, weight=1)

    ttk.Label(job_fr, text="Chọn tài khoản:").grid(row=0, column=0, sticky="w", pady=2)
    var_acc = tk.StringVar(value="")
    cb_acc = ttk.Combobox(job_fr, textvariable=var_acc, width=28, state="readonly")
    cb_acc.grid(row=0, column=1, sticky="w", pady=2)

    ttk.Label(job_fr, text="Video:").grid(row=1, column=0, sticky="nw", pady=2)
    var_video = tk.StringVar(value="")
    ttk.Entry(job_fr, textvariable=var_video, width=56).grid(row=1, column=1, columnspan=3, sticky="ew", pady=2)

    ttk.Label(job_fr, text="Caption:").grid(row=2, column=0, sticky="nw", pady=2)
    txt_cap = tk.Text(job_fr, height=3, width=60, wrap="word")
    txt_cap.grid(row=2, column=1, columnspan=5, sticky="ew", pady=2)

    ttk.Label(job_fr, text="Hashtags (cách space):").grid(row=3, column=0, sticky="w", pady=2)
    var_tags = tk.StringVar(value="")
    ttk.Entry(job_fr, textvariable=var_tags, width=56).grid(row=3, column=1, columnspan=5, sticky="ew", pady=2)

    ttk.Label(job_fr, text="Privacy:").grid(row=4, column=0, sticky="w", pady=2)
    var_privacy = tk.StringVar(value="public")
    ttk.Combobox(
        job_fr,
        textvariable=var_privacy,
        values=("public", "friends", "private"),
        state="readonly",
        width=12,
    ).grid(row=4, column=1, sticky="w", pady=2)

    var_allow_c = tk.BooleanVar(value=True)
    var_allow_d = tk.BooleanVar(value=True)
    var_allow_s = tk.BooleanVar(value=True)
    var_schedule = tk.BooleanVar(value=False)
    ttk.Checkbutton(job_fr, text="Allow comments", variable=var_allow_c).grid(row=5, column=0, sticky="w")
    ttk.Checkbutton(job_fr, text="Allow duet", variable=var_allow_d).grid(row=5, column=1, sticky="w")
    ttk.Checkbutton(job_fr, text="Allow stitch", variable=var_allow_s).grid(row=5, column=2, sticky="w")
    ttk.Checkbutton(job_fr, text="Lên lịch (một lần, cần Bắt đầu lịch)", variable=var_schedule).grid(
        row=5, column=3, sticky="w"
    )

    tz_name = os.environ.get("SCHEDULER_TZ", "Asia/Ho_Chi_Minh").strip() or "Asia/Ho_Chi_Minh"
    ttk.Label(job_fr, text="Hẹn đăng (YYYY-MM-DD HH:MM):").grid(row=6, column=0, sticky="w", pady=2)
    var_sched_wall = tk.StringVar(value="")
    ttk.Entry(job_fr, textvariable=var_sched_wall, width=22).grid(row=6, column=1, sticky="w", pady=2)
    ttk.Label(job_fr, text=f"wall = {tz_name}", font=("Segoe UI", 8)).grid(row=6, column=2, columnspan=2, sticky="w")

    jb = ttk.Frame(job_fr)
    jb.grid(row=7, column=0, columnspan=6, sticky="w", pady=(8, 0))

    list_fr = ttk.LabelFrame(tab_job, text="Danh sách job", padding=6)
    list_fr.grid(row=1, column=0, sticky="nsew", pady=4)
    list_fr.columnconfigure(0, weight=1)
    list_fr.rowconfigure(0, weight=1)

    cols_j = ("id", "account_id", "video_path", "scheduled_at", "status", "step", "error")
    tree_job = ttk.Treeview(list_fr, columns=cols_j, show="headings", height=10, selectmode="extended")
    heads = ("id", "account", "video", "hẹn (UTC)", "status", "step", "lỗi")
    _job_col_cfg: list[tuple[str, str, int, int, bool]] = [
        ("id", heads[0], 100, 72, False),
        ("account_id", heads[1], 100, 72, True),
        ("video_path", heads[2], 180, 100, True),
        ("scheduled_at", heads[3], 130, 96, True),
        ("status", heads[4], 80, 64, True),
        ("step", heads[5], 88, 64, True),
        ("error", heads[6], 160, 80, True),
    ]
    for c, t, w, mw, st in _job_col_cfg:
        tree_job.heading(c, text=t)
        tree_job.column(c, width=w, minwidth=mw, stretch=st, anchor="w")
    sy2 = ttk.Scrollbar(list_fr, orient=tk.VERTICAL, command=tree_job.yview)
    tree_job.configure(yscrollcommand=sy2.set)
    tree_job.grid(row=0, column=0, sticky="nsew")
    sy2.grid(row=0, column=1, sticky="ns")
    install_treeview_shortcuts(tree_job, owner=root, info_callback=log_tt)

    log_fr = ttk.LabelFrame(tab_job, text="Nhật ký TikTok", padding=4)
    log_fr.grid(row=2, column=0, sticky="nsew", pady=4)
    log_fr.columnconfigure(0, weight=1)
    log_fr.rowconfigure(0, weight=1)
    txt_log = tk.Text(log_fr, height=8, state="disabled", wrap="word", font=("Consolas", 9))
    txt_log.grid(row=0, column=0, sticky="nsew")
    sl = ttk.Scrollbar(log_fr, orient=tk.VERTICAL, command=txt_log.yview)
    txt_log.configure(yscrollcommand=sl.set)
    sl.grid(row=0, column=1, sticky="ns")

    def _split_proxy_server(raw: str) -> tuple[str, str]:
        s = str(raw or "").strip()
        if not s:
            return ("", "")
        if "://" in s:
            s = s.split("://", 1)[1]
        host, sep, port = s.rpartition(":")
        if not sep:
            return (s, "")
        if port.isdigit():
            return (host, port)
        return (s, "")

    def account_dialog(edit_aid: str | None = None) -> None:
        init_row = acc_store.get_by_id(edit_aid or "") if edit_aid else None
        px_init = init_row.get("proxy") if isinstance(init_row, dict) and isinstance(init_row.get("proxy"), dict) else {}
        if not isinstance(px_init, dict):
            px_init = {}
        px_host_init, px_port_init = _split_proxy_server(str(px_init.get("server", "")))
        top = tk.Toplevel(root)
        top.title("Sửa tài khoản TikTok" if init_row else "Thêm tài khoản TikTok")
        top.geometry("560x420")
        top.minsize(500, 360)
        top.transient(root)
        top.grab_set()

        var_name = tk.StringVar(value=str((init_row or {}).get("name", "")))
        var_id = tk.StringVar(value=str((init_row or {}).get("id", f"tt_acc_{uuid.uuid4().hex[:10]}")))
        var_user = tk.StringVar(value=str((init_row or {}).get("username", "")))
        var_bt = tk.StringVar(value=str((init_row or {}).get("browser_type", "chrome") or "chrome"))
        var_exe = tk.StringVar(value=str((init_row or {}).get("browser_exe_path", "")))
        var_prof = tk.StringVar(value=str((init_row or {}).get("profile_path", "")))
        var_px_on = tk.BooleanVar(value=bool(px_init.get("enabled")))
        var_px_host = tk.StringVar(value=px_host_init)
        var_px_port = tk.StringVar(value=px_port_init)
        var_px_u = tk.StringVar(value=str(px_init.get("username", "")))
        var_px_p = tk.StringVar(value=str(px_init.get("password", "")))

        r = 0
        ttk.Label(top, text="ID tài khoản:").grid(row=r, column=0, sticky="w", padx=8, pady=4)
        id_fr = ttk.Frame(top)
        id_fr.grid(row=r, column=1, sticky="ew", padx=8, pady=4)
        ttk.Entry(id_fr, textvariable=var_id, width=30, state=("readonly" if init_row else "normal")).pack(side=tk.LEFT)
        if not init_row:
            ttk.Button(id_fr, text="Tự sinh", command=lambda: var_id.set(f"tt_acc_{uuid.uuid4().hex[:10]}")).pack(
                side=tk.LEFT, padx=4
            )
        r += 1
        ttk.Label(top, text="Tên hiển thị:").grid(row=r, column=0, sticky="w", padx=8, pady=4)
        ttk.Entry(top, textvariable=var_name, width=40).grid(row=r, column=1, sticky="ew", padx=8, pady=4)
        r += 1
        ttk.Label(top, text="Username (@...):").grid(row=r, column=0, sticky="w", padx=8, pady=4)
        ttk.Entry(top, textvariable=var_user, width=40).grid(row=r, column=1, sticky="ew", padx=8, pady=4)
        r += 1
        ttk.Label(top, text="Browser type:").grid(row=r, column=0, sticky="w", padx=8, pady=4)
        ttk.Combobox(top, textvariable=var_bt, values=("chrome", "firefox"), state="readonly", width=12).grid(
            row=r, column=1, sticky="w", padx=8, pady=4
        )
        r += 1
        ttk.Label(top, text="Profile path (có thể để trống):").grid(row=r, column=0, sticky="nw", padx=8, pady=4)
        fp = ttk.Frame(top)
        fp.grid(row=r, column=1, sticky="ew", padx=8, pady=4)
        ttk.Entry(fp, textvariable=var_prof, width=36).pack(side=tk.LEFT)
        ttk.Button(
            fp,
            text="Chọn…",
            command=lambda: var_prof.set(filedialog.askdirectory(parent=top, title="Thư mục profile TikTok") or var_prof.get()),
        ).pack(side=tk.LEFT, padx=4)
        r += 1
        ttk.Checkbutton(top, text="Dùng proxy khi mở trình duyệt", variable=var_px_on).grid(
            row=r, column=0, columnspan=2, sticky="w", padx=8, pady=4
        )
        r += 1
        px_fr = ttk.Frame(top)
        px_fr.grid(row=r, column=0, columnspan=2, sticky="ew", padx=8, pady=4)
        px_fr.columnconfigure(1, weight=1)
        pr = 0
        ttk.Label(px_fr, text="Host").grid(row=pr, column=0, sticky="w", padx=(0, 6), pady=2)
        ttk.Entry(px_fr, textvariable=var_px_host, width=36).grid(row=pr, column=1, sticky="ew", pady=2)
        pr += 1
        ttk.Label(px_fr, text="Port").grid(row=pr, column=0, sticky="w", padx=(0, 6), pady=2)
        ttk.Entry(px_fr, textvariable=var_px_port, width=12).grid(row=pr, column=1, sticky="w", pady=2)
        pr += 1
        ttk.Label(px_fr, text="User").grid(row=pr, column=0, sticky="w", padx=(0, 6), pady=2)
        ttk.Entry(px_fr, textvariable=var_px_u, width=36).grid(row=pr, column=1, sticky="ew", pady=2)
        pr += 1
        ttk.Label(px_fr, text="Password").grid(row=pr, column=0, sticky="w", padx=(0, 6), pady=2)
        ttk.Entry(px_fr, textvariable=var_px_p, width=36, show="*").grid(row=pr, column=1, sticky="ew", pady=2)
        r += 1

        def on_check_proxy_dialog() -> None:
            if not bool(var_px_on.get()):
                messagebox.showinfo("Proxy", "Đang tắt «Dùng proxy» — không kiểm tra.", parent=top)
                return
            host = var_px_host.get().strip()
            if not host:
                messagebox.showwarning("Proxy", "Nhập Host proxy trước khi kiểm tra.", parent=top)
                return
            try:
                port = int(str(var_px_port.get()).strip() or "0")
            except ValueError:
                messagebox.showerror("Proxy", "Port proxy không hợp lệ.", parent=top)
                return
            if not (1 <= port <= 65535):
                messagebox.showerror("Proxy", "Port proxy phải trong khoảng 1..65535.", parent=top)
                return
            ok, msg = check_http_proxy(host, port, user=var_px_u.get().strip(), password=var_px_p.get().strip())
            if ok:
                messagebox.showinfo("Proxy", f"LIVE — IP: {msg}", parent=top)
            else:
                messagebox.showerror("Proxy", msg, parent=top)

        ttk.Button(top, text="Kiểm tra proxy", command=on_check_proxy_dialog).grid(
            row=r, column=1, sticky="w", padx=8, pady=(0, 4)
        )
        r += 1

        def build_account_row() -> dict[str, Any] | None:
            aid = var_id.get().strip()
            if not aid:
                messagebox.showwarning("TikTok", "Nhập ID tài khoản.", parent=top)
                return None
            name = var_name.get().strip()
            if not name:
                messagebox.showwarning("TikTok", "Nhập tên tài khoản.", parent=top)
                return None
            prof = var_prof.get().strip()
            if not prof:
                prof = str((ensure_tiktok_layout()["root"] / "profiles" / aid).resolve())
            host = var_px_host.get().strip()
            port_raw = var_px_port.get().strip()
            if bool(var_px_on.get()) and (not host or not port_raw):
                messagebox.showwarning("TikTok", "Bật proxy thì cần nhập Host và Port.", parent=top)
                return None
            if port_raw:
                try:
                    port_num = int(port_raw)
                except ValueError:
                    messagebox.showwarning("TikTok", "Port proxy không hợp lệ.", parent=top)
                    return None
                if not (1 <= port_num <= 65535):
                    messagebox.showwarning("TikTok", "Port proxy phải trong khoảng 1..65535.", parent=top)
                    return None
            else:
                port_num = 0

            resolved_exe = var_exe.get().strip()
            if not resolved_exe:
                # Cho phép bỏ qua bước chọn .exe khi tạo mới; thử tự tìm trong thư mục profile portable.
                resolved_exe = find_browser_exe_in_directory(prof)

            row = default_account_dict(
                name=name,
                username=var_user.get().strip(),
                browser_type=var_bt.get().strip(),
                browser_exe_path=resolved_exe,
                profile_path=prof,
            )
            row["id"] = aid
            proxy_server = f"{host}:{port_num}" if host and port_num else ""
            row["proxy"] = {
                "enabled": bool(var_px_on.get()),
                "server": proxy_server,
                "username": var_px_u.get().strip(),
                "password": var_px_p.get().strip(),
            }
            return row

        def save_new() -> None:
            row = build_account_row()
            if row is None:
                return
            aid = str(row.get("id", "")).strip()
            if (not init_row) and acc_store.get_by_id(aid):
                messagebox.showwarning("TikTok", f"ID đã tồn tại: {aid}\nHãy đổi ID khác.", parent=top)
                return
            acc_store.upsert(row)
            refresh_accounts()
            top.destroy()
            messagebox.showinfo(
                "TikTok",
                "Đã lưu tài khoản TikTok.",
                parent=root,
            )

        def open_login_now() -> None:
            row = build_account_row()
            if row is None:
                return
            try:
                open_tiktok_profile_for_manual_login(row)
                messagebox.showinfo(
                    "TikTok",
                    "Đã mở browser profile để đăng nhập TikTok.\nĐăng nhập xong quay lại bấm «Lưu».",
                    parent=top,
                )
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("TikTok", str(exc), parent=top)

        bf = ttk.Frame(top)
        bf.grid(row=r + 1, column=0, columnspan=2, pady=12)
        ttk.Button(bf, text="Mở browser đăng nhập", command=open_login_now).pack(side=tk.LEFT, padx=6)
        ttk.Button(bf, text=("Cập nhật" if init_row else "Lưu"), command=save_new).pack(side=tk.LEFT, padx=6)
        ttk.Button(bf, text="Hủy", command=top.destroy).pack(side=tk.LEFT)
        top.columnconfigure(1, weight=1)

    def add_account_dialog() -> None:
        account_dialog(None)

    def selected_account_id() -> str:
        sel = tree_acc.selection()
        if sel:
            return str(sel[0])
        return var_acc.get().strip()

    def on_check_login() -> None:
        aid = selected_account_id()
        acc = acc_store.get_by_id(aid)
        if not acc:
            messagebox.showwarning("TikTok", "Chọn một tài khoản.", parent=root)
            return

        def work() -> None:
            ok, err = run_tiktok_login_check_sync(acc, log=log_tt)
            st = acc_store.get_by_id(aid) or acc
            st["last_check"] = datetime.now().replace(microsecond=0).isoformat()
            st["status"] = "active" if ok else "need_manual_check"
            acc_store.upsert(st)

            def ui() -> None:
                refresh_accounts()
                if ok:
                    messagebox.showinfo("TikTok", "Đã đăng nhập (kiểm tra nhanh).", parent=root)
                else:
                    messagebox.showwarning("TikTok", err or "Chưa đăng nhập.", parent=root)

            root.after(0, ui)

        threading.Thread(target=work, daemon=True).start()

    def on_open_profile() -> None:
        aid = selected_account_id()
        acc = acc_store.get_by_id(aid)
        if not acc:
            messagebox.showwarning("TikTok", "Chọn một tài khoản.", parent=root)
            return
        try:
            open_tiktok_profile_for_manual_login(acc)
            messagebox.showinfo("TikTok", "Đã mở trình duyệt với profile — đăng nhập TikTok thủ công.", parent=root)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("TikTok", str(exc), parent=root)

    def on_delete_account() -> None:
        aid = selected_account_id()
        if not aid:
            return
        if not messagebox.askyesno("TikTok", f"Xóa tài khoản {aid}?", parent=root):
            return
        acc_store.delete(aid)
        refresh_accounts()

    def on_edit_account() -> None:
        aid = selected_account_id()
        if not aid or not acc_store.get_by_id(aid):
            messagebox.showwarning("TikTok", "Chọn một tài khoản để sửa.", parent=root)
            return
        account_dialog(aid)

    def pick_local_video() -> None:
        p = filedialog.askopenfilename(
            parent=root,
            title="Chọn video",
            filetypes=[("Video", "*.mp4 *.webm *.mov *.mkv"), ("All", "*.*")],
        )
        if p:
            var_video.set(p)

    def pick_video_library() -> None:
        rows = ai_store.load_all()
        candidates: list[tuple[str, str]] = []
        for r in rows:
            vid = str(r.get("id", "")).strip()
            outs = r.get("output_files")
            if not isinstance(outs, list):
                continue
            for o in outs:
                p = Path(str(o))
                if p.suffix.lower() in {".mp4", ".webm", ".mov", ".mkv"} and p.is_file():
                    candidates.append((vid, str(p)))
                    break

        if not candidates:
            messagebox.showinfo("TikTok", "Video Library chưa có file video nào (AI Video outputs).", parent=root)
            return

        top = tk.Toplevel(root)
        top.title("Chọn video từ Video Library")
        top.geometry("560x320")
        top.minsize(480, 280)
        lb = tk.Listbox(top, height=14)
        lb.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        for vid, path in candidates:
            lb.insert("end", f"{vid}  —  {path}")

        def take() -> None:
            sel = lb.curselection()
            if not sel:
                return
            _, path = candidates[sel[0]]
            var_video.set(path)
            top.destroy()

        ttk.Button(top, text="Chọn", command=take).pack(pady=6)

    def on_create_job() -> None:
        aid = var_acc.get().strip()
        if not acc_store.get_by_id(aid):
            messagebox.showwarning("TikTok", "Chọn tài khoản hợp lệ.", parent=root)
            return
        vp = var_video.get().strip()
        if not vp or not Path(vp).is_file():
            messagebox.showwarning("TikTok", "Chọn file video hợp lệ.", parent=root)
            return
        cap = txt_cap.get("1.0", "end").strip()
        tags_raw = var_tags.get().strip()
        tags = [t for t in tags_raw.split() if t.strip()]
        job = default_job_dict(
            account_id=aid,
            video_path=vp,
            caption=cap,
            hashtags=tags,
            privacy=var_privacy.get().strip(),
        )
        job["allow_comments"] = bool(var_allow_c.get())
        job["allow_duet"] = bool(var_allow_d.get())
        job["allow_stitch"] = bool(var_allow_s.get())
        sched_on = bool(var_schedule.get())
        wall = var_sched_wall.get().strip()
        scheduled_at_iso = ""
        if sched_on:
            if not wall:
                messagebox.showwarning(
                    "TikTok",
                    "Bật «Lên lịch» cần nhập ngày giờ dạng YYYY-MM-DD HH:MM (theo múi giờ SCHEDULER_TZ).",
                    parent=root,
                )
                return
            try:
                scheduled_at_iso = once_local_wall_to_utc_iso(wall)
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("TikTok", f"Giờ lịch không hợp lệ: {exc}", parent=root)
                return
        job["schedule_enabled"] = sched_on
        job["scheduled_at"] = scheduled_at_iso if sched_on else ""
        job["schedule_time"] = wall if sched_on else ""
        job_store.upsert(job)
        refresh_jobs()
        messagebox.showinfo("TikTok", f"Đã tạo job {job['id']}", parent=root)

    def patch_job_by_id(job_id: str, partial: dict[str, Any]) -> None:
        cur = job_store.get_by_id(job_id)
        if not cur:
            return
        cur.update(partial)
        job_store.upsert(cur)
        root.after(0, refresh_jobs)

    def on_run_selected_job() -> None:
        sel = tree_job.selection()
        if not sel:
            messagebox.showwarning("TikTok", "Chọn một job trong bảng.", parent=root)
            return
        jid = str(sel[0])
        job = job_store.get_by_id(jid)
        if not job:
            return
        acc = acc_store.get_by_id(str(job.get("account_id", "")))
        if not acc:
            messagebox.showerror("TikTok", "Không tìm thấy tài khoản của job.", parent=root)
            return

        job = dict(job)

        def runner() -> None:
            def patch(p: dict[str, Any]) -> None:
                patch_job_by_id(jid, p)

            def inner() -> None:
                run_tiktok_upload_job_sync(job, acc, log=log_tt, patch_job=patch)

            ok, msg = try_run_for_account(str(job["account_id"]), inner)
            if not ok:

                def warn() -> None:
                    messagebox.showwarning("TikTok", msg, parent=root)

                root.after(0, warn)

        threading.Thread(target=runner, daemon=True).start()

    def on_delete_job() -> None:
        sel = tree_job.selection()
        if not sel:
            return
        jid = str(sel[0])
        if messagebox.askyesno("TikTok", f"Xóa job {jid}?", parent=root):
            job_store.delete(jid)
            refresh_jobs()

    ttk.Button(acc_bar, text="Thêm tài khoản", command=lambda: add_account_dialog()).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(acc_bar, text="Sửa tài khoản", command=on_edit_account).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(acc_bar, text="Kiểm tra login", command=on_check_login).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(acc_bar, text="Mở profile", command=on_open_profile).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(acc_bar, text="Xóa tài khoản", command=on_delete_account).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(acc_bar, text="Làm mới", command=lambda: (refresh_accounts(), refresh_jobs())).pack(side=tk.LEFT)

    ttk.Button(job_fr, text="Video Library…", command=pick_video_library).grid(row=1, column=4, padx=4)
    ttk.Button(job_fr, text="File local…", command=pick_local_video).grid(row=1, column=5, padx=4)

    ttk.Button(jb, text="Tạo job", command=on_create_job).pack(side=tk.LEFT, padx=(0, 8))
    ttk.Button(jb, text="Chạy ngay (job đang chọn)", command=on_run_selected_job).pack(side=tk.LEFT, padx=(0, 8))
    ttk.Button(jb, text="Xóa job chọn", command=on_delete_job).pack(side=tk.LEFT)

    refresh_accounts()
    refresh_jobs()
