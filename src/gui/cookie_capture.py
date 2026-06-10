"""
Đăng nhập Facebook qua Playwright persistent context → ghi ``storage_state`` (cookie JSON).

Dùng chung cho form «Thêm tài khoản» và bảng quản lý chính.
"""

from __future__ import annotations

import os
import queue
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import tkinter as tk
from tkinter import messagebox, ttk

from loguru import logger
from playwright.sync_api import BrowserContext, Page

from src.automation.browser_factory import BrowserFactory, sync_close_persistent_context
from src.services.facebook_session_recovery import confirm_facebook_session_logged_in
from src.utils.account_browser_profile import ensure_account_browser_profile_ready
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.paths import project_root


def _page_usable(page: Page) -> bool:
    try:
        return not page.is_closed()
    except Exception:
        return False


def _manual_launch_wait_sec() -> float:
    raw = os.environ.get("FB_MANUAL_BROWSER_LAUNCH_WAIT_SEC", "120").strip()
    try:
        return max(30.0, float(raw))
    except ValueError:
        return 120.0


def prepare_manual_login_session(
    page: Page,
    context: BrowserContext,
    acc: dict[str, Any],
    *,
    cookie_path: str,
    progress: Callable[[str], None] | None = None,
) -> str:
    """
    Chuẩn bị phiên đăng nhập thủ công: registry, profile, cookie, mở Facebook (không auto recovery).

    Returns:
        Mô tả ngắn hiển thị trên hộp thoại GUI.
    """

    def _prog(msg: str) -> None:
        if progress:
            progress(msg)

    from src.automation.facebook_actions import (
        facebook_session_appears_logged_in,
        prime_facebook_session_page,
    )

    notes: list[str] = ["Profile + proxy + stealth"]
    if not _page_usable(page):
        return "Trình duyệt đã đóng"
    _prog("Đang mở facebook.com — kiểm tra phiên profile…")
    prime_facebook_session_page(page)
    try:
        page.bring_to_front()
    except Exception:
        pass
    if facebook_session_appears_logged_in(page):
        notes.append("đã đăng nhập sẵn trong profile")
    else:
        from src.services.facebook_session_persist import cookie_file_has_session, restore_facebook_session

        ok, mode = restore_facebook_session(page, acc, cookie_path=cookie_path, prime=False)
        if ok:
            notes.append(f"phiên từ {mode}")
        elif cookie_file_has_session(cookie_path):
            notes.append("chưa vào được — đăng nhập tay hoặc bấm Lưu sau khi vào feed")
        else:
            notes.append("sẵn sàng đăng nhập / 2FA / captcha tay")
    return " · ".join(notes)


def cookie_storage_dest(ck_rel: str, root: Path) -> Path:
    """Đường dẫn tuyệt đối tới file storage_state / cookie JSON."""
    ck_path = Path(ck_rel.strip())
    return ck_path.resolve() if ck_path.is_absolute() else (root / ck_path).resolve()


def account_cookie_path_field(dest: Path) -> str:
    """Chuỗi lưu vào ``cookie_path`` (tương đối dự án nếu nằm trong repo)."""
    r = project_root().resolve()
    try:
        return dest.resolve().relative_to(r).as_posix()
    except ValueError:
        return str(dest.resolve())


def run_fb_cookie_capture_dialog(
    parent: tk.Misc,
    manager: AccountsDatabaseManager,
    acc_preview: dict[str, Any],
    ck_rel: str,
    *,
    log_label: str,
    tip_extra: str = "(File ghi đúng cookie_path trong form.)",
    on_after_save: Callable[[], None] | None = None,
    on_dialog_done: Callable[[], None] | None = None,
    on_launch_failed: Callable[[str], None] | None = None,
    grid_viewport: tuple[int, int] | None = None,
    window_position: tuple[int, int] | None = None,
    dialog_title: str | None = None,
) -> None:
    """
    Mở trình duyệt persistent theo ``acc_preview``; user đăng nhập tay → «Lưu cookie».

    Hộp thoại hiện **trước** khi launch Firefox; không gọi recovery tự động (tránh treo GUI).
    """
    logger.info("[FB manual open] Mở dialog capture account={}", log_label)
    root = project_root()

    cmd_q: queue.Queue[str] = queue.Queue()
    progress_q: queue.Queue[str] = queue.Queue()
    done_evt = threading.Event()
    err_holder: list[str] = []
    action_holder: list[str] = []
    success_detail: list[str] = []
    prep_holder: list[str] = []

    try:
        tip_parent = parent.winfo_toplevel()
    except tk.TclError:
        tip_parent = parent
    tip = tk.Toplevel(tip_parent)
    tip.title(dialog_title or "Đăng nhập Facebook")
    try:
        tip.transient(tip_parent)
    except tk.TclError:
        pass
    tip.geometry("520x240")
    tip.minsize(440, 200)

    def _fit_tip_window() -> None:
        """Co giãn chiều cao hộp thoại — tránh nhãn dài che mất hàng nút."""
        try:
            tip.update_idletasks()
            w = max(440, int(tip.winfo_width()), int(tip.winfo_reqwidth()))
            h = max(200, min(int(tip.winfo_reqheight()) + 12, 480))
            tip.geometry(f"{w}x{h}")
        except tk.TclError:
            pass

    try:
        tip.lift()
        tip.attributes("-topmost", True)
        tip.after(350, lambda: tip.attributes("-topmost", False))
        tip.focus_force()
    except tk.TclError:
        pass

    def send(cmd: str) -> None:
        action_holder.append(cmd)
        try:
            cmd_q.put(cmd)
        except Exception:
            pass
        try:
            tip.destroy()
        except tk.TclError:
            pass

    bf = ttk.Frame(tip, padding=8)
    bf.pack(side=tk.BOTTOM, fill=tk.X)
    ttk.Button(bf, text="Lưu cookie vào file", command=lambda: send("save")).pack(side=tk.RIGHT, padx=4)
    ttk.Button(bf, text="Hủy (đóng trình duyệt)", command=lambda: send("cancel")).pack(side=tk.RIGHT)

    lbl_tip = ttk.Label(
        tip,
        text=(
            "Đang mở Firefox (profile + proxy)…\n"
            "Cửa sổ trình duyệt sẽ hiện sau vài giây — giữ hộp thoại này mở.\n"
            f"{tip_extra}"
        ),
        wraplength=480,
        justify=tk.LEFT,
    )
    lbl_tip.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=12, pady=(12, 4))

    def _on_tip_configure(event: tk.Event) -> None:
        if event.widget is not tip:
            return
        try:
            lbl_tip.configure(wraplength=max(300, int(tip.winfo_width()) - 48))
        except tk.TclError:
            pass

    tip.bind("<Configure>", _on_tip_configure, add="+")
    tip.after_idle(_fit_tip_window)

    def on_tip_close() -> None:
        send("cancel")

    tip.protocol("WM_DELETE_WINDOW", on_tip_close)

    def worker() -> None:
        factory: BrowserFactory | None = None
        ctx = None
        try:
            logger.info("[FB manual open] Worker bắt đầu account={}", log_label)
            progress_q.put("Chuẩn bị profile Firefox…")
            ensure_account_browser_profile_ready(acc_preview)
            progress_q.put("Khởi động Playwright…")
            logger.info("[FB manual open] Bắt đầu launch Firefox account={}", log_label)
            factory = BrowserFactory(accounts=manager, headless=False)
            progress_q.put("Đang mở Firefox…")
            ctx = factory.launch_persistent_context_from_account_dict(
                acc_preview,
                headless=False,
                grid_viewport=grid_viewport,
                window_position=window_position,
                disable_notifications=True,
                launch_slot_timeout_sec=_manual_launch_wait_sec(),
                skip_geo_lookup=True,
                force_desktop_facebook=True,
            )
            prof = str(acc_preview.get("portable_path") or acc_preview.get("profile_path") or "").strip()
            if prof:
                from src.utils.win_browser_window import (
                    firefox_outer_window_size,
                    foreground_firefox_for_profile,
                    place_firefox_window_for_profile,
                )

                wx, wy = (80, 80)
                cw, ch = 1280, 900
                if window_position:
                    wx, wy = int(window_position[0]), int(window_position[1])
                if grid_viewport and len(grid_viewport) >= 2:
                    cw, ch = max(320, int(grid_viewport[0])), max(400, int(grid_viewport[1]))
                outer_w, outer_h = firefox_outer_window_size(cw, ch)
                place_firefox_window_for_profile(
                    prof, x=wx, y=wy, width=outer_w, height=outer_h, timeout_s=15.0
                )
                foreground_firefox_for_profile(prof, timeout_s=8.0)
            progress_q.put("Firefox đã mở — tải Facebook…")
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                page.bring_to_front()
            except Exception:
                pass
            summary = prepare_manual_login_session(
                page,
                ctx,
                acc_preview,
                cookie_path=ck_rel,
                progress=lambda m: progress_q.put(m),
            )
            prep_holder.append(summary)
            logger.info("[FB manual open] {} — {}", log_label, summary)
            progress_q.put("Sẵn sàng — dùng cửa sổ Firefox phía trước")

            cmd = ""
            while not cmd:
                if not _page_usable(page):
                    err_holder.append(
                        "Trình duyệt đã đóng — mở lại từ «Mở trình duyệt» hoặc bấm «Hủy» trước khi đóng."
                    )
                    cmd = "cancel"
                    break
                try:
                    cmd = cmd_q.get(timeout=0.45)
                except queue.Empty:
                    continue
            if cmd == "save":
                if not _page_usable(page):
                    err_holder.append("Trình duyệt đã đóng — không lưu được cookie.")
                else:
                    ok_in, detail = confirm_facebook_session_logged_in(page, acc_preview)
                    if not ok_in:
                        err_holder.append(
                            detail or "Chưa xác nhận đăng nhập — hãy vào bảng tin Facebook rồi bấm Lưu lại."
                        )
                    else:
                        from src.services.facebook_session_persist import (
                            ensure_account_cookie_path,
                            persist_confirmed_facebook_session,
                        )

                        ensure_account_cookie_path(acc_preview, ck_rel)
                        if persist_confirmed_facebook_session(
                            page,
                            acc_preview,
                            cookie_path=ck_rel,
                            log_label="manual_capture",
                        ):
                            success_detail.append(detail)
                        else:
                            err_holder.append(
                                "Không ghi được cookie — hãy vào bảng tin Facebook rồi bấm Lưu lại."
                            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if "has been closed" in msg or type(exc).__name__ == "TargetClosedError":
                err_holder.append("Trình duyệt đã đóng trước khi hoàn tất.")
            else:
                err_holder.append(msg)
            logger.exception("login_capture_cookie: {}", log_label)
        finally:
            sync_close_persistent_context(ctx, log_label=log_label)
            if factory is not None:
                try:
                    factory.close()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Đóng factory sau login capture: {}", exc)
            done_evt.set()

    def _start_worker() -> None:
        threading.Thread(target=worker, name="fb_login_capture", daemon=False).start()

    try:
        tip.update_idletasks()
    except tk.TclError:
        pass
    tip.after(80, _start_worker)

    def poll() -> None:
        try:
            while True:
                msg = progress_q.get_nowait()
                if lbl_tip.winfo_exists():
                    lbl_tip.configure(
                        text=f"{msg}\n"
                        "Sau khi đăng nhập xong trên Firefox, bấm «Lưu cookie vào file».\n"
                        f"{tip_extra}"
                    )
                    _fit_tip_window()
        except queue.Empty:
            pass
        if prep_holder and lbl_tip.winfo_exists():
            lbl_tip.configure(
                text=(
                    f"✓ {prep_holder[0]}\n"
                    "Hoàn tất đăng nhập / 2FA / captcha trên Firefox, rồi bấm «Lưu cookie vào file».\n"
                    f"{tip_extra}"
                )
            )
            _fit_tip_window()
        if not done_evt.is_set():
            parent.after(350, poll)
            return
        try:
            if tip.winfo_exists():
                tip.destroy()
        except tk.TclError:
            pass
        if err_holder:
            err_msg = err_holder[0]
            if on_launch_failed is not None:
                on_launch_failed(err_msg)
            messagebox.showerror("Trình duyệt", err_msg, parent=parent)
        elif action_holder and action_holder[-1] == "save":
            extra = success_detail[0] if success_detail else "Đã xác nhận vào tài khoản"
            messagebox.showinfo(
                "Đăng nhập thành công",
                f"{extra}\n\nĐã lưu cookie:\n{ck_rel}",
                parent=parent,
            )
            if on_after_save is not None:
                on_after_save()
        if on_dialog_done is not None:
            on_dialog_done()

    parent.after(200, poll)
