"""
Thanh thông báo lịch đăng (tab Lịch đăng) + thông báo nổi góc màn hình.

- Hiện bài sắp đăng kế tiếp (Page, tài khoản, giờ, đếm ngược) và vài bài tiếp theo.
- Nhấp nháy khi sắp tới giờ (≤ 5 phút), xanh khi đang đăng, nút đỏ nhấp nháy khi có bài quá giờ.
- Thông báo nổi trượt lên ở góc phải dưới, không chiếm chuột/bàn phím.
"""

from __future__ import annotations

import math
import sys
import time
import tkinter as tk
from datetime import datetime, timezone
from tkinter import ttk
from typing import Any, Callable

from loguru import logger

from src.gui.ui_responsiveness import run_background_then_main
from src.services.schedule_alerts import AlertSnapshot, UpcomingPost, build_alert_snapshot, format_countdown
from src.utils.page_schedule import scheduler_tz

_BG_IDLE = "#f6f8fa"
_BG_URGENT_A = "#fff8c5"
_BG_URGENT_B = "#ffd33d"
_BG_RUNNING = "#dafbe1"
_BG_FLASH = "#ffe58f"
_FG_MAIN = "#1f2328"
_FG_MUTED = "#57606a"
_RED_A = "#cf222e"
_RED_B = "#ff8182"
_URGENT_SECONDS = 5 * 60
_TOAST_LEAD_SECONDS = 5 * 60
_FRAME_MS = 100
_REFRESH_MS = 15_000


def _mix(c1: str, c2: str, t: float) -> str:
    """Trộn hai màu hex theo tỉ lệ ``t`` (0 → c1, 1 → c2)."""
    t = max(0.0, min(1.0, t))
    a = [int(c1[i : i + 2], 16) for i in (1, 3, 5)]
    b = [int(c2[i : i + 2], 16) for i in (1, 3, 5)]
    return "#" + "".join(f"{round(x + (y - x) * t):02x}" for x, y in zip(a, b))


def _local_hhmm(at: datetime, now: datetime) -> str:
    """Giờ địa phương «HH:MM», thêm ngày nếu không phải hôm nay."""
    tz = scheduler_tz()
    loc = at.astimezone(tz)
    if loc.date() != now.astimezone(tz).date():
        return loc.strftime("%d/%m %H:%M")
    return loc.strftime("%H:%M")


def _work_area_right_bottom(w: tk.Misc) -> tuple[int, int]:
    """Góc phải dưới vùng làm việc (không tính taskbar) của màn hình chính."""
    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            rect = wintypes.RECT()
            if ctypes.windll.user32.SystemParametersInfoW(0x0030, 0, ctypes.byref(rect), 0):
                return int(rect.right), int(rect.bottom)
        except Exception:  # noqa: BLE001
            pass
    return w.winfo_screenwidth(), w.winfo_screenheight() - 60


class ToastNotifier:
    """Thông báo nổi góc phải dưới: trượt lên + hiện dần, tự ẩn, xếp chồng nhiều cái."""

    WIDTH = 340
    SHOW_MS = 8000

    def __init__(self, root: tk.Misc) -> None:
        """Gắn với cửa sổ gốc để dùng chung vòng lặp sự kiện."""
        self._root = root
        self._live: list[tk.Toplevel] = []

    def show(self, title: str, body: str, *, accent: str = "#0969da", on_click: Callable[[], None] | None = None) -> None:
        """Hiện một thông báo; bấm vào sẽ gọi ``on_click`` rồi đóng."""
        try:
            top = tk.Toplevel(self._root)
        except tk.TclError:
            return
        top.overrideredirect(True)
        try:
            top.attributes("-topmost", True)
            top.attributes("-alpha", 0.0)
        except tk.TclError:
            pass
        outer = tk.Frame(top, bg=accent)
        outer.pack(fill=tk.BOTH, expand=True)
        inner = tk.Frame(outer, bg="white")
        inner.pack(fill=tk.BOTH, expand=True, padx=(5, 1), pady=1)
        lbl_t = tk.Label(
            inner, text=title, bg="white", fg=accent, font=("Segoe UI", 10, "bold"), anchor="w",
            justify=tk.LEFT, wraplength=self.WIDTH - 30,
        )
        lbl_t.pack(fill=tk.X, padx=10, pady=(8, 0))
        lbl_b = tk.Label(
            inner, text=body, bg="white", fg=_FG_MAIN, font=("Segoe UI", 9), justify=tk.LEFT,
            anchor="w", wraplength=self.WIDTH - 30,
        )
        lbl_b.pack(fill=tk.X, padx=10, pady=(2, 8))

        def _close(_e: Any = None) -> None:
            self._fade_out(top)

        def _click(_e: Any = None) -> None:
            if on_click is not None:
                try:
                    on_click()
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Toast click lỗi: {}", exc)
            _close()

        for w in (top, outer, inner, lbl_t, lbl_b):
            w.bind("<Button-1>", _click)
        top.update_idletasks()
        h = max(70, top.winfo_reqheight())
        self._live = [t for t in self._live if t.winfo_exists()]
        offset = sum(t.winfo_height() + 8 for t in self._live)
        right, bottom = _work_area_right_bottom(top)
        x = right - self.WIDTH - 16
        y_final = bottom - 16 - h - offset
        self._live.append(top)
        self._animate_in(top, x, y_final, h, step=0)
        top.after(self.SHOW_MS, _close)

    def _animate_in(self, top: tk.Toplevel, x: int, y_final: int, h: int, *, step: int) -> None:
        """Trượt lên 30px và tăng độ đậm trong khoảng 300ms."""
        steps = 12
        if not top.winfo_exists():
            return
        t = (step + 1) / steps
        ease = 1 - (1 - t) ** 3
        y = int(y_final + 30 * (1 - ease))
        try:
            top.geometry(f"{self.WIDTH}x{h}+{x}+{y}")
            top.attributes("-alpha", 0.96 * ease)
        except tk.TclError:
            return
        if step + 1 < steps:
            top.after(25, lambda: self._animate_in(top, x, y_final, h, step=step + 1))

    def _fade_out(self, top: tk.Toplevel, alpha: float = 0.96) -> None:
        """Mờ dần rồi đóng."""
        try:
            if not top.winfo_exists():
                return
            if alpha <= 0.05:
                top.destroy()
                return
            top.attributes("-alpha", alpha)
        except tk.TclError:
            return
        top.after(30, lambda: self._fade_out(top, alpha - 0.12))


class ScheduleAlertBar:
    """Thanh thông báo bài sắp đăng / quá giờ trên tab Lịch đăng."""

    def __init__(
        self,
        parent: tk.Misc,
        root: tk.Misc,
        *,
        load_jobs: Callable[[], list[dict[str, Any]]],
        page_label: Callable[[str], str],
        account_label: Callable[[str], str],
        scheduler_running: Callable[[], bool],
        on_start_scheduler: Callable[[], None],
        on_show_missed: Callable[[list[str]], None],
        on_reschedule_missed: Callable[[list[str]], None],
        on_focus_job: Callable[[str], None],
    ) -> None:
        """Dựng widget; gọi ``start()`` để bắt đầu cập nhật."""
        self._root = root
        self._load_jobs = load_jobs
        self._page_label = page_label
        self._account_label = account_label
        self._scheduler_running = scheduler_running
        self._on_start_scheduler = on_start_scheduler
        self._on_show_missed = on_show_missed
        self._on_reschedule_missed = on_reschedule_missed
        self._on_focus_job = on_focus_job
        self._toast = ToastNotifier(root)
        self.snapshot = AlertSnapshot()
        self._busy = False
        self._frame_no = 0
        self._flash_until = 0.0
        self._last_head_id = ""
        self._notified_upcoming: set[str] = set()
        self._notified_missed: set[str] = set()
        self._refresh_after_id: str | None = None
        self.var_toast = tk.BooleanVar(value=True)

        self.frame = tk.Frame(parent, bg=_BG_IDLE, highlightthickness=1, highlightbackground="#d0d7de")
        # Nút bên phải phải pack trước để chữ dài bên trái không đẩy chúng ra ngoài.
        ttk.Checkbutton(self.frame, text="Thông báo nổi", variable=self.var_toast).pack(side=tk.RIGHT, padx=(4, 8))
        self._btn_resched = ttk.Button(self.frame, text="Dời giờ quá hạn…", command=self._click_reschedule)
        self._btn_resched.pack(side=tk.RIGHT, padx=(4, 0))
        self._btn_missed = tk.Button(
            self.frame, text="Quá hạn: 0", relief=tk.FLAT, bd=0, padx=10, pady=2,
            font=("Segoe UI", 9, "bold"), fg="white", bg="#8c959f", activeforeground="white",
            cursor="hand2", command=self._click_missed,
        )
        self._btn_missed.pack(side=tk.RIGHT, padx=(8, 0), pady=3)
        self._btn_start = ttk.Button(self.frame, text="Bật lịch", command=self._click_start)
        self._icon = tk.Label(self.frame, text="🔔", bg=_BG_IDLE, font=("Segoe UI Emoji", 13))
        self._icon.pack(side=tk.LEFT, padx=(8, 4), pady=3)
        self._main = tk.Label(
            self.frame, text="Đang đọc lịch đăng…", bg=_BG_IDLE, fg=_FG_MAIN,
            font=("Segoe UI", 10, "bold"), anchor="w", cursor="hand2",
        )
        self._main.pack(side=tk.LEFT)
        self._pill = tk.Label(self.frame, text="", bg=_BG_IDLE, fg=_FG_MAIN, font=("Segoe UI", 10, "bold"), padx=8)
        self._pill.pack(side=tk.LEFT, padx=(8, 8))
        self._next = tk.Label(self.frame, text="", bg=_BG_IDLE, fg=_FG_MUTED, font=("Segoe UI", 9), anchor="w")
        self._next.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self._main.bind("<Button-1>", lambda _e: self._focus_head())
        self._pill.bind("<Button-1>", lambda _e: self._focus_head())

    # ---------- vòng lặp ----------

    def start(self) -> None:
        """Bắt đầu đọc lịch định kỳ và chạy hiệu ứng."""
        self.refresh_now()
        self._root.after(_FRAME_MS, self._frame_loop)

    def refresh_now(self) -> None:
        """Đọc lại lịch ở luồng nền rồi cập nhật thanh (bỏ qua nếu đang đọc dở)."""
        if self._busy:
            return
        self._busy = True

        def _work() -> AlertSnapshot:
            return build_alert_snapshot(self._load_jobs())

        def _done(snap: AlertSnapshot) -> None:
            self._busy = False
            self._apply(snap)

        def _err(exc: BaseException) -> None:
            self._busy = False
            logger.debug("Thanh thông báo lịch: không đọc được lịch: {}", exc)
            self._schedule_refresh()

        run_background_then_main(self._root, _work, _done, on_error=_err)

    def _schedule_refresh(self) -> None:
        """Hẹn lần đọc lịch tiếp theo (chỉ giữ một hẹn giờ)."""
        try:
            if self._refresh_after_id is not None:
                self._root.after_cancel(self._refresh_after_id)
            self._refresh_after_id = self._root.after(_REFRESH_MS, self._refresh_tick)
        except tk.TclError:
            pass

    def _refresh_tick(self) -> None:
        """Một nhịp đọc lịch định kỳ."""
        self._refresh_after_id = None
        self.refresh_now()

    def _apply(self, snap: AlertSnapshot) -> None:
        """Nhận ảnh chụp mới: cập nhật chữ, bắn thông báo nổi cho bài mới sắp tới / mới quá hạn."""
        self.snapshot = snap
        head = snap.upcoming[0].job_id if snap.upcoming else ""
        if head != self._last_head_id:
            self._last_head_id = head
            self._flash_until = time.monotonic() + 1.6
        self._render_text()
        self._maybe_toast(snap)
        self._schedule_refresh()

    def _frame_loop(self) -> None:
        """Nhịp 100ms: hiệu ứng màu; mỗi giây cập nhật đếm ngược."""
        try:
            if not self.frame.winfo_exists():
                return
        except tk.TclError:
            return
        self._frame_no += 1
        if self._frame_no % 10 == 0:
            self._render_text()
            self._maybe_toast_due()
        if self.frame.winfo_ismapped():
            self._paint()
        self._root.after(_FRAME_MS, self._frame_loop)

    # ---------- hiển thị ----------

    def _post_line(self, p: UpcomingPost, now: datetime) -> str:
        """«18:07 · Tên Page — Tài khoản»."""
        page = self._page_label(p.page_id) or p.page_id or "?"
        acc = self._account_label(p.account_id)
        return f"{_local_hhmm(p.at, now)} · {page}" + (f" — {acc}" if acc else "")

    def _head_seconds(self, now: datetime) -> float | None:
        """Số giây tới bài kế tiếp (None nếu không có)."""
        if not self.snapshot.upcoming:
            return None
        return (self.snapshot.upcoming[0].at - now).total_seconds()

    def _render_text(self) -> None:
        """Cập nhật nội dung chữ trên thanh."""
        now = datetime.now(timezone.utc)
        snap = self.snapshot
        if snap.upcoming:
            head = snap.upcoming[0]
            verb = "Đang đăng" if head.status in {"running", "processing"} else "Sắp đăng"
            self._main.configure(text=f"{verb}: {self._post_line(head, now)}")
            self._pill.configure(text=format_countdown(head.at, now=now, status=head.status))
            rest = [self._post_line(p, now).split(" — ")[0] for p in snap.upcoming[1:4]]
            more = len(snap.upcoming) - 1 - len(rest)
            tail = "Tiếp: " + "  ·  ".join(rest) if rest else ""
            if more > 0:
                tail += f"  (+{more} bài)"
            if snap.running > 1:
                tail = f"{snap.running} bài đang đăng  ·  " + tail
            self._next.configure(text=tail)
        else:
            self._main.configure(text="Không có bài nào sắp đăng trong 24 giờ tới")
            self._pill.configure(text="")
            self._next.configure(text="")
        self._btn_missed.configure(text=f"Quá hạn chưa đăng: {snap.missed}")
        if snap.missed:
            self._btn_resched.state(["!disabled"])
        else:
            self._btn_resched.state(["disabled"])
        due = snap.missed > 0 or (self._head_seconds(now) or 1e9) <= _URGENT_SECONDS
        need_start = due and not self._safe_running()
        if need_start and not self._btn_start.winfo_ismapped():
            self._btn_start.pack(side=tk.RIGHT, padx=(8, 0), before=self._btn_missed)
        elif not need_start and self._btn_start.winfo_ismapped():
            self._btn_start.pack_forget()
        if need_start:
            self._next.configure(text="⚠ Lịch đăng đang TẮT — bấm «Bật lịch» để chạy.  " + self._next.cget("text"))

    def _paint(self) -> None:
        """Tô màu theo trạng thái: nhấp nháy khi sắp tới giờ, xanh khi đang đăng, chớp khi đổi bài."""
        now = datetime.now(timezone.utc)
        snap = self.snapshot
        phase = (math.sin(self._frame_no * _FRAME_MS / 1000 * math.pi * 1.6) + 1) / 2
        head = snap.upcoming[0] if snap.upcoming else None
        secs = self._head_seconds(now)
        if head is not None and head.status in {"running", "processing"}:
            bg = _BG_RUNNING
            pill_bg = _mix("#1a7f37", "#2da44e", phase)
            pill_fg = "white"
        elif secs is not None and secs <= _URGENT_SECONDS:
            bg = _mix(_BG_URGENT_A, _BG_URGENT_B, phase * 0.6)
            pill_bg = _mix("#bf8700", "#d4a72c", phase)
            pill_fg = "white"
        else:
            bg = _BG_IDLE
            pill_bg = "#ddf4ff" if head is not None else _BG_IDLE
            pill_fg = "#0969da"
        left = self._flash_until - time.monotonic()
        if left > 0:
            bg = _mix(bg, _BG_FLASH, min(1.0, left / 1.6))
        for w in (self.frame, self._icon, self._main, self._next):
            if w.cget("bg") != bg:
                w.configure(bg=bg)
        if self._pill.cget("bg") != pill_bg:
            self._pill.configure(bg=pill_bg, fg=pill_fg)
        if snap.missed:
            red = _mix(_RED_A, _RED_B, phase)
            self._btn_missed.configure(bg=red, activebackground=_RED_A)
        elif self._btn_missed.cget("bg") != "#8c959f":
            self._btn_missed.configure(bg="#8c959f", activebackground="#8c959f")
        if secs is not None and secs <= _URGENT_SECONDS and head is not None and head.status == "pending":
            self._icon.configure(text="🔔" if self._frame_no % 6 < 3 else "🔕")
        elif self._icon.cget("text") != "🔔":
            self._icon.configure(text="🔔")

    # ---------- thông báo nổi ----------

    def _maybe_toast(self, snap: AlertSnapshot) -> None:
        """Báo bài mới quá giờ (lần đầu mở app chỉ báo tổng, không báo từng bài)."""
        new_missed = [j for j in snap.missed_ids if j not in self._notified_missed]
        self._notified_missed.update(snap.missed_ids)
        if new_missed and self.var_toast.get():
            title = f"⚠ {len(new_missed)} bài quá giờ chưa đăng"
            body = "Bấm để xem và xóa hoặc đổi giờ đăng."
            if not self._safe_running():
                body = "Lịch đăng đang tắt. " + body
            self._toast.show(title, body, accent=_RED_A, on_click=lambda ids=list(snap.missed_ids): self._open_missed(ids))
        self._maybe_toast_due()

    def notify_outcome(self, ok: bool, title: str, body: str, *, job_id: str = "") -> None:
        """Thông báo nổi khi một bài vừa đăng xong hoặc lỗi (tab Lịch đăng đang mở)."""
        if not self.var_toast.get():
            return
        accent = "#1a7f37" if ok else _RED_A
        focus = (lambda jid=job_id: self._focus(jid)) if job_id else None
        self._toast.show(title, body, accent=accent, on_click=focus)

    def _maybe_toast_due(self) -> None:
        """Báo bài sẽ đăng trong 5 phút tới (mỗi bài một lần)."""
        if not self.var_toast.get():
            return
        now = datetime.now(timezone.utc)
        due = [
            p for p in self.snapshot.upcoming
            if p.status == "pending"
            and p.job_id not in self._notified_upcoming
            and 0 <= (p.at - now).total_seconds() <= _TOAST_LEAD_SECONDS
        ]
        if not due:
            return
        self._notified_upcoming.update(p.job_id for p in due)
        head = due[0]
        title = f"🔔 Sắp đăng lúc {_local_hhmm(head.at, now)} ({format_countdown(head.at, now=now)})"
        lines = [self._post_line(p, now) for p in due[:4]]
        if len(due) > 4:
            lines.append(f"… và {len(due) - 4} bài khác")
        self._toast.show(title, "\n".join(lines), accent="#bf8700", on_click=lambda jid=head.job_id: self._focus(jid))

    # ---------- thao tác ----------

    def _safe_running(self) -> bool:
        """Lịch đăng có đang bật không (lỗi coi như đang bật để khỏi báo nhầm)."""
        try:
            return bool(self._scheduler_running())
        except Exception:  # noqa: BLE001
            return True

    def _raise_root(self) -> None:
        """Đưa cửa sổ chính lên nếu đang thu nhỏ."""
        try:
            if self._root.state() == "iconic":
                self._root.deiconify()
            self._root.lift()
        except tk.TclError:
            pass

    def _focus(self, job_id: str) -> None:
        """Mở cửa sổ và chọn job trên bảng."""
        self._raise_root()
        self._on_focus_job(job_id)

    def _focus_head(self) -> None:
        """Bấm vào dòng chính: chọn bài kế tiếp trên bảng."""
        if self.snapshot.upcoming:
            self._on_focus_job(self.snapshot.upcoming[0].job_id)

    def _open_missed(self, ids: list[str]) -> None:
        """Mở cửa sổ và lọc bài quá hạn."""
        self._raise_root()
        self._on_show_missed(ids)

    def _click_missed(self) -> None:
        """Nút «Quá hạn»: lọc bảng chỉ còn bài quá giờ chưa đăng."""
        self._on_show_missed(list(self.snapshot.missed_ids))

    def _click_reschedule(self) -> None:
        """Nút «Dời giờ quá hạn…»."""
        if self.snapshot.missed_ids:
            self._on_reschedule_missed(list(self.snapshot.missed_ids))

    def _click_start(self) -> None:
        """Nút «Bật lịch»."""
        self._on_start_scheduler()
        self._root.after(800, self._render_text)
