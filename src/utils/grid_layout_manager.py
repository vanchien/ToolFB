"""
Thuật toán chia lưới cửa sổ trình duyệt (Grid Layout Manager).

Tính viewport và tọa độ (X, Y) cho từng luồng trước khi khởi chạy Playwright.
"""

from __future__ import annotations

import contextlib
import math
import os
from collections.abc import Iterator
from dataclasses import dataclass


@dataclass(frozen=True)
class GridWindowSlot:
    """Một ô lưới: kích thước và vị trí cửa sổ."""

    index: int
    x: int
    y: int
    width: int
    height: int
    col: int
    row: int
    cols: int = 0
    rows: int = 0


def get_screen_resolution(*, fallback_width: int = 1920, fallback_height: int = 1080) -> tuple[int, int]:
    """
    Lấy độ phân giải màn hình chính.

    Trên Windows dùng vùng làm việc Win32 (trừ taskbar), cùng hệ tọa độ với
    ``SetWindowPos``, để máy khách 125%/150% không bị lệch ô.
    """
    if os.name == "nt":
        area = win32_work_area()
        if area is not None:
            _x, _y, w, h = area
            if w > 0 and h > 0:
                return w, h
    try:
        import tkinter as tk

        root = tk.Tk()
        root.withdraw()
        w = int(root.winfo_screenwidth())
        h = int(root.winfo_screenheight())
        root.destroy()
        if w > 0 and h > 0:
            return w, h
    except Exception:
        pass
    return fallback_width, fallback_height


@contextlib.contextmanager
def win32_dpi_aware_thread() -> Iterator[None]:
    """
    Tạm cho luồng hiện tại dùng tọa độ vật lý (PER_MONITOR_AWARE_V2) rồi trả lại như cũ.

    Không đổi chế độ DPI của cả process: nếu đổi, Windows thôi phóng giao diện Tk
    và cửa sổ ToolFB bị thu nhỏ trên màn hình 125%/150%/200%.
    """
    if os.name != "nt":
        yield
        return
    previous = None
    user32 = None
    try:
        import ctypes

        user32 = ctypes.windll.user32  # type: ignore[attr-defined]
        user32.SetThreadDpiAwarenessContext.restype = ctypes.c_void_p
        user32.SetThreadDpiAwarenessContext.argtypes = [ctypes.c_void_p]
        previous = user32.SetThreadDpiAwarenessContext(ctypes.c_void_p(-4))
    except Exception:
        previous = None
    try:
        yield
    finally:
        if previous and user32 is not None:
            try:
                user32.SetThreadDpiAwarenessContext(previous)
            except Exception:
                pass


def win32_work_area() -> tuple[int, int, int, int] | None:
    """``(x, y, width, height)`` vùng làm việc màn hình chính, đã trừ taskbar."""
    if os.name != "nt":
        return None
    try:
        import ctypes
        from ctypes import wintypes

        user32 = ctypes.windll.user32  # type: ignore[attr-defined]
        rect = wintypes.RECT()
        spi_getworkarea = 0x0030
        with win32_dpi_aware_thread():
            ok = user32.SystemParametersInfoW(spi_getworkarea, 0, ctypes.byref(rect), 0)
        if not ok:
            return None
        width = int(rect.right - rect.left)
        height = int(rect.bottom - rect.top)
        if width < 320 or height < 240:
            return None
        return int(rect.left), int(rect.top), width, height
    except Exception:
        return None


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def _grid_dimensions(thread_count: int, *, max_cols: int = 4) -> tuple[int, int]:
    """
    Số cột × hàng cho ``thread_count`` luồng.

    Chọn cách chia đều trong giới hạn ``max_cols``. Ví dụ 4 luồng → 2×2, 8 luồng → 4×2.
    """
    n = max(1, int(thread_count))
    cap = max(1, min(int(max_cols), n))
    best_cols = 1
    best_score: tuple[int, int, int] | None = None
    for cols in range(1, cap + 1):
        rows = int(math.ceil(n / cols))
        empty = cols * rows - n
        score = (empty, abs(cols - rows), -cols)
        if best_score is None or score < best_score:
            best_score = score
            best_cols = cols
    return best_cols, int(math.ceil(n / best_cols))


def grid_work_area(
    *,
    screen_width: int | None = None,
    screen_height: int | None = None,
) -> tuple[int, int, int, int]:
    """
    Vùng làm việc cho lưới: ``(origin_x, origin_y, width, height)``.

    Dùng ``FB_GRID_ORIGIN_X/Y`` và ``FB_GRID_WORK_WIDTH/HEIGHT`` để dời lưới sang
    màn hình phụ (ví dụ origin_x=1920) — tránh che desktop chính của người dùng.
    """
    if screen_width is None or screen_height is None:
        sw, sh = get_screen_resolution()
    else:
        sw, sh = int(screen_width), int(screen_height)
    # Mặc định: góc trên-trái màn hình chính (không căn giữa, không dời sang màn phụ).
    ox = _env_int("FB_GRID_ORIGIN_X", 0)
    oy = _env_int("FB_GRID_ORIGIN_Y", 0)
    ox += max(0, _env_int("FB_GRID_MARGIN_LEFT", 8))
    oy += max(0, _env_int("FB_GRID_MARGIN_TOP", 8))
    ww = _env_int("FB_GRID_WORK_WIDTH", 0) or max(800, sw - ox)
    margin_bottom = max(0, _env_int("FB_GRID_MARGIN_BOTTOM", 48))
    wh = _env_int("FB_GRID_WORK_HEIGHT", 0) or max(600, sh - oy)
    wh = max(400, wh - margin_bottom)
    return ox, oy, max(320, ww), wh


def _cell_height(wh: int, rows: int) -> int:
    """Chiều cao mỗi ô. Chia hết chiều cao màn hình để các hàng không chồng."""
    return max(1, int(wh) // max(1, rows))


def fit_slot_to_work_area(
    slot: GridWindowSlot,
    work: tuple[int, int, int, int],
) -> tuple[int, int, int, int]:
    """Tính lại ô theo vùng làm việc thật của máy khách.

    ``cols``/``rows`` lấy từ lúc chia luồng. Mỗi máy có độ phân giải khác nhau
    vẫn ra lưới kín, không chồng.
    """
    cols = max(0, int(getattr(slot, "cols", 0) or 0))
    rows = max(0, int(getattr(slot, "rows", 0) or 0))
    if cols < 1 or rows < 1:
        return int(slot.x), int(slot.y), int(slot.width), int(slot.height)
    ox, oy, ww, wh = (int(work[0]), int(work[1]), int(work[2]), int(work[3]))
    cell_w = max(1, ww // cols)
    cell_h = max(1, wh // rows)
    return ox + int(slot.col) * cell_w, oy + int(slot.row) * cell_h, cell_w, cell_h


def compute_grid_layout(
    thread_count: int,
    *,
    screen_width: int | None = None,
    screen_height: int | None = None,
    max_cols: int | None = None,
    rows: int | None = None,
) -> list[GridWindowSlot]:
    """
    Tính lưới cửa sổ cho ``thread_count`` luồng chạy đồng thời.

    Công thức (theo thiết kế):
    - ``width = screen_width / cols``
    - ``height = screen_height / rows``
    - ``x = (i % cols) * width``, ``y = (i // cols) * height``

    Args:
        thread_count: Số luồng tối đa (N).
        screen_width: Chiều ngang màn hình; None → đo tự động.
        screen_height: Chiều dọc màn hình; None → đo tự động.

    Returns:
        Danh sách slot theo index 0..thread_count-1.
    """
    n = max(1, int(thread_count))
    mc = max_cols if max_cols is not None else _env_int("FB_GRID_MAX_COLS", 4)
    ox, oy, ww, wh = grid_work_area(screen_width=screen_width, screen_height=screen_height)

    forced_rows = int(rows) if rows else 0
    if forced_rows > 0:
        cols = max(1, min(int(mc), n))
        row_count = forced_rows
        if n > cols * row_count:
            row_count = int(math.ceil(n / cols))
    else:
        cols, row_count = _grid_dimensions(n, max_cols=mc)
    cell_w = max(1, ww // cols)
    cell_h = _cell_height(wh, row_count)
    # Neo lưới sát góc trên-trái (ox/oy đã gồm margin trong grid_work_area).
    y0 = oy

    slots: list[GridWindowSlot] = []
    for i in range(n):
        col = i % cols
        row = i // cols
        slots.append(
            GridWindowSlot(
                index=i,
                x=ox + col * cell_w,
                y=y0 + row * cell_h,
                width=cell_w,
                height=cell_h,
                col=col,
                row=row,
                cols=cols,
                rows=row_count,
            )
        )
    return slots
