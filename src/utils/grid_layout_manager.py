"""
Thuật toán chia lưới cửa sổ trình duyệt (Grid Layout Manager).

Tính viewport và tọa độ (X, Y) cho từng luồng trước khi khởi chạy Playwright.
"""

from __future__ import annotations

import math
import os
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


def get_screen_resolution(*, fallback_width: int = 1920, fallback_height: int = 1080) -> tuple[int, int]:
    """
    Lấy độ phân giải màn hình chính (động trên Windows qua Tk, fallback cố định).

    Returns:
        ``(width, height)`` pixel.
    """
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

    Mặc định tối đa ``max_cols`` cột (Full HD: 4 cột × ceil(N/4) hàng).
  Ví dụ: N=4 → ``(4, 1)``; N=8 → ``(4, 2)``.
    """
    n = max(1, int(thread_count))
    cols = max(1, min(int(max_cols), n))
    rows = int(math.ceil(n / cols))
    return cols, rows


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
    """Chiều cao mỗi ô — giới hạn để cửa sổ gọn, không full màn 1440px."""
    per_row = max(400, int(wh) // max(1, rows))
    cap = _env_int("FB_GRID_MAX_CELL_HEIGHT", 780)
    if cap <= 0:
        return per_row
    return min(per_row, cap)


def compute_grid_layout(
    thread_count: int,
    *,
    screen_width: int | None = None,
    screen_height: int | None = None,
    max_cols: int | None = None,
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

    cols, rows = _grid_dimensions(n, max_cols=mc)
    cell_w = max(320, ww // cols)
    cell_h = _cell_height(wh, rows)
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
            )
        )
    return slots
