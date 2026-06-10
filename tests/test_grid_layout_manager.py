"""Tests Grid Layout Manager."""

from __future__ import annotations

from src.utils.grid_layout_manager import GridWindowSlot, _grid_dimensions, compute_grid_layout


def test_grid_dimensions_four_threads() -> None:
    cols, rows = _grid_dimensions(4)
    assert cols == 4
    assert rows == 1


def test_compute_grid_layout_positions() -> None:
    slots = compute_grid_layout(4, screen_width=1920, screen_height=1080)
    assert len(slots) == 4
    assert slots[0].height == 780
    assert slots[0].col == 0 and slots[0].row == 0
    # Neo góc trên-trái (margin mặc định 8px).
    assert slots[0].x == 8
    assert slots[0].y == 8
    assert slots[1].x == slots[0].x + slots[0].width
    assert slots[3].x == slots[0].x + 3 * slots[0].width


def test_compute_grid_layout_eight_threads_four_cols() -> None:
    cols, rows = _grid_dimensions(8, max_cols=4)
    assert cols == 4
    assert rows == 2
    slots = compute_grid_layout(8, screen_width=1920, screen_height=1080, max_cols=4)
    assert len(slots) == 8
    assert 400 <= slots[0].height <= 780
    assert slots[0].y == 8
    assert slots[4].y == 8 + slots[0].height
    assert slots[4].x == slots[0].x
