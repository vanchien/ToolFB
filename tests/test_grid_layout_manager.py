"""Tests Grid Layout Manager."""

from __future__ import annotations

from src.utils.grid_layout_manager import GridWindowSlot, _grid_dimensions, compute_grid_layout


def test_fit_slot_uses_client_work_area() -> None:
    from src.utils.grid_layout_manager import fit_slot_to_work_area

    slots = compute_grid_layout(4, screen_width=1920, screen_height=1080)
    # Máy khách 1366×768, vùng làm việc trừ taskbar.
    fitted = [fit_slot_to_work_area(slot, (0, 0, 1366, 728)) for slot in slots]
    assert fitted[0] == (0, 0, 683, 364)
    assert fitted[1][0] == 683
    assert fitted[2][1] == 364
    assert fitted[0][2] == fitted[1][2]
    assert fitted[1][0] + fitted[1][2] <= 1366
    assert fitted[2][1] + fitted[2][3] <= 728
    assert slots[0].cols == 2 and slots[0].rows == 2
    cols, rows = _grid_dimensions(4)
    assert (cols, rows) == (2, 2)


def test_compute_grid_layout_positions() -> None:
    slots = compute_grid_layout(4, screen_width=1920, screen_height=1080)
    assert len(slots) == 4
    assert slots[0].col == 0 and slots[0].row == 0
    assert slots[1].col == 1 and slots[1].row == 0
    assert slots[2].col == 0 and slots[2].row == 1
    assert slots[3].col == 1 and slots[3].row == 1
    assert slots[0].width == slots[1].width == slots[2].width == slots[3].width
    assert slots[0].height == slots[1].height == slots[2].height == slots[3].height
    # Neo góc trên-trái (margin mặc định 8px).
    assert slots[0].x == 8
    assert slots[0].y == 8
    assert slots[1].x == slots[0].x + slots[0].width
    assert slots[2].y == slots[0].y + slots[0].height
    assert slots[3].x == slots[1].x
    assert slots[3].y == slots[2].y


def test_six_by_three_is_eighteen_small_windows() -> None:
    big = compute_grid_layout(4, screen_width=1920, screen_height=1080)
    slots = compute_grid_layout(18, screen_width=1920, screen_height=1080, max_cols=6, rows=3)
    assert len(slots) == 18
    assert slots[0].col == 0 and slots[0].row == 0
    assert slots[5].col == 5 and slots[5].row == 0
    assert slots[6].col == 0 and slots[6].row == 1
    assert slots[17].col == 5 and slots[17].row == 2
    assert slots[0].width < big[0].width
    assert slots[0].height < big[0].height
    assert slots[5].x + slots[5].width <= 1920
    assert slots[17].y + slots[17].height <= 1080


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
