"""Cắt nguồn khớp timeline; vị trí logo theo % khung."""

from __future__ import annotations

from src.services.video_editor.overlay_utils import logo_corner_xy_from_label
from src.services.video_editor.timeline_manager import effective_source_span


def test_effective_source_span_clamps_long_source_end() -> None:
    clip = {
        "type": "video",
        "source_start": 0.0,
        "source_end": 120.0,
        "duration": 10.0,
        "speed": 1.0,
    }
    ss, se = effective_source_span(clip, media_duration=120.0)
    assert se <= ss + 10.0 + 0.05
    assert se > ss + 9.9


def test_logo_corner_uses_percent_margin() -> None:
    x1, y1 = logo_corner_xy_from_label("Trái trên", 1080, 1920, 100, 50, margin_x_ratio=0.1, margin_y_ratio=0.1)
    x2, y2 = logo_corner_xy_from_label("Trái trên", 1080, 1920, 200, 100, margin_x_ratio=0.1, margin_y_ratio=0.1)
    assert x1 == x2 == 108
    assert y1 == y2 == 192
