"""Zoom sau bước vào khung — CanvasFilterBuilder."""

from __future__ import annotations

from src.services.video_editor.canvas_filter_builder import CanvasFilterBuilder


def test_zoom_vf_one_returns_empty() -> None:
    b = CanvasFilterBuilder()
    clip = {"zoom": 1.0}
    assert b.build_canvas_zoom_vf(clip, 1080, 1920) == ""


def test_zoom_in_contains_scale_and_crop() -> None:
    b = CanvasFilterBuilder()
    clip = {"zoom": 1.5}
    s = b.build_canvas_zoom_vf(clip, 1080, 1920)
    assert "scale=" in s and "crop=1080:1920" in s and "increase" in s


def test_zoom_out_contains_pad() -> None:
    b = CanvasFilterBuilder()
    clip = {"zoom": 0.75}
    s = b.build_canvas_zoom_vf(clip, 1080, 1920)
    assert "pad=1080:1920" in s and "decrease" in s
