"""Độ dài timeline từ source × speed — khớp FFmpeg."""

from __future__ import annotations

from src.services.video_editor.timeline_manager import (
    reconcile_clip_duration_from_source,
    timeline_duration_from_source,
)


def test_timeline_duration_from_source_with_speed() -> None:
    assert timeline_duration_from_source(0.0, 10.6, 1.06) == round(10.6 / 1.06, 4)


def test_reconcile_clip_duration_video() -> None:
    clip = {
        "type": "video",
        "source_start": 0.2,
        "source_end": 13.406893,
        "speed": 1.06,
        "duration": 99.0,
    }
    reconcile_clip_duration_from_source(clip)
    assert clip["duration"] == timeline_duration_from_source(0.2, 13.406893, 1.06)
