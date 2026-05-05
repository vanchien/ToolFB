"""Đảm bảo thư mục data/video_editor/* tồn tại."""

from __future__ import annotations

from pathlib import Path

from src.utils.paths import project_root


def ensure_video_editor_layout() -> dict[str, Path]:
    root = project_root() / "data" / "video_editor"
    paths = {
        "root": root,
        "projects": root / "projects",
        "media": root / "media",
        "stock_audio": root / "stock_audio",
        "temp": root / "temp",
        "renders": root / "renders",
        "thumbnails": root / "thumbnails",
        "waveforms": root / "waveforms",
        "subtitles": root / "subtitles",
        "presets": root / "presets",
        "templates": root / "templates",
        "logs": root / "logs",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths
