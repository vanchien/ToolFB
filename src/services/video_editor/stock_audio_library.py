"""Thư mục âm thanh có sẵn (stock) — copy file .mp3/.wav/… vào data/video_editor/stock_audio/."""

from __future__ import annotations

from pathlib import Path

from src.services.video_editor.layout import ensure_video_editor_layout

_STOCK_AUDIO_EXTS = frozenset({".mp3", ".wav", ".m4a", ".aac", ".ogg", ".flac", ".opus", ".wma"})


def stock_audio_dir(paths: dict[str, Path] | None = None) -> Path:
    p = (paths or ensure_video_editor_layout())["stock_audio"]
    p.mkdir(parents=True, exist_ok=True)
    return p


def list_stock_audio_paths(paths: dict[str, Path] | None = None) -> list[Path]:
    d = stock_audio_dir(paths)
    files = [p for p in d.iterdir() if p.is_file() and p.suffix.lower() in _STOCK_AUDIO_EXTS]
    return sorted(files, key=lambda x: x.name.lower())


def stock_audio_dir_display_hint(paths: dict[str, Path] | None = None) -> str:
    return str(stock_audio_dir(paths).resolve())
