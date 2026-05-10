"""Thư mục âm thanh có sẵn (stock) — copy file .mp3/.wav/… vào data/video_editor/stock_audio/."""

from __future__ import annotations

from pathlib import Path

from src.services.video_editor.layout import ensure_video_editor_layout

_STOCK_AUDIO_EXTS = frozenset({".mp3", ".wav", ".m4a", ".aac", ".ogg", ".flac", ".opus", ".wma"})
STOCK_TOPIC_FILTER_ALL = "Tất cả"
_TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "Trống / Beat": ("drum", "beat", "percussion", "kick", "snare", "loop"),
    "Piano": ("piano", "keys", "keyboard"),
    "Điện tử": ("edm", "electro", "electronic", "synth", "house", "techno"),
    "Chill / Lo-fi": ("chill", "lofi", "lo-fi", "ambient", "downtempo"),
    "Cinematic": ("cinematic", "epic", "trailer", "orchestral", "score"),
    "Vui tươi": ("happy", "fun", "upbeat", "bright"),
    "Buồn / Nhẹ": ("sad", "soft", "piano solo", "mellow"),
}


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


def stock_topic_filter_labels() -> list[str]:
    return [STOCK_TOPIC_FILTER_ALL, *_TOPIC_KEYWORDS.keys()]


def filter_stock_paths_by_topic(paths: list[Path], topic_label: str) -> list[Path]:
    topic = str(topic_label or "").strip()
    if not topic or topic == STOCK_TOPIC_FILTER_ALL:
        return list(paths)
    keys = _TOPIC_KEYWORDS.get(topic)
    if not keys:
        return list(paths)
    out: list[Path] = []
    for p in paths:
        name = p.name.lower()
        if any(k in name for k in keys):
            out.append(p)
    return out
