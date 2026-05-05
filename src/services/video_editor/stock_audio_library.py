"""Thư mục âm thanh có sẵn (stock) — copy file .mp3/.wav/… vào data/video_editor/stock_audio/."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Sequence

from src.services.video_editor.layout import ensure_video_editor_layout
from src.services.video_editor.remote_stock_audio import FREE_AUDIO_TOPIC_QUERIES

_STOCK_AUDIO_EXTS = frozenset({".mp3", ".wav", ".m4a", ".aac", ".ogg", ".flac", ".opus", ".wma"})

# Hiển thị trong combobox lọc thư viện cục bộ
STOCK_TOPIC_FILTER_ALL = "Tất cả"

# Từ khóa thêm cho tên file (stem) — bổ sung ngoài chuỗi tìm kiếm tiếng Anh của từng chủ đề
_STOCK_TOPIC_SYNONYMS: dict[str, frozenset[str]] = {
    "Ambient / thư giãn": frozenset({"ambience", "atmospheric", "atmosphere", "grey", "gray", "darkgrey", "pad"}),
    "Vlog / acoustic vui": frozenset({"vlog", "ukulele", "pluck", "uplift"}),
    "Corporate / năng động": frozenset({"office", "presentation", "xd250"}),
    "Cinematic / hùng tráng": frozenset({"score", "film", "movie", "trailer"}),
    "Nature / thiên nhiên": frozenset({"rain", "thunder", "sea", "river", "wind"}),
    "Electronic / synth chill": frozenset({"edm", "house", "techno", "synthwave"}),
    "Lofi / beat nhẹ": frozenset({"lo-fi", "study", "chillhop"}),
    "Động lực / workout": frozenset({"sport", "gym", "running", "power"}),
    "Hài hước / quirky": frozenset({"comedy", "cartoon", "silly", "game"}),
    "Hùng vĩ / drone": frozenset({"tension", "suspense", "horror"}),
    "Percussion / trống & rhythm": frozenset({"drum", "drums", "perc", "stick", "hit", "snare", "kick"}),
    "Bass / sub & low end": frozenset({"sub", "808", "lowend", "subwoofer"}),
    "Piano / keyboard nhẹ": frozenset({"keys", "grand", "felt"}),
    "Jazz / lounge": frozenset({"swing", "bebop", "bossa"}),
    "Hip-hop / trap beat": frozenset({"808", "hihat", "cypher"}),
    "Strings / orchestra": frozenset({"viola", "ensemble", "symphony"}),
    "Stems / stem & mix": frozenset({"stem", "stems", "multitrack", "mixdown", "full_mix", "bus", "master"}),
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


def _filter_tokens_for_topic(topic_label: str) -> frozenset[str]:
    label = (topic_label or "").strip()
    qmap = dict(FREE_AUDIO_TOPIC_QUERIES)
    q = qmap.get(label, "")
    tokens = {t for t in re.split(r"\W+", q.lower()) if len(t) >= 3}
    extra = _STOCK_TOPIC_SYNONYMS.get(label, frozenset())
    tokens |= {x.lower() for x in extra if len(x) >= 2}
    return frozenset(tokens)


def filter_stock_paths_by_topic(paths: Sequence[Path], topic_label: str) -> list[Path]:
    """
    Lọc file stock theo chủ đề: khớp nếu **bất kỳ** từ khóa nào có trong tên file (stem + tên đầy đủ).
    ``topic_label`` là nhãn tiếng Việt giống combobox «Chủ đề» tìm kiếm remote, hoặc ``STOCK_TOPIC_FILTER_ALL``.
    """
    label = (topic_label or "").strip()
    if not label or label == STOCK_TOPIC_FILTER_ALL:
        return list(paths)
    tokens = _filter_tokens_for_topic(label)
    if not tokens:
        return list(paths)
    out: list[Path] = []
    for p in paths:
        hay = f"{p.stem} {p.name}".lower()
        if any(tok in hay for tok in tokens):
            out.append(p)
    return out
