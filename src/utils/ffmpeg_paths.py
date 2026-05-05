"""
Đường dẫn ffmpeg/ffprobe dùng chung (PATH hoặc tools/ffmpeg/bin portable).
Tránh trùng logic giữa GUI lịch đăng, AI Video thumbnail, v.v.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from src.utils.paths import project_root


def portable_ffmpeg_bin_dir() -> Path:
    return project_root() / "tools" / "ffmpeg" / "bin"


def resolve_ffmpeg_ffprobe_paths() -> tuple[str | None, str | None]:
    """Ưu tiên ffmpeg+ffprobe trên PATH; nếu thiếu một trong hai thì thử bản portable trong repo."""
    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    if ffmpeg and ffprobe:
        return ffmpeg, ffprobe
    exe_ffmpeg = "ffmpeg.exe" if os.name == "nt" else "ffmpeg"
    exe_ffprobe = "ffprobe.exe" if os.name == "nt" else "ffprobe"
    pff = portable_ffmpeg_bin_dir() / exe_ffmpeg
    pfp = portable_ffmpeg_bin_dir() / exe_ffprobe
    if pff.is_file() and pfp.is_file():
        return str(pff), str(pfp)
    return ffmpeg, ffprobe


def resolve_ffmpeg_executable() -> str | None:
    """Chỉ cần ffmpeg (thumbnail, transcode đơn giản)."""
    ff, _ = resolve_ffmpeg_ffprobe_paths()
    return ff


def resolve_ffplay_executable() -> str | None:
    """ffplay (PATH hoặc cạnh ffmpeg trong tools/ffmpeg/bin)."""
    fp = shutil.which("ffplay")
    if fp:
        return fp
    exe = "ffplay.exe" if os.name == "nt" else "ffplay"
    p = portable_ffmpeg_bin_dir() / exe
    return str(p) if p.is_file() else None
