"""Tách audio từ file video bằng FFmpeg."""

from __future__ import annotations

import subprocess
from pathlib import Path


class AudioExtractor:
    def extract_audio(
        self,
        video_path: str,
        output_path: str,
        *,
        ffmpeg_bin: str,
        fmt: str = "aac",
    ) -> str:
        """
        fmt: aac (copy nếu có thể), mp3 (libmp3lame), wav
        """
        ff = Path(ffmpeg_bin).resolve()
        inp = Path(video_path).expanduser().resolve()
        out = Path(output_path).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        fmt_l = (fmt or "aac").lower().strip()
        cmd: list[str] = [str(ff), "-y", "-i", str(inp), "-vn"]
        if fmt_l == "mp3":
            cmd.extend(["-codec:a", "libmp3lame", "-q:a", "2", str(out)])
        elif fmt_l in ("aac", "m4a", "copy"):
            cmd.extend(["-acodec", "copy", str(out)])
        elif fmt_l == "wav":
            cmd.extend(["-acodec", "pcm_s16le", str(out)])
        else:
            cmd.extend(["-acodec", "aac", "-b:a", "192k", str(out)])
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=600)
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or proc.stdout or "")[-1200:])
        return str(out.resolve())
