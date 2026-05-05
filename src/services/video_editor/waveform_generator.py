"""Tạo ảnh waveform bằng FFmpeg showwavespic (cần file có luồng audio: mp3, wav, hoặc video có tiếng — không dùng cho ảnh tĩnh)."""

from __future__ import annotations

import subprocess
from pathlib import Path


class WaveformGenerator:
    def generate_waveform(self, media_path: str, output_path: str, *, ffmpeg_bin: str, size: str = "1200x120") -> str:
        ff = Path(ffmpeg_bin).resolve()
        inp = Path(media_path).expanduser().resolve()
        out = Path(output_path).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            str(ff),
            "-y",
            "-i",
            str(inp),
            "-filter_complex",
            f"aformat=channel_layouts=mono,showwavespic=s={size}:colors=white",
            "-frames:v",
            "1",
            str(out.resolve()),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=180)
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or proc.stdout or "")[-800:])
        return str(out.resolve())
