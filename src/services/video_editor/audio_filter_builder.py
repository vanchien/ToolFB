"""Chuỗi filter audio: volume (kể cả mute), fade — tốc độ vẫn dùng SpeedManager ở FFmpegCommandBuilder."""

from __future__ import annotations

from typing import Any


class AudioFilterBuilder:
    def build_volume_fade_filters(self, clip: dict[str, Any], duration: float) -> str:
        """
        Phần sau atrim (trước aresample/atempo tùy thứ tự gộp ở builder chính).
        """
        parts: list[str] = []
        if clip.get("muted"):
            parts.append("volume=0")
        else:
            try:
                vol = float(clip.get("volume") if clip.get("volume") is not None else 1.0)
            except (TypeError, ValueError):
                vol = 1.0
            parts.append(f"volume={vol}")
        fi = float(clip.get("fade_in") or 0)
        fo = float(clip.get("fade_out") or 0)
        du = float(duration)
        if fi > 0:
            parts.append(f"afade=t=in:st=0:d={fi}")
        if fo > 0 and du > fo:
            st_out = max(0.0, du - fo)
            parts.append(f"afade=t=out:st={st_out}:d={fo}")
        return ",".join(parts)
