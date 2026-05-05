"""Tốc độ clip — setpts / atempo."""

from __future__ import annotations

from typing import Any


class SpeedManager:
    def set_speed(self, project: dict[str, Any], clip_id: str, speed: float) -> dict[str, Any]:
        sp = float(speed)
        if sp <= 0:
            raise ValueError("speed phải > 0")
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict):
                continue
            for cl in tr.get("clips") or []:
                if isinstance(cl, dict) and str(cl.get("id")) == str(clip_id):
                    cl["speed"] = sp
                    return project
        raise ValueError("Không tìm thấy clip")

    def build_speed_filter(self, speed: float) -> tuple[str, str]:
        """
        Trả về (vf_fragment, af_chain) áp sau trim.
        vf: setpts=PTS/S
        af: chain atempo trong [0.5,2] lặp nếu cần.
        """
        s = float(speed)
        if abs(s - 1.0) < 1e-6:
            return "", ""
        vf = f"setpts=PTS/{s}"
        af = self._atempo_chain(s)
        return vf, af

    def _atempo_chain(self, speed: float) -> str:
        """Chuỗi atempo (librosa-style chaining)."""
        s = float(speed)
        parts: list[str] = []
        rem = s
        while rem > 2.0 + 1e-6:
            parts.append("atempo=2.0")
            rem /= 2.0
        while rem < 0.5 - 1e-6:
            parts.append("atempo=0.5")
            rem /= 0.5
        parts.append(f"atempo={rem:.6f}")
        return ",".join(parts)
