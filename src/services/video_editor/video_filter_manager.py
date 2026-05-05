"""Filter màu preset → chuỗi FFmpeg eq/hue/curves."""

from __future__ import annotations

from typing import Any


class VideoFilterManager:
    PRESETS: dict[str, str] = {
        "normal": "",
        "warm": "eq=contrast=1.02:saturation=1.08:gamma=1.05",
        "cool": "eq=contrast=1.02:saturation=1.08:gamma=0.98",
        "cinematic": "eq=contrast=1.08:saturation=0.92:brightness=0.02,gamma=1.1",
        "black_white": "hue=s=0",
        "high_contrast": "eq=contrast=1.25:saturation=1.05",
        "vintage": "eq=contrast=1.05:saturation=0.75:gamma=1.08",
    }

    def build_ffmpeg_filter(self, filter_config: dict[str, Any]) -> str:
        """Trả về chuỗi vf (rỗng nếu normal)."""
        t = str(filter_config.get("type") or "normal").lower()
        base = self.PRESETS.get(t, "")
        extra: list[str] = []
        if filter_config.get("brightness") is not None:
            extra.append(f"brightness={float(filter_config['brightness']):.4f}")
        if filter_config.get("contrast") is not None:
            extra.append(f"contrast={float(filter_config['contrast']):.4f}")
        if filter_config.get("saturation") is not None:
            extra.append(f"saturation={float(filter_config['saturation']):.4f}")
        if extra and base:
            return base + "," + "eq=" + ":".join(extra)
        if extra:
            return "eq=" + ":".join(extra)
        return base

    def apply_filter(self, project: dict[str, Any], clip_id: str, filter_config: dict[str, Any]) -> dict[str, Any]:
        fl = project.setdefault("filters", [])
        clip_id = str(clip_id)
        fc = dict(filter_config)
        fc["id"] = fc.get("id") or f"filter_{clip_id[:16]}"
        fc["clip_id"] = clip_id
        fl = [x for x in fl if not (isinstance(x, dict) and str(x.get("clip_id")) == clip_id)]
        fl.append(fc)
        project["filters"] = fl
        return project

    def remove_filter_for_clip(self, project: dict[str, Any], clip_id: str) -> dict[str, Any]:
        cid = str(clip_id)
        project["filters"] = [
            x for x in (project.get("filters") or []) if not (isinstance(x, dict) and str(x.get("clip_id")) == cid)
        ]
        return project
