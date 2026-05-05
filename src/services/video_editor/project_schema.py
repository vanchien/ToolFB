"""Merge field mặc định Phase 2 vào project JSON (không phá project cũ)."""

from __future__ import annotations

from copy import deepcopy
from typing import Any


def merge_phase2_defaults(project: dict[str, Any]) -> dict[str, Any]:
    """Bổ sung key thiếu; giữ nguyên dữ liệu đã có."""
    defaults: dict[str, Any] = {
        "version": 2,
        "aspect_ratio": "9:16",
        "audio_mode": "mix",
        "transitions": [],
        "subtitles": [],
        "filters": [],
        "audio_settings": {"bgm": [], "ducking": []},
        "template_id": "",
        "features": {
            "transitions": True,
            "subtitles": True,
            "waveform": True,
            "color_filters": True,
            "speed": True,
            "bgm": True,
            "ducking": True,
            "proxy_preview": True,
            "templates": True,
            "animation": True,
        },
    }
    for k, v in defaults.items():
        if k not in project:
            project[k] = deepcopy(v)
        elif k == "audio_settings" and isinstance(project.get("audio_settings"), dict):
            base = deepcopy(v)
            base.update(project["audio_settings"])
            for sk, sv in defaults["audio_settings"].items():
                if sk not in base:
                    base[sk] = deepcopy(sv)
            project["audio_settings"] = base
    if int(project.get("version") or 1) < 2:
        project["version"] = 2
    return project
