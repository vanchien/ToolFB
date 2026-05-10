"""Merge field mặc định Phase 2 vào project JSON (không phá project cũ)."""

from __future__ import annotations

from copy import deepcopy
from typing import Any
from uuid import uuid4


def ensure_timeline_tracks(project: dict[str, Any]) -> None:
    """Bảo đảm có đủ track video / overlay / text / audio — project cũ hoặc JSON tay có thể thiếu track text."""
    raw = project.get("tracks")
    if not isinstance(raw, list):
        project["tracks"] = []
        raw = project["tracks"]
    seen: set[str] = set()
    for tr in raw:
        if isinstance(tr, dict):
            t = str(tr.get("type") or "").strip()
            if t:
                seen.add(t)
    for typ, prefix in (
        ("video", "track_video"),
        ("overlay", "track_overlay"),
        ("text", "track_text"),
        ("audio", "track_audio"),
    ):
        if typ not in seen:
            raw.append({"id": f"{prefix}_{uuid4().hex[:8]}", "type": typ, "clips": []})
            seen.add(typ)


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
    ensure_timeline_tracks(project)
    # Đã gỡ tính năng blur nền (boxblur) — bỏ khóa cũ để JSON gọn và tránh nhầm lẫn.
    for tr in project.get("tracks") or []:
        if not isinstance(tr, dict) or str(tr.get("type") or "") != "video":
            continue
        for cl in tr.get("clips") or []:
            if isinstance(cl, dict):
                cl.pop("blur_background", None)
    return project
