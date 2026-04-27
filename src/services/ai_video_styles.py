from __future__ import annotations

from typing import Any

from src.services.ai_styles_registry import load_style_registry, save_style_registry, style_items


def default_video_styles() -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for row in style_items("video_styles"):
        name = str(row.get("name", "")).strip()
        prompt = str(row.get("prompt_addon", "")).strip()
        if name and prompt:
            out.append({"name": name, "prompt": prompt})
    return out or [{"name": "Cinematic Realistic", "prompt": "cinematic realistic video, smooth camera movement"}]


def load_video_styles() -> list[dict[str, str]]:
    return default_video_styles()


def save_video_styles(styles: list[dict[str, Any]]) -> None:
    normalized: list[dict[str, str]] = []
    for row in styles:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        prompt = str(row.get("prompt", "")).strip()
        if not name or not prompt:
            continue
        sid = (
            str(row.get("id", "")).strip()
            or name.lower().replace(" ", "_").replace("-", "_")
        )
        normalized.append({"id": sid, "name": name, "prompt_addon": prompt})
    reg = load_style_registry()
    reg["video_styles"] = normalized
    save_style_registry(reg)
