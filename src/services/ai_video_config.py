from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.services.ai_video_modes import default_video_mode_registry
from src.utils.paths import project_root


def default_ai_video_config() -> dict[str, Any]:
    return {
        "enabled": True,
        "default_provider": "gemini",
        "default_mode": "text_to_video",
        "output_root": "data/ai_video/outputs",
        "metadata_file": "data/ai_video/generated_videos.json",
        "providers": {
            "gemini": {
                "enabled": True,
                "api_key_env": "GEMINI_API_KEY",
                "default_model": "veo-3.1-generate-preview",
                "fast_model": "veo-3.1-fast-generate-preview",
                "poll_interval_sec": 10,
                "timeout_sec": 900,
                "max_retries": 3,
                "supported_modes": [
                    "text_to_video",
                    "image_to_video",
                    "first_last_frame_to_video",
                    "ingredients_to_video",
                    "extend_video",
                    "prompt_to_vertical_video",
                    "image_to_vertical_video",
                ],
                "default_options": {
                    "aspect_ratio": "9:16",
                    "duration_sec": 8,
                    "output_count": 1,
                    "resolution": "720p",
                },
            }
        },
        "modes": default_video_mode_registry(),
    }


def ai_video_config_path() -> Path:
    return project_root() / "config" / "ai_video_config.json"


def load_ai_video_config() -> dict[str, Any]:
    cfg = default_ai_video_config()
    p = ai_video_config_path()
    if not p.is_file():
        return cfg
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return cfg
    if not isinstance(raw, dict):
        return cfg
    cfg.update(raw)
    return cfg

