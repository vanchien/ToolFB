from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.utils.paths import project_root


def default_ai_image_config() -> dict[str, Any]:
    """Cấu hình mặc định cho sinh ảnh AI (character map, batch, v.v.)."""
    return {
        "default_provider": "nano_banana_pro",
        "providers": {
            "nano_banana_pro": {
                "enabled": True,
                "api_key_env": "GEMINI_API_KEY",
                "model": "gemini-3-pro-image-preview",
                "output_format": "png",
                "default_size": "1024x1024",
                "timeout_sec": 180,
                "max_retries": 3,
            }
        },
    }


def ai_image_config_path() -> Path:
    return project_root() / "config" / "ai_image_config.json"


def load_ai_image_config() -> dict[str, Any]:
    """Merge file config với default (file thiếu field vẫn dùng default)."""
    cfg = default_ai_image_config()
    p = ai_image_config_path()
    if not p.is_file():
        return cfg
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return cfg
    if not isinstance(raw, dict):
        return cfg
    if "ai_image" in raw and isinstance(raw["ai_image"], dict):
        inner = raw["ai_image"]
        cfg["default_provider"] = str(inner.get("default_provider", cfg["default_provider"])).strip()
        prov_in = inner.get("providers")
        if isinstance(prov_in, dict):
            merged = dict(cfg.get("providers") or {})
            for k, v in prov_in.items():
                if isinstance(v, dict) and isinstance(merged.get(k), dict):
                    merged[k] = {**merged[k], **v}
                elif isinstance(v, dict):
                    merged[k] = v
            cfg["providers"] = merged
    else:
        if isinstance(raw.get("default_provider"), str):
            cfg["default_provider"] = str(raw["default_provider"]).strip()
        prov_in = raw.get("providers")
        if isinstance(prov_in, dict):
            merged = dict(cfg.get("providers") or {})
            for k, v in prov_in.items():
                if isinstance(v, dict) and isinstance(merged.get(k), dict):
                    merged[k] = {**merged[k], **v}
                elif isinstance(v, dict):
                    merged[k] = v
            cfg["providers"] = merged
    return cfg


def nano_banana_pro_settings() -> dict[str, Any]:
    """Trả về block cấu hình provider nano_banana_pro (đã merge default)."""
    cfg = load_ai_image_config()
    prov = cfg.get("providers") or {}
    block = prov.get("nano_banana_pro")
    if isinstance(block, dict):
        defaults = (default_ai_image_config().get("providers") or {}).get("nano_banana_pro") or {}
        return {**defaults, **block}
    return dict((default_ai_image_config().get("providers") or {}).get("nano_banana_pro") or {})
