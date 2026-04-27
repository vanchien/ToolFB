from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.utils.paths import project_root


def _preset_path() -> Path:
    return project_root() / "config" / "ai_video_prompt_presets.json"


def default_prompt_presets() -> dict[str, list[str]]:
    return {
        "visual_style": [
            {"name": "Điện ảnh", "description": "cinematic, dramatic composition, filmic depth, high detail"},
            {"name": "Chân thực", "description": "realistic, natural textures, lifelike motion"},
            {"name": "Anime", "description": "anime style, vibrant colors, clean outlines"},
            {"name": "Dựng 3D", "description": "3D render, polished surfaces, physically plausible lighting"},
            {"name": "Quảng cáo sản phẩm", "description": "product commercial, clean branding, premium look"},
            {"name": "Phóng sự", "description": "documentary style, authentic moments, natural framing"},
            {"name": "Sang trọng", "description": "luxury, premium visual language, elegant lighting"},
            {"name": "Đường phố", "description": "street style, dynamic urban vibe, spontaneous motion"},
            {"name": "Mạng xã hội viral", "description": "social media viral style, bold hooks, fast engagement"},
            {"name": "Tối, bí ẩn", "description": "dark mood, shadows, high contrast, suspense atmosphere"},
            {"name": "Sáng, sạch", "description": "bright clean commercial, fresh colors, minimal clutter"},
        ],
        "mood": [
            {"name": "Cảm xúc", "description": "emotional"},
            {"name": "Truyền cảm hứng", "description": "inspiring"},
            {"name": "Cao cấp", "description": "premium"},
            {"name": "Vui tươi", "description": "fun"},
            {"name": "Bí ẩn", "description": "mysterious"},
            {"name": "Năng động", "description": "energetic"},
            {"name": "Bình tĩnh", "description": "calm"},
        ],
        "camera_style": [
            {"name": "Theo dõi mượt", "description": "smooth tracking shot"},
            {"name": "Tiến chậm vào chủ thể", "description": "slow dolly in"},
            {"name": "Cầm tay chân thực", "description": "handheld realistic"},
            {"name": "Cận cảnh", "description": "close-up"},
            {"name": "Toàn cảnh rộng", "description": "wide shot"},
            {"name": "Pan điện ảnh", "description": "cinematic pan"},
            {"name": "Macro sản phẩm", "description": "product macro shot"},
        ],
        "lighting": [
            {"name": "Giờ vàng", "description": "golden hour"},
            {"name": "Ánh sáng studio", "description": "studio lighting"},
            {"name": "Ánh sáng tự nhiên dịu", "description": "soft natural light"},
            {"name": "Đêm neon", "description": "neon night"},
            {"name": "Tương phản cao điện ảnh", "description": "high contrast cinematic"},
            {"name": "Sáng sạch kiểu quảng cáo", "description": "bright clean commercial"},
        ],
        "motion_style": [
            {"name": "Chậm và mượt", "description": "slow and smooth"},
            {"name": "Năng động", "description": "energetic"},
            {"name": "Chuyển động người tự nhiên", "description": "natural human movement"},
            {"name": "Lộ diện kịch tính", "description": "dramatic reveal"},
            {"name": "Xoay sản phẩm", "description": "product rotation"},
            {"name": "Cảnh đi bộ", "description": "walking scene"},
        ],
    }


def load_prompt_presets() -> dict[str, list[dict[str, str]]]:
    defaults = default_prompt_presets()
    p = _preset_path()
    if not p.is_file():
        return defaults
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return defaults
    if not isinstance(raw, dict):
        return defaults
    out: dict[str, list[dict[str, str]]] = {}
    for key, default_vals in defaults.items():
        vals = raw.get(key, default_vals)
        if not isinstance(vals, list):
            out[key] = [dict(x) for x in default_vals]
            continue
        norm: list[dict[str, str]] = []
        for row in vals:
            if isinstance(row, dict):
                name = str(row.get("name", "")).strip()
                desc = str(row.get("description", "")).strip()
                if name:
                    norm.append({"name": name, "description": desc or name})
            else:
                # Backward compatibility với format cũ list[str]
                s = str(row).strip()
                if s:
                    norm.append({"name": s, "description": s})
        out[key] = norm or [dict(x) for x in default_vals]
    return out


def save_prompt_presets(presets: dict[str, Any]) -> None:
    defaults = default_prompt_presets()
    out: dict[str, list[dict[str, str]]] = {}
    for key in defaults:
        vals = presets.get(key, defaults[key])
        if not isinstance(vals, list):
            out[key] = [dict(x) for x in defaults[key]]
            continue
        norm: list[dict[str, str]] = []
        for row in vals:
            if not isinstance(row, dict):
                s = str(row).strip()
                if s:
                    norm.append({"name": s, "description": s})
                continue
            name = str(row.get("name", "")).strip()
            desc = str(row.get("description", "")).strip()
            if name:
                norm.append({"name": name, "description": desc or name})
        out[key] = norm or [dict(x) for x in defaults[key]]
    p = _preset_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
