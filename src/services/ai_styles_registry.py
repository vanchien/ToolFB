from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.utils.paths import project_root


StyleItem = dict[str, str]
StyleRegistry = dict[str, Any]


_GROUPS: tuple[str, ...] = (
    "image_styles",
    "character_image_styles",
    "environment_styles",
    "video_styles",
    "camera_styles",
    "lighting_styles",
    "motion_styles",
)


def styles_registry_path() -> Path:
    return project_root() / "config" / "ai_styles.json"


def default_style_registry() -> StyleRegistry:
    p = styles_registry_path()
    try:
        if p.is_file():
            raw = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return _normalize_registry(raw)
    except Exception:
        pass
    return _normalize_registry({})


def _normalize_style_rows(raw_rows: Any) -> list[StyleItem]:
    out: list[StyleItem] = []
    if not isinstance(raw_rows, list):
        return out
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        sid = str(row.get("id", "")).strip()
        name = str(row.get("name", "")).strip()
        addon = str(row.get("prompt_addon", "")).strip()
        if sid and name and addon:
            out.append({"id": sid, "name": name, "prompt_addon": addon})
    return out


def _normalize_registry(raw: dict[str, Any]) -> StyleRegistry:
    out: StyleRegistry = {}
    for g in _GROUPS:
        out[g] = _normalize_style_rows(raw.get(g))
    d = dict(raw.get("defaults") or {})
    out["defaults"] = {
        "character_image_style_id": str(d.get("character_image_style_id", "character_cinematic_realistic")).strip()
        or "character_cinematic_realistic",
        "environment_style_id": str(d.get("environment_style_id", "environment_cinematic")).strip()
        or "environment_cinematic",
        "video_style_id": str(d.get("video_style_id", "video_cinematic_realistic")).strip() or "video_cinematic_realistic",
        "camera_style_id": str(d.get("camera_style_id", "smooth_dolly_in")).strip() or "smooth_dolly_in",
        "lighting_style_id": str(d.get("lighting_style_id", "soft_natural_light")).strip() or "soft_natural_light",
        "motion_style_id": str(d.get("motion_style_id", "slow_and_smooth")).strip() or "slow_and_smooth",
    }
    return out


def load_style_registry() -> StyleRegistry:
    p = styles_registry_path()
    if not p.is_file():
        return default_style_registry()
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default_style_registry()
    if not isinstance(raw, dict):
        return default_style_registry()
    merged = default_style_registry()
    cur = _normalize_registry(raw)
    for g in _GROUPS:
        if cur[g]:
            merged[g] = cur[g]
    merged["defaults"] = {**dict(merged.get("defaults") or {}), **dict(cur.get("defaults") or {})}
    return merged


def save_style_registry(registry: dict[str, Any]) -> None:
    normalized = _normalize_registry(dict(registry or {}))
    p = styles_registry_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(normalized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def style_items(group: str) -> list[StyleItem]:
    data = load_style_registry()
    rows = data.get(group)
    return list(rows) if isinstance(rows, list) else []


def default_style_id(group_default_key: str, fallback: str) -> str:
    data = load_style_registry()
    d = dict(data.get("defaults") or {})
    return str(d.get(group_default_key, fallback)).strip() or fallback


def style_prompt_addon(group: str, style_id: str, *, fallback: str = "") -> str:
    sid = str(style_id or "").strip()
    for row in style_items(group):
        if str(row.get("id", "")).strip() == sid:
            return str(row.get("prompt_addon", "")).strip()
    return fallback


def style_name(group: str, style_id: str, *, fallback: str = "") -> str:
    sid = str(style_id or "").strip()
    for row in style_items(group):
        if str(row.get("id", "")).strip() == sid:
            return str(row.get("name", "")).strip()
    return fallback
