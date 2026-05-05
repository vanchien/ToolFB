"""Template chỉnh sửa — lưu / áp dụng tracks + phase2 nhẹ."""

from __future__ import annotations

import json
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any

from src.services.video_editor.export_preset_manager import ExportPresetManager
from src.services.video_editor.layout import ensure_video_editor_layout


class TemplateManager:
    def __init__(self) -> None:
        paths = ensure_video_editor_layout()
        self._dir = paths["templates"]

    def save_template(self, project: dict[str, Any], name: str, *, template_id: str | None = None) -> dict[str, Any]:
        tid = template_id or f"template_{uuid.uuid4().hex[:10]}"
        rec = {
            "id": tid,
            "name": str(name or tid),
            "width": project.get("width"),
            "height": project.get("height"),
            "fps": project.get("fps"),
            "tracks": deepcopy(project.get("tracks") or []),
            "transitions": deepcopy(project.get("transitions") or []),
            "filters": deepcopy(project.get("filters") or []),
            "audio_settings": deepcopy(project.get("audio_settings") or {}),
            "default_export_preset": (project.get("meta") or {}).get("last_export_preset_id") or "facebook_reels",
        }
        self._dir.mkdir(parents=True, exist_ok=True)
        fp = self._dir / f"{tid}.json"
        fp.write_text(json.dumps(rec, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return rec

    def list_templates(self) -> list[dict[str, Any]]:
        self._dir.mkdir(parents=True, exist_ok=True)
        out: list[dict[str, Any]] = []
        for fp in sorted(self._dir.glob("*.json")):
            try:
                raw = json.loads(fp.read_text(encoding="utf-8"))
                if isinstance(raw, dict) and raw.get("id"):
                    out.append({"id": raw["id"], "name": raw.get("name", "")})
            except Exception:
                continue
        return out

    def apply_template(self, project: dict[str, Any], template_id: str) -> dict[str, Any]:
        fp = self._dir / f"{template_id}.json"
        if not fp.is_file():
            raise FileNotFoundError(template_id)
        raw = json.loads(fp.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("Template không hợp lệ")
        project["tracks"] = deepcopy(raw.get("tracks") or project.get("tracks"))
        project["transitions"] = deepcopy(raw.get("transitions") or [])
        project["filters"] = deepcopy(raw.get("filters") or [])
        project["audio_settings"] = deepcopy(raw.get("audio_settings") or {"bgm": [], "ducking": []})
        if raw.get("width"):
            project["width"] = int(raw["width"])
        if raw.get("height"):
            project["height"] = int(raw["height"])
        if raw.get("fps"):
            project["fps"] = int(raw["fps"])
        dep = str(raw.get("default_export_preset") or "")
        if dep:
            try:
                ExportPresetManager().apply_to_project(project, dep)
            except Exception:
                pass
        project["template_id"] = str(raw.get("id") or template_id)
        return project
