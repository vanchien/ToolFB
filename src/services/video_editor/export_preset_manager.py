"""Preset xuất video (Facebook Reels, TikTok, …)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.services.video_editor.layout import ensure_video_editor_layout


def default_export_presets() -> list[dict[str, Any]]:
    return [
        {
            "id": "facebook_reels",
            "label": "Facebook Reels",
            "width": 1080,
            "height": 1920,
            "fps": 30,
            "format": "mp4",
            "codec": "libx264",
            "preset": "veryfast",
            "crf": 23,
            "audio_codec": "aac",
        },
        {
            "id": "tiktok",
            "label": "TikTok",
            "width": 1080,
            "height": 1920,
            "fps": 30,
            "format": "mp4",
            "codec": "libx264",
            "preset": "veryfast",
            "crf": 23,
            "audio_codec": "aac",
        },
        {
            "id": "youtube_shorts",
            "label": "YouTube Shorts",
            "width": 1080,
            "height": 1920,
            "fps": 30,
            "format": "mp4",
            "codec": "libx264",
            "preset": "veryfast",
            "crf": 23,
            "audio_codec": "aac",
        },
        {
            "id": "youtube_landscape",
            "label": "YouTube 1920×1080",
            "width": 1920,
            "height": 1080,
            "fps": 30,
            "format": "mp4",
            "codec": "libx264",
            "preset": "veryfast",
            "crf": 23,
            "audio_codec": "aac",
        },
        {
            "id": "square_1080",
            "label": "Square 1080×1080",
            "width": 1080,
            "height": 1080,
            "fps": 30,
            "format": "mp4",
            "codec": "libx264",
            "preset": "veryfast",
            "crf": 23,
            "audio_codec": "aac",
        },
    ]


class ExportPresetManager:
    def __init__(self, *, presets_path: Path | None = None) -> None:
        paths = ensure_video_editor_layout()
        self._path = presets_path or paths["presets"] / "export_presets.json"

    def ensure_file(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if not self._path.is_file():
            self._path.write_text(
                json.dumps(default_export_presets(), ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

    def list_presets(self) -> list[dict[str, Any]]:
        self.ensure_file()
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            return [x for x in raw if isinstance(x, dict)]
        except Exception:
            return default_export_presets()

    def get_preset(self, preset_id: str) -> dict[str, Any] | None:
        for p in self.list_presets():
            if str(p.get("id")) == str(preset_id):
                return dict(p)
        return None

    def apply_to_project(self, project: dict[str, Any], preset_id: str) -> dict[str, Any]:
        pr = self.get_preset(preset_id)
        if not pr:
            raise ValueError(f"Không tìm thấy preset: {preset_id}")
        project["width"] = int(pr.get("width") or project.get("width") or 1080)
        project["height"] = int(pr.get("height") or project.get("height") or 1920)
        project["fps"] = int(pr.get("fps") or project.get("fps") or 30)
        exp = project.setdefault("export", {})
        exp["format"] = str(pr.get("format") or exp.get("format") or "mp4")
        exp["codec"] = str(pr.get("codec") or exp.get("codec") or "libx264")
        exp["preset"] = str(pr.get("preset") or exp.get("preset") or "veryfast")
        exp["crf"] = int(pr.get("crf") if pr.get("crf") is not None else exp.get("crf") or 23)
        exp["audio_codec"] = str(pr.get("audio_codec") or exp.get("audio_codec") or "aac")
        project.setdefault("meta", {})["last_export_preset_id"] = preset_id
        return project
