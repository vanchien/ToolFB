"""Quản lý project JSON Video Editor."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.services.video_editor.layout import ensure_video_editor_layout
from src.services.video_editor.project_schema import merge_phase2_defaults


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _default_tracks() -> list[dict[str, Any]]:
    return [
        {"id": "track_video_001", "type": "video", "clips": []},
        {"id": "track_overlay_001", "type": "overlay", "clips": []},
        {"id": "track_text_001", "type": "text", "clips": []},
        {"id": "track_audio_001", "type": "audio", "clips": []},
    ]


class VideoEditorProjectManager:
    """Quản lý project JSON của Video Editor."""

    def __init__(self, *, paths: dict[str, Path] | None = None) -> None:
        self._paths = paths or ensure_video_editor_layout()

    def _project_path(self, project_id: str) -> Path:
        safe = "".join(c for c in project_id if c.isalnum() or c in "-_")
        return self._paths["projects"] / f"{safe}.json"

    def create_project(self, name: str, width: int = 1080, height: int = 1920, fps: int = 30) -> dict[str, Any]:
        pid = f"edit_{uuid.uuid4().hex[:10]}"
        now = _now_iso()
        project: dict[str, Any] = {
            "id": pid,
            "name": str(name or "Untitled").strip() or "Untitled",
            "width": int(width),
            "height": int(height),
            "fps": int(fps),
            "duration": 0.0,
            "media": [],
            "tracks": _default_tracks(),
            "export": {
                "format": "mp4",
                "codec": "libx264",
                "preset": "veryfast",
                "crf": 23,
                "audio_codec": "aac",
            },
            "created_at": now,
            "updated_at": now,
        }
        merge_phase2_defaults(project)
        self.save_project(project)
        return project

    def load_project(self, project_id: str) -> dict[str, Any]:
        p = self._project_path(project_id)
        if not p.is_file():
            raise FileNotFoundError(f"Không tìm thấy project: {project_id}")
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("Project JSON không hợp lệ")
        merge_phase2_defaults(raw)
        return raw

    def save_project(self, project: dict[str, Any]) -> None:
        pid = str(project.get("id") or "").strip()
        if not pid:
            raise ValueError("project thiếu id")
        project["updated_at"] = _now_iso()
        self._paths["projects"].mkdir(parents=True, exist_ok=True)
        path = self._project_path(pid)
        path.write_text(json.dumps(project, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def delete_project(self, project_id: str) -> None:
        p = self._project_path(project_id)
        if p.is_file():
            p.unlink()

    def list_projects(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        self._paths["projects"].mkdir(parents=True, exist_ok=True)
        for fp in sorted(self._paths["projects"].glob("*.json")):
            try:
                raw = json.loads(fp.read_text(encoding="utf-8"))
                if isinstance(raw, dict) and raw.get("id"):
                    out.append(
                        {
                            "id": raw.get("id"),
                            "name": raw.get("name", ""),
                            "updated_at": raw.get("updated_at", ""),
                        }
                    )
            except Exception:
                continue
        return out
