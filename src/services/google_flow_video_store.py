from __future__ import annotations

import json
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from src.utils.paths import project_root


def now_iso() -> str:
    """Return local ISO timestamp without microseconds."""
    return datetime.now().replace(microsecond=0).isoformat()


def google_flow_root() -> Path:
    """Return root directory for Google Flow video module."""
    return project_root() / "data" / "google_flow_video"


def ensure_google_flow_layout() -> dict[str, Path]:
    """Create Google Flow data layout and metadata file when missing."""
    root = google_flow_root()
    paths: dict[str, Path] = {
        "root": root,
        "inputs_prompts": root / "inputs" / "prompts",
        "inputs_assets": root / "inputs" / "assets",
        "prompts": root / "prompts",
        "outputs": root / "outputs",
        "downloads": root / "downloads",
        "temp": root / "temp",
        "logs": root / "logs",
        "screenshots": root / "logs" / "screenshots",
        "character_profiles": root / "character_profiles",
        "metadata": root / "flow_video_jobs.json",
    }
    for key, path in paths.items():
        if key == "metadata":
            continue
        path.mkdir(parents=True, exist_ok=True)
    if not paths["metadata"].is_file():
        paths["metadata"].write_text("[]\n", encoding="utf-8")
    return paths


class GoogleFlowVideoStore:
    """Persist Google Flow text-to-video jobs in local JSON storage."""

    def __init__(self) -> None:
        self._paths = ensure_google_flow_layout()

    @property
    def metadata_path(self) -> Path:
        """Return metadata file path."""
        return self._paths["metadata"]

    def load_all(self) -> list[dict[str, Any]]:
        """Load all saved jobs."""
        p = self.metadata_path
        if not p.is_file():
            return []
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return []
        if not isinstance(raw, list):
            return []
        return [x for x in raw if isinstance(x, dict)]

    def save_all(self, rows: list[dict[str, Any]]) -> None:
        """Atomically save all jobs."""
        text = json.dumps(rows, ensure_ascii=False, indent=2) + "\n"
        p = self.metadata_path
        p.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix="google_flow_jobs_", suffix=".tmp.json", dir=str(p.parent))
        import os

        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, p)

    def create_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Create a new Google Flow job with default fields."""
        rows = self.load_all()
        job_id = f"flow_vid_{uuid.uuid4().hex[:10]}"
        rec: dict[str, Any] = {
            "id": job_id,
            "provider": "google_flow",
            "tool_url": "https://labs.google/fx/vi/tools/flow",
            "mode": str(payload.get("mode", "text_to_video")).strip() or "text_to_video",
            "idea": str(payload.get("idea", "")).strip(),
            "language": str(payload.get("language", "Vietnamese")).strip() or "Vietnamese",
            "visual_style": str(payload.get("visual_style", "")).strip(),
            "goal": str(payload.get("goal", "")).strip(),
            "topic": str(payload.get("topic", "")).strip(),
            "character_mode": str(payload.get("character_mode", "auto")).strip() or "auto",
            "character_profile_id": str(payload.get("character_profile_id", "")).strip(),
            "character_profile": dict(payload.get("character_profile") or {}),
            "settings": dict(payload.get("settings") or {}),
            "scene_plan": dict(payload.get("scene_plan") or {}),
            "start_prompt": str(payload.get("start_prompt", "")).strip(),
            "end_prompt": str(payload.get("end_prompt", "")).strip(),
            "final_prompt": str(payload.get("final_prompt", "")).strip(),
            "status": str(payload.get("status", "pending")).strip() or "pending",
            "step": str(payload.get("step", "")).strip(),
            "download_dir": str(payload.get("download_dir", self._paths["downloads"])).strip(),
            "output_path": str(payload.get("output_path", "")).strip(),
            "output_files": list(payload.get("output_files") or []),
            "browser": dict(payload.get("browser") or {}),
            "browser_profile": dict(payload.get("browser_profile") or {}),
            "created_at": now_iso(),
            "started_at": "",
            "completed_at": "",
            "error_message": "",
            "retry_count": int(payload.get("retry_count") or 0),
            "max_retry": int(payload.get("max_retry") or 3),
            "locked_by": "",
            "locked_at": "",
        }
        rows.append(rec)
        self.save_all(rows)
        return rec

    def update_job(self, job_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        """Update job by id and return updated object."""
        rows = self.load_all()
        for i, row in enumerate(rows):
            if str(row.get("id", "")) != job_id:
                continue
            merged = dict(row)
            merged.update(patch)
            rows[i] = merged
            self.save_all(rows)
            return merged
        raise ValueError(f"Không tìm thấy job_id={job_id}")

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        """Lấy 1 job theo id, hoặc None."""
        for row in self.load_all():
            if str(row.get("id", "")).strip() == str(job_id).strip():
                return dict(row)
        return None

    def lock_next_pending_job(self, *, lock_owner: str) -> dict[str, Any] | None:
        """
        Lấy job pending đầu tiên và lock atomically (theo file metadata hiện tại).
        Chỉ đổi trạng thái sang ``running`` khi lock thành công.
        """
        rows = self.load_all()
        for i, row in enumerate(rows):
            status = str(row.get("status", "")).strip().lower()
            if status != "pending":
                continue
            if str(row.get("locked_by", "")).strip():
                continue
            rec = dict(row)
            rec["locked_by"] = lock_owner
            rec["locked_at"] = now_iso()
            rec["status"] = "running"
            rec["started_at"] = str(rec.get("started_at", "")).strip() or now_iso()
            rows[i] = rec
            self.save_all(rows)
            return rec
        return None

    def unlock_job(self, job_id: str) -> dict[str, Any]:
        """Bỏ lock job (không đổi status hiện tại)."""
        return self.update_job(
            job_id,
            {
                "locked_by": "",
                "locked_at": "",
            },
        )

    def create_regenerate_job(self, job_id: str, *, edited_prompt: str = "") -> dict[str, Any]:
        """
        Tạo job regenerate từ job cũ; luôn reset sang project mới (pending).
        """
        src = self.get_job(job_id)
        if not src:
            raise ValueError(f"Không tìm thấy job_id={job_id} để regenerate.")
        prompt = str(edited_prompt or src.get("final_prompt", "")).strip()
        payload: dict[str, Any] = {
            "provider": "google_flow",
            "mode": str(src.get("mode", "text_to_video")).strip() or "text_to_video",
            "idea": str(src.get("idea", "")).strip(),
            "language": str(src.get("language", "Vietnamese")).strip() or "Vietnamese",
            "visual_style": str(src.get("visual_style", "")).strip(),
            "goal": str(src.get("goal", "")).strip(),
            "topic": str(src.get("topic", "")).strip(),
            "character_mode": str(src.get("character_mode", "auto")).strip() or "auto",
            "character_profile_id": str(src.get("character_profile_id", "")).strip(),
            "character_profile": dict(src.get("character_profile") or {}),
            "settings": dict(src.get("settings") or {}),
            "scene_plan": dict(src.get("scene_plan") or {}),
            "start_prompt": str(src.get("start_prompt", "")).strip(),
            "end_prompt": str(src.get("end_prompt", "")).strip(),
            "final_prompt": prompt,
            "download_dir": str(src.get("download_dir", self._paths["downloads"])).strip(),
            "browser": dict(src.get("browser") or {}),
            "browser_profile": dict(src.get("browser_profile") or {}),
            "status": "pending",
            "step": "INIT",
            "retry_count": 0,
            "max_retry": int(src.get("max_retry") or 3),
        }
        return self.create_job(payload)
