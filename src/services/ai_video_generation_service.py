from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

from loguru import logger

from src.services.ai_video_config import load_ai_video_config
from src.services.ai_video_store import AIVideoStore, ai_video_project_output_dir, ensure_ai_video_layout
from src.services.video_providers.gemini_video_provider import GeminiVideoProvider

VIDEO_STATUSES: tuple[str, ...] = (
    "draft",
    "queued",
    "submitting",
    "generating",
    "polling",
    "downloading",
    "completed",
    "failed",
    "cancelled",
)


class AIVideoGenerationService:
    def __init__(self) -> None:
        self._cfg = load_ai_video_config()
        self._store = AIVideoStore()
        self._worker_threads: dict[str, threading.Thread] = {}

    def _provider(self, provider: str):
        p = str(provider or "gemini").strip().lower()
        if p == "gemini":
            return GeminiVideoProvider()
        raise ValueError(f"AI video provider không hỗ trợ: {provider!r}")

    def create_video_record(self, request: dict[str, Any]) -> dict[str, Any]:
        self._validate_request(request)
        rec = self._store.create_record(request)
        return self._store.update_record(rec["id"], {"status": "queued"})

    def submit_video_generation(self, video_id: str) -> dict[str, Any]:
        rec = self._get(video_id)
        if rec.get("status") in {"cancelled", "completed"}:
            return rec
        rec = self._store.update_record(video_id, {"status": "submitting", "started_at": rec.get("started_at") or _now_iso()})
        prov = self._provider(rec.get("provider", "gemini"))
        sub = prov.submit_generation(rec)
        rec = self._store.update_record(
            video_id,
            {
                "status": sub.get("status", "generating"),
                "operation_id": str(sub.get("operation_id", "")).strip(),
            },
        )
        return rec

    def poll_video_generation(self, video_id: str) -> dict[str, Any]:
        rec = self._get(video_id)
        op = str(rec.get("operation_id", "")).strip()
        if not op:
            raise ValueError("Record chưa có operation_id để poll.")
        rec = self._store.update_record(video_id, {"status": "polling"})
        prov = self._provider(rec.get("provider", "gemini"))
        pol = prov.poll_operation(op)
        st = str(pol.get("status", "")).strip().lower() or "failed"
        patch: dict[str, Any] = {"status": st}
        if pol.get("error_message"):
            patch["error_message"] = str(pol["error_message"])
        if st == "completed":
            patch["completed_at"] = _now_iso()
        if st == "failed":
            patch["completed_at"] = _now_iso()
        return self._store.update_record(video_id, patch)

    def download_completed_video(self, video_id: str) -> dict[str, Any]:
        rec = self._get(video_id)
        op = str(rec.get("operation_id", "")).strip()
        if not op:
            raise ValueError("Record chưa có operation_id để download.")
        opts = dict(rec.get("options") or {})
        raw_out_dir = str(opts.get("output_dir", "") or "").strip()
        if raw_out_dir:
            out_dir = Path(raw_out_dir).resolve()
            out_dir.mkdir(parents=True, exist_ok=True)
        else:
            pid = str(rec.get("project_id", "") or "").strip()
            out_dir = ai_video_project_output_dir(pid) if pid else ensure_ai_video_layout()["outputs"]
        rec = self._store.update_record(video_id, {"status": "downloading"})
        prov = self._provider(rec.get("provider", "gemini"))
        res = prov.download_result(op, str(out_dir))
        st = str(res.get("status", "failed")).strip().lower()
        patch: dict[str, Any] = {"status": st, "output_files": list(res.get("output_files") or [])}
        if res.get("error_message"):
            patch["error_message"] = str(res["error_message"])
        if st in {"completed", "failed"}:
            patch["completed_at"] = _now_iso()
        return self._store.update_record(video_id, patch)

    def sync_pending_videos(self) -> None:
        rows = self._store.load_all()
        for r in rows:
            st = str(r.get("status", "")).strip().lower()
            vid = str(r.get("id", "")).strip()
            if not vid:
                continue
            if st in {"queued", "submitting", "generating", "polling", "downloading"}:
                self.start_background_worker(vid)

    def cancel_video(self, video_id: str) -> dict[str, Any]:
        return self._store.update_record(video_id, {"status": "cancelled", "completed_at": _now_iso()})

    def list_records(self) -> list[dict[str, Any]]:
        return self._store.load_all()

    def delete_video(self, video_id: str) -> bool:
        rows = self._store.load_all()
        new_rows = [r for r in rows if str(r.get("id", "")) != str(video_id)]
        if len(new_rows) == len(rows):
            return False
        self._store.save_all(new_rows)
        self._worker_threads.pop(str(video_id), None)
        return True

    def delete_all_videos(self) -> int:
        rows = self._store.load_all()
        count = len(rows)
        if count <= 0:
            return 0
        self._store.save_all([])
        self._worker_threads.clear()
        return count

    def delete_records_for_project(self, project_id: str) -> int:
        """Xóa mọi job video thuộc ``project_id`` (metadata)."""
        return int(self._store.delete_records_for_project(project_id))

    def start_background_worker(self, video_id: str) -> None:
        th = self._worker_threads.get(video_id)
        if th is not None and th.is_alive():
            return

        def run() -> None:
            try:
                rec = self._get(video_id)
                if rec.get("status") == "queued":
                    rec = self.submit_video_generation(video_id)
                poll_interval = int(self._cfg.get("providers", {}).get("gemini", {}).get("poll_interval_sec", 10))
                for _ in range(120):
                    rec = self._get(video_id)
                    st = str(rec.get("status", "")).lower()
                    if st in {"cancelled", "completed", "failed"}:
                        return
                    rec = self.poll_video_generation(video_id)
                    st = str(rec.get("status", "")).lower()
                    if st == "completed":
                        self.download_completed_video(video_id)
                        return
                    if st == "failed":
                        return
                    time.sleep(max(2, poll_interval))
            except Exception as exc:  # noqa: BLE001
                logger.warning("AI video worker lỗi (video_id={}): {}", video_id, exc)
                try:
                    self._store.update_record(video_id, {"status": "failed", "error_message": str(exc), "completed_at": _now_iso()})
                except Exception:
                    pass

        th = threading.Thread(target=run, daemon=True, name=f"ai_video_worker_{video_id[:8]}")
        self._worker_threads[video_id] = th
        th.start()

    def _get(self, video_id: str) -> dict[str, Any]:
        for r in self._store.load_all():
            if str(r.get("id", "")) == video_id:
                return r
        raise ValueError(f"Không tìm thấy video_id={video_id}")

    def _validate_request(self, req: dict[str, Any]) -> None:
        mode = str(req.get("mode", "")).strip()
        prompt = str(req.get("prompt", "")).strip()
        if not mode:
            raise ValueError("Thiếu mode video.")
        modes = dict(self._cfg.get("modes") or {})
        mcfg = dict(modes.get(mode) or {})
        if not mcfg:
            raise ValueError(f"Mode không hỗ trợ: {mode}")
        if not bool(mcfg.get("enabled", False)):
            raise ValueError(f"Mode đang tắt/experimental: {mode}")
        assets = dict(req.get("input_assets") or {})
        required = list(mcfg.get("requires") or [])
        for key in required:
            if key == "prompt":
                if not prompt:
                    raise ValueError("Mode này yêu cầu prompt.")
                continue
            if key == "reference_images":
                refs = assets.get("reference_images") or []
                if not isinstance(refs, list) or not refs:
                    raise ValueError("Mode này yêu cầu reference_images không rỗng.")
                for p in refs:
                    if not Path(str(p)).is_file():
                        raise ValueError(f"Thiếu ảnh tham chiếu: {p}")
                continue
            p = str(assets.get(key, "")).strip()
            if not p:
                raise ValueError(f"Mode này yêu cầu {key}.")
            if key.endswith("_path") and not Path(p).is_file():
                raise ValueError(f"File không tồn tại: {p}")


def _now_iso() -> str:
    from datetime import datetime

    return datetime.now().replace(microsecond=0).isoformat()

