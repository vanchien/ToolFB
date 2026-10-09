"""Lưu batch và job tạo Page (JSON, sống sau khi tắt app)."""

from __future__ import annotations

import json
import os
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.json_store_lock import json_file_lock
from src.utils.paths import project_root

from .states import JOB_PENDING


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def default_store_path() -> Path:
    return project_root() / "config" / "page_creation.json"


def _copy_row(row: dict[str, Any]) -> dict[str, Any]:
    """Bản sao nông, tách list log để sửa job không đụng cache."""
    copied = dict(row)
    logs = copied.get("logs")
    if isinstance(logs, list):
        copied["logs"] = list(logs)
    return copied


class PageCreationStore:
    """Đọc/ghi ``config/page_creation.json``. Cache trong RAM, chỉ ghi đĩa khi flush."""

    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path) if path else default_store_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.disk_writes = 0
        self._doc: dict[str, Any] | None = None
        self._mtime: float | None = None
        self._dirty = False
        if not self.path.is_file():
            self._write({"batches": [], "jobs": [], "ui": {}}, sync=True)
            self._mtime = self._file_mtime()

    def _file_mtime(self) -> float | None:
        try:
            return self.path.stat().st_mtime
        except OSError:
            return None

    def _doc_unlocked(self) -> dict[str, Any]:
        """Cache RAM. File đổi từ bên ngoài thì nạp lại khi chưa có sửa chưa ghi."""
        mtime = self._file_mtime()
        if self._doc is None or (mtime != self._mtime and not self._dirty):
            self._doc = self._read()
            self._mtime = mtime
            self._dirty = False
        return self._doc

    def _read(self) -> dict[str, Any]:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            raw = {}
        if not isinstance(raw, dict):
            raw = {}
        raw.setdefault("batches", [])
        raw.setdefault("jobs", [])
        if not isinstance(raw.get("ui"), dict):
            raw["ui"] = {}
        return raw

    def _write(self, data: dict[str, Any], *, sync: bool = True) -> None:
        text = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
        fd, tmp = tempfile.mkstemp(prefix="page_creation_", suffix=".tmp.json", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
                fh.flush()
                if sync:
                    os.fsync(fh.fileno())
            last_error: Exception | None = None
            for attempt in range(8):
                try:
                    os.replace(tmp, self.path)
                    self.disk_writes += 1
                    return
                except PermissionError as exc:
                    last_error = exc
                    time.sleep(0.02 * (attempt + 1))
            if last_error is not None:
                raise last_error
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def flush(self, *, sync: bool = True) -> None:
        """Ghi cache xuống đĩa. ``sync=True`` tại mốc CREATING / COMPLETED / PAUSED."""
        with json_file_lock(self.path):
            if self._doc is None or not self._dirty:
                return
            self._write(self._doc, sync=sync)
            self._mtime = self._file_mtime()
            self._dirty = False

    def load(self) -> dict[str, Any]:
        with json_file_lock(self.path):
            return self._doc_unlocked()

    def save_batch(self, batch: dict[str, Any], *, flush: bool = True, sync: bool = True) -> None:
        with json_file_lock(self.path):
            data = self._doc_unlocked()
            batch["updated_at"] = _now()
            rows = [b for b in data["batches"] if str(b.get("id")) != str(batch.get("id"))]
            rows.append(_copy_row(batch))
            data["batches"] = rows
            self._dirty = True
        if flush:
            self.flush(sync=sync)

    def save_job(self, job: dict[str, Any], *, flush: bool = True, sync: bool = True) -> None:
        with json_file_lock(self.path):
            data = self._doc_unlocked()
            job["updated_at"] = _now()
            rows = [j for j in data["jobs"] if str(j.get("id")) != str(job.get("id"))]
            rows.append(_copy_row(job))
            data["jobs"] = rows
            self._dirty = True
        if flush:
            self.flush(sync=sync)

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        with json_file_lock(self.path):
            for row in self._doc_unlocked()["batches"]:
                if str(row.get("id")) == str(batch_id):
                    return _copy_row(row)
        return None

    def jobs_for_batch(self, batch_id: str) -> list[dict[str, Any]]:
        with json_file_lock(self.path):
            rows = [
                _copy_row(j)
                for j in self._doc_unlocked()["jobs"]
                if str(j.get("batch_id")) == str(batch_id)
            ]
        rows.sort(key=lambda j: int(j.get("seq") or 0))
        return rows

    def job_summaries(
        self, batch_id: str
    ) -> tuple[dict[str, Any] | None, list[tuple[str, int, str, str, int, int, str, str]]]:
        """
        Dòng nhẹ cho UI: id, seq, tên, trạng thái, retry, max_retry, UID, link.

        Không chép log. Bảng hàng nghìn job chỉ cần các trường này.
        """
        with json_file_lock(self.path):
            data = self._doc_unlocked()
            batch = next((b for b in data["batches"] if str(b.get("id")) == str(batch_id)), None)
            jobs = [j for j in data["jobs"] if str(j.get("batch_id")) == str(batch_id)]
        jobs.sort(key=lambda j: int(j.get("seq") or 0))
        rows = [
            (
                str(j.get("id") or ""),
                int(j.get("seq") or 0),
                str(j.get("page_name") or ""),
                str(j.get("status") or ""),
                int(j.get("retry_count") or 0),
                int(j.get("max_retry") or 3),
                str(j.get("page_id") or ""),
                str(j.get("page_url") or ""),
            )
            for j in jobs
        ]
        return (_copy_row(batch) if isinstance(batch, dict) else None), rows

    def batch_view(self, batch_id: str) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """Một lần khóa cho UI: batch + job của batch đó."""
        with json_file_lock(self.path):
            data = self._doc_unlocked()
            batch = next((b for b in data["batches"] if str(b.get("id")) == str(batch_id)), None)
            jobs = [_copy_row(j) for j in data["jobs"] if str(j.get("batch_id")) == str(batch_id)]
        jobs.sort(key=lambda j: int(j.get("seq") or 0))
        return (_copy_row(batch) if isinstance(batch, dict) else None), jobs

    def list_batches(self) -> list[dict[str, Any]]:
        with json_file_lock(self.path):
            rows = [_copy_row(b) for b in self._doc_unlocked()["batches"]]
        rows.sort(key=lambda b: str(b.get("created_at") or ""))
        return rows

    def ui_prefs(self) -> dict[str, Any]:
        """Cài đặt form tạo Page (thư mục ảnh, ngôn ngữ, …) sống sau khi tắt dialog."""
        with json_file_lock(self.path):
            raw = self._doc_unlocked().get("ui")
        return dict(raw) if isinstance(raw, dict) else {}

    def save_ui_prefs(self, prefs: dict[str, Any], *, flush: bool = True) -> None:
        """Gộp và lưu prefs UI; không xóa khóa cũ nếu lần gọi không gửi đủ."""
        if not isinstance(prefs, dict):
            return
        with json_file_lock(self.path):
            data = self._doc_unlocked()
            current = data.get("ui") if isinstance(data.get("ui"), dict) else {}
            merged = dict(current)
            for key, value in prefs.items():
                if value is None:
                    continue
                merged[str(key)] = value
            data["ui"] = merged
            self._dirty = True
        if flush:
            self.flush(sync=True)

    def create_batch(
        self,
        *,
        account_id: str,
        business_id: str,
        pages: list[dict[str, str]],
        settings: dict[str, Any],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Tạo batch + job PENDING. Chưa chạy."""
        batch_id = uuid.uuid4().hex[:12]
        now = _now()
        jobs: list[dict[str, Any]] = []
        for index, page in enumerate(pages, start=1):
            jobs.append(
                {
                    "id": uuid.uuid4().hex[:12],
                    "batch_id": batch_id,
                    "seq": index,
                    "account_id": account_id,
                    "business_id": business_id,
                    "page_name": str(page.get("page_name") or "").strip(),
                    "avatar_path": str(page.get("avatar_path") or "").strip(),
                    "category": str(page.get("category") or "").strip(),
                    "bio": str(page.get("bio") or "").strip(),
                    "phone": str(page.get("phone") or "").strip(),
                    "email": str(page.get("email") or "").strip(),
                    "website": str(page.get("website") or "").strip(),
                    "address": str(page.get("address") or "").strip(),
                    "create_mode": str(settings.get("create_mode") or "bm"),
                    "admin_targets": list(settings.get("admin_targets") or []),
                    "status": JOB_PENDING,
                    "retry_count": 0,
                    "max_retry": int(settings.get("max_retry") or 3),
                    "page_id": "",
                    "page_url": "",
                    "error_code": "",
                    "error_message": "",
                    "created_at": now,
                    "started_at": "",
                    "finished_at": "",
                    "updated_at": now,
                }
            )
        batch = {
            "id": batch_id,
            "account_id": account_id,
            "business_id": business_id,
            "create_mode": str(settings.get("create_mode") or "bm"),
            "admin_targets": list(settings.get("admin_targets") or []),
            "total_jobs": len(jobs),
            "delay_enabled": bool(settings.get("delay_enabled", True)),
            "delay_mode": str(settings.get("delay_mode") or "fixed"),
            "delay_fixed_seconds": int(settings.get("delay_fixed_seconds") or 30),
            "delay_min_seconds": int(settings.get("delay_min_seconds") or 30),
            "delay_max_seconds": int(settings.get("delay_max_seconds") or 60),
            "max_retry": int(settings.get("max_retry") or 3),
            "continue_on_error": bool(settings.get("continue_on_error", True)),
            "pause_on_checkpoint": bool(settings.get("pause_on_checkpoint", True)),
            "pause_on_captcha": bool(settings.get("pause_on_captcha", True)),
            "pause_on_rate_limit": bool(settings.get("pause_on_rate_limit", True)),
            "auto_resume_after_restart": bool(settings.get("auto_resume_after_restart", False)),
            "account_creation_lock": "",
            "pause_reason": "",
            "status": "READY",
            "delay_remaining_seconds": 0,
            "resume_with_delay": False,
            "current_job_id": "",
            "created_at": now,
            "started_at": "",
            "finished_at": "",
            "updated_at": now,
        }
        with json_file_lock(self.path):
            data = self._doc_unlocked()
            data["batches"].append(_copy_row(batch))
            data["jobs"].extend(_copy_row(job) for job in jobs)
            self._dirty = True
        self.flush(sync=True)
        return batch, jobs

    def refresh_counts(self, batch_id: str, *, flush: bool = True, sync: bool = True) -> dict[str, Any] | None:
        with json_file_lock(self.path):
            data = self._doc_unlocked()
            batch = next((b for b in data["batches"] if str(b.get("id")) == str(batch_id)), None)
            if batch is None:
                return None
            jobs = [j for j in data["jobs"] if str(j.get("batch_id")) == str(batch_id)]

            def _n(status: str) -> int:
                return sum(1 for j in jobs if str(j.get("status")) == status)

            batch["total_jobs"] = len(jobs)
            batch["pending_jobs"] = _n(JOB_PENDING) + _n("DELAYED")
            batch["running_jobs"] = sum(
                1
                for j in jobs
                if str(j.get("status"))
                in {
                    "VALIDATING",
                    "CHECKING_SESSION",
                    "CREATING",
                    "UPLOADING_AVATAR",
                    "FILLING_DETAILS",
                    "ADDING_ADMIN",
                    "VERIFYING",
                    "RECOVERY_REQUIRED",
                }
            )
            batch["completed_jobs"] = _n("COMPLETED")
            batch["failed_jobs"] = _n("FAILED")
            batch["cancelled_jobs"] = _n("CANCELLED")
            batch["updated_at"] = _now()
            self._dirty = True
            snapshot = _copy_row(batch)
        if flush:
            self.flush(sync=sync)
        return snapshot
