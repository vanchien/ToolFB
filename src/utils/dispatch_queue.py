"""
Hàng đợi đăng bài (Pending / Processing / Failed) — ``data/dispatch_queue.json``.
"""

from __future__ import annotations

import json
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, TypedDict

from loguru import logger

from src.utils.paths import project_root


class QueueJob(TypedDict, total=False):
    """Một job trong hàng đợi điều phối."""

    id: str
    account_id: str
    entity_id: str
    draft_id: str
    status: str
    scheduled_at: str
    created_at: str
    error_message: str


def _queue_path() -> Path:
    p = project_root() / "data" / "dispatch_queue.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


class DispatchQueueStore:
    """Lưu trữ JSON đơn giản cho dashboard & automation."""

    STATUSES: tuple[str, ...] = ("pending", "processing", "failed", "done")

    def __init__(self, path: Optional[Path] = None) -> None:
        """
        Args:
            path: File JSON hàng đợi (mặc định ``data/dispatch_queue.json``).
        """
        self.file_path = path or _queue_path()
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.file_path.is_file():
            self._write_raw([])

    def _write_raw(self, data: list[Any]) -> None:
        text = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
        d = self.file_path.parent
        fd, tmp = tempfile.mkstemp(prefix="queue_", suffix=".tmp.json", dir=str(d))
        try:
            import os

            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.file_path)
        except Exception:
            try:
                import os

                os.unlink(tmp)
            except OSError:
                pass
            raise

    def load_all(self) -> list[QueueJob]:
        """Đọc toàn bộ job (không validate nghiêm để tương thích bản cũ)."""
        if not self.file_path.is_file():
            return []
        raw = json.loads(self.file_path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            return []
        return [x for x in raw if isinstance(x, dict)]  # type: ignore[return-value]

    def save_all(self, jobs: Iterable[QueueJob]) -> None:
        """Ghi đè danh sách."""
        lst = list(jobs)
        self._write_raw(lst)
        logger.info("Đã ghi {} job hàng đợi.", len(lst))

    def append_job(
        self,
        *,
        account_id: str,
        entity_id: str = "",
        draft_id: str = "",
        status: str = "pending",
        scheduled_at: str | None = None,
    ) -> QueueJob:
        """
        Thêm job mới (ví dụ chạy khẩn cấp hoặc lịch sinh tự động sau này).

        Args:
            account_id: Tài khoản thực thi.
            entity_id: Đích Page/Group (có thể rỗng nếu pipeline cũ).
            draft_id: Bản thảo nội dung (có thể rỗng).
            status: ``pending`` / ``processing`` / …
            scheduled_at: ISO thời điểm hẹn (mặc định ``now``).

        Returns:
            Bản ghi job đã thêm.
        """
        jobs = self.load_all()
        now = datetime.now(timezone.utc).isoformat()
        job: QueueJob = {
            "id": uuid.uuid4().hex[:16],
            "account_id": account_id.strip(),
            "entity_id": str(entity_id).strip(),
            "draft_id": str(draft_id).strip(),
            "status": status,
            "scheduled_at": scheduled_at or now,
            "created_at": now,
        }
        jobs.append(job)
        self.save_all(jobs)
        return job

    def update_job(self, job_id: str, **fields: Any) -> bool:
        """Cập nhật các trường của một job theo ``id``."""
        jobs = self.load_all()
        found = False
        new_list: list[QueueJob] = []
        for j in jobs:
            if str(j.get("id")) == job_id:
                merged = dict(j)
                merged.update(fields)
                new_list.append(merged)  # type: ignore[arg-type]
                found = True
            else:
                new_list.append(j)
        if found:
            self.save_all(new_list)
        return found
