"""Hàng đợi job đăng + một tick quét job đến hạn (ủy quyền manager + ``scheduler``)."""

from __future__ import annotations

from typing import Any, Iterable, Optional

from src.models.post_job import SchedulePostJob
from src.scheduler import tick_schedule_post_jobs
from src.utils.schedule_posts_manager import SchedulePostsManager, get_default_schedule_posts_manager


class SchedulerService:
    """CRUD ``schedule_posts.json`` + ``tick_schedule_post_jobs``."""

    def __init__(self, manager: Optional[SchedulePostsManager] = None) -> None:
        self._mgr = manager or get_default_schedule_posts_manager()

    def reload_from_disk(self) -> list[SchedulePostJob]:
        return self._mgr.reload_from_disk()

    def load_all(self) -> list[SchedulePostJob]:
        return self._mgr.load_all()

    def get_by_id(self, job_id: str) -> Optional[SchedulePostJob]:
        return self._mgr.get_by_id(job_id)

    def upsert(self, row: SchedulePostJob) -> None:
        self._mgr.upsert(row)

    def save_all(self, rows: Iterable[SchedulePostJob]) -> None:
        self._mgr.save_all(rows)

    def delete_by_id(self, job_id: str) -> bool:
        return self._mgr.delete_by_id(job_id)

    def update_job_fields(self, job_id: str, **fields: Any) -> bool:
        return self._mgr.update_job_fields(job_id, **fields)

    def poll_due_jobs(self) -> None:
        """Một vòng quét pending đến hạn → gọi PostExecutor qua ``scheduler``."""
        tick_schedule_post_jobs()
