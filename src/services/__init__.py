"""Facade mỏng bọc manager hiện tại — dùng dần thay import trực tiếp từ GUI/worker."""

from __future__ import annotations

from src.services.account_service import AccountService
from src.services.library_service import LibraryService
from src.services.page_service import PageService
from src.services.post_executor import PostExecutor
from src.services.post_history_service import PostHistoryService

# SchedulerService import scheduler → tránh nạp khi scheduler đang import post_executor (vòng import).
__all__ = (
    "AccountService",
    "AIContentService",
    "PageService",
    "LibraryService",
    "PostHistoryService",
    "SchedulerService",
    "PostExecutor",
)


def __getattr__(name: str):
    if name == "AIContentService":
        from src.services.ai_content_service import AIContentService

        return AIContentService
    if name == "SchedulerService":
        from src.services.scheduler_service import SchedulerService

        return SchedulerService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
