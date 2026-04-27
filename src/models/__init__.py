"""Kiểu dữ liệu domain (TypedDict / alias) — tách khỏi tầng service."""

from src.models.account import AccountRecord
from src.models.page import PageRecord
from src.models.page_ai_config import PageAIConfigRecord
from src.models.media_item import MediaImportMeta, MediaKind
from src.models.post_job import SchedulePostJob

__all__ = (
    "AccountRecord",
    "PageRecord",
    "PageAIConfigRecord",
    "SchedulePostJob",
    "MediaKind",
    "MediaImportMeta",
)
