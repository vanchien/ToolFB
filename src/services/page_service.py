"""Quản lý Page + workspace + cấu hình AI theo Page."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional

from src.models.page import PageRecord
from src.utils.page_workspace import (
    ensure_page_workspace,
    load_page_ai_config,
    page_workspace_root,
    save_page_ai_config,
)
from src.utils.pages_manager import PagesManager, get_default_pages_manager


class PageService:
    """Kết hợp ``PagesManager`` và ``page_workspace``."""

    def __init__(self, pages: Optional[PagesManager] = None) -> None:
        self._pages = pages or get_default_pages_manager()

    def reload_from_disk(self) -> list[PageRecord]:
        return self._pages.reload_from_disk()

    def load_all(self) -> list[PageRecord]:
        return self._pages.load_all()

    def get_by_id(self, page_id: str) -> Optional[PageRecord]:
        return self._pages.get_by_id(page_id)

    def upsert(self, row: PageRecord) -> None:
        self._pages.upsert(row)

    def save_all(self, rows: Iterable[PageRecord]) -> None:
        self._pages.save_all(rows)

    def delete_by_id(self, page_id: str) -> bool:
        return self._pages.delete_by_id(page_id)

    def ensure_workspace(self, page_id: str) -> Path:
        return ensure_page_workspace(page_id)

    def workspace_root(self, page_id: str) -> Path:
        return page_workspace_root(page_id)

    def load_ai_config(self, page_id: str) -> dict[str, Any]:
        return load_page_ai_config(page_id)

    def save_ai_config(self, page_id: str, data: dict[str, Any]) -> None:
        save_page_ai_config(page_id, data)
