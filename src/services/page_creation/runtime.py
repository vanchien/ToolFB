"""Engine dùng chung cho GUI. Batch dở không tự chạy khi mở app."""

from __future__ import annotations

import threading
import uuid
from typing import Any

from .store import PageCreationStore

_guard = threading.Lock()
_engine: PageCreationEngine | None = None


def record_created_page(job: dict[str, Any]) -> None:
    """Ghi Page đã xác minh vào ``pages.json``. Gọi sau khi job COMPLETED."""
    from src.utils.pages_manager import PagesManager

    page_id = str(job.get("page_id") or "").strip()
    page_url = str(job.get("page_url") or "").strip()
    if page_id and not page_url.startswith("http"):
        page_url = f"https://www.facebook.com/{page_id}"
    if not page_url.startswith("http"):
        return
    manager = PagesManager()
    name = str(job.get("page_name") or "").strip()
    account_id = str(job.get("account_id") or "").strip()
    business_id = str(job.get("business_id") or "").strip()
    for row in manager.load_all():
        same_id = page_id and str(row.get("fb_page_id") or "") == page_id
        same_name = (
            str(row.get("page_name") or "").strip().casefold() == name.casefold()
            and str(row.get("account_id") or "") == account_id
            and str(row.get("business_id") or "") == business_id
        )
        if not same_id and not same_name:
            continue
        row["page_name"] = name or row.get("page_name", "")
        row["page_url"] = page_url
        if page_id:
            row["fb_page_id"] = page_id
        row["business_id"] = business_id
        row["account_id"] = account_id
        row["source"] = "page_creator"
        manager.upsert(row)
        return
    manager.upsert(
        {
            "id": uuid.uuid4().hex[:12],
            "account_id": account_id,
            "page_name": name,
            "page_url": page_url,
            "fb_page_id": page_id,
            "business_id": business_id,
            "page_kind": "fanpage",
            "post_style": "post",
            "status": "active",
            "source": "page_creator",
        }
    )


def get_engine():
    """Một engine cho process GUI để pause/resume trúng đúng batch đang chạy."""
    global _engine
    with _guard:
        if _engine is None:
            from .engine import PageCreationEngine
            from .facebook_provider import FacebookPageCreationProvider

            _engine = PageCreationEngine(
                PageCreationStore(),
                FacebookPageCreationProvider(),
                on_completed=record_created_page,
            )
        return _engine


def resume_incomplete_batches() -> list[str]:
    """Không tự chạy khi mở app. Batch dở chỉ chạy khi người dùng bấm Tiếp tục hoặc Tạo tất cả."""
    return []
