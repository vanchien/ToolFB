"""
Lưu bản thảo nội dung (Content Studio) dưới dạng JSON trong ``data/drafts/``.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict

from loguru import logger

from src.utils.paths import project_root


class DraftRecord(TypedDict, total=False):
    """Một bản thảo đã sinh hoặc chỉnh tay."""

    id: str
    topic: str
    body: str
    image_alt: str
    media_paths: list[str]
    created_at: str


def drafts_dir() -> Path:
    """
    Thư mục chứa file ``*.draft.json``.
    """
    d = project_root() / "data" / "drafts"
    d.mkdir(parents=True, exist_ok=True)
    return d


def list_drafts() -> list[DraftRecord]:
    """
    Liệt kê tất cả bản thảo (đọc từng file).

    Returns:
        Danh sách metadata + nội dung đầy đủ, sắp theo ``created_at`` giảm dần.
    """
    rows: list[DraftRecord] = []
    for p in sorted(drafts_dir().glob("*.draft.json"), reverse=True):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict) and "id" in data:
                rows.append(data)  # type: ignore[arg-type]
        except Exception as exc:  # noqa: BLE001
            logger.warning("Bỏ qua draft lỗi {}: {}", p, exc)
    rows.sort(key=lambda r: str(r.get("created_at", "")), reverse=True)
    return rows


def save_draft(
    *,
    topic: str,
    body: str,
    image_alt: str,
    media_paths: list[str] | None = None,
    draft_id: str | None = None,
) -> DraftRecord:
    """
    Lưu hoặc ghi đè một bản thảo.

    Args:
        topic: Chủ đề / từ khóa.
        body: Nội dung bài.
        image_alt: Gợi ý alt-text.
        media_paths: Đường dẫn media tương đối gốc dự án (tùy chọn).
        draft_id: Nếu có thì cập nhật file cùng id; không thì tạo mới.

    Returns:
        Bản ghi đã lưu.
    """
    did = (draft_id or uuid.uuid4().hex[:16]).strip()
    rec: DraftRecord = {
        "id": did,
        "topic": topic.strip(),
        "body": body.strip(),
        "image_alt": (image_alt or "").strip(),
        "media_paths": list(media_paths or []),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    path = drafts_dir() / f"{did}.draft.json"
    path.write_text(json.dumps(rec, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logger.info("Đã lưu draft id={}", did)
    return rec


def load_draft(draft_id: str) -> DraftRecord | None:
    """
    Đọc một bản thảo theo ``id``.
    """
    path = drafts_dir() / f"{draft_id.strip()}.draft.json"
    if not path.is_file():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return data  # type: ignore[return-value]
    return None


def delete_draft(draft_id: str) -> bool:
    """
    Xóa file bản thảo.
    """
    path = drafts_dir() / f"{draft_id.strip()}.draft.json"
    if path.is_file():
        path.unlink()
        logger.info("Đã xóa draft {}", draft_id)
        return True
    return False
