"""
Sinh / lên lịch nội dung theo Page (skeleton — mở rộng plan 7 ngày, ảnh, queue).

Không có ``GEMINI_API_KEY``: các hàm sinh caption trả về chuỗi stub để test không gọi API.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from loguru import logger

from src.ai.content_creator import GeneratedPost, generate_post
from src.services.library_service import LibraryService
from src.services.page_service import PageService
from src.utils.page_workspace import page_workspace_root, sanitize_page_id


class AIContentService:
    """Đọc ``PageAIConfig``, gọi Gemini khi có key, lưu draft text trong workspace Page."""

    def __init__(
        self,
        *,
        pages: PageService | None = None,
        library: LibraryService | None = None,
    ) -> None:
        self._pages = pages or PageService()
        self._library = library or LibraryService()

    def load_ai_config(self, page_id: str) -> dict[str, Any]:
        return self._pages.load_ai_config(page_id)

    def generate_body_and_alt(self, topic: str, *, style: str | None = None) -> GeneratedPost:
        """Một bài đầy đủ (body + image_alt) — thin wrapper ``content_creator.generate_post``."""
        return generate_post(topic, style=style)

    def suggest_captions(self, page_id: str, topic: str, *, count: int = 3) -> list[str]:
        """
        3–5 gợi ý caption (stub nếu không có API key; nếu có key thì gọi Gemini ``count`` lần).
        """
        n = max(1, min(5, int(count)))
        if not os.environ.get("GEMINI_API_KEY", "").strip():
            logger.debug("GEMINI_API_KEY trống — trả caption stub cho Page {}.", page_id)
            return [f"[stub {i + 1}/{n}] {topic}".strip() for i in range(n)]
        cfg = self.load_ai_config(page_id)
        style = str(cfg.get("brand_voice") or "").strip() or None
        out: list[str] = []
        for i in range(n):
            g = generate_post(topic if i == 0 else f"{topic} (biến thể {i + 1})", style=style)
            t = str(g.get("body", "")).strip()
            if t:
                out.append(t)
        return out if out else [f"[stub] {topic}"]

    def plan_week_stub(self, page_id: str) -> list[dict[str, Any]]:
        """Stub kế hoạch 7 ngày (chưa đọc model multi-step)."""
        sid = sanitize_page_id(page_id)
        return [{"day_index": i, "page_id": sid, "note": "stub"} for i in range(7)]

    def save_caption_as_draft_file(self, page_id: str, caption: str, *, stem: str = "ai_draft") -> Path:
        """Lưu ``.txt`` vào ``library/drafts/`` (bản nháp đơn giản)."""
        self._library.ensure_structure(page_id)
        sid = sanitize_page_id(page_id)
        d = page_workspace_root(sid) / "library" / "drafts"
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{stem}.txt"
        path.write_text(caption.strip() + "\n", encoding="utf-8")
        logger.info("Đã lưu draft caption → {}", path)
        return path
