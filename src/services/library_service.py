"""
Thư viện nội dung theo Page: import file, metadata, (sau này) chọn ngẫu nhiên / history.

Đường dẫn: ``data/pages/<page_id>/library/{texts,images,videos}/``.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Optional

from loguru import logger

from src.models.media_item import MediaImportMeta, MediaKind
from src.utils.page_workspace import ensure_page_workspace, page_workspace_root, sanitize_page_id

if TYPE_CHECKING:
    from src.services.post_history_service import PostHistoryService

_TEXT_EXT = frozenset({".txt", ".md"})
_IMAGE_EXT = frozenset({".jpg", ".jpeg", ".png", ".webp", ".gif"})
_VIDEO_EXT = frozenset({".mp4", ".webm", ".mov", ".mkv"})


def _subdir_for_kind(kind: MediaKind) -> str:
    return {"text": "texts", "image": "images", "video": "videos"}[kind]


def _allowed_ext(kind: MediaKind) -> frozenset[str]:
    if kind == "text":
        return _TEXT_EXT
    if kind == "image":
        return _IMAGE_EXT
    return _VIDEO_EXT


def _unique_dest(dest_dir: Path, original_name: str) -> Path:
    base = Path(original_name).name
    stem = Path(base).stem
    suf = Path(base).suffix
    candidate = dest_dir / base
    n = 1
    while candidate.exists():
        candidate = dest_dir / f"{stem}_{n}{suf}"
        n += 1
    return candidate


class LibraryService:
    """Import + metadata JSON cạnh file (``*.import.meta.json``) + chọn file theo history."""

    def ensure_structure(self, page_id: str) -> Path:
        return ensure_page_workspace(page_id)

    def list_media_paths(self, page_id: str, kind: MediaKind) -> list[Path]:
        """Danh sách file trong thư mục thư viện (bỏ ``*.import.meta.json``)."""
        sid = sanitize_page_id(page_id)
        d = page_workspace_root(sid) / "library" / _subdir_for_kind(kind)
        if not d.is_dir():
            return []
        allowed = _allowed_ext(kind)
        out: list[Path] = []
        for p in sorted(d.iterdir()):
            if not p.is_file():
                continue
            if p.name.endswith(".import.meta.json"):
                continue
            if p.suffix.lower() not in allowed:
                continue
            out.append(p)
        return out

    def infer_text_topic(self, text_file: Path) -> str:
        """Ưu tiên ``topic`` trong ``*.import.meta.json``, không có thì dùng stem tên file."""
        meta = text_file.parent / f"{text_file.stem}.import.meta.json"
        if meta.is_file():
            try:
                raw = json.loads(meta.read_text(encoding="utf-8"))
                t = str(raw.get("topic", "")).strip()
                if t:
                    return t
            except (OSError, json.JSONDecodeError):
                pass
        return text_file.stem

    def pick_random_eligible_image(
        self,
        page_id: str,
        *,
        history: Optional["PostHistoryService"] = None,
    ) -> Optional[Path]:
        """
        Chọn ngẫu nhiên ảnh chưa dùng trong ``history.image_cooldown_days`` (mặc định 14).
        ``history`` None = bỏ qua rule, chọn ngẫu nhiên trong thư mục.
        """
        from src.services.post_history_service import PostHistoryService, pick_random_path

        paths = self.list_media_paths(page_id, "image")
        if not paths:
            return None
        h = history or PostHistoryService()
        eligible = [p for p in paths if not h.was_image_used_within_days(page_id, p)]
        pool = eligible if eligible else paths
        return pick_random_path(pool)

    def pick_random_eligible_text(
        self,
        page_id: str,
        *,
        history: Optional["PostHistoryService"] = None,
    ) -> Optional[Path]:
        """
        Chọn file text ngẫu nhiên, ưu tiên các file có ``topic``/stem chưa bị hook cooldown
        (``history.hook_cooldown_days``, mặc định 7 ngày).
        """
        from src.services.post_history_service import PostHistoryService, pick_random_path

        paths = self.list_media_paths(page_id, "text")
        if not paths:
            return None
        if history is None:
            return pick_random_path(paths)
        eligible = [p for p in paths if not history.was_hook_used_within_days(page_id, self.infer_text_topic(p))]
        pool = eligible if eligible else paths
        return pick_random_path(pool)

    def import_file(self, page_id: str, src: Path | str, kind: MediaKind) -> Path:
        """
        Kiểm tra tồn tại + đuôi file, copy vào thư viện Page, ghi metadata.

        Raises:
            ValueError: file không tồn tại / sai định dạng / page_id không hợp lệ.
        """
        sid = sanitize_page_id(page_id)
        p = Path(src).expanduser().resolve()
        if not p.is_file():
            raise ValueError(f"File không tồn tại: {p}")
        ext = p.suffix.lower()
        allowed = _allowed_ext(kind)
        if ext not in allowed:
            raise ValueError(f"Định dạng {ext!r} không hợp lệ cho kind={kind!r} (chấp nhận: {sorted(allowed)}).")

        self.ensure_structure(sid)
        dest_dir = page_workspace_root(sid) / "library" / _subdir_for_kind(kind)
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = _unique_dest(dest_dir, p.name)
        shutil.copy2(p, dest)
        size = dest.stat().st_size
        imported_at = datetime.now(timezone.utc).isoformat()
        meta: MediaImportMeta = {
            "kind": kind,
            "page_id": sid,
            "source_path": str(p),
            "stored_path": str(dest),
            "original_name": p.name,
            "imported_at": imported_at,
            "size_bytes": size,
        }
        meta_path = dest.parent / f"{dest.stem}.import.meta.json"
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        logger.info("Đã import {} → {} (metadata {})", p, dest, meta_path.name)
        return dest
