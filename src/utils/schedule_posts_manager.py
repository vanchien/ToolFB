"""
Hàng đợi job đăng bài tách khỏi ``pages.json`` — ``config/schedule_posts.json``.

Chưa nối scheduler; dùng làm nguồn dữ liệu cho UI / worker sau này.
"""

from __future__ import annotations

import json
import tempfile
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, TypedDict

from loguru import logger

from src.utils.paths import project_root


class SchedulePostJob(TypedDict, total=False):
    """Một job đăng bài trong hàng đợi (lịch + AI theo job; mở rộng theo PRD)."""

    id: str
    page_id: str
    account_id: str
    post_type: str
    page_post_style: str
    schedule_recurrence: str
    schedule_slot: str
    ai_topic: str
    ai_content_style: str
    ai_language: str
    ai_provider_text: str
    ai_provider_image: str
    ai_model_text: str
    ai_model_image: str
    job_post_image_path: str
    image_alt: str
    ai_config: dict[str, Any]
    title: str
    content: str
    hashtags: list[str]
    cta: str
    link: str
    media_files: list[str]
    scheduled_at: str
    timezone: str
    jitter_minutes: int
    status: str
    retry_count: int
    max_retry: int
    error_note: str
    created_by: str
    created_at: str
    posted_at: str
    draft_id: str
    # Lịch «theo khung giờ mỗi ngày» (batch / sửa job) — tùy chọn, để mở lại form đúng như lúc tạo
    schedule_daily_slots: str
    schedule_delay_min: int
    schedule_delay_max: int
    schedule_start_date: str
    slot_base_local: str
    schedule_delay_applied_min: int
    # Per-job override: hiển thị browser khi chạy job.
    # "inherit" | "hide" | "show" — mặc định "inherit" (theo toggle global ở manager_app).
    hide_browser: str


def _default_schedule_posts_path() -> Path:
    return project_root() / "config" / "schedule_posts.json"


class SchedulePostsManager:
    """Đọc/ghi ``schedule_posts.json`` — danh sách job đăng."""

    POST_TYPES: tuple[str, ...] = ("text", "image", "video", "text_image", "text_video")
    STATUSES: tuple[str, ...] = (
        "pending",
        "processing",
        "running",
        "success",
        "failed",
        "paused",
        "cancelled",
        "need_manual_check",
    )
    AI_TEXT_PROVIDERS: tuple[str, ...] = ("gemini", "openai")
    AI_IMAGE_PROVIDERS: tuple[str, ...] = ("gemini", "openai", "nanobanana")

    def __init__(self, json_path: Optional[Path | str] = None) -> None:
        self.file_path = Path(json_path).resolve() if json_path else _default_schedule_posts_path()
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._rows_cache: Optional[list[SchedulePostJob]] = None
        self._rows_mtime: Optional[float] = None
        if not self.file_path.is_file():
            self._atomic_write(json.dumps([], ensure_ascii=False, indent=2) + "\n")
            logger.info("Đã tạo schedule_posts.json rỗng: {}", self.file_path)

    def _atomic_write(self, text: str) -> None:
        d = self.file_path.parent
        fd, tmp = tempfile.mkstemp(prefix="schedule_posts_", suffix=".tmp.json", dir=str(d))
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

    def _invalidate_cache(self) -> None:
        self._rows_cache = None
        self._rows_mtime = None

    def reload_from_disk(self) -> list[SchedulePostJob]:
        self._invalidate_cache()
        return self.load_all()

    def _validate_row(self, row: dict[str, Any]) -> None:
        pid = str(row.get("page_id", "")).strip()
        aid = str(row.get("account_id", "")).strip()
        if not pid:
            raise ValueError("Thiếu page_id.")
        if not aid:
            raise ValueError("Thiếu account_id.")
        pt = str(row.get("post_type", "text")).strip().lower()
        if pt not in self.POST_TYPES:
            raise ValueError(f"post_type phải là một trong: {', '.join(self.POST_TYPES)}")
        row["post_type"] = pt
        st = str(row.get("status", "pending")).strip().lower()
        if st not in self.STATUSES:
            raise ValueError(f"status phải là một trong: {', '.join(self.STATUSES)}")
        row["status"] = st
        rc = row.get("retry_count", 0)
        try:
            row["retry_count"] = max(0, int(rc))
        except (TypeError, ValueError):
            row["retry_count"] = 0
        mr = row.get("max_retry", 3)
        try:
            row["max_retry"] = max(1, min(50, int(mr)))
        except (TypeError, ValueError):
            row["max_retry"] = 3
        tprov = str(row.get("ai_provider_text", "gemini") or "gemini").strip().lower()
        iprov = str(row.get("ai_provider_image", "gemini") or "gemini").strip().lower()
        if tprov not in self.AI_TEXT_PROVIDERS:
            tprov = "gemini"
        if iprov not in self.AI_IMAGE_PROVIDERS:
            iprov = "gemini"
        row["ai_provider_text"] = tprov
        row["ai_provider_image"] = iprov
        row["ai_model_text"] = str(row.get("ai_model_text", "") or "").strip()
        row["ai_model_image"] = str(row.get("ai_model_image", "") or "").strip()

    def validate_record(self, row: dict[str, Any]) -> None:
        d = dict(row)
        if not str(d.get("id", "")).strip():
            d["id"] = uuid.uuid4().hex[:16]
        self._validate_row(d)

    def load_all(self) -> list[SchedulePostJob]:
        if not self.file_path.is_file():
            self._invalidate_cache()
            raise FileNotFoundError(str(self.file_path))
        mtime = self.file_path.stat().st_mtime
        if self._rows_cache is not None and self._rows_mtime == mtime:
            return list(self._rows_cache)

        raw = json.loads(self.file_path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError("schedule_posts.json phải là mảng.")
        out: list[SchedulePostJob] = []
        for i, item in enumerate(raw):
            if not isinstance(item, dict):
                raise ValueError(f"Phần tử {i} không phải object.")
            self._validate_row(item)
            out.append(item)  # type: ignore[arg-type]
        self._rows_cache = out
        self._rows_mtime = mtime
        logger.debug("schedule_posts load_all: {} job", len(out))
        return list(out)

    def save_all(self, rows: Iterable[SchedulePostJob]) -> None:
        lst = list(rows)
        for r in lst:
            self._validate_row(dict(r))
        self._atomic_write(json.dumps(lst, ensure_ascii=False, indent=2) + "\n")
        self._rows_cache = list(lst)
        self._rows_mtime = self.file_path.stat().st_mtime
        logger.info("Đã ghi {} job schedule_posts vào {}", len(lst), self.file_path)

    def upsert(self, row: SchedulePostJob) -> None:
        d: dict[str, Any] = dict(row)
        if not str(d.get("id", "")).strip():
            d["id"] = uuid.uuid4().hex[:16]
        self._validate_row(d)
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        if not str(d.get("created_at", "")).strip():
            d["created_at"] = now
        jid = str(d["id"])
        cur = self.load_all()
        replaced = False
        new_list: list[SchedulePostJob] = []
        for x in cur:
            if str(x.get("id")) == jid:
                new_list.append(d)  # type: ignore[arg-type]
                replaced = True
            else:
                new_list.append(x)
        if not replaced:
            new_list.append(d)  # type: ignore[arg-type]
        self.save_all(new_list)

    def delete_by_id(self, job_id: str) -> bool:
        jid = str(job_id).strip()
        cur = self.load_all()
        new_list = [x for x in cur if str(x.get("id")) != jid]
        if len(new_list) == len(cur):
            return False
        self.save_all(new_list)
        return True

    def delete_by_ids(self, job_ids: Iterable[str]) -> tuple[int, list[str]]:
        """Xóa nhiều job một lần (một lần ghi file). Trả về (số đã xóa, id không tồn tại)."""
        want = list(dict.fromkeys(str(x).strip() for x in job_ids if str(x).strip()))
        if not want:
            return (0, [])
        want_set = set(want)
        cur = self.load_all()
        existing = {str(x.get("id", "")).strip() for x in cur}
        new_list = [x for x in cur if str(x.get("id", "")).strip() not in want_set]
        removed = len(cur) - len(new_list)
        if removed:
            self.save_all(new_list)
        missing = [j for j in want if j not in existing]
        return (removed, missing)

    def get_by_id(self, job_id: str) -> Optional[SchedulePostJob]:
        jid = str(job_id).strip()
        for x in self.load_all():
            if str(x.get("id")) == jid:
                return x
        return None

    def list_for_page(self, page_id: str) -> list[SchedulePostJob]:
        pid = str(page_id).strip()
        return [x for x in self.load_all() if str(x.get("page_id", "")).strip() == pid]

    def list_for_account(self, account_id: str) -> list[SchedulePostJob]:
        aid = str(account_id).strip()
        return [x for x in self.load_all() if str(x.get("account_id", "")).strip() == aid]

    def list_by_status(self, status: str) -> list[SchedulePostJob]:
        st = str(status).strip().lower()
        return [x for x in self.load_all() if str(x.get("status", "")).strip().lower() == st]

    def update_job_fields(self, job_id: str, **fields: Any) -> bool:
        """Merge các trường vào job có ``id`` = ``job_id``."""
        jid = str(job_id).strip()
        if not jid:
            return False
        cur = self.load_all()
        new_list: list[SchedulePostJob] = []
        found = False
        for x in cur:
            if str(x.get("id")) != jid:
                new_list.append(x)
                continue
            found = True
            merged: dict[str, Any] = dict(x)
            for k, v in fields.items():
                if v is None:
                    merged.pop(k, None)
                else:
                    merged[k] = v
            self._validate_row(merged)
            new_list.append(merged)  # type: ignore[arg-type]
        if not found:
            return False
        self.save_all(new_list)
        return True


_default_sp_lock = threading.Lock()
_default_sp: Optional[SchedulePostsManager] = None


def get_default_schedule_posts_manager() -> SchedulePostsManager:
    global _default_sp
    with _default_sp_lock:
        if _default_sp is None:
            _default_sp = SchedulePostsManager()
        return _default_sp
