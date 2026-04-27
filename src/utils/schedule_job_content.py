"""
Hợp nhất cấu hình AI/lịch từ job queue (``schedule_posts.json``) với bản ghi Page.

Dùng bởi ``scheduler.run_scheduled_post_for_account`` khi có ``schedule_post_job_id``.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

from src.utils.page_schedule import parse_cron_hh_mm, scheduler_tz


def _parse_iso_to_aware_utc(raw: str) -> datetime:
    s = str(raw or "").strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_next_daily_scheduled_utc_iso(schedule_slot: str, *, from_utc: datetime | None = None) -> str:
    """Lần chạy tiếp theo (UTC ISO) cho lịch ``HH:MM`` mỗi ngày theo ``SCHEDULER_TZ``."""
    tz = scheduler_tz()
    now_u = from_utc or datetime.now(timezone.utc)
    now_l = now_u.astimezone(tz)
    h, m = parse_cron_hh_mm(schedule_slot)
    cand = now_l.replace(hour=h, minute=m, second=0, microsecond=0)
    if cand <= now_l:
        cand = cand + timedelta(days=1)
    return cand.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def once_local_wall_to_utc_iso(once_local_yyyy_mm_dd_hh_mm: str) -> str:
    """``YYYY-MM-DD HH:MM`` (wall ``SCHEDULER_TZ``) → UTC ISO."""
    from src.utils.page_schedule import parse_once_local

    tz = scheduler_tz()
    dt = parse_once_local(once_local_yyyy_mm_dd_hh_mm, tz)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def merge_queue_job_content_into_page_row(
    page_row: dict[str, Any] | None,
    queue_job: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Job có ``ai_*`` / ``ai_config`` / ``job_post_image_path`` → ghi đè trường tương ứng trên Page (bản sao)."""
    if not page_row:
        return page_row
    if not queue_job:
        return page_row
    out = dict(page_row)
    if str(queue_job.get("ai_topic", "")).strip():
        out["topic"] = str(queue_job["ai_topic"]).strip()
    if str(queue_job.get("ai_content_style", "")).strip():
        out["content_style"] = str(queue_job["ai_content_style"]).strip()
    if str(queue_job.get("job_post_image_path", "")).strip():
        out["post_image_path"] = str(queue_job["job_post_image_path"]).strip()
    cfg = queue_job.get("ai_config")
    if isinstance(cfg, dict):
        parts: list[str] = []
        bv = str(cfg.get("brand_voice", "")).strip()
        if bv:
            parts.append(bv)
        ta = str(cfg.get("target_audience", "")).strip()
        if ta:
            parts.append(f"Đối tượng đọc: {ta}")
        pl = cfg.get("content_pillars")
        if isinstance(pl, list) and pl:
            parts.append("Trụ cột nội dung: " + ", ".join(str(x) for x in pl[:8]))
        av = cfg.get("avoid_keywords")
        if isinstance(av, list) and av:
            parts.append("Không dùng từ: " + ", ".join(str(x) for x in av[:12]))
        if parts:
            base_style = str(out.get("content_style", "")).strip()
            out["content_style"] = " | ".join(parts + ([base_style] if base_style else []))
    return out


def deserialize_job_schedule_for_ui(job: dict[str, Any]) -> tuple[Literal["once", "daily"], date, int, int]:
    """Đổ widget lịch từ job đã lưu."""
    rec = str(job.get("schedule_recurrence", "")).strip().lower()
    slot = str(job.get("schedule_slot", "")).strip()
    tz = scheduler_tz()
    if rec == "daily" and slot:
        h, m = parse_cron_hh_mm(slot)
        return ("daily", datetime.now(tz).date(), h, m)
    raw = str(job.get("scheduled_at", "")).strip()
    if raw:
        dtu = _parse_iso_to_aware_utc(raw)
        loc = dtu.astimezone(tz)
        return ("once", loc.date(), loc.hour, loc.minute)
    return ("once", datetime.now(tz).date(), 9, 0)


def build_schedule_slot_hhmm(hour: int, minute: int) -> str:
    from src.utils.page_schedule import normalize_hh_mm

    return normalize_hh_mm(hour, minute)
