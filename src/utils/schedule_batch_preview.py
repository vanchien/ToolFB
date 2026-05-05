"""
Logic preview / lịch chuỗi cho «Thêm batch job lịch đăng» (Tkinter).

Không phụ thuộc GUI — dễ test.
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from src.utils.page_schedule import scheduler_tz

ScheduleRule = Literal["immediate", "once", "daily", "interval"]

VIDEO_EXTENSIONS = frozenset({".mp4", ".mov", ".avi", ".mkv", ".webm"})

CONTENT_KIND_TO_POST_TYPE: dict[str, str] = {
    "text": "text",
    "image": "image",
    "video": "video",
    "text_image": "text_image",
    "text_video": "text_video",
}

POST_TYPE_TO_PAGE_STYLE: dict[str, str] = {
    "text": "post",
    "image": "image",
    "video": "video",
    "text_image": "image",
    "text_video": "video",
    "reel": "video",
}


def post_type_for_kind(kind_key: str) -> str:
    return CONTENT_KIND_TO_POST_TYPE.get(str(kind_key).strip().lower(), "text")


def page_post_style_for_post_type(post_type: str) -> str:
    return POST_TYPE_TO_PAGE_STYLE.get(str(post_type).strip().lower(), "post")


def scan_video_files(folder: Path, *, sort: str = "name") -> list[Path]:
    """Quét video trong thư mục (không đệ quy). ``sort``: name | mtime | random | duration (duration → coi như name)."""
    root = Path(folder).expanduser().resolve()
    if not root.is_dir():
        return []
    files: list[Path] = []
    for p in root.iterdir():
        if p.is_file() and p.suffix.lower() in VIDEO_EXTENSIONS:
            files.append(p)
    sk = str(sort or "name").strip().lower()
    if sk == "mtime":
        files.sort(key=lambda x: x.stat().st_mtime_ns)
    elif sk == "random":
        random.shuffle(files)
    else:
        files.sort(key=lambda x: x.name.lower())
    return files


def compute_scheduled_at_series(
    count: int,
    rule: ScheduleRule,
    *,
    start_date: date,
    hour: int,
    minute: int,
    interval_unit: Literal["hours", "days"],
    interval_value: int,
    jitter_max_min: int,
) -> list[str]:
    """
    Sinh ``count`` mốc ``scheduled_at`` (ISO UTC).

    - ``immediate``: từ bây giờ, cách nhau theo ``interval_*`` (mặc định 0 = cùng lúc + jitter).
    - ``once``: neo ``start_date`` + giờ; mỗi bài cách ``interval_value`` giờ hoặc ngày.
    - ``daily``: cùng giờ các ngày liên tiếp từ neo (neo không quá khứ thì giữ).
    - ``interval``: giống ``once`` (alias).
    """
    if count < 1:
        return []
    tz = scheduler_tz()
    rnd = random.Random()
    iv = max(0, int(interval_value))
    jm = max(0, int(jitter_max_min))

    def jitter_local(dt: datetime) -> datetime:
        if jm <= 0:
            return dt
        return dt + timedelta(minutes=rnd.randint(0, jm))

    def to_iso(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

    out: list[str] = []

    if rule == "immediate":
        base = datetime.now(tz).replace(second=0, microsecond=0)
        for i in range(count):
            delta = timedelta(0)
            if iv > 0:
                if interval_unit == "hours":
                    delta = timedelta(hours=iv * i)
                else:
                    delta = timedelta(days=iv * i)
            dt = jitter_local(base + delta)
            out.append(to_iso(dt))
        return out

    anchor = datetime(start_date.year, start_date.month, start_date.day, hour, minute, 0, tzinfo=tz)
    now = datetime.now(tz)

    if rule == "daily":
        cur = anchor
        if cur < now:
            cur = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if cur < now:
                cur = cur + timedelta(days=1)
        for i in range(count):
            dt = jitter_local(cur + timedelta(days=i))
            out.append(to_iso(dt))
        return out

    # once | interval
    cur = anchor
    if cur < now:
        cur = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    step = max(1, iv) if iv else 1
    for i in range(count):
        dt = jitter_local(cur)
        out.append(to_iso(dt))
        if interval_unit == "hours":
            cur = cur + timedelta(hours=step)
        else:
            cur = cur + timedelta(days=step)
    return out


def build_schedule_by_daily_slots(
    start_date: date,
    time_slots: list[str],
    job_count: int,
    delay_min_minutes: int = 0,
    delay_max_minutes: int = 0,
    timezone_name: str = "Asia/Ho_Chi_Minh",
) -> list[dict[str, Any]]:
    """
    Tạo danh sách scheduled_at theo các khung giờ cố định trong ngày.

    - Hết slot trong ngày sẽ tự nhảy sang ngày kế.
    - Delay random nằm trong đoạn [delay_min_minutes, delay_max_minutes].
    - Delay chỉ cộng theo hướng trễ thêm.
    """
    if job_count < 1:
        return []
    slots: list[tuple[int, int]] = []
    for raw in time_slots:
        s = str(raw).strip()
        if not s:
            continue
        parts = s.split(":")
        if len(parts) != 2:
            raise ValueError(f"Khung giờ không hợp lệ: {s!r}. Dùng HH:MM.")
        try:
            hh = int(parts[0])
            mm = int(parts[1])
        except ValueError as exc:
            raise ValueError(f"Khung giờ không hợp lệ: {s!r}. Dùng HH:MM.") from exc
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            raise ValueError(f"Khung giờ không hợp lệ: {s!r}.")
        slots.append((hh, mm))
    slots = sorted(set(slots))
    if not slots:
        raise ValueError("Cần ít nhất 1 khung giờ/ngày theo HH:MM.")
    dmin = int(delay_min_minutes)
    dmax = int(delay_max_minutes)
    if dmin < 0 or dmax < 0:
        raise ValueError("Delay phải >= 0.")
    if dmin > dmax:
        raise ValueError("Delay tối thiểu không được lớn hơn delay tối đa.")
    try:
        tz = ZoneInfo(str(timezone_name).strip() or "Asia/Ho_Chi_Minh")
    except Exception:
        tz = scheduler_tz()
    rnd = random.Random()
    slot_count = len(slots)
    out: list[dict[str, Any]] = []
    for job_index in range(job_count):
        day_offset = job_index // slot_count
        slot_index = job_index % slot_count
        hh, mm = slots[slot_index]
        base_date = start_date + timedelta(days=day_offset)
        base_local = datetime(base_date.year, base_date.month, base_date.day, hh, mm, tzinfo=tz)
        delay = rnd.randint(dmin, dmax) if dmax > 0 else 0
        final_local = base_local + timedelta(minutes=delay)
        out.append(
            {
                "slot_base_local": base_local.strftime("%Y-%m-%d %H:%M"),
                "delay_applied_min": delay,
                "scheduled_at": final_local.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
            }
        )
    return out


def preview_row_to_schedule_job(
    row: dict[str, Any],
    *,
    account_id: str,
    page_id: str,
    post_type: str,
    page_post_style: str,
    schedule_recurrence: str,
    schedule_slot: str,
) -> dict[str, Any]:
    """Chuyển một dòng preview → dict lưu ``schedule_posts.json``."""
    hashtags = row.get("hashtags")
    if not isinstance(hashtags, list):
        hashtags = []
    media = row.get("media_files")
    if not isinstance(media, list):
        media = []
    job: dict[str, Any] = {
        "account_id": account_id,
        "page_id": page_id,
        "post_type": post_type,
        "page_post_style": page_post_style,
        "title": str(row.get("title", "")).strip(),
        "content": str(row.get("content", "")).strip(),
        "hashtags": [str(h).strip() for h in hashtags if str(h).strip()],
        "cta": str(row.get("cta", "")).strip(),
        "media_files": [str(m).strip() for m in media if str(m).strip()],
        "scheduled_at": str(row.get("scheduled_at", "")).strip(),
        "status": "pending",
        "created_by": "gui_batch",
        "schedule_recurrence": schedule_recurrence,
        "schedule_slot": schedule_slot,
    }
    if row.get("ai_topic"):
        job["ai_topic"] = str(row["ai_topic"])
    if row.get("ai_content_style"):
        job["ai_content_style"] = str(row["ai_content_style"])
    if row.get("ai_language"):
        job["ai_language"] = str(row["ai_language"]).strip()
    if row.get("ai_provider_text"):
        job["ai_provider_text"] = str(row["ai_provider_text"]).strip().lower()
    if row.get("ai_provider_image"):
        job["ai_provider_image"] = str(row["ai_provider_image"]).strip().lower()
    if row.get("ai_model_text"):
        job["ai_model_text"] = str(row["ai_model_text"]).strip()
    if row.get("ai_model_image"):
        job["ai_model_image"] = str(row["ai_model_image"]).strip()
    if row.get("ai_config") and isinstance(row["ai_config"], dict):
        job["ai_config"] = dict(row["ai_config"])
    if row.get("image_alt"):
        job["image_alt"] = str(row["image_alt"]).strip()[:900]
    if row.get("image_prompt"):
        job["image_prompt"] = str(row["image_prompt"]).strip()[:1800]
    mf = job["media_files"]
    if mf:
        p0 = Path(mf[0])
        if p0.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp", ".gif"):
            job["job_post_image_path"] = mf[0]
    if row.get("error"):
        job["error_note"] = str(row["error"])[:900]
    sbl = str(row.get("slot_base_local", "")).strip()
    if sbl:
        job["slot_base_local"] = sbl[:80]
    try:
        dam = int(row.get("delay_applied_min", 0))
    except (TypeError, ValueError):
        dam = 0
    if "delay_applied_min" in row:
        job["schedule_delay_applied_min"] = max(0, min(180, dam))
    rtc = str(row.get("reel_thumbnail_choice", "")).strip()
    if rtc:
        job["reel_thumbnail_choice"] = rtc
    return job
