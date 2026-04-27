"""
Chuẩn hóa ``schedule_time`` trên Page: HH:MM (cron hàng ngày) hoặc ``YYYY-MM-DD HH:MM`` (một lần).

Múi giờ wall-time trùng với ``SCHEDULER_TZ`` / ``Asia/Ho_Chi_Minh`` (như ``scheduler.py``).
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Literal
from zoneinfo import ZoneInfo

import os

ScheduleKind = Literal["cron", "date"]


def scheduler_tz() -> ZoneInfo:
    tz_name = os.environ.get("SCHEDULER_TZ", "Asia/Ho_Chi_Minh").strip() or "Asia/Ho_Chi_Minh"
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("Asia/Ho_Chi_Minh")


_CRON_ONLY = re.compile(r"^(\d{1,2}):(\d{2})$")
_ONCE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})(?::(\d{2}))?$")


def normalize_hh_mm(hour: int, minute: int) -> str:
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("Giờ/phút không hợp lệ.")
    return f"{hour:02d}:{minute:02d}"


def h12_to_h24(hour12: int, ampm: str) -> int:
    ap = str(ampm).strip().upper()
    if ap not in {"AM", "PM"}:
        raise ValueError("Chọn AM hoặc PM.")
    if not (1 <= hour12 <= 12):
        raise ValueError("Giờ (12h) phải từ 1 đến 12.")
    if ap == "AM":
        return 0 if hour12 == 12 else hour12
    return 12 if hour12 == 12 else hour12 + 12


def h24_to_h12(hour24: int) -> tuple[int, str]:
    if not (0 <= hour24 <= 23):
        raise ValueError("Giờ 24h không hợp lệ.")
    if hour24 == 0:
        return 12, "AM"
    if hour24 == 12:
        return 12, "PM"
    if hour24 < 12:
        return hour24, "AM"
    return hour24 - 12, "PM"


def parse_cron_hh_mm(raw: str) -> tuple[int, int]:
    m = _CRON_ONLY.match(str(raw).strip())
    if not m:
        raise ValueError("Lịch hàng ngày phải dạng HH:MM (ví dụ 09:00).")
    h, mi = int(m.group(1)), int(m.group(2))
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        raise ValueError("Giờ/phút ngoài phạm vi.")
    return h, mi


def parse_once_local(raw: str, tz: ZoneInfo) -> datetime:
    m = _ONCE.match(str(raw).strip())
    if not m:
        raise ValueError("Lịch một lần phải dạng YYYY-MM-DD HH:MM.")
    y, mo, d, hh, mm = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)), int(m.group(5))
    try:
        naive = datetime(y, mo, d, hh, mm, 0)
    except ValueError as exc:
        raise ValueError("Ngày hoặc giờ không tồn tại.") from exc
    return naive.replace(tzinfo=tz)


def classify_schedule_raw(raw: str) -> ScheduleKind:
    s = str(raw).strip()
    if not s:
        raise ValueError("Chuỗi lịch rỗng.")
    if _CRON_ONLY.match(s):
        return "cron"
    if _ONCE.match(s):
        return "date"
    raise ValueError(
        "Lịch không hợp lệ: dùng HH:MM (hàng ngày) hoặc YYYY-MM-DD HH:MM (một lần)."
    )


def parse_page_schedule_for_apscheduler(
    raw: str,
    *,
    tz: ZoneInfo | None = None,
) -> tuple[Literal["cron"], int, int] | tuple[Literal["date"], datetime]:
    """
    Parse ``schedule_time`` của Page thành cron (giờ, phút) hoặc datetime một lần (timezone-aware).
    """
    tz = tz or scheduler_tz()
    kind = classify_schedule_raw(raw)
    if kind == "cron":
        h, m = parse_cron_hh_mm(raw)
        return ("cron", h, m)
    dt = parse_once_local(raw, tz)
    return ("date", dt)


def validate_schedule_time_field(raw: str) -> None:
    """Giống lưu JSON: bắt buộc parse được (cron hoặc once)."""
    s = str(raw).strip()
    if not s:
        raise ValueError("Lịch trống.")
    parse_page_schedule_for_apscheduler(s)


def format_once_schedule(d: date, hour12: int, minute: int, ampm: str) -> str:
    h24 = h12_to_h24(hour12, ampm)
    return format_once_schedule_24h(d, h24, minute)


def format_once_schedule_24h(d: date, hour24: int, minute: int) -> str:
    if not (0 <= hour24 <= 23 and 0 <= minute <= 59):
        raise ValueError("Giờ/phút không hợp lệ.")
    try:
        naive = datetime(d.year, d.month, d.day, hour24, minute, 0)
    except ValueError as exc:
        raise ValueError("Ngày không hợp lệ.") from exc
    return naive.strftime("%Y-%m-%d %H:%M")


def parse_date_only_yyyy_mm_dd(s: str) -> date:
    s = str(s).strip()
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("Ngày phải dạng YYYY-MM-DD (ví dụ 2026-04-20).") from exc


def load_schedule_into_ui_parts(
    raw: str,
    *,
    tz: ZoneInfo | None = None,
) -> tuple[Literal["daily", "once"], date, int, int]:
    """
    Đổ dữ liệu đã lưu vào widget: (kind, date cho ô ngày, giờ 24h, phút).

    - ``daily`` (HH:MM): ``date`` là hôm nay (gợi ý nếu đổi sang «một lần»).
    - ``once``: ``date`` và giờ/phút theo bản ghi.
    """
    tz = tz or scheduler_tz()
    s = str(raw).strip()
    if not s:
        today = datetime.now(tz).date()
        return ("once", today, 9, 0)
    if _CRON_ONLY.match(s):
        h24, mi = parse_cron_hh_mm(s)
        today = datetime.now(tz).date()
        return ("daily", today, h24, mi)
    dt = parse_once_local(s, tz)
    return ("once", dt.date(), dt.hour, dt.minute)


def normalize_schedule_for_compare(raw: str) -> str:
    """Chuẩn hóa nhẹ để so sánh đổi lịch (đặt lại pending)."""
    s = str(raw).strip()
    if not s:
        return ""
    kind = classify_schedule_raw(s)
    if kind == "cron":
        h, m = parse_cron_hh_mm(s)
        return normalize_hh_mm(h, m)
    dt = parse_once_local(s, scheduler_tz())
    return dt.strftime("%Y-%m-%d %H:%M")
