"""Xếp giờ chia sẻ cho một list link video. Không tải file video."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

_DATE_TIME = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})(?:\s+(.*))?$"
)
_CLOCK = re.compile(r"^(\d{1,2}):(\d{2})(?:\s+(.*))?$")


@dataclass
class PlannedShareLink:
    """Một link trong list chờ và thời điểm được phép bắt đầu."""

    when: datetime
    url: str
    label: str = ""
    state: str = "waiting"
    qid: str = ""


def make_queue_item(
    url: str,
    clock: str = "",
    *,
    now: datetime | None = None,
    immediate: bool = False,
) -> PlannedShareLink:
    """Một link một giờ. ``immediate`` là chia sẻ ngay, không cần nhập giờ."""
    moment = now or datetime.now().replace(microsecond=0)
    cleaned = " ".join(str(url or "").split())
    if not _looks_like_url(cleaned):
        raise ValueError("Dán link video. Tool không tải video.")
    if immediate:
        return PlannedShareLink(when=moment, url=cleaned, label="Ngay")
    stamp = str(clock or "").strip()
    if not stamp:
        raise ValueError("Nhập giờ HH:MM cho link này, hoặc bấm Chia sẻ ngay.")
    when = _resolve_stamp(stamp, moment)
    label = when.strftime("%H:%M") if when.date() == moment.date() else when.strftime("%d/%m %H:%M")
    return PlannedShareLink(when=when, url=cleaned, label=label)


def queue_state_from_results(
    *,
    created: int,
    job_statuses: list[str],
    paused: bool,
    cancelled: bool,
) -> str:
    """Trạng thái một link sau khi chạy. Dừng giữa chừng thì để lại trong list chờ."""
    if cancelled:
        return "waiting"
    if paused:
        return "error"
    if int(created) <= 0:
        return "skipped"
    done = sum(status == "COMPLETED" for status in job_statuses)
    failed = sum(status in {"FAILED", "SKIPPED"} for status in job_statuses)
    if done and not failed:
        return "done"
    if done:
        return "partial"
    return "error"


def clamp_gap_minutes(raw: object) -> int:
    """Số phút cách nhau giữa các link. Để trống là 0."""
    text = str(raw if raw is not None else "").strip()
    if not text:
        return 0
    try:
        minutes = int(text)
    except ValueError as exc:
        raise ValueError("Cách mỗi link phải là số phút từ 0 đến 1440") from exc
    if minutes < 0 or minutes > 1440:
        raise ValueError("Cách mỗi link phải từ 0 đến 1440 phút")
    return minutes


def plan_share_links(
    raw: str,
    *,
    start_clock: str = "",
    gap_minutes: int = 0,
    now: datetime | None = None,
    single_url: str = "",
) -> list[PlannedShareLink]:
    """Đọc list link. Dòng có giờ thì dùng giờ đó. Dòng không giờ thì tính từ «Hẹn lúc» và khoảng cách."""
    moment = now or datetime.now().replace(microsecond=0)
    gap = timedelta(minutes=max(0, int(gap_minutes)))
    rows = _link_rows(raw)
    if not rows and str(single_url or "").strip():
        rows = [("", str(single_url).strip())]
    if not rows:
        return []
    start = _resolve_clock(start_clock, moment) if str(start_clock or "").strip() else moment
    cursor = start
    planned: list[PlannedShareLink] = []
    for clock, url in rows:
        if clock:
            when = _resolve_stamp(clock, moment)
            cursor = when
        else:
            when = cursor
        planned.append(PlannedShareLink(when=when, url=url))
        cursor = when + gap
    return planned


def _link_rows(raw: str) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    for line in str(raw or "").replace("\r", "\n").split("\n"):
        text = " ".join(line.split())
        if not text or text.startswith("#"):
            continue
        clock, url = _split_clock(text)
        if not _looks_like_url(url):
            raise ValueError(f"Dòng không phải link video: {text[:80]}")
        found.append((clock, url))
    return found


def _split_clock(text: str) -> tuple[str, str]:
    dated = _DATE_TIME.match(text)
    if dated:
        clock = f"{dated.group(1)}-{dated.group(2)}-{dated.group(3)} {int(dated.group(4)):02d}:{dated.group(5)}"
        return clock, str(dated.group(6) or "").strip()
    clocked = _CLOCK.match(text)
    if clocked and _looks_like_url(str(clocked.group(3) or "")):
        return f"{int(clocked.group(1)):02d}:{clocked.group(2)}", str(clocked.group(3) or "").strip()
    return "", text


def _looks_like_url(text: str) -> bool:
    low = str(text or "").strip().casefold()
    return low.startswith("http://") or low.startswith("https://") or "facebook.com" in low


def _resolve_stamp(clock: str, now: datetime) -> datetime:
    if " " in clock:
        day, hm = clock.split(" ", 1)
        year, month, day_no = (int(part) for part in day.split("-"))
        hour, minute = _hour_minute(hm)
        try:
            return datetime(year, month, day_no, hour, minute)
        except ValueError as exc:
            raise ValueError(f"Giờ không hợp lệ: {clock}") from exc
    return _resolve_clock(clock, now)


def hour_choices(now: datetime | None = None) -> list[str]:
    """24 giờ, bắt đầu từ giờ hiện tại của máy rồi vòng qua ngày hôm sau."""
    start = (now or datetime.now()).hour
    return [f"{(start + offset) % 24:02d}" for offset in range(24)]


def minute_choices() -> list[str]:
    """Phút 00 đến 59 để chọn đúng một mốc trong giờ đã chọn."""
    return [f"{minute:02d}" for minute in range(60)]


def compose_clock(hour: str, minute: str) -> str:
    """Ghép giờ và phút thành HH:MM. Thiếu giờ thì coi như chưa hẹn."""
    hour_text = str(hour or "").strip()
    if not hour_text:
        return ""
    minute_text = str(minute or "").strip() or "00"
    parsed_hour, parsed_minute = _hour_minute(f"{hour_text}:{minute_text}")
    return f"{parsed_hour:02d}:{parsed_minute:02d}"


def machine_clock_text(now: datetime | None = None) -> str:
    """Nhãn giờ máy đang chạy Tool, dùng làm mốc cho khung 24 giờ."""
    moment = now or datetime.now().replace(microsecond=0)
    return f"Giờ máy {moment.strftime('%H:%M')}"


def _resolve_clock(clock: str, now: datetime) -> datetime:
    hour, minute = _hour_minute(clock)
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    now_minute = now.replace(second=0, microsecond=0)
    if candidate < now_minute:
        candidate += timedelta(days=1)
    return candidate


def _hour_minute(clock: str) -> tuple[int, int]:
    parts = str(clock or "").strip().split(":")
    if len(parts) != 2:
        raise ValueError("Giờ hẹn dùng dạng HH:MM, ví dụ 21:30")
    try:
        hour, minute = int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise ValueError("Giờ hẹn dùng dạng HH:MM, ví dụ 21:30") from exc
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError("Giờ hẹn dùng dạng HH:MM, ví dụ 21:30")
    return hour, minute
