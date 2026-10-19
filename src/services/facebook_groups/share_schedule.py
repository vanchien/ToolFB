"""Xếp giờ chia sẻ cho một list link video. Không tải file video."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

_DATE_TIME = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})(?:\s+(.*))?$"
)
_CLOCK = re.compile(r"^(\d{1,2}):(\d{2})(?:\s+(.*))?$")


def as_local_naive(value: datetime | None) -> datetime:
    """
    Chuẩn hóa giờ về naive local — tránh so sánh naive/aware làm worker hàng chờ chết TypeError.
    """
    if value is None:
        return datetime.now().replace(microsecond=0)
    when = value
    if when.tzinfo is not None:
        try:
            when = when.astimezone().replace(tzinfo=None)
        except Exception:  # noqa: BLE001
            when = when.replace(tzinfo=None)
    return when.replace(microsecond=0)


@dataclass
class PlannedShareLink:
    """Một link trong list chờ và thời điểm được phép bắt đầu."""

    when: datetime
    url: str
    label: str = ""
    state: str = "waiting"
    qid: str = ""
    # Page nhận riêng (iid trong bảng Page) — dòng do «Theo dõi Page nguồn» thêm; rỗng = Page đang tích.
    targets: list[str] = field(default_factory=list)
    source: str = ""
    # Tên / Meta ID Page nguồn (hiển thị Hàng chờ) — lấy lúc quét hoặc tra cứu.
    source_name: str = ""
    source_id: str = ""


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
        raise ValueError("Chọn ngày và giờ cho link này, hoặc bấm Chia sẻ ngay.")
    when = _resolve_stamp(stamp, moment)
    label = when.strftime("%H:%M") if when.date() == moment.date() else when.strftime("%d/%m %H:%M")
    return PlannedShareLink(when=when, url=cleaned, label=label)


def queue_to_settings(items: list[PlannedShareLink]) -> list[dict[str, Any]]:
    """Link còn chờ, để mở lại app vẫn thấy list."""
    rows: list[dict[str, Any]] = []
    for item in items:
        if item.state not in {"waiting", "running"}:
            continue
        row: dict[str, Any] = {
            "url": item.url,
            "when": item.when.replace(microsecond=0).isoformat(),
            "label": item.label or "",
            "state": "waiting",
        }
        if item.targets:
            row["targets"] = list(item.targets)
        if item.source:
            row["source"] = item.source
        if item.source_name:
            row["source_name"] = item.source_name
        if item.source_id:
            row["source_id"] = item.source_id
        rows.append(row)
    return rows


def queue_from_settings(rows: object) -> list[PlannedShareLink]:
    """Đọc list chờ đã lưu. Bỏ dòng thiếu link hoặc thiếu giờ."""
    found: list[PlannedShareLink] = []
    if not isinstance(rows, list):
        return found
    for row in rows:
        if not isinstance(row, dict):
            continue
        url = str(row.get("url") or "").strip()
        raw_when = str(row.get("when") or "").strip()
        if not url or not raw_when:
            continue
        try:
            when = as_local_naive(datetime.fromisoformat(raw_when))
        except ValueError:
            continue
        found.append(
            PlannedShareLink(
                when=when,
                url=url,
                label=str(row.get("label") or ""),
                state="waiting",
                targets=[str(x) for x in (row.get("targets") or []) if str(x).strip()],
                source=str(row.get("source") or ""),
                source_name=str(row.get("source_name") or ""),
                source_id=str(row.get("source_id") or ""),
            )
        )
    return found


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
    done = sum(status == "COMPLETED" for status in job_statuses)
    failed = sum(status in {"FAILED", "SKIPPED"} for status in job_statuses)
    # Resume batch dở: created=0 nhưng vẫn có job COMPLETED / đang chạy.
    if int(created) <= 0 and not job_statuses:
        return "skipped"
    if int(created) <= 0 and done and not failed:
        return "done"
    if int(created) <= 0 and done:
        return "partial"
    if int(created) <= 0 and failed and not done:
        return "error"
    if int(created) <= 0:
        # Có job nhưng chưa xong (resume) — giữ waiting để vòng sau chạy tiếp.
        return "waiting"
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


def date_choices(now: datetime | None = None, days: int = 30) -> list[str]:
    """Hôm nay và các ngày tới, để xếp nhiều link theo ngày."""
    moment = (now or datetime.now()).replace(microsecond=0)
    found: list[str] = []
    for offset in range(max(1, min(int(days), 90))):
        day = moment + timedelta(days=offset)
        label = day.strftime("%d/%m/%Y")
        if offset == 0:
            label = f"{label} hôm nay"
        found.append(label)
    return found


def parse_date_choice(label: str, now: datetime | None = None) -> str:
    """Đổi nhãn ngày trên dropdown thành YYYY-MM-DD."""
    text = str(label or "").replace("hôm nay", "").strip()
    if not text:
        moment = now or datetime.now()
        return moment.strftime("%Y-%m-%d")
    parts = text.replace("-", "/").split("/")
    if len(parts) != 3:
        raise ValueError("Ngày dùng dạng dd/mm/yyyy")
    try:
        day_no, month, year = int(parts[0]), int(parts[1]), int(parts[2])
        chosen = datetime(year, month, day_no)
    except ValueError as exc:
        raise ValueError("Ngày dùng dạng dd/mm/yyyy") from exc
    return chosen.strftime("%Y-%m-%d")


def date_choice_is_today(label: str, now: datetime | None = None) -> bool:
    """Ô ngày đang chọn hôm nay thì giờ bắt đầu từ giờ máy."""
    try:
        iso = parse_date_choice(label, now)
    except ValueError:
        return True
    moment = now or datetime.now()
    return iso == moment.strftime("%Y-%m-%d")


def hour_choices(now: datetime | None = None, *, from_midnight: bool = False) -> list[str]:
    """24 giờ. Ngày khác hôm nay thì bắt đầu từ 00."""
    if from_midnight:
        return [f"{hour:02d}" for hour in range(24)]
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


def compose_stamp(date_label: str, hour: str, minute: str, *, now: datetime | None = None) -> str:
    """Ghép ngày, giờ, phút thành YYYY-MM-DD HH:MM để xếp lịch nhiều ngày."""
    clock = compose_clock(hour, minute)
    if not clock:
        return ""
    day = parse_date_choice(date_label, now)
    return f"{day} {clock}"


def machine_clock_text(now: datetime | None = None) -> str:
    """Nhãn ngày giờ máy đang chạy Tool."""
    moment = now or datetime.now().replace(microsecond=0)
    return f"Máy {moment.strftime('%d/%m %H:%M')}"


def compute_allowed_share_accounts(
    *,
    checked_account_ids: set[str] | list[str],
    filter_account_id: str | None = None,
) -> set[str]:
    """
    Tài khoản được phép chia sẻ = Page đang tick ∩ (lọc combobox nếu có).

    - Bỏ hết tick một TK → TK đó không còn trong kết quả → phải dừng ngay.
    - Lọc «Andy» nhưng không tick Page Andy → tập rỗng → dừng cả Andy.
    - «Tất cả» + tick A,B → {A,B}; bỏ tick B → {A}.
    """
    checked = {str(a).strip() for a in (checked_account_ids or []) if str(a or "").strip()}
    filt = str(filter_account_id or "").strip()
    if filt:
        return checked & {filt}
    return checked


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
