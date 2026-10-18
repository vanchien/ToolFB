"""
Logic preview / lịch chuỗi cho «Thêm batch job lịch đăng» (Tkinter).

Không phụ thuộc GUI — dễ test.
"""

from __future__ import annotations

import random
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from src.utils.page_schedule import scheduler_tz

_HHMM_RE = re.compile(r"^(\d{1,2}):(\d{2})$")
_WINDOW_RE = re.compile(r"^(\d{1,2}):(\d{2})\s*[-–]\s*(\d{1,2}):(\d{2})$")


def parse_hhmm_clock(text: str) -> tuple[int, int]:
    """Đổi ``HH:MM`` thành (giờ, phút)."""
    raw = str(text or "").strip()
    match = _HHMM_RE.match(raw)
    if not match:
        raise ValueError(f"Giờ không hợp lệ: {raw!r}. Dùng HH:MM.")
    hour, minute = int(match.group(1)), int(match.group(2))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"Giờ không hợp lệ: {raw!r}.")
    return hour, minute


def parse_daily_time_windows(text: str) -> list[tuple[tuple[int, int], tuple[int, int]]]:
    """
    Nhiều khung giờ trong một ngày, lặp lại mỗi ngày.

    Ví dụ: ``08:00-10:00, 14:00-16:00, 20:00-22:00``.
    Một mốc ``08:00`` coi như khung điểm (đăng đúng giờ đó rồi sang khung sau).
    """
    blob = str(text or "").replace(";", ",").replace("，", ",")
    parts = [p.strip() for p in blob.split(",") if p.strip()]
    if not parts:
        raise ValueError("Nhập ít nhất một khung giờ, ví dụ 08:00-10:00, 14:00-16:00.")
    found: list[tuple[tuple[int, int], tuple[int, int]]] = []
    for part in parts:
        rng = _WINDOW_RE.match(part)
        if rng:
            sh, sm, eh, em = (int(rng.group(1)), int(rng.group(2)), int(rng.group(3)), int(rng.group(4)))
            if not (0 <= sh <= 23 and 0 <= sm <= 59 and 0 <= eh <= 23 and 0 <= em <= 59):
                raise ValueError(f"Khung giờ không hợp lệ: {part!r}.")
            if (eh, em) < (sh, sm):
                raise ValueError(f"Khung {part} kết thúc trước khi bắt đầu.")
            found.append(((sh, sm), (eh, em)))
            continue
        point = _HHMM_RE.match(part)
        if point:
            hour, minute = int(point.group(1)), int(point.group(2))
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError(f"Giờ không hợp lệ: {part!r}.")
            found.append(((hour, minute), (hour, minute)))
            continue
        raise ValueError(f"Khung giờ không hợp lệ: {part!r}. Dùng 08:00-10:00 hoặc 08:00.")
    found.sort(key=lambda x: (x[0][0], x[0][1], x[1][0], x[1][1]))
    return found


def format_daily_time_windows(windows: list[tuple[tuple[int, int], tuple[int, int]]]) -> str:
    """Ghép khung giờ thành chuỗi lưu trên job."""
    bits: list[str] = []
    for start, end in windows or []:
        sh, sm = start
        eh, em = end
        if (sh, sm) == (eh, em):
            bits.append(f"{sh:02d}:{sm:02d}")
        else:
            bits.append(f"{sh:02d}:{sm:02d}-{eh:02d}:{em:02d}")
    return ", ".join(bits)


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


def build_random_window_schedules_per_group(
    group_keys: list[str],
    *,
    start_date: date,
    end_date: date | None = None,
    window_start: tuple[int, int] = (8, 0),
    window_end: tuple[int, int] = (22, 0),
    windows: list[tuple[tuple[int, int], tuple[int, int]]] | None = None,
    gap_min_minutes: int = 30,
    gap_max_minutes: int = 90,
    timezone_name: str = "Asia/Ho_Chi_Minh",
    start_jitter_max_min: int = 45,
    rng: random.Random | None = None,
) -> list[dict[str, Any]]:
    """
    Mỗi Page một dòng lịch. Một ngày có nhiều khung giờ, lặp lại ngày sau.

    Trong khung: cách nhau random ``gap_min``–``gap_max`` phút.
    Hết khung thì sang khung tiếp (cùng ngày) hoặc khung đầu ngày hôm sau.
    """
    n = len(group_keys or [])
    if n < 1:
        return []
    ranges = list(windows) if windows else [(window_start, window_end)]
    cleaned: list[tuple[tuple[int, int], tuple[int, int]]] = []
    for start, end in ranges:
        sh, sm = int(start[0]), int(start[1])
        eh, em = int(end[0]), int(end[1])
        if not (0 <= sh <= 23 and 0 <= sm <= 59 and 0 <= eh <= 23 and 0 <= em <= 59):
            raise ValueError("Khung giờ phải là giờ 0–23, phút 0–59.")
        if (eh, em) < (sh, sm):
            raise ValueError("Giờ kết thúc khung phải sau hoặc bằng giờ bắt đầu.")
        cleaned.append(((sh, sm), (eh, em)))
    cleaned.sort(key=lambda x: (x[0][0], x[0][1], x[1][0], x[1][1]))
    if not cleaned:
        raise ValueError("Cần ít nhất một khung giờ trong ngày.")
    gmin = max(0, int(gap_min_minutes))
    gmax = max(gmin, int(gap_max_minutes))
    try:
        tz = ZoneInfo(str(timezone_name).strip() or "Asia/Ho_Chi_Minh")
    except Exception:
        tz = scheduler_tz()
    rnd = rng or random.Random()
    end_d = end_date or start_date
    slot_label = format_daily_time_windows(cleaned)

    def _at(day: date, hour: int, minute: int) -> datetime:
        return datetime(day.year, day.month, day.day, hour, minute, 0, tzinfo=tz)

    def _spans(day: date) -> list[tuple[datetime, datetime]]:
        out_spans: list[tuple[datetime, datetime]] = []
        for (sh, sm), (eh, em) in cleaned:
            start_dt = _at(day, sh, sm)
            end_dt = _at(day, eh, em)
            if end_dt < start_dt:
                continue
            out_spans.append((start_dt, end_dt))
        return out_spans

    first_len = max(1, int((_spans(start_date)[0][1] - _spans(start_date)[0][0]).total_seconds() // 60) or 1)
    grouped: dict[str, list[int]] = {}
    for i, raw_key in enumerate(group_keys):
        key = str(raw_key or "").strip() or f"_item_{i}"
        grouped.setdefault(key, []).append(i)
    out: list[dict[str, Any] | None] = [None] * n

    def _place(prev: datetime | None) -> datetime:
        if prev is None:
            day = start_date
            start_dt, end_dt = _spans(day)[0]
            span_min = max(0, int((end_dt - start_dt).total_seconds() // 60))
            extra = rnd.randint(0, max(0, min(int(start_jitter_max_min), span_min)))
            cur = start_dt + timedelta(minutes=extra)
            return min(cur, end_dt)
        cur = prev + timedelta(minutes=rnd.randint(gmin, gmax))
        day = prev.date()
        hops = 0
        while hops < 900:
            spans = _spans(day)
            for start_dt, end_dt in spans:
                if cur < start_dt:
                    extra = rnd.randint(0, min(12, max(0, int((end_dt - start_dt).total_seconds() // 60) // 4)))
                    placed = start_dt + timedelta(minutes=extra)
                    return min(placed, end_dt)
                if start_dt <= cur <= end_dt:
                    return cur
            day = day + timedelta(days=1)
            start_dt, end_dt = _spans(day)[0]
            extra = rnd.randint(0, min(10, first_len // 5 if first_len > 5 else 0))
            cur = start_dt + timedelta(minutes=extra)
            hops += 1
        return cur

    for key, indexes in grouped.items():
        prev: datetime | None = None
        for item_i in indexes:
            cur = _place(prev)
            prev = cur
            out[item_i] = {
                "scheduled_at": cur.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                "wall": cur.strftime("%Y-%m-%d %H:%M"),
                "schedule_recurrence": "once",
                "schedule_slot": slot_label,
                "schedule_daily_slots": slot_label,
                "schedule_start_date": start_date.isoformat(),
                "schedule_end_date": end_d.isoformat(),
                "schedule_delay_min": gmin,
                "schedule_delay_max": gmax,
                "slot_base_local": cur.strftime("%Y-%m-%d %H:%M"),
                "delay_applied_min": 0,
                "schedule_group": key,
            }
    return [row for row in out if isinstance(row, dict)]


def daily_post_slots(
    windows: list[tuple[tuple[int, int], tuple[int, int]]],
    per_day: int,
) -> list[tuple[int, int, int]]:
    """
    Rải ``per_day`` bài trong các khung giờ một ngày.

    Trả về ``(phút bắt đầu, phút kết thúc, chỉ số khung)`` tính từ 00:00 cho từng bài.
    Ít bài hơn khung: chọn khung cách đều. Nhiều hơn: chia nhỏ khung thành từng đoạn.
    """
    if not windows:
        raise ValueError("Cần ít nhất một khung giờ trong ngày.")
    n = max(1, int(per_day))
    w = len(windows)
    per_window = [0] * w
    if n <= w:
        for k in range(n):
            per_window[(k * w) // n] += 1
    else:
        base, extra = divmod(n, w)
        per_window = [base + (1 if i < extra else 0) for i in range(w)]
    out: list[tuple[int, int, int]] = []
    for wi, ((sh, sm), (eh, em)) in enumerate(windows):
        cnt = per_window[wi]
        if cnt <= 0:
            continue
        a = sh * 60 + sm
        b = eh * 60 + em
        span = max(0, b - a)
        for j in range(cnt):
            s0 = a + (span * j) // cnt
            s1 = a + (span * (j + 1)) // cnt
            out.append((s0, max(s0, s1), wi))
    return out


ACCOUNT_MIN_GAP_MINUTES = 2
ACCOUNT_MAX_GAP_MINUTES = 10


def random_account_gap_minutes(
    rng: random.Random | None = None,
    *,
    low: int = ACCOUNT_MIN_GAP_MINUTES,
    high: int = ACCOUNT_MAX_GAP_MINUTES,
) -> int:
    """Số phút ngẫu nhiên giữa hai Page cùng tài khoản, trong khoảng 2–10."""
    lo = max(0, int(low))
    hi = max(lo, int(high))
    pick = rng.randint if rng is not None else random.randint
    return int(pick(lo, hi))


def next_free_time(
    busy: list[datetime],
    want: datetime,
    gap_minutes: float = ACCOUNT_MIN_GAP_MINUTES,
    *,
    gap_max_minutes: float = ACCOUNT_MAX_GAP_MINUTES,
    rng: random.Random | None = None,
) -> datetime:
    """
    Mốc ``>= want`` cách lịch cùng tài khoản một khoảng ngẫu nhiên từ ``gap_minutes`` tới ``gap_max_minutes``.

    Lịch đã cách hơn mức tối đa (khung giờ khác, ngày sau) giữ nguyên.
    Làm tròn về phút chẵn để cột «Hẹn đăng» không còn hai Page cùng một phút.
    """
    lo = max(0, int(gap_minutes))
    hi = max(lo, int(gap_max_minutes))
    cur = want.replace(second=0, microsecond=0)
    if hi <= 0:
        return cur
    ordered = sorted(t.replace(second=0, microsecond=0) for t in busy)
    restretched = False
    guard = 0
    while guard < 1000:
        guard += 1
        earlier = [t for t in ordered if t <= cur]
        if earlier and not restretched:
            prev = earlier[-1]
            delta = int((cur - prev).total_seconds() // 60)
            if delta <= hi:
                cur = prev + timedelta(minutes=random_account_gap_minutes(rng, low=lo, high=hi))
                restretched = True
                continue
        blocked = next((t for t in ordered if abs((t - cur).total_seconds()) < lo * 60), None)
        if blocked is None:
            return cur
        cur = blocked + timedelta(minutes=random_account_gap_minutes(rng, low=lo, high=hi))
    return cur


def build_page_daily_schedules(
    group_keys: list[str],
    *,
    start_date: date,
    windows: list[tuple[tuple[int, int], tuple[int, int]]],
    per_day: int,
    timezone_name: str = "Asia/Ho_Chi_Minh",
    not_before_by_group: dict[str, datetime] | None = None,
    now: datetime | None = None,
    rng: random.Random | None = None,
    account_by_group: dict[str, str] | None = None,
    busy_by_account: dict[str, list[datetime]] | None = None,
    busy_by_page: dict[str, list[datetime]] | None = None,
    min_gap_minutes: float = ACCOUNT_MIN_GAP_MINUTES,
    max_gap_minutes: float = ACCOUNT_MAX_GAP_MINUTES,
) -> list[dict[str, Any]]:
    """
    Mỗi Page đăng ``per_day`` video/ngày trong các khung giờ, lặp sang ngày sau đến hết video.

    ``group_keys[i]`` là Page của video i. Mỗi Page có dòng thời gian riêng.
    Bỏ các mốc đã qua và các mốc trước ``not_before_by_group[page]`` (bài cuối của Page,
    kể cả đã đăng) — lần thêm sau cộng tiếp slot tuần tự, không đụng giờ cũ.
    Các Page cùng tài khoản (``account_by_group``) đăng cách nhau ngẫu nhiên
    ``min_gap_minutes``–``max_gap_minutes`` phút, tính cả lịch cũ trong ``busy_by_account``.
    Cùng một Page không trùng phút (``busy_by_page``).
    """
    n = len(group_keys or [])
    if n < 1:
        return []
    try:
        tz = ZoneInfo(str(timezone_name).strip() or "Asia/Ho_Chi_Minh")
    except Exception:
        tz = scheduler_tz()
    cleaned = sorted(windows, key=lambda x: (x[0], x[1]))
    slots = daily_post_slots(cleaned, per_day)
    rnd = rng or random.Random()
    now_dt = (now or datetime.now(tz)).astimezone(tz) + timedelta(minutes=2)
    label = format_daily_time_windows(cleaned)
    limits = dict(not_before_by_group or {})

    def _slot_times(key: str):
        floor = now_dt
        nb = limits.get(key)
        nb_local = nb.astimezone(tz) if nb is not None else None
        day = start_date
        if nb_local is not None:
            day = max(day, nb_local.date())
        if floor.date() > day:
            day = floor.date()
        while True:
            for s0, s1, _wi in slots:
                base = datetime(day.year, day.month, day.day, tzinfo=tz)
                lo = base + timedelta(minutes=s0)
                hi = base + timedelta(minutes=s1)
                if hi < floor:
                    continue
                if nb_local is not None and lo <= nb_local:
                    continue
                full = max(0, int((hi - lo).total_seconds() // 60))
                margin = full // 5
                lo = max(lo + timedelta(minutes=margin), floor)
                hi = hi - timedelta(minutes=margin)
                if hi < lo:
                    hi = lo
                span = max(0, int((hi - lo).total_seconds() // 60))
                yield lo + timedelta(minutes=rnd.randint(0, span) if span else 0)
            day = day + timedelta(days=1)

    acct_of = {str(k).strip(): str(v).strip() for k, v in (account_by_group or {}).items() if str(v).strip()}
    busy: dict[str, list[datetime]] = {
        str(a): [t.astimezone(tz) for t in ts] for a, ts in (busy_by_account or {}).items()
    }
    busy_pages: dict[str, list[datetime]] = {
        str(p): [t.astimezone(tz) for t in ts] for p, ts in (busy_by_page or {}).items()
    }
    gens: dict[str, Any] = {}
    out: list[dict[str, Any]] = []
    for i, raw_key in enumerate(group_keys):
        key = str(raw_key or "").strip() or f"_item_{i}"
        gen = gens.setdefault(key, _slot_times(key))
        cur = next(gen)
        acct = acct_of.get(key) or f"_page_{key}"
        lst = busy.setdefault(acct, [])
        cur = next_free_time(
            lst, cur, min_gap_minutes, gap_max_minutes=max_gap_minutes, rng=rnd
        ).replace(microsecond=0)
        plst = busy_pages.setdefault(key, [])
        cur = next_free_time(
            plst, cur, min_gap_minutes, gap_max_minutes=max_gap_minutes, rng=rnd
        ).replace(microsecond=0)
        lst.append(cur)
        plst.append(cur)
        out.append(
            {
                "scheduled_at": cur.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                "wall": cur.strftime("%Y-%m-%d %H:%M"),
                "schedule_recurrence": "once",
                "schedule_slot": label,
                "schedule_daily_slots": label,
                "schedule_start_date": start_date.isoformat(),
                "schedule_per_day": int(per_day),
                "slot_base_local": cur.strftime("%Y-%m-%d %H:%M"),
                "delay_applied_min": 0,
                "schedule_group": key,
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
