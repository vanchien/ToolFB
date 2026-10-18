"""
Thông báo lịch đăng: bài sắp đăng, bài quá giờ chưa đăng, và dời giờ hàng loạt.

Chỉ xử lý dữ liệu (không có Tkinter) để dùng được trong luồng nền và test.
"""

from __future__ import annotations

import os
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from zoneinfo import ZoneInfo

UPCOMING_STATUSES = frozenset({"pending", "ready_queue", "running", "processing"})
NOT_POSTED_STATUSES = frozenset({"pending", "ready_queue", "paused", "failed", "need_manual_check"})
RESCHEDULABLE_STATUSES = NOT_POSTED_STATUSES - {"ready_queue"}
MISSED_GRACE_MINUTES = 5


def job_time_utc(job: dict[str, Any]) -> datetime | None:
    """Giờ hẹn đăng của job theo UTC; ``None`` nếu trống hoặc sai định dạng."""
    raw = str((job or {}).get("scheduled_at") or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _status(job: dict[str, Any]) -> str:
    """Trạng thái job viết thường."""
    return str((job or {}).get("status") or "").strip().lower()


def stuck_after_minutes() -> float:
    """Ngưỡng coi job «running» là kẹt — cùng ``SCHEDULE_JOB_MAX_MINUTES`` của scheduler (mặc định 40)."""
    try:
        return max(10.0, min(240.0, float(os.environ.get("SCHEDULE_JOB_MAX_MINUTES", "40").strip())))
    except ValueError:
        return 40.0


def is_stuck(job: dict[str, Any], *, now: datetime | None = None) -> bool:
    """True nếu job ở trạng thái đang chạy quá lâu (worker chết / app tắt giữa chừng)."""
    if _status(job) not in {"running", "processing"}:
        return False
    raw = str(job.get("running_started_at") or "").strip()
    started = job_time_utc({"scheduled_at": raw}) if raw else job_time_utc(job)
    if started is None:
        return False
    cur = now or datetime.now(timezone.utc)
    return started < cur - timedelta(minutes=stuck_after_minutes())


def is_missed(job: dict[str, Any], *, now: datetime | None = None, grace_minutes: float = MISSED_GRACE_MINUTES) -> bool:
    """
    True nếu đã quá giờ hẹn hơn ``grace_minutes`` mà job vẫn chưa đăng được.

    Tính cả job lỗi / tạm dừng / cần kiểm tra tay và job «running» bị kẹt; job đã đăng thì không.
    """
    if is_stuck(job, now=now):
        return True
    if _status(job) not in NOT_POSTED_STATUSES:
        return False
    at = job_time_utc(job)
    if at is None:
        return False
    cur = now or datetime.now(timezone.utc)
    return at < cur - timedelta(minutes=grace_minutes)


@dataclass
class UpcomingPost:
    """Một bài sắp đăng để hiện trên thanh thông báo."""

    job_id: str
    page_id: str
    account_id: str
    at: datetime
    status: str


@dataclass
class AlertSnapshot:
    """Ảnh chụp tình hình lịch đăng tại một thời điểm."""

    upcoming: list[UpcomingPost] = field(default_factory=list)
    missed_ids: list[str] = field(default_factory=list)
    running: int = 0

    @property
    def missed(self) -> int:
        """Số bài quá giờ chưa đăng."""
        return len(self.missed_ids)


def build_alert_snapshot(
    jobs: Iterable[dict[str, Any]],
    *,
    now: datetime | None = None,
    horizon_hours: float = 24,
    limit: int = 20,
    grace_minutes: float = MISSED_GRACE_MINUTES,
) -> AlertSnapshot:
    """
    Gom bài sắp đăng (trong ``horizon_hours`` giờ tới) và danh sách bài quá giờ chưa đăng.

    Bài đang chạy luôn đứng đầu danh sách sắp đăng; bài vừa tới giờ (trong thời gian chờ
    ``grace_minutes``) vẫn tính là sắp đăng chứ chưa tính quá hạn.
    """
    cur = now or datetime.now(timezone.utc)
    horizon = cur + timedelta(hours=horizon_hours)
    snap = AlertSnapshot()
    upcoming: list[UpcomingPost] = []
    for job in jobs:
        st = _status(job)
        jid = str(job.get("id") or "").strip()
        if not jid:
            continue
        if is_missed(job, now=cur, grace_minutes=grace_minutes):
            snap.missed_ids.append(jid)
            continue
        if st in {"running", "processing"}:
            snap.running += 1
        if st not in UPCOMING_STATUSES:
            continue
        at = job_time_utc(job)
        if at is None or at > horizon:
            continue
        if st not in {"running", "processing"} and at < cur - timedelta(minutes=grace_minutes):
            continue
        upcoming.append(
            UpcomingPost(
                job_id=jid,
                page_id=str(job.get("page_id") or "").strip(),
                account_id=str(job.get("account_id") or "").strip(),
                at=at,
                status=st,
            )
        )
    upcoming.sort(key=lambda p: (p.status not in {"running", "processing"}, p.at))
    snap.upcoming = upcoming[: max(0, int(limit))]
    return snap


def format_countdown(at: datetime, *, now: datetime | None = None, status: str = "") -> str:
    """Chuỗi đếm ngược dễ đọc: «còn 12 phút», «còn 1 giờ 05 phút», «đang đăng», «tới giờ»."""
    cur = now or datetime.now(timezone.utc)
    if status in {"running", "processing"}:
        late = int((cur - at).total_seconds() // 60)
        if late < MISSED_GRACE_MINUTES:
            return "đang đăng"
        if late < 60:
            return f"đang đăng (trễ {late} phút)"
        return f"đang đăng (trễ {late // 60} giờ {late % 60:02d} phút)"
    secs = int((at - cur).total_seconds())
    if secs <= 0:
        return "tới giờ"
    if secs < 60:
        return f"còn {secs} giây"
    mins = secs // 60
    if mins < 60:
        return f"còn {mins} phút"
    hours, mins = divmod(mins, 60)
    if hours < 24:
        return f"còn {hours} giờ {mins:02d} phút"
    days, hours = divmod(hours, 24)
    return f"còn {days} ngày {hours} giờ"


def can_reschedule(job: dict[str, Any], *, now: datetime | None = None) -> bool:
    """Job chưa đăng (không tính đang xếp hàng) hoặc «running» bị kẹt thì dời giờ được."""
    return _status(job) in RESCHEDULABLE_STATUSES or is_stuck(job, now=now)


def reschedule_jobs(
    rows: list[dict[str, Any]],
    job_ids: Iterable[str],
    *,
    start_at: datetime,
    gap_min_minutes: int = 2,
    gap_max_minutes: int = 10,
    rng: random.Random | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """
    Dời giờ các job chưa đăng sang mốc mới, đưa về ``pending`` để chạy lại từ đầu.

    Page cùng tài khoản đăng lần lượt, cách nhau ngẫu nhiên ``gap_min``–``gap_max`` phút
    (giữ thứ tự cũ); tài khoản khác nhau cùng bắt đầu từ ``start_at``. Không đè lên lịch
    còn chờ khác của cùng tài khoản. Job «running» bị kẹt cũng được dời; job đang chạy
    bình thường hoặc đã đăng thì giữ nguyên. Sửa ``rows`` tại chỗ, trả về các dòng đã đổi.
    """
    from src.utils.schedule_batch_preview import next_free_time

    if start_at.tzinfo is None:
        raise ValueError("start_at cần có múi giờ")
    want = {str(x).strip() for x in job_ids if str(x).strip()}
    lo = max(0, int(gap_min_minutes))
    hi = max(lo, int(gap_max_minutes))
    pick = rng or random.Random()
    busy: dict[str, list[datetime]] = {}
    targets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        rid = str(row.get("id") or "").strip()
        aid = str(row.get("account_id") or "").strip()
        st = _status(row)
        if rid in want and can_reschedule(row, now=now):
            targets.setdefault(aid, []).append(row)
            continue
        at = job_time_utc(row)
        if st in UPCOMING_STATUSES and at is not None and aid:
            busy.setdefault(aid, []).append(at)
    base = start_at.astimezone(timezone.utc).replace(second=0, microsecond=0)
    changed: list[dict[str, Any]] = []
    far = datetime.max.replace(tzinfo=timezone.utc)
    for aid, lst in targets.items():
        taken = list(busy.get(aid, []))
        cur = base
        for idx, row in enumerate(sorted(lst, key=lambda r: job_time_utc(r) or far)):
            if idx > 0:
                cur = cur + timedelta(minutes=pick.randint(lo, hi))
            if hi > 0:
                cur = next_free_time(taken, cur, lo, gap_max_minutes=hi, rng=pick) if lo > 0 else cur
            taken.append(cur)
            row["scheduled_at"] = cur.isoformat()
            row["status"] = "pending"
            row["retry_count"] = 0
            row["error_note"] = ""
            row.pop("running_started_at", None)
            if row.get("slot_base_local"):
                tzn = str(row.get("timezone") or "").strip()
                try:
                    local = cur.astimezone(ZoneInfo(tzn)) if tzn else cur
                except Exception:  # noqa: BLE001
                    local = cur
                row["slot_base_local"] = local.strftime("%Y-%m-%d %H:%M")
            changed.append(row)
    return changed


ROLLABLE_STATUSES = frozenset({"pending", "ready_queue"})


def overdue_roll_minutes() -> float:
    """
    Trễ quá số phút này mà chưa bắt đầu đăng → tự dời lịch (không dồn đăng bù).

    Env ``SCHEDULE_OVERDUE_ROLL_MINUTES`` (mặc định 30); ``0`` = tắt, bài trễ vẫn vào
    ``ready_queue`` và đăng bù ngay khi mở lại app.
    """
    try:
        return max(0.0, float(os.environ.get("SCHEDULE_OVERDUE_ROLL_MINUTES", "30").strip()))
    except ValueError:
        return 30.0


def _series_slots(job: dict[str, Any]) -> str:
    """Chuỗi khung giờ lặp hằng ngày của job (rỗng nếu job đăng lẻ, không thuộc chuỗi)."""
    return str(job.get("schedule_daily_slots") or "").strip()


def _overdue_rollable(job: dict[str, Any], cutoff: datetime) -> bool:
    """Job chưa bắt đầu đăng và giờ hẹn đã trước ``cutoff`` (kể cả đăng lẻ / ready_queue)."""
    if _status(job) not in ROLLABLE_STATUSES:
        return False
    at = job_time_utc(job)
    return at is not None and at < cutoff


def has_overdue_to_roll(
    rows: Iterable[dict[str, Any]],
    *,
    now: datetime | None = None,
    late_minutes: float | None = None,
) -> bool:
    """Kiểm tra nhanh (không sửa dữ liệu) có job quá hạn cần dời hay không."""
    limit = overdue_roll_minutes() if late_minutes is None else float(late_minutes)
    if limit <= 0:
        return False
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(minutes=limit)
    return any(_overdue_rollable(r, cutoff) for r in rows)


def _mark_rolled(
    row: dict[str, Any],
    *,
    old_at: datetime,
    new_at: datetime,
    note: str,
) -> None:
    """Gắn ghi chú quá giờ + dời lịch lên job."""
    row["rolled_from"] = row.get("rolled_from") or old_at.isoformat()
    row["overdue_roll_count"] = int(row.get("overdue_roll_count") or 0) + 1
    row["scheduled_at"] = new_at.isoformat()
    row["status"] = "pending"
    row.pop("running_started_at", None)
    row["error_note"] = note
    row["retry_count"] = 0


def _roll_standalone_overdue(
    rows: list[dict[str, Any]],
    overdue: list[dict[str, Any]],
    *,
    now: datetime,
    rng: random.Random | None,
) -> list[dict[str, Any]]:
    """
    Job đăng lẻ quá hạn: dời về sau «bây giờ», cách nhau 2–10 phút theo tài khoản.

    Không dồn đăng bù khi mở lại app sau sự cố — xếp lại như nút «Dời giờ».
    """
    if not overdue:
        return []
    # ready_queue cũng dời được (reschedule_jobs mặc định bỏ qua ready_queue).
    for row in overdue:
        if _status(row) == "ready_queue":
            row["status"] = "pending"
    old_by_id = {id(row): job_time_utc(row) for row in overdue}
    start = (now + timedelta(minutes=1)).astimezone(timezone.utc).replace(second=0, microsecond=0)
    changed = reschedule_jobs(
        rows,
        [str(r.get("id") or "") for r in overdue],
        start_at=start,
        gap_min_minutes=2,
        gap_max_minutes=10,
        rng=rng,
        now=now,
    )
    for row in changed:
        old_at = old_by_id.get(id(row))
        new_at = job_time_utc(row)
        if old_at is None or new_at is None:
            continue
        tzn = str(row.get("timezone") or "Asia/Ho_Chi_Minh").strip() or "Asia/Ho_Chi_Minh"
        try:
            tz = ZoneInfo(tzn)
        except Exception:  # noqa: BLE001
            tz = ZoneInfo("Asia/Ho_Chi_Minh")
        old_local = old_at.astimezone(tz).strftime("%H:%M %d/%m")
        new_local = new_at.astimezone(tz).strftime("%H:%M %d/%m")
        row["rolled_from"] = row.get("rolled_from") or old_at.isoformat()
        row["overdue_roll_count"] = int(row.get("overdue_roll_count") or 0) + 1
        row["error_note"] = (
            f"Quá giờ {old_local} chưa đăng (mở lại sau sự cố) — tự dời sang {new_local}, không dồn đăng bù."
        )
        if row.get("slot_base_local"):
            row["slot_base_local"] = new_at.astimezone(tz).strftime("%Y-%m-%d %H:%M")
    return changed


def roll_overdue_to_series_end(
    rows: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    late_minutes: float | None = None,
    rng: random.Random | None = None,
) -> list[dict[str, Any]]:
    """
    Dời mọi job quá hạn chưa đăng — không dồn đăng bù khi mở lại app sau sự cố.

    - Job thuộc chuỗi lịch (``schedule_daily_slots``): xếp sau bài cuối của Page theo khung giờ lặp.
    - Job đăng lẻ: dời về sau thời điểm hiện tại, cách 2–10 phút theo tài khoản (như «Dời giờ»).

    Sửa ``rows`` tại chỗ, trả về các dòng đã đổi. Ghi ``error_note`` báo quá giờ + giờ mới.
    """
    from src.utils.schedule_batch_preview import build_page_daily_schedules, parse_daily_time_windows

    cur = now or datetime.now(timezone.utc)
    limit = overdue_roll_minutes() if late_minutes is None else float(late_minutes)
    if limit <= 0:
        return []
    cutoff = cur - timedelta(minutes=limit)
    overdue = [r for r in rows if _overdue_rollable(r, cutoff)]
    if not overdue:
        return []
    series_overdue = [r for r in overdue if _series_slots(r)]
    standalone_overdue = [r for r in overdue if not _series_slots(r)]
    overdue_ids = {id(r) for r in series_overdue}
    last_by_page: dict[str, datetime] = {}
    busy_by_account: dict[str, list[datetime]] = {}
    for row in rows:
        if id(row) in overdue_ids or _status(row) not in UPCOMING_STATUSES:
            continue
        at = job_time_utc(row)
        if at is None:
            continue
        pid = str(row.get("page_id") or "").strip()
        aid = str(row.get("account_id") or "").strip()
        if pid and (pid not in last_by_page or at > last_by_page[pid]):
            last_by_page[pid] = at
        if aid:
            busy_by_account.setdefault(aid, []).append(at)

    groups: dict[tuple[str, int, str], list[dict[str, Any]]] = {}
    for row in series_overdue:
        label = _series_slots(row)
        try:
            windows = parse_daily_time_windows(label)
        except ValueError:
            # Khung giờ hỏng → xử lý như đăng lẻ.
            standalone_overdue.append(row)
            continue
        try:
            per_day = max(1, int(row.get("schedule_per_day") or len(windows)))
        except (TypeError, ValueError):
            per_day = len(windows)
        tzn = str(row.get("timezone") or "Asia/Ho_Chi_Minh").strip() or "Asia/Ho_Chi_Minh"
        groups.setdefault((label, per_day, tzn), []).append(row)

    far = datetime.max.replace(tzinfo=timezone.utc)
    changed: list[dict[str, Any]] = []
    for (label, per_day, tzn), lst in groups.items():
        lst.sort(key=lambda r: (str(r.get("page_id") or ""), job_time_utc(r) or far))
        try:
            tz = ZoneInfo(tzn)
        except Exception:  # noqa: BLE001
            tz = ZoneInfo("Asia/Ho_Chi_Minh")
        keys = [str(r.get("page_id") or "").strip() or f"_job_{r.get('id')}" for r in lst]
        plan = build_page_daily_schedules(
            keys,
            start_date=cur.astimezone(tz).date(),
            windows=parse_daily_time_windows(label),
            per_day=per_day,
            timezone_name=tzn,
            not_before_by_group={k: last_by_page[k] for k in keys if k in last_by_page},
            now=cur,
            rng=rng,
            account_by_group={k: str(r.get("account_id") or "").strip() for k, r in zip(keys, lst)},
            busy_by_account=busy_by_account,
        )
        for key, row, slot in zip(keys, lst, plan):
            old_at = job_time_utc(row)
            new_at = job_time_utc({"scheduled_at": slot["scheduled_at"]})
            if old_at is None or new_at is None:
                continue
            old_local = old_at.astimezone(tz).strftime("%H:%M %d/%m")
            new_local = new_at.astimezone(tz).strftime("%H:%M %d/%m")
            _mark_rolled(
                row,
                old_at=old_at,
                new_at=new_at,
                note=(
                    f"Quá giờ {old_local} chưa đăng — tự dời sang {new_local} "
                    f"(sau bài cuối của Page, không dồn đăng bù)."
                ),
            )
            row["slot_base_local"] = slot["slot_base_local"]
            last_by_page[key] = new_at
            busy_by_account.setdefault(str(row.get("account_id") or "").strip(), []).append(new_at)
            changed.append(row)

    changed.extend(
        _roll_standalone_overdue(rows, standalone_overdue, now=cur, rng=rng)
    )
    return changed
