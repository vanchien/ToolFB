"""
Dispatcher lịch đăng theo ``schedule_time`` — không cron theo Page/Account.

Luồng: ``pending`` → (đến giờ) → ``ready_queue`` → ``running`` → terminal.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from loguru import logger

from src.utils.paths import project_root
from src.utils.schedule_posts_manager import SchedulePostsManager, SchedulePostJob

_consecutive_lock = threading.Lock()
_account_consecutive_streak: dict[str, int] = {}
_last_dispatched_account: str = ""
_recovery_done = False


def schedule_queue_monitor_path(*, root: Path | None = None) -> Path:
    d = (root or project_root()) / "data" / "runtime"
    d.mkdir(parents=True, exist_ok=True)
    return d / "schedule_queue_monitor.json"


def parse_job_schedule_time(raw: Any) -> datetime:
    """ISO 8601 → UTC; rỗng = coi như đến hạn ngay (job trễ vẫn chạy)."""
    s = str(raw or "").strip()
    if not s:
        return datetime.now(timezone.utc)
    s2 = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


def parse_job_created_at(raw: Any) -> datetime:
    dt = parse_job_schedule_time(raw)
    if str(raw or "").strip():
        return dt
    return datetime.min.replace(tzinfo=timezone.utc)


def sort_queue_jobs(jobs: list[SchedulePostJob]) -> list[SchedulePostJob]:
    """Sort: ``scheduled_at`` asc, rồi ``created_at`` asc."""
    def _key(j: SchedulePostJob) -> tuple[float, float, str]:
        sa = parse_job_schedule_time(j.get("scheduled_at")).timestamp()
        ca = parse_job_created_at(j.get("created_at")).timestamp()
        return (sa, ca, str(j.get("id", "")))

    return sorted(jobs, key=_key)


def _poll_interval_sec() -> int:
    raw = os.environ.get("SCHEDULE_POSTS_POLL_SEC", "10").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 10
    return max(5, min(120, n))


def _max_consecutive_per_account() -> int:
    raw = os.environ.get("SCHEDULE_MAX_CONSECUTIVE_PER_ACCOUNT", "2").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 2
    return max(1, min(10, n))


def compute_smart_delay_ms(*, ready_count: int, running_count: int) -> int:
    """
    Queue dài → giảm delay giữa các lần dispatch (ms).
    Env ``SCHEDULE_SMART_DELAY_BASE_MS`` (mặc định 800).
    """
    base = 800
    try:
        base = max(0, int(os.environ.get("SCHEDULE_SMART_DELAY_BASE_MS", "800")))
    except ValueError:
        pass
    depth = int(ready_count) + int(running_count)
    if depth >= 25:
        return 0
    if depth >= 12:
        return max(0, base // 4)
    if depth >= 6:
        return max(0, base // 2)
    if depth >= 3:
        return max(0, int(base * 0.75))
    return base


def write_queue_monitor(
    *,
    ready_jobs: list[SchedulePostJob],
    running_jobs: list[SchedulePostJob],
    pending_due: int,
    dispatched_ids: list[str],
    account_inflight: dict[str, int],
    smart_delay_ms: int,
    extra: dict[str, Any] | None = None,
) -> None:
    path = schedule_queue_monitor_path()
    payload: dict[str, Any] = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "poll_sec": _poll_interval_sec(),
        "ready_count": len(ready_jobs),
        "running_count": len(running_jobs),
        "pending_due_count": pending_due,
        "dispatched_this_tick": dispatched_ids,
        "account_inflight": account_inflight,
        "account_consecutive_streak": dict(_account_consecutive_streak),
        "last_dispatched_account": _last_dispatched_account,
        "smart_delay_ms": smart_delay_ms,
        "ready_preview": [
            {
                "id": str(j.get("id", "")),
                "account_id": str(j.get("account_id", "")),
                "page_id": str(j.get("page_id", "")),
                "scheduled_at": str(j.get("scheduled_at", "")),
                "created_at": str(j.get("created_at", "")),
            }
            for j in ready_jobs[:40]
        ],
    }
    if extra:
        payload.update(extra)
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except Exception as exc:
        logger.debug("Không ghi schedule_queue_monitor: {}", exc)


def note_job_dispatched(account_id: str) -> None:
    """Cập nhật streak «liên tiếp cùng account» sau mỗi lần dispatch."""
    global _last_dispatched_account
    aid = str(account_id or "").strip()
    if not aid:
        return
    with _consecutive_lock:
        if _last_dispatched_account == aid:
            _account_consecutive_streak[aid] = _account_consecutive_streak.get(aid, 0) + 1
        else:
            for k in list(_account_consecutive_streak):
                if k != aid:
                    _account_consecutive_streak[k] = 0
            _account_consecutive_streak[aid] = 1
        _last_dispatched_account = aid


def _should_defer_account_for_consecutive(
    account_id: str,
    *,
    sorted_ready: list[SchedulePostJob],
    planned_per_account: dict[str, int],
    account_inflight: Callable[[str], int],
    per_account_limit: int,
) -> bool:
    """Sau ``max_consecutive`` job liên tiếp — ưu tiên account khác nếu còn job trong queue."""
    aid = str(account_id or "").strip()
    if not aid:
        return False
    with _consecutive_lock:
        streak = int(_account_consecutive_streak.get(aid, 0))
    if streak < _max_consecutive_per_account():
        return False
    for j in sorted_ready:
        other = str(j.get("account_id", "")).strip()
        if not other or other == aid:
            continue
        if account_inflight(other) + planned_per_account.get(other, 0) >= per_account_limit:
            continue
        return True
    return False


def select_dispatch_batch(
    sorted_ready: list[SchedulePostJob],
    *,
    account_inflight: Callable[[str], int],
    per_account_limit: int = 1,
) -> list[SchedulePostJob]:
    """
    Chọn batch job dispatch: mỗi tài khoản một Page. Account đang đăng thì job tới giờ ở lại ready_queue, chạy sau khi Page hiện tại xong — không bỏ.
    """
    selected: list[SchedulePostJob] = []
    selected_ids: set[str] = set()
    planned: dict[str, int] = {}
    deferred_accounts: set[str] = set()

    def _try_add(job: SchedulePostJob, *, allow_deferred: bool) -> bool:
        jid = str(job.get("id", "")).strip()
        aid = str(job.get("account_id", "")).strip()
        if not jid or not aid or jid in selected_ids:
            return False
        if account_inflight(aid) + planned.get(aid, 0) >= per_account_limit:
            return False
        if not allow_deferred and aid in deferred_accounts:
            return False
        if not allow_deferred and _should_defer_account_for_consecutive(
            aid,
            sorted_ready=sorted_ready,
            planned_per_account=planned,
            account_inflight=account_inflight,
            per_account_limit=per_account_limit,
        ):
            deferred_accounts.add(aid)
            return False
        selected.append(job)
        selected_ids.add(jid)
        planned[aid] = planned.get(aid, 0) + 1
        return True

    for job in sorted_ready:
        _try_add(job, allow_deferred=False)

    if deferred_accounts:
        for job in sorted_ready:
            _try_add(job, allow_deferred=True)

    return selected


def promote_due_pending_to_ready(
    sp: SchedulePostsManager,
    *,
    now: datetime | None = None,
) -> int:
    """``pending`` + ``scheduled_at <= now`` → ``ready_queue`` (không bỏ job trễ)."""
    now = now or datetime.now(timezone.utc)
    jobs = sp.load_all()
    any_due = False
    for job in jobs:
        st = str(job.get("status", "")).strip().lower()
        if st == "pending" and parse_job_schedule_time(job.get("scheduled_at")) <= now:
            any_due = True
            break
    if not any_due:
        return 0
    promoted = 0
    out: list[SchedulePostJob] = []
    dirty = False
    for job in jobs:
        row: dict[str, Any] = dict(job)
        st = str(row.get("status", "")).strip().lower()
        if st == "pending" and parse_job_schedule_time(row.get("scheduled_at")) <= now:
            row["status"] = "ready_queue"
            promoted += 1
            dirty = True
        out.append(row)  # type: ignore[arg-type]
    if dirty:
        sp.save_all(out)
        logger.info("[Schedule queue] Promote {} job pending → ready_queue.", promoted)
    return promoted


def recover_stale_running_jobs(
    sp: SchedulePostsManager,
    *,
    force_all_running: bool = False,
) -> int:
    """
    Resume sau crash: ``running`` cũ → ``ready_queue``.
    ``force_all_running=True`` trên startup — mọi ``running`` → ``ready_queue``.
    """
    now = datetime.now(timezone.utc)
    recovered = 0
    jobs = sp.load_all()
    out: list[SchedulePostJob] = []
    dirty = False
    suffix = " [recovered after crash → ready_queue]"
    for job in jobs:
        row: dict[str, Any] = dict(job)
        if str(row.get("status", "")).strip().lower() != "running":
            out.append(row)  # type: ignore[arg-type]
            continue
        if not force_all_running:
            out.append(row)  # type: ignore[arg-type]
            continue
        note = str(row.get("error_note", "") or "").strip()
        if suffix not in note:
            note = (note + suffix).strip()[:900]
            row["error_note"] = note or None
        row["status"] = "ready_queue"
        recovered += 1
        dirty = True
        out.append(row)  # type: ignore[arg-type]
    if dirty:
        sp.save_all(out)
        logger.warning(
            "[Schedule queue] Recovered {} job running → ready_queue (crash/resume).",
            recovered,
        )
    return recovered


def ensure_schedule_queue_recovery(sp: SchedulePostsManager) -> None:
    """Chạy một lần khi scheduler start."""
    global _recovery_done
    if _recovery_done:
        return
    _recovery_done = True
    recover_stale_running_jobs(sp, force_all_running=True)


_ARCHIVE_STATUSES = frozenset({"success", "cancelled"})


def schedule_archive_path(*, root: Path | None = None) -> Path:
    """File lưu bài đã đăng xong lâu ngày (mỗi dòng một job JSON)."""
    d = (root or project_root()) / "data" / "runtime"
    d.mkdir(parents=True, exist_ok=True)
    return d / "schedule_posts_archive.jsonl"


def archive_finished_jobs(
    sp: SchedulePostsManager,
    *,
    older_than_days: int = 7,
    now: datetime | None = None,
    archive_path: Path | None = None,
) -> int:
    """
    Chuyển job ``success``/``cancelled`` cũ hơn ``older_than_days`` ngày ra file archive.

    Giữ ``schedule_posts.json`` gọn để mỗi lần đọc/ghi khi chạy nhiều Page 24/7 vẫn nhanh.
    """
    now = now or datetime.now(timezone.utc)
    cutoff = now.timestamp() - max(1, int(older_than_days)) * 86400
    jobs = sp.load_all()
    keep: list[SchedulePostJob] = []
    old: list[dict[str, Any]] = []
    for job in jobs:
        st = str(job.get("status", "")).strip().lower()
        if st in _ARCHIVE_STATUSES:
            raw = job.get("posted_at") or job.get("updated_at") or job.get("scheduled_at")
            if str(raw or "").strip() and parse_job_schedule_time(raw).timestamp() < cutoff:
                old.append(dict(job))
                continue
        keep.append(job)
    if not old:
        return 0
    path = archive_path or schedule_archive_path()
    with path.open("a", encoding="utf-8") as fh:
        for row in old:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    sp.save_all(keep)
    logger.info("[Schedule queue] Đã chuyển {} job xong lâu ngày sang {}.", len(old), path)
    return len(old)


_archived_ids_cache: tuple[float, frozenset[str]] = (0.0, frozenset())


def archived_job_ids(*, archive_path: Path | None = None) -> frozenset[str]:
    """Id job đã chuyển sang archive (cache theo mtime) — để tab Chờ đăng không coi là mất lịch."""
    global _archived_ids_cache
    path = archive_path or schedule_archive_path()
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return frozenset()
    if archive_path is None and _archived_ids_cache[0] == mtime:
        return _archived_ids_cache[1]
    ids: set[str] = set()
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                jid = str(json.loads(line).get("id") or "").strip()
            except (ValueError, AttributeError):
                continue
            if jid:
                ids.add(jid)
    out = frozenset(ids)
    if archive_path is None:
        _archived_ids_cache = (mtime, out)
    return out


def find_orphan_running_jobs(
    jobs: list[SchedulePostJob],
    *,
    tracked_ids: set[str],
    now: datetime | None = None,
    grace_minutes: float = 5.0,
) -> list[str]:
    """
    Job ``running`` trong file nhưng không còn worker nào chạy (worker chết giữa chừng).

    Chỉ tính job có ``running_started_at`` quá ``grace_minutes`` phút.
    """
    now = now or datetime.now(timezone.utc)
    out: list[str] = []
    for job in jobs:
        if str(job.get("status", "")).strip().lower() != "running":
            continue
        jid = str(job.get("id", "")).strip()
        if not jid or jid in tracked_ids:
            continue
        raw = str(job.get("running_started_at") or "").strip()
        if not raw:
            continue
        if (now - parse_job_schedule_time(raw)).total_seconds() >= grace_minutes * 60:
            out.append(jid)
    return out


def count_pending_due(jobs: list[SchedulePostJob], *, now: datetime | None = None) -> int:
    now = now or datetime.now(timezone.utc)
    n = 0
    for job in jobs:
        if str(job.get("status", "")).strip().lower() != "pending":
            continue
        if parse_job_schedule_time(job.get("scheduled_at")) <= now:
            n += 1
    return n
