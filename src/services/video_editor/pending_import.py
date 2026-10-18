"""
Chọn clip trong job chờ đăng để nạp vào lịch Facebook.

Chỉ đăng lên fanpage đã chỉ định: clip đã chia Page thì dùng đúng Page đó;
clip chưa có Page trong job đã chia sẽ để lại cho lần chia sau, không đổ vào Page khác.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.services.video_editor.page_split import item_has_facebook_page
from src.utils.schedule_batch_preview import page_post_style_for_post_type
from src.utils.schedule_job_content import internal_post_title_from_body

JOB_STATUS_PARTIAL = "partial"
JOB_STATUS_IMPORTED = "imported"


def item_already_scheduled(it: dict[str, Any]) -> bool:
    """Clip đã có job lịch."""
    return bool(str(it.get("item_scheduled_job_id") or "").strip())


def job_uses_page_split(items: list[Any]) -> bool:
    """Job đã được «Chia đều Page» (ít nhất một clip có Page riêng)."""
    return any(isinstance(it, dict) and item_has_facebook_page(it) for it in items or [])


def select_items_for_schedule(
    items: list[Any],
    *,
    reimport: bool = False,
    check_files: bool = True,
) -> dict[str, Any]:
    """
    Trả về ``entries`` = ``[(index, item)]`` cần nạp và số clip bị bỏ qua theo lý do.

    - Đã có lịch: bỏ qua (trừ khi ``reimport``).
    - File video không còn trên đĩa: bỏ qua.
    - Job đã chia Page: clip chưa có Page để lại (``waiting_page``).
    """
    split = job_uses_page_split(items)
    entries: list[tuple[int, dict[str, Any]]] = []
    scheduled = missing = waiting = 0
    for i, it in enumerate(items or []):
        if not isinstance(it, dict):
            continue
        vp = str(it.get("video_path") or "").strip()
        if not vp:
            continue
        if item_already_scheduled(it) and not reimport:
            scheduled += 1
            continue
        if check_files and not Path(vp).is_file():
            missing += 1
            continue
        if split and str(it.get("item_publish_target") or "").strip().lower() != "tiktok" and not item_has_facebook_page(it):
            waiting += 1
            continue
        entries.append((i, it))
    return {
        "entries": entries,
        "uses_split": split,
        "skipped_scheduled": scheduled,
        "skipped_missing": missing,
        "waiting_page": waiting,
    }


def job_status_after_import(items: list[Any]) -> str:
    """``imported`` khi mọi clip có file đã có lịch; còn lại ``partial`` để lần sau nạp tiếp."""
    for it in items or []:
        if not isinstance(it, dict):
            continue
        if not str(it.get("video_path") or "").strip():
            continue
        if not item_already_scheduled(it):
            return JOB_STATUS_PARTIAL
    return JOB_STATUS_IMPORTED


def build_ve_fb_schedule_row(
    it: dict[str, Any],
    rec: dict[str, Any],
    account_id: str,
    page_id: str,
    plan: dict[str, Any],
    tz_name: str,
) -> dict[str, Any]:
    """Dòng ``schedule_posts`` đăng video clip lên đúng Page đã chọn."""
    vp = str(it.get("video_path") or "").strip()
    line = internal_post_title_from_body(str(it.get("content") or "").strip(), fallback="")
    if not line:
        line = internal_post_title_from_body(
            str(it.get("title") or Path(vp).stem).strip(), fallback=Path(vp).stem
        )
    row: dict[str, Any] = {
        "id": f"sched_{uuid.uuid4().hex[:10]}",
        "page_id": page_id,
        "account_id": account_id,
        "post_type": "video",
        "page_post_style": page_post_style_for_post_type("video"),
        "title": line,
        "content": line,
        "hashtags": [str(x).strip() for x in (it.get("hashtags") or []) if str(x).strip()],
        "media_files": [vp],
        "video_path": vp,
        "scheduled_at": plan["scheduled_at"],
        "timezone": tz_name,
        "schedule_recurrence": str(plan.get("schedule_recurrence") or ""),
        "schedule_slot": str(plan.get("schedule_slot") or ""),
        "status": "pending",
        "retry_count": 0,
        "max_retry": 3,
        "created_by": "video_editor_saved_job_import",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source_project_id": str(rec.get("source_project_id") or ""),
        "source_download_job_id": str(rec.get("source_download_job_id") or ""),
        "source_download_job_label": str(rec.get("source_download_job_label") or ""),
        "source_download_video_id": str(it.get("source_download_video_id") or ""),
    }
    if plan.get("schedule_daily_slots"):
        row["schedule_daily_slots"] = str(plan["schedule_daily_slots"])
        row["schedule_delay_min"] = int(plan.get("schedule_delay_min") or 0)
        row["schedule_delay_max"] = int(plan.get("schedule_delay_max") or 0)
        row["schedule_start_date"] = str(plan.get("schedule_start_date") or "")
    if plan.get("schedule_per_day"):
        row["schedule_per_day"] = int(plan["schedule_per_day"])
    sbl = str(plan.get("slot_base_local") or "").strip()
    if sbl:
        row["slot_base_local"] = sbl[:80]
    if "delay_applied_min" in plan:
        row["schedule_delay_applied_min"] = max(0, min(180, int(plan["delay_applied_min"] or 0)))
    return row


_OPEN_SCHEDULE_STATUSES = frozenset({"pending", "ready_queue", "processing", "running", "paused"})
_IGNORED_SCHEDULE_STATUSES = frozenset({"cancelled", "canceled", "deleted", "skipped"})
DEFAULT_DAILY_WINDOWS = "08:00-10:00, 14:00-16:00, 20:00-22:00"
DEFAULT_PER_DAY = 3


def _row_scheduled_dt(row: dict[str, Any]) -> datetime | None:
    """Đọc ``scheduled_at`` (ISO có múi giờ) của một dòng lịch."""
    raw = str(row.get("scheduled_at") or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo is not None else None


def _row_counts_toward_busy(row: dict[str, Any]) -> bool:
    """True nếu mốc này vẫn chiếm slot (bài đã đăng / đang chờ / lỗi — trừ lịch đã hủy)."""
    st = str(row.get("status") or "").strip().lower()
    return st not in _IGNORED_SCHEDULE_STATUSES


def _collect_schedule_times(existing_rows: list[Any], key_field: str) -> dict[str, list[datetime]]:
    """Gom mốc ``scheduled_at`` theo ``page_id`` hoặc ``account_id``."""
    out: dict[str, list[datetime]] = {}
    for row in existing_rows or []:
        if not isinstance(row, dict) or not _row_counts_toward_busy(row):
            continue
        key = str(row.get(key_field) or "").strip()
        dt = _row_scheduled_dt(row)
        if key and dt is not None:
            out.setdefault(key, []).append(dt)
    return out


def page_last_scheduled_at(existing_rows: list[Any]) -> dict[str, datetime]:
    """Mốc lịch muộn nhất của từng Page (kể cả đã đăng) — lần chia sau xếp tiếp từ đây."""
    out: dict[str, datetime] = {}
    for pid, times in _collect_schedule_times(existing_rows, "page_id").items():
        out[pid] = max(times)
    return out


def page_busy_times(existing_rows: list[Any]) -> dict[str, list[datetime]]:
    """Mọi mốc đã dùng của từng Page — không xếp trùng phút trên cùng Page."""
    return _collect_schedule_times(existing_rows, "page_id")


def account_busy_times(existing_rows: list[Any]) -> dict[str, list[datetime]]:
    """Mốc đã dùng theo tài khoản — Page cùng TK không đăng trùng phút / sát nhau."""
    return _collect_schedule_times(existing_rows, "account_id")


def infer_schedule_pattern(
    existing_rows: list[Any],
    *,
    page_ids: set[str] | None = None,
) -> dict[str, Any]:
    """
    Khung giờ + số video/ngày lấy từ lịch gần nhất (ưu tiên Page đang chọn).

    Để lần «Chia Page + lên lịch» sau giữ đúng lịch trình cũ, chỉ cộng slot tiếp theo.
    """
    from collections import Counter

    from src.utils.schedule_batch_preview import parse_daily_time_windows

    scored: list[tuple[datetime, str, int, bool]] = []
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    for row in existing_rows or []:
        if not isinstance(row, dict) or not _row_counts_toward_busy(row):
            continue
        slots = str(row.get("schedule_daily_slots") or row.get("schedule_slot") or "").strip()
        if not slots:
            continue
        try:
            windows = parse_daily_time_windows(slots)
        except ValueError:
            continue
        try:
            per_day = max(1, int(row.get("schedule_per_day") or 0))
        except (TypeError, ValueError):
            per_day = 0
        if per_day < 1:
            per_day = max(1, len(windows))
        dt = _row_scheduled_dt(row) or epoch
        pid = str(row.get("page_id") or "").strip()
        in_sel = bool(page_ids) and pid in page_ids
        scored.append((dt, slots, per_day, in_sel))
    if not scored:
        return {"windows": DEFAULT_DAILY_WINDOWS, "per_day": DEFAULT_PER_DAY}
    prefer = [x for x in scored if x[3]] or scored
    prefer.sort(key=lambda x: x[0], reverse=True)
    counted = Counter((x[1], x[2]) for x in prefer[:80])
    windows, per_day = counted.most_common(1)[0][0]
    return {"windows": windows, "per_day": int(per_day)}


_MOVABLE_SCHEDULE_STATUSES = frozenset({"pending", "paused"})


def respace_account_schedules(
    rows: list[Any],
    *,
    only_ids: set[str] | None = None,
    min_gap_minutes: float | None = None,
    max_gap_minutes: float | None = None,
    rng: Any = None,
) -> list[dict[str, Any]]:
    """
    Giãn lịch còn chờ: Page cùng tài khoản cách nhau ngẫu nhiên 2–10 phút.

    Chỉ dời lịch ``pending``/``paused`` (trong ``only_ids`` nếu có) về sau; lịch đang chạy giữ nguyên.
    Sửa ``rows`` tại chỗ, trả về các dòng đã đổi giờ.
    """
    from src.utils.schedule_batch_preview import (
        ACCOUNT_MAX_GAP_MINUTES,
        ACCOUNT_MIN_GAP_MINUTES,
        next_free_time,
    )

    gap = ACCOUNT_MIN_GAP_MINUTES if min_gap_minutes is None else float(min_gap_minutes)
    gap_hi = ACCOUNT_MAX_GAP_MINUTES if max_gap_minutes is None else float(max_gap_minutes)
    fixed: dict[str, list[datetime]] = {}
    movable: dict[str, list[tuple[datetime, dict[str, Any]]]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        st = str(row.get("status") or "").strip().lower()
        if st not in _OPEN_SCHEDULE_STATUSES:
            continue
        aid = str(row.get("account_id") or "").strip()
        dt = _row_scheduled_dt(row)
        if not aid or dt is None:
            continue
        rid = str(row.get("id") or "").strip()
        if st in _MOVABLE_SCHEDULE_STATUSES and (only_ids is None or rid in only_ids):
            movable.setdefault(aid, []).append((dt, row))
        else:
            fixed.setdefault(aid, []).append(dt)
    changed: list[dict[str, Any]] = []
    for aid, lst in movable.items():
        busy = list(fixed.get(aid, []))
        for dt, row in sorted(lst, key=lambda x: x[0]):
            new_dt = next_free_time(busy, dt, gap, gap_max_minutes=gap_hi, rng=rng).replace(microsecond=0)
            busy.append(new_dt)
            if new_dt != dt:
                row["scheduled_at"] = new_dt.astimezone(timezone.utc).isoformat()
                if row.get("slot_base_local"):
                    tzn = str(row.get("timezone") or "").strip()
                    try:
                        from zoneinfo import ZoneInfo

                        local = new_dt.astimezone(ZoneInfo(tzn)) if tzn else new_dt
                    except Exception:  # noqa: BLE001
                        local = new_dt
                    row["slot_base_local"] = local.strftime("%Y-%m-%d %H:%M")
                changed.append(row)
    return changed


def schedule_split_chunks(
    items: list[Any],
    rec: dict[str, Any],
    chunks: list[dict[str, Any]],
    *,
    start_date: Any,
    windows: list[tuple[tuple[int, int], tuple[int, int]]],
    per_day: int,
    tz_name: str,
    existing_rows: list[Any] | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """
    Lên lịch ngay cho các khúc vừa chia: mỗi Page ``per_day`` video/ngày trong khung giờ, lặp ngày sau.

    Gắn ``item_scheduled_*`` vào clip. Trả về các dòng ``schedule_posts`` cần lưu.
    """
    from src.utils.schedule_batch_preview import build_page_daily_schedules

    order: list[tuple[int, str, str]] = []
    for ch in chunks or []:
        aid = str(ch.get("account_id") or "").strip()
        pid = str(ch.get("page_id") or "").strip()
        for idx in ch.get("indexes") or []:
            if 0 <= int(idx) < len(items) and isinstance(items[int(idx)], dict):
                if item_already_scheduled(items[int(idx)]):
                    continue
                order.append((int(idx), aid, pid))
    if not order:
        return []
    existing = list(existing_rows or [])
    plans = build_page_daily_schedules(
        [pid for _i, _a, pid in order],
        start_date=start_date,
        windows=windows,
        per_day=per_day,
        timezone_name=tz_name,
        not_before_by_group=page_last_scheduled_at(existing),
        now=now,
        account_by_group={pid: aid for _i, aid, pid in order if aid},
        busy_by_account=account_busy_times(existing),
        busy_by_page=page_busy_times(existing),
    )
    stamp = datetime.now().isoformat(timespec="seconds")
    rows: list[dict[str, Any]] = []
    for (idx, aid, pid), plan in zip(order, plans):
        it = items[idx]
        row = build_ve_fb_schedule_row(it, rec, aid, pid, plan, tz_name)
        rows.append(row)
        it["item_scheduled_platform"] = "facebook"
        it["item_scheduled_job_id"] = str(row["id"])
        it["item_scheduled_at"] = stamp
    return rows


_SCHEDULE_ITEM_KEYS = ("item_scheduled_platform", "item_scheduled_job_id", "item_scheduled_at")
_PAGE_ITEM_KEYS = ("item_publish_target", "item_preset_fb_account_id", "item_preset_fb_page_id")


def release_items_for_deleted_schedules(jobs: list[Any], deleted_ids: list[str]) -> dict[str, int]:
    """
    Lịch đã xóa → clip về trạng thái chờ: bỏ liên kết lịch và Page đã gán, để chia lại.

    Sửa ``jobs`` tại chỗ; cập nhật trạng thái job (``saved`` / ``partial`` / ``imported``).
    """
    ids = {str(x).strip() for x in deleted_ids or [] if str(x).strip()}
    released = 0
    jobs_changed = 0
    if not ids:
        return {"released": 0, "jobs": 0}
    for job in jobs or []:
        if not isinstance(job, dict):
            continue
        items = job.get("items") if isinstance(job.get("items"), list) else []
        hit = 0
        for it in items:
            if not isinstance(it, dict):
                continue
            if str(it.get("item_scheduled_job_id") or "").strip() not in ids:
                continue
            for k in _SCHEDULE_ITEM_KEYS:
                it.pop(k, None)
            if str(it.get("item_publish_target") or "").strip().lower() != "tiktok":
                for k in _PAGE_ITEM_KEYS:
                    it.pop(k, None)
            hit += 1
        if not hit:
            continue
        released += hit
        jobs_changed += 1
        if any(isinstance(it, dict) and item_already_scheduled(it) for it in items):
            job["status"] = JOB_STATUS_PARTIAL
        else:
            job["status"] = "saved"
            for k in ("imported_at", "imported_to_platform", "imported_to_account_id", "imported_to_page_id"):
                job.pop(k, None)
    return {"released": released, "jobs": jobs_changed}


def job_item_counts(job: dict[str, Any], *, check_files: bool = True) -> dict[str, int]:
    """Đếm clip: tổng, đã có lịch, đang chờ (có file, chưa lịch), mất file."""
    total = scheduled = waiting = missing = with_page = 0
    for it in job.get("items") or []:
        if not isinstance(it, dict) or not str(it.get("video_path") or "").strip():
            continue
        total += 1
        if item_already_scheduled(it):
            scheduled += 1
            continue
        if check_files and not Path(str(it["video_path"])).is_file():
            missing += 1
            continue
        waiting += 1
        if item_has_facebook_page(it):
            with_page += 1
    return {"total": total, "scheduled": scheduled, "waiting": waiting, "missing": missing, "with_page": with_page}


def job_visible_in_pending(job: dict[str, Any], counts: dict[str, int], *, show_all: bool = False) -> bool:
    """Hiện job trong tab Chờ đăng khi còn video chưa lên lịch (job cũ đã import xong thì ẩn)."""
    if show_all:
        return True
    if str(job.get("status") or "").strip().lower() == JOB_STATUS_IMPORTED:
        return False
    return counts.get("waiting", 0) > 0


def unassign_pages(items: list[Any], indexes: list[int] | None = None) -> int:
    """Bỏ Page đã gán cho clip chưa lên lịch (tất cả hoặc theo ``indexes``)."""
    allowed = set(indexes) if indexes else None
    n = 0
    for i, it in enumerate(items or []):
        if allowed is not None and i not in allowed:
            continue
        if not isinstance(it, dict) or item_already_scheduled(it):
            continue
        if not item_has_facebook_page(it):
            continue
        for k in _PAGE_ITEM_KEYS:
            it.pop(k, None)
        n += 1
    return n


def schedule_ids_of_items(items: list[Any], indexes: list[int] | None = None) -> list[str]:
    """Mã lịch của các clip (tất cả hoặc theo ``indexes``)."""
    allowed = set(indexes) if indexes else None
    out: list[str] = []
    for i, it in enumerate(items or []):
        if allowed is not None and i not in allowed:
            continue
        if isinstance(it, dict) and item_already_scheduled(it):
            out.append(str(it["item_scheduled_job_id"]).strip())
    return out


def fb_page_owner_error(page_row: dict[str, Any] | None, account_id: str, page_id: str) -> str:
    """Lỗi nếu Page không có trong danh sách hoặc không thuộc tài khoản."""
    if not page_row:
        return f"Page {page_id} không còn trong danh sách Page."
    if str(page_row.get("account_id") or "").strip() != str(account_id or "").strip():
        return f"Page {page_id} không thuộc tài khoản {account_id}."
    return ""
