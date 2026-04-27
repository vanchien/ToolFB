"""
Pure filter/sort helpers cho danh sách job ``schedule_posts.json``.

- Không phụ thuộc GUI — dễ unit-test.
- ``apply_job_filters``: lọc theo search text + các bộ lọc (account, page, post_type, status, retry).
- ``sort_jobs``: sắp xếp theo cột (datetime cho scheduled_at, số nguyên cho retry_count).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable


RETRY_MODES: tuple[str, ...] = ("all", "retry_0", "retry_gt_0", "retry_ge_2")

SORT_KEYS: tuple[str, ...] = (
    "id",
    "page_id",
    "account_id",
    "post_type",
    "ai_language",
    "title",
    "image_prompt",
    "scheduled_at",
    "status",
    "retry_count",
)


def _norm(v: Any) -> str:
    """Chuẩn hóa về chuỗi lowercase để so khớp chuỗi con."""
    if v is None:
        return ""
    return str(v).strip().lower()


def _job_search_haystack(job: dict[str, Any]) -> str:
    """
    Dồn các trường có thể tìm kiếm thành 1 blob để khớp chuỗi con.

    Trường được gom: id, page_id, account_id, post_type, ai_language,
    title, content, image_prompt, scheduled_at (ISO + local), status, hashtags.
    """
    parts: list[str] = []
    for key in (
        "id",
        "page_id",
        "account_id",
        "post_type",
        "ai_language",
        "title",
        "content",
        "image_prompt",
        "image_alt",
        "status",
    ):
        parts.append(_norm(job.get(key)))
    sched_raw = _norm(job.get("scheduled_at"))
    parts.append(sched_raw)
    local_str = _norm(job.get("_display_scheduled_local"))
    if local_str and local_str != sched_raw:
        parts.append(local_str)
    tags = job.get("hashtags") or job.get("tags") or []
    if isinstance(tags, list):
        parts.append(" ".join(_norm(t) for t in tags))
    else:
        parts.append(_norm(tags))
    return " | ".join(p for p in parts if p)


def _retry_ok(job: dict[str, Any], mode: str) -> bool:
    """Kiểm tra job có khớp lựa chọn retry không."""
    m = _norm(mode) or "all"
    if m == "all":
        return True
    try:
        rc = int(job.get("retry_count") or 0)
    except (TypeError, ValueError):
        rc = 0
    if m == "retry_0":
        return rc == 0
    if m == "retry_gt_0":
        return rc > 0
    if m == "retry_ge_2":
        return rc >= 2
    return True


def apply_job_filters(
    jobs: Iterable[dict[str, Any]],
    *,
    search_text: str = "",
    account: str = "",
    page_id: str = "",
    post_type: str = "",
    status: str = "",
    retry_mode: str = "",
) -> list[dict[str, Any]]:
    """
    Lọc danh sách job theo nhiều tiêu chí kết hợp (AND).

    - ``search_text``: chuỗi con, không phân biệt hoa thường, match trong nhiều trường.
    - ``account``, ``page_id``, ``post_type``, ``status``: so khớp đúng (lowercase).
      Giá trị rỗng = không lọc.
    - ``retry_mode``: ``all`` | ``retry_0`` | ``retry_gt_0`` | ``retry_ge_2``.

    Returns:
        Danh sách job thỏa mãn toàn bộ điều kiện.
    """
    search_lc = _norm(search_text)
    acc_lc = _norm(account)
    pid_lc = _norm(page_id)
    pt_lc = _norm(post_type)
    st_lc = _norm(status)

    out: list[dict[str, Any]] = []
    for j in jobs:
        if acc_lc and _norm(j.get("account_id")) != acc_lc:
            continue
        if pid_lc and _norm(j.get("page_id")) != pid_lc:
            continue
        if pt_lc and _norm(j.get("post_type")) != pt_lc:
            continue
        if st_lc and _norm(j.get("status")) != st_lc:
            continue
        if not _retry_ok(j, retry_mode):
            continue
        if search_lc and search_lc not in _job_search_haystack(j):
            continue
        out.append(j)
    return out


def _parse_iso_to_utc(value: Any) -> datetime:
    """Parse ``scheduled_at`` về UTC; giá trị sai/trống → datetime.max để xếp cuối."""
    s = str(value or "").strip()
    if not s:
        return datetime.max.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return datetime.max.replace(tzinfo=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def sort_jobs(
    jobs: Iterable[dict[str, Any]],
    *,
    sort_key: str,
    ascending: bool = True,
) -> list[dict[str, Any]]:
    """
    Sắp xếp danh sách job theo ``sort_key``.

    - ``scheduled_at``: parse sang datetime UTC (đúng thứ tự thời gian).
    - ``retry_count``: sắp theo số nguyên.
    - Các cột còn lại: sắp theo chuỗi lowercase.
    """
    key = _norm(sort_key) or "scheduled_at"
    lst = list(jobs)

    if key == "scheduled_at":
        def keyfunc(j: dict[str, Any]) -> Any:
            return _parse_iso_to_utc(j.get("scheduled_at"))
    elif key == "retry_count":
        def keyfunc(j: dict[str, Any]) -> Any:
            try:
                return int(j.get("retry_count") or 0)
            except (TypeError, ValueError):
                return 0
    else:
        def keyfunc(j: dict[str, Any]) -> Any:
            return _norm(j.get(key))

    lst.sort(key=keyfunc, reverse=not ascending)
    return lst


def is_overdue(job: dict[str, Any], *, now_utc: datetime | None = None) -> bool:
    """True nếu job ``pending`` có ``scheduled_at`` trong quá khứ."""
    st = _norm(job.get("status"))
    if st != "pending":
        return False
    sched = _parse_iso_to_utc(job.get("scheduled_at"))
    cur = now_utc or datetime.now(timezone.utc)
    return sched < cur
