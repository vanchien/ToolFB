"""
Tình trạng từng Page cho bảng quản lý hàng nghìn Page / nhiều tài khoản.

Gộp ``pages.json`` với ``schedule_posts.json`` một lần (O(số Page + số job)):
số bài chờ đăng, giờ bài kế tiếp, lỗi gần đây, lâu chưa đăng, thiếu link/ID.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

HEALTH_OK = "ok"
HEALTH_FAILING = "failing"
HEALTH_NO_SCHEDULE = "no_schedule"
HEALTH_STALE = "stale"
HEALTH_MISSING = "missing"

HEALTH_LABELS: dict[str, str] = {
    HEALTH_OK: "Ổn",
    HEALTH_FAILING: "Lỗi đăng",
    HEALTH_NO_SCHEDULE: "Hết lịch",
    HEALTH_STALE: "Lâu chưa đăng",
    HEALTH_MISSING: "Thiếu link/ID",
}
# Thứ tự ưu tiên khi sắp xếp: cần xử lý trước nằm trên.
HEALTH_ORDER: tuple[str, ...] = (
    HEALTH_MISSING,
    HEALTH_FAILING,
    HEALTH_NO_SCHEDULE,
    HEALTH_STALE,
    HEALTH_OK,
)

_OPEN_STATUSES = frozenset({"pending", "ready_queue", "running", "processing", "paused"})
_FAIL_STATUSES = frozenset({"failed", "need_manual_check"})

STATUS_LABELS: dict[str, str] = {
    "pending": "Chưa đăng",
    "success": "Đã đăng",
    "failed": "Lỗi",
}


@dataclass
class PageHealth:
    """Tình trạng một Page."""

    health: str = HEALTH_OK
    upcoming: int = 0
    next_at: datetime | None = None
    failed_recent: int = 0
    success_recent: int = 0
    last_post_at: datetime | None = None
    last_fail_at: datetime | None = None
    last_error: str = ""
    last_failed_job_id: str = ""
    notes: list[str] = field(default_factory=list)

    @property
    def label(self) -> str:
        """Nhãn tiếng Việt cho cột «Tình trạng»."""
        return HEALTH_LABELS.get(self.health, self.health)


def _parse_dt(raw: Any) -> datetime | None:
    """ISO / ``YYYY-MM-DD HH:MM`` → datetime có múi giờ; không đọc được thì None."""
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00").replace(" ", "T", 1))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.astimezone()
    return dt


def _job_time(job: dict[str, Any]) -> datetime | None:
    """Giờ của job: đã đăng thì lấy posted_at, chưa thì scheduled_at."""
    return _parse_dt(job.get("posted_at") or job.get("updated_at") or job.get("scheduled_at"))


def _clip_error(note: str, *, max_len: int = 160) -> str:
    """Rút gọn ghi chú lỗi để hiện trên bảng Page."""
    text = " ".join(str(note or "").split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def build_page_health(
    pages: Iterable[dict[str, Any]],
    jobs: Iterable[dict[str, Any]],
    *,
    now: datetime | None = None,
    recent_hours: float = 24.0,
    stale_days: float = 3.0,
    fail_threshold: int = 1,
) -> dict[str, PageHealth]:
    """
    Tính tình trạng cho mọi Page.

    Quy tắc (ưu tiên từ trên xuống):

    - Thiếu ``page_url`` hoặc ``fb_page_id`` → «Thiếu link/ID».
    - ≥ ``fail_threshold`` job lỗi trong ``recent_hours`` giờ (mặc định 1) mà chưa có
      đăng thành công mới hơn → «Lỗi đăng». Page có ``status=failed`` cũng tính.
    - Không còn job chờ đăng → «Hết lịch».
    - Lần đăng gần nhất cũ hơn ``stale_days`` ngày → «Lâu chưa đăng».
    - Còn lại → «Ổn».
    """
    now = now or datetime.now(timezone.utc)
    recent_from = now - timedelta(hours=recent_hours)
    by_page: dict[str, PageHealth] = {}
    last_fail: dict[str, datetime] = {}
    last_ok: dict[str, datetime] = {}

    for job in jobs:
        pid = str(job.get("page_id") or "").strip()
        if not pid:
            continue
        h = by_page.setdefault(pid, PageHealth())
        st = str(job.get("status") or "").strip().lower()
        if st in _OPEN_STATUSES:
            h.upcoming += 1
            when = _parse_dt(job.get("scheduled_at"))
            if when is not None and (h.next_at is None or when < h.next_at):
                h.next_at = when
            continue
        when = _job_time(job)
        if when is None:
            continue
        if st == "success":
            if h.last_post_at is None or when > h.last_post_at:
                h.last_post_at = when
            if when >= recent_from:
                h.success_recent += 1
            if pid not in last_ok or when > last_ok[pid]:
                last_ok[pid] = when
        elif st in _FAIL_STATUSES:
            note = _clip_error(str(job.get("error_note") or ""))
            if h.last_fail_at is None or when > h.last_fail_at:
                h.last_fail_at = when
                h.last_error = note
                h.last_failed_job_id = str(job.get("id") or "").strip()
            if when >= recent_from:
                h.failed_recent += 1
            if pid not in last_fail or when > last_fail[pid]:
                last_fail[pid] = when

    out: dict[str, PageHealth] = {}
    for page in pages:
        pid = str(page.get("id") or "").strip()
        if not pid:
            continue
        h = by_page.get(pid) or PageHealth()
        page_last = _parse_dt(page.get("last_post_at"))
        if page_last is not None and (h.last_post_at is None or page_last > h.last_post_at):
            h.last_post_at = page_last
        missing = [
            name
            for key, name in (("page_url", "link"), ("fb_page_id", "Meta Page ID"))
            if not str(page.get(key) or "").strip()
        ]
        page_failed = str(page.get("status") or "").strip().lower() == "failed"
        # Phục hồi = đã có đăng thành công sau lần lỗi gần nhất (job hoặc chỉ còn status trên Page).
        if pid in last_ok and pid in last_fail:
            recovered = last_ok[pid] > last_fail[pid]
        elif pid in last_ok and page_failed:
            recovered = True  # job success mới hơn, pages.json chưa kịp đổi → không báo lỗi nữa
        else:
            recovered = False

        last_attempt_failed = (
            (pid in last_fail and (pid not in last_ok or last_fail[pid] >= last_ok[pid]))
            or (page_failed and not recovered)
        )

        if missing:
            h.health = HEALTH_MISSING
            h.notes.append("Thiếu " + ", ".join(missing))
        elif not recovered and (h.failed_recent >= fail_threshold or page_failed):
            h.health = HEALTH_FAILING
            if h.failed_recent:
                h.notes.append(f"{h.failed_recent} lần lỗi trong {recent_hours:.0f} giờ")
            else:
                h.notes.append("Lần đăng gần nhất thất bại")
            if h.last_error:
                h.notes.append(h.last_error)
        elif h.upcoming == 0:
            h.health = HEALTH_NO_SCHEDULE
            if last_attempt_failed and h.last_error:
                h.notes.append(f"Lần đăng cuối lỗi · {h.last_error}")
            elif last_attempt_failed:
                h.notes.append("Lần đăng cuối lỗi — thêm lịch hoặc xem job lỗi")
        elif h.last_post_at is not None and now - h.last_post_at > timedelta(days=stale_days):
            h.health = HEALTH_STALE
            h.notes.append(f"Lần cuối {h.last_post_at.astimezone().strftime('%d/%m %H:%M')}")
        else:
            h.health = HEALTH_OK
        out[pid] = h
    return out


def summarize_health(health: dict[str, PageHealth], page_ids: Iterable[str] | None = None) -> dict[str, int]:
    """Đếm số Page theo từng tình trạng (chỉ trong ``page_ids`` nếu truyền)."""
    counts = {key: 0 for key in HEALTH_ORDER}
    ids = health.keys() if page_ids is None else page_ids
    for pid in ids:
        h = health.get(pid)
        if h is not None:
            counts[h.health] = counts.get(h.health, 0) + 1
    return counts


def pages_per_account(pages: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Số Page của từng tài khoản (để hiện trên ô chọn tài khoản)."""
    out: dict[str, int] = {}
    for page in pages:
        aid = str(page.get("account_id") or "").strip()
        if aid:
            out[aid] = out.get(aid, 0) + 1
    return out


def account_ids_for_page_filter(
    pages: Iterable[dict[str, Any]],
    accounts: Iterable[dict[str, Any]] | None = None,
) -> list[str]:
    """
    Tài khoản hiện trên ô lọc / hộp quét Page.

    Mọi id trong ``accounts.json`` (kể cả 0 Page) rồi mới tới owner còn sót
    trong pages.json — TK mới thêm vẫn chọn được để quét.
    """
    seen: list[str] = []
    known: set[str] = set()
    for acc in accounts or ():
        aid = str(acc.get("id") or acc.get("account_id") or "").strip()
        if aid and aid not in known:
            known.add(aid)
            seen.append(aid)
    for page in pages:
        aid = str(page.get("account_id") or "").strip()
        if aid and aid not in known:
            known.add(aid)
            seen.append(aid)
    return seen


def format_schedule_cell(h: PageHealth | None, *, now: datetime | None = None) -> str:
    """Ô «Lịch chờ»: số bài chờ + giờ bài kế tiếp (giờ máy)."""
    if h is None or h.upcoming <= 0:
        return "—"
    if h.next_at is None:
        return f"{h.upcoming} bài"
    local = h.next_at.astimezone()
    today = (now or datetime.now(timezone.utc)).astimezone().date()
    when = local.strftime("%H:%M") if local.date() == today else local.strftime("%d/%m %H:%M")
    return f"{h.upcoming} bài · {when}"


def format_status_label(raw: str) -> str:
    """Trạng thái lần đăng cuối bằng tiếng Việt."""
    key = str(raw or "").strip().lower() or "pending"
    return STATUS_LABELS.get(key, key)


def format_last_outcome_cell(
    status: str,
    health: PageHealth | None = None,
    *,
    max_len: int = 72,
) -> str:
    """
    Ô «Lần cuối»: Đã đăng / Chưa đăng / Lỗi · lý do ngắn.

    Lý do lấy từ job lỗi mới nhất của Page (``PageHealth.last_error``).
    """
    key = str(status or "").strip().lower() or "pending"
    base = format_status_label(key)
    if key != "failed":
        return base
    err = _clip_error((health.last_error if health else "") or "", max_len=max(20, max_len - len(base) - 3))
    if not err:
        return base
    text = f"{base} · {err}"
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"
