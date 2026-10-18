"""
Khóa tài khoản / Page dùng chung giữa **lịch đăng** và **chia sẻ**.

Ưu tiên: **đăng lịch xong → mới chia sẻ**. Trùng giờ thì chia sẻ chờ + giãn cách 2–5 phút.
Cả hai luồng đều chờ tới lượt — không bỏ job vì xung đột.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from loguru import logger

FEATURE_SCHEDULE = "schedule"
FEATURE_SHARE = "share"

# Giãn cách mặc định sau khi đăng lịch xong rồi mới chia sẻ (phút) — người dùng đổi được.
CONFLICT_GAP_MIN_MINUTES = 2
CONFLICT_GAP_MAX_MINUTES = 5

_FEATURE_LABEL = {
    FEATURE_SCHEDULE: "đăng lịch",
    FEATURE_SHARE: "chia sẻ",
}

_gate = threading.RLock()
_accounts: dict[str, "_Holder"] = {}
_pages: dict[str, "_Holder"] = {}
_rng = random.Random()
_conflict_gap_min = float(CONFLICT_GAP_MIN_MINUTES)
_conflict_gap_max = float(CONFLICT_GAP_MAX_MINUTES)


def get_conflict_gap_minutes() -> tuple[float, float]:
    """Khoảng giãn cách hiện tại (phút) giữa đăng lịch và chia sẻ."""
    with _gate:
        return (_conflict_gap_min, _conflict_gap_max)


def set_conflict_gap_minutes(lo: float, hi: float) -> tuple[float, float]:
    """
    Cài khoảng giãn cách do người dùng chọn (phút).

    Trả về ``(từ, đến)`` đã chuẩn hóa (0 ≤ từ ≤ đến ≤ 180).
    """
    global _conflict_gap_min, _conflict_gap_max
    try:
        a = float(lo)
        b = float(hi)
    except (TypeError, ValueError):
        a, b = float(CONFLICT_GAP_MIN_MINUTES), float(CONFLICT_GAP_MAX_MINUTES)
    a = max(0.0, min(180.0, a))
    b = max(0.0, min(180.0, b))
    if a > b:
        a, b = b, a
    with _gate:
        same = (_conflict_gap_min, _conflict_gap_max) == (a, b)
        _conflict_gap_min, _conflict_gap_max = a, b
    if not same:
        logger.info("[FeatureBusy] Giãn cách đăng→chia sẻ: {}–{} phút", a, b)
    return (a, b)


def parse_conflict_gap_minutes(low: object, high: object) -> tuple[float, float]:
    """Đọc ô «từ–đến» phút; rỗng / lỗi → mặc định 2–5."""
    try:
        a = float(str(low).strip())
        b = float(str(high).strip())
    except (TypeError, ValueError):
        return (float(CONFLICT_GAP_MIN_MINUTES), float(CONFLICT_GAP_MAX_MINUTES))
    return set_conflict_gap_minutes(a, b)


@dataclass
class _Holder:
    feature: str
    count: int = 1
    detail: str = ""
    since: float = field(default_factory=time.monotonic)


@dataclass
class BusyLease:
    """Giữ chỗ tài khoản/Page; gọi ``release()`` hoặc dùng ``with``."""

    feature: str
    account_id: str
    page_keys: frozenset[str]
    detail: str = ""
    _released: bool = False

    def release(self) -> None:
        """Trả chỗ đã giữ."""
        if self._released:
            return
        self._released = True
        _release_lease(self)

    def __enter__(self) -> "BusyLease":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()


def feature_label(feature: str) -> str:
    """Nhãn tiếng Việt cho log / UI."""
    return _FEATURE_LABEL.get(str(feature or "").strip(), str(feature or "tác vụ khác"))


def normalize_page_keys(
    *refs: str,
    page_row: dict[str, Any] | None = None,
) -> frozenset[str]:
    """
    Gom mọi mã nhận diện cùng một Page (local id, Meta id, …) thành một tập khóa.

    Hai luồng trùng bất kỳ mã nào → coi là cùng Page.
    """
    out: set[str] = set()
    for raw in refs:
        value = str(raw or "").strip()
        if value:
            out.add(value)
    if isinstance(page_row, dict):
        for key in ("id", "fb_page_id", "page_id"):
            value = str(page_row.get(key) or "").strip()
            if value:
                out.add(value)
    return frozenset(out)


def conflict_gap_seconds(
    *,
    lo_minutes: float | None = None,
    hi_minutes: float | None = None,
    rng: random.Random | None = None,
) -> float:
    """Số giây nghỉ ngẫu nhiên trong khoảng người dùng đã cài (mặc định cấu hình hiện tại)."""
    cur_lo, cur_hi = get_conflict_gap_minutes()
    low = float(cur_lo if lo_minutes is None else lo_minutes)
    high = float(cur_hi if hi_minutes is None else hi_minutes)
    pick = rng or _rng
    low, high = sorted((max(0.0, low), max(0.0, high)))
    high = max(low, high)
    return pick.uniform(low * 60.0, high * 60.0)


def busy_reason(*, account_id: str = "", page_keys: Iterable[str] = ()) -> str:
    """Mô tả ngắn vì sao chưa chạy được (rỗng nếu không bận chéo feature)."""
    aid = str(account_id or "").strip()
    keys = {str(k).strip() for k in page_keys if str(k).strip()}
    with _gate:
        if aid:
            holder = _accounts.get(aid)
            if holder:
                return f"Tài khoản đang {feature_label(holder.feature)}" + (
                    f" ({holder.detail})" if holder.detail else ""
                )
        for key in keys:
            holder = _pages.get(key)
            if holder:
                return f"Page đang {feature_label(holder.feature)}" + (
                    f" ({holder.detail})" if holder.detail else ""
                )
    return ""


def is_cross_busy(
    *,
    feature: str,
    account_id: str = "",
    page_keys: Iterable[str] = (),
) -> bool:
    """True nếu tài khoản/Page đang bị feature khác giữ."""
    want = str(feature or "").strip()
    aid = str(account_id or "").strip()
    keys = {str(k).strip() for k in page_keys if str(k).strip()}
    with _gate:
        if aid:
            holder = _accounts.get(aid)
            if holder and holder.feature != want:
                return True
        for key in keys:
            holder = _pages.get(key)
            if holder and holder.feature != want:
                return True
    return False


_schedule_busy_lock = threading.Lock()
_schedule_busy_snap: tuple[float, set[str], set[str]] | None = None
_SCHEDULE_BUSY_TTL_SEC = 2.5


def _page_id_aliases() -> dict[str, set[str]]:
    """Map mọi mã Page → tập alias (local + Meta) — cache ngắn theo singleton PagesManager."""
    try:
        from src.utils.pages_manager import get_default_pages_manager

        rows = get_default_pages_manager().load_all()
    except Exception:  # noqa: BLE001
        return {}
    aliases: dict[str, set[str]] = {}
    for row in rows:
        lid = str(row.get("id") or "").strip()
        fb = str(row.get("fb_page_id") or "").strip()
        group = {x for x in (lid, fb) if x}
        for key in group:
            aliases.setdefault(key, set()).update(group)
    return aliases


def _schedule_busy_index() -> tuple[set[str], set[str]]:
    """
    Tập account / page đang sẵn sàng hoặc đang đăng — TTL ngắn để poll chờ không đọc lại cả queue.
    """
    global _schedule_busy_snap
    now = time.monotonic()
    with _schedule_busy_lock:
        snap = _schedule_busy_snap
        if snap is not None and (now - snap[0]) < _SCHEDULE_BUSY_TTL_SEC:
            return snap[1], snap[2]
    accounts: set[str] = set()
    pages: set[str] = set()
    try:
        from src.utils.schedule_posts_manager import get_default_schedule_posts_manager

        jobs = get_default_schedule_posts_manager().load_all()
    except Exception:  # noqa: BLE001
        jobs = []
    aliases = _page_id_aliases()
    for job in jobs:
        status = str(job.get("status") or "").strip().lower()
        if status not in {"ready_queue", "running"}:
            continue
        job_aid = str(job.get("account_id") or "").strip()
        if job_aid:
            accounts.add(job_aid)
        job_pid = str(job.get("page_id") or "").strip()
        if job_pid:
            pages.add(job_pid)
            pages.update(aliases.get(job_pid) or ())
    with _schedule_busy_lock:
        _schedule_busy_snap = (now, accounts, pages)
    return accounts, pages


def schedule_queue_blocks_share(*, account_id: str = "", page_keys: Iterable[str] = ()) -> bool:
    """
    True nếu lịch đăng đang / sắp dùng account hoặc Page này (ready_queue / running).

    Dùng để chia sẻ nhường ưu tiên — đăng lịch xong mới chia sẻ.
    """
    aid = str(account_id or "").strip()
    keys = {str(k).strip() for k in page_keys if str(k).strip()}
    busy_accounts, busy_pages = _schedule_busy_index()
    if aid and aid in busy_accounts:
        return True
    if keys and busy_pages.intersection(keys):
        return True
    return False


def try_acquire(
    *,
    feature: str,
    account_id: str = "",
    page_keys: Iterable[str] = (),
    detail: str = "",
) -> BusyLease | None:
    """
    Giữ chỗ ngay nếu không xung đột chéo feature.

    Cùng feature: tăng refcount (cho phép song song nội bộ).
    """
    feat = str(feature or "").strip()
    if not feat:
        return None
    aid = str(account_id or "").strip()
    keys = frozenset(str(k).strip() for k in page_keys if str(k).strip())
    note = str(detail or "").strip()
    with _gate:
        if aid:
            holder = _accounts.get(aid)
            if holder and holder.feature != feat:
                return None
        for key in keys:
            holder = _pages.get(key)
            if holder and holder.feature != feat:
                return None
        if aid:
            existing = _accounts.get(aid)
            if existing:
                existing.count += 1
                if note:
                    existing.detail = note
            else:
                _accounts[aid] = _Holder(feature=feat, count=1, detail=note)
        for key in keys:
            existing = _pages.get(key)
            if existing:
                existing.count += 1
                if note:
                    existing.detail = note
            else:
                _pages[key] = _Holder(feature=feat, count=1, detail=note)
        lease = BusyLease(feature=feat, account_id=aid, page_keys=keys, detail=note)
        logger.debug(
            "[FeatureBusy] giữ {} | account={} | pages={} | detail={}",
            feat,
            aid or "—",
            ",".join(sorted(keys)) or "—",
            note or "—",
        )
        return lease


def acquire(
    *,
    feature: str,
    account_id: str = "",
    page_keys: Iterable[str] = (),
    detail: str = "",
    timeout: float = 90.0,
    poll: float = 1.0,
) -> BusyLease | None:
    """Chờ tới ``timeout`` giây để giữ chỗ; hết giờ trả ``None``."""
    deadline = time.monotonic() + max(0.0, float(timeout))
    interval = max(0.2, float(poll))
    while True:
        lease = try_acquire(
            feature=feature,
            account_id=account_id,
            page_keys=page_keys,
            detail=detail,
        )
        if lease is not None:
            return lease
        if time.monotonic() >= deadline:
            return None
        time.sleep(min(interval, max(0.0, deadline - time.monotonic())))


def acquire_share_after_schedule(
    *,
    account_id: str = "",
    page_keys: Iterable[str] = (),
    detail: str = "",
    poll: float = 2.0,
    should_stop: Callable[[], bool] | None = None,
    on_wait: Callable[[str], None] | None = None,
    gap_min_minutes: float | None = None,
    gap_max_minutes: float | None = None,
    rng: random.Random | None = None,
) -> BusyLease | None:
    """
    Chia sẻ nhường đăng lịch: chờ lịch xong → nghỉ theo khoảng người dùng cài → giữ chỗ.

    Không bỏ cuộc vì xung đột; trả ``None`` chỉ khi ``should_stop()``.
    """
    aid = str(account_id or "").strip()
    keys = frozenset(str(k).strip() for k in page_keys if str(k).strip())
    interval = max(0.5, float(poll))
    stop = should_stop or (lambda: False)
    notify = on_wait or (lambda _msg: None)
    saw_schedule = False
    cfg_lo, cfg_hi = get_conflict_gap_minutes()
    gap_lo = cfg_lo if gap_min_minutes is None else float(gap_min_minutes)
    gap_hi = cfg_hi if gap_max_minutes is None else float(gap_max_minutes)

    while not stop():
        # Ưu tiên lịch: còn job ready/running hoặc đang giữ khóa schedule → chờ.
        if schedule_queue_blocks_share(account_id=aid, page_keys=keys) or is_cross_busy(
            feature=FEATURE_SHARE, account_id=aid, page_keys=keys
        ):
            saw_schedule = True
            reason = busy_reason(account_id=aid, page_keys=keys) or "đang ưu tiên đăng lịch"
            notify(f"Chờ đăng lịch xong — {reason}")
            time.sleep(interval)
            continue

        if saw_schedule:
            gap = conflict_gap_seconds(lo_minutes=gap_lo, hi_minutes=gap_hi, rng=rng)
            minutes = max(1, int(round(gap / 60.0))) if gap >= 30 else max(1, int(round(gap)))
            unit = "phút" if gap >= 30 else "giây"
            notify(f"Đăng lịch xong — nghỉ giãn cách ~{minutes} {unit} rồi mới chia sẻ")
            logger.info(
                "[FeatureBusy] Chia sẻ chờ giãn cách {:.0f}s sau đăng lịch | account={} | pages={}",
                gap,
                aid or "—",
                ",".join(sorted(keys)) or "—",
            )
            deadline = time.monotonic() + gap
            while time.monotonic() < deadline and not stop():
                # Lịch mới xen vào trong lúc nghỉ → quay lại chờ đăng.
                if schedule_queue_blocks_share(account_id=aid, page_keys=keys) or is_cross_busy(
                    feature=FEATURE_SHARE, account_id=aid, page_keys=keys
                ):
                    saw_schedule = True
                    break
                time.sleep(min(interval, max(0.2, deadline - time.monotonic())))
            else:
                if stop():
                    return None
                # Hết giãn cách, không bị lịch cắt → thử giữ chỗ.
                saw_schedule = False
                lease = try_acquire(
                    feature=FEATURE_SHARE,
                    account_id=aid,
                    page_keys=keys,
                    detail=detail,
                )
                if lease is not None:
                    return lease
            continue

        lease = try_acquire(
            feature=FEATURE_SHARE,
            account_id=aid,
            page_keys=keys,
            detail=detail,
        )
        if lease is not None:
            return lease
        time.sleep(interval)

    return None


def acquire_schedule_preferring(
    *,
    account_id: str = "",
    page_keys: Iterable[str] = (),
    detail: str = "",
    timeout: float = 180.0,
    poll: float = 2.0,
) -> BusyLease | None:
    """
    Đăng lịch: chờ chia sẻ nhả chỗ (tối đa ``timeout``), không ghi lỗi — tick sau chạy tiếp nếu hết giờ.
    """
    return acquire(
        feature=FEATURE_SCHEDULE,
        account_id=account_id,
        page_keys=page_keys,
        detail=detail,
        timeout=timeout,
        poll=poll,
    )


def _release_lease(lease: BusyLease) -> None:
    with _gate:
        if lease.account_id:
            holder = _accounts.get(lease.account_id)
            if holder and holder.feature == lease.feature:
                holder.count -= 1
                if holder.count <= 0:
                    _accounts.pop(lease.account_id, None)
        for key in lease.page_keys:
            holder = _pages.get(key)
            if holder and holder.feature == lease.feature:
                holder.count -= 1
                if holder.count <= 0:
                    _pages.pop(key, None)
    logger.debug(
        "[FeatureBusy] trả {} | account={} | pages={}",
        lease.feature,
        lease.account_id or "—",
        ",".join(sorted(lease.page_keys)) or "—",
    )


def reset_for_tests() -> None:
    """Xóa toàn bộ chỗ đang giữ — chỉ dùng trong unit test."""
    global _conflict_gap_min, _conflict_gap_max, _schedule_busy_snap
    with _gate:
        _accounts.clear()
        _pages.clear()
        _conflict_gap_min = float(CONFLICT_GAP_MIN_MINUTES)
        _conflict_gap_max = float(CONFLICT_GAP_MAX_MINUTES)
    with _schedule_busy_lock:
        _schedule_busy_snap = None
