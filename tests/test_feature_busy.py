"""Khóa chéo giữa lịch đăng và chia sẻ — ưu tiên đăng rồi mới chia sẻ."""

from __future__ import annotations

import random
import threading
import time

from src.utils import feature_busy as fb
from src.utils.feature_busy import (
    FEATURE_SCHEDULE,
    FEATURE_SHARE,
    acquire,
    acquire_share_after_schedule,
    busy_reason,
    conflict_gap_seconds,
    is_cross_busy,
    normalize_page_keys,
    reset_for_tests,
    try_acquire,
)


def setup_function() -> None:
    reset_for_tests()


def teardown_function() -> None:
    reset_for_tests()


def test_same_page_blocks_cross_feature() -> None:
    keys = normalize_page_keys("local-1", "fb-99")
    lease = try_acquire(feature=FEATURE_SCHEDULE, account_id="accA", page_keys=keys, detail="job1")
    assert lease is not None
    assert try_acquire(feature=FEATURE_SHARE, account_id="accB", page_keys={"fb-99"}) is None
    assert is_cross_busy(feature=FEATURE_SHARE, page_keys={"fb-99"})
    assert "đăng lịch" in busy_reason(page_keys={"fb-99"})
    lease.release()
    assert try_acquire(feature=FEATURE_SHARE, account_id="accB", page_keys={"fb-99"}) is not None


def test_same_account_blocks_cross_feature() -> None:
    a = try_acquire(feature=FEATURE_SHARE, account_id="acc1", page_keys=())
    assert a is not None
    assert try_acquire(feature=FEATURE_SCHEDULE, account_id="acc1", page_keys={"p1"}) is None
    a.release()
    assert try_acquire(feature=FEATURE_SCHEDULE, account_id="acc1", page_keys={"p1"}) is not None


def test_same_feature_refcount_allows_parallel() -> None:
    a = try_acquire(feature=FEATURE_SCHEDULE, account_id="acc1", page_keys={"p1"})
    b = try_acquire(feature=FEATURE_SCHEDULE, account_id="acc1", page_keys={"p2"})
    assert a is not None and b is not None
    a.release()
    assert is_cross_busy(feature=FEATURE_SHARE, account_id="acc1")
    b.release()
    assert not is_cross_busy(feature=FEATURE_SHARE, account_id="acc1")


def test_normalize_page_keys_from_row() -> None:
    keys = normalize_page_keys(page_row={"id": "abc", "fb_page_id": "123"})
    assert keys == frozenset({"abc", "123"})


def test_conflict_gap_is_two_to_five_minutes() -> None:
    rng = random.Random(1)
    for _ in range(20):
        sec = conflict_gap_seconds(rng=rng)
        assert 120.0 <= sec <= 300.0


def test_user_can_set_conflict_gap_range() -> None:
    assert fb.set_conflict_gap_minutes(3, 8) == (3.0, 8.0)
    assert fb.get_conflict_gap_minutes() == (3.0, 8.0)
    rng = random.Random(2)
    for _ in range(15):
        sec = conflict_gap_seconds(rng=rng)
        assert 180.0 <= sec <= 480.0
    assert fb.parse_conflict_gap_minutes("1", "4") == (1.0, 4.0)
    assert fb.set_conflict_gap_minutes(1, 4) == (1.0, 4.0)


def test_auto_gap_empty_schedule_is_short(monkeypatch) -> None:
    """Không có job lịch → giãn rất ngắn (~12–36s) để chia sẻ không đứng chờ."""
    monkeypatch.setattr(fb, "schedule_load_for_account", lambda _aid="": (0, 0, 0))
    fb.set_conflict_gap_auto(True)
    assert fb.recommend_conflict_gap_minutes("accX") == (0.2, 0.6)
    assert fb.effective_conflict_gap_minutes("accX") == (0.2, 0.6)
    assert "Tự động" in fb.describe_conflict_gap("accX")
    assert "giây" in fb.describe_conflict_gap("accX")


def test_auto_gap_scales_with_schedule_load(monkeypatch) -> None:
    monkeypatch.setattr(fb, "schedule_load_for_account", lambda _aid="": (2, 0, 0))
    assert fb.recommend_conflict_gap_minutes("a") == (0.8, 1.5)
    monkeypatch.setattr(fb, "schedule_load_for_account", lambda _aid="": (3, 1, 0))
    assert fb.recommend_conflict_gap_minutes("a") == (1.5, 3.0)
    monkeypatch.setattr(fb, "schedule_load_for_account", lambda _aid="": (5, 2, 2))
    assert fb.recommend_conflict_gap_minutes("a") == (2.5, 4.5)


def test_manual_gap_capped_at_ten_minutes() -> None:
    fb.set_conflict_gap_auto(False)
    fb.set_conflict_gap_minutes(8, 30)
    lo, hi = fb.effective_conflict_gap_minutes()
    assert lo == 8.0
    assert hi == fb.CONFLICT_GAP_MANUAL_CAP_MINUTES


def test_share_auto_gap_after_schedule(monkeypatch) -> None:
    """Chế độ auto: sau đăng lịch nghỉ theo recommend, không dùng khoảng tay dài."""
    held = try_acquire(feature=FEATURE_SCHEDULE, account_id="accAuto", page_keys={"pA"})
    assert held is not None
    monkeypatch.setattr(fb, "schedule_queue_blocks_share", lambda **_kwargs: False)
    monkeypatch.setattr(fb, "schedule_load_for_account", lambda _aid="": (0, 0, 0))
    # Giãn test ngắn — tránh chờ 12–36s thật.
    monkeypatch.setattr(fb, "recommend_conflict_gap_minutes", lambda _aid="": (0.01, 0.02))
    monkeypatch.setattr(fb, "effective_conflict_gap_minutes", lambda _aid="": (0.01, 0.02))
    fb.set_conflict_gap_auto(True)
    fb.set_conflict_gap_minutes(8, 30)  # tay dài — auto phải bỏ qua
    waits: list[str] = []

    def _release_soon() -> None:
        def _go() -> None:
            time.sleep(0.2)
            held.release()

        threading.Thread(target=_go, daemon=True).start()

    _release_soon()
    lease = acquire_share_after_schedule(
        account_id="accAuto",
        page_keys={"pA"},
        poll=0.1,
        rng=random.Random(0),
        on_wait=waits.append,
    )
    assert lease is not None
    assert any("tự động" in w for w in waits)
    lease.release()


def test_acquire_waits_then_gets_slot() -> None:
    held = try_acquire(feature=FEATURE_SCHEDULE, account_id="accW", page_keys={"px"})
    assert held is not None

    def _release_soon() -> None:
        def _go() -> None:
            time.sleep(0.3)
            held.release()

        threading.Thread(target=_go, daemon=True).start()

    _release_soon()
    got = acquire(feature=FEATURE_SHARE, account_id="accW", page_keys={"px"}, timeout=2.0, poll=0.1)
    assert got is not None
    got.release()


def test_share_waits_for_schedule_then_gap_then_runs(monkeypatch) -> None:
    """Chia sẻ chờ đăng lịch xong, nghỉ giãn cách ngắn (test), rồi giữ chỗ — không bỏ cuộc."""
    held = try_acquire(feature=FEATURE_SCHEDULE, account_id="accS", page_keys={"page1"})
    assert held is not None
    monkeypatch.setattr(fb, "schedule_queue_blocks_share", lambda **_kwargs: False)
    waits: list[str] = []

    def _release_soon() -> None:
        def _go() -> None:
            time.sleep(0.25)
            held.release()

        threading.Thread(target=_go, daemon=True).start()

    _release_soon()
    lease = acquire_share_after_schedule(
        account_id="accS",
        page_keys={"page1"},
        detail="test",
        poll=0.1,
        gap_min_minutes=0.01,
        gap_max_minutes=0.02,
        rng=random.Random(0),
        on_wait=waits.append,
    )
    assert lease is not None
    assert any("đăng lịch" in w or "giãn cách" in w for w in waits)
    lease.release()


def test_share_stops_when_cancelled(monkeypatch) -> None:
    held = try_acquire(feature=FEATURE_SCHEDULE, account_id="accC", page_keys={"p"})
    assert held is not None
    monkeypatch.setattr(fb, "schedule_queue_blocks_share", lambda **_kwargs: False)
    stop_after = time.monotonic() + 0.4

    lease = acquire_share_after_schedule(
        account_id="accC",
        page_keys={"p"},
        poll=0.1,
        gap_min_minutes=0.01,
        gap_max_minutes=0.02,
        should_stop=lambda: time.monotonic() >= stop_after,
    )
    assert lease is None
    held.release()
