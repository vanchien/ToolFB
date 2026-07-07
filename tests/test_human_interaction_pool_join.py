"""Thời gian chờ pool — đủ cho nhiều TK / nhiều luồng."""

from __future__ import annotations

from src.services.human_interaction_pool import compute_pool_join_timeout_sec


def test_join_timeout_targets_fifteen_to_twenty_minutes(monkeypatch) -> None:
    monkeypatch.delenv("FB_POOL_TARGET_MINUTES", raising=False)
    monkeypatch.delenv("FB_POOL_JOIN_BUFFER_SEC", raising=False)
    monkeypatch.delenv("FB_POOL_JOIN_MIN_SEC", raising=False)
    monkeypatch.delenv("FB_POOL_JOIN_MAX_SEC", raising=False)
    monkeypatch.delenv("FB_HUMAN_WORKER_MAX_SEC", raising=False)
    monkeypatch.delenv("FB_HUMAN_INTERACTION_MAX_RETRIES", raising=False)
    t4 = compute_pool_join_timeout_sec(9, max_concurrent=4, login_only=False)
    t9 = compute_pool_join_timeout_sec(9, max_concurrent=9, login_only=False)
    assert 900.0 <= t4 <= 3600.0
    assert 900.0 <= t9 <= 3600.0
    # 9 TK / 4 luồng × worker 300s × 3 lần thử ≈ 2820s
    assert t4 >= 2400.0
    assert compute_pool_join_timeout_sec(0) == 120.0


def test_join_timeout_scales_with_retries(monkeypatch) -> None:
    monkeypatch.setenv("FB_HUMAN_WORKER_MAX_SEC", "300")
    monkeypatch.setenv("FB_HUMAN_INTERACTION_MAX_RETRIES", "0")
    t_no_retry = compute_pool_join_timeout_sec(8, max_concurrent=4)
    monkeypatch.setenv("FB_HUMAN_INTERACTION_MAX_RETRIES", "2")
    t_retry = compute_pool_join_timeout_sec(8, max_concurrent=4)
    assert t_retry > t_no_retry


def test_should_abort_worker_respects_graceful_shutdown() -> None:
    from src.models.mapped_account import MappedAccount
    from src.services.human_interaction_pool import HumanInteractionPool

    ma = MappedAccount(account_id="UID_1", use_proxy=False)
    pool = HumanInteractionPool([ma], max_concurrent=1, login_only=False)
    pool._stop.set()
    assert pool.should_abort_worker() is True
    pool._graceful_shutdown = True
    assert pool.should_abort_worker() is False


def test_format_human_pool_error_timeout() -> None:
    from src.services.human_interaction_pool import format_human_pool_error

    msg = format_human_pool_error(
        TimeoutError("Page.goto: Timeout 90000ms exceeded. Call log: ...")
    )
    assert "timeout" in msg.lower() or "chậm" in msg.lower()
