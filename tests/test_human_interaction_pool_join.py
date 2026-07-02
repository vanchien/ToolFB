"""Thời gian chờ pool — đủ cho nhiều TK / nhiều luồng."""

from __future__ import annotations

from src.services.human_interaction_pool import compute_pool_join_timeout_sec


def test_join_timeout_scales_with_accounts_and_threads(monkeypatch) -> None:
    monkeypatch.delenv("FB_POOL_JOIN_SEC_PER_ACCOUNT", raising=False)
    monkeypatch.delenv("FB_POOL_JOIN_BUFFER_SEC", raising=False)
    t4 = compute_pool_join_timeout_sec(9, max_concurrent=4, login_only=False)
    t9 = compute_pool_join_timeout_sec(9, max_concurrent=9, login_only=False)
    assert t4 >= 300.0
    assert t9 >= 300.0
    assert compute_pool_join_timeout_sec(0) == 120.0


def test_format_human_pool_error_timeout() -> None:
    from src.services.human_interaction_pool import format_human_pool_error

    msg = format_human_pool_error(
        TimeoutError("Page.goto: Timeout 90000ms exceeded. Call log: ...")
    )
    assert "timeout" in msg.lower() or "chậm" in msg.lower()
