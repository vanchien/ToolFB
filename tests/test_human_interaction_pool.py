"""Tests điều phối đa luồng HumanInteractionPool."""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest

from src.models.mapped_account import MappedAccount
from src.services.human_interaction_pool import HumanInteractionPool


def _fake_mapped(aid: str, *, proxy: str = "127.0.0.1:8001") -> MappedAccount:
    return MappedAccount.from_dict(
        {
            "account_id": aid,
            "uid": aid,
            "password": "x",
            "proxy_server": f"socks5://{proxy}",
            "status": "pending",
        }
    )


def test_enter_slot_respects_stop() -> None:
    pool = HumanInteractionPool([_fake_mapped("a1")], max_concurrent=1, login_only=True)
    pool._running = 1
    pool._stop.set()
    assert pool._enter_slot() is False
    assert pool._running == 1


def test_stop_notifies_waiting_workers() -> None:
    pool = HumanInteractionPool([_fake_mapped("a1")], max_concurrent=1, login_only=True)
    pool._running = 1
    results: list[bool] = []

    def waiter() -> None:
        results.append(pool._enter_slot())

    t = threading.Thread(target=waiter, daemon=True)
    t.start()
    time.sleep(0.3)
    assert not results
    pool.stop()
    t.join(timeout=2.0)
    assert results == [False]


def test_running_not_leaked_on_worker_exception() -> None:
    acc = _fake_mapped("ex1")
    pool = HumanInteractionPool([acc], max_concurrent=2, login_only=True)

    with patch(
        "src.services.human_interaction_pool.run_human_interaction_worker",
        side_effect=RuntimeError("boom"),
    ):
        pool.start()
        pool.join(timeout=30.0)

    assert pool._running == 0
    assert acc.status == "error"


def test_login_only_ignores_lowered_dynamic_limit() -> None:
    pool = HumanInteractionPool([_fake_mapped("a1")], max_concurrent=4, login_only=True)
    pool._dynamic_limit = 1
    assert pool._enter_slot() is True
    pool._on_worker_done("success")
    assert pool._running == 0


def test_proxy_busy_requeues_without_leaking_running() -> None:
    a1 = _fake_mapped("p1", proxy="127.0.0.1:9001")
    a2 = _fake_mapped("p2", proxy="127.0.0.1:9001")
    done: list[str] = []

    def fake_worker(mapped, **kwargs):  # noqa: ANN001
        done.append(mapped.account_id)
        return "login_ok"

    with patch(
        "src.services.human_interaction_pool.run_human_interaction_worker",
        side_effect=fake_worker,
    ):
        pool = HumanInteractionPool([a1, a2], max_concurrent=2, login_only=True)
        pool.start()
        pool.join(timeout=15.0)

    assert pool._running == 0
    assert len(done) == 2
    assert not pool._threads


def test_pool_drains_more_than_max_concurrent() -> None:
    """Sau khi 4 luồng xong, pool phải tiếp tục TK 5+ (không kẹt ở batch đầu)."""
    accounts = [_fake_mapped(f"a{i}", proxy=f"127.0.0.1:90{i:02d}") for i in range(6)]
    order: list[str] = []
    max_running = {"n": 0}

    def fake_worker(mapped, **kwargs):  # noqa: ANN001
        order.append(mapped.account_id)
        with pool._state_lock:
            max_running["n"] = max(max_running["n"], pool._running)
        time.sleep(0.05)
        cb = kwargs.get("on_work_finished")
        if cb is not None:
            cb("success")
        return "success"

    with patch(
        "src.services.human_interaction_pool.run_human_interaction_worker",
        side_effect=fake_worker,
    ):
        pool = HumanInteractionPool(accounts, max_concurrent=4, login_only=True)
        pool.start()
        pool.join(timeout=30.0)

    assert pool._running == 0
    assert len(order) == 6
    assert set(order) == {a.account_id for a in accounts}
    assert pool._completed_accounts == 6
    assert max_running["n"] <= 4
