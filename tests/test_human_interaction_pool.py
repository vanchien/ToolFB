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


def test_stop_drained_queue_calls_task_done() -> None:
    """Hủy item trong hàng đợi phải ``task_done`` — ``join`` không kẹt vì ``unfinished_tasks``."""
    pending = [_fake_mapped(f"s{i}", proxy=f"127.0.0.1:91{i:02d}") for i in range(3)]

    def fast_fail(mapped, **kwargs):  # noqa: ANN001
        return "proxy_error"

    with patch(
        "src.services.human_interaction_pool.run_human_interaction_worker",
        side_effect=fast_fail,
    ):
        pool = HumanInteractionPool(pending, max_concurrent=1, login_only=True)
        pool.start()
        time.sleep(0.15)
        pool.stop()
        assert pool.join(timeout=15.0) is True
    assert pool._work_q.unfinished_tasks == 0


def test_join_waits_after_stop_until_workers_finish() -> None:
    acc = _fake_mapped("slow1")
    started = threading.Event()
    release = threading.Event()

    def slow_worker(mapped, **kwargs):  # noqa: ANN001
        started.set()
        release.wait(timeout=5.0)
        return "cancelled"

    with patch(
        "src.services.human_interaction_pool.run_human_interaction_worker",
        side_effect=slow_worker,
    ):
        pool = HumanInteractionPool([acc], max_concurrent=1, login_only=True)
        pool.start()
        assert started.wait(timeout=5.0)
        pool.stop()
        release.set()
        assert pool.join(timeout=20.0) is True
        assert pool._join_workers_alive is False


def test_enter_slot_rereads_dynamic_limit_while_waiting() -> None:
    """Health nâng limit khi đang chờ — worker phải vào slot, không kẹt số cũ."""
    pool = HumanInteractionPool([_fake_mapped("a1")], max_concurrent=4, login_only=False)
    pool._running = 2
    pool._dynamic_limit = 2
    got: list[bool] = []

    def waiter() -> None:
        got.append(pool._enter_slot())

    t = threading.Thread(target=waiter, daemon=True)
    t.start()
    time.sleep(0.35)
    assert not got
    with pool._state_cv:
        pool._dynamic_limit = 4
        pool._state_cv.notify_all()
    t.join(timeout=2.0)
    assert got == [True]
    assert pool._running == 3


def test_interrupted_rotates_to_waiting_account_immediately(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Hết thời gian lượt: nhả slot, chạy TK đang chờ ngay — không đợi cả lượt 14 luồng.
    """
    monkeypatch.setenv("FB_POOL_WORKER_WARMUP_MS", "0")
    monkeypatch.setenv("FB_HUMAN_ACCOUNT_ROTATE_MAX", "2")
    accounts = [_fake_mapped(f"a{i}", proxy=f"127.0.0.1:81{i:02d}") for i in range(4)]
    order: list[str] = []
    visits: dict[str, int] = {}
    a1_hold = threading.Event()
    a2_started = threading.Event()

    def fake_worker(mapped, **kwargs):  # noqa: ANN001
        aid = mapped.account_id
        visits[aid] = visits.get(aid, 0) + 1
        order.append(aid)
        if aid == "a0" and visits[aid] == 1:
            mapped.status = "pending"
            mapped.status_detail = "Hết thời gian lượt"
            return "interrupted"
        if aid == "a1" and visits[aid] == 1:
            assert a2_started.wait(timeout=5.0), "TK chờ phải chạy khi luồng kia xong, không đợi cả lượt"
            a1_hold.set()
            return "success"
        if aid == "a2":
            a2_started.set()
            assert a1_hold.wait(timeout=5.0)
            return "success"
        return "success"

    with patch(
        "src.services.human_interaction_pool.run_human_interaction_worker",
        side_effect=fake_worker,
    ):
        pool = HumanInteractionPool(accounts, max_concurrent=2, login_only=True)
        pool.start()
        assert pool.join(timeout=20.0) is True

    assert "a2" in order
    assert visits.get("a0", 0) >= 2
    assert pool._completed_accounts == 4
    assert pool._running == 0
    assert set(order) >= {"a0", "a1", "a2", "a3"}


def test_pool_start_does_not_prepare_accounts_on_caller() -> None:
    """start() chỉ xếp hàng — không chuẩn bị profile trên luồng gọi (GUI)."""
    accounts = [_fake_mapped(f"a{i}", proxy=f"127.0.0.1:70{i:02d}") for i in range(5)]
    prep_ids: list[str] = []

    def track_prep(mapped, **kwargs):  # noqa: ANN001
        del kwargs
        prep_ids.append(mapped.account_id)
        return {}

    def fake_worker(mapped, **kwargs):  # noqa: ANN001
        del mapped, kwargs
        return "login_ok"

    with patch(
        "src.utils.account_proxy_mapper.prepare_mapped_account_for_browser_run",
        side_effect=track_prep,
    ), patch(
        "src.services.human_interaction_pool.run_human_interaction_worker",
        side_effect=fake_worker,
    ):
        pool = HumanInteractionPool(accounts, max_concurrent=2, login_only=True)
        pool.start()
        pool.join(timeout=15.0)
    assert prep_ids == []
    assert pool._completed_accounts == 5


def test_health_floor_does_not_collapse_concurrency() -> None:
    pool = HumanInteractionPool(
        [_fake_mapped(f"a{i}") for i in range(4)],
        max_concurrent=14,
        login_only=False,
    )
    assert pool._health_floor() == 7
