"""Trình duyệt đóng bất ngờ không được gán nhầm «cancelled»; pool tự thử lại."""

from __future__ import annotations

from unittest.mock import patch

from src.automation.browser_factory import is_playwright_target_closed_error
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


def test_is_playwright_target_closed_error() -> None:
    class TargetClosedError(Exception):
        pass

    assert is_playwright_target_closed_error(TargetClosedError("bye"))
    assert is_playwright_target_closed_error(RuntimeError("Target page, context or browser has been closed"))
    assert not is_playwright_target_closed_error(RuntimeError("timeout"))


def test_browser_closed_retries_then_succeeds(monkeypatch) -> None:
    monkeypatch.setenv("FB_HUMAN_INTERACTION_MAX_RETRIES", "2")
    acc = _fake_mapped("bc1")
    calls = {"n": 0}

    def fake_worker(mapped, **kwargs):  # noqa: ANN001
        calls["n"] += 1
        if calls["n"] == 1:
            return "browser_closed"
        mapped.status = "success"
        mapped.status_detail = "ok"
        return "success"

    with patch(
        "src.services.human_interaction_pool.run_human_interaction_worker",
        side_effect=fake_worker,
    ):
        pool = HumanInteractionPool([acc], max_concurrent=1, login_only=False)
        pool.start()
        assert pool.join(timeout=20.0) is True

    assert calls["n"] == 2
    assert acc.status == "success"
    assert pool._completed_accounts == 1


def test_join_shutdown_without_user_cancel_is_not_stopped() -> None:
    pool = HumanInteractionPool([_fake_mapped("j1")], max_concurrent=1, login_only=True)
    pool._shutting_down = True
    pool._stop.set()
    assert pool.is_stopped() is False
