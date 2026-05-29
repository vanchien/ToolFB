"""Luồng tích hợp end-to-end (không Playwright / không mạng)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.services import schedule_queue_dispatcher as sqd
from src.utils.account_credentials import (
    account_can_auto_reauth,
    set_account_credentials,
)
from src.utils.concurrency_runtime import apply_multi_task_defaults
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.schedule_posts_manager import SchedulePostsManager


def _account_row(aid: str) -> dict:
    return {
        "id": aid,
        "name": "Test",
        "browser_type": "firefox",
        "portable_path": f"data/profiles/firefox/{aid}",
        "profile_path": f"data/profiles/firefox/{aid}",
        "cookie_path": f"data/cookies/{aid}.json",
        "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
        "use_proxy": False,
        "email": "u@example.com",
        "totp_enabled": True,
        "password_ref": f"account:{aid}",
        "totp_secret_ref": f"account:{aid}",
    }


def test_e2e_schedule_queue_pending_to_ready(tmp_path: Path) -> None:
    """Job pending quá hạn → ready_queue → dispatch batch."""
    sp_path = tmp_path / "schedule_posts.json"
    sp_path.write_text("[]\n", encoding="utf-8")

    past = "2020-01-01T00:00:00+00:00"
    mgr = SchedulePostsManager(json_path=sp_path)
    mgr.upsert(
        {
            "id": "j1",
            "account_id": "acc1",
            "page_id": "p1",
            "post_type": "text",
            "status": "pending",
            "scheduled_at": past,
            "created_at": past,
        }
    )

    now = datetime(2026, 5, 29, 12, 0, tzinfo=timezone.utc)
    promoted = sqd.promote_due_pending_to_ready(mgr, now=now)
    assert promoted >= 1
    rows = mgr.load_all()
    assert any(str(r.get("status")) == "ready_queue" for r in rows)

    batch = sqd.select_dispatch_batch(
        [r for r in rows if str(r.get("status")) == "ready_queue"],
        account_inflight=lambda _a: 0,
    )
    assert len(batch) >= 1


def test_e2e_account_credentials_and_auto_reauth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    acc_path = tmp_path / "accounts.json"
    cred_path = tmp_path / "account_credentials.json"
    acc_path.write_text("[]\n", encoding="utf-8")
    monkeypatch.setattr("src.utils.account_credentials.account_credentials_path", lambda: cred_path)

    aid = "acc_e2e"
    set_account_credentials(aid, password="secret", totp_secret="JBSWY3DPEHPK3PXP")
    AccountsDatabaseManager(json_path=acc_path).upsert(_account_row(aid))
    rec = AccountsDatabaseManager(json_path=acc_path).get_by_id(aid)
    assert rec is not None
    assert account_can_auto_reauth(dict(rec))


def test_e2e_multitask_defaults_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BROWSER_CONCURRENCY", raising=False)
    apply_multi_task_defaults(gui=True)
    assert os.environ.get("TOOLFB_AUTO_MULTITASK") == "1"
    assert int(os.environ.get("SCHEDULE_PER_ACCOUNT_MAX_PARALLEL", "1")) >= 1
