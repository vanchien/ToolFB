"""Tự động tối ưu đa tác vụ + khóa JSON."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

import pytest

from src.utils.concurrency_runtime import (
    KIND_DOWNLOAD,
    KIND_SCHEDULER,
    KIND_VIDEO_EDITOR,
    apply_multi_task_defaults,
    reconcile_multi_task_limits,
    workload_begin,
    workload_end,
    workload_snapshot,
)
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.json_store_lock import json_file_lock
from src.utils.schedule_posts_manager import SchedulePostsManager


def test_apply_defaults_sets_auto_multitask(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TOOLFB_AUTO_MULTITASK", raising=False)
    monkeypatch.delenv("BROWSER_CONCURRENCY", raising=False)
    apply_multi_task_defaults(gui=True)
    assert os.environ.get("TOOLFB_AUTO_MULTITASK") == "1"
    assert int(os.environ["BROWSER_CONCURRENCY"]) >= 2


def test_reconcile_lowers_limits_when_many_kinds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TOOLFB_AUTO_MULTITASK", "1")
    monkeypatch.delenv("TOOLFB_MANUAL_CONCURRENCY", raising=False)
    apply_multi_task_defaults()
    base = int(os.environ["BROWSER_CONCURRENCY"])
    workload_begin(KIND_DOWNLOAD)
    workload_begin(KIND_VIDEO_EDITOR)
    workload_begin(KIND_SCHEDULER)
    applied = reconcile_multi_task_limits(browser_slots_in_use=2)
    assert int(applied["BROWSER_CONCURRENCY"]) <= base
    assert int(applied["TOOLFB_FFMPEG_CONCURRENCY"]) >= 1
    workload_end(KIND_DOWNLOAD)
    workload_end(KIND_VIDEO_EDITOR)
    workload_end(KIND_SCHEDULER)


def test_manual_concurrency_skips_reconcile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TOOLFB_AUTO_MULTITASK", "1")
    monkeypatch.setenv("TOOLFB_MANUAL_CONCURRENCY", "1")
    monkeypatch.setenv("BROWSER_CONCURRENCY", "5")
    workload_begin(KIND_DOWNLOAD)
    applied = reconcile_multi_task_limits()
    assert applied == {}
    assert os.environ["BROWSER_CONCURRENCY"] == "5"
    workload_end(KIND_DOWNLOAD)


def test_json_lock_serializes_parallel_writes(tmp_path: Path) -> None:
    p = tmp_path / "x.json"
    p.write_text("[]\n", encoding="utf-8")
    errors: list[str] = []

    def writer(prefix: str) -> None:
        try:
            with json_file_lock(p):
                data = json.loads(p.read_text(encoding="utf-8"))
                data.append({"id": prefix})
                p.write_text(json.dumps(data) + "\n", encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))

    threads = [threading.Thread(target=writer, args=(f"t{i}",)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    final = json.loads(p.read_text(encoding="utf-8"))
    assert len(final) == 6


def test_accounts_upsert_under_lock(tmp_path: Path) -> None:
    path = tmp_path / "accounts.json"
    path.write_text("[]\n", encoding="utf-8")
    mgr = AccountsDatabaseManager(json_path=path)
    for i in range(4):
        mgr.upsert(
            {
                "id": f"acc_{i}",
                "name": f"n{i}",
                "browser_type": "firefox",
                "portable_path": f"data/profiles/firefox/acc_{i}",
                "profile_path": f"data/profiles/firefox/acc_{i}",
                "cookie_path": f"data/cookies/acc_{i}.json",
                "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
                "use_proxy": False,
            }
        )
    rows = mgr.load_all()
    assert len(rows) == 4


def test_schedule_posts_concurrent_upsert(tmp_path: Path) -> None:
    path = tmp_path / "schedule_posts.json"
    path.write_text("[]\n", encoding="utf-8")
    mgr = SchedulePostsManager(json_path=path)

    def add(j: int) -> None:
        mgr.upsert(
            {
                "id": f"job_{j}",
                "account_id": "a1",
                "page_id": "p1",
                "post_type": "text",
                "status": "pending",
                "scheduled_at": "2026-05-29T10:00:00+00:00",
                "created_at": "2026-05-29T09:00:00+00:00",
            }
        )

    threads = [threading.Thread(target=add, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(mgr.load_all()) == 5


def test_workload_snapshot() -> None:
    workload_begin(KIND_DOWNLOAD)
    snap = workload_snapshot()
    assert snap[KIND_DOWNLOAD] >= 1
    workload_end(KIND_DOWNLOAD)
    assert workload_snapshot()[KIND_DOWNLOAD] == 0
