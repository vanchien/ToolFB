"""UI chia sẻ: bước live phải hiện đúng TK + Page đang chạy (kể cả Page đang chờ cùng TK)."""

from __future__ import annotations

from src.gui.share_tab import describe_page_jobs
from src.services.facebook_groups.engine import GroupEngine
from src.services.facebook_groups.store import GroupStore


class _SilentProvider:
    pass


def test_live_step_for_current_and_sibling_pages(tmp_path) -> None:
    store = GroupStore(tmp_path / "facebook_groups.json")
    engine = GroupEngine(store, _SilentProvider(), sleep=lambda _s: None)
    engine._account_labels = {"accA": "Andy"}
    batch = {"account_id": "accA", "id": "b1"}
    engine._share_step(batch, page_id="p1", page_name="Page Một", step="Đang chuyển Page")
    assert engine.live_step_for("accA", "p1") == "Đang chuyển Page"
    assert engine.live_step_for("accA", "p2").startswith("Chờ trong TK — đang: Page Một")
    board = engine.live_board()
    assert "Andy" in board and "Page Một" in board


def test_describe_page_jobs_shows_running_and_wait_same_account() -> None:
    jobs = [{"status": "PENDING", "account_id": "a", "page_id": "1"}]
    assert describe_page_jobs(jobs, live_step="Đang chia sẻ link")[0] == "Đang chạy"
    label, detail, tag = describe_page_jobs(jobs, live_step="Chờ trong TK — đang: Page X")
    assert label == "Chờ TK"
    assert "Page X" in detail
    assert tag == "wait"


def test_live_steps_snapshot_copies(tmp_path) -> None:
    store = GroupStore(tmp_path / "facebook_groups.json")
    engine = GroupEngine(store, _SilentProvider(), sleep=lambda _s: None)
    engine._share_step({"account_id": "acc1"}, page_id="p", page_name="P", step="X")
    snap = engine.live_steps_snapshot()
    assert snap["acc1"]["step"] == "X"
    snap["acc1"]["step"] = "mutated"
    assert engine.live_step_for("acc1", "p") == "X"
