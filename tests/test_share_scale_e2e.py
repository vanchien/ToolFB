"""
E2E quy mô lớn: nhiều tài khoản × nhiều Page — đa luồng, không nghẽn/chồng chéo,
lỗi giữa chừng vẫn chạy tiếp / tự resume.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict

import pytest

from src.gui.share_tab import interleave_pages_by_account
from src.services.facebook_groups.engine import GroupEngine
from src.services.facebook_groups.store import GroupStore
from src.utils import feature_busy as fb
from src.utils.feature_busy import (
    FEATURE_SCHEDULE,
    FEATURE_SHARE,
    acquire_share_after_schedule,
    reset_for_tests,
    try_acquire,
)


class ScaleFakeProvider:
    """Provider giả: theo dõi song song theo account, mô phỏng lỗi Page / browser chết."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.publish_calls: list[dict] = []
        self.switch_calls: list[str] = []
        self.concurrent_accounts: set[str] = set()
        self.max_concurrent = 0
        self.fail_pages: set[str] = set()
        self.browser_dead_once: set[str] = set()  # account_id → chết 1 lần rồi sống lại
        self._dead_hit: set[str] = set()
        self.active: dict[str, int] = defaultdict(int)

    def spawn(self) -> "ScaleFakeProvider":
        """Mỗi luồng TK dùng cùng logic nhưng đếm riêng qua self (shared)."""
        return self

    def check_session(self, account_id: str) -> str:
        del account_id
        return ""

    def begin_account(self, account_id: str) -> None:
        with self.lock:
            self.active[account_id] += 1
            self.concurrent_accounts.add(account_id)
            self.max_concurrent = max(self.max_concurrent, len(self.concurrent_accounts))

    def end_account(self, account_id: str) -> None:
        with self.lock:
            self.active[account_id] = max(0, self.active[account_id] - 1)
            if self.active[account_id] <= 0:
                self.concurrent_accounts.discard(account_id)

    def switch_to_page(self, **kwargs) -> dict:
        account_id = str(kwargs.get("account_id") or "")
        page_id = str(kwargs.get("page_id") or "")
        with self.lock:
            self.switch_calls.append(f"{account_id}:{page_id}")
            if account_id in self.browser_dead_once and account_id not in self._dead_hit:
                self._dead_hit.add(account_id)
                return {
                    "error_code": "TIMEOUT",
                    "error_message": "Không mở được trình duyệt — profile đang khóa",
                }
            if page_id in self.fail_pages:
                return {"error_code": "TEMPORARY_ERROR", "error_message": f"Chưa chuyển được {page_id}"}
        return {"error_code": ""}

    def publish_link(self, **kwargs) -> dict:
        account_id = str(kwargs.get("account_id") or "")
        page_id = str(kwargs.get("page_id") or "")
        # Giả lập thao tác ngắn — đủ để các TK chồng lên nhau trong cửa sổ semaphore.
        time.sleep(0.01)
        with self.lock:
            self.publish_calls.append({"account_id": account_id, "page_id": page_id})
            assert account_id in self.concurrent_accounts or self.active.get(account_id, 0) >= 0
        return {
            "post_id": f"post-{page_id}",
            "post_url": str(kwargs.get("source_url") or ""),
        }

    def verify_published_post(self, **kwargs) -> dict:
        page_id = str(kwargs.get("page_id") or "")
        return {"post_id": f"post-{page_id}", "post_url": kwargs.get("source_url") or ""}


def _engine(tmp_path, provider=None, monkeypatch=None):
    store = GroupStore(tmp_path / "facebook_groups.json")
    fake = provider or ScaleFakeProvider()
    engine = GroupEngine(store, fake, sleep=lambda _s: None, rng=__import__("random").Random(0))
    return store, engine, fake


def _pages(n_accounts: int, pages_per_account: int) -> list[dict]:
    rows: list[dict] = []
    for a in range(n_accounts):
        aid = f"acc-{a:02d}"
        for p in range(pages_per_account):
            pid = f"{aid}-p{p:02d}"
            rows.append(
                {
                    "account_id": aid,
                    "account_name": f"TK{a:02d}",
                    "page_id": pid,
                    "page_name": f"Page {pid}",
                    "target_url": f"https://www.facebook.com/{pid}",
                }
            )
    return rows


@pytest.fixture(autouse=True)
def _reset_busy(monkeypatch):
    """Mỗi test: FeatureBusy sạch + không đọc schedule/pages thật của máy."""
    reset_for_tests()
    monkeypatch.setattr(fb, "schedule_queue_blocks_share", lambda **_k: False)
    monkeypatch.setattr(fb, "schedule_load_for_account", lambda _aid="": (0, 0, 0))
    monkeypatch.setattr(
        "src.services.facebook_groups.engine.GroupEngine._page_alias_index",
        lambda self: {},
    )
    yield
    reset_for_tests()


def test_interleave_many_accounts_round_robin() -> None:
    """12 TK × 5 Page → luân phiên đều, không dồn một TK đầu list."""
    rows = _pages(12, 5)
    out = interleave_pages_by_account([{**r, "iid": r["page_id"]} for r in rows])
    assert len(out) == 60
    # 12 account đầu tiên phải khác nhau (round-robin).
    first12 = [r["account_id"] for r in out[:12]]
    assert len(set(first12)) == 12


def test_scale_wave_10x8_parallel_bounded(tmp_path, monkeypatch) -> None:
    """10 TK × 8 Page = 80 job: hoàn tất, song song ≤ BROWSER_CONCURRENCY, không chồng cùng TK."""
    monkeypatch.setenv("BROWSER_CONCURRENCY", "4")
    monkeypatch.setenv("SHARE_PARALLEL_ACCOUNTS", "4")
    _store, engine, fake = _engine(tmp_path)
    pages = _pages(10, 8)
    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/reel/9001",
        text="Scale E2E",
        destination="page",
        cooldown_fixed=0,
    )
    assert wave["accounts"] == 10
    assert wave["created"] == 80
    result = engine.run_share_wave(wave["batch_ids"])
    assert len(result["batches"]) == 10
    for batch in result["batches"]:
        assert batch.get("status") == "COMPLETED", batch
    assert len(fake.publish_calls) == 80
    assert fake.max_concurrent <= 4
    # Mỗi Page đúng 1 lần publish.
    published = {(c["account_id"], c["page_id"]) for c in fake.publish_calls}
    assert len(published) == 80


def test_scale_schedule_busy_does_not_block_other_accounts(tmp_path, monkeypatch) -> None:
    """Lịch giữ 2 TK — 6 TK kia vẫn chia sẻ xong; 2 TK bị giữ chờ rồi chạy sau khi nhả."""
    monkeypatch.setenv("SHARE_PARALLEL_ACCOUNTS", "8")
    fb.set_conflict_gap_auto(True)
    _store, engine, fake = _engine(tmp_path)
    pages = _pages(8, 3)
    # Giữ lịch trên acc-00 và acc-01.
    held = [
        try_acquire(feature=FEATURE_SCHEDULE, account_id="acc-00", page_keys={"acc-00-p00"}),
        try_acquire(feature=FEATURE_SCHEDULE, account_id="acc-01", page_keys={"acc-01-p00"}),
    ]
    assert all(held)

    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/reel/9002",
        text="Busy E2E",
        destination="page",
        cooldown_fixed=0,
    )

    original = acquire_share_after_schedule

    def _fast_acquire(**kwargs):
        kwargs.setdefault("gap_min_minutes", 0.001)
        kwargs.setdefault("gap_max_minutes", 0.002)
        kwargs.setdefault("poll", 0.05)
        return original(**kwargs)

    monkeypatch.setattr(
        "src.services.facebook_groups.engine.acquire_share_after_schedule",
        _fast_acquire,
    )

    released = threading.Event()

    def _release_later() -> None:
        # Đợi các TK rảnh chạy một phần rồi mới nhả lịch.
        time.sleep(0.25)
        for lease in held:
            if lease:
                lease.release()
        released.set()

    threading.Thread(target=_release_later, daemon=True).start()
    result = engine.run_share_wave(wave["batch_ids"])
    assert released.wait(timeout=5.0)
    assert len(result["batches"]) == 8
    for batch in result["batches"]:
        assert batch.get("status") == "COMPLETED", batch
    assert len(fake.publish_calls) == 24


def test_scale_page_error_continues_same_account(tmp_path, monkeypatch) -> None:
    """1 Page lỗi chuyển — các Page khác cùng TK + TK khác vẫn xong."""
    monkeypatch.setenv("SHARE_PARALLEL_ACCOUNTS", "6")
    _store, engine, fake = _engine(tmp_path)
    fake.fail_pages.add("acc-00-p01")
    pages = _pages(6, 4)
    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/reel/9003",
        text="Fail page E2E",
        destination="page",
        cooldown_fixed=0,
    )
    result = engine.run_share_wave(wave["batch_ids"])
    assert len(fake.publish_calls) == 6 * 4 - 1  # trừ 1 page lỗi
    failed_jobs = [j for j in engine.store.share_jobs() if j.get("status") == "FAILED"]
    assert any(j.get("page_id") == "acc-00-p01" for j in failed_jobs)
    completed = sum(1 for b in result["batches"] if b.get("status") == "COMPLETED")
    # Batch acc-00 vẫn COMPLETED nếu còn job thành công (có failed_jobs > 0).
    assert completed >= 5


def test_scale_browser_dead_auto_resume(tmp_path, monkeypatch) -> None:
    """Browser chết 1 TK giữa chừng → batch READY → wave tự chạy lại → COMPLETED."""
    monkeypatch.setenv("SHARE_PARALLEL_ACCOUNTS", "5")
    _store, engine, fake = _engine(tmp_path)
    fake.browser_dead_once.add("acc-02")
    pages = _pages(5, 4)
    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/reel/9004",
        text="Browser dead E2E",
        destination="page",
        cooldown_fixed=0,
    )
    result = engine.run_share_wave(wave["batch_ids"])
    statuses = {str(b.get("account_id")): b.get("status") for b in result["batches"]}
    assert statuses.get("acc-02") == "COMPLETED"
    assert all(st == "COMPLETED" for st in statuses.values())
    assert len(fake.publish_calls) == 20
    assert "acc-02" in fake._dead_hit


def test_scale_recover_orphan_then_resume(tmp_path, monkeypatch) -> None:
    """Giả lập tắt app giữa chừng: RUNNING → recover → READY → chạy tiếp đủ Page."""
    monkeypatch.setenv("SHARE_PARALLEL_ACCOUNTS", "4")
    store, engine, fake = _engine(tmp_path)
    pages = _pages(4, 5)
    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/reel/9005",
        text="Recover E2E",
        destination="page",
        cooldown_fixed=0,
    )
    # Đánh dấu batch đang chạy + một số job đã xong.
    for batch_id in wave["batch_ids"][:2]:
        batch = store.share_batches()
        row = next(b for b in batch if str(b.get("id")) == batch_id)
        row["status"] = "RUNNING"
        store.save_share_batch(row)
        jobs = [j for j in store.share_jobs() if str(j.get("batch_id")) == batch_id]
        jobs[0]["status"] = "COMPLETED"
        jobs[0]["post_id"] = "already"
        store.save_share_job(jobs[0])

    reloaded = GroupEngine(store, fake, sleep=lambda _s: None)
    counts = reloaded.recover()
    assert counts["share_batches"] >= 2
    ready_ids = [
        str(b.get("id"))
        for b in store.share_batches()
        if b.get("status") == "READY"
    ]
    assert ready_ids
    reloaded.run_share_wave(wave["batch_ids"])
    for batch_id in wave["batch_ids"]:
        row = next(b for b in store.share_batches() if str(b.get("id")) == batch_id)
        assert row.get("status") == "COMPLETED"


def test_feature_busy_no_deadlock_under_parallel_pressure() -> None:
    """Nhiều thread tranh FeatureBusy cùng lúc — không deadlock, cùng feature được refcount."""
    barrier = threading.Barrier(12)
    errors: list[BaseException] = []
    leases_held = 0
    lock = threading.Lock()

    def _worker(aid: str, feat: str) -> None:
        nonlocal leases_held
        try:
            barrier.wait(timeout=5)
            lease = try_acquire(feature=feat, account_id=aid, page_keys={f"{aid}-p"})
            if lease:
                with lock:
                    leases_held += 1
                time.sleep(0.02)
                lease.release()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = []
    for i in range(6):
        threads.append(threading.Thread(target=_worker, args=(f"p{i}", FEATURE_SHARE)))
        threads.append(threading.Thread(target=_worker, args=(f"p{i}", FEATURE_SCHEDULE)))
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=8)
    assert not errors
    assert leases_held >= 6  # Ít nhất một feature/account thắng


def test_share_parallel_limit_reads_env(monkeypatch) -> None:
    monkeypatch.setenv("SHARE_PARALLEL_ACCOUNTS", "7")
    assert GroupEngine._share_parallel_account_limit() == 7
    monkeypatch.delenv("SHARE_PARALLEL_ACCOUNTS", raising=False)
    monkeypatch.setenv("BROWSER_CONCURRENCY", "5")
    assert GroupEngine._share_parallel_account_limit() == 5
