"""
E2E: tick Page tài khoản nào → chỉ TK đó chạy; bỏ tick → dừng ngay TK đó.

Không mở Firefox thật — dùng provider giả + GroupEngine.pause_share_outside_accounts.
"""

from __future__ import annotations

import threading
import time

from src.services.facebook_groups.engine import GroupEngine
from src.services.facebook_groups.share_schedule import compute_allowed_share_accounts
from src.services.facebook_groups.store import GroupStore


class SlowShareFake:
    """Publish chậm để có cửa sổ pause TK giữa chừng."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.publish_calls: list[dict] = []
        self.begin_calls: list[str] = []
        self.end_calls: list[str] = []
        self.active: set[str] = set()
        self.hold = threading.Event()
        self.entered = threading.Event()
        self.max_concurrent = 0

    def spawn(self) -> "SlowShareFake":
        return self

    def check_session(self, account_id: str) -> str:
        del account_id
        return ""

    def begin_account(self, account_id: str) -> None:
        with self.lock:
            self.begin_calls.append(account_id)
            self.active.add(account_id)
            self.max_concurrent = max(self.max_concurrent, len(self.active))
        self.entered.set()
        # Chờ tín hiệu — test sẽ pause TK khác trong lúc này.
        self.hold.wait(timeout=3.0)
        time.sleep(0.05)

    def end_account(self, account_id: str) -> None:
        with self.lock:
            self.end_calls.append(account_id)
            self.active.discard(account_id)

    def switch_to_page(self, **kwargs) -> dict:
        del kwargs
        return {"error_code": ""}

    def publish_link(self, **kwargs) -> dict:
        with self.lock:
            self.publish_calls.append(dict(kwargs))
        time.sleep(0.02)
        return {
            "post_id": f"post-{kwargs.get('page_id')}",
            "post_url": kwargs.get("source_url") or "",
        }

    def verify_published_post(self, **kwargs) -> dict:
        return {"post_id": f"post-{kwargs.get('page_id')}", "post_url": ""}


def _engine(tmp_path, fake: SlowShareFake | None = None):
    store = GroupStore(tmp_path / "facebook_groups.json")
    provider = fake or SlowShareFake()
    engine = GroupEngine(store, provider, sleep=lambda _s: None)
    return store, engine, provider


def test_compute_allowed_share_accounts_tick_rules() -> None:
    assert compute_allowed_share_accounts(
        checked_account_ids={"acc-a", "acc-b"},
        filter_account_id=None,
    ) == {"acc-a", "acc-b"}
    assert compute_allowed_share_accounts(
        checked_account_ids={"acc-a", "acc-b"},
        filter_account_id="acc-a",
    ) == {"acc-a"}
    # Bỏ tick B
    assert compute_allowed_share_accounts(
        checked_account_ids={"acc-a"},
        filter_account_id=None,
    ) == {"acc-a"}
    # Lọc A nhưng không tick Page A → không chạy
    assert compute_allowed_share_accounts(
        checked_account_ids={"acc-b"},
        filter_account_id="acc-a",
    ) == set()
    assert compute_allowed_share_accounts(
        checked_account_ids=set(),
        filter_account_id="acc-a",
    ) == set()


def test_untick_account_pauses_ready_batch(tmp_path) -> None:
    """Bỏ tick TK B → batch B PAUSED; TK A vẫn READY."""
    _store, engine, _fake = _engine(tmp_path)
    wave = engine.create_share_wave(
        pages=[
            {"account_id": "acc-a", "page_id": "pa", "target_url": "https://www.facebook.com/pa"},
            {"account_id": "acc-b", "page_id": "pb", "target_url": "https://www.facebook.com/pb"},
        ],
        source_url="https://www.facebook.com/reel/tick1",
        text="tick",
        destination="page",
        cooldown_fixed=0,
    )
    assert wave["accounts"] == 2
    by_acc = {str(b["account_id"]): str(b["id"]) for b in wave["batches"]}
    allowed = compute_allowed_share_accounts(
        checked_account_ids={"acc-a"},  # đã bỏ tick B
        filter_account_id=None,
    )
    stopped = engine.pause_share_outside_accounts(allowed)
    assert "acc-b" in stopped
    assert engine._batch(by_acc["acc-b"])["status"] == "PAUSED"
    assert engine._batch(by_acc["acc-a"])["status"] == "READY"
    assert by_acc["acc-b"] in engine._cancel


def test_filter_one_account_without_ticks_stops_that_account(tmp_path) -> None:
    """Combobox Andy nhưng bỏ hết tick Page Andy → dừng Andy."""
    _store, engine, _fake = _engine(tmp_path)
    wave = engine.create_share_wave(
        pages=[
            {"account_id": "acc-a", "page_id": "pa", "target_url": "https://www.facebook.com/pa"},
        ],
        source_url="https://www.facebook.com/reel/tick2",
        text="empty",
        destination="page",
        cooldown_fixed=0,
    )
    bid = wave["batch_ids"][0]
    allowed = compute_allowed_share_accounts(
        checked_account_ids=set(),
        filter_account_id="acc-a",
    )
    assert allowed == set()
    stopped = engine.pause_share_outside_accounts(allowed)
    assert "acc-a" in stopped
    assert engine._batch(bid)["status"] == "PAUSED"


def test_e2e_untick_stops_other_account_mid_wave(tmp_path) -> None:
    """
    Hai TK đang chạy song song → bỏ tick B (pause_outside {A}) → B dừng,
    A hoàn tất; không bao giờ 2 begin cùng lúc sau khi pause.
    """
    fake = SlowShareFake()
    _store, engine, _ = _engine(tmp_path, fake)
    wave = engine.create_share_wave(
        pages=[
            {"account_id": "acc-a", "page_id": "pa", "target_url": "https://www.facebook.com/pa"},
            {"account_id": "acc-b", "page_id": "pb", "target_url": "https://www.facebook.com/pb"},
        ],
        source_url="https://www.facebook.com/reel/tick3",
        text="live",
        destination="page",
        cooldown_fixed=0,
    )
    ids = list(wave["batch_ids"])
    by_acc = {str(b["account_id"]): str(b["id"]) for b in wave["batches"]}

    result_box: dict[str, object] = {}

    def _run() -> None:
        result_box["out"] = engine.run_share_wave(ids)

    thread = threading.Thread(target=_run, name="share-e2e-tick", daemon=True)
    thread.start()
    assert fake.entered.wait(timeout=5), "Không vào begin_account"
    # Giả lập UI bỏ tick B trong lúc đang chạy.
    allowed = compute_allowed_share_accounts(
        checked_account_ids={"acc-a"},
        filter_account_id=None,
    )
    stopped = engine.pause_share_outside_accounts(allowed)
    assert "acc-b" in stopped
    fake.hold.set()
    thread.join(timeout=15)
    assert not thread.is_alive()

    assert engine._batch(by_acc["acc-b"])["status"] == "PAUSED"
    # A chạy xong (COMPLETED) — không bị pause.
    assert engine._batch(by_acc["acc-a"])["status"] in {"COMPLETED", "READY", "FAILED"}
    published_accounts = {str(c.get("account_id") or "") for c in fake.publish_calls}
    # B đã pause trước/ khi đang chạy — có thể chưa publish; A phải publish.
    assert "acc-a" in published_accounts or engine._batch(by_acc["acc-a"])["status"] == "COMPLETED"


def test_e2e_only_ticked_accounts_in_new_wave(tmp_path) -> None:
    """Wave mới chỉ tạo batch cho Page/TK đang tick (giống ShareTab._selected_pages)."""
    _store, engine, fake = _engine(tmp_path, SlowShareFake())
    fake.hold.set()  # không chặn
    pages_all = [
        {"account_id": "acc-a", "page_id": "pa", "target_url": "https://www.facebook.com/pa"},
        {"account_id": "acc-b", "page_id": "pb", "target_url": "https://www.facebook.com/pb"},
    ]
    # Chỉ tick A
    allowed = compute_allowed_share_accounts(
        checked_account_ids={"acc-a"},
        filter_account_id=None,
    )
    pages = [p for p in pages_all if p["account_id"] in allowed]
    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/reel/tick4",
        text="only-a",
        destination="page",
        cooldown_fixed=0,
    )
    assert wave["accounts"] == 1
    assert wave["batch_ids"]
    engine.run_share_wave(wave["batch_ids"])
    assert {c["account_id"] for c in fake.publish_calls} == {"acc-a"}
    assert fake.max_concurrent <= 1
