"""Hàng đợi tạo Page: tuần tự, delay sau job, retry, pause, khôi phục. Không mở Facebook."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from src.services.page_creation.engine import PageCreationEngine, calculate_delay_seconds
from src.services.page_creation.facebook_provider import classify_facebook_surface
from src.services.page_creation.importer import parse_page_import
from src.services.page_creation.provider import PageCreationOutcome, PageCreationRequest
from src.services.page_creation.store import PageCreationStore
from src.services.page_creation.validate import avatar_is_image, validate_page_list


class FakeProvider:
    def __init__(self) -> None:
        self.created: list[str] = []
        self.verify_hit: list[str] = []
        self.admin_calls: list[tuple[str, tuple[str, ...]]] = []
        self.active = 0
        self.max_active = 0
        self._mu = threading.Lock()
        self.fail_names: set[str] = set()
        self.block_code = ""
        self.verify_ids: dict[str, str] = {}
        self.session_bad: set[str] = set()
        self.allow_create = True
        self.probe_code = ""
        self.block_accounts: set[str] | None = None
        self.create_modes: list[str] = []

    def check_session(self, account_id: str) -> PageCreationOutcome:
        if account_id in self.session_bad:
            return PageCreationOutcome(ok=False, error_code="SESSION_EXPIRED", error_message="hết phiên")
        return PageCreationOutcome(ok=True)

    def probe_page_creation_gate(self, account_id: str, business_id: str, create_mode: str = "bm") -> PageCreationOutcome:
        if account_id in self.session_bad:
            return PageCreationOutcome(ok=False, error_code="SESSION_EXPIRED", error_message="hết phiên")
        if self.block_accounts is not None and account_id not in self.block_accounts:
            return PageCreationOutcome(ok=True)
        code = self.probe_code or (self.block_code if not self.allow_create else "")
        if code:
            message = (
                "Facebook yêu cầu hoàn tất SMS verification trên mobile app."
                if code == "SMS_VERIFICATION_REQUIRED"
                else code
            )
            return PageCreationOutcome(ok=False, error_code=code, error_message=message)
        return PageCreationOutcome(ok=True)

    def verify_existing(self, request: PageCreationRequest) -> PageCreationOutcome:
        self.verify_hit.append(request.page_name)
        page_id = self.verify_ids.get(request.page_name, "")
        if not page_id:
            return PageCreationOutcome(ok=False)
        return PageCreationOutcome(ok=True, page_id=page_id, page_url=f"https://www.facebook.com/{page_id}")

    def create_and_verify(self, request: PageCreationRequest) -> PageCreationOutcome:
        if request.should_stop():
            return PageCreationOutcome(ok=False, error_code="CANCELLED", error_message="Đã hủy")
        blocked_for_account = self.block_accounts is None or request.account_id in self.block_accounts
        if not self.allow_create and blocked_for_account:
            code = self.block_code or "CAPTCHA_DETECTED"
            message = (
                "Facebook yêu cầu hoàn tất SMS verification trên mobile app."
                if code == "SMS_VERIFICATION_REQUIRED"
                else "chặn"
            )
            return PageCreationOutcome(ok=False, error_code=code, error_message=message)
        with self._mu:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if request.page_name in self.fail_names:
                return PageCreationOutcome(ok=False, error_code="TEMPORARY_ERROR", error_message="tạm")
            if self.block_code and blocked_for_account:
                return PageCreationOutcome(ok=False, error_code=self.block_code, error_message=self.block_code)
            self.created.append(request.page_name)
            self.create_modes.append(request.create_mode or "bm")
            if request.admin_targets:
                self.admin_calls.append((request.page_name, tuple(request.admin_targets)))
                request.on_state("ADDING_ADMIN")
            return PageCreationOutcome(
                ok=True,
                page_id=f"100{len(self.created):04d}",
                page_url=f"https://www.facebook.com/100{len(self.created):04d}",
            )
        finally:
            with self._mu:
                self.active -= 1


def _engine(tmp_path: Path, provider: FakeProvider, *, sleep=None, rng=None) -> tuple[PageCreationStore, PageCreationEngine]:
    store = PageCreationStore(tmp_path / "page_creation.json")

    def _sleep(seconds: float) -> None:
        if sleep is not None:
            sleep(seconds)

    return store, PageCreationEngine(store, provider, sleep=_sleep if sleep else (lambda _s: None), rng=rng)


def _batch(store: PageCreationStore, names: list[str], **settings):
    avatar = settings.pop("avatar", "a.png")
    cfg = {
        "delay_enabled": False,
        "delay_mode": "fixed",
        "delay_fixed_seconds": 30,
        "max_retry": 3,
        "continue_on_error": True,
    }
    cfg.update(settings)
    return store.create_batch(
        account_id=str(cfg.pop("account_id", "acc")),
        business_id=str(cfg.pop("business_id", "bm")),
        pages=[{"page_name": name, "avatar_path": avatar} for name in names],
        settings=cfg,
    )


def _png(path: Path) -> str:
    path.write_bytes(b"\x89PNG\r\n\x1a\n" + b"png")
    return str(path)


def test_five_pages_delay_after_each_completion(tmp_path: Path) -> None:
    provider = FakeProvider()
    sleeps: list[float] = []
    store, engine = _engine(tmp_path, provider, sleep=lambda s: sleeps.append(s) if s >= 1 else None)
    batch, _jobs = _batch(store, ["P1", "P2", "P3", "P4", "P5"], delay_enabled=True, delay_fixed_seconds=2)
    engine.run_batch(batch["id"])
    assert provider.created == ["P1", "P2", "P3", "P4", "P5"]
    assert sleeps == [1, 1] * 4
    done = store.get_batch(batch["id"])
    assert done is not None
    assert done["status"] == "COMPLETED"
    assert all(job["status"] == "COMPLETED" and job["page_id"] for job in store.jobs_for_batch(batch["id"]))
    second = store.jobs_for_batch(batch["id"])[1]
    text = " ".join(second.get("logs") or [])
    assert "Delay started" in text
    assert text.index("Delay started") < text.index("Creating Page")


def test_hundred_pages_stay_sequential(tmp_path: Path) -> None:
    provider = FakeProvider()
    store, engine = _engine(tmp_path, provider)
    names = [f"Page {i:03d}" for i in range(1, 101)]
    batch, _jobs = _batch(store, names)
    before = store.disk_writes
    started = time.perf_counter()
    engine.run_batch(batch["id"])
    elapsed = time.perf_counter() - started
    assert provider.created == names
    assert provider.max_active == 1
    assert store.get_batch(batch["id"])["status"] == "COMPLETED"
    assert store.disk_writes - before < 350
    assert elapsed < 5


def test_delay_starts_only_after_previous_page(tmp_path: Path) -> None:
    provider = FakeProvider()
    sleeps: list[float] = []

    def _sleep(seconds: float) -> None:
        if seconds >= 1:
            sleeps.append(seconds)

    store, engine = _engine(tmp_path, provider, sleep=_sleep)
    batch, _jobs = _batch(store, ["A", "B"], delay_enabled=True, delay_fixed_seconds=30)
    engine.run_batch(batch["id"])
    assert provider.created == ["A", "B"]
    assert sleeps == [1] * 30


def test_restart_during_delay_does_not_duplicate(tmp_path: Path) -> None:
    provider = FakeProvider()

    def _crash(seconds: float) -> None:
        if seconds >= 1:
            raise RuntimeError("crash")

    store, engine = _engine(tmp_path, provider, sleep=_crash)
    batch, _jobs = _batch(store, ["P1", "P2"], delay_enabled=True, delay_fixed_seconds=5)
    with pytest.raises(RuntimeError):
        engine.run_batch(batch["id"])
    assert provider.created == ["P1"]
    assert store.jobs_for_batch(batch["id"])[0]["status"] == "COMPLETED"

    def _fast(seconds: float) -> None:
        return None

    _store2, engine2 = _engine(tmp_path, provider, sleep=_fast)
    engine2.store = store
    resumed = engine2.recover_incomplete()
    assert batch["id"] not in resumed
    engine2.run_batch(batch["id"])
    assert provider.created == ["P1", "P2"]
    assert store.jobs_for_batch(batch["id"])[0]["page_id"]


def test_restart_while_creating_verifies_before_create(tmp_path: Path) -> None:
    provider = FakeProvider()
    provider.verify_ids["P1"] = "555"
    store, engine = _engine(tmp_path, provider)
    batch, jobs = _batch(store, ["P1"])
    jobs[0]["status"] = "CREATING"
    store.save_job(jobs[0])
    batch["status"] = "RUNNING"
    store.save_batch(batch)
    engine.recover_incomplete()
    saved = store.jobs_for_batch(batch["id"])[0]
    assert saved["status"] == "COMPLETED"
    assert saved["page_id"] == "555"
    assert provider.created == []


def test_page_with_id_is_kept_and_next_page_is_created(tmp_path: Path) -> None:
    """Page đã có UID thì không tạo lại, kể cả khi còn thiếu ảnh. Lượt sau là Page kế tiếp."""
    provider = FakeProvider()
    store, engine = _engine(tmp_path, provider)
    batch, jobs = _batch(store, ["Hoài Niệm Thành Phố Buổi Tối", "Page Kế Tiếp"])
    jobs[0]["page_id"] = "615800000000001"
    jobs[0]["page_url"] = "https://www.facebook.com/615800000000001"
    jobs[0]["status"] = "PENDING"
    store.save_job(jobs[0])
    engine.run_batch(batch["id"])
    saved = store.jobs_for_batch(batch["id"])
    assert provider.created == ["Page Kế Tiếp"]
    assert saved[0]["status"] == "COMPLETED"
    assert saved[0]["page_id"] == "615800000000001"
    assert saved[0]["page_url"] == "https://www.facebook.com/615800000000001"
    assert saved[1]["status"] == "COMPLETED"
    assert saved[1]["page_id"]


def test_created_id_completes_job_even_if_details_missing(tmp_path: Path) -> None:
    provider = FakeProvider()
    original = provider.create_and_verify

    def _create(request: PageCreationRequest) -> PageCreationOutcome:
        if request.page_name == "Đã tạo":
            provider.created.append(request.page_name)
            return PageCreationOutcome(
                ok=False,
                page_id="615800000000002",
                page_url="https://www.facebook.com/615800000000002",
                error_code="TEMPORARY_ERROR",
                error_message="thiếu ảnh",
            )
        return original(request)

    provider.create_and_verify = _create  # type: ignore[method-assign]
    store, engine = _engine(tmp_path, provider)
    batch, _jobs = _batch(store, ["Đã tạo", "Trang sau"])
    engine.run_batch(batch["id"])
    saved = store.jobs_for_batch(batch["id"])
    assert provider.created == ["Đã tạo", "Trang sau"]
    assert saved[0]["page_id"] == "615800000000002"
    assert saved[0]["page_url"] == "https://www.facebook.com/615800000000002"
    assert saved[0]["status"] == "COMPLETED"
    assert saved[1]["status"] == "COMPLETED"


def test_creating_without_existing_page_returns_to_queue(tmp_path: Path) -> None:
    provider = FakeProvider()
    store, engine = _engine(tmp_path, provider)
    batch, jobs = _batch(store, ["P1"])
    jobs[0]["status"] = "CREATING"
    store.save_job(jobs[0])
    batch["status"] = "RUNNING"
    store.save_batch(batch)
    engine.recover_incomplete()
    assert store.jobs_for_batch(batch["id"])[0]["status"] == "PENDING"
    assert provider.created == []


def test_captcha_pauses_batch_until_resume(tmp_path: Path) -> None:
    provider = FakeProvider()
    provider.allow_create = False
    provider.block_code = "CAPTCHA_DETECTED"
    store, engine = _engine(tmp_path, provider)
    batch, _jobs = _batch(store, ["P1", "P2"])
    worker = threading.Thread(target=engine.run_batch, args=(batch["id"],), daemon=True)
    worker.start()
    try:
        paused = False
        for _ in range(200):
            current = store.get_batch(batch["id"])
            if current and current["status"] in {"PAUSED", "PAUSED_FOR_USER"}:
                paused = True
                break
            time.sleep(0.01)
        assert paused
        assert provider.created == []
        jobs = store.jobs_for_batch(batch["id"])
        assert jobs[0]["status"] == "CAPTCHA_DETECTED"
        assert jobs[1]["status"] == "BLOCKED_BY_ACCOUNT_VERIFICATION"
        assert store.get_batch(batch["id"])["account_creation_lock"] == "SECURITY_VERIFICATION_REQUIRED"
        provider.allow_create = True
        provider.block_code = ""
        assert engine.check_again(batch["id"]).ok
        for _ in range(200):
            if store.get_batch(batch["id"])["status"] == "COMPLETED":
                break
            time.sleep(0.02)
        if store.get_batch(batch["id"])["status"] != "COMPLETED" and not engine.is_running(batch["id"]):
            engine.run_batch(batch["id"])
        assert provider.created == ["P1", "P2"]
        assert store.get_batch(batch["id"])["status"] == "COMPLETED"
    finally:
        engine.cancel(batch["id"])
        worker.join(1)


def test_sms_verification_pauses_account_without_retry(tmp_path: Path) -> None:
    provider = FakeProvider()
    provider.allow_create = False
    provider.block_code = "SMS_VERIFICATION_REQUIRED"
    provider.block_accounts = {"A"}
    store, engine = _engine(tmp_path, provider)
    batch_a, _ = _batch(store, ["A1", "A2", "A3"], account_id="A")
    batch_b, _ = _batch(store, ["B1"], account_id="B")
    worker_a = threading.Thread(target=engine.run_batch, args=(batch_a["id"],), daemon=True)
    worker_b = threading.Thread(target=engine.run_batch, args=(batch_b["id"],), daemon=True)
    worker_a.start()
    worker_b.start()
    try:
        worker_b.join(3)
        paused = False
        for _ in range(200):
            current = store.get_batch(batch_a["id"])
            if current and current["status"] == "PAUSED_FOR_USER":
                paused = True
                break
            time.sleep(0.01)
        assert paused
        assert provider.created == ["B1"]
        jobs_a = store.jobs_for_batch(batch_a["id"])
        assert jobs_a[0]["status"] == "SMS_VERIFICATION_REQUIRED"
        assert "SMS verification" in jobs_a[0]["error_message"]
        assert jobs_a[0]["retry_count"] == 0
        assert jobs_a[1]["status"] == "BLOCKED_BY_ACCOUNT_VERIFICATION"
        assert jobs_a[2]["status"] == "BLOCKED_BY_ACCOUNT_VERIFICATION"
        assert store.get_batch(batch_a["id"])["account_creation_lock"] == "SECURITY_VERIFICATION_REQUIRED"
        assert store.jobs_for_batch(batch_b["id"])[0]["status"] == "COMPLETED"
        engine.resume(batch_a["id"])
        time.sleep(0.05)
        assert store.jobs_for_batch(batch_a["id"])[0]["status"] == "SMS_VERIFICATION_REQUIRED"
        assert provider.created == ["B1"]
        still = engine.check_again(batch_a["id"])
        assert not still.ok
        assert still.error_code == "SMS_VERIFICATION_REQUIRED"
        provider.allow_create = True
        provider.block_code = ""
        assert engine.check_again(batch_a["id"]).ok
        for _ in range(200):
            if store.get_batch(batch_a["id"])["status"] == "COMPLETED":
                break
            time.sleep(0.02)
        if store.get_batch(batch_a["id"])["status"] != "COMPLETED" and not engine.is_running(batch_a["id"]):
            engine.run_batch(batch_a["id"])
        assert provider.created == ["B1", "A1", "A2", "A3"]
        assert all(job["status"] == "COMPLETED" for job in store.jobs_for_batch(batch_a["id"]))
        assert store.get_batch(batch_a["id"])["account_creation_lock"] == "READY"
    finally:
        engine.cancel(batch_a["id"])
        engine.cancel(batch_b["id"])
        worker_a.join(1)
        worker_b.join(1)


def test_session_expired_pauses_only_that_account(tmp_path: Path) -> None:
    provider = FakeProvider()
    provider.session_bad.add("A")
    store, engine = _engine(tmp_path, provider)
    batch_a, _ = _batch(store, ["A1"], account_id="A")
    batch_b, _ = _batch(store, ["B1"], account_id="B")
    worker_a = threading.Thread(target=engine.run_batch, args=(batch_a["id"],))
    worker_b = threading.Thread(target=engine.run_batch, args=(batch_b["id"],))
    worker_a.start()
    worker_b.start()
    worker_b.join(3)
    assert store.jobs_for_batch(batch_b["id"])[0]["status"] == "COMPLETED"
    assert provider.created == ["B1"]
    job_a = store.jobs_for_batch(batch_a["id"])[0]
    assert job_a["status"] == "WAITING_REAUTH"
    assert store.get_batch(batch_a["id"])["status"] in {"PAUSED", "PAUSED_FOR_USER"}
    engine.cancel(batch_a["id"])
    worker_a.join(3)
    assert not worker_a.is_alive()


def test_temporary_error_retries_then_continues(tmp_path: Path) -> None:
    provider = FakeProvider()
    provider.fail_names.add("Bad")
    store, engine = _engine(tmp_path, provider)
    batch, _jobs = _batch(store, ["Bad", "Good"], max_retry=3, continue_on_error=True)
    engine.run_batch(batch["id"])
    jobs = store.jobs_for_batch(batch["id"])
    assert jobs[0]["status"] == "FAILED"
    assert jobs[0]["retry_count"] == 3
    assert jobs[1]["status"] == "COMPLETED"
    assert provider.created == ["Good"]


def test_timeout_verifies_existing_page_instead_of_second_create(tmp_path: Path) -> None:
    provider = FakeProvider()
    provider.block_code = "NAVIGATION_TIMEOUT"
    provider.verify_ids["P1"] = "777"
    store, engine = _engine(tmp_path, provider)
    batch, _jobs = _batch(store, ["P1"])
    engine.run_batch(batch["id"])
    job = store.jobs_for_batch(batch["id"])[0]
    assert job["status"] == "COMPLETED"
    assert job["page_id"] == "777"
    assert provider.created == []
    assert provider.verify_hit == ["P1"]


def test_completed_job_is_not_created_again(tmp_path: Path) -> None:
    provider = FakeProvider()
    store, engine = _engine(tmp_path, provider)
    batch, jobs = _batch(store, ["P1", "P2"])
    jobs[0]["status"] = "COMPLETED"
    jobs[0]["page_id"] = "111"
    jobs[0]["page_url"] = "https://www.facebook.com/111"
    store.save_job(jobs[0])
    engine.run_batch(batch["id"])
    assert provider.created == ["P2"]


def test_pause_freezes_delay_countdown(tmp_path: Path) -> None:
    provider = FakeProvider()
    seen: dict[str, int] = {}
    store_holder: dict[str, PageCreationStore] = {}
    engine_holder: dict[str, PageCreationEngine] = {}

    def _sleep(seconds: float) -> None:
        engine = engine_holder["engine"]
        batch_id = seen["batch"]
        if seconds >= 1 and "paused" not in seen:
            engine.pause(batch_id)
            seen["paused"] = 1
            return
        if seconds < 1 and "paused" in seen and "resumed" not in seen:
            remaining = int(store_holder["store"].get_batch(batch_id)["delay_remaining_seconds"])
            seen["frozen"] = remaining
            engine.resume(batch_id)
            seen["resumed"] = 1

    store, engine = _engine(tmp_path, provider, sleep=_sleep)
    store_holder["store"] = store
    engine_holder["engine"] = engine
    batch, _jobs = _batch(store, ["P1", "P2"], delay_enabled=True, delay_fixed_seconds=5)
    seen["batch"] = batch["id"]
    engine.run_batch(batch["id"])
    assert seen["frozen"] == 5
    assert provider.created == ["P1", "P2"]


def test_cancel_keeps_completed_and_drops_pending(tmp_path: Path) -> None:
    provider = FakeProvider()
    store, engine = _engine(tmp_path, provider)
    batch, _jobs = _batch(store, ["P1", "P2", "P3"])

    def _create(request: PageCreationRequest) -> PageCreationOutcome:
        if request.page_name == "P1":
            provider.created.append("P1")
            engine.cancel(batch["id"])
            return PageCreationOutcome(ok=True, page_id="101", page_url="https://www.facebook.com/101")
        return PageCreationOutcome(ok=False, error_code="CANCELLED", error_message="Đã hủy")

    provider.create_and_verify = _create  # type: ignore[method-assign]
    engine.run_batch(batch["id"])
    jobs = store.jobs_for_batch(batch["id"])
    assert jobs[0]["status"] == "COMPLETED"
    assert jobs[1]["status"] == "CANCELLED"
    assert jobs[2]["status"] == "CANCELLED"
    assert store.get_batch(batch["id"])["status"] == "CANCELLED"


def test_same_account_lock_allows_one_active_create(tmp_path: Path) -> None:
    provider = FakeProvider()
    store, engine = _engine(tmp_path, provider, sleep=lambda _s: time.sleep(0.01))

    def _create(request: PageCreationRequest) -> PageCreationOutcome:
        with provider._mu:
            provider.active += 1
            provider.max_active = max(provider.max_active, provider.active)
        time.sleep(0.05)
        with provider._mu:
            provider.active -= 1
        provider.created.append(request.page_name)
        return PageCreationOutcome(ok=True, page_id="1", page_url="https://www.facebook.com/1")

    provider.create_and_verify = _create  # type: ignore[method-assign]
    first, _ = _batch(store, ["A1", "A2"])
    second, _ = _batch(store, ["B1", "B2"])
    threads = [
        threading.Thread(target=engine.run_batch, args=(first["id"],)),
        threading.Thread(target=engine.run_batch, args=(second["id"],)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)
    assert provider.max_active == 1
    assert sorted(provider.created) == ["A1", "A2", "B1", "B2"]


def test_range_delay_uses_bounds() -> None:
    import random

    rng = random.Random(1)
    batch = {"delay_enabled": True, "delay_mode": "range", "delay_min_seconds": 30, "delay_max_seconds": 60}
    for _ in range(20):
        value = calculate_delay_seconds(batch, rng)
        assert 30 <= value <= 60


def test_validate_all_rows_before_start(tmp_path: Path) -> None:
    good = _png(tmp_path / "ok.png")
    missing = str(tmp_path / "nope.png")
    (tmp_path / "note.txt").write_text("x", encoding="utf-8")
    errors = validate_page_list(
        account_id="acc",
        business_id="bm",
        pages=[
            {"page_name": "A", "avatar_path": good},
            {"page_name": "", "avatar_path": good},
            {"page_name": "A", "avatar_path": good},
            {"page_name": "C", "avatar_path": missing},
            {"page_name": "D", "avatar_path": str(tmp_path / "note.txt")},
        ],
        known_account_ids={"acc"},
        known_business_ids={"bm"},
        busy_account_ids=set(),
    )
    assert any("trống" in item for item in errors)
    assert any("trùng" in item for item in errors)
    assert any("không tồn tại" in item for item in errors)
    assert any("không phải file ảnh" in item for item in errors)
    assert avatar_is_image(good)
    nameless = validate_page_list(
        account_id="acc",
        business_id="bm",
        pages=[{"page_name": "Chỉ tên", "avatar_path": ""}],
        known_account_ids={"acc"},
        known_business_ids={"bm"},
        busy_account_ids=set(),
    )
    assert nameless == []


def test_import_csv_and_json() -> None:
    rows = parse_page_import("page_name,avatar_path\nBrazil Funny,avatars/01.jpg\n", kind="csv")
    assert rows == [{"page_name": "Brazil Funny", "avatar_path": "avatars/01.jpg"}]
    imported = parse_page_import('[{"page_name":"A","avatar_path":"a.png"}]', kind="json")
    assert imported[0]["page_name"] == "A"


def test_end_to_end_batch_flow(tmp_path: Path) -> None:
    """Validate → import → tạo → delay/pause → CAPTCHA → resume → không tạo trùng sau restart."""
    avatar = _png(tmp_path / "a.png")
    imported = parse_page_import(
        f"page_name,avatar_path\nBrazil Funny,{avatar}\nBrazil Movies,{avatar}\nFunny Brazil,{avatar}\n",
        kind="csv",
    )
    errors = validate_page_list(
        account_id="acc",
        business_id="bm",
        pages=imported,
        known_account_ids={"acc"},
        known_business_ids={"bm"},
        busy_account_ids=set(),
    )
    assert errors == []

    provider = FakeProvider()
    store_holder: dict[str, PageCreationStore] = {}
    engine_holder: dict[str, PageCreationEngine] = {}
    state = {"phase": "run"}

    def _sleep(seconds: float) -> None:
        if seconds < 1 or state["phase"] != "delay":
            return
        engine_holder["engine"].pause(state["batch"])
        state["phase"] = "paused"
        frozen = int(store_holder["store"].get_batch(state["batch"])["delay_remaining_seconds"])
        state["frozen"] = frozen
        engine_holder["engine"].resume(state["batch"])
        state["phase"] = "after"

    store, engine = _engine(tmp_path, provider, sleep=_sleep)
    store_holder["store"] = store
    engine_holder["engine"] = engine
    batch, _jobs = _batch(store, [row["page_name"] for row in imported], avatar=avatar, delay_enabled=True, delay_fixed_seconds=4)
    state["batch"] = batch["id"]
    state["phase"] = "delay"
    engine.run_batch(batch["id"])
    assert state["frozen"] == 4
    assert [job["status"] for job in store.jobs_for_batch(batch["id"])] == ["COMPLETED", "COMPLETED", "COMPLETED"]
    first_ids = [job["page_id"] for job in store.jobs_for_batch(batch["id"])]

    provider.allow_create = False
    provider.block_code = "CAPTCHA_DETECTED"
    blocked, _ = _batch(store, ["Blocked", "Later"], avatar=avatar, delay_enabled=False)
    worker = threading.Thread(target=engine.run_batch, args=(blocked["id"],))
    worker.start()
    for _ in range(200):
        current = store.get_batch(blocked["id"])
        if current and current["status"] in {"PAUSED", "PAUSED_FOR_USER"}:
            break
        time.sleep(0.01)
    assert store.jobs_for_batch(blocked["id"])[0]["status"] == "CAPTCHA_DETECTED"
    assert store.jobs_for_batch(blocked["id"])[1]["status"] == "BLOCKED_BY_ACCOUNT_VERIFICATION"
    provider.allow_create = True
    provider.block_code = ""
    assert engine.check_again(blocked["id"]).ok
    worker.join(3)
    if store.get_batch(blocked["id"])["status"] != "COMPLETED":
        engine.run_batch(blocked["id"])
    assert [job["status"] for job in store.jobs_for_batch(blocked["id"])] == ["COMPLETED", "COMPLETED"]

    crashed = store.jobs_for_batch(batch["id"])[0]
    assert crashed["page_id"] == first_ids[0]
    provider.created = [name for name in provider.created if name not in {"Brazil Funny", "Brazil Movies", "Funny Brazil"}]
    _store2, engine2 = _engine(tmp_path, provider)
    engine2.store = store
    engine2.recover_incomplete()
    assert store.jobs_for_batch(batch["id"])[0]["page_id"] == first_ids[0]
    assert "Brazil Funny" not in provider.created


def test_large_batch_stays_unique_fast_and_sequential(tmp_path: Path) -> None:
    """Vài trăm Page: một luồng, tên không trùng, tiến độ nhẹ, xong trong vài giây."""
    provider = FakeProvider()
    store, engine = _engine(tmp_path, provider)
    names = [f"Page {i:04d}" for i in range(1, 401)]
    batch, jobs = _batch(store, names)
    assert len(jobs) == 400
    assert len({job["page_name"] for job in jobs}) == 400
    before = store.disk_writes
    started = time.perf_counter()
    engine.run_batch(batch["id"])
    elapsed = time.perf_counter() - started
    snap = engine.progress(batch["id"])
    assert provider.created == names
    assert provider.max_active == 1
    assert snap["status"] == "COMPLETED"
    assert snap["completed"] == 400
    assert len(snap["rows"]) == 400
    assert "jobs" not in snap
    assert all(row[3] == "COMPLETED" for row in snap["rows"])
    assert store.disk_writes - before < 1400
    assert elapsed < 12


def test_validate_many_rows_reuses_image_cache(tmp_path: Path) -> None:
    first = _png(tmp_path / "a.png")
    second = _png(tmp_path / "b.png")
    pages = [
        {"page_name": f"Page {index:04d}", "avatar_path": first if index % 2 == 0 else second}
        for index in range(800)
    ]
    kwargs = dict(
        account_id="acc",
        business_id="bm",
        pages=pages,
        known_account_ids={"acc"},
        known_business_ids={"bm"},
        busy_account_ids=set(),
    )
    started = time.perf_counter()
    assert validate_page_list(**kwargs) == []
    again = time.perf_counter()
    assert validate_page_list(**kwargs) == []
    elapsed = time.perf_counter() - again
    assert time.perf_counter() - started < 2
    assert elapsed < 0.5


def test_changed_job_rows_skips_unchanged_status() -> None:
    from src.gui.page_creator_dialog import changed_job_rows

    known = {"a": ("PENDING", 0, ""), "b": ("COMPLETED", 0, "9")}
    rows = [
        ("a", 1, "A", "PENDING", 0, 3, "", ""),
        ("b", 2, "B", "COMPLETED", 0, 3, "9", "https://www.facebook.com/9"),
        ("c", 3, "C", "CREATING", 1, 3, "", ""),
    ]
    changed = changed_job_rows(known, rows)
    assert [row[0] for row in changed] == ["c"]
    known["c"] = ("CREATING", 1, "")
    rows[2] = ("c", 3, "C", "VERIFYING", 1, 3)
    assert [row[0] for row in changed_job_rows(known, rows)] == ["c"]


def test_batch_reuses_one_browser_session(tmp_path: Path) -> None:
    class SessionProvider(FakeProvider):
        def __init__(self) -> None:
            super().__init__()
            self.begins = 0
            self.ends = 0

        def begin_account(self, account_id: str) -> None:
            self.begins += 1

        def end_account(self, account_id: str) -> None:
            self.ends += 1

    provider = SessionProvider()
    store, engine = _engine(tmp_path, provider)
    names = ["Một", "Hai", "Ba"]
    batch, _jobs = _batch(store, names)
    engine.run_batch(batch["id"])
    assert provider.created == names
    assert provider.begins == 1
    assert provider.ends == 1


def test_create_page_labels_and_id_match_the_named_page() -> None:
    from src.services.page_creation.facebook_provider import (
        category_option_matches,
        category_query_for_form,
        confirm_button_rank,
        is_confirm_page_label,
        is_add_menu_label,
        is_create_page_label,
        is_page_switch_label,
        is_switch_now_label,
        page_switch_prompt_text,
        is_page_name_field,
        is_terms_checkbox_label,
        name_was_entered,
        page_match_from_markup,
    )

    assert is_create_page_label("Tạo Trang mới")
    assert is_create_page_label("Tạo Trang Facebook mới")
    assert is_create_page_label("Create a new Page")
    assert not is_create_page_label("Create a Page")
    assert not is_create_page_label("Thêm trang")
    assert not is_create_page_label("Thêm Trang Facebook có sẵn")
    assert not is_create_page_label("Yêu cầu quyền truy cập chung vào Trang Facebook")
    assert not is_create_page_label("Mời bạn bè")
    assert not is_create_page_label("Bắt đầu trên Meta Business Suite")
    assert not is_create_page_label("Thêm")
    assert is_add_menu_label("+ Add")
    assert is_add_menu_label("Add")
    assert is_add_menu_label("Thêm")
    assert not is_add_menu_label("Assign people")
    assert not is_add_menu_label("Create a new Page")
    prompt = "Switch into Mạnh Mẽ Kênh Clip Hay's Page to start managing it."
    assert page_switch_prompt_text(prompt)
    assert page_switch_prompt_text("Switch into Cedar Pets World's Page to take more actions")
    assert not page_switch_prompt_text("Manage Page")
    assert is_switch_now_label("Switch Now")
    assert not is_switch_now_label("Switch")
    assert is_page_switch_label("Switch")
    assert not is_page_switch_label("Switch Now")
    assert not is_page_switch_label("Switch account")
    assert is_confirm_page_label("Tạo Trang")
    assert is_confirm_page_label("Create Page")
    assert is_confirm_page_label("Next")
    assert is_confirm_page_label("Tiếp theo")
    assert is_confirm_page_label("Xác nhận")
    assert not is_confirm_page_label("Hủy")
    assert not is_confirm_page_label("Quay lại")
    assert not is_confirm_page_label("Tạo Trang mới")
    assert not is_confirm_page_label("Tạo Trang Facebook mới")
    assert confirm_button_rank("Tạo Trang") > confirm_button_rank("Xác nhận")
    assert confirm_button_rank("Quay lại") == 0
    agreement = (
        "Thay mặt trang quản lý tài sản doanh nghiệp Qly Team CVC, "
        "tôi đồng ý với Điều khoản thương mại của Meta"
    )
    assert is_terms_checkbox_label(agreement)
    assert not is_terms_checkbox_label("Quyền truy cập Trang")
    assert not is_terms_checkbox_label("Quay lại")
    assert name_was_entered("Đêm Thể Thao Xem Ngay", "Đêm Thể Thao Xem Ngay")
    assert not name_was_entered("", "Đêm Thể Thao Xem Ngay")
    assert category_query_for_form(False, preferred="Cộng đồng") == "Cộng đồng"
    assert category_query_for_form(True, preferred="Brand") == "Brand"
    assert category_query_for_form(True, preferred="Trang web giải trí") == "Entertainment website"
    assert category_query_for_form(False, preferred="Product/service") == "Sản phẩm/Dịch vụ"
    from src.services.page_creation.page_details import category_label_for_form, pick_page_category

    assert category_label_for_form("Trang web giải trí", english=True) == "Entertainment website"
    assert pick_page_category("Alpha", "en")
    cats = {pick_page_category(f"Page {i}", "en") for i in range(40)}
    assert len(cats) >= 3
    assert category_option_matches("Sản phẩm/Dịch vụ")
    assert category_option_matches("Product/service")
    assert category_option_matches("Community", "Community")
    assert category_option_matches("Cộng đồng", "Cộng đồng")
    assert category_option_matches("Entertainment website", "Entertainment website")
    assert category_option_matches("Product/service Product/service", "Product/service")
    assert category_option_matches("Sản phẩm/Dịch vụ", "Product/service")
    assert not category_option_matches("Website", "Entertainment website")
    assert not category_option_matches("Entertainment", "Entertainment website")
    assert not category_option_matches("Local service", "Product/service")
    assert not category_option_matches("Blog", "Product/service")
    from src.services.page_creation.facebook_provider import is_bio_field_label

    assert is_bio_field_label("Tell people what your Page is about")
    assert is_bio_field_label("Bio")
    assert is_bio_field_label("Description")
    assert not is_bio_field_label("Page name")
    assert not is_bio_field_label("Category")
    from src.services.page_creation.facebook_provider import (
        _category_value_is_pending_query,
        _normalize_category_choice,
    )

    assert _normalize_category_choice("Interest", english=True) == "Product/service"
    assert _normalize_category_choice("Sở thích", english=False) == "Sản phẩm/Dịch vụ"
    assert _normalize_category_choice("Brand", english=True) == "Brand"
    assert category_query_for_form(True, preferred="Interest") == "Product/service"
    assert _category_value_is_pending_query("Interest", "Interest")
    assert _category_value_is_pending_query("Brand", "Brand")
    assert _category_value_is_pending_query("Interest")
    assert _category_value_is_pending_query("Product/servi", "Product/service")
    from src.services.page_creation.facebook_provider import category_label_is_selected, should_type_another_category

    assert category_label_is_selected(
        "Entertainment website Entertainment website",
        "Entertainment website",
        "Entertainment website",
    )
    assert not category_label_is_selected(
        "Entertainment website",
        "Entertainment website",
        "Entertainment website",
    )
    assert category_label_is_selected("Entertainment website", "", "Entertainment website")
    assert should_type_another_category(["Entertainment website"], "Product/servi") is False
    assert should_type_another_category(["Entertainment website", "Product/service"], "") is False
    assert should_type_another_category([], "") is True
    assert not category_option_matches("Sản phẩm")
    assert not category_option_matches("Local service")
    assert not category_option_matches("Shopping & retail")
    assert not category_option_matches("Legal")
    assert not category_option_matches("Restaurant")
    assert not category_option_matches("Thêm Trang Facebook có sẵn")
    from src.services.page_creation.facebook_provider import (
        name_feedback_is_invalid,
        parse_suggested_page_name,
    )

    feedback = "The name 'Solace News Official' is invalid. We have suggested 'Solace News'. Learn more."
    assert name_feedback_is_invalid(feedback)
    assert parse_suggested_page_name(feedback) == "Solace News"
    from src.services.page_creation.facebook_provider import (
        is_avatar_upload_label,
        is_save_photo_label,
        is_skip_setup_label,
    )

    assert is_avatar_upload_label("Add profile picture")
    assert is_avatar_upload_label("Thêm ảnh đại diện")
    assert not is_avatar_upload_label("Skip")
    assert is_skip_setup_label("Bỏ qua")
    assert is_skip_setup_label("Not now")
    assert is_save_photo_label("Save")
    assert is_save_photo_label("Lưu")
    assert not is_save_photo_label("Skip")
    from src.services.page_creation.facebook_provider import (
        is_finish_setting_done_label,
        is_skip_setup_label as _skip2,
    )

    assert is_finish_setting_done_label("Done")
    assert is_finish_setting_done_label("Hoàn tất")
    assert not is_finish_setting_done_label("Skip")
    assert _skip2("Not now")
    from src.services.page_creation.facebook_provider import ensure_page_complete
    from src.services.page_creation.page_details import fill_missing_details

    assert callable(ensure_page_complete)
    filled_cat = fill_missing_details("Alpha Hub", {}, language="en", rng=__import__("random").Random(7))
    assert filled_cat["category"] in {
        "Product/service",
        "Community",
        "Entertainment website",
        "Media/news company",
        "Digital creator",
        "Brand",
        "Arts & entertainment",
        "Interest",
        "Blog",
        "Just for fun",
        "Education website",
        "Topic",
    }
    assert not is_confirm_page_label("Mời bạn bè")
    assert is_page_name_field("Tên Trang")
    assert not is_page_name_field("Tìm kiếm")
    markup = """
    <a href="?business_id=11111111">BM</a>
    <div data-surface="lib:business_scope:page:22222222:Page%20Khac"></div>
    <div data-surface="lib:business_scope:page:33333333:Grand%20Animals%20Plus"></div>
    """
    assert page_match_from_markup(markup, "Grand Animals Plus") == (
        "33333333",
        "https://www.facebook.com/33333333",
    )
    assert page_match_from_markup(markup, "Page Khac")[0] == "22222222"
    assert page_match_from_markup(markup, "Không có") == ("", "")
    confirm_markup = """
    <div role="dialog">Are you sure you want to continue?
    The Page will be created and added to the CB Coffee business portfolio.
    I agree to Meta Commercial Terms
    <a href="?asset_id=61582438296851">Bất Ngờ Thành Phố Tin Mới</a>
    </div>
    """
    assert page_match_from_markup(confirm_markup, "Bất Ngờ Thành Phố Tin Mới") == ("", "")
    listed = confirm_markup + '<div data-surface="lib:business_scope:page:615811111111111:Ho%C3%A0i%20Ni%E1%BB%87m"></div>'
    assert page_match_from_markup(listed, "Hoài Niệm")[0] == "615811111111111"
    from src.services.page_creation.facebook_provider import created_page_id_from_url

    assert created_page_id_from_url(
        "https://business.facebook.com/latest/settings/pages?business_id=61582438296851"
    ) == ""
    assert (
        created_page_id_from_url(
            "https://business.facebook.com/latest/settings/pages?business_id=111111111111&selected_asset_id=615800000000009"
        )
        == "615800000000009"
    )
    listed_row = '<a href="?business_id=111111111111&asset_id=615800000000010">Cedar Pets World</a>'
    assert page_match_from_markup(listed_row, "Cedar Pets World")[0] == "615800000000010"
    search_only = (
        '<input placeholder="Search by name or ID" value="Mạnh Mẽ Kênh Clip Hay" />'
        '<a href="?asset_id=1341590235706574">Cedar Pets World</a>'
    )
    assert page_match_from_markup(search_only, "Mạnh Mẽ Kênh Clip Hay") == ("", "")
    assert page_match_from_markup(search_only, "Cedar Pets World")[0] == "1341590235706574"
    assert created_page_id_from_url("https://www.facebook.com/profile.php?id=100064000000001") == (
        "100064000000001"
    )
    agreement_en = (
        "I agree to Meta Commercial Terms and Pages, groups and events policies "
        "on behalf of CB Coffee business portfolio."
    )
    assert is_terms_checkbox_label(agreement_en)
    from src.services.page_creation.validate import open_batch_for_account

    paused = {"id": "b1", "account_id": "acc", "status": "PAUSED", "created_at": "1"}
    done = {"id": "b0", "account_id": "acc", "status": "COMPLETED", "created_at": "0"}
    assert open_batch_for_account([done, paused], "acc")["id"] == "b1"
    assert open_batch_for_account([done], "acc") is None


def test_classify_security_surfaces_do_not_look_like_success() -> None:
    assert classify_facebook_surface("https://www.facebook.com/checkpoint/", "security check") == "CHECKPOINT"
    assert classify_facebook_surface("https://www.facebook.com/", "I'm not a robot") == "CAPTCHA_DETECTED"
    assert classify_facebook_surface("https://www.facebook.com/login.php", "Log in") == "SESSION_EXPIRED"
    assert classify_facebook_surface("https://business.facebook.com/", "too many attempts") == "RATE_LIMITED"
    assert classify_facebook_surface("https://business.facebook.com/latest/settings/pages", "Pages") == ""
    sms_body = (
        "We noticed suspicious activity on your account. "
        "Finish SMS verification on mobile app before creating a new page."
    )
    assert (
        classify_facebook_surface("https://business.facebook.com/latest/settings/pages", sms_body)
        == "SMS_VERIFICATION_REQUIRED"
    )


def test_profile_mode_skips_business_manager_and_keeps_admins(tmp_path: Path) -> None:
    from src.services.page_creation.page_admin import (
        admin_query_from_target,
        is_add_people_label,
        is_admin_role_label,
        parse_admin_targets,
    )

    assert parse_admin_targets("10001\nname.two, https://www.facebook.com/10002") == [
        "10001",
        "name.two",
        "https://www.facebook.com/10002",
    ]
    assert admin_query_from_target("https://www.facebook.com/profile.php?id=123456789") == "123456789"
    assert admin_query_from_target("https://www.facebook.com/cool.page") == "cool.page"
    assert is_add_people_label("Add People")
    assert is_add_people_label("Thêm người")
    assert is_admin_role_label("Admin")
    assert is_admin_role_label("Quản trị viên")
    assert not is_admin_role_label("Editor")

    pages = [{"page_name": "Harbor Moments", "avatar_path": ""}]
    missing_bm = validate_page_list(
        account_id="acc",
        business_id="",
        pages=pages,
        known_account_ids={"acc"},
        known_business_ids=set(),
        busy_account_ids=set(),
        create_mode="bm",
    )
    assert "Business Manager không tồn tại." in missing_bm
    assert (
        validate_page_list(
            account_id="acc",
            business_id="",
            pages=pages,
            known_account_ids={"acc"},
            known_business_ids=set(),
            busy_account_ids=set(),
            create_mode="profile",
        )
        == []
    )

    store, engine = _engine(tmp_path, FakeProvider())
    provider = engine.provider
    assert isinstance(provider, FakeProvider)
    batch, jobs = store.create_batch(
        account_id="acc",
        business_id="",
        pages=pages,
        settings={
            "delay_enabled": False,
            "max_retry": 3,
            "continue_on_error": True,
            "create_mode": "profile",
            "admin_targets": ["10001", "friend.name"],
        },
    )
    assert batch["create_mode"] == "profile"
    assert batch["admin_targets"] == ["10001", "friend.name"]
    assert jobs[0]["create_mode"] == "profile"
    assert jobs[0]["admin_targets"] == ["10001", "friend.name"]
    request = engine._request(jobs[0])
    assert request.create_mode == "profile"
    assert request.admin_targets == ["10001", "friend.name"]
    assert request.business_id == ""
    engine.run_batch(batch["id"])
    assert store.jobs_for_batch(batch["id"])[0]["status"] == "COMPLETED"
    assert provider.create_modes == ["profile"]
    assert provider.admin_calls == [("Harbor Moments", ("10001", "friend.name"))]


def test_generated_page_details_follow_language() -> None:
    from src.services.page_creation.page_details import (
        attach_generated_details,
        fill_missing_details,
        generate_page_details,
        slug_from_page_name,
        summarize_page_details,
    )

    vi = generate_page_details("Nắng Khoảnh Khắc", language="vi", rng=__import__("random").Random(1))
    assert vi["phone"].startswith("+84")
    assert "@" in vi["email"]
    assert vi["website"].startswith("https://")
    assert vi["bio"]
    assert any(token in vi["address"] for token in ("Phường", "Quận", "Hà Nội", "Đà Nẵng", "Cần Thơ", "Hồ Chí Minh"))

    en = generate_page_details("Sunny Moments", language="en", rng=__import__("random").Random(2))
    assert en["phone"].startswith("+1")
    assert "@" in en["email"]
    assert en["website"].startswith("https://")
    assert "Sunny" in en["bio"] or "Moments" in en["bio"]

    slug = slug_from_page_name("Đêm Thể Thao")
    assert "dem" in slug and "thao" in slug
    rows = attach_generated_details(
        [{"page_name": "Harbor TV", "avatar_path": ""}],
        language="en",
        enabled=True,
        rng=__import__("random").Random(3),
    )
    assert rows[0]["phone"]
    assert summarize_page_details(rows[0])
    empty = attach_generated_details([{"page_name": "X", "avatar_path": ""}], enabled=False)
    assert empty[0]["bio"] == ""
    assert empty[0]["phone"] == ""

    kept = fill_missing_details(
        "Harbor TV",
        {"bio": "Keep this bio", "phone": "", "email": "", "website": "", "address": "", "category": ""},
        language="en",
        rng=__import__("random").Random(4),
    )
    assert kept["bio"] == "Keep this bio"
    assert kept["phone"].startswith("+1")
    assert "@" in kept["email"]
    assert kept["category"]
    blank_name = fill_missing_details("", {"bio": "", "phone": "1"}, language="en")
    assert blank_name["phone"] == "1"
    assert blank_name["bio"] == ""


def test_ensure_page_complete_pipeline_is_callable() -> None:
    """Pipeline hậu tạo luôn tồn tại và bổ sung contact trống từ tên Page."""
    import inspect

    from src.services.page_creation.facebook_provider import (
        _pipeline_is_complete,
        _refresh_request_details,
        ensure_page_complete,
        is_avatar_upload_label,
        is_skip_setup_label,
    )
    from src.services.page_creation.provider import PageCreationRequest

    assert callable(ensure_page_complete)
    params = list(inspect.signature(ensure_page_complete).parameters)
    assert params == ["page", "request", "page_id"]

    request = PageCreationRequest(
        job_id="j1",
        batch_id="b1",
        account_id="acc",
        business_id="bm",
        page_name="Solace News",
        avatar_path="",
        bio="",
        phone="",
        email="",
    )
    _refresh_request_details(request)
    assert request.bio
    assert request.phone
    assert request.email
    assert request.category
    assert "Official" not in request.page_name
    assert is_avatar_upload_label("Upload profile picture")
    assert is_skip_setup_label("Skip")
    assert not is_avatar_upload_label("Skip")

    incomplete, reason = _pipeline_is_complete(
        request,
        {"avatar": False, "details_filled": 0, "admins_added": 0},
    )
    assert incomplete is False
    assert "thông tin" in reason

    complete, _ = _pipeline_is_complete(
        request,
        {"avatar": False, "details_filled": 5, "admins_added": 0},
    )
    assert complete is True


def test_request_exposes_on_progress(tmp_path: Path) -> None:
    store, engine = _engine(tmp_path, FakeProvider())
    batch, jobs = store.create_batch(
        account_id="acc",
        business_id="bm",
        pages=[{"page_name": "Step Page", "avatar_path": ""}],
        settings={"delay_enabled": False, "max_retry": 1, "continue_on_error": True, "create_mode": "bm"},
    )
    request = engine._request(jobs[0])
    assert callable(request.on_progress)
    request.on_progress("Bước 1/10: Mở trang tạo Page")
    saved = store.jobs_for_batch(batch["id"])[0]
    assert "Bước 1/10" in str(saved.get("progress_message") or "")
    snap = engine.progress(batch["id"])
    assert "Bước 1/10" in str(snap.get("progress_message") or "")


def test_batch_persists_generated_details(tmp_path: Path) -> None:
    store, engine = _engine(tmp_path, FakeProvider())
    batch, jobs = store.create_batch(
        account_id="acc",
        business_id="bm",
        pages=[
            {
                "page_name": "Detail Page",
                "avatar_path": "",
                "bio": "Hello bio",
                "phone": "+1 (415) 200-3000",
                "email": "contact.detail@example.com",
                "website": "https://www.detail.com",
                "address": "12 Main Street, Austin, TX",
            }
        ],
        settings={"delay_enabled": False, "max_retry": 1, "continue_on_error": True, "create_mode": "bm"},
    )
    job = jobs[0]
    assert job["bio"] == "Hello bio"
    assert job["phone"].startswith("+1")
    request = engine._request(job)
    assert request.bio == "Hello bio"
    assert request.email == "contact.detail@example.com"
    engine.run_batch(batch["id"])
    assert store.jobs_for_batch(batch["id"])[0]["status"] == "COMPLETED"



def test_end_to_end_profile_and_bm_modes_with_security_gate(tmp_path: Path) -> None:
    """E2E: profile+admin → BM tuần tự → SMS pause → Kiểm tra lại → COMPLETED, không tạo trùng."""
    provider = FakeProvider()
    store, engine = _engine(tmp_path, provider)

    profile_batch, _ = store.create_batch(
        account_id="acc",
        business_id="",
        pages=[
            {"page_name": "Profile One", "avatar_path": ""},
            {"page_name": "Profile Two", "avatar_path": ""},
        ],
        settings={
            "delay_enabled": False,
            "max_retry": 3,
            "continue_on_error": True,
            "create_mode": "profile",
            "admin_targets": ["90001"],
        },
    )
    engine.run_batch(profile_batch["id"])
    assert [job["status"] for job in store.jobs_for_batch(profile_batch["id"])] == ["COMPLETED", "COMPLETED"]
    assert provider.create_modes == ["profile", "profile"]
    assert provider.admin_calls == [
        ("Profile One", ("90001",)),
        ("Profile Two", ("90001",)),
    ]
    profile_ids = [job["page_id"] for job in store.jobs_for_batch(profile_batch["id"])]

    bm_batch, _ = _batch(store, ["BM Alpha", "BM Beta"], account_id="acc", business_id="bm1")
    engine.run_batch(bm_batch["id"])
    assert provider.created[-2:] == ["BM Alpha", "BM Beta"]
    assert provider.create_modes[-2:] == ["bm", "bm"]

    provider.allow_create = False
    provider.block_code = "SMS_VERIFICATION_REQUIRED"
    provider.block_accounts = {"acc"}
    sms_batch, _ = _batch(store, ["SMS Hold", "SMS Next"], account_id="acc", business_id="bm1")
    worker = threading.Thread(target=engine.run_batch, args=(sms_batch["id"],), daemon=True)
    worker.start()
    try:
        for _ in range(200):
            current = store.get_batch(sms_batch["id"])
            if current and current["status"] == "PAUSED_FOR_USER":
                break
            time.sleep(0.01)
        jobs = store.jobs_for_batch(sms_batch["id"])
        assert jobs[0]["status"] == "SMS_VERIFICATION_REQUIRED"
        assert jobs[1]["status"] == "BLOCKED_BY_ACCOUNT_VERIFICATION"
        assert "Profile One" in provider.created
        before = list(provider.created)
        provider.allow_create = True
        provider.block_code = ""
        assert engine.check_again(sms_batch["id"]).ok
        for _ in range(200):
            if store.get_batch(sms_batch["id"])["status"] == "COMPLETED":
                break
            time.sleep(0.02)
        if store.get_batch(sms_batch["id"])["status"] != "COMPLETED" and not engine.is_running(sms_batch["id"]):
            engine.run_batch(sms_batch["id"])
        assert [job["status"] for job in store.jobs_for_batch(sms_batch["id"])] == ["COMPLETED", "COMPLETED"]
        assert provider.created == before + ["SMS Hold", "SMS Next"]
    finally:
        engine.cancel(sms_batch["id"])
        worker.join(1)

    provider.verify_ids["Profile One"] = profile_ids[0]
    again = engine.provider.verify_existing(engine._request(store.jobs_for_batch(profile_batch["id"])[0]))
    assert again.ok and again.page_id == profile_ids[0]
    assert provider.created.count("Profile One") == 1
