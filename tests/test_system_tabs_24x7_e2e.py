"""
E2E hệ thống 24/7: mọi tab chính + chức năng con, quy mô lớn TK/Page,
đồng bộ đa tác vụ, FeatureBusy lịch↔chia sẻ, tự nâng cấp giữ data/config.
"""

from __future__ import annotations

import inspect
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import src.scheduler as sch
from src.gui.share_tab import interleave_pages_by_account
from src.services import schedule_queue_dispatcher as sqd
from src.services.account_name_refresh import plan_name_refresh_ids
from src.services.app_updater import apply_update_package, read_local_version
from src.services.facebook_groups.engine import GroupEngine
from src.services.facebook_groups.share_schedule import compute_allowed_share_accounts
from src.services.facebook_groups.store import GroupStore
from src.utils import feature_busy as fb
from src.utils.account_browser_profile import is_foreign_browser_exe_path
from src.utils.concurrency_runtime import (
    KIND_BROWSER_POST,
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
from src.utils.feature_busy import (
    FEATURE_SCHEDULE,
    FEATURE_SHARE,
    acquire_share_after_schedule,
    reset_for_tests,
    try_acquire,
)
from src.utils.pages_manager import PagesManager
from src.utils.paths import project_root
from src.utils.playwright_browser_lock import load_browser_manifest
from src.utils.schedule_posts_manager import SchedulePostsManager


# ── Tab map (notebook chính + notebook con) ─────────────────────────────────

SYSTEM_TABS = (
    "Tài khoản",
    "Page",
    "Lịch đăng",
    "AI",
    "Video",
    "Tải video",
    "Chờ đăng",
    "TikTok",
    "Tương tác",
    "Chia sẻ",
    "AI Agent",
)

SHARE_SUBTABS = ("Hàng chờ", "Kết quả", "Chia sẻ tay", "Nội dung & cài đặt")
TIKTOK_SUBTABS = ("Tài khoản", "Lịch đăng")
AI_FLEET_SUBTABS = ("Master", "Pages", "Writer", "Review")


class _ShareFake:
    """Provider chia sẻ giả — đếm song song + workload thật."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.publish_calls: list[dict] = []
        self.max_concurrent = 0
        self.active: set[str] = set()

    def spawn(self) -> "_ShareFake":
        return self

    def check_session(self, account_id: str) -> str:
        del account_id
        return ""

    def begin_account(self, account_id: str) -> None:
        with self.lock:
            self.active.add(account_id)
            self.max_concurrent = max(self.max_concurrent, len(self.active))
        time.sleep(0.008)

    def end_account(self, account_id: str) -> None:
        with self.lock:
            self.active.discard(account_id)

    def switch_to_page(self, **kwargs) -> dict:
        del kwargs
        return {"error_code": ""}

    def publish_link(self, **kwargs) -> dict:
        time.sleep(0.005)
        with self.lock:
            self.publish_calls.append(
                {"account_id": kwargs.get("account_id"), "page_id": kwargs.get("page_id")}
            )
        return {
            "post_id": f"post-{kwargs.get('page_id')}",
            "post_url": str(kwargs.get("source_url") or ""),
        }

    def verify_published_post(self, **kwargs) -> dict:
        return {"post_id": f"post-{kwargs.get('page_id')}", "post_url": ""}


@pytest.fixture(autouse=True)
def _clean_gates(monkeypatch: pytest.MonkeyPatch):
    reset_for_tests()
    monkeypatch.setattr(fb, "schedule_queue_blocks_share", lambda **_k: False)
    monkeypatch.setattr(fb, "schedule_load_for_account", lambda _aid="": (0, 0, 0))
    monkeypatch.setattr(
        "src.services.facebook_groups.engine.GroupEngine._page_alias_index",
        lambda self: {},
    )
    for kind in (KIND_DOWNLOAD, KIND_VIDEO_EDITOR, KIND_SCHEDULER, KIND_BROWSER_POST):
        while workload_snapshot().get(kind, 0) > 0:
            workload_end(kind)
    yield
    reset_for_tests()
    for kind in (KIND_DOWNLOAD, KIND_VIDEO_EDITOR, KIND_SCHEDULER, KIND_BROWSER_POST):
        while workload_snapshot().get(kind, 0) > 0:
            workload_end(kind)


def _pages(n_accounts: int, pages_per: int) -> list[dict]:
    rows: list[dict] = []
    for a in range(n_accounts):
        aid = f"acc-{a:02d}"
        for p in range(pages_per):
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


def _patch_project_root(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    """Cô lập profile/cookie vào tmp — không ghi data/profiles thật."""
    monkeypatch.setattr("src.utils.paths.project_root", lambda: root)
    monkeypatch.setattr("src.utils.db_manager.project_root", lambda: root)
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: root)
    monkeypatch.setattr("src.utils.pages_manager.ensure_page_workspace", lambda _pid: None)


def test_system_tab_modules_importable() -> None:
    """Mọi tab GUI chính import được (không crash khi load module)."""
    import src.gui.account_workbench  # noqa: F401
    import src.gui.ai_fleet_tab  # noqa: F401
    import src.gui.human_interaction_tab  # noqa: F401
    import src.gui.page_management  # noqa: F401
    import src.gui.share_tab  # noqa: F401
    import src.gui.tiktok_manager_tab  # noqa: F401
    import src.gui.video_editor_tab  # noqa: F401

    assert len(SYSTEM_TABS) >= 10
    assert "Chia sẻ" in SYSTEM_TABS
    assert SHARE_SUBTABS and TIKTOK_SUBTABS and AI_FLEET_SUBTABS


def test_accounts_pages_scale_cache_24x7(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """50 TK × 20 Page: ghi/đọc cache không đọc đĩa lần 2, strip exe ngoại lai."""
    _patch_project_root(monkeypatch, tmp_path)
    acc_path = tmp_path / "accounts.json"
    page_path = tmp_path / "pages.json"
    acc_path.write_text("[]\n", encoding="utf-8")
    page_path.write_text("[]\n", encoding="utf-8")
    am = AccountsDatabaseManager(json_path=acc_path)
    pm = PagesManager(json_path=page_path)

    via_root = tmp_path / "D_drive" / "ViaBox"
    (via_root / "App" / "Firefox").mkdir(parents=True)
    foreign = via_root / "App" / "Firefox" / "firefox.exe"
    foreign.write_bytes(b"MZ")
    assert is_foreign_browser_exe_path(str(foreign), project_root_dir=tmp_path)

    for i in range(50):
        aid = f"UID_{100000000000000 + i}"
        am.upsert(
            {
                "id": aid,
                "name": f"TK {i}",
                "browser_type": "firefox",
                "portable_path": f"data/profiles/firefox/{aid}",
                "profile_path": f"data/profiles/firefox/{aid}",
                "cookie_path": f"data/cookies/{aid}.json",
                "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
                "use_proxy": False,
                "browser_exe_path": str(foreign) if i % 7 == 0 else "",
            }
        )
        for j in range(20):
            pm.upsert(
                {
                    "id": "",
                    "account_id": aid,
                    "page_name": f"P{i}-{j}",
                    "page_url": f"https://www.facebook.com/p{i}_{j}",
                    "post_style": "post",
                }
            )

    accounts = am.load_all()
    pages = pm.load_all()
    assert len(accounts) == 50
    assert len(pages) == 1000
    dirty = [a for a in accounts if str(a.get("browser_exe_path") or "").strip()]
    assert not dirty, f"còn exe ngoại lai: {dirty[:3]}"

    reads = {"n": 0}
    real_read = Path.read_text

    def _counted(self: Path, *args, **kwargs):
        if self.resolve() in {acc_path.resolve(), page_path.resolve()}:
            reads["n"] += 1
        return real_read(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _counted)
    assert len(AccountsDatabaseManager(json_path=acc_path).load_all()) == 50
    assert len(PagesManager(json_path=page_path).load_all()) == 1000
    assert reads["n"] == 0, "cache 24/7 phải tránh đọc lại JSON lớn"


def test_schedule_share_feature_busy_handoff() -> None:
    """Lịch giữ TK → chia sẻ chờ; nhả lịch → chia sẻ lấy được (ưu tiên đăng trước)."""
    lease_s = try_acquire(feature=FEATURE_SCHEDULE, account_id="acc-busy", detail="job-1")
    assert lease_s is not None
    blocked = try_acquire(feature=FEATURE_SHARE, account_id="acc-busy", detail="share-1")
    assert blocked is None
    lease_s.release()
    got = acquire_share_after_schedule(
        account_id="acc-busy",
        detail="share-1",
        gap_min_minutes=0.0,
        gap_max_minutes=0.0,
        poll=0.05,
    )
    assert got is not None
    got.release()


def test_share_wave_registers_browser_workload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Chia sẻ đăng ký KIND_BROWSER_POST → reconcile hạ slot khi kèm download."""
    monkeypatch.setenv("TOOLFB_AUTO_MULTITASK", "1")
    monkeypatch.delenv("TOOLFB_MANUAL_CONCURRENCY", raising=False)
    monkeypatch.delenv("SHARE_PARALLEL_ACCOUNTS", raising=False)
    apply_multi_task_defaults()
    base = int(os.environ["BROWSER_CONCURRENCY"])

    workload_begin(KIND_DOWNLOAD)
    workload_begin(KIND_SCHEDULER)
    lowered = reconcile_multi_task_limits()
    assert int(lowered.get("BROWSER_CONCURRENCY", base)) <= base

    store = GroupStore(tmp_path / "facebook_groups.json")
    fake = _ShareFake()
    engine = GroupEngine(store, fake, sleep=lambda _s: None)
    monkeypatch.setenv("BROWSER_CONCURRENCY", "3")
    pages = _pages(6, 3)
    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/watch/?v=1",
        text="e2e",
        destination="page",
        cooldown_fixed=0,
    )
    ids = list(wave.get("batch_ids") or [])
    assert ids
    seen = {"max": 0}

    def _watch() -> None:
        for _ in range(80):
            snap = workload_snapshot()
            seen["max"] = max(seen["max"], int(snap.get(KIND_BROWSER_POST, 0)))
            time.sleep(0.01)

    t = threading.Thread(target=_watch, daemon=True)
    t.start()
    engine.run_share_wave(ids)
    t.join(timeout=3.0)
    assert len(fake.publish_calls) == 18
    assert fake.max_concurrent <= 3
    assert seen["max"] >= 1, "share phải đăng ký workload browser"
    assert workload_snapshot().get(KIND_BROWSER_POST, 0) == 0
    workload_end(KIND_DOWNLOAD)
    workload_end(KIND_SCHEDULER)


def test_schedule_dispatch_scale_with_share_interleave(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """30 job lịch + round-robin Page: worker giới hạn, không chồng cùng TK."""
    past = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    sp = SchedulePostsManager(json_path=tmp_path / "schedule_posts.json")
    rows = []
    for i in range(30):
        rows.append(
            {
                "id": f"j{i}",
                "account_id": f"acc{i % 10}",
                "page_id": f"page{i}",
                "post_type": "text",
                "status": "pending",
                "scheduled_at": past,
                "created_at": past,
            }
        )
    sp.upsert_many(rows)

    class _Pool:
        def __init__(self) -> None:
            self._max_workers = 4
            self.submitted: list[str] = []

        def submit(self, fn, *args, **kwargs):  # noqa: ANN001, ANN201
            from concurrent.futures import Future

            self.submitted.append(str(kwargs.get("schedule_post_job_id")))
            return Future()

    pool = _Pool()
    monkeypatch.setattr(sch, "get_default_schedule_posts_manager", lambda: sp)
    monkeypatch.setattr(sch, "get_schedule_posts_dispatch_pool", lambda: pool)
    monkeypatch.setattr(sch, "ensure_schedule_queue_recovery", lambda _sp: None)
    monkeypatch.setattr(sch, "_maybe_archive_finished_jobs", lambda _sp: None)
    monkeypatch.setattr(sch, "_draft_id_for_queue_job", lambda _j: "")
    monkeypatch.setattr(sch, "_resolve_engine_for_account", lambda _a, accounts=None: "firefox")
    monkeypatch.setattr(sch, "write_queue_monitor", lambda **_k: None)
    monkeypatch.setattr(sch, "compute_smart_delay_ms", lambda **_k: 0)
    monkeypatch.setattr(sqd, "_max_consecutive_per_account", lambda: 0)
    sch._dispatch_tracked.clear()
    sch._account_run_inflight.clear()

    sch.tick_schedule_post_jobs()
    assert len(pool.submitted) == 4
    running = [r for r in sp.load_all() if r["status"] == "running"]
    aids = [r["account_id"] for r in running]
    assert len(aids) == len(set(aids)), "không dispatch 2 job cùng TK"

    interleaved = interleave_pages_by_account(
        [{**p, "iid": p["page_id"]} for p in _pages(10, 5)]
    )
    assert len(interleaved) == 50
    assert len({r["account_id"] for r in interleaved[:10]}) == 10


def test_name_refresh_and_tick_scope_consistency() -> None:
    """Cập nhật tên: plan tuần tự; tick share: chỉ TK được chọn."""
    ids, forced = plan_name_refresh_ids(
        [
            {"id": "a", "name": "10001"},
            {"id": "b", "name": "Real Name"},
            {"id": "c", "name": "UID_999"},
        ]
    )
    assert forced is False
    assert "a" in ids
    assert "c" in ids
    assert "b" not in ids
    assert compute_allowed_share_accounts(
        checked_account_ids={"a", "b"}, filter_account_id="a"
    ) == {"a"}
    assert compute_allowed_share_accounts(
        checked_account_ids=set(), filter_account_id=None
    ) == set()


def test_smart_upgrade_preserves_data_config_and_version_aligned() -> None:
    """Updater giữ data/config; version.json ↔ browser manifest đồng nhất."""
    defaults = inspect.signature(apply_update_package).parameters
    preserved = defaults["preserve_on_apply_dirs"].default
    assert "data" in preserved and "config" in preserved

    root = project_root()
    ver = read_local_version(root)
    manifest = load_browser_manifest(root) or {}
    app_ver = str(manifest.get("app_version") or "").strip()
    assert ver, "version.json trống"
    assert app_ver == ver, f"manifest app_version={app_ver!r} ≠ version={ver!r}"


def test_multitask_reconcile_all_tabs_active(monkeypatch: pytest.MonkeyPatch) -> None:
    """Download + VE + lịch + browser cùng lúc → hạ browser/ffmpeg (máy 24/7)."""
    monkeypatch.setenv("TOOLFB_AUTO_MULTITASK", "1")
    monkeypatch.delenv("TOOLFB_MANUAL_CONCURRENCY", raising=False)
    apply_multi_task_defaults(gui=True)
    base_b = int(os.environ["BROWSER_CONCURRENCY"])
    workload_begin(KIND_DOWNLOAD)
    workload_begin(KIND_VIDEO_EDITOR)
    workload_begin(KIND_SCHEDULER)
    workload_begin(KIND_BROWSER_POST)
    applied = reconcile_multi_task_limits(browser_slots_in_use=3)
    assert int(applied["BROWSER_CONCURRENCY"]) < base_b or int(applied["BROWSER_CONCURRENCY"]) <= 2
    assert int(applied["TOOLFB_FFMPEG_CONCURRENCY"]) == 1
    assert os.environ.get("SCHEDULE_PER_ACCOUNT_MAX_PARALLEL") == "1"
    for k in (KIND_DOWNLOAD, KIND_VIDEO_EDITOR, KIND_SCHEDULER, KIND_BROWSER_POST):
        workload_end(k)


def test_share_parallel_respects_reconciled_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SHARE đọc BROWSER_CONCURRENCY sau reconcile (không vượt khi tab khác bận)."""
    del tmp_path
    monkeypatch.setenv("TOOLFB_AUTO_MULTITASK", "1")
    monkeypatch.delenv("TOOLFB_MANUAL_CONCURRENCY", raising=False)
    monkeypatch.delenv("SHARE_PARALLEL_ACCOUNTS", raising=False)
    apply_multi_task_defaults()
    monkeypatch.setenv("BROWSER_CONCURRENCY", "5")
    workload_begin(KIND_DOWNLOAD)
    workload_begin(KIND_VIDEO_EDITOR)
    reconcile_multi_task_limits()
    limit = GroupEngine._share_parallel_account_limit()
    assert limit <= int(os.environ["BROWSER_CONCURRENCY"])
    assert limit >= 1
    workload_end(KIND_DOWNLOAD)
    workload_end(KIND_VIDEO_EDITOR)


def test_ai_fleet_and_tiktok_entrypoints_exist() -> None:
    """Tab AI Agent + TikTok: entry điểm resume/tick tồn tại (chạy nền 24/7)."""
    from src.services.ai_fleet import pipeline as fleet_pipe
    from src.services.tiktok import schedule_tick as tt_tick

    assert callable(fleet_pipe.resume_task)
    assert callable(tt_tick.tick_tiktok_upload_jobs)


def test_end_to_end_cross_tab_day_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Một vòng ngày: import TK → Page → lịch promote → share wave → workload sạch.
    Mô phỏng vận hành xuyên suốt không leak lock/workload.
    """
    _patch_project_root(monkeypatch, tmp_path)
    monkeypatch.setenv("BROWSER_CONCURRENCY", "2")
    monkeypatch.setenv("SHARE_PARALLEL_ACCOUNTS", "2")

    acc_path = tmp_path / "accounts.json"
    page_path = tmp_path / "pages.json"
    acc_path.write_text("[]\n", encoding="utf-8")
    page_path.write_text("[]\n", encoding="utf-8")
    am = AccountsDatabaseManager(json_path=acc_path)
    pm = PagesManager(json_path=page_path)

    for i in range(8):
        aid = f"sys-{i:02d}"
        am.upsert(
            {
                "id": aid,
                "name": f"Name {i}",
                "browser_type": "firefox",
                "portable_path": f"data/profiles/firefox/{aid}",
                "cookie_path": f"data/cookies/{aid}.json",
                "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
                "use_proxy": False,
            }
        )
        for j in range(4):
            pm.upsert(
                {
                    "id": "",
                    "account_id": aid,
                    "page_name": f"{aid}-p{j}",
                    "page_url": f"https://www.facebook.com/{aid}_p{j}",
                    "post_style": "post",
                }
            )

    past = "2020-01-01T00:00:00+00:00"
    sp = SchedulePostsManager(json_path=tmp_path / "schedule_posts.json")
    sp.upsert(
        {
            "id": "day-job",
            "account_id": "sys-00",
            "page_id": "p0",
            "post_type": "text",
            "status": "pending",
            "scheduled_at": past,
            "created_at": past,
        }
    )
    now = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
    assert sqd.promote_due_pending_to_ready(sp, now=now) >= 1

    store = GroupStore(tmp_path / "facebook_groups.json")
    fake = _ShareFake()
    engine = GroupEngine(store, fake, sleep=lambda _s: None)
    pages = _pages(8, 4)
    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://facebook.com/x",
        text="day",
        destination="page",
        cooldown_fixed=0,
    )
    ids = list(wave["batch_ids"])
    engine.run_share_wave(ids)
    assert len(fake.publish_calls) == 32
    assert fake.max_concurrent <= 2
    assert workload_snapshot().get(KIND_BROWSER_POST, 0) == 0
    assert json.loads(acc_path.read_text(encoding="utf-8"))
    assert len(pm.load_all()) == 32
