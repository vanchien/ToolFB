"""
E2E tích hợp: import Via (ổ ngoài) → copy data/profiles → xóa nguồn + exe D
→ cập nhật tên lần lượt → không mở browser khi chưa có session → version/manifest khớp.
"""

from __future__ import annotations

import json
import shutil
import threading
import time
from pathlib import Path

import pytest

from src.services.account_name_refresh import (
    assert_sequential_browser_opens,
    plan_name_refresh_ids,
    run_name_refresh_sequential,
)
from src.services.app_updater import read_local_version
from src.utils.account_browser_profile import (
    import_external_profile_into_account,
    is_foreign_browser_exe_path,
    portable_profile_likely_has_session,
)
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.paths import project_root
from src.utils.playwright_browser_lock import load_browser_manifest
from src.utils.schedule_posts_filters import account_name_is_placeholder


def _make_firefox_profile(d: Path, *, with_session: bool = True) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / "prefs.js").write_text("// Firefox\n", encoding="utf-8")
    if with_session:
        (d / "cookies.sqlite").write_bytes(b"SQLite format 3\x00" + b"\x00" * 200)
        (d / "places.sqlite").write_bytes(b"SQLite format 3\x00" + b"\x00" * 1200)
    else:
        (d / "compatibility.ini").write_text("[Compatibility]\n", encoding="utf-8")


def _make_via_portable(root: Path, uid: str) -> Path:
    """Layout Via/GPM: App/Firefox/firefox.exe + Data/profile."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "App" / "Firefox").mkdir(parents=True)
    (root / "App" / "Firefox" / "firefox.exe").write_bytes(b"MZ")
    (root / "FirefoxPortable.exe").write_bytes(b"MZ")
    profile = root / "Data" / "profile"
    _make_firefox_profile(profile, with_session=True)
    return profile


def test_e2e_import_via_clears_d_exe_and_source_deletable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)
    monkeypatch.setattr("src.utils.paths.project_root", lambda: tmp_path)
    monkeypatch.setattr("src.utils.db_manager.project_root", lambda: tmp_path)

    via_root = tmp_path / "D_drive" / "face bokk" / "via us" / "558700732"
    _make_via_portable(via_root, "558700732")
    fake_d_exe = via_root / "App" / "Firefox" / "firefox.exe"
    assert fake_d_exe.is_file()
    assert is_foreign_browser_exe_path(str(fake_d_exe), project_root_dir=tmp_path)

    acc_json = tmp_path / "accounts.json"
    acc_json.write_text("[]\n", encoding="utf-8")
    mgr = AccountsDatabaseManager(acc_json)
    mgr.upsert(
        {
            "id": "UID_558700732",
            "name": "558700732",
            "browser_type": "firefox",
            "portable_path": str(via_root / "Data" / "profile"),
            "cookie_path": "data/cookies/UID_558700732.json",
            "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
            "use_proxy": False,
            "import_type": "folder",
            "browser_exe_path": str(fake_d_exe),
        }
    )
    rec = mgr.get_by_id("UID_558700732")
    assert rec is not None
    assert rec["portable_path"] == "data/profiles/firefox/UID_558700732"
    assert rec["browser_exe_path"] == ""
    assert account_name_is_placeholder(str(rec["name"]), "UID_558700732")
    dest = tmp_path / "data" / "profiles" / "firefox" / "UID_558700732"
    assert (dest / "cookies.sqlite").is_file()

    shutil.rmtree(via_root)
    assert not via_root.exists()
    assert (dest / "prefs.js").is_file()
    # Load lại vẫn strip exe ngoại lai
    again = AccountsDatabaseManager(acc_json).get_by_id("UID_558700732")
    assert again is not None
    assert again["browser_exe_path"] == ""


def test_e2e_name_refresh_skips_no_session_and_runs_sequential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)

    rows = [
        {
            "id": "a_ok",
            "name": "111111",
            "portable_path": "data/profiles/firefox/a_ok",
        },
        {
            "id": "a_empty",
            "name": "222222",
            "portable_path": "data/profiles/firefox/a_empty",
        },
        {
            "id": "a_cookie",
            "name": "Tài khoản mới",
            "portable_path": "data/profiles/firefox/a_cookie",
        },
    ]
    _make_firefox_profile(tmp_path / "data/profiles/firefox/a_ok", with_session=True)
    (tmp_path / "data/profiles/firefox/a_empty").mkdir(parents=True)
    (tmp_path / "data/profiles/firefox/a_empty" / "prefs.js").write_text("x", encoding="utf-8")
    _make_firefox_profile(tmp_path / "data/profiles/firefox/a_cookie", with_session=True)

    open_log: list[str] = []
    saved: dict[str, str] = {}

    def lookup(acc: dict) -> tuple[str, str]:
        if acc["id"] == "a_cookie":
            return "Cookie Name", ""
        return "", "no cookie file"

    def browser(aid: str) -> tuple[str, str]:
        open_log.append(f"open:{aid}")
        time.sleep(0.02)
        open_log.append(f"close:{aid}")
        return f"Browser {aid}", ""

    def session_ready(acc: dict) -> bool:
        return portable_profile_likely_has_session(
            str(acc.get("portable_path") or ""),
            project_root_dir=tmp_path,
        )

    ids, forced = plan_name_refresh_ids(rows, [r["id"] for r in rows])
    result = run_name_refresh_sequential(
        rows,
        ids,
        forced=forced,
        lookup_cookie=lookup,
        read_browser=browser,
        save_name=lambda a, n: saved.__setitem__(a, n),
        session_ready=session_ready,
    )
    assert_sequential_browser_opens(open_log)
    # Cookie hit — không mở browser
    assert "open:a_cookie" not in open_log
    # Empty profile — skip, không mở
    assert "open:a_empty" not in open_log
    assert any("a_empty:" in s and "phiên" in s for s in result.skipped)
    # Có session + cookie miss → đúng 1 browser
    assert open_log == ["open:a_ok", "close:a_ok"]
    assert saved["a_ok"] == "Browser a_ok"
    assert saved["a_cookie"] == "Cookie Name"
    assert "a_empty" not in saved


def test_e2e_version_and_manifest_aligned_for_update() -> None:
    """Nâng cấp/sửa chữa: version.json khớp browser_bundle_manifest — tránh chặn launch."""
    root = project_root()
    ver = read_local_version(root)
    mf = load_browser_manifest(root)
    assert ver and not ver.endswith("-dev")
    assert mf is not None
    assert str(mf.get("app_version") or "") == ver
    browsers = mf.get("browsers") or {}
    assert "firefox" in browsers
    assert "chromium" in browsers


def test_e2e_repair_foreign_exe_on_normalize(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """accounts.json còn exe ổ D → load tự sửa về rỗng (repair)."""
    monkeypatch.setattr("src.utils.db_manager.project_root", lambda: tmp_path)
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)
    (tmp_path / "data" / "profiles" / "firefox" / "UID_1").mkdir(parents=True)
    (tmp_path / "data" / "cookies").mkdir(parents=True)
    (tmp_path / "data" / "cookies" / "UID_1.json").write_text("[]\n", encoding="utf-8")
    payload = [
        {
            "id": "UID_1",
            "name": "Tài khoản mới",
            "browser_type": "firefox",
            "portable_path": "data/profiles/firefox/UID_1",
            "profile_path": "data/profiles/firefox/UID_1",
            "cookie_path": "data/cookies/UID_1.json",
            "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
            "use_proxy": False,
            "browser_exe_path": r"D:\face bokk\via\1\App\Firefox\firefox.exe",
            "import_type": "folder",
            "notes": "",
            "facebook_uid": "1",
            "email": "",
            "totp_enabled": False,
            "password_ref": "",
            "totp_secret_ref": "",
            "session_status": "active",
        }
    ]
    acc_json = tmp_path / "accounts.json"
    acc_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    mgr = AccountsDatabaseManager(acc_json)
    rows = mgr.load_all()
    assert rows[0]["browser_exe_path"] == ""
    # Đã ghi lại file đã sửa
    disk = json.loads(acc_json.read_text(encoding="utf-8"))
    assert disk[0]["browser_exe_path"] == ""


def test_e2e_concurrent_name_refresh_never_double_opens() -> None:
    """Hai worker giả — mỗi browser vẫn tuần tự trong một wave."""
    rows = [{"id": f"t{i}", "name": f"{100000 + i}"} for i in range(6)]
    lock = threading.Lock()
    open_now = 0
    max_open = 0
    open_log: list[str] = []

    def lookup(_acc: dict) -> tuple[str, str]:
        return "", "x"

    def browser(aid: str) -> tuple[str, str]:
        nonlocal open_now, max_open
        with lock:
            open_log.append(f"open:{aid}")
            open_now += 1
            max_open = max(max_open, open_now)
        time.sleep(0.03)
        with lock:
            open_now -= 1
            open_log.append(f"close:{aid}")
        return f"N{aid}", ""

    result = run_name_refresh_sequential(
        rows,
        [r["id"] for r in rows],
        forced=True,
        lookup_cookie=lookup,
        read_browser=browser,
        save_name=lambda _a, _n: None,
        session_ready=lambda _a: True,
    )
    assert len(result.updated) == 6
    assert max_open == 1
    assert_sequential_browser_opens(open_log)
