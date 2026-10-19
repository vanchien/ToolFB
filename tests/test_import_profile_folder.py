"""Nhận diện / liệt kê profile Firefox khi thêm tài khoản từ thư mục có sẵn."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.utils.account_browser_profile import (
    detect_browser_profile_kind,
    import_external_profile_into_account,
    list_importable_profile_dirs,
    suggest_account_id_from_profile_dir,
)
from src.gui.account_workbench import _folder_row_is_valid
from src.utils.db_manager import AccountsDatabaseManager


def _make_firefox_profile(d: Path) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / "prefs.js").write_text("// Firefox\n", encoding="utf-8")
    (d / "cookies.sqlite").write_bytes(b"SQLite format 3\x00" + b"\x00" * 80)
    (d / "places.sqlite").write_bytes(b"SQLite format 3\x00" + b"\x00" * 1200)


def test_detect_firefox_profile(tmp_path: Path) -> None:
    prof = tmp_path / "543677287"
    _make_firefox_profile(prof)
    assert detect_browser_profile_kind(prof) == "firefox"
    assert detect_browser_profile_kind(tmp_path / "empty") == ""


def test_list_importable_single_profile(tmp_path: Path) -> None:
    prof = tmp_path / "543677287"
    _make_firefox_profile(prof)
    found = list_importable_profile_dirs(prof)
    assert found == [prof.resolve()]


def test_list_importable_parent_skips_cache(tmp_path: Path) -> None:
    parent = tmp_path / "via us reels 2"
    _make_firefox_profile(parent / "543677287")
    _make_firefox_profile(parent / "999888777")
    (parent / "cache2").mkdir()
    (parent / "cache2" / "prefs.js").write_text("x", encoding="utf-8")
    found = list_importable_profile_dirs(parent)
    names = sorted(p.name for p in found)
    assert names == ["543677287", "999888777"]


def test_suggest_id_from_uid_folder() -> None:
    aid, uid = suggest_account_id_from_profile_dir(Path(r"D:\face bokk\via us reels 2\543677287"))
    assert aid == "UID_543677287"
    assert uid == "543677287"


def test_folder_row_valid_without_exe_for_firefox() -> None:
    assert _folder_row_is_valid(profile_ok=True, exe_ok=False, browser_type="firefox")
    assert _folder_row_is_valid(profile_ok=False, exe_ok=True, browser_type="firefox")
    assert not _folder_row_is_valid(profile_ok=False, exe_ok=False, browser_type="firefox")


def test_import_copies_profile_so_source_can_be_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sau import: bản copy trong data/profiles; xóa nguồn không làm mất cookie DB."""
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)
    external = tmp_path / "external" / "543677287"
    _make_firefox_profile(external)
    (external / "parent.lock").write_text("lock", encoding="utf-8")
    (external / "cache2").mkdir()
    (external / "cache2" / "junk").write_text("x", encoding="utf-8")

    rel = import_external_profile_into_account("UID_543677287", str(external), "firefox")
    dest = tmp_path / rel
    assert dest.is_dir()
    assert (dest / "prefs.js").is_file()
    assert (dest / "cookies.sqlite").is_file()
    assert not (dest / "parent.lock").exists()
    assert not (dest / "cache2").exists()
    assert (dest / ".toolfb_account_id").read_text(encoding="utf-8").strip() == "UID_543677287"

    # Xóa nguồn — bản copy vẫn còn.
    import shutil

    shutil.rmtree(external)
    assert not external.exists()
    assert (dest / "cookies.sqlite").is_file()


def _make_firefox_portable_root(root: Path) -> Path:
    """Giả lập layout Firefox Portable: App + Data/profile + FirefoxPortable.exe."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "App" / "Firefox64").mkdir(parents=True)
    (root / "App" / "Firefox64" / "firefox.exe").write_bytes(b"MZ")
    (root / "Other").mkdir()
    (root / "FirefoxPortable.exe").write_bytes(b"MZ")
    profile = root / "Data" / "profile"
    _make_firefox_profile(profile)
    return profile


def test_resolve_skips_firefox_portable_wrapper(tmp_path: Path) -> None:
    from src.utils.account_browser_profile import resolve_firefox_executable_for_playwright

    portable = tmp_path / "543677287"
    _make_firefox_portable_root(portable)
    wrapper = portable / "FirefoxPortable.exe"
    real = resolve_firefox_executable_for_playwright(wrapper)
    assert real
    assert Path(real).name.lower() == "firefox.exe"
    assert "portable" not in Path(real).stem.lower()
    # Không có App → trả rỗng (Playwright bundle).
    lone = tmp_path / "only" / "FirefoxPortable.exe"
    lone.parent.mkdir(parents=True)
    lone.write_bytes(b"MZ")
    assert resolve_firefox_executable_for_playwright(lone) == ""


def test_firefox_portable_detected_as_one_account(tmp_path: Path) -> None:
    portable = tmp_path / "543677287"
    profile = _make_firefox_portable_root(portable)
    from src.utils.account_browser_profile import (
        is_firefox_portable_root,
        list_importable_profile_dirs,
        resolve_import_profile_source,
        suggest_account_id_from_profile_dir,
    )

    assert is_firefox_portable_root(portable)
    found = list_importable_profile_dirs(portable)
    assert found == [portable.resolve()]
    # Không tách App/Data thành 2 tài khoản.
    assert "App" not in [p.name for p in found]
    assert "Data" not in [p.name for p in found]

    display, prof, exe = resolve_import_profile_source(portable)
    assert display.resolve() == portable.resolve()
    assert prof.resolve() == profile.resolve()
    # Ưu tiên App/.../firefox.exe — không dùng FirefoxPortable.exe (chặn multi-instance).
    assert exe and Path(exe).name.lower() == "firefox.exe"
    assert "App" in Path(exe).parts or "app" in str(Path(exe)).lower()

    aid, uid = suggest_account_id_from_profile_dir(portable)
    assert aid == "UID_543677287"
    assert uid == "543677287"


def test_parent_of_portables_lists_uid_folders_not_app_data(tmp_path: Path) -> None:
    parent = tmp_path / "via us reels 2"
    _make_firefox_portable_root(parent / "543677287")
    _make_firefox_portable_root(parent / "999888777")
    (parent / "App").mkdir()  # nhiễu — không phải portable đủ App+Data+exe ở cấp này đúng cách...
    from src.utils.account_browser_profile import list_importable_profile_dirs

    found = list_importable_profile_dirs(parent)
    names = sorted(p.name for p in found)
    assert names == ["543677287", "999888777"]


def test_import_portable_copies_only_profile_not_app(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)
    portable = tmp_path / "ext" / "543677287"
    _make_firefox_portable_root(portable)
    from src.utils.account_browser_profile import import_external_profile_into_account

    rel = import_external_profile_into_account("UID_543677287", str(portable), "firefox")
    dest = tmp_path / rel
    assert (dest / "prefs.js").is_file()
    assert (dest / "cookies.sqlite").is_file()
    # Không copy cả cây App vào data/profiles.
    assert not (dest / "App").exists()
    assert not (dest / "FirefoxPortable.exe").exists()


def test_upsert_folder_rewrites_portable_to_project_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.utils.account_browser_profile.project_root", lambda: tmp_path)
    monkeypatch.setattr("src.utils.paths.project_root", lambda: tmp_path)
    monkeypatch.setattr("src.utils.db_manager.project_root", lambda: tmp_path)
    external = tmp_path / "ext" / "999"
    _make_firefox_profile(external)
    acc_json = tmp_path / "accounts.json"
    acc_json.write_text("[]\n", encoding="utf-8")
    mgr = AccountsDatabaseManager(acc_json)
    mgr.upsert(
        {
            "id": "UID_999",
            "name": "999",
            "browser_type": "firefox",
            "portable_path": str(external),
            "cookie_path": "data/cookies/UID_999.json",
            "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
            "use_proxy": False,
            "import_type": "folder",
            "browser_exe_path": str(tmp_path / "via" / "App" / "Firefox" / "firefox.exe"),
        }
    )
    rec = mgr.get_by_id("UID_999")
    assert rec is not None
    assert str(rec["portable_path"]).replace("\\", "/") == "data/profiles/firefox/UID_999"
    assert rec.get("browser_exe_path") == ""
    assert (tmp_path / "data/profiles/firefox/UID_999/cookies.sqlite").is_file()
    # Nguồn vẫn còn sau upsert (chưa xóa) — nhưng TK không phụ thuộc nó.
    assert external.is_dir()
    import shutil

    shutil.rmtree(external)
    assert (tmp_path / "data/profiles/firefox/UID_999/prefs.js").is_file()
