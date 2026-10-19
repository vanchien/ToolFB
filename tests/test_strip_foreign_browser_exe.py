"""Import xong → không còn browser_exe_path ổ D / Via."""

from __future__ import annotations

from pathlib import Path

from src.utils.account_browser_profile import (
    is_foreign_browser_exe_path,
    strip_foreign_browser_exe_path,
)
from src.utils.paths import project_root


def test_via_d_drive_exe_is_foreign() -> None:
    via = r"D:\face bokk\via us reels 2\558700732\App\Firefox\firefox.exe"
    assert is_foreign_browser_exe_path(via)
    assert strip_foreign_browser_exe_path(via) == ""


def test_chrome_via_exe_is_foreign() -> None:
    via = r"D:\face bokk\via us\100003691058260\App\Chrome-bin\chrome.exe"
    assert is_foreign_browser_exe_path(via)
    assert strip_foreign_browser_exe_path(via) == ""


def test_empty_and_inside_project_kept() -> None:
    assert strip_foreign_browser_exe_path("") == ""
    root = project_root()
    inside = root / "browsers" / "firefox" / "firefox.exe"
    # Chưa cần file tồn tại — chỉ kiểm tra path trong project.
    assert not is_foreign_browser_exe_path(str(inside))
    assert strip_foreign_browser_exe_path(str(inside)) == str(inside)
    rel = "data/profiles/firefox/UID_1"
    assert not is_foreign_browser_exe_path(rel)


def test_normalize_account_drops_d_exe(tmp_path: Path, monkeypatch) -> None:
    from src.utils import db_manager as dm

    accounts = tmp_path / "accounts.json"
    accounts.write_text("[]\n", encoding="utf-8")
    monkeypatch.setattr(dm, "project_root", lambda: tmp_path)
    # constructor: đường dẫn file qua positional / thuộc tính nội bộ.
    mgr = dm.AccountsDatabaseManager(str(accounts))
    normalized = mgr._normalize_account_dict(
        {
            "id": "UID_1",
            "name": "Tài khoản mới",
            "browser_type": "firefox",
            "portable_path": "data/profiles/firefox/UID_1",
            "cookie_path": "data/cookies/UID_1.json",
            "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
            "use_proxy": False,
            "browser_exe_path": r"D:\face bokk\via\1\App\Firefox\firefox.exe",
        }
    )
    assert normalized["browser_exe_path"] == ""
    assert normalized["portable_path"] == "data/profiles/firefox/UID_1"
