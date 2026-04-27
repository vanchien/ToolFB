"""Tìm .exe trong thư mục profile (cookie capture / portable)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.utils.browser_exe_discover import find_browser_exe_in_directory


def test_find_browser_exe_empty_dir(tmp_path: Path) -> None:
    d = tmp_path / "prof"
    d.mkdir()
    assert find_browser_exe_in_directory(d) == ""


def test_find_firefox_exe_in_root(tmp_path: Path) -> None:
    d = tmp_path / "pf"
    d.mkdir()
    exe = d / "firefox.exe"
    exe.write_bytes(b"")
    out = find_browser_exe_in_directory(d)
    assert Path(out).resolve() == exe.resolve()


def test_find_chrome_in_nested_chrome_win(tmp_path: Path) -> None:
    d = tmp_path / "p2"
    nested = d / "sub" / "chrome-win"
    nested.mkdir(parents=True)
    exe = nested / "chrome.exe"
    exe.write_bytes(b"")
    out = find_browser_exe_in_directory(d)
    assert Path(out).resolve() == exe.resolve()
