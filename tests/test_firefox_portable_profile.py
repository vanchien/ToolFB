"""Launcher + xuất profile Firefox dùng được khi copy ra ngoài."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.utils.firefox_portable_profile import (
    LAUNCHER_BAT_NAME,
    ensure_firefox_portable_launcher,
    export_firefox_profile_portable,
    optimize_firefox_profile_for_standalone,
)


def _fake_profile(d: Path) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / "prefs.js").write_text("//x\n", encoding="utf-8")
    (d / "cookies.sqlite").write_bytes(b"SQLite format 3\x00" + b"\x00" * 80)
    (d / "parent.lock").write_text("lock", encoding="utf-8")
    (d / "cache2").mkdir()
    (d / "cache2" / "a").write_text("c", encoding="utf-8")


def test_ensure_launcher_writes_bat(tmp_path: Path) -> None:
    prof = tmp_path / "acc_x"
    _fake_profile(prof)
    bat = ensure_firefox_portable_launcher(prof)
    assert bat.is_file()
    assert bat.name == LAUNCHER_BAT_NAME
    text = bat.read_text(encoding="utf-8")
    assert "-no-remote" in text
    assert "-profile" in text
    assert not (prof / "parent.lock").exists()
    assert (prof / "user.js").is_file()


def test_export_without_browser_skips_cache(tmp_path: Path) -> None:
    src = tmp_path / "src_prof"
    _fake_profile(src)
    dest = tmp_path / "out" / "acc_x"
    info = export_firefox_profile_portable(src, dest, include_firefox_app=False)
    assert Path(info["dest"]).is_dir()
    assert Path(info["bat"]).is_file()
    assert not info["firefox_copied"]
    assert (dest / "prefs.js").is_file()
    assert (dest / "cookies.sqlite").is_file()
    assert not (dest / "cache2").exists()
    assert not (dest / "parent.lock").exists()


def test_export_with_fake_firefox_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = tmp_path / "src_prof"
    _fake_profile(src)
    fake_ff_dir = tmp_path / "ff_install"
    fake_ff_dir.mkdir()
    fake_exe = fake_ff_dir / "firefox.exe"
    fake_exe.write_bytes(b"MZ")
    (fake_ff_dir / "mozglue.dll").write_bytes(b"dll")
    monkeypatch.setattr(
        "src.utils.firefox_portable_profile.resolve_firefox_executable",
        lambda prefer=None: fake_exe,
    )
    dest = tmp_path / "portable_out"
    info = export_firefox_profile_portable(src, dest, include_firefox_app=True)
    assert info["firefox_copied"] is True
    assert Path(info["firefox_exe"]).is_file()
    assert (dest / "FirefoxApp" / "firefox.exe").is_file()
    assert (dest / "FirefoxApp" / "mozglue.dll").is_file()
    bat = Path(info["bat"]).read_text(encoding="utf-8")
    assert "FirefoxApp\\firefox.exe" in bat or "FirefoxApp" in bat


def test_optimize_idempotent(tmp_path: Path) -> None:
    prof = tmp_path / "p"
    _fake_profile(prof)
    optimize_firefox_profile_for_standalone(prof)
    optimize_firefox_profile_for_standalone(prof)
    assert (prof / "user.js").is_file()
