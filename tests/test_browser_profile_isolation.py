"""Mỗi tài khoản / profile Firefox tách biệt — không dùng chung slot profile."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.automation.browser_factory import (
    BrowserFactory,
    acquire_profile_browser_slot,
    release_profile_browser_slot,
)


def test_profile_slot_blocks_second_account(tmp_path: Path) -> None:
    prof = tmp_path / "firefox_profile_a"
    prof.mkdir()
    lock = acquire_profile_browser_slot(prof, "acc_a")
    try:
        with pytest.raises(RuntimeError, match="đang dùng bởi"):
            acquire_profile_browser_slot(prof, "acc_b")
    finally:
        release_profile_browser_slot(prof, "acc_a", lock)


def test_profile_slot_released_for_other_account(tmp_path: Path) -> None:
    prof = tmp_path / "shared"
    prof.mkdir()
    lock_a = acquire_profile_browser_slot(prof, "acc_a")
    release_profile_browser_slot(prof, "acc_a", lock_a)
    lock_b = acquire_profile_browser_slot(prof, "acc_b")
    release_profile_browser_slot(prof, "acc_b", lock_b)


def test_browser_factory_defaults_owned_playwright(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FB_PLAYWRIGHT_SHARED", raising=False)
    started: list[str] = []

    class _FakePw:
        def stop(self) -> None:
            started.append("stop")

    class _FakeCm:
        def start(self) -> _FakePw:
            started.append("start")
            return _FakePw()

    monkeypatch.setattr("src.automation.browser_factory.sync_playwright", lambda: _FakeCm())
    factory = BrowserFactory(headless=True)
    assert factory._pw_mode == "owned"  # noqa: SLF001
    assert started == ["start"]
    factory.close()
    assert started == ["start", "stop"]
