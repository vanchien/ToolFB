"""Kiểm tra cấu hình auto git pull khi khởi động."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.services.app_updater import (
    AutoGitPullSettings,
    _load_auto_git_pull_settings,
    is_auto_git_pull_enabled,
    maybe_auto_git_pull_on_startup,
)


def test_auto_pull_enabled_by_default_without_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TOOLFB_AUTO_GIT_PULL", raising=False)
    assert is_auto_git_pull_enabled(tmp_path) is True


def test_auto_pull_disabled_by_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TOOLFB_AUTO_GIT_PULL", "0")
    assert is_auto_git_pull_enabled(tmp_path) is False


def test_load_settings_from_json(tmp_path: Path) -> None:
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "auto_update.json").write_text(
        json.dumps(
            {
                "git_pull_on_startup": False,
                "min_interval_minutes": 5,
                "pip_install_after_pull": False,
            }
        ),
        encoding="utf-8",
    )
    s = _load_auto_git_pull_settings(tmp_path)
    assert s == AutoGitPullSettings(
        git_pull_on_startup=False,
        min_interval_minutes=5,
        pip_install_after_pull=False,
    )


def test_maybe_auto_pull_skips_non_git(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TOOLFB_AUTO_GIT_PULL", raising=False)
    out = maybe_auto_git_pull_on_startup(tmp_path)
    assert out.skipped_reason == "not_git_clone"
    assert out.pulled is False
