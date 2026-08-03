"""Kiểm tra bản zip máy mới: version.json, launcher, heal version."""

from __future__ import annotations

import json
from pathlib import Path

from src.services.app_updater import read_local_version


def test_read_local_version_from_version_json(tmp_path: Path) -> None:
    (tmp_path / "version.json").write_text(
        json.dumps({"version": "1.0.92"}), encoding="utf-8"
    )
    assert read_local_version(tmp_path) == "1.0.92"


def test_read_local_version_heals_from_browser_manifest(tmp_path: Path) -> None:
    """Zip thiếu version.json cạnh EXE — heal từ browser_bundle_manifest."""
    internal = tmp_path / "_internal"
    internal.mkdir()
    (internal / "browser_bundle_manifest.json").write_text(
        json.dumps({"app_version": "1.0.93", "browsers": {"chromium": "chromium-1208"}}),
        encoding="utf-8",
    )
    assert read_local_version(tmp_path) == "1.0.93"
    healed = tmp_path / "version.json"
    assert healed.is_file()
    raw = json.loads(healed.read_text(encoding="utf-8"))
    assert raw["version"] == "1.0.93"


def test_read_local_version_fallback_dev(tmp_path: Path) -> None:
    assert read_local_version(tmp_path) == "0.0.0-dev"


def test_build_exe_copies_version_helper_exists() -> None:
    """Smoke: helper copy version tồn tại trong build_exe_gui."""
    root = Path(__file__).resolve().parents[1]
    src = (root / "tools" / "build_exe_gui.py").read_text(encoding="utf-8")
    assert "def _copy_version_json" in src
    assert "def _copy_portable_ffmpeg_if_present" in src
    assert "VERSION_JSON_COPIED" in src
    bundle = (root / "tools" / "build_release_bundle.py").read_text(encoding="utf-8")
    assert "Start_ToolFB.bat" in bundle
    assert "Thiếu {vf}" in bundle or "version.json" in bundle
