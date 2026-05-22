"""Tests khóa manifest trình duyệt Playwright."""

from __future__ import annotations

import json
from pathlib import Path

from src.utils.playwright_browser_lock import (
    build_browser_manifest,
    bundled_browsers_dir_near_exe,
    scan_browser_folders,
    validate_browser_bundle,
    write_browser_manifest_file,
)


def test_scan_and_validate_browser_folders(tmp_path: Path) -> None:
    root = tmp_path / "ms-playwright"
    (root / "chromium-1208").mkdir(parents=True)
    (root / "firefox-1509").mkdir(parents=True)
    scanned = scan_browser_folders(root)
    assert scanned["chromium"] == "chromium-1208"
    assert scanned["firefox"] == "firefox-1509"
    app_v = "9.9.9-test"
    (tmp_path / "version.json").write_text(
        json.dumps({"version": app_v}) + "\n", encoding="utf-8"
    )
    mf = build_browser_manifest(app_version=app_v, browsers_root=root)
    manifest_path = tmp_path / "browser_bundle_manifest.json"
    write_browser_manifest_file(manifest_path, mf)
    loaded = json.loads(manifest_path.read_text(encoding="utf-8"))
    errs = validate_browser_bundle(
        project_root=tmp_path,
        manifest=loaded,
        browsers_path=root,
    )
    assert errs == []
    (root / "chromium-9999").mkdir()
    import shutil

    shutil.rmtree(root / "chromium-1208")
    errs2 = validate_browser_bundle(
        project_root=tmp_path,
        manifest=loaded,
        browsers_path=root,
    )
    assert any("chromium" in e.lower() for e in errs2)


def test_bundled_browsers_dir_near_exe(tmp_path: Path) -> None:
    internal = tmp_path / "_internal" / "ms-playwright" / "firefox-1509"
    internal.mkdir(parents=True)
    found = bundled_browsers_dir_near_exe(tmp_path)
    assert found is not None
    assert found.name == "ms-playwright"
