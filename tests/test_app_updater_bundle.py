"""Kiểm tra gói cập nhật không chồng _internal cũ/mới."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from src.services.app_updater import _merge_exe_gui_bundle


def test_merge_exe_gui_replaces_internal_not_overlay(tmp_path: Path) -> None:
    """``_internal`` phải thay mới hoàn toàn — không giữ file stale từ bản cũ."""
    project = tmp_path / "app"
    exe_gui = tmp_path / "bundle" / "exe_gui"
    project.mkdir()
    exe_gui.mkdir(parents=True)

    old_internal = project / "_internal"
    old_internal.mkdir()
    (old_internal / "stale_module.pyc").write_bytes(b"old")
    (old_internal / "keep.txt").write_bytes(b"old")

    new_internal = exe_gui / "_internal"
    new_internal.mkdir(parents=True)
    (exe_gui / "ToolFB_GUI.exe").write_bytes(b"exe")
    (new_internal / "keep.txt").write_bytes(b"new")
    (new_internal / "fresh.py").write_bytes(b"new")

    _merge_exe_gui_bundle(exe_gui, project)

    merged = project / "_internal"
    assert (project / "ToolFB_GUI.exe").read_bytes() == b"exe"
    assert not (merged / "stale_module.pyc").exists()
    assert (merged / "keep.txt").read_bytes() == b"new"
    assert (merged / "fresh.py").is_file()


def test_manual_extract_should_replace_exe_gui_folder(tmp_path: Path) -> None:
    """
    Hướng dẫn máy khác: giải nén thủ công phải xóa ``exe_gui`` cũ trước khi copy.

    Test mô phỏng copy đè không xóa _internal → vẫn còn file stale (cảnh báo vận hành).
    """
    target = tmp_path / "exe_gui" / "_internal"
    target.mkdir(parents=True)
    stale = target / "legacy_overlay.pyc"
    stale.write_bytes(b"legacy")

    # Copy đè kiểu "chỉ copy file mới" (không MIR) — file cũ vẫn sót
    new_file = tmp_path / "new" / "_internal" / "app.py"
    new_file.parent.mkdir(parents=True)
    new_file.write_bytes(b"v2")
    shutil.copy2(new_file, target / "app.py")

    assert stale.exists(), "Cảnh báo: extract đè từng file có thể giữ bytecode cũ"
    assert (target / "app.py").read_bytes() == b"v2"
