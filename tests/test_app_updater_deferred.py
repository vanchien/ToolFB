"""Kiểm tra cập nhật tự động tại chỗ (Windows deferred apply)."""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.services.app_updater import (
    ApplyUpdateResult,
    UpdateManifest,
    _stage_deferred_full_apply_windows,
    apply_update_package,
    update_pending_deferred_apply,
)
from src.utils.app_restart import DEFERRED_GUI_BAT_NAME


def _minimal_release_zip(path: Path) -> None:
    """Zip nhỏ có portable_clean + exe_gui."""
    root = path.parent / "build_src"
    pc = root / "ToolFB_release_bundle" / "portable_clean"
    eg = root / "ToolFB_release_bundle" / "exe_gui"
    pc.mkdir(parents=True)
    eg.mkdir(parents=True)
    (pc / "main.py").write_text("print('ok')\n", encoding="utf-8")
    (pc / "src").mkdir()
    (pc / "src" / "app.py").write_text("# stub\n", encoding="utf-8")
    (pc / "version.json").write_text('{"version": "9.9.9"}\n', encoding="utf-8")
    (eg / "ToolFB_GUI.exe").write_bytes(b"exe")
    (eg / "_internal").mkdir()
    (eg / "_internal" / "mod.py").write_text("x=1\n", encoding="utf-8")
    with zipfile.ZipFile(path, "w") as zf:
        for f in pc.rglob("*"):
            if f.is_file():
                zf.write(f, f.relative_to(root).as_posix())
        for f in eg.rglob("*"):
            if f.is_file():
                zf.write(f, f.relative_to(root).as_posix())


def test_deferred_batch_excludes_internal_and_relaunch_portable(tmp_path: Path) -> None:
  payload = tmp_path / "payload"
  payload.mkdir()
  (payload / "main.py").write_text("x=1\n", encoding="utf-8")
  (payload / "src").mkdir()
  project = tmp_path / "app"
  project.mkdir()
  updates = project / "data" / "updates"
  updates.mkdir(parents=True)
  bat = updates / DEFERRED_GUI_BAT_NAME

  _stage_deferred_full_apply_windows(
      payload_root=payload,
      exe_gui_root=None,
      project_root=project,
      updates_dir=updates,
      version="1.2.3",
      bat_out=bat,
      preserve_on_apply_dirs=("data", "config"),
  )
  text = bat.read_text(encoding="utf-8")
  assert "/XD" in text and "_internal" in text
  assert "/XF ToolFB_GUI.exe" in text
  assert "main.py" in text or "python" in text.lower()


def test_apply_update_windows_returns_deferred(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    if sys.platform != "win32":
        pytest.skip("Windows deferred apply")
    project = tmp_path / "app"
    project.mkdir()
    (project / "data").mkdir()
    zip_path = tmp_path / "bundle.zip"
    _minimal_release_zip(zip_path)
    data = zip_path.read_bytes()

    mf = UpdateManifest(
        version="9.9.9",
        download_url="https://example.com/bundle.zip",
        sha256="",
        notes="",
    )

    class _Resp:
        _offset = 0

        def geturl(self) -> str:
            return mf.download_url

        def read(self, n: int = -1) -> bytes:
            if _Resp._offset >= len(data):
                return b""
            chunk = data[_Resp._offset : _Resp._offset + (n if n > 0 else len(data))]
            _Resp._offset += len(chunk)
            return chunk

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: _Resp())
    monkeypatch.setattr("src.services.app_updater._make_short_extract_root", lambda **k: tmp_path / "ext")

    result = apply_update_package(project_root=project, manifest=mf)
    assert isinstance(result, ApplyUpdateResult)
    assert result.deferred is True
    assert (project / "data" / "updates" / DEFERRED_GUI_BAT_NAME).is_file()
    assert update_pending_deferred_apply(project) is True
