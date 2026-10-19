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
    _pin_release_download_url,
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


def test_apply_replaces_old_code_before_marking_version(tmp_path: Path) -> None:
    """Máy đích còn file bản cũ: mã bị thay, version.json chỉ đổi sau khi chép xong."""
    if sys.platform != "win32":
        pytest.skip("Windows robocopy apply")
    import subprocess

    payload = tmp_path / "payload"
    (payload / "src").mkdir(parents=True)
    (payload / "src" / "app.py").write_text("new\n", encoding="utf-8")
    (payload / "version.json").write_text('{"version": "9.9.9"}\n', encoding="utf-8")
    project = tmp_path / "app"
    (project / "src").mkdir(parents=True)
    (project / "src" / "app.py").write_text("old\n", encoding="utf-8")
    (project / "src" / "stale.py").write_text("stale\n", encoding="utf-8")
    (project / "version.json").write_text('{"version": "1.0.0"}\n', encoding="utf-8")
    (project / "data").mkdir()
    (project / "data" / "keep.txt").write_text("profile\n", encoding="utf-8")
    updates = project / "data" / "updates"
    updates.mkdir()
    bat = updates / DEFERRED_GUI_BAT_NAME
    _stage_deferred_full_apply_windows(
        payload_root=payload,
        exe_gui_root=None,
        project_root=project,
        updates_dir=updates,
        version="9.9.9",
        bat_out=bat,
        preserve_on_apply_dirs=("data", "config"),
    )
    staged_root = next(updates.glob("staged_apply_*"))
    staged = staged_root / "portable_apply"
    subprocess.run(
        [
            "robocopy",
            str(staged),
            str(project),
            "/MIR",
            "/IS",
            "/IT",
            "/R:1",
            "/W:1",
            "/NP",
            "/XF",
            "ToolFB_GUI.exe",
            "version.json",
            "/XD",
            "data",
            "config",
            "_internal",
        ],
        check=False,
    )
    assert (project / "src" / "app.py").read_text(encoding="utf-8") == "new\n"
    assert not (project / "src" / "stale.py").exists()
    assert (project / "data" / "keep.txt").read_text(encoding="utf-8") == "profile\n"
    assert '"1.0.0"' in (project / "version.json").read_text(encoding="utf-8")
    subprocess.run(
        ["cmd", "/c", "copy", "/Y", str(staged / "version.json"), str(project / "version.json")],
        check=True,
    )
    assert '"9.9.9"' in (project / "version.json").read_text(encoding="utf-8")


def test_frozen_startup_removes_shadow_src(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from frozen_shadow import drop_frozen_shadow_source

    shadow = tmp_path / "src"
    shadow.mkdir()
    (shadow / "old_mod.py").write_text("old\n", encoding="utf-8")
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", str(tmp_path / "ToolFB_GUI.exe"))
    drop_frozen_shadow_source()
    assert not shadow.exists()


def test_deferred_bat_runs_on_windows_even_for_source_install(tmp_path: Path) -> None:
    from src.utils.app_restart import should_run_deferred_apply

    missing = tmp_path / "no.bat"
    assert should_run_deferred_apply(missing) is False
    present = tmp_path / "toolfb_deferred_gui_apply.bat"
    present.write_text("@echo off\r\n", encoding="utf-8")
    assert should_run_deferred_apply(present) is (sys.platform == "win32")


def test_disk_full_error_mentions_updates_folder() -> None:
    from src.services.app_updater import _disk_full_error, format_update_error

    err = OSError(28, "No space left on device")
    err.winerror = 112  # type: ignore[attr-defined]
    mapped = _disk_full_error(err, Path("C:/ToolFB/data/updates/staged"))
    assert mapped is not None
    assert "data\\updates" in str(mapped)
    assert _disk_full_error(OSError(13, "denied"), Path("x")) is None
    text = format_update_error(err, target=Path("C:/ToolFB/data/updates"))
    assert "không đủ chỗ" in text.lower()
    assert "No space left" not in text or "ổ đĩa" in text.lower()
    # Errno 28 bọc trong RuntimeError vẫn phải dịch (máy cũ hay hiện raw errno).
    wrapped = RuntimeError("Cập nhật thất bại")
    wrapped.__cause__ = err
    wrapped_text = format_update_error(wrapped, target=Path("C:/ToolFB/data/updates"))
    assert "không đủ chỗ" in wrapped_text.lower()
    assert "[Errno 28]" not in wrapped_text


def test_ensure_disk_space_raises_when_short(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.services import app_updater as au

    monkeypatch.setattr(au, "_free_bytes", lambda _p: 50 * 1024 * 1024)
    with pytest.raises(RuntimeError, match="không đủ chỗ"):
        au._ensure_disk_space(tmp_path, 900 * 1024 * 1024, label="test")


def test_extract_root_prefers_updates_dir_same_volume(tmp_path: Path) -> None:
    """Giải nén ưu tiên data/updates — tránh đổ đầy ổ C: khi app nằm ổ khác."""
    from src.services.app_updater import _make_short_extract_root

    updates = tmp_path / "data" / "updates"
    updates.mkdir(parents=True)
    root = _make_short_extract_root(project_root=tmp_path, updates_dir=updates)
    assert root.is_dir()
    assert updates in root.parents or root.parent == updates
    shutil_rm = __import__("shutil")
    shutil_rm.rmtree(root, ignore_errors=True)


def test_purge_stale_removes_old_zip(tmp_path: Path) -> None:
    from src.services.app_updater import _purge_stale_update_files

    project = tmp_path / "app"
    updates = project / "data" / "updates"
    updates.mkdir(parents=True)
    (updates / "update_1.0.0.zip").write_bytes(b"PK\x03\x04junk")
    old_stage = updates / "staged_apply_abc"
    old_stage.mkdir()
    (old_stage / "x.txt").write_text("x", encoding="utf-8")
    extract_tmp = updates / "eabcdef123456"
    extract_tmp.mkdir()
    (extract_tmp / "a.txt").write_text("a", encoding="utf-8")
    _purge_stale_update_files(project, updates)
    assert not (updates / "update_1.0.0.zip").exists()
    assert not old_stage.exists()
    assert not extract_tmp.exists()


def test_download_url_is_pinned_to_the_manifest_version() -> None:
    floating = "https://github.com/vanchien/ToolFB/releases/latest/download/ToolFB_release_bundle.zip"
    pinned = _pin_release_download_url(floating, "1.0.105")
    assert pinned == "https://github.com/vanchien/ToolFB/releases/download/v1.0.105/ToolFB_release_bundle.zip"
    import sys
    from pathlib import Path as _Path

    tools = _Path(__file__).resolve().parents[1] / "tools"
    sys.path.insert(0, str(tools))
    from github_release_urls import pin_release_download_url

    assert pin_release_download_url(floating, "1.0.105") == pinned
    assert _pin_release_download_url("https://example.com/a.zip", "1.0.105") == "https://example.com/a.zip"


def test_frozen_apply_replaces_internal_and_drops_shadow_src(tmp_path: Path) -> None:
    """Bản exe: đổi cả _internal và xóa src nằm cạnh exe để không còn nạp file cũ."""
    if sys.platform != "win32":
        pytest.skip("Windows move/ren apply")
    import subprocess

    payload = tmp_path / "payload"
    (payload / "src").mkdir(parents=True)
    (payload / "main.py").write_text("print(1)\n", encoding="utf-8")
    (payload / "version.json").write_text('{"version": "9.9.9"}\n', encoding="utf-8")
    exe = tmp_path / "exe_gui"
    (exe / "_internal").mkdir(parents=True)
    (exe / "ToolFB_GUI.exe").write_bytes(b"new-exe")
    (exe / "_internal" / "fresh.py").write_text("new\n", encoding="utf-8")
    project = tmp_path / "app"
    (project / "_internal").mkdir(parents=True)
    (project / "_internal" / "stale.py").write_text("old\n", encoding="utf-8")
    (project / "src").mkdir()
    (project / "src" / "old_mod.py").write_text("shadow\n", encoding="utf-8")
    (project / "data").mkdir()
    (project / "data" / "keep.txt").write_text("profile\n", encoding="utf-8")
    (project / "version.json").write_text('{"version": "1.0.0"}\n', encoding="utf-8")
    updates = project / "data" / "updates"
    updates.mkdir(parents=True)
    bat = updates / DEFERRED_GUI_BAT_NAME
    _stage_deferred_full_apply_windows(
        payload_root=payload,
        exe_gui_root=exe,
        project_root=project,
        updates_dir=updates,
        version="9.9.9",
        bat_out=bat,
        preserve_on_apply_dirs=("data", "config"),
        frozen_install=True,
    )
    text = bat.read_text(encoding="utf-8")
    assert 'robocopy "' not in text.split("portable_apply", 1)[0] or "/MIR" not in text.split("version.json", 1)[0]
    assert "/MIR" not in text
    assert "_internal.old" in text
    assert "toolfb_release.txt" in text
    assert f'rd /s /q "{project.resolve()}\\src"' in text
    staged = next(updates.glob("staged_apply_*"))
    assert not exe.exists()
    assert (staged / "exe_gui" / "ToolFB_GUI.exe").is_file()
    assert (staged / "portable_apply" / "version.json").is_file()
    assert not (staged / "portable_apply" / "src").exists()
    assert (staged / "exe_gui" / "_internal" / "toolfb_release.txt").read_text(encoding="ascii").strip() == "9.9.9"
    # Chạy đúng các lệnh đổi thư mục, không relaunch app.
    from src.services.app_updater import _internal_replace_commands

    swap = updates / "swap_only.bat"
    swap.write_text(
        "\r\n".join(
            [
                "@echo off",
                "setlocal",
                *_internal_replace_commands(str(staged.resolve()), str(project.resolve()), "9.9.9"),
                "exit /b 0",
                ":L",
                "exit /b 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    ran = subprocess.run(["cmd", "/c", str(swap)], check=False)
    assert ran.returncode == 0
    assert (project / "_internal" / "fresh.py").read_text(encoding="utf-8") == "new\n"
    assert not (project / "_internal" / "stale.py").exists()
    assert not (project / "_internal.old").exists()
    assert (project / "data" / "keep.txt").read_text(encoding="utf-8") == "profile\n"
    subprocess.run(["cmd", "/c", "rd", "/s", "/q", str(project / "src")], check=False)
    assert not (project / "src").exists()


def test_source_install_still_mirrors_code_tree(tmp_path: Path) -> None:
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
        frozen_install=False,
    )
    text = bat.read_text(encoding="utf-8")
    assert "/MIR /IS /IT" in text
    assert "__pycache__" in text
    assert "/XF ToolFB_GUI.exe version.json" in text
    assert text.index("/XF ToolFB_GUI.exe version.json") < text.index("portable_apply\\version.json")


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
  assert "/MIR /IS /IT" in text
  assert "/XD" in text and "_internal" in text
  assert "/XF ToolFB_GUI.exe version.json" in text
  assert text.index("/XF ToolFB_GUI.exe version.json") < text.index("portable_apply\\version.json")
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
