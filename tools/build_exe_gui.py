from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from runtime_layout_seed import seed_default_runtime_at


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_playwright_browser_cache() -> Path:
    raw = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "").strip()
    if raw:
        return Path(raw)
    if os.name == "nt":
        lad = os.environ.get("LOCALAPPDATA", "").strip()
        if lad:
            return Path(lad) / "ms-playwright"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "ms-playwright"
    return Path.home() / ".cache" / "ms-playwright"


def _bundle_playwright_browsers(*, project_root: Path, dist_dir: Path) -> None:
    """
    Sao chép cache ``ms-playwright`` (chromium/firefox/webkit) vào ``dist_dir/_internal`` để EXE chạy máy sạch.
    """
    skip = os.environ.get("TOOLFB_SKIP_BROWSER_BUNDLE", "").strip().lower()
    if skip in {"1", "true", "yes", "on"}:
        print("TOOLFB_SKIP_BROWSER_BUNDLE — bỏ qua playwright install + copy trình duyệt.", file=sys.stderr)
        return
    subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium", "firefox", "webkit"],
        cwd=str(project_root),
        check=True,
    )
    src = _default_playwright_browser_cache()
    if not src.is_dir() or not any(src.iterdir()):
        raise RuntimeError(f"Không thấy thư mục trình duyệt Playwright sau install: {src}")
    dest = dist_dir / "_internal" / "ms-playwright"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        shutil.rmtree(dest, ignore_errors=True)
    shutil.copytree(src, dest)
    print(f"PLAYWRIGHT_BROWSERS_BUNDLED={dest}", file=sys.stderr)


def _copy_portable_ffmpeg_if_present(*, project_root: Path, dist_dir: Path) -> None:
    ffmpeg_root = project_root / "tools" / "ffmpeg"
    bin_dir = ffmpeg_root / "bin"
    if not (bin_dir / "ffmpeg.exe").is_file() and not (bin_dir / "ffmpeg").is_file():
        return
    target = dist_dir / "tools" / "ffmpeg"
    if target.exists():
        shutil.rmtree(target, ignore_errors=True)
    shutil.copytree(ffmpeg_root, target)


def _copy_config_template_files(*, project_root: Path, dist_dir: Path) -> None:
    example = project_root / "config" / "app_secrets.example.json"
    if example.is_file():
        dest = dist_dir / "config" / "app_secrets.example.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(example, dest)


def _pyinstaller_cmd() -> list[str]:
    root = _project_root()
    venv_pyinstaller = root / ".venv" / "Scripts" / "pyinstaller.exe"
    if venv_pyinstaller.is_file():
        return [str(venv_pyinstaller)]
    if shutil.which("pyinstaller"):
        return ["pyinstaller"]
    return []


def build_exe() -> Path:
    root = _project_root()
    cmd = _pyinstaller_cmd()
    if not cmd:
        raise RuntimeError("Không tìm thấy PyInstaller. Cài bằng: pip install pyinstaller")

    dist = root / "dist" / "ToolFB_GUI"
    build = root / "build" / "ToolFB_GUI"
    spec = root / "build" / "ToolFB_GUI.spec"
    if dist.exists():
        shutil.rmtree(dist, ignore_errors=True)
    if build.exists():
        shutil.rmtree(build, ignore_errors=True)
    if spec.exists():
        spec.unlink()

    # playwright_stealth loads *.js at import time from Path(__file__).parent / "js".
    # PyInstaller does not ship those data files unless we collect them (fixes portable EXE on other PCs).
    py_cmd = cmd + [
        "--noconfirm",
        "--onedir",
        "--windowed",
        "--name",
        "ToolFB_GUI",
        "--specpath",
        str(root / "build"),
        "--collect-all",
        "playwright_stealth",
        str(root / "main.py"),
    ]
    subprocess.run(py_cmd, cwd=str(root), check=True)
    exe_path = dist / "ToolFB_GUI.exe"
    if not exe_path.is_file():
        raise RuntimeError("Build xong nhưng không thấy ToolFB_GUI.exe")

    seed_default_runtime_at(dist)
    _copy_config_template_files(project_root=root, dist_dir=dist)
    _copy_portable_ffmpeg_if_present(project_root=root, dist_dir=dist)
    _bundle_playwright_browsers(project_root=root, dist_dir=dist)

    # Launcher click-1 cho bản portable EXE.
    launcher = dist / "Start_ToolFB_GUI.bat"
    launcher.write_text('@echo off\r\ncd /d "%~dp0"\r\nstart "" "ToolFB_GUI.exe" --gui\r\n', encoding="utf-8")
    return exe_path


if __name__ == "__main__":
    try:
        out = build_exe()
    except Exception as exc:  # noqa: BLE001
        print(f"BUILD_EXE_ERROR={exc}", file=sys.stderr)
        raise
    print(f"BUILD_EXE_OK={out}")
