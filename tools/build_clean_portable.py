from __future__ import annotations

import shutil
from pathlib import Path

from runtime_layout_seed import seed_default_runtime_at


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def build_clean_portable() -> tuple[Path, Path]:
    root = _project_root()
    dist_root = root / "dist"
    out_dir = dist_root / "ToolFB_portable_clean"
    zip_base = dist_root / "ToolFB_portable_clean"

    if out_dir.exists():
        shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    ignore_names = shutil.ignore_patterns(
        ".git",
        ".venv",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "dist",
        "logs",
        "data",
        ".cursor",
        "*.pyc",
        "*.pyo",
    )
    shutil.copytree(root, out_dir, dirs_exist_ok=True, ignore=ignore_names)

    seed_default_runtime_at(out_dir)

    # Thêm launcher click-1 để người dùng chạy GUI không cần gõ lệnh.
    launcher = out_dir / "Start_ToolFB_GUI.bat"
    launcher.write_text(
        "@echo off\r\n"
        "setlocal\r\n"
        "cd /d \"%~dp0\"\r\n"
        "if exist \".venv\\Scripts\\python.exe\" (\r\n"
        "  \".venv\\Scripts\\python.exe\" \"main.py\" --gui\r\n"
        ") else (\r\n"
        "  python \"main.py\" --gui\r\n"
        ")\r\n"
        "endlocal\r\n",
        encoding="utf-8",
    )

    zip_path = Path(shutil.make_archive(str(zip_base), "zip", root_dir=out_dir.parent, base_dir=out_dir.name))
    return out_dir, zip_path


if __name__ == "__main__":
    folder, archive = build_clean_portable()
    print(f"CLEAN_PORTABLE_FOLDER={folder}")
    print(f"CLEAN_PORTABLE_ZIP={archive}")
