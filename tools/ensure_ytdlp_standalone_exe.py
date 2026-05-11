"""
Tải yt-dlp.exe (Windows) vào tools/yt-dlp/ trước khi đóng gói — thư mục này thường gitignore.

Chạy: ``python tools/ensure_ytdlp_standalone_exe.py`` (từ thư mục gốc ToolFB).
"""

from __future__ import annotations

import shutil
import sys
import urllib.request
from pathlib import Path

YTDLP_WIN_EXE_URL = "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe"


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def ensure_ytdlp_windows_standalone_exe(
    project_root: Path | None = None,
    *,
    force: bool = False,
    min_size_bytes: int = 400_000,
) -> Path | None:
    """Tải yt-dlp.exe nếu thiếu hoặc file quá nhỏ (lỗi tải). Chỉ Windows."""
    if sys.platform != "win32":
        return None
    root = project_root or _project_root()
    out = root / "tools" / "yt-dlp" / "yt-dlp.exe"
    if out.is_file() and not force and out.stat().st_size >= min_size_bytes:
        print(f"YTDLP_EXE_SKIP_EXISTS={out}", file=sys.stderr)
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    req = urllib.request.Request(YTDLP_WIN_EXE_URL, headers={"User-Agent": "ToolFB-build/ensure-ytdlp"})
    try:
        with urllib.request.urlopen(req, timeout=180) as resp, tmp.open("wb") as fh:
            shutil.copyfileobj(resp, fh, length=256 * 1024)
    except Exception as exc:
        print(f"YTDLP_EXE_DOWNLOAD_FAIL={exc}", file=sys.stderr)
        try:
            tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
        except OSError:
            pass
        return None
    if not tmp.is_file() or tmp.stat().st_size < min_size_bytes:
        print("YTDLP_EXE_DOWNLOAD_TOO_SMALL", file=sys.stderr)
        tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
        return None
    tmp.replace(out)
    print(f"YTDLP_EXE_OK={out}", file=sys.stderr)
    return out


def copy_ytdlp_exe_to_exe_gui_dist(project_root: Path, dist_gui_dir: Path) -> Path | None:
    """Sau PyInstaller: chép yt-dlp.exe kèm ToolFB_GUI (project_root() khi chạy frozen = thư mục exe)."""
    src = project_root / "tools" / "yt-dlp" / "yt-dlp.exe"
    if not src.is_file():
        return None
    dest = dist_gui_dir / "tools" / "yt-dlp" / "yt-dlp.exe"
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    print(f"YTDLP_EXE_COPIED_TO_DIST={dest}", file=sys.stderr)
    return dest


if __name__ == "__main__":
    p = ensure_ytdlp_windows_standalone_exe()
    raise SystemExit(0 if p else 1)
