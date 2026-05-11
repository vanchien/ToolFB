"""
Đảm bảo ffmpeg/ffprobe portable tồn tại ở tools/ffmpeg/bin trước khi đóng gói.

Windows:
- Mặc định tải từ gyan.dev essentials zip.
- Có thể override bằng env FFMPEG_PORTABLE_URL.
"""

from __future__ import annotations

import os
import shutil
import sys
import urllib.request
import zipfile
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def ensure_ffmpeg_portable(*, project_root: Path | None = None) -> bool:
    root = project_root or _project_root()
    ffmpeg_exe = "ffmpeg.exe" if os.name == "nt" else "ffmpeg"
    ffprobe_exe = "ffprobe.exe" if os.name == "nt" else "ffprobe"
    bin_dir = root / "tools" / "ffmpeg" / "bin"
    ffmpeg_path = bin_dir / ffmpeg_exe
    ffprobe_path = bin_dir / ffprobe_exe
    if ffmpeg_path.is_file() and ffprobe_path.is_file():
        print(f"FFMPEG_PORTABLE_SKIP_EXISTS={bin_dir}", file=sys.stderr)
        return True
    if os.name != "nt":
        return False

    ffmpeg_root = root / "tools" / "ffmpeg"
    dl_dir = ffmpeg_root / "downloads"
    ex_dir = ffmpeg_root / "extracted"
    dl_dir.mkdir(parents=True, exist_ok=True)
    ex_dir.mkdir(parents=True, exist_ok=True)
    bin_dir.mkdir(parents=True, exist_ok=True)

    url = os.environ.get(
        "FFMPEG_PORTABLE_URL",
        "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip",
    ).strip()
    zip_path = dl_dir / "ffmpeg-release-essentials.zip"
    try:
        urllib.request.urlretrieve(url, str(zip_path))
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = zf.namelist()
            ff_member = next((m for m in members if m.lower().endswith("/bin/" + ffmpeg_exe.lower())), "")
            fp_member = next((m for m in members if m.lower().endswith("/bin/" + ffprobe_exe.lower())), "")
            if ff_member and fp_member:
                with zf.open(ff_member) as src, ffmpeg_path.open("wb") as dst:
                    dst.write(src.read())
                with zf.open(fp_member) as src, ffprobe_path.open("wb") as dst:
                    dst.write(src.read())
            else:
                zf.extractall(ex_dir)
                found_ffmpeg = next((p for p in ex_dir.rglob(ffmpeg_exe) if p.is_file()), None)
                found_ffprobe = next((p for p in ex_dir.rglob(ffprobe_exe) if p.is_file()), None)
                if not found_ffmpeg or not found_ffprobe:
                    return False
                shutil.copy2(found_ffmpeg, ffmpeg_path)
                shutil.copy2(found_ffprobe, ffprobe_path)
        keep_cache = os.environ.get("FFMPEG_KEEP_INSTALL_CACHE", "0").strip().lower() in {"1", "true", "yes", "on"}
        if not keep_cache:
            shutil.rmtree(ex_dir, ignore_errors=True)
            try:
                if zip_path.is_file():
                    zip_path.unlink()
            except OSError:
                pass
        ok = ffmpeg_path.is_file() and ffprobe_path.is_file()
        if ok:
            print(f"FFMPEG_PORTABLE_OK={bin_dir}", file=sys.stderr)
        return ok
    except Exception as exc:  # noqa: BLE001
        print(f"FFMPEG_PORTABLE_FAIL={exc}", file=sys.stderr)
        return False


if __name__ == "__main__":
    raise SystemExit(0 if ensure_ffmpeg_portable() else 1)
