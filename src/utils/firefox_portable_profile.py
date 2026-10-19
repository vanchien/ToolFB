"""
Làm profile Firefox trong ``data/profiles`` dùng được khi copy ra ngoài ToolFB.

- Ghi ``Mo_Firefox.bat``: double-click mở Firefox với đúng thư mục profile.
- Tuỳ chọn xuất kèm bản copy ``firefox.exe`` (Playwright / hệ thống) → thật sự portable.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

from loguru import logger

from src.utils.paths import project_root

LAUNCHER_BAT_NAME = "Mo_Firefox.bat"
LAUNCHER_README_NAME = "HUONG_DAN_MO_FIREFOX.txt"
BROWSER_SUBDIR = "FirefoxApp"

_LAUNCHER_BAT = r"""@echo off
chcp 65001 >nul
setlocal EnableExtensions
cd /d "%~dp0"
set "PROFILE=%~dp0"
if "%PROFILE:~-1%"=="\" set "PROFILE=%PROFILE:~0,-1%"

REM Go lock profile (Firefox dang mo / thoat bat thuong)
if exist "%PROFILE%\parent.lock" del /f /q "%PROFILE%\parent.lock" >nul 2>&1
if exist "%PROFILE%\lock" del /f /q "%PROFILE%\lock" >nul 2>&1
if exist "%PROFILE%\.parentlock" del /f /q "%PROFILE%\.parentlock" >nul 2>&1

REM KHONG dung FirefoxPortable.exe — wrapper PortableApps chan multi-instance
REM ("Another instance of Firefox is already running") khi ToolFB/Firefox khac dang mo.
REM Dung firefox.exe + -no-remote -profile de mo song song nhieu profile.

REM 1) firefox.exe nam canh profile / FirefoxApp (xuat kem tu ToolFB)
if exist "%~dp0firefox.exe" (
  start "" "%~dp0firefox.exe" -no-remote -profile "%PROFILE%"
  exit /b 0
)
if exist "%~dp0FirefoxApp\firefox.exe" (
  start "" "%~dp0FirefoxApp\firefox.exe" -no-remote -profile "%PROFILE%"
  exit /b 0
)
if exist "%~dp0browser\firefox.exe" (
  start "" "%~dp0browser\firefox.exe" -no-remote -profile "%PROFILE%"
  exit /b 0
)
if exist "%~dp0Firefox\firefox.exe" (
  start "" "%~dp0Firefox\firefox.exe" -no-remote -profile "%PROFILE%"
  exit /b 0
)

REM 2) Firefox cai he thong
if exist "%ProgramFiles%\Mozilla Firefox\firefox.exe" (
  start "" "%ProgramFiles%\Mozilla Firefox\firefox.exe" -no-remote -profile "%PROFILE%"
  exit /b 0
)
if exist "%ProgramFiles(x86)%\Mozilla Firefox\firefox.exe" (
  start "" "%ProgramFiles(x86)%\Mozilla Firefox\firefox.exe" -no-remote -profile "%PROFILE%"
  exit /b 0
)

REM 3) Playwright cache / bundle ToolFB
if defined LOCALAPPDATA (
  for /d %%D in ("%LOCALAPPDATA%\ms-playwright\firefox-*") do (
    if exist "%%~D\firefox\firefox.exe" (
      start "" "%%~D\firefox\firefox.exe" -no-remote -profile "%PROFILE%"
      exit /b 0
    )
  )
)

where firefox.exe >nul 2>&1
if %ERRORLEVEL%==0 (
  start "" firefox.exe -no-remote -profile "%PROFILE%"
  exit /b 0
)

echo Khong tim thay firefox.exe.
echo - Trong ToolFB: «Xuất Firefox portable» + chon kem FirefoxApp
echo - Hoac cai Mozilla Firefox
echo - KHONG double-click FirefoxPortable.exe khi ToolFB dang mo profile
pause
exit /b 1
"""

_README_VI = """HƯỚNG DẪN MỞ PROFILE FIREFOX (copy ra ngoài ToolFB)

1) Double-click: Mo_Firefox.bat
   → Mở firefox.exe với -no-remote -profile (cho phép nhiều cửa sổ).

2) Nếu gặp hộp thoại:
   «Another instance of Firefox is already running»
   → Bạn đang mở FirefoxPortable.exe (wrapper PortableApps).
   Cách xử lý:
   - Đóng hết Firefox / trong ToolFB bấm «Đóng profile»
   - Hoặc chỉ dùng Mo_Firefox.bat (không dùng FirefoxPortable.exe)
   - Trong ToolFB: «Xuất Firefox portable» kèm FirefoxApp

3) Không kéo thả thư mục vào Firefox hệ thống — dễ lẫn profile.

4) Bản xuất kèm FirefoxApp độc lập — có thể xóa thư mục nguồn.
"""


def resolve_firefox_executable(*, prefer: str | Path | None = None) -> Path | None:
    """
    Tìm ``firefox.exe``: prefer → cạnh project → Playwright → Program Files → PATH.
    """
    cands: list[Path] = []
    if prefer:
        p = Path(prefer)
        if p.is_file():
            return p.resolve()
        if p.is_dir():
            for name in ("firefox.exe", "Firefox/firefox.exe", "browser/firefox.exe"):
                hit = p / name
                if hit.is_file():
                    return hit.resolve()

    root = project_root()
    for br in (
        root / "_internal" / "ms-playwright",
        root / "dist" / "ToolFB_GUI" / "_internal" / "ms-playwright",
    ):
        if br.is_dir():
            for d in sorted(br.glob("firefox-*"), reverse=True):
                cands.append(d / "firefox" / "firefox.exe")

    local = os.environ.get("LOCALAPPDATA", "").strip()
    if local:
        ms = Path(local) / "ms-playwright"
        if ms.is_dir():
            for d in sorted(ms.glob("firefox-*"), reverse=True):
                cands.append(d / "firefox" / "firefox.exe")

    for base in (
        os.environ.get("ProgramFiles", ""),
        os.environ.get("ProgramFiles(x86)", ""),
    ):
        if base:
            cands.append(Path(base) / "Mozilla Firefox" / "firefox.exe")

    for c in cands:
        try:
            if c.is_file():
                return c.resolve()
        except OSError:
            continue

    which = shutil.which("firefox.exe") or shutil.which("firefox")
    if which:
        wp = Path(which)
        if wp.is_file():
            return wp.resolve()
    return None


def optimize_firefox_profile_for_standalone(profile_dir: Path) -> None:
    """
    Dọn khóa / session crash để profile mở bằng Firefox ngoài Playwright không bị kẹt.
    Không xóa cookie / places / storage.
    """
    if not profile_dir.is_dir():
        return
    for name in (
        "parent.lock",
        "lock",
        ".parentlock",
        "compatibility.ini",
        "sessionstore.jsonlz4",
        "sessionstore.js",
        "sessionCheckpoints.json",
    ):
        target = profile_dir / name
        if target.is_file() or target.is_symlink():
            try:
                target.unlink()
            except OSError:
                pass
    # user.js nhẹ: không resume session crash; homepage FB.
    user_js = profile_dir / "user.js"
    snippet = (
        'user_pref("browser.sessionstore.resume_from_crash", false);\n'
        'user_pref("browser.startup.homepage", "https://www.facebook.com/");\n'
        'user_pref("browser.startup.page", 1);\n'
    )
    try:
        existing = user_js.read_text(encoding="utf-8", errors="replace") if user_js.is_file() else ""
        if "browser.startup.homepage" not in existing:
            user_js.write_text(existing + ("\n" if existing and not existing.endswith("\n") else "") + snippet, encoding="utf-8")
    except OSError as exc:
        logger.debug("Không ghi user.js portable: {}", exc)


def ensure_firefox_portable_launcher(profile_dir: Path | str) -> Path:
    """
    Ghi ``Mo_Firefox.bat`` + hướng dẫn vào thư mục profile.

    Returns:
        Path tới file ``.bat``.
    """
    pdir = Path(profile_dir)
    if not pdir.is_dir():
        raise ValueError(f"Thư mục profile không tồn tại: {pdir}")
    optimize_firefox_profile_for_standalone(pdir)
    bat = pdir / LAUNCHER_BAT_NAME
    bat.write_text(_LAUNCHER_BAT, encoding="utf-8", newline="\r\n")
    readme = pdir / LAUNCHER_README_NAME
    try:
        readme.write_text(_README_VI, encoding="utf-8")
    except OSError:
        pass
    logger.debug("Đã ghi launcher Firefox portable: {}", bat)
    return bat


def _copy_firefox_app_tree(firefox_exe: Path, dest_app_dir: Path) -> Path:
    """
    Copy cả thư mục chứa ``firefox.exe`` (cần DLL/.ini cạnh exe) vào ``dest_app_dir``.
    """
    src_dir = firefox_exe.resolve().parent
    if dest_app_dir.exists():
        shutil.rmtree(dest_app_dir, ignore_errors=True)
    dest_app_dir.parent.mkdir(parents=True, exist_ok=True)
    # Bỏ cache nặng nếu có trong tree browser (hiếm).
    def _ignore(directory: str, names: list[str]) -> set[str]:
        skip: set[str] = set()
        for n in names:
            low = n.casefold()
            if low in {"cache", "cache2", "gpu-cache", "shader-cache"}:
                skip.add(n)
        return skip

    shutil.copytree(src_dir, dest_app_dir, ignore=_ignore)
    out = dest_app_dir / "firefox.exe"
    if not out.is_file():
        # Một số bản đặt tên khác
        for hit in dest_app_dir.glob("*.exe"):
            if hit.name.lower().startswith("firefox"):
                return hit
        raise FileNotFoundError(f"Copy Firefox xong nhưng không thấy firefox.exe trong {dest_app_dir}")
    return out


_EXPORT_SKIP_DIRS = frozenset(
    {
        "cache2",
        "startupcache",
        "thumbnails",
        "safebrowsing",
        "crashes",
        "minidumps",
        "datareporting",
        "saved-telemetry-pings",
        "shader-cache",
        "gpu-cache",
        "jumplistcache",
        "offlinecache",
        "firefoxapp",
        "__pycache__",
    }
)


def export_firefox_profile_portable(
    source_profile: Path | str,
    dest_dir: Path | str,
    *,
    include_firefox_app: bool = True,
    browser_exe_hint: str | Path | None = None,
) -> dict[str, Any]:
    """
    Xuất một bản profile độc lập ra ``dest_dir`` (copy, tối ưu, launcher; tuỳ chọn kèm Firefox).

    Returns:
        Dict thông tin: dest, bat, firefox_copied, firefox_exe.
    """
    src = Path(source_profile).resolve()
    if not src.is_dir():
        raise ValueError(f"Profile nguồn không tồn tại: {src}")
    dest = Path(dest_dir).resolve()
    if dest.exists() and any(dest.iterdir()):
        # Cho phép ghi đè nội dung profile đã xuất trước
        for child in list(dest.iterdir()):
            if child.name.casefold() == BROWSER_SUBDIR.casefold():
                continue
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                try:
                    child.unlink()
                except OSError:
                    pass
    dest.mkdir(parents=True, exist_ok=True)

    def _ignore(_d: str, names: list[str]) -> set[str]:
        skipped: set[str] = set()
        for name in names:
            low = name.casefold()
            if low in _EXPORT_SKIP_DIRS or low in {"parent.lock", "lock", ".parentlock", LAUNCHER_BAT_NAME.casefold()}:
                skipped.add(name)
            if low == BROWSER_SUBDIR.casefold():
                skipped.add(name)
        return skipped

    # Copy nội dung profile vào dest (không bọc thêm thư mục con).
    for item in src.iterdir():
        if item.name.casefold() in _EXPORT_SKIP_DIRS:
            continue
        if item.name.casefold() == BROWSER_SUBDIR.casefold():
            continue
        target = dest / item.name
        if item.is_dir():
            shutil.copytree(item, target, ignore=_ignore, dirs_exist_ok=True)
        elif item.is_file():
            shutil.copy2(item, target)

    bat = ensure_firefox_portable_launcher(dest)
    ff_copied = False
    ff_exe_out: str | None = None
    if include_firefox_app:
        ff = resolve_firefox_executable(prefer=browser_exe_hint)
        if ff is None:
            logger.warning("Xuất portable: không tìm thấy firefox.exe — chỉ ghi launcher.")
        else:
            app_dir = dest / BROWSER_SUBDIR
            out_exe = _copy_firefox_app_tree(ff, app_dir)
            ff_copied = True
            ff_exe_out = str(out_exe)
            logger.info("Đã kèm Firefox portable → {}", out_exe)

    return {
        "dest": str(dest),
        "bat": str(bat),
        "firefox_copied": ff_copied,
        "firefox_exe": ff_exe_out,
    }
