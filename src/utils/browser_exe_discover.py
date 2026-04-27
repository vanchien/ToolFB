"""
Tìm file ``.exe`` trình duyệt trong thư mục profile portable (Chrome / Firefox / Edge…).

Dùng cho «Lấy cookie (Playwright)» khi ``browser_exe_path`` trống và form quét thư mục.
"""

from __future__ import annotations

from pathlib import Path

_BROWSER_EXE_BASENAMES: tuple[str, ...] = (
    "firefox.exe",
    "chrome.exe",
    "chromium.exe",
    "msedge.exe",
    "brave.exe",
    "vivaldi.exe",
)


def find_browser_exe_in_directory(root: Path | str, *, max_subdirs: int = 120) -> str:
    """
    Tìm ``.exe`` trong ``root`` (gốc + thư mục con + vài thư mục lồng quen).

    Firefox portable thường đặt ``firefox.exe`` ngay trong profile — ưu tiên trước Chrome.

    Args:
        root: Thư mục profile portable (tuyệt đối hoặc tương đối).
        max_subdirs: Giới hạn số thư mục con duyệt ở cấp một.

    Returns:
        Đường dẫn tuyệt đối tới ``.exe`` hoặc chuỗi rỗng.
    """
    root_p = Path(root).expanduser()
    if not root_p.is_absolute():
        from src.utils.paths import project_root

        root_p = (project_root() / root_p).resolve()
    else:
        root_p = root_p.resolve()
    if not root_p.is_dir():
        return ""
    for name in _BROWSER_EXE_BASENAMES:
        hit = root_p / name
        if hit.is_file():
            return str(hit.resolve())
    subdirs = sorted([p for p in root_p.iterdir() if p.is_dir()], key=lambda p: p.name.lower())[:max_subdirs]
    for sub in subdirs:
        for name in _BROWSER_EXE_BASENAMES:
            hit = sub / name
            if hit.is_file():
                return str(hit.resolve())
        for nest in ("chrome-win", "Chrome-bin", "Chromium", "application", "browser", "App"):
            nested = sub / nest
            if not nested.is_dir():
                continue
            for name in _BROWSER_EXE_BASENAMES:
                hit = nested / name
                if hit.is_file():
                    return str(hit.resolve())
    return ""
