"""
Đặt vị trí/kích thước cửa sổ Firefox (Windows) sau khi Playwright khởi chạy.

Playwright persistent context không luôn tôn ``-left``/``-top`` trên Windows — dùng Win32
``SetWindowPos`` theo thư mục profile để các ô lưới không chồng lên nhau.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

from loguru import logger


def firefox_outer_window_size(content_width: int, content_height: int) -> tuple[int, int]:
    """
    Kích thước cửa sổ Firefox (chrome + tab) để vùng nội dung ~ ``content_width`` × ``content_height``.

    Playwright ``viewport`` là vùng trang; ``SetWindowPos`` cần kích thước ngoài lớn hơn.
    """
    pad_w = max(0, _env_int("FB_FIREFOX_CHROME_WIDTH_PAD", 16))
    pad_h = max(60, _env_int("FB_FIREFOX_CHROME_HEIGHT_PAD", 96))
    return (
        max(400, int(content_width) + pad_w),
        max(480, int(content_height) + pad_h),
    )


def _env_int(name: str, default: int) -> int:
    import os

    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def reposition_browser_to_grid_slot(
    profile_dir: str | Path,
    slot: Any,
    *,
    timeout_s: float = 18.0,
) -> bool:
    """
    Đặt cửa sổ Firefox theo ``GridWindowSlot`` (x, y, width, height).

    Dùng sau launch Playwright vì Firefox trên Windows hay bỏ qua ``-left``/``-top``.
    """
    try:
        x = int(slot.x)
        y = int(slot.y)
        cw = int(slot.width)
        ch = int(slot.height)
    except (AttributeError, TypeError, ValueError):
        return False
    outer_w, outer_h = firefox_outer_window_size(cw, ch)
    return place_firefox_window_for_profile(
        profile_dir,
        x=x,
        y=y,
        width=outer_w,
        height=outer_h,
        timeout_s=timeout_s,
    )


def place_firefox_window_for_profile(
    profile_dir: str | Path,
    *,
    x: int,
    y: int,
    width: int,
    height: int,
    timeout_s: float = 12.0,
) -> bool:
    """
  Tìm cửa sổ Firefox gắn ``profile_dir`` và đặt ``(x, y, width, height)``.

  Returns:
      True nếu đã đặt ít nhất một HWND.
    """
    if sys.platform != "win32":
        return False
    prof = Path(profile_dir).resolve()
    prof_key = str(prof).lower().replace("/", "\\")
    if not prof_key:
        return False

    deadline = time.monotonic() + max(1.0, float(timeout_s))
    while time.monotonic() < deadline:
        if _try_set_window(prof_key, x=x, y=y, width=width, height=height):
            return True
        time.sleep(0.35)
    logger.debug("Không tìm thấy HWND Firefox cho profile={} trong {}s", prof, timeout_s)
    return False


def _firefox_pids_for_profile(profile_key: str) -> list[int]:
    """Liệt kê PID firefox.exe có command line chứa đường dẫn profile."""
    try:
        import subprocess

        ps = (
            "Get-CimInstance Win32_Process -Filter \"Name='firefox.exe'\" | "
            "Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress"
        )
        raw = subprocess.check_output(
            ["powershell", "-NoProfile", "-Command", ps],
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=8,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("Liệt kê process Firefox: {}", exc)
        return []

    import json

    pids: list[int] = []
    try:
        data = json.loads(raw.strip() or "[]")
    except json.JSONDecodeError:
        return []
    rows = data if isinstance(data, list) else [data]
    for row in rows:
        if not isinstance(row, dict):
            continue
        cmd = str(row.get("CommandLine") or "").lower().replace("/", "\\")
        if profile_key not in cmd:
            continue
        try:
            pids.append(int(row["ProcessId"]))
        except (KeyError, TypeError, ValueError):
            continue
    return pids


def _try_set_window(profile_key: str, *, x: int, y: int, width: int, height: int) -> bool:
    import ctypes
    from ctypes import wintypes

    user32 = ctypes.windll.user32  # type: ignore[attr-defined]
    SWP_SHOWWINDOW = 0x0040
    placed = False
    target_pids = set(_firefox_pids_for_profile(profile_key))

    if not target_pids:
        return False

    @ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    def _enum_cb(hwnd: int, _lparam: int) -> bool:
        nonlocal placed
        if not user32.IsWindowVisible(hwnd):
            return True
        pid = wintypes.DWORD()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        if int(pid.value) not in target_pids:
            return True
        # Bỏ qua cửa sổ phụ rất nhỏ (devtools / popup ẩn).
        rect = wintypes.RECT()
        if not user32.GetWindowRect(hwnd, ctypes.byref(rect)):
            return True
        w = rect.right - rect.left
        h = rect.bottom - rect.top
        if w < 200 or h < 200:
            return True
        user32.SetWindowPos(
            hwnd,
            None,
            int(x),
            int(y),
            max(320, int(width)),
            max(400, int(height)),
            SWP_SHOWWINDOW,
        )
        try:
            user32.SetForegroundWindow(hwnd)
        except Exception:
            pass
        placed = True
        return False  # stop after first main window

    user32.EnumWindows(_enum_cb, 0)
    if placed:
        logger.info(
            "Đã đặt cửa sổ Firefox profile~{} → ({}, {}) {}x{}",
            profile_key[-48:],
            x,
            y,
            width,
            height,
        )
    return placed


def foreground_firefox_for_profile(
    profile_dir: str | Path,
    *,
    timeout_s: float = 10.0,
) -> bool:
    """Đưa cửa sổ Firefox (theo profile) lên trước — Windows."""
    if sys.platform != "win32":
        return False
    prof_key = str(Path(profile_dir).resolve()).lower().replace("/", "\\")
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    while time.monotonic() < deadline:
        if _try_set_window(prof_key, x=80, y=80, width=1280, height=900):
            return True
        time.sleep(0.35)
    return False
