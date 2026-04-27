"""
Khởi động lại chính app (sau cập nhật hoặc đổi cấu hình cần nạp lại process).
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from loguru import logger

from src.utils.paths import project_root


def build_restart_command() -> list[str]:
    """
    Lệnh tương đương phiên hiện tại (dùng khi cần list argv thuần).

    Bản đóng gói Windows: ``cmd /c start`` được dùng trong ``relaunch_same_app_and_exit``
    để tách process; hàm này vẫn trả ``[exe, --gui]`` cho script/ghi log.
    """
    if getattr(sys, "frozen", False):
        return [str(Path(sys.executable).resolve()), "--gui"]
    argv = list(sys.argv)
    if "--gui" not in argv:
        argv.append("--gui")
    return [sys.executable, *argv]


def relaunch_same_app_and_exit(*, cwd: Path | None = None, tk_root: Any | None = None) -> None:
    """
    Mở một process mới (cùng cwd) rồi thoát process hiện tại.

    Windows + frozen: ``cmd /c start "" exe --gui`` để process con tách hẳn khỏi Tk/parent,
    tránh trường hợp mở lại không lên GUI sau cập nhật.
    """
    root = (cwd or project_root()).resolve()
    root_s = str(root)
    frozen = getattr(sys, "frozen", False)

    if frozen:
        exe = str(Path(sys.executable).resolve())
        if os.name == "nt":
            cmd: list[str] = ["cmd.exe", "/c", "start", "", exe, "--gui"]
        else:
            cmd = [exe, "--gui"]
    else:
        argv = list(sys.argv)
        if "--gui" not in argv:
            argv.append("--gui")
        cmd = [sys.executable, *argv]

    logger.info("Relaunch: cwd={} cmd={}", root_s, cmd)
    try:
        subprocess.Popen(cmd, cwd=root_s, close_fds=False)
    except OSError as exc:
        logger.exception("Relaunch: không spawn được process: {}", exc)
        raise
    # Cho process con kịp detach (ổ chậm / Windows Defender quét exe).
    time.sleep(0.4)
    if tk_root is not None:
        try:
            tk_root.destroy()
        except Exception:
            pass
    os._exit(0)
