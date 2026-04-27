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

# Đồng bộ với app_updater: batch hoàn tất copy exe/_internal sau khi process cũ thoát (Windows).
DEFERRED_GUI_BAT_NAME = "toolfb_deferred_gui_apply.bat"


def deferred_gui_bat_path(root: Path | None = None) -> Path:
    return (root or project_root()).resolve() / "data" / "updates" / DEFERRED_GUI_BAT_NAME


def build_restart_command() -> list[str]:
    """
    Lệnh tương đương phiên hiện tại (dùng khi cần list argv thuần).

    Bản đóng gói Windows: ``cmd /c start`` được dùng trong ``relaunch_same_app_and_exit``;
    hàm này vẫn trả ``[exe, --gui]`` cho script/ghi log.
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

    Windows + frozen: nếu có batch hoãn copy ``exe``/``_internal`` (sau update), chạy batch
    nền trước rồi thoát — tránh WinError 32 (file đang bị process giữ).
    Ngược lại: ``cmd /c start "" exe --gui`` để tách process.
    """
    root = (cwd or project_root()).resolve()
    root_s = str(root)
    frozen = getattr(sys, "frozen", False)
    bat = deferred_gui_bat_path(root)

    if frozen and os.name == "nt" and bat.is_file():
        # Script đợi file nhả khóa rồi copy staged exe/_internal rồi mở lại GUI.
        bat_s = str(bat.resolve())
        cmd = ["cmd.exe", "/c", "start", "/min", "", "cmd.exe", "/c", bat_s]
        logger.info("Relaunch: chạy hoãn copy exe/_internal rồi mở GUI: {}", cmd)
        try:
            subprocess.Popen(cmd, cwd=root_s, close_fds=False)
        except OSError as exc:
            logger.exception("Relaunch: không chạy được batch hoãn: {}", exc)
            raise
        time.sleep(0.5)
    else:
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
        time.sleep(0.4)

    if tk_root is not None:
        try:
            tk_root.destroy()
        except Exception:
            pass
    os._exit(0)
