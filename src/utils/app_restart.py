"""
Khởi động lại chính app (sau cập nhật hoặc đổi cấu hình cần nạp lại process).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from src.utils.paths import project_root


def build_restart_command() -> list[str]:
    """
    Lệnh tương đương phiên hiện tại: bản đóng gói thêm ``--gui``; dev dùng ``sys.executable`` + ``sys.argv``.
    """
    if getattr(sys, "frozen", False):
        return [sys.executable, "--gui"]
    return [sys.executable, *sys.argv]


def relaunch_same_app_and_exit(*, cwd: Path | None = None, tk_root: Any | None = None) -> None:
    """
    Mở một process mới (cùng cwd) rồi thoát process hiện tại.

    ``tk_root``: nếu có, lên lịch ``destroy`` trước khi thoát (một số máy mượt hơn khi đóng Tk trước ``_exit``).
    """
    cmd = build_restart_command()
    d = str((cwd or project_root()).resolve())
    popen_kw: dict[str, Any] = {"cwd": d}
    if os.name == "nt":
        det = getattr(subprocess, "DETACHED_PROCESS", 0)
        if det:
            popen_kw["creationflags"] = det
    subprocess.Popen(cmd, **popen_kw)
    if tk_root is not None:
        try:
            tk_root.after(0, tk_root.destroy)
        except Exception:
            pass
    os._exit(0)
