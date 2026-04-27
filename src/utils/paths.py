"""Đường dẫn gốc dự án (dùng chung cho utils, dashboard, automation)."""

from __future__ import annotations

import sys
from pathlib import Path


def project_root() -> Path:
    """
    Thư mục gốc repository (ToolFB).

    PyInstaller (``--onedir``): ``config/`` và ``data/`` nằm cạnh file ``.exe``, không phải trong ``_internal``.

    Returns:
        Path tuyệt đối tới thư mục chứa ``src/``, ``config/``, ``data/`` (hoặc cạnh ``.exe`` khi frozen).
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]
