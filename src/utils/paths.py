"""Đường dẫn gốc dự án (dùng chung cho utils, dashboard, automation)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_DATA_ROOT_CACHE: Path | None = None


def project_root() -> Path:
    """
    Thư mục gốc repository (ToolFB).

    PyInstaller (``--onedir``): ``config/`` và ``data/`` nằm cạnh file ``.exe``, không phải trong ``_internal``.

    ``TOOLFB_DATA_DIR``: thư mục gốc riêng (chứa ``config/``, ``data/``, ``logs/``) — mỗi cửa sổ app
    một bản sao dữ liệu để mở nhiều instance trên cùng máy mà không đụng JSON/profile.

    Returns:
        Path tuyệt đối tới thư mục chứa ``src/``, ``config/``, ``data/`` (hoặc cạnh ``.exe`` khi frozen).
    """
    global _DATA_ROOT_CACHE
    if _DATA_ROOT_CACHE is not None:
        return _DATA_ROOT_CACHE
    custom = os.environ.get("TOOLFB_DATA_DIR", "").strip()
    if custom:
        _DATA_ROOT_CACHE = Path(custom).expanduser().resolve()
        return _DATA_ROOT_CACHE
    if getattr(sys, "frozen", False):
        _DATA_ROOT_CACHE = Path(sys.executable).resolve().parent
    else:
        _DATA_ROOT_CACHE = Path(__file__).resolve().parents[2]
    return _DATA_ROOT_CACHE


def reset_project_root_cache() -> None:
    """Gọi sau khi đổi ``TOOLFB_DATA_DIR`` (ví dụ ``--data-dir``)."""
    global _DATA_ROOT_CACHE
    _DATA_ROOT_CACHE = None
