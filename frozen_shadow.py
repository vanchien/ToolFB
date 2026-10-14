"""Gỡ thư mục src cũ nằm cạnh file exe trước khi Python nạp code."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path


def drop_frozen_shadow_source() -> None:
    """Bản đóng gói chỉ dùng code trong ``_internal``.

    Lần cập nhật trước có thể để lại ``src`` cạnh ``ToolFB_GUI.exe``. Python thấy
    thư mục đó trước và chạy file cũ dù ``_internal`` đã là bản mới.
    """
    if not getattr(sys, "frozen", False):
        return
    exe_dir = Path(sys.executable).resolve().parent
    shadow = exe_dir / "src"
    if not shadow.is_dir():
        return
    try:
        shutil.rmtree(shadow)
        return
    except OSError:
        pass
    parked = exe_dir / "src.shadow_old"
    if parked.exists():
        shutil.rmtree(parked, ignore_errors=True)
    try:
        shadow.rename(parked)
    except OSError:
        return
