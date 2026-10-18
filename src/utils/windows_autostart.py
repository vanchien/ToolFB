"""
Đăng ký / gỡ ToolFB khỏi khởi động cùng Windows (HKCU Run).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from loguru import logger

from src.utils.paths import project_root

RUN_VALUE_NAME = "ToolFB"


def _quote_win(path: str) -> str:
    """Bọc đường dẫn có khoảng trắng bằng dấu ngoặc kép."""
    text = str(path).strip()
    if not text:
        return '""'
    if text.startswith('"') and text.endswith('"'):
        return text
    return f'"{text}"'


def build_autostart_command(*, root: Path | None = None) -> str:
    """
    Lệnh mở GUI khi Windows đăng nhập.

    - Bản đóng gói: đường dẫn ``.exe``.
    - Dev: ``pythonw.exe`` + ``main.py --gui`` (không mở console).
    - Tôn trọng ``TOOLFB_DATA_DIR`` / ``--data-dir``.
    """
    base = root or project_root()
    parts: list[str] = []
    if getattr(sys, "frozen", False):
        parts.append(_quote_win(str(Path(sys.executable).resolve())))
    else:
        py = Path(sys.executable).resolve()
        pyw = py.with_name("pythonw.exe")
        runner = pyw if pyw.is_file() else py
        main_py = (base / "main.py").resolve()
        parts.append(_quote_win(str(runner)))
        parts.append(_quote_win(str(main_py)))
        parts.append("--gui")
    data_dir = os.environ.get("TOOLFB_DATA_DIR", "").strip()
    if data_dir:
        parts.append("--data-dir")
        parts.append(_quote_win(str(Path(data_dir).expanduser().resolve())))
    return " ".join(parts)


def is_autostart_enabled() -> bool:
    """True nếu đã có khóa Run ``ToolFB`` trên Windows."""
    if os.name != "nt":
        return False
    try:
        import winreg
    except ImportError:
        return False
    try:
        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Run",
            0,
            winreg.KEY_READ,
        ) as key:
            winreg.QueryValueEx(key, RUN_VALUE_NAME)
        return True
    except OSError:
        return False


def enable_autostart(*, root: Path | None = None) -> str:
    """
    Ghi lệnh mở app vào HKCU Run.

    Returns:
        Chuỗi lệnh đã đăng ký.
    """
    command = build_autostart_command(root=root)
    if os.name != "nt":
        logger.info("Bỏ qua autostart Windows (không phải Windows): {}", command)
        return command
    try:
        import winreg
    except ImportError as exc:
        raise RuntimeError("Thiếu winreg — không đăng ký được khởi động cùng Windows.") from exc
    with winreg.OpenKey(
        winreg.HKEY_CURRENT_USER,
        r"Software\Microsoft\Windows\CurrentVersion\Run",
        0,
        winreg.KEY_SET_VALUE,
    ) as key:
        winreg.SetValueEx(key, RUN_VALUE_NAME, 0, winreg.REG_SZ, command)
    logger.info("Đã bật mở cùng Windows: {}", command)
    return command


def disable_autostart() -> None:
    """Xóa khóa Run ``ToolFB`` nếu có."""
    if os.name != "nt":
        return
    try:
        import winreg
    except ImportError:
        return
    try:
        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Run",
            0,
            winreg.KEY_SET_VALUE,
        ) as key:
            winreg.DeleteValue(key, RUN_VALUE_NAME)
        logger.info("Đã tắt mở cùng Windows.")
    except FileNotFoundError:
        return
    except OSError as exc:
        # Value không tồn tại
        if getattr(exc, "winerror", None) == 2:
            return
        logger.warning("Không gỡ được autostart Windows: {}", exc)


def sync_autostart(enabled: bool, *, root: Path | None = None) -> str | None:
    """Bật hoặc tắt theo ``enabled``; trả lệnh khi bật."""
    if enabled:
        return enable_autostart(root=root)
    disable_autostart()
    return None
