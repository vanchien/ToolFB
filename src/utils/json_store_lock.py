"""
Khóa đọc/ghi theo đường dẫn file JSON — tránh mất dữ liệu khi GUI + scheduler cùng ghi.

Dùng ``threading.RLock`` theo file (một process). Nhiều process ToolFB: dùng ``TOOLFB_DATA_DIR`` riêng mỗi cửa sổ.
"""

from __future__ import annotations

import threading
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

_registry_guard = threading.Lock()
_file_locks: dict[str, threading.RLock] = {}


def _lock_for(path: Path | str) -> threading.RLock:
    key = str(Path(path).resolve())
    with _registry_guard:
        lk = _file_locks.get(key)
        if lk is None:
            lk = threading.RLock()
            _file_locks[key] = lk
        return lk


@contextmanager
def json_file_lock(path: Path | str) -> Generator[None, None, None]:
    """Giữ khóa trong suốt load → sửa → save một file JSON."""
    lk = _lock_for(path)
    lk.acquire()
    try:
        yield
    finally:
        lk.release()
