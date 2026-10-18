"""
Cache danh sách JSON theo đường dẫn + mtime — dùng chung mọi instance manager.

``AccountsDatabaseManager()`` / ``PagesManager()`` tạo mới mỗi lần gọi nên cache theo instance
không giúp GUI + scheduler 24/7. Cache process-wide tránh đọc đĩa và khóa file lặp.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

_guard = threading.RLock()
# path -> (mtime, size, rows)
_cache: dict[str, tuple[float, int, list[Any]]] = {}


def _key(path: Path | str) -> str:
    return str(Path(path).resolve())


def _stat_sig(path: Path) -> tuple[float, int] | None:
    try:
        st = path.stat()
    except OSError:
        return None
    return (st.st_mtime, int(st.st_size))


def peek_json_list_fresh(path: Path | str) -> list[Any] | None:
    """
    Trả bản sao nếu cache khớp ``mtime`` + kích thước trên đĩa — không cần khóa file JSON.

    Returns:
        Danh sách bản ghi hoặc ``None`` nếu chưa có / đã cũ.
    """
    p = Path(path)
    sig = _stat_sig(p)
    if sig is None:
        return None
    mtime, size = sig
    with _guard:
        hit = _cache.get(_key(p))
        if hit is None or hit[0] != mtime or hit[1] != size:
            return None
        return list(hit[2])


def peek_json_list_stale(path: Path | str) -> list[Any] | None:
    """Bản sao cache dù mtime đã đổi — dùng khi file đang bị ghi và GUI không được đợi khóa."""
    with _guard:
        hit = _cache.get(_key(path))
        if hit is None:
            return None
        return list(hit[2])


def remember_json_list(path: Path | str, mtime: float, rows: list[Any], size: int | None = None) -> None:
    """Ghi cache sau khi đọc hoặc ghi thành công."""
    p = Path(path)
    if size is None:
        try:
            size = int(p.stat().st_size)
        except OSError:
            size = 0
    with _guard:
        _cache[_key(p)] = (float(mtime), int(size), list(rows))


def forget_json_list(path: Path | str) -> None:
    """Xóa cache khi sắp ghi đè hoặc buộc đọc lại từ đĩa."""
    with _guard:
        _cache.pop(_key(path), None)
