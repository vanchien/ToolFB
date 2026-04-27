"""
Gỡ trùng file output video (cùng nội dung, khác tên đường dẫn) để metadata và đĩa gọn, ổn định.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable


def media_file_fingerprint(path: Path) -> str:
    """
    Dấu vân tay nội dung: kích thước + sha256 (tối đa 8MB đầu file).
    Trả về chuỗi rỗng nếu không đọc được file.
    """
    try:
        size = int(path.stat().st_size)
    except OSError:
        return ""
    h = hashlib.sha256()
    remaining = 8 * 1024 * 1024
    try:
        with path.open("rb") as fh:
            while remaining > 0:
                chunk = fh.read(min(131072, remaining))
                if not chunk:
                    break
                h.update(chunk)
                remaining -= len(chunk)
    except OSError:
        return ""
    return f"{size}:{h.hexdigest()}"


def partition_new_output_files(
    paths: Iterable[str],
    seen_hashes: set[str],
    *,
    delete_duplicate_files: bool = True,
) -> list[str]:
    """
    Giữ các file có fingerprint chưa nằm trong ``seen_hashes``; bổ sung set sau mỗi file giữ.
    File trùng fingerprint: xóa khỏi đĩa (tuỳ chọn) và bỏ qua khỏi danh sách trả về.
    """
    kept: list[str] = []
    for raw in paths:
        s = str(raw or "").strip()
        if not s:
            continue
        p = Path(s)
        if not p.is_file():
            continue
        fp = media_file_fingerprint(p)
        if not fp:
            kept.append(s)
            continue
        if fp in seen_hashes:
            if delete_duplicate_files:
                try:
                    p.unlink(missing_ok=True)
                except OSError:
                    pass
            continue
        seen_hashes.add(fp)
        kept.append(s)
    return kept


def dedupe_output_file_paths(paths: Iterable[str], *, delete_duplicate_files: bool = True) -> list[str]:
    """
    Lọc một danh sách path: giữ thứ tự, chỉ giữ bản đầu tiên cho mỗi nội dung trùng; xóa file trùng sau.
    Path không tồn tại vẫn được giữ nguyên trong list (để caller/log xử lý).
    """
    kept: list[str] = []
    seen: set[str] = set()
    for raw in paths:
        s = str(raw or "").strip()
        if not s:
            continue
        p = Path(s)
        if not p.is_file():
            kept.append(s)
            continue
        fp = media_file_fingerprint(p)
        if not fp:
            kept.append(s)
            continue
        if fp in seen:
            if delete_duplicate_files:
                try:
                    p.unlink(missing_ok=True)
                except OSError:
                    pass
            continue
        seen.add(fp)
        kept.append(s)
    return kept
