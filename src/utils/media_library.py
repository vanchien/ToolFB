"""
Thư mục media dùng cho bài đăng — đổi tên file ngẫu nhiên để giảm trùng hash.
"""

from __future__ import annotations

import secrets
import string
import shutil
from pathlib import Path

from loguru import logger

from src.utils.paths import project_root


def media_library_dir() -> Path:
    """
    ``data/media_library/`` — upload / rename tại đây.
    """
    d = project_root() / "data" / "media_library"
    d.mkdir(parents=True, exist_ok=True)
    return d


def list_media_files() -> list[Path]:
    """
    Liệt kê file (không đệ quy) trong thư viện media.

    Returns:
        Danh sách path, sắp tên file.
    """
    d = media_library_dir()
    exts = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".webm"}
    files = [p for p in d.iterdir() if p.is_file() and p.suffix.lower() in exts]
    return sorted(files, key=lambda p: p.name.lower())


def random_rename_file(path: Path, *, length: int = 14) -> Path:
    """
    Đổi tên file trong cùng thư mục sang chuỗi chữ thường + số ngẫu nhiên (giữ extension).

    Args:
        path: File tồn tại trong ``data/media_library/``.
        length: Độ dài phần tên (không tính extension).

    Returns:
        Path mới sau khi đổi tên.

    Raises:
        ValueError: File không nằm trong thư viện hoặc không tồn tại.
    """
    path = path.resolve()
    base = media_library_dir().resolve()
    if path.parent.resolve() != base:
        raise ValueError("Chỉ đổi tên file trong data/media_library/")
    if not path.is_file():
        raise ValueError("File không tồn tại.")
    alphabet = string.ascii_lowercase + string.digits
    new_stem = "".join(secrets.choice(alphabet) for _ in range(length))
    new_path = path.parent / f"{new_stem}{path.suffix.lower()}"
    while new_path.exists():
        new_stem = "".join(secrets.choice(alphabet) for _ in range(length))
        new_path = path.parent / f"{new_stem}{path.suffix.lower()}"
    path.rename(new_path)
    logger.info("Đã đổi tên media: {} → {}", path.name, new_path.name)
    return new_path


def save_upload_to_library(data: bytes, original_name: str) -> Path:
    """
    Lưu bytes upload vào thư viện (tên an toàn, tránh path traversal).

    Args:
        data: Nội dung file.
        original_name: Tên gốc (chỉ lấy basename + suffix).

    Returns:
        Path file đã lưu.
    """
    safe = Path(original_name).name
    if not safe or ".." in safe or "/" in safe or "\\" in safe:
        safe = "upload.bin"
    dest = media_library_dir() / safe
    if dest.exists():
        stem = dest.stem
        suf = dest.suffix
        dest = media_library_dir() / f"{stem}_{secrets.token_hex(4)}{suf}"
    dest.write_bytes(data)
    logger.info("Đã upload media: {}", dest)
    return dest
