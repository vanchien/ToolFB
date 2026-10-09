"""Kiểm tra toàn bộ danh sách Page trước khi tạo batch."""

from __future__ import annotations

import stat as statmod
from pathlib import Path

OPEN_BATCH_STATUSES = {"RUNNING", "DELAYING", "PAUSED", "PAUSED_FOR_USER", "READY"}
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
_IMAGE_CACHE: dict[str, tuple[int, int, bool]] = {}
_MAGIC = (
    b"\xff\xd8\xff",
    b"\x89PNG\r\n\x1a\n",
    b"GIF87a",
    b"GIF89a",
    b"RIFF",
)


def open_batch_for_account(batches: list[dict], account_id: str) -> dict | None:
    """Batch chưa kết thúc của account. Batch sau cùng được chọn."""
    found: dict | None = None
    for batch in batches:
        if str(batch.get("account_id") or "") != account_id:
            continue
        if str(batch.get("status") or "") in OPEN_BATCH_STATUSES:
            found = batch
    return found


def avatar_is_image(path: str) -> bool:
    """File tồn tại, đuôi ảnh, và có magic byte của ảnh. Cùng file thì chỉ đọc đĩa một lần."""
    p = Path(path)
    try:
        file_stat = p.stat()
    except OSError:
        return False
    if not statmod.S_ISREG(file_stat.st_mode) or p.suffix.lower() not in IMAGE_EXT:
        return False
    key = str(p)
    token = (file_stat.st_mtime_ns, file_stat.st_size)
    cached = _IMAGE_CACHE.get(key)
    if cached is not None and cached[0] == token[0] and cached[1] == token[1]:
        return cached[2]
    try:
        head = p.read_bytes()[:16]
    except OSError:
        ok = False
    else:
        if head.startswith(b"RIFF") and b"WEBP" not in head:
            ok = False
        else:
            ok = any(head.startswith(sig) for sig in _MAGIC)
    _IMAGE_CACHE[key] = (token[0], token[1], ok)
    return ok


def validate_page_list(
    *,
    account_id: str,
    business_id: str,
    pages: list[dict[str, str]],
    known_account_ids: set[str],
    known_business_ids: set[str],
    busy_account_ids: set[str],
    create_mode: str = "bm",
) -> list[str]:
    """
    Trả danh sách lỗi. Rỗng nghĩa là được phép tạo batch.

    Kiểm tra hết mọi dòng trước khi trả về — không dừng ở lỗi đầu tiên.
    """
    errors: list[str] = []
    acc = (account_id or "").strip()
    bm = (business_id or "").strip()
    mode = (create_mode or "bm").strip().casefold()
    if mode not in {"bm", "profile"}:
        mode = "bm"
    if not acc or acc not in known_account_ids:
        errors.append("Account không tồn tại.")
    if mode == "bm":
        if not bm or bm not in known_business_ids:
            errors.append("Business Manager không tồn tại.")
    if acc and acc in busy_account_ids:
        errors.append("Account đang có batch tạo Page chưa xong.")
    if not pages:
        errors.append("Danh sách Page trống.")
    seen: set[str] = set()
    for index, row in enumerate(pages, start=1):
        name = str(row.get("page_name") or "").strip()
        avatar = str(row.get("avatar_path") or "").strip()
        if not name:
            errors.append(f"Dòng {index}: tên Page trống.")
        else:
            key = name.casefold()
            if key in seen:
                errors.append(f"Dòng {index}: tên Page bị trùng trong batch ({name}).")
            seen.add(key)
        if not avatar:
            continue
        if not Path(avatar).is_file():
            errors.append(f"Dòng {index}: avatar không tồn tại.")
        elif not avatar_is_image(avatar):
            errors.append(f"Dòng {index}: avatar không phải file ảnh.")
    return errors
