from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Iterable, Literal

DeleteKind = Literal["any", "file", "dir"]


def _is_within(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def safe_delete_path(
    target: Path | str,
    *,
    allowed_roots: Iterable[Path | str],
    kind: DeleteKind = "any",
    missing_ok: bool = True,
    retries: int = 0,
    retry_sleep_sec: float = 0.15,
) -> bool:
    """
    Xóa an toàn 1 file/thư mục chỉ khi nằm trong danh sách root cho phép.

    Returns:
        True nếu đã xóa hoặc không tồn tại (khi ``missing_ok=True``), False nếu từ chối/xóa lỗi.
    """
    try:
        tgt = Path(target).resolve()
    except OSError:
        return False
    roots: list[Path] = []
    for r in allowed_roots:
        try:
            roots.append(Path(r).resolve())
        except OSError:
            continue
    if not roots:
        return False
    if not any(_is_within(root, tgt) for root in roots):
        return False

    if kind == "file" and tgt.exists() and not tgt.is_file():
        return False
    if kind == "dir" and tgt.exists() and not tgt.is_dir():
        return False

    attempts = max(0, int(retries)) + 1
    for i in range(attempts):
        try:
            if tgt.is_dir():
                shutil.rmtree(tgt, ignore_errors=False)
            elif tgt.is_file():
                tgt.unlink()
            else:
                return bool(missing_ok)
            return True
        except OSError:
            if i >= attempts - 1:
                return False
            time.sleep(max(0.01, float(retry_sleep_sec)) * (i + 1))
    return False
