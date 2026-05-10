from __future__ import annotations

import errno
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any


def write_json_resilient(path: Path, payload: Any, *, tmp_prefix: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    tmp_root = Path(tempfile.gettempdir())
    fd, tmp_name = tempfile.mkstemp(prefix=tmp_prefix, suffix=".tmp.json", dir=str(tmp_root))
    tmp_path = Path(tmp_name)
    last_err: OSError | None = None
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        for attempt in range(25):
            try:
                os.replace(str(tmp_path), str(path))
                return
            except OSError as e:
                last_err = e
                code = getattr(e, "winerror", None)
                if code in (5, 32) or e.errno in (errno.EACCES, errno.EPERM):
                    time.sleep(0.05 * min(attempt + 1, 12))
                    continue
                break
        try:
            shutil.copyfile(str(tmp_path), str(path))
            return
        except OSError as e2:
            last_err = e2
        path.write_text(text, encoding="utf-8")
    finally:
        try:
            if tmp_path.is_file():
                tmp_path.unlink()
        except Exception:
            pass
    if last_err and not path.is_file():
        raise last_err


def read_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, UnicodeError):
        return []
    if not isinstance(raw, list):
        return []
    return [x for x in raw if isinstance(x, dict)]
