"""
Repository JSON dạng ``list[dict]`` — ghi atomic (pattern giống ``pages.json``).

Dùng cho module mới; các manager hiện tại có thể dần chuyển sang đây.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Iterable, Optional

from loguru import logger


class JsonListRepository:
    """Đọc/ghi file JSON mảng các object."""

    def __init__(self, json_path: Path | str) -> None:
        self.file_path = Path(json_path).resolve()
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def _atomic_write(self, text: str) -> None:
        d = self.file_path.parent
        fd, tmp = tempfile.mkstemp(prefix="jsonlist_", suffix=".tmp.json", dir=str(d))
        try:
            import os

            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.file_path)
        except Exception:
            try:
                import os

                os.unlink(tmp)
            except OSError:
                pass
            raise

    def load(self) -> list[dict[str, Any]]:
        if not self.file_path.is_file():
            return []
        raw = json.loads(self.file_path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError(f"{self.file_path.name} phải là mảng JSON.")
        return [x for x in raw if isinstance(x, dict)]

    def save(self, rows: Iterable[dict[str, Any]]) -> None:
        lst = list(rows)
        self._atomic_write(json.dumps(lst, ensure_ascii=False, indent=2) + "\n")
        logger.debug("JsonListRepository ghi {} dòng → {}", len(lst), self.file_path)

    def exists(self) -> bool:
        return self.file_path.is_file()
