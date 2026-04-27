"""
Lịch đăng bài tổng hợp (``config/schedule.json``) — cầu nối cho scheduler sau này.

Schema: mảng object (tùy mở rộng). Hiện tại chỉ lưu/truy xuất an toàn JSON.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Iterable, Optional

from loguru import logger

from src.utils.paths import project_root


def _default_schedule_path() -> Path:
    return project_root() / "config" / "schedule.json"


class ScheduleStore:
    """Đọc/ghi ``schedule.json`` (danh sách job / slot lịch)."""

    def __init__(self, json_path: Optional[Path | str] = None) -> None:
        self.file_path = Path(json_path).resolve() if json_path else _default_schedule_path()
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.file_path.is_file():
            self._atomic_write(json.dumps([], ensure_ascii=False, indent=2) + "\n")
            logger.info("Đã tạo schedule.json rỗng: {}", self.file_path)

    def _atomic_write(self, text: str) -> None:
        d = self.file_path.parent
        fd, tmp = tempfile.mkstemp(prefix="schedule_", suffix=".tmp.json", dir=str(d))
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

    def load_all(self) -> list[dict[str, Any]]:
        raw = json.loads(self.file_path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError("schedule.json phải là mảng.")
        return [x for x in raw if isinstance(x, dict)]

    def save_all(self, rows: Iterable[dict[str, Any]]) -> None:
        lst = list(rows)
        self._atomic_write(json.dumps(lst, ensure_ascii=False, indent=2) + "\n")
        logger.info("Đã ghi {} mục schedule vào {}", len(lst), self.file_path)
