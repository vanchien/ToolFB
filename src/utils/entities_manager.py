"""
Quản lý ánh xạ Tài khoản → Page/Group/Tường nhà (``config/entities.json``).
"""

from __future__ import annotations

import json
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any, Iterable, Optional, TypedDict

from loguru import logger

from src.utils.paths import project_root


class EntityRecord(TypedDict, total=False):
    """Một đích đăng (Page / Group / timeline)."""

    id: str
    account_id: str
    name: str
    target_type: str
    target_url: str
    schedule_time: str


def _default_entities_path() -> Path:
    """
    Đường dẫn ``config/entities.json``.
    """
    return project_root() / "config" / "entities.json"


class EntitiesManager:
    """Đọc/ghi danh sách entity (Page/Group) gắn với tài khoản."""

    REQUIRED: tuple[str, ...] = ("id", "account_id", "name", "target_type", "target_url", "schedule_time")
    TARGET_TYPES: tuple[str, ...] = ("timeline", "fanpage", "group")

    def __init__(self, json_path: Optional[Path | str] = None) -> None:
        """
        Khởi tạo đường dẫn file JSON.

        Args:
            json_path: Tùy chọn; mặc định ``config/entities.json``.
        """
        self.file_path = Path(json_path).resolve() if json_path else _default_entities_path()
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._rows_cache: Optional[list[EntityRecord]] = None
        self._rows_mtime: Optional[float] = None
        if not self.file_path.is_file():
            self._atomic_write(json.dumps([], ensure_ascii=False, indent=2) + "\n")
            logger.info("Đã tạo file entities rỗng: {}", self.file_path)

    def _atomic_write(self, text: str) -> None:
        """Ghi file JSON an toàn."""
        d = self.file_path.parent
        fd, tmp = tempfile.mkstemp(prefix="entities_", suffix=".tmp.json", dir=str(d))
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

    def _invalidate_rows_cache(self) -> None:
        """Xóa cache khi không còn khớp file trên đĩa."""
        self._rows_cache = None
        self._rows_mtime = None

    def reload_from_disk(self) -> list[EntityRecord]:
        """
        Bỏ cache và đọc lại ``entities.json`` (sau khi sửa tay ngoài process).

        Returns:
            Danh sách entity mới nhất.
        """
        self._invalidate_rows_cache()
        return self.load_all()

    def _validate(self, row: dict[str, Any]) -> None:
        """Kiểm tra schema một entity."""
        for k in self.REQUIRED:
            if k not in row or row[k] in (None, ""):
                raise ValueError(f"Thiếu hoặc rỗng trường bắt buộc: {k}")
        tt = str(row["target_type"]).strip().lower()
        if tt not in self.TARGET_TYPES:
            raise ValueError(f"target_type phải là một trong: {', '.join(self.TARGET_TYPES)}")
        url = str(row["target_url"]).strip()
        if not url.startswith("http"):
            raise ValueError("target_url phải bắt đầu bằng http:// hoặc https://")

    def load_all(self) -> list[EntityRecord]:
        """
        Đọc toàn bộ entity.

        Dùng cache theo ``mtime`` để giảm đọc đĩa lặp (Streamlit / scheduler gọi nhiều lần).

        Returns:
            Danh sách bản ghi.
        """
        if not self.file_path.is_file():
            self._invalidate_rows_cache()
            raise FileNotFoundError(str(self.file_path))
        mtime = self.file_path.stat().st_mtime
        if self._rows_cache is not None and self._rows_mtime == mtime:
            logger.debug("entities load_all: dùng cache ({} dòng)", len(self._rows_cache))
            return list(self._rows_cache)

        raw = json.loads(self.file_path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError("entities.json phải là mảng.")
        out: list[EntityRecord] = []
        for i, item in enumerate(raw):
            if not isinstance(item, dict):
                raise ValueError(f"Phần tử {i} không phải object.")
            self._validate(item)
            out.append(item)  # type: ignore[arg-type]
        self._rows_cache = out
        self._rows_mtime = mtime
        logger.info("Đã đọc {} entity từ {}", len(out), self.file_path)
        return list(out)

    def save_all(self, rows: Iterable[EntityRecord]) -> None:
        """Ghi đè danh sách (đã validate)."""
        lst = list(rows)
        for r in lst:
            self._validate(dict(r))
        self._atomic_write(json.dumps(lst, ensure_ascii=False, indent=2) + "\n")
        self._rows_cache = list(lst)
        self._rows_mtime = self.file_path.stat().st_mtime
        logger.info("Đã ghi {} entity vào {}", len(lst), self.file_path)

    def upsert(self, row: EntityRecord) -> None:
        """Thêm hoặc cập nhật theo ``id``."""
        d = dict(row)
        if not str(d.get("id", "")).strip():
            d["id"] = uuid.uuid4().hex[:12]
        self._validate(d)
        nid = str(d["id"])
        current = self.load_all()
        replaced = False
        new_list: list[EntityRecord] = []
        for x in current:
            if str(x.get("id")) == nid:
                new_list.append(d)  # type: ignore[arg-type]
                replaced = True
            else:
                new_list.append(x)
        if not replaced:
            new_list.append(d)  # type: ignore[arg-type]
        self.save_all(new_list)

    def delete_by_id(self, entity_id: str) -> bool:
        """Xóa theo ``id``."""
        cur = self.load_all()
        new_list = [x for x in cur if str(x.get("id")) != entity_id]
        if len(new_list) == len(cur):
            return False
        self.save_all(new_list)
        return True

    def list_for_account(self, account_id: str) -> list[EntityRecord]:
        """Lọc entity thuộc một ``account_id``."""
        aid = str(account_id).strip()
        return [x for x in self.load_all() if str(x.get("account_id")) == aid]

    def get_by_id(self, entity_id: str) -> Optional[EntityRecord]:
        """
        Lấy một entity theo ``id``.

        Args:
            entity_id: Khóa ``id`` trong JSON.

        Returns:
            Bản ghi hoặc ``None``.
        """
        eid = str(entity_id).strip()
        for x in self.load_all():
            if str(x.get("id")) == eid:
                return x
        return None


_default_entities_mgr: Optional[EntitiesManager] = None
_default_entities_lock = threading.Lock()


def get_default_entities_manager() -> EntitiesManager:
    """
    Singleton ``EntitiesManager`` mặc định (đường dẫn ``config/entities.json``).

    Dùng cho scheduler / worker để tái sử dụng cache ``mtime`` giữa các job.
    """
    global _default_entities_mgr
    with _default_entities_lock:
        if _default_entities_mgr is None:
            _default_entities_mgr = EntitiesManager()
        return _default_entities_mgr
