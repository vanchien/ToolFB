"""Danh sách Business Manager đã biết, gắn với từng account."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.json_store_lock import json_file_lock
from src.utils.paths import project_root


def default_business_path() -> Path:
    return project_root() / "config" / "business_managers.json"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class BusinessManagerStore:
    """Đọc/ghi ``config/business_managers.json``."""

    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path) if path else default_business_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.is_file():
            self.path.write_text("[]\n", encoding="utf-8")

    def list_all(self) -> list[dict[str, Any]]:
        with json_file_lock(self.path):
            try:
                raw = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                raw = []
        if not isinstance(raw, list):
            return []
        return [dict(row) for row in raw if isinstance(row, dict)]

    def for_account(self, account_id: str) -> list[dict[str, Any]]:
        acc = (account_id or "").strip()
        rows = [row for row in self.list_all() if str(row.get("account_id") or "") == acc and str(row.get("business_id") or "").strip()]
        rows.sort(key=lambda row: str(row.get("business_name") or "").casefold())
        return rows

    def save(self, *, account_id: str, business_id: str, business_name: str) -> dict[str, Any]:
        """Thêm hoặc cập nhật tên BM của account. Trùng id thì không tạo dòng mới."""
        acc = account_id.strip()
        bid = business_id.strip()
        title = " ".join(business_name.split())
        if not acc or not bid:
            raise ValueError("Thiếu account hoặc business id.")
        record = {
            "account_id": acc,
            "business_id": bid,
            "business_name": title or bid,
            "updated_at": _now(),
        }
        with json_file_lock(self.path):
            try:
                raw = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                raw = []
            if not isinstance(raw, list):
                raw = []
            replaced = False
            out: list[dict[str, Any]] = []
            for row in raw:
                if not isinstance(row, dict):
                    continue
                same = str(row.get("account_id") or "") == acc and str(row.get("business_id") or "") == bid
                if same:
                    merged = dict(row)
                    merged.update(record)
                    if not merged.get("created_at"):
                        merged["created_at"] = record["updated_at"]
                    out.append(merged)
                    record = merged
                    replaced = True
                else:
                    out.append(row)
            if not replaced:
                record["created_at"] = record["updated_at"]
                out.append(record)
            self._write(out)
        return record

    def _write(self, rows: list[dict[str, Any]]) -> None:
        text = json.dumps(rows, ensure_ascii=False, indent=2) + "\n"
        fd, tmp = tempfile.mkstemp(prefix="business_managers_", suffix=".tmp.json", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
