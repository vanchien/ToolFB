"""Đọc danh sách Page từ CSV hoặc JSON."""

from __future__ import annotations

import csv
import io
import json
from typing import Any


def parse_page_import(text: str, *, kind: str) -> list[dict[str, str]]:
    """
    CSV cột ``page_name,avatar_path`` hoặc JSON mảng object cùng khóa.

    Dòng lỗi định dạng bị bỏ qua; validation đầy đủ chạy sau khi người dùng xác nhận.
    """
    raw = (text or "").strip()
    if not raw:
        return []
    mode = (kind or "csv").strip().lower()
    if mode == "json":
        data = json.loads(raw)
        if isinstance(data, dict):
            data = data.get("pages") or data.get("jobs") or []
        if not isinstance(data, list):
            raise ValueError("JSON phải là mảng các Page.")
        out: list[dict[str, str]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            out.append(
                {
                    "page_name": str(item.get("page_name") or item.get("name") or "").strip(),
                    "avatar_path": str(item.get("avatar_path") or item.get("avatar") or "").strip(),
                }
            )
        return out
    reader = csv.DictReader(io.StringIO(raw))
    rows: list[dict[str, str]] = []
    if reader.fieldnames:
        fields = [str(f or "").strip().lower() for f in reader.fieldnames]
        if "page_name" in fields:
            for item in reader:
                lowered = {str(k or "").strip().lower(): v for k, v in item.items()}
                rows.append(
                    {
                        "page_name": str(lowered.get("page_name") or "").strip(),
                        "avatar_path": str(lowered.get("avatar_path") or "").strip(),
                    }
                )
            return rows
    simple = csv.reader(io.StringIO(raw))
    for cols in simple:
        if not cols or str(cols[0]).strip().lower() in {"page_name", "name"}:
            continue
        name = str(cols[0]).strip() if cols else ""
        avatar = str(cols[1]).strip() if len(cols) > 1 else ""
        if name:
            rows.append({"page_name": name, "avatar_path": avatar})
    return rows


def dumps_preview(pages: list[dict[str, Any]]) -> str:
    """Tóm tắt danh sách để người dùng xác nhận trước khi tạo batch."""
    lines = [f"{i}. {row.get('page_name') or '(trống)'} — {row.get('avatar_path') or '(không avatar)'}" for i, row in enumerate(pages, start=1)]
    return "\n".join(lines)
