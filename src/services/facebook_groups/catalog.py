"""Danh mục quốc gia và ngôn ngữ đọc từ cấu hình, không gắn cứng trong bộ lọc."""

from __future__ import annotations

from typing import Any


def seed_catalog() -> dict[str, list[dict[str, str]]]:
    """Bản mẫu ghi một lần vào file cấu hình. Sửa file để thêm nước hoặc ngôn ngữ."""
    return {
        "countries": [
            {"code": "BR", "name": "Brazil"},
            {"code": "PT", "name": "Portugal"},
            {"code": "ES", "name": "Spain"},
            {"code": "MX", "name": "Mexico"},
            {"code": "US", "name": "United States"},
            {"code": "VN", "name": "Vietnam"},
        ],
        "languages": [
            {"code": "pt", "name": "Portuguese"},
            {"code": "es", "name": "Spanish"},
            {"code": "en", "name": "English"},
            {"code": "vi", "name": "Vietnamese"},
        ],
    }


def catalog_codes(rows: list[dict[str, Any]] | None) -> list[str]:
    """Mã đang bật trong cấu hình."""
    out: list[str] = []
    for row in rows or []:
        code = str(row.get("code") or "").strip()
        if code and code not in out:
            out.append(code)
    return out
