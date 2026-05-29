"""Xuất danh sách Page ra CSV (Tên page, Link page)."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable

CSV_HEADERS = ("Tên page", "Link page")


def page_csv_row(page: dict[str, Any]) -> tuple[str, str]:
    name = str(page.get("page_name", "") or "").strip()
    url = str(page.get("page_url", "") or "").strip()
    return name, url


def pages_to_csv_rows(pages: Iterable[dict[str, Any]]) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    for p in pages:
        if not isinstance(p, dict):
            continue
        row = page_csv_row(p)
        if not row[0] and not row[1]:
            continue
        key = row[1].lower() or row[0].lower()
        if key in seen_urls:
            continue
        seen_urls.add(key)
        rows.append(row)
    return rows


def write_pages_csv(path: Path | str, pages: Iterable[dict[str, Any]]) -> int:
    """
    Ghi CSV UTF-8 BOM (Excel Windows). Trả về số dòng dữ liệu (không tính header).
    """
    out = Path(path).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    rows = pages_to_csv_rows(pages)
    with out.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_HEADERS)
        writer.writerows(rows)
    return len(rows)
