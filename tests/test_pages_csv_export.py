from pathlib import Path

from src.utils.pages_csv_export import CSV_HEADERS, pages_to_csv_rows, write_pages_csv


def test_pages_to_csv_rows_dedupe_url() -> None:
    rows = pages_to_csv_rows(
        [
            {"page_name": "A", "page_url": "https://facebook.com/a"},
            {"page_name": "A dup", "page_url": "https://facebook.com/a"},
            {"page_name": "B", "page_url": "https://facebook.com/b"},
        ]
    )
    assert len(rows) == 2
    assert rows[0][0] == "A"


def test_write_pages_csv(tmp_path: Path) -> None:
    out = tmp_path / "out.csv"
    n = write_pages_csv(
        out,
        [{"page_name": "Page 1", "page_url": "https://facebook.com/p1"}],
    )
    assert n == 1
    text = out.read_text(encoding="utf-8-sig")
    assert CSV_HEADERS[0] in text
    assert "Page 1" in text
