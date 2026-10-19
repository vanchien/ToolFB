"""Copy link video từ Hàng chờ — logic trích URL không phụ thuộc Tk UI."""

from __future__ import annotations


def test_extract_queue_urls_prefer_url_column() -> None:
    """Cột url (index 3) là link video chính."""
    rows = [
        ("1", "Mới 22:55", "Healthy Life Hub", "https://www.facebook.com/reel/1655797918437275", "Đang chờ"),
        ("2", "Mới 23:05", "Arad ASMR", "https://www.facebook.com/reel/1137610765356675", "Đang chờ"),
    ]
    links: list[str] = []
    for vals in rows:
        url = str(vals[3] if len(vals) > 3 else "").strip()
        if url.startswith("http"):
            links.append(url)
    assert links == [
        "https://www.facebook.com/reel/1655797918437275",
        "https://www.facebook.com/reel/1137610765356675",
    ]


def test_queue_copy_dedupes_links() -> None:
    raw = [
        "https://www.facebook.com/reel/1",
        "https://www.facebook.com/reel/1",
        "https://www.facebook.com/reel/2",
    ]
    assert list(dict.fromkeys(raw)) == [
        "https://www.facebook.com/reel/1",
        "https://www.facebook.com/reel/2",
    ]
