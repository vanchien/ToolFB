"""Cộng dồn kết quả quét nhiều kênh vào một danh sách tải."""

from pathlib import Path

from src.services.universal_video_downloader import (
    extract_job_pending_urls,
    load_scan_lists,
    mark_job_url_downloaded,
    merge_scan_entry_rows,
    merge_scan_url_list,
    remaining_scan_entry_rows,
    remaining_urls_after_success,
    save_scan_lists,
    upsert_job_failed_item,
)


def test_merge_scan_url_list_appends_and_dedupes() -> None:
    existing = [
        "https://www.tiktok.com/@a/video/1111111111111111111",
        "https://www.tiktok.com/@a/video/2222222222222222222",
    ]
    incoming = [
        "https://www.tiktok.com/@a/video/2222222222222222222",
        "https://www.tiktok.com/@b/video/3333333333333333333",
    ]
    merged, added, dup = merge_scan_url_list(existing, incoming)
    assert added == 1
    assert dup == 1
    assert merged == [
        "https://www.tiktok.com/@a/video/1111111111111111111",
        "https://www.tiktok.com/@a/video/2222222222222222222",
        "https://www.tiktok.com/@b/video/3333333333333333333",
    ]


def test_merge_scan_entry_rows_keeps_old_title() -> None:
    existing = [{"title": "TikTok 1", "url": "https://www.tiktok.com/@a/video/1111111111111111111"}]
    incoming = [
        {"title": "other title", "url": "https://www.tiktok.com/@a/video/1111111111111111111"},
        {"title": "TikTok 2", "url": "https://www.tiktok.com/@b/video/2222222222222222222"},
    ]
    merged, added, dup = merge_scan_entry_rows(existing, incoming)
    assert added == 1
    assert dup == 1
    assert merged[0]["title"] == "TikTok 1"
    assert merged[1]["url"].endswith("/2222222222222222222")


def test_merge_empty_existing() -> None:
    merged, added, dup = merge_scan_entry_rows(
        [],
        [{"title": "A", "url": "https://www.tiktok.com/@a/video/1111111111111111111"}],
    )
    assert added == 1
    assert dup == 0
    assert len(merged) == 1


def test_remaining_scan_keeps_undownloaded() -> None:
    rows = [
        {"title": "A", "url": "https://www.tiktok.com/@a/video/1111111111111111111"},
        {"title": "B", "url": "https://www.tiktok.com/@a/video/2222222222222222222"},
        {"title": "C", "url": "https://www.tiktok.com/@a/video/3333333333333333333"},
    ]
    kept = remaining_scan_entry_rows(
        rows,
        ["https://www.tiktok.com/@a/video/2222222222222222222"],
    )
    assert [r["title"] for r in kept] == ["A", "C"]
    leftover = remaining_urls_after_success(
        [r["url"] for r in rows],
        ["https://www.tiktok.com/@a/video/1111111111111111111"],
    )
    assert leftover == [
        "https://www.tiktok.com/@a/video/2222222222222222222",
        "https://www.tiktok.com/@a/video/3333333333333333333",
    ]


def test_scan_lists_roundtrip(tmp_path: Path) -> None:
    paths = {"root": tmp_path, "scan_lists_file": tmp_path / "scan_lists.json"}
    payload = {
        "last_job_id": "dl_abc123",
        "tiktok": {
            "rows": [{"title": "TikTok 1", "url": "https://www.tiktok.com/@a/video/1111111111111111111"}],
            "sources": ["https://www.tiktok.com/@a"],
        },
    }
    save_scan_lists(payload, paths=paths)
    loaded = load_scan_lists(paths=paths)
    assert loaded["last_job_id"] == "dl_abc123"
    assert loaded["tiktok"]["rows"][0]["url"].endswith("1111111111111111111")
    assert loaded["tiktok"]["sources"] == ["https://www.tiktok.com/@a"]
    assert loaded["youtube"]["rows"] == []


def test_job_pending_urls_survive_fail_and_drop_on_success() -> None:
    job = {
        "queued_urls": [
            "https://www.tiktok.com/@a/video/1111111111111111111",
            "https://www.tiktok.com/@a/video/2222222222222222222",
        ],
        "pending_urls": [
            "https://www.tiktok.com/@a/video/1111111111111111111",
            "https://www.tiktok.com/@a/video/2222222222222222222",
        ],
        "failed_items": [],
    }
    upsert_job_failed_item(job, job["queued_urls"][0], "IP blocked")
    assert extract_job_pending_urls(job) == job["queued_urls"]
    mark_job_url_downloaded(job, job["queued_urls"][0])
    assert extract_job_pending_urls(job) == [job["queued_urls"][1]]
    assert job["failed_items"] == []
