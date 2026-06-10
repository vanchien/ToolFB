"""E2E/unit cho luồng quét playlist và tải tuần tự tab Tải video."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.services.universal_video_downloader import (
    UV_DOWNLOAD_SEQUENTIAL_THRESHOLD,
    UV_MAX_PLAYLIST_ENTRIES,
    UV_PLAYLIST_SCAN_CHUNK,
    UniversalVideoDownloader,
    UniversalYTDLPWrapper,
    extract_failed_download_pairs,
    write_failed_download_urls_log,
)


def test_playlist_limits_constants() -> None:
    assert UV_MAX_PLAYLIST_ENTRIES >= 5000
    assert UV_PLAYLIST_SCAN_CHUNK >= 200
    assert UV_DOWNLOAD_SEQUENTIAL_THRESHOLD >= 10


def test_orchestrated_merge_dedup_and_stop_on_empty_page() -> None:
    wrapper = UniversalYTDLPWrapper(yt_cfg={}, log=lambda _m: None)
    calls: list[tuple[int, int]] = []

    def _fake_page(
        url: str,
        *,
        max_entries: int = 500,
        on_partial=None,
        playlist_start: int = 1,
        playlist_end: int | None = None,
        _orchestrating: bool = False,
    ) -> dict[str, Any]:
        del url, max_entries, on_partial, _orchestrating
        ps = int(playlist_start)
        pe = int(playlist_end or ps)
        calls.append((ps, pe))
        if ps > 900:
            return {"success": True, "entries": []}
        entries = [
            {"title": f"v{i}", "url": f"https://youtu.be/{i}"}
            for i in range(ps, pe + 1)
        ]
        if ps == 401:
            entries.append({"title": "dup", "url": "https://youtu.be/400"})
        return {
            "success": True,
            "entries": entries,
            "playlist_title": "Test Channel",
            "extractor": "youtube",
        }

    with patch.object(wrapper, "list_flat_playlist_entries", side_effect=_fake_page):
        with patch("src.services.universal_video_downloader.time.sleep"):
            res = wrapper._list_flat_playlist_orchestrated(
                "https://www.youtube.com/@x/shorts",
                platform="youtube",
                max_entries=850,
                chunk=400,
            )

    assert res["success"] is True
    urls = [e["url"] for e in res["entries"]]
    assert len(urls) == len(set(urls))
    assert len(urls) == 850
    assert calls[0] == (1, 400)
    assert calls[1] == (401, 800)
    assert calls[2] == (801, 850)


def test_youtube_only_orchestration_not_tiktok() -> None:
    wrapper = UniversalYTDLPWrapper(yt_cfg={}, log=lambda _m: None)
    orchestrated = {"called": False}

    def _orch(*_a, **_k):
        orchestrated["called"] = True
        return {"success": True, "entries": [], "playlist_title": "", "extractor": ""}

    with patch.object(wrapper, "_list_flat_playlist_orchestrated", side_effect=_orch):
        with patch.object(
            wrapper,
            "_resolve_prefix",
            return_value=["yt-dlp"],
        ):
            with patch("src.services.universal_video_downloader.subprocess.Popen") as popen:
                proc = MagicMock()
                proc.stdout = iter([])
                proc.stderr = iter([])
                proc.poll.return_value = 0
                proc.wait.return_value = 0
                popen.return_value = proc
                wrapper.list_flat_playlist_entries(
                    "https://www.tiktok.com/@user",
                    max_entries=5000,
                )
    assert orchestrated["called"] is False


def test_sequential_download_reuses_videos_rows_cache(tmp_path: Path) -> None:
    paths = {
        "jobs_file": tmp_path / "jobs.json",
        "videos_file": tmp_path / "videos.json",
        "archive": tmp_path / "archive.txt",
        "root": tmp_path,
    }
    paths["jobs_file"].write_text("[]\n", encoding="utf-8")
    paths["videos_file"].write_text("[]\n", encoding="utf-8")

    down = UniversalVideoDownloader(log=lambda _m: None)
    down._paths = paths  # type: ignore[assignment]
    down._store._paths = paths  # type: ignore[attr-defined]

    job = down.create_download_job(
        "https://www.youtube.com/@x",
        {
            "platform": "youtube",
            "url_type": "single_video",
            "max_videos": 1,
            "output_dir": str(tmp_path / "out"),
            "organize_by_platform": False,
            "organize_by_uploader": False,
            "skip_existing": False,
            "write_info_json": False,
            "write_thumbnail": False,
        },
    )
    jid = str(job["id"])
    urls = ["https://youtu.be/a", "https://youtu.be/b", "https://youtu.be/c"]
    list_calls: list[int] = []

    real_list = down._store.list_downloaded_videos

    def _counting_list() -> list[dict[str, Any]]:
        list_calls.append(1)
        return real_list()

    with patch.object(down._store, "list_downloaded_videos", side_effect=_counting_list):
        with patch.object(
            down,
            "run_download_url_for_job",
            side_effect=lambda job_id, url, **kw: down._store.get_job(job_id) or {},
        ) as run_one:
            down.run_download_urls_sequential_for_job(jid, urls)

    assert run_one.call_count == 3
    assert list_calls, "phải đọc metadata ít nhất một lần"
    assert len(list_calls) <= 2, "cache phải giảm số lần đọc file videos.json"


def test_run_download_url_for_job_accepts_cached_rows(tmp_path: Path) -> None:
    paths = {
        "jobs_file": tmp_path / "jobs.json",
        "videos_file": tmp_path / "videos.json",
        "archive": tmp_path / "archive.txt",
        "root": tmp_path,
    }
    paths["jobs_file"].write_text("[]\n", encoding="utf-8")
    paths["videos_file"].write_text("[]\n", encoding="utf-8")

    down = UniversalVideoDownloader(log=lambda _m: None)
    down._paths = paths  # type: ignore[assignment]
    down._store._paths = paths  # type: ignore[attr-defined]

    job = down.create_download_job(
        "https://youtu.be/x",
        {
            "platform": "youtube",
            "url_type": "single_video",
            "max_videos": 1,
            "output_dir": str(tmp_path / "out"),
            "organize_by_platform": False,
            "organize_by_uploader": False,
            "skip_existing": True,
            "write_info_json": False,
            "write_thumbnail": False,
        },
    )
    jid = str(job["id"])
    cache: list[dict[str, Any]] = []
    list_calls = 0

    def _no_list() -> list[dict[str, Any]]:
        nonlocal list_calls
        list_calls += 1
        return cache

    fake_ret = {"success": True, "filepaths": [], "skipped_only": True}

    with patch.object(down._store, "list_downloaded_videos", side_effect=_no_list):
        with patch.object(down._yt, "download", return_value=fake_ret):
            with patch.object(down, "_attach_existing_sources_to_job", return_value=0):
                down.run_download_url_for_job(
                    jid,
                    "https://youtu.be/x",
                    videos_rows=cache,
                    skip_output_dir_validate=True,
                )

    assert list_calls == 0


def test_extract_failed_pairs_dedup() -> None:
    job = {
        "failed_items": [
            {"url": "https://youtu.be/a", "error": "err1"},
            {"url": "https://youtu.be/a", "error": "dup"},
            {"url": "https://youtu.be/b", "error": "err2"},
        ]
    }
    pairs = extract_failed_download_pairs(job)
    assert pairs == [
        ("https://youtu.be/a", "err1"),
        ("https://youtu.be/b", "err2"),
    ]


def test_write_failed_download_urls_log(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.services.universal_video_downloader as uvd

    monkeypatch.setattr(uvd, "project_root", lambda: tmp_path)
    logged: list[str] = []
    path = write_failed_download_urls_log(
        platform="youtube",
        job_id="dl_abc123",
        failed_pairs=[("https://youtu.be/x", "timeout"), ("https://youtu.be/y", "")],
        log_fn=logged.append,
    )
    assert path is not None and path.is_file()
    text = path.read_text(encoding="utf-8")
    assert "https://youtu.be/x" in text
    assert "timeout" in text
    assert logged and "URL lỗi" in logged[0]
    assert (
        write_failed_download_urls_log(
            platform="youtube",
            job_id="dl_abc123",
            failed_pairs=[],
        )
        is None
    )
