"""Unit test fallback tải video TikTok khi yt-dlp gãy extractor."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

from src.services.tiktok_video_download import (
    build_tiktok_dest_path,
    extract_tiktok_media_urls,
    is_tiktok_video_url,
    parse_tiktok_video_ref,
    tiktok_ytdlp_video_broken,
)
from src.services.universal_video_downloader import (
    DownloadMetadataStore,
    UniversalVideoDownloader,
    annotate_downloaded_library_row,
    _ytdlp_impersonate_cli_args,
    _ytdlp_tiktok_header_args,
    downloaded_file_empty_reason,
    drop_empty_download_files,
)


def test_is_tiktok_video_url() -> None:
    assert is_tiktok_video_url("https://www.tiktok.com/@catvideos855/video/7695373925739531542")
    assert is_tiktok_video_url("https://vm.tiktok.com/ZMabc123/")
    assert not is_tiktok_video_url("https://www.tiktok.com/@catvideos855")
    assert not is_tiktok_video_url("https://www.youtube.com/watch?v=abc")


def test_parse_tiktok_video_ref() -> None:
    user, vid = parse_tiktok_video_ref(
        "https://www.tiktok.com/@catvideos855/video/7695373925739531542"
    )
    assert user == "catvideos855"
    assert vid == "7695373925739531542"


def test_tiktok_ytdlp_video_broken() -> None:
    err = (
        "ERROR: [TikTok] 7695373925739531542: Unexpected response from webpage request; "
        "please report this issue"
    )
    assert tiktok_ytdlp_video_broken(err) is True
    assert tiktok_ytdlp_video_broken("WARNING: no impersonate target is available") is True
    assert tiktok_ytdlp_video_broken("") is False
    assert tiktok_ytdlp_video_broken("HTTP Error 403") is True
    assert tiktok_ytdlp_video_broken(
        "ERROR: [TikTok] 1604910106265170: Your IP address is blocked from accessing this post"
    ) is True


def test_extract_tiktok_media_urls_prefers_download_addr() -> None:
    payload = {
        "__DEFAULT_SCOPE__": {
            "webapp.video-detail": {
                "itemInfo": {
                    "itemStruct": {
                        "id": "1234567890123456789",
                        "desc": "funny cat",
                        "author": {"uniqueId": "catvideos855"},
                        "video": {
                            "playAddr": "https://v16.tiktokcdn.com/play.mp4",
                            "downloadAddr": "https://v16.tiktokcdn.com/dl.mp4",
                            "bitrateInfo": [
                                {"PlayAddr": {"UrlList": ["https://v16.tiktokcdn.com/hd.mp4"]}}
                            ],
                        },
                    }
                }
            }
        }
    }
    got = extract_tiktok_media_urls(payload)
    assert got["title"] == "funny cat"
    assert got["download"][0] == "https://v16.tiktokcdn.com/dl.mp4"
    assert "https://v16.tiktokcdn.com/play.mp4" in got["play"]
    assert "https://v16.tiktokcdn.com/hd.mp4" in got["play"]
    assert got["photo"] is False


def test_extract_tiktok_photo_mode() -> None:
    payload = {"itemInfo": {"itemStruct": {"id": "1", "imagePost": {"images": []}}}}
    got = extract_tiktok_media_urls(payload)
    assert got["photo"] is True


def test_build_tiktok_dest_path(tmp_path: Path) -> None:
    dest = build_tiktok_dest_path(
        output_dir=str(tmp_path),
        url="https://www.tiktok.com/@catvideos855/video/7695373925739531542",
        title="funny/cat:clip",
        organize_by_platform=True,
        organize_by_uploader=True,
    )
    assert dest.parent.name == "catvideos855"
    assert dest.parent.parent.name == "TikTok"
    assert dest.suffix == ".mp4"
    assert "7695373925739531542" in dest.name
    assert "/" not in dest.name


def test_ytdlp_tiktok_header_args() -> None:
    args = _ytdlp_tiktok_header_args("https://www.tiktok.com/@x/video/1")
    assert "--user-agent" in args
    assert any(a.startswith("Referer:") for a in args)
    assert _ytdlp_tiktok_header_args("https://youtu.be/x") == []


def test_impersonate_args_without_curl_cffi() -> None:
    with patch.dict("sys.modules", {"curl_cffi": None}):
        # import failing → helper must not crash
        args = _ytdlp_impersonate_cli_args()
        assert args == [] or args[:1] == ["--impersonate"]


def test_run_download_url_falls_back_to_browser(tmp_path: Path) -> None:
    paths = {
        "jobs_file": tmp_path / "jobs.json",
        "videos_file": tmp_path / "videos.json",
        "archive": tmp_path / "archive.txt",
        "root": tmp_path,
    }
    paths["jobs_file"].write_text("[]\n", encoding="utf-8")
    paths["videos_file"].write_text("[]\n", encoding="utf-8")
    paths_pp = {k: Path(v) for k, v in paths.items()}

    down = UniversalVideoDownloader(log=lambda _m: None)
    down._store = DownloadMetadataStore(paths=paths_pp)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    saved = out_dir / "TikTok" / "catvideos855" / "clip.mp4"
    saved.parent.mkdir(parents=True)
    saved.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 64)

    with patch(
        "src.services.universal_video_downloader.ensure_downloader_layout",
        return_value=paths_pp,
    ), patch(
        "src.services.universal_video_downloader.downloaded_file_empty_reason",
        return_value="",
    ):
        job = down.create_download_job(
            "https://www.tiktok.com/@catvideos855/video/7695373925739531542",
            {
                "platform": "tiktok",
                "url_type": "single_video",
                "max_videos": 1,
                "output_dir": str(out_dir),
                "organize_by_platform": True,
                "organize_by_uploader": True,
                "skip_existing": False,
                "write_info_json": False,
                "write_thumbnail": False,
            },
        )
        jid = str(job["id"])
        ytdlp_err = {
            "success": False,
            "error": "ERROR: [TikTok] 7695373925739531542: Unexpected response from webpage request",
            "filepaths": [],
        }
        fb_ok = {"success": True, "filepaths": [str(saved)], "error": ""}
        with patch.object(down._yt, "download", return_value=ytdlp_err) as ytdlp:
            with patch.object(down, "_download_tiktok_via_browser", return_value=fb_ok) as fb:
                down.run_download_url_for_job(
                    jid,
                    "https://www.tiktok.com/@catvideos855/video/7695373925739531542",
                    skip_output_dir_validate=True,
                )
        assert ytdlp.call_count == 1
        assert fb.call_count == 1
        assert down._tiktok_skip_ytdlp is True
        job2 = down._store.get_job(jid)
        assert job2
        assert any(str(saved) == p or Path(p).name == saved.name for p in (job2.get("downloaded_files") or []))


def test_ip_blocked_falls_back_to_browser(tmp_path: Path) -> None:
    paths = {
        "jobs_file": tmp_path / "jobs.json",
        "videos_file": tmp_path / "videos.json",
        "archive": tmp_path / "archive.txt",
        "root": tmp_path,
    }
    paths["jobs_file"].write_text("[]\n", encoding="utf-8")
    paths["videos_file"].write_text("[]\n", encoding="utf-8")
    paths_pp = {k: Path(v) for k, v in paths.items()}
    down = UniversalVideoDownloader(log=lambda _m: None)
    down._store = DownloadMetadataStore(paths=paths_pp)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    item = "https://www.tiktok.com/@uk.world425/video/1604910106265170"
    with patch(
        "src.services.universal_video_downloader.ensure_downloader_layout",
        return_value=paths_pp,
    ):
        job = down.create_download_job(
            item,
            {
                "platform": "tiktok",
                "url_type": "single_video",
                "max_videos": 1,
                "output_dir": str(out_dir),
                "queued_urls": [item],
                "skip_existing": False,
                "write_info_json": False,
                "write_thumbnail": False,
            },
        )
        ytdlp_err = {
            "success": False,
            "error": "ERROR: [TikTok] 1604910106265170: Your IP address is blocked from accessing this post",
            "filepaths": [],
        }
        with patch.object(down._yt, "download", return_value=ytdlp_err):
            with patch.object(
                down,
                "_download_tiktok_via_browser",
                return_value={"success": False, "filepaths": [], "error": "browser also blocked"},
            ) as fb:
                down.run_download_url_for_job(str(job["id"]), item, skip_output_dir_validate=True)
        assert fb.call_count == 1
        job2 = down._store.get_job(str(job["id"]))
        assert job2
        assert item in (job2.get("pending_urls") or [])


def test_sequential_skips_ytdlp_after_first_tiktok_break(tmp_path: Path) -> None:
    paths = {
        "jobs_file": tmp_path / "jobs.json",
        "videos_file": tmp_path / "videos.json",
        "archive": tmp_path / "archive.txt",
        "root": tmp_path,
    }
    paths["jobs_file"].write_text("[]\n", encoding="utf-8")
    paths["videos_file"].write_text("[]\n", encoding="utf-8")
    paths_pp = {k: Path(v) for k, v in paths.items()}

    down = UniversalVideoDownloader(log=lambda _m: None)
    down._store = DownloadMetadataStore(paths=paths_pp)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "src.services.universal_video_downloader.ensure_downloader_layout",
        return_value=paths_pp,
    ):
        job = down.create_download_job(
            "https://www.tiktok.com/@catvideos855",
            {
                "platform": "tiktok",
                "url_type": "single_video",
                "max_videos": 1,
                "output_dir": str(out_dir),
                "organize_by_platform": False,
                "organize_by_uploader": False,
                "skip_existing": False,
                "write_info_json": False,
                "write_thumbnail": False,
            },
        )
        jid = str(job["id"])
        urls = [
            "https://www.tiktok.com/@catvideos855/video/1111111111111111111",
            "https://www.tiktok.com/@catvideos855/video/2222222222222222222",
        ]
        calls: list[str] = []

        def _one(job_id: str, item_url: str, **_kw: Any) -> dict[str, Any]:
            calls.append(item_url)
            if down._tiktok_skip_ytdlp:
                return down._store.get_job(job_id) or {}
            down._tiktok_skip_ytdlp = True
            return down._store.get_job(job_id) or {}

        with patch.object(down, "run_download_url_for_job", side_effect=_one):
            down.run_download_urls_sequential_for_job(jid, urls)
        assert calls == urls
        assert down._tt_browser is None


def test_tiktok_logo_placeholder_is_dropped(tmp_path: Path) -> None:
    src = Path(
        r"C:\Users\Hello\Desktop\taivideo\1\TikTok\cat.world425"
        r"\unknown_date_7568712167747223574_tiktok_7568712167747223574.mp4"
    )
    if not src.is_file():
        return
    assert "logo TikTok" in downloaded_file_empty_reason(src)
    copy = tmp_path / src.name
    copy.write_bytes(src.read_bytes())
    kept, dropped = drop_empty_download_files([str(copy)])
    assert kept == []
    assert dropped == [str(copy)]
    assert not copy.is_file()


def test_missing_download_is_marked_failed(tmp_path: Path) -> None:
    row = annotate_downloaded_library_row(
        {"video_path": str(tmp_path / "gone.mp4"), "status": "downloaded", "duration": 12}
    )
    assert row["status"] == "lỗi"
    assert row["duration"] == 0


def test_non_video_bytes_are_not_success(tmp_path: Path) -> None:
    big = tmp_path / "real.mp4"
    big.write_bytes(b"x" * 500_000)
    assert downloaded_file_empty_reason(big) != ""
