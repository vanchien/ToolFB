"""Unit test quét profile TikTok khi yt-dlp gãy secondary user ID."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.services.tiktok_profile_catalog import (
    extract_tiktok_video_urls,
    extract_tiktok_video_urls_from_json,
    normalize_tiktok_profile_url,
    scan_tiktok_profile_page,
    tiktok_username_from_url,
    tiktok_ytdlp_profile_broken,
)
from src.services.universal_video_downloader import UniversalYTDLPWrapper


def test_normalize_tiktok_profile_url() -> None:
    assert (
        normalize_tiktok_profile_url("https://www.tiktok.com/@catvermont6425?lang=en")
        == "https://www.tiktok.com/@catvermont6425"
    )
    assert tiktok_username_from_url("https://www.tiktok.com/@catvermont6425/video/1") == "catvermont6425"


def test_extract_tiktok_video_urls_from_hrefs() -> None:
    urls = extract_tiktok_video_urls(
        "catvermont6425",
        [
            "/@catvermont6425/video/1234567890123456789",
            "https://www.tiktok.com/@other/video/9876543210987654321",
            "/photo/not-a-video",
        ],
    )
    assert urls == [
        "https://www.tiktok.com/@catvermont6425/video/1234567890123456789",
        "https://www.tiktok.com/@other/video/9876543210987654321",
    ]


def test_extract_tiktok_video_urls_from_json() -> None:
    payload = {
        "itemList": [
            {
                "id": "1234567890123456789",
                "desc": "clip",
                "author": {"uniqueId": "catvermont6425"},
                "video": {},
            }
        ]
    }
    urls = extract_tiktok_video_urls_from_json(payload, "catvermont6425")
    assert urls == ["https://www.tiktok.com/@catvermont6425/video/1234567890123456789"]


def test_ytdlp_secondary_user_id_is_broken() -> None:
    err = (
        "ERROR: [TikTokUser] catvermont6425: Unable to extract secondary user ID. "
        "If you are able to get this URL working with yt-dlp, please create an issue"
    )
    assert tiktok_ytdlp_profile_broken(err) is True
    assert tiktok_ytdlp_profile_broken("") is False


def test_scan_tiktok_profile_page_collects_hrefs(monkeypatch) -> None:
    class _FakePage:
        url = "https://www.tiktok.com/@catvermont6425"

        def inner_text(self, *_a, **_k) -> str:
            return "catvermont6425"

        def eval_on_selector_all(self, *_a, **_k) -> list[str]:
            return ["/@catvermont6425/video/1234567890123456789"]

        def evaluate(self, *_a, **_k) -> list[str]:
            return ["https://www.tiktok.com/@catvermont6425/video/1234567890123456789"]

        def goto(self, *_a, **_k) -> None:
            return None

        def on(self, *_a, **_k) -> None:
            return None

    class _FakeContext:
        def new_page(self) -> _FakePage:
            return _FakePage()

        def close(self) -> None:
            return None

    class _FakeBrowser:
        def new_context(self, **_k) -> _FakeContext:
            return _FakeContext()

        def close(self) -> None:
            return None

    class _FakeChromium:
        def launch(self, **_k) -> _FakeBrowser:
            return _FakeBrowser()

    class _FakePW:
        chromium = _FakeChromium()

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    monkeypatch.setattr("src.services.tiktok_profile_catalog.sync_playwright", lambda: _FakePW())
    monkeypatch.setattr("src.services.tiktok_profile_catalog.time.sleep", lambda *_a: None)
    monkeypatch.setattr(
        "src.services.tiktok_profile_catalog.Stealth",
        lambda: type("S", (), {"apply_stealth_sync": lambda self, page: None})(),
    )
    res = scan_tiktok_profile_page(
        page_url="https://www.tiktok.com/@catvermont6425",
        max_scroll_rounds=4,
        max_videos=10,
    )
    assert res["ok"] is True
    assert res["items"][0]["url"].endswith("/video/1234567890123456789")


def test_flat_playlist_falls_back_when_ytdlp_cannot_read_user() -> None:
    wrapper = UniversalYTDLPWrapper(yt_cfg={}, log=lambda _m: None)
    err = "ERROR: Unable to extract secondary user ID."

    def _fake_popen(*_a, **_k):
        proc = MagicMock()
        proc.stdout = iter([])
        proc.stderr = iter([err + "\n"])
        proc.poll.return_value = 1
        proc.wait.return_value = 1
        return proc

    browser_res = {
        "success": True,
        "entries": [
            {
                "title": "TikTok 1",
                "url": "https://www.tiktok.com/@catvermont6425/video/1234567890123456789",
            }
        ],
        "playlist_title": "",
        "extractor": "tiktok_playwright",
        "partial": False,
        "warning": "Quét bằng trình duyệt vì yt-dlp không lấy được danh sách kênh.",
    }
    with patch("src.services.universal_video_downloader.subprocess.Popen", side_effect=_fake_popen):
        with patch.object(wrapper, "_resolve_prefix", return_value=["yt-dlp"]):
            with patch.object(wrapper, "_scan_tiktok_profile_via_browser", return_value=browser_res):
                res = wrapper.list_flat_playlist_entries(
                    "https://www.tiktok.com/@catvermont6425",
                    max_entries=20,
                )
    assert res["success"] is True
    assert res["entries"][0]["url"].endswith("/video/1234567890123456789")
    assert res["extractor"] == "tiktok_playwright"
