"""Nhận diện URL Instagram cho tab Tải video."""

from __future__ import annotations

from src.services.universal_video_downloader import (
    augment_instagram_auth_message,
    classify_url_type,
    detect_platform,
)


def test_detect_instagram_reel() -> None:
    url = "https://www.instagram.com/reel/ABC123xyz/"
    assert detect_platform(url) == "instagram"
    assert classify_url_type(url) == "single_video"


def test_detect_instagram_profile() -> None:
    url = "https://www.instagram.com/somecreator/"
    assert detect_platform(url) == "instagram"
    assert classify_url_type(url) == "profile"


def test_detect_instagram_reels_tab() -> None:
    url = "https://www.instagram.com/somecreator/reels/"
    assert detect_platform(url) == "instagram"
    assert classify_url_type(url) == "profile"


def test_augment_instagram_login_hint() -> None:
    err = augment_instagram_auth_message(
        "https://www.instagram.com/user/",
        "login required",
    )
    assert "Instagram" in err
    assert "cookie" in err.lower()
