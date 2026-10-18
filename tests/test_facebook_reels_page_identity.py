"""Đúng Page nguồn khi quét Reels — không nhảy sang Reels toàn cục / trang khác."""

from __future__ import annotations

from src.services.facebook_reels_catalog import (
    _clone_ignore,
    _clone_profile_for_scan,
    facebook_page_identity,
    normalize_facebook_reels_tab_url,
    url_matches_facebook_page,
)
from src.services.facebook_groups.source_watch import parse_source_urls
import pytest


def test_page_identity_vanity_and_id() -> None:
    assert facebook_page_identity("https://www.facebook.com/MyCoolPage") == "vanity:mycoolpage"
    assert facebook_page_identity("https://www.facebook.com/MyCoolPage/reels/") == "vanity:mycoolpage"
    assert (
        facebook_page_identity("https://www.facebook.com/profile.php?id=123456789")
        == "id:123456789"
    )


def test_global_reels_has_no_page_identity() -> None:
    assert facebook_page_identity("https://www.facebook.com/reels/") == ""
    assert facebook_page_identity("https://www.facebook.com/reel/999") == ""
    assert facebook_page_identity("https://www.facebook.com/watch") == ""


def test_url_matches_same_page_only() -> None:
    src = "https://www.facebook.com/SourcePage"
    assert url_matches_facebook_page("https://www.facebook.com/SourcePage/reels/", src)
    assert not url_matches_facebook_page("https://www.facebook.com/reels/", src)
    assert not url_matches_facebook_page("https://www.facebook.com/OtherPage/reels/", src)


def test_normalize_reels_keeps_vanity() -> None:
    assert normalize_facebook_reels_tab_url("https://www.facebook.com/SourcePage") == (
        "https://www.facebook.com/SourcePage/reels/"
    )


def test_parse_source_rejects_global_reel_link() -> None:
    with pytest.raises(ValueError, match="Page nguồn"):
        parse_source_urls("https://www.facebook.com/reel/123456789012")
    assert parse_source_urls("https://www.facebook.com/SourcePage") == [
        "https://www.facebook.com/SourcePage"
    ]


def test_clone_profile_skips_lock_and_cache(tmp_path) -> None:
    """Clone quét không copy parent.lock / cache2 — tránh WinError khi Firefox đang mở."""
    src = tmp_path / "profile"
    (src / "cache2" / "entries").mkdir(parents=True)
    (src / "cache2" / "entries" / "blob").write_bytes(b"x" * 100)
    (src / "parent.lock").write_text("locked", encoding="utf-8")
    (src / "cookies.sqlite").write_bytes(b"session")
    assert "parent.lock" in _clone_ignore(str(src), ["parent.lock", "cookies.sqlite", "cache2"])
    dst = _clone_profile_for_scan(src)
    try:
        assert (dst / "cookies.sqlite").is_file()
        assert not (dst / "parent.lock").exists()
        assert not (dst / "cache2").exists()
    finally:
        import shutil

        shutil.rmtree(dst.parent, ignore_errors=True)
