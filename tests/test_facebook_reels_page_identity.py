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
    assert facebook_page_identity("https://www.facebook.com/310447015484122") == "id:310447015484122"
    assert facebook_page_identity("https://www.facebook.com/310447015484122/videos/") == "id:310447015484122"


def test_global_reels_has_no_page_identity() -> None:
    assert facebook_page_identity("https://www.facebook.com/reels/") == ""
    assert facebook_page_identity("https://www.facebook.com/reel/999") == ""
    assert facebook_page_identity("https://www.facebook.com/watch") == ""


def test_href_belongs_to_source_page_rejects_other_vanity() -> None:
    from src.services.facebook_reels_catalog import href_belongs_to_source_page

    src = "https://www.facebook.com/healthylifehub2026"
    assert href_belongs_to_source_page(
        "https://www.facebook.com/healthylifehub2026/videos/2482358802253174", src
    )
    assert href_belongs_to_source_page("https://www.facebook.com/reel/2482358802253174", src)
    assert not href_belongs_to_source_page(
        "https://www.facebook.com/reel/2482358802253174", src, allow_bare=False
    )
    assert not href_belongs_to_source_page(
        "https://www.facebook.com/OtherPage/videos/2482358802253174", src
    )


def test_merge_owner_verified_drops_bare_suggestion_ids() -> None:
    from src.services.facebook_reels_catalog import (
        merge_owner_verified_video_ids,
        resolve_newest_page_video_ids,
    )

    # Lưới: mới → cũ; harvest xác nhận chủ — bỏ ID gợi ý giữa các ô đã xác nhận.
    reel, video = merge_owner_verified_video_ids(
        harvest_ids=["2482358802253174", "1156958673946775"],
        owned_ids=["2482358802253174", "999999999999999", "1156958673946775"],
        video_tab_ids=["2230774414426929", "1156958673946775"],
    )
    assert reel[0] == "2482358802253174"
    assert reel[1] == "1156958673946775"
    assert "999999999999999" not in reel
    assert "2230774414426929" not in video
    assert "650496281688565899" not in reel

    only_grid = resolve_newest_page_video_ids(
        visual_ids=["2482358802253174", "1156958673946775"],
        harvest_ids=[],
        max_ids=5,
    )
    assert only_grid == ["2482358802253174", "1156958673946775"]

    # GraphQL-only không được đội lên đầu (tránh video cũ giả mới).
    mixed = resolve_newest_page_video_ids(
        visual_ids=["1156958673946775"],
        harvest_ids=["2482358802253174", "1156958673946775"],
        max_ids=5,
    )
    assert mixed == ["1156958673946775"]

    # Video mới trên đầu lưới, GraphQL chưa kịp — vẫn lấy.
    fresh_top = resolve_newest_page_video_ids(
        visual_ids=["27741558852187173", "2482358802253174", "1156958673946775"],
        harvest_ids=["2482358802253174", "1156958673946775"],
        max_ids=5,
    )
    assert fresh_top[0] == "27741558852187173"
    assert fresh_top[1] == "2482358802253174"


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


def test_wait_profile_unlocked_times_out_while_locked(tmp_path) -> None:
    """Profile đang mở → chờ hết giờ trả False (không clone / không mở Firefox 2)."""
    from src.services.facebook_reels_catalog import _wait_profile_unlocked

    src = tmp_path / "profile"
    src.mkdir()
    (src / "parent.lock").write_text("locked", encoding="utf-8")
    notes: list[str] = []
    ok = _wait_profile_unlocked(
        src,
        "firefox",
        timeout_sec=0.6,
        poll_sec=0.2,
        status=notes.append,
    )
    assert ok is False
    assert notes and "không mở Firefox thứ 2" in notes[0]


def test_wait_profile_unlocked_ok_when_free(tmp_path) -> None:
    from src.services.facebook_reels_catalog import _wait_profile_unlocked

    src = tmp_path / "profile"
    src.mkdir()
    assert _wait_profile_unlocked(src, "firefox", timeout_sec=2.0, poll_sec=0.1) is True
