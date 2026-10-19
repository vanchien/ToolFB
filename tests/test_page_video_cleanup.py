"""Quét/lọc/chuẩn hoá list video Page trước khi xóa trên Facebook."""

from __future__ import annotations

from datetime import datetime

from src.services.page_video_cleanup import (
    candidate_video_open_urls,
    classify_page_video_url,
    content_library_urls,
    enrich_items_from_fingerprints,
    filter_page_videos,
    href_belongs_to_page,
    is_page_post_delete_label,
    library_row_matches_item,
    library_row_matches_wanted,
    normalize_scanned_video_items,
    normalize_video_duration,
    parse_content_library_row,
    resolve_page_cleanup_target,
    sanitize_scanned_video_items,
)


def test_classify_reel_and_video_urls() -> None:
    reel = classify_page_video_url("https://www.facebook.com/reel/123456789012345")
    assert reel["kind"] == "Reel"
    assert reel["video_id"] == "123456789012345"
    assert reel["url"].endswith("/reel/123456789012345")

    video = classify_page_video_url("https://www.facebook.com/SomePage/videos/98765432109876")
    assert video["kind"] == "Video"
    assert video["video_id"] == "98765432109876"


def test_normalize_dedupes_and_keeps_real_ids() -> None:
    items = normalize_scanned_video_items(
        [
            {"video_id": "1", "url": "https://www.facebook.com/reel/11111111111111"},
            {"video_id": "2", "url": "https://www.facebook.com/reel/11111111111111"},
            "https://www.facebook.com/foo/videos/22222222222222",
            "",
        ]
    )
    assert [x["video_id"] for x in items] == ["11111111111111", "22222222222222"]
    assert items[0]["kind"] == "Reel"
    assert items[1]["kind"] == "Video"


def test_resolve_target_uses_numeric_page_url_and_meta_id() -> None:
    t = resolve_page_cleanup_target(
        {
            "page_name": "Metro Music Extra",
            "page_url": "https://www.facebook.com/310447015484122",
            "fb_page_id": "310447015484122",
            "account_id": "acc1",
        }
    )
    assert t["fb_page_id"] == "310447015484122"
    assert t["identity"] == "id:310447015484122"
    assert t["account_id"] == "acc1"


def test_href_belongs_only_to_selected_page() -> None:
    page = "https://www.facebook.com/310447015484122"
    live = "https://www.facebook.com/MetroMusicExtra/reels/"
    fb_id = "310447015484122"
    assert href_belongs_to_page(
        "https://www.facebook.com/310447015484122/videos/55555555555555",
        expected_page_url=page,
        live_page_url=live,
        fb_page_id=fb_id,
    )
    assert href_belongs_to_page(
        "/reel/11111111111111",
        expected_page_url=page,
        live_page_url=live,
        fb_page_id=fb_id,
        allow_bare_reel=True,
    )
    assert not href_belongs_to_page(
        "https://www.facebook.com/OtherPage/videos/99999999999999",
        expected_page_url=page,
        live_page_url=live,
        fb_page_id=fb_id,
        allow_bare_reel=True,
    )
    assert not href_belongs_to_page(
        "https://www.facebook.com/reels/",
        expected_page_url=page,
        live_page_url=live,
        fb_page_id=fb_id,
        allow_bare_reel=True,
    )


def test_content_library_url_not_reels_tab() -> None:
    from src.services.page_video_cleanup import is_content_library_url

    assert is_content_library_url(
        "https://www.facebook.com/professional_dashboard/content/content_library/?asset_id=1"
    )
    assert not is_content_library_url("https://www.facebook.com/111490197685020/reels/")
    assert not is_content_library_url("https://www.facebook.com/watch/?v=123")


def test_content_library_urls_include_asset_id() -> None:
    urls = content_library_urls("310447015484122")
    assert "professional_dashboard/content/content_library" in urls[0]
    assert any("date_preset=lifetime" in u for u in urls)
    assert any("published_posts" in u and "asset_id=310447015484122" in u for u in urls)
    assert not urls[0].startswith("https://business.facebook.com")


def test_parse_content_library_row_video_with_duration() -> None:
    rec = parse_content_library_row(
        {
            "text": "No text content 3m 11s 0 2 0 0 Yesterday at 11:51 PM 0 0 0",
            "hrefs": ["https://www.facebook.com/reel/555555555555555"],
            "aria": "4",
        },
        page_url="https://www.facebook.com/310447015484122",
    )
    assert rec is not None
    assert rec["kind"] == "Reel"
    assert rec["video_id"] == "555555555555555"
    assert rec["published_ts"].isdigit()
    assert rec["published"].count("-") == 2
    assert rec["duration"] == "3m 11s"


def test_parse_content_library_skips_header_row() -> None:
    assert (
        parse_content_library_row(
            {"text": "Title Views Reach Published Engagement", "hrefs": [], "aria": "1"}
        )
        is None
    )


def test_parse_skips_row_without_real_video_id() -> None:
    assert (
        parse_content_library_row(
            {
                "text": "+0.2x 2026-10-17 09:56 --",
                "hrefs": [],
                "cells": ["+0.2x", "2026-10-17 09:56", "--"],
                "aria": "3",
            }
        )
        is None
    )


def test_parse_keeps_thumb_and_real_title() -> None:
    rec = parse_content_library_row(
        {
            "text": "+0.2x No text content 0:15 2026-10-17 09:56",
            "hrefs": ["https://www.facebook.com/reel/2482358802253174"],
            "cells": ["+0.2x", "No text content", "0:15", "2026-10-17 09:56"],
            "thumb": "https://scontent.xx.fbcdn.net/v/t15/a.jpg",
            "html_ids": ["2482358802253174"],
        }
    )
    assert rec is not None
    assert rec["video_id"] == "2482358802253174"
    assert rec["title"] == "No text content"
    assert rec["thumb_url"].endswith("a.jpg")
    assert rec["kind"] == "Reel"


def test_sanitize_drops_date_shaped_fake_ids() -> None:
    rows = sanitize_scanned_video_items(
        [
            {"video_id": "2026101710011001", "url": "library://x", "title": "+0.2x"},
            {
                "video_id": "2482358802253174",
                "url": "https://www.facebook.com/reel/2482358802253174",
                "title": "Reel",
            },
        ]
    )
    assert [x["video_id"] for x in rows] == ["2482358802253174"]
    assert rows[0]["title"] == ""


def test_library_row_matches_id_in_html_and_thumb() -> None:
    vid = "2482358802253174"
    assert library_row_matches_wanted(
        {
            "text": "No text content 0:15 Yesterday",
            "hrefs": [],
            "html_ids": [vid],
            "thumb": "",
        },
        {vid},
    )
    assert library_row_matches_wanted(
        {
            "text": "Reel",
            "hrefs": [],
            "html_ids": [],
            "thumb": f"https://scontent.xx.fbcdn.net/v/t15/{vid}_n.jpg",
        },
        {vid},
    )
    assert not library_row_matches_wanted(
        {"text": "+0.2x 2026-10-17", "hrefs": [], "html_ids": [], "thumb": ""},
        {vid},
    )


def test_post_delete_label_skips_comment_delete() -> None:
    assert not is_page_post_delete_label("Delete")
    assert not is_page_post_delete_label("Delete comment")
    assert not is_page_post_delete_label("Hide comment")
    assert is_page_post_delete_label("Move to trash")
    assert is_page_post_delete_label("Delete post")
    assert is_page_post_delete_label("Delete reel")
    assert is_page_post_delete_label("Delete posts")
    assert is_page_post_delete_label("Xóa bài viết")
    assert is_page_post_delete_label("Chuyển vào thùng rác")


def test_normalize_duration_variants() -> None:
    assert normalize_video_duration("3m 11s") == "3:11"
    assert normalize_video_duration("0:15") == "0:15"
    assert normalize_video_duration("15s") == "0:15"


def test_library_row_matches_by_duration_and_published() -> None:
    ts = str(int(datetime(2026, 10, 17, 20, 28).timestamp()))
    item = {
        "video_id": "2482358802253174",
        "duration": "3m 11s",
        "published": "2026-10-17 20:28",
        "published_ts": ts,
        "published_raw": "Yesterday at 8:28 PM",
        "title": "No text content",
    }
    raw = {
        "text": "No text content 3m 11s Yesterday at 8:28 PM",
        "hrefs": [],
        "html_ids": [],
        "thumb": "",
    }
    assert library_row_matches_item(raw, item)
    assert not library_row_matches_item(
        {"text": "Other 0:09 Today at 1:00 AM", "hrefs": [], "html_ids": [], "thumb": ""},
        item,
    )


def test_enrich_attaches_duration_from_fingerprint() -> None:
    items = [{"video_id": "2482358802253174", "title": "", "duration": ""}]
    fps = [
        {
            "video_id": "2482358802253174",
            "html_ids": "2482358802253174",
            "duration": "0:15",
            "published_raw": "Yesterday at 8:28 PM",
        }
    ]
    out = enrich_items_from_fingerprints(items, fps)
    assert out[0]["duration"] == "0:15"
    assert "Yesterday" in out[0]["published_raw"]


def test_candidate_open_urls_cover_reel_watch_and_page() -> None:
    urls = candidate_video_open_urls(
        {"video_id": "2482358802253174", "url": "https://www.facebook.com/reel/2482358802253174"},
        fb_page_id="111490197685020",
        page_url="https://www.facebook.com/111490197685020",
    )
    blob = " ".join(urls)
    assert "/reel/2482358802253174" in blob
    assert "watch/?v=2482358802253174" in blob
    assert "/111490197685020/videos/2482358802253174" in blob


def test_filter_page_videos_by_id_kind_and_url() -> None:
    rows = [
        {"video_id": "111", "kind": "Reel", "url": "https://www.facebook.com/reel/111"},
        {"video_id": "222", "kind": "Video", "url": "https://www.facebook.com/p/videos/222"},
    ]
    assert len(filter_page_videos(rows, "")) == 2
    assert [x["video_id"] for x in filter_page_videos(rows, "222")] == ["222"]
    assert [x["kind"] for x in filter_page_videos(rows, "reel")] == ["Reel"]
    assert filter_page_videos(rows, "không có") == []
