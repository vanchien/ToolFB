"""Harvest GraphQL: chỉ ID video của Page, không dính Page ID / video chủ khác."""

from datetime import datetime

from src.services.page_video_harvest import (
    expand_lifetime_in_graphql_body,
    harvest_video_records_from_obj,
    harvest_video_records_from_text,
    is_real_facebook_video_id,
    merge_video_item_lists,
    parse_facebook_publish_stamp,
    response_looks_like_facebook_graphql,
    sort_videos_by_published,
)


def test_harvest_takes_playable_id_of_page_owner() -> None:
    payload = {
        "data": {
            "node": {
                "__typename": "Video",
                "id": "555555555555555",
                "playable_id": "555555555555555",
                "owner": {"id": "102949712869335"},
            }
        }
    }
    recs = harvest_video_records_from_obj(payload, fb_page_id="102949712869335")
    assert [r["video_id"] for r in recs] == ["555555555555555"]
    assert recs[0]["kind"] in {"Reel", "Video"}


def test_harvest_skips_other_page_owner_and_page_id() -> None:
    payload = {
        "data": {
            "nodes": [
                {
                    "__typename": "Video",
                    "playable_id": "999999999999999",
                    "owner": {"id": "111111111111111"},
                },
                {
                    "__typename": "Video",
                    "playable_id": "102949712869335",
                    "owner": {"id": "102949712869335"},
                },
            ]
        }
    }
    recs = harvest_video_records_from_obj(payload, fb_page_id="102949712869335")
    ids = {r["video_id"] for r in recs}
    assert "999999999999999" not in ids
    assert "102949712869335" not in ids


def test_expand_graphql_preset_to_lifetime_and_old_since() -> None:
    now = 1_760_000_000
    body = '{"timeRange":{"preset":"LAST_28_DAYS"},"since":1750000000}'
    out = expand_lifetime_in_graphql_body(body, years=10, now_ts=now)
    assert "LIFETIME" in out
    assert "LAST_28_DAYS" not in out
    assert str(now - 10 * 365 * 24 * 3600) in out


def test_harvest_strips_facebook_ajax_prefix() -> None:
    body = (
        'for (;;);{"data":{"node":{"__typename":"Video",'
        '"playable_id":"555555555555555","owner":{"id":"102949712869335"}}}}'
    )
    recs = harvest_video_records_from_text(body, fb_page_id="102949712869335")
    assert recs[0]["video_id"] == "555555555555555"


def test_harvest_vanity_owner_keeps_page_video() -> None:
    payload = {
        "data": {
            "node": {
                "__typename": "Video",
                "playable_id": "2482358802253174",
                "owner": {"id": "999000111222", "username": "healthylifehub2026"},
            }
        }
    }
    recs = harvest_video_records_from_obj(
        payload,
        fb_page_vanity="healthylifehub2026",
        require_owner=True,
        trusted_surface=False,
    )
    assert [r["video_id"] for r in recs] == ["2482358802253174"]


def test_harvest_require_owner_skips_other_vanity_and_bare_reel() -> None:
    other = {
        "data": {
            "node": {
                "__typename": "Video",
                "playable_id": "2482358802253174",
                "owner": {"username": "OtherPage"},
            }
        }
    }
    recs = harvest_video_records_from_obj(
        other,
        fb_page_vanity="healthylifehub2026",
        require_owner=True,
        trusted_surface=False,
    )
    assert recs == []
    bare = harvest_video_records_from_text(
        '{"permalink":"https://www.facebook.com/reel/888888888888888"}',
        fb_page_vanity="healthylifehub2026",
        require_owner=True,
        trusted_surface=False,
    )
    assert bare == []


def test_harvest_vanity_in_permalink_counts_as_owned() -> None:
    recs = harvest_video_records_from_text(
        '{"permalink":"https://www.facebook.com/healthylifehub2026/videos/2482358802253174"}',
        fb_page_vanity="healthylifehub2026",
        require_owner=True,
        trusted_surface=False,
    )
    assert recs[0]["video_id"] == "2482358802253174"


def test_harvest_reel_url_in_text() -> None:
    recs = harvest_video_records_from_text(
        '{"permalink":"https://www.facebook.com/reel/888888888888888"}',
        fb_page_id="1",
    )
    assert recs[0]["video_id"] == "888888888888888"
    assert recs[0]["kind"] == "Reel"


def test_merge_prefers_library_title() -> None:
    merged = merge_video_item_lists(
        [{"video_id": "1", "kind": "Reel", "url": "https://www.facebook.com/reel/1", "title": "", "published": ""}],
        [
            {
                "video_id": "1",
                "kind": "Reel",
                "url": "https://www.facebook.com/reel/1",
                "title": "No text content",
                "published": "Yesterday",
            }
        ],
    )
    assert len(merged) == 1
    assert merged[0]["title"] == "No text content"
    assert merged[0]["published"] == "Yesterday"


def test_parse_yesterday_and_unix_publish() -> None:
    now = datetime(2026, 10, 18, 13, 0, 0)
    ts, label = parse_facebook_publish_stamp("Yesterday at 8:28 PM", now=now)
    assert ts.isdigit()
    assert label == "2026-10-17 20:28"
    ts2, label2 = parse_facebook_publish_stamp(1_729_180_000)
    assert ts2 == "1729180000"
    assert "-" in label2


def test_harvest_keeps_creation_time() -> None:
    payload = {
        "data": {
            "node": {
                "__typename": "Video",
                "playable_id": "555555555555555",
                "creation_time": 1729180800,
                "owner": {"id": "102949712869335"},
            }
        }
    }
    recs = harvest_video_records_from_obj(payload, fb_page_id="102949712869335")
    assert recs[0]["published_ts"] == "1729180800"
    assert recs[0]["published"]


def test_sort_oldest_to_newest() -> None:
    rows = [
        {"video_id": "3", "published_ts": "300", "kind": "Reel", "url": "u3"},
        {"video_id": "1", "published_ts": "100", "kind": "Reel", "url": "u1"},
        {"video_id": "2", "published_ts": "200", "kind": "Reel", "url": "u2"},
    ]
    old_first = sort_videos_by_published(rows, newest_first=False)
    assert [r["video_id"] for r in old_first] == ["1", "2", "3"]
    new_first = sort_videos_by_published(rows, newest_first=True)
    assert [r["video_id"] for r in new_first] == ["3", "2", "1"]


def test_sort_newest_does_not_put_undated_old_ahead_of_recent() -> None:
    """Thiếu ngày đăng không được reverse lên trước video mới có mốc."""
    rows = [
        {"video_id": "1111111111", "published_ts": "1700000000", "kind": "Reel", "url": "u1"},
        {"video_id": "999999999999999", "published_ts": "", "kind": "Reel", "url": "u2"},
        {"video_id": "2222222222", "published_ts": "1760000000", "kind": "Reel", "url": "u3"},
    ]
    ids = [r["video_id"] for r in sort_videos_by_published(rows, newest_first=True)]
    assert ids[0] == "2222222222"
    assert ids[1] == "1111111111"


def test_rejects_date_shaped_fake_ids() -> None:
    assert is_real_facebook_video_id("2482358802253174")
    assert is_real_facebook_video_id("1156958673946775")
    assert is_real_facebook_video_id("27741558852187173")
    assert not is_real_facebook_video_id("650496281688565899")
    assert not is_real_facebook_video_id("2026101710011001")
    assert not is_real_facebook_video_id("202610180800800")
    assert not is_real_facebook_video_id("02x202610170956")
    assert not is_real_facebook_video_id("Reel")


def test_harvest_extracts_duration() -> None:
    payload = {
        "data": {
            "node": {
                "__typename": "Video",
                "playable_id": "555555555555555",
                "length_in_second": 191,
                "owner": {"id": "102949712869335"},
            }
        }
    }
    recs = harvest_video_records_from_obj(payload, fb_page_id="102949712869335")
    assert recs[0]["duration"] == "3:11"


def test_harvest_extracts_thumbnail_and_title() -> None:
    payload = {
        "data": {
            "node": {
                "__typename": "Video",
                "playable_id": "555555555555555",
                "title": {"text": "Chiller night"},
                "image": {"uri": "https://scontent.xx.fbcdn.net/v/t15.5256-10/thumb.jpg"},
                "owner": {"id": "102949712869335"},
            }
        }
    }
    recs = harvest_video_records_from_obj(payload, fb_page_id="102949712869335")
    assert recs[0]["thumb_url"].endswith("thumb.jpg")
    assert recs[0]["title"] == "Chiller night"


def test_merge_keeps_better_title_and_thumb() -> None:
    merged = merge_video_item_lists(
        [{"video_id": "1", "kind": "Reel", "url": "u", "title": "Reel", "thumb_url": ""}],
        [
            {
                "video_id": "1",
                "kind": "Reel",
                "url": "u",
                "title": "Đêm chiller",
                "thumb_url": "https://scontent.xx.fbcdn.net/t.jpg",
            }
        ],
    )
    assert merged[0]["title"] == "Đêm chiller"
    assert merged[0]["thumb_url"].endswith("t.jpg")


def test_graphql_url_filter() -> None:
    assert response_looks_like_facebook_graphql("https://www.facebook.com/api/graphql/")
    assert not response_looks_like_facebook_graphql("https://www.facebook.com/images/x.png")
    assert not response_looks_like_facebook_graphql("https://example.com/api/graphql")
