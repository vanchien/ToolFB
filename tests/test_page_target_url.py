"""resolve_target_url_from_page_row / extract Facebook numeric id."""

from __future__ import annotations

from src.automation.facebook_actions import (
    default_meta_business_composer_url,
    entity_dict_from_pages_row,
    extract_facebook_numeric_id_from_url,
    merge_asset_id_into_business_composer_url,
    page_row_facebook_asset_id,
    resolve_target_url_from_page_row,
)


def test_extract_id_from_profile_php() -> None:
    u = "https://www.facebook.com/profile.php?id=1023267504212506"
    assert extract_facebook_numeric_id_from_url(u) == "1023267504212506"


def test_extract_id_from_numeric_path() -> None:
    u = "https://www.facebook.com/1023267504212506"
    assert extract_facebook_numeric_id_from_url(u) == "1023267504212506"


def test_merge_asset_into_composer() -> None:
    base = "https://business.facebook.com/latest/composer/?ref=POSTS"
    out = merge_asset_id_into_business_composer_url(base, "1023267504212506")
    assert "asset_id=1023267504212506" in out
    assert "ref=POSTS" in out


def test_resolve_composer_url_fills_asset_from_fb_page_id() -> None:
    row = {
        "id": "x",
        "account_id": "a",
        "page_name": "Test",
        "page_url": "https://business.facebook.com/latest/composer/",
        "fb_page_id": "1023267504212506",
    }
    assert "asset_id=1023267504212506" in resolve_target_url_from_page_row(row)


def test_use_business_composer_builds_url() -> None:
    row = {
        "id": "x",
        "account_id": "a",
        "page_name": "Test",
        "page_url": "https://www.facebook.com/myvanity",
        "fb_page_id": "1023267504212506",
        "use_business_composer": True,
    }
    u = resolve_target_url_from_page_row(row)
    assert u == default_meta_business_composer_url("1023267504212506")


def test_entity_dict_uses_resolved_url() -> None:
    row = {
        "id": "x",
        "account_id": "a",
        "page_name": "Test",
        "page_url": "https://www.facebook.com/1023267504212506/posts",
        "page_kind": "fanpage",
        "use_business_composer": True,
    }
    ent = entity_dict_from_pages_row(row)
    assert "business.facebook.com" in ent["target_url"]
    assert "asset_id=1023267504212506" in ent["target_url"]


def test_page_row_asset_id_prefers_field() -> None:
    row = {
        "page_url": "https://www.facebook.com/9999999999999999",
        "fb_page_id": "1023267504212506",
    }
    assert page_row_facebook_asset_id(row) == "1023267504212506"
