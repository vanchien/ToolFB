"""reel_thumbnail_choice + HTML tham chiếu (Cách 1)."""

from __future__ import annotations

from src.utils.reel_thumbnail_choice import (
    REEL_THUMBNAIL_METHOD1_FIRST_AUTO,
    REEL_THUMBNAIL_OFF,
    first_img_src_from_meta_reel_thumbnail_html,
    normalize_reel_thumbnail_choice,
)


def test_normalize_default_and_method1() -> None:
    assert normalize_reel_thumbnail_choice(None) == REEL_THUMBNAIL_OFF
    assert normalize_reel_thumbnail_choice("") == REEL_THUMBNAIL_OFF
    assert normalize_reel_thumbnail_choice("off") == REEL_THUMBNAIL_OFF
    assert normalize_reel_thumbnail_choice("method1_first_auto") == REEL_THUMBNAIL_METHOD1_FIRST_AUTO
    assert normalize_reel_thumbnail_choice("METHOD1") == REEL_THUMBNAIL_METHOD1_FIRST_AUTO
    assert normalize_reel_thumbnail_choice("first_auto") == REEL_THUMBNAIL_METHOD1_FIRST_AUTO


def test_first_img_src_from_sample_html() -> None:
    html = '<div><img src="https://example.com/thumb1.jpg" alt="a"/><img src="https://example.com/t2.jpg"/></div>'
    assert first_img_src_from_meta_reel_thumbnail_html(html) == "https://example.com/thumb1.jpg"
