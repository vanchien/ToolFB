"""Selector / nhãn menu Reel sau nút Create (Professional Dashboard)."""

from __future__ import annotations

from src.automation.facebook_actions import (
    _REEL_LEXICAL_TEXTBOX_SELECTORS,
    _REEL_MENU_LABEL_RE,
    _REEL_MENU_NAME_RE,
    _REEL_UPLOAD_READY_RE,
    _build_reel_text_payload,
    _reel_caption_screen_markers_visible,
    _reel_edit_reel_header_visible,
    _reel_lexical_description_usable,
    _reel_pre_text_wizard_screen,
    advance_reel_wizard_until_description_input,
    detect_meta_reel_ui_way,
    fill_reel_lexical_description,
)


def test_reel_menu_label_regex_matches_common_labels() -> None:
    for label in ("Reel", "Reels", "Thước phim", "Video", "Short video", "Video ngắn"):
        assert _REEL_MENU_LABEL_RE.search(label), label
    assert not _REEL_MENU_LABEL_RE.search("Professional Dashboard")


def test_reel_menu_name_regex_partial() -> None:
    assert _REEL_MENU_NAME_RE.search("Reel")
    assert _REEL_MENU_NAME_RE.search("Thước phim")


def test_reel_upload_ready_markers() -> None:
    assert _REEL_UPLOAD_READY_RE.search("Add video or drag and drop")
    assert _REEL_UPLOAD_READY_RE.search("Thêm video")


def test_detect_meta_reel_ui_way_is_callable() -> None:
    assert callable(detect_meta_reel_ui_way)


def test_advance_until_text_helpers_callable() -> None:
    assert callable(advance_reel_wizard_until_description_input)
    assert callable(_reel_pre_text_wizard_screen)
    assert callable(_reel_edit_reel_header_visible)
    assert callable(_reel_caption_screen_markers_visible)
    assert callable(_reel_lexical_description_usable)
    assert callable(fill_reel_lexical_description)


def test_reel_lexical_selectors_match_describe_placeholder() -> None:
    joined = " ".join(_REEL_LEXICAL_TEXTBOX_SELECTORS)
    assert "data-lexical-editor" in joined
    assert "Describe" in joined


def test_build_reel_text_payload_merges_title_content_tags() -> None:
    out = _build_reel_text_payload("Tiêu đề", "Nội dung", ["tag1", "tag2"])
    assert "Tiêu đề" in out
    assert "Nội dung" in out
    assert "#tag1" in out
