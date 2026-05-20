"""Ánh xạ post_type → pipeline đăng bài."""

from __future__ import annotations

from src.services.post_executor import (
    _is_reel_dashboard_post_type,
    resolve_post_flow_id,
)


def test_reel_dashboard_post_types() -> None:
    for pt in ("video", "text_video", "reel"):
        assert _is_reel_dashboard_post_type(pt)
        assert resolve_post_flow_id(pt) == "FLOW_REEL_DASHBOARD"


def test_text_image_legacy_post_types() -> None:
    for pt in ("text", "image", "text_image"):
        assert resolve_post_flow_id(pt) == "FLOW_TEXT_IMAGE_LEGACY"
        assert resolve_post_flow_id(pt, has_video_in_draft=False) == "FLOW_TEXT_IMAGE_LEGACY"


def test_legacy_mb_reel_when_composer_and_video() -> None:
    assert (
        resolve_post_flow_id(
            "text_image",
            has_video_in_draft=True,
            meta_business_composer=True,
        )
        == "FLOW_LEGACY_MB_REEL"
    )


def test_video_post_type_never_legacy_mb_at_start() -> None:
    """Job video|reel luôn dashboard — không vào composer Share."""
    assert resolve_post_flow_id("video", has_video_in_draft=True) == "FLOW_REEL_DASHBOARD"
