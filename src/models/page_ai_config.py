"""Cấu hình AI theo Page (``page_ai_config.json``)."""

from __future__ import annotations

from typing import TypedDict


class PageAIConfigRecord(TypedDict, total=False):
    page_id: str
    brand_voice: str
    content_pillars: list[str]
    target_audience: str
    post_length: str
    emoji_style: str
    cta_style: str
    hashtags: list[str]
    image_style: str
    avoid_keywords: list[str]
    auto_generate_image: bool
    auto_generate_caption: bool
