"""
Compatibility layer cho AI text generation.

Mọi tác vụ text đi qua ``AITextService`` + provider factory.
"""

from __future__ import annotations

import os
import warnings
from typing import TypedDict

warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    message=r"(?is).*generativeai.*package has ended.*",
)

from src.services.ai_text_service import AITextService


class GeneratedPost(TypedDict):
    """Kết quả sinh nội dung: thân bài và gợi ý alt-text ảnh (SEO)."""

    body: str
    image_alt: str


def generate_post(topic: str, *, style: str | None = None) -> GeneratedPost:
    """Sinh body + image_alt qua text service."""
    svc = AITextService()
    writing_style = (style or os.environ.get("CONTENT_STYLE", "thân mật, tự nhiên")).strip()
    provider = os.environ.get("AI_PROVIDER_TEXT", "gemini")
    model = os.environ.get("AI_MODEL_TEXT", "").strip() or None
    out = svc.generate_post(
        topic=topic,
        style=writing_style,
        language="Tiếng Việt",
        provider=provider,
        model=model,
    )
    body = str(out.get("body", "")).strip()
    image_alt = str(out.get("image_alt", "")).strip()
    return GeneratedPost(body=body, image_alt=image_alt)


def generate_content_plan_topics(
    idea: str,
    count: int,
    *,
    goal: str = "",
    length_hint: str = "",
) -> list[str]:
    """Chia ``idea`` thành ``count`` chủ đề con qua text service."""
    idea = str(idea or "").strip()
    n = max(1, min(50, int(count)))
    if not idea:
        return [f"Chủ đề {i + 1}" for i in range(n)]
    provider = os.environ.get("AI_PROVIDER_TEXT", "gemini")
    model = os.environ.get("AI_MODEL_TEXT", "").strip() or None
    svc = AITextService()
    topics = svc.generate_topics(
        idea=idea,
        count=n,
        goal=goal,
        length_hint=length_hint,
        provider=provider,
        model=model,
    )
    return topics[:n]
