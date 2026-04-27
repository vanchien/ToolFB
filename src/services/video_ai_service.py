from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger

from src.services.ai_text_service import AITextService


def generate_video_caption_from_frames(
    *,
    vp: Path,
    meta: dict[str, Any],
    frame_paths: list[Path],
    language: str,
    idea: str,
    video_context: str,
    provider: str | None = None,
    model: str | None = None,
) -> str:
    del vp, meta, frame_paths
    try:
        out = AITextService().generate_post(
            topic=f"{idea}\n{video_context}",
            style="Viết caption video ngắn 2-5 dòng, có CTA cuối.",
            language=language,
            provider=provider,
            model=model,
        )
        return str(out.get("body", "")).strip()[:1600]
    except Exception as exc:
        logger.warning("Video caption service lỗi: {}", exc)
        return ""


def generate_video_title_from_frames(
    *,
    vp: Path,
    meta: dict[str, Any],
    frame_paths: list[Path],
    language: str,
    idea: str,
    video_context: str,
    provider: str | None = None,
    model: str | None = None,
) -> str:
    del vp, meta, frame_paths
    try:
        out = AITextService().generate_post(
            topic=f"{idea}\n{video_context}",
            style="Chỉ trả về 1 tiêu đề video ngắn 6-12 từ, không hashtag, không emoji.",
            language=language,
            provider=provider,
            model=model,
        )
        return str(out.get("body", "")).strip()
    except Exception as exc:
        logger.warning("Video title service lỗi: {}", exc)
        return ""


def generate_video_hashtags(
    *,
    language: str,
    idea: str,
    title: str,
    video_context: str,
    count: int,
    provider: str | None = None,
    model: str | None = None,
) -> list[str]:
    try:
        return AITextService().generate_hashtags(
            title=title or idea,
            body=video_context,
            language=language,
            count=count,
            provider=provider,
            model=model,
        )
    except Exception as exc:
        logger.warning("Video hashtags service lỗi: {}", exc)
        return []
