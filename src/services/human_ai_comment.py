"""
Sinh bình luận Facebook ngắn qua AI (Gemini/OpenAI) — không dùng list văn mẫu.
"""

from __future__ import annotations

import os
import re

from loguru import logger

from src.services.ai_provider_factory import AIProviderFactory


def generate_facebook_comment(
    post_text: str,
    *,
    language: str = "Tiếng Việt",
    max_len: int = 120,
) -> str:
    """
    Tạo một câu bình luận tự nhiên dựa trên nội dung bài viết.

    Returns:
        Chuỗi comment hoặc rỗng nếu thiếu API / lỗi.
    """
    snippet = re.sub(r"\s+", " ", str(post_text or "").strip())[:800]
    if len(snippet) < 8:
        return ""
    provider = (os.environ.get("AI_PROVIDER_TEXT", "gemini") or "gemini").strip().lower()
    model = os.environ.get("AI_MODEL_TEXT", "").strip() or None
    prompt = (
        f"Bạn là người dùng Facebook Việt Nam. Đọc bài viết sau và viết MỘT câu bình luận "
        f"ngắn ({max_len} ký tự), tự nhiên, phù hợp ngữ cảnh, không quảng cáo, không hashtag, "
        f"không emoji quá nhiều. Chỉ trả về nội dung comment, không giải thích.\n\n"
        f"Bài viết:\n{snippet}\n\nComment ({language}):"
    )
    try:
        out = AIProviderFactory.text(provider).generate_text(prompt=prompt, model=model).strip()
        out = out.strip('"').strip("'")
        if len(out) > max_len:
            out = out[: max_len - 1].rsplit(" ", 1)[0] + "…"
        logger.info("[Human][AI] Comment sinh từ {} (len={})", provider, len(out))
        return out
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Human][AI] Không sinh được comment: {}", exc)
        return ""
