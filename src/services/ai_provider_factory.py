from __future__ import annotations

from src.ai.providers.base import ImageProvider, TextProvider
from src.ai.providers.gemini_image_provider import GeminiImageProvider
from src.ai.providers.gemini_text_provider import GeminiTextProvider
from src.ai.providers.nanobanana_image_provider import NanobananaImageProvider
from src.ai.providers.nanobanana_pro_image_provider import NanoBananaProImageProvider
from src.ai.providers.openai_image_provider import OpenAIImageProvider
from src.ai.providers.openai_text_provider import OpenAITextProvider


class AIProviderFactory:
    @staticmethod
    def text(provider: str) -> TextProvider:
        p = str(provider or "").strip().lower()
        if p == "gemini":
            return GeminiTextProvider()
        if p == "openai":
            return OpenAITextProvider()
        raise ValueError(f"AI text provider không hỗ trợ: {provider!r}")

    @staticmethod
    def image(provider: str) -> ImageProvider:
        p = str(provider or "").strip().lower()
        if p == "gemini":
            return GeminiImageProvider()
        if p == "openai":
            return OpenAIImageProvider()
        if p == "nanobanana":
            return NanobananaImageProvider()
        if p in {"nano_banana_pro", "nanobanana_pro"}:
            return NanoBananaProImageProvider()
        raise ValueError(f"AI image provider không hỗ trợ: {provider!r}")
