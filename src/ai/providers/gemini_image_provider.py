from __future__ import annotations

from src.ai.image_generation import generate_post_images_png
from src.ai.providers.base import ImageProvider


class GeminiImageProvider(ImageProvider):
    provider_name = "gemini"

    def generate_images(
        self,
        *,
        prompt: str,
        number_of_images: int,
        model: str | None = None,
    ) -> list[bytes]:
        return generate_post_images_png(
            prompt=prompt,
            number_of_images=number_of_images,
            model=model,
            provider="imagen",
        )
