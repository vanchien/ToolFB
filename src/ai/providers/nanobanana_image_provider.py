from __future__ import annotations

from src.ai.image_generation import generate_post_images_png
from src.ai.providers.base import ImageProvider


class NanobananaImageProvider(ImageProvider):
    provider_name = "nanobanana"

    def generate_images(
        self,
        *,
        prompt: str,
        number_of_images: int,
        model: str | None = None,
    ) -> list[bytes]:
        # Dùng NanoBanana trực tiếp (browser/API theo cấu hình runtime trong image_generation).
        return generate_post_images_png(
            prompt=prompt,
            number_of_images=number_of_images,
            model=model,
            provider="nanobanana",
        )
