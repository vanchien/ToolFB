from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger

from src.ai.image_generation import _canonical_nano_banana_pro_gemini_model, generate_post_images_png
from src.ai.providers.base import ImageProvider
from src.services.ai_image_config import nano_banana_pro_settings


class NanoBananaProImageProvider(ImageProvider):
    """
    Provider tạo ảnh bằng Nano Banana Pro (Gemini 3 Pro Image, API ``generate_content``).
    """

    provider_name = "nano_banana_pro"

    def generate_images(
        self,
        *,
        prompt: str,
        number_of_images: int,
        model: str | None = None,
    ) -> list[bytes]:
        cfg = nano_banana_pro_settings()
        raw = (model or str(cfg.get("model") or "")).strip() or "gemini-3-pro-image-preview"
        m = _canonical_nano_banana_pro_gemini_model(raw) or raw
        return generate_post_images_png(
            prompt=prompt,
            number_of_images=number_of_images,
            model=m,
            provider="nano_banana_pro",
        )

    def generate_image(
        self,
        prompt: str,
        output_path: str,
        options: dict | None = None,
    ) -> dict[str, Any]:
        """
        Tạo ảnh từ prompt và lưu ra output_path (một file).

        Returns:
            Dict chứa đường dẫn output và metadata tối thiểu.
        """
        opts = options or {}
        model = opts.get("model")
        if model is not None:
            model = str(model).strip() or None
        blobs = self.generate_images(prompt=prompt, number_of_images=1, model=model)
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(blobs[0])
        cfg = nano_banana_pro_settings()
        raw = (model or str(cfg.get("model") or "")).strip() or "gemini-3-pro-image-preview"
        resolved_model = _canonical_nano_banana_pro_gemini_model(raw) or raw
        logger.info("[NanoBananaPro] Đã lưu ảnh: {}", out.resolve())
        return {
            "character_image_path": str(out.resolve()),
            "character_image_prompt": prompt,
            "image_provider": self.provider_name,
            "image_model": resolved_model,
        }
