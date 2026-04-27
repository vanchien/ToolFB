from __future__ import annotations

import os
import re
import uuid
from pathlib import Path

from loguru import logger

from src.ai.image_generation import build_imagen_prompt_from_post
from src.services.ai_image_config import load_ai_image_config
from src.services.ai_provider_factory import AIProviderFactory
from src.utils.page_workspace import ensure_page_workspace


class AIImageService:
    def _default_provider(self) -> str:
        env = (os.environ.get("AI_PROVIDER_IMAGE", "") or "").strip().lower()
        if env:
            return env
        cfg = load_ai_image_config()
        return str(cfg.get("default_provider") or "nano_banana_pro").strip().lower() or "nano_banana_pro"

    def generate_images(
        self,
        *,
        prompt: str,
        number_of_images: int = 1,
        provider: str | None = None,
        model: str | None = None,
    ) -> list[bytes]:
        p = (provider or self._default_provider()).strip().lower()
        logger.info("[AI][provider={}][model={}][task=image] generate_images", p, model or "")
        try:
            return AIProviderFactory.image(p).generate_images(
                prompt=prompt,
                number_of_images=number_of_images,
                model=model,
            )
        except Exception as exc:
            raise RuntimeError(f"[{p}] image generate_images lỗi: {exc}") from exc

    def generate_and_save_for_batch(
        self,
        *,
        page_id: str,
        file_stem: str,
        title: str,
        body: str,
        image_style: str = "",
        image_prompt: str = "",
        number_of_images: int = 1,
        provider: str | None = None,
        model: str | None = None,
    ) -> list[Path]:
        prompt = str(image_prompt).strip() or build_imagen_prompt_from_post(title=title, body=body, image_style=image_style)
        blobs = self.generate_images(
            prompt=prompt,
            number_of_images=number_of_images,
            provider=provider,
            model=model,
        )
        safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", file_stem).strip("._")[:72] or uuid.uuid4().hex[:12]
        out_dir = ensure_page_workspace(page_id) / "library" / "generated"
        out_dir.mkdir(parents=True, exist_ok=True)
        out: list[Path] = []
        for i, blob in enumerate(blobs):
            fn = f"{safe}.png" if len(blobs) == 1 else f"{safe}_{i}.png"
            p = out_dir / fn
            p.write_bytes(blob)
            out.append(p.resolve())
        return out
