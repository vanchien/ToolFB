from __future__ import annotations

import base64
import os

import requests
from loguru import logger

from src.ai.providers.base import ImageProvider


class OpenAIImageProvider(ImageProvider):
    provider_name = "openai"

    def generate_images(
        self,
        *,
        prompt: str,
        number_of_images: int,
        model: str | None = None,
    ) -> list[bytes]:
        key = os.environ.get("OPENAI_API_KEY", "").strip()
        if not key:
            raise RuntimeError("Thiếu OPENAI_API_KEY cho provider OpenAI.")
        model_name = (model or os.environ.get("OPENAI_IMAGE_MODEL", "gpt-image-2")).strip()
        logger.info("[AI][provider=openai][model={}][task=image] calling image generation API", model_name)
        r = requests.post(
            "https://api.openai.com/v1/images/generations",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": model_name,
                "prompt": prompt[:4000],
                "n": max(1, min(4, int(number_of_images))),
                "size": "1024x1024",
                "response_format": "b64_json",
            },
            timeout=120,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"OpenAI image lỗi HTTP {r.status_code}: {r.text[:500]}")
        data = r.json()
        out: list[bytes] = []
        for item in data.get("data", []) or []:
            b64 = str(item.get("b64_json", "")).strip()
            if b64:
                out.append(base64.b64decode(b64))
        if not out:
            raise RuntimeError("OpenAI image không trả dữ liệu ảnh.")
        return out
