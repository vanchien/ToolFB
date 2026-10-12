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
        body = {
            "model": model_name,
            "prompt": prompt[:4000],
            "n": max(1, min(4, int(number_of_images))),
            "size": "1024x1024",
        }
        # gpt-image-* luôn trả b64_json và từ chối response_format bằng HTTP 400.
        if not model_name.lower().startswith("gpt-image"):
            body["response_format"] = "b64_json"
        r = requests.post(
            "https://api.openai.com/v1/images/generations",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json=body,
            timeout=120,
        )
        if r.status_code >= 400:
            code = ""
            try:
                error = (r.json() or {}).get("error") or {}
                code = str(error.get("code") or error.get("type") or "").strip().lower()
            except Exception:
                code = ""
            if r.status_code == 429 and code in {"credit_balance_exhausted", "insufficient_quota"}:
                return _images_from_chatgpt_web(prompt, number_of_images)
            raise RuntimeError(f"OpenAI image lỗi HTTP {r.status_code}.")
        data = r.json()
        out: list[bytes] = []
        for item in data.get("data", []) or []:
            b64 = str(item.get("b64_json", "")).strip()
            if b64:
                out.append(base64.b64decode(b64))
        if not out:
            raise RuntimeError("OpenAI image không trả dữ liệu ảnh.")
        return out


def _images_from_chatgpt_web(prompt: str, number_of_images: int) -> list[bytes]:
    """API hết credit thì tạo ảnh trên chatgpt.com bằng phiên trình duyệt đã đăng nhập."""
    from src.ai.image_generation import generate_chatgpt_browser_images
    from src.services.ai_fleet.account_check import existing_key_id

    account_id = os.environ.get("TOOLFB_CHATGPT_ACCOUNT_ID", "").strip()
    if not account_id:
        account_id = existing_key_id("openai", os.environ.get("OPENAI_API_KEY", ""))
    if not account_id:
        raise RuntimeError("Tài khoản ChatGPT đã hết credit API. Chưa có phiên web để tạo ảnh.")
    logger.info("[AI][provider=openai][task=image] API hết credit, tạo ảnh trên chatgpt.com")
    try:
        blobs = generate_chatgpt_browser_images(prompt, account_id=account_id, number_of_images=number_of_images)
    except Exception as exc:
        detail = str(exc).replace(os.environ.get("OPENAI_API_KEY", ""), "")
        raise RuntimeError(f"Tài khoản ChatGPT đã hết credit API. {detail}") from exc
    if not blobs:
        raise RuntimeError("Tài khoản ChatGPT đã hết credit API. ChatGPT web chưa trả ảnh.")
    return blobs
