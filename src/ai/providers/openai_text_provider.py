from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Callable

import requests
from loguru import logger

from src.ai.providers.base import TextProvider, TextResult

_RETRY_STATUS = {500, 502, 503, 504}


def _credit_exhausted(response: Any) -> bool:
    """Chỉ đọc mã lỗi. Không đưa nội dung phản hồi hay khóa vào thông báo."""
    try:
        data = response.json()
    except Exception:
        return False
    error = data.get("error") if isinstance(data, dict) else None
    if not isinstance(error, dict):
        return False
    code = str(error.get("code") or error.get("type") or "").strip().lower()
    return code in {"credit_balance_exhausted", "insufficient_quota"}


def _usage(data: dict[str, Any], model_name: str) -> dict[str, Any]:
    """Chỉ lấy model và token. Không lưu nội dung request hay API key."""
    raw = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    return {
        "provider": "openai",
        "model": model_name,
        "input_tokens": int(raw.get("input_tokens") or raw.get("prompt_tokens") or 0),
        "output_tokens": int(raw.get("output_tokens") or raw.get("completion_tokens") or 0),
    }


class OpenAITextProvider(TextProvider):
    provider_name = "openai"

    def __init__(
        self,
        post: Callable[..., Any] | None = None,
        sleep: Callable[[float], None] | None = None,
        max_attempts: int = 3,
    ) -> None:
        self._post = post or requests.post
        self._sleep = sleep or time.sleep
        self._max_attempts = max(1, int(max_attempts))

    def _responses(self, *, prompt: str, model: str | None = None) -> tuple[str, dict[str, Any]]:
        key = os.environ.get("OPENAI_API_KEY", "").strip()
        if not key:
            raise RuntimeError("Thiếu OPENAI_API_KEY cho provider OpenAI.")
        model_name = (model or os.environ.get("OPENAI_TEXT_MODEL", "gpt-4o-mini")).strip()
        logger.info("[AI][provider=openai][model={}][task=text] calling Responses API", model_name)
        last_status = 0
        for attempt in range(1, self._max_attempts + 1):
            response = self._post(
                "https://api.openai.com/v1/responses",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "model": model_name,
                    "input": prompt,
                    "temperature": 0.8,
                },
                timeout=90,
            )
            last_status = int(getattr(response, "status_code", 0) or 0)
            if last_status in _RETRY_STATUS and attempt < self._max_attempts:
                self._sleep(min(4, attempt))
                continue
            if last_status >= 400:
                if last_status == 429 and _credit_exhausted(response):
                    web_text = _text_from_chatgpt_web(prompt)
                    return web_text, {
                        "provider": "openai",
                        "model": "chatgpt-web",
                        "input_tokens": 0,
                        "output_tokens": 0,
                    }
                raise RuntimeError(f"OpenAI text lỗi HTTP {last_status}. Không đổi sang provider khác.")
            data = response.json()
            if not isinstance(data, dict):
                raise RuntimeError("OpenAI text trả dữ liệu không hợp lệ.")
            return _output_text(data), _usage(data, model_name)
        raise RuntimeError(f"OpenAI text lỗi HTTP {last_status}. Không đổi sang provider khác.")

    def generate_content_package(self, *, prompt: str, model: str | None = None) -> dict[str, Any]:
        """Gói bài viết JSON: tiêu đề, thân, caption, hashtag và prompt ảnh."""
        raw, usage = self._responses(prompt=prompt, model=model)
        data = _json_object(raw)
        title = str(data.get("title") or "").strip()
        body = str(data.get("body") or "").strip()
        caption = str(data.get("caption") or "").strip()
        hashtags = [str(item).strip() for item in (data.get("hashtags") or []) if str(item).strip()]
        if not title or not body or not caption or not hashtags:
            raise RuntimeError("OpenAI thiếu title, body, caption hoặc hashtag.")
        return {
            "title": title,
            "body": body,
            "caption": caption,
            "hashtags": hashtags,
            "image_prompt": str(data.get("image_prompt") or "").strip(),
            "language": str(data.get("language") or "").strip(),
            "provider": "openai",
            "model": usage["model"],
            "usage": usage,
        }

    def generate_post(self, *, prompt: str, model: str | None = None) -> TextResult:
        raw, _usage_info = self._responses(prompt=prompt, model=model)
        s, e = raw.find("{"), raw.rfind("}")
        if s != -1 and e != -1 and e > s:
            raw = raw[s : e + 1]
        data = json.loads(raw)
        body = str(data.get("body", "")).strip()
        image_alt = str(data.get("image_alt", "")).strip()
        if not body:
            raise RuntimeError("OpenAI trả body rỗng.")
        return TextResult(body=body, image_alt=image_alt)

    def generate_text(self, *, prompt: str, model: str | None = None) -> str:
        raw, _usage_info = self._responses(prompt=prompt, model=model)
        out = re.sub(r"\s+", " ", raw).strip()
        if not out:
            raise RuntimeError("OpenAI trả text rỗng.")
        return out

    def generate_topics(self, *, prompt: str, count: int, model: str | None = None) -> list[str]:
        raw, _usage_info = self._responses(prompt=prompt, model=model)
        s, e = raw.find("{"), raw.rfind("}")
        if s != -1 and e != -1 and e > s:
            raw = raw[s : e + 1]
        data = json.loads(raw)
        arr = data.get("topics")
        if not isinstance(arr, list):
            raise RuntimeError("OpenAI không trả topics hợp lệ.")
        out = [str(x).strip() for x in arr if str(x).strip()]
        if len(out) < count:
            raise RuntimeError("OpenAI trả thiếu topics.")
        return out[:count]

    def generate_hashtags(self, *, prompt: str, model: str | None = None) -> str:
        raw, _usage_info = self._responses(prompt=prompt, model=model)
        return re.sub(r"\s+", " ", raw).strip()


def _text_from_chatgpt_web(prompt: str) -> str:
    """API hết credit thì viết trên chatgpt.com, không mở cửa sổ."""
    from src.ai.image_generation import generate_chatgpt_browser_text
    from src.services.ai_fleet.account_check import existing_key_id

    account_id = os.environ.get("TOOLFB_CHATGPT_ACCOUNT_ID", "").strip()
    if not account_id:
        account_id = existing_key_id("openai", os.environ.get("OPENAI_API_KEY", ""))
    if not account_id:
        raise RuntimeError("Tài khoản ChatGPT đã hết credit API. Chưa có phiên web để viết bài.")
    logger.info("[AI][provider=openai][task=text] API hết credit, viết bài trên chatgpt.com chạy ngầm")
    try:
        return generate_chatgpt_browser_text(prompt, account_id=account_id)
    except Exception as exc:
        detail = str(exc).replace(os.environ.get("OPENAI_API_KEY", ""), "")
        raise RuntimeError(f"Tài khoản ChatGPT đã hết credit API. {detail}") from exc


def _output_text(data: dict[str, Any]) -> str:
    """Đọc chữ từ Responses API. Không đọc header xác thực."""
    text = str(data.get("output_text") or "").strip()
    if text:
        return text
    chunks: list[str] = []
    output = data.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for part in content:
                if not isinstance(part, dict):
                    continue
                if str(part.get("type") or "").strip() in {"output_text", "text"}:
                    piece = str(part.get("text") or "").strip()
                    if piece:
                        chunks.append(piece)
    return "\n".join(chunks).strip()


def _json_object(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end <= start:
        raise RuntimeError("OpenAI không trả JSON.")
    data = json.loads(text[start : end + 1])
    if not isinstance(data, dict):
        raise RuntimeError("OpenAI JSON phải là object.")
    return data
