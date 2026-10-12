from __future__ import annotations

import json
import os
import re
from typing import Any

import google.generativeai as genai

from src.ai.providers.base import TextProvider, TextResult


def _hashtag_list(raw: object) -> list[str]:
    if isinstance(raw, str):
        parts = raw.replace(",", " ").split()
    else:
        parts = list(raw or [])
    return [str(item).strip() for item in parts if str(item).strip()]


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = text.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-zA-Z]*\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw, flags=re.DOTALL)
    if not raw.startswith("{"):
        s = raw.find("{")
        e = raw.rfind("}")
        if s != -1 and e != -1 and e > s:
            raw = raw[s : e + 1]
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("Gemini JSON phải là object.")
    return data


class GeminiTextProvider(TextProvider):
    provider_name = "gemini"

    def _model(self, model: str | None = None):
        key = os.environ.get("GEMINI_API_KEY", "").strip()
        if not key:
            raise RuntimeError("Thiếu GEMINI_API_KEY cho provider Gemini.")
        genai.configure(api_key=key)
        return genai.GenerativeModel((model or os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")).strip())

    def generate_content_package(self, *, prompt: str, model: str | None = None) -> dict[str, Any]:
        """Gói bài viết JSON cùng cấu trúc với ChatGPT: tiêu đề, thân, caption, hashtag."""
        model_name = (model or os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")).strip()
        try:
            response = self._model(model_name).generate_content(prompt)
            data = _extract_json_object((response.text or "").strip())
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError("Gemini chưa trả được bài viết.") from exc
        title = str(data.get("title") or "").strip()
        body = str(data.get("body") or "").strip()
        caption = str(data.get("caption") or "").strip()
        hashtags = _hashtag_list(data.get("hashtags"))
        if not title or not body or not caption or not hashtags:
            raise RuntimeError("Gemini thiếu title, body, caption hoặc hashtag.")
        usage_raw = getattr(response, "usage_metadata", None)
        return {
            "title": title,
            "body": body,
            "caption": caption,
            "hashtags": hashtags,
            "image_prompt": str(data.get("image_prompt") or "").strip(),
            "language": str(data.get("language") or "").strip(),
            "provider": "gemini",
            "model": model_name,
            "usage": {
                "provider": "gemini",
                "model": model_name,
                "input_tokens": int(getattr(usage_raw, "prompt_token_count", 0) or 0),
                "output_tokens": int(getattr(usage_raw, "candidates_token_count", 0) or 0),
            },
        }

    def generate_post(self, *, prompt: str, model: str | None = None) -> TextResult:
        resp = self._model(model).generate_content(prompt)
        data = _extract_json_object((resp.text or "").strip())
        body = str(data.get("body", "")).strip()
        image_alt = str(data.get("image_alt", "")).strip()
        if not body:
            raise RuntimeError("Gemini trả body rỗng.")
        return TextResult(body=body, image_alt=image_alt)

    def generate_text(self, *, prompt: str, model: str | None = None) -> str:
        resp = self._model(model).generate_content(prompt)
        out = (resp.text or "").strip()
        if not out:
            raise RuntimeError("Gemini trả text rỗng.")
        return out

    def generate_topics(self, *, prompt: str, count: int, model: str | None = None) -> list[str]:
        resp = self._model(model).generate_content(prompt)
        data = _extract_json_object((resp.text or "").strip())
        topics = data.get("topics")
        if not isinstance(topics, list):
            raise RuntimeError("Gemini không trả mảng topics.")
        out = [str(t).strip() for t in topics if str(t).strip()]
        if len(out) < count:
            raise RuntimeError("Gemini trả thiếu topics.")
        return out[:count]

    def generate_hashtags(self, *, prompt: str, model: str | None = None) -> str:
        resp = self._model(model).generate_content(prompt)
        return (resp.text or "").strip()
