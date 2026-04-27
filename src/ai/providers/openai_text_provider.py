from __future__ import annotations

import json
import os
import re

import requests
from loguru import logger

from src.ai.providers.base import TextProvider, TextResult


class OpenAITextProvider(TextProvider):
    provider_name = "openai"

    def _responses(self, *, prompt: str, model: str | None = None) -> str:
        key = os.environ.get("OPENAI_API_KEY", "").strip()
        if not key:
            raise RuntimeError("Thiếu OPENAI_API_KEY cho provider OpenAI.")
        model_name = (model or os.environ.get("OPENAI_TEXT_MODEL", "gpt-4o-mini")).strip()
        logger.info("[AI][provider=openai][model={}][task=text] calling Responses API", model_name)
        r = requests.post(
            "https://api.openai.com/v1/responses",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": model_name,
                "input": prompt,
                "temperature": 0.8,
            },
            timeout=90,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"OpenAI text lỗi HTTP {r.status_code}: {r.text[:500]}")
        data = r.json()
        txt = str(data.get("output_text", "") or "").strip()
        if txt:
            return txt
        out = data.get("output")
        if isinstance(out, list):
            chunks: list[str] = []
            for item in out:
                if not isinstance(item, dict):
                    continue
                content = item.get("content")
                if not isinstance(content, list):
                    continue
                for c in content:
                    if not isinstance(c, dict):
                        continue
                    if str(c.get("type", "")).strip() in {"output_text", "text"}:
                        t = str(c.get("text", "") or "").strip()
                        if t:
                            chunks.append(t)
            return "\n".join(chunks).strip()
        return ""

    def generate_post(self, *, prompt: str, model: str | None = None) -> TextResult:
        raw = self._responses(prompt=prompt, model=model)
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
        raw = self._responses(prompt=prompt, model=model)
        out = re.sub(r"\s+", " ", raw).strip()
        if not out:
            raise RuntimeError("OpenAI trả text rỗng.")
        return out

    def generate_topics(self, *, prompt: str, count: int, model: str | None = None) -> list[str]:
        raw = self._responses(prompt=prompt, model=model)
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
        raw = self._responses(prompt=prompt, model=model)
        return re.sub(r"\s+", " ", raw).strip()
