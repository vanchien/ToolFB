from __future__ import annotations

import os
import re

from loguru import logger

from src.ai.prompt_builder import (
    build_cta_prompt,
    build_hashtags_prompt,
    build_image_prompt_text_prompt,
    build_post_json_prompt,
    build_title_prompt,
    build_topics_prompt,
)
from src.services.ai_provider_factory import AIProviderFactory


class AITextService:
    def _default_provider(self) -> str:
        return (os.environ.get("AI_PROVIDER_TEXT", "gemini") or "gemini").strip().lower()

    def generate_post(
        self,
        *,
        topic: str,
        style: str,
        language: str,
        provider: str | None = None,
        model: str | None = None,
    ) -> dict[str, str]:
        p = (provider or self._default_provider()).strip().lower()
        prompt = build_post_json_prompt(topic=topic, style=style, language=language)
        logger.info("[AI][provider={}][model={}][task=content] generate_post", p, model or "")
        try:
            out = AIProviderFactory.text(p).generate_post(prompt=prompt, model=model)
            return {"body": out.body, "image_alt": out.image_alt}
        except Exception as exc:
            raise RuntimeError(f"[{p}] text generate_post lỗi: {exc}") from exc

    def generate_topics(
        self,
        *,
        idea: str,
        count: int,
        goal: str = "",
        length_hint: str = "",
        provider: str | None = None,
        model: str | None = None,
    ) -> list[str]:
        p = (provider or self._default_provider()).strip().lower()
        prompt = build_topics_prompt(idea=idea, count=count, goal=goal, length_hint=length_hint)
        logger.info("[AI][provider={}][model={}][task=topics] generate_topics", p, model or "")
        try:
            return AIProviderFactory.text(p).generate_topics(prompt=prompt, count=count, model=model)
        except Exception as exc:
            raise RuntimeError(f"[{p}] text generate_topics lỗi: {exc}") from exc

    def generate_hashtags(
        self,
        *,
        title: str,
        body: str,
        language: str,
        count: int,
        provider: str | None = None,
        model: str | None = None,
    ) -> list[str]:
        p = (provider or self._default_provider()).strip().lower()
        prompt = build_hashtags_prompt(title=title, body=body, language=language, count=count)
        logger.info("[AI][provider={}][model={}][task=hashtags] generate_hashtags", p, model or "")
        try:
            raw = AIProviderFactory.text(p).generate_hashtags(prompt=prompt, model=model)
        except Exception as exc:
            raise RuntimeError(f"[{p}] text generate_hashtags lỗi: {exc}") from exc
        tags: list[str] = []
        for token in re.split(r"[, \n]+", raw):
            t = token.strip()
            if not t:
                continue
            if not t.startswith("#"):
                t = f"#{t}"
            if t not in tags:
                tags.append(t)
            if len(tags) >= max(1, int(count)):
                break
        return tags

    def generate_title(
        self,
        *,
        topic: str,
        language: str,
        provider: str | None = None,
        model: str | None = None,
    ) -> str:
        p = (provider or self._default_provider()).strip().lower()
        prompt = build_title_prompt(topic=topic, language=language)
        logger.info("[AI][provider={}][model={}][task=title] generate_title", p, model or "")
        try:
            return AIProviderFactory.text(p).generate_text(prompt=prompt, model=model).strip()
        except Exception as exc:
            raise RuntimeError(f"[{p}] text generate_title lỗi: {exc}") from exc

    def generate_cta(
        self,
        *,
        topic: str,
        language: str,
        provider: str | None = None,
        model: str | None = None,
    ) -> str:
        p = (provider or self._default_provider()).strip().lower()
        prompt = build_cta_prompt(topic=topic, language=language)
        logger.info("[AI][provider={}][model={}][task=cta] generate_cta", p, model or "")
        try:
            return AIProviderFactory.text(p).generate_text(prompt=prompt, model=model).strip()
        except Exception as exc:
            raise RuntimeError(f"[{p}] text generate_cta lỗi: {exc}") from exc

    def generate_image_prompt_text(
        self,
        *,
        title: str,
        body: str,
        language: str,
        provider: str | None = None,
        model: str | None = None,
    ) -> str:
        p = (provider or self._default_provider()).strip().lower()
        prompt = build_image_prompt_text_prompt(title=title, body=body, language=language)
        logger.info("[AI][provider={}][model={}][task=image_prompt] generate_image_prompt_text", p, model or "")
        try:
            return AIProviderFactory.text(p).generate_text(prompt=prompt, model=model).strip()
        except Exception as exc:
            raise RuntimeError(f"[{p}] text generate_image_prompt lỗi: {exc}") from exc
