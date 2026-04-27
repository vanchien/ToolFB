from __future__ import annotations

import json
import re
from typing import Any, Protocol

from loguru import logger

from src.services.ai_styles_registry import default_style_id, load_style_registry


class _TextAI(Protocol):
    def generate_text(self, *, prompt: str, model: str | None = None) -> str: ...


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
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
        raise ValueError("AutoStyleSelector: AI output phải là JSON object.")
    return data


class AutoStyleSelector:
    """
    Tự động chọn style ảnh/video từ ý tưởng.
    """

    def __init__(self, ai_text_service: _TextAI, style_registry: dict[str, Any] | None = None) -> None:
        self.ai_text_service = ai_text_service
        self.style_registry = dict(style_registry or load_style_registry())

    def _fallback(self) -> dict[str, Any]:
        return {
            "image_style_id": default_style_id("character_image_style_id", "character_cinematic_realistic"),
            "video_style_id": default_style_id("video_style_id", "cinematic_story"),
            "camera_style_id": default_style_id("camera_style_id", "smooth_dolly_in"),
            "lighting_style_id": default_style_id("lighting_style_id", "soft_natural_light"),
            "motion_style_id": default_style_id("motion_style_id", "slow_and_smooth"),
            "mood": "cinematic and polished",
            "aspect_ratio": "9:16",
            "duration_sec": 8,
            "reason": "Fallback defaults.",
        }

    def _id_set(self, group: str) -> set[str]:
        rows = self.style_registry.get(group)
        if not isinstance(rows, list):
            return set()
        out: set[str] = set()
        for row in rows:
            if isinstance(row, dict):
                sid = str(row.get("id", "")).strip()
                if sid:
                    out.add(sid)
        return out

    def _style_row(self, group: str, style_id: str) -> dict[str, Any] | None:
        sid = str(style_id or "").strip()
        if not sid:
            return None
        rows = self.style_registry.get(group)
        if not isinstance(rows, list):
            return None
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("id", "")).strip() == sid:
                return dict(row)
        return None

    def _validate(self, data: dict[str, Any], *, preferred_video_style_id: str = "") -> dict[str, Any]:
        out = dict(self._fallback())
        for key, group in (
            ("image_style_id", "character_image_styles"),
            ("video_style_id", "video_styles"),
            ("camera_style_id", "camera_styles"),
            ("lighting_style_id", "lighting_styles"),
            ("motion_style_id", "motion_styles"),
        ):
            v = str(data.get(key, "")).strip()
            if v and v in self._id_set(group):
                out[key] = v
            elif v:
                logger.warning("AutoStyleSelector: {}={} không tồn tại, fallback default.", key, v)

        mood = str(data.get("mood", "")).strip()
        if mood:
            out["mood"] = mood

        ar = str(data.get("aspect_ratio", "")).strip()
        if ar in {"9:16", "16:9", "1:1"}:
            out["aspect_ratio"] = ar

        try:
            dur = int(data.get("duration_sec") or 8)
        except Exception:
            dur = 8
        if dur not in {4, 6, 8}:
            logger.warning("AutoStyleSelector: duration_sec={} không hợp lệ, fallback.", dur)
            dur = 8
        out["duration_sec"] = dur

        rs = str(data.get("reason", "")).strip()
        if rs:
            out["reason"] = rs
        preferred_sid = str(preferred_video_style_id or "").strip()
        if preferred_sid and preferred_sid in self._id_set("video_styles"):
            out["video_style_id"] = preferred_sid
        return out

    def select_styles(
        self,
        idea: str,
        target_platform: str,
        content_goal: str,
        language: str,
        *,
        preferred_video_style_id: str = "",
        model: str | None = None,
    ) -> dict[str, Any]:
        """
        Gọi AI để chọn style phù hợp.
        """
        reg_min = {
            "image_styles": self.style_registry.get("image_styles") or [],
            "character_image_styles": self.style_registry.get("character_image_styles") or [],
            "environment_styles": self.style_registry.get("environment_styles") or [],
            "video_styles": self.style_registry.get("video_styles") or [],
            "camera_styles": self.style_registry.get("camera_styles") or [],
            "lighting_styles": self.style_registry.get("lighting_styles") or [],
            "motion_styles": self.style_registry.get("motion_styles") or [],
        }
        preferred_sid = str(preferred_video_style_id or "").strip()
        preferred_row = self._style_row("video_styles", preferred_sid)
        preferred_label = ""
        preferred_addon = ""
        if preferred_row:
            preferred_label = str(preferred_row.get("name", "")).strip()
            preferred_addon = str(preferred_row.get("prompt_addon", "")).strip()
        prompt = f"""You are an expert AI art director for image and video generation.

User idea:
{str(idea or '').strip()}

Target platform:
{str(target_platform or '').strip() or 'Facebook Reels'}

Content goal:
{str(content_goal or '').strip() or 'viral'}

Language:
{str(language or '').strip() or 'Vietnamese'}

Available style registry:
{json.dumps(reg_min, ensure_ascii=False)}

Video style is the main anchor and should stay fixed if provided:
- preferred_video_style_id: {preferred_sid or "none"}
- preferred_video_style_label: {preferred_label or "none"}
- preferred_video_style_prompt_addon: {preferred_addon or "none"}

Choose the best complementary styles for camera, lighting, motion, mood, aspect ratio, and duration around the main video style.

Return strict JSON only:
{{
  "image_style_id": "",
  "video_style_id": "",
  "camera_style_id": "",
  "lighting_style_id": "",
  "motion_style_id": "",
  "mood": "",
  "aspect_ratio": "",
  "duration_sec": 8,
  "reason": ""
}}

Rules:
- Only choose style IDs that exist in the registry.
- If preferred_video_style_id is provided and valid, keep video_style_id exactly that value.
- For Facebook Reels / TikTok / Shorts, prefer 9:16.
- For cinematic story, prefer cinematic or dark mystery styles.
- For product promotion, prefer product commercial / brand film.
- For UGC or natural content, prefer social / UGC style.
- Return JSON only.
""".strip()
        try:
            text = self.ai_text_service.generate_text(prompt=prompt, model=model)
            parsed = _extract_json_object(text)
            return self._validate(parsed, preferred_video_style_id=preferred_sid)
        except Exception as exc:  # noqa: BLE001
            logger.warning("AutoStyleSelector lỗi, fallback default: {}", exc)
            return self._validate({}, preferred_video_style_id=preferred_sid)
