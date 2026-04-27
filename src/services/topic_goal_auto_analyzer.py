from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Protocol

from loguru import logger

from src.services.ai_styles_registry import load_style_registry
from src.utils.paths import project_root


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
        raise ValueError("TopicGoalAutoAnalyzer: AI output phải là JSON object.")
    return data


def goal_registry_path() -> Path:
    return project_root() / "config" / "goal_registry.json"


def default_goal_registry() -> list[dict[str, str]]:
    return [
        {"id": "viral", "label": "Viral", "description": "Tối ưu hook mạnh, gây tò mò, dễ chia sẻ"},
        {"id": "storytelling", "label": "Storytelling", "description": "Tập trung kể chuyện có mở đầu, cao trào, kết thúc"},
        {"id": "kids_discovery", "label": "Kids Discovery", "description": "Video trẻ em khám phá thế giới kỳ diệu"},
        {"id": "mystery", "label": "Mystery", "description": "Tạo cảm giác bí ẩn, hồi hộp, khám phá"},
        {"id": "alien_discovery", "label": "Alien Discovery", "description": "Khám phá người ngoài hành tinh / hành tinh lạ"},
        {"id": "cave_exploration", "label": "Cave Exploration", "description": "Khám phá hang động, thế giới ngầm, bí ẩn"},
        {"id": "beauty_macro", "label": "Beauty / Macro", "description": "Cận cảnh vẻ đẹp, texture, chi tiết nhỏ"},
        {"id": "product_promo", "label": "Product Promotion", "description": "Quảng cáo sản phẩm"},
        {"id": "education", "label": "Education", "description": "Giải thích / giáo dục / hướng dẫn"},
        {"id": "entertainment", "label": "Entertainment", "description": "Giải trí, hài, thú vị"},
        {"id": "cinematic", "label": "Cinematic", "description": "Phong cách điện ảnh, cảm xúc, thẩm mỹ"},
    ]


def load_goal_registry() -> list[dict[str, str]]:
    p = goal_registry_path()
    if not p.is_file():
        return default_goal_registry()
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default_goal_registry()
    if not isinstance(raw, list):
        return default_goal_registry()
    out: list[dict[str, str]] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        gid = str(row.get("id", "")).strip()
        label = str(row.get("label", "")).strip()
        desc = str(row.get("description", "")).strip()
        if gid and label:
            out.append({"id": gid, "label": label, "description": desc})
    return out or default_goal_registry()


class TopicGoalAutoAnalyzer:
    """Tự động phân tích idea để sinh topic, goal và style gợi ý."""

    def __init__(self, ai_text_service: _TextAI) -> None:
        self.ai_text_service = ai_text_service
        self._last_quota_warn_at = 0.0

    def _retry_delay_seconds_from_error(self, msg: str) -> int:
        s = str(msg or "")
        m = re.search(r"retry(?:\s+in)?\s+(\d+(?:\.\d+)?)s", s, flags=re.IGNORECASE)
        if m:
            try:
                return max(0, int(float(m.group(1))))
            except Exception:
                return 0
        m2 = re.search(r"retry_delay\s*\{[^}]*seconds:\s*(\d+)", s, flags=re.IGNORECASE | re.DOTALL)
        if m2:
            try:
                return max(0, int(m2.group(1)))
            except Exception:
                return 0
        return 0

    def _goal_ids(self, rows: list[dict[str, str]]) -> set[str]:
        return {str(x.get("id", "")).strip() for x in rows if str(x.get("id", "")).strip()}

    def _style_ids(self, rows: list[dict[str, Any]]) -> set[str]:
        out: set[str] = set()
        for row in rows:
            sid = str((row or {}).get("id", "")).strip()
            if sid:
                out.add(sid)
        return out

    def _fallback(self, *, idea: str, goal_registry: list[dict[str, str]], style_registry: dict[str, Any]) -> dict[str, Any]:
        goals = self._goal_ids(goal_registry)
        lower = str(idea or "").lower()
        goal = "viral"
        if any(x in lower for x in ("kid", "child", "bé", "trẻ em")) and "kids_discovery" in goals:
            goal = "kids_discovery"
        elif any(x in lower for x in ("alien", "ngoài hành tinh")) and "alien_discovery" in goals:
            goal = "alien_discovery"
        elif any(x in lower for x in ("cave", "hang")) and "cave_exploration" in goals:
            goal = "cave_exploration"
        elif any(x in lower for x in ("story", "kể chuyện")) and "storytelling" in goals:
            goal = "storytelling"
        elif any(x in lower for x in ("product", "sản phẩm", "sale")) and "product_promo" in goals:
            goal = "product_promo"
        video_rows = list(style_registry.get("video_styles") or [])
        video_ids = self._style_ids(video_rows)
        preferred_styles: list[str] = []
        for sid in ("kids_exploration", "alien_world", "mysterious_cave", "cinematic_story", "viral_reel", "product_showcase"):
            if sid in video_ids and sid not in preferred_styles:
                preferred_styles.append(sid)
        picked_style = preferred_styles[:1]
        return {
            "main_topic": str(idea or "").strip(),
            "sub_topics": [],
            "goal_id": goal if goal in goals else "viral",
            "content_type": "short-form video",
            "visual_hooks": [],
            "emotional_hook": "curiosity",
            "recommended_video_style_ids": picked_style,
            "recommended_camera_style_id": "",
            "recommended_lighting_style_id": "",
            "recommended_motion_style_id": "",
            "reason": "Fallback local analyzer.",
        }

    def _validate(
        self,
        data: dict[str, Any],
        *,
        idea: str,
        goal_registry: list[dict[str, str]],
        style_registry: dict[str, Any],
    ) -> dict[str, Any]:
        out = self._fallback(idea=idea, goal_registry=goal_registry, style_registry=style_registry)
        goals = self._goal_ids(goal_registry)
        goal_id = str(data.get("goal_id", "")).strip()
        if goal_id in goals:
            out["goal_id"] = goal_id
        main_topic = str(data.get("main_topic", "")).strip()
        if main_topic:
            out["main_topic"] = main_topic
        for key in ("content_type", "emotional_hook", "reason"):
            raw = str(data.get(key, "")).strip()
            if raw:
                out[key] = raw
        for key in ("sub_topics", "visual_hooks"):
            vals = data.get(key)
            if isinstance(vals, list):
                out[key] = [str(x).strip() for x in vals if str(x).strip()][:8]

        video_ids = self._style_ids(list(style_registry.get("video_styles") or []))
        camera_ids = self._style_ids(list(style_registry.get("camera_styles") or []))
        lighting_ids = self._style_ids(list(style_registry.get("lighting_styles") or []))
        motion_ids = self._style_ids(list(style_registry.get("motion_styles") or []))
        rec_vid = data.get("recommended_video_style_ids")
        if isinstance(rec_vid, list):
            out["recommended_video_style_ids"] = [str(x).strip() for x in rec_vid if str(x).strip() in video_ids][:3]
        c = str(data.get("recommended_camera_style_id", "")).strip()
        l = str(data.get("recommended_lighting_style_id", "")).strip()
        m = str(data.get("recommended_motion_style_id", "")).strip()
        if c in camera_ids:
            out["recommended_camera_style_id"] = c
        if l in lighting_ids:
            out["recommended_lighting_style_id"] = l
        if m in motion_ids:
            out["recommended_motion_style_id"] = m
        return out

    def analyze(
        self,
        *,
        idea: str,
        page_niche: str,
        target_platform: str,
        goal_registry: list[dict[str, str]] | None = None,
        style_registry: dict[str, Any] | None = None,
        model: str | None = None,
    ) -> dict[str, Any]:
        goals = list(goal_registry or load_goal_registry())
        styles = dict(style_registry or load_style_registry())
        prompt = f"""You are an AI video strategist for short-form viral videos.

Analyze the user idea and automatically generate the best Topic and Goal.

User idea:
{str(idea or '').strip()}

Page/Niche:
{str(page_niche or '').strip()}

Target platform:
{str(target_platform or '').strip() or 'Facebook Reels'}

Available goals:
{json.dumps(goals, ensure_ascii=False)}

Available video styles:
{json.dumps(styles.get('video_styles') or [], ensure_ascii=False)}

Return strict JSON:
{{
  "main_topic": "",
  "sub_topics": [],
  "goal_id": "",
  "content_type": "",
  "visual_hooks": [],
  "emotional_hook": "",
  "recommended_video_style_ids": [],
  "recommended_camera_style_id": "",
  "recommended_lighting_style_id": "",
  "recommended_motion_style_id": "",
  "reason": ""
}}

Rules:
- goal_id must exist in available goals.
- style IDs must exist in video styles registry.
- If the idea involves children discovering something magical, prefer kids_discovery.
- If the idea involves alien, prefer alien_discovery.
- If the idea involves caves, prefer cave_exploration or mystery.
- If the idea is general short-form hook content, prefer viral.
- Return JSON only.
""".strip()
        try:
            text = self.ai_text_service.generate_text(prompt=prompt, model=model)
            parsed = _extract_json_object(text)
            return self._validate(parsed, idea=idea, goal_registry=goals, style_registry=styles)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            now = time.time()
            if "429" in msg or "quota" in msg.lower():
                retry_sec = self._retry_delay_seconds_from_error(msg)
                if now - float(self._last_quota_warn_at) >= 60:
                    if retry_sec > 0:
                        logger.warning(
                            "TopicGoalAutoAnalyzer: quota Gemini đã hết, dùng fallback local (thử lại sau ~{}s).",
                            retry_sec,
                        )
                    else:
                        logger.warning("TopicGoalAutoAnalyzer: quota Gemini đã hết, dùng fallback local.")
                    self._last_quota_warn_at = now
                else:
                    logger.debug("TopicGoalAutoAnalyzer quota tiếp diễn, dùng fallback local.")
            else:
                logger.warning("TopicGoalAutoAnalyzer fallback vì lỗi: {}", msg)
            return self._fallback(idea=idea, goal_registry=goals, style_registry=styles)

