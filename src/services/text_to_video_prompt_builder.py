from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
import re
from typing import Any
from urllib.parse import quote

import requests

from src.services.character_profile_normalize import migrate_auto_character_profiles
from src.services.ai_styles_registry import default_style_id, style_prompt_addon


SUPPORTED_LANGUAGES: dict[str, str] = {
    "Tiếng Việt": "Vietnamese",
    "Vietnamese": "Vietnamese",
    "English": "English",
    "Indonesian": "Indonesian",
    "Thai": "Thai",
    "Español": "Spanish",
    "Português": "Portuguese",
    "Français": "French",
    "Deutsch": "German",
    "Italiano": "Italian",
    "日本語": "Japanese",
    "한국어": "Korean",
    "中文 (简体)": "Simplified Chinese",
    "中文 (繁體)": "Traditional Chinese",
    "ไทย": "Thai",
    "Bahasa Indonesia": "Indonesian",
    "हिन्दी": "Hindi",
}

SUPPORTED_GOALS: set[str] = {
    "viral",
    "storytelling",
    "kids_discovery",
    "mystery",
    "alien_discovery",
    "cave_exploration",
    "beauty_macro",
    "product_promo",
    "education",
    "entertainment",
    "cinematic",
    # Backward compatibility
    "bán hàng",
    "gioi thieu san pham",
    "giới thiệu sản phẩm",
    "kể chuyện",
    "giao duc",
    "giáo dục",
    "cinematic reel",
}

SUPPORTED_ASPECTS: set[str] = {"9:16", "16:9", "1:1"}
SUPPORTED_DURATIONS: set[int] = {4, 6, 8}
SUPPORTED_RESOLUTIONS: set[str] = {"720p", "1080p"}


@dataclass
class TextToVideoBuildResult:
    normalized: dict[str, Any]
    character_profile: dict[str, Any]
    scene_plan: dict[str, str]
    final_prompt: str
    analysis: dict[str, Any]
    characters: list[dict[str, Any]]
    environments: list[dict[str, Any]]
    scenes: list[dict[str, Any]]
    video_map: dict[str, Any]


class TextToVideoPromptBuilder:
    """
    Tạo prompt chuẩn cho text-to-video theo pipeline:
    normalize -> character profile -> scene plan -> final prompt.
    """

    def __init__(self) -> None:
        # Cache kết quả phân tích kịch bản bằng Gemini để tránh gọi API lặp lại.
        self._story_analysis_cache: dict[str, dict[str, Any]] = {}

    def build(self, raw_input: dict[str, Any], *, existing_character_profile: dict[str, Any] | None = None) -> TextToVideoBuildResult:
        normalized = self.normalize_video_input(raw_input)
        prebuilt_pipeline = dict(raw_input.get("prebuilt_pipeline") or {})
        pipeline = prebuilt_pipeline if prebuilt_pipeline else self._build_text_to_video_pipeline(input_data=normalized)
        if pipeline:
            normalized["_gemini_story_analysis"] = dict(pipeline.get("analysis") or {})
            normalized["_gemini_characters"] = list(pipeline.get("characters") or [])
            normalized["_gemini_environments"] = list(pipeline.get("environments") or [])
            normalized["_gemini_scenes"] = list(pipeline.get("scenes") or [])
            normalized["_gemini_video_map"] = dict(pipeline.get("video_map") or {})
            normalized["story_contexts"] = list((pipeline.get("analysis") or {}).get("required_environments") or [])
        character_profile = self.build_character_profile(normalized, existing_character_profile=existing_character_profile)
        scene_plan = self.build_scene_plan(input_data=normalized, character_profile=character_profile)
        final_prompt = self.build_final_prompt(
            input_data=normalized,
            character_profile=character_profile,
            scene_plan=scene_plan,
        )
        text_cast_suffix = self._text_only_full_cast_block(normalized, scene_plan)
        prebuilt_final = str(pipeline.get("final_prompt") or "").strip() if isinstance(pipeline, dict) else ""
        source_idea = str((prebuilt_pipeline or {}).get("source_idea") or "").strip()
        current_idea = str(normalized.get("idea") or "").strip()
        # Chỉ tái dùng bản final_prompt đã cache từ Gemini khi ý tưởng input vẫn là bản đã phân tích
        # (cùng biến thể). Nếu khác (vd. tập 2/3 chuỗi series), giữ final_prompt sinh từ idea hiện tại
        # để «Main concept» phản ánh đúng tập đó — tránh 3 prompt giống hệt biến thể 1.
        reuse_cached_final = bool(
            prebuilt_final
            and (
                not source_idea
                or source_idea == current_idea
            )
        )
        if pipeline and prebuilt_final and reuse_cached_final:
            final_prompt = prebuilt_final
            if text_cast_suffix.strip():
                final_prompt = f"{final_prompt.rstrip()}\n\n{text_cast_suffix.strip()}\n"
        return TextToVideoBuildResult(
            normalized=normalized,
            character_profile=character_profile,
            scene_plan=scene_plan,
            final_prompt=final_prompt,
            analysis=dict(pipeline.get("analysis") or {}) if pipeline else {},
            characters=list(pipeline.get("characters") or []) if pipeline else [],
            environments=list(pipeline.get("environments") or []) if pipeline else [],
            scenes=list(pipeline.get("scenes") or []) if pipeline else [],
            video_map=dict(pipeline.get("video_map") or {}) if pipeline else {},
        )

    def normalize_video_input(self, raw_input: dict[str, Any]) -> dict[str, Any]:
        idea = str(raw_input.get("idea", "")).strip()
        topic_goal = dict(raw_input.get("topic_goal") or {})
        topic = str(raw_input.get("topic", "")).strip()
        if not topic:
            topic = str(topic_goal.get("main_topic", "")).strip()
        goal = str(raw_input.get("goal", "")).strip().lower()
        if not goal:
            goal = str(topic_goal.get("goal_id", "")).strip().lower()
        language = str(raw_input.get("language", "")).strip()
        visual_style = str(raw_input.get("visual_style", "")).strip()
        camera_style = str(raw_input.get("camera_style", "")).strip()
        lighting = str(raw_input.get("lighting", "")).strip()
        motion_style = str(raw_input.get("motion_style", "")).strip()
        mood = str(raw_input.get("mood", "")).strip()
        aspect_ratio = str(raw_input.get("aspect_ratio", "")).strip()
        resolution = str(raw_input.get("resolution", "")).strip()
        style_prompt = str(raw_input.get("style_prompt", "")).strip()
        video_style_id = str(raw_input.get("video_style_id", "")).strip()
        character_mode = str(raw_input.get("character_mode", "auto")).strip().lower()
        lock_character_roles = bool(raw_input.get("lock_character_roles", True))
        character_manual = dict(raw_input.get("character_manual") or {})
        auto_character_profiles = migrate_auto_character_profiles(list(raw_input.get("auto_character_profiles") or []))
        duration_sec = int(raw_input.get("duration_sec") or 8)

        if not idea:
            raise ValueError("Thiếu idea (ý tưởng video).")
        if not language:
            raise ValueError("Thiếu language.")
        if language not in SUPPORTED_LANGUAGES and language.lower() != "custom":
            raise ValueError(f"Language chưa hỗ trợ: {language}")
        if goal and goal not in SUPPORTED_GOALS:
            raise ValueError(f"Goal chưa hỗ trợ: {goal}")
        if aspect_ratio and aspect_ratio not in SUPPORTED_ASPECTS:
            raise ValueError(f"Aspect ratio không hợp lệ: {aspect_ratio}")
        if duration_sec not in SUPPORTED_DURATIONS:
            raise ValueError(f"Duration không hợp lệ: {duration_sec}")
        if resolution and resolution not in SUPPORTED_RESOLUTIONS:
            raise ValueError(f"Resolution không hợp lệ: {resolution}")
        if character_mode not in {"auto", "manual"}:
            raise ValueError("Character mode phải là auto hoặc manual.")
        if character_mode == "manual" and not str(character_manual.get("appearance", "")).strip():
            raise ValueError("Manual character cần mô tả appearance.")

        default_video_style = style_prompt_addon(
            "video_styles",
            default_style_id("video_style_id", "cinematic_story"),
            fallback="cinematic realistic video, smooth camera movement, natural motion, high-quality details, coherent visual continuity",
        )
        selected_video_style = style_prompt_addon("video_styles", video_style_id, fallback="")
        primary_video_style = selected_video_style or style_prompt or visual_style or default_video_style
        primary_style_lock = (
            "PRIMARY VIDEO STYLE LOCK (apply to all character/environment/scene details): "
            + str(primary_video_style).strip()
        )
        default_camera = style_prompt_addon(
            "camera_styles",
            default_style_id("camera_style_id", "smooth_dolly_in"),
            fallback="smooth dolly in",
        )
        default_lighting = style_prompt_addon(
            "lighting_styles",
            default_style_id("lighting_style_id", "soft_natural_light"),
            fallback="soft natural light",
        )
        default_motion = style_prompt_addon(
            "motion_styles",
            default_style_id("motion_style_id", "slow_and_smooth"),
            fallback="slow and smooth",
        )
        env_style_prompt = str(raw_input.get("environment_style_prompt", "")).strip() or style_prompt_addon(
            "environment_styles",
            default_style_id("environment_style_id", "environment_cinematic"),
            fallback="cinematic environment, atmospheric lighting, rich details, movie still composition",
        )
        char_img_style_prompt = str(raw_input.get("character_image_style_prompt", "")).strip() or style_prompt_addon(
            "character_image_styles",
            default_style_id("character_image_style_id", "character_cinematic_realistic"),
            fallback="cinematic realistic portrait, natural skin texture, professional composition, soft cinematic lighting, high detail",
        )
        env_style_prompt = f"{primary_style_lock}. {env_style_prompt}".strip()
        char_img_style_prompt = f"{primary_style_lock}. {char_img_style_prompt}".strip()
        return {
            "idea": idea,
            "topic": topic,
            "goal": goal or "viral",
            "topic_goal": topic_goal,
            "language": language,
            "language_provider_label": SUPPORTED_LANGUAGES.get(language, language),
            "visual_style": primary_video_style,
            "camera_style": camera_style or default_camera,
            "lighting": lighting or default_lighting,
            "motion_style": motion_style or default_motion,
            "mood": mood or "inspiring",
            "style_prompt": f"{primary_style_lock}. {style_prompt}".strip(". "),
            "aspect_ratio": aspect_ratio or "9:16",
            "duration_sec": duration_sec,
            "resolution": resolution or "720p",
            "character_mode": character_mode,
            "lock_character_roles": lock_character_roles,
            "character_manual": character_manual,
            "auto_character_profiles": auto_character_profiles,
            "environment_style_prompt": env_style_prompt,
            "character_image_style_prompt": char_img_style_prompt,
            "story_contexts": list(raw_input.get("story_contexts") or []) if isinstance(raw_input.get("story_contexts"), list) else [],
        }

    def build_character_profile(
        self,
        input_data: dict[str, Any],
        *,
        existing_character_profile: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if existing_character_profile:
            profile = dict(existing_character_profile)
            if not str(profile.get("character_lock_id", "")).strip():
                profile["character_lock_id"] = self._character_lock_id(profile)
            return profile
        if input_data["character_mode"] == "manual":
            m = dict(input_data.get("character_manual") or {})
            profile = {
                "character_name": str(m.get("name", "Character")).strip() or "Character",
                "character_description": str(m.get("appearance", "")).strip(),
                "outfit": str(m.get("outfit", "keep consistent outfit")).strip(),
                "facial_features": str(m.get("facial_features", "keep facial identity consistent")).strip(),
                "personality": str(m.get("personality", "confident and natural")).strip(),
                "consistency_rules": [
                    "Keep the same character identity throughout the whole video",
                    "Do not change face, hairstyle, outfit, age, body shape or ethnicity",
                    "Maintain consistent clothing colors and facial features in every shot",
                ],
            }
            profile["character_lock_id"] = self._character_lock_id(profile)
            return profile
        auto_chars = migrate_auto_character_profiles(list(input_data.get("auto_character_profiles") or []))
        if auto_chars:
            chars: list[dict[str, Any]] = []
            for row in auto_chars:
                if not isinstance(row, dict):
                    continue
                name = str(row.get("name", "")).strip() or "Character"
                role = str(row.get("role", "")).strip()
                gender = str(row.get("gender", "")).strip()
                age = str(row.get("age", "")).strip()
                appearance = str(row.get("appearance", "")).strip() or "consistent appearance"
                facial_features = str(row.get("facial_features", "")).strip() or "stable facial identity"
                outfit = str(row.get("outfit", "")).strip() or "consistent outfit"
                personality = str(row.get("personality", "")).strip() or "natural personality"
                consistency_note = str(row.get("consistency_note", "")).strip()
                reference_image_path = str(row.get("reference_image_path", "")).strip()
                chars.append(
                    {
                        "name": name,
                        "role": role,
                        "gender": gender,
                        "age": age,
                        "appearance": appearance,
                        "facial_features": facial_features,
                        "outfit": outfit,
                        "personality": personality,
                        "consistency_note": consistency_note,
                        "reference_image_path": reference_image_path,
                    }
                )
            if chars:
                main = chars[0]
                supports = [
                    (
                        f"{x['name']} (role={x.get('role','')}, gender={x.get('gender','')}, age={x.get('age','')}): "
                        f"{x['appearance']}; facial={x.get('facial_features','')}; outfit={x['outfit']}; "
                        f"personality={x['personality']}; consistency_note={x.get('consistency_note','')}; "
                        f"reference_image={x.get('reference_image_path','') or 'none'}"
                    )
                    for x in chars[1:]
                ]
                profile = {
                    "character_name": main["name"],
                    "character_description": main["appearance"],
                    "outfit": main["outfit"],
                    "facial_features": main.get("facial_features", "keep facial identity consistent for each character"),
                    "personality": main["personality"],
                    "reference_image_path": str(main.get("reference_image_path", "")).strip(),
                    "supporting_characters": supports,
                    "consistency_rules": [
                        "Keep each character identity consistent throughout the whole video",
                        "Do not swap faces/outfits/roles between characters across shots",
                        "Maintain stable relationships and role logic among all characters",
                        main.get("consistency_note", "").strip() or "Maintain stable identity for all characters",
                    ],
                }
                profile["character_lock_id"] = self._character_lock_id(profile)
                return profile
        analysis = input_data.get("_gemini_story_analysis") if isinstance(input_data.get("_gemini_story_analysis"), dict) else {}
        analyzed_chars = input_data.get("_gemini_characters") if isinstance(input_data.get("_gemini_characters"), list) else []
        if not analyzed_chars and isinstance(analysis, dict):
            analyzed_chars = analysis.get("characters")
        if isinstance(analyzed_chars, list) and analyzed_chars:
            chars: list[dict[str, str]] = []
            for row in analyzed_chars:
                if not isinstance(row, dict):
                    continue
                name = str(row.get("name", "")).strip() or "Character"
                role = str(row.get("role", "")).strip() or "support"
                gender = str(row.get("gender", "")).strip() or "unspecified"
                age = str(row.get("age", "")).strip()
                appearance = str(row.get("appearance", "")).strip() or "consistent appearance"
                facial_features = str(row.get("facial_features", "")).strip() or "stable facial identity"
                outfit = str(row.get("outfit", "")).strip() or "consistent outfit"
                personality = str(row.get("personality", "")).strip() or "natural personality"
                consistency_note = str(row.get("consistency_note", "")).strip()
                chars.append(
                    {
                        "name": name,
                        "role": role,
                        "gender": gender,
                        "age": age,
                        "appearance": appearance,
                        "facial_features": facial_features,
                        "outfit": outfit,
                        "personality": personality,
                        "consistency_note": consistency_note,
                        "reference_image_path": str(row.get("reference_image_path", "")).strip(),
                    }
                )
            if chars:
                main = chars[0]
                supports = [
                    (
                        f"{x['name']} (role={x.get('role','')}, gender={x.get('gender','')}, age={x.get('age','')}): "
                        f"{x['appearance']}; facial={x.get('facial_features','')}; outfit={x['outfit']}; "
                        f"personality={x['personality']}; consistency_note={x.get('consistency_note','')}; "
                        f"reference_image={x.get('reference_image_path','') or 'none'}"
                    )
                    for x in chars[1:]
                ]
                context_rules = [
                    f"Keep context stable for {ctx}" for ctx in list(input_data.get("story_contexts") or [])[:5] if str(ctx).strip()
                ]
                profile = {
                    "character_name": main["name"],
                    "character_description": main["appearance"],
                    "outfit": main["outfit"],
                    "facial_features": main.get("facial_features", "keep facial identity consistent for each character"),
                    "personality": main["personality"],
                    "reference_image_path": str(main.get("reference_image_path", "")).strip(),
                    "supporting_characters": supports,
                    "consistency_rules": [
                        "Keep each character identity consistent throughout the whole video",
                        "Do not swap faces/outfits/roles between characters across shots",
                        "Maintain stable relationships and role logic among all characters",
                        main.get("consistency_note", "").strip() or "Maintain stable identity for all characters",
                        *context_rules,
                    ],
                }
                profile["character_lock_id"] = self._character_lock_id(profile)
                return profile
        inferred_chars = self._infer_characters_from_idea(
            idea=str(input_data.get("idea", "")).strip(),
            topic=str(input_data.get("topic", "")).strip(),
        )
        if inferred_chars:
            main = inferred_chars[0]
            supports = [
                (
                    f"{x['name']} (role={x.get('role','')}): {x['appearance']}; "
                    f"facial={x.get('facial_features','stable facial identity')}; "
                    f"outfit={x.get('outfit','consistent outfit')}; "
                    f"personality={x.get('personality','natural personality')}"
                )
                for x in inferred_chars[1:]
            ]
            profile = {
                "character_name": main["name"],
                "character_description": main["appearance"],
                "outfit": main.get("outfit", "consistent outfit"),
                "facial_features": main.get("facial_features", "stable facial identity"),
                "personality": main.get("personality", "natural personality"),
                "supporting_characters": supports,
                "consistency_rules": [
                    "Keep each character identity consistent throughout the whole video",
                    "Do not swap faces/outfits/roles between characters across shots",
                    "Maintain stable relationships and role logic among all characters",
                    "Respect the original cast inferred from user idea; do not collapse to one generic person",
                ],
            }
            profile["character_lock_id"] = self._character_lock_id(profile)
            return profile
        # Auto character profile heuristic (không gọi model text để tránh phụ thuộc runtime).
        profile = {
            "character_name": "Linh",
            "character_description": "A 25-year-old Vietnamese creator with natural black hair, warm smile and slim build",
            "outfit": "clean neutral outfit with consistent colors",
            "facial_features": "oval face, expressive eyes, natural skin texture",
            "personality": "confident, calm, friendly",
            "consistency_rules": [
                "Keep the same character identity throughout the whole video",
                "Do not change face, hairstyle, outfit, age, body shape or ethnicity",
                "Maintain consistent clothing colors and facial features in every shot",
            ],
        }
        profile["character_lock_id"] = self._character_lock_id(profile)
        return profile

    def _infer_characters_from_idea(self, *, idea: str, topic: str) -> list[dict[str, str]]:
        """
        Heuristic tách nhiều nhân vật từ ý tưởng/topic khi user không nhập bảng character thủ công.
        """
        text = " ".join([str(idea or "").strip(), str(topic or "").strip()]).strip()
        if not text:
            return []
        lowered = text.lower()
        # Ví dụ: "nhân vật: mèo cam, chó Luka và bé An"
        markers = ("nhân vật", "characters", "character", "cast", "gồm", "bao gồm", "with", "với")
        segment = text
        for mk in markers:
            idx = lowered.find(mk)
            if idx >= 0:
                segment = text[idx:]
                break
        segment = re.sub(r"(?i)\b(nhân vật|characters?|cast|gồm|bao gồm|with|với)\b[:：]?", " ", segment)
        # Chuẩn hóa dấu nối danh sách nhân vật.
        segment = re.sub(r"(?i)\b(và|and|cùng|,|;|\/|\|)\b", "|", segment)
        segment = segment.replace(" - ", "|")
        parts = [p.strip(" .:-\n\t") for p in segment.split("|")]
        cleaned: list[str] = []
        for p in parts:
            if not p:
                continue
            # Bỏ các cụm mô tả hành động quá dài, giữ phần tên/cụm nhân vật.
            if len(p.split()) > 10:
                continue
            if p.lower() in {"video", "story", "câu chuyện", "scene", "bối cảnh"}:
                continue
            cleaned.append(p)
        # Khử trùng lặp theo lowercase.
        uniq: list[str] = []
        for p in cleaned:
            key = p.lower()
            if key not in [x.lower() for x in uniq]:
                uniq.append(p)
        # Nếu không tách được danh sách, thử pattern "A và B".
        if len(uniq) <= 1:
            m = re.search(r"(?i)([A-Za-zÀ-ỹ0-9_ ]{2,40})\s+(và|and)\s+([A-Za-zÀ-ỹ0-9_ ]{2,40})", text)
            if m:
                a = m.group(1).strip(" .:-")
                b = m.group(3).strip(" .:-")
                uniq = [a, b]
        # Giới hạn để prompt không rối.
        uniq = [x for x in uniq if x][:5]
        if not uniq:
            return []
        out: list[dict[str, str]] = []
        for idx, name in enumerate(uniq):
            role = "main" if idx == 0 else "supporting"
            out.append(
                {
                    "name": name,
                    "role": role,
                    "appearance": f"{name}: keep visual identity consistent with user story idea",
                    "facial_features": "stable facial identity",
                    "outfit": "consistent outfit across all shots",
                    "personality": "natural personality aligned with story role",
                }
            )
        return out

    def build_scene_plan(self, input_data: dict[str, Any], character_profile: dict[str, Any]) -> dict[str, str]:
        _ = character_profile
        gemini_scenes = input_data.get("_gemini_scenes") if isinstance(input_data.get("_gemini_scenes"), list) else []
        if gemini_scenes:
            out: dict[str, str] = {}
            for i, row in enumerate(gemini_scenes, start=1):
                if not isinstance(row, dict):
                    continue
                role = str(row.get("scene_role", "")).strip().lower()
                key = "scene_1" if role == "start" else ("scene_3" if role == "end" else f"scene_{i}")
                text = str(row.get("action", "")).strip()
                cam = str(row.get("camera", "")).strip()
                rng = str(row.get("time_range", "")).strip()
                val = " | ".join([x for x in [rng, text, f"Camera: {cam}" if cam else ""] if x]).strip()
                if val:
                    out[key] = val
            if out:
                return out
        d = int(input_data.get("duration_sec", 8))
        if d <= 4:
            return {
                "scene_1": "0-1.5s: Strong hook with clear subject framing",
                "scene_2": "1.5-4s: Main action + concise visual reveal",
            }
        if d <= 6:
            return {
                "scene_1": "0-2s: Establishing hook and context",
                "scene_2": "2-4.5s: Main action with subject focus",
                "scene_3": "4.5-6s: End reveal and clean finish",
            }
        return {
            "scene_1": "0-2s: Establishing/hook shot",
            "scene_2": "2-5s: Main action and narrative focus",
            "scene_3": "5-8s: Ending reveal / payoff",
        }

    def build_final_prompt(self, input_data: dict[str, Any], character_profile: dict[str, Any], scene_plan: dict[str, Any]) -> str:
        lock_id = str(character_profile.get("character_lock_id", "")).strip() or self._character_lock_id(character_profile)
        # Final prompt cho model tạo video luôn giữ tiếng Anh để ổn định chất lượng instruction.
        idea_localized = self._localize_text_for_language(
            text=str(input_data.get("idea", "")).strip(),
            language_label="English",
        )
        topic_localized = self._localize_text_for_language(
            text=str(input_data.get("topic", "")).strip(),
            language_label="English",
        )
        scene_text = "\n".join([f"- {k}: {v}" for k, v in scene_plan.items()])
        consistency = "\n".join([f"- {x}" for x in character_profile.get("consistency_rules", [])])
        cultural_rules = self._cultural_localization_rules(input_data["language_provider_label"])
        role_lock_rules = (
            "- Preserve each character's role by name across all shots (main/support mapping is immutable).\n"
            "- Never reassign actions of one named character to another named character."
            if bool(input_data.get("lock_character_roles", True))
            else "- Character role lock is relaxed; still keep visual identity stable."
        )
        ref_lines = self._reference_image_lines(character_profile)
        text_cast_block = self._text_only_full_cast_block(input_data, scene_plan)
        style_lock = str(input_data.get("style_prompt", "")).strip()
        context_lines = [f"- {str(x).strip()}" for x in list(input_data.get("story_contexts") or []) if str(x).strip()]
        tg = dict(input_data.get("topic_goal") or {})
        tg_sub_topics = [str(x).strip() for x in list(tg.get("sub_topics") or []) if str(x).strip()]
        tg_hooks = [str(x).strip() for x in list(tg.get("visual_hooks") or []) if str(x).strip()]
        tg_content_type = str(tg.get("content_type", "")).strip()
        tg_emotional = str(tg.get("emotional_hook", "")).strip()
        tg_sub_topic_lines = "\n".join([f"- {x}" for x in tg_sub_topics]) if tg_sub_topics else "- None"
        tg_hook_lines = "\n".join([f"- {x}" for x in tg_hooks]) if tg_hooks else "- None"
        visual_style_text = str(input_data.get("visual_style", "")).strip()
        style_lock_text = f"{visual_style_text} {style_lock}".lower()
        non_realistic_style = any(
            k in style_lock_text
            for k in ("cartoon", "anime", "3d", "pixar", "stylized", "toon", "chibi", "hoạt hình")
        )
        visual_quality_line = (
            "Keep stylized non-photorealistic rendering. Do NOT generate realistic live-action look."
            if non_realistic_style
            else "High-quality realistic details."
        )
        return f"""Create a {input_data['duration_sec']}-second {input_data['aspect_ratio']} video.

Global language enforcement:
Use {input_data['language_provider_label']} as the primary and only language for narration tone, spoken words, and any on-screen text unless proper names require otherwise.
If source idea/topic is written in another language, adapt and express all creative output semantics in {input_data['language_provider_label']}.

Main concept:
{idea_localized or input_data['idea']}

Topic:
{topic_localized or input_data.get('topic') or 'General social content'}

Story contexts:
{chr(10).join(context_lines) if context_lines else "- infer coherent context from idea and topic"}

Goal:
{input_data['goal']}

Content type:
{tg_content_type or "short-form narrative video"}

Sub-topics:
{tg_sub_topic_lines}

Visual hooks:
{tg_hook_lines}

Emotional hook:
{tg_emotional or "curiosity and emotional engagement"}

Character:
Character lock id: {lock_id}
{character_profile['character_description']}
Outfit: {character_profile['outfit']}
Facial features: {character_profile['facial_features']}
Personality: {character_profile['personality']}
Reference image: {str(character_profile.get('reference_image_path', '')).strip() or 'none'}
Supporting characters:
{chr(10).join([f"- {x}" for x in character_profile.get("supporting_characters", [])]) or "- None"}

Character consistency:
{consistency}

Character lock rules (very strict):
- Treat Character lock id as immutable identity across all shots and episodes.
- Keep face geometry, skin tone, hairstyle, age impression, body shape, outfit palette, and core accessories unchanged.
- Never replace, re-cast, or morph the main character into another person.
- Supporting characters must keep their own fixed identities; do not swap roles or faces.
- Single on-screen body per named cast member: never show the same named character as two separate identical people in one shot unless the story explicitly requires twins; do not clone the lead or elders as unnamed duplicate extras.
- If continuity conflict occurs, prioritize identity consistency over cinematic variation.
{role_lock_rules}

Scene plan:
{scene_text}

Action:
{input_data['motion_style']}

Motion:
{input_data['motion_style']}

Camera:
{input_data['camera_style']}. Smooth continuous camera movement. Avoid abrupt cuts unless necessary.

Lighting:
{input_data['lighting']}

Visual style:
{visual_style_text}. {visual_quality_line}
Mood:
{input_data['mood']}
Environment style:
{str(input_data.get('environment_style_prompt', '')).strip() or 'cinematic environment, atmospheric lighting, rich details'}
Selected style lock:
{style_lock or "Use the selected UI style as strict visual direction."}

Language rules:
If any text appears in the video, it must be in {input_data['language_provider_label']}. Keep text short, natural and readable.
All spoken dialogue and captions must be fully in {input_data['language_provider_label']}. Do not mix languages in dialogue.

Cultural localization rules:
{cultural_rules}

Continuity rules:
The video must feel like one continuous, coherent scene. Maintain consistent environment, character, outfit, lighting, camera direction and object positions.

Character reference image mapping (strict):
{ref_lines}

{text_cast_block if text_cast_block.strip() else ""}
Style lock rules (strict):
- Keep ONE consistent visual style across the whole clip; do not mix cartoon/realistic styles.
- Treat the selected Video Style as the single source of truth for characters, environments, props, and all scene renders.
- Do not override the selected Video Style with camera/lighting/mood defaults; those are only supporting constraints.
- If reference character images are provided, match face identity, body proportions, hair, outfit palette, and overall rendering style to those references.
- If no usable reference files exist for some characters, strictly follow the FULL CAST — TEXT-ONLY section, Environment style, and Scene plan together as the source of truth for identity and staging.
- Prioritize selected style + reference images over random stylistic variation.

Negative rules:
No distorted hands, no extra fingers, no changing faces, no duplicated characters (including two copies of the same grandfather/elder with identical face/outfit in one frame), no flickering text, no unreadable text, no random logos, no watermark, no sudden scene jumps, no inconsistent clothing.
Avoid mixing multiple languages in captions or on-screen typography.
"""

    def _build_text_to_video_pipeline(self, input_data: dict[str, Any]) -> dict[str, Any]:
        """
        Luồng chuẩn Text-to-Video:
        idea -> analysis -> character bible -> environment bible -> scene breakdown -> video map -> final prompt.
        """
        use_story_ai = os.environ.get("FB_T2V_GEMINI_STORY_ANALYSIS", "1").strip().lower() not in {"0", "false", "off"}
        if not use_story_ai:
            return {}
        if str(input_data.get("character_mode", "auto")).strip().lower() != "auto":
            return {}
        if not self._resolve_gemini_api_key():
            return {}
        idea = str(input_data.get("idea", "")).strip()
        if not idea:
            return {}
        topic = str(input_data.get("topic", "")).strip()
        lang = str(input_data.get("language_provider_label", "")).strip()
        style = str(input_data.get("visual_style", "")).strip()
        cache_key = hashlib.sha1(
            "|".join([idea, topic, lang, style, str(input_data.get("duration_sec", 8))]).encode("utf-8")
        ).hexdigest()
        if cache_key in self._story_analysis_cache:
            return dict(self._story_analysis_cache[cache_key])
        analysis = self.analyze_video_idea_with_gemini(input_data)
        if not analysis:
            return {}
        characters = self.build_character_bible_with_gemini(analysis=analysis, input_data=input_data)
        environments = self.build_environment_bible_with_gemini(analysis=analysis, input_data=input_data)
        scenes = self.build_scene_breakdown_with_gemini(
            analysis=analysis,
            characters=characters,
            environments=environments,
            input_data=input_data,
        )
        video_map = self.build_video_prompt_map(
            analysis=analysis,
            characters=characters,
            environments=environments,
            scenes=scenes,
            input_data=input_data,
        )
        final_prompt = self.build_final_veo_prompt_from_video_map(video_map)
        out = {
            "analysis": analysis,
            "characters": characters,
            "environments": environments,
            "scenes": scenes,
            "video_map": video_map,
            "final_prompt": final_prompt,
        }
        self._story_analysis_cache[cache_key] = dict(out)
        return out

    def analyze_video_idea_with_gemini(self, input_data: dict[str, Any]) -> dict[str, Any]:
        schema = {
            "main_concept": "",
            "genre": "",
            "mood": "",
            "core_action": "",
            "visual_keywords": [],
            "required_characters": [],
            "required_environments": [],
            "continuity_focus": [],
            "story_arc": {
                "opening": "",
                "rising_action": "",
                "climax": "",
                "ending": "",
            },
            "related_story_threads": [],
            "story_flow_rules": [],
        }
        prompt = f"""You are a professional AI video director and prompt architect.

Analyze the following video idea and return a strict JSON object.

User idea:
{str(input_data.get("idea", "")).strip()}

User settings:
- Language: {str(input_data.get("language_provider_label", "Vietnamese")).strip()}
- Visual style: {str(input_data.get("visual_style", "")).strip()}
- Duration: {int(input_data.get("duration_sec") or 8)}s
- Aspect ratio: {str(input_data.get("aspect_ratio", "9:16")).strip()}
- Mood: {str(input_data.get("mood", "")).strip()}
- Camera style: {str(input_data.get("camera_style", "")).strip()}
- Lighting: {str(input_data.get("lighting", "")).strip()}

Return JSON with:
{json.dumps(schema, ensure_ascii=False)}

Rules:
- Do not write markdown.
- Return JSON only.
- Keep the analysis useful for text-to-video prompt generation.
- Build a coherent narrative arc: opening -> rising action -> climax -> ending.
- Add related story threads that are connected to the main story (not random side plots).
- Keep continuity strict across character identity, environment, props, and emotional progression.
"""
        data = self._gemini_generate_json_object(prompt)
        if not data:
            return {}
        return {
            "main_concept": str(data.get("main_concept", "")).strip(),
            "genre": str(data.get("genre", "")).strip(),
            "mood": str(data.get("mood", "")).strip(),
            "core_action": str(data.get("core_action", "")).strip(),
            "visual_keywords": [str(x).strip() for x in list(data.get("visual_keywords") or []) if str(x).strip()],
            "required_characters": [str(x).strip() for x in list(data.get("required_characters") or []) if str(x).strip()],
            "required_environments": [str(x).strip() for x in list(data.get("required_environments") or []) if str(x).strip()],
            "continuity_focus": [str(x).strip() for x in list(data.get("continuity_focus") or []) if str(x).strip()],
            "story_arc": {
                "opening": str((data.get("story_arc") or {}).get("opening", "")).strip(),
                "rising_action": str((data.get("story_arc") or {}).get("rising_action", "")).strip(),
                "climax": str((data.get("story_arc") or {}).get("climax", "")).strip(),
                "ending": str((data.get("story_arc") or {}).get("ending", "")).strip(),
            },
            "related_story_threads": [str(x).strip() for x in list(data.get("related_story_threads") or []) if str(x).strip()],
            "story_flow_rules": [str(x).strip() for x in list(data.get("story_flow_rules") or []) if str(x).strip()],
        }

    def build_character_bible_with_gemini(self, analysis: dict[str, Any], input_data: dict[str, Any]) -> list[dict[str, Any]]:
        schema = [{
            "character_id": "char_001",
            "name": "",
            "role": "",
            "age": "",
            "gender": "",
            "ethnicity_or_look": "",
            "face": "",
            "hair": "",
            "body": "",
            "outfit": "",
            "outfit_colors": [],
            "accessories": [],
            "personality": "",
            "expression_range": "",
            "movement_style": "",
            "consistency_rules": [],
        }]
        prompt = f"""You are a character designer for AI video generation.

Based on the analysis below, create detailed character bible records for all characters needed in the video.

Analysis JSON:
{json.dumps(analysis, ensure_ascii=False)}

User idea:
{str(input_data.get("idea", "")).strip()}

Return strict JSON array. Each character must include:
{json.dumps(schema[0], ensure_ascii=False)}

Rules:
- Make characters visually specific and reusable.
- Keep details stable for text-to-video consistency.
- Do not return markdown.
- Return JSON only.
"""
        rows = self._gemini_generate_json_array(prompt)
        out: list[dict[str, Any]] = []
        for i, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                continue
            cid = str(row.get("character_id", "")).strip() or f"char_{i:03d}"
            out.append(
                {
                    "character_id": cid,
                    "name": str(row.get("name", "")).strip() or f"Character {i}",
                    "role": str(row.get("role", "")).strip() or ("main character" if i == 1 else "support"),
                    "age": str(row.get("age", "")).strip(),
                    "gender": str(row.get("gender", "unspecified")).strip().lower() or "unspecified",
                    "ethnicity_or_look": str(row.get("ethnicity_or_look", "")).strip(),
                    "face": str(row.get("face", "")).strip(),
                    "hair": str(row.get("hair", "")).strip(),
                    "body": str(row.get("body", "")).strip(),
                    "outfit": str(row.get("outfit", "")).strip(),
                    "outfit_colors": [str(x).strip() for x in list(row.get("outfit_colors") or []) if str(x).strip()],
                    "accessories": [str(x).strip() for x in list(row.get("accessories") or []) if str(x).strip()],
                    "personality": str(row.get("personality", "")).strip(),
                    "expression_range": str(row.get("expression_range", "")).strip(),
                    "movement_style": str(row.get("movement_style", "")).strip(),
                    "consistency_rules": [str(x).strip() for x in list(row.get("consistency_rules") or []) if str(x).strip()],
                }
            )
        return out

    def build_environment_bible_with_gemini(self, analysis: dict[str, Any], input_data: dict[str, Any]) -> list[dict[str, Any]]:
        schema = [{
            "environment_id": "env_001",
            "name": "",
            "location_type": "",
            "time_of_day": "",
            "lighting": "",
            "color_palette": [],
            "props": [],
            "spatial_layout": "",
            "atmosphere": "",
            "consistency_rules": [],
        }]
        prompt = f"""You are an environment designer for AI video generation.

Based on the analysis below, create detailed environment bible records for all locations and contexts needed in the video.

Analysis JSON:
{json.dumps(analysis, ensure_ascii=False)}

User idea:
{str(input_data.get("idea", "")).strip()}

Return strict JSON array. Each environment must include:
{json.dumps(schema[0], ensure_ascii=False)}

Rules:
- Make the environment visually specific and reusable.
- Include props important for continuity.
- Do not return markdown.
- Return JSON only.
"""
        rows = self._gemini_generate_json_array(prompt)
        out: list[dict[str, Any]] = []
        for i, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                continue
            eid = str(row.get("environment_id", "")).strip() or f"env_{i:03d}"
            out.append(
                {
                    "environment_id": eid,
                    "name": str(row.get("name", "")).strip() or f"Environment {i}",
                    "location_type": str(row.get("location_type", "")).strip(),
                    "time_of_day": str(row.get("time_of_day", "")).strip(),
                    "lighting": str(row.get("lighting", "")).strip(),
                    "color_palette": [str(x).strip() for x in list(row.get("color_palette") or []) if str(x).strip()],
                    "props": [str(x).strip() for x in list(row.get("props") or []) if str(x).strip()],
                    "spatial_layout": str(row.get("spatial_layout", "")).strip(),
                    "atmosphere": str(row.get("atmosphere", "")).strip(),
                    "consistency_rules": [str(x).strip() for x in list(row.get("consistency_rules") or []) if str(x).strip()],
                }
            )
        return out

    def build_scene_breakdown_with_gemini(
        self,
        analysis: dict[str, Any],
        characters: list[dict[str, Any]],
        environments: list[dict[str, Any]],
        input_data: dict[str, Any],
    ) -> list[dict[str, Any]]:
        duration = int(input_data.get("duration_sec") or 8)
        beat_guide = self._duration_micro_beats(duration)
        prompt = f"""You are a professional video director.

Create a detailed scene breakdown for a short text-to-video generation.

Inputs:
Analysis:
{json.dumps(analysis, ensure_ascii=False)}

Characters:
{json.dumps(characters, ensure_ascii=False)}

Environments:
{json.dumps(environments, ensure_ascii=False)}

User settings:
- Duration: {duration}s
- Aspect ratio: {str(input_data.get("aspect_ratio", "9:16")).strip()}
- Visual style: {str(input_data.get("visual_style", "")).strip()}
- Mood: {str(input_data.get("mood", "")).strip()}
- Camera style: {str(input_data.get("camera_style", "")).strip()}
- Lighting: {str(input_data.get("lighting", "")).strip()}

Narrative micro-beat guide (must follow tightly):
{beat_guide}

Return strict JSON array. Each scene must include:
{{
  "scene_id": "scene_001",
  "time_range": "",
  "scene_role": "start|middle|end",
  "character_ids": [],
  "environment_id": "",
  "action": "",
  "camera": "",
  "lighting": "",
  "emotion": "",
  "continuity_notes": []
}}

Rules:
- The scenes must feel like one continuous video.
- Reuse the same character IDs and environment IDs.
- Avoid abrupt location changes.
- Keep actions possible within the duration.
- Explicitly preserve narrative progression from opening -> rising action -> climax -> ending.
- Integrate related story threads naturally as supporting details, not detached events.
- Ensure each scene includes a concise micro-beat transition so the arc feels complete even in short duration.
- Return JSON only.
"""
        rows = self._gemini_generate_json_array(prompt)
        out: list[dict[str, Any]] = []
        for i, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                continue
            out.append(
                {
                    "scene_id": str(row.get("scene_id", "")).strip() or f"scene_{i:03d}",
                    "time_range": str(row.get("time_range", "")).strip(),
                    "scene_role": str(row.get("scene_role", "")).strip() or ("start" if i == 1 else ("end" if i >= 3 else "middle")),
                    "character_ids": [str(x).strip() for x in list(row.get("character_ids") or []) if str(x).strip()],
                    "environment_id": str(row.get("environment_id", "")).strip(),
                    "action": str(row.get("action", "")).strip(),
                    "camera": str(row.get("camera", "")).strip(),
                    "lighting": str(row.get("lighting", "")).strip(),
                    "emotion": str(row.get("emotion", "")).strip(),
                    "continuity_notes": [str(x).strip() for x in list(row.get("continuity_notes") or []) if str(x).strip()],
                }
            )
        return out

    def build_video_prompt_map(
        self,
        analysis: dict[str, Any],
        characters: list[dict[str, Any]],
        environments: list[dict[str, Any]],
        scenes: list[dict[str, Any]],
        input_data: dict[str, Any],
    ) -> dict[str, Any]:
        duration = int(input_data.get("duration_sec") or 8)
        style_settings = {
            "visual_style": str(input_data.get("visual_style", "")).strip(),
            "aspect_ratio": str(input_data.get("aspect_ratio", "9:16")).strip(),
            "duration_sec": duration,
            "camera_style": str(input_data.get("camera_style", "")).strip(),
            "lighting": str(input_data.get("lighting", "")).strip(),
            "mood": str(input_data.get("mood", "")).strip(),
            "language": str(input_data.get("language_provider_label", "Vietnamese")).strip(),
        }
        continuity: list[str] = [str(x).strip() for x in list(analysis.get("continuity_focus") or []) if str(x).strip()]
        continuity.extend([str(x).strip() for x in list(analysis.get("story_flow_rules") or []) if str(x).strip()])
        # khử trùng lặp, giữ thứ tự.
        uniq_continuity: list[str] = []
        for item in continuity:
            if item and item not in uniq_continuity:
                uniq_continuity.append(item)
        continuity = uniq_continuity
        for c in characters:
            for r in list(c.get("consistency_rules") or [])[:2]:
                rs = str(r).strip()
                if rs and rs not in continuity:
                    continuity.append(rs)
        for e in environments:
            for r in list(e.get("consistency_rules") or [])[:2]:
                rs = str(r).strip()
                if rs and rs not in continuity:
                    continuity.append(rs)
        negative_rules = [
            "No face changes",
            "No outfit changes",
            "No duplicated character",
            "No distorted hands",
            "No extra fingers",
            "No unreadable text",
            "No random logos",
            "No watermark",
            "No sudden scene jumps",
        ]
        return {
            "video_id": f"flow_vid_{hashlib.sha1(str(input_data.get('idea', '')).encode('utf-8')).hexdigest()[:8]}",
            "mode": "text_to_video",
            "main_concept": str(analysis.get("main_concept", "")).strip() or str(input_data.get("idea", "")).strip(),
            "story_arc": dict(analysis.get("story_arc") or {}),
            "related_story_threads": [str(x).strip() for x in list(analysis.get("related_story_threads") or []) if str(x).strip()],
            "micro_beats": self._duration_micro_beats(duration),
            "style_settings": style_settings,
            "analysis": analysis,
            "characters": characters,
            "environments": environments,
            "scenes": scenes,
            "characters_used": [str(x.get("character_id", "")).strip() for x in characters if str(x.get("character_id", "")).strip()],
            "environments_used": [str(x.get("environment_id", "")).strip() for x in environments if str(x.get("environment_id", "")).strip()],
            "scene_ids": [str(x.get("scene_id", "")).strip() for x in scenes if str(x.get("scene_id", "")).strip()],
            "continuity_rules": continuity,
            "negative_rules": negative_rules,
        }

    def build_final_veo_prompt_from_video_map(self, video_map: dict[str, Any]) -> str:
        st = dict(video_map.get("style_settings") or {})
        chars = list(video_map.get("characters") or [])
        envs = list(video_map.get("environments") or [])
        scenes = list(video_map.get("scenes") or [])
        char_desc = "\n".join(
            [
                f"- {c.get('name','Character')}: {c.get('face','')}; hair={c.get('hair','')}; body={c.get('body','')}; "
                f"outfit={c.get('outfit','')}; personality={c.get('personality','')}"
                for c in chars
            ]
        ) or "- None"
        env_desc = "\n".join(
            [
                f"- {e.get('name','Environment')}: {e.get('location_type','')}; lighting={e.get('lighting','')}; "
                f"palette={', '.join(e.get('color_palette') or [])}; props={', '.join(e.get('props') or [])}"
                for e in envs
            ]
        ) or "- None"
        scene_desc = "\n".join(
            [
                f"- {s.get('scene_role','scene')} {s.get('time_range','')}: {s.get('action','')} "
                f"Camera: {s.get('camera','')}. Lighting: {s.get('lighting','')}."
                for s in scenes
            ]
        ) or "- None"
        story_arc = dict(video_map.get("story_arc") or {})
        micro_beats = str(video_map.get("micro_beats", "")).strip()
        story_arc_text = "\n".join(
            [
                f"- Opening: {str(story_arc.get('opening', '')).strip()}",
                f"- Rising action: {str(story_arc.get('rising_action', '')).strip()}",
                f"- Climax: {str(story_arc.get('climax', '')).strip()}",
                f"- Ending: {str(story_arc.get('ending', '')).strip()}",
            ]
        )
        related_threads = "\n".join([f"- {x}" for x in list(video_map.get("related_story_threads") or []) if str(x).strip()]) or "- None"
        continuity = "\n".join([f"- {x}" for x in list(video_map.get("continuity_rules") or [])]) or "- Keep continuity stable."
        negative = ", ".join([str(x).strip() for x in list(video_map.get("negative_rules") or []) if str(x).strip()])
        main_concept = self._to_english_for_final_prompt(str(video_map.get("main_concept", "")).strip())
        story_arc_text = self._to_english_for_final_prompt(story_arc_text)
        micro_beats = self._to_english_for_final_prompt(micro_beats)
        related_threads = self._to_english_for_final_prompt(related_threads)
        scene_desc = self._to_english_for_final_prompt(scene_desc)
        continuity = self._to_english_for_final_prompt(continuity)
        visual_style_text = str(st.get("visual_style", "")).strip()
        non_realistic_style = any(
            k in f"{visual_style_text} {str(st.get('mood', '')).strip()}".lower()
            for k in ("cartoon", "anime", "3d", "pixar", "stylized", "toon", "chibi", "hoạt hình")
        )
        visual_quality_line = (
            "Keep stylized non-photorealistic rendering. Do NOT generate realistic live-action look."
            if non_realistic_style
            else "High-quality realistic details."
        )
        return f"""Create an {int(st.get('duration_sec', 8))}-second {str(st.get('aspect_ratio', '9:16')).strip()} video for Google Flow / Veo 3.

Main concept:
{main_concept}

Narrative arc (strict):
{story_arc_text}

Duration micro-beat pacing (strict):
{micro_beats or "- Keep dense but readable storytelling beats across full duration."}

Related story threads (must stay coherent with main arc):
{related_threads}

Character bible:
{char_desc}

Environment bible:
{env_desc}

Start -> End scene structure:
{scene_desc}

Character consistency:
{continuity}

Environment consistency:
{continuity}

Continuity:
The video must feel like one continuous coherent scene from opening to climax to ending. Actions should flow naturally between beats. Keep object positions, lighting direction, camera movement, character identity and environment consistent.

Camera:
{str(st.get('camera_style', '')).strip()}

Lighting:
{str(st.get('lighting', '')).strip()}

Visual style:
{visual_style_text}. {str(st.get('mood', '')).strip()} mood. {visual_quality_line}
PRIMARY VIDEO STYLE LOCK:
Use the selected video style above as the single source of truth for characters, environment, props, and scene rendering.
Do not drift to another genre/style.

Language rules:
If any visible text, subtitle, sign, or spoken line appears, it must be in {str(st.get('language', 'Vietnamese')).strip()}. Keep it short, natural and readable.

Negative rules:
{negative}
"""

    def _to_english_for_final_prompt(self, text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return raw
        out = self._localize_text_for_language(text=raw, language_label="English").strip()
        if out:
            return out
        if self._contains_vietnamese_chars(raw):
            return "Convert this section to natural English while preserving the original story meaning."
        return raw

    def _duration_micro_beats(self, duration_sec: int) -> str:
        d = int(duration_sec or 8)
        if d <= 4:
            return (
                "- 0.0-0.8s: Opening hook with clear subject and location.\n"
                "- 0.8-2.2s: Rising action with one key discovery/change.\n"
                "- 2.2-3.4s: Climax visual/emotional peak.\n"
                "- 3.4-4.0s: Ending payoff and clean final frame."
            )
        if d <= 6:
            return (
                "- 0.0-1.2s: Opening hook + context.\n"
                "- 1.2-3.2s: Rising action with progressive tension.\n"
                "- 3.2-4.8s: Climax beat with strongest emotional/visual reveal.\n"
                "- 4.8-6.0s: Ending resolution and memorable closing frame."
            )
        return (
            "- 0.0-2.0s: Opening setup with clear world + character intent.\n"
            "- 2.0-5.0s: Rising action, linked events, and tension build.\n"
            "- 5.0-6.8s: Climax reveal/turning point.\n"
            "- 6.8-8.0s: Ending resolution, afterglow, and stable final composition."
        )

    def _gemini_generate_json_object(self, prompt: str) -> dict[str, Any]:
        raw = self._gemini_generate_text(prompt)
        return self._parse_json_object(raw)

    def _gemini_generate_json_array(self, prompt: str) -> list[dict[str, Any]]:
        raw = self._gemini_generate_text(prompt)
        return self._parse_json_array(raw)

    def _gemini_generate_text(self, prompt: str) -> str:
        key = self._resolve_gemini_api_key()
        if not key:
            return ""
        try:
            from google import genai
        except Exception:
            return ""
        try:
            client = genai.Client(api_key=key)
            model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
            resp = client.models.generate_content(model=model, contents=prompt)
            return str(getattr(resp, "text", "") or "").strip()
        except Exception:
            return ""

    def _extract_json_payload(self, raw: str, *, expect: str) -> str:
        s = str(raw or "").strip()
        if not s:
            return ""
        if "```" in s:
            m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", s, flags=re.IGNORECASE)
            if m:
                s = m.group(1).strip()
        if expect == "object":
            if not s.startswith("{"):
                i = s.find("{")
                j = s.rfind("}")
                if i >= 0 and j > i:
                    s = s[i : j + 1]
        else:
            if not s.startswith("["):
                i = s.find("[")
                j = s.rfind("]")
                if i >= 0 and j > i:
                    s = s[i : j + 1]
        return s

    def _parse_json_object(self, raw: str) -> dict[str, Any]:
        s = self._extract_json_payload(raw, expect="object")
        if not s:
            return {}
        try:
            data = json.loads(s)
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    def _parse_json_array(self, raw: str) -> list[dict[str, Any]]:
        s = self._extract_json_payload(raw, expect="array")
        if not s:
            return []
        try:
            data = json.loads(s)
        except Exception:
            return []
        return [x for x in data if isinstance(x, dict)] if isinstance(data, list) else []

    def _character_lock_id(self, profile: dict[str, Any]) -> str:
        base = "|".join(
            [
                str(profile.get("character_name", "")).strip().lower(),
                str(profile.get("character_description", "")).strip().lower(),
                str(profile.get("outfit", "")).strip().lower(),
                str(profile.get("facial_features", "")).strip().lower(),
                str(profile.get("personality", "")).strip().lower(),
                "|".join(str(x).strip().lower() for x in (profile.get("supporting_characters") or [])),
            ]
        )
        digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
        return f"char-{digest}"

    def _reference_image_lines(self, profile: dict[str, Any]) -> str:
        lines: list[str] = []
        main_ref = str(profile.get("reference_image_path", "")).strip()
        if main_ref:
            lines.append(f"- Main character reference image: {main_ref}")
        for x in list(profile.get("supporting_characters") or []):
            s = str(x).strip()
            if "reference_image=" in s:
                lines.append(f"- {s}")
        if not lines:
            return "- none"
        return "\n".join(lines)

    def _reference_path_is_valid_file(self, path: str) -> bool:
        """True nếu đường dẫn ảnh map tồn tại và là file."""
        p = str(path or "").strip()
        if not p:
            return False
        try:
            return os.path.isfile(p)
        except OSError:
            return False

    def _auto_cast_all_references_resolved(self, input_data: dict[str, Any]) -> bool:
        """Cast auto: mọi nhân vật đều có file ảnh map hợp lệ."""
        if str(input_data.get("character_mode", "")).strip().lower() != "auto":
            return True
        rows = migrate_auto_character_profiles(list(input_data.get("auto_character_profiles") or []))
        if not rows:
            return True
        for row in rows:
            if not self._reference_path_is_valid_file(str(row.get("reference_image_path", "")).strip()):
                return False
        return True

    def _latest_generation_image_prompt(self, row: dict[str, Any]) -> str:
        """Lấy prompt ảnh gần nhất từ lịch sử tạo ảnh (nếu có)."""
        gens = row.get("character_image_generations")
        if not isinstance(gens, list) or not gens:
            return ""
        last = gens[-1]
        if isinstance(last, dict):
            return str(last.get("character_image_prompt", "")).strip()
        return ""

    def _text_only_full_cast_block(self, input_data: dict[str, Any], scene_plan: dict[str, Any]) -> str:
        """
        Khi cast auto thiếu ảnh map: gom mô tả từng nhân vật + neo bối cảnh + phân cảnh
        để model bám identity bằng text thay vì reference file.
        """
        if str(input_data.get("character_mode", "")).strip().lower() != "auto":
            return ""
        if self._auto_cast_all_references_resolved(input_data):
            return ""
        rows = migrate_auto_character_profiles(list(input_data.get("auto_character_profiles") or []))
        if not rows:
            return ""
        env = str(input_data.get("environment_style_prompt", "")).strip()
        char_render = str(input_data.get("character_image_style_prompt", "")).strip()
        scene_bits = [str(v).strip() for v in scene_plan.values() if str(v).strip()]
        scene_join = " | ".join(scene_bits)
        header = (
            "FULL CAST — TEXT-ONLY VISUAL LOCKS (no valid uploaded reference image for one or more characters):\n"
            "- Obey every named bullet as strict appearance; do not swap faces/outfits between names.\n"
            f"- Environment anchor for all shots: {env or 'coherent cinematic world, consistent lighting'}\n"
            f"- Scene integration: align each character's look with the Scene plan beats — {scene_join or 'use Scene plan section above verbatim'}\n"
        )
        if char_render:
            header += f"- Portrait / still rendering hint: {char_render}\n"
        lines: list[str] = [header.rstrip(), ""]
        for row in rows:
            name = str(row.get("name", "")).strip() or "Character"
            role = str(row.get("role", "")).strip()
            gender = str(row.get("gender", "")).strip()
            age = str(row.get("age", "")).strip()
            appearance = str(row.get("appearance", "")).strip()
            facial = str(row.get("facial_features", "")).strip()
            outfit = str(row.get("outfit", "")).strip()
            personality = str(row.get("personality", "")).strip()
            note = str(row.get("consistency_note", "")).strip()
            img_prompt = str(row.get("character_image_prompt", "")).strip()
            if not img_prompt:
                img_prompt = self._latest_generation_image_prompt(row)
            bits = [
                f"role={role}" if role else "",
                f"gender={gender}" if gender else "",
                f"age={age}" if age else "",
                f"appearance={appearance}" if appearance else "",
                f"facial={facial}" if facial else "",
                f"outfit={outfit}" if outfit else "",
                f"personality={personality}" if personality else "",
                f"consistency={note}" if note else "",
                f"image_generation_prompt={img_prompt}" if img_prompt else "",
            ]
            desc = "; ".join(b for b in bits if b)
            lines.append(f"- {name}: {desc or 'keep identity stable across all shots'}")
        return "\n".join(lines).strip() + "\n"

    def _localize_text_for_language(self, *, text: str, language_label: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        target = self._language_to_iso639_1(language_label)
        if not target:
            return raw
        # Nếu text đã gần như cùng ngôn ngữ mục tiêu (basic heuristic), giữ nguyên.
        if target == "en" and raw.isascii():
            return raw
        translated = self._translate_text_via_gemini(text=raw, language_label=language_label)
        if not translated:
            translated = self._translate_text_public_api(text=raw, target_lang=target)
        if translated:
            return translated
        # Khi ép prompt tiếng Anh mà dịch thất bại, tránh để lọt tiếng Việt vào final prompt.
        if target == "en" and self._contains_vietnamese_chars(raw):
            return "Translate source idea/topic to natural English and keep original meaning."
        return raw

    def _language_to_iso639_1(self, language_label: str) -> str:
        m: dict[str, str] = {
            "Vietnamese": "vi",
            "English": "en",
            "Indonesian": "id",
            "Thai": "th",
            "Spanish": "es",
            "Portuguese": "pt",
            "French": "fr",
            "German": "de",
            "Italian": "it",
            "Japanese": "ja",
            "Korean": "ko",
            "Simplified Chinese": "zh-CN",
            "Traditional Chinese": "zh-TW",
            "Hindi": "hi",
        }
        return m.get(str(language_label or "").strip(), "")

    def _translate_text_public_api(self, *, text: str, target_lang: str) -> str:
        """
        Best-effort translate qua endpoint public, không hard-fail nếu lỗi mạng.
        """
        try:
            q = quote(text)
            url = (
                "https://translate.googleapis.com/translate_a/single"
                f"?client=gtx&sl=auto&tl={target_lang}&dt=t&q={q}"
            )
            resp = requests.get(url, timeout=8)
            if not resp.ok:
                return ""
            raw = resp.json()
            if not isinstance(raw, list) or not raw:
                return ""
            segs = raw[0]
            if not isinstance(segs, list):
                return ""
            out: list[str] = []
            for s in segs:
                if not isinstance(s, list) or not s:
                    continue
                out.append(str(s[0] or ""))
            return "".join(out).strip()
        except Exception:
            return ""

    def _translate_text_via_gemini(self, *, text: str, language_label: str) -> str:
        key = os.environ.get("GEMINI_API_KEY", "").strip()
        if not key:
            return ""
        try:
            from google import genai
        except Exception:
            return ""
        try:
            client = genai.Client(api_key=key)
            model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
            prompt = (
                f"Translate the following text into {language_label}. "
                "Return only translated text, no explanation.\n\n"
                f"{text}"
            )
            resp = client.models.generate_content(model=model, contents=prompt)
            out = str(getattr(resp, "text", "") or "").strip()
            return out
        except Exception:
            return ""

    def gemini_api_key_configured(self) -> bool:
        """True nếu có thể gọi Gemini (env hoặc key đã lưu)."""
        return bool(self._resolve_gemini_api_key())

    def _resolve_gemini_api_key(self) -> str:
        """Lấy API key từ biến môi trường hoặc kho key đã lưu trong app."""
        k = os.environ.get("GEMINI_API_KEY", "").strip()
        if k:
            return k
        try:
            from src.utils.app_secrets import get_active_gemini_api_key

            return (get_active_gemini_api_key() or "").strip()
        except Exception:
            return ""

    def _parse_gemini_json_character_array(self, raw: str) -> list[dict[str, Any]]:
        """Trích mảng JSON nhân vật từ nội dung model (có thể bọc markdown)."""
        s = str(raw or "").strip()
        if not s:
            return []
        if "```" in s:
            m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", s, flags=re.IGNORECASE)
            if m:
                s = m.group(1).strip()
        if not s.startswith("["):
            i = s.find("[")
            j = s.rfind("]")
            if i >= 0 and j > i:
                s = s[i : j + 1]
        try:
            data = json.loads(s)
        except Exception:
            return []
        return data if isinstance(data, list) else []

    def infer_characters_from_script_via_gemini(
        self,
        *,
        script: str,
        topic: str = "",
        language_display: str = "Tiếng Việt",
        style_hint: str = "",
        max_characters: int = 12,
    ) -> tuple[list[dict[str, Any]], str]:
        """
        Phân tích toàn bộ kịch bản/ý tưởng bằng Gemini (hiểu ngữ cảnh), trả về danh sách nhân vật chuẩn hóa.

        Không dùng quy tắc tách từ khóa cục bộ; model đọc nội dung và liệt kê cast cần đồng nhất hình ảnh.

        Returns:
            (danh_sách_dict, thông_báo_lỗi) — lỗi rỗng khi thành công.
        """
        key = self._resolve_gemini_api_key()
        if not key:
            return [], "Chưa có Gemini API key (GEMINI_API_KEY hoặc key trong tab AI)."

        try:
            from google import genai
        except Exception as exc:  # noqa: BLE001
            return [], f"Thiếu gói google-genai: {exc}"

        lang_label = SUPPORTED_LANGUAGES.get(str(language_display or "").strip(), str(language_display or "Vietnamese").strip())
        parts: list[str] = []
        sc = str(script or "").strip()
        tp = str(topic or "").strip()
        if sc:
            parts.append(sc)
        if tp:
            parts.append(f"Chủ đề / bối cảnh thêm: {tp}")
        combined = "\n\n".join(parts).strip()
        if not combined:
            return [], "Nội dung kịch bản rỗng."
        if len(combined) > 14000:
            combined = combined[:14000] + "\n\n[... đoạn sau đã cắt bớt do giới hạn độ dài API ...]"

        style_block = ""
        if str(style_hint or "").strip():
            style_block = f"\nGợi ý phong cách hình ảnh (áp dụng cho mô tả ngoại hình, không thay đổi vai trừ khi kịch bản yêu cầu):\n{str(style_hint).strip()[:600]}\n"

        schema = (
            '[{"name":"...","role":"main_character|support|child|pet|other",'
            '"gender":"male|female|unspecified",'
            '"age":"...","appearance":"...","facial_features":"...","outfit":"...",'
            '"personality":"...","consistency_note":"..."}]'
        )

        prompt = f"""Bạn là chuyên gia phân tích kịch bản cho sản xuất video AI.

Nhiệm vụ: ĐỌC TOÀN BỘ văn bản kịch bản / ý tưởng bên dưới (hiểu ngữ cảnh, không phân tích theo từ khóa rời rạc).
Liệt kê mọi nhân vật hoặc thực thể đóng vai trò cast cần giữ đồng nhất hình ảnh (người, thú có vai trò, v.v.) theo đúng kịch bản.

Quy tắc:
- Ngôn ngữ tên và mô tả: {lang_label} (tên riêng có thể giữ như trong kịch bản).
- Tối đa {max_characters} mục; ưu tiên vai quan trọng lặp lại trên màn hình.
- Nhân vật chính (nếu có) đặt role là main_character; các vai khác support / child / pet / other cho phù hợp.
- Mỗi mục phải đủ trường: name, role, gender, age, appearance, facial_features, outfit, personality, consistency_note.
- gender chỉ dùng một trong: male, female, unspecified.
{style_block}
Chỉ trả về một mảng JSON hợp lệ (không giải thích, không markdown), đúng cấu trúc ví dụ:
{schema}

--- KỊCH BẢN / Ý TƯỞNG ---
{combined}
"""

        try:
            client = genai.Client(api_key=key)
            model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
            resp = client.models.generate_content(model=model, contents=prompt)
            raw_out = str(getattr(resp, "text", "") or "").strip()
        except Exception as exc:  # noqa: BLE001
            return [], f"Gemini lỗi: {exc}"

        rows = self._parse_gemini_json_character_array(raw_out)
        if not rows:
            return [], "Gemini không trả về JSON mảng nhân vật hợp lệ."

        out: list[dict[str, Any]] = []
        allowed_g = {"male", "female", "unspecified"}
        for item in rows[:max_characters]:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if not name:
                continue
            g = str(item.get("gender", "unspecified")).strip().lower()
            if g not in allowed_g:
                g = "unspecified"
            out.append(
                {
                    "name": name,
                    "role": str(item.get("role", "support") or "support").strip() or "support",
                    "gender": g,
                    "age": str(item.get("age", "") or "").strip() or "—",
                    "appearance": str(item.get("appearance", "") or "").strip() or "consistent appearance",
                    "facial_features": str(item.get("facial_features", "") or "").strip() or "stable facial identity",
                    "outfit": str(item.get("outfit", "") or "").strip() or "consistent outfit",
                    "personality": str(item.get("personality", "") or "").strip() or "natural personality",
                    "consistency_note": str(item.get("consistency_note", "") or "").strip()
                    or "keep same visual identity across shots",
                    "reference_image_path": str(item.get("reference_image_path", "") or "").strip(),
                }
            )

        if not out:
            return [], "Không trích được nhân vật nào từ JSON Gemini."
        return migrate_auto_character_profiles(out), ""

    def _contains_vietnamese_chars(self, text: str) -> bool:
        s = str(text or "")
        vi_chars = "ăâđêôơưàáảãạằắẳẵặầấẩẫậèéẻẽẹềếểễệìíỉĩịòóỏõọồốổỗộờớởỡợùúủũụừứửữựỳýỷỹỵ"
        lower = s.lower()
        return any(ch in lower for ch in vi_chars)

    def _cultural_localization_rules(self, language_label: str) -> str:
        lang = str(language_label or "").strip()
        rules: dict[str, str] = {
            "Vietnamese": (
                "Represent people, styling, social behaviors, and environment cues consistent with modern Vietnamese culture. "
                "Dialogue style should feel natural to native Vietnamese speakers."
            ),
            "English": (
                "Use globally understandable contemporary English-speaking cultural cues and natural conversational style."
            ),
            "Indonesian": (
                "Reflect Indonesian daily-life context, social expressions, and visual cues naturally aligned with Indonesian culture."
            ),
            "Thai": (
                "Reflect Thai cultural context, social etiquette, and visual environment cues suitable for Thai audiences."
            ),
            "Spanish": (
                "Use Spanish-speaking cultural tone and context naturally; keep expressions idiomatic for native Spanish audiences."
            ),
            "Portuguese": (
                "Use Portuguese-speaking cultural tone and context naturally; keep expressions idiomatic and region-neutral when possible."
            ),
            "French": (
                "Use French-speaking cultural tone and context naturally with authentic social and visual cues."
            ),
            "German": (
                "Use German-speaking cultural tone and context naturally with realistic social behavior and visual cues."
            ),
            "Italian": (
                "Use Italian-speaking cultural tone and context naturally with authentic social expressions."
            ),
            "Japanese": (
                "Use Japanese cultural context and communication style naturally, including appropriate social tone and visual cues."
            ),
            "Korean": (
                "Use Korean cultural context and communication style naturally, with coherent local social and visual cues."
            ),
            "Simplified Chinese": (
                "Use Mainland Chinese contemporary cultural context and simplified Chinese language usage naturally."
            ),
            "Traditional Chinese": (
                "Use Traditional Chinese language usage and culturally coherent context for Traditional Chinese audiences."
            ),
            "Hindi": (
                "Use Hindi-speaking cultural context and expressions naturally, aligned with contemporary Indian social cues."
            ),
        }
        return rules.get(
            lang,
            "Localize people, environment, social cues, and dialogue style to match the selected language audience naturally.",
        )
