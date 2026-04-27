from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.services.google_flow_video_store import ensure_google_flow_layout
from src.services.ai_styles_registry import default_style_id, style_prompt_addon

SUPPORTED_ASPECTS: set[str] = {"9:16", "16:9", "1:1"}
SUPPORTED_DURATIONS: set[int] = {4, 6, 8}
SUPPORTED_RESOLUTIONS: set[str] = {"720p", "1080p"}


def normalize_flow_video_input(raw_input: dict[str, Any]) -> dict[str, Any]:
    """Normalize and validate input for Google Flow text-to-video prompt."""
    idea = str(raw_input.get("idea", "")).strip()
    final_prompt = str(raw_input.get("final_prompt", "")).strip()
    if not idea and not final_prompt:
        raise ValueError("Thiếu idea hoặc final_prompt.")
    settings = dict(raw_input.get("settings") or {})
    default_video_style = style_prompt_addon(
        "video_styles",
        default_style_id("video_style_id", "cinematic_story"),
        fallback="cinematic realistic video, smooth camera movement, natural motion, high-quality details, coherent visual continuity",
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
    default_env = style_prompt_addon(
        "environment_styles",
        default_style_id("environment_style_id", "environment_cinematic"),
        fallback="cinematic environment, atmospheric lighting, rich details, movie still composition",
    )
    # Job chỉ có final_prompt: dùng mặc định từ settings / trường tùy chọn.
    if not idea:
        language = str(raw_input.get("language", "") or settings.get("language", "Vietnamese")).strip() or "Vietnamese"
        visual_style = (
            str(raw_input.get("visual_style", "") or settings.get("visual_style", default_video_style)).strip()
            or default_video_style
        )
        aspect = str(settings.get("aspect_ratio", "9:16")).strip() or "9:16"
        duration = int(settings.get("duration_sec") or 8)
        resolution = str(settings.get("resolution", "720p")).strip() or "720p"
        if aspect not in SUPPORTED_ASPECTS:
            raise ValueError(f"Aspect ratio không hợp lệ: {aspect}")
        if duration not in SUPPORTED_DURATIONS:
            raise ValueError(f"Duration không hợp lệ: {duration}")
        if resolution not in SUPPORTED_RESOLUTIONS:
            raise ValueError(f"Resolution không hợp lệ: {resolution}")
        return {
            "idea": "",
            "topic": str(raw_input.get("topic", "")).strip(),
            "goal": str(raw_input.get("goal", "") or "storytelling").strip(),
            "language": language,
            "visual_style": visual_style,
            "character_mode": str(raw_input.get("character_mode", "auto")).strip() or "auto",
            "character_profile_id": str(raw_input.get("character_profile_id", "")).strip(),
            "settings": {
                "aspect_ratio": aspect,
                "duration_sec": duration,
                "resolution": resolution,
                "camera_style": str(settings.get("camera_style", default_camera)).strip(),
                "lighting": str(settings.get("lighting", default_lighting)).strip(),
                "motion_style": str(settings.get("motion_style", default_motion)).strip(),
                "mood": str(settings.get("mood", "cinematic and coherent")).strip(),
                "environment_style_prompt": str(settings.get("environment_style_prompt", default_env)).strip(),
            },
        }
    language = str(raw_input.get("language", "")).strip()
    if not language:
        raise ValueError("Thiếu ngôn ngữ video.")
    aspect = str(settings.get("aspect_ratio", "9:16")).strip() or "9:16"
    duration = int(settings.get("duration_sec") or 8)
    resolution = str(settings.get("resolution", "720p")).strip() or "720p"
    visual_style = str(raw_input.get("visual_style", "") or settings.get("visual_style", "")).strip()
    if not visual_style:
        raise ValueError("Thiếu visual_style.")
    if aspect not in SUPPORTED_ASPECTS:
        raise ValueError(f"Aspect ratio không hợp lệ: {aspect}")
    if duration not in SUPPORTED_DURATIONS:
        raise ValueError(f"Duration không hợp lệ: {duration}")
    if resolution not in SUPPORTED_RESOLUTIONS:
        raise ValueError(f"Resolution không hợp lệ: {resolution}")
    return {
        "idea": idea,
        "topic": str(raw_input.get("topic", "")).strip(),
        "goal": str(raw_input.get("goal", "")).strip() or "storytelling",
        "language": language,
        "visual_style": visual_style,
        "character_mode": str(raw_input.get("character_mode", "auto")).strip() or "auto",
        "character_profile_id": str(raw_input.get("character_profile_id", "")).strip(),
        "settings": {
            "aspect_ratio": aspect,
            "duration_sec": duration,
            "resolution": resolution,
            "camera_style": str(settings.get("camera_style", default_camera)).strip(),
            "lighting": str(settings.get("lighting", default_lighting)).strip(),
            "motion_style": str(settings.get("motion_style", default_motion)).strip(),
            "mood": str(settings.get("mood", "cinematic and coherent")).strip(),
            "environment_style_prompt": str(settings.get("environment_style_prompt", default_env)).strip(),
        },
    }


def build_or_load_character_profile(input_data: dict[str, Any], raw_input: dict[str, Any]) -> dict[str, Any]:
    """Build manual/auto profile or load existing profile from disk."""
    mode = str(input_data.get("character_mode", "auto")).strip().lower()
    existing_id = str(input_data.get("character_profile_id", "")).strip()
    if existing_id:
        loaded = _load_character_profile(existing_id)
        if loaded:
            return loaded
    if mode == "manual":
        manual = dict(raw_input.get("character_profile") or {})
        return {
            "name": str(manual.get("name", "Main Character")).strip() or "Main Character",
            "age": str(manual.get("age", "25")).strip() or "25",
            "gender": str(manual.get("gender", "female")).strip() or "female",
            "appearance": str(manual.get("appearance", "natural realistic appearance")).strip() or "natural realistic appearance",
            "outfit": str(manual.get("outfit", "consistent neutral outfit")).strip() or "consistent neutral outfit",
            "facial_features": str(manual.get("facial_features", "stable face geometry and skin texture")).strip(),
            "personality": str(manual.get("personality", "calm and confident")).strip() or "calm and confident",
            "consistency_note": str(manual.get("consistency_note", "Keep same identity for all shots")).strip(),
            "consistency_rules": [
                "Keep the same face throughout the entire video.",
                "Keep the same hairstyle and outfit throughout the entire video.",
                "Do not change age, body shape, ethnicity or clothing colors.",
            ],
        }
    return {
        "name": "Linh",
        "age": "25",
        "gender": "female",
        "appearance": "Vietnamese woman with long black hair and warm smile",
        "outfit": "white linen shirt and beige trousers",
        "facial_features": "oval face, expressive eyes, natural makeup",
        "personality": "confident, calm, friendly",
        "consistency_note": "Keep the same face, hairstyle, outfit and body proportions throughout the video.",
        "consistency_rules": [
            "Keep the same face throughout the entire video.",
            "Keep the same hairstyle and outfit throughout the entire video.",
            "Do not change age, body shape, ethnicity or clothing colors.",
        ],
    }


def build_start_end_scene_plan(input_data: dict[str, Any], character_profile: dict[str, Any]) -> dict[str, str]:
    """Build Start/Middle/End scene plan for short vertical video."""
    _ = character_profile
    idea = str(input_data.get("idea", "")).strip()
    topic = str(input_data.get("topic", "")).strip()
    core = f"{idea}. Topic: {topic}" if topic else idea
    return {
        "start": f"0-2s: Establish main subject and context clearly. {core}",
        "middle": "2-5s: Show the key action with stable camera and coherent movement.",
        "end": "5-8s: Deliver payoff with clear subject framing and emotional closure.",
    }


def build_google_flow_text_to_video_prompt(
    input_data: dict[str, Any],
    character_profile: dict[str, Any],
    scene_plan: dict[str, str],
) -> str:
    """Build final English prompt for Google Flow / Veo browser."""
    settings = dict(input_data.get("settings") or {})
    return f"""Create an {settings.get('duration_sec', 8)}-second {settings.get('aspect_ratio', '9:16')} video for Google Flow / Veo 3.

Main concept:
{input_data.get('idea', '')}

Start -> End structure:
Start, 0-2 seconds:
{scene_plan.get('start', '')}

Middle, 2-5 seconds:
{scene_plan.get('middle', '')}

End, 5-8 seconds:
{scene_plan.get('end', '')}

Main character:
{character_profile.get('name', 'Main Character')}, a {character_profile.get('age', '25')}-year-old {character_profile.get('gender', 'female')}. {character_profile.get('appearance', '')}
Facial features: {character_profile.get('facial_features', '')}
Outfit: {character_profile.get('outfit', '')}
Personality: {character_profile.get('personality', '')}

Character consistency:
Keep the same character identity from start to end. Do not change the face, hairstyle, outfit, age, body shape, ethnicity, or clothing colors.

Action and continuity:
The video must feel like one continuous coherent shot from start to end. Maintain environment, object positions, lighting direction, camera movement and character appearance.

Camera:
{settings.get('camera_style', '')}. Smooth continuous movement. Avoid abrupt cuts.

Lighting:
{settings.get('lighting', '')}

Motion:
{settings.get('motion_style', '')}

Visual style:
{input_data.get('visual_style', '')}. High-quality realistic details.
Mood:
{settings.get('mood', '')}
Environment style:
{str(input_data.get('environment_style_prompt', '') or settings.get('environment_style_prompt', '')).strip()}

Language rules:
If any visible text, subtitle, sign, or spoken line appears, it must be in {input_data.get('language', 'Vietnamese')}. Keep text short, natural and readable.

Negative rules:
No changing faces, no inconsistent clothing, no distorted hands, no extra fingers, no duplicated character, no random logos, no watermark, no unreadable text, no flickering, no sudden scene jumps, no low-quality artifacts.
""".strip()


def save_character_profile(profile_id: str, profile: dict[str, Any]) -> Path:
    """Save character profile to module folder and return file path."""
    paths = ensure_google_flow_layout()
    p = paths["character_profiles"] / f"{profile_id}.json"
    p.write_text(json.dumps(profile, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return p


def _load_character_profile(profile_id: str) -> dict[str, Any] | None:
    """Load character profile by id when exists."""
    paths = ensure_google_flow_layout()
    p = paths["character_profiles"] / f"{profile_id}.json"
    if not p.is_file():
        return None
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    return dict(raw) if isinstance(raw, dict) else None
