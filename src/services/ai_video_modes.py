from __future__ import annotations

from typing import Any


def default_video_mode_registry() -> dict[str, dict[str, Any]]:
    return {
        "text_to_video": {
            "enabled": True,
            "label": "Text to Video",
            "requires": ["prompt"],
            "optional": ["aspect_ratio", "duration_sec", "output_count", "resolution", "seed"],
        },
        "image_to_video": {
            "enabled": True,
            "label": "Image to Video",
            "requires": ["prompt", "image_path"],
            "optional": ["aspect_ratio", "duration_sec", "resolution"],
        },
        "first_last_frame_to_video": {
            "enabled": True,
            "label": "First & Last Frame",
            "requires": ["prompt", "first_frame_path", "last_frame_path"],
            "optional": ["aspect_ratio", "duration_sec", "resolution"],
        },
        "ingredients_to_video": {
            "enabled": True,
            "label": "Ingredients to Video",
            "requires": ["prompt", "reference_images"],
            "optional": ["reference_type", "aspect_ratio", "duration_sec", "resolution"],
        },
        "extend_video": {
            "enabled": True,
            "label": "Extend Video",
            "requires": ["source_video_path", "prompt"],
            "optional": ["extend_duration_sec", "aspect_ratio", "resolution"],
        },
        "prompt_to_vertical_video": {
            "enabled": True,
            "label": "Prompt to Vertical (9:16)",
            "requires": ["prompt"],
            "optional": ["duration_sec", "output_count", "resolution", "seed"],
            "alias_for": "text_to_video",
            "fixed_options": {"aspect_ratio": "9:16"},
        },
        "image_to_vertical_video": {
            "enabled": True,
            "label": "Image to Vertical (9:16)",
            "requires": ["prompt", "image_path"],
            "optional": ["duration_sec", "resolution"],
            "alias_for": "image_to_video",
            "fixed_options": {"aspect_ratio": "9:16"},
        },
        "insert_object": {
            "enabled": False,
            "label": "Insert Object (Experimental)",
            "requires": ["source_video_path", "prompt"],
            "optional": [],
            "experimental": True,
        },
        "remove_object": {
            "enabled": False,
            "label": "Remove Object (Experimental)",
            "requires": ["source_video_path", "prompt"],
            "optional": [],
            "experimental": True,
        },
        "video_upscale": {
            "enabled": False,
            "label": "Video Upscale (Experimental)",
            "requires": ["source_video_path"],
            "optional": ["resolution"],
            "experimental": True,
        },
        "video_variation": {
            "enabled": False,
            "label": "Video Variation (Experimental)",
            "requires": ["source_video_path", "prompt"],
            "optional": [],
            "experimental": True,
        },
    }

