"""Chuỗi filter FFmpeg: lật, xoay, crop, scale (trước bước canvas)."""

from __future__ import annotations

from typing import Any


def ensure_video_transform_defaults(clip: dict[str, Any], project: dict[str, Any]) -> None:
    """Đảm bảo các key transform tồn tại trên clip video (không ghi đè giá trị đã có)."""
    pw = int(project.get("width") or 1080)
    ph = int(project.get("height") or 1920)
    if "flip_horizontal" not in clip:
        clip["flip_horizontal"] = False
    if "flip_vertical" not in clip:
        clip["flip_vertical"] = False
    if "rotation" not in clip:
        clip["rotation"] = 0
    cr = clip.get("crop")
    if not isinstance(cr, dict):
        clip["crop"] = {"enabled": False, "x": 0, "y": 0, "width": pw, "height": ph}
    else:
        cr.setdefault("enabled", False)
        cr.setdefault("x", 0)
        cr.setdefault("y", 0)
        cr.setdefault("width", pw)
        cr.setdefault("height", ph)
    sc = clip.get("scale")
    if not isinstance(sc, dict):
        clip["scale"] = {"enabled": False, "width": pw, "height": ph, "keep_aspect": True}
    else:
        sc.setdefault("enabled", False)
        sc.setdefault("width", pw)
        sc.setdefault("height", ph)
        sc.setdefault("keep_aspect", True)
    if "canvas_mode" not in clip:
        clip["canvas_mode"] = "fit"
    bb = clip.get("blur_background")
    if not isinstance(bb, dict):
        clip["blur_background"] = {"enabled": False, "blur": 20}
    else:
        bb.setdefault("enabled", False)
        bb.setdefault("blur", 20)
    if "muted" not in clip:
        clip["muted"] = False


class VideoTransformFilterBuilder:
    def build_transform_filters(self, clip: dict[str, Any], project: dict[str, Any]) -> str:
        ensure_video_transform_defaults(clip, project)
        parts: list[str] = []
        if clip.get("flip_horizontal"):
            parts.append("hflip")
        if clip.get("flip_vertical"):
            parts.append("vflip")
        rot = int(clip.get("rotation") or 0) % 360
        if rot == 90:
            parts.append("transpose=1")
        elif rot == 180:
            parts.extend(["transpose=1", "transpose=1"])
        elif rot == 270:
            parts.append("transpose=2")
        cr = clip.get("crop") or {}
        if cr.get("enabled"):
            cw = max(2, int(cr.get("width") or 2))
            ch = max(2, int(cr.get("height") or 2))
            cx = max(0, int(cr.get("x") or 0))
            cy = max(0, int(cr.get("y") or 0))
            parts.append(f"crop={cw}:{ch}:{cx}:{cy}")
        sc = clip.get("scale") or {}
        if sc.get("enabled"):
            sw = max(2, int(sc.get("width") or 2))
            sh = max(2, int(sc.get("height") or 2))
            if sc.get("keep_aspect", True):
                parts.append(f"scale={sw}:{sh}:force_original_aspect_ratio=decrease")
            else:
                parts.append(f"scale={sw}:{sh}")
        return ",".join(parts)
