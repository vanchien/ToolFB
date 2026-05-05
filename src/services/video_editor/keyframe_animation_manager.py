"""Preset animation đơn giản cho overlay — fade trên layer logo."""

from __future__ import annotations

from typing import Any


class KeyframeAnimationManager:
    PRESETS = (
        "none",
        "fade_in",
        "fade_out",
        "slide_from_left",
        "slide_from_right",
        "zoom_in",
        "zoom_out",
    )

    def add_animation_preset(self, project: dict[str, Any], clip_id: str, preset: str) -> dict[str, Any]:
        pr = str(preset or "none").lower()
        if pr not in self.PRESETS:
            pr = "none"
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict):
                continue
            for cl in tr.get("clips") or []:
                if isinstance(cl, dict) and str(cl.get("id")) == str(clip_id):
                    cl["animation_preset"] = pr
                    return project
        raise ValueError("Không tìm thấy clip")

    def build_overlay_expression(self, clip: dict[str, Any]) -> dict[str, Any]:
        """
        Trả về extra_vf trên chuỗi [ii:v]scale=... (fade alpha).
        slide/zoom: Phase 2+ nâng cao — hiện dùng none/fade.
        """
        du = float(clip.get("duration") or 1.0)
        ts = float(clip.get("timeline_start") or 0)
        te = ts + du
        pr = str(clip.get("animation_preset") or "none").lower()
        out: dict[str, Any] = {"use_expr": False, "x": int(clip.get("x") or 0), "y": int(clip.get("y") or 0), "enable": f"between(t,{ts},{te})"}

        fd = min(0.8, max(0.1, du * 0.35))
        if pr == "fade_in":
            out["extra_vf"] = f"fade=t=in:st=0:d={fd}:alpha=1"
        elif pr == "fade_out":
            out["extra_vf"] = f"fade=t=out:st={max(0.0, du - fd)}:d={fd}:alpha=1"
        elif pr in ("slide_from_left", "slide_from_right", "zoom_in", "zoom_out"):
            # Giữ chỗ — fallback fade nhẹ
            out["extra_vf"] = f"fade=t=in:st=0:d={fd * 0.5}:alpha=1"
        return out
