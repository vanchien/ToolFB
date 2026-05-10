"""Fit / Fill / Stretch — đưa clip về kích thước canvas project."""

from __future__ import annotations

from typing import Any


class CanvasFilterBuilder:
    def build_simple_canvas_vf(self, clip: dict[str, Any], w: int, h: int) -> str:
        """
        Một chuỗi filter (không có label đầu/cuối) áp sau stream đã transform.
        fit: vừa khung + pad; fill: phủ + crop giữa; stretch: kéo đủ khung.
        """
        mode = str(clip.get("canvas_mode") or "fit").lower().strip()
        if mode == "fill":
            return f"scale={w}:{h}:force_original_aspect_ratio=increase,crop={w}:{h}"
        if mode == "stretch":
            return f"scale={w}:{h}"
        # fit (default)
        return (
            f"scale={w}:{h}:force_original_aspect_ratio=decrease,"
            f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2:black"
        )
