"""Fit / Fill / Stretch / blur nền — đưa clip về kích thước canvas project."""

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

    def build_blur_background_chain(
        self,
        pre_label: str,
        out_label: str,
        w: int,
        h: int,
        blur_amount: int,
        *,
        seg_index: int = 0,
    ) -> list[str]:
        """
        [pre] → split → nền blur + foreground fit → overlay giữa.
        """
        a, b = f"bs{seg_index}a", f"bs{seg_index}b"
        bg, fg = f"bbg{seg_index}", f"bfg{seg_index}"
        b = max(1, min(int(blur_amount), 50))
        return [
            f"[{pre_label}]split=2[{a}][{b}]",
            f"[{a}]scale={w}:{h},boxblur={b}[{bg}]",
            f"[{b}]scale={w}:{h}:force_original_aspect_ratio=decrease[{fg}]",
            f"[{bg}][{fg}]overlay=(W-w)/2:(H-h)/2[{out_label}]",
        ]
