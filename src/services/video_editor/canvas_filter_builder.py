"""Fit / Fill / Stretch — đưa clip về kích thước canvas project; zoom tĩnh sau bước vào khung."""

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

    def build_canvas_zoom_vf(self, clip: dict[str, Any], w: int, h: int) -> str:
        """
        Sau khi clip đã được đưa về khung dự án (w×h): phóng to / thu nhỏ nhìn trong khung.

        - ``zoom`` = 1: không thêm filter.
        - ``zoom`` > 1: phóng to (scale rồi crop giữa — cắt viền).
        - ``zoom`` < 1: thu nhỏ (scale rồi pad đen quanh).
        """
        try:
            z = float(clip.get("zoom") or 1.0)
        except (TypeError, ValueError):
            z = 1.0
        z = max(0.1, min(8.0, z))
        if abs(z - 1.0) < 1e-5:
            return ""
        wi = max(2, int(w))
        hi = max(2, int(h))
        if z > 1.0:
            sw = max(wi + 2, int(round(wi * z)))
            sh = max(hi + 2, int(round(hi * z)))
            return (
                f"scale={sw}:{sh}:force_original_aspect_ratio=increase,"
                f"crop={wi}:{hi}:(iw-{wi})/2:(ih-{hi})/2"
            )
        sw = max(2, int(round(wi * z)))
        sh = max(2, int(round(hi * z)))
        return (
            f"scale={sw}:{sh}:force_original_aspect_ratio=decrease,"
            f"pad={wi}:{hi}:(ow-iw)/2:(oh-ih)/2:black"
        )
