"""Filter màu preset → chuỗi FFmpeg eq/hue/curves."""

from __future__ import annotations

from typing import Any


class VideoFilterManager:
    PRESETS: dict[str, str] = {
        "normal": "",
        "warm": "eq=contrast=1.02:saturation=1.08:gamma=1.05",
        "cool": "eq=contrast=1.02:saturation=1.08:gamma=0.98",
        "cinematic": "eq=contrast=1.08:saturation=0.92:brightness=0.02,gamma=1.1",
        "black_white": "hue=s=0",
        "high_contrast": "eq=contrast=1.25:saturation=1.05",
        "vintage": "eq=contrast=1.05:saturation=0.75:gamma=1.08",
    }

    #: Hiệu ứng ánh sáng (lưu trong ``clip["light_effect"]``) — chuỗi vf nối sau preset filter + độ sáng clip.
    LIGHT_EFFECT_PRESETS: dict[str, str] = {
        "none": "",
        "golden_hour": "eq=saturation=1.1:gamma=1.06:brightness=0.02",
        "soft_warm": "eq=gamma=1.05:saturation=1.04:brightness=0.015",
        "soft_cool": "eq=gamma=0.98:saturation=1.03:brightness=0.01",
        "high_key": "eq=brightness=0.06:contrast=0.94",
        "low_key": "eq=brightness=-0.05:contrast=1.14:gamma=1.05",
        "haze_soft": "eq=contrast=0.96:saturation=0.93:gamma=1.03",
    }

    #: Nhãn hiển thị combobox (tiếng Việt); khóa bên trái là giá trị lưu trong ``clip["light_effect"]``.
    LIGHT_EFFECT_LABELS_VI: dict[str, str] = {
        "none": "Không hiệu ứng",
        "golden_hour": "Ánh hoàng hôn (ấm vàng)",
        "soft_warm": "Ánh ấm nhẹ",
        "soft_cool": "Ánh mát nhẹ",
        "high_key": "Tông sáng cao (ít bóng đổ)",
        "low_key": "Tông tối (tương phản cao)",
        "haze_soft": "Sương mờ nhẹ (haze)",
    }

    @classmethod
    def light_effect_ordered_keys(cls) -> list[str]:
        return [k for k in cls.LIGHT_EFFECT_PRESETS]

    @classmethod
    def light_effect_batch_combo_display_values(cls) -> list[str]:
        """Hàng loạt: dòng đầu trống = giữ nguyên từng clip; các dòng sau là chú thích tiếng Việt."""
        return [""] + [cls.LIGHT_EFFECT_LABELS_VI[k] for k in cls.light_effect_ordered_keys()]

    @classmethod
    def light_effect_single_combo_display_values(cls) -> list[str]:
        """Một clip: chỉ nhãn tiếng Việt (không có «giữ nguyên»)."""
        return [cls.LIGHT_EFFECT_LABELS_VI[k] for k in cls.light_effect_ordered_keys()]

    @classmethod
    def light_effect_normalize_to_label_ui(cls, stored: str) -> str:
        """Nạp UI từ project/draft: chấp nhận khóa tiếng Anh hoặc nhãn tiếng Việt → nhãn combobox."""
        s = str(stored or "").strip()
        if not s:
            return ""
        k = cls.light_effect_label_ui_to_key(s)
        return cls.LIGHT_EFFECT_LABELS_VI.get(k, cls.LIGHT_EFFECT_LABELS_VI["none"])

    @classmethod
    def light_effect_label_ui_to_key(cls, ui: str) -> str:
        """Từ nhãn combobox (hoặc khóa tiếng Anh cũ) → khóa lưu file; ``\"\"`` = không đổi (hàng loạt)."""
        s = str(ui or "").strip()
        if not s:
            return ""
        low = s.lower()
        if low in cls.LIGHT_EFFECT_PRESETS:
            return low
        for k, lab in cls.LIGHT_EFFECT_LABELS_VI.items():
            if lab == s:
                return k
        return "none"

    @classmethod
    def light_effect_combo_values(cls) -> list[str]:
        """Tương thích cũ: danh sách khóa tiếng Anh (batch dùng ``light_effect_batch_combo_display_values``)."""
        return ["", *list(cls.LIGHT_EFFECT_PRESETS.keys())]

    def build_clip_color_adjust_vf(self, clip: dict[str, Any], project: dict[str, Any]) -> str:
        """
        Gộp filter màu theo clip (``project["filters"]``) + độ sáng clip + hiệu ứng ánh sáng.
        Độ sáng: ``clip["brightness"]`` số thực khoảng -1…1 (0 = gốc).
        """
        cid = str(clip.get("id") or "")
        base = ""
        for f in project.get("filters") or []:
            if isinstance(f, dict) and str(f.get("clip_id")) == cid:
                base = self.build_ffmpeg_filter(f).strip()
                break
        parts: list[str] = []
        if base:
            parts.append(base)
        br_raw = clip.get("brightness")
        try:
            bfv = float(br_raw) if br_raw is not None and str(br_raw).strip() != "" else 0.0
        except (TypeError, ValueError):
            bfv = 0.0
        bfv = max(-1.0, min(1.0, bfv))
        if abs(bfv) > 1e-6:
            parts.append(f"eq=brightness={bfv:.4f}")
        le = str(clip.get("light_effect") or "none").strip().lower()
        fx = (self.LIGHT_EFFECT_PRESETS.get(le) or "").strip()
        if fx:
            parts.append(fx)
        return ",".join(p for p in parts if p)

    def build_ffmpeg_filter(self, filter_config: dict[str, Any]) -> str:
        """Trả về chuỗi vf (rỗng nếu normal)."""
        t = str(filter_config.get("type") or "normal").lower()
        base = self.PRESETS.get(t, "")
        extra: list[str] = []
        if filter_config.get("brightness") is not None:
            extra.append(f"brightness={float(filter_config['brightness']):.4f}")
        if filter_config.get("contrast") is not None:
            extra.append(f"contrast={float(filter_config['contrast']):.4f}")
        if filter_config.get("saturation") is not None:
            extra.append(f"saturation={float(filter_config['saturation']):.4f}")
        if extra and base:
            return base + "," + "eq=" + ":".join(extra)
        if extra:
            return "eq=" + ":".join(extra)
        return base

    def apply_filter(self, project: dict[str, Any], clip_id: str, filter_config: dict[str, Any]) -> dict[str, Any]:
        fl = project.setdefault("filters", [])
        clip_id = str(clip_id)
        fc = dict(filter_config)
        fc["id"] = fc.get("id") or f"filter_{clip_id[:16]}"
        fc["clip_id"] = clip_id
        fl = [x for x in fl if not (isinstance(x, dict) and str(x.get("clip_id")) == clip_id)]
        fl.append(fc)
        project["filters"] = fl
        return project

    def remove_filter_for_clip(self, project: dict[str, Any], clip_id: str) -> dict[str, Any]:
        cid = str(clip_id)
        project["filters"] = [
            x for x in (project.get("filters") or []) if not (isinstance(x, dict) and str(x.get("clip_id")) == cid)
        ]
        return project
