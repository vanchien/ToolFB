"""
Preset hiệu năng cho luồng tương tác giống người dùng.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any


@dataclass(frozen=True)
class HumanInteractionProfile:
    name: str
    newsfeed_prob: float
    reels_prob: float
    search_prob: float
    post_prob: float
    deep_delay_min_sec: float
    deep_delay_max_sec: float
    sync_wait_sec: float
    like_rate_pct: float = 0.30
    comment_rate_pct: float = 0.10
    virtual_cursor: bool = True
    ai_comments: bool = True
    # Cuộn bảng tin: số vòng + thời gian đọc (dwell_scale nhân hệ số chờ sau mỗi lần cuộn)
    scroll_rounds_min: int = 16
    scroll_rounds_max: int = 26
    scroll_rounds_short_min: int = 8
    scroll_rounds_short_max: int = 14
    dwell_scale: float = 1.15
    module_pause_min_sec: float = 2.0
    module_pause_max_sec: float = 4.0
    # Giới hạn số module thực sự chạy mỗi lượt (tránh 1 TK kéo dài 20+ phút)
    max_modules_per_run: int = 3
    reels_clip_min_ms: int = 5000
    reels_clip_max_ms: int = 9500
    page_load_pause_min_sec: float = 1.0
    page_load_pause_max_sec: float = 2.2


PROFILES: dict[str, HumanInteractionProfile] = {
    "safe": HumanInteractionProfile(
        name="safe",
        newsfeed_prob=0.75,
        reels_prob=0.65,
        search_prob=0.45,
        post_prob=0.15,
        deep_delay_min_sec=14.0,
        deep_delay_max_sec=24.0,
        sync_wait_sec=4.0,
        scroll_rounds_min=12,
        scroll_rounds_max=18,
        scroll_rounds_short_min=7,
        scroll_rounds_short_max=11,
        dwell_scale=1.0,
        module_pause_min_sec=1.4,
        module_pause_max_sec=2.6,
        max_modules_per_run=3,
        reels_clip_min_ms=6500,
        reels_clip_max_ms=12_000,
        page_load_pause_min_sec=1.4,
        page_load_pause_max_sec=2.8,
    ),
    "normal": HumanInteractionProfile(
        name="normal",
        newsfeed_prob=0.70,
        reels_prob=0.60,
        search_prob=0.40,
        post_prob=0.20,
        deep_delay_min_sec=7.0,
        deep_delay_max_sec=14.0,
        sync_wait_sec=2.0,
        scroll_rounds_min=8,
        scroll_rounds_max=13,
        scroll_rounds_short_min=5,
        scroll_rounds_short_max=8,
        dwell_scale=0.82,
        module_pause_min_sec=0.7,
        module_pause_max_sec=1.5,
        max_modules_per_run=3,
        reels_clip_min_ms=4500,
        reels_clip_max_ms=9000,
        page_load_pause_min_sec=1.0,
        page_load_pause_max_sec=2.0,
    ),
    "fast": HumanInteractionProfile(
        name="fast",
        newsfeed_prob=0.60,
        reels_prob=0.50,
        search_prob=0.30,
        post_prob=0.10,
        deep_delay_min_sec=4.0,
        deep_delay_max_sec=9.0,
        sync_wait_sec=1.5,
        scroll_rounds_min=6,
        scroll_rounds_max=10,
        scroll_rounds_short_min=4,
        scroll_rounds_short_max=7,
        dwell_scale=0.72,
        module_pause_min_sec=0.5,
        module_pause_max_sec=1.1,
        max_modules_per_run=2,
        reels_clip_min_ms=3500,
        reels_clip_max_ms=6500,
        page_load_pause_min_sec=0.7,
        page_load_pause_max_sec=1.4,
    ),
}


def _pct_to_rate(raw: Any, default: float) -> float:
    """Chuyển 30 hoặc 0.30 thành tỷ lệ 0–1."""
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return default
    if v > 1.0:
        v = v / 100.0
    return max(0.0, min(1.0, v))


def resolve_profile(name: str | None, *, settings: dict[str, Any] | None = None) -> HumanInteractionProfile:
    key = str(name or "normal").strip().lower()
    if key == "auto":
        key = "normal"
    base = PROFILES.get(key, PROFILES["normal"])
    if not settings:
        return base
    overrides: dict[str, Any] = {}
    if "like_rate_pct" in settings:
        overrides["like_rate_pct"] = _pct_to_rate(settings["like_rate_pct"], base.like_rate_pct)
    if "comment_rate_pct" in settings:
        overrides["comment_rate_pct"] = _pct_to_rate(settings["comment_rate_pct"], base.comment_rate_pct)
    if "virtual_cursor" in settings:
        overrides["virtual_cursor"] = bool(settings["virtual_cursor"])
    if "ai_comments" in settings:
        overrides["ai_comments"] = bool(settings["ai_comments"])
    for key in (
        "scroll_rounds_min",
        "scroll_rounds_max",
        "scroll_rounds_short_min",
        "scroll_rounds_short_max",
        "dwell_scale",
        "module_pause_min_sec",
        "module_pause_max_sec",
        "deep_delay_min_sec",
        "deep_delay_max_sec",
        "max_modules_per_run",
        "reels_clip_min_ms",
        "reels_clip_max_ms",
        "page_load_pause_min_sec",
        "page_load_pause_max_sec",
    ):
        if key in settings:
            try:
                if key in ("max_modules_per_run", "reels_clip_min_ms", "reels_clip_max_ms"):
                    overrides[key] = int(settings[key])
                elif "scale" in key or "sec" in key:
                    overrides[key] = float(settings[key])
                else:
                    overrides[key] = int(settings[key])
            except (TypeError, ValueError):
                pass
    return replace(base, **overrides) if overrides else base

