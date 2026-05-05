"""Nhạc nền + ducking (manual) — metadata; FFmpeg trong builder."""

from __future__ import annotations

import uuid
from typing import Any


class AudioMixManager:
    def auto_add_existing_audio_as_bgm(
        self,
        project: dict[str, Any],
        volume: float,
        *,
        only_first: bool = True,
        skip_existing_media: bool = True,
    ) -> int:
        """
        Tự quét media audio đã import trong project và thêm vào BGM.
        Trả về số lượng audio được thêm.
        """
        media = [m for m in (project.get("media") or []) if isinstance(m, dict) and str(m.get("type") or "") == "audio"]
        if not media:
            return 0
        au = project.setdefault("audio_settings", {})
        bgm = au.setdefault("bgm", [])
        existing_ids = {str(x.get("media_id") or "") for x in bgm if isinstance(x, dict)}
        added = 0
        for m in media:
            mid = str(m.get("id") or "")
            if not mid:
                continue
            if skip_existing_media and mid in existing_ids:
                continue
            dur = float(m.get("duration") or 0) or float(project.get("duration") or 0) or 60.0
            self.add_background_music(project, mid, volume, duration=dur, loop=True)
            existing_ids.add(mid)
            added += 1
            if only_first and added >= 1:
                break
        return added

    def add_background_music(
        self,
        project: dict[str, Any],
        media_id: str,
        volume: float,
        *,
        timeline_start: float = 0.0,
        duration: float | None = None,
        fade_in: float = 0.0,
        fade_out: float = 0.0,
        loop: bool = True,
    ) -> dict[str, Any]:
        au = project.setdefault("audio_settings", {})
        bgm = au.setdefault("bgm", [])
        dur = float(duration) if duration is not None else float(project.get("duration") or 0)
        bgm.append(
            {
                "id": f"bgm_{uuid.uuid4().hex[:10]}",
                "type": "background_music",
                "media_id": str(media_id),
                "timeline_start": float(timeline_start),
                "duration": max(0.1, dur),
                "volume": float(volume),
                "fade_in": float(fade_in),
                "fade_out": float(fade_out),
                "loop": bool(loop),
            }
        )
        return project

    def add_ducking_range(
        self,
        project: dict[str, Any],
        start: float,
        end: float,
        bgm_volume: float,
    ) -> dict[str, Any]:
        au = project.setdefault("audio_settings", {})
        duck = au.setdefault("ducking", [])
        duck.append({"start": float(start), "end": float(end), "bgm_volume": float(bgm_volume)})
        return project

    def clear_bgm(self, project: dict[str, Any]) -> dict[str, Any]:
        project.setdefault("audio_settings", {})["bgm"] = []
        return project

    def build_bgm_volume_expression(self, base_volume: float, ducking: list[dict[str, Any]]) -> str:
        """
        Biểu thức volume cho BGM (eval=frame); dấu phẩy escape cho filter_complex.
        """
        v = float(base_volume)
        if not ducking:
            return str(v)
        cur = f"{v}"
        for d in ducking:
            if not isinstance(d, dict):
                continue
            a, b = float(d.get("start") or 0), float(d.get("end") or 0)
            tgt = float(d.get("bgm_volume") if d.get("bgm_volume") is not None else v * 0.5)
            # between(t,a,b) — cần \\, trong chuỗi filter
            cur = f"if(between(t\\,{a}\\,{b})\\,{tgt}\\,{cur})"
        return cur
