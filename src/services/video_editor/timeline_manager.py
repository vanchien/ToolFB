"""Thao tác timeline / clip."""

from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any

from src.services.video_editor.project_manager import VideoEditorProjectManager


def _clip_duration(source_start: float, source_end: float) -> float:
    return max(0.0, float(source_end) - float(source_start))


def _find_track(project: dict[str, Any], track_type: str) -> dict[str, Any] | None:
    for tr in project.get("tracks") or []:
        if isinstance(tr, dict) and str(tr.get("type") or "") == track_type:
            return tr
    return None


def _find_clip(project: dict[str, Any], clip_id: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
    cid = str(clip_id)
    for tr in project.get("tracks") or []:
        if not isinstance(tr, dict):
            continue
        for cl in tr.get("clips") or []:
            if isinstance(cl, dict) and str(cl.get("id") or "") == cid:
                return tr, cl
    return None


def _update_project_duration(project: dict[str, Any]) -> None:
    """duration = max(timeline_start + duration_clip)."""
    end = 0.0
    for tr in project.get("tracks") or []:
        if not isinstance(tr, dict):
            continue
        for cl in tr.get("clips") or []:
            if not isinstance(cl, dict):
                continue
            ts = float(cl.get("timeline_start") or 0)
            du = float(cl.get("duration") or 0)
            end = max(end, ts + du)
    project["duration"] = round(end, 4)


class TimelineManager:
    """Xử lý timeline."""

    def __init__(self, *, project_manager: VideoEditorProjectManager | None = None) -> None:
        self._pm = project_manager or VideoEditorProjectManager()

    def add_clip(self, project: dict[str, Any], media_id: str, track_type: str) -> dict[str, Any]:
        tt = str(track_type or "").strip().lower()
        media = None
        for m in project.get("media") or []:
            if isinstance(m, dict) and str(m.get("id") or "") == str(media_id):
                media = m
                break
        if not media:
            raise ValueError("Không tìm thấy media trong project.")

        mtype = str(media.get("type") or "")
        tr = _find_track(project, tt)
        if not tr:
            raise ValueError(f"Không tìm thấy track loại {tt}")

        if tt == "video" and mtype != "video":
            raise ValueError("Track video chỉ nhận media video.")
        if tt == "overlay" and mtype != "image":
            raise ValueError("Track overlay chỉ nhận ảnh/logo.")
        if tt == "audio" and mtype != "audio":
            raise ValueError("Track audio chỉ nhận media audio.")

        clips = tr.setdefault("clips", [])
        clip_id = f"clip_{uuid.uuid4().hex[:10]}"
        duration = float(media.get("duration") or 0)
        width = int(project.get("width") or 1080)
        height = int(project.get("height") or 1920)

        if tt == "video":
            src_end = float(media.get("duration") or 0)
            clip: dict[str, Any] = {
                "id": clip_id,
                "media_id": str(media_id),
                "type": "video",
                "timeline_start": 0.0,
                "duration": src_end,
                "source_start": 0.0,
                "source_end": src_end,
                "speed": 1.0,
                "volume": 1.0,
                "muted": False,
                "fade_in": 0.0,
                "fade_out": 0.0,
                "x": 0,
                "y": 0,
                "width": width,
                "height": height,
                "opacity": 1.0,
                "flip_horizontal": False,
                "flip_vertical": False,
                "rotation": 0,
                "crop": {
                    "enabled": False,
                    "x": 0,
                    "y": 0,
                    "width": width,
                    "height": height,
                },
                "scale": {
                    "enabled": False,
                    "width": width,
                    "height": height,
                    "keep_aspect": True,
                },
                "canvas_mode": "fit",
                "blur_background": {"enabled": False, "blur": 20},
            }
            if clips:
                last_end = 0.0
                for c in clips:
                    if isinstance(c, dict):
                        ts = float(c.get("timeline_start") or 0)
                        du = float(c.get("duration") or 0)
                        last_end = max(last_end, ts + du)
                clip["timeline_start"] = round(last_end, 4)
        elif tt == "overlay":
            pdur = float(project.get("duration") or 0)
            dur = pdur if pdur > 0 else 8.0
            clip = {
                "id": clip_id,
                "media_id": str(media_id),
                "type": "image",
                "timeline_start": 0.0,
                "duration": round(dur, 4),
                "x": 30,
                "y": 30,
                "width": 180,
                "height": 180,
                "opacity": 0.8,
                "random_motion_enabled": False,
                "random_motion_interval": 2.0,
                "random_motion_seed": 0,
                "random_motion_smooth": True,
            }
        elif tt == "audio":
            clip = {
                "id": clip_id,
                "media_id": str(media_id),
                "type": "audio",
                "timeline_start": 0.0,
                "duration": duration,
                "source_start": 0.0,
                "source_end": duration,
                "volume": 1.0,
                "fade_in": 0.0,
                "fade_out": 0.0,
                "loop": True,
            }
        else:
            raise ValueError("Track text dùng add_text_clip hoặc clip type text.")

        clips.append(clip)
        _update_project_duration(project)
        self._pm.save_project(project)
        return project

    def add_text_clip(
        self,
        project: dict[str, Any],
        text: str,
        *,
        timeline_start: float = 0.0,
        duration: float = 5.0,
    ) -> dict[str, Any]:
        tr = _find_track(project, "text")
        if not tr:
            raise ValueError("Không tìm thấy track text.")
        clip_id = f"clip_{uuid.uuid4().hex[:10]}"
        clip = {
            "id": clip_id,
            "type": "text",
            "text": str(text or ""),
            "timeline_start": float(timeline_start),
            "duration": float(duration),
            "x": 100,
            "y": 150,
            "font_size": 48,
            "color": "white",
            "font_file": "",
            "fade_in": 0.0,
            "fade_out": 0.0,
            "random_motion_enabled": False,
            "random_motion_interval": 2.0,
            "random_motion_seed": 0,
            "random_motion_smooth": True,
        }
        tr.setdefault("clips", []).append(clip)
        _update_project_duration(project)
        self._pm.save_project(project)
        return project

    def flip_clip(self, project: dict[str, Any], clip_id: str, *, horizontal: bool = False, vertical: bool = False) -> dict[str, Any]:
        return self.update_clip(project, clip_id, {"flip_horizontal": bool(horizontal), "flip_vertical": bool(vertical)})

    def rotate_clip(self, project: dict[str, Any], clip_id: str, rotation: int) -> dict[str, Any]:
        r = int(rotation) % 360
        if r not in (0, 90, 180, 270):
            raise ValueError("rotation phải là 0, 90, 180 hoặc 270")
        return self.update_clip(project, clip_id, {"rotation": r})

    def crop_clip(self, project: dict[str, Any], clip_id: str, crop: dict[str, Any]) -> dict[str, Any]:
        return self.update_clip(project, clip_id, {"crop": deepcopy(crop)})

    def set_canvas_mode(self, project: dict[str, Any], clip_id: str, mode: str) -> dict[str, Any]:
        m = str(mode or "fit").lower().strip()
        if m not in ("fit", "fill", "stretch"):
            raise ValueError("canvas_mode phải là fit, fill hoặc stretch")
        return self.update_clip(project, clip_id, {"canvas_mode": m})

    def set_blur_background(self, project: dict[str, Any], clip_id: str, enabled: bool, blur: int = 20) -> dict[str, Any]:
        return self.update_clip(
            project,
            clip_id,
            {"blur_background": {"enabled": bool(enabled), "blur": int(blur)}},
        )

    def set_speed(self, project: dict[str, Any], clip_id: str, speed: float) -> dict[str, Any]:
        sp = float(speed)
        if sp <= 0:
            raise ValueError("speed phải > 0")
        return self.update_clip(project, clip_id, {"speed": sp})

    def mute_clip(self, project: dict[str, Any], clip_id: str, muted: bool) -> dict[str, Any]:
        return self.update_clip(project, clip_id, {"muted": bool(muted)})

    def trim_clip(self, project: dict[str, Any], clip_id: str, source_start: float, source_end: float) -> dict[str, Any]:
        found = _find_clip(project, clip_id)
        if not found:
            raise ValueError("Không tìm thấy clip.")
        _, clip = found
        ctype = str(clip.get("type") or "")
        if ctype not in ("video", "audio"):
            raise ValueError("Trim chỉ áp dụng clip video hoặc audio.")
        ss, se = float(source_start), float(source_end)
        if ss >= se:
            raise ValueError("source_start phải nhỏ hơn source_end.")
        clip["source_start"] = ss
        clip["source_end"] = se
        du = _clip_duration(ss, se)
        clip["duration"] = round(du, 4)
        _update_project_duration(project)
        self._pm.save_project(project)
        return project

    def split_clip(self, project: dict[str, Any], clip_id: str, split_time: float) -> dict[str, Any]:
        """split_time: thời điểm trên timeline (giây)."""
        found = _find_clip(project, clip_id)
        if not found:
            raise ValueError("Không tìm thấy clip.")
        _, clip = found
        t0 = float(clip.get("timeline_start") or 0)
        du = float(clip.get("duration") or 0)
        st = float(split_time)
        if st <= t0 or st >= t0 + du:
            raise ValueError("split_time phải nằm trong khoảng clip trên timeline.")

        local = st - t0
        ctype = str(clip.get("type") or "")

        if ctype == "video":
            ss = float(clip.get("source_start") or 0)
            orig_end = float(clip.get("source_end") or 0)
            split_source = ss + local
            clip["duration"] = round(local, 4)
            clip["source_end"] = round(split_source, 4)

            new_id = f"clip_{uuid.uuid4().hex[:10]}"
            second = deepcopy(clip)
            second["id"] = new_id
            second["timeline_start"] = round(st, 4)
            second["duration"] = round(du - local, 4)
            second["source_start"] = round(split_source, 4)
            second["source_end"] = orig_end

            found[0].setdefault("clips", []).append(second)
        elif ctype == "image":
            clip["duration"] = round(local, 4)
            new_id = f"clip_{uuid.uuid4().hex[:10]}"
            second = deepcopy(clip)
            second["id"] = new_id
            second["timeline_start"] = round(st, 4)
            second["duration"] = round(du - local, 4)
            found[0].setdefault("clips", []).append(second)
        elif ctype == "text":
            clip["duration"] = round(local, 4)
            new_id = f"clip_{uuid.uuid4().hex[:10]}"
            second = deepcopy(clip)
            second["id"] = new_id
            second["timeline_start"] = round(st, 4)
            second["duration"] = round(du - local, 4)
            found[0].setdefault("clips", []).append(second)
        elif ctype == "audio":
            ss = float(clip.get("source_start") or 0)
            orig_end = float(clip.get("source_end") or 0)
            split_source = ss + local
            clip["duration"] = round(local, 4)
            clip["source_end"] = round(split_source, 4)
            new_id = f"clip_{uuid.uuid4().hex[:10]}"
            second = deepcopy(clip)
            second["id"] = new_id
            second["timeline_start"] = round(st, 4)
            second["duration"] = round(du - local, 4)
            second["source_start"] = round(split_source, 4)
            second["source_end"] = orig_end
            found[0].setdefault("clips", []).append(second)
        else:
            raise ValueError(f"Không hỗ trợ split cho type {ctype}")

        _update_project_duration(project)
        self._pm.save_project(project)
        return project

    def move_clip(self, project: dict[str, Any], clip_id: str, new_start: float) -> dict[str, Any]:
        found = _find_clip(project, clip_id)
        if not found:
            raise ValueError("Không tìm thấy clip.")
        _, clip = found
        clip["timeline_start"] = float(new_start)
        _update_project_duration(project)
        self._pm.save_project(project)
        return project

    def delete_clip(self, project: dict[str, Any], clip_id: str) -> dict[str, Any]:
        cid = str(clip_id)
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict):
                continue
            clips = tr.get("clips") or []
            tr["clips"] = [c for c in clips if not (isinstance(c, dict) and str(c.get("id") or "") == cid)]
        _update_project_duration(project)
        self._pm.save_project(project)
        return project

    def update_clip(self, project: dict[str, Any], clip_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        found = _find_clip(project, clip_id)
        if not found:
            raise ValueError("Không tìm thấy clip.")
        _, clip = found
        for k, v in (patch or {}).items():
            if k == "id":
                continue
            clip[str(k)] = deepcopy(v)
        if "source_start" in patch or "source_end" in patch:
            ss = float(clip.get("source_start") or 0)
            se = float(clip.get("source_end") or 0)
            if str(clip.get("type")) == "video":
                clip["duration"] = round(max(0.0, se - ss), 4)
        _update_project_duration(project)
        self._pm.save_project(project)
        return project
