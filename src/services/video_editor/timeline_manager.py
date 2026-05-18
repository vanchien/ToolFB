"""Thao tác timeline / clip."""

from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any

from src.services.video_editor.project_manager import VideoEditorProjectManager


def _clip_duration(source_start: float, source_end: float) -> float:
    return max(0.0, float(source_end) - float(source_start))


def timeline_duration_from_source(
    source_start: float,
    source_end: float,
    speed: float = 1.0,
) -> float:
    """
    Độ dài clip trên timeline (giây) từ đoạn nguồn đã cắt và tốc độ.

    Khớp FFmpeg: output ≈ (source_end - source_start) / speed.
    """
    span = _clip_duration(source_start, source_end)
    sp = float(speed) if float(speed) > 1e-6 else 1.0
    return round(max(0.05, span / sp), 4)


def effective_source_span(
    clip: dict[str, Any],
    *,
    media_duration: float | None = None,
    slack_sec: float = 0.02,
) -> tuple[float, float]:
    """
    Khung ``source_start`` / ``source_end`` khớp ``duration`` × ``speed`` trên timeline.

    Tránh ``source_end`` = cả file (phút) trong khi timeline chỉ vài giây — gây đuôi video thừa 1–2s khi export.
    """
    ss = float(clip.get("source_start") or 0.0)
    try:
        sp = float(clip.get("speed") or 1.0)
    except (TypeError, ValueError):
        sp = 1.0
    if sp <= 0:
        sp = 1.0
    du = max(0.0, float(clip.get("duration") or 0.0))
    se_raw = float(clip.get("source_end") or 0.0)
    need = max(0.05, du * sp)
    se = se_raw if se_raw > ss + 1e-6 else (ss + need)
    cap = ss + need + max(0.0, float(slack_sec))
    if se > cap:
        se = cap
    if media_duration is not None and float(media_duration) > 0:
        se = min(se, max(ss + 0.05, float(media_duration)))
    if se <= ss + 0.05:
        se = ss + 0.05
    return ss, se


def reconcile_clip_duration_from_source(clip: dict[str, Any]) -> None:
    """Cập nhật ``duration`` trên clip video/audio từ ``source_start`` / ``source_end`` / ``speed``."""
    if str(clip.get("type") or "") not in ("video", "audio"):
        return
    ss = float(clip.get("source_start") or 0)
    se = float(clip.get("source_end") or ss)
    try:
        sp = float(clip.get("speed") or 1.0)
    except (TypeError, ValueError):
        sp = 1.0
    if sp <= 0:
        sp = 1.0
    clip["duration"] = timeline_duration_from_source(ss, se, sp)


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


def compute_video_timeline_end(project: dict[str, Any]) -> float:
    """Độ dài timeline chỉ tính từ track video (không kéo dài vì clip audio cũ)."""
    end = 0.0
    for tr in project.get("tracks") or []:
        if not isinstance(tr, dict) or str(tr.get("type") or "") != "video":
            continue
        for cl in tr.get("clips") or []:
            if not isinstance(cl, dict) or str(cl.get("type") or "") != "video":
                continue
            ts = float(cl.get("timeline_start") or 0)
            du = float(cl.get("duration") or 0)
            end = max(end, ts + du)
    return round(end, 4)


def audio_source_bounds_for_timeline(
    *,
    timeline_duration: float,
    speed: float,
    media_duration: float = 0.0,
    source_start: float = 0.0,
) -> tuple[float, float, bool]:
    """
    Nguồn cần đọc để phát ``timeline_duration`` giây trên timeline (sau atempo).

    Returns:
        (source_start, source_end, loop) — ``loop`` chỉ khi file nguồn ngắn hơn đoạn cần phát.
    """
    sp = float(speed) if float(speed) > 1e-6 else 1.0
    du = max(0.05, float(timeline_duration))
    ss = max(0.0, float(source_start))
    need_src = du * sp
    md = max(0.0, float(media_duration))
    if md > 0:
        avail = max(0.05, md - ss)
        use_src = min(avail, max(0.05, need_src))
        se = ss + use_src
        loop = use_src + 1e-6 < need_src
    else:
        se = ss + max(0.05, need_src)
        loop = False
    return ss, round(se, 4), loop


def _intervals_overlap(a0: float, a1: float, b0: float, b1: float) -> bool:
    return min(a1, b1) > max(a0, b0)


def sync_overlapping_audio_clips_to_video(
    project: dict[str, Any],
    video_clip_id: str,
    *,
    align_timeline_start: bool = False,
    speed: float | None = None,
) -> int:
    """
    Cắt / kéo clip audio chồng khung thời gian clip video — tránh audio dài hơn video sau khi trim.

    ``align_timeline_start``: True khi gán âm mới (cùng điểm bắt đầu với video).
    """
    found = _find_clip(project, str(video_clip_id))
    if not found:
        return 0
    _, vc = found
    if str(vc.get("type") or "") != "video":
        return 0
    v_ts = float(vc.get("timeline_start") or 0)
    v_du = max(0.05, float(vc.get("duration") or 0))
    v_te = v_ts + v_du
    media_by_id = {
        str(m.get("id") or ""): m
        for m in (project.get("media") or [])
        if isinstance(m, dict) and m.get("id")
    }
    n = 0
    for tr in project.get("tracks") or []:
        if not isinstance(tr, dict) or str(tr.get("type") or "") != "audio":
            continue
        for ac in tr.get("clips") or []:
            if not isinstance(ac, dict) or str(ac.get("type") or "") != "audio":
                continue
            a_ts = float(ac.get("timeline_start") or 0)
            a_du = max(0.0, float(ac.get("duration") or 0))
            a_te = a_ts + a_du
            if not _intervals_overlap(v_ts, v_te, a_ts, a_te):
                continue
            if align_timeline_start or abs(a_ts - v_ts) < 0.05:
                ac["timeline_start"] = round(v_ts, 4)
                a_ts = v_ts
            o0 = max(v_ts, a_ts)
            o1 = min(v_te, a_te)
            new_du = max(0.05, o1 - o0)
            ac["duration"] = round(new_du, 4)
            try:
                sp_a = float(speed) if speed is not None else float(ac.get("speed") or 1.0)
            except (TypeError, ValueError):
                sp_a = 1.0
            if sp_a <= 0:
                sp_a = 1.0
            if speed is not None:
                ac["speed"] = sp_a
            mid = str(ac.get("media_id") or "")
            md = 0.0
            if mid and mid in media_by_id:
                try:
                    md = max(0.0, float(media_by_id[mid].get("duration") or 0))
                except (TypeError, ValueError):
                    md = 0.0
            ss, se, loop = audio_source_bounds_for_timeline(
                timeline_duration=new_du,
                speed=sp_a,
                media_duration=md,
                source_start=float(ac.get("source_start") or 0),
            )
            ac["source_start"] = ss
            ac["source_end"] = se
            ac["loop"] = loop
            n += 1
    return n


class TimelineManager:
    """Xử lý timeline."""

    def __init__(self, *, project_manager: VideoEditorProjectManager | None = None) -> None:
        self._pm = project_manager or VideoEditorProjectManager()

    def refresh_project_duration(self, project: dict[str, Any]) -> None:
        """Cập nhật ``project['duration']`` từ toàn bộ clip (gọi sau thao tác hàng loạt nếu đã tắt recompute từng bước)."""
        _update_project_duration(project)

    def sync_overlapping_audio_to_video(
        self,
        project: dict[str, Any],
        video_clip_id: str,
        *,
        align_timeline_start: bool = False,
        speed: float | None = None,
    ) -> int:
        """Đồng bộ clip audio chồng khung với clip video (độ dài + nguồn + tốc độ)."""
        return sync_overlapping_audio_clips_to_video(
            project,
            video_clip_id,
            align_timeline_start=align_timeline_start,
            speed=speed,
        )

    def add_clip(
        self,
        project: dict[str, Any],
        media_id: str,
        track_type: str,
        *,
        persist: bool = True,
        recompute_duration: bool = True,
        out_new_clip: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
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
                "zoom": 1.0,
                "brightness": 0.0,
                "light_effect": "none",
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
                "speed": 1.0,
                "fade_in": 0.0,
                "fade_out": 0.0,
                "loop": True,
            }
        else:
            raise ValueError("Track text dùng add_text_clip hoặc clip type text.")

        clips.append(clip)
        if recompute_duration:
            _update_project_duration(project)
        if persist:
            self._pm.save_project(project)
        if out_new_clip is not None:
            out_new_clip.append(clip)
        return project

    def add_text_clip(
        self,
        project: dict[str, Any],
        text: str,
        *,
        timeline_start: float = 0.0,
        duration: float = 5.0,
        persist: bool = True,
        recompute_duration: bool = True,
        out_new_clip: list[dict[str, Any]] | None = None,
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
        if recompute_duration:
            _update_project_duration(project)
        if persist:
            self._pm.save_project(project)
        if out_new_clip is not None:
            out_new_clip.append(clip)
        return project

    def flip_clip(
        self,
        project: dict[str, Any],
        clip_id: str,
        *,
        horizontal: bool = False,
        vertical: bool = False,
        persist: bool = True,
        recompute_duration: bool = True,
    ) -> dict[str, Any]:
        return self.update_clip(
            project,
            clip_id,
            {"flip_horizontal": bool(horizontal), "flip_vertical": bool(vertical)},
            persist=persist,
            recompute_duration=recompute_duration,
        )

    def rotate_clip(
        self,
        project: dict[str, Any],
        clip_id: str,
        rotation: int,
        *,
        persist: bool = True,
        recompute_duration: bool = True,
    ) -> dict[str, Any]:
        r = int(rotation) % 360
        if r not in (0, 90, 180, 270):
            raise ValueError("rotation phải là 0, 90, 180 hoặc 270")
        return self.update_clip(project, clip_id, {"rotation": r}, persist=persist, recompute_duration=recompute_duration)

    def crop_clip(
        self,
        project: dict[str, Any],
        clip_id: str,
        crop: dict[str, Any],
        *,
        persist: bool = True,
        recompute_duration: bool = True,
    ) -> dict[str, Any]:
        return self.update_clip(
            project,
            clip_id,
            {"crop": deepcopy(crop)},
            persist=persist,
            recompute_duration=recompute_duration,
        )

    def set_canvas_mode(
        self,
        project: dict[str, Any],
        clip_id: str,
        mode: str,
        *,
        persist: bool = True,
        recompute_duration: bool = True,
    ) -> dict[str, Any]:
        m = str(mode or "fit").lower().strip()
        if m not in ("fit", "fill", "stretch"):
            raise ValueError("canvas_mode phải là fit, fill hoặc stretch")
        return self.update_clip(project, clip_id, {"canvas_mode": m}, persist=persist, recompute_duration=recompute_duration)

    def set_speed(
        self,
        project: dict[str, Any],
        clip_id: str,
        speed: float,
        *,
        persist: bool = True,
        recompute_duration: bool = True,
    ) -> dict[str, Any]:
        sp = float(speed)
        if sp <= 0:
            raise ValueError("speed phải > 0")
        return self.update_clip(project, clip_id, {"speed": sp}, persist=persist, recompute_duration=recompute_duration)

    def mute_clip(
        self,
        project: dict[str, Any],
        clip_id: str,
        muted: bool,
        *,
        persist: bool = True,
        recompute_duration: bool = True,
    ) -> dict[str, Any]:
        return self.update_clip(project, clip_id, {"muted": bool(muted)}, persist=persist, recompute_duration=recompute_duration)

    def trim_clip(
        self,
        project: dict[str, Any],
        clip_id: str,
        source_start: float,
        source_end: float,
        *,
        persist: bool = True,
        recompute_duration: bool = True,
    ) -> dict[str, Any]:
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
        try:
            sp = float(clip.get("speed") or 1.0)
        except (TypeError, ValueError):
            sp = 1.0
        if sp <= 0:
            sp = 1.0
        clip["duration"] = timeline_duration_from_source(ss, se, sp)
        if recompute_duration:
            _update_project_duration(project)
        if persist:
            self._pm.save_project(project)
        return project

    def split_clip(
        self,
        project: dict[str, Any],
        clip_id: str,
        split_time: float,
        *,
        persist: bool = True,
        recompute_duration: bool = True,
    ) -> dict[str, Any]:
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

        if recompute_duration:
            _update_project_duration(project)
        if persist:
            self._pm.save_project(project)
        return project

    def move_clip(
        self,
        project: dict[str, Any],
        clip_id: str,
        new_start: float,
        *,
        persist: bool = True,
        recompute_duration: bool = True,
    ) -> dict[str, Any]:
        found = _find_clip(project, clip_id)
        if not found:
            raise ValueError("Không tìm thấy clip.")
        _, clip = found
        clip["timeline_start"] = float(new_start)
        if recompute_duration:
            _update_project_duration(project)
        if persist:
            self._pm.save_project(project)
        return project

    def delete_clip(
        self,
        project: dict[str, Any],
        clip_id: str,
        *,
        persist: bool = True,
        recompute_duration: bool = True,
    ) -> dict[str, Any]:
        cid = str(clip_id)
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict):
                continue
            clips = tr.get("clips") or []
            tr["clips"] = [c for c in clips if not (isinstance(c, dict) and str(c.get("id") or "") == cid)]
        if recompute_duration:
            _update_project_duration(project)
        if persist:
            self._pm.save_project(project)
        return project

    def update_clip(
        self,
        project: dict[str, Any],
        clip_id: str,
        patch: dict[str, Any],
        *,
        persist: bool = True,
        recompute_duration: bool = True,
    ) -> dict[str, Any]:
        found = _find_clip(project, clip_id)
        if not found:
            raise ValueError("Không tìm thấy clip.")
        _, clip = found
        for k, v in (patch or {}).items():
            if k == "id":
                continue
            clip[str(k)] = deepcopy(v)
        if "source_start" in patch or "source_end" in patch or "speed" in patch:
            if str(clip.get("type") or "") in ("video", "audio"):
                reconcile_clip_duration_from_source(clip)
        if recompute_duration:
            _update_project_duration(project)
        if persist:
            self._pm.save_project(project)
        return project
