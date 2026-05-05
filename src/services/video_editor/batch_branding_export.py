"""Tạo project tối giản: 1 video + logo overlay + BGM — dùng cho xuất hàng loạt cùng một kiểu."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from src.services.video_editor.audio_mix_manager import AudioMixManager
from src.services.video_editor.layout import ensure_video_editor_layout
from src.services.video_editor.media_manager import MediaManager
from src.services.video_editor.project_manager import VideoEditorProjectManager
from src.services.video_editor.project_schema import merge_phase2_defaults
from src.services.video_editor.timeline_manager import TimelineManager

VIDEO_EXTENSIONS = frozenset(
    {".mp4", ".mov", ".mkv", ".webm", ".avi", ".m4v", ".wmv", ".mpeg", ".mpg", ".3gp"}
)


def list_videos_in_folder(folder: Path) -> list[Path]:
    if not folder.is_dir():
        return []
    return sorted(p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in VIDEO_EXTENSIONS)


def create_branded_project_for_video(
    video_path: Path,
    *,
    template_project: dict[str, Any] | None,
    logo_path: Path | None,
    audio_path: Path | None,
    audio_mode: str,
    bgm_volume: float,
    logo_xywh: tuple[int, int, int, int] | None = None,
    logo_opacity: float = 0.92,
    copy_inputs_to_library: bool = False,
    paths: dict[str, Path] | None = None,
) -> dict[str, Any]:
    """
    Tạo project mới (đã lưu JSON), một clip video + overlay logo (tuỳ chọn) + BGM (tuỳ chọn).
    Gọi ``delete_project`` sau khi export xong để tránh đầy thư mục projects.
    """
    paths = paths or ensure_video_editor_layout()
    tpl = template_project or {}
    w = int(tpl.get("width") or 1080)
    h = int(tpl.get("height") or 1920)
    fps = int(tpl.get("fps") or 30)

    pm = VideoEditorProjectManager(paths=paths)
    mm = MediaManager(paths=paths)
    tm = TimelineManager(project_manager=pm)
    amix = AudioMixManager()

    proj = pm.create_project(str(video_path.stem)[:80], width=w, height=h, fps=fps)
    if isinstance(tpl.get("export"), dict) and tpl["export"]:
        proj["export"] = copy.deepcopy(tpl["export"])
        pm.save_project(proj)

    vrec = mm.import_media(str(video_path), "video", copy_to_library=copy_inputs_to_library)
    proj.setdefault("media", []).append(vrec)
    tm.add_clip(proj, str(vrec["id"]), "video")

    if logo_path is not None and logo_path.is_file():
        lrec = mm.import_media(str(logo_path), "image", copy_to_library=copy_inputs_to_library)
        proj.setdefault("media", []).append(lrec)
        tm.add_clip(proj, str(lrec["id"]), "overlay")
        if logo_xywh is not None:
            ox, oy, ow, oh = logo_xywh
        else:
            ow = max(100, int(w * 0.15))
            ox, oy, oh = 24, 24, ow
        for tr in proj.get("tracks") or []:
            if str(tr.get("type") or "") != "overlay":
                continue
            for cl in tr.get("clips") or []:
                if isinstance(cl, dict) and str(cl.get("media_id") or "") == str(lrec["id"]):
                    cl["x"] = int(ox)
                    cl["y"] = int(oy)
                    cl["width"] = int(ow)
                    cl["height"] = int(oh)
                    cl["opacity"] = max(0.0, min(1.0, float(logo_opacity)))
                    break
        pm.save_project(proj)

    if audio_path is not None and audio_path.is_file():
        arec = mm.import_media(str(audio_path), "audio", copy_to_library=copy_inputs_to_library)
        proj.setdefault("media", []).append(arec)
        proj["audio_mode"] = str(audio_mode or "mix").lower().strip()
        pm.save_project(proj)
        dur = float(proj.get("duration") or 0)
        amix.add_background_music(
            proj,
            str(arec["id"]),
            float(bgm_volume),
            duration=max(dur, 1.0),
            loop=True,
        )

    merge_phase2_defaults(proj)
    pm.save_project(proj)
    return proj
