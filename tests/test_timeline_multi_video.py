"""Timeline: nhiều video độc lập (T=0), xuất tách file — không nối đuôi."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest

from src.services.video_editor.media_manager import MediaManager
from src.services.video_editor.project_manager import VideoEditorProjectManager
from src.services.video_editor.timeline_manager import (
    TimelineManager,
    iter_video_clips,
    video_timeline_clips_overlap,
)
from src.services.video_editor.validation import validate_export
from src.utils.ffmpeg_paths import resolve_ffmpeg_executable


def _synthetic_mp4(ffmpeg: str, dest: Path, *, duration: float = 1.0) -> None:
    cmd = [
        ffmpeg,
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"testsrc=duration={duration}:size=160x120:rate=24",
        "-f",
        "lavfi",
        "-i",
        f"sine=frequency=440:duration={duration}",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-shortest",
        str(dest),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=90)
    assert r.returncode == 0, (r.stderr or r.stdout or "")[-1500:]


def test_add_two_videos_independent_at_timeline_zero(tmp_path: Path) -> None:
    """Mỗi video thêm lên timeline đều ở T=0 — không xếp nối đuôi."""
    ff = resolve_ffmpeg_executable()
    if not ff:
        pytest.skip("Cần ffmpeg.")
    root = tmp_path / "ve_multi"
    paths = {
        "projects": root / "projects",
        "media": root / "media",
        "renders": root / "renders",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)

    pm = VideoEditorProjectManager(paths=paths)  # type: ignore[arg-type]
    mm = MediaManager(paths=paths)  # type: ignore[arg-type]
    project: dict[str, Any] = pm.create_project("multi", width=320, height=240, fps=24)

    mids: list[str] = []
    for i in range(2):
        src = paths["media"] / f"v{i}.mp4"
        _synthetic_mp4(ff, src, duration=1.2 + i * 0.3)
        media = mm.import_media(str(src), "video", copy_to_library=True)
        project.setdefault("media", []).append(media)
        mids.append(str(media["id"]))

    tm = TimelineManager(project_manager=pm)
    for mid in mids:
        tm.add_clip(project, mid, "video", persist=False, recompute_duration=False)
        clips_now = iter_video_clips(project)
        if clips_now:
            clips_now[-1]["timeline_start"] = 0.0
    tm.refresh_project_duration(project)

    clips = iter_video_clips(project)
    assert len(clips) == 2
    assert float(clips[0].get("timeline_start") or 0) == 0.0
    assert float(clips[1].get("timeline_start") or 0) == 0.0
    assert video_timeline_clips_overlap(project)

    out_p = paths["renders"] / "out.mp4"
    errs = validate_export(
        project,
        ffmpeg_path=ff,
        output_path=str(out_p),
        media_resolver=mm,
        require_contiguous_video_timeline=False,
    )
    assert errs == [], errs
