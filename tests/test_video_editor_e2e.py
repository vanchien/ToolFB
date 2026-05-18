"""
E2E tự động cho pipeline lõi Video Editor (không mở Tk).

Luồng: tạo project → import video → thêm clip timeline → validate_export →
FFmpegCommandBuilder → RenderWorker — tương đương bước «Xuất» trong GUI.

Phần giao diện (inspector, double-click timeline, preview nháp) cần kiểm thử tay.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest

from src.services.video_editor.ffmpeg_builder import FFmpegCommandBuilder
from src.services.video_editor.media_manager import MediaManager
from src.services.video_editor.project_manager import VideoEditorProjectManager
from src.services.video_editor.render_worker import RenderWorker
from src.services.video_editor.timeline_manager import TimelineManager, effective_source_span
from src.services.video_editor.validation import validate_export
from src.utils.ffmpeg_paths import resolve_ffmpeg_executable, resolve_ffmpeg_ffprobe_paths


def _ve_paths(root: Path) -> dict[str, Path]:
    d: dict[str, Path] = {
        "root": root,
        "projects": root / "projects",
        "media": root / "media",
        "stock_audio": root / "stock_audio",
        "temp": root / "temp",
        "renders": root / "renders",
        "thumbnails": root / "thumbnails",
        "waveforms": root / "waveforms",
        "subtitles": root / "subtitles",
        "presets": root / "presets",
        "templates": root / "templates",
        "logs": root / "logs",
    }
    for p in d.values():
        p.mkdir(parents=True, exist_ok=True)
    return d


def _require_ffmpeg() -> str:
    ff = resolve_ffmpeg_executable()
    _, fp = resolve_ffmpeg_ffprobe_paths()
    if not ff or not fp:
        pytest.skip("Cần ffmpeg và ffprobe (PATH hoặc tools/ffmpeg/bin).")
    return ff


def _synthetic_mp4(ffmpeg: str, dest: Path) -> None:
    cmd = [
        ffmpeg,
        "-y",
        "-f",
        "lavfi",
        "-i",
        "testsrc=duration=2.5:size=320x240:rate=30",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:sample_rate=44100:duration=2.5",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-shortest",
        str(dest),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
    assert r.returncode == 0, (r.stderr or r.stdout or "")[-2000:]


def test_video_editor_export_pipeline_e2e(tmp_path: Path) -> None:
    ffmpeg_bin = _require_ffmpeg()
    src = tmp_path / "source.mp4"
    _synthetic_mp4(ffmpeg_bin, src)
    assert src.is_file()

    paths = _ve_paths(tmp_path / "video_editor_data")
    pm = VideoEditorProjectManager(paths=paths)
    project: dict[str, Any] = pm.create_project("e2e-auto", width=1080, height=1920, fps=30)

    mm = MediaManager(paths=paths)
    media = mm.import_media(str(src), "video", copy_to_library=True)
    project["media"] = list(project.get("media") or [])
    project["media"].append(media)
    pm.save_project(project)

    tm = TimelineManager(project_manager=pm)
    tm.add_clip(project, str(media["id"]), "video", persist=True)

    project = pm.load_project(str(project["id"]))
    out_p = paths["renders"] / "e2e_export.mp4"

    errs = validate_export(
        project,
        ffmpeg_path=ffmpeg_bin,
        output_path=str(out_p),
        media_resolver=mm,
        require_contiguous_video_timeline=True,
    )
    assert errs == [], errs

    builder = FFmpegCommandBuilder()
    cmd = builder.build_export_command(
        project,
        str(out_p),
        ffmpeg_bin=ffmpeg_bin,
        output_duration_limit_sec=1.0,
        lightweight_mode_override=True,
    )
    assert cmd and Path(cmd[0]).name.replace(".exe", "") in ("ffmpeg", "ffmpeg")

    dur = min(1.0, float(project.get("duration") or 1.0))
    worker = RenderWorker()
    result = worker.render(project, str(out_p), cmd, duration_sec=max(0.5, dur))
    assert result.get("ok") is True, result.get("error_message", result)
    assert out_p.is_file()
    assert out_p.stat().st_size > 800


def test_validate_export_rejects_empty_timeline(tmp_path: Path) -> None:
    ffmpeg_bin = _require_ffmpeg()
    paths = _ve_paths(tmp_path / "ve2")
    pm = VideoEditorProjectManager(paths=paths)
    project = pm.create_project("empty", width=720, height=1280, fps=30)
    out_p = paths["renders"] / "noop.mp4"
    errs = validate_export(project, ffmpeg_path=ffmpeg_bin, output_path=str(out_p))
    assert any("ít nhất một clip video" in e for e in errs)


def test_validate_export_non_contiguous_timeline_ok_by_default(tmp_path: Path) -> None:
    """Hai clip cùng mốc T=0 (xếp chồng / độc lập) — mặc định không chặn xuất."""
    ffmpeg_bin = _require_ffmpeg()
    paths = _ve_paths(tmp_path / "ve_nc")
    pm = VideoEditorProjectManager(paths=paths)
    mm = MediaManager(paths=paths)
    project = pm.create_project("nc", width=320, height=240, fps=30)
    src = paths["media"] / "a.mp4"
    _synthetic_mp4(ffmpeg_bin, src)
    media = mm.import_media(str(src), "video", copy_to_library=True)
    project.setdefault("media", []).append(media)
    mid = str(media["id"])
    vtrack = next(t for t in project["tracks"] if isinstance(t, dict) and str(t.get("type")) == "video")
    vtrack.setdefault("clips", []).extend(
        [
            {
                "id": "clip_a",
                "type": "video",
                "media_id": mid,
                "timeline_start": 0.0,
                "duration": 1.0,
                "source_start": 0.0,
                "source_end": 1.0,
            },
            {
                "id": "clip_b",
                "type": "video",
                "media_id": mid,
                "timeline_start": 0.0,
                "duration": 1.5,
                "source_start": 0.5,
                "source_end": 2.0,
            },
        ]
    )
    out_p = paths["renders"] / "out.mp4"
    errs = validate_export(project, ffmpeg_path=ffmpeg_bin, output_path=str(out_p), media_resolver=mm)
    assert not any("nối tiếp" in e for e in errs), errs
    strict = validate_export(
        project,
        ffmpeg_path=ffmpeg_bin,
        output_path=str(out_p),
        media_resolver=mm,
        require_contiguous_video_timeline=True,
    )
    assert any("nối tiếp" in e for e in strict), strict


def _probe_duration_sec(mm: MediaManager, path: Path) -> float:
    info = mm.probe_video(str(path))
    return float(info.get("duration") or 0)


def test_export_duration_respects_timeline_not_inflated_source_end(tmp_path: Path) -> None:
    """Cắt timeline 1s nhưng source_end = cả file — xuất không kéo dài theo metadata."""
    ffmpeg_bin = _require_ffmpeg()
    src = tmp_path / "long_src.mp4"
    _synthetic_mp4(ffmpeg_bin, src)

    paths = _ve_paths(tmp_path / "ve_trim")
    pm = VideoEditorProjectManager(paths=paths)
    mm = MediaManager(paths=paths)
    project = pm.create_project("trim-e2e", width=320, height=240, fps=30)
    media = mm.import_media(str(src), "video", copy_to_library=True)
    project.setdefault("media", []).append(media)
    mid = str(media["id"])
    probed = float(media.get("duration") or 2.5)

    tm = TimelineManager(project_manager=pm)
    tm.add_clip(project, mid, "video", persist=True)
    project = pm.load_project(str(project["id"]))
    vclip = next(
        c
        for t in project["tracks"]
        if isinstance(t, dict) and t.get("type") == "video"
        for c in t.get("clips") or []
        if isinstance(c, dict) and c.get("type") == "video"
    )
    vclip["duration"] = 1.0
    vclip["source_start"] = 0.0
    vclip["source_end"] = probed
    ss, se = effective_source_span(vclip, media_duration=probed)
    vclip["source_start"] = ss
    vclip["source_end"] = se
    pm.save_project(project)

    out_p = paths["renders"] / "trim_export.mp4"
    errs = validate_export(project, ffmpeg_path=ffmpeg_bin, output_path=str(out_p), media_resolver=mm)
    assert errs == [], errs

    cmd = FFmpegCommandBuilder().build_export_command(
        project,
        str(out_p),
        ffmpeg_bin=ffmpeg_bin,
        output_duration_limit_sec=None,
        lightweight_mode_override=True,
    )
    worker = RenderWorker()
    result = worker.render(project, str(out_p), cmd, duration_sec=2.0)
    assert result.get("ok") is True, result.get("error_message", result)
    assert out_p.is_file()

    out_dur = _probe_duration_sec(mm, out_p)
    assert 0.75 <= out_dur <= 1.35, f"Kỳ vọng ~1s, ffprobe={out_dur:.3f}"


def test_export_with_logo_overlay(tmp_path: Path) -> None:
    """Project có overlay PNG — validate + xuất MP4 (luồng logo)."""
    ffmpeg_bin = _require_ffmpeg()
    src = tmp_path / "src.mp4"
    logo = tmp_path / "logo.png"
    _synthetic_mp4(ffmpeg_bin, src)
    subprocess.run(
        [
            ffmpeg_bin,
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=c=blue@0.6:s=48x48:d=1",
            "-frames:v",
            "1",
            str(logo),
        ],
        capture_output=True,
        timeout=60,
        check=True,
    )

    paths = _ve_paths(tmp_path / "ve_logo")
    pm = VideoEditorProjectManager(paths=paths)
    mm = MediaManager(paths=paths)
    project = pm.create_project("logo-e2e", width=320, height=240, fps=30)
    v_media = mm.import_media(str(src), "video", copy_to_library=True)
    i_media = mm.import_media(str(logo), "image", copy_to_library=True)
    project.setdefault("media", []).extend([v_media, i_media])
    mid_v, mid_i = str(v_media["id"]), str(i_media["id"])

    tm = TimelineManager(project_manager=pm)
    tm.add_clip(project, mid_v, "video", persist=True)
    project = pm.load_project(str(project["id"]))
    otrack = next(t for t in project["tracks"] if isinstance(t, dict) and t.get("type") == "overlay")
    otrack.setdefault("clips", []).append(
        {
            "id": "ov_logo",
            "media_id": mid_i,
            "timeline_start": 0.0,
            "duration": 1.0,
            "x": 8,
            "y": 8,
            "width": 48,
            "height": 48,
            "opacity": 0.9,
        }
    )
    pm.save_project(project)

    out_p = paths["renders"] / "logo_export.mp4"
    errs = validate_export(project, ffmpeg_path=ffmpeg_bin, output_path=str(out_p), media_resolver=mm)
    assert errs == [], errs

    cmd = FFmpegCommandBuilder().build_export_command(
        project,
        str(out_p),
        ffmpeg_bin=ffmpeg_bin,
        output_duration_limit_sec=1.0,
        lightweight_mode_override=True,
    )
    fc_idx = cmd.index("-filter_complex")
    fc = cmd[fc_idx + 1]
    assert "overlay" in fc.lower()

    worker = RenderWorker()
    result = worker.render(project, str(out_p), cmd, duration_sec=1.2)
    assert result.get("ok") is True, result.get("error_message", result)
    assert out_p.is_file() and out_p.stat().st_size > 500
