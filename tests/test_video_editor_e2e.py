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
from src.services.video_editor.timeline_manager import (
    TimelineManager,
    effective_source_span,
    iter_video_clips,
    sync_overlapping_audio_clips_to_video,
    video_timeline_clips_overlap,
)
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


def _synthetic_mp4(ffmpeg: str, dest: Path, *, duration: float = 2.5) -> None:
    cmd = [
        ffmpeg,
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"testsrc=duration={duration}:size=320x240:rate=30",
        "-f",
        "lavfi",
        "-i",
        f"sine=frequency=440:sample_rate=44100:duration={duration}",
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


def _synthetic_logo_png(ffmpeg: str, dest: Path) -> None:
    subprocess.run(
        [
            ffmpeg,
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=c=red@0.7:s=40x40:d=1",
            "-frames:v",
            "1",
            str(dest),
        ],
        capture_output=True,
        timeout=60,
        check=True,
    )


def _first_video_clip(project: dict[str, Any]) -> dict[str, Any]:
    clips = iter_video_clips(project)
    assert clips, "Cần ít nhất một clip video."
    return clips[0]


def _attach_overlay_logo(
    project: dict[str, Any],
    tm: TimelineManager,
    *,
    logo_mid: str,
    timeline_start: float,
    duration: float,
) -> None:
    """Mô phỏng «Chỉnh clip» gắn logo theo khung clip video."""
    buf: list[dict[str, Any]] = []
    tm.add_clip(
        project,
        logo_mid,
        "overlay",
        persist=False,
        recompute_duration=False,
        out_new_clip=buf,
    )
    assert buf, "Không tạo được clip overlay logo."
    oc = buf[0]
    oc["timeline_start"] = float(timeline_start)
    oc["duration"] = max(0.1, float(duration))
    oc["x"] = 6
    oc["y"] = 6
    oc["width"] = 40
    oc["height"] = 40
    oc["opacity"] = 0.88


def _render_project(
    project: dict[str, Any],
    *,
    mm: MediaManager,
    pm: VideoEditorProjectManager,
    ffmpeg_bin: str,
    out_p: Path,
    duration_hint: float,
    limit_sec: float | None = None,
) -> None:
    errs = validate_export(
        project,
        ffmpeg_path=ffmpeg_bin,
        output_path=str(out_p),
        media_resolver=mm,
        require_contiguous_video_timeline=True,
    )
    assert errs == [], errs
    cmd = FFmpegCommandBuilder().build_export_command(
        project,
        str(out_p),
        ffmpeg_bin=ffmpeg_bin,
        output_duration_limit_sec=limit_sec,
        lightweight_mode_override=True,
    )
    assert cmd
    worker = RenderWorker()
    cap = max(0.5, float(duration_hint) + 1.5)
    if limit_sec is not None:
        cap = min(cap, float(limit_sec) + 1.5)
    result = worker.render(project, str(out_p), cmd, duration_sec=cap)
    assert result.get("ok") is True, result.get("error_message", result)
    assert out_p.is_file() and out_p.stat().st_size > 400


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


def test_e2e_single_video_chinh_clip_trim_zoom_text_logo(tmp_path: Path) -> None:
    """
    E2E 1 video: cắt nguồn → zoom → tốc độ → logo + chữ → xuất MP4.
    Tương đương tab «Chỉnh clip» + «Áp dụng» cho một clip.
    """
    ffmpeg_bin = _require_ffmpeg()
    src = tmp_path / "one.mp4"
    logo = tmp_path / "logo.png"
    _synthetic_mp4(ffmpeg_bin, src, duration=2.2)
    _synthetic_logo_png(ffmpeg_bin, logo)

    paths = _ve_paths(tmp_path / "ve_one_edit")
    pm = VideoEditorProjectManager(paths=paths)
    mm = MediaManager(paths=paths)
    project = pm.create_project("one-edit", width=320, height=240, fps=24)
    v_media = mm.import_media(str(src), "video", copy_to_library=True)
    i_media = mm.import_media(str(logo), "image", copy_to_library=True)
    project.setdefault("media", []).extend([v_media, i_media])
    mid_v, mid_i = str(v_media["id"]), str(i_media["id"])

    tm = TimelineManager(project_manager=pm)
    tm.add_clip(project, mid_v, "video", persist=False, recompute_duration=False)
    vclip = _first_video_clip(project)
    cid = str(vclip["id"])
    probed = float(v_media.get("duration") or 2.2)

    ss0 = 0.25
    se0 = min(probed, 1.35)
    tm.trim_clip(project, cid, ss0, se0, persist=False, recompute_duration=False)
    tm.set_speed(project, cid, 1.1, persist=False, recompute_duration=False)
    tm.update_clip(
        project,
        cid,
        {"zoom": 1.2, "brightness": 0.08, "canvas_mode": "fit"},
        persist=False,
        recompute_duration=False,
    )
    vclip = _first_video_clip(project)
    ts = float(vclip.get("timeline_start") or 0)
    du = float(vclip.get("duration") or 0)
    assert 0.85 <= du <= 1.15, f"duration timeline sau cắt+tốc độ: {du}"

    _attach_overlay_logo(project, tm, logo_mid=mid_i, timeline_start=ts, duration=du)
    tm.add_text_clip(project, "E2E-1", timeline_start=ts, duration=du, persist=False, recompute_duration=False)
    sync_overlapping_audio_clips_to_video(project, cid, speed=float(vclip.get("speed") or 1.0))
    tm.refresh_project_duration(project)
    pm.save_project(project)

    out_p = paths["renders"] / "one_edit.mp4"
    cmd = FFmpegCommandBuilder().build_export_command(
        project,
        str(out_p),
        ffmpeg_bin=ffmpeg_bin,
        output_duration_limit_sec=None,
        lightweight_mode_override=True,
    )
    fc = cmd[cmd.index("-filter_complex") + 1]
    assert "overlay" in fc.lower()
    assert "drawtext" in fc.lower()

    _render_project(
        project,
        mm=mm,
        pm=pm,
        ffmpeg_bin=ffmpeg_bin,
        out_p=out_p,
        duration_hint=du,
    )
    out_dur = _probe_duration_sec(mm, out_p)
    assert 0.75 <= out_dur <= 1.35, f"Xuất 1 clip kỳ vọng ~1s, ffprobe={out_dur:.3f}"


def test_e2e_multi_video_per_clip_export_not_concat(tmp_path: Path) -> None:
    """
    E2E nhiều video: mỗi clip T=0 độc lập → chỉnh riêng → xuất 2 file MP4 (ID/clip riêng), không concat.
    """
    ffmpeg_bin = _require_ffmpeg()
    logo = tmp_path / "logo.png"
    _synthetic_logo_png(ffmpeg_bin, logo)

    paths = _ve_paths(tmp_path / "ve_multi_edit")
    pm = VideoEditorProjectManager(paths=paths)
    mm = MediaManager(paths=paths)
    project = pm.create_project("multi-edit", width=320, height=240, fps=24)

    mids: list[str] = []
    for i in range(2):
        src = paths["media"] / f"v{i}.mp4"
        dur = 1.15 + i * 0.35
        _synthetic_mp4(ffmpeg_bin, src, duration=dur)
        media = mm.import_media(str(src), "video", copy_to_library=True)
        media["source_download_video_id"] = f"src_vid_{i}"
        project.setdefault("media", []).append(media)
        mids.append(str(media["id"]))

    i_media = mm.import_media(str(logo), "image", copy_to_library=True)
    project.setdefault("media", []).append(i_media)
    mid_i = str(i_media["id"])

    tm = TimelineManager(project_manager=pm)
    for mid in mids:
        tm.add_clip(project, mid, "video", persist=False, recompute_duration=False)
        cl = iter_video_clips(project)[-1]
        cl["timeline_start"] = 0.0

    trim_head = 0.15
    dur_by_cid: dict[str, float] = {}
    for cl in iter_video_clips(project):
        cid = str(cl["id"])
        ss = float(cl.get("source_start") or 0)
        se = float(cl.get("source_end") or 0)
        tm.trim_clip(project, cid, ss + trim_head, se, persist=False, recompute_duration=False)
        tm.update_clip(
            project,
            cid,
            {"zoom": 1.15, "canvas_mode": "fill"},
            persist=False,
            recompute_duration=False,
        )
        fc = _find_clip_in_project(project, cid)
        assert fc is not None
        assert float(fc.get("timeline_start") or 0) == 0.0
        du = float(fc.get("duration") or 0)
        dur_by_cid[cid] = du
        _attach_overlay_logo(project, tm, logo_mid=mid_i, timeline_start=0.0, duration=du)
        sync_overlapping_audio_clips_to_video(project, cid, speed=float(fc.get("speed") or 1.0))

    assert video_timeline_clips_overlap(project)
    tm.refresh_project_duration(project)
    pm.save_project(project)

    out_dir = paths["renders"] / "per_clip"
    out_dir.mkdir(parents=True, exist_ok=True)
    exported: list[tuple[str, Path]] = []
    rows = [(str(c.get("id") or ""), c) for c in iter_video_clips(project)]
    for idx, (cid, _cl) in enumerate(sorted(rows, key=lambda x: x[0]), start=1):
        bproj, _meta = _build_clip_only_project_for_test(project, str(cid))
        assert bproj is not None
        out_p = out_dir / f"clip_{idx}_{cid}.mp4"
        _render_project(
            bproj,
            mm=mm,
            pm=pm,
            ffmpeg_bin=ffmpeg_bin,
            out_p=out_p,
            duration_hint=dur_by_cid.get(cid, 1.0),
            limit_sec=None,
        )
        exported.append((cid, out_p))

    assert len(exported) == 2
    assert exported[0][1] != exported[1][1]
    for cid, out_p in exported:
        d = _probe_duration_sec(mm, out_p)
        exp = dur_by_cid[cid]
        assert exp * 0.72 <= d <= exp * 1.45, f"{cid}: kỳ vọng ~{exp:.2f}s, ffprobe={d:.3f}s"


def _build_clip_only_project_for_test(
    src_project: dict[str, Any], clip_id: str
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Bản rút gọn logic _build_clip_only_project trong video_editor_tab."""
    import copy

    tracks = src_project.get("tracks") or []
    src_video_clip: dict[str, Any] | None = None
    for tr in tracks:
        if not isinstance(tr, dict) or str(tr.get("type") or "") != "video":
            continue
        for cl in tr.get("clips") or []:
            if isinstance(cl, dict) and str(cl.get("id") or "") == clip_id:
                src_video_clip = cl
                break
        if src_video_clip is not None:
            break
    if src_video_clip is None:
        return None, {}
    ts = float(src_video_clip.get("timeline_start") or 0.0)
    dur = max(0.1, float(src_video_clip.get("duration") or 0.0))
    te = ts + dur
    out = copy.deepcopy(src_project)
    out_tracks: list[dict[str, Any]] = []
    for tr in out.get("tracks") or []:
        if not isinstance(tr, dict):
            continue
        tr_type = str(tr.get("type") or "")
        new_clips: list[dict[str, Any]] = []
        for cl in tr.get("clips") or []:
            if not isinstance(cl, dict):
                continue
            cs = float(cl.get("timeline_start") or 0.0)
            cd = max(0.0, float(cl.get("duration") or 0.0))
            ce = cs + cd
            ov_start = max(ts, cs)
            ov_end = min(te, ce)
            if ov_end <= ov_start:
                continue
            if tr_type == "video" and str(cl.get("id") or "") != clip_id:
                continue
            ncl = dict(cl)
            old_cs = cs
            ncl["timeline_start"] = max(0.0, ov_start - ts)
            ncl["duration"] = max(0.05, ov_end - ov_start)
            if "source_start" in ncl:
                try:
                    sp = float(cl.get("speed") or 1.0)
                    if sp <= 0:
                        sp = 1.0
                    ncl["source_start"] = float(cl.get("source_start") or 0.0) + max(0.0, ov_start - old_cs) * sp
                except Exception:
                    pass
            if "source_end" in ncl:
                try:
                    sp = float(cl.get("speed") or 1.0)
                    if sp <= 0:
                        sp = 1.0
                    ss1 = float(ncl.get("source_start") or 0.0)
                    ncl["source_end"] = ss1 + max(0.05, (ov_end - ov_start) * sp)
                except Exception:
                    pass
            new_clips.append(ncl)
        ntr = dict(tr)
        ntr["clips"] = new_clips
        out_tracks.append(ntr)
    out["tracks"] = out_tracks
    out["duration"] = dur
    return out, {}


def _find_clip_in_project(project: dict[str, Any], clip_id: str) -> dict[str, Any] | None:
    cid = str(clip_id)
    for tr in project.get("tracks") or []:
        if not isinstance(tr, dict):
            continue
        for cl in tr.get("clips") or []:
            if isinstance(cl, dict) and str(cl.get("id") or "") == cid:
                return cl
    return None
