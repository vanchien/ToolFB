"""Kiểm tra toàn bộ filter_complex — tránh lỗi FFmpeg «No such filter: ''»."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest

from src.services.video_editor.ffmpeg_builder import (
    FFmpegCommandBuilder,
    _build_still_image_overlay_chain,
    _build_video_overlay_chain,
    _join_vfilters,
    _validate_filter_complex_graph,
)
from src.utils.ffmpeg_paths import resolve_ffmpeg_executable


def test_validate_rejects_comma_before_output_pad() -> None:
    with pytest.raises(ValueError, match="phẩy trước pad"):
        _validate_filter_complex_graph(["[0:v]trim=start=0:end=1,setpts=PTS-STARTPTS,[pre0]"])


def test_validate_rejects_double_comma() -> None:
    with pytest.raises(ValueError, match="filter rỗng"):
        _validate_filter_complex_graph(["[0:v]scale=2:2,,format=yuv420p[v0]"])


def test_overlay_chains_end_with_pad_not_comma_pad() -> None:
    for chain in (
        _build_still_image_overlay_chain(
            1, ow=180, oh=180, fps=30, opacity=0.8, extra_vf="fade=t=in:st=0:d=0.5:alpha=1", out_label="ov0"
        ),
        _build_video_overlay_chain(2, ow=100, oh=100, fps=30, opacity=1.0, extra_vf="", out_label="ov1"),
    ):
        _validate_filter_complex_graph([chain])
        assert ",[ov" not in chain


def test_builder_validates_all_local_projects() -> None:
    root = Path("data/video_editor/projects")
    if not root.is_dir():
        pytest.skip("Không có thư mục projects.")
    ff = resolve_ffmpeg_executable()
    if not ff:
        pytest.skip("Không có ffmpeg.")
    builder = FFmpegCommandBuilder()
    checked = 0
    for p in sorted(root.glob("*.json"), key=lambda x: -x.stat().st_mtime)[:12]:
        try:
            proj = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        try:
            cmd = builder.build_export_command(
                proj,
                str(root.parent / "temp" / "_fc_test.mp4"),
                ffmpeg_bin=ff,
                output_duration_limit_sec=3.0,
                lightweight_mode_override=True,
            )
        except ValueError as e:
            if "Thiếu media" in str(e) or "Không resolve" in str(e) or "Không có đoạn video" in str(e):
                continue
            raise
        fc = cmd[cmd.index("-filter_complex") + 1]
        assert not re.search(r",\[[A-Za-z_][A-Za-z0-9_]*\](?:;|$)", fc)
        checked += 1
    if checked == 0:
        pytest.skip("Không có project hợp lệ để kiểm tra filter_complex.")


def test_ffmpeg_null_mux_synthetic_project(tmp_path: Path) -> None:
    """Chạy FFmpeg null mux với project tổng hợp (video + overlay + text + audio timeline)."""
    ff = resolve_ffmpeg_executable()
    if not ff:
        pytest.skip("Không có ffmpeg.")
    src = tmp_path / "src.mp4"
    png = tmp_path / "logo.png"
    mp3 = tmp_path / "bg.mp3"
    _make_test_mp4(ff, src)
    _make_test_png(ff, png)
    _make_test_mp3(ff, mp3)

    proj = _synthetic_full_project(src, png, mp3)
    out = tmp_path / "out.mp4"
    cmd = FFmpegCommandBuilder().build_export_command(
        proj,
        str(out),
        ffmpeg_bin=ff,
        output_duration_limit_sec=2.0,
        lightweight_mode_override=True,
    )
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=180)
    assert r.returncode == 0, (r.stderr or r.stdout or "")[-2500:]
    assert out.is_file() and out.stat().st_size > 500


def _make_test_mp4(ff: str, dest: Path) -> None:
    cmd = [
        ff,
        "-y",
        "-f",
        "lavfi",
        "-i",
        "testsrc=duration=2:size=320x240:rate=30",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:duration=2",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-shortest",
        str(dest),
    ]
    assert subprocess.run(cmd, capture_output=True, timeout=120).returncode == 0


def _make_test_png(ff: str, dest: Path) -> None:
    cmd = [
        ff,
        "-y",
        "-f",
        "lavfi",
        "-i",
        "color=c=red@0.5:s=64x64:d=1",
        "-frames:v",
        "1",
        str(dest),
    ]
    assert subprocess.run(cmd, capture_output=True, timeout=60).returncode == 0


def _make_test_mp3(ff: str, dest: Path) -> None:
    cmd = [
        ff,
        "-y",
        "-f",
        "lavfi",
        "-i",
        "sine=duration=2",
        "-c:a",
        "libmp3lame",
        str(dest),
    ]
    assert subprocess.run(cmd, capture_output=True, timeout=60).returncode == 0


def _synthetic_full_project(video: Path, logo: Path, audio: Path) -> dict[str, Any]:
    mid_v, mid_i, mid_a = "mv1", "mi1", "ma1"
    cid_v, cid_o, cid_t, cid_a = "cv1", "co1", "ct1", "ca1"
    return {
        "id": "fc_test",
        "width": 320,
        "height": 240,
        "fps": 30,
        "duration": 2.0,
        "features": {
            "animation": True,
            "color_filters": True,
            "bgm": False,
            "timeline_audio": True,
            "transitions": False,
        },
        "media": [
            {"id": mid_v, "type": "video", "local_path": str(video.resolve()), "has_audio": True},
            {"id": mid_i, "type": "image", "local_path": str(logo.resolve())},
            {"id": mid_a, "type": "audio", "local_path": str(audio.resolve())},
        ],
        "tracks": [
            {
                "type": "video",
                "clips": [
                    {
                        "id": cid_v,
                        "type": "video",
                        "media_id": mid_v,
                        "timeline_start": 0,
                        "duration": 2.0,
                        "source_start": 0,
                        "source_end": 2.0,
                        "speed": 1.0,
                        "canvas_mode": "fit",
                        "opacity": 1.0,
                    }
                ],
            },
            {
                "type": "overlay",
                "clips": [
                    {
                        "id": cid_o,
                        "media_id": mid_i,
                        "timeline_start": 0,
                        "duration": 2.0,
                        "x": 10,
                        "y": 10,
                        "width": 64,
                        "height": 64,
                        "opacity": 0.75,
                        "animation_preset": "fade_in",
                    }
                ],
            },
            {
                "type": "text",
                "clips": [
                    {
                        "id": cid_t,
                        "text": "Test",
                        "timeline_start": 0,
                        "duration": 1.5,
                        "x": 8,
                        "y": 8,
                        "font_size": 20,
                        "color": "white",
                    }
                ],
            },
            {
                "type": "audio",
                "clips": [
                    {
                        "id": cid_a,
                        "type": "audio",
                        "media_id": mid_a,
                        "timeline_start": 0,
                        "duration": 2.0,
                        "source_start": 0,
                        "source_end": 2.0,
                        "speed": 1.0,
                        "volume": 0.5,
                    }
                ],
            },
        ],
        "export": {"codec": "libx264", "preset": "ultrafast", "crf": 28},
    }
