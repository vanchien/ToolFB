"""Regression: overlay= phải dùng x=/y= — tránh FFmpeg «No such filter: ''»."""

from __future__ import annotations

from src.services.video_editor.ffmpeg_builder import _join_vfilters, _overlay_filter_params


def test_overlay_filter_params_static_coords() -> None:
    p = _overlay_filter_params(ts=1.0, te=5.0, x="24", y="48")
    assert p.startswith("x=24:y=48:")
    assert "format=auto" in p
    assert "gte(t,1.0)*lte(t,5.0)" in p


def test_overlay_filter_params_expr_coords() -> None:
    p = _overlay_filter_params(ts=0, te=10, x="max(0,main_w-overlay_w)", y="10")
    assert "x=max(0,main_w-overlay_w)" in p
    assert "y=10" in p


def test_join_vfilters_skips_empty_segments() -> None:
    s = _join_vfilters(["[0:v]scale=2:2", "", "format=rgba"], out_label="ov0")
    assert ",," not in s
    assert s == "[0:v]scale=2:2,format=rgba[ov0]"
    assert ",[ov0]" not in s


def test_join_vfilters_no_comma_before_output_pad() -> None:
    s = _join_vfilters(
        [f"[0:v]trim=start=0:end=1,setpts=PTS-STARTPTS"],
        out_label="pre0",
    )
    assert s.endswith("[pre0]")
    assert ",[pre0]" not in s
