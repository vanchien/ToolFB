"""Tests overlay_utils — resolve path, kích thước logo, validate."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.services.video_editor.overlay_utils import (
    compute_logo_overlay_dimensions,
    normalize_path_for_ffmpeg,
    resolve_media_file_path,
    validate_overlay_clips,
)


def test_compute_logo_overlay_dimensions_square_fallback():
    w, h = compute_logo_overlay_dimensions(None, canvas_w=1080, logo_ratio=0.15)
    assert w == max(80, int(1080 * 0.15))
    assert h == w


def test_compute_logo_overlay_dimensions_preserves_aspect():
    media = {"width": 400, "height": 200}
    w, h = compute_logo_overlay_dimensions(media, canvas_w=1000, logo_ratio=0.2)
    assert w == max(80, 200)
    assert h == max(2, int(round(w * 200 / 400)))


def test_resolve_media_file_path_prefers_local_path(tmp_path: Path):
    real = tmp_path / "logo.png"
    real.write_bytes(b"\x89PNG\r\n")
    media = {"local_path": str(real), "path": str(tmp_path / "missing.png")}
    got = resolve_media_file_path(media)
    assert got is not None
    assert got.name == "logo.png"


def test_validate_overlay_clips_missing_file():
    project = {
        "media": [{"id": "m1", "type": "image", "path": "/no/such/file.png"}],
        "tracks": [
            {
                "type": "overlay",
                "clips": [
                    {
                        "id": "c1",
                        "type": "image",
                        "media_id": "m1",
                        "width": 100,
                        "height": 50,
                    }
                ],
            }
        ],
    }
    errs = validate_overlay_clips(project)
    assert errs
    assert "c1" in errs[0]


@pytest.mark.skipif(os.name != "nt", reason="long-path prefix is Windows-only")
def test_normalize_path_for_ffmpeg_long_windows_path(monkeypatch):
    monkeypatch.delenv("TOOLFB_FFMPEG_NO_LONGPATH", raising=False)
    long_rel = "a" * 250 + ".png"
    p = Path("C:/") / long_rel
    out = normalize_path_for_ffmpeg(p)
    assert out.startswith("\\\\?\\")
