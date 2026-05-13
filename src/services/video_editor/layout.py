"""Đảm bảo thư mục data/video_editor/* tồn tại."""

from __future__ import annotations

from pathlib import Path

from src.utils.paths import project_root


def ensure_video_editor_layout() -> dict[str, Path]:
    root = project_root() / "data" / "video_editor"
    paths = {
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
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths


_SCHEDULE_JOBS_JSON = "video_editor_schedule_jobs.json"


def video_editor_schedule_jobs_json_path() -> Path:
    """
    Đường dẫn tuyệt đối tới ``video_editor_schedule_jobs.json``.

    - Ưu tiên ``data/video_editor/`` (cùng cây dữ liệu Video Editor).
    - Nếu chỉ tồn tại bản cũ trong ``data/downloader/`` thì dùng đó (tương thích ngược).
    - Nếu cả hai đều có: dùng file được sửa gần đây hơn (mtime).
    - Chưa có file: trả về đường trong ``data/video_editor/`` (lần lưu đầu tạo ở đây).
    """
    ve_root = ensure_video_editor_layout()["root"]
    dl_root = project_root() / "data" / "downloader"
    p_ve = ve_root / _SCHEDULE_JOBS_JSON
    p_dl = dl_root / _SCHEDULE_JOBS_JSON
    ve_ok = p_ve.is_file()
    dl_ok = p_dl.is_file()
    if ve_ok and dl_ok:
        try:
            if p_ve.stat().st_mtime >= p_dl.stat().st_mtime:
                return p_ve.resolve()
            return p_dl.resolve()
        except OSError:
            return p_ve.resolve()
    if ve_ok:
        return p_ve.resolve()
    if dl_ok:
        return p_dl.resolve()
    return p_ve.resolve()
