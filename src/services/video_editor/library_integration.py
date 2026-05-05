"""Đưa video export vào thư viện downloaded_videos.json."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.services.universal_video_downloader import DownloadMetadataStore, ensure_downloader_layout


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def add_editor_export_to_library(
    *,
    project_id: str,
    output_video_path: str,
    title: str,
    duration_sec: float = 0.0,
) -> dict[str, Any]:
    """
    Tạo record tương thích Video Library (AI Video / downloader).
    Không tạo job đăng Facebook.
    """
    vp = Path(output_video_path).expanduser().resolve()
    vid = f"edited_{uuid.uuid4().hex[:10]}"
    record: dict[str, Any] = {
        "id": vid,
        "download_job_id": "",
        "platform": "video_editor",
        "source_url": "",
        "source": "video_editor",
        "project_id": str(project_id),
        "title": str(title or vp.stem),
        "uploader": "Video Editor",
        "duration": float(duration_sec),
        "upload_date": "",
        "video_path": str(vp),
        "thumbnail_path": "",
        "info_json_path": "",
        "status": "downloaded",
        "ready_for_analysis": True,
        "created_at": _now_iso(),
    }
    store = DownloadMetadataStore(paths=ensure_downloader_layout())
    store.save_downloaded_video(record)
    return record
