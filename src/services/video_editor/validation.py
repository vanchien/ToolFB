"""Kiểm tra project trước khi export."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from src.services.video_editor.media_manager import MediaManager


def _ffmpeg_executable_ok(ffmpeg_path: str | None) -> bool:
    if not str(ffmpeg_path or "").strip():
        return False
    p = Path(str(ffmpeg_path).strip())
    if p.is_file():
        return True
    w = shutil.which(str(ffmpeg_path).strip())
    return bool(w and Path(w).is_file())


def validate_export(
    project: dict[str, Any],
    *,
    ffmpeg_path: str | None,
    output_path: str,
    media_resolver: MediaManager | None = None,
) -> list[str]:
    """
    Trả về danh sách lỗi (rỗng nếu OK).
    Thông điệp tiếng Việt.
    """
    errors: list[str] = []
    mr = media_resolver or MediaManager()

    if not _ffmpeg_executable_ok(ffmpeg_path):
        errors.append("ffmpeg: Không tìm thấy hoặc không chạy được ffmpeg (PATH hoặc tools/ffmpeg/bin).")

    out = Path(output_path).expanduser()
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        # kiểm tra ghi được thư mục
        test = out.parent / ".video_editor_write_test"
        test.write_text("ok", encoding="utf-8")
        test.unlink(missing_ok=True)
    except OSError as e:
        errors.append(f"output_path: Không ghi được thư mục đích ({e}).")

    tracks = project.get("tracks") or []
    video_clips: list[dict[str, Any]] = []
    for tr in tracks:
        if isinstance(tr, dict) and str(tr.get("type") or "") == "video":
            for cl in tr.get("clips") or []:
                if isinstance(cl, dict) and str(cl.get("type") or "") == "video":
                    video_clips.append(cl)

    if not video_clips:
        errors.append("project: Cần ít nhất một clip video trên track video.")

    media_list = project.get("media") or []
    media_by_id = {str(m.get("id")): m for m in media_list if isinstance(m, dict) and m.get("id")}

    ordered = sorted(video_clips, key=lambda c: float(c.get("timeline_start") or 0))

    if ordered:
        first_ts = float(ordered[0].get("timeline_start") or 0)
        if abs(first_ts) > 1e-6:
            errors.append("timeline: Clip video đầu tiên phải có timeline_start = 0 (MVP).")

        for i in range(len(ordered) - 1):
            a, b = ordered[i], ordered[i + 1]
            ta = float(a.get("timeline_start") or 0)
            da = float(a.get("duration") or 0)
            tb = float(b.get("timeline_start") or 0)
            exp = ta + da
            if abs(tb - exp) > 0.05:
                errors.append(
                    "timeline: Các clip video phải nối tiếp không khoảng trống (MVP). "
                    f"Sau clip {a.get('id')} kỳ vọng timeline_start={exp:.3f}, nhận {tb:.3f}."
                )

    for cl in ordered:
        mid = str(cl.get("media_id") or "")
        media = media_by_id.get(mid)
        if not media:
            errors.append(f"clip {cl.get('id')}: Không tìm thấy media_id={mid} trong project.")
            continue
        pth = mr.resolve_media_path_on_disk(media)
        if not pth or not pth.is_file():
            errors.append(f"clip {cl.get('id')}: File media không tồn tại trên đĩa.")
            continue
        ss = float(cl.get("source_start") or 0)
        se = float(cl.get("source_end") or 0)
        if ss >= se:
            errors.append(f"clip {cl.get('id')}: source_start phải nhỏ hơn source_end.")
        du = float(cl.get("duration") or 0)
        if du <= 0:
            errors.append(f"clip {cl.get('id')}: duration phải > 0.")

    return errors
