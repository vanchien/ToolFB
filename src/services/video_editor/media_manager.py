"""Import media và ffprobe."""

from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Any

from src.services.video_editor.layout import ensure_video_editor_layout
from src.utils.ffmpeg_paths import resolve_ffmpeg_ffprobe_paths


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class MediaManager:
    """Import media và đọc metadata."""

    def __init__(self, *, paths: dict[str, Path] | None = None) -> None:
        self._paths = paths or ensure_video_editor_layout()

    def resolve_media_path_on_disk(self, media: dict[str, Any]) -> Path | None:
        lp = str(media.get("local_path") or "").strip()
        op = str(media.get("path") or "").strip()
        for candidate in (lp, op):
            if not candidate:
                continue
            p = Path(candidate).expanduser()
            if p.is_file():
                return p.resolve()
        return None

    def probe_video(self, file_path: str) -> dict[str, Any]:
        _, ffprobe = resolve_ffmpeg_ffprobe_paths()
        if not ffprobe:
            raise RuntimeError("Không tìm thấy ffprobe (PATH hoặc tools/ffmpeg/bin).")
        p = Path(file_path).expanduser()
        if not p.is_file():
            raise FileNotFoundError(f"File không tồn tại: {p}")
        cmd = [
            ffprobe,
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(p.resolve()),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
        if proc.returncode != 0:
            raise RuntimeError(f"ffprobe lỗi: {(proc.stderr or proc.stdout or '').strip()[-800:]}")
        try:
            data = json.loads(proc.stdout or "{}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"ffprobe trả JSON không đọc được: {e}") from e

        duration = float(data.get("format", {}).get("duration") or 0)
        width, height, fps = 0, 0, 30.0
        has_audio = False
        for st in data.get("streams") or []:
            if st.get("codec_type") == "video" and not width:
                width = int(st.get("width") or 0)
                height = int(st.get("height") or 0)
                afr = st.get("avg_frame_rate") or ""
                if isinstance(afr, str) and "/" in afr:
                    num, den = afr.split("/", 1)
                    try:
                        n, d = float(num), float(den)
                        if d:
                            fps = n / d
                    except ValueError:
                        pass
                dur_v = st.get("duration")
                if dur_v:
                    try:
                        duration = max(duration, float(dur_v))
                    except ValueError:
                        pass
            if st.get("codec_type") == "audio":
                has_audio = True

        return {
            "duration": duration,
            "width": width,
            "height": height,
            "fps": round(fps, 4) if fps else 30.0,
            "has_audio": has_audio,
        }

    def import_media(self, file_path: str, media_type: str, copy_to_library: bool = True) -> dict[str, Any]:
        src = Path(file_path).expanduser()
        if not src.is_file():
            raise FileNotFoundError("File không tồn tại.")

        mt = str(media_type or "").strip().lower()
        if mt not in ("video", "image", "audio"):
            raise ValueError("media_type phải là video, image hoặc audio.")

        mid = f"media_{uuid.uuid4().hex[:10]}"
        original_name = src.name
        record: dict[str, Any] = {
            "id": mid,
            "type": mt,
            "path": str(src.resolve()),
            "local_path": "",
            "original_name": original_name,
            "duration": 0.0,
            "width": 0,
            "height": 0,
            "fps": 30,
            "has_audio": False,
            "created_at": _now_iso(),
        }

        if mt == "video":
            meta = self.probe_video(str(src))
            record.update(meta)
        elif mt == "image":
            record["duration"] = 0.0
            record["width"] = 0
            record["height"] = 0
            record["fps"] = 30
            record["has_audio"] = False
        else:
            meta = self.probe_video(str(src))
            record["duration"] = meta.get("duration") or 0.0
            record["has_audio"] = True

        dest: Path | None = None
        if copy_to_library:
            self._paths["media"].mkdir(parents=True, exist_ok=True)
            ext = src.suffix.lower() or (".mp4" if mt == "video" else ".bin")
            dest = self._paths["media"] / f"{mid}{ext}"
            try:
                shutil.copy2(src, dest)
            except OSError as e:
                raise RuntimeError(f"Không copy được vào thư viện media: {e}") from e
            record["local_path"] = str(dest.resolve())
        else:
            record["local_path"] = ""

        if mt == "image" and dest and dest.is_file():
            try:
                probe_img = self.probe_video(str(dest))
                record["width"] = int(probe_img.get("width") or 0)
                record["height"] = int(probe_img.get("height") or 0)
            except Exception:
                pass

        return record

    def create_thumbnail(self, video_path: str, output_path: str) -> str:
        ffmpeg, _ = resolve_ffmpeg_ffprobe_paths()
        if not ffmpeg:
            raise RuntimeError("Không tìm thấy ffmpeg.")
        vp = Path(video_path).expanduser()
        out = Path(output_path).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            ffmpeg,
            "-y",
            "-ss",
            "0.3",
            "-i",
            str(vp.resolve()),
            "-frames:v",
            "1",
            "-q:v",
            "3",
            str(out.resolve()),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
        if proc.returncode != 0:
            raise RuntimeError(f"Không tạo được thumbnail: {(proc.stderr or '')[-600:]}")
        return str(out.resolve())

    def generate_proxy(
        self,
        media: dict[str, Any],
        *,
        ffmpeg_bin: str,
        height: int = 720,
    ) -> str:
        """Tạo proxy preview nhẹ; ghi `proxy_path` vào media dict."""
        vp = self.resolve_media_path_on_disk(media)
        if not vp:
            raise FileNotFoundError("Không tìm thấy file media.")
        mid = str(media.get("id") or "media")
        safe = "".join(c for c in mid if c.isalnum() or c in "-_")[:48]
        out = self._paths["temp"] / f"proxy_{safe}.mp4"
        out.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            str(Path(ffmpeg_bin).resolve()),
            "-y",
            "-i",
            str(vp),
            "-vf",
            f"scale=-2:{int(height)}",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-crf",
            "28",
            "-c:a",
            "aac",
            "-movflags",
            "+faststart",
            str(out.resolve()),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=600)
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or "")[-1200:])
        media["proxy_path"] = str(out.resolve())
        return str(out.resolve())
