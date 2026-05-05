"""Render FFmpeg trong thread; parse progress từ stderr."""

from __future__ import annotations

import os
import re
import subprocess
import threading
from pathlib import Path
from typing import Any, Callable

from src.services.video_editor.layout import ensure_video_editor_layout


ProgressCb = Callable[[float], None]


class RenderWorker:
    """Render video bằng FFmpeg trong thread riêng."""

    _OUT_TIME_MS_RE = re.compile(r"out_time_ms=(\d+)")
    _TIME_EQ_RE = re.compile(r"time=(\d+):(\d+):(\d+(?:\.\d+)?)")

    def render(
        self,
        project: dict[str, Any],
        output_path: str,
        command: list[str],
        *,
        duration_sec: float,
        progress_callback: ProgressCb | None = None,
        log_path: Path | None = None,
    ) -> dict[str, Any]:
        """
        Chạy FFmpeg export MP4.
        `command` là argv đầy đủ (ffmpeg đầu tiên).
        """
        out: dict[str, Any] = {"ok": False, "error_message": "", "log_file": ""}
        lp = log_path
        if lp is None:
            logs = ensure_video_editor_layout()["logs"]
            pid = str(project.get("id") or "export")
            safe = "".join(c for c in pid if c.isalnum() or c in "-_")[:64]
            lp = logs / f"render_{safe}.log"

        lp.parent.mkdir(parents=True, exist_ok=True)
        lines: list[str] = []

        popen_kw: dict = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
        }
        if os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW"):
            popen_kw["creationflags"] = subprocess.CREATE_NO_WINDOW
        try:
            proc = subprocess.Popen(command, **popen_kw)
        except OSError as e:
            out["error_message"] = f"Không chạy được FFmpeg: {e}"
            return out

        assert proc.stderr is not None
        total_us = max(1.0, float(duration_sec) * 1_000_000.0)

        for line in proc.stderr:
            lines.append(line)
            if progress_callback and duration_sec > 0:
                prog = self.parse_progress(line, total_duration_us=total_us)
                if prog is not None:
                    try:
                        progress_callback(prog)
                    except Exception:
                        pass

        proc.wait()
        body = "".join(lines)
        try:
            lp.write_text(body[-400_000:], encoding="utf-8", errors="replace")
        except OSError:
            pass
        out["log_file"] = str(lp)

        if proc.returncode != 0:
            tail = body.strip()[-1200:] if body.strip() else "Không có stderr."
            out["error_message"] = f"FFmpeg lỗi (mã {proc.returncode}). Chi tiết:\n{tail}"
            return out

        outp = Path(output_path).expanduser()
        if not outp.is_file():
            out["error_message"] = f"Export xong nhưng không thấy file: {outp}"
            return out

        out["ok"] = True
        return out

    def render_thread(
        self,
        project: dict[str, Any],
        output_path: str,
        command: list[str],
        *,
        duration_sec: float,
        progress_callback: ProgressCb | None = None,
        done_callback: Callable[[dict[str, Any]], None] | None = None,
        log_path: Path | None = None,
    ) -> None:
        """Chạy render trong thread nền; gọi done_callback khi xong (ở thread đó)."""

        def _run() -> None:
            result = self.render(
                project,
                output_path,
                command,
                duration_sec=duration_sec,
                progress_callback=progress_callback,
                log_path=log_path,
            )
            if done_callback:
                done_callback(result)

        threading.Thread(target=_run, daemon=True).start()

    def parse_progress(self, ffmpeg_line: str, *, total_duration_us: float) -> float | None:
        """
        Parse progress từ stderr FFmpeg (out_time_ms hoặc time=HH:MM:SS.xx).
        Trả về 0..1 hoặc None.
        """
        line = ffmpeg_line or ""
        m = self._OUT_TIME_MS_RE.search(line)
        if m:
            try:
                us = float(m.group(1))
            except ValueError:
                return None
            if total_duration_us <= 0:
                return None
            return max(0.0, min(1.0, us / total_duration_us))
        m2 = self._TIME_EQ_RE.search(line)
        if m2:
            try:
                hh, mm, ss = float(m2.group(1)), float(m2.group(2)), float(m2.group(3))
                sec = hh * 3600 + mm * 60 + ss
                us = sec * 1_000_000.0
            except ValueError:
                return None
            if total_duration_us <= 0:
                return None
            return max(0.0, min(1.0, us / total_duration_us))
        return None
