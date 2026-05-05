from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

from src.utils.app_secrets import get_nanobanana_runtime_config
from src.utils.ffmpeg_paths import resolve_ffmpeg_ffprobe_paths
from src.utils.paths import project_root

LogFn = Callable[[str], None]
_profile_launch_locks: dict[str, threading.Lock] = {}
_profile_launch_guard = threading.Lock()


def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _safe_slug(s: str) -> str:
    out = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(s or "").strip())
    return out.strip("_") or "reverse_job"


def ensure_reverse_video_layout() -> dict[str, Path]:
    root = project_root() / "data" / "reverse_video"
    paths = {
        "root": root,
        "input": root / "input",
        "frames": root / "frames",
        "frame_sets": root / "frame_sets",
        "gemini_uploads": root / "gemini_uploads",
        "analysis": root / "analysis",
        "prompts": root / "prompts",
        "outputs": root / "outputs",
        "logs": root / "logs",
        "screenshots": root / "logs" / "screenshots",
        "jobs": root / "reverse_jobs.json",
    }
    for p in paths.values():
        if p.suffix:
            continue
        p.mkdir(parents=True, exist_ok=True)
    if not paths["jobs"].is_file():
        paths["jobs"].write_text("[]\n", encoding="utf-8")
    return paths


def _default_ytdlp_config() -> dict[str, Any]:
    return {
        "enabled": True,
        "use_exe": False,
        "exe_path": str(project_root() / "tools" / "yt-dlp" / ("yt-dlp.exe" if os.name == "nt" else "yt-dlp")),
        "timeout_sec": 300,
        "max_filesize_mb": 200,
        "output_dir": str(ensure_reverse_video_layout()["input"]),
        "proxy": "",
    }


def load_reverse_video_config() -> dict[str, Any]:
    cfg_path = project_root() / "config" / "reverse_video_config.json"
    base = {"yt_dlp": _default_ytdlp_config()}
    if not cfg_path.is_file():
        return base
    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        return base
    if not isinstance(raw, dict):
        return base
    y = dict(base["yt_dlp"])
    y.update(dict(raw.get("yt_dlp") or {}))
    return {"yt_dlp": y}


@dataclass
class ReverseVideoJob:
    id: str
    source_type: str = "url"
    source_url: str = ""
    local_video_path: str = ""
    target_platform: str = "Facebook Reels"
    output_language: str = "Vietnamese"
    duration_sec: int = 8
    aspect_ratio: str = "9:16"
    analysis_mode: str = "gemini_browser"
    keyframe_mode: str = "hybrid"
    max_frames: int = 20
    replacement: dict[str, Any] | None = None
    continuous_series: dict[str, Any] | None = None
    gemini_browser: dict[str, Any] | None = None


class YTDLPDownloader:
    """
    Tải video từ URL bằng yt-dlp.
    """

    def __init__(self, *, config: dict[str, Any] | None = None, log: LogFn | None = None) -> None:
        cfg = dict(_default_ytdlp_config())
        cfg.update(dict(config or {}))
        self._cfg = cfg
        self._log = log or (lambda _m: None)

    @staticmethod
    def is_supported_url(url: str) -> bool:
        u = str(url or "").strip().lower()
        return any(x in u for x in ("youtube.com", "youtu.be", "tiktok.com", "facebook.com", "fb.watch"))

    def download(self, url: str, output_path: str) -> dict[str, Any]:
        """
        Return:
        {
            "success": True,
            "video_path": "...",
            "title": "",
            "duration": 0,
            "ext": "mp4"
        }
        """
        if not bool(self._cfg.get("enabled", True)):
            return {"success": False, "error": "yt-dlp đang tắt trong config."}
        if not self.is_supported_url(url):
            return {"success": False, "error": "URL không hỗ trợ (chỉ YouTube/TikTok/Facebook public)."}
        out = Path(output_path).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        cmd = self._build_cmd(url=url, output_template=str(out.parent / "%(id)s.%(ext)s"))
        # Retry 1 lần nếu fail.
        for attempt in (1, 2):
            p = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=int(self._cfg.get("timeout_sec") or 300),
            )
            if p.returncode == 0:
                info = self._parse_info_json(p.stdout or "")
                if not info:
                    return {"success": False, "error": "yt-dlp trả về nhưng parse JSON thất bại."}
                vid = str(info.get("id") or "").strip()
                ext = str(info.get("ext") or "mp4").strip() or "mp4"
                source_path = out.parent / f"{vid}.{ext}"
                if not source_path.is_file() and ext != "mp4":
                    # Ưu tiên file mp4 sau merge.
                    source_path = out.parent / f"{vid}.mp4"
                if not source_path.is_file():
                    return {"success": False, "error": "Không tìm thấy file video sau khi tải."}
                source_path = source_path.resolve()
                out = out.resolve()
                if source_path != out:
                    out = self._copy_download_to_output(source_path=source_path, target_path=out)
                return {
                    "success": True,
                    "video_path": str(out),
                    "title": str(info.get("title") or ""),
                    "duration": float(info.get("duration") or 0),
                    "ext": "mp4",
                }
            err = (p.stderr or p.stdout or "").strip()
            if attempt == 1:
                self._log(f"[WARNING] yt-dlp lỗi lần 1, thử lại lần 2: {err[:240]}")
                continue
            return {"success": False, "error": err[:800]}
        return {"success": False, "error": "Download thất bại không rõ nguyên nhân."}

    def _copy_download_to_output(self, *, source_path: Path, target_path: Path) -> Path:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        # Retry copy vì Windows có thể còn giữ handle vài trăm ms sau merge.
        for attempt in range(12):
            try:
                shutil.copy2(source_path, target_path)
                return target_path
            except OSError as exc:
                win_e = int(getattr(exc, "winerror", 0) or 0)
                if win_e in {32, 5}:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                raise
        # Fallback: ghi sang tên khác nếu file chuẩn đang bị lock.
        alt = target_path.with_name(f"{target_path.stem}_{uuid.uuid4().hex[:6]}{target_path.suffix}")
        shutil.copy2(source_path, alt)
        self._log(f"[WARNING] File đích đang bị khóa; đã lưu sang file fallback: {alt.name}")
        return alt

    def _build_cmd(self, *, url: str, output_template: str) -> list[str]:
        cmd = [
            *self._resolve_ytdlp_cmd_prefix(),
            "-f",
            "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/best",
            "--merge-output-format",
            "mp4",
            "--no-playlist",
            "--quiet",
            "--no-warnings",
            "--print-json",
            "--max-filesize",
            f"{int(self._cfg.get('max_filesize_mb') or 200)}M",
            "-o",
            output_template,
            url,
        ]
        proxy = str(self._cfg.get("proxy") or "").strip()
        if proxy:
            cmd.extend(["--proxy", proxy])
        return cmd

    def _configured_ytdlp_exe_path(self) -> Path:
        raw = str(self._cfg.get("exe_path") or "").strip()
        if not raw:
            return Path()
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = (project_root() / p).resolve()
        return p

    @staticmethod
    def _probe_python_m_ytdlp() -> list[str] | None:
        try:
            p = subprocess.run(
                [sys.executable, "-m", "yt_dlp", "--version"],
                capture_output=True,
                text=True,
                timeout=25,
                encoding="utf-8",
                errors="replace",
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        if p.returncode == 0:
            return [sys.executable, "-m", "yt_dlp"]
        return None

    def _resolve_ytdlp_cmd_prefix(self) -> list[str]:
        use_exe = bool(self._cfg.get("use_exe", False))
        exe_path = self._configured_ytdlp_exe_path()
        if use_exe and exe_path.is_file():
            return [str(exe_path.resolve())]
        by_path = shutil.which("yt-dlp")
        if by_path:
            return [by_path]
        if exe_path.is_file():
            return [str(exe_path.resolve())]
        try:
            import yt_dlp as _  # type: ignore # noqa: F401

            return [sys.executable, "-m", "yt_dlp"]
        except Exception:
            pass
        prefix = self._probe_python_m_ytdlp()
        if prefix:
            return prefix
        raise RuntimeError(
            "Không tìm thấy yt-dlp cho Python đang chạy app. "
            f"Thử: `{sys.executable} -m pip install yt-dlp` "
            "hoặc đặt tools/yt-dlp/yt-dlp.exe và bật use_exe trong config."
        )

    @staticmethod
    def _parse_info_json(stdout_text: str) -> dict[str, Any] | None:
        lines = [x.strip() for x in str(stdout_text or "").splitlines() if x.strip()]
        for line in reversed(lines):
            if not line.startswith("{"):
                continue
            try:
                data = json.loads(line)
            except Exception:
                continue
            if isinstance(data, dict):
                return data
        return None


class VideoSourceImporter:
    ALLOWED_EXTS = {".mp4", ".mov", ".webm", ".mkv"}

    def __init__(self, *, log: LogFn | None = None) -> None:
        self._paths = ensure_reverse_video_layout()
        self._log = log or (lambda _m: None)
        ycfg = dict(load_reverse_video_config().get("yt_dlp") or {})
        self._downloader = YTDLPDownloader(config=ycfg, log=self._log)

    def import_video(self, job: ReverseVideoJob) -> Path:
        self._log("[INFO] Bắt đầu import video")
        out_name = f"{_safe_slug(job.id)}.mp4"
        out_path = self._paths["input"] / out_name
        st = str(job.source_type or "").strip().lower()
        if st == "url":
            raise RuntimeError(
                "Tải video từ URL đã chuyển sang tab «Tải video» (Universal Video Downloader). "
                "Hãy tải về bằng yt-dlp ở đó, rồi chọn file local hoặc bấm «Phân tích Reverse» từ thư viện."
            )
        if job.source_type == "local":
            src = Path(job.local_video_path).expanduser().resolve()
            if not src.is_file():
                raise FileNotFoundError(f"Không thấy file local: {src}")
            if src.suffix.lower() not in self.ALLOWED_EXTS:
                raise ValueError(f"Đuôi file không hỗ trợ: {src.suffix}")
            real_path = self._copy_local_video_resilient(source_path=src, target_path=out_path)
            self._log(f"[SUCCESS] Đã copy file local vào {real_path}")
            return real_path
        url = str(job.source_url or "").strip()
        if not url:
            raise ValueError("Thiếu source_url")
        real_path = self._download_via_yt_dlp(url=url, out_path=out_path)
        self._log(f"[SUCCESS] Đã tải video URL vào {real_path}")
        return real_path

    def _download_via_yt_dlp(self, *, url: str, out_path: Path) -> Path:
        self._log("[INFO] Đang tải video URL bằng yt-dlp")
        ret = self._downloader.download(url=url, output_path=str(out_path))
        if not bool(ret.get("success")):
            raise RuntimeError(f"Tải URL thất bại: {ret.get('error')}")
        p = Path(str(ret.get("video_path") or "")).expanduser().resolve()
        if not p.is_file():
            raise RuntimeError("yt-dlp báo thành công nhưng không tìm thấy file video.")
        return p

    def _copy_local_video_resilient(self, *, source_path: Path, target_path: Path) -> Path:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        source_path = source_path.resolve()
        target_path = target_path.resolve()
        if source_path == target_path:
            self._log("[INFO] File local đã nằm trong thư mục input; bỏ qua bước copy.")
            return target_path
        # Tránh copy đè vào file cũ đang bị Windows giữ handle bởi preview/player/ffmpeg.
        candidates = [target_path]
        if target_path.exists():
            candidates.append(target_path.with_name(f"{target_path.stem}_{uuid.uuid4().hex[:6]}{target_path.suffix}"))
        last_error: OSError | None = None
        for candidate in candidates:
            for attempt in range(10):
                try:
                    shutil.copy2(source_path, candidate)
                    return candidate
                except OSError as exc:
                    last_error = exc
                    win_e = int(getattr(exc, "winerror", 0) or 0)
                    if win_e in {32, 5}:
                        if attempt == 0:
                            self._log(f"[WARNING] File đang bị khóa, thử lại copy: {source_path.name}")
                        time.sleep(0.25 * (attempt + 1))
                        continue
                    raise
            # Nếu file đích bị khóa, thử tên khác ngay; nếu source bị khóa thì fallback này cũng sẽ fail.
            if candidate == target_path and len(candidates) > 1:
                self._log(f"[WARNING] File đích đang bị khóa; thử lưu sang tên mới: {candidates[1].name}")
                continue
        msg = str(last_error or "file đang bị process khác sử dụng")
        raise RuntimeError(
            "Không copy được video vì file đang bị chương trình khác giữ. "
            "Hãy đóng trình phát video/thư mục preview/Gemini/Veo3Studio nếu đang mở file này rồi thử lại. "
            f"Chi tiết: {msg}"
        )


class FFmpegService:
    def __init__(self) -> None:
        self.ffmpeg, self.ffprobe = resolve_ffmpeg_ffprobe_paths()

    def check_ffmpeg_available(self) -> bool:
        return bool(self.ffmpeg and self.ffprobe)

    def read_metadata(self, video_path: Path) -> dict[str, Any]:
        if not self.check_ffmpeg_available():
            raise RuntimeError("Thiếu ffmpeg/ffprobe")
        cmd = [
            str(self.ffprobe),
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height,r_frame_rate,codec_name:format=duration",
            "-of",
            "json",
            str(video_path),
        ]
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            raise RuntimeError((p.stderr or p.stdout or "ffprobe error").strip()[:400])
        raw = json.loads(p.stdout or "{}")
        streams = raw.get("streams") or []
        fmt = raw.get("format") or {}
        st = streams[0] if streams else {}
        width = int(st.get("width") or 0)
        height = int(st.get("height") or 0)
        fps = self._parse_fps(str(st.get("r_frame_rate") or "0/1"))
        duration = float(fmt.get("duration") or st.get("duration") or 0.0)
        return {
            "duration": round(duration, 3),
            "fps": round(fps, 3),
            "width": width,
            "height": height,
            "resolution": f"{width}x{height}" if width and height else "",
            "codec": str(st.get("codec_name") or ""),
        }

    @staticmethod
    def _parse_fps(v: str) -> float:
        if "/" in v:
            a, b = v.split("/", 1)
            try:
                aa = float(a)
                bb = float(b)
                if bb:
                    return aa / bb
            except Exception:
                return 0.0
        try:
            return float(v)
        except Exception:
            return 0.0


class KeyframeExtractor:
    def __init__(self, *, ff: FFmpegService, log: LogFn | None = None) -> None:
        self._ff = ff
        self._paths = ensure_reverse_video_layout()
        self._log = log or (lambda _m: None)

    def extract(
        self,
        *,
        job_id: str,
        video_path: Path,
        mode: str,
        max_frames: int,
        duration: float,
    ) -> list[dict[str, Any]]:
        out_dir = self._paths["frames"] / _safe_slug(job_id)
        if out_dir.is_dir():
            shutil.rmtree(out_dir, ignore_errors=True)
        out_dir.mkdir(parents=True, exist_ok=True)
        mode = str(mode or "hybrid").strip().lower()
        if mode == "auto":
            mode = self._resolve_auto_mode(duration=duration)
            self._log(f"[INFO] Keyframe auto => chọn mode '{mode}' theo độ dài video")
        try:
            if mode == "fixed_interval":
                self._extract_fixed(video_path, out_dir, fps=1.0)
            elif mode == "scene_detection":
                self._extract_scene(video_path, out_dir, threshold=0.30)
            elif mode == "thumbnail":
                self._extract_thumbnail(video_path, out_dir, bucket=60)
            else:
                self._extract_hybrid(video_path, out_dir, duration=duration)
        except Exception as exc:
            self._log(f"[WARNING] Mode keyframe '{mode}' lỗi, chuyển sang fallback fixed_interval: {exc}")
            self._extract_fallback(video_path=video_path, out_dir=out_dir, duration=duration)
        self._ensure_minimum_timeline_frames(video_path=video_path, out_dir=out_dir, duration=duration, target=min(max_frames, 12))
        return self._normalize_frames(video_path=video_path, out_dir=out_dir, max_frames=max_frames, duration=duration)

    @staticmethod
    def _resolve_auto_mode(*, duration: float) -> str:
        if duration <= 10:
            return "fixed_interval"
        if duration <= 30:
            return "hybrid"
        return "scene_detection"

    def _run(self, args: list[str]) -> None:
        p = subprocess.run(args, capture_output=True, text=True)
        if p.returncode != 0:
            raise RuntimeError(self._format_process_error(p.stderr or p.stdout or "ffmpeg error"))

    def _run_image_extract(self, args: list[str], output_pattern: Path) -> None:
        try:
            self._run(args)
            return
        except RuntimeError as exc:
            if output_pattern.suffix.lower() != ".jpg":
                raise
            self._log(f"[WARNING] Xuất JPG lỗi, thử fallback PNG: {exc}")
        png_pattern = output_pattern.with_suffix(".png")
        png_args = list(args)
        png_args[-1] = str(png_pattern)
        cleaned: list[str] = []
        skip_next = False
        for idx, token in enumerate(png_args):
            if skip_next:
                skip_next = False
                continue
            if token == "-q:v":
                skip_next = True
                continue
            cleaned.append(token)
        self._run(cleaned)

    @staticmethod
    def _format_process_error(text: str) -> str:
        lines = [x.strip() for x in str(text or "").splitlines() if x.strip()]
        important = [
            x
            for x in lines
            if any(token in x.lower() for token in ("error", "invalid", "failed", "unable", "cannot", "no such", "permission"))
        ]
        picked = important[-8:] if important else lines[-12:]
        msg = "\n".join(picked).strip() or "ffmpeg error"
        return msg[-1200:]

    def _extract_fixed(self, video_path: Path, out_dir: Path, fps: float) -> None:
        self._log("[INFO] Đang tách keyframes mode fixed_interval")
        fps_tag = int(max(fps, 0.001) * 1000)
        out_pattern = out_dir / f"fixed_fps{fps_tag:04d}_%04d.jpg"
        self._run_image_extract(
            [
                str(self._ff.ffmpeg),
                "-y",
                "-i",
                str(video_path),
                "-vf",
                f"fps={fps},scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuvj420p",
                "-q:v",
                "2",
                str(out_pattern),
            ],
            out_pattern,
        )

    def _extract_scene(self, video_path: Path, out_dir: Path, threshold: float) -> None:
        self._log("[INFO] Đang tách keyframes mode scene_detection")
        out_pattern = out_dir / "scene_%04d.jpg"
        self._run_image_extract(
            [
                str(self._ff.ffmpeg),
                "-y",
                "-i",
                str(video_path),
                "-vf",
                f"select='gt(scene,{threshold})',scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuvj420p",
                "-vsync",
                "vfr",
                "-q:v",
                "2",
                str(out_pattern),
            ],
            out_pattern,
        )

    def _extract_thumbnail(self, video_path: Path, out_dir: Path, bucket: int) -> None:
        self._log("[INFO] Đang tách keyframes mode thumbnail")
        out_pattern = out_dir / "thumb_%04d.jpg"
        self._run_image_extract(
            [
                str(self._ff.ffmpeg),
                "-y",
                "-i",
                str(video_path),
                "-vf",
                f"thumbnail={bucket},scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuvj420p",
                "-vsync",
                "vfr",
                "-q:v",
                "2",
                str(out_pattern),
            ],
            out_pattern,
        )

    def _extract_hybrid(self, video_path: Path, out_dir: Path, *, duration: float) -> None:
        self._log("[INFO] Đang tách keyframes mode hybrid")
        self._extract_stills_at_times(video_path, out_dir, [0.0, max(duration / 2.0, 0.0), max(duration - 0.1, 0.0)])
        if duration <= 15:
            try:
                self._extract_fixed(video_path, out_dir, fps=1.0)
            except Exception as exc:
                self._log(f"[WARNING] fixed_interval trong hybrid lỗi, vẫn dùng anchor frames: {exc}")
        else:
            try:
                self._extract_scene(video_path, out_dir, threshold=0.30)
            except Exception as exc:
                self._log(f"[WARNING] scene_detection trong hybrid lỗi, fallback fixed_interval: {exc}")
                try:
                    self._extract_fixed(video_path, out_dir, fps=0.5)
                except Exception as exc2:
                    self._log(f"[WARNING] fixed_interval fallback cũng lỗi, vẫn dùng anchor frames: {exc2}")

    def _extract_stills_at_times(self, video_path: Path, out_dir: Path, times: list[float]) -> None:
        for idx, ts in enumerate(times, start=1):
            out = out_dir / f"anchor_t{int(max(ts, 0.0) * 1000):09d}ms_{idx:02d}.jpg"
            self._run_image_extract(
                [
                    str(self._ff.ffmpeg),
                    "-y",
                    "-ss",
                    f"{max(ts, 0.0):.3f}",
                    "-i",
                    str(video_path),
                    "-vf",
                    "scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuvj420p",
                    "-frames:v",
                    "1",
                    "-q:v",
                    "2",
                    str(out),
                ],
                out,
            )

    def _extract_fallback(self, *, video_path: Path, out_dir: Path, duration: float) -> None:
        existing = [p for p in out_dir.glob("*.jpg") if p.is_file()]
        if existing:
            return
        try:
            self._extract_fixed(video_path, out_dir, fps=0.5)
            return
        except Exception as exc:
            self._log(f"[WARNING] fallback fixed_interval lỗi: {exc}")
        points = [0.0]
        if duration > 1:
            points.extend([duration * 0.25, duration * 0.5, duration * 0.75, max(duration - 0.1, 0.0)])
        self._extract_stills_at_times(video_path, out_dir, points)

    def _ensure_minimum_timeline_frames(self, *, video_path: Path, out_dir: Path, duration: float, target: int) -> None:
        target = max(3, int(target or 0))
        raw_files = [p for p in out_dir.iterdir() if p.is_file() and p.suffix.lower() in {".jpg", ".png"} and not p.name.startswith("frame_")]
        if len(raw_files) >= target:
            return
        if duration <= 0:
            return
        missing_target = target - len(raw_files)
        self._log(f"[INFO] Hybrid chỉ có {len(raw_files)} frame; bổ sung {missing_target} timeline frame để đủ ngữ cảnh.")
        # Tạo mốc đều theo thời gian, bỏ qua 0/mid/end nếu đã có anchor gần đó.
        existing_ts = [
            self._estimate_frame_timestamp(p, duration=duration, group_count=len(raw_files))
            for p in raw_files
        ]
        points: list[float] = []
        for i in range(target):
            if target == 1:
                ts = duration / 2.0
            else:
                ts = i * (duration / max(1, target - 1))
            ts = min(max(ts, 0.0), max(duration - 0.05, 0.0))
            if any(abs(ts - old) < 0.35 for old in existing_ts + points):
                continue
            points.append(ts)
        if points:
            self._extract_stills_at_times(video_path, out_dir, points)

    def _normalize_frames(self, *, video_path: Path, out_dir: Path, max_frames: int, duration: float) -> list[dict[str, Any]]:
        raw_files = [p for p in out_dir.iterdir() if p.is_file() and p.suffix.lower() in {".jpg", ".png"} and not p.name.startswith("frame_")]
        group_counts: dict[str, int] = {}
        for p in raw_files:
            group = self._raw_frame_group(p)
            group_counts[group] = group_counts.get(group, 0) + 1
        files = sorted(
            raw_files,
            key=lambda p: self._raw_frame_sort_key(p, duration=duration, group_count=group_counts.get(self._raw_frame_group(p), 0)),
        )
        raw_ts = {
            p: self._estimate_frame_timestamp(p, duration=duration, group_count=group_counts.get(self._raw_frame_group(p), 0))
            for p in files
        }
        if not files:
            raise RuntimeError("Không tách được frame nào")
        uniq: list[Path] = []
        seen_sizes: set[int] = set()
        for p in files:
            sz = p.stat().st_size
            if sz in seen_sizes:
                continue
            seen_sizes.add(sz)
            uniq.append(p)
        if len(uniq) > max_frames:
            uniq = self._sample_paths_evenly(uniq, max_frames)
        normalized: list[dict[str, Any]] = []
        for i, src in enumerate(uniq, start=1):
            ts = raw_ts.get(src, self._estimate_frame_timestamp(src, duration=duration, group_count=len(uniq)))
            out_name = f"frame_{i:04d}_t{int(max(ts, 0.0) * 1000):09d}ms.jpg"
            out = out_dir / out_name
            self._run(
                [
                    str(self._ff.ffmpeg),
                    "-y",
                    "-i",
                    str(src),
                    "-vf",
                    "scale='min(1280,iw)':-2,scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuvj420p",
                    "-frames:v",
                    "1",
                    "-q:v",
                    "2",
                    str(out),
                ]
            )
            normalized.append({"path": str(out), "timestamp": round(ts, 3)})
        self._log(f"[INFO] Đã tách {len(normalized)} keyframes")
        return normalized

    def _raw_frame_sort_key(self, path: Path, *, duration: float, group_count: int = 0) -> tuple[float, str]:
        return (self._estimate_frame_timestamp(path, duration=duration, group_count=group_count), path.name)

    @staticmethod
    def _raw_frame_group(path: Path) -> str:
        name = path.name.lower()
        if name.startswith("fixed"):
            return "fixed"
        if name.startswith("scene"):
            return "scene"
        if name.startswith("thumb"):
            return "thumb"
        if name.startswith("anchor"):
            return "anchor"
        return "other"

    def _estimate_frame_timestamp(self, path: Path, *, duration: float, group_count: int = 0) -> float:
        name = path.name.lower()
        m = re.search(r"_t(\d+)ms", name)
        if m:
            return int(m.group(1)) / 1000.0
        m = re.search(r"_t(\d+)", name)
        if m:
            return float(int(m.group(1)))
        m = re.search(r"fixed_fps(\d+)_(\d+)", name)
        if m:
            fps = max(0.001, int(m.group(1)) / 1000.0)
            idx = max(0, int(m.group(2)) - 1)
            return idx / fps
        for prefix in ("fixed", "scene", "thumb"):
            m = re.search(rf"{prefix}_(\d+)", name)
            if m:
                idx = max(0, int(m.group(1)) - 1)
                count = max(1, group_count or 1)
                if duration > 0 and count > 1:
                    return min(duration, idx * (duration / max(1, count - 1)))
                return float(idx)
        return float(path.stat().st_mtime_ns) / 1_000_000_000.0

    @staticmethod
    def _sample_paths_evenly(items: list[Path], count: int) -> list[Path]:
        if count <= 0:
            return []
        if len(items) <= count:
            return list(items)
        if count == 1:
            return [items[len(items) // 2]]
        step = (len(items) - 1) / (count - 1)
        out: list[Path] = []
        seen: set[Path] = set()
        for i in range(count):
            p = items[round(i * step)]
            if p not in seen:
                seen.add(p)
                out.append(p)
        return out


class GeminiBrowserAnalyzer:
    def __init__(self, *, log: LogFn | None = None) -> None:
        self._log = log or (lambda _m: None)
        self._paths = ensure_reverse_video_layout()

    def analyze(
        self,
        *,
        job: ReverseVideoJob,
        frame_paths: list[str],
        video_path: str,
    ) -> str:
        cfg_runtime = get_nanobanana_runtime_config()
        cfg_job = dict(job.gemini_browser or {})
        profile_path = (
            os.environ.get("NANOBANANA_BROWSER_PROFILE_DIR", "").strip()
            or os.environ.get("VEO3_BROWSER_PROFILE_DIR", "").strip()
            or str(cfg_job.get("profile_path") or "").strip()
            or str(project_root() / "data" / "nanobanana" / "browser_profile")
        )
        runtime_profile = self._prepare_runtime_profile(Path(profile_path))
        browser_exe_path = str(cfg_job.get("browser_exe_path") or "").strip() or None
        url = (
            os.environ.get("NANOBANANA_WEB_URL", "").strip()
            or os.environ.get("VEO3_WEB_URL", "").strip()
            or str(cfg_runtime.get("web_url") or "").strip()
            or str(cfg_job.get("url") or "").strip()
            or "https://gemini.google.com/app"
        )
        show_browser = bool(cfg_job.get("show_browser", False))
        self._log("[INFO] Mở Gemini Browser bằng profile đã login")
        prof = runtime_profile
        prof.mkdir(parents=True, exist_ok=True)
        with _profile_lock(prof):
            with sync_playwright() as pw:
                ctx, temp_profile = self._launch_context_resilient(
                    pw=pw,
                    profile_path=prof,
                    browser_exe_path=browser_exe_path,
                    show_browser=show_browser,
                )
                try:
                    page = ctx.pages[0] if ctx.pages else ctx.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    page.wait_for_timeout(1200)
                    if self._looks_like_login_required(page):
                        self._capture(page, f"{job.id}_need_login")
                        raise RuntimeError("need_manual_check: Gemini chưa đăng nhập")
                    upload_mode = str((job.gemini_browser or {}).get("upload_mode") or "best_10").strip().lower()
                    selected = self._prepare_upload_files(frame_paths=frame_paths, video_path=video_path)
                    if upload_mode == "auto_optimal":
                        upload_mode = "detailed_chunks" if len(selected) > 10 else "best_10"
                        self._log(f"[INFO] Upload auto_optimal => chọn {upload_mode} cho {len(selected)} file.")
                    if upload_mode == "detailed_chunks" and len(selected) > 10:
                        chunk_texts: list[str] = []
                        chunks = self._build_timeline_chunks(selected, max_files=10, overlap=3)
                        self._validate_chunk_order(chunks)
                        self._log(
                            f"[INFO] Chunk mode tối ưu: {len(chunks)} lượt phân tích Gemini "
                            f"(timeline ordered, overlap context, mỗi lượt <=10 file)."
                        )
                        self._log_chunk_plan(chunks)
                        self._save_chunk_manifest(job_id=job.id, chunks=chunks)
                        self._start_new_chat_if_possible(page)
                        for idx, ch in enumerate(chunks, start=1):
                            self._log(f"[INFO] Chunk {idx}/{len(chunks)} upload {len(ch)} frame: {Path(ch[0]).name} -> {Path(ch[-1]).name}")
                            self._upload_inputs(page=page, files=ch)
                            page.wait_for_timeout(250)
                            self._log(
                                f"[INFO] Bỏ qua xác nhận upload chunk {idx}/{len(chunks)}, chuyển bước tiếp."
                            )
                            self._prepare_gemini_composer_for_prompt(
                                page,
                                timeout_ms=120_000,
                                require_send_ready=True,
                            )
                            previous_context = "\n\n".join(
                                [f"CHUNK {i + 1} SUMMARY:\n{txt}" for i, txt in enumerate(chunk_texts)]
                            )
                            part_prompt = build_gemini_chunk_prompt(
                                chunk_index=idx,
                                total_chunks=len(chunks),
                                previous_context=previous_context,
                                frame_list=[Path(x).name for x in ch],
                            )
                            baseline_count = self._count_response_blocks(page)
                            baseline_text = self._get_last_gemini_response_text(page)
                            self._input_prompt_and_send(
                                page=page,
                                prompt=part_prompt,
                                require_send_ready_before_input=True,
                            )
                            self._wait_gemini_response_started(
                                page,
                                baseline_text=baseline_text,
                                baseline_count=baseline_count,
                            )
                            part_text = self._wait_gemini_response_complete_and_get_text(
                                page,
                                baseline_text=baseline_text,
                                baseline_count=baseline_count,
                            )
                            chunk_texts.append(part_text)
                        self._prepare_gemini_composer_for_prompt(page, timeout_ms=60_000)
                        merge_prompt = build_gemini_merge_prompt(chunk_texts)
                        baseline_count = self._count_response_blocks(page)
                        baseline_text = self._get_last_gemini_response_text(page)
                        self._input_prompt_and_send(page=page, prompt=merge_prompt)
                        self._wait_gemini_response_started(
                            page,
                            baseline_text=baseline_text,
                            baseline_count=baseline_count,
                        )
                        return self._wait_gemini_response_complete_and_get_text(
                            page,
                            baseline_text=baseline_text,
                            baseline_count=baseline_count,
                        )
                    # Mặc định: best 10
                    best10 = self._select_best_keyframes(selected, max_files=10)
                    self._log(f"[INFO] Gemini upload mode=best_10 | extracted={len(selected)} | selected={len(best10)}")
                    self._upload_inputs(page=page, files=best10)
                    page.wait_for_timeout(250)
                    self._log("[INFO] Bỏ qua bước xác nhận upload theo cấu hình hiện tại, chuyển bước tiếp.")
                    self._prepare_gemini_composer_for_prompt(
                        page,
                        timeout_ms=120_000,
                        require_send_ready=True,
                    )
                    prompt = build_gemini_strict_prompt()
                    baseline = self._get_last_gemini_response_text(page)
                    baseline_count = self._count_response_blocks(page)
                    self._input_prompt_and_send(
                        page=page,
                        prompt=prompt,
                        require_send_ready_before_input=True,
                    )
                    self._wait_gemini_response_started(page, baseline_text=baseline, baseline_count=baseline_count)
                    return self._wait_gemini_response_complete_and_get_text(
                        page,
                        baseline_text=baseline,
                        baseline_count=baseline_count,
                    )
                finally:
                    try:
                        ctx.close()
                    except Exception:
                        pass
                    if temp_profile and temp_profile.is_dir():
                        shutil.rmtree(temp_profile, ignore_errors=True)

    def _launch_context_resilient(
        self,
        *,
        pw: Any,
        profile_path: Path,
        browser_exe_path: str | None,
        show_browser: bool,
    ) -> tuple[Any, Path | None]:
        launch_errs: list[str] = []
        exe_candidates: list[str | None] = []
        if browser_exe_path:
            exe_candidates.append(browser_exe_path)
        exe_candidates.extend(self._candidate_chrome_executables())
        exe_candidates.append(None)  # fallback playwright-managed browser

        for idx in range(2):
            temp_profile: Path | None = None
            use_profile = profile_path
            if idx == 1:
                temp_profile = self._make_temp_profile_clone(profile_path)
                use_profile = temp_profile
                self._log("[WARNING] Profile chính mở lỗi; thử lại với profile clone tạm.")
            for exe in exe_candidates:
                for use_channel in (True, False):
                    try:
                        kw: dict[str, Any] = {
                            "user_data_dir": str(use_profile),
                            "headless": not show_browser,
                            "viewport": {"width": 1280, "height": 900},
                            "accept_downloads": True,
                        }
                        if exe:
                            kw["executable_path"] = exe
                        elif use_channel:
                            kw["channel"] = "chrome"
                        # Giảm tối đa biến số gây crash: chỉ thêm arg nhẹ ở pass cuối.
                        if not exe and not use_channel:
                            kw["args"] = ["--disable-gpu"]
                        ctx = pw.chromium.launch_persistent_context(**kw)
                        return ctx, temp_profile
                    except Exception as exc:  # noqa: BLE001
                        launch_errs.append(str(exc))
                        continue
            if temp_profile and temp_profile.is_dir():
                shutil.rmtree(temp_profile, ignore_errors=True)
        raise RuntimeError("Không mở được Gemini browser profile. " + " | ".join(launch_errs[-2:]))

    def _prepare_runtime_profile(self, source_profile: Path) -> Path:
        """
        Tách profile runtime riêng cho Reverse để tránh lock với luồng login/veo khác.
        Copy nhẹ từ profile login sang lần đầu; các lần sau dùng lại runtime profile.
        """
        source = source_profile.expanduser().resolve()
        runtime = (project_root() / "data" / "reverse_video" / "gemini_runtime_profile").resolve()
        runtime.mkdir(parents=True, exist_ok=True)
        # Dọn lock files stale ở runtime trước khi mở.
        for name in ("SingletonLock", "SingletonCookie", "SingletonSocket", "LOCK", "lockfile", "DevToolsActivePort"):
            p = runtime / name
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
        # Nếu runtime trống, clone từ profile login để giữ session.
        try:
            is_empty = not any(runtime.iterdir())
        except Exception:
            is_empty = False
        if is_empty and source.is_dir():
            self._log("[INFO] Tạo runtime profile riêng cho Reverse từ profile đăng nhập Gemini.")
            skip_names = {"SingletonLock", "SingletonCookie", "SingletonSocket", "LOCK", "lockfile", "DevToolsActivePort"}
            for item in source.iterdir():
                if item.name in skip_names:
                    continue
                dst = runtime / item.name
                try:
                    if item.is_dir():
                        shutil.copytree(item, dst, dirs_exist_ok=True)
                    elif item.is_file():
                        shutil.copy2(item, dst)
                except Exception:
                    continue
        return runtime

    def _make_temp_profile_clone(self, src_profile: Path) -> Path:
        tmp = Path(tempfile.mkdtemp(prefix="rvp_gemini_profile_"))
        if not src_profile.is_dir():
            return tmp
        skip_names = {
            "SingletonLock",
            "SingletonCookie",
            "SingletonSocket",
            "LOCK",
            "lockfile",
            "DevToolsActivePort",
        }
        skip_parts = {"Crashpad", "Code Cache", "GPUCache", "ShaderCache", "BrowserMetrics"}
        for item in src_profile.iterdir():
            if item.name in skip_names:
                continue
            if any(p in skip_parts for p in item.parts):
                continue
            dst = tmp / item.name
            try:
                if item.is_dir():
                    shutil.copytree(item, dst, dirs_exist_ok=True)
                elif item.is_file():
                    shutil.copy2(item, dst)
            except Exception:
                continue
        return tmp

    @staticmethod
    def _candidate_chrome_executables() -> list[str]:
        cands: list[Path] = []
        if os.name == "nt":
            local = Path(os.environ.get("LOCALAPPDATA", "")).expanduser()
            pf = Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
            pfx86 = Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"))
            cands.extend(
                [
                    local / "Google" / "Chrome" / "Application" / "chrome.exe",
                    pf / "Google" / "Chrome" / "Application" / "chrome.exe",
                    pfx86 / "Google" / "Chrome" / "Application" / "chrome.exe",
                    pf / "Microsoft" / "Edge" / "Application" / "msedge.exe",
                    pfx86 / "Microsoft" / "Edge" / "Application" / "msedge.exe",
                ]
            )
        out: list[str] = []
        seen: set[str] = set()
        for p in cands:
            sp = str(p)
            if p.is_file() and sp not in seen:
                seen.add(sp)
                out.append(sp)
        return out

    def _looks_like_login_required(self, page: Any) -> bool:
        markers = ["Sign in", "Đăng nhập", "Continue with Google"]
        for t in markers:
            try:
                if page.get_by_text(t, exact=False).first.is_visible(timeout=1000):
                    return True
            except Exception:
                continue
        return False

    def _prepare_upload_files(self, *, frame_paths: list[str], video_path: str) -> list[str]:
        files = sorted([p for p in frame_paths if Path(p).is_file()], key=self._frame_sort_key)
        if not files and Path(video_path).is_file():
            files = [video_path]
        if not files:
            raise RuntimeError("Không có file để upload Gemini")
        if len(files) > 1:
            files = self._make_ordered_upload_copies(files)
        return files

    def _select_best_keyframes(self, files: list[str], *, max_files: int = 10) -> list[str]:
        if len(files) <= max_files:
            return sorted(files, key=self._frame_sort_key)
        # Best mode: chọn đều theo timeline để đại diện đủ đầu -> giữa -> cuối.
        # Không đưa frame cuối lên quá sớm vì Gemini có thể trộn đoạn kết vào Part 1.
        sorted_files = sorted(files, key=self._frame_sort_key)
        picked = self._sample_evenly(sorted_files, max_files)
        return sorted(dict.fromkeys(picked), key=self._frame_sort_key)[:max_files]

    def _build_timeline_chunks(self, files: list[str], *, max_files: int = 10, overlap: int = 3) -> list[list[str]]:
        ordered = sorted(files, key=self._frame_sort_key)
        if len(ordered) <= max_files:
            return [ordered]
        overlap = max(0, min(overlap, max_files - 1))
        step = max(1, max_files - overlap)
        chunks: list[list[str]] = []
        start = 0
        while start < len(ordered):
            end = min(len(ordered), start + max_files)
            chunk = ordered[start:end]
            if not chunks or chunk != chunks[-1]:
                chunks.append(chunk)
            if end >= len(ordered):
                break
            start += step
        return chunks

    def _validate_chunk_order(self, chunks: list[list[str]]) -> None:
        prev_last: tuple[int, str] | None = None
        for idx, ch in enumerate(chunks, start=1):
            keys = [self._frame_sort_key(x) for x in ch]
            if keys != sorted(keys):
                raise RuntimeError(f"Chunk {idx} bị đảo thứ tự frame trước khi upload Gemini.")
            if prev_last and keys[-1] <= prev_last:
                raise RuntimeError(f"Chunk {idx} không tiến lên timeline so với chunk trước.")
            prev_last = keys[-1]

    def _log_chunk_plan(self, chunks: list[list[str]]) -> None:
        for idx, ch in enumerate(chunks, start=1):
            names = [Path(x).name for x in ch]
            overlap_note = ""
            if idx > 1:
                prev = {Path(x).name for x in chunks[idx - 2]}
                overlap = [name for name in names if name in prev]
                if overlap:
                    overlap_note = f" | overlap context: {', '.join(overlap)}"
            self._log(
                f"[INFO] Chunk plan {idx}/{len(chunks)}: {names[0]} -> {names[-1]} "
                f"({len(names)} frame){overlap_note}"
            )

    def _save_chunk_manifest(self, *, job_id: str, chunks: list[list[str]]) -> None:
        try:
            manifest = {
                "job_id": job_id,
                "created_at": _now_ts(),
                "chunk_count": len(chunks),
                "chunks": [
                    {
                        "chunk": idx,
                        "frame_count": len(ch),
                        "frames": [Path(x).name for x in ch],
                        "paths": list(ch),
                    }
                    for idx, ch in enumerate(chunks, start=1)
                ],
            }
            out = self._paths["gemini_uploads"] / f"{_safe_slug(job_id)}_chunk_manifest.json"
            out.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            self._log(f"[INFO] Đã lưu chunk manifest để kiểm tra: {out}")
        except Exception as exc:
            self._log(f"[WARNING] Không lưu được chunk manifest: {exc}")

    def _make_ordered_upload_copies(self, files: list[str]) -> list[str]:
        upload_dir = self._paths["gemini_uploads"] / f"ordered_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        upload_dir.mkdir(parents=True, exist_ok=True)
        out: list[str] = []
        total = max(1, len(files))
        for idx, src in enumerate(files, start=1):
            p = Path(src)
            suffix = p.suffix.lower() or ".jpg"
            # Tên file nói rõ thứ tự để Gemini không nhầm frame cuối thành frame đầu.
            dst = upload_dir / f"timeline_{idx:04d}_of_{total:04d}{suffix}"
            try:
                shutil.copy2(p, dst)
                out.append(str(dst))
            except Exception:
                out.append(str(p))
        return out

    @staticmethod
    def _frame_sort_key(path: str) -> tuple[int, str]:
        name = Path(path).name.lower()
        for pat in (r"timeline_(\d+)_of_", r"frame_(\d+)", r"anchor_(\d+)", r"fixed_(\d+)", r"scene_(\d+)", r"thumb_(\d+)"):
            m = re.search(pat, name)
            if m:
                return (int(m.group(1)), name)
        m = re.search(r"_t(\d+)", name)
        if m:
            return (int(m.group(1)), name)
        return (10**9, name)

    @staticmethod
    def _sample_evenly(items: list[str], count: int) -> list[str]:
        if count <= 0 or not items:
            return []
        if len(items) <= count:
            return list(items)
        if count == 1:
            return [items[len(items) // 2]]
        step = (len(items) - 1) / (count - 1)
        out: list[str] = []
        for i in range(count):
            idx = round(i * step)
            out.append(items[idx])
        return out

    def _upload_inputs(self, *, page: Any, files: list[str]) -> None:
        # Chiến lược 1: set_input_files trực tiếp vào input file (kể cả hidden).
        direct_selectors = [
            "input[type='file']",
            "input[accept*='image']",
            "input[accept*='video']",
        ]
        for sel in direct_selectors:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0:
                    loc.set_input_files(files, timeout=10_000)
                    self._log(f"[INFO] Upload {len(files)} file vào Gemini (direct: {sel})")
                    return
            except Exception:
                continue
        if self._try_set_files_on_any_frame_input(page=page, files=files, timeout_ms=4000):
            self._log(f"[INFO] Upload {len(files)} file vào Gemini (scan all frames inputs)")
            return

        # Chiến lược 2: bấm nút Attach/Upload/Add files để bắt filechooser.
        trigger_selectors = [
            "button:has-text('Attach')",
            "button:has-text('Upload')",
            "button:has-text('Add files')",
            "button:has-text('Tải lên')",
            "button:has-text('Đính kèm')",
            "[aria-label*='Attach']",
            "[aria-label*='Upload']",
            "[data-testid*='upload']",
            "[data-testid*='attach']",
        ]
        for sel in trigger_selectors:
            try:
                trg = page.locator(sel).first
                if trg.count() <= 0:
                    continue
                with page.expect_file_chooser(timeout=8000) as fc_info:
                    trg.click(timeout=4000)
                fc = fc_info.value
                fc.set_files(files)
                self._log(f"[INFO] Upload {len(files)} file vào Gemini (file chooser: {sel})")
                return
            except Exception:
                continue

        # Chiến lược 2b: UI Gemini mới cần mở menu (+ / Tools) rồi chọn đúng item "Upload files".
        menu_openers = [
            "button[aria-controls='upload-file-menu']",
            "button[aria-label*='upload file menu']",
            "button.close.upload-card-button",
            "button:has(mat-icon[fonticon='add_2'])",
            "mat-icon[fonticon='add_2']",
            "button:has-text('Tools')",
            "[role='button']:has-text('Tools')",
            "button:has-text('+')",
            "[aria-label*='Tools']",
            "[data-test-id*='tool']",
        ]
        for opener_sel in menu_openers:
            try:
                opener = page.locator(opener_sel).first
                if opener.count() <= 0:
                    continue
                opener.click(timeout=4000)
            except Exception:
                continue
            upload_item_selectors = [
                "div.menu-text:has-text('Upload files')",
                "div.gem-menu-item-label:has-text('Upload files')",
                "span.item:has-text('Upload files')",
                "[role='menuitem']:has-text('Upload files')",
                "button:has-text('Upload files')",
            ]
            for item_sel in upload_item_selectors:
                try:
                    item = page.locator(item_sel).first
                    if item.count() <= 0:
                        continue
                    with page.expect_file_chooser(timeout=8000) as fc_info:
                        item.click(timeout=4000)
                    fc = fc_info.value
                    fc.set_files(files)
                    self._log(f"[INFO] Upload {len(files)} file vào Gemini (menu Upload files: {item_sel})")
                    return
                except Exception:
                    # Có UI không phát file chooser event; bấm item xong input mới xuất hiện.
                    try:
                        item.click(timeout=3000)
                    except Exception:
                        pass
                    if self._try_set_files_on_any_frame_input(page=page, files=files, timeout_ms=6000):
                        self._log(f"[INFO] Upload {len(files)} file vào Gemini (menu->input: {item_sel})")
                        return
                    continue

        # Chiến lược 3: thử phím tắt mở file chooser.
        for combo in ("Control+O", "Meta+O"):
            try:
                with page.expect_file_chooser(timeout=6000) as fc_info:
                    page.keyboard.press(combo)
                fc = fc_info.value
                fc.set_files(files)
                self._log(f"[INFO] Upload {len(files)} file vào Gemini (shortcut: {combo})")
                return
            except Exception:
                continue
        if self._try_set_files_on_any_frame_input(page=page, files=files, timeout_ms=5000):
            self._log(f"[INFO] Upload {len(files)} file vào Gemini (final frame-scan fallback)")
            return

        raise RuntimeError("Không tìm được cách upload file vào Gemini (không thấy input/file chooser).")

    def _wait_upload_ready(self, page: Any, *, expected_count: int) -> None:
        """
        Chờ UI upload ổn định trước khi nhập prompt:
        - Có dấu hiệu attachment đã vào composer
        - Không còn trạng thái uploading/spinner rõ ràng
        """
        deadline = time.time() + 45
        stable = 0
        while time.time() < deadline:
            try:
                # Đếm số "chip/thumbnail" file đã hiện trên composer.
                chip_count = self._count_uploaded_file_chips(page)
                has_attachment = False
                attach_markers = [
                    "[aria-label*='uploaded']",
                    "[aria-label*='attachment']",
                    "[data-test-id*='attachment']",
                    "[data-test-id*='upload']",
                    "button[aria-label*='Close upload file menu']",
                ]
                for sel in attach_markers:
                    try:
                        if page.locator(sel).count() > 0:
                            has_attachment = True
                            break
                    except Exception:
                        continue
                uploading_visible = False
                for text in ("Uploading", "Đang tải", "processing", "Processing"):
                    try:
                        if page.get_by_text(text, exact=False).first.is_visible(timeout=200):
                            uploading_visible = True
                            break
                    except Exception:
                        continue
                progress_count = self._visible_count(page, "[role='progressbar']")
                # Quy tắc thực tế:
                # - Có đủ chip theo expected_count (hoặc ít nhất có attachment marker)
                # - Không còn progressbar rõ ràng
                # - Gemini idle (không Stop/progress; Send có thể chưa ready khi ô prompt trống)
                enough_files = chip_count >= max(1, expected_count) or has_attachment
                idle = not self._get_composer_state(page)["busy"]
                if enough_files and not uploading_visible and progress_count == 0 and idle:
                    stable += 1
                    if stable >= 1:
                        self._log(
                            f"[INFO] Upload đã ổn định (chips={chip_count}/{expected_count}), chuyển sang bước chọn mode + nhập prompt."
                        )
                        page.wait_for_timeout(120)
                        return
                else:
                    stable = 0
            except Exception:
                pass
            time.sleep(0.2)
        raise RuntimeError("Timeout: chưa xác nhận upload hoàn tất rõ ràng (không chuyển bước).")

    def _count_uploaded_file_chips(self, page: Any) -> int:
        selectors = [
            # Theo HTML thực tế Gemini sau upload
            "uploader-file-preview.file-preview-chip",
            "uploader-file-preview-container uploader-file-preview",
            "button.image-preview img[data-test-id='image-preview']",
            "img[data-test-id='image-preview']",
            ".attachment-preview-wrapper .file-preview-chip",
            # Chip ảnh như screenshot UI
            "img",
            # Một số UI render bằng thumbnail div
            "[data-test-id*='attachment']",
            "[data-test-id*='upload']",
            ".upload-card",
        ]
        best = 0
        for sel in selectors:
            try:
                cnt = page.locator(sel).count()
                if cnt > best:
                    best = cnt
            except Exception:
                continue
        # Loại trừ nhiễu lớn từ ảnh/icon ngoài vùng composer:
        # nếu số quá nhiều bất thường thì xem như không tin cậy.
        if best > 100:
            return 0
        return best

    def _visible_count(self, page: Any, selector: str, *, limit: int = 20) -> int:
        try:
            loc = page.locator(selector)
            cnt = loc.count()
            visible = 0
            for i in range(min(cnt, limit)):
                try:
                    if loc.nth(i).is_visible(timeout=100):
                        visible += 1
                except Exception:
                    continue
            return visible
        except Exception:
            return 0

    def _get_composer_state(self, page: Any) -> dict[str, Any]:
        """
        Snapshot duy nhất cho vùng composer Gemini.
        Tránh mỗi hàm tự đoán Stop/Send bằng selector riêng dẫn tới lệch trạng thái.
        """
        default = {
            "action": "unknown",
            "stop": False,
            "send": False,
            "send_ready": False,
            "busy": False,
            "box_ready": False,
        }
        try:
            state = page.evaluate(
                r"""
                () => {
                  const visible = (el) => {
                    if (!el) return false;
                    const r = el.getBoundingClientRect();
                    const s = window.getComputedStyle(el);
                    return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none';
                  };
                  const textRoot = document.querySelector("rich-textarea")
                    || document.querySelector("textarea")
                    || document.querySelector("[contenteditable='true']")
                    || document.querySelector("[role='textbox']");
                  const root = document.querySelector("[data-node-type='input-area']")
                    || document.querySelector(".input-area")
                    || textRoot?.closest("[data-node-type='input-area'], .input-area, form")
                    || document.body;
                  const boxReady = !!textRoot && visible(textRoot);
                  const globalStopButton = Array.from(document.querySelectorAll("button")).find((btn) => {
                    if (!visible(btn)) return false;
                    const cls = (btn.getAttribute("class") || "").toLowerCase();
                    const aria = (btn.getAttribute("aria-label") || "").toLowerCase();
                    const title = (btn.getAttribute("title") || "").toLowerCase();
                    const html = (btn.innerHTML || "").toLowerCase();
                    const container = btn.closest(".send-button-container");
                    const joined = `${cls} ${aria} ${title} ${html}`;
                    return (
                      (cls.includes("send-button") && cls.includes("stop"))
                      || aria.includes("stop response")
                      || aria.includes("stop")
                      || aria.includes("dừng")
                      || joined.includes("stop response")
                      || joined.includes("stop-icon")
                      || joined.includes("blue-circle")
                      || joined.includes("stop_circle")
                      || !!btn.querySelector(".blue-circle.stop-icon")
                      || !!btn.querySelector("[class*='stop-icon'], .blue-circle")
                      || !!btn.querySelector("mat-icon[fonticon='stop'], mat-icon[data-mat-icon-name='stop']")
                      || html.includes("blue-circle stop-icon")
                      || html.includes("stop-icon")
                      || html.includes("blue-circle")
                      || html.includes('fonticon="stop"')
                      || html.includes("fonticon='stop'")
                      || html.includes('data-mat-icon-name="stop"')
                      || html.includes("data-mat-icon-name='stop'")
                      || (!!container && container.classList.contains("visible") && container.classList.contains("disabled") && cls.includes("send-button") && html.includes("stop-icon"))
                    );
                  });
                  if (globalStopButton) {
                    return {
                      action: "stop",
                      stop: true,
                      send: false,
                      send_ready: false,
                      busy: true,
                      box_ready: boxReady
                    };
                  }
                  const globalSendButton = Array.from(document.querySelectorAll(
                    "button.send-button.submit[aria-label*='Send message'], div.send-button-container.visible:not(.disabled) button.send-button.submit"
                  )).find((btn) => {
                    if (!visible(btn)) return false;
                    const aria = (btn.getAttribute("aria-label") || "").toLowerCase();
                    const cls = (btn.getAttribute("class") || "").toLowerCase();
                    const container = btn.closest(".send-button-container");
                    const disabled = btn.disabled
                      || btn.getAttribute("aria-disabled") === "true"
                      || btn.hasAttribute("disabled")
                      || (!!container && container.classList.contains("disabled"));
                    const visibleSendIcon = Array.from(btn.querySelectorAll(
                      "mat-icon[fonticon='send'], mat-icon[data-mat-icon-name='send']"
                    )).some((ic) => visible(ic) && !ic.classList.contains("hidden"));
                    return cls.includes("send-button")
                      && cls.includes("submit")
                      && aria.includes("send")
                      && !disabled
                      && visibleSendIcon
                      && !btn.querySelector(".blue-circle.stop-icon, [class*='stop-icon'], mat-icon[fonticon='stop'], mat-icon[data-mat-icon-name='stop']");
                  });
                  if (globalSendButton) {
                    return {
                      action: "send",
                      stop: false,
                      send: true,
                      send_ready: true,
                      busy: false,
                      box_ready: boxReady
                    };
                  }
                  const buttons = Array.from(root.querySelectorAll("button")).filter(visible);
                  const candidates = buttons.filter((btn) => {
                    const aria = (btn.getAttribute("aria-label") || "").toLowerCase();
                    const cls = (btn.getAttribute("class") || "").toLowerCase();
                    return cls.includes("send-button")
                      || !!btn.closest(".send-button-container")
                      || aria.includes("send")
                      || aria.includes("submit")
                      || aria.includes("gửi")
                      || aria.includes("stop")
                      || aria.includes("dừng");
                  });
                  candidates.sort((a, b) => {
                    const ar = a.getBoundingClientRect();
                    const br = b.getBoundingClientRect();
                    return (ar.top - br.top) || (ar.left - br.left);
                  });
                  const btn = candidates[candidates.length - 1] || null;
                  let action = "unknown";
                  let send = false;
                  let stop = false;
                  let sendReady = false;
                  if (btn) {
                    const cls = (btn.getAttribute("class") || "").toLowerCase();
                    const aria = (btn.getAttribute("aria-label") || "").toLowerCase();
                    const title = (btn.getAttribute("title") || "").toLowerCase();
                    const text = (btn.innerText || "").toLowerCase();
                    const html = (btn.innerHTML || "").toLowerCase();
                    const joined = `${cls} ${aria} ${title} ${text} ${html}`;
                    const container = btn.closest(".send-button-container");
                    const disabled = btn.disabled
                      || btn.getAttribute("aria-disabled") === "true"
                      || btn.hasAttribute("disabled")
                      || (!!container && container.classList.contains("disabled"));
                    const visibleSendIcon = Array.from(btn.querySelectorAll(
                      "mat-icon[fonticon='send'], mat-icon[data-mat-icon-name='send']"
                    )).some((ic) => visible(ic) && !ic.classList.contains("hidden"));
                    const visibleStopIcon = Array.from(btn.querySelectorAll(
                      "[class*='stop-icon'], .blue-circle, mat-icon[fonticon='stop'], mat-icon[data-mat-icon-name='stop']"
                    )).some(visible);
                    stop = cls.includes(" stop")
                      || btn.classList.contains("stop")
                      || aria.includes("stop")
                      || aria.includes("dừng")
                      || joined.includes("cancel response")
                      || joined.includes("stop response")
                      || joined.includes("stop_circle")
                      || joined.includes("stop-icon")
                      || joined.includes("blue-circle")
                      || joined.includes("blue-circle stop-icon")
                      || joined.includes('fonticon="stop"')
                      || joined.includes("fonticon='stop'")
                      || joined.includes('data-mat-icon-name="stop"')
                      || joined.includes("data-mat-icon-name='stop'")
                      || visibleStopIcon;
                    send = !stop && visibleSendIcon && (
                      aria.includes("send")
                      || aria.includes("submit")
                      || aria.includes("gửi")
                      || cls.includes("submit")
                    );
                    action = stop ? "stop" : (send ? "send" : "unknown");
                    sendReady = send && !disabled;
                  }
                  const busy = stop
                    || Array.from(document.querySelectorAll("[role='progressbar'], mat-progress-spinner, mat-spinner, .spinner, .loading"))
                      .some(visible);
                  return { action, stop, send, send_ready: sendReady, busy, box_ready: boxReady };
                }
                """
            )
            if isinstance(state, dict):
                return {**default, **state}
        except Exception:
            pass
        return default

    def _wait_until_gemini_idle(self, page: Any, *, timeout_ms: int) -> None:
        """
        Chỉ cho qua bước khi Gemini thật sự idle:
        - Stop không còn hiển thị
        - Không còn progressbar rõ ràng
        Lưu ý: trước khi nhập prompt, ô trống thì Send có thể chưa ready.
        """
        deadline = time.time() + timeout_ms / 1000.0
        stable = 0
        while time.time() < deadline:
            state = self._get_composer_state(page)
            if not state["busy"]:
                stable += 1
                if stable >= 2:
                    return
            else:
                stable = 0
            time.sleep(0.35)
        raise RuntimeError("Gemini chưa idle (Stop chưa tắt hoặc vẫn còn progress).")

    def _prepare_gemini_composer_for_prompt(
        self,
        page: Any,
        *,
        timeout_ms: int,
        require_send_ready: bool = False,
    ) -> None:
        """
        Điểm chặn bắt buộc trước khi nhập prompt:
        - Gemini phải idle
        - Mode phải là Thinking
        - Sau khi đổi mode, kiểm tra idle lại một lần nữa
        """
        self._log("[INFO] Đang chuẩn bị composer: chờ idle trước khi chọn mode Thinking.")
        self._wait_until_gemini_idle(page, timeout_ms=timeout_ms)
        self._log("[INFO] Composer idle, bắt đầu chọn/xác nhận mode Thinking.")
        self._select_gemini_mode_thinking(page)
        if not self._is_mode_thinking_selected(page):
            raise RuntimeError('Chưa chọn được mode Gemini "Thinking"; dừng trước khi nhập prompt.')
        self._log("[INFO] Đã xác nhận mode Thinking, chờ composer ổn định trước khi nhập prompt.")
        self._wait_until_gemini_idle(page, timeout_ms=60_000)
        if require_send_ready:
            self._log("[INFO] Chờ Gemini xử lý upload xong: nút Send phải sẵn sàng trước khi nhập prompt.")
            self._wait_send_button_ready(page, timeout_ms=timeout_ms)

    def _select_gemini_mode_thinking(self, page: Any) -> None:
        """
        Sau upload, mở mode picker và chọn Thinking nếu có.
        """
        if self._is_mode_thinking_selected(page):
            self._log("[INFO] Mode Gemini đã là Thinking.")
            return

        openers = [
            "button[data-test-id='bard-mode-menu-button']",
            "button[aria-label*='Open mode picker']",
            "button:has-text('Fast')",
            "button:has-text('Thinking')",
            ".input-area button:has-text('Fast')",
            ".input-area button:has-text('Thinking')",
        ]
        opened = False
        for sel in openers:
            try:
                btn = page.locator(sel).last
                if btn.count() > 0 and btn.is_visible(timeout=700):
                    btn.click(timeout=3000)
                    opened = True
                    self._log("[INFO] Đã mở menu chọn mode Gemini.")
                    break
            except Exception:
                continue
        if not opened:
            raise RuntimeError('Không mở được menu chọn mode Gemini để chuyển sang "Thinking".')

        page.wait_for_timeout(350)
        candidates = [
            "[role='menuitem']:has-text('Thinking')",
            "button:has-text('Thinking')",
            "span.mode-title:has-text('Thinking')",
            "div.title-and-description:has-text('Thinking')",
            "mat-menu[data-test-id='desktop-nested-mode-menu'] div:has-text('Thinking')",
        ]
        for sel in candidates:
            try:
                item = page.locator(sel).first
                if item.count() > 0 and item.is_visible(timeout=900):
                    item.scroll_into_view_if_needed(timeout=1000)
                    item.click(timeout=3000)
                    page.wait_for_timeout(800)
                    if self._is_mode_thinking_selected(page):
                        self._log("[INFO] Đã chọn mode Gemini: Thinking.")
                        return
            except Exception:
                continue
        raise RuntimeError('Không chọn/xác nhận được mode Gemini "Thinking"; dừng trước khi nhập prompt.')

    def _is_mode_thinking_selected(self, page: Any) -> bool:
        checks = [
            "button[data-test-id='bard-mode-menu-button']:has-text('Thinking')",
            "[data-node-type='input-area'] button:has-text('Thinking')",
            ".input-area button:has-text('Thinking')",
            ".input-area-switch-label:has-text('Thinking')",
            "div[data-test-id='logo-pill-label-container']:has-text('Thinking')",
        ]
        for sel in checks:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0 and loc.is_visible(timeout=300):
                    return True
            except Exception:
                continue
        return False

    def _try_set_files_on_any_frame_input(self, *, page: Any, files: list[str], timeout_ms: int) -> bool:
        deadline = time.time() + max(1.0, timeout_ms / 1000.0)
        while time.time() < deadline:
            frames = [page]
            try:
                frames.extend(list(page.frames))
            except Exception:
                pass
            for fr in frames:
                for sel in ("input[type='file']", "input[accept*='image']", "input[accept*='video']"):
                    try:
                        loc = fr.locator(sel)
                        cnt = loc.count()
                        if cnt <= 0:
                            continue
                        for i in range(min(cnt, 3)):
                            try:
                                loc.nth(i).set_input_files(files, timeout=1000)
                                return True
                            except Exception:
                                continue
                    except Exception:
                        continue
            time.sleep(0.4)
        return False

    def _input_prompt_and_send(
        self,
        *,
        page: Any,
        prompt: str,
        require_send_ready_before_input: bool = False,
    ) -> None:
        box = page.locator("textarea, [contenteditable='true'], [role='textbox']").first
        box.wait_for(timeout=40_000)
        # Chỉ cần composer idle để nhập; Send chỉ ready sau khi đã có text.
        self._wait_composer_ready_for_input(
            page,
            timeout_ms=90_000,
            require_send_ready=require_send_ready_before_input,
        )
        self._log(f"[INFO] Composer trước khi nhập: {self._format_composer_state(page)}")
        page.wait_for_timeout(300)
        self._set_prompt_text(page=page, box=box, prompt=prompt)
        page.wait_for_timeout(1000)
        self._log(f"[INFO] Composer sau khi nhập prompt: {self._format_composer_state(page)}")
        self._wait_send_button_ready(page, timeout_ms=30_000)
        self._click_send(page)
        started = self._wait_generation_started_after_send(page, timeout_ms=20_000)
        if not started:
            state = self._get_composer_state(page)
            if state["send_ready"] and not state["stop"] and not state["busy"]:
                self._log("[WARNING] Send lần đầu chưa chạy, nút Send vẫn sẵn sàng; bấm Send lại lần 2.")
                self._click_send(page)
                started = self._wait_generation_started_after_send(page, timeout_ms=25_000)
        if not started:
            self._log(f"[WARNING] Chưa xác nhận được Gemini bắt đầu sau Send ({self._format_composer_state(page)}); chuyển sang watcher phản hồi.")
        self._log("[INFO] Đã bấm Send, bắt đầu chờ Gemini phản hồi.")

    def _set_prompt_text(self, *, page: Any, box: Any, prompt: str) -> None:
        """
        Không dùng press_sequentially cho prompt dài/multiline vì Gemini có thể bắt Enter
        và tự gửi khi prompt chưa nhập xong.
        """
        single_pass_prompt = prompt.replace("\r\n", "\n").replace("\r", "\n")
        try:
            box.click(timeout=5000)
            box.fill(single_pass_prompt, timeout=8000)
            return
        except Exception:
            pass
        try:
            box.evaluate(
                """
                (el, value) => {
                  el.focus();
                  if ('value' in el) {
                    el.value = value;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    return;
                  }
                  el.textContent = value;
                  el.dispatchEvent(new InputEvent('input', {
                    bubbles: true,
                    inputType: 'insertText',
                    data: value
                  }));
                }
                """,
                single_pass_prompt,
            )
            return
        except Exception:
            pass
        # Fallback cuối: paste clipboard thường an toàn hơn gõ từng phím Enter.
        try:
            box.click(timeout=5000)
            page.evaluate("text => navigator.clipboard.writeText(text)", single_pass_prompt)
            page.keyboard.press("Control+V")
            return
        except Exception:
            pass
        raise RuntimeError("Không nhập được prompt vào Gemini composer.")

    def _wait_composer_ready_for_input(
        self,
        page: Any,
        *,
        timeout_ms: int,
        require_send_ready: bool = False,
    ) -> None:
        """
        Chỉ dùng trước khi nhập prompt:
        - không được có nút Stop/square
        - không còn progressbar upload/generate
        - ô nhập phải visible
        """
        deadline = time.time() + timeout_ms / 1000.0
        stable = 0
        while time.time() < deadline:
            state = self._get_composer_state(page)
            # Trước khi nhập prompt chỉ cần composer không bận và ô nhập sẵn sàng.
            # Send chỉ có thể ready thật sau khi đã có text.
            ready = not state["busy"] and state["box_ready"] and state["action"] != "stop"
            if require_send_ready:
                ready = ready and state["send_ready"]
            if ready:
                stable += 1
                if stable >= 2:
                    return
            else:
                stable = 0
            time.sleep(0.35)
        state = self._get_composer_state(page)
        raise RuntimeError(
            f"Composer Gemini chưa sẵn sàng để nhập prompt "
            f"(action={state['action']}, stop={state['stop']}, busy={state['busy']}, "
            f"box_ready={state['box_ready']})."
        )

    def _format_composer_state(self, page: Any) -> str:
        state = self._get_composer_state(page)
        return (
            f"action={state['action']} stop={state['stop']} send={state['send']} "
            f"send_ready={state['send_ready']} busy={state['busy']} box_ready={state['box_ready']}"
        )

    def _wait_send_button_ready(self, page: Any, *, timeout_ms: int) -> None:
        deadline = time.time() + timeout_ms / 1000.0
        while time.time() < deadline:
            state = self._get_composer_state(page)
            # Composer snapshot là nguồn tin chính. Nếu nút Send thật đã ready thì
            # không để các selector loading/spinner rộng trong page chặn nhầm.
            if state["send_ready"]:
                return
            # Nếu đang hiện Stop/generate thật thì Gemini đang bận, chưa được bấm.
            if state["stop"] or state["busy"] or self._is_response_activity_visible(page):
                time.sleep(0.6)
                continue
            # Send button theo HTML người dùng cung cấp.
            send_selectors = [
                "button.send-button.submit[aria-label*='Send message']",
                "div.send-button-container button.send-button.submit",
                "button[aria-label*='Send message']",
                "button[aria-label*='Send']",
            ]
            for sel in send_selectors:
                try:
                    btn = page.locator(sel).last
                    if btn.count() > 0 and btn.is_visible(timeout=300):
                        aria_disabled = (btn.get_attribute("aria-disabled") or "").strip().lower()
                        disabled_attr = btn.get_attribute("disabled")
                        if (
                            aria_disabled not in {"true", "1"}
                            and disabled_attr is None
                            and not self._button_looks_like_stop(btn)
                            and self._button_looks_like_send(btn)
                        ):
                            return
                except Exception:
                    continue
            time.sleep(0.4)
        raise RuntimeError(f"Timeout chờ nút Send sẵn sàng ({self._format_composer_state(page)}).")

    def _is_send_ready_visible(self, page: Any) -> bool:
        state = self._get_composer_state(page)
        if state["send_ready"]:
            return True
        if state["busy"]:
            return False
        send_selectors = [
            "button.send-button.submit[aria-label*='Send message']",
            "div.send-button-container button.send-button.submit",
            "button[aria-label*='Send message']",
            "button[aria-label*='Send']",
            "button[aria-label*='Submit']",
        ]
        for sel in send_selectors:
            try:
                btn = page.locator(sel).last
                if btn.count() > 0 and btn.is_visible(timeout=200):
                    aria_disabled = (btn.get_attribute("aria-disabled") or "").strip().lower()
                    disabled_attr = btn.get_attribute("disabled")
                    if (
                        aria_disabled not in {"true", "1"}
                        and disabled_attr is None
                        and not self._button_looks_like_stop(btn)
                        and self._button_looks_like_send(btn)
                    ):
                        return True
            except Exception:
                continue
        return False

    def _is_gemini_busy(self, page: Any) -> bool:
        state = self._get_composer_state(page)
        if state["busy"]:
            return True
        if self._is_response_activity_visible(page):
            return True
        busy_selectors = [
            "[role='progressbar']",
            "mat-progress-spinner",
            "mat-spinner",
            ".spinner",
            ".loading",
        ]
        for sel in busy_selectors:
            try:
                if self._visible_count(page, sel, limit=3) > 0:
                    return True
            except Exception:
                continue
        return False

    def _is_response_activity_visible(self, page: Any, *, include_analysis: bool = False) -> bool:
        activity_selectors = [
            "[aria-label*='Generating']",
            "[aria-label*='generating']",
            "[aria-label*='Đang tạo']",
            "[aria-label*='Loading']",
            "[aria-label*='loading']",
        ]
        if include_analysis:
            activity_selectors.append("text=Analysis")
        for sel in activity_selectors:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0 and loc.is_visible(timeout=150):
                    return True
            except Exception:
                continue
        try:
            body = (page.locator("body").inner_text(timeout=500) or "").lower()
            tokens = ["generating", "đang tạo"]
            if any(token in body for token in tokens):
                return True
        except Exception:
            pass
        return False

    def _is_stop_visible(self, page: Any) -> bool:
        if self._get_composer_state(page)["stop"]:
            return True
        if self._page_has_stop_button(page):
            return True
        for btn_sel in (
            "button.send-button.stop",
            "button.mdc-icon-button.send-button.stop",
            "button.mat-mdc-icon-button.send-button.stop",
            "button.send-button[aria-label*='Stop response']",
            "button.send-button[aria-label*='Stop']",
            "div.send-button-container.disabled button.send-button.stop",
            "div.send-button-container.visible.disabled button.send-button",
            "div.send-button-container.visible.disabled button.send-button[aria-label*='Stop response']",
            "button.send-button.submit",
            "div.send-button-container button.send-button.submit",
            "button[aria-label*='Stop']",
            "button[aria-label*='stop' i]",
            "button[aria-label*='Dừng']",
            "button[aria-label*='dừng' i]",
            "button:has-text('Stop')",
            "button:has-text('Dừng')",
        ):
            try:
                btn = page.locator(btn_sel).last
                if btn.count() > 0 and btn.is_visible(timeout=200) and self._button_looks_like_stop(btn):
                    return True
            except Exception:
                continue
        # Trường hợp phổ biến: nút submit vẫn giữ class cũ nhưng icon đã chuyển thành stop/square.
        for stop_icon_sel in (
            "button.send-button.stop mat-icon[fonticon='stop']",
            "button.send-button.stop mat-icon[data-mat-icon-name='stop']",
            "button.send-button .blue-circle.stop-icon mat-icon[fonticon='stop']",
            "button.send-button .blue-circle.stop-icon mat-icon[data-mat-icon-name='stop']",
            "button.send-button.stop .blue-circle.stop-icon",
            "div.send-button-container.disabled .blue-circle.stop-icon",
            "div.send-button-container.visible.disabled .blue-circle.stop-icon",
            "button.send-button.submit mat-icon[fonticon='stop']",
            "button.send-button.submit mat-icon[data-mat-icon-name='stop']",
            "button.send-button.submit mat-icon[fonticon='square']",
            "button.send-button.submit mat-icon[data-mat-icon-name='square']",
            "button.send-button.submit mat-icon[fonticon='stop_circle']",
        ):
            try:
                ic = page.locator(stop_icon_sel).last
                if ic.count() > 0 and ic.is_visible(timeout=200):
                    return True
            except Exception:
                continue
        for stop_sel in (
            "button[aria-label*='Stop response']",
            "button[aria-label*='Stop']",
            "button[aria-label*='Dừng']",
            "button.stop-button",
        ):
            try:
                sb = page.locator(stop_sel).last
                if sb.count() > 0 and sb.is_visible(timeout=200):
                    return True
            except Exception:
                continue
        # Trường hợp nút gửi đổi icon từ send sang icon khác (square/stop) nhưng class giữ nguyên.
        try:
            icon = page.locator("button.send-button mat-icon").last
            if icon.count() > 0 and icon.is_visible(timeout=200):
                fonticon = str(icon.get_attribute("fonticon") or "").strip().lower()
                mat_name = str(icon.get_attribute("data-mat-icon-name") or "").strip().lower()
                if fonticon and fonticon not in {"send"}:
                    return True
                if mat_name and mat_name not in {"send"}:
                    return True
        except Exception:
            pass
        return False

    def _composer_action_button_state(self, page: Any) -> str:
        return str(self._get_composer_state(page).get("action") or "unknown")

    def _button_looks_like_stop(self, btn: Any) -> bool:
        try:
            cls = str(btn.get_attribute("class") or "").strip().lower()
            aria = str(btn.get_attribute("aria-label") or "").strip().lower()
            title = str(btn.get_attribute("title") or "").strip().lower()
            text = str(btn.inner_text(timeout=200) or "").strip().lower()
            html = str(btn.inner_html(timeout=200) or "").strip().lower()
            joined = " ".join([cls, aria, title, text, html])
            if any(token in joined for token in ("stop", "dừng", "square", "cancel response")):
                return True
            # UI Gemini có lúc giữ aria-label Send nhưng icon chuyển thành ô vuông SVG.
            if any(
                token in html
                for token in (
                    "<rect",
                    "stop_circle",
                    "blue-circle stop-icon",
                    "data-mat-icon-name=\"stop\"",
                    "fonticon=\"stop\"",
                )
            ):
                return True
            icon = btn.locator("mat-icon, [data-mat-icon-name], [fonticon]").last
            if icon.count() > 0:
                fonticon = str(icon.get_attribute("fonticon") or "").strip().lower()
                mat_name = str(icon.get_attribute("data-mat-icon-name") or "").strip().lower()
                icon_cls = str(icon.get_attribute("class") or "").strip().lower()
                icon_text = str(icon.inner_text(timeout=200) or "").strip().lower()
                icon_state = " ".join([fonticon, mat_name, icon_cls, icon_text])
                if any(token in icon_state for token in ("stop", "square", "cancel")):
                    return True
                if (fonticon or mat_name or icon_text) and not any(
                    token in icon_state for token in ("send", "arrow", "submit")
                ):
                    return True
        except Exception:
            return False
        return False

    def _button_looks_like_send(self, btn: Any) -> bool:
        try:
            cls = str(btn.get_attribute("class") or "").strip().lower()
            aria = str(btn.get_attribute("aria-label") or "").strip().lower()
            aria_disabled = str(btn.get_attribute("aria-disabled") or "").strip().lower()
            title = str(btn.get_attribute("title") or "").strip().lower()
            text = str(btn.inner_text(timeout=200) or "").strip().lower()
            html = str(btn.evaluate("(el) => el.outerHTML") or "").strip().lower()
            joined = " ".join([cls, aria, title, text, html])
            if aria_disabled in {"true", "1"} or btn.get_attribute("disabled") is not None:
                return False
            if any(token in joined for token in ("stop", "dừng", "square", "cancel response", "stop response")):
                return False
            if any(token in joined for token in ("blue-circle", "stop-icon", "fonticon=\"stop\"", "data-mat-icon-name=\"stop\"")):
                return False
            icon = btn.locator("mat-icon, [data-mat-icon-name], [fonticon], svg").last
            if icon.count() > 0:
                icon_cls = str(icon.get_attribute("class") or "").strip().lower()
                if "hidden" in icon_cls:
                    return False
                icon_state = str(icon.evaluate("(el) => el.outerHTML") or "").strip().lower()
                if any(token in icon_state for token in ("stop", "square", "cancel")):
                    return False
                if any(token in icon_state for token in ("fonticon=\"send\"", "data-mat-icon-name=\"send\"")) and any(
                    token in joined for token in ("send", "gửi", "submit")
                ):
                    return True
        except Exception:
            return False
        return False

    def _page_has_stop_button(self, page: Any) -> bool:
        try:
            return bool(
                page.evaluate(
                    """
                    () => {
                      const visible = (el) => {
                        const r = el.getBoundingClientRect();
                        const s = window.getComputedStyle(el);
                        return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none';
                      };
                      for (const btn of Array.from(document.querySelectorAll('button'))) {
                        if (!visible(btn)) continue;
                        const aria = (btn.getAttribute('aria-label') || '').toLowerCase();
                        const title = (btn.getAttribute('title') || '').toLowerCase();
                        const text = (btn.innerText || '').toLowerCase();
                        const html = (btn.innerHTML || '').toLowerCase();
                        const cls = (btn.getAttribute('class') || '').toLowerCase();
                        const joined = `${cls} ${aria} ${title} ${text} ${html}`;
                        const container = btn.closest('.send-button-container');
                        if (cls.includes('send-button') && cls.includes('stop')) return true;
                        if (aria.includes('stop response')) return true;
                        if (btn.querySelector('.blue-circle.stop-icon, mat-icon[fonticon="stop"], mat-icon[data-mat-icon-name="stop"]')) return true;
                        if (joined.includes('stop') || joined.includes('dừng') || joined.includes('stop_circle')) return true;
                        if (html.includes('blue-circle stop-icon') || html.includes('fonticon="stop"') || html.includes('data-mat-icon-name="stop"')) return true;
                        if (container && container.classList.contains('visible') && container.classList.contains('disabled') && cls.includes('send-button') && html.includes('stop-icon')) return true;
                        if ((btn.matches('.send-button, .submit') || btn.closest('.send-button-container')) && (html.includes('<rect') || html.includes('blue-circle stop-icon'))) return true;
                      }
                      return false;
                    }
                    """
                )
            )
        except Exception:
            return False

    def _click_send(self, page: Any) -> None:
        # Nếu đang ở trạng thái Stop/generating thì coi là chưa sẵn sàng gửi.
        state = self._get_composer_state(page)
        if state["stop"] or state["busy"]:
            raise RuntimeError("Gemini đang ở trạng thái Stop/Generating, chưa thể Send prompt mới.")
        if state["send_ready"]:
            clicked = self._click_composer_action_button(page)
            if clicked:
                page.wait_for_timeout(200)
                return
        patterns = [
            "button.send-button.submit[aria-label*='Send message']",
            "div.send-button-container button.send-button.submit",
            "button[aria-label*='Send']",
            "button[aria-label*='Gửi']",
            "button[aria-label*='Submit']",
        ]
        for sel in patterns:
            try:
                btn = page.locator(sel).last
                if btn.count() > 0 and btn.is_visible(timeout=1000):
                    if btn.get_attribute("disabled") is None:
                        if self._button_looks_like_stop(btn) or not self._button_looks_like_send(btn):
                            continue
                        # Retry click ngắn để tránh trường hợp click đầu không ăn.
                        btn.click(timeout=4000)
                        page.wait_for_timeout(200)
                        if not self._is_stop_visible(page):
                            btn.click(timeout=4000)
                        return
            except Exception:
                continue
        raise RuntimeError("Không tìm được nút Send khả dụng (hoặc đang ở trạng thái Stop).")

    def _click_composer_action_button(self, page: Any) -> bool:
        try:
            return bool(
                page.evaluate(
                    """
                    () => {
                      const visible = (el) => {
                        const r = el.getBoundingClientRect();
                        const s = window.getComputedStyle(el);
                        return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none';
                      };
                      const composers = [
                        document.querySelector("[data-node-type='input-area']"),
                        document.querySelector(".input-area"),
                        document.querySelector("rich-textarea")?.closest("[data-node-type='input-area'], .input-area, form"),
                        document.querySelector("textarea")?.closest("[data-node-type='input-area'], .input-area, form"),
                        document.querySelector("[contenteditable='true']")?.closest("[data-node-type='input-area'], .input-area, form")
                      ].filter(Boolean);
                      const root = composers[0] || document.body;
                      const buttons = Array.from(root.querySelectorAll("button")).filter(visible).filter((btn) => {
                        const aria = (btn.getAttribute("aria-label") || "").toLowerCase();
                        const cls = (btn.getAttribute("class") || "").toLowerCase();
                        return cls.includes("send-button")
                          || !!btn.closest(".send-button-container")
                          || aria.includes("send")
                          || aria.includes("submit")
                          || aria.includes("gửi");
                      });
                      if (!buttons.length) return false;
                      buttons.sort((a, b) => {
                        const ar = a.getBoundingClientRect();
                        const br = b.getBoundingClientRect();
                        return (ar.top - br.top) || (ar.left - br.left);
                      });
                      const btn = buttons[buttons.length - 1];
                      const joined = `${btn.getAttribute("class") || ""} ${btn.getAttribute("aria-label") || ""} ${btn.innerHTML || ""}`.toLowerCase();
                      if (joined.includes("stop") || joined.includes("dừng") || joined.includes("blue-circle stop-icon")) return false;
                      if (btn.disabled || btn.getAttribute("aria-disabled") === "true" || btn.hasAttribute("disabled")) return false;
                      btn.click();
                      return true;
                    }
                    """
                )
            )
        except Exception:
            return False

    def _wait_generation_started_after_send(self, page: Any, *, timeout_ms: int) -> bool:
        deadline = time.time() + timeout_ms / 1000.0
        while time.time() < deadline:
            if self._is_stop_visible(page):
                return True
            if self._composer_action_button_state(page) == "stop":
                return True
            if self._is_response_activity_visible(page, include_analysis=True):
                return True
            time.sleep(0.25)
        self._log("[WARNING] Đã bấm Send nhưng chưa detect được Stop rõ ràng; tiếp tục chờ phản hồi.")
        return False

    def _wait_gemini_response_started(
        self,
        page: Any,
        timeout_ms: int = 60000,
        baseline_text: str = "",
        baseline_count: int = 0,
    ) -> None:
        deadline = time.time() + timeout_ms / 1000.0
        while time.time() < deadline:
            body = ""
            try:
                body = page.locator("body").inner_text(timeout=2000).lower()
            except Exception:
                pass
            for s in ("stop", "dừng", "generating", "đang tạo"):
                if s in body:
                    self._log("[INFO] Gemini đã bắt đầu phản hồi.")
                    return
            if self._is_stop_visible(page):
                self._log("[INFO] Gemini đã bắt đầu phản hồi (detect stop icon).")
                return
            if self._is_response_activity_visible(page, include_analysis=True):
                self._log("[INFO] Gemini đã bắt đầu phản hồi (detect activity).")
                return
            if self._count_response_blocks(page) > baseline_count:
                self._log("[INFO] Gemini đã bắt đầu phản hồi (detect response block mới).")
                return
            cur = self._get_last_gemini_response_text(page)
            if cur and cur != baseline_text:
                self._log("[INFO] Gemini đã bắt đầu phản hồi (detect text).")
                return
            time.sleep(1.0)
        raise RuntimeError("Gemini chưa bắt đầu trả lời sau khi gửi prompt.")

    def _wait_gemini_response_complete_and_get_text(
        self,
        page: Any,
        timeout_ms: int = 600000,
        baseline_text: str = "",
        baseline_count: int = 0,
    ) -> str:
        deadline = time.time() + timeout_ms / 1000.0
        stable_rounds = 0
        stale_stop_rounds = 0
        last = ""
        saw_stop = False
        last_debug = 0.0
        while time.time() < deadline:
            body = ""
            try:
                body = page.locator("body").inner_text(timeout=4000).lower()
            except Exception:
                pass
            stop_visible = self._is_stop_visible(page)
            if stop_visible:
                saw_stop = True
            activity_visible = self._is_response_activity_visible(page)
            if activity_visible:
                saw_stop = True
            progress = self._visible_count(page, "[role='progressbar']")
            cur = self._get_last_gemini_response_text(page)
            if not cur:
                cur = self._get_visible_response_text_fallback(page)
            is_new_response = bool(cur) and cur != baseline_text
            has_new_block = self._count_response_blocks(page) > baseline_count
            composer_state = self._get_composer_state(page)
            composer_ready = composer_state["box_ready"] and (not composer_state["busy"])
            # Gemini UI có thể giữ activity/loading ảo trong DOM sau khi đã trả lời xong.
            # Nếu Stop đã mất, composer nhập lại được và không còn progressbar thì activity đó không còn đáng tin.
            effective_activity = activity_visible and not (composer_ready and not stop_visible and progress == 0)
            body_generating = any(s in body for s in ("generating", "đang tạo")) and not (
                composer_ready and not stop_visible and progress == 0
            )
            still_generating = stop_visible or effective_activity or body_generating
            if is_new_response and cur == last and not still_generating and progress == 0:
                stable_rounds += 1
            else:
                stable_rounds = 0
            # Gemini đôi khi giữ nút Stop/square trong UI dù không còn activity thật
            # và text đã đứng yên. Trường hợp này nếu chờ Stop mất sẽ kẹt vô hạn.
            stale_stop = (
                stop_visible
                and (not effective_activity)
                and (not body_generating)
                and progress == 0
                and bool(cur)
                and cur == last
            )
            if stale_stop:
                stale_stop_rounds += 1
            else:
                stale_stop_rounds = 0
            last = cur
            # Hoàn tất khi text ổn định + hết generating + composer quay lại sẵn sàng.
            # Quy tắc chốt theo yêu cầu:
            # - Đã từng thấy Stop (đang chạy)
            # - Sau đó Stop biến mất
            # - Composer nhập lại được. Khi ô trống Gemini có thể hiện mic, không có Send.
            response_started = saw_stop or is_new_response or has_new_block
            response_done_by_button_state = (
                response_started
                and (not stop_visible)
                and (not effective_activity)
                and composer_ready
            )
            if stable_rounds >= 2 and (is_new_response or has_new_block) and response_done_by_button_state:
                self._log("[INFO] Gemini đã phản hồi hoàn tất.")
                return cur
            if stale_stop_rounds >= 10 and bool(cur) and response_started:
                self._log(
                    "[WARNING] Gemini còn hiện Stop nhưng text đã ổn định và không còn activity; "
                    "coi phản hồi là hoàn tất để tránh kẹt."
                )
                return cur
            now = time.time()
            if now - last_debug >= 15:
                last_debug = now
                self._log(
                    "[INFO] Đang chờ Gemini hoàn tất | "
                    f"stop={stop_visible} activity={activity_visible} effective_activity={effective_activity} "
                    f"body_generating={body_generating} composer_ready={composer_ready} "
                    f"stable={stable_rounds} new_text={is_new_response} new_block={has_new_block} "
                    f"text_len={len(cur or '')} stale_stop={stale_stop_rounds}"
                )
            time.sleep(3.0)
        self._capture(page, "gemini_response_timeout")
        raise TimeoutError("Timeout khi chờ Gemini trả lời hoàn tất.")

    def _get_last_gemini_response_text(self, page: Any) -> str:
        selectors = [
            "message-content",
            "model-response",
            ".model-response",
            ".model-response-text",
            ".response-container",
            ".response-content",
            "div.markdown",
            "markdown",
            "[data-response-index]",
            "[data-testid*='response']",
            "[data-test-id*='model']",
            # Gemini UI mới thường render text trả lời trong vùng message user/model tách nhau.
            "div[data-test-id*='response']",
        ]
        for sel in selectors:
            try:
                loc = page.locator(sel)
                cnt = loc.count()
                if cnt > 0:
                    txt = loc.nth(cnt - 1).inner_text(timeout=1500).strip()
                    if txt:
                        return txt
            except Exception:
                continue
        # Không fallback về toàn bộ main/body vì dễ lẫn text composer làm sai state-machine.
        return ""

    def _get_visible_response_text_fallback(self, page: Any) -> str:
        """
        Gemini UI thay selector thường xuyên. Khi các selector response không bắt được,
        lấy text vùng nội dung chính nhưng loại composer/input để vẫn parse được JSON.
        """
        try:
            txt = page.evaluate(
                r"""
                () => {
                  const root = document.querySelector("main") || document.body;
                  const clone = root.cloneNode(true);
                  const removeSelectors = [
                    "[data-node-type='input-area']",
                    ".input-area",
                    "rich-textarea",
                    "textarea",
                    "[contenteditable='true']",
                    "[role='textbox']",
                    "button",
                    "nav",
                    "header"
                  ];
                  for (const sel of removeSelectors) {
                    for (const el of Array.from(clone.querySelectorAll(sel))) {
                      el.remove();
                    }
                  }
                  return (clone.innerText || clone.textContent || "").trim();
                }
                """
            )
            txt = str(txt or "").strip()
            if "{" in txt and "}" in txt:
                return txt
        except Exception:
            pass
        return ""

    def _count_response_blocks(self, page: Any) -> int:
        selectors = [
            "message-content",
            ".model-response-text",
            "div.markdown",
            "[data-response-index]",
            "div[data-test-id*='response']",
        ]
        max_count = 0
        for sel in selectors:
            try:
                cnt = page.locator(sel).count()
                if cnt > max_count:
                    max_count = cnt
            except Exception:
                continue
        return max_count

    def _start_new_chat_if_possible(self, page: Any) -> None:
        for sel in (
            "button[aria-label*='New chat']",
            "button:has-text('New chat')",
            "[data-test-id*='new-chat']",
            "a:has-text('New chat')",
        ):
            try:
                loc = page.locator(sel).first
                if loc.count() > 0 and loc.is_visible(timeout=500):
                    loc.click(timeout=2000)
                    page.wait_for_timeout(1000)
                    self._log("[INFO] Đã mở chat mới trước lượt phân tích.")
                    return
            except Exception:
                continue

    def _capture(self, page: Any, stem: str) -> None:
        out = self._paths["screenshots"] / f"{_safe_slug(stem)}_{int(time.time())}.png"
        try:
            page.screenshot(path=str(out), full_page=True)
        except Exception:
            pass


class GeminiResultParser:
    def extract_json(self, raw_text: str) -> dict[str, Any]:
        text = str(raw_text or "").strip()
        if not text:
            raise ValueError("Gemini output rỗng")
        text = self._strip_md_codeblock(text)
        for candidate in self._collect_json_object_candidates(text):
            try:
                obj = json.loads(candidate)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                continue
        return self.repair_json_if_needed(text)

    def repair_json_if_needed(self, raw_text: str) -> dict[str, Any]:
        txt = raw_text.strip()
        txt = re.sub(r",\s*([}\]])", r"\1", txt)
        txt = txt.replace("\u201c", '"').replace("\u201d", '"').replace("\u2019", "'")
        for candidate in self._collect_json_object_candidates(txt):
            try:
                obj = json.loads(candidate)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                continue
        raise ValueError("Không parse được JSON Gemini")

    @staticmethod
    def _strip_md_codeblock(text: str) -> str:
        m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, re.IGNORECASE)
        return m.group(1).strip() if m else text

    @staticmethod
    def _collect_json_object_candidates(text: str) -> list[str]:
        out: list[str] = []
        depth = 0
        start = -1
        in_str = False
        esc = False
        for i, ch in enumerate(text):
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
                continue
            if ch == "{":
                if depth == 0:
                    start = i
                depth += 1
            elif ch == "}" and depth > 0:
                depth -= 1
                if depth == 0 and start >= 0:
                    out.append(text[start : i + 1])
                    start = -1
        if text.startswith("{") and text.endswith("}"):
            out.append(text)
        return out


class SubjectBibleBuilder:
    def build(self, parsed: dict[str, Any]) -> list[dict[str, Any]]:
        rows = parsed.get("subjects")
        return rows if isinstance(rows, list) else []


class EnvironmentBibleBuilder:
    def build(self, parsed: dict[str, Any]) -> list[dict[str, Any]]:
        rows = parsed.get("environments")
        return rows if isinstance(rows, list) else []


class SceneBreakdownBuilder:
    def build(self, parsed: dict[str, Any]) -> list[dict[str, Any]]:
        rows = parsed.get("scene_breakdown")
        timeline = parsed.get("master_timeline")
        if isinstance(timeline, list):
            out_timeline: list[dict[str, Any]] = []
            for idx, row in enumerate(timeline, start=1):
                if isinstance(row, dict):
                    desc = str(
                        row.get("description")
                        or row.get("action")
                        or row.get("event")
                        or row.get("beat")
                        or row.get("summary")
                        or ""
                    ).strip()
                    rr = dict(row)
                    rr.setdefault("description", desc or f"Timeline event {idx}")
                    out_timeline.append(rr)
                elif str(row).strip():
                    out_timeline.append({"description": str(row).strip()})
            if out_timeline and (not isinstance(rows, list) or len(out_timeline) > len(rows)):
                return out_timeline
        beats = parsed.get("detailed_story_beats")
        if isinstance(beats, list):
            out = []
            for idx, row in enumerate(beats, start=1):
                if isinstance(row, dict):
                    desc = str(row.get("description") or row.get("action") or row.get("event") or row.get("beat") or row.get("summary") or "").strip()
                    rr = dict(row)
                    rr.setdefault("description", desc or f"Story beat {idx}")
                    out.append(rr)
                elif str(row).strip():
                    out.append({"description": str(row).strip()})
            if out and (not isinstance(rows, list) or len(out) > len(rows)):
                return out
        if isinstance(rows, list) and rows:
            return rows
        return []


class StyleAnalyzer:
    def build(self, parsed: dict[str, Any]) -> dict[str, Any]:
        val = parsed.get("style_analysis")
        return val if isinstance(val, dict) else {}


class StoryMapBuilder:
    def build(self, parsed: dict[str, Any]) -> dict[str, Any]:
        val = parsed.get("story_map")
        return val if isinstance(val, dict) else {}


class SubjectReplacementEngine:
    def apply(self, *, parsed: dict[str, Any], replacement: dict[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any]]:
        cfg = dict(replacement or {})
        if not cfg.get("enabled"):
            return parsed, {}
        old_id = str(cfg.get("old_subject_id") or "").strip()
        new_subject = str(cfg.get("new_subject") or "").strip()
        if not old_id or not new_subject:
            return parsed, {}
        out = dict(parsed)
        repl_map: dict[str, Any] = {}
        subjects = out.get("subjects")
        if isinstance(subjects, list):
            new_rows = []
            for row in subjects:
                if not isinstance(row, dict):
                    new_rows.append(row)
                    continue
                if str(row.get("subject_id") or "").strip() == old_id:
                    old = str(row.get("appearance") or row.get("type") or "subject")
                    rr = dict(row)
                    rr["appearance"] = new_subject
                    rr["type"] = new_subject
                    new_rows.append(rr)
                    repl_map[old_id] = {"old": old, "new": new_subject}
                else:
                    new_rows.append(row)
            out["subjects"] = new_rows
        rp = str(out.get("reverse_prompt") or "")
        if rp and new_subject:
            out["reverse_prompt"] = rp.replace(old_id, new_subject)
        return out, repl_map


class ReversePromptBuilder:
    def build(self, *, parsed: dict[str, Any], job: ReverseVideoJob) -> str:
        story = str(parsed.get("main_story") or parsed.get("video_summary") or "")
        style = dict(parsed.get("style_analysis") or {})
        scenes = parsed.get("scene_breakdown") if isinstance(parsed.get("scene_breakdown"), list) else []
        subjects = parsed.get("subjects") if isinstance(parsed.get("subjects"), list) else []
        environments = parsed.get("environments") if isinstance(parsed.get("environments"), list) else []
        anchors = parsed.get("continuity_anchors") if isinstance(parsed.get("continuity_anchors"), dict) else {}
        character_bible = parsed.get("character_continuity_bible") if isinstance(parsed.get("character_continuity_bible"), dict) else {}
        story_bible = parsed.get("story_continuity_bible") if isinstance(parsed.get("story_continuity_bible"), dict) else {}
        fingerprint = parsed.get("visual_fingerprint") if isinstance(parsed.get("visual_fingerprint"), dict) else {}
        scene_chain = self._scene_chain(scenes)
        subject_text = self._list_summary(subjects, keys=("subject_id", "type", "appearance", "clothing", "distinctive_features", "emotion"), limit=4)
        env_text = self._list_summary(environments, keys=("name", "location", "description", "props", "weather", "background"), limit=3)
        neg = []
        sm = parsed.get("story_map")
        if isinstance(sm, dict):
            nr = sm.get("negative_rules")
            if isinstance(nr, list):
                neg = [str(x) for x in nr]
        return (
            f"Create an {job.duration_sec}-second {job.aspect_ratio} video for Google Flow / Veo 3.\n\n"
            "Use the source video analysis as a strict reference. The generated video must clearly preserve the same story logic, subject identity, environment logic, camera language, lighting, and motion continuity.\n\n"
            f"Main story spine:\n{story}\n\n"
            f"Character continuity bible:\n{self._dict_summary(character_bible) or subject_text or 'Keep the exact same subject identity, body scale, silhouette, outfit/surface, expression and emotional state.'}\n\n"
            f"Environment continuity bible:\n{env_text or 'Keep the same environment layout, props, background depth, weather, atmosphere, and spatial relationships.'}\n\n"
            f"Visual fingerprint that must not drift:\n{self._dict_summary(fingerprint) or 'Preserve recurring source-video details exactly.'}\n\n"
            f"Style reference:\nKeep the same analyzed style: "
            f"{style.get('visual_style','')}, {style.get('camera_style','')}, {style.get('lighting_style','')}, "
            f"{style.get('motion_style','')}, {style.get('mood','')}, pacing={style.get('pacing','')}.\n\n"
            f"Full chronological story timeline:\n{scene_chain or 'Preserve the full beginning-middle-end action chain from the keyframes.'}\n\n"
            f"Story continuity bible:\n{self._dict_summary(story_bible)}\n\n"
            "Opening/ending/action anchors:\n"
            f"- Opening frame: {anchors.get('opening_frame','')}\n"
            f"- Ending frame: {anchors.get('ending_frame','')}\n"
            f"- Subject motion path: {anchors.get('subject_motion_path','')}\n"
            f"- Camera motion path: {anchors.get('camera_motion_path','')}\n"
            f"- Transition logic: {anchors.get('scene_transition_logic','')}\n\n"
            "Continuity:\nKeep the same subject identity, environment, lighting, camera direction, lens feel, subject scale, background geometry, visual style, motion vector, and story pacing from start to end.\n\n"
            f"{self._realism_contract()}\n\n"
            f"Negative rules:\n{'; '.join(neg)}"
        ).strip()

    def _scene_chain(self, scenes: list[Any]) -> str:
        rows: list[str] = []
        for idx, row in enumerate(scenes, start=1):
            if not isinstance(row, dict):
                if str(row).strip():
                    rows.append(f"{idx}. {str(row).strip()}")
                continue
            desc = str(row.get("description") or row.get("action") or row.get("event") or row.get("beat") or "").strip()
            start = str(row.get("start_state") or "").strip()
            end = str(row.get("end_state") or "").strip()
            cause = str(row.get("cause_effect") or row.get("continuity_to_next") or "").strip()
            parts = [desc or f"Scene {idx}"]
            if start:
                parts.append(f"starts: {start}")
            if end:
                parts.append(f"ends: {end}")
            if cause:
                parts.append(f"cause/effect: {cause}")
            rows.append(f"{idx}. " + " | ".join(parts))
        return "\n".join(rows[:18])

    def _list_summary(self, rows: list[Any], *, keys: tuple[str, ...], limit: int) -> str:
        chunks: list[str] = []
        for row in rows[:limit]:
            if not isinstance(row, dict):
                continue
            vals = [str(row.get(k) or "").strip() for k in keys if str(row.get(k) or "").strip()]
            if vals:
                chunks.append(", ".join(vals))
        return "\n".join(f"- {x}" for x in chunks)

    def _dict_summary(self, row: dict[str, Any]) -> str:
        chunks: list[str] = []
        for key, value in row.items():
            if isinstance(value, list):
                txt = "; ".join(str(x) for x in value if str(x).strip())
            else:
                txt = str(value or "").strip()
            if txt:
                chunks.append(f"- {key}: {txt}")
        return "\n".join(chunks)

    def _realism_contract(self) -> str:
        return (
            "Realism and physical continuity contract:\n"
            "- Photorealistic, grounded, natural motion unless the source video clearly shows a stylized effect.\n"
            "- No random VFX, glow, particles, morphing, teleportation, impossible object changes, sudden costume changes, or fantasy transitions.\n"
            "- Preserve physical weight, inertia, contact with ground/objects, shadow direction, reflections, and realistic camera exposure.\n"
            "- All action must be caused by the previous action; no unrelated jump cuts or artificial scene resets."
        )


class ContinuousStoryEngine:
    def _zero_drift_contract(self, *, part: int, total_parts: int) -> str:
        is_final = part >= total_parts
        subject_frame_rule = (
            f"- SUBJECT-IN-FRAME LOCK (Part {part}/{total_parts}): The primary subject that drives the action must stay clearly visible (face and/or hands/body as in the source) and continue the same gesture thread. "
            "Forbidden: environment-only shots, potted-plant nursery cutaways, unrelated flower gardens, or removing the human/animal while the action is unfinished.\n"
            if not is_final
            else "- FINAL PART VISIBILITY: Widen or settle only if the assigned segment explicitly finishes the story; the same subject and same location must remain recognizable — no new place, no new person.\n"
        )
        return (
            "ZERO-DRIFT CONTRACT (non-negotiable — violated prompts are invalid):\n"
            "- BIOLOGY LOCK: One continuous real subject. Same apparent age, same skin texture on face AND hands (wrinkles, veins, pores, spots if visible in Part 1). "
            "Forbidden: younger hands, smoothed skin, beauty filter, de-aging, hand swap, body double look, silhouette change.\n"
            "- SINGLE-LOCATION LOCK: One coherent place from the source (same ground, same plant species/layout, same depth cues). "
            "Forbidden: jumping from wild brush to ornamental tropical flowers to commercial nursery pots unless the source video clearly shows that continuous path in order.\n"
            f"{subject_frame_rule}"
            "- LIGHTING / OPTICS LOCK: Match the opening clip's light quality (soft sun, overcast, shade). "
            "Forbidden: adding heavy lens flare, bloom, fake sun rays, golden-hour regrade, or Hollywood relight unless effects_continuity_bible explicitly allows it from the source.\n"
            "- STYLE LOCK: Same realism level, grain, sharpness, saturation, and color grade across all parts. Forbidden: glossy AI look, cartoon drift, or different film look per part.\n"
        ).strip()

    def build(
        self,
        *,
        final_prompt: str,
        scenes: list[dict[str, Any]],
        total_parts: int,
        parsed: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if total_parts < 2:
            return [{"part": 1, "title": "Part 1", "state_in": {}, "state_out": {}, "prompt": final_prompt}]
        out: list[dict[str, Any]] = []
        parsed = dict(parsed or {})
        bible = self._continuity_bible(parsed=parsed, scenes=scenes)
        series_bible = self._series_bible_text(bible=bible, parsed=parsed, scenes=scenes)
        reference_prompt = self._compact_reference_prompt(final_prompt)
        progression_plan = self._story_progression_plan(total_parts=total_parts, bible=bible, scenes=scenes)
        prev_out = {
            "final_frame_anchor": "the subject is mid-action in the same location, with the same camera angle, lighting, outfit, scale, and motion direction",
            "next_action": "continue the same physical motion without restarting",
            "camera_handoff": bible["camera_lock"],
            "motion_handoff": bible["motion_lock"],
        }
        story_beats = self._story_beats(total_parts)
        for idx in range(1, total_parts + 1):
            plan = progression_plan[idx - 1] if idx - 1 < len(progression_plan) else {}
            beat = str(plan.get("beat") or (story_beats[idx - 1] if idx - 1 < len(story_beats) else f"Continue with a new non-repeating story beat for part {idx}."))
            scene_desc = str(plan.get("action") or self._part_scene_focus(scenes=scenes, part=idx, total_parts=total_parts) or beat)
            state_in = {
                "first_frame_anchor": (bible.get("opening_frame") or "original opening frame") if idx == 1 else str(prev_out.get("final_frame_anchor") or ""),
                "previous_action": "" if idx == 1 else str(prev_out.get("next_action") or ""),
                "camera_handoff": bible["camera_lock"] if idx == 1 else str(prev_out.get("camera_handoff") or bible["camera_lock"]),
                "motion_handoff": bible["motion_lock"] if idx == 1 else str(prev_out.get("motion_handoff") or bible["motion_lock"]),
                "environment": bible["environment_lock"],
                "subject_identity": bible["subject_lock"],
            }
            state_out = {
                "final_frame_anchor": self._end_frame_anchor(
                    scene_desc=scene_desc,
                    beat=beat,
                    planned_end=str(plan.get("end_state") or ""),
                    part=idx,
                    total_parts=total_parts,
                    bible=bible,
                ),
                "next_action": self._next_action(scene_desc=scene_desc, beat=beat, part=idx, total_parts=total_parts),
                "camera_handoff": bible["camera_lock"],
                "motion_handoff": bible["motion_lock"],
            }
            first_frame_rules = (
                "FIRST FRAME LOCK:\n"
                "- Start from the original opening frame of the full reference story.\n"
                "- Establish the subject and begin only the first assigned timeline segment.\n"
                "- Do not jump ahead to later story beats assigned to later parts.\n"
                if idx == 1
                else (
                    "FIRST FRAME LOCK:\n"
                    f"- The first frame MUST visually match the final frame of Part {idx - 1}: {state_in['first_frame_anchor']}.\n"
                    "- Continue the same body pose, object position, camera angle, light direction, background layout, color palette, and motion direction.\n"
                    "- This part must continue the next assigned timeline segment. Do not recreate earlier source keyframes, opening action, or previous part scenes.\n"
                    "- Do not use a new establishing shot, new location, new outfit, new camera setup, fade-in, title card, or time jump.\n"
                )
            )
            end_frame_rules = (
                "END FRAME HANDOFF:\n"
                f"- End Part {idx} on this exact continuation state: {state_out['final_frame_anchor']}.\n"
                "- Leave the motion readable and ready for the next part; avoid a hard reset or disconnected ending.\n"
                if idx < total_parts
                else (
                    "END FRAME HANDOFF:\n"
                    f"- Resolve the full action naturally while preserving the same visual identity: {state_out['final_frame_anchor']}.\n"
                    "- End on a stable final composition, not a new unrelated scene.\n"
                )
            )
            continuity_block = (
                "GLOBAL CONTINUITY LOCKS FOR ALL PARTS:\n"
                f"- Subject identity lock: {bible['subject_lock']}.\n"
                f"- Environment lock: {bible['environment_lock']}.\n"
                f"- Visual style lock: {bible['style_lock']}.\n"
                f"- Camera lock: {bible['camera_lock']}.\n"
                f"- Lighting/color lock: {bible['lighting_lock']}.\n"
                f"- Motion/pacing lock: {bible['motion_lock']}.\n"
                "- Keep scale, lens feel, subject size in frame, background geometry, shadows, and color grading consistent across every part.\n\n"
                "MATCH-CUT RULES:\n"
                "- Treat all parts as consecutive clips from one continuous video, not separate generations.\n"
                "- The cut between parts should feel invisible when videos are joined together.\n"
                "- No scene reset, no duplicated opening, no repeated first action, no sudden costume/environment/camera/lighting change.\n"
                "- Use the same action vector: if the subject moves left-to-right, forward, upward, turning, falling, or reaching, preserve that direction into the next part.\n\n"
            )
            realism_block = (
                "REALISTIC PHYSICAL CONTINUITY LOCK:\n"
                "- Keep the scene photorealistic and physically plausible.\n"
                "- No random cinematic VFX, glow, particles, morphing, teleporting, magical transformations, surreal effects, artificial speed ramps, or style changes unless explicitly present in the source analysis.\n"
                "- Preserve gravity, weight, inertia, contact points, shadows, reflections, object permanence, body proportions, and camera exposure from frame to frame.\n"
                "- Movement must be motivated by the previous frame. The next action must look like a natural continuation, not a new generated clip.\n\n"
            )
            camera_environment_block = (
                "CAMERA + ENVIRONMENT MATCH LOCK:\n"
                f"- Preserve this environment spatial map exactly: {bible.get('environment_spatial_map') or bible['environment_lock']}.\n"
                f"- Preserve this camera continuity map exactly: {bible.get('camera_continuity_map') or bible['camera_lock']}.\n"
                f"- Preserve this effects/lighting continuity exactly: {bible.get('effects_continuity_bible') or bible['lighting_lock']}.\n"
                "- Do not change camera angle between parts unless the previous part's motion explicitly causes that same continuous camera movement.\n"
                "- Keep the same horizon line, vanishing point, subject screen position, lens compression, camera height, focus distance, foreground/background layout, and prop positions.\n"
                "- The first frame of this part must match the previous part like a match cut, not a new coverage angle.\n\n"
            )
            previous_part_contract = ""
            if idx > 1:
                previous_part_contract = (
                    f"PREVIOUS PART CONTINUITY CONTRACT - MANDATORY FOR PART {idx}:\n"
                    f"- Part {idx - 1} ended at this exact visual state: {state_in['first_frame_anchor']}.\n"
                    f"- Part {idx} must begin from that exact same visual state, as if the video was cut at that frame.\n"
                    "- Keep the same subject face/body/silhouette/outfit or surface, same props, same background geometry, same lens, same color grade, same lighting direction, same camera height, and same motion momentum.\n"
                    "- The first second of this part must feel like the immediate next second after the previous part, not a new shot and not a new generation.\n"
                    "- Only the action may advance. Nothing else may redesign.\n\n"
                )
            separate_job_contract = (
                "SEPARATE GENERATION WARNING:\n"
                "This part may be generated as a separate Veo/Flow job, so the prompt itself must carry all continuity memory. "
                "Do not rely on hidden chat memory. Recreate the exact same subject/world/style from the SERIES BIBLE, then continue only the assigned action.\n\n"
            )
            prompt = (
                f"GENERATE ONLY SERIES PART {idx}/{total_parts} AS ONE CONTINUOUS SERIAL VIDEO SEGMENT.\n"
                "Important: this is NOT a standalone video prompt. This is only the next segment of the same story, same characters, same world.\n"
                "Do not recreate the full reference story. Do not restart the plot. Do not invent a different character, place, style, or action.\n\n"
                f"{separate_job_contract}"
                f"{self._zero_drift_contract(part=idx, total_parts=total_parts)}\n\n"
                f"{series_bible}\n\n"
                f"{self._source_reference_block(part=idx, reference_prompt=reference_prompt)}\n\n"
                f"{continuity_block}"
                f"{realism_block}"
                f"{camera_environment_block}"
                f"{previous_part_contract}"
                f"{first_frame_rules}\n"
                f"THIS PART'S ONLY STORY BEAT:\n{beat}\n\n"
                f"THIS PART'S ONLY SCENE ACTION:\n{scene_desc}\n\n"
                f"{self._part_detail_block(part=idx, total_parts=total_parts, scene_desc=scene_desc, state_in=state_in, state_out=state_out)}\n\n"
                "TIMELINE ORDER SAFETY:\n"
                f"- Part {idx} must contain ONLY its assigned chronological segment above.\n"
                "- Do not borrow the ending scene unless this is the final part.\n"
                "- Do not combine the first and last source-video frames into the same part.\n"
                "- If the assigned segment is short, extend it with micro-actions that happen immediately around that segment, not with scenes from earlier/later parts.\n\n"
                "ACTION PROGRESSION:\n"
                f"- Begin from: {state_in['first_frame_anchor']}.\n"
                f"- Continue/advance with: {state_out['next_action']}.\n"
                "- Generate only this small time slice of the story.\n"
                "- The subject must remain the exact same entity described in the SERIES BIBLE.\n"
                "- The environment must remain the exact same world described in the SERIES BIBLE.\n"
                "- Do not replay earlier timeline segments or restart the opening beat. The camera/framing must stay visually continuous from the previous part's last frame. "
                "Only the action advances with one clear new movement, reveal, or consequence.\n\n"
                f"{end_frame_rules}\n"
                "NEGATIVE CONTINUITY RULES:\n"
                "- No alternate version of the subject, no sudden environment swap, no style remix, no new camera language.\n"
                "- No new protagonist, side story, random cutaway, montage, time skip, intro shot, closing shot, text overlay, subtitles, watermark, or separate mini-story.\n"
                "- No lens flare, heavy bloom, fake sun rays, or beauty retouch unless explicitly allowed in the source effects continuity analysis.\n"
                "- Do not make this part visually self-contained; it must feel like the next seconds of the same shot.\n"
                "- If uncertain, prioritize character identity and action continuity over novelty."
            ).strip()
            out.append({"part": idx, "title": f"Part {idx}", "state_in": state_in, "state_out": state_out, "prompt": prompt})
            prev_out = state_out
        return out

    def _continuity_bible(self, *, parsed: dict[str, Any], scenes: list[dict[str, Any]]) -> dict[str, str]:
        style = parsed.get("style_analysis") if isinstance(parsed.get("style_analysis"), dict) else {}
        story_map = parsed.get("story_map") if isinstance(parsed.get("story_map"), dict) else {}
        anchors = parsed.get("continuity_anchors") if isinstance(parsed.get("continuity_anchors"), dict) else {}
        character_bible = parsed.get("character_continuity_bible") if isinstance(parsed.get("character_continuity_bible"), dict) else {}
        story_bible = parsed.get("story_continuity_bible") if isinstance(parsed.get("story_continuity_bible"), dict) else {}
        fingerprint = parsed.get("visual_fingerprint") if isinstance(parsed.get("visual_fingerprint"), dict) else {}
        spatial_map = parsed.get("environment_spatial_map") if isinstance(parsed.get("environment_spatial_map"), dict) else {}
        camera_map = parsed.get("camera_continuity_map") if isinstance(parsed.get("camera_continuity_map"), dict) else {}
        effects_bible = parsed.get("effects_continuity_bible") if isinstance(parsed.get("effects_continuity_bible"), dict) else {}
        subjects = parsed.get("subjects") if isinstance(parsed.get("subjects"), list) else []
        environments = parsed.get("environments") if isinstance(parsed.get("environments"), list) else []
        subject_from_rows = self._join_descriptions(
            subjects,
            ("subject_id", "type", "appearance", "clothing", "distinctive_features", "emotion"),
            fallback="same subject identity, face/body proportions, outfit, texture, expression, and silhouette",
            limit=3,
        )
        mut_forbid = (
            "FORBIDDEN DRIFT: "
            + "; ".join(str(v) for v in character_bible.get("forbidden_subject_mutations", []) if str(v).strip())
            if isinstance(character_bible.get("forbidden_subject_mutations"), list)
            and character_bible.get("forbidden_subject_mutations")
            else ""
        )
        subject_lock = "; ".join(
            x
            for x in [
                str(character_bible.get("primary_subject_identity") or "").strip(),
                str(character_bible.get("fixed_appearance") or "").strip(),
                str(character_bible.get("biological_age_and_skin_texture") or "").strip(),
                str(character_bible.get("hands_and_closeup_signature") or "").strip(),
                str(character_bible.get("subject_visibility_rule") or "").strip(),
                str(character_bible.get("fixed_clothing_or_surface") or "").strip(),
                str(character_bible.get("fixed_scale_and_silhouette") or "").strip(),
                str(fingerprint.get("subject_signature") or "").strip(),
                "; ".join(str(v) for v in character_bible.get("do_not_change", []) if str(v).strip())
                if isinstance(character_bible.get("do_not_change"), list)
                else "",
                mut_forbid,
                subject_from_rows,
            ]
            if x
        )
        single_loc = str(story_bible.get("single_fixed_location_summary") or "").strip()
        environment_lock = self._join_descriptions(
            environments,
            ("name", "location", "description", "weather", "props", "background"),
            fallback=self._first_non_empty_scene(scenes) or "same location, same props, same background geometry, and same atmosphere",
            limit=2,
        )
        environment_lock = "; ".join(
            x
            for x in [
                single_loc,
                environment_lock,
                str(fingerprint.get("environment_signature") or "").strip(),
                self._dict_inline(spatial_map),
            ]
            if x
        )
        continuity_rules = story_map.get("continuity_rules")
        if isinstance(continuity_rules, list):
            continuity_text = "; ".join(str(x) for x in continuity_rules if str(x).strip())
        else:
            continuity_text = ""
        style_lock = "; ".join(
            x
            for x in [
                str(style.get("visual_style") or "").strip(),
                str(style.get("mood") or "").strip(),
                str(story_bible.get("single_story_spine") or "").strip(),
                str(fingerprint.get("style_signature") or "").strip(),
                "; ".join(str(v) for v in story_bible.get("what_must_not_reset", []) if str(v).strip())
                if isinstance(story_bible.get("what_must_not_reset"), list)
                else "",
                continuity_text,
                "; ".join(str(v) for v in anchors.get("must_match_between_parts", []) if str(v).strip())
                if isinstance(anchors.get("must_match_between_parts"), list)
                else "",
            ]
            if x
        ) or "same cinematic visual style, mood, texture, and rendering quality"
        camera_lock = "; ".join(
            x
            for x in [
                str(style.get("camera_style") or "").strip(),
                str(fingerprint.get("camera_signature") or "").strip(),
                self._dict_inline(camera_map),
                str(anchors.get("camera_motion_path") or "").strip(),
                str(anchors.get("scene_transition_logic") or "").strip(),
            ]
            if x
        ) or "same lens feel, framing, camera height, direction, and movement path"
        lighting_lock = "; ".join(
            x
            for x in [
                str(style.get("lighting_style") or "").strip(),
                str(fingerprint.get("lighting_signature") or "").strip(),
                ", ".join(str(v) for v in style.get("color_palette", []) if str(v).strip())
                if isinstance(style.get("color_palette"), list)
                else "",
            ]
            if x
        ) or "same light direction, exposure, shadows, highlights, and color grade"
        motion_lock = "; ".join(
            x
            for x in [
                str(style.get("motion_style") or "").strip(),
                str(style.get("pacing") or "").strip(),
                str(fingerprint.get("motion_signature") or "").strip(),
                self._dict_inline(effects_bible),
                str(anchors.get("subject_motion_path") or "").strip(),
            ]
            if x
        ) or "same motion speed, action direction, physical momentum, and pacing"
        return {
            "subject_lock": subject_lock,
            "environment_lock": environment_lock,
            "style_lock": style_lock,
            "camera_lock": camera_lock,
            "lighting_lock": lighting_lock,
            "motion_lock": motion_lock,
            "opening_frame": str(anchors.get("opening_frame") or "").strip(),
            "ending_frame": str(anchors.get("ending_frame") or "").strip(),
            "story_spine": str(story_bible.get("single_story_spine") or "").strip(),
            "handoff_logic": str(story_bible.get("part_handoff_logic") or "").strip(),
            "cause_effect_chain": " -> ".join(str(v) for v in story_bible.get("cause_effect_chain", []) if str(v).strip())
            if isinstance(story_bible.get("cause_effect_chain"), list)
            else "",
            "visual_fingerprint": "; ".join(str(v) for v in fingerprint.values() if str(v).strip()),
            "environment_spatial_map": self._dict_inline(spatial_map),
            "camera_continuity_map": self._dict_inline(camera_map),
            "effects_continuity_bible": self._dict_inline(effects_bible),
        }

    def _series_bible_text(self, *, bible: dict[str, str], parsed: dict[str, Any], scenes: list[dict[str, Any]]) -> str:
        story = str(parsed.get("main_story") or parsed.get("video_summary") or "").strip()
        story_map = parsed.get("story_map") if isinstance(parsed.get("story_map"), dict) else {}
        story_arc = story_map.get("story_arc") if isinstance(story_map.get("story_arc"), dict) else {}
        scene_chain = self._scene_chain_text(scenes)
        return (
            "SERIES BIBLE - MUST BE IDENTICAL IN EVERY PART:\n"
            f"- Main story spine: {bible.get('story_spine') or story or 'one continuous story with the same subject moving through one coherent action'}.\n"
            f"- Cause/effect chain: {bible.get('cause_effect_chain') or 'each part must directly cause the next part'}.\n"
            f"- Part handoff logic: {bible.get('handoff_logic') or 'the last frame and unfinished action of one part becomes the first frame and starting action of the next part'}.\n"
            f"- Story arc lock: start={story_arc.get('start') or 'same opening situation'}; "
            f"middle={story_arc.get('middle') or 'same escalating action'}; "
            f"end={story_arc.get('end') or 'same final consequence'}.\n"
            f"- Subject bible: {bible['subject_lock']}.\n"
            f"- Environment bible: {bible['environment_lock']}.\n"
            f"- Environment spatial map: {bible.get('environment_spatial_map') or 'preserve the same spatial layout, horizon line, object positions, background depth, floor/ground plane, and prop placement'}.\n"
            f"- Style bible: {bible['style_lock']}.\n"
            f"- Camera bible: {bible['camera_lock']}.\n"
            f"- Camera continuity map: {bible.get('camera_continuity_map') or 'preserve the same shot size, camera height, lens feel, angle, subject screen position, and camera movement direction'}.\n"
            f"- Lighting/color bible: {bible['lighting_lock']}.\n"
            f"- Motion bible: {bible['motion_lock']}.\n"
            f"- Effects continuity bible: {bible.get('effects_continuity_bible') or 'no new effects; if effects exist, preserve the same physical timing, opacity, blur, light interaction, and direction'}.\n"
            f"- Visual fingerprint lock: {bible.get('visual_fingerprint') or 'preserve the exact recurring visual details from the source video'}.\n"
            f"- Opening frame anchor: {bible.get('opening_frame') or 'the first visual state of the original story'}.\n"
            f"- Final destination anchor: {bible.get('ending_frame') or 'the final visual state of the original story'}.\n"
            f"- Scene chain order: {scene_chain}.\n"
            "- HARD LOCKS (series dies if any break): same biological age + skin/hands texture; same single location/plant palette; main subject stays on-screen through unfinished beats; "
            "no added lens flare/bloom/grade shift; no environment-only mid-story cutaways.\n"
            "- All parts must look like they were filmed in the same continuous take with the same actor/subject, same lens, same set, same lighting setup, and same physical action."
        ).strip()

    def _scene_chain_text(self, scenes: list[dict[str, Any]]) -> str:
        descs = [
            self._scene_text(row)
            for row in scenes
            if isinstance(row, dict) and self._scene_text(row)
        ]
        if not descs:
            return "single continuous action from opening state to final state"
        return " -> ".join(descs[:12])

    def _compact_reference_prompt(self, final_prompt: str) -> str:
        text = re.sub(r"\s+", " ", str(final_prompt or "")).strip()
        if len(text) <= 1400:
            return text
        return text[:1400].rsplit(" ", 1)[0] + "..."

    def _source_reference_block(self, *, part: int, reference_prompt: str) -> str:
        return (
            "FULL SOURCE VIDEO REFERENCE - TIMELINE GUIDE, DO NOT GENERATE ALL OF IT IN THIS PART:\n"
            f"{reference_prompt}\n"
            f"Use this as the full beginning-to-ending story map. Generate only Part {part}'s assigned timeline segment, "
            "while preserving the same subject, environment, style, camera, lighting, and action continuity."
        )

    def _part_detail_block(
        self,
        *,
        part: int,
        total_parts: int,
        scene_desc: str,
        state_in: dict[str, str],
        state_out: dict[str, str],
    ) -> str:
        return (
            "DETAILED DIRECTOR INSTRUCTIONS FOR THIS PART:\n"
            f"- Duration target: one compact segment of the {total_parts}-part story, focused only on Part {part}.\n"
            f"- Opening micro-state: {state_in.get('first_frame_anchor') or 'continue from the previous frame'}.\n"
            f"- Core micro-action: {scene_desc}.\n"
            f"- Ending micro-state: {state_out.get('final_frame_anchor') or 'leave a clear handoff state'}.\n"
            "- Include concrete visual details: subject pose, limb/body direction, eye line or face direction, object placement, foreground/background relationship, texture, shadow, and color temperature.\n"
            "- Include concrete camera details: framing size, camera height, lens feel, movement direction, speed, and where the subject remains inside the frame.\n"
            "- Include concrete motion details: what moves first, what follows, how momentum carries across the cut, and what remains unfinished for the next part.\n"
            "- Keep the same physical laws and spatial map; do not teleport the subject or props.\n"
            "- Unless this is the true final beat of the whole series, keep the primary subject visible in frame (not plants-only scenery).\n"
            "- The part should feel like 2-4 connected shot beats inside one continuous take, not a generic summary."
        )

    def _story_progression_plan(
        self,
        *,
        total_parts: int,
        bible: dict[str, str],
        scenes: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        source_opening = self._first_non_empty_scene(scenes) or bible.get("opening_frame") or "the source opening action begins"
        story_spine = bible.get("story_spine") or "the same subject continues one coherent story"
        handoff = bible.get("handoff_logic") or "each part starts from the previous final frame"
        if total_parts <= 1:
            return [{"beat": "Source opening", "action": source_opening, "end_state": "the action remains unfinished"}]
        scene_descs = [
            self._scene_text(row)
            for row in scenes
            if isinstance(row, dict) and self._scene_text(row)
        ]
        if scene_descs:
            plan: list[dict[str, str]] = []
            segments = self._split_timeline_for_parts(scene_descs, total_parts)
            for idx, segment in enumerate(segments, start=1):
                segment_text = " -> ".join(segment)
                is_final = idx == total_parts
                plan.append(
                    {
                        "beat": (
                            f"Timeline segment {idx}/{total_parts}: continue the full source story in chronological order. "
                            f"Do not repeat segments before this one."
                        ),
                        "action": (
                            f"Generate this assigned story segment only: {segment_text}. "
                            f"Keep cause/effect continuity from the previous segment using handoff logic: {handoff}."
                        ),
                        "end_state": (
                            f"final resolved state after this segment: {segment_text}"
                            if is_final
                            else f"unfinished end of timeline segment {idx}: {segment[-1]}, subject mid-action and ready for segment {idx + 1}"
                        ),
                    }
                )
            return plan
        phases = [
            (
                "Immediate continuation",
                "Continue the exact motion from the previous timeline segment without changing identity or location.",
            ),
            (
                "Commitment",
                "The subject makes a clear next movement or decision caused by the previous frame, pushing the story forward.",
            ),
            (
                "Obstacle",
                "A physical obstacle, tension, or complication appears inside the same world and forces the subject to adjust.",
            ),
            (
                "Escalation",
                "The subject continues the same action vector with more intensity; camera and lighting stay matched.",
            ),
            (
                "Reveal",
                "Reveal a consequence or detail that was implied by the previous action, not a new unrelated scene.",
            ),
            (
                "Close pursuit",
                "Stay close to the subject as the motion continues; preserve pose continuity and background geometry.",
            ),
            (
                "Turning point",
                "The action changes direction or reaches a critical choice while remaining in the same story chain.",
            ),
            (
                "Climax setup",
                "Build toward the final consequence; keep the same subject, same environment, same camera language.",
            ),
            (
                "Climax",
                "The main action reaches its strongest moment as a direct result of all previous parts.",
            ),
            (
                "Aftermath",
                "Show the immediate consequence while the subject and world remain visually continuous.",
            ),
            (
                "Resolution",
                "Resolve the story spine naturally and settle toward the final visual state.",
            ),
        ]
        plan: list[dict[str, str]] = [
            {
                "beat": "Source opening only",
                    "action": f"Generate the first chronological segment of the full source story: {source_opening}. Do not jump to later beats yet.",
                    "end_state": "the first source-story segment is unfinished, with the subject mid-motion and ready for the next segment",
            }
        ]
        continuation_slots = max(1, total_parts - 1)
        for idx in range(2, total_parts + 1):
            phase_idx = round((idx - 2) * (len(phases) - 1) / max(1, continuation_slots - 1))
            phase_name, phase_action = phases[min(phase_idx, len(phases) - 1)]
            is_final = idx == total_parts
            plan.append(
                {
                    "beat": f"{phase_name}: continue the same story spine, not the source opening again. Story spine: {story_spine}.",
                    "action": (
                        f"{phase_action} This is Part {idx}, after the previous timeline segment. "
                        f"Follow handoff logic: {handoff}. Do not repeat earlier timeline segments."
                    ),
                    "end_state": (
                        f"final resolved state of the story spine: {story_spine}"
                        if is_final
                        else f"unfinished {phase_name.lower()} state, subject mid-motion, ready for Part {idx + 1}"
                    ),
                }
            )
        return plan

    def _split_timeline_for_parts(self, items: list[str], total_parts: int) -> list[list[str]]:
        if total_parts <= 0:
            return []
        if not items:
            return [[] for _ in range(total_parts)]
        n = len(items)
        if n >= total_parts:
            segments: list[list[str]] = []
            for idx in range(total_parts):
                start = (idx * n) // total_parts
                end = max(start + 1, ((idx + 1) * n) // total_parts)
                segments.append(items[start:end])
            return segments
        # Khi timeline ít hơn số part, không gom đầu và cuối chung một part.
        # Mỗi part bám một vị trí tăng dần trên timeline; nếu phải dùng lại cùng một mốc,
        # biến nó thành micro-phase khác nhau để Veo không tạo 2 video giống/đứt mạch.
        segments = []
        for idx in range(total_parts):
            pos = min(n - 1, (idx * n) // total_parts)
            base = items[pos]
            if idx == 0:
                text = (
                    f"BEGINNING PHASE of this timeline event only: {base}. "
                    "Show the first visible cause and the first physical movement. "
                    "End before the action resolves, with a clear unfinished pose for the next part."
                )
            elif idx == total_parts - 1:
                text = (
                    f"FINAL CONTINUATION PHASE of the same timeline event: {base}. "
                    "Start from the exact unfinished pose left by the previous part, continue the same motion, "
                    "show the consequence, and resolve the action. Do not replay the beginning."
                )
            else:
                text = (
                    f"MIDDLE CONTINUATION PHASE {idx + 1}/{total_parts} of the same timeline event: {base}. "
                    "Start from the previous final frame, advance the physical action with a new micro-movement, "
                    "and end unfinished for the next part. Do not replay earlier motion."
                )
            segments.append([text])
        return segments

    def _join_descriptions(
        self,
        rows: list[Any],
        keys: tuple[str, ...],
        *,
        fallback: str,
        limit: int,
    ) -> str:
        chunks: list[str] = []
        for row in rows[:limit]:
            if not isinstance(row, dict):
                continue
            vals = [str(row.get(k) or "").strip() for k in keys if str(row.get(k) or "").strip()]
            if vals:
                chunks.append(", ".join(vals))
        return "; ".join(chunks) if chunks else fallback

    def _dict_inline(self, row: dict[str, Any]) -> str:
        chunks: list[str] = []
        for key, value in row.items():
            if isinstance(value, list):
                txt = ", ".join(str(x) for x in value if str(x).strip())
            elif isinstance(value, dict):
                txt = ", ".join(f"{k}: {v}" for k, v in value.items() if str(v).strip())
            else:
                txt = str(value or "").strip()
            if txt:
                chunks.append(f"{key}: {txt}")
        return "; ".join(chunks)

    def _first_non_empty_scene(self, scenes: list[dict[str, Any]]) -> str:
        for row in scenes:
            if isinstance(row, dict):
                desc = self._scene_text(row)
                if desc:
                    return desc
        return ""

    def _part_scene_focus(self, *, scenes: list[dict[str, Any]], part: int, total_parts: int) -> str:
        descs = [
            self._scene_text(row)
            for row in scenes
            if isinstance(row, dict) and self._scene_text(row)
        ]
        if not descs:
            return ""
        if total_parts == 2 and len(descs) >= 3:
            if part == 1:
                return f"{descs[0]} Continue into the beginning of: {descs[1]}. Stop before the action resolves."
            return f"Start from the unfinished middle action: {descs[1]} Then continue into: {descs[-1]}."
        start = round((part - 1) * len(descs) / total_parts)
        end = max(start + 1, round(part * len(descs) / total_parts))
        return " ".join(descs[start:min(end, len(descs))]) or descs[min(part - 1, len(descs) - 1)]

    def _scene_text(self, row: dict[str, Any]) -> str:
        base = str(
            row.get("description")
            or row.get("action")
            or row.get("event")
            or row.get("beat")
            or row.get("summary")
            or row.get("visual")
            or ""
        ).strip()
        start_state = str(row.get("start_state") or "").strip()
        end_state = str(row.get("end_state") or "").strip()
        cause = str(row.get("cause_effect") or row.get("continuity_to_next") or "").strip()
        pose = str(row.get("subject_pose") or "").strip()
        camera = str(row.get("camera_framing") or row.get("camera_motion") or "").strip()
        lighting = str(row.get("lighting") or "").strip()
        environment = str(row.get("environment_details") or "").strip()
        props = row.get("visible_props")
        extras = []
        if start_state:
            extras.append(f"start: {start_state}")
        if end_state:
            extras.append(f"end: {end_state}")
        if cause:
            extras.append(f"cause/effect: {cause}")
        if pose:
            extras.append(f"pose: {pose}")
        if camera:
            extras.append(f"camera: {camera}")
        if lighting:
            extras.append(f"lighting: {lighting}")
        if environment:
            extras.append(f"environment: {environment}")
        if isinstance(props, list) and props:
            extras.append("props: " + ", ".join(str(x) for x in props if str(x).strip()))
        if extras:
            return f"{base} ({'; '.join(extras)})" if base else "; ".join(extras)
        return base

    def _end_frame_anchor(
        self,
        *,
        scene_desc: str,
        beat: str,
        planned_end: str = "",
        part: int,
        total_parts: int,
        bible: dict[str, str],
    ) -> str:
        if planned_end.strip():
            return f"{planned_end.strip()}; same subject, same environment, same lighting, same framing, same motion continuity"
        if part >= total_parts:
            if bible.get("ending_frame"):
                return f"{bible['ending_frame']}; still preserving {bible['environment_lock']} and {bible['camera_lock']}"
            return f"the final resolved pose/action from this story, still in {bible['environment_lock']}, with {bible['camera_lock']}"
        return (
            f"the subject is still in motion after this beat: {scene_desc or beat}; "
            f"same environment, same lighting, same framing, same motion direction, with the action intentionally unfinished"
        )

    def _next_action(self, *, scene_desc: str, beat: str, part: int, total_parts: int) -> str:
        if part >= total_parts:
            return "complete the action and settle into the final visual state"
        return f"continue directly from the unfinished movement of Part {part}: {scene_desc or beat}"

    def _story_beats(self, total_parts: int) -> list[str]:
        if total_parts == 2:
            return [
                "Part 1 establishes the subject and environment, starts the main physical action, and ends mid-motion without resolution.",
                "Part 2 starts from the exact mid-motion end frame of Part 1, continues the same action vector, reveals the consequence, and reaches the ending.",
            ]
        if total_parts == 3:
            return [
                "Part 1 establishes the scene and starts the inciting motion, ending on a clear unfinished action pose.",
                "Part 2 starts from that unfinished pose, continues the same motion, and escalates with a new reveal or complication.",
                "Part 3 starts from Part 2's final frame, resolves the action, and lands on the final visual state.",
            ]
        return [
            "Opening continuation: establish the world once and begin the motion without resolving it.",
            *[
                f"Middle continuation {i}: start from the previous final frame and advance with a new action beat, camera progression, or reveal."
                for i in range(2, max(2, total_parts))
            ],
            "Final continuation: start from the previous final frame, resolve the motion, and end on a clear final visual state.",
        ]


class ExportToAIVideoEngine:
    def __init__(self) -> None:
        self._paths = ensure_reverse_video_layout()

    def export_prompt_package(self, *, job_id: str, payload: dict[str, Any]) -> Path:
        out_path = self._paths["outputs"] / f"{_safe_slug(job_id)}_export.json"
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return out_path


class VideoReversePromptEngine:
    def __init__(self, *, log: LogFn | None = None) -> None:
        self._log = log or (lambda _m: None)
        self._paths = ensure_reverse_video_layout()
        self.ff = FFmpegService()
        self.importer = VideoSourceImporter(log=self._log)
        self.extractor = KeyframeExtractor(ff=self.ff, log=self._log)
        self.gemini = GeminiBrowserAnalyzer(log=self._log)
        self.parser = GeminiResultParser()
        self.subject_builder = SubjectBibleBuilder()
        self.env_builder = EnvironmentBibleBuilder()
        self.scene_builder = SceneBreakdownBuilder()
        self.style_builder = StyleAnalyzer()
        self.story_builder = StoryMapBuilder()
        self.replacement_engine = SubjectReplacementEngine()
        self.prompt_builder = ReversePromptBuilder()
        self.series_engine = ContinuousStoryEngine()
        self.exporter = ExportToAIVideoEngine()

    def create_job_from_local_video(
        self,
        *,
        local_video_path: str,
        video_id: str | None = None,
        job_id: str | None = None,
        base_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Payload để đồng bộ UI Reverse sau khi chọn video từ Universal Video Downloader.
        """
        p = Path(str(local_video_path or "")).expanduser().resolve()
        if not p.is_file():
            raise FileNotFoundError(f"Không thấy file video: {p}")
        base = dict(base_payload or {})
        jid = str(job_id or base.get("id") or "").strip() or f"reverse_{time.strftime('%Y%m%d_%H%M%S')}"
        out = {
            "id": jid,
            "source_type": "local",
            "source_url": str(base.get("source_url") or ""),
            "local_video_path": str(p),
            "target_platform": str(base.get("target_platform") or "Facebook Reels"),
            "output_language": str(base.get("output_language") or "Vietnamese"),
            "duration_sec": int(base.get("duration_sec") or 8),
            "aspect_ratio": str(base.get("aspect_ratio") or "9:16"),
            "analysis_mode": str(base.get("analysis_mode") or "gemini_browser"),
            "keyframe_mode": str(base.get("keyframe_mode") or "hybrid"),
            "max_frames": int(base.get("max_frames") or 20),
            "replacement": dict(base.get("replacement") or {}),
            "continuous_series": dict(base.get("continuous_series") or {}),
            "gemini_browser": dict(base.get("gemini_browser") or {}),
        }
        if video_id:
            out["downloader_video_id"] = str(video_id)
        return out

    def build_job_from_input(self, payload: dict[str, Any]) -> ReverseVideoJob:
        jid = str(payload.get("id") or f"reverse_{uuid.uuid4().hex[:8]}")
        st = str(payload.get("source_type") or "local").strip().lower()
        if st == "downloaded_video":
            st = "local"
        return ReverseVideoJob(
            id=jid,
            source_type=st,
            source_url=str(payload.get("source_url") or ""),
            local_video_path=str(payload.get("local_video_path") or ""),
            target_platform=str(payload.get("target_platform") or "Facebook Reels"),
            output_language=str(payload.get("output_language") or "Vietnamese"),
            duration_sec=int(payload.get("duration_sec") or 8),
            aspect_ratio=str(payload.get("aspect_ratio") or "9:16"),
            analysis_mode=str(payload.get("analysis_mode") or "gemini_browser"),
            keyframe_mode=str(payload.get("keyframe_mode") or "hybrid"),
            max_frames=int(payload.get("max_frames") or 20),
            replacement=dict(payload.get("replacement") or {}),
            continuous_series=dict(payload.get("continuous_series") or {}),
            gemini_browser=dict(payload.get("gemini_browser") or {}),
        )

    def run_pipeline(self, payload: dict[str, Any]) -> dict[str, Any]:
        job = self.build_job_from_input(payload)
        self._log(f"[INFO] ===== Reverse Job {job.id} bắt đầu =====")
        if not self.ff.check_ffmpeg_available():
            raise RuntimeError("FFmpeg chưa sẵn sàng")
        video_path = self.importer.import_video(job)
        metadata = self.ff.read_metadata(video_path)
        self._log(f"[INFO] Đã đọc metadata video: {metadata.get('resolution')}, {metadata.get('duration')}s")
        frames = self.extractor.extract(
            job_id=job.id,
            video_path=video_path,
            mode=job.keyframe_mode,
            max_frames=max(1, min(job.max_frames, 40)),
            duration=float(metadata.get("duration") or 0.0),
        )
        raw_text = self.gemini.analyze(job=job, frame_paths=[str(x["path"]) for x in frames], video_path=str(video_path))
        parsed = self.parser.extract_json(raw_text)
        parsed, replacement_map = self.replacement_engine.apply(parsed=parsed, replacement=job.replacement)
        final_prompt = self.prompt_builder.build(parsed=parsed, job=job)
        continuous_prompts: list[dict[str, Any]] = []
        c = dict(job.continuous_series or {})
        if c.get("enabled"):
            total_parts = int(c.get("total_parts") or 2)
            scenes = self.scene_builder.build(parsed)
            continuous_prompts = self.series_engine.build(final_prompt=final_prompt, scenes=scenes, total_parts=total_parts, parsed=parsed)
        out = {
            "id": job.id,
            "source_url": job.source_url,
            "video_path": str(video_path),
            "video_metadata": metadata,
            "frames": frames,
            "frame_zip_path": "",
            "gemini_raw_output": raw_text,
            "visual_analysis": parsed,
            "subjects": self.subject_builder.build(parsed),
            "environments": self.env_builder.build(parsed),
            "scenes": self.scene_builder.build(parsed),
            "style_analysis": self.style_builder.build(parsed),
            "story_map": self.story_builder.build(parsed),
            "replacement_map": replacement_map,
            "final_prompt": final_prompt,
            "continuous_prompts": continuous_prompts,
            "status": "completed",
            "error_message": "",
            "updated_at": _now_ts(),
        }
        self._save_outputs(job_id=job.id, output=out)
        self._log("[SUCCESS] Hoàn tất reverse prompt")
        return out

    def _save_outputs(self, *, job_id: str, output: dict[str, Any]) -> None:
        ensure_reverse_video_layout()
        analysis_path = self._paths["analysis"] / f"{_safe_slug(job_id)}.json"
        prompt_path = self._paths["prompts"] / f"{_safe_slug(job_id)}.txt"
        analysis_path.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        prompt_path.write_text(str(output.get("final_prompt") or ""), encoding="utf-8")
        self.exporter.export_prompt_package(job_id=job_id, payload=output)
        jobs_path = self._paths["jobs"]
        rows: list[dict[str, Any]] = []
        try:
            raw = json.loads(jobs_path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                rows = [x for x in raw if isinstance(x, dict)]
        except Exception:
            rows = []
        rows = [r for r in rows if str(r.get("id") or "") != str(output.get("id") or "")]
        rows.insert(0, output)
        jobs_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_gemini_strict_prompt() -> str:
    return (
        "You are a professional video director, visual analyst, and reverse prompt engineer.\n\n"
        "You are analyzing representative keyframes extracted from a video in strict chronological order.\n"
        "The uploaded filenames are ordered like timeline_0001_of_XXXX, timeline_0002_of_XXXX, ... from the beginning to the end of the source video.\n"
        "You MUST analyze the images in filename order only. Do not move the last frame into the beginning. Do not mix ending frames into the opening scene.\n\n"
        "Return STRICT JSON ONLY. Do not write markdown.\n\n"
        "Required JSON schema:\n"
        "{\n"
        '  "main_story": "",\n'
        '  "video_summary": "",\n'
        '  "visual_fingerprint": {\n'
        '    "subject_signature": "",\n'
        '    "environment_signature": "",\n'
        '    "camera_signature": "",\n'
        '    "lighting_signature": "",\n'
        '    "motion_signature": "",\n'
        '    "style_signature": ""\n'
        "  },\n"
        '  "environment_spatial_map": {\n'
        '    "fixed_location_layout": "",\n'
        '    "foreground_elements": [],\n'
        '    "midground_elements": [],\n'
        '    "background_elements": [],\n'
        '    "ground_plane_or_horizon": "",\n'
        '    "prop_positions": [],\n'
        '    "what_must_not_move_or_change": []\n'
        "  },\n"
        '  "camera_continuity_map": {\n'
        '    "shot_size": "",\n'
        '    "camera_height": "",\n'
        '    "camera_angle": "",\n'
        '    "lens_feel": "",\n'
        '    "camera_motion_path": "",\n'
        '    "subject_screen_position": "",\n'
        '    "focus_depth": "",\n'
        '    "what_must_match_between_parts": []\n'
        "  },\n"
        '  "effects_continuity_bible": {\n'
        '    "realism_level": "",\n'
        '    "allowed_effects_from_source": [],\n'
        '    "effect_motion_direction": "",\n'
        '    "effect_light_interaction": "",\n'
        '    "motion_blur_or_exposure": "",\n'
        '    "forbidden_effects": []\n'
        "  },\n"
        '  "detailed_story_beats": [],\n'
        '  "subjects": [],\n'
        '  "environments": [],\n'
        '  "scene_breakdown": [],\n'
        '  "character_continuity_bible": {\n'
        '    "primary_subject_identity": "",\n'
        '    "fixed_appearance": "",\n'
        '    "biological_age_and_skin_texture": "",\n'
        '    "hands_and_closeup_signature": "",\n'
        '    "subject_visibility_rule": "",\n'
        '    "fixed_clothing_or_surface": "",\n'
        '    "fixed_scale_and_silhouette": "",\n'
        '    "forbidden_subject_mutations": [],\n'
        '    "do_not_change": []\n'
        "  },\n"
        '  "story_continuity_bible": {\n'
        '    "single_story_spine": "",\n'
        '    "single_fixed_location_summary": "",\n'
        '    "cause_effect_chain": [],\n'
        '    "part_handoff_logic": "",\n'
        '    "what_must_not_reset": []\n'
        "  },\n"
        '  "continuity_anchors": {\n'
        '    "opening_frame": "",\n'
        '    "ending_frame": "",\n'
        '    "subject_motion_path": "",\n'
        '    "camera_motion_path": "",\n'
        '    "scene_transition_logic": "",\n'
        '    "must_match_between_parts": []\n'
        "  },\n"
        '  "style_analysis": {\n'
        '    "visual_style": "",\n'
        '    "camera_style": "",\n'
        '    "lighting_style": "",\n'
        '    "motion_style": "",\n'
        '    "mood": "",\n'
        '    "color_palette": [],\n'
        '    "pacing": "",\n'
        '    "continuity_rules": []\n'
        "  },\n"
        '  "story_map": {\n'
        '    "main_story": "",\n'
        '    "story_arc": {"start":"","middle":"","end":""},\n'
        '    "continuity_rules": [],\n'
        '    "negative_rules": []\n'
        "  },\n"
        '  "reverse_prompt": ""\n'
        "}\n\n"
        "Analysis goals:\n"
        "- Detect all important characters, animals, creatures, objects.\n"
        "- Describe concrete visual fingerprints: exact subject signature, environment signature, camera signature, lighting signature, motion signature, and style signature.\n"
        "- Build environment_spatial_map so generated parts keep the exact same location layout, foreground/midground/background, horizon/ground plane, and prop positions.\n"
        "- Build camera_continuity_map so generated parts keep the same shot size, camera height, camera angle, lens feel, subject screen position, and camera movement path.\n"
        "- Build effects_continuity_bible. If the source is realistic, forbid artificial VFX. If effects exist, describe their physical timing, light interaction, opacity, blur, and direction so they stay continuous.\n"
        "- If lighting is natural/overcast/shade, put lens flare, bloom, artificial sun rays, and heavy warm grade into forbidden_effects unless those are clearly visible in the uploaded frames.\n"
        "- Build a strict character_continuity_bible so future video parts keep the exact same subject identity, appearance, size, silhouette, clothing/surface, and emotion.\n"
        "- biological_age_and_skin_texture MUST describe apparent age (child/adult/elderly), wrinkles, veins, sun damage, and skin gloss level so separate generations cannot de-age or beautify hands/face.\n"
        "- hands_and_closeup_signature MUST describe visible hands (knuckles, joints, nails, hair on skin) when hands appear in the source, so Part 2+ cannot swap to a different person's younger/smooth hands.\n"
        "- subject_visibility_rule MUST state whether the main subject stays on-screen for the whole story; if the subject is visible in early frames, require visibility through all parts until the final resolution (forbid random environment-only B-roll mid-story).\n"
        "- forbidden_subject_mutations MUST list concrete drift risks (e.g. younger hands, different garden, lens flare, new flower species, potted nursery swap).\n"
        "- story_continuity_bible.single_fixed_location_summary MUST name ONE coherent place (layout + dominant plants + ground type). Forbid unrelated location jumps unless the source clearly shows continuous travel through that path in order.\n"
        "- Detect environment and visual setting.\n"
        "- Detect scene order, action flow, cause/effect chain, and why each scene leads to the next.\n"
        "- Build scene_breakdown strictly in the same chronological order as the uploaded filename order.\n"
        "- The first scene_breakdown item must describe only the earliest visible action; the final scene_breakdown item must describe only the ending visible action.\n"
        "- scene_breakdown must be very detailed. For each scene include: description, action, start_state, end_state, subject_pose, camera_framing, camera_motion, lighting, environment_details, visible_props, cause_effect, continuity_to_next.\n"
        "- detailed_story_beats must list 8-20 chronological micro-beats if visible, not just 3 generic scenes.\n"
        "- Build a story_continuity_bible that can prevent disconnected or unrelated generated parts.\n"
        "- Detect exact opening visual state, ending visual state, subject motion path, camera motion path, and transition continuity anchors.\n"
        "- Detect camera movement, lighting, style, mood, color palette.\n"
        "- Prioritize realistic physical continuity: no random effects, no impossible transitions, no sudden camera angle changes, no environment redesign.\n"
        "- Preserve story structure without copying exact copyrighted elements.\n"
        "- reverse_prompt must be suitable for Google Flow / Veo 3.\n"
        "- If the video will be split into multiple generated parts, describe how the last frame of one part should match the first frame of the next.\n"
    ).strip()


def build_gemini_chunk_prompt(
    *,
    chunk_index: int,
    total_chunks: int,
    previous_context: str = "",
    frame_list: list[str] | None = None,
) -> str:
    continuity = ""
    if previous_context.strip():
        continuity = (
            "\n\nPREVIOUS CHUNK CONTEXT (must preserve continuity):\n"
            f"{previous_context.strip()}\n\n"
            "For this chunk, continue the same full-video story from the previous context.\n"
            "Do NOT restart the scene, do NOT repeat the same opening, and do NOT create a separate unrelated story.\n"
            "Focus on what changes in this chronological chunk: new action, camera progression, subject movement, reveal, escalation, or ending state.\n"
            "The end_state of the previous chunk must connect directly to the start_state of this chunk.\n"
        )
    frame_order = ""
    if frame_list:
        frame_order = (
            "\n\nUPLOADED FRAME ORDER FOR THIS CHUNK (strict timeline order):\n"
            + "\n".join(f"{i + 1}. {name}" for i, name in enumerate(frame_list))
            + "\nUse this order exactly. Frames that overlap with the previous chunk are only context for continuity.\n"
        )
    return (
        f"You are analyzing chunk {chunk_index}/{total_chunks} of keyframes from a longer video.\n"
        "Analyze only this chunk in strict chronological filename order.\n"
        "The uploaded filenames are ordered like timeline_0001_of_XXXX, timeline_0002_of_XXXX, ... within the full source video.\n"
        "This chunk is one continuous segment of the same video, not a separate video.\n"
        "Some chunks may intentionally overlap with the previous chunk by a few frames. Treat overlapping frames as continuity context, not as a new repeated story event.\n"
        "Important: because this chat may contain earlier chunk uploads, analyze ONLY the newly uploaded files listed under UPLOADED FRAME ORDER FOR THIS CHUNK. Earlier uploaded images are history/context only; do not re-analyze them as part of this chunk.\n"
        "If a frame filename is not in this chunk list, do not include it in this chunk's scene_breakdown or timeline_events.\n"
        "Do not place later frames before earlier frames. Do not mix the ending of this chunk into its beginning.\n"
        "Do not create a final prompt yet. First reconstruct this chunk's timeline precisely.\n"
        f"{continuity}"
        f"{frame_order}"
        "Return STRICT JSON ONLY (no markdown) with fields:\n"
        "{\n"
        f'  "chunk_index": {chunk_index},\n'
        f'  "total_chunks": {total_chunks},\n'
        '  "main_story": "",\n'
        '  "chunk_role_in_full_story": "",\n'
        '  "visual_fingerprint": {},\n'
        '  "environment_spatial_map": {},\n'
        '  "camera_continuity_map": {},\n'
        '  "effects_continuity_bible": {},\n'
        '  "start_state": "",\n'
        '  "end_state": "",\n'
        '  "timeline_events": [],\n'
        '  "detailed_story_beats": [],\n'
        '  "cause_effect_links": [],\n'
        '  "handoff_to_next_chunk": "",\n'
        '  "subjects": [],\n'
        '  "environments": [],\n'
        '  "scene_breakdown": [],\n'
        '  "character_continuity_bible": {},\n'
        '  "story_continuity_bible": {},\n'
        '  "continuity_anchors": {},\n'
        '  "style_analysis": {},\n'
        '  "story_map": {},\n'
        '  "reverse_prompt": ""\n'
        "}\n"
    ).strip()


def build_gemini_merge_prompt(chunk_texts: list[str]) -> str:
    packed = "\n\n".join([f"--- PART {i + 1} ---\n{t}" for i, t in enumerate(chunk_texts)])
    return (
        "You are given multiple partial analyses of consecutive video keyframe chunks.\n"
        "These chunks together represent ONE complete source video from beginning to end.\n"
        "Merge them into one final coherent master analysis before any prompt generation.\n"
        "Your primary job is to reconstruct the full story timeline, action flow, subject continuity, and chunk-to-chunk handoffs.\n"
        "Return STRICT JSON ONLY with schema:\n"
        "{\n"
        '  "main_story": "",\n'
        '  "video_summary": "",\n'
        '  "visual_fingerprint": {},\n'
        '  "environment_spatial_map": {},\n'
        '  "camera_continuity_map": {},\n'
        '  "effects_continuity_bible": {},\n'
        '  "master_timeline": [],\n'
        '  "timeline_handoffs": [],\n'
        '  "detailed_story_beats": [],\n'
        '  "character_continuity_bible": {},\n'
        '  "story_continuity_bible": {},\n'
        '  "subjects": [],\n'
        '  "environments": [],\n'
        '  "scene_breakdown": [],\n'
        '  "continuity_anchors": {},\n'
        '  "style_analysis": {},\n'
        '  "story_map": {},\n'
        '  "reverse_prompt": ""\n'
        "}\n\n"
        "Rules:\n"
        "- Preserve strict chronological scene order from chunk 1 through the final chunk.\n"
        "- Treat chunk boundaries as invisible; the end_state of chunk N must become the start_state of chunk N+1.\n"
        "- Remove duplicates, but do NOT collapse different story beats into one repeated generic scene.\n"
        "- Keep action continuity and consistent subject/style/camera/lighting.\n"
        "- visual_fingerprint must combine the most stable details from all chunks so later prompts do not drift.\n"
        "- environment_spatial_map, camera_continuity_map, and effects_continuity_bible must combine stable details from all chunks and must be strict enough for separate generated parts to match.\n"
        "- master_timeline and detailed_story_beats must contain enough ordered micro-steps to split into multiple generated parts later.\n"
        "- scene_breakdown must be detailed, not generic. Each item should include description/action/start_state/end_state/subject_pose/camera_framing/camera_motion/lighting/environment_details/visible_props/cause_effect/continuity_to_next when possible.\n\n"
        "- If the source video is realistic, keep the merged story realistic. Do not introduce artificial effects, fantasy transitions, morphing, or unrelated cinematic VFX.\n"
        "- Merge character_continuity_bible and story_continuity_bible into one stable bible for the whole video.\n"
        "- Preserve biological_age_and_skin_texture, hands_and_closeup_signature, subject_visibility_rule, forbidden_subject_mutations, and single_fixed_location_summary across the merge.\n"
        "- Preserve opening_frame, ending_frame, subject_motion_path, camera_motion_path, and must_match_between_parts continuity anchors.\n"
        "- Build ONE coherent reverse_prompt for the whole video, not repeated prompts per chunk.\n"
        "- Later chunks must continue the action from earlier chunks.\n"
        "- Avoid generating two nearly identical scene prompts; each scene must advance the story.\n"
        "- The final result must make it clear what happens at the beginning, middle, and end of the source video.\n\n"
        f"PARTIAL ANALYSES:\n{packed}"
    ).strip()


@contextlib.contextmanager
def _profile_lock(profile_path: Path):
    key = str(profile_path.resolve())
    with _profile_launch_guard:
        lock = _profile_launch_locks.setdefault(key, threading.Lock())
    lock.acquire()
    try:
        yield
    finally:
        lock.release()

