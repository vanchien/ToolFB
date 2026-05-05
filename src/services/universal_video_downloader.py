from __future__ import annotations

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
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from src.utils.paths import project_root

LogFn = Callable[[str], None]

YTDLP_PYPI_JSON_URL = "https://pypi.org/pypi/yt-dlp/json"


def _parse_ytdlp_semverish(text: str) -> tuple[int, ...]:
    """Chuỗi từ ``yt-dlp --version`` hoặc PyPI ``info.version`` → tuple số để so sánh."""
    t = (text or "").strip()
    if not t:
        return ()
    t = t.split()[0]
    t = t.split("+", 1)[0]
    parts: list[int] = []
    for seg in t.replace("-", ".").split("."):
        if seg.isdigit():
            parts.append(int(seg))
        else:
            break
    return tuple(parts)


def _compare_version_tuples(a: tuple[int, ...], b: tuple[int, ...]) -> int:
    if not a or not b:
        return 0
    n = max(len(a), len(b))
    aa = a + (0,) * (n - len(a))
    bb = b + (0,) * (n - len(b))
    if aa > bb:
        return 1
    if aa < bb:
        return -1
    return 0


def fetch_ytdlp_latest_version_pypi(*, timeout_sec: float = 20.0) -> dict[str, Any]:
    """Lấy phiên bản mới nhất của gói ``yt-dlp`` trên PyPI (chỉ đọc JSON, không cài)."""
    try:
        req = urllib.request.Request(
            YTDLP_PYPI_JSON_URL,
            headers={"User-Agent": "ToolFB-universal-downloader/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = json.loads(resp.read().decode("utf-8", errors="replace"))
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        return {"ok": False, "version": "", "error": str(exc)}
    except Exception as exc:
        return {"ok": False, "version": "", "error": str(exc)}
    if not isinstance(raw, dict):
        return {"ok": False, "version": "", "error": "PyPI JSON không hợp lệ"}
    info = raw.get("info")
    if not isinstance(info, dict):
        return {"ok": False, "version": "", "error": "Thiếu trường info trong PyPI JSON"}
    ver = str(info.get("version") or "").strip()
    if not ver:
        return {"ok": False, "version": "", "error": "PyPI không có version"}
    return {"ok": True, "version": ver, "error": ""}


def run_pip_upgrade_ytdlp(*, timeout_sec: int = 300) -> dict[str, Any]:
    """``python -m pip install -U yt-dlp`` với cùng interpreter đang chạy app."""
    cmd = [sys.executable, "-m", "pip", "install", "-U", "yt-dlp"]
    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            encoding="utf-8",
            errors="replace",
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "message": f"Hết thời gian ({timeout_sec}s) khi chạy pip.",
            "stdout": "",
            "stderr": "",
        }
    except Exception as exc:
        return {"ok": False, "message": str(exc), "stdout": "", "stderr": ""}
    out = (p.stdout or "").strip()
    err = (p.stderr or "").strip()
    blob = "\n".join(x for x in (out, err) if x)
    ok = p.returncode == 0
    return {
        "ok": ok,
        "message": (blob[-1200:] if blob else f"pip thoát với mã {p.returncode}"),
        "stdout": out,
        "stderr": err,
        "returncode": p.returncode,
    }


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _to_ytdlp_cookie_file(cookie_path: str | None) -> tuple[str | None, Path | None]:
    """
    Trả về đường dẫn cookie dùng cho yt-dlp.
    - Nếu là file txt/netscape: dùng trực tiếp.
    - Nếu là JSON Playwright (`[]` hoặc `{"cookies": [...]}`): convert sang file Netscape tạm.
    """
    raw = str(cookie_path or "").strip()
    if not raw:
        return None, None
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (project_root() / p).resolve()
    if not p.is_file():
        return None, None
    if p.suffix.lower() in (".txt", ".cookies"):
        return str(p), None
    if p.suffix.lower() != ".json":
        return None, None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None, None
    cookies = data.get("cookies") if isinstance(data, dict) else data
    if not isinstance(cookies, list):
        return None, None
    lines = ["# Netscape HTTP Cookie File"]
    for c in cookies:
        if not isinstance(c, dict):
            continue
        name = str(c.get("name") or "").strip()
        value = str(c.get("value") or "")
        domain = str(c.get("domain") or "").strip()
        path = str(c.get("path") or "/") or "/"
        secure = "TRUE" if bool(c.get("secure")) else "FALSE"
        host_only = bool(c.get("hostOnly"))
        include_sub = "FALSE" if host_only else "TRUE"
        if not name or not domain:
            continue
        exp_raw = c.get("expires")
        try:
            exp = int(float(exp_raw)) if exp_raw not in (None, "", -1) else 0
        except Exception:
            exp = 0
        lines.append("\t".join([domain, include_sub, path, secure, str(max(0, exp)), name, value]))
    if len(lines) <= 1:
        return None, None
    tmp = Path(tempfile.gettempdir()) / f"toolfb_ytdlp_cookie_{uuid.uuid4().hex[:8]}.txt"
    try:
        tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except OSError:
        return None, None
    return str(tmp), tmp


def ensure_downloader_layout() -> dict[str, Path]:
    root = project_root() / "data" / "downloader"
    paths = {
        "root": root,
        "jobs_file": root / "download_jobs.json",
        "videos_file": root / "downloaded_videos.json",
        "archive": root / "archive.txt",
    }
    root.mkdir(parents=True, exist_ok=True)
    for key in ("jobs_file", "videos_file"):
        p = paths[key]
        if not p.is_file():
            p.write_text("[]\n", encoding="utf-8")
    if not paths["archive"].is_file():
        paths["archive"].write_text("", encoding="utf-8")
    return paths


def default_universal_video_downloader_config() -> dict[str, Any]:
    return {
        "enabled": True,
        "yt_dlp": {
            "bin": "yt-dlp",
            "use_exe": False,
            "exe_path": str(project_root() / "tools" / "yt-dlp" / ("yt-dlp.exe" if os.name == "nt" else "yt-dlp")),
            "format": "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/best",
            "merge_output_format": "mp4",
            "timeout_sec": 600,
            "sleep_interval_sec": 2,
            "max_videos_default": 50,
            "max_filesize_mb": 300,
            "write_info_json": True,
            "write_thumbnail": True,
            "proxy": "",
        },
        "download": {
            "default_output_dir": str(project_root() / "data" / "downloads"),
            "last_output_dir": "",
            "remember_last_output_dir": True,
            "organize_by_platform": True,
            "organize_by_uploader": True,
            "skip_existing": True,
        },
        "facebook_reels": {
            "cookie_path": "",
            "max_collect": 300,
            "max_scroll_rounds": 100,
            "max_scan_minutes": 30,
            "scroll_until_end": True,
        },
    }


def load_universal_video_downloader_config() -> dict[str, Any]:
    cfg_path = project_root() / "config" / "universal_video_downloader.json"
    base = default_universal_video_downloader_config()
    if not cfg_path.is_file():
        return {"universal_video_downloader": base}
    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        return {"universal_video_downloader": base}
    if not isinstance(raw, dict):
        return {"universal_video_downloader": base}
    uvd = raw.get("universal_video_downloader")
    if not isinstance(uvd, dict):
        return {"universal_video_downloader": base}
    merged = {"enabled": bool(uvd.get("enabled", base["enabled"]))}
    yt = dict(base["yt_dlp"])
    yt.update(dict(uvd.get("yt_dlp") or {}))
    dl = dict(base["download"])
    dl.update(dict(uvd.get("download") or {}))
    fb = dict(base["facebook_reels"])
    fb.update(dict(uvd.get("facebook_reels") or {}))
    merged["yt_dlp"] = yt
    merged["download"] = dl
    merged["facebook_reels"] = fb
    return {"universal_video_downloader": merged}


def persist_facebook_reels_settings(
    *,
    cookie_path: str | None = None,
    max_collect: int | None = None,
    max_scroll_rounds: int | None = None,
    max_scan_minutes: int | None = None,
    scroll_until_end: bool | None = None,
) -> None:
    """Merge ``facebook_reels`` vào ``config/universal_video_downloader.json``. Chỉ cập nhật tham số khác ``None``."""
    cfg_path = project_root() / "config" / "universal_video_downloader.json"
    raw: dict[str, Any] = {}
    if cfg_path.is_file():
        try:
            raw = json.loads(cfg_path.read_text(encoding="utf-8"))
        except Exception:
            raw = {}
    if not isinstance(raw, dict):
        raw = {}
    uvd = dict(raw.get("universal_video_downloader") or {})
    fb = dict(uvd.get("facebook_reels") or {})
    if cookie_path is not None:
        fb["cookie_path"] = str(cookie_path or "").strip()
    if max_collect is not None:
        fb["max_collect"] = max(10, min(500, int(max_collect)))
    if max_scroll_rounds is not None:
        fb["max_scroll_rounds"] = max(5, min(280, int(max_scroll_rounds)))
    if max_scan_minutes is not None:
        fb["max_scan_minutes"] = max(1, min(180, int(max_scan_minutes)))
    if scroll_until_end is not None:
        fb["scroll_until_end"] = bool(scroll_until_end)
    uvd["facebook_reels"] = fb
    raw["universal_video_downloader"] = uvd
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def detect_platform(url: str) -> str:
    u = url.lower()
    if "youtube.com" in u or "youtu.be" in u:
        return "youtube"
    if "tiktok.com" in u:
        return "tiktok"
    if "facebook.com" in u or "fb.watch" in u:
        return "facebook"
    return "unknown"


_FB_PROFILE_UNSUPPORTED_HINT = (
    "\n\n── Gợi ý (Facebook) ──\n"
    "yt-dlp không hỗ trợ quét cả tab Reels hay trang profile chỉ có tên người dùng (không kèm ID video). "
    "Trên tab «Tải video», dùng «Quét Reels (Playwright)» để lấy danh sách từ tab /reels/, hoặc dán link từng reel:\n"
    "  • https://www.facebook.com/reel/1234567890\n"
    "  • https://www.facebook.com/TênTrang/videos/3676516585958356\n\n"
    "Nội dung cần đăng nhập: cập nhật yt-dlp, có thể cài pip install \"yt-dlp[default,curl-cffi]\" "
    "và dùng cookie trình duyệt (--cookies-from-browser) theo hướng dẫn yt-dlp."
)


def augment_facebook_unsupported_url_message(url: str, err: str) -> str:
    """Khi yt-dlp báo Unsupported URL với link dạng profile/tab Reels, thêm hướng dẫn tiếng Việt."""
    if not err or "facebook.com" not in url.lower():
        return err
    if "unsupported url" not in err.lower():
        return err
    if facebook_url_is_ytdlp_supported_shape(url):
        return err
    return err.rstrip() + _FB_PROFILE_UNSUPPORTED_HINT


def facebook_url_is_ytdlp_supported_shape(url: str) -> bool:
    """
    Heuristic khớp extractor Facebook của yt-dlp: /reel/SỐ, .../videos/SỐ, v.v.
    Trả về False với tab ``.../reels/`` hoặc profile chỉ có vanity name.
    """
    low = url.strip().lower()
    if "facebook.com" not in low and "fb.watch" not in low:
        return True
    if "fb.watch" in low:
        return True
    if re.search(r"facebook\.com/reel/\d+", low):
        return True
    if "watch/?v=" in low or re.search(r"[?&]v=\d+", low):
        return True
    if "video.php" in low or "story.php" in low:
        return True
    if re.search(r"facebook\.com/[^/]+/videos/[^\s?]*\d{8,}", low):
        return True
    if "/share/" in low:
        return True
    if re.search(r"facebook\.com/[^/]+/reels", low):
        return False
    if re.search(r"facebook\.com/[^/]+/videos", low) and not re.search(
        r"facebook\.com/[^/]+/videos/[^\s?]*\d{8,}", low
    ):
        return False
    m = re.match(r"https?://(?:[\w-]+\.)?facebook\.com/([^/?#]+)/?(?:[\?#].*)?$", url.strip(), re.I)
    if m:
        seg = m.group(1).lower()
        reserved = {
            "watch",
            "groups",
            "events",
            "pages",
            "reel",
            "share",
            "stories",
            "ads",
            "marketplace",
            "gaming",
            "login",
            "reg",
            "policies",
            "help",
        }
        if seg not in reserved and not seg.startswith("pfbid"):
            return False
    return True


def classify_url_type(url: str) -> str:
    u = url.lower()
    # YouTube Shorts:
    # - /shorts/<id> => single video
    # - /@channel/shorts => danh sách shorts của kênh (channel/profile)
    if re.search(r"youtube\.com/shorts/[a-z0-9_-]{6,}", u):
        return "single_video"
    if re.search(r"youtube\.com/@[^/]+/shorts/?(?:[?#].*)?$", u):
        return "channel"
    if "watch?v=" in u or "youtu.be/" in u:
        return "single_video"
    if "playlist?list=" in u:
        return "playlist"
    if "youtube.com/@" in u or "/channel/" in u or "/c/" in u or "/user/" in u:
        return "channel"
    if "tiktok.com/@" in u and "/video/" not in u:
        return "profile"
    if "tiktok.com/@" in u and "/video/" in u:
        return "single_video"
    if "facebook.com" in u or "fb.watch" in u:
        if "fb.watch" in u:
            return "single_video"
        if re.search(r"facebook\.com/[^/]+/reels", u):
            return "profile"
        if re.search(r"facebook\.com/reel/\d+", u):
            return "single_video"
        if re.search(r"facebook\.com/[^/]+/videos/\d", u):
            return "single_video"
        if re.search(r"facebook\.com/[^/]+/videos", u):
            return "profile"
        if "facebook.com/reel/" in u:
            return "single_video"
    if "/videos/" in u:
        return "single_video"
    return "unknown"


class DownloadFolderManager:
    @staticmethod
    def validate_output_dir(output_dir: str) -> None:
        p = Path(output_dir).expanduser().resolve()
        p.mkdir(parents=True, exist_ok=True)
        probe = p / ".toolfb_write_probe"
        try:
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
        except OSError as exc:
            raise RuntimeError(f"Không ghi được vào thư mục: {p}\n{exc}") from exc

    @staticmethod
    def build_output_template(job: dict[str, Any]) -> str:
        output_dir = str(Path(job["output_dir"]).expanduser().resolve())
        by_plat = bool(job.get("organize_by_platform"))
        by_up = bool(job.get("organize_by_uploader"))
        # yt-dlp field names: extractor, uploader, upload_date, id, title, ext
        if by_plat and by_up:
            return str(
                Path(output_dir)
                / "%(extractor)s"
                / "%(uploader|UnknownUploader)s"
                / "%(upload_date|unknown_date)s_%(id)s_%(title).80s.%(ext)s"
            )
        if by_plat:
            return str(
                Path(output_dir) / "%(extractor)s" / "%(upload_date|unknown_date)s_%(id)s_%(title).80s.%(ext)s"
            )
        return str(Path(output_dir) / "%(upload_date|unknown_date)s_%(id)s_%(title).80s.%(ext)s")


class DownloadMetadataStore:
    def __init__(self, *, paths: dict[str, Path] | None = None) -> None:
        self._paths = paths or ensure_downloader_layout()

    def _read_jobs(self) -> list[dict[str, Any]]:
        try:
            raw = json.loads(self._paths["jobs_file"].read_text(encoding="utf-8"))
            return [x for x in raw if isinstance(x, dict)]
        except Exception:
            return []

    def _write_jobs(self, rows: list[dict[str, Any]]) -> None:
        self._paths["jobs_file"].parent.mkdir(parents=True, exist_ok=True)
        self._paths["jobs_file"].write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _read_videos(self) -> list[dict[str, Any]]:
        try:
            raw = json.loads(self._paths["videos_file"].read_text(encoding="utf-8"))
            return [x for x in raw if isinstance(x, dict)]
        except Exception:
            return []

    def _write_videos(self, rows: list[dict[str, Any]]) -> None:
        self._paths["videos_file"].parent.mkdir(parents=True, exist_ok=True)
        self._paths["videos_file"].write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def list_jobs(self) -> list[dict[str, Any]]:
        return self._read_jobs()

    def save_job(self, job: dict[str, Any]) -> None:
        rows = self._read_jobs()
        jid = str(job.get("id") or "")
        rows = [r for r in rows if str(r.get("id") or "") != jid]
        rows.insert(0, dict(job))
        self._write_jobs(rows)

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        for r in self._read_jobs():
            if str(r.get("id") or "") == str(job_id):
                return r
        return None

    def save_downloaded_video(self, record: dict[str, Any]) -> None:
        rows = self._read_videos()
        vid = str(record.get("id") or "")
        rows = [r for r in rows if str(r.get("id") or "") != vid]
        rows.insert(0, dict(record))
        self._write_videos(rows)

    def list_downloaded_videos(self) -> list[dict[str, Any]]:
        return self._read_videos()

    def get_video(self, video_id: str) -> dict[str, Any] | None:
        for r in self._read_videos():
            if str(r.get("id") or "") == str(video_id):
                return r
        return None

    def delete_video_record(self, video_id: str, *, delete_file: bool = False) -> bool:
        rows = self._read_videos()
        found = None
        rest: list[dict[str, Any]] = []
        for r in rows:
            if str(r.get("id") or "") == str(video_id):
                found = r
            else:
                rest.append(r)
        if not found:
            return False
        if delete_file:
            vp = str(found.get("video_path") or "")
            if vp:
                try:
                    Path(vp).unlink(missing_ok=True)
                except OSError:
                    pass
            for key in ("thumbnail_path", "info_json_path"):
                pp = str(found.get(key) or "")
                if pp:
                    try:
                        Path(pp).unlink(missing_ok=True)
                    except OSError:
                        pass
        self._write_videos(rest)
        return True


class UniversalYTDLPWrapper:
    """Gọi yt-dlp: kiểm tra, lấy metadata, tải đơn / playlist."""

    def __init__(self, *, yt_cfg: dict[str, Any], log: LogFn | None = None) -> None:
        self._yt = dict(yt_cfg or {})
        self._log = log or (lambda _m: None)

    def check_available(self) -> bool:
        try:
            self._resolve_prefix()
            return True
        except Exception:
            return False

    def get_runtime_status(self) -> dict[str, Any]:
        """
        Kiểm tra yt-dlp thực sự chạy được (không chỉ tìm thấy file/module).
        Trả về ok + cách gọi + dòng phiên bản từ ``yt-dlp --version``.
        """
        try:
            prefix = self._resolve_prefix()
        except Exception as exc:
            return {
                "ok": False,
                "message": str(exc),
                "label": "",
                "version": "",
            }
        label = self._human_label_for_prefix(prefix)
        try:
            p = subprocess.run(
                [*prefix, "--version"],
                capture_output=True,
                text=True,
                timeout=25,
                encoding="utf-8",
                errors="replace",
            )
        except subprocess.TimeoutExpired:
            return {
                "ok": False,
                "message": "Timeout khi chạy yt-dlp --version.",
                "label": label,
                "version": "",
            }
        except Exception as exc:
            return {
                "ok": False,
                "message": f"Không chạy được yt-dlp: {exc}",
                "label": label,
                "version": "",
            }
        blob = (p.stdout or p.stderr or "").strip()
        version_line = blob.splitlines()[0] if blob else ""
        if p.returncode != 0:
            return {
                "ok": False,
                "message": version_line or f"Lỗi (mã {p.returncode})",
                "label": label,
                "version": "",
            }
        return {
            "ok": True,
            "message": "",
            "label": label,
            "version": version_line or "yt-dlp",
        }

    def get_install_kind(self) -> str:
        """
        ``pip_module``: app đang gọi ``python -m yt_dlp`` — ``pip install -U`` cập nhật đúng bản đang dùng.
        ``standalone``: exe/PATH — cần thay file hoặc đổi cấu hình nếu muốn dùng bản pip.
        """
        try:
            prefix = self._resolve_prefix()
        except Exception:
            return "unknown"
        if len(prefix) >= 3 and prefix[1] == "-m" and str(prefix[2]) == "yt_dlp":
            return "pip_module"
        return "standalone"

    @staticmethod
    def _human_label_for_prefix(prefix: list[str]) -> str:
        if len(prefix) >= 3 and prefix[1] == "-m" and str(prefix[2]) == "yt_dlp":
            py = Path(str(prefix[0])).name
            return f"Gói pip / Python: python -m yt_dlp (trình thực thi: {py})"
        exe = Path(prefix[0])
        name = exe.name.lower()
        if name in ("yt-dlp", "yt-dlp.exe"):
            try:
                rel = exe.resolve().relative_to(project_root())
                return f"Lệnh yt-dlp: {rel}"
            except ValueError:
                return f"Lệnh yt-dlp: {exe}"
        return f"Tiền tố lệnh: {' '.join(prefix[:3])}"

    def _configured_exe_path(self) -> Path:
        raw = str(self._yt.get("exe_path") or "").strip()
        if not raw:
            return Path()
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = (project_root() / p).resolve()
        return p

    @staticmethod
    def _probe_python_m_ytdlp() -> list[str] | None:
        """
        Cùng interpreter đang chạy app: ``python -m yt_dlp`` thường chạy được
        khi pip đã cài gói, kể cả khi ``import yt_dlp`` trong process lỗi (hiếm)
        hoặc PATH không có lệnh ``yt-dlp``.
        """
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

    def _resolve_prefix(self) -> list[str]:
        use_exe = bool(self._yt.get("use_exe", False))
        exe_path = self._configured_exe_path()
        if use_exe and exe_path.is_file():
            return [str(exe_path.resolve())]
        by_path = shutil.which(str(self._yt.get("bin") or "yt-dlp"))
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
            "hoặc đặt file exe vào tools/yt-dlp/yt-dlp.exe và bật use_exe trong config."
        )

    def get_info(self, url: str) -> dict[str, Any]:
        ut = classify_url_type(url)
        cmd = [
            *self._resolve_prefix(),
            "-J",
            "--skip-download",
            "--quiet",
            "--no-warnings",
        ]
        if ut in ("playlist", "channel", "profile"):
            cmd.append("--flat-playlist")
        proxy = str(self._yt.get("proxy") or "").strip()
        if proxy:
            cmd.extend(["--proxy", proxy])
        cmd.append(url)
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=min(120, int(self._yt.get("timeout_sec") or 600)))
        if p.returncode != 0:
            err = (p.stderr or p.stdout or "").strip()
            err = augment_facebook_unsupported_url_message(url, err[:1200])
            return {"success": False, "error": err[:2200]}
        try:
            data = json.loads(p.stdout or "{}")
        except Exception as exc:
            return {"success": False, "error": f"Parse JSON lỗi: {exc}"}
        if not isinstance(data, dict):
            return {"success": False, "error": "Không phải object JSON"}
        entries = data.get("entries")
        n = 0
        if isinstance(entries, list):
            n = len([e for e in entries if e])
        return {
            "success": True,
            "extractor": str(data.get("extractor") or data.get("ie_key") or ""),
            "title": str(data.get("title") or data.get("playlist_title") or ""),
            "uploader": str(data.get("uploader") or data.get("playlist_uploader") or ""),
            "entry_count": n if n else (1 if data.get("id") else 0),
            "url_type": ut,
        }

    @staticmethod
    def _flat_playlist_entry_url(entry: dict[str, Any]) -> str:
        """Lấy URL tải được từ một phần tử JSON flat-playlist (YouTube)."""
        u = str(entry.get("url") or "").strip()
        if u.startswith("http://") or u.startswith("https://"):
            return u
        if u.startswith("watch?"):
            return "https://www.youtube.com/" + u
        if u.startswith("/watch"):
            return "https://www.youtube.com" + u
        vid = str(entry.get("id") or "").strip()
        if vid and re.fullmatch(r"[A-Za-z0-9_-]{11}", vid):
            return f"https://www.youtube.com/watch?v={vid}"
        return ""

    def list_flat_playlist_entries(self, url: str, *, max_entries: int = 500) -> dict[str, Any]:
        """
        Liệt kê entry trong kênh / playlist (``--flat-playlist``) không tải video.
        Chỉ YouTube + loại URL ``channel`` hoặc ``playlist``.
        """
        raw = str(url or "").strip()
        if not raw:
            return {"success": False, "error": "Thiếu URL."}
        if detect_platform(raw) != "youtube":
            return {"success": False, "error": "Chỉ hỗ trợ quét danh sách cho YouTube."}
        ut = classify_url_type(raw)
        if ut not in ("playlist", "channel"):
            return {
                "success": False,
                "error": "Cần URL kênh hoặc playlist (ví dụ tab Shorts, /videos, ?list=…), không phải một video đơn.",
            }
        n = max(1, min(int(max_entries or 500), 2000))
        cmd = [
            *self._resolve_prefix(),
            "-J",
            "--skip-download",
            "--quiet",
            "--no-warnings",
            "--flat-playlist",
            "--playlist-end",
            str(n),
        ]
        proxy = str(self._yt.get("proxy") or "").strip()
        if proxy:
            cmd.extend(["--proxy", proxy])
        cmd.append(raw)
        timeout = int(self._yt.get("timeout_sec") or 600)
        try:
            p = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=min(max(120, n // 2 + 60), timeout),
                encoding="utf-8",
                errors="replace",
            )
        except subprocess.TimeoutExpired:
            return {"success": False, "error": f"Hết thời gian khi quét danh sách (>{timeout}s)."}
        except Exception as exc:  # noqa: BLE001
            return {"success": False, "error": str(exc)}
        if p.returncode != 0:
            err = (p.stderr or p.stdout or "").strip()
            return {"success": False, "error": err[:2200]}
        try:
            data = json.loads(p.stdout or "{}")
        except Exception as exc:
            return {"success": False, "error": f"Parse JSON lỗi: {exc}"}
        if not isinstance(data, dict):
            return {"success": False, "error": "Không phải object JSON"}
        entries_raw = data.get("entries")
        out: list[dict[str, str]] = []
        if isinstance(entries_raw, list):
            for e in entries_raw:
                if not isinstance(e, dict):
                    continue
                play_url = self._flat_playlist_entry_url(e)
                if not play_url:
                    continue
                title = str(e.get("title") or e.get("id") or play_url)[:500]
                out.append({"title": title, "url": play_url})
        return {
            "success": True,
            "entries": out,
            "playlist_title": str(data.get("title") or data.get("playlist_title") or ""),
            "extractor": str(data.get("extractor") or data.get("ie_key") or ""),
        }

    def download(
        self,
        *,
        url: str,
        output_template: str,
        archive_path: Path,
        url_type: str,
        max_videos: int,
        skip_existing: bool,
        write_info_json: bool,
        write_thumbnail: bool,
        cancel_event: threading.Event | None,
        cookie_path: str = "",
        log_lines: LogFn | None = None,
    ) -> dict[str, Any]:
        log_lines = log_lines or self._log
        prefix = self._resolve_prefix()
        fmt = str(self._yt.get("format") or "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/best")
        merge_fmt = str(self._yt.get("merge_output_format") or "mp4")
        sleep_sec = max(0, int(self._yt.get("sleep_interval_sec") or 0))
        max_fs = int(self._yt.get("max_filesize_mb") or 300)
        timeout = int(self._yt.get("timeout_sec") or 600)
        cmd: list[str] = [
            *prefix,
            "-f",
            fmt,
            "--merge-output-format",
            merge_fmt,
            "--newline",
            "--no-progress",
            "--print",
            "after_move:%(filepath)s",
            "--max-filesize",
            f"{max_fs}M",
            "-o",
            output_template,
        ]
        if sleep_sec:
            cmd.extend(["--sleep-interval", str(sleep_sec), "--max-sleep-interval", str(max(sleep_sec, 5))])
        if write_info_json:
            cmd.append("--write-info-json")
        if write_thumbnail:
            cmd.append("--write-thumbnail")
        if skip_existing:
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            cmd.extend(["--download-archive", str(archive_path)])
        ut = (url_type or "unknown").lower()
        if ut in ("playlist", "channel", "profile"):
            cmd.append("--yes-playlist")
            cmd.extend(["--playlist-end", str(max(1, int(max_videos)))])
        else:
            cmd.append("--no-playlist")
        proxy = str(self._yt.get("proxy") or "").strip()
        if proxy:
            cmd.extend(["--proxy", proxy])
        cookie_arg, cookie_tmp = _to_ytdlp_cookie_file(cookie_path)
        if cookie_arg:
            cmd.extend(["--cookies", cookie_arg])
        cmd.append(url)

        log_lines(f"[yt-dlp] {' '.join(cmd[:12])} ... ({len(cmd)} args)")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        filepaths: list[str] = []
        stderr_chunks: list[str] = []

        def _read_stderr() -> None:
            if not proc.stderr:
                return
            for line in proc.stderr:
                stderr_chunks.append(line)
                if len(stderr_chunks) <= 30 or "ERROR" in line:
                    log_lines(line.rstrip())

        rt = threading.Thread(target=_read_stderr, daemon=True)
        rt.start()

        if proc.stdout:
            for line in proc.stdout:
                if cancel_event and cancel_event.is_set():
                    proc.terminate()
                    break
                line = line.strip()
                if not line:
                    continue
                # after_move:/path/to/file.mp4
                if line.startswith("after_move:"):
                    fp = line.split(":", 1)[1].strip().strip('"')
                    if fp and Path(fp).is_file():
                        filepaths.append(fp)
                elif Path(line).is_file() and line.lower().endswith((".mp4", ".webm", ".mkv", ".mov")):
                    filepaths.append(line)

        try:
            rc = proc.wait(timeout=timeout) if proc.poll() is None else (proc.returncode or 0)
        except subprocess.TimeoutExpired:
            proc.kill()
            rc = -1
            log_lines("[yt-dlp] Timeout — đã dừng process.")
        rt.join(timeout=2)
        err_full = "".join(stderr_chunks)
        if cancel_event and cancel_event.is_set():
            if cookie_tmp is not None:
                cookie_tmp.unlink(missing_ok=True)
            return {"success": False, "error": "Đã hủy/tạm dừng bởi người dùng.", "filepaths": filepaths, "stderr": err_full[-2000:]}
        if rc != 0:
            low = err_full.lower()
            if any(x in low for x in ("private", "login required", "sign in", "drm", "members only")):
                if cookie_tmp is not None:
                    cookie_tmp.unlink(missing_ok=True)
                return {
                    "success": False,
                    "error": "need_manual_upload",
                    "message": "Không tải được bằng yt-dlp (private/login/DRM). Vui lòng tải tay và chọn file local.",
                    "stderr": err_full[-2000:],
                    "filepaths": filepaths,
                }
            err_snip = err_full.strip()[-1200:] or f"yt-dlp exit {rc}"
            err_snip = augment_facebook_unsupported_url_message(url, err_snip)
            if cookie_tmp is not None:
                cookie_tmp.unlink(missing_ok=True)
            return {"success": False, "error": err_snip[:2200], "filepaths": filepaths}
        if not filepaths:
            low = err_full.lower()
            if skip_existing and any(
                x in low for x in ("already been downloaded", "has already been recorded", "skipping", "in the archive")
            ):
                if cookie_tmp is not None:
                    cookie_tmp.unlink(missing_ok=True)
                return {"success": True, "filepaths": [], "stderr": err_full[-1500:], "skipped_only": True}
            if cookie_tmp is not None:
                cookie_tmp.unlink(missing_ok=True)
            return {
                "success": False,
                "error": "Không nhận được đường dẫn file từ yt-dlp (có thể đã skip vì trùng archive).",
                "stderr": err_full[-1500:],
                "filepaths": [],
            }
        if cookie_tmp is not None:
            cookie_tmp.unlink(missing_ok=True)
        return {"success": True, "filepaths": filepaths, "stderr": err_full[-1000:]}


class BulkDownloadManager:
    """Một job bulk = một lần gọi yt-dlp (playlist/channel)."""

    def __init__(self, *, wrapper: UniversalYTDLPWrapper) -> None:
        self._w = wrapper


class DownloadQueueManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._queue: list[str] = []

    def enqueue(self, job_id: str) -> None:
        with self._lock:
            self._queue.append(job_id)

    def dequeue(self) -> str | None:
        with self._lock:
            if not self._queue:
                return None
            return self._queue.pop(0)


@dataclass
class DownloadJobOptions:
    url: str
    platform: str
    url_type: str
    max_videos: int
    output_dir: str
    organize_by_platform: bool
    organize_by_uploader: bool
    skip_existing: bool
    write_info_json: bool
    write_thumbnail: bool


class UniversalVideoDownloader:
    """
    Module độc lập: tạo job, chạy yt-dlp, lưu metadata, thư viện video.
    Không tách keyframes / Gemini.
    """

    def __init__(self, *, log: LogFn | None = None) -> None:
        self._log = log or (lambda _m: None)
        self._cfg_root = load_universal_video_downloader_config()
        self._uvd = dict(self._cfg_root.get("universal_video_downloader") or {})
        self._paths = ensure_downloader_layout()
        self._store = DownloadMetadataStore(paths=self._paths)
        self._yt = UniversalYTDLPWrapper(yt_cfg=dict(self._uvd.get("yt_dlp") or {}), log=self._log)
        self._cancel = threading.Event()
        self._active_job_id: str | None = None

    def check_ytdlp(self) -> bool:
        return self._yt.check_available()

    def get_ytdlp_status(self) -> dict[str, Any]:
        """Giống kiểm tra tay ``yt-dlp --version``; dùng cho tab Tải video."""
        return self._yt.get_runtime_status()

    def get_ytdlp_update_check(self) -> dict[str, Any]:
        """
        So sánh bản đang chạy với PyPI.
        ``needs_upgrade``: nên chạy ``pip install -U yt-dlp`` (hoặc cài mới nếu chưa có).
        """
        kind = self._yt.get_install_kind()
        st = self._yt.get_runtime_status()
        local_line = str(st.get("version") or "").strip()
        pypi = fetch_ytdlp_latest_version_pypi()
        result: dict[str, Any] = {
            "local_ok": bool(st.get("ok")),
            "local_version_line": local_line,
            "local_label": str(st.get("label") or ""),
            "install_kind": kind,
            "pypi_ok": bool(pypi.get("ok")),
            "pypi_version": str(pypi.get("version") or ""),
            "pypi_error": str(pypi.get("error") or ""),
            "comparison": None,
            "needs_upgrade": False,
            "offer_optional_pip": False,
        }
        if not pypi.get("ok"):
            return result
        pt = _parse_ytdlp_semverish(str(pypi.get("version") or ""))
        if not pt:
            return result
        if not st.get("ok"):
            result["needs_upgrade"] = True
            result["comparison"] = -1
            return result
        lt = _parse_ytdlp_semverish(local_line)
        if not lt:
            result["comparison_uncertain"] = True
            result["offer_optional_pip"] = True
            return result
        cmp_ = _compare_version_tuples(lt, pt)
        result["comparison"] = cmp_
        result["needs_upgrade"] = cmp_ < 0
        return result

    def upgrade_ytdlp_via_pip(self) -> dict[str, Any]:
        """Chạy ``pip install -U yt-dlp`` cho ``sys.executable`` của app."""
        return run_pip_upgrade_ytdlp()

    def cancel_current(self) -> None:
        self._cancel.set()

    def clear_cancel(self) -> None:
        self._cancel.clear()

    def is_cancel_requested(self) -> bool:
        return self._cancel.is_set()

    def create_download_job(self, url: str, options: dict[str, Any]) -> dict[str, Any]:
        dl = dict(self._uvd.get("download") or {})
        url = str(url or "").strip()
        if not url:
            raise ValueError("Thiếu URL")
        platform = str(options.get("platform") or "").strip().lower()
        if platform in ("", "auto", "auto detect"):
            platform = detect_platform(url)
        url_type = str(options.get("url_type") or "").strip().lower()
        if url_type in ("", "auto", "auto detect"):
            url_type = classify_url_type(url)
        out_dir = str(options.get("output_dir") or dl.get("default_output_dir") or "").strip()
        if not out_dir:
            out_dir = str(project_root() / "data" / "downloads")
        out_dir = str(Path(out_dir).expanduser().resolve())
        max_videos = int(options.get("max_videos") or self._uvd.get("yt_dlp", {}).get("max_videos_default") or 50)
        job = {
            "id": f"dl_{uuid.uuid4().hex[:10]}",
            "url": url,
            "platform": platform,
            "url_type": url_type,
            "max_videos": max(1, max_videos),
            "output_dir": out_dir,
            "organize_by_platform": bool(options.get("organize_by_platform", dl.get("organize_by_platform", True))),
            "organize_by_uploader": bool(options.get("organize_by_uploader", dl.get("organize_by_uploader", True))),
            "skip_existing": bool(options.get("skip_existing", dl.get("skip_existing", True))),
            "write_info_json": bool(options.get("write_info_json", self._uvd.get("yt_dlp", {}).get("write_info_json", True))),
            "write_thumbnail": bool(options.get("write_thumbnail", self._uvd.get("yt_dlp", {}).get("write_thumbnail", True))),
            "cookie_path": str(options.get("cookie_path") or ""),
            "status": "pending",
            "downloaded_files": [],
            "failed_items": [],
            "created_at": _now_iso(),
            "started_at": "",
            "completed_at": "",
            "error_message": "",
        }
        self._store.save_job(job)
        return job

    def run_download_job(self, job_id: str) -> dict[str, Any]:
        job = self._store.get_job(job_id)
        if not job:
            raise KeyError(f"Không có job: {job_id}")
        self._active_job_id = job_id
        self.clear_cancel()
        DownloadFolderManager.validate_output_dir(job["output_dir"])
        tmpl = DownloadFolderManager.build_output_template(job)
        job["status"] = "running"
        job["started_at"] = _now_iso()
        job["error_message"] = ""
        self._store.save_job(job)

        ret = self._yt.download(
            url=str(job["url"]),
            output_template=tmpl,
            archive_path=self._paths["archive"],
            url_type=str(job["url_type"]),
            max_videos=int(job["max_videos"]),
            skip_existing=bool(job["skip_existing"]),
            write_info_json=bool(job["write_info_json"]),
            write_thumbnail=bool(job["write_thumbnail"]),
            cookie_path=str(job.get("cookie_path") or ""),
            cancel_event=self._cancel,
            log_lines=self._log,
        )
        filepaths: list[str] = list(dict.fromkeys(ret.get("filepaths") or []))
        if ret.get("skipped_only"):
            job["status"] = "completed"
            job["completed_at"] = _now_iso()
            job["error_message"] = ""
            job["downloaded_files"] = []
            self._store.save_job(job)
            self._active_job_id = None
            return job
        if not bool(ret.get("success")):
            err = str(ret.get("error") or "Lỗi không xác định")
            if err == "need_manual_upload":
                job["status"] = "need_manual_upload"
                job["error_message"] = str(ret.get("message") or err)
            else:
                job["status"] = "failed"
                job["error_message"] = err[:2000]
            job["completed_at"] = _now_iso()
            job["failed_items"] = [{"url": job["url"], "error": job["error_message"]}]
            self._store.save_job(job)
            self._active_job_id = None
            return job

        records: list[dict[str, Any]] = []
        for fp in filepaths:
            rec = self._build_video_record(video_path=fp, job=job)
            records.append(rec)
            self._store.save_downloaded_video(rec)
        job["downloaded_files"] = [r["video_path"] for r in records]
        job["status"] = "completed"
        job["completed_at"] = _now_iso()
        self._store.save_job(job)
        self._active_job_id = None
        return job

    def _build_video_record(self, *, video_path: str, job: dict[str, Any]) -> dict[str, Any]:
        vp = Path(video_path).resolve()
        info_path = vp.with_suffix(".info.json")
        if not info_path.is_file():
            alt = Path(str(vp) + ".info.json")
            if alt.is_file():
                info_path = alt
        thumb = self._find_thumbnail(vp)
        title = vp.stem
        uploader = ""
        duration = 0.0
        upload_date = ""
        source_url = str(job.get("url") or "")
        if info_path.is_file():
            try:
                meta = json.loads(info_path.read_text(encoding="utf-8"))
                if isinstance(meta, dict):
                    title = str(meta.get("title") or title)
                    uploader = str(meta.get("uploader") or meta.get("channel") or "")
                    duration = float(meta.get("duration") or 0)
                    upload_date = str(meta.get("upload_date") or "")
                    source_url = str(meta.get("webpage_url") or meta.get("original_url") or source_url)
            except Exception:
                pass
        return {
            "id": f"src_video_{uuid.uuid4().hex[:10]}",
            "download_job_id": str(job.get("id") or ""),
            "platform": str(job.get("platform") or ""),
            "source_url": source_url,
            "title": title,
            "uploader": uploader,
            "duration": duration,
            "upload_date": upload_date,
            "video_path": str(vp),
            "thumbnail_path": str(thumb) if thumb else "",
            "info_json_path": str(info_path) if info_path.is_file() else "",
            "status": "downloaded",
            "ready_for_analysis": True,
            "created_at": _now_iso(),
        }

    @staticmethod
    def _find_thumbnail(video_path: Path) -> Path | None:
        base = video_path.with_suffix("")
        for ext in (".jpg", ".webp", ".png", ".jpeg"):
            p = Path(str(base) + ext)
            if p.is_file():
                return p
        parent = video_path.parent
        stem = video_path.stem
        for p in parent.glob(stem + ".*"):
            if p.suffix.lower() in (".jpg", ".webp", ".png", ".jpeg"):
                return p
        return None

    def list_downloaded_videos(self) -> list[dict[str, Any]]:
        return self._store.list_downloaded_videos()

    def get_downloaded_video(self, video_id: str) -> dict[str, Any] | None:
        return self._store.get_video(video_id)

    def list_jobs(self) -> list[dict[str, Any]]:
        return self._store.list_jobs()

    def check_url(self, url: str) -> dict[str, Any]:
        return self._yt.get_info(url)

    def list_flat_playlist_entries(self, url: str, *, max_entries: int = 500) -> dict[str, Any]:
        """Ủy quyền tới ``UniversalYTDLPWrapper`` (quét flat-playlist cho kênh/playlist YouTube)."""
        return self._yt.list_flat_playlist_entries(url, max_entries=max_entries)

    def send_to_reverse_prompt_engine(self, video_id: str) -> dict[str, Any]:
        v = self._store.get_video(video_id)
        if not v:
            raise KeyError("Không tìm thấy video trong thư viện")
        p = Path(str(v.get("video_path") or ""))
        if not p.is_file():
            raise FileNotFoundError(f"File không tồn tại: {p}")
        return {
            "source_type": "downloaded_video",
            "video_id": str(v.get("id") or ""),
            "local_video_path": str(p.resolve()),
            "source_url": str(v.get("source_url") or ""),
            "ready_for_analysis": True,
            "title": str(v.get("title") or ""),
        }

    def send_to_ai_video_library(self, video_id: str) -> dict[str, Any]:
        v = self._store.get_video(video_id)
        if not v:
            raise KeyError("Không tìm thấy video trong thư viện")
        p = Path(str(v.get("video_path") or ""))
        if not p.is_file():
            raise FileNotFoundError(f"File không tồn tại: {p}")
        from src.services.ai_video_store import ensure_ai_video_layout

        temp = ensure_ai_video_layout()["temp"]
        temp.mkdir(parents=True, exist_ok=True)
        out = temp / "downloader_picked_source.json"
        payload = {
            "schema": "toolfb.downloader.ai_video_source.v1",
            "video_id": str(v.get("id") or ""),
            "local_video_path": str(p.resolve()),
            "title": str(v.get("title") or ""),
            "platform": str(v.get("platform") or ""),
            "saved_at": _now_iso(),
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return {"manifest_path": str(out), "payload": payload}

    def delete_downloaded_video(self, video_id: str, *, delete_file: bool = False) -> bool:
        return self._store.delete_video_record(video_id, delete_file=delete_file)

    def remember_output_dir(self, path: str) -> None:
        cfg_path = project_root() / "config" / "universal_video_downloader.json"
        if not bool(self._uvd.get("download", {}).get("remember_last_output_dir", True)):
            return
        # Cập nhật nhẹ last_output_dir trong file config nếu có
        try:
            raw: dict[str, Any]
            if cfg_path.is_file():
                raw = json.loads(cfg_path.read_text(encoding="utf-8"))
            else:
                raw = {}
            uvd = dict(raw.get("universal_video_downloader") or {})
            dl = dict(uvd.get("download") or {})
            dl["last_output_dir"] = str(Path(path).expanduser().resolve())
            uvd["download"] = dl
            raw["universal_video_downloader"] = uvd
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            cfg_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        except Exception:
            pass
