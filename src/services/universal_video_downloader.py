from __future__ import annotations

import json
import os
import queue
from collections import deque
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

from src.utils.json_store_lock import json_file_lock
from src.utils.paths import project_root

LogFn = Callable[[str], None]
# ``download()``: ``event`` = start | file_complete | stderr_activity | error_line
ProgressHook = Callable[[dict[str, Any]], None]

# Giới hạn quét playlist/kênh (YouTube/TikTok) — có thể vài nghìn entry.
UV_MAX_PLAYLIST_ENTRIES = 10_000
UV_PLAYLIST_SCAN_CHUNK = 400
# Trên ngưỡng này tab Tải video dùng tải tuần tự từng URL (ổn định, dễ hủy).
UV_DOWNLOAD_SEQUENTIAL_THRESHOLD = 25

# Sự kiện Tk: tab Tải video báo job xong → Video Editor làm mới combobox job.
DOWNLOAD_JOB_FINISHED_TK_EVENT = "<<ToolFB_DownloadJobFinished>>"
PENDING_VE_JOB_FILE = "pending_video_editor_job.json"
TOOLFB_PENDING_DOWNLOAD_JOB_ATTR = "_toolfb_pending_download_job_id"
TOOLFB_VE_AUTO_IMPORT_DOWNLOAD_ATTR = "_toolfb_ve_auto_import_download"
_DOWNLOADER_MERGE_LOCK = threading.Lock()


def pending_video_editor_job_path(*, paths: dict[str, Path] | None = None) -> Path:
    layout = paths or ensure_downloader_layout()
    return layout["root"] / PENDING_VE_JOB_FILE


def write_pending_video_editor_job(job_id: str, *, paths: dict[str, Path] | None = None) -> None:
    """Ghi job chờ nạp vào Video Editor (file JSON + caller nên set attr trên root Tk)."""
    jid = str(job_id or "").strip()
    if not jid:
        return
    p = pending_video_editor_job_path(paths=paths)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            json.dumps(
                {"job_id": jid, "saved_at": datetime.now().replace(microsecond=0).isoformat()},
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    except OSError:
        pass


def read_pending_video_editor_job(*, paths: dict[str, Path] | None = None, consume: bool = False) -> str:
    """Đọc job chờ Video Editor — quét mọi thư mục metadata (hoặc một ``paths`` cụ thể)."""
    if paths is None:
        ensure_downloader_layout()
        scan_roots = discover_downloader_data_roots()
    else:
        scan_roots = [Path(paths["root"]).resolve()]
    best_jid = ""
    best_ts = 0.0
    for dl_root in scan_roots:
        pf = dl_root / PENDING_VE_JOB_FILE
        if not pf.is_file():
            continue
        try:
            raw = json.loads(pf.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(raw, dict):
            continue
        jid = str(raw.get("job_id") or "").strip()
        if not jid:
            continue
        ts = _parse_download_job_time(raw.get("saved_at"))
        if ts >= best_ts:
            best_ts = ts
            best_jid = jid
    if best_jid:
        if consume:
            try:
                pending_video_editor_job_path(paths=paths).unlink(missing_ok=True)
            except OSError:
                pass
        return best_jid
    return ""


def clear_pending_video_editor_job(*, paths: dict[str, Path] | None = None) -> None:
    pending_video_editor_job_path(paths=paths).unlink(missing_ok=True)


def set_root_pending_download_job(root: Any, job_id: str) -> None:
    jid = str(job_id or "").strip()
    if not jid:
        return
    try:
        setattr(root, TOOLFB_PENDING_DOWNLOAD_JOB_ATTR, jid)
    except Exception:
        pass


def get_root_pending_download_job(root: Any, *, paths: dict[str, Path] | None = None) -> str:
    """
    Đọc job chờ từ attr Tk (main thread) hoặc file pending.

    **Chỉ gọi với ``root`` trên main thread** — worker dùng ``read_pending_video_editor_job(paths=…)``.
    """
    try:
        jid = str(getattr(root, TOOLFB_PENDING_DOWNLOAD_JOB_ATTR, "") or "").strip()
    except Exception:
        jid = ""
    if jid:
        return jid
    return read_pending_video_editor_job(paths=paths, consume=False)


def clear_root_pending_download_job(root: Any, *, paths: dict[str, Path] | None = None) -> None:
    try:
        if hasattr(root, TOOLFB_PENDING_DOWNLOAD_JOB_ATTR):
            delattr(root, TOOLFB_PENDING_DOWNLOAD_JOB_ATTR)
    except Exception:
        try:
            setattr(root, TOOLFB_PENDING_DOWNLOAD_JOB_ATTR, "")
        except Exception:
            pass
    clear_pending_video_editor_job(paths=paths)


def arm_auto_import_download_job(root: Any) -> None:
    try:
        setattr(root, TOOLFB_VE_AUTO_IMPORT_DOWNLOAD_ATTR, True)
    except Exception:
        pass


def consume_auto_import_download_job(root: Any) -> bool:
    try:
        armed = bool(getattr(root, TOOLFB_VE_AUTO_IMPORT_DOWNLOAD_ATTR, False))
    except Exception:
        armed = False
    if not armed:
        return False
    try:
        delattr(root, TOOLFB_VE_AUTO_IMPORT_DOWNLOAD_ATTR)
    except Exception:
        try:
            setattr(root, TOOLFB_VE_AUTO_IMPORT_DOWNLOAD_ATTR, False)
        except Exception:
            pass
    return True

# yt-dlp: ưu tiên tối thiểu 720p (HD); trần chiều cao 2160 (Short dọc / 4K). Fallback dần xuống thấp hơn nếu site không có HD.
YTDLP_FORMAT_HD_MERGE = (
    "bestvideo[height>=720][height<=2160]+bestaudio/"
    "bestvideo[height>=720]+bestaudio/"
    "bestvideo*+bestaudio/"
    "best[height>=720]/best"
)
YTDLP_FORMAT_HD_SINGLE = (
    "best[height>=720][height<=2160]/best[height>=720]/best[ext=mp4]/best"
)

# yt-dlp YouTube (EJS): stderr có thể lặp cảnh báo thiếu JS runtime.
_YTDLP_JS_WARN_SNIP = "No supported JavaScript runtime"


def ytdlp_js_runtimes_cli_args(yt_cfg: dict[str, Any]) -> list[str]:
    """
    Trả về ``--js-runtimes …`` cho yt-dlp khi cấu hình hoặc tự tìm node/deno/bun trên PATH.
    Chuỗi rỗng trong ``js_runtimes`` = tự phát hiện; không tìm thấy runtime → danh sách rỗng.
    """
    raw = str(yt_cfg.get("js_runtimes") or "").strip()
    if raw:
        return ["--js-runtimes", raw]
    for name in ("node", "deno", "bun"):
        path = shutil.which(name)
        if path:
            return ["--js-runtimes", f"{name}:{path}"]
    return []


def _paths_seen_and_list_for_job(video_rows: list[dict[str, Any]], job_id: str) -> tuple[set[str], list[str]]:
    """Một lần đọc danh sách video: đường dẫn đã thuộc job + tập lower-case để trùng lặp."""
    jid = str(job_id)
    seen: set[str] = set()
    paths: list[str] = []
    for r in video_rows:
        if not isinstance(r, dict):
            continue
        if str(r.get("download_job_id") or "") != jid:
            continue
        vp = str(r.get("video_path") or "").strip()
        if not vp:
            continue
        try:
            norm = str(Path(vp).expanduser().resolve())
            key = norm.lower()
        except OSError:
            norm = vp
            key = vp.lower()
        if key in seen:
            continue
        seen.add(key)
        paths.append(norm)
    return seen, paths


def extract_failed_download_pairs(job: dict[str, Any] | None) -> list[tuple[str, str]]:
    """Lấy danh sách (url, lỗi) từ ``failed_items`` của job tải video."""
    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()
    for it in (job or {}).get("failed_items") or []:
        if not isinstance(it, dict):
            continue
        url = str(it.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        pairs.append((url, str(it.get("error") or "").strip()))
    return pairs


def write_failed_download_urls_log(
    *,
    platform: str,
    job_id: str,
    failed_pairs: list[tuple[str, str]],
    log_fn: LogFn | None = None,
) -> Path | None:
    """
    Ghi URL lỗi sau batch tải vào ``logs/download_failures/failed_<nền_tảng>_<job>_<time>.txt``.
    """
    if not failed_pairs:
        return None
    out_dir = project_root() / "logs" / "download_failures"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_plat = re.sub(r"[^\w.-]+", "_", str(platform or "unknown").strip())[:48] or "unknown"
    safe_jid = re.sub(r"[^\w.-]+", "_", str(job_id or "job").strip())[:32] or "job"
    path = out_dir / f"failed_{safe_plat}_{safe_jid}_{ts}.txt"
    lines = [
        f"# Job: {job_id}",
        f"# Nền tảng: {platform}",
        f"# Số URL lỗi: {len(failed_pairs)}",
        f"# Ghi lúc: {datetime.now().replace(microsecond=0).isoformat()}",
        "",
    ]
    for url, err in failed_pairs:
        lines.append(url)
        if err:
            lines.append(f"  # {err.replace(chr(10), ' ')[:500]}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    msg = f"[INFO] Đã ghi {len(failed_pairs)} URL lỗi → {path}"
    if log_fn is not None:
        log_fn(msg)
    return path


def _ytdlp_subprocess_kw() -> dict[str, Any]:
    """
    Windows: không gắn cửa sổ console cho yt-dlp.exe / python -m yt_dlp
    (tránh nhấp nháy cửa sổ đen khi tải nhiều URL liên tiếp).
    """
    if os.name != "nt":
        return {}
    if not hasattr(subprocess, "CREATE_NO_WINDOW"):
        return {}
    return {"creationflags": int(subprocess.CREATE_NO_WINDOW)}


def _resolve_ffmpeg_for_ytdlp() -> str:
    """Ưu tiên ffmpeg cạnh app, rồi PATH."""
    local = project_root() / "tools" / "ffmpeg" / "bin" / ("ffmpeg.exe" if os.name == "nt" else "ffmpeg")
    if local.is_file():
        return str(local.resolve())
    via_path = shutil.which("ffmpeg")
    return str(via_path or "").strip()

YTDLP_PYPI_JSON_URL = "https://pypi.org/pypi/yt-dlp/json"
# File yt-dlp.exe đóng gói kèm app; nhỏ hơn ngưỡng này coi như tải lỗi / placeholder.
YTDLP_BUNDLE_EXE_MIN_BYTES = 400_000
YTDLP_WIN_EXE_URL = "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe"

_YTDLP_MEDIA_EXTENSIONS = frozenset({".mp4", ".webm", ".mkv", ".mov", ".m4v", ".avi"})

# Chuỗi stderr yt-dlp khi đã có trong archive / bỏ qua tải lại (đa ngôn ngữ / nhiều phiên bản).
_YTDLP_SKIP_OR_ARCHIVE_MARKERS = (
    "already been downloaded",
    "has already been recorded",
    "has already been downloaded",
    "in the archive",
    "in archive",
    "skipping",
    "already present",
    "was skipped",
    "nothing to download",
    "not downloading",
    "video already exists",
    "already in the archive",
    "đã có trong archive",
    "recorded in the archive",
)


def _output_root_dir_from_ytdlp_template(output_template: str) -> Path | None:
    """Phần thư mục thật trước ký hiệu %(… trong -o template của yt-dlp."""
    tmpl = str(output_template or "").strip().replace("/", os.sep)
    if "%(" in tmpl:
        tmpl = tmpl.split("%(", 1)[0].rstrip(os.sep)
    if not tmpl:
        return None
    try:
        p = Path(tmpl)
        if not p.is_absolute():
            return (Path.cwd() / p).resolve()
        return p
    except OSError:
        return None


def _extract_video_id_for_scan(url: str) -> str:
    u = str(url or "").strip()
    if not u:
        return ""
    m = re.search(r"youtube\.com/shorts/([A-Za-z0-9_-]{11})", u, re.I)
    if m:
        return m.group(1)
    m = re.search(r"youtube\.com/live/([A-Za-z0-9_-]{11})", u, re.I)
    if m:
        return m.group(1)
    m = re.search(r"music\.youtube\.com/watch\?v=([A-Za-z0-9_-]{11})", u, re.I)
    if m:
        return m.group(1)
    m = re.search(r"[?&]v=([A-Za-z0-9_-]{11})", u, re.I)
    if m:
        return m.group(1)
    m = re.search(r"youtu\.be/([A-Za-z0-9_-]{11})", u, re.I)
    if m:
        return m.group(1)
    m = re.search(r"/embed/([A-Za-z0-9_-]{11})", u, re.I)
    if m:
        return m.group(1)
    m = re.search(r"tiktok\.com/@[^/]+/video/(\d+)", u, re.I)
    if m:
        return m.group(1)
    m = re.search(r"facebook\.com/reel/(\d+)", u, re.I)
    if m:
        return m.group(1)
    m = re.search(r"instagram\.com/(?:reel|p|tv)/([A-Za-z0-9_-]+)", u, re.I)
    if m:
        return m.group(1)
    m = re.search(r"instagr\.am/p/([A-Za-z0-9_-]+)", u, re.I)
    if m:
        return m.group(1)
    return ""


def _norm_url_key(u: str) -> str:
    u = str(u or "").strip().split("&list=", 1)[0].strip()
    return u.rstrip("/")


def _batch_urls_still_missing(requested: list[str], resolved_media_paths: list[str]) -> list[str]:
    """
    Sau tải batch một lần yt-dlp: URL nào không ghép được một file riêng.
    Mỗi đường dẫn chỉ khớp tối đa một URL (tránh báo «lỗi» hàng loạt khi nhiều file cùng meta).
    """
    if not requested:
        return []
    if not resolved_media_paths:
        return list(requested)

    def _meta_for_fp(fp: str) -> tuple[set[str], set[str]]:
        keys: set[str] = set()
        ids: set[str] = set()
        vp = Path(fp)
        for inf in (vp.with_suffix(".info.json"), Path(str(vp) + ".info.json")):
            if not inf.is_file():
                continue
            try:
                meta = json.loads(inf.read_text(encoding="utf-8"))
            except Exception:
                break
            if not isinstance(meta, dict):
                break
            w = str(meta.get("webpage_url") or meta.get("original_url") or "").strip()
            if w:
                keys.add(_norm_url_key(w))
            mid = str(meta.get("id") or "").strip()
            if mid:
                ids.add(mid)
            break
        return keys, ids

    meta_by_idx = [_meta_for_fp(fp) for fp in resolved_media_paths]
    unused: set[int] = set(range(len(resolved_media_paths)))
    missing: list[str] = []
    for u in requested:
        nk = _norm_url_key(u)
        vid = _extract_video_id_for_scan(u)
        chosen = -1
        for i in sorted(unused):
            keys, ids = meta_by_idx[i]
            if vid and vid in ids:
                chosen = i
                break
            if nk and nk in keys:
                chosen = i
                break
            matched_gk = False
            for gk in keys:
                if nk and (nk in gk or gk in nk):
                    matched_gk = True
                    break
            if matched_gk:
                chosen = i
                break
        if chosen < 0 and vid:
            for i in sorted(unused):
                if vid in Path(resolved_media_paths[i]).name:
                    chosen = i
                    break
        if chosen >= 0:
            unused.discard(chosen)
        else:
            missing.append(u)
    return missing


def _repair_mojibake_text(text: str) -> str:
    """
    Sửa chuỗi tiếng Việt bị vỡ dấu khi nguồn bị decode sai (UTF-8 <-> cp1252/latin-1).
    """
    s = str(text or "")
    if not s:
        return s
    low = s.lower()
    if ("�" not in s) and not any(tok in low for tok in ("ã", "â", "ä", "á»", "áº")):
        return s
    for src_enc in ("cp1252", "latin-1"):
        try:
            fixed = s.encode(src_enc, errors="strict").decode("utf-8", errors="strict")
        except Exception:
            continue
        if fixed and fixed != s:
            return fixed
    return s


def scan_output_dir_for_existing_media(*, root: Path, url: str) -> list[str]:
    """
    Khi yt-dlp không in after_move (skip archive, merge, v.v.) nhưng file đã nằm trên đĩa.
    """
    root = Path(root).expanduser().resolve()
    if not root.is_dir():
        return []
    vid = _extract_video_id_for_scan(url)
    hits: list[tuple[float, str]] = []
    key = _norm_url_key(url)

    if vid and len(vid) >= 8:
        try:
            for p in root.rglob(f"*{vid}*"):
                if p.is_file() and p.suffix.lower() in _YTDLP_MEDIA_EXTENSIONS:
                    try:
                        hits.append((p.stat().st_mtime, str(p.resolve())))
                    except OSError:
                        pass
        except OSError:
            pass

    try:
        for meta_path in root.rglob("*.info.json"):
            if not meta_path.is_file():
                continue
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(meta, dict):
                continue
            mid = str(meta.get("id") or "").strip()
            purl = str(meta.get("webpage_url") or meta.get("original_url") or "").strip()
            match = False
            if vid and mid == vid:
                match = True
            elif key and purl:
                pk = _norm_url_key(purl)
                if pk == key or key in pk or pk in key:
                    match = True
            if not match:
                continue
            stem = meta_path.name
            if not stem.endswith(".info.json"):
                continue
            base = stem[: -len(".info.json")]
            parent = meta_path.parent
            for ext in _YTDLP_MEDIA_EXTENSIONS:
                cand = parent / (base + ext)
                if cand.is_file():
                    try:
                        hits.append((cand.stat().st_mtime, str(cand.resolve())))
                    except OSError:
                        pass
                    break
    except OSError:
        pass

    hits.sort(key=lambda x: x[0], reverse=True)
    seen: set[str] = set()
    out: list[str] = []
    for _, fp in hits:
        if fp not in seen:
            seen.add(fp)
            out.append(fp)
    return out


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
    if getattr(sys, "frozen", False):
        return {
            "ok": False,
            "message": (
                "Bản .exe đóng gói không cài yt-dlp bằng pip vào interpreter này. "
                "Dùng file yt-dlp.exe kèm app (thư mục tools/yt-dlp/) hoặc tải bản phát hành mới."
            ),
            "stdout": "",
            "stderr": "",
            "returncode": -1,
        }
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


_RELEASE_BUNDLE_DIR_NAMES = ("ToolFB_release_bundle",)
_APP_VARIANT_DIR_NAMES = ("exe_gui", "portable_clean", "ToolFB_portable_clean")


def _row_recency_ts(row: dict[str, Any], *keys: str) -> float:
    """Timestamp mới nhất từ các trường ISO trong một bản ghi metadata."""
    best = 0.0
    for key in keys:
        best = max(best, _parse_download_job_time(row.get(key)))
    return best


def _should_replace_metadata_row(existing: dict[str, Any], new: dict[str, Any], *time_keys: str) -> bool:
    """True nếu ``new`` mới hơn hoặc bằng ``existing`` (gộp đa nguồn)."""
    ex = _row_recency_ts(existing, *time_keys)
    nw = _row_recency_ts(new, *time_keys)
    return nw >= ex


def _video_row_merge_key(row: dict[str, Any]) -> str:
    """Khóa gộp video: job + đường dẫn chuẩn hóa (tránh trùng khi copy metadata)."""
    jid = str(row.get("download_job_id") or "").strip()
    vp = str(row.get("video_path") or "").strip()
    try:
        vp = str(Path(vp).expanduser().resolve()).lower()
    except OSError:
        vp = vp.lower()
    return f"{jid}\0{vp}"


def downloader_layout_candidate_roots() -> list[Path]:
    """
    Mọi thư mục gốc app có thể chứa ``data/downloader``.

    Bao phủ: dev repo, ``exe_gui`` / ``portable_clean``, bundle zip, ``dist/``, thư mục cha.
    """
    seen: set[str] = set()
    out: list[Path] = []
    pr = project_root().resolve()

    def add(root: Path) -> None:
        try:
            r = root.resolve()
            key = str(r)
        except OSError:
            return
        if key in seen:
            return
        if not r.is_dir():
            return
        seen.add(key)
        out.append(r)

    def add_standard_layout(base: Path) -> None:
        add(base)
        for variant in _APP_VARIANT_DIR_NAMES:
            add(base / variant)
        for bundle in _RELEASE_BUNDLE_DIR_NAMES:
            b = base / bundle
            add(b)
            for variant in _APP_VARIANT_DIR_NAMES:
                add(b / variant)

    add_standard_layout(pr)
    if pr.parent.is_dir():
        add_standard_layout(pr.parent)
    if pr.parent.parent.is_dir():
        add(pr.parent.parent)
        add_standard_layout(pr.parent.parent)
    # Build local: dist/ToolFB_release_bundle/{exe_gui,portable_clean}
    add_standard_layout(pr / "dist")
    return out


def discover_downloader_data_roots() -> list[Path]:
    """Các thư mục ``…/data/downloader`` có file metadata (để gộp / đọc pending)."""
    roots: list[Path] = []
    seen: set[str] = set()
    for base in downloader_layout_candidate_roots():
        dl_root = (base / "data" / "downloader").resolve()
        key = str(dl_root)
        if key in seen:
            continue
        if not dl_root.is_dir():
            continue
        has_file = any(
            (dl_root / name).is_file()
            for name in (
                "download_jobs.json",
                "downloaded_videos.json",
                PENDING_VE_JOB_FILE,
                "archive.txt",
            )
        )
        if not has_file:
            continue
        seen.add(key)
        roots.append(dl_root)
    return roots


def _merge_downloader_metadata_canonical() -> None:
    """
    Gộp ``download_jobs.json`` / ``downloaded_videos.json`` về ``project_root()/data/downloader``.

    Máy khách: metadata có thể nằm rải rác (exe_gui, portable_clean, bundle, cập nhật cũ).
    """
    with _DOWNLOADER_MERGE_LOCK:
        canonical_root = (project_root() / "data" / "downloader").resolve()
        canonical_root.mkdir(parents=True, exist_ok=True)
        jobs_file = canonical_root / "download_jobs.json"
        videos_file = canonical_root / "downloaded_videos.json"
        archive_file = canonical_root / "archive.txt"

        jobs_by_id: dict[str, dict[str, Any]] = {}
        videos_by_key: dict[str, dict[str, Any]] = {}
        archive_lines: list[str] = []
        archive_seen: set[str] = set()
        sources: list[Path] = []
        best_pending: dict[str, Any] | None = None
        best_pending_ts = 0.0

        for dl_root in discover_downloader_data_roots():
            jf = dl_root / "download_jobs.json"
            vf = dl_root / "downloaded_videos.json"
            af = dl_root / "archive.txt"
            pf = dl_root / PENDING_VE_JOB_FILE
            sources.append(dl_root)
            for row in _read_json_object_list_file(jf):
                jid = str(row.get("id") or "").strip()
                if not jid:
                    continue
                prev = jobs_by_id.get(jid)
                if prev is None or _should_replace_metadata_row(
                    prev, row, "updated_at", "completed_at", "created_at"
                ):
                    jobs_by_id[jid] = row
            for row in _read_json_object_list_file(vf):
                vkey = _video_row_merge_key(row)
                if not vkey.strip("\0"):
                    continue
                prev = videos_by_key.get(vkey)
                if prev is None or _should_replace_metadata_row(prev, row, "created_at", "upload_date"):
                    videos_by_key[vkey] = row
            if pf.is_file():
                try:
                    raw = json.loads(pf.read_text(encoding="utf-8"))
                except Exception:
                    raw = None
                if isinstance(raw, dict) and str(raw.get("job_id") or "").strip():
                    ts = _parse_download_job_time(raw.get("saved_at"))
                    if ts >= best_pending_ts:
                        best_pending_ts = ts
                        best_pending = raw
            if af.is_file():
                try:
                    for line in af.read_text(encoding="utf-8").splitlines():
                        s = line.strip()
                        if s and s not in archive_seen:
                            archive_seen.add(s)
                            archive_lines.append(s)
                except OSError:
                    pass

        if len(sources) <= 1 and not jobs_by_id and not videos_by_key and not best_pending:
            return

        jobs_out = sorted(
            jobs_by_id.values(),
            key=lambda j: (
                _parse_download_job_time(j.get("updated_at")),
                _parse_download_job_time(j.get("completed_at")),
                _parse_download_job_time(j.get("created_at")),
            ),
            reverse=True,
        )
        videos_out = list(videos_by_key.values())

        with json_file_lock(jobs_file):
            _atomic_write_text(jobs_file, json.dumps(jobs_out, ensure_ascii=False, indent=2) + "\n")
        with json_file_lock(videos_file):
            _atomic_write_text(videos_file, json.dumps(videos_out, ensure_ascii=False, indent=2) + "\n")
        if archive_lines:
            with json_file_lock(archive_file):
                _atomic_write_text(archive_file, "\n".join(archive_lines) + ("\n" if archive_lines else ""))
        if best_pending:
            pending_path = canonical_root / PENDING_VE_JOB_FILE
            try:
                with json_file_lock(pending_path):
                    _atomic_write_text(
                        pending_path,
                        json.dumps(best_pending, ensure_ascii=False, indent=2) + "\n",
                    )
            except OSError:
                pass


def ensure_downloader_layout() -> dict[str, Path]:
    _merge_downloader_metadata_canonical()
    root = (project_root() / "data" / "downloader").resolve()
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
            with json_file_lock(p):
                if not p.is_file():
                    _atomic_write_text(p, "[]\n")
    if not paths["archive"].is_file():
        with json_file_lock(paths["archive"]):
            if not paths["archive"].is_file():
                _atomic_write_text(paths["archive"], "")
    return paths


def list_videos_for_download_job(
    job_id: str,
    *,
    job: dict[str, Any] | None = None,
    store: DownloadMetadataStore | None = None,
) -> list[dict[str, Any]]:
    """
    Video đã tải thuộc job — từ ``downloaded_videos.json``; fallback ``job.downloaded_files``.

    Dùng khi metadata video chưa kịp ghi nhưng job đã có đường dẫn file trên đĩa.
    """
    jid = str(job_id or "").strip()
    if not jid:
        return []
    paths = ensure_downloader_layout()
    meta_store = store or DownloadMetadataStore(paths=paths)
    rows = [
        r
        for r in meta_store.list_downloaded_videos()
        if str(r.get("download_job_id") or "").strip() == jid
    ]
    if rows:
        return rows
    job_row = job if isinstance(job, dict) else meta_store.get_job(jid)
    if not isinstance(job_row, dict):
        return []
    out: list[dict[str, Any]] = []
    for fp in job_row.get("downloaded_files") or []:
        vp = str(fp or "").strip()
        if not vp:
            continue
        try:
            p = Path(vp).expanduser().resolve()
        except OSError:
            continue
        if not p.is_file():
            continue
        out.append(
            {
                "id": f"src_fallback_{uuid.uuid4().hex[:8]}",
                "download_job_id": jid,
                "download_job_name": str(job_row.get("name") or ""),
                "platform": str(job_row.get("platform") or ""),
                "video_path": str(p),
                "title": p.stem,
                "status": "downloaded",
                "created_at": str(job_row.get("completed_at") or job_row.get("updated_at") or ""),
            }
        )
    return out


def downloader_metadata_summary() -> dict[str, Any]:
    """Tóm tắt metadata tải video (đường dẫn + số lượng) — hiển thị trên Video Editor."""
    paths = ensure_downloader_layout()
    jobs = _read_json_object_list_file(paths["jobs_file"])
    videos = _read_json_object_list_file(paths["videos_file"])
    return {
        "root": str(paths["root"]),
        "jobs_file": str(paths["jobs_file"]),
        "job_count": len(jobs),
        "video_count": len(videos),
    }


def default_universal_video_downloader_config() -> dict[str, Any]:
    return {
        "enabled": True,
        "yt_dlp": {
            "bin": "yt-dlp",
            "use_exe": False,
            "exe_path": str(project_root() / "tools" / "yt-dlp" / ("yt-dlp.exe" if os.name == "nt" else "yt-dlp")),
            "format": YTDLP_FORMAT_HD_MERGE,
            # Khi bật prefer_fast_single_video: file đơn (ít merge), vẫn ưu tiên >=720p nếu có.
            "single_video_fast_format": YTDLP_FORMAT_HD_SINGLE,
            # False = URL đơn cũng dùng format (merge video+audio) → chất lượng HD ổn định hơn.
            "prefer_fast_single_video": False,
            "merge_output_format": "mp4",
            "timeout_sec": 600,
            # Giới hạn treo socket (yt-dlp); máy khách mạng chập chờn vẫn fail nhanh thay vì chờ vô hạn.
            "socket_timeout_sec": 45,
            # Quét playlist/kênh (flat): timeout socket riêng, thường cần lớn hơn tải 1 file để ít lỗi giữa chừng.
            "playlist_scan_socket_timeout_sec": 90,
            # Mặc định ưu tiên tốc độ; máy yếu sẽ đỡ bị "lag" khi tải nhiều URL.
            "sleep_interval_sec": 0,
            # Chỉ khi tải batch (-a nhiều URL): yt-dlp --sleep-interval giữa các video (giảm 403/rate-limit YouTube).
            # Đặt 0 trong config để tắt hành vi mặc định.
            "batch_inter_request_sleep_sec": 0.75,
            # Sau batch: thử lại từng URL còn thiếu/ghép meta lỗi bằng một lần yt-dlp mỗi URL.
            "batch_sequential_retry": True,
            # Số URL tối đa được retry tuần tự sau batch (0 = không giới hạn; trần 500).
            "batch_sequential_retry_max_urls": 20,
            # DASH/HLS: tăng mặc định giúp máy mạng tốt tải nhanh hơn (trần 16 trong download()).
            "concurrent_fragments": 12,
            "max_videos_default": 50,
            # HD merge thường >300MB cho video dài; giảm lỗi «file quá lớn» khi tải 720p/1080p.
            "max_filesize_mb": 1536,
            "write_info_json": True,
            # Thumbnail = thêm request/ghi file mỗi video; tắt mặc định để tải batch/Short nhanh hơn.
            "write_thumbnail": False,
            # Trì hoãn tự tải yt-dlp.exe nền để không tranh băng thông/ổ với lệnh tải vừa mở app.
            "auto_update_delay_sec": 90,
            # Giảm log ffmpeg khi merge (ít stderr → nhẹ hơn trên Windows/AV).
            "ffmpeg_quiet_log": True,
            "proxy": "",
            # Rỗng = tự tìm node → deno → bun trên PATH (YouTube bản yt-dlp mới cần JS runtime).
            # Ví dụ ép: "node" hoặc "node:C:\\Program Files\\nodejs\\node.exe"
            "js_runtimes": "",
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
            # Mặc định "nhanh", tránh quét quá nặng trên máy khách.
            "max_collect": 120,
            "max_scroll_rounds": 35,
            "max_scan_minutes": 12,
            "scroll_until_end": True,
            "show_browser": False,
        },
        "instagram_reels": {
            "cookie_path": "",
            "max_collect": 120,
            "max_scroll_rounds": 60,
            "max_scan_minutes": 15,
            "scroll_until_end": True,
            "show_browser": False,
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
    ig = dict(base["instagram_reels"])
    ig.update(dict(uvd.get("instagram_reels") or {}))
    merged["yt_dlp"] = yt
    merged["download"] = dl
    merged["facebook_reels"] = fb
    merged["instagram_reels"] = ig
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
        fb["max_collect"] = max(10, min(UV_MAX_PLAYLIST_ENTRIES, int(max_collect)))
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


def persist_instagram_reels_settings(
    *,
    cookie_path: str | None = None,
    max_collect: int | None = None,
    max_scroll_rounds: int | None = None,
    max_scan_minutes: int | None = None,
    scroll_until_end: bool | None = None,
) -> None:
    """Merge ``instagram_reels`` vào ``config/universal_video_downloader.json``. Chỉ cập nhật tham số khác ``None``."""
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
    ig = dict(uvd.get("instagram_reels") or {})
    if cookie_path is not None:
        ig["cookie_path"] = str(cookie_path or "").strip()
    if max_collect is not None:
        ig["max_collect"] = max(10, min(UV_MAX_PLAYLIST_ENTRIES, int(max_collect)))
    if max_scroll_rounds is not None:
        ig["max_scroll_rounds"] = max(5, min(280, int(max_scroll_rounds)))
    if max_scan_minutes is not None:
        ig["max_scan_minutes"] = max(1, min(180, int(max_scan_minutes)))
    if scroll_until_end is not None:
        ig["scroll_until_end"] = bool(scroll_until_end)
    uvd["instagram_reels"] = ig
    raw["universal_video_downloader"] = uvd
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def detect_platform(url: str) -> str:
    u = url.lower()
    if "youtube.com" in u or "youtu.be" in u:
        return "youtube"
    if "tiktok.com" in u:
        return "tiktok"
    if "instagram.com" in u or "instagr.am" in u:
        return "instagram"
    if "facebook.com" in u or "fb.watch" in u:
        return "facebook"
    return "unknown"


_IG_AUTH_HINT = (
    "\n\n── Gợi ý (Instagram) ──\n"
    "Reel/Post công khai: dán link trực tiếp «/reel/…» hoặc «/p/…».\n"
    "Quét profile/tab Reels: cần đăng nhập — export cookie trình duyệt (Netscape JSON) "
    "và đặt ``cookie_path`` trong config ``universal_video_downloader.json`` hoặc dùng "
    "``--cookies-from-browser`` khi chạy yt-dlp tay.\n"
    "Cập nhật yt-dlp: ``pip install -U yt-dlp``."
)


def augment_instagram_auth_message(url: str, err: str) -> str:
    """Thêm hướng dẫn khi Instagram báo login/private/unsupported."""
    if not err or "instagram" not in str(url or "").lower():
        return err
    low = err.lower()
    if any(
        k in low
        for k in (
            "login",
            "private",
            "cookies",
            "sign in",
            "authentication",
            "unsupported url",
            "not available",
        )
    ):
        return err.rstrip() + _IG_AUTH_HINT
    return err


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
    if "instagram.com" in u or "instagr.am" in u:
        if re.search(r"instagram\.com/(reel|p|tv)/[A-Za-z0-9_-]+", u):
            return "single_video"
        if re.search(r"instagr\.am/p/[A-Za-z0-9_-]+", u):
            return "single_video"
        if re.search(r"instagram\.com/[^/]+/reels/?(?:[?#].*)?$", u):
            return "profile"
        m = re.match(r"https?://(?:www\.)?instagram\.com/([^/?#]+)/?(?:[?#].*)?$", url.strip(), re.I)
        if m:
            seg = m.group(1).lower()
            reserved = {
                "p",
                "reel",
                "reels",
                "stories",
                "explore",
                "accounts",
                "direct",
                "tv",
                "about",
                "legal",
                "developer",
            }
            if seg not in reserved:
                return "profile"
    if "/videos/" in u:
        return "single_video"
    return "unknown"


def _extract_hashtags_from_text(text: str) -> list[str]:
    """Trích hashtag có dấu # trong mô tả (Unicode, không chỉ ASCII)."""
    raw = str(text or "")
    if not raw:
        return []
    # Mọi cụm #... tới khoảng trắng hoặc # tiếp theo (hỗ trợ tiếng Việt và ký tự hashtag thông thường)
    found = re.findall(r"#([^\s#]{1,120})", raw)
    out: list[str] = []
    seen: set[str] = set()
    for tag in found:
        body = str(tag).strip()
        if not body:
            continue
        v = "#" + body
        k = v.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(v)
        if len(out) >= 50:
            break
    return out


def _prune_empty_parent_dirs(start_path: Path, *, stop_at_parent: Path | None = None) -> None:
    """
    Xóa dần thư mục rỗng từ thư mục chứa file đi lên trên.
    Dừng ở ``stop_at_parent`` (không xóa thư mục này).
    """
    cur = Path(start_path).expanduser().resolve()
    if cur.is_file():
        cur = cur.parent
    stop = Path(stop_at_parent).expanduser().resolve() if stop_at_parent else None
    while True:
        if stop and cur == stop:
            break
        if not cur.exists() or not cur.is_dir():
            break
        try:
            if any(cur.iterdir()):
                break
        except OSError:
            break
        try:
            cur.rmdir()
        except OSError:
            break
        parent = cur.parent
        if parent == cur:
            break
        cur = parent


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


def _parse_download_job_time(raw: Any) -> float:
    s = str(raw or "").strip()
    if not s:
        return 0.0
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return 0.0


def build_download_job_combo_options(
    jobs: list[dict[str, Any]],
    videos: list[dict[str, Any]],
    *,
    show_empty: bool = False,
) -> tuple[list[str], dict[str, str]]:
    """
    Nhãn combobox «Job tải» cho Video Editor.

    Luôn hiện job có video; job ``completed``/``running``/``failed`` dù chưa ghi metadata video;
    job ``pending`` rỗng chỉ khi bật «Hiện cả job rỗng/lỗi».
    """
    count_by_job: dict[str, int] = {}
    for r in videos:
        if not isinstance(r, dict):
            continue
        jid = str(r.get("download_job_id") or "").strip()
        if not jid:
            continue
        count_by_job[jid] = int(count_by_job.get(jid, 0)) + 1

    jobs_by_id: dict[str, dict[str, Any]] = {}
    for j in jobs:
        if not isinstance(j, dict):
            continue
        jid = str(j.get("id") or "").strip()
        if jid:
            jobs_by_id[jid] = j

    def _label_for(jid: str, j: dict[str, Any] | None, vcount: int) -> str:
        plat = str((j or {}).get("platform") or "").strip() or "unknown"
        st = str((j or {}).get("status") or "").strip() or "-"
        jname = str((j or {}).get("name") or "").strip()
        short_id = jid[-6:] if len(jid) > 6 else jid
        if jname:
            return f"{jname} | {plat} | {vcount} video | {st} | #{short_id}"
        return f"{plat} | {vcount} video | {st} | #{short_id}"

    def _include_job(jid: str, vcount: int, j: dict[str, Any] | None) -> bool:
        if vcount > 0:
            return True
        if show_empty:
            return True
        st = str((j or {}).get("status") or "").strip().lower()
        return st in ("completed", "running", "failed", "need_manual_upload")

    ordered_ids: list[str] = []
    seen: set[str] = set()

    jobs_sorted = sorted(
        [j for j in jobs if isinstance(j, dict)],
        key=lambda j: (
            int(count_by_job.get(str(j.get("id") or "").strip(), 0)),
            _parse_download_job_time(j.get("updated_at")),
            _parse_download_job_time(j.get("completed_at")),
            _parse_download_job_time(j.get("created_at")),
        ),
        reverse=True,
    )
    for j in jobs_sorted:
        jid = str(j.get("id") or "").strip()
        if not jid or jid in seen:
            continue
        vcount = int(count_by_job.get(jid, 0))
        if not _include_job(jid, vcount, j):
            continue
        ordered_ids.append(jid)
        seen.add(jid)

    for jid, vcount in sorted(count_by_job.items(), key=lambda x: (-x[1], x[0])):
        if jid in seen or vcount <= 0:
            continue
        ordered_ids.append(jid)
        seen.add(jid)

    vals: list[str] = []
    new_map: dict[str, str] = {}
    for jid in ordered_ids:
        j = jobs_by_id.get(jid)
        vcount = int(count_by_job.get(jid, 0))
        label = _label_for(jid, j, vcount)
        if label in new_map:
            label = f"{label} ({jid[-4:]})"
        vals.append(label)
        new_map[label] = jid
    return vals, new_map


def _read_json_object_list_file(path: Path, *, retries: int = 5) -> list[dict[str, Any]]:
    """
    Đọc JSON list an toàn — retry khi file đang được ghi (Windows / máy khách hay lỗi parse tạm).
    """
    if not path.is_file():
        return []
    last_exc: Exception | None = None
    for attempt in range(max(1, int(retries))):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                return [x for x in raw if isinstance(x, dict)]
            return []
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt + 1 < retries:
                time.sleep(0.04 * (attempt + 1))
    return []


def _atomic_write_text(path: Path, text: str) -> None:
    """Ghi file JSON atomically (temp + replace) — tránh đọc dở khi tab Video Editor refresh."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex[:10]}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


class DownloadMetadataStore:
    def __init__(self, *, paths: dict[str, Path] | None = None) -> None:
        self._paths_override = paths

    def _refresh_paths(self) -> dict[str, Path]:
        paths = self._paths_override or ensure_downloader_layout()
        return paths

    def _read_jobs(self) -> list[dict[str, Any]]:
        paths = self._refresh_paths()
        return _read_json_object_list_file(paths["jobs_file"])

    def _write_jobs(self, rows: list[dict[str, Any]]) -> None:
        paths = self._refresh_paths()
        payload = json.dumps(rows, ensure_ascii=False, indent=2) + "\n"
        with json_file_lock(paths["jobs_file"]):
            _atomic_write_text(paths["jobs_file"], payload)

    def _read_videos(self) -> list[dict[str, Any]]:
        paths = self._refresh_paths()
        return _read_json_object_list_file(paths["videos_file"])

    def _write_videos(self, rows: list[dict[str, Any]]) -> None:
        paths = self._refresh_paths()
        payload = json.dumps(rows, ensure_ascii=False, indent=2) + "\n"
        with json_file_lock(paths["videos_file"]):
            _atomic_write_text(paths["videos_file"], payload)

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

    def save_downloaded_videos(self, records: list[dict[str, Any]]) -> None:
        """Ghi nhiều bản ghi trong một lần đọc/ghi file — giảm tải khi tải batch."""
        if not records:
            return
        rows = self._read_videos()
        ids = {str(r.get("id") or "").strip() for r in records if str(r.get("id") or "").strip()}
        rows = [r for r in rows if str(r.get("id") or "").strip() not in ids]
        for rec in reversed(records):
            rid = str(rec.get("id") or "").strip()
            if rid:
                rows.insert(0, dict(rec))
        self._write_videos(rows)

    def list_downloaded_videos(self) -> list[dict[str, Any]]:
        return self._read_videos()

    def get_video(self, video_id: str) -> dict[str, Any] | None:
        for r in self._read_videos():
            if str(r.get("id") or "") == str(video_id):
                return r
        return None

    def delete_video_record(self, video_id: str, *, delete_file: bool = False, prune_empty_dirs: bool = False) -> bool:
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
            removed_paths: list[Path] = []
            if vp:
                try:
                    p = Path(vp)
                    p.unlink(missing_ok=True)
                    removed_paths.append(p)
                except OSError:
                    pass
            for key in ("thumbnail_path", "info_json_path"):
                pp = str(found.get(key) or "")
                if pp:
                    try:
                        p = Path(pp)
                        p.unlink(missing_ok=True)
                        removed_paths.append(p)
                    except OSError:
                        pass
            if prune_empty_dirs:
                for p in removed_paths:
                    try:
                        _prune_empty_parent_dirs(p)
                    except Exception:
                        pass
        self._write_videos(rest)
        return True

    def delete_job(self, job_id: str) -> bool:
        jid = str(job_id or "").strip()
        if not jid:
            return False
        rows = self._read_jobs()
        new_rows = [r for r in rows if str(r.get("id") or "").strip() != jid]
        if len(new_rows) == len(rows):
            return False
        self._write_jobs(new_rows)
        return True

    def delete_videos_by_job(
        self,
        job_id: str,
        *,
        delete_file: bool = False,
        prune_empty_dirs: bool = False,
    ) -> int:
        jid = str(job_id or "").strip()
        if not jid:
            return 0
        rows = self._read_videos()
        targets = [r for r in rows if str(r.get("download_job_id") or "").strip() == jid]
        if not targets:
            return 0
        if delete_file:
            for r in targets:
                self.delete_video_record(
                    str(r.get("id") or ""),
                    delete_file=True,
                    prune_empty_dirs=prune_empty_dirs,
                )
            return len(targets)
        rest = [r for r in rows if str(r.get("download_job_id") or "").strip() != jid]
        self._write_videos(rest)
        return len(targets)


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
                stdin=subprocess.DEVNULL,
                **_ytdlp_subprocess_kw(),
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
        js_args = ytdlp_js_runtimes_cli_args(dict(self._yt))
        return {
            "ok": True,
            "message": "",
            "label": label,
            "version": version_line or "yt-dlp",
            "js_runtimes_resolved": js_args[1] if len(js_args) > 1 else "",
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
    def _auto_update_state_file() -> Path:
        return ensure_downloader_layout()["root"] / "ytdlp_auto_update.json"

    @classmethod
    def maybe_auto_update_standalone_exe(cls, *, min_hours_between_attempts: int = 24) -> dict[str, Any]:
        """Best-effort: tự cập nhật yt-dlp.exe mỗi ~24h (Windows)."""
        if os.name != "nt":
            return {"ok": False, "reason": "not_windows"}
        target = project_root() / "tools" / "yt-dlp" / "yt-dlp.exe"
        target.parent.mkdir(parents=True, exist_ok=True)
        state_path = cls._auto_update_state_file()
        now_ts = time.time()
        state: dict[str, Any] = {}
        if state_path.is_file():
            try:
                raw = json.loads(state_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    state = raw
            except Exception:
                state = {}
        last_attempt = float(state.get("last_attempt_ts") or 0.0)
        if now_ts - last_attempt < max(1, int(min_hours_between_attempts)) * 3600:
            return {"ok": False, "reason": "recently_attempted"}
        state["last_attempt_ts"] = now_ts
        try:
            state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        except OSError:
            pass
        tmp = target.with_suffix(".tmp")
        req = urllib.request.Request(YTDLP_WIN_EXE_URL, headers={"User-Agent": "ToolFB-runtime-ytdlp-auto-update"})
        try:
            with urllib.request.urlopen(req, timeout=180) as resp, tmp.open("wb") as fh:
                shutil.copyfileobj(resp, fh, length=256 * 1024)
            if not tmp.is_file() or tmp.stat().st_size < YTDLP_BUNDLE_EXE_MIN_BYTES:
                tmp.unlink(missing_ok=True)
                return {"ok": False, "reason": "download_too_small"}
            tmp.replace(target)
            state["last_success_ts"] = time.time()
            try:
                state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            except OSError:
                pass
            return {"ok": True, "path": str(target)}
        except Exception as exc:  # noqa: BLE001
            tmp.unlink(missing_ok=True)
            return {"ok": False, "reason": str(exc)}

    @staticmethod
    def _probe_python_m_ytdlp() -> list[str] | None:
        """
        Cùng interpreter đang chạy app: ``python -m yt_dlp`` thường chạy được
        khi pip đã cài gói, kể cả khi ``import yt_dlp`` trong process lỗi (hiếm)
        hoặc PATH không có lệnh ``yt-dlp``.
        """
        if getattr(sys, "frozen", False):
            # PyInstaller: sys.executable là ToolFB_GUI.exe — không hiểu ``-m yt_dlp``.
            return None
        try:
            p = subprocess.run(
                [sys.executable, "-m", "yt_dlp", "--version"],
                capture_output=True,
                text=True,
                timeout=25,
                encoding="utf-8",
                errors="replace",
                stdin=subprocess.DEVNULL,
                **_ytdlp_subprocess_kw(),
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
        bundled = project_root() / "tools" / "yt-dlp" / "yt-dlp.exe"
        if bundled.is_file() and bundled.stat().st_size >= YTDLP_BUNDLE_EXE_MIN_BYTES:
            return [str(bundled.resolve())]
        if not getattr(sys, "frozen", False):
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
            "hoặc đặt file yt-dlp.exe vào tools/yt-dlp/ cạnh thư mục app (bản .exe: cùng thư mục ToolFB_GUI.exe). "
            "Có thể tải yt-dlp.exe từ https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe"
        )

    def get_info(self, url: str) -> dict[str, Any]:
        ut = classify_url_type(url)
        cmd = [
            *self._resolve_prefix(),
            *ytdlp_js_runtimes_cli_args(self._yt),
            "-J",
            "--skip-download",
            "--quiet",
            "--no-warnings",
            "--encoding",
            "utf-8",
        ]
        sock_to = int(self._yt.get("socket_timeout_sec") or 0)
        if sock_to > 0:
            cmd.extend(["--socket-timeout", str(max(5, min(600, sock_to)))])
        if ut in ("playlist", "channel", "profile"):
            cmd.append("--flat-playlist")
        proxy = str(self._yt.get("proxy") or "").strip()
        if proxy:
            cmd.extend(["--proxy", proxy])
        cmd.append(url)
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=min(120, int(self._yt.get("timeout_sec") or 600)),
            encoding="utf-8",
            errors="replace",
            stdin=subprocess.DEVNULL,
            **_ytdlp_subprocess_kw(),
        )
        if p.returncode != 0:
            err = (p.stderr or p.stdout or "").strip()
            err = augment_facebook_unsupported_url_message(url, err[:1200])
            err = augment_instagram_auth_message(url, err[:1200])
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
            "title": _repair_mojibake_text(str(data.get("title") or data.get("playlist_title") or "")),
            "uploader": _repair_mojibake_text(str(data.get("uploader") or data.get("playlist_uploader") or "")),
            "entry_count": n if n else (1 if data.get("id") else 0),
            "url_type": ut,
        }

    @staticmethod
    def _flat_playlist_entry_url(entry: dict[str, Any], *, source_url: str, platform: str) -> str:
        """Lấy URL tải được từ một phần tử JSON flat-playlist (YouTube/TikTok)."""
        for key in ("webpage_url", "original_url", "url"):
            u = str(entry.get(key) or "").strip()
            if u.startswith("http://") or u.startswith("https://"):
                return u
            if platform == "youtube":
                if u.startswith("watch?"):
                    return "https://www.youtube.com/" + u
                if u.startswith("/watch"):
                    return "https://www.youtube.com" + u
        vid = str(entry.get("id") or "").strip()
        if platform == "youtube" and vid and re.fullmatch(r"[A-Za-z0-9_-]{11}", vid):
            return f"https://www.youtube.com/watch?v={vid}"
        if platform == "tiktok" and vid:
            owner = str(entry.get("uploader_id") or entry.get("channel_id") or "").strip()
            if owner:
                if not owner.startswith("@"):
                    owner = "@" + owner
                return f"https://www.tiktok.com/{owner}/video/{vid}"
            m = re.search(r"tiktok\.com/@[^/?#]+", source_url, re.I)
            if m:
                return f"{m.group(0)}/video/{vid}"
        if platform == "instagram" and vid and re.fullmatch(r"[A-Za-z0-9_-]{5,}", vid):
            low_src = source_url.lower()
            if "/reel/" in low_src or str(entry.get("ie_key") or "").lower() == "instagramreel":
                return f"https://www.instagram.com/reel/{vid}/"
            return f"https://www.instagram.com/p/{vid}/"
        return ""

    @staticmethod
    def _parse_flat_print_line(line: str, *, source_url: str, platform: str) -> dict[str, str] | None:
        """
        Parse output dòng từ:
        ``--print "%(id)s\\t%(title)s\\t%(webpage_url)s\\t%(url)s\\t%(uploader_id)s"``
        """
        raw = str(line or "").strip()
        if not raw:
            return None
        parts = raw.split("\t", 4)
        while len(parts) < 5:
            parts.append("")
        vid = str(parts[0] or "").strip()
        title = _repair_mojibake_text(str(parts[1] or "").strip())
        webpage_url = str(parts[2] or "").strip()
        url_raw = str(parts[3] or "").strip()
        uploader_id = str(parts[4] or "").strip()
        entry = {
            "id": vid,
            "title": title or vid,
            "webpage_url": webpage_url,
            "url": url_raw,
            "uploader_id": uploader_id,
        }
        final_url = UniversalYTDLPWrapper._flat_playlist_entry_url(entry, source_url=source_url, platform=platform)
        if not final_url:
            return None
        return {"title": (title or vid or final_url)[:500], "url": final_url}

    def _list_flat_playlist_orchestrated(
        self,
        raw: str,
        *,
        platform: str,
        max_entries: int,
        chunk: int,
        on_partial: Callable[[list[dict[str, str]]], None] | None = None,
    ) -> dict[str, Any]:
        """Quét playlist dài theo từng lô ``--playlist-start`` / ``--playlist-end`` để tránh timeout một lần."""
        all_out: list[dict[str, str]] = []
        seen: set[str] = set()
        warnings: list[str] = []
        playlist_title = ""
        extractor = ""
        n = max(1, int(max_entries))
        step = max(80, min(800, int(chunk)))
        for page_start in range(1, n + 1, step):
            page_end = min(page_start + step - 1, n)
            page = self.list_flat_playlist_entries(
                raw,
                max_entries=n,
                playlist_start=page_start,
                playlist_end=page_end,
                _orchestrating=True,
            )
            if not page.get("success"):
                if all_out:
                    warnings.append(str(page.get("error") or "Lỗi lô quét"))
                    break
                return page
            if not playlist_title:
                playlist_title = str(page.get("playlist_title") or "").strip()
            if not extractor:
                extractor = str(page.get("extractor") or "").strip()
            page_entries = page.get("entries") or []
            added = 0
            for rec in page_entries:
                if not isinstance(rec, dict):
                    continue
                u = str(rec.get("url") or "").strip()
                if not u or u in seen:
                    continue
                seen.add(u)
                all_out.append(rec)
                added += 1
            if on_partial and all_out:
                try:
                    on_partial(list(all_out))
                except Exception:
                    pass
            if added == 0 or len(page_entries) < (page_end - page_start + 1):
                break
            if len(all_out) >= n:
                break
            pause = float(self._yt.get("playlist_scan_inter_page_sleep_sec") or 0.35)
            if pause > 0:
                time.sleep(min(2.0, pause))
        if not all_out:
            return {"success": False, "error": "Không lấy được entry nào từ playlist/kênh."}
        warn = "; ".join(warnings).strip()
        return {
            "success": True,
            "entries": all_out[:n],
            "playlist_title": playlist_title,
            "extractor": extractor,
            "partial": bool(warn),
            "warning": warn,
        }

    def list_flat_playlist_entries(
        self,
        url: str,
        *,
        max_entries: int = 500,
        on_partial: Callable[[list[dict[str, str]]], None] | None = None,
        playlist_start: int = 1,
        playlist_end: int | None = None,
        _orchestrating: bool = False,
    ) -> dict[str, Any]:
        """
        Liệt kê entry trong kênh / playlist (``--flat-playlist``) không tải video.
        Hỗ trợ YouTube (channel/playlist) và TikTok (profile).
        """
        raw = str(url or "").strip()
        if not raw:
            return {"success": False, "error": "Thiếu URL."}
        platform = detect_platform(raw)
        if platform not in ("youtube", "tiktok", "instagram"):
            return {
                "success": False,
                "error": "Chỉ hỗ trợ quét danh sách cho YouTube, TikTok hoặc Instagram.",
            }
        ut = classify_url_type(raw)
        if platform == "youtube" and ut not in ("playlist", "channel"):
            return {
                "success": False,
                "error": "Cần URL kênh hoặc playlist (ví dụ tab Shorts, /videos, ?list=…), không phải một video đơn.",
            }
        if platform == "tiktok" and ut != "profile":
            return {"success": False, "error": "TikTok cần URL profile (dạng https://www.tiktok.com/@user)."}
        if platform == "instagram" and ut != "profile":
            return {
                "success": False,
                "error": (
                    "Instagram cần URL profile hoặc tab Reels "
                    "(ví dụ https://www.instagram.com/username/ hoặc …/username/reels/)."
                ),
            }
        n = max(1, min(int(max_entries or 500), UV_MAX_PLAYLIST_ENTRIES))
        if not _orchestrating:
            chunk = int(self._yt.get("playlist_scan_chunk") or UV_PLAYLIST_SCAN_CHUNK)
            chunk = max(80, min(800, chunk))
            # TikTok profile: yt-dlp thường không phân trang playlist-start/end ổn định — quét một lần.
            if platform == "youtube" and n > chunk:
                return self._list_flat_playlist_orchestrated(
                    raw,
                    platform=platform,
                    max_entries=n,
                    chunk=chunk,
                    on_partial=on_partial,
                )
        pstart = max(1, int(playlist_start or 1))
        pend = int(playlist_end) if playlist_end is not None else n
        pend = max(pstart, min(pend, n))
        page_count = max(1, pend - pstart + 1)
        # Fast path: tránh parse JSON lớn cho playlist dài (máy yếu sẽ đỡ lag/đỡ RAM).
        cmd = [
            *self._resolve_prefix(),
            *ytdlp_js_runtimes_cli_args(self._yt),
            "--skip-download",
            "--quiet",
            "--no-warnings",
            "--encoding",
            "utf-8",
            "--flat-playlist",
            "--lazy-playlist",
            "--print",
            "%(id)s\t%(title)s\t%(webpage_url)s\t%(url)s\t%(uploader_id)s",
        ]
        if pstart > 1:
            cmd.extend(["--playlist-start", str(pstart)])
        cmd.extend(["--playlist-end", str(pend)])
        scan_sock = int(self._yt.get("playlist_scan_socket_timeout_sec") or 0)
        if scan_sock <= 0:
            scan_sock = int(self._yt.get("socket_timeout_sec") or 0)
        if scan_sock > 0:
            cmd.extend(["--socket-timeout", str(max(10, min(180, scan_sock)))])
        proxy = str(self._yt.get("proxy") or "").strip()
        if proxy:
            cmd.extend(["--proxy", proxy])
        cmd.append(raw)
        timeout = int(self._yt.get("timeout_sec") or 600)
        # Trần theo số entry trong lô; TikTok một lần có thể cần timeout dài hơn.
        scan_cap = 1800 if platform in ("tiktok", "instagram") and not _orchestrating else 900
        timeout_scan = min(timeout, max(90, min(scan_cap, page_count * 6 + 72)))
        try:
            p = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                **_ytdlp_subprocess_kw(),
            )
        except Exception as exc:  # noqa: BLE001
            return {"success": False, "error": str(exc)}
        out: list[dict[str, str]] = []
        seen: set[str] = set()
        stderr_tail: deque[str] = deque(maxlen=1200)
        t0 = time.monotonic()
        last_partial_ts = 0.0
        _stderr_stop = threading.Event()
        line_q: queue.Queue[str | None] = queue.Queue(maxsize=max(4096, page_count * 8))

        def _read_stderr() -> None:
            if p.stderr is None:
                return
            for ln in p.stderr:
                stderr_tail.append(ln)
                if _stderr_stop.is_set():
                    break

        def _read_stdout() -> None:
            """Đọc stdout trên luồng riêng để luồng chính luôn kiểm tra deadline (tránh treo trước dòng đầu)."""
            try:
                if p.stdout is None:
                    line_q.put(None)
                    return
                for ln in p.stdout:
                    line_q.put(ln)
            except Exception:
                pass
            finally:
                try:
                    line_q.put(None)
                except Exception:
                    pass

        t_err = threading.Thread(target=_read_stderr, daemon=True, name="uv_scan_stderr")
        t_out = threading.Thread(target=_read_stdout, daemon=True, name="uv_scan_stdout")
        t_err.start()
        t_out.start()
        timed_out = False
        try:
            while True:
                if time.monotonic() - t0 > timeout_scan:
                    timed_out = True
                    try:
                        p.kill()
                    except Exception:
                        pass
                    break
                try:
                    ln = line_q.get(timeout=0.2)
                except queue.Empty:
                    if p.poll() is not None:
                        try:
                            ln = line_q.get(timeout=4.0)
                        except queue.Empty:
                            ln = None
                    else:
                        continue
                if ln is None:
                    break
                rec = self._parse_flat_print_line(ln, source_url=raw, platform=platform)
                if not rec:
                    continue
                u = rec["url"]
                if u in seen:
                    continue
                seen.add(u)
                out.append(rec)
                if on_partial and (len(out) <= 25 or time.monotonic() - last_partial_ts > 0.22):
                    try:
                        on_partial(list(out))
                    except Exception:
                        pass
                    last_partial_ts = time.monotonic()

            _stderr_stop.set()
            t_out.join(timeout=4.0)
            t_err.join(timeout=1.5)

            if timed_out:
                if out:
                    return {
                        "success": True,
                        "entries": out,
                        "playlist_title": "",
                        "extractor": "",
                        "partial": True,
                        "warning": f"Quét chậm nên dừng sau {timeout_scan}s (đã lấy {len(out)} video).",
                    }
                return {"success": False, "error": f"Hết thời gian khi quét danh sách (>{timeout_scan}s)."}

            remain = max(5.0, float(timeout) - (time.monotonic() - t0))
            reap = max(5, min(45, int(min(remain, 55.0))))
            try:
                rc = p.wait(timeout=reap)
            except subprocess.TimeoutExpired:
                try:
                    p.kill()
                except Exception:
                    pass
                if out:
                    return {
                        "success": True,
                        "entries": out,
                        "playlist_title": "",
                        "extractor": "",
                        "partial": True,
                        "warning": f"yt-dlp chưa thoát sau khi quét xong (>{reap}s); đã giữ {len(out)} video đã đọc.",
                    }
                return {"success": False, "error": f"Hết thời gian khi quét danh sách (>{reap}s)."}
        except Exception as exc:  # noqa: BLE001
            try:
                p.kill()
            except Exception:
                pass
            _stderr_stop.set()
            t_out.join(timeout=2.0)
            t_err.join(timeout=1.5)
            return {"success": False, "error": str(exc)}
        if rc != 0:
            err = ("".join(stderr_tail) or "").strip()
            # Fallback tương thích cho bản yt-dlp cũ không hỗ trợ một số cờ scan nhanh.
            cmd_fallback = [
                *self._resolve_prefix(),
                *ytdlp_js_runtimes_cli_args(self._yt),
                "-J",
                "--skip-download",
                "--quiet",
                "--no-warnings",
                "--encoding",
                "utf-8",
                "--flat-playlist",
            ]
            if pstart > 1:
                cmd_fallback.extend(["--playlist-start", str(pstart)])
            cmd_fallback.extend(["--playlist-end", str(pend)])
            scan_sock = int(self._yt.get("playlist_scan_socket_timeout_sec") or 0)
            if scan_sock <= 0:
                scan_sock = int(self._yt.get("socket_timeout_sec") or 0)
            if scan_sock > 0:
                cmd_fallback.extend(["--socket-timeout", str(max(10, min(180, scan_sock)))])
            if proxy:
                cmd_fallback.extend(["--proxy", proxy])
            cmd_fallback.append(raw)
            try:
                p2 = subprocess.run(
                    cmd_fallback,
                    capture_output=True,
                    text=True,
                    timeout=min(max(180, page_count + 120), timeout_scan + 60),
                    encoding="utf-8",
                    errors="replace",
                    stdin=subprocess.DEVNULL,
                    **_ytdlp_subprocess_kw(),
                )
            except Exception:
                return {"success": False, "error": err[:2200] or "yt-dlp scan failed."}
            if p2.returncode != 0:
                err2 = (p2.stderr or p2.stdout or "").strip()
                return {"success": False, "error": (err2 or err)[:2200] or "yt-dlp scan failed."}
            try:
                data = json.loads(p2.stdout or "{}")
            except Exception as exc:
                return {"success": False, "error": f"Parse JSON lỗi: {exc}"}
            if not isinstance(data, dict):
                return {"success": False, "error": "Không phải object JSON"}
            entries_raw = data.get("entries")
            out2: list[dict[str, str]] = []
            if isinstance(entries_raw, list):
                for e in entries_raw:
                    if not isinstance(e, dict):
                        continue
                    play_url = self._flat_playlist_entry_url(e, source_url=raw, platform=platform)
                    if not play_url:
                        continue
                    title = _repair_mojibake_text(str(e.get("title") or e.get("id") or play_url))[:500]
                    out2.append({"title": title, "url": play_url})
            return {
                "success": True,
                "entries": out2,
                "playlist_title": str(data.get("title") or data.get("playlist_title") or ""),
                "extractor": str(data.get("extractor") or data.get("ie_key") or ""),
            }
        if on_partial and out:
            try:
                on_partial(list(out))
            except Exception:
                pass
        return {
            "success": True,
            "entries": out,
            "playlist_title": "",
            "extractor": "",
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
        batch_urls: list[str] | None = None,
        progress_hook: ProgressHook | None = None,
    ) -> dict[str, Any]:
        log_lines = log_lines or self._log
        prefix = self._resolve_prefix()
        urls_batch = [str(x).strip() for x in (batch_urls or []) if str(x).strip()]
        use_batch_file = len(urls_batch) >= 2
        ut = (url_type or "unknown").lower()
        if use_batch_file:
            # Một tiến trình yt-dlp đọc nhiều URL: luôn coi từng dòng là video đơn (không mở playlist).
            ut = "single_video"
        fmt = str(self._yt.get("format") or YTDLP_FORMAT_HD_MERGE)
        fast_single = str(self._yt.get("single_video_fast_format") or YTDLP_FORMAT_HD_SINGLE)
        # ``unknown`` = URL đơn chưa khớp extractor; prefer_fast=True tránh merge (nhanh hơn, có thể thấp hơn).
        prefer_fast = bool(self._yt.get("prefer_fast_single_video", False))
        if prefer_fast and ut in ("single_video", "unknown"):
            fmt = fast_single
        merge_fmt = str(self._yt.get("merge_output_format") or "mp4")
        ffmpeg_bin = _resolve_ffmpeg_for_ytdlp()
        has_ffmpeg = bool(ffmpeg_bin)
        if not has_ffmpeg:
            # Máy khách thiếu ffmpeg trong bundle/PATH: dùng profile không cần merge
            # để tránh chậm/lỗi do yt-dlp thử ghép AV rồi fail.
            fmt = YTDLP_FORMAT_HD_SINGLE
            log_lines("[yt-dlp] Không thấy ffmpeg -> fallback format không cần merge (ưu tiên HD nếu có).")
        sleep_sec = max(0.0, float(self._yt.get("sleep_interval_sec") or 0))
        # Batch (-a): thêm sleep giữa các video (mặc định batch_inter_request_sleep_sec) để YouTube ít chặn hàng loạt.
        if use_batch_file:
            bsleep = float(self._yt.get("batch_inter_request_sleep_sec", 0.75) or 0)
            if bsleep > 0:
                sleep_sec = min(30.0, max(sleep_sec, bsleep))
        elif ut in ("single_video", "unknown"):
            # Một URL đơn: không chèn sleep (tránh chậm); playlist/kênh vẫn dùng sleep_interval_sec.
            sleep_sec = 0.0
        max_fs = int(self._yt.get("max_filesize_mb") or 300)
        timeout = int(self._yt.get("timeout_sec") or 600)
        if use_batch_file:
            timeout = min(7200, max(timeout, timeout * max(1, (len(urls_batch) + 8) // 10)))
        frag_workers = max(1, min(16, int(self._yt.get("concurrent_fragments") or 1)))
        sock_to = int(self._yt.get("socket_timeout_sec") or 0)
        js_rt = ytdlp_js_runtimes_cli_args(self._yt)
        if js_rt:
            log_lines(f"[yt-dlp] YouTube JS runtime: {js_rt[1]}")
        cmd: list[str] = [
            *prefix,
            *js_rt,
            "-f",
            fmt,
            "--newline",
            "--no-progress",
            "--encoding",
            "utf-8",
            "--concurrent-fragments",
            str(frag_workers),
            "--print",
            "after_move:%(filepath)s",
            "--max-filesize",
            f"{max_fs}M",
            "-o",
            output_template,
        ]
        if sock_to > 0:
            cmd.extend(["--socket-timeout", str(max(5, min(600, sock_to)))])
        if has_ffmpeg:
            cmd.extend(["--merge-output-format", merge_fmt, "--ffmpeg-location", ffmpeg_bin])
            if bool(self._yt.get("ffmpeg_quiet_log", True)):
                cmd.extend(["--postprocessor-args", "ffmpeg:-loglevel error -hide_banner -nostats"])
        if sleep_sec > 0:
            mx = max(float(sleep_sec), 5.0)
            cmd.extend(["--sleep-interval", str(sleep_sec), "--max-sleep-interval", str(mx)])
        if write_info_json:
            cmd.append("--write-info-json")
        if write_thumbnail:
            cmd.append("--write-thumbnail")
        if skip_existing:
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            cmd.extend(["--download-archive", str(archive_path)])
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
        batch_tmp: Path | None = None
        effective_url = str(url or "").strip()
        if use_batch_file:
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_name = tempfile.mkstemp(prefix="toolfb_ytdlp_batch_", suffix=".txt", dir=str(archive_path.parent))
            os.close(fd)
            batch_tmp = Path(tmp_name)
            batch_tmp.write_text("\n".join(urls_batch) + "\n", encoding="utf-8")
            cmd.append("--ignore-errors")
            cmd.extend(["-a", str(batch_tmp)])
            if not effective_url:
                effective_url = urls_batch[0]
            log_lines(f"[yt-dlp] batch-file {len(urls_batch)} URL (1 process) → {batch_tmp.name}")
        else:
            if not effective_url:
                if cookie_tmp is not None:
                    cookie_tmp.unlink(missing_ok=True)
                return {"success": False, "error": "Thiếu URL"}
            cmd.append(effective_url)

        batch_total = len(urls_batch) if use_batch_file else 0
        completed_stream = 0

        def _safe_progress(payload: dict[str, Any]) -> None:
            if not progress_hook:
                return
            try:
                progress_hook(payload)
            except Exception:
                pass

        if progress_hook:
            _safe_progress(
                {
                    "event": "start",
                    "batch_total": batch_total,
                    "url_type": ut,
                    "max_videos": int(max_videos),
                    "single_url": (effective_url[:400] if effective_url else ""),
                }
            )

        log_lines(f"[yt-dlp] {' '.join(cmd[:12])} ... ({len(cmd)} args)")
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                **_ytdlp_subprocess_kw(),
            )
            filepaths: list[str] = []
            stderr_tail: deque[str] = deque(maxlen=1200)
            last_hook_ts: list[float] = [0.0]
            last_err_hook_ts: list[float] = [0.0]

            def _read_stderr() -> None:
                if not proc.stderr:
                    return
                n = 0
                js_warn_logged = False
                for line in proc.stderr:
                    stderr_tail.append(line)
                    n += 1
                    if _YTDLP_JS_WARN_SNIP in line and "javascript" in line.lower():
                        if not js_warn_logged:
                            js_warn_logged = True
                            log_lines(
                                line.rstrip()
                                + " → Cài Node.js (PATH) hoặc yt_dlp.js_runtimes trong config; bỏ qua các dòng lặp."
                            )
                        continue
                    low_ln = line.lower()
                    if progress_hook and (
                        "[download]" in low_ln
                        or "[merger]" in low_ln
                        or "destination:" in low_ln
                        or "merging formats into" in low_ln
                    ):
                        now = time.monotonic()
                        if now - last_hook_ts[0] >= 0.4:
                            last_hook_ts[0] = now
                            _safe_progress({"event": "stderr_activity", "line": line.strip()[:260]})
                    if "ERROR" in line and progress_hook:
                        nowe = time.monotonic()
                        if nowe - last_err_hook_ts[0] >= 0.75:
                            last_err_hook_ts[0] = nowe
                            _safe_progress({"event": "error_line", "line": line.strip()[:420]})
                    if n <= 30 or "ERROR" in line:
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
                        # Không ép is_file ngay lúc stream stdout vì một số hệ/FS báo trễ.
                        if fp:
                            filepaths.append(fp)
                            completed_stream += 1
                            _safe_progress(
                                {
                                    "event": "file_complete",
                                    "completed": completed_stream,
                                    "total": batch_total,
                                    "path": fp,
                                }
                            )
                    elif line.lower().endswith((".mp4", ".webm", ".mkv", ".mov")):
                        filepaths.append(line)
                        completed_stream += 1
                        _safe_progress(
                            {
                                "event": "file_complete",
                                "completed": completed_stream,
                                "total": batch_total,
                                "path": line,
                            }
                        )

            try:
                rc = proc.wait(timeout=timeout) if proc.poll() is None else (proc.returncode or 0)
            except subprocess.TimeoutExpired:
                proc.kill()
                rc = -1
                log_lines("[yt-dlp] Timeout — đã dừng process.")
            rt.join(timeout=2)
            err_full = "".join(stderr_tail)
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
                err_snip = augment_facebook_unsupported_url_message(effective_url, err_snip)
                err_snip = augment_instagram_auth_message(effective_url, err_snip)
                if not (use_batch_file and filepaths):
                    if cookie_tmp is not None:
                        cookie_tmp.unlink(missing_ok=True)
                    return {"success": False, "error": err_snip[:2200], "filepaths": filepaths}
                log_lines("[yt-dlp] batch: mã thoát khác 0 nhưng đã có file — tiếp tục ghép metadata.")
            # Chuẩn hóa danh sách đường dẫn báo về và lọc file thực sự tồn tại.
            resolved_paths: list[str] = []
            seen_resolved: set[str] = set()

            def _consume_fps(raw_list: list[str]) -> None:
                for fp in raw_list:
                    raw = str(fp or "").strip().strip('"')
                    if not raw:
                        continue
                    p = Path(raw).expanduser()
                    candidates: list[Path] = [p]
                    if not p.is_absolute():
                        candidates.append((Path.cwd() / p).resolve())
                    for cand in candidates:
                        try:
                            if cand.is_file():
                                s = str(cand.resolve())
                                if s not in seen_resolved:
                                    seen_resolved.add(s)
                                    resolved_paths.append(s)
                                break
                        except OSError:
                            continue

            _consume_fps(filepaths)

            if not resolved_paths and rc == 0:
                od = _output_root_dir_from_ytdlp_template(output_template)
                if od is not None:
                    _consume_fps(scan_output_dir_for_existing_media(root=od, url=effective_url))

            if not resolved_paths:
                low = err_full.lower()
                if skip_existing and any(x in low for x in _YTDLP_SKIP_OR_ARCHIVE_MARKERS):
                    if cookie_tmp is not None:
                        cookie_tmp.unlink(missing_ok=True)
                    return {"success": True, "filepaths": [], "stderr": err_full[-1500:], "skipped_only": True}
                # Có trường hợp yt-dlp tải xong nhưng không in đúng after_move/path parser.
                # Nếu rc=0 và stderr có dấu hiệu hoàn tất download thì coi là thành công mềm.
                if any(x in low for x in ("[download] 100%", "destination:", "merging formats into")):
                    if cookie_tmp is not None:
                        cookie_tmp.unlink(missing_ok=True)
                    return {"success": True, "filepaths": [], "stderr": err_full[-1500:], "paths_unreported": True}
                # Một số bản yt-dlp + extractor Facebook trả rc=0 nhưng không in after_move/stderr.
                # Trường hợp này coi như skip mềm để UI không báo lỗi giả.
                if rc == 0:
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
            return {"success": True, "filepaths": resolved_paths, "stderr": err_full[-1000:]}
        finally:
            if batch_tmp is not None:
                batch_tmp.unlink(missing_ok=True)


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
        self._store = DownloadMetadataStore()
        self._yt = UniversalYTDLPWrapper(yt_cfg=dict(self._uvd.get("yt_dlp") or {}), log=self._log)
        self._cancel = threading.Event()
        self._active_job_id: str | None = None
        self._auto_refresh_ytdlp_background()

    @property
    def _paths(self) -> dict[str, Path]:
        """Luôn trỏ metadata downloader chuẩn (sau merge exe_gui / thư mục gốc)."""
        return ensure_downloader_layout()

    def _auto_refresh_ytdlp_background(self) -> None:
        def _work() -> None:
            yt = dict(self._uvd.get("yt_dlp") or {})
            delay = int(yt.get("auto_update_delay_sec", 90) or 0)
            delay = max(0, min(600, delay))
            if delay:
                time.sleep(delay)
            ret = self._yt.maybe_auto_update_standalone_exe(min_hours_between_attempts=24)
            if ret.get("ok"):
                self._log("[yt-dlp] Đã tự cập nhật yt-dlp.exe nền.")

        threading.Thread(target=_work, daemon=True, name="uv_auto_update_ytdlp").start()

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
        raw_job_name = str(options.get("job_name") or "").strip()
        job_name = raw_job_name[:120]
        job = {
            "id": f"dl_{uuid.uuid4().hex[:10]}",
            "name": job_name,
            "url": url,
            "platform": platform,
            "url_type": url_type,
            "max_videos": max(1, max_videos),
            "output_dir": out_dir,
            "organize_by_platform": bool(options.get("organize_by_platform", dl.get("organize_by_platform", True))),
            "organize_by_uploader": bool(options.get("organize_by_uploader", dl.get("organize_by_uploader", True))),
            "skip_existing": bool(options.get("skip_existing", dl.get("skip_existing", True))),
            "write_info_json": bool(options.get("write_info_json", self._uvd.get("yt_dlp", {}).get("write_info_json", True))),
            "write_thumbnail": bool(options.get("write_thumbnail", self._uvd.get("yt_dlp", {}).get("write_thumbnail", False))),
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

    def run_download_job(self, job_id: str, *, on_progress: ProgressHook | None = None) -> dict[str, Any]:
        from src.utils.concurrency_runtime import workload_scope

        job = self._store.get_job(job_id)
        if not job:
            raise KeyError(f"Không có job: {job_id}")
        with workload_scope("download"):
            return self._run_download_job_inner(job_id, on_progress=on_progress)

    def _run_download_job_inner(
        self, job_id: str, *, on_progress: ProgressHook | None = None
    ) -> dict[str, Any]:
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
            progress_hook=on_progress,
        )
        filepaths: list[str] = list(dict.fromkeys(ret.get("filepaths") or []))
        if ret.get("skipped_only") and not filepaths:
            od = Path(str(job.get("output_dir") or "")).expanduser().resolve()
            if od.is_dir():
                rescue = scan_output_dir_for_existing_media(root=od, url=str(job.get("url") or ""))
                if rescue:
                    filepaths = list(dict.fromkeys(rescue))
                    ret = {**ret, "skipped_only": False, "filepaths": filepaths, "success": True}
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
        jid0 = str(job.get("id") or "")
        videos_rows0 = self._store.list_downloaded_videos()
        seen_lower, job_paths_acc = _paths_seen_and_list_for_job(videos_rows0, jid0)
        for fp in filepaths:
            try:
                norm = str(Path(fp).expanduser().resolve())
                pl = norm.lower()
            except OSError:
                norm = str(fp)
                pl = norm.lower()
            if pl in seen_lower:
                continue
            rec = self._build_video_record(video_path=fp, job=job)
            records.append(rec)
            seen_lower.add(pl)
            job_paths_acc.append(norm)
        if records:
            self._store.save_downloaded_videos(records)
        job["downloaded_files"] = list(dict.fromkeys(job_paths_acc))
        job["status"] = "completed"
        job["completed_at"] = _now_iso()
        if ret.get("paths_unreported") and not job["downloaded_files"]:
            job["error_message"] = "yt-dlp hoàn tất nhưng không trả về đường dẫn file; kiểm tra thư mục tải."
        else:
            job["error_message"] = ""
        self._store.save_job(job)
        self._active_job_id = None
        return job

    def run_download_url_for_job(
        self,
        job_id: str,
        item_url: str,
        *,
        on_progress: ProgressHook | None = None,
        videos_rows: list[dict[str, Any]] | None = None,
        skip_output_dir_validate: bool = False,
    ) -> dict[str, Any]:
        """
        Tải một URL đơn và gộp kết quả vào job có sẵn (dùng cho batch: một job — nhiều video).
        Giữ ``status=running`` cho tới khi gọi ``finalize_batch_download_job``.
        """
        item_url = str(item_url or "").strip()
        if not item_url:
            raise ValueError("Thiếu URL")
        job = self._store.get_job(job_id)
        if not job:
            raise KeyError(f"Không có job: {job_id}")
        if self.is_cancel_requested():
            self._active_job_id = None
            return job
        self._active_job_id = job_id
        jid_q = str(job.get("id") or "")
        if videos_rows is None:
            videos_rows = self._store.list_downloaded_videos()
        seen_lower, job_paths_acc = _paths_seen_and_list_for_job(videos_rows, jid_q)
        if not skip_output_dir_validate:
            DownloadFolderManager.validate_output_dir(job["output_dir"])
        tmpl = DownloadFolderManager.build_output_template(job)
        if not str(job.get("started_at") or "").strip():
            job["status"] = "running"
            job["started_at"] = _now_iso()
        job["error_message"] = ""
        self._store.save_job(job)

        ret = self._yt.download(
            url=item_url,
            output_template=tmpl,
            archive_path=self._paths["archive"],
            url_type="single_video",
            max_videos=1,
            skip_existing=bool(job["skip_existing"]),
            write_info_json=bool(job["write_info_json"]),
            write_thumbnail=bool(job["write_thumbnail"]),
            cookie_path=str(job.get("cookie_path") or ""),
            cancel_event=self._cancel,
            log_lines=self._log,
            progress_hook=on_progress,
        )
        filepaths: list[str] = list(dict.fromkeys(ret.get("filepaths") or []))
        if ret.get("skipped_only") and not filepaths:
            od = Path(str(job.get("output_dir") or "")).expanduser().resolve()
            if od.is_dir():
                rescue = scan_output_dir_for_existing_media(root=od, url=item_url)
                if rescue:
                    filepaths = list(dict.fromkeys(rescue))
                    ret = {**ret, "skipped_only": False, "filepaths": filepaths, "success": True}
        if ret.get("skipped_only"):
            job = self._store.get_job(job_id) or job
            self._attach_existing_sources_to_job(job=job, source_url=item_url, videos_rows=videos_rows)
            job["status"] = "running"
            self._store.save_job(job)
            self._active_job_id = None
            return job
        if not bool(ret.get("success")):
            err = str(ret.get("error") or "Lỗi không xác định")
            job = self._store.get_job(job_id) or job
            failed_items = list(job.get("failed_items") or [])
            if err == "need_manual_upload":
                failed_items.append({"url": item_url, "error": str(ret.get("message") or err)})
            else:
                failed_items.append({"url": item_url, "error": err[:1500]})
            job["failed_items"] = failed_items
            job["status"] = "running"
            self._store.save_job(job)
            self._active_job_id = None
            return job

        records: list[dict[str, Any]] = []
        for fp in filepaths:
            try:
                norm = str(Path(fp).expanduser().resolve())
                pl = norm.lower()
            except OSError:
                norm = str(fp)
                pl = norm.lower()
            if pl in seen_lower:
                continue
            rec = self._build_video_record(video_path=fp, job=job, item_url=item_url)
            records.append(rec)
            seen_lower.add(pl)
            job_paths_acc.append(norm)
        if records:
            self._store.save_downloaded_videos(records)
            for rec in records:
                videos_rows.insert(0, rec)
        job["downloaded_files"] = list(dict.fromkeys(job_paths_acc))
        job["status"] = "running"
        if ret.get("paths_unreported") and not filepaths:
            pass
        self._store.save_job(job)
        self._active_job_id = None
        return job

    def run_download_urls_batch_for_job(
        self, job_id: str, urls: list[str], *, on_progress: ProgressHook | None = None
    ) -> dict[str, Any]:
        """
        Tải nhiều URL đơn trong **một** tiến trình yt-dlp (``-a`` batch file + ``--ignore-errors``),
        tránh khởi động lại yt-dlp/merge từng URL — nhanh hơn rõ rệt trên máy khách so với tuần tự.
        """
        urls = [str(u).strip() for u in urls if str(u).strip()]
        if not urls:
            raise ValueError("Thiếu URL")
        if len(urls) == 1:
            return self.run_download_url_for_job(job_id, urls[0], on_progress=on_progress)
        job = self._store.get_job(job_id)
        if not job:
            raise KeyError(f"Không có job: {job_id}")
        if self.is_cancel_requested():
            self._active_job_id = None
            return job
        self._active_job_id = job_id
        jid_q = str(job.get("id") or "")
        videos_rows = self._store.list_downloaded_videos()
        seen_lower, job_paths_acc = _paths_seen_and_list_for_job(videos_rows, jid_q)
        DownloadFolderManager.validate_output_dir(job["output_dir"])
        tmpl = DownloadFolderManager.build_output_template(job)
        if not str(job.get("started_at") or "").strip():
            job["status"] = "running"
            job["started_at"] = _now_iso()
        job["error_message"] = ""
        self._store.save_job(job)

        ret = self._yt.download(
            url=urls[0],
            output_template=tmpl,
            archive_path=self._paths["archive"],
            url_type="single_video",
            max_videos=len(urls),
            skip_existing=bool(job["skip_existing"]),
            write_info_json=bool(job["write_info_json"]),
            write_thumbnail=bool(job["write_thumbnail"]),
            cookie_path=str(job.get("cookie_path") or ""),
            cancel_event=self._cancel,
            log_lines=self._log,
            batch_urls=urls,
            progress_hook=on_progress,
        )
        filepaths: list[str] = list(dict.fromkeys(ret.get("filepaths") or []))
        if ret.get("skipped_only") and not filepaths:
            od = Path(str(job.get("output_dir") or "")).expanduser().resolve()
            if od.is_dir():
                rescue_all: list[str] = []
                for u in urls[:80]:
                    rescue_all.extend(scan_output_dir_for_existing_media(root=od, url=u))
                if rescue_all:
                    filepaths = list(dict.fromkeys(rescue_all))
                    ret = {**ret, "skipped_only": False, "filepaths": filepaths, "success": True}
        job = self._store.get_job(job_id) or job
        failed_items = list(job.get("failed_items") or [])
        if ret.get("skipped_only") and not filepaths:
            for u in urls:
                self._attach_existing_sources_to_job(job=job, source_url=u, videos_rows=videos_rows)
            job["status"] = "running"
            self._store.save_job(job)
            self._active_job_id = None
            return job
        if not bool(ret.get("success")) and not filepaths:
            err = str(ret.get("error") or "Lỗi không xác định")
            for u in urls:
                if err == "need_manual_upload":
                    failed_items.append({"url": u, "error": str(ret.get("message") or err)})
                else:
                    failed_items.append({"url": u, "error": err[:900]})
            job["failed_items"] = failed_items
            job["status"] = "running"
            self._store.save_job(job)
            self._active_job_id = None
            return job

        records: list[dict[str, Any]] = []
        for fp in filepaths:
            try:
                norm = str(Path(fp).expanduser().resolve())
                pl = norm.lower()
            except OSError:
                norm = str(fp)
                pl = norm.lower()
            if pl in seen_lower:
                continue
            rec = self._build_video_record(video_path=fp, job=job, item_url="")
            records.append(rec)
            seen_lower.add(pl)
            job_paths_acc.append(norm)
        if records:
            self._store.save_downloaded_videos(records)

        missing = _batch_urls_still_missing(urls, job_paths_acc)
        od = Path(str(job.get("output_dir") or "")).expanduser().resolve()
        rescue_records: list[dict[str, Any]] = []
        if missing and od.is_dir():
            for m in missing:
                for fp in scan_output_dir_for_existing_media(root=od, url=m):
                    try:
                        norm = str(Path(fp).expanduser().resolve())
                        pl = norm.lower()
                    except OSError:
                        norm = str(fp)
                        pl = norm.lower()
                    if pl in seen_lower:
                        continue
                    rec = self._build_video_record(video_path=fp, job=job, item_url=m)
                    rescue_records.append(rec)
                    seen_lower.add(pl)
                    job_paths_acc.append(norm)
        if rescue_records:
            self._store.save_downloaded_videos(rescue_records)

        missing = _batch_urls_still_missing(urls, job_paths_acc)
        yt_opts = dict(self._uvd.get("yt_dlp") or {})
        if missing and bool(yt_opts.get("batch_sequential_retry", True)):
            raw_cap = yt_opts.get("batch_sequential_retry_max_urls", 20)
            try:
                cap = int(raw_cap)
            except (TypeError, ValueError):
                cap = 20
            cap = max(0, min(500, cap))
            retry_list = list(missing) if cap == 0 else list(missing)[:cap]
            skipped = len(missing) - len(retry_list)
            if skipped > 0:
                self._log(
                    f"[yt-dlp] batch retry: giới hạn {len(retry_list)}/{len(missing)} URL "
                    f"(batch_sequential_retry_max_urls={cap}; {skipped} URL bỏ qua retry, vẫn báo lỗi/ghép sau)."
                )
            for m in retry_list:
                try:
                    self.run_download_url_for_job(job_id, m, on_progress=on_progress)
                except Exception as exc:  # noqa: BLE001
                    self._log(f"[yt-dlp] batch retry URL lỗi: {m[:120]}… | {exc}")
            job = self._store.get_job(job_id) or job
            failed_items = list(job.get("failed_items") or [])
            videos_rows_after = self._store.list_downloaded_videos()
            _, job_paths_acc = _paths_seen_and_list_for_job(videos_rows_after, jid_q)
            missing = _batch_urls_still_missing(urls, job_paths_acc)

        failed_by_url = {str(x.get("url") or "").strip() for x in failed_items if str(x.get("url") or "").strip()}
        for m in missing:
            if m in failed_by_url:
                continue
            failed_items.append({"url": m, "error": "Không tải được hoặc không khớp metadata sau batch (xem log)."})
            failed_by_url.add(m)
        job["failed_items"] = failed_items
        job["downloaded_files"] = list(dict.fromkeys(job_paths_acc))
        job["status"] = "running"
        self._store.save_job(job)
        self._active_job_id = None
        return job

    def run_download_urls_sequential_for_job(
        self,
        job_id: str,
        urls: list[str],
        *,
        on_progress: ProgressHook | None = None,
        on_item_done: Callable[[int, int, str], None] | None = None,
    ) -> dict[str, Any]:
        """
        Tải lần lượt từng URL trong một job — ổn định với danh sách dài (vài nghìn video).
        Gọi ``finalize_batch_download_job`` sau khi vòng lặp kết thúc.
        """
        urls = [str(u).strip() for u in urls if str(u).strip()]
        if not urls:
            raise ValueError("Thiếu URL")
        total = len(urls)
        job = self._store.get_job(job_id)
        if not job:
            raise KeyError(f"Không có job: {job_id}")
        videos_rows = self._store.list_downloaded_videos()
        output_validated = False
        refresh_cache_every = max(8, min(40, int(self._uvd.get("yt_dlp", {}).get("sequential_cache_refresh_every") or 20)))
        for idx, item_url in enumerate(urls, start=1):
            if self.is_cancel_requested():
                break
            if on_item_done is not None:
                try:
                    on_item_done(idx, total, item_url)
                except Exception:
                    pass
            try:
                job = self.run_download_url_for_job(
                    job_id,
                    item_url,
                    on_progress=on_progress,
                    videos_rows=videos_rows,
                    skip_output_dir_validate=output_validated,
                )
                output_validated = True
            except Exception as exc:  # noqa: BLE001
                self._log(f"[yt-dlp] tuần tự URL {idx}/{total} lỗi: {item_url[:120]}… | {exc}")
            if idx % refresh_cache_every == 0:
                videos_rows = self._store.list_downloaded_videos()
        return self._store.get_job(job_id) or job

    def _attach_existing_sources_to_job(
        self,
        *,
        job: dict[str, Any],
        source_url: str,
        videos_rows: list[dict[str, Any]] | None = None,
    ) -> int:
        """
        Nếu URL bị skip (archive) nhưng video đã tồn tại từ job cũ, tạo bản ghi mới
        trỏ cùng file vào job hiện tại để Bước 4/Video Editor nhìn thấy theo job mới.
        """
        jid = str(job.get("id") or "").strip()
        if not jid:
            return 0
        src_key = _norm_url_key(source_url).lower()
        src_vid = _extract_video_id_for_scan(source_url)
        if not src_key and not src_vid:
            return 0
        rows = [r for r in (videos_rows or self._store.list_downloaded_videos()) if isinstance(r, dict)]
        existing_by_path: set[str] = set()
        for r in rows:
            if str(r.get("download_job_id") or "").strip() != jid:
                continue
            p = str(r.get("video_path") or "").strip()
            if p:
                existing_by_path.add(p.lower())
        added = 0
        clones: list[dict[str, Any]] = []
        for r in rows:
            old_job = str(r.get("download_job_id") or "").strip()
            if not old_job or old_job == jid:
                continue
            vp = str(r.get("video_path") or "").strip()
            if not vp:
                continue
            rk = _norm_url_key(str(r.get("source_url") or "")).lower()
            rvid = _extract_video_id_for_scan(str(r.get("source_url") or ""))
            matched = False
            if src_vid and rvid and src_vid == rvid:
                matched = True
            elif src_key and rk and (src_key == rk or src_key in rk or rk in src_key):
                matched = True
            if not matched:
                continue
            if vp.lower() in existing_by_path:
                continue
            clone = dict(r)
            clone["id"] = f"src_video_{uuid.uuid4().hex[:10]}"
            clone["download_job_id"] = jid
            clone["download_job_name"] = str(job.get("name") or "")
            clone["created_at"] = _now_iso()
            clones.append(clone)
            existing_by_path.add(vp.lower())
            added += 1
        if clones:
            self._store.save_downloaded_videos(clones)
            job["downloaded_files"] = [
                str(r.get("video_path") or "")
                for r in self._store.list_downloaded_videos()
                if str(r.get("download_job_id") or "") == jid and str(r.get("video_path") or "").strip()
            ]
        return added

    def finalize_batch_download_job(self, job_id: str) -> dict[str, Any]:
        """Đóng batch: cập nhật trạng thái job sau khi đã gọi ``run_download_url_for_job`` nhiều lần."""
        job = self._store.get_job(job_id)
        if not job:
            raise KeyError(f"Không có job: {job_id}")
        paths = [
            str(r.get("video_path") or "")
            for r in self._store.list_downloaded_videos()
            if str(r.get("download_job_id") or "") == str(job.get("id") or "") and str(r.get("video_path") or "").strip()
        ]
        job["downloaded_files"] = paths
        fails = list(job.get("failed_items") or [])
        n_ok = len(paths)
        n_fail = len(fails)
        if n_ok == 0 and n_fail > 0:
            job["status"] = "failed"
            job["error_message"] = f"Tất cả {n_fail} URL lỗi (xem failed_items / log)."
        elif n_fail > 0:
            job["status"] = "completed"
            job["error_message"] = f"Hoàn tất: {n_ok} file, {n_fail} URL lỗi."
        else:
            job["status"] = "completed"
            job["error_message"] = ""
        job["completed_at"] = _now_iso()
        job["updated_at"] = _now_iso()
        self._store.save_job(job)
        return job

    def _build_video_record(
        self,
        *,
        video_path: str,
        job: dict[str, Any],
        item_url: str = "",
    ) -> dict[str, Any]:
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
        # Batch nhiều URL: job["url"] là URL gốc (tab/kênh); ưu tiên URL từng video.
        source_url = str(item_url or "").strip() or str(job.get("url") or "")
        description = ""
        hashtags: list[str] = []
        if info_path.is_file():
            try:
                meta = json.loads(info_path.read_text(encoding="utf-8"))
                if isinstance(meta, dict):
                    title = str(meta.get("title") or title)
                    uploader = str(meta.get("uploader") or meta.get("channel") or "")
                    duration = float(meta.get("duration") or 0)
                    upload_date = str(meta.get("upload_date") or "")
                    source_url = str(meta.get("webpage_url") or meta.get("original_url") or source_url)
                    description = str(meta.get("description") or "").strip()
                    tags_raw = meta.get("tags")
                    if isinstance(tags_raw, list):
                        for t in tags_raw:
                            s = str(t or "").strip()
                            if not s:
                                continue
                            if not s.startswith("#"):
                                s = "#" + s.lstrip("#")
                            if s.lower() not in {x.lower() for x in hashtags}:
                                hashtags.append(s)
                            if len(hashtags) >= 50:
                                break
                    elif isinstance(tags_raw, str) and tags_raw.strip():
                        for t in re.split(r"[,;\n\r]+", tags_raw):
                            s = str(t or "").strip()
                            if not s:
                                continue
                            if not s.startswith("#"):
                                s = "#" + s.lstrip("#")
                            if s.lower() not in {x.lower() for x in hashtags}:
                                hashtags.append(s)
                            if len(hashtags) >= 50:
                                break
                    for s in _extract_hashtags_from_text(description):
                        if s.lower() not in {x.lower() for x in hashtags}:
                            hashtags.append(s)
                            if len(hashtags) >= 50:
                                break
            except Exception:
                pass
        return {
            "id": f"src_video_{uuid.uuid4().hex[:10]}",
            "download_job_id": str(job.get("id") or ""),
            "download_job_name": str(job.get("name") or ""),
            "platform": str(job.get("platform") or ""),
            "source_url": source_url,
            "title": title,
            "description": description,
            "hashtags": hashtags,
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

    def get_download_job(self, job_id: str) -> dict[str, Any] | None:
        return self._store.get_job(str(job_id or "").strip())

    def check_url(self, url: str) -> dict[str, Any]:
        return self._yt.get_info(url)

    def list_flat_playlist_entries(
        self,
        url: str,
        *,
        max_entries: int = 500,
        on_partial: Callable[[list[dict[str, str]]], None] | None = None,
    ) -> dict[str, Any]:
        """Ủy quyền tới ``UniversalYTDLPWrapper`` (quét flat-playlist cho kênh/playlist YouTube)."""
        return self._yt.list_flat_playlist_entries(url, max_entries=max_entries, on_partial=on_partial)

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

    def delete_downloaded_video(
        self,
        video_id: str,
        *,
        delete_file: bool = False,
        prune_empty_dirs: bool = True,
    ) -> bool:
        return self._store.delete_video_record(
            video_id,
            delete_file=delete_file,
            prune_empty_dirs=prune_empty_dirs,
        )

    def delete_download_job(
        self,
        job_id: str,
        *,
        delete_files: bool = True,
        prune_empty_dirs: bool = True,
    ) -> dict[str, int | bool]:
        deleted_videos = self._store.delete_videos_by_job(
            job_id,
            delete_file=delete_files,
            prune_empty_dirs=prune_empty_dirs,
        )
        deleted_job = self._store.delete_job(job_id)
        return {
            "deleted_job": bool(deleted_job),
            "deleted_videos": int(deleted_videos),
        }

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
