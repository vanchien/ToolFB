"""
Thêm batch job lịch đăng — 3 chế độ: thủ công, AI nhiều bài, thư mục video (Tkinter).

Đối chiếu thiết kế: khối A–E; preview trước khi lưu; lưu nhiều bản ghi ``schedule_posts.json``.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import threading
import uuid
import tkinter as tk
import urllib.request
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any, Literal
from zoneinfo import ZoneInfo

from loguru import logger

from src.ai.image_generation import build_imagen_prompt_from_post
from src.services.ai_image_service import AIImageService
from src.services.ai_text_service import AITextService
from src.services.video_ai_service import (
    generate_video_caption_from_frames,
    generate_video_hashtags,
    generate_video_title_from_frames,
)
from src.utils.page_schedule import parse_date_only_yyyy_mm_dd, scheduler_tz
from src.utils.page_workspace import load_page_ai_config
from src.utils.pages_manager import PagesManager
from src.utils.reel_thumbnail_choice import REEL_THUMBNAIL_METHOD1_FIRST_AUTO
from src.utils.schedule_batch_preview import (
    build_schedule_by_daily_slots,
    compute_scheduled_at_series,
    page_post_style_for_post_type,
    post_type_for_kind,
    preview_row_to_schedule_job,
    scan_video_files,
)
from src.utils.schedule_posts_manager import SchedulePostsManager
from src.utils.ffmpeg_paths import portable_ffmpeg_bin_dir, resolve_ffmpeg_ffprobe_paths

_FFMPEG_INSTALL_ATTEMPTED = False


def _split_comma(s: str) -> list[str]:
    return [p.strip() for p in str(s).split(",") if p.strip()]


def _hashtags_from_text(raw: str, *, limit: int = 8) -> list[str]:
    tags: list[str] = []
    for m in re.findall(r"#?[A-Za-z0-9_À-ỹ]+", str(raw or "")):
        t = m.strip()
        if not t:
            continue
        if not t.startswith("#"):
            t = "#" + t
        if t not in tags:
            tags.append(t)
        if len(tags) >= max(1, int(limit)):
            break
    return tags


def _normalize_short_video_title(raw: str, fallback: str = "") -> str:
    """
    Chuẩn hoá tiêu đề ngắn cho video:
    - ưu tiên 1 câu ngắn
    - giới hạn tối đa 12 từ
    - nếu quá ngắn/rỗng thì fallback tên file
    """
    s = str(raw or "").strip().replace("\n", " ")
    while "  " in s:
        s = s.replace("  ", " ")
    if not s:
        return ""
    s = re.sub(r"^(title|caption|tiêu đề)\s*[:\-]\s*", "", s, flags=re.I).strip()
    # Lấy câu đầu tiên nếu AI trả nhiều câu.
    for sep in (".", "!", "?", ";"):
        if sep in s:
            s = s.split(sep, 1)[0].strip()
            break
    words = [w for w in s.split(" ") if w]
    if len(words) > 12:
        s = " ".join(words[:12]).strip()
        words = [w for w in s.split(" ") if w]
    if not words:
        return ""
    return s[:200]


def _title_from_body_first_sentence(body: str) -> str:
    s = str(body or "").strip().replace("\n", " ")
    while "  " in s:
        s = s.replace("  ", " ")
    if not s:
        return ""
    for sep in (".", "!", "?", ";", "\u3002", "\uff01", "\uff1f"):
        if sep in s:
            s = s.split(sep, 1)[0].strip()
            break
    parts = [p for p in s.split(" ") if p]
    if len(parts) > 12:
        s = " ".join(parts[:12]).strip()
    return s[:200]


_VI_DIACRITICS_RE = re.compile(r"[ăâđêôơưáàảãạấầẩẫậắằẳẵặéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ]", re.I)
_THAI_RE = re.compile(r"[\u0E00-\u0E7F]")
_HANGUL_RE = re.compile(r"[\uAC00-\uD7AF\u1100-\u11FF]")
_HIRAGANA_KATAKANA_RE = re.compile(r"[\u3040-\u30FF]")
_CJK_RE = re.compile(r"[\u4E00-\u9FFF]")
_LATIN_WORD_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ0-9'&\-]+")


def _contains_non_latin_script(s: str) -> bool:
    return bool(_THAI_RE.search(s) or _HANGUL_RE.search(s) or _HIRAGANA_KATAKANA_RE.search(s) or _CJK_RE.search(s))


def _title_matches_language(title: str, lang_label: str) -> bool:
    s = str(title or "").strip()
    if not s:
        return False
    low = str(lang_label or "").strip().lower()
    has_thai = bool(_THAI_RE.search(s))
    has_hangul = bool(_HANGUL_RE.search(s))
    has_jp = bool(_HIRAGANA_KATAKANA_RE.search(s))
    has_cjk = bool(_CJK_RE.search(s))
    if "english" in low:
        # English: không có dấu tiếng Việt và phần lớn token là ký tự latin cơ bản.
        if _VI_DIACRITICS_RE.search(s) or _contains_non_latin_script(s):
            return False
        toks = [t for t in re.split(r"\s+", s) if t]
        if not toks:
            return False
        ascii_like = sum(1 for t in toks if re.fullmatch(r"[A-Za-z0-9'&\-]+", t))
        return ascii_like >= max(2, int(len(toks) * 0.7))
    if "日本語" in low or "japanese" in low:
        # Japanese: cần có Kana hoặc Kanji; loại các câu Latin thuần.
        return bool(has_jp or has_cjk)
    if "한국어" in low or "korean" in low:
        return has_hangul
    if "中文" in low or "chinese" in low:
        return has_cjk
    if "thai" in low or "ไทย" in low:
        return has_thai
    if "tiếng việt" in low:
        # Tiếng Việt: chấp nhận không dấu, nhưng không chấp nhận script CJK/Thai/Hangul.
        return not _contains_non_latin_script(s)
    if any(x in low for x in ("bahasa", "español", "portugu", "français", "deutsch", "spanish", "french", "german")):
        # Các ngôn ngữ Latin: không nhận script châu Á.
        if _contains_non_latin_script(s):
            return False
        words = _LATIN_WORD_RE.findall(s)
        return len(words) >= 2
    # Ngôn ngữ khác: chưa có detector tin cậy ở local, chấp nhận kết quả không rỗng.
    return True


def _scan_video_metadata(vp: Path) -> dict[str, Any]:
    """
    Quét metadata kỹ thuật của video (best-effort) để làm ngữ cảnh sinh caption.

    Không decode full video để tránh nặng CPU; ưu tiên ``ffprobe`` nếu có.
    """
    out: dict[str, Any] = {
        "name": vp.name,
        "stem": vp.stem,
        "size_mb": round((vp.stat().st_size or 0) / (1024 * 1024), 2),
        "duration_s": None,
        "width": None,
        "height": None,
    }
    _ffmpeg, ffprobe = _resolve_ffmpeg_probe_paths()
    if not ffprobe:
        return out
    try:
        cmd = [
            ffprobe,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height,duration",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=0",
            str(vp),
        ]
        cp = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if cp.returncode != 0:
            return out
        for line in (cp.stdout or "").splitlines():
            s = line.strip()
            if not s or "=" not in s:
                continue
            k, v = s.split("=", 1)
            if k == "width":
                try:
                    out["width"] = int(float(v))
                except (TypeError, ValueError):
                    pass
            elif k == "height":
                try:
                    out["height"] = int(float(v))
                except (TypeError, ValueError):
                    pass
            elif k == "duration":
                try:
                    dur = float(v)
                    if dur > 0:
                        out["duration_s"] = int(round(dur))
                except (TypeError, ValueError):
                    pass
    except Exception as exc:  # noqa: BLE001
        logger.debug("ffprobe metadata lỗi ({}): {}", vp.name, exc)
    return out


def _video_context_for_prompt(vp: Path, meta: dict[str, Any]) -> str:
    dur = meta.get("duration_s")
    wh = ""
    if meta.get("width") and meta.get("height"):
        wh = f"{meta.get('width')}x{meta.get('height')}"
    return (
        f"Tên file: {vp.name}\n"
        f"Tên không đuôi: {vp.stem}\n"
        f"Dung lượng: {meta.get('size_mb')} MB\n"
        f"Thời lượng: {dur if dur is not None else 'unknown'} giây\n"
        f"Độ phân giải: {wh or 'unknown'}\n"
    )


def _project_root_dir() -> Path:
    from src.utils.paths import project_root

    return project_root()


def _resolve_ffmpeg_probe_paths() -> tuple[str | None, str | None]:
    return resolve_ffmpeg_ffprobe_paths()


def _install_portable_ffmpeg_into_tool() -> bool:
    """
    Cài ffmpeg portable ngay trong project để mang tool sang máy khác vẫn dùng được.
    """
    exe_ffmpeg = "ffmpeg.exe" if os.name == "nt" else "ffmpeg"
    exe_ffprobe = "ffprobe.exe" if os.name == "nt" else "ffprobe"
    bin_dir = portable_ffmpeg_bin_dir()
    local_ffmpeg = bin_dir / exe_ffmpeg
    local_ffprobe = bin_dir / exe_ffprobe
    if local_ffmpeg.is_file() and local_ffprobe.is_file():
        return True
    if os.name != "nt":
        return False
    root = _project_root_dir() / "tools" / "ffmpeg"
    download_dir = root / "downloads"
    extract_dir = root / "extracted"
    download_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)
    bin_dir.mkdir(parents=True, exist_ok=True)
    url = os.environ.get(
        "FFMPEG_PORTABLE_URL",
        "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip",
    ).strip()
    zip_path = download_dir / "ffmpeg-release-essentials.zip"
    try:
        urllib.request.urlretrieve(url, str(zip_path))
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = zf.namelist()
            ff_member = next((m for m in members if m.lower().endswith("/bin/" + exe_ffmpeg.lower())), "")
            fp_member = next((m for m in members if m.lower().endswith("/bin/" + exe_ffprobe.lower())), "")
            # Ưu tiên giải nén trực tiếp 2 file cần dùng để giảm file dư thừa.
            if ff_member and fp_member:
                with zf.open(ff_member) as src, local_ffmpeg.open("wb") as dst:
                    dst.write(src.read())
                with zf.open(fp_member) as src, local_ffprobe.open("wb") as dst:
                    dst.write(src.read())
            else:
                # Fallback: layout zip khác chuẩn -> extract và dò lại.
                zf.extractall(extract_dir)
                found_ffmpeg = None
                found_ffprobe = None
                for p in extract_dir.rglob(exe_ffmpeg):
                    if p.is_file():
                        found_ffmpeg = p
                        break
                for p in extract_dir.rglob(exe_ffprobe):
                    if p.is_file():
                        found_ffprobe = p
                        break
                if not found_ffmpeg or not found_ffprobe:
                    return False
                shutil.copy2(found_ffmpeg, local_ffmpeg)
                shutil.copy2(found_ffprobe, local_ffprobe)
        keep_cache = os.environ.get("FFMPEG_KEEP_INSTALL_CACHE", "0").strip().lower() in {"1", "true", "yes", "on"}
        if not keep_cache:
            try:
                shutil.rmtree(extract_dir, ignore_errors=True)
            except Exception:
                pass
            try:
                if zip_path.is_file():
                    zip_path.unlink()
            except Exception:
                pass
        return local_ffmpeg.is_file() and local_ffprobe.is_file()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không cài được ffmpeg portable nội bộ: {}", exc)
        return False


def _ensure_ffmpeg_available() -> bool:
    global _FFMPEG_INSTALL_ATTEMPTED
    ffmpeg_path, ffprobe_path = _resolve_ffmpeg_probe_paths()
    if ffmpeg_path and ffprobe_path:
        return True
    if _FFMPEG_INSTALL_ATTEMPTED:
        ffmpeg_path, ffprobe_path = _resolve_ffmpeg_probe_paths()
        return bool(ffmpeg_path and ffprobe_path)
    _FFMPEG_INSTALL_ATTEMPTED = True
    # 1) Ưu tiên cài portable vào trong tool.
    if _install_portable_ffmpeg_into_tool():
        ffmpeg_path, ffprobe_path = _resolve_ffmpeg_probe_paths()
        if ffmpeg_path and ffprobe_path:
            logger.info("Đã cài ffmpeg portable trong tools/ffmpeg/bin.")
            return True
    # 2) Fallback cài hệ thống bằng winget.
    if os.name != "nt":
        return False
    winget = shutil.which("winget")
    if not winget:
        logger.warning("Không tìm thấy winget để tự cài ffmpeg.")
        return False
    try:
        # Cài tự động best-effort trên Windows; có thể fail nếu thiếu quyền.
        cp = subprocess.run(
            [
                winget,
                "install",
                "--id",
                "Gyan.FFmpeg",
                "-e",
                "--accept-package-agreements",
                "--accept-source-agreements",
                "--silent",
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if cp.returncode != 0:
            logger.warning("Tự cài ffmpeg thất bại (code={}): {}", cp.returncode, (cp.stderr or cp.stdout or "").strip()[:300])
    except Exception as exc:  # noqa: BLE001
        logger.warning("Lỗi tự cài ffmpeg qua winget: {}", exc)
    ffmpeg_path, ffprobe_path = _resolve_ffmpeg_probe_paths()
    return bool(ffmpeg_path and ffprobe_path)


def _sampling_timestamps(duration_s: int | None) -> list[int]:
    """
    Chọn mốc chụp frame theo độ dài video:
    - video ngắn: có mốc 2s, 5s, 8s + khoảng cách 4-8s
    - video dài: ưu tiên vùng 10s..20s và tăng bước để chạy nhanh
    """
    if duration_s is None or duration_s <= 0:
        return [2, 6, 10]
    d = int(duration_s)
    if d <= 20:
        base = [2, 5, 8, 12, 16]
    elif d <= 60:
        base = [2, 6, 10, 16, 24, 32, 40, 50]
    elif d <= 180:
        base = [8, 14, 20, 30, 42, 56, 72, 90, 120, 150]
    else:
        base = [10, 16, 20, 30, 45, 65, 90, 120, 160, 210]
    # Cắt mốc vượt duration-1 và loại trùng
    limit = max(2, d - 1)
    out = sorted({x for x in base if 1 <= x <= limit})
    if not out:
        return [2]
    return out


def _extract_video_frames_for_ai(vp: Path, meta: dict[str, Any]) -> list[Path]:
    """
    Trích frame JPEG theo timestamps để AI đọc nội dung video chính xác hơn.
    """
    if not _ensure_ffmpeg_available():
        return []
    ffmpeg, _ffprobe = _resolve_ffmpeg_probe_paths()
    if not ffmpeg:
        return []
    out_dir = (Path.cwd() / "logs" / "video_ai_frames" / f"{vp.stem}_{uuid.uuid4().hex[:8]}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    ts_list = _sampling_timestamps(meta.get("duration_s"))
    frames: list[Path] = []
    for sec in ts_list:
        out = out_dir / f"frame_{sec:04d}s.jpg"
        try:
            cp = subprocess.run(
                [
                    ffmpeg,
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-ss",
                    str(sec),
                    "-i",
                    str(vp),
                    "-frames:v",
                    "1",
                    "-q:v",
                    "3",
                    "-y",
                    str(out),
                ],
                capture_output=True,
                text=True,
                timeout=20,
            )
            if cp.returncode == 0 and out.is_file() and out.stat().st_size > 0:
                frames.append(out)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Cắt frame lỗi {} @{}s: {}", vp.name, sec, exc)
    return frames


def _generate_video_caption_from_frames(
    *,
    vp: Path,
    meta: dict[str, Any],
    frame_paths: list[Path],
    language: str,
    idea: str,
    provider: str | None = None,
    model: str | None = None,
) -> str:
    return generate_video_caption_from_frames(
        vp=vp,
        meta=meta,
        frame_paths=frame_paths,
        language=language,
        idea=idea,
        video_context=_video_context_for_prompt(vp, meta),
        provider=provider,
        model=model,
    )


def _generate_video_title_from_frames(
    *,
    vp: Path,
    meta: dict[str, Any],
    frame_paths: list[Path],
    language: str,
    idea: str,
    provider: str | None = None,
    model: str | None = None,
) -> str:
    return generate_video_title_from_frames(
        vp=vp,
        meta=meta,
        frame_paths=frame_paths,
        language=language,
        idea=idea,
        video_context=_video_context_for_prompt(vp, meta),
        provider=provider,
        model=model,
    )


def _generate_video_hashtags(
    *,
    language: str,
    idea: str,
    title: str,
    video_context: str,
    count: int = 6,
    provider: str | None = None,
    model: str | None = None,
) -> list[str]:
    n = max(1, min(12, int(count)))
    return generate_video_hashtags(
        language=language,
        idea=idea,
        title=title,
        video_context=video_context,
        count=n,
        provider=provider,
        model=model,
    )


class ScheduleBatchJobDialog:
    """Dialog thêm batch job (không dùng cho sửa 1 job chi tiết — dùng ``SchedulePostJobDialog``)."""

    MODE_MANUAL = "manual"
    MODE_AI = "ai"
    MODE_VIDEO = "video"
    AI_LANG_OPTIONS: tuple[str, ...] = (
        "Tiếng Việt",
        "English",
        "Bahasa Indonesia",
        "ไทย (Thai)",
        "Español",
        "Português",
        "Français",
        "Deutsch",
        "日本語",
        "한국어",
        "中文",
    )

    def __init__(
        self,
        parent: tk.Tk | tk.Toplevel,
        store: SchedulePostsManager,
        pages: PagesManager,
        owner_account_ids: list[str],
        *,
        title: str = "Thêm batch job lịch đăng",
    ) -> None:
        self._store = store
        self._pages = pages
        self._owner_ids = [str(x).strip() for x in owner_account_ids if str(x).strip()]
        self._page_label_to_id: dict[str, str] = {}
        self._page_id_to_label: dict[str, str] = {}
        self.saved_count = 0
        self._preview_rows: list[dict[str, Any]] = []
        self._sel_render_after_id: str | None = None
        self._batch_ai_override: dict[str, Any] | None = None
        self._preview_busy = False
        self._ai_text_service = AITextService()
        self._ai_image_service = AIImageService()

        self._top = tk.Toplevel(parent)
        self._top.title(title)
        self._top.transient(parent)
        self._top.grab_set()
        self._top.geometry("980x720")
        self._top.minsize(880, 600)

        self._main_canvas = tk.Canvas(self._top, highlightthickness=0, borderwidth=0)
        self._main_vsb = ttk.Scrollbar(self._top, orient=tk.VERTICAL, command=self._main_canvas.yview)
        self._main_canvas.configure(yscrollcommand=self._main_vsb.set)
        self._main_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._main_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        root = ttk.Frame(self._main_canvas, padding=8)
        self._main_window_id = self._main_canvas.create_window((0, 0), window=root, anchor="nw")
        root.rowconfigure(3, weight=1)
        root.columnconfigure(0, weight=1)
        root.bind("<Configure>", self._sync_main_scrollregion)
        self._main_canvas.bind("<Configure>", self._sync_main_scrollregion)
        self._main_canvas.bind("<Enter>", lambda _e: self._bind_main_mousewheel(True))
        self._main_canvas.bind("<Leave>", lambda _e: self._bind_main_mousewheel(False))

        self._build_block_a(root)
        self._mode_panel_container = ttk.Frame(root, padding=(0, 8))
        self._mode_panel_container.grid(row=1, column=0, sticky="ew")
        self._build_mode_panels()
        self._build_block_c(root)
        self._build_block_d(root)
        self._build_block_e(root)

        self._on_mode_changed()
        self._refresh_ai_image_style_choices(apply_suggestion=True)
        self._sync_image_provider_controls(source="top")
        self._top.protocol("WM_DELETE_WINDOW", self._cancel)
        self.window = self._top

    def _sync_main_scrollregion(self, _event: tk.Event | None = None) -> None:
        """Đồng bộ vùng cuộn cho toàn bộ dialog batch job."""
        self._main_canvas.update_idletasks()
        bbox = self._main_canvas.bbox("all")
        if bbox:
            self._main_canvas.configure(scrollregion=bbox)
        w = self._main_canvas.winfo_width()
        if w > 1:
            self._main_canvas.itemconfigure(self._main_window_id, width=w)

    def _bind_main_mousewheel(self, enable: bool) -> None:
        if enable:
            self._top.bind("<MouseWheel>", self._on_main_mousewheel, add="+")
            self._top.bind("<Button-4>", self._on_main_mousewheel, add="+")
            self._top.bind("<Button-5>", self._on_main_mousewheel, add="+")
            return
        self._top.unbind("<MouseWheel>")
        self._top.unbind("<Button-4>")
        self._top.unbind("<Button-5>")

    def _on_main_mousewheel(self, event: tk.Event) -> None:
        if hasattr(event, "delta") and event.delta:
            step = -1 if event.delta > 0 else 1
            self._main_canvas.yview_scroll(step, "units")
            return
        num = getattr(event, "num", None)
        if num == 4:
            self._main_canvas.yview_scroll(-1, "units")
        elif num == 5:
            self._main_canvas.yview_scroll(1, "units")

    def _make_scrollable_popup(
        self,
        *,
        title: str,
        geometry: str,
        padding: int = 8,
        minsize: tuple[int, int] | None = None,
    ) -> tuple[tk.Toplevel, ttk.Frame]:
        """Tạo popup có canvas+scrollbar dọc để hiển thị mượt trên màn hình thấp."""
        top = tk.Toplevel(self._top)
        top.title(title)
        top.geometry(geometry)
        if minsize:
            top.minsize(*minsize)
        canvas = tk.Canvas(top, highlightthickness=0, borderwidth=0)
        vsb = ttk.Scrollbar(top, orient=tk.VERTICAL, command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        content = ttk.Frame(canvas, padding=padding)
        win_id = canvas.create_window((0, 0), window=content, anchor="nw")

        def sync(_event: tk.Event | None = None) -> None:
            content.update_idletasks()
            bbox = canvas.bbox("all")
            if bbox:
                canvas.configure(scrollregion=bbox)
            w = canvas.winfo_width()
            if w > 1:
                canvas.itemconfigure(win_id, width=w)

        def on_wheel(event: tk.Event) -> None:
            if hasattr(event, "delta") and event.delta:
                canvas.yview_scroll(-1 if event.delta > 0 else 1, "units")
                return
            num = getattr(event, "num", None)
            if num == 4:
                canvas.yview_scroll(-1, "units")
            elif num == 5:
                canvas.yview_scroll(1, "units")

        content.bind("<Configure>", sync)
        canvas.bind("<Configure>", sync)
        canvas.bind("<Enter>", lambda _e: (top.bind("<MouseWheel>", on_wheel, add="+"), top.bind("<Button-4>", on_wheel, add="+"), top.bind("<Button-5>", on_wheel, add="+")))
        canvas.bind("<Leave>", lambda _e: (top.unbind("<MouseWheel>"), top.unbind("<Button-4>"), top.unbind("<Button-5>")))
        return top, content

    # --- Block A ---
    def _build_block_a(self, root: ttk.Frame) -> None:
        fr = ttk.LabelFrame(root, text="A — Thông tin chung", padding=8)
        fr.grid(row=0, column=0, sticky="ew")
        fr.columnconfigure(1, weight=1)
        r = 0
        self._cb_acc = ttk.Combobox(fr, values=self._owner_ids or [""], state="readonly", width=40)
        if self._owner_ids:
            self._cb_acc.set(self._owner_ids[0])
        ttk.Label(fr, text="Tài khoản *").grid(row=r, column=0, sticky="nw", padx=(0, 8))
        self._cb_acc.grid(row=r, column=1, sticky="w")
        r += 1

        self._cb_page = ttk.Combobox(fr, state="readonly", width=40)
        ttk.Label(fr, text="Page *").grid(row=r, column=0, sticky="nw", padx=(0, 8))
        self._cb_page.grid(row=r, column=1, sticky="w")
        self._cb_acc.bind("<<ComboboxSelected>>", lambda _e: self._refresh_pages())
        self._cb_page.bind("<<ComboboxSelected>>", lambda _e: self._refresh_ai_image_style_choices(apply_suggestion=True))
        self._refresh_pages()
        r += 1

        kinds = [
            ("Text", "text"),
            ("Ảnh", "image"),
            ("Video", "video"),
            ("Text + Ảnh", "text_image"),
            ("Text + Video", "text_video"),
        ]
        self._kind_pairs = kinds
        self._cb_kind = ttk.Combobox(fr, values=[x[0] for x in kinds], state="readonly", width=38)
        self._cb_kind.set(kinds[0][0])
        ttk.Label(fr, text="Loại nội dung *").grid(row=r, column=0, sticky="nw", padx=(0, 8))
        self._cb_kind.grid(row=r, column=1, sticky="w")
        r += 1

        self._cb_mode = ttk.Combobox(
            fr,
            values=("Một bài thủ công", "AI sinh nhiều bài", "Từ thư mục video"),
            state="readonly",
            width=38,
        )
        self._cb_mode.set("Một bài thủ công")
        ttk.Label(fr, text="Chế độ tạo job *").grid(row=r, column=0, sticky="nw", padx=(0, 8))
        self._cb_mode.grid(row=r, column=1, sticky="w")
        self._cb_mode.bind("<<ComboboxSelected>>", lambda _e: self._on_mode_changed())
        r += 1
        self._ai_lang = ttk.Combobox(fr, values=self.AI_LANG_OPTIONS, state="readonly", width=24)
        self._ai_lang.set("Tiếng Việt")
        ttk.Label(fr, text="Ngôn ngữ AI").grid(row=r, column=0, sticky="nw", padx=(0, 8))
        self._ai_lang.grid(row=r, column=1, sticky="w")
        r += 1
        self._cb_ai_provider_text = ttk.Combobox(fr, values=("gemini", "openai"), state="readonly", width=14)
        self._cb_ai_provider_text.set(os.environ.get("AI_PROVIDER_TEXT", "gemini").strip().lower() or "gemini")
        ttk.Label(fr, text="Provider AI text").grid(row=r, column=0, sticky="nw", padx=(0, 8))
        self._cb_ai_provider_text.grid(row=r, column=1, sticky="w")
        r += 1
        self._cb_ai_provider_image = ttk.Combobox(
            fr,
            values=("gemini", "openai", "nanobanana"),
            state="readonly",
            width=14,
        )
        self._cb_ai_provider_image.set(os.environ.get("AI_PROVIDER_IMAGE", "gemini").strip().lower() or "gemini")
        ttk.Label(fr, text="Provider AI image").grid(row=r, column=0, sticky="nw", padx=(0, 8))
        self._cb_ai_provider_image.grid(row=r, column=1, sticky="w")
        self._cb_ai_provider_image.bind("<<ComboboxSelected>>", lambda _e: self._sync_image_provider_controls(source="top"))
        r += 1
        self._e_ai_model_text = ttk.Combobox(
            fr,
            values=("auto", "gpt-4o-mini", "gpt-4.1-mini", "gemini-2.5-flash"),
            width=30,
        )
        self._e_ai_model_text.set(os.environ.get("AI_MODEL_TEXT", "").strip() or "auto")
        ttk.Label(fr, text="Model text (tuỳ chọn)").grid(row=r, column=0, sticky="nw", padx=(0, 8))
        self._e_ai_model_text.grid(row=r, column=1, sticky="w")
        r += 1
        self._e_ai_model_image = ttk.Combobox(
            fr,
            values=("auto", "gpt-image-2", "gpt-image-1", "imagen-3.0-generate-002"),
            width=30,
        )
        self._e_ai_model_image.set(os.environ.get("AI_MODEL_IMAGE", "").strip() or "auto")
        ttk.Label(fr, text="Model image (tuỳ chọn)").grid(row=r, column=0, sticky="nw", padx=(0, 8))
        self._e_ai_model_image.grid(row=r, column=1, sticky="w")
        r += 1

        self._browser_vis_labels: tuple[tuple[str, str], ...] = (
            ("Theo cài đặt chung", "inherit"),
            ("Ẩn (chạy nền)", "hide"),
            ("Hiện (xem chạy)", "show"),
        )
        self._cb_browser_vis = ttk.Combobox(
            fr,
            values=[lbl for lbl, _ in self._browser_vis_labels],
            state="readonly",
            width=24,
        )
        # Mặc định ẩn browser cho batch job để tiết kiệm tài nguyên; người dùng vẫn có thể đổi sang Hiện.
        self._cb_browser_vis.set("Ẩn (chạy nền)")
        ttk.Label(fr, text="Hiển thị browser").grid(row=r, column=0, sticky="nw", padx=(0, 8))
        self._cb_browser_vis.grid(row=r, column=1, sticky="w")
        r += 1

        ttk.Label(
            fr,
            text="Đổi tài khoản → lọc Page. Đổi chế độ → panel B thay đổi. «Hiển thị browser» áp cho mọi job trong batch.",
            foreground="gray",
            font=("Segoe UI", 8),
        ).grid(row=r, column=0, columnspan=2, sticky="w")

    def _kind_key(self) -> str:
        label = self._cb_kind.get().strip()
        for lab, key in self._kind_pairs:
            if lab == label:
                return key
        return "text"

    def _browser_visibility_key(self) -> str:
        """Trả về 'inherit' | 'hide' | 'show' cho combobox «Hiển thị browser»."""
        if not hasattr(self, "_cb_browser_vis"):
            return "inherit"
        label = self._cb_browser_vis.get().strip()
        for lab, key in self._browser_vis_labels:
            if lab == label:
                return key
        return "inherit"

    def _mode_key(self) -> str:
        m = self._cb_mode.get().strip()
        if m == "AI sinh nhiều bài":
            return self.MODE_AI
        if m == "Từ thư mục video":
            return self.MODE_VIDEO
        return self.MODE_MANUAL

    def _refresh_pages(self) -> None:
        aid = self._cb_acc.get().strip()
        opts: list[str] = []
        self._page_label_to_id = {}
        self._page_id_to_label = {}
        try:
            for p in self._pages.load_all():
                if str(p.get("account_id", "")).strip() == aid:
                    pid = str(p.get("id", "")).strip()
                    if pid:
                        pname = str(p.get("page_name", "") or "").strip()
                        base = f"{pname} ({pid})" if pname else pid
                        label = base
                        suffix = 2
                        while label in self._page_label_to_id:
                            label = f"{base} [{suffix}]"
                            suffix += 1
                        self._page_label_to_id[label] = pid
                        self._page_id_to_label[pid] = label
                        opts.append(label)
        except Exception:  # noqa: BLE001
            pass
        self._cb_page.configure(values=opts or [""])
        if opts:
            cur = self._cb_page.get().strip()
            if cur in self._page_label_to_id:
                self._cb_page.set(cur)
            elif cur in self._page_id_to_label:
                self._cb_page.set(self._page_id_to_label[cur])
            else:
                self._cb_page.set(opts[0])
        else:
            self._cb_page.set("")
        if hasattr(self, "_ai_img_style"):
            self._refresh_ai_image_style_choices(apply_suggestion=True)

    def _selected_page_id(self) -> str:
        raw = self._cb_page.get().strip()
        if not raw:
            return ""
        mapped = self._page_label_to_id.get(raw)
        return str(mapped or raw).strip()

    # --- Block B: stacked panels ---
    def _build_mode_panels(self) -> None:
        self._manual_fr = ttk.LabelFrame(self._mode_panel_container, text="B — Thủ công (1 job)", padding=8)
        self._ai_fr = ttk.LabelFrame(self._mode_panel_container, text="B — AI sinh nhiều bài", padding=8)
        self._vid_fr = ttk.LabelFrame(self._mode_panel_container, text="B — Thư mục video", padding=8)

        # Manual
        self._m_title = ttk.Entry(self._manual_fr, width=56)
        self._m_body = tk.Text(self._manual_fr, height=5, width=56, wrap="word", font=("Segoe UI", 9))
        self._m_tags = ttk.Entry(self._manual_fr, width=56)
        self._m_cta = ttk.Entry(self._manual_fr, width=56)
        self._m_media_lb = tk.Listbox(self._manual_fr, height=4, width=70)
        mr = 0
        ttk.Label(self._manual_fr, text="Tiêu đề nội bộ").grid(row=mr, column=0, sticky="nw")
        self._m_title.grid(row=mr, column=1, sticky="ew")
        mr += 1
        ttk.Label(self._manual_fr, text="Nội dung").grid(row=mr, column=0, sticky="nw")
        self._m_body.grid(row=mr, column=1, sticky="ew")
        mr += 1
        ttk.Label(self._manual_fr, text="Hashtags (phẩy)").grid(row=mr, column=0, sticky="nw")
        self._m_tags.grid(row=mr, column=1, sticky="ew")
        mr += 1
        ttk.Label(self._manual_fr, text="CTA").grid(row=mr, column=0, sticky="nw")
        self._m_cta.grid(row=mr, column=1, sticky="ew")
        mr += 1
        bf = ttk.Frame(self._manual_fr)
        bf.grid(row=mr, column=1, sticky="w")
        ttk.Button(bf, text="Thêm file media…", command=self._manual_add_media).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(bf, text="Xóa media chọn", command=self._manual_remove_media).pack(side=tk.LEFT)
        mr += 1
        self._m_media_lb.grid(row=mr, column=1, sticky="ew")
        self._manual_fr.columnconfigure(1, weight=1)
        self._var_m_ai_cap = tk.BooleanVar(value=False)
        self._var_m_ai_ht = tk.BooleanVar(value=False)
        ttk.Checkbutton(self._manual_fr, text="Dùng AI viết lại caption", variable=self._var_m_ai_cap).grid(
            row=mr + 1, column=1, sticky="w"
        )
        ttk.Checkbutton(self._manual_fr, text="Dùng AI thêm hashtag", variable=self._var_m_ai_ht).grid(
            row=mr + 2, column=1, sticky="w"
        )

        # AI batch
        self._ai_idea = tk.Text(self._ai_fr, height=4, width=56, wrap="word", font=("Segoe UI", 9))
        self._ai_count = ttk.Spinbox(self._ai_fr, from_=1, to=50, width=6)
        self._ai_count.delete(0, tk.END)
        self._ai_count.insert(0, "5")
        self._ai_goal = ttk.Combobox(
            self._ai_fr,
            values=("Tăng tương tác", "Bán hàng", "Xây thương hiệu", "Kéo inbox"),
            state="readonly",
            width=40,
        )
        self._ai_goal.set("Tăng tương tác")
        self._ai_len = ttk.Combobox(self._ai_fr, values=("Ngắn", "Trung bình", "Dài"), state="readonly", width=40)
        self._ai_len.set("Trung bình")
        self._ai_style = ttk.Combobox(
            self._ai_fr,
            values=("Tự nhiên", "Chuyên gia", "Viral", "Bán hàng mềm"),
            state="readonly",
            width=40,
        )
        self._ai_style.set("Tự nhiên")
        self._var_ai_img = tk.BooleanVar(value=False)
        self._ai_img_n = ttk.Spinbox(self._ai_fr, from_=1, to=5, width=4)
        self._ai_img_n.insert(0, "1")
        self._ai_img_style_auto_label = "Auto (theo tiêu đề & nội dung)"
        self._ai_img_style_presets = [
            self._ai_img_style_auto_label,
            "Photorealistic lifestyle, natural light",
            "Flat illustration, clean vector, pastel colors",
            "Minimal modern composition, brand color focus",
            "Product-focused close-up, studio lighting",
            "Cinematic storytelling, depth of field",
            "Kawaii friendly social media illustration",
        ]
        self._ai_img_style = ttk.Combobox(self._ai_fr, values=self._ai_img_style_presets, width=42)
        self._ai_img_provider = ttk.Combobox(
            self._ai_fr,
            values=("Gemini", "OpenAI", "NanoBanana"),
            state="readonly",
            width=18,
        )
        self._ai_img_provider.set("Gemini")
        self._ai_img_provider.bind("<<ComboboxSelected>>", lambda _e: self._sync_image_provider_controls(source="ai_panel"))
        self._ai_ht_base = ttk.Entry(self._ai_fr, width=44)
        self._ai_ht_n = ttk.Spinbox(self._ai_fr, from_=0, to=30, width=4)
        self._ai_ht_n.insert(0, "5")
        self._var_use_page_ai = tk.BooleanVar(value=True)
        ar = 0
        ttk.Label(self._ai_fr, text="Ý tưởng chính *").grid(row=ar, column=0, sticky="nw")
        self._ai_idea.grid(row=ar, column=1, sticky="ew")
        ar += 1
        ttk.Label(self._ai_fr, text="Số bài").grid(row=ar, column=0, sticky="nw")
        self._ai_count.grid(row=ar, column=1, sticky="w")
        ar += 1
        ttk.Label(self._ai_fr, text="Mục tiêu").grid(row=ar, column=0, sticky="nw")
        self._ai_goal.grid(row=ar, column=1, sticky="w")
        ar += 1
        ttk.Label(self._ai_fr, text="Độ dài").grid(row=ar, column=0, sticky="nw")
        self._ai_len.grid(row=ar, column=1, sticky="w")
        ar += 1
        ttk.Label(self._ai_fr, text="Phong cách").grid(row=ar, column=0, sticky="nw")
        self._ai_style.grid(row=ar, column=1, sticky="w")
        ar += 1
        ttk.Checkbutton(
            self._ai_fr,
            text="Tự sinh ảnh AI (ưu tiên NanoBanana, fallback Imagen rồi Pollinations)",
            variable=self._var_ai_img,
        ).grid(row=ar, column=1, sticky="w")
        ar += 1
        ttk.Label(self._ai_fr, text="Phong cách ảnh (prompt cho NanoBanana/Imagen)").grid(row=ar, column=0, sticky="nw")
        self._ai_img_style.grid(row=ar, column=1, sticky="ew")
        ar += 1
        ttk.Label(self._ai_fr, text="Provider sinh ảnh").grid(row=ar, column=0, sticky="nw")
        self._ai_img_provider.grid(row=ar, column=1, sticky="w")
        ar += 1
        ttk.Label(self._ai_fr, text="Hashtag gốc (phẩy)").grid(row=ar, column=0, sticky="nw")
        self._ai_ht_base.grid(row=ar, column=1, sticky="ew")
        ar += 1
        ttk.Checkbutton(
            self._ai_fr,
            text="Dùng AI config của Page",
            variable=self._var_use_page_ai,
            command=lambda: self._refresh_ai_image_style_choices(apply_suggestion=True),
        ).grid(row=ar, column=1, sticky="w")
        ar += 1
        ttk.Button(self._ai_fr, text="Override AI config (popup)…", command=self._open_ai_override).grid(
            row=ar, column=1, sticky="w"
        )
        self._ai_fr.columnconfigure(1, weight=1)

        # Video folder
        self._v_caption_mode = ttk.Combobox(
            self._vid_fr,
            values=("Không caption", "Caption chung", "AI caption riêng từng video"),
            state="readonly",
            width=38,
        )
        self._v_caption_mode.set("Không caption")
        self._v_sort = ttk.Combobox(
            self._vid_fr,
            values=("Theo tên file", "Theo ngày sửa", "Đảo ngẫu nhiên"),
            state="readonly",
            width=38,
        )
        self._v_sort.set("Theo tên file")
        vr = 0
        ttk.Label(self._vid_fr, text="Thư mục video").grid(row=vr, column=0, sticky="nw")
        vf = ttk.Frame(self._vid_fr)
        vf.grid(row=vr, column=1, sticky="ew")
        self._v_folder = ttk.Entry(vf, width=44)
        self._v_folder.grid(row=0, column=0, sticky="ew")
        vf.columnconfigure(0, weight=1)
        ttk.Button(vf, text="Chọn…", command=self._browse_video_folder, width=8).grid(row=0, column=1, padx=(6, 0))
        vr += 1
        ff = ttk.Frame(self._vid_fr)
        ff.grid(row=vr, column=1, sticky="w", pady=(0, 2))
        self._v_ffmpeg_status_var = tk.StringVar(value="")
        self._btn_ffmpeg_check = ttk.Button(ff, text="Kiểm tra ffmpeg ngay", command=self._on_check_ffmpeg_now, width=22)
        self._btn_ffmpeg_check.pack(side=tk.LEFT)
        ttk.Label(ff, textvariable=self._v_ffmpeg_status_var, foreground="gray").pack(side=tk.LEFT, padx=(8, 0))
        self._ffmpeg_check_busy = False
        vr += 1
        ttk.Label(self._vid_fr, text="Sắp xếp").grid(row=vr, column=0, sticky="nw")
        self._v_sort.grid(row=vr, column=1, sticky="w")
        vr += 1
        ttk.Label(self._vid_fr, text="Caption").grid(row=vr, column=0, sticky="nw")
        self._v_caption_mode.grid(row=vr, column=1, sticky="w")
        self._v_caption_mode.bind("<<ComboboxSelected>>", lambda _e: self._toggle_video_caption_fields())
        vr += 1
        ttk.Label(self._vid_fr, text="Reel thumbnail (wizard Meta)").grid(row=vr, column=0, sticky="nw")
        self._v_reel_thumb = ttk.Combobox(
            self._vid_fr,
            values=("Mặc định (Meta tự chọn)", "Cách 1 — Thumbnail auto đầu tiên"),
            state="readonly",
            width=38,
        )
        self._v_reel_thumb.set("Mặc định (Meta tự chọn)")
        self._v_reel_thumb.grid(row=vr, column=1, sticky="w")
        vr += 1
        self._v_cap_fr = ttk.Frame(self._vid_fr)
        self._v_cap_fr.grid(row=vr, column=1, sticky="ew")
        self._vid_fr.columnconfigure(1, weight=1)
        cap = self._v_cap_fr
        self._v_lbl_shared_title = ttk.Label(cap, text="Tiêu đề mẫu")
        self._v_shared_title = ttk.Entry(cap, width=50)
        self._v_lbl_shared_cap = ttk.Label(cap, text="Caption mẫu")
        self._v_shared_cap = tk.Text(cap, height=3, width=50, wrap="word")
        self._v_lbl_shared_ht = ttk.Label(cap, text="Hashtag mẫu")
        self._v_shared_ht = ttk.Entry(cap, width=50)
        self._v_lbl_shared_cta = ttk.Label(cap, text="CTA mẫu")
        self._v_shared_cta = ttk.Entry(cap, width=50)
        self._v_lbl_ai_idea = ttk.Label(cap, text="Ý tưởng chung")
        self._v_ai_idea = ttk.Entry(cap, width=50)
        self._v_lbl_ai_shared_ht = ttk.Label(cap, text="Hashtag chung (phẩy)")
        self._v_ai_shared_ht = ttk.Entry(cap, width=50)
        self._v_var_ai_auto_ht = tk.BooleanVar(value=True)
        self._v_ai_auto_ht = ttk.Checkbutton(
            cap,
            text="Tự sinh hashtag theo nội dung video (AI)",
            variable=self._v_var_ai_auto_ht,
        )
        self._v_lbl_ai_ht_n = ttk.Label(cap, text="Số hashtag AI")
        self._v_ai_ht_n = ttk.Spinbox(cap, from_=1, to=12, width=6)
        self._v_ai_ht_n.delete(0, tk.END)
        self._v_ai_ht_n.insert(0, "5")
        self._v_cap_widgets_hideable = (
            self._v_lbl_shared_title,
            self._v_shared_title,
            self._v_lbl_shared_cap,
            self._v_shared_cap,
            self._v_lbl_shared_ht,
            self._v_shared_ht,
            self._v_lbl_shared_cta,
            self._v_shared_cta,
            self._v_lbl_ai_idea,
            self._v_ai_idea,
            self._v_lbl_ai_shared_ht,
            self._v_ai_shared_ht,
            self._v_ai_auto_ht,
            self._v_lbl_ai_ht_n,
            self._v_ai_ht_n,
        )
        self._build_video_caption_inner()

    def _build_video_caption_inner(self) -> None:
        for w in self._v_cap_widgets_hideable:
            w.grid_remove()
        mode = self._v_caption_mode.get()
        r = 0
        if mode == "Caption chung":
            self._v_lbl_shared_title.grid(row=r, column=0, sticky="nw")
            self._v_shared_title.grid(row=r, column=1, sticky="ew")
            r += 1
            self._v_lbl_shared_cap.grid(row=r, column=0, sticky="nw")
            self._v_shared_cap.grid(row=r, column=1, sticky="ew")
            r += 1
            self._v_lbl_shared_ht.grid(row=r, column=0, sticky="nw")
            self._v_shared_ht.grid(row=r, column=1, sticky="ew")
            r += 1
            self._v_lbl_shared_cta.grid(row=r, column=0, sticky="nw")
            self._v_shared_cta.grid(row=r, column=1, sticky="ew")
        elif mode == "AI caption riêng từng video":
            self._v_lbl_ai_idea.grid(row=r, column=0, sticky="nw")
            self._v_ai_idea.grid(row=r, column=1, sticky="ew")
            r += 1
            self._v_lbl_ai_shared_ht.grid(row=r, column=0, sticky="nw")
            self._v_ai_shared_ht.grid(row=r, column=1, sticky="ew")
            r += 1
            self._v_ai_auto_ht.grid(row=r, column=1, sticky="w")
            r += 1
            self._v_lbl_ai_ht_n.grid(row=r, column=0, sticky="nw")
            self._v_ai_ht_n.grid(row=r, column=1, sticky="w")
        self._v_cap_fr.columnconfigure(1, weight=1)

    def _ai_language_prompt(self) -> str:
        v = self._ai_lang.get().strip()
        if v:
            return v
        return "Tiếng Việt"

    def _toggle_video_caption_fields(self) -> None:
        self._build_video_caption_inner()

    def _browse_video_folder(self) -> None:
        p = filedialog.askdirectory(parent=self._top, title="Thư mục chứa video")
        if p:
            self._v_folder.delete(0, tk.END)
            self._v_folder.insert(0, p)

    def _set_ffmpeg_check_busy(self, busy: bool, msg: str = "") -> None:
        self._ffmpeg_check_busy = bool(busy)
        if not hasattr(self, "_btn_ffmpeg_check"):
            return
        try:
            self._btn_ffmpeg_check.configure(
                state=tk.DISABLED if busy else tk.NORMAL,
                text="Đang kiểm tra..." if busy else "Kiểm tra ffmpeg ngay",
            )
            self._v_ffmpeg_status_var.set(msg)
        except tk.TclError:
            return

    def _on_check_ffmpeg_now(self) -> None:
        if self._ffmpeg_check_busy:
            return
        self._set_ffmpeg_check_busy(True, "Đang kiểm tra/cài ffmpeg...")
        threading.Thread(target=self._ffmpeg_check_worker, daemon=True, name="ffmpeg_check_worker").start()

    def _ffmpeg_check_worker(self) -> None:
        ok = False
        msg = ""
        try:
            ok = _ensure_ffmpeg_available()
            ffmpeg_path, ffprobe_path = _resolve_ffmpeg_probe_paths()
            ffmpeg_path = ffmpeg_path or ""
            ffprobe_path = ffprobe_path or ""
            if ok:
                msg = "Đã sẵn sàng ffmpeg."
                detail = (
                    "ffmpeg/ffprobe đã sẵn sàng.\n\n"
                    f"ffmpeg: {ffmpeg_path or '(không rõ)'}\n"
                    f"ffprobe: {ffprobe_path or '(không rõ)'}"
                )
            else:
                msg = "Chưa có ffmpeg."
                detail = (
                    "Chưa phát hiện ffmpeg/ffprobe trong PATH.\n"
                    "Hãy cài ffmpeg (hoặc mở lại app sau khi cài) để bật cắt frame video chính xác."
                )
            self._top.after(0, lambda: self._on_ffmpeg_check_done(ok, msg, detail))
        except Exception as exc:  # noqa: BLE001
            self._top.after(0, lambda: self._on_ffmpeg_check_done(False, "Lỗi kiểm tra ffmpeg.", str(exc)))

    def _on_ffmpeg_check_done(self, ok: bool, status_msg: str, detail: str) -> None:
        self._set_ffmpeg_check_busy(False, status_msg)
        if ok:
            messagebox.showinfo("ffmpeg", detail, parent=self._top)
        else:
            messagebox.showwarning("ffmpeg", detail, parent=self._top)

    def _manual_add_media(self) -> None:
        paths = filedialog.askopenfilenames(parent=self._top, title="Chọn file media")
        for p in paths:
            self._m_media_lb.insert(tk.END, p)

    def _manual_remove_media(self) -> None:
        sel = self._m_media_lb.curselection()
        for i in reversed(sel):
            self._m_media_lb.delete(i)

    def _on_mode_changed(self) -> None:
        for fr in (self._manual_fr, self._ai_fr, self._vid_fr):
            fr.pack_forget()
        mode = self._mode_key()
        if mode == self.MODE_MANUAL:
            self._manual_fr.pack(fill=tk.BOTH, expand=True)
        elif mode == self.MODE_AI:
            self._ai_fr.pack(fill=tk.BOTH, expand=True)
        else:
            self._vid_fr.pack(fill=tk.BOTH, expand=True)
        self._apply_preview_columns_for_mode(mode)

    def _apply_preview_columns_for_mode(self, mode: str) -> None:
        if not hasattr(self, "_tree"):
            return
        if mode == self.MODE_VIDEO:
            self._tree.column("tom_tat", width=0, minwidth=0, stretch=False)
            self._tree.heading("tom_tat", text="")
            self._tree.column("prompt_anh", width=0, minwidth=0, stretch=False)
            self._tree.heading("prompt_anh", text="")
        else:
            self._tree.column("tom_tat", width=180, minwidth=80, stretch=True)
            self._tree.heading("tom_tat", text="Nội dung rút gọn")
            w = int(getattr(self, "_prompt_col_default_width", 260))
            self._tree.column("prompt_anh", width=w, minwidth=80, stretch=True)
            self._tree.heading("prompt_anh", text="Prompt ảnh (English)")

    # --- Block C: schedule ---
    def _build_block_c(self, root: ttk.Frame) -> None:
        fr = ttk.LabelFrame(root, text="C — Lịch đăng (áp dụng chuỗi job)", padding=8)
        fr.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        fr.columnconfigure(1, weight=1)
        self._sched_rule = ttk.Combobox(
            fr,
            values=("Đăng ngay", "Một lần", "Theo khung giờ mỗi ngày"),
            state="readonly",
            width=44,
        )
        self._sched_rule.set("Theo khung giờ mỗi ngày")
        self._sched_rule.bind("<<ComboboxSelected>>", lambda _e: self._on_schedule_rule_changed())
        ttk.Label(fr, text="Kiểu lịch").grid(row=0, column=0, sticky="nw", padx=(0, 8))
        self._sched_rule.grid(row=0, column=1, sticky="w")
        self._e_start_date = ttk.Entry(fr, width=14)
        self._e_start_date.insert(0, date.today().strftime("%Y-%m-%d"))
        ttk.Label(fr, text="Ngày bắt đầu (YYYY-MM-DD)").grid(row=1, column=0, sticky="nw", padx=(0, 8), pady=4)
        self._e_start_date.grid(row=1, column=1, sticky="w", pady=4)
        tf = ttk.Frame(fr)
        tf.grid(row=2, column=1, sticky="w")
        self._sched_once_time_frame = tf
        ttk.Label(tf, text="Giờ:").pack(side=tk.LEFT)
        self._sp_h = ttk.Spinbox(tf, from_=0, to=23, width=4, format="%.0f")
        self._sp_h.insert(0, "9")
        self._sp_h.pack(side=tk.LEFT, padx=4)
        ttk.Label(tf, text="Phút:").pack(side=tk.LEFT)
        self._sp_m = ttk.Spinbox(tf, from_=0, to=59, width=4, format="%.0f")
        self._sp_m.insert(0, "0")
        self._sp_m.pack(side=tk.LEFT, padx=4)
        self._lbl_once_time = ttk.Label(fr, text="Giờ/phút (cho kiểu Một lần)")
        self._lbl_once_time.grid(row=2, column=0, sticky="nw", padx=(0, 8), pady=4)
        self._lbl_legacy_interval = ttk.Label(fr, text="Khoảng cách — đơn vị")
        self._iv_unit = ttk.Combobox(fr, values=("hours", "days"), state="readonly", width=10)
        self._iv_unit.set("days")
        self._lbl_legacy_step = ttk.Label(fr, text="Bước (số giờ hoặc số ngày)")
        self._iv_step = ttk.Spinbox(fr, from_=0, to=168, width=6)
        self._iv_step.insert(0, "1")
        self._lbl_daily_slots = ttk.Label(fr, text="Khung giờ/ngày (HH:MM, phẩy)")
        self._lbl_daily_slots.grid(row=5, column=0, sticky="nw", padx=(0, 8), pady=4)
        self._e_daily_slots = ttk.Entry(fr, width=32)
        self._e_daily_slots.insert(0, "04:30,10:15,22:30")
        self._e_daily_slots.grid(row=5, column=1, sticky="w", pady=4)
        self._lbl_delay_min = ttk.Label(fr, text="Delay tối thiểu (phút)")
        self._lbl_delay_min.grid(row=6, column=0, sticky="nw", padx=(0, 8), pady=4)
        self._delay_min = ttk.Spinbox(fr, from_=0, to=180, width=6)
        self._delay_min.insert(0, "0")
        self._delay_min.grid(row=6, column=1, sticky="w", pady=4)
        self._lbl_delay_max = ttk.Label(fr, text="Delay tối đa (phút)")
        self._lbl_delay_max.grid(row=7, column=0, sticky="nw", padx=(0, 8), pady=4)
        self._delay_max = ttk.Spinbox(fr, from_=0, to=180, width=6)
        self._delay_max.insert(0, "0")
        self._delay_max.grid(row=7, column=1, sticky="w", pady=4)
        tz = scheduler_tz()
        tz_label = getattr(tz, "key", None) or "Asia/Ho_Chi_Minh"
        self._lbl_timezone = ttk.Label(fr, text="Múi giờ")
        self._lbl_timezone.grid(row=8, column=0, sticky="nw", padx=(0, 8), pady=4)
        self._e_timezone = ttk.Entry(fr, width=30)
        self._e_timezone.insert(0, str(tz_label))
        self._e_timezone.grid(row=8, column=1, sticky="w", pady=4)
        self._lbl_schedule_hint = ttk.Label(
            fr,
            text="Khuyến nghị: Theo khung giờ mỗi ngày để batch chạy đúng slot từ trên xuống dưới.",
            foreground="gray",
        )
        self._lbl_schedule_hint.grid(row=9, column=1, sticky="w")
        self._default_label_fg = self._lbl_daily_slots.cget("foreground")
        self._on_schedule_rule_changed()

    def _on_schedule_rule_changed(self) -> None:
        rule = self._schedule_rule_key()
        is_once = rule == "once"
        is_daily_slots = rule == "daily_slots"
        if is_once:
            self._lbl_once_time.grid()
            self._sched_once_time_frame.grid()
        else:
            self._lbl_once_time.grid_remove()
            self._sched_once_time_frame.grid_remove()
        if is_daily_slots:
            self._lbl_legacy_interval.grid_remove()
            self._iv_unit.grid_remove()
            self._lbl_legacy_step.grid_remove()
            self._iv_step.grid_remove()
            self._lbl_daily_slots.grid()
            self._e_daily_slots.grid()
            self._lbl_delay_min.grid()
            self._delay_min.grid()
            self._lbl_delay_max.grid()
            self._delay_max.grid()
            self._lbl_timezone.grid()
            self._e_timezone.grid()
        else:
            self._lbl_legacy_interval.grid(row=3, column=0, sticky="nw", padx=(0, 8), pady=4)
            self._iv_unit.grid(row=3, column=1, sticky="w", pady=4)
            self._lbl_legacy_step.grid(row=4, column=0, sticky="nw", padx=(0, 8))
            self._iv_step.grid(row=4, column=1, sticky="w")
            self._lbl_daily_slots.grid_remove()
            self._e_daily_slots.grid_remove()
            self._lbl_delay_min.grid_remove()
            self._delay_min.grid_remove()
            self._lbl_delay_max.grid_remove()
            self._delay_max.grid_remove()
            self._lbl_timezone.grid_remove()
            self._e_timezone.grid_remove()

    def _schedule_rule_key(self) -> Literal["immediate", "once", "daily_slots"]:
        s = self._sched_rule.get()
        if "Đăng ngay" in s:
            return "immediate"
        if "Theo khung giờ" in s:
            return "daily_slots"
        return "once"

    def _schedule_plan(self, count: int) -> list[dict[str, Any]]:
        if count < 1:
            return []
        self._clear_schedule_validation_marks()
        rule = self._schedule_rule_key()
        tz_name = self._resolved_timezone_name()
        if rule == "immediate":
            now_iso = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()
            return [{"scheduled_at": now_iso, "slot_base_local": "NOW", "delay_applied_min": 0} for _ in range(count)]
        d = parse_date_only_yyyy_mm_dd(self._e_start_date.get())
        if rule == "daily_slots":
            slots = self._parse_daily_slot_strings()
            return build_schedule_by_daily_slots(
                start_date=d,
                time_slots=slots,
                job_count=count,
                delay_min_minutes=self._delay_min_value(),
                delay_max_minutes=self._delay_max_value(),
                timezone_name=tz_name,
            )
        h = int(self._sp_h.get())
        m = int(self._sp_m.get())
        base_series = compute_scheduled_at_series(
            count,
            "once",
            start_date=d,
            hour=h,
            minute=m,
            interval_unit="days",
            interval_value=1,
            jitter_max_min=0,
        )
        slot = f"{d.strftime('%Y-%m-%d')} {h:02d}:{m:02d}"
        return [{"scheduled_at": s, "slot_base_local": slot, "delay_applied_min": 0} for s in base_series]

    def _parse_daily_slot_strings(self) -> list[str]:
        raw = self._e_daily_slots.get().strip()
        if not raw:
            self._mark_invalid(self._lbl_daily_slots)
            raise ValueError("Khung giờ/ngày không được để trống.")
        out: list[str] = []
        for token in raw.split(","):
            s = token.strip()
            if not s:
                continue
            parts = s.split(":")
            if len(parts) != 2:
                self._mark_invalid(self._lbl_daily_slots)
                raise ValueError(f"Khung giờ không hợp lệ: {s!r}. Dùng HH:MM, ví dụ 08:30")
            h = int(parts[0])
            m = int(parts[1])
            if not (0 <= h <= 23 and 0 <= m <= 59):
                self._mark_invalid(self._lbl_daily_slots)
                raise ValueError(f"Khung giờ không hợp lệ: {s!r}.")
            out.append(f"{h:02d}:{m:02d}")
        out = sorted(set(out))
        return out

    def _delay_min_value(self) -> int:
        try:
            v = int(self._delay_min.get() or "0")
        except ValueError as exc:
            self._mark_invalid(self._lbl_delay_min)
            raise ValueError("Delay tối thiểu phải là số nguyên >= 0.") from exc
        if v < 0:
            self._mark_invalid(self._lbl_delay_min)
            raise ValueError("Delay tối thiểu phải >= 0.")
        return v

    def _delay_max_value(self) -> int:
        try:
            v = int(self._delay_max.get() or "0")
        except ValueError as exc:
            self._mark_invalid(self._lbl_delay_max)
            raise ValueError("Delay tối đa phải là số nguyên >= 0.") from exc
        if v < 0:
            self._mark_invalid(self._lbl_delay_max)
            raise ValueError("Delay tối đa phải >= 0.")
        dmin = self._delay_min_value()
        if dmin > v:
            self._mark_invalid(self._lbl_delay_min)
            self._mark_invalid(self._lbl_delay_max)
            raise ValueError("Delay tối thiểu không được lớn hơn delay tối đa.")
        return v

    def _resolved_timezone_name(self) -> str:
        name = (self._e_timezone.get() or "").strip() or "Asia/Ho_Chi_Minh"
        try:
            ZoneInfo(name)
            return name
        except Exception:
            self._mark_invalid(self._lbl_timezone)
            return "Asia/Ho_Chi_Minh"

    def _mark_invalid(self, widget: ttk.Label) -> None:
        widget.configure(foreground="red")

    def _clear_schedule_validation_marks(self) -> None:
        for w in (self._lbl_daily_slots, self._lbl_delay_min, self._lbl_delay_max, self._lbl_timezone):
            w.configure(foreground=self._default_label_fg)

    # --- Block D ---
    def _build_block_d(self, root: ttk.Frame) -> None:
        fr = ttk.LabelFrame(root, text="D — Preview job", padding=8)
        fr.grid(row=3, column=0, sticky="nsew", pady=(8, 0))
        fr.rowconfigure(0, weight=1)
        fr.columnconfigure(0, weight=3)
        fr.columnconfigure(2, weight=2)
        cols = (
            "ok",
            "stt",
            "loai",
            "ngon_ngu_ai",
            "tieu_de",
            "tom_tat",
            "prompt_anh",
            "media",
            "hashtags",
            "slot_goc",
            "delay",
            "lich",
            "trang_thai",
            "loi",
        )
        self._tree = ttk.Treeview(fr, columns=cols, show="headings", height=10, selectmode="extended")
        heads = {
            "ok": "✓",
            "stt": "STT",
            "loai": "Loại",
            "ngon_ngu_ai": "Ngôn ngữ AI",
            "tieu_de": "Tiêu đề",
            "tom_tat": "Nội dung rút gọn",
            "prompt_anh": "Prompt ảnh (English)",
            "media": "Media",
            "hashtags": "Hashtag",
            "slot_goc": "Slot gốc",
            "delay": "Delay",
            "lich": "Lịch (Local)",
            "trang_thai": "Trạng thái",
            "loi": "Lỗi",
        }
        widths = (36, 40, 72, 96, 140, 180, 260, 120, 100, 140, 80, 160, 88, 80)
        for c, w in zip(cols, widths):
            self._tree.heading(c, text=heads[c])
            self._tree.column(c, width=w, stretch=c in ("tieu_de", "tom_tat", "prompt_anh", "media", "lich"))
        self._prompt_col_default_width = 260
        sy = ttk.Scrollbar(fr, orient=tk.VERTICAL, command=self._tree.yview)
        sx = ttk.Scrollbar(fr, orient=tk.HORIZONTAL, command=self._tree.xview)
        self._tree.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)
        self._tree.grid(row=0, column=0, sticky="nsew")
        sy.grid(row=0, column=1, sticky="ns")
        sx.grid(row=1, column=0, sticky="ew")
        self._tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self._apply_preview_columns_for_mode(self._mode_key())

        side = ttk.LabelFrame(fr, text="Xem nhanh dòng chọn", padding=6)
        side.grid(row=0, column=2, sticky="nsew", padx=(8, 0))
        side.columnconfigure(0, weight=1)
        side.rowconfigure(1, weight=1)
        self._sel_img_label = ttk.Label(side, text="(Chưa có ảnh)")
        self._sel_img_label.grid(row=0, column=0, sticky="n", pady=(0, 6))
        self._sel_img_obj: tk.PhotoImage | None = None
        self._sel_text = tk.Text(side, height=12, width=40, wrap="word", font=("Segoe UI", 9), state="disabled")
        self._sel_text.grid(row=1, column=0, sticky="nsew")

    def _delete_selected_preview_rows(self) -> None:
        sel = list(self._tree.selection())
        if not sel:
            messagebox.showwarning("Preview", "Chọn ít nhất một dòng để xóa.", parent=self._top)
            return
        idxs: list[int] = []
        for iid in sel:
            try:
                idx = int(str(iid)) - 1
            except (TypeError, ValueError):
                continue
            if 0 <= idx < len(self._preview_rows):
                idxs.append(idx)
        if not idxs:
            return
        for idx in sorted(set(idxs), reverse=True):
            self._preview_rows.pop(idx)
        self._render_preview()

    def _edit_selected_preview_row(self) -> None:
        row = self._selected_preview_row()
        if not row:
            messagebox.showwarning("Preview", "Chọn một dòng để sửa.", parent=self._top)
            return
        top, f = self._make_scrollable_popup(title="Sửa nhanh dòng preview", geometry="760x560", minsize=(640, 420))
        f.columnconfigure(1, weight=1)

        ttk.Label(f, text="Tiêu đề").grid(row=0, column=0, sticky="nw", padx=(0, 8), pady=2)
        e_title = ttk.Entry(f, width=64)
        e_title.insert(0, str(row.get("title", "")))
        e_title.grid(row=0, column=1, sticky="ew", pady=2)

        ttk.Label(f, text="Nội dung").grid(row=1, column=0, sticky="nw", padx=(0, 8), pady=2)
        t_body = tk.Text(f, height=12, width=64, wrap="word", font=("Segoe UI", 9))
        t_body.insert("1.0", str(row.get("content", "")))
        t_body.grid(row=1, column=1, sticky="nsew", pady=2)
        f.rowconfigure(1, weight=1)

        ttk.Label(f, text="Hashtags (phẩy)").grid(row=2, column=0, sticky="nw", padx=(0, 8), pady=2)
        e_tags = ttk.Entry(f, width=64)
        e_tags.insert(0, ", ".join(str(x) for x in (row.get("hashtags") or [])))
        e_tags.grid(row=2, column=1, sticky="ew", pady=2)

        ttk.Label(f, text="Lịch (UTC ISO)").grid(row=3, column=0, sticky="nw", padx=(0, 8), pady=2)
        e_sched = ttk.Entry(f, width=64)
        e_sched.insert(0, str(row.get("scheduled_at", "")))
        e_sched.grid(row=3, column=1, sticky="ew", pady=2)
        ttk.Label(f, text="Lịch local (YYYY-MM-DD HH:MM)").grid(row=4, column=0, sticky="nw", padx=(0, 8), pady=2)
        e_sched_local = ttk.Entry(f, width=64)
        e_sched_local.insert(0, self._utc_iso_to_local_wall(str(row.get("scheduled_at", ""))))
        e_sched_local.grid(row=4, column=1, sticky="ew", pady=2)

        def do_save() -> None:
            row["title"] = e_title.get().strip()
            row["content"] = t_body.get("1.0", tk.END).strip()
            row["hashtags"] = [x.strip() for x in e_tags.get().split(",") if x.strip()]
            try:
                local_raw = e_sched_local.get().strip()
                if local_raw:
                    row["scheduled_at"] = self._local_wall_to_utc_iso(local_raw)
                else:
                    row["scheduled_at"] = e_sched.get().strip()
            except ValueError as exc:
                messagebox.showerror("Lịch không hợp lệ", str(exc), parent=top)
                return
            self._render_preview()
            top.destroy()

        bf = ttk.Frame(f)
        bf.grid(row=5, column=1, sticky="e", pady=(8, 0))
        ttk.Button(bf, text="Hủy", command=top.destroy).pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Button(bf, text="Lưu dòng", command=do_save).pack(side=tk.RIGHT)

    def _utc_iso_to_local_wall(self, raw: str) -> str:
        s = str(raw or "").strip().replace("Z", "+00:00")
        if not s:
            return ""
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            loc = dt.astimezone(scheduler_tz())
            return loc.strftime("%Y-%m-%d %H:%M")
        except Exception:
            return ""

    def _local_wall_to_utc_iso(self, local_wall: str) -> str:
        s = str(local_wall or "").strip()
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
        except ValueError as exc:
            raise ValueError("Định dạng phải là YYYY-MM-DD HH:MM") from exc
        dt = dt.replace(tzinfo=scheduler_tz())
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

    def _render_preview(self) -> None:
        for iid in self._tree.get_children():
            self._tree.delete(iid)
        for i, row in enumerate(self._preview_rows, start=1):
            snippet = (row.get("content") or "")[:120].replace("\n", " ")
            media = ", ".join(str(x) for x in (row.get("media_files") or [])[:2])
            if len(row.get("media_files") or []) > 2:
                media += "…"
            ht = ", ".join(str(x) for x in (row.get("hashtags") or [])[:4])
            image_prompt = str(row.get("image_prompt", "")).replace("\n", " ").strip()
            if str(row.get("_mode", "")).strip() == "video":
                image_prompt = ""
            self._tree.insert(
                "",
                tk.END,
                iid=str(i),
                values=(
                    "☑" if row.get("_selected", True) else "☐",
                    i,
                    row.get("_mode", ""),
                    str(row.get("ai_language", "") or "")[:24],
                    (row.get("title") or "")[:80],
                    snippet,
                    image_prompt[:160],
                    media[:100],
                    ht[:80],
                    str(row.get("slot_base_local", ""))[:80],
                    f"+{int(row.get('delay_applied_min', 0))} phút",
                    self._utc_iso_to_local_wall(str(row.get("scheduled_at", ""))) or str(row.get("scheduled_at", "")),
                    row.get("status", "preview"),
                    row.get("error", ""),
                ),
            )
        self._render_selected_preview()

    def _selected_preview_row(self) -> dict[str, Any] | None:
        sel = self._tree.selection()
        if not sel:
            return None
        try:
            idx = int(sel[0]) - 1
        except (TypeError, ValueError):
            return None
        if 0 <= idx < len(self._preview_rows):
            return self._preview_rows[idx]
        return None

    def _on_tree_select(self, _event: tk.Event | None = None) -> None:
        # Debounce để tránh render panel bên phải quá dày khi kéo resize cửa sổ.
        if self._sel_render_after_id:
            try:
                self._top.after_cancel(self._sel_render_after_id)
            except Exception:
                pass
        self._sel_render_after_id = self._top.after(120, self._render_selected_preview)

    def _render_selected_preview(self) -> None:
        self._sel_render_after_id = None
        row = self._selected_preview_row()
        self._sel_text.configure(state="normal")
        self._sel_text.delete("1.0", tk.END)
        self._sel_img_label.configure(image="", text="(Chưa có ảnh)")
        self._sel_img_obj = None
        if not row:
            self._sel_text.insert("1.0", "Chọn một dòng trong bảng để xem chi tiết nhanh.")
            self._sel_text.configure(state="disabled")
            return
        media = row.get("media_files") or []
        first = str(media[0]).strip() if media else ""
        if first:
            p = Path(first)
            if p.is_file():
                if p.suffix.lower() in (".png", ".gif"):
                    try:
                        # Ảnh lớn decode bằng tk.PhotoImage có thể làm UI "Not Responding" khi resize/relayout.
                        if p.stat().st_size > 2 * 1024 * 1024:
                            self._sel_img_label.configure(text=f"(Ảnh lớn >2MB, bỏ preview nhanh: {p.name})")
                        else:
                            self._sel_img_obj = tk.PhotoImage(file=str(p))
                            self._sel_img_label.configure(image=self._sel_img_obj, text="")
                    except tk.TclError:
                        self._sel_img_label.configure(text=f"(Không preview được ảnh: {p.name})")
                else:
                    self._sel_img_label.configure(text=f"(Preview nhanh hỗ trợ PNG/GIF: {p.name})")
            else:
                self._sel_img_label.configure(text="(File media không tồn tại)")
        else:
            self._sel_img_label.configure(text="(Dòng này không có media)")
        lines = [
            f"Tiêu đề: {row.get('title', '')}",
            f"Ngôn ngữ AI: {row.get('ai_language', '')}",
            f"Lịch: {row.get('scheduled_at', '')}",
            f"Slot gốc: {row.get('slot_base_local', '')}",
            f"Delay: +{int(row.get('delay_applied_min', 0))} phút",
            f"Hashtags: {', '.join(row.get('hashtags') or [])}",
            f"Image prompt (EN): {'' if str(row.get('_mode','')) == 'video' else row.get('image_prompt', '')}",
            "",
            "" if str(row.get("_mode", "")).strip() == "video" else str(row.get("content", "")).strip()[:1200],
        ]
        self._sel_text.insert("1.0", "\n".join(lines))
        self._sel_text.configure(state="disabled")

    # --- Block E ---
    def _build_block_e(self, root: ttk.Frame) -> None:
        fr = ttk.Frame(root, padding=(0, 8))
        fr.grid(row=4, column=0, sticky="ew")
        self._btn_preview = ttk.Button(fr, text="Tạo preview", command=self._on_preview)
        self._btn_preview.pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(fr, text="Sắp lại lịch theo khung giờ", command=self._reslot_preview_schedule).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(fr, text="Sắp lại lịch dòng chọn", command=self._reslot_selected_preview_schedule).pack(
            side=tk.LEFT, padx=(0, 8)
        )
        ttk.Button(fr, text="Xem preview dòng chọn", command=self._open_selected_preview_dialog).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(fr, text="Sửa dòng chọn", command=self._edit_selected_preview_row).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(fr, text="Xóa dòng chọn", command=self._delete_selected_preview_rows).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(fr, text="Bỏ chọn / chọn dòng (toggle)", command=self._toggle_sel).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(fr, text="Lưu batch job", command=self._on_save).pack(side=tk.LEFT, padx=(0, 8))
        self._preview_status_var = tk.StringVar(value="")
        ttk.Label(fr, textvariable=self._preview_status_var, foreground="gray").pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(fr, text="Đóng", command=self._cancel).pack(side=tk.RIGHT)

    def _set_preview_busy(self, busy: bool, msg: str = "") -> None:
        self._preview_busy = bool(busy)
        try:
            self._btn_preview.configure(state=tk.DISABLED if busy else tk.NORMAL, text="Đang tạo preview..." if busy else "Tạo preview")
            self._preview_status_var.set(msg if busy else "")
            self._top.configure(cursor="watch" if busy else "")
        except tk.TclError:
            return

    def _reslot_preview_schedule(self) -> None:
        if not self._preview_rows:
            messagebox.showwarning("Lịch", "Chưa có preview để sắp lịch.", parent=self._top)
            return
        try:
            plans = self._schedule_plan(len(self._preview_rows))
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lịch", f"Không thể tạo lịch theo khung giờ: {exc}", parent=self._top)
            return
        for i, row in enumerate(self._preview_rows):
            if i < len(plans):
                row["scheduled_at"] = plans[i]["scheduled_at"]
                row["slot_base_local"] = plans[i].get("slot_base_local", "")
                row["delay_applied_min"] = int(plans[i].get("delay_applied_min", 0))
        self._render_preview()

    def _reslot_selected_preview_schedule(self) -> None:
        if not self._preview_rows:
            messagebox.showwarning("Lịch", "Chưa có preview để sắp lịch.", parent=self._top)
            return
        sel = list(self._tree.selection())
        if not sel:
            messagebox.showwarning("Lịch", "Hãy chọn ít nhất một dòng trong preview.", parent=self._top)
            return
        idxs: list[int] = []
        for iid in sel:
            try:
                idx = int(str(iid)) - 1
            except (TypeError, ValueError):
                continue
            if 0 <= idx < len(self._preview_rows):
                idxs.append(idx)
        idxs = sorted(set(idxs))
        if not idxs:
            messagebox.showwarning("Lịch", "Không đọc được dòng chọn hợp lệ.", parent=self._top)
            return
        try:
            plans = self._schedule_plan(len(idxs))
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lịch", f"Không thể tạo lịch theo khung giờ: {exc}", parent=self._top)
            return
        for i, idx in enumerate(idxs):
            if i < len(plans):
                self._preview_rows[idx]["scheduled_at"] = plans[i]["scheduled_at"]
                self._preview_rows[idx]["slot_base_local"] = plans[i].get("slot_base_local", "")
                self._preview_rows[idx]["delay_applied_min"] = int(plans[i].get("delay_applied_min", 0))
        self._render_preview()

    def _toggle_sel(self) -> None:
        for iid in self._tree.selection():
            idx = int(iid) - 1
            if 0 <= idx < len(self._preview_rows):
                self._preview_rows[idx]["_selected"] = not self._preview_rows[idx].get("_selected", True)
        self._render_preview()

    def _open_selected_preview_dialog(self) -> None:
        row = self._selected_preview_row()
        if not row:
            messagebox.showwarning("Preview", "Chọn một dòng trong bảng preview.", parent=self._top)
            return
        top, f = self._make_scrollable_popup(title="Preview dòng job", geometry="760x560", minsize=(640, 420))
        ttk.Label(f, text=f"Tiêu đề: {row.get('title', '')}", font=("Segoe UI", 10, "bold")).pack(anchor="w", pady=(0, 4))
        ttk.Label(f, text=f"Slot gốc: {row.get('slot_base_local', '')} | Delay: +{int(row.get('delay_applied_min', 0))} phút").pack(anchor="w")
        ttk.Label(f, text=f"Lịch: {row.get('scheduled_at', '')}").pack(anchor="w")
        ttk.Label(f, text=f"Hashtags: {', '.join(row.get('hashtags') or [])}").pack(anchor="w", pady=(0, 8))
        media = row.get("media_files") or []
        if media:
            p = Path(str(media[0]))
            if p.is_file() and p.suffix.lower() in (".png", ".gif"):
                try:
                    if p.stat().st_size > 4 * 1024 * 1024:
                        ttk.Label(f, text=f"(Ảnh lớn >4MB, bỏ preview trong popup: {p.name})").pack(anchor="w", pady=(0, 8))
                    else:
                        img_obj = tk.PhotoImage(file=str(p))
                        img_lbl = ttk.Label(f, image=img_obj)
                        img_lbl.image = img_obj
                        img_lbl.pack(anchor="center", pady=(0, 8))
                except tk.TclError:
                    ttk.Label(f, text=f"(Không render được ảnh: {p.name})").pack(anchor="w", pady=(0, 8))
            else:
                ttk.Label(f, text=f"Media: {p}").pack(anchor="w", pady=(0, 8))
        txt = tk.Text(f, wrap="word", font=("Segoe UI", 9))
        txt.pack(fill=tk.BOTH, expand=True)
        txt.insert("1.0", str(row.get("content", "")))
        txt.configure(state="disabled")

    def _open_ai_override(self) -> None:
        top, f = self._make_scrollable_popup(title="Override AI config (batch)", geometry="520x420", padding=10, minsize=(440, 320))
        fields: dict[str, tk.Text | ttk.Entry] = {}
        r = 0
        for label, key, multiline in (
            ("brand_voice", "brand_voice", True),
            ("target_audience", "target_audience", False),
            ("content_pillars (phẩy)", "content_pillars", False),
            ("avoid_keywords (phẩy)", "avoid_keywords", False),
            ("image_style", "image_style", False),
        ):
            ttk.Label(f, text=label).grid(row=r, column=0, sticky="nw")
            if multiline:
                w = tk.Text(f, height=3, width=50, wrap="word")
            else:
                w = ttk.Entry(f, width=52)
            w.grid(row=r, column=1, sticky="ew", pady=2)
            prev = (self._batch_ai_override or {}).get(key, "")
            if isinstance(w, tk.Text):
                w.insert("1.0", str(prev))
            else:
                w.insert(0, str(prev))
            fields[key] = w
            r += 1
        f.columnconfigure(1, weight=1)

        def ok() -> None:
            out: dict[str, Any] = {}
            for key, w in fields.items():
                if isinstance(w, tk.Text):
                    v = w.get("1.0", tk.END).strip()
                else:
                    v = w.get().strip()
                if v:
                    if key in ("content_pillars", "avoid_keywords"):
                        out[key] = _split_comma(v)
                    else:
                        out[key] = v
            self._batch_ai_override = out or None
            self._refresh_ai_image_style_choices(apply_suggestion=True)
            top.destroy()

        ttk.Button(f, text="OK", command=ok).grid(row=r, column=1, sticky="e", pady=8)

    def _merged_ai_config(self, page_id: str) -> dict[str, Any] | None:
        cfg: dict[str, Any] = {}
        if self._var_use_page_ai.get():
            try:
                base = load_page_ai_config(page_id)
                for k in (
                    "brand_voice",
                    "target_audience",
                    "content_pillars",
                    "hashtags",
                    "image_style",
                    "avoid_keywords",
                    "auto_generate_image",
                    "auto_generate_caption",
                ):
                    if k in base and base[k]:
                        cfg[k] = base[k]
            except Exception as exc:  # noqa: BLE001
                logger.warning("Không load page_ai_config: {}", exc)
        if self._batch_ai_override:
            cfg.update(self._batch_ai_override)
        return cfg or None

    def _refresh_ai_image_style_choices(self, *, apply_suggestion: bool = False) -> None:
        values = list(self._ai_img_style_presets)
        pid = self._selected_page_id()
        cfg_style = ""
        if pid:
            cfg = self._merged_ai_config(pid)
            if isinstance(cfg, dict):
                cfg_style = str(cfg.get("image_style", "")).strip()
        if cfg_style and cfg_style not in values:
            values.insert(0, cfg_style)
        self._ai_img_style.configure(values=values)
        cur = self._ai_img_style.get().strip()
        if apply_suggestion and cfg_style and not cur:
            self._ai_img_style.set(cfg_style)

    def _style_for_ai(self) -> str:
        g = self._ai_goal.get()
        ln = self._ai_len.get()
        st = self._ai_style.get()
        lang = self._ai_language_prompt()
        return (
            f"{st}. Mục tiêu: {g}. Độ dài: {ln}. "
            f"Viết hoàn toàn bằng ngôn ngữ {lang}. Không trộn ngôn ngữ khác."
        )

    def _text_provider_key(self) -> str:
        p = self._cb_ai_provider_text.get().strip().lower()
        return "openai" if p == "openai" else "gemini"

    def _text_model_name(self) -> str | None:
        v = self._e_ai_model_text.get().strip()
        if not v or v.lower() == "auto":
            return None
        return v

    def _image_model_name(self) -> str | None:
        v = self._e_ai_model_image.get().strip()
        if not v or v.lower() == "auto":
            return None
        return v

    def _image_provider_key(self) -> str:
        if hasattr(self, "_cb_ai_provider_image"):
            top = self._cb_ai_provider_image.get().strip().lower()
            if top in {"gemini", "openai", "nanobanana"}:
                return top
        m = self._ai_img_provider.get().strip().lower()
        if "nanobanana" in m or "nano banana" in m:
            return "nanobanana"
        if "openai" in m:
            return "openai"
        return "gemini"

    def _image_model_choices_for_provider(self, provider: str) -> tuple[str, ...]:
        p = str(provider or "").strip().lower()
        if p == "openai":
            return ("auto", "gpt-image-2", "gpt-image-1")
        if p == "nanobanana":
            return ("auto", "nano-banana-pro")
        return ("auto", "imagen-3.0-generate-002", "imagen-4.0-generate-preview")

    def _sync_image_provider_controls(self, *, source: str) -> None:
        """
        Đồng bộ provider ảnh giữa block A và panel AI + cập nhật gợi ý model theo provider.
        """
        if source == "ai_panel":
            p = self._image_provider_key()
            self._cb_ai_provider_image.set(p)
        else:
            p = self._cb_ai_provider_image.get().strip().lower() or "gemini"
            if p == "openai":
                self._ai_img_provider.set("OpenAI")
            elif p == "nanobanana":
                self._ai_img_provider.set("NanoBanana")
            else:
                self._ai_img_provider.set("Gemini")
        choices = self._image_model_choices_for_provider(p)
        self._e_ai_model_image.configure(values=choices)
        cur = self._e_ai_model_image.get().strip()
        if not cur or cur.lower() == "auto":
            self._e_ai_model_image.set("auto")
            return
        if cur not in choices:
            # Giữ model custom nhưng đưa lên đầu để người dùng vẫn thấy giá trị hiện tại.
            self._e_ai_model_image.configure(values=(cur, *choices))

    def _on_preview(self) -> None:
        if self._preview_busy:
            return
        aid = self._cb_acc.get().strip()
        pid = self._selected_page_id()
        if not aid or not pid:
            messagebox.showerror("Thiếu", "Chọn tài khoản và Page.", parent=self._top)
            return
        mode = self._mode_key()
        self._set_preview_busy(True, "Đang tạo preview nền...")
        threading.Thread(
            target=self._preview_worker,
            args=(mode,),
            daemon=True,
            name="batch_preview_builder",
        ).start()

    def _preview_worker(self, mode: str) -> None:
        rows: list[dict[str, Any]] = []
        err_title = ""
        err_msg = ""
        try:
            if mode == self.MODE_MANUAL:
                rows = self._build_manual_preview()
            elif mode == self.MODE_AI:
                rows = self._build_ai_preview()
            else:
                rows = self._build_video_preview()
        except ValueError as exc:
            err_title = "Lỗi"
            err_msg = str(exc)
        except RuntimeError as exc:
            err_title = "AI"
            err_msg = str(exc)
        except Exception as exc:  # noqa: BLE001
            err_title = "Preview"
            err_msg = str(exc)
        try:
            self._top.after(0, lambda: self._on_preview_done(rows, err_title, err_msg))
        except tk.TclError:
            return

    def _on_preview_done(self, rows: list[dict[str, Any]], err_title: str, err_msg: str) -> None:
        self._set_preview_busy(False)
        if err_msg:
            messagebox.showerror(err_title, err_msg, parent=self._top)
            return
        self._preview_rows = rows
        self._render_preview()
        messagebox.showinfo("Preview", f"Đã tạo {len(self._preview_rows)} dòng preview.", parent=self._top)

    def _build_manual_preview(self) -> list[dict[str, Any]]:
        k = self._kind_key()
        pt = post_type_for_kind(k)
        title = self._m_title.get().strip()
        body = self._m_body.get("1.0", tk.END).strip()
        tags = _split_comma(self._m_tags.get())
        cta = self._m_cta.get().strip()
        media = [self._m_media_lb.get(i) for i in range(self._m_media_lb.size())]
        image_prompt = build_imagen_prompt_from_post(title=title, body=body, image_style="")
        if pt in ("text", "text_image", "text_video") and not body and not self._var_m_ai_cap.get():
            if "text" in pt:
                raise ValueError("Bài text cần nội dung hoặc bật AI caption.")
        if pt in ("image", "text_image") and not media:
            raise ValueError("Cần ít nhất một ảnh.")
        if pt in ("video", "text_video") and not media:
            raise ValueError("Cần ít nhất một video.")
        if self._var_m_ai_cap.get() and body:
            body = self._ai_rewrite_body(body)
        if self._var_m_ai_ht.get():
            lang = self._ai_language_prompt()
            ai_tags = self._ai_text_service.generate_hashtags(
                title=title or "Hashtag",
                body=body,
                language=lang,
                count=8,
                provider=self._text_provider_key(),
                model=self._text_model_name(),
            )
            tags = list(dict.fromkeys(tags + ai_tags))
        plans = self._schedule_plan(1)
        ai_lang = self._ai_language_prompt()
        return [
            {
                "_mode": "manual",
                "_selected": True,
                "job_type": pt,
                "title": title,
                "content": body,
                "hashtags": tags,
                "cta": cta,
                "media_files": media,
                "image_prompt": image_prompt,
                "scheduled_at": plans[0]["scheduled_at"],
                "slot_base_local": plans[0].get("slot_base_local", ""),
                "delay_applied_min": int(plans[0].get("delay_applied_min", 0)),
                "status": "preview_ready",
                "error": "",
                "ai_language": ai_lang if (self._var_m_ai_cap.get() or self._var_m_ai_ht.get()) else "",
                "ai_provider_text": self._text_provider_key(),
                "ai_provider_image": self._image_provider_key(),
                "ai_model_text": self._text_model_name() or "",
                "ai_model_image": self._image_model_name() or "",
            }
        ]

    def _ai_rewrite_body(self, body: str) -> str:
        lang = self._ai_language_prompt()
        g = self._ai_text_service.generate_post(
            topic=body[:500],
            style="Viết lại súc tích, giữ ý chính.",
            language=lang,
            provider=self._text_provider_key(),
            model=self._text_model_name(),
        )
        return g["body"]

    def _build_ai_preview(self) -> list[dict[str, Any]]:
        idea = self._ai_idea.get("1.0", tk.END).strip()
        if not idea:
            raise ValueError("Nhập ý tưởng chính.")
        n = int(self._ai_count.get())
        if n < 1:
            raise ValueError("Số bài phải ≥ 1.")
        pid = self._selected_page_id()
        topics = self._ai_text_service.generate_topics(
            idea=idea,
            count=n,
            goal=self._ai_goal.get(),
            length_hint=self._ai_len.get(),
            provider=self._text_provider_key(),
            model=self._text_model_name(),
        )
        style = self._style_for_ai()
        lang = self._ai_language_prompt()
        ai_cfg = self._merged_ai_config(pid)
        rows: list[dict[str, Any]] = []
        has_key = True
        want_img = bool(self._var_ai_img.get())
        img_provider = self._image_provider_key()
        try:
            n_img = max(1, min(4, int(float(self._ai_img_n.get()))))
        except (TypeError, ValueError):
            n_img = 1
        raw_img_style = self._ai_img_style.get().strip()
        auto_img_style = raw_img_style.lower().startswith("auto")
        img_style_hint = "" if auto_img_style else raw_img_style
        if want_img and not auto_img_style:
            cfg_style = ""
            if isinstance(ai_cfg, dict):
                cfg_style = str(ai_cfg.get("image_style", "")).strip()
            img_style_hint = (img_style_hint or cfg_style).strip()
            if not img_style_hint:
                raise ValueError(
                    "Nhập «Phong cách ảnh», chọn «Auto (theo tiêu đề & nội dung)» hoặc cấu hình image_style trong AI Page / override."
                )
        base_kind_pt = post_type_for_kind(self._kind_key())
        plans = self._schedule_plan(len(topics))
        base_ht = _split_comma(self._ai_ht_base.get())

        from src.ai.image_generation import suggest_image_style_from_post

        for i, t in enumerate(topics):
            image_alt = ""
            image_prompt = ""
            media: list[str] = []
            row_status = "preview_ready"
            row_error = ""
            if has_key:
                g = self._ai_text_service.generate_post(
                    topic=t,
                    style=style,
                    language=lang,
                    provider=self._text_provider_key(),
                    model=self._text_model_name(),
                )
                body = g["body"]
                image_alt = str(g.get("image_alt", "")).strip()
                cand_title = _title_from_body_first_sentence(body)
                title = cand_title if _title_matches_language(cand_title, lang) else ""
            row_img_style = img_style_hint
            if want_img and auto_img_style:
                # Auto: suy ra style riêng cho từng bài từ tiêu đề + nội dung.
                if has_key:
                    try:
                        suggested = suggest_image_style_from_post(
                            title=title,
                            body=(image_alt or body),
                            language_hint="English",
                        ).strip()
                    except Exception as exc:  # noqa: BLE001
                        suggested = ""
                        logger.warning("Row {}: suggest_image_style lỗi, fallback default. {}", i + 1, exc)
                    row_img_style = suggested
                    logger.info(
                        "Row {}: Auto style → {!r}",
                        i + 1,
                        row_img_style or "(default)",
                    )
                else:
                    row_img_style = ""
            image_prompt = build_imagen_prompt_from_post(
                title=title,
                body=(image_alt or body),
                image_style=row_img_style,
            )
            row_pt = base_kind_pt
            if want_img and has_key:
                row_pt = "text_image" if base_kind_pt in ("text", "text_image") else base_kind_pt
                if row_pt in ("image", "text_image"):
                    stem = f"ai_{uuid.uuid4().hex[:10]}_{i}"
                    try:
                        paths = self._ai_image_service.generate_and_save_for_batch(
                            page_id=pid,
                            file_stem=stem,
                            title=title,
                            body=body,
                            image_style=row_img_style,
                            image_prompt=image_prompt,
                            number_of_images=n_img,
                            provider=img_provider,
                            model=self._image_model_name(),
                        )
                        media = [str(p) for p in paths]
                    except Exception as exc:  # noqa: BLE001
                        msg = str(exc)[:220]
                        if base_kind_pt in ("text", "text_image"):
                            # Fallback mềm: vẫn tạo job dạng text nếu ảnh lỗi
                            row_pt = "text"
                            row_status = "preview_no_image"
                            logger.warning("Row {}: sinh ảnh lỗi provider={}, fallback text. {}", i + 1, img_provider, msg)
                        else:
                            row_status = "preview_error"
                            row_error = f"Không sinh được ảnh: {msg}"
                else:
                    raise ValueError("Loại nội dung hiện tại không hỗ trợ đính ảnh (chọn Text+Ảnh hoặc Ảnh).")

            rows.append(
                {
                    "_mode": "ai",
                    "_selected": True,
                    "job_type": row_pt,
                    "title": title,
                    "content": body,
                    "hashtags": list(base_ht),
                    "cta": "",
                    "media_files": media,
                    "image_alt": image_alt,
                    "image_prompt": image_prompt,
                    "scheduled_at": plans[i]["scheduled_at"],
                    "slot_base_local": plans[i].get("slot_base_local", ""),
                    "delay_applied_min": int(plans[i].get("delay_applied_min", 0)),
                    "status": row_status,
                    "error": row_error,
                    "ai_topic": t,
                    "ai_content_style": style,
                    "ai_language": lang,
                    "ai_config": ai_cfg or {},
                    "ai_provider_text": self._text_provider_key(),
                    "ai_provider_image": self._image_provider_key(),
                    "ai_model_text": self._text_model_name() or "",
                    "ai_model_image": self._image_model_name() or "",
                }
            )
        return rows

    def _sort_key(self) -> str:
        s = self._v_sort.get()
        if "ngày sửa" in s:
            return "mtime"
        if "ngẫu nhiên" in s.lower():
            return "random"
        return "name"

    def _build_video_preview(self) -> list[dict[str, Any]]:
        folder = Path(self._v_folder.get().strip())
        if not folder.is_dir():
            raise ValueError("Thư mục video không hợp lệ.")
        vids = scan_video_files(folder, sort=self._sort_key())
        if not vids:
            raise ValueError("Không tìm thấy file video (.mp4 .mov .avi .mkv .webm).")
        mode = self._v_caption_mode.get()
        rows: list[dict[str, Any]] = []
        plans = self._schedule_plan(len(vids))
        pt = post_type_for_kind(self._kind_key())
        if "video" not in pt:
            pt = "video"
        for i, vp in enumerate(vids):
            title = vp.stem
            body = ""
            ht: list[str] = []
            cta = ""
            if mode == "Caption chung":
                # Nếu để trống tiêu đề mẫu thì giữ trống (không fallback tên file).
                title = self._v_shared_title.get().strip()
                body = self._v_shared_cap.get("1.0", tk.END).strip()
                ht = _split_comma(self._v_shared_ht.get())
                cta = self._v_shared_cta.get().strip()
            elif mode == "AI caption riêng từng video":
                idea = self._v_ai_idea.get().strip()
                lang = self._ai_language_prompt()
                meta = _scan_video_metadata(vp)
                frame_paths: list[Path] = []
                ai_generated_text = ""
                ht = _split_comma(self._v_ai_shared_ht.get())
                if True:
                    topic = f"{idea} — video: {vp.name}"
                    vctx = _video_context_for_prompt(vp, meta)
                    try:
                        # Ưu tiên đọc frame video thật (2s/5s/8s… hoặc 10-20s với video dài) để sinh tiêu đề đúng nội dung.
                        frame_paths = _extract_video_frames_for_ai(vp, meta)
                        title_from_frames = _generate_video_title_from_frames(
                            vp=vp,
                            meta=meta,
                            frame_paths=frame_paths,
                            language=lang,
                            idea=idea,
                            provider=self._text_provider_key(),
                            model=self._text_model_name(),
                        )
                        if title_from_frames and _title_matches_language(title_from_frames, lang):
                            title = _normalize_short_video_title(title_from_frames)
                        else:
                            # Fallback text-only khi thiếu frame/vision.
                            g = self._ai_text_service.generate_post(
                                topic=topic,
                                style="Only return ONE short video title sentence, around 6-12 words. No hashtags. No emoji.",
                                language=lang,
                                provider=self._text_provider_key(),
                                model=self._text_model_name(),
                            )
                            ai_generated_text = str(g.get("body", "")).strip()
                            cand_title = _normalize_short_video_title(_title_from_body_first_sentence(ai_generated_text))
                            if _title_matches_language(cand_title, lang):
                                title = cand_title
                            else:
                                title = cand_title
                        if not title:
                            # Fallback cuối: vẫn ép model tạo title theo ngôn ngữ đã chọn, không dùng tên file.
                            g2 = self._ai_text_service.generate_post(
                                topic=vctx,
                                style="Return ONE short natural video title 6-12 words, no hashtags, no emoji.",
                                language=lang,
                                provider=self._text_provider_key(),
                                model=self._text_model_name(),
                            )
                            t2 = _normalize_short_video_title(_title_from_body_first_sentence(str(g2.get("body", "") or "")))
                            if _title_matches_language(t2, lang):
                                title = t2
                        if self._v_var_ai_auto_ht.get():
                            try:
                                n_ht = max(1, min(12, int(float(self._v_ai_ht_n.get()))))
                            except (TypeError, ValueError):
                                n_ht = 5
                            auto_ht = _generate_video_hashtags(
                                language=lang,
                                idea=idea,
                                title=title,
                                video_context=vctx,
                                count=n_ht,
                                provider=self._text_provider_key(),
                                model=self._text_model_name(),
                            )
                            if auto_ht:
                                ht = list(dict.fromkeys(ht + auto_ht))
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("AI caption video lỗi, fallback AI text-only: {}", exc)
                        try:
                            g3 = self._ai_text_service.generate_post(
                                topic=_video_context_for_prompt(vp, meta),
                                style="Return ONE short video title 6-12 words. No hashtag, no emoji.",
                                language=lang,
                                provider=self._text_provider_key(),
                                model=self._text_model_name(),
                            )
                            t3 = _normalize_short_video_title(_title_from_body_first_sentence(str(g3.get("body", "") or "")))
                            title = t3 if _title_matches_language(t3, lang) else ""
                        except Exception:
                            title = ""
                    finally:
                        # Dọn frame tạm sau khi AI đọc xong để tránh đầy ổ đĩa.
                        for fp in frame_paths:
                            try:
                                fp.unlink(missing_ok=True)
                            except Exception:
                                pass
                        if frame_paths:
                            try:
                                frame_paths[0].parent.rmdir()
                            except Exception:
                                pass
                else:
                    # Không có API key: fallback cuối cùng mới dùng tên file.
                    title = _normalize_short_video_title(vp.stem)
                # Theo yêu cầu: chế độ video + AI riêng chỉ sinh tiêu đề, không sinh content.
                body = ""
            rows.append(
                {
                    "_mode": "video",
                    "_selected": True,
                    "job_type": pt,
                    "title": title,
                    "content": body,
                    "hashtags": ht,
                    "cta": cta,
                    "media_files": [str(vp.resolve())],
                    # Chế độ thư mục video: không tạo prompt ảnh English.
                    "image_prompt": "",
                    "scheduled_at": plans[i]["scheduled_at"],
                    "slot_base_local": plans[i].get("slot_base_local", ""),
                    "delay_applied_min": int(plans[i].get("delay_applied_min", 0)),
                    "status": "preview_ready",
                    "error": "",
                    "ai_language": lang if mode == "AI caption riêng từng video" else "",
                    "ai_provider_text": self._text_provider_key(),
                    "ai_provider_image": self._image_provider_key(),
                    "ai_model_text": self._text_model_name() or "",
                    "ai_model_image": self._image_model_name() or "",
                }
            )
        return rows

    def _on_save(self) -> None:
        if not self._preview_rows:
            messagebox.showwarning("Preview", "Chưa có preview — bấm «Tạo preview».", parent=self._top)
            return
        aid = self._cb_acc.get().strip()
        pid = self._selected_page_id()
        pt = post_type_for_kind(self._kind_key())
        ok_rows = [
            r
            for r in self._preview_rows
            if r.get("_selected", True) and not r.get("error") and r.get("status") != "preview_error"
        ]
        if not ok_rows:
            messagebox.showerror("Lỗi", "Không có dòng hợp lệ để lưu (kiểm tra lỗi / bỏ chọn sai).", parent=self._top)
            return
        daily_meta: dict[str, Any] = {}
        rule_save = self._schedule_rule_key()
        if rule_save == "daily_slots":
            try:
                self._clear_schedule_validation_marks()
                slots_joined = ",".join(self._parse_daily_slot_strings())
                daily_meta = {
                    "schedule_daily_slots": slots_joined,
                    "schedule_delay_min": self._delay_min_value(),
                    "schedule_delay_max": self._delay_max_value(),
                    "schedule_start_date": self._e_start_date.get().strip(),
                    "timezone": self._resolved_timezone_name(),
                }
            except ValueError as exc:
                messagebox.showerror("Lịch", str(exc), parent=self._top)
                return
        hide_browser_mode = self._browser_visibility_key()
        n = 0
        for r in ok_rows:
            jpt = str(r.get("job_type", pt))
            pps = page_post_style_for_post_type(jpt)
            job = preview_row_to_schedule_job(
                r,
                account_id=aid,
                page_id=pid,
                post_type=jpt,
                page_post_style=pps,
                schedule_recurrence="",
                schedule_slot="",
            )
            if daily_meta:
                job.update(daily_meta)
                job.pop("schedule_delay_applied_min", None)
            if hide_browser_mode and hide_browser_mode != "inherit":
                job["hide_browser"] = hide_browser_mode
            if jpt in ("video", "text_video") and "Cách 1" in self._v_reel_thumb.get():
                job["reel_thumbnail_choice"] = REEL_THUMBNAIL_METHOD1_FIRST_AUTO
            try:
                self._store.upsert(job)  # type: ignore[arg-type]
                n += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning("Bỏ qua job lỗi validate: {}", exc)
        self.saved_count = n
        messagebox.showinfo("Đã lưu", f"Đã lưu {n} job vào schedule_posts.json.", parent=self._top)
        self._top.grab_release()
        self._top.destroy()

    def _cancel(self) -> None:
        try:
            self._top.grab_release()
        except tk.TclError:
            pass
        self._top.destroy()
