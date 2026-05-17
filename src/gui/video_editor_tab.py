"""
Tab Video Editor — Media / Preview / Timeline / Inspector / Export (MVP + Phase 2 tùy chọn).
"""

from __future__ import annotations

import copy
import gc
import hashlib
import json
import os
import sys
import queue
import re
import shutil
import subprocess
import threading
import time
import tkinter as tk
import tkinter.font as tkfont
import uuid
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk
from typing import Any, Callable

from loguru import logger

from src.services.video_editor.overlay_utils import compute_logo_overlay_dimensions
from src.services.video_editor.timeline_manager import (
    audio_source_bounds_for_timeline,
    timeline_duration_from_source,
)
from src.services.video_editor import (
    AudioExtractor,
    AudioMixManager,
    FFmpegCommandBuilder,
    MediaManager,
    RenderWorker,
    SubtitleManager,
    TemplateManager,
    TimelineManager,
    TransitionManager,
    VideoEditorProjectManager,
    VideoFilterManager,
    WaveformGenerator,
    ensure_video_editor_layout,
    video_editor_export_ui_prefs_path,
    merge_phase2_defaults,
    validate_export,
    video_editor_schedule_jobs_json_path,
)
from src.services.video_editor.keyframe_animation_manager import KeyframeAnimationManager
from src.services.video_editor.remote_stock_audio import (
    FREE_AUDIO_TOPIC_QUERIES,
    download_hit_to_stock,
    gather_background_fill_hits,
    load_remote_audio_config,
    save_remote_audio_config,
    search_commons_audio,
    search_freesound,
    search_jamendo,
    search_openverse,
    take_next_background_fill_topic,
)
from src.services.video_editor.stock_audio_library import list_stock_audio_paths, stock_audio_dir_display_hint
from src.services.video_editor.stock_audio_library import (
    STOCK_TOPIC_FILTER_ALL,
    filter_stock_paths_by_topic,
    stock_topic_filter_labels,
)
from src.services.universal_video_downloader import (
    DownloadMetadataStore,
    _extract_hashtags_from_text,
    ensure_downloader_layout,
)
from src.utils.ffmpeg_paths import (
    ffplay_resolve_skips_ensure_heavy_work,
    resolve_ffmpeg_executable,
    resolve_ffplay_executable,
)
from src.utils.schedule_job_content import internal_post_title_from_body

# Preset W×H cho Crop / Scale (nhãn, (rộng, cao) hoặc None = nhập tay)
RES_WH_PRESETS: tuple[tuple[str, tuple[int, int] | None], ...] = (
    ("Tuỳ chỉnh (nhập tay)", None),
    ("1080 × 1920 (9:16 dọc)", (1080, 1920)),
    ("1920 × 1080 (16:9 ngang)", (1920, 1080)),
    ("1080 × 1080 (1:1)", (1080, 1080)),
    ("720 × 1280 (9:16 HD)", (720, 1280)),
    ("1080 × 1350 (4:5 Feed)", (1080, 1350)),
    ("3840 × 2160 (4K 16:9)", (3840, 2160)),
)

# Hàng loạt trên timeline: bỏ preview tự động khi số clip vượt ngưỡng (tránh tải FFmpeg/UI).
# Sau batch lớn, tự render preview nháp rất nặng — giới hạn để tránh spike RAM/CPU.
VE_BATCH_AUTO_PREVIEW_MAX_CLIPS = 120
# Từ ngưỡng này hiển thị tiến trình trên thanh trạng thái khi «Áp dụng thay đổi» / áp nhanh tỉ lệ.
VE_BATCH_SHOW_PROGRESS_MIN_CLIPS = 50
# Debounce ô lọc tên timeline / nhận diện chỉnh sửa (ms) — giảm rebuild tree khi gõ.
_VE_TIMELINE_FILTER_DEBOUNCE_MS = 220


def _match_res_wh_label(width: int, height: int) -> str:
    for lab, dims in RES_WH_PRESETS:
        if dims is None:
            continue
        if dims[0] == width and dims[1] == height:
            return lab
    return RES_WH_PRESETS[0][0]


def _pack_scrollable_vertical(parent: ttk.Widget) -> ttk.Frame:
    """
    Đặt Canvas + thanh cuộn dọc; trả về frame bên trong để grid/pack nội dung.
    Chuột nằm trên vùng cuộn (canvas / viền nội dung) có thể lăn; luôn dùng được thanh bên phải.
    """
    outer = ttk.Frame(parent)
    outer.pack(fill=tk.BOTH, expand=True)
    canvas = tk.Canvas(outer, highlightthickness=0, borderwidth=0)
    vsb = ttk.Scrollbar(outer, orient=tk.VERTICAL, command=canvas.yview)
    canvas.configure(yscrollcommand=vsb.set)
    inner = ttk.Frame(canvas)
    inner.columnconfigure(0, weight=1)
    win = canvas.create_window((0, 0), window=inner, anchor="nw")
    _canvas_width_cbs: list[Callable[[int], None]] = []

    def _register_canvas_width_cb(cb: Callable[[int], None]) -> None:
        """Gọi lại mỗi khi canvas đổi rộng (để cập nhật wraplength nhãn dài)."""
        _canvas_width_cbs.append(cb)
        try:
            cw = int(canvas.winfo_width())
        except tk.TclError:
            return
        if cw > 2:
            try:
                cb(cw)
            except tk.TclError:
                pass

    setattr(inner, "_ve_on_canvas_width", _register_canvas_width_cb)

    def _sync_region(_e: Any = None) -> None:
        canvas.update_idletasks()
        b = canvas.bbox("all")
        if b:
            canvas.configure(scrollregion=b)

    def _fill_width(e: tk.Event) -> None:
        try:
            w = int(e.width)
            if w > 1:
                canvas.itemconfigure(win, width=w)
                for cb in list(_canvas_width_cbs):
                    try:
                        cb(w)
                    except tk.TclError:
                        pass
                inner.update_idletasks()
                _sync_region()
        except (tk.TclError, ValueError):
            pass

    inner.bind("<Configure>", lambda _e: _sync_region())
    canvas.bind("<Configure>", _fill_width)

    def _wheel(e: tk.Event) -> None:
        d = getattr(e, "delta", 0) or 0
        if d:
            canvas.yview_scroll(int(-1 * (int(d) / 120)), "units")
            return
        n = getattr(e, "num", None)
        if n == 4:
            canvas.yview_scroll(-3, "units")
        elif n == 5:
            canvas.yview_scroll(3, "units")

    canvas.bind("<MouseWheel>", _wheel)
    canvas.bind("<Button-4>", _wheel)
    canvas.bind("<Button-5>", _wheel)
    inner.bind("<MouseWheel>", _wheel)
    inner.bind("<Button-4>", _wheel)
    inner.bind("<Button-5>", _wheel)

    canvas.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    outer.columnconfigure(0, weight=1)
    outer.rowconfigure(0, weight=1)
    return inner


def _ve_tree_sort_toggle(state: dict[str, Any], col: str) -> None:
    """Đổi cột / đảo chiều sort khi bấm lại cùng cột."""
    cur = str(state.get("col") or "")
    if cur == col:
        state["asc"] = not bool(state.get("asc", True))
    else:
        state["col"] = col
        state["asc"] = True


def _ve_apply_tree_heading_marks(
    tv: ttk.Treeview,
    columns: tuple[str, ...],
    base_labels: dict[str, str],
    sort_col: str,
    asc: bool,
) -> None:
    """Hiển thị mũi tên ↑/↓ trên cột đang sort."""
    for c in columns:
        base = str(base_labels.get(c, c))
        if c == sort_col:
            base += " ↑" if asc else " ↓"
        try:
            tv.heading(c, text=base)
        except tk.TclError:
            pass


def _normalize_post_caption_title(title: str, *, description: str = "") -> str:
    """
    Facebook/Reels đôi khi ghép thống kê vào title: «706K views · 25K reactions | caption thật».
    Tiêu đề đăng bài cần là phần caption (sau dấu |), không giữ khối views/reactions.
    """
    raw = str(title or "").strip()
    desc = str(description or "").strip()
    if not raw and desc:
        raw = desc

    pat_stats_bar = re.compile(
        r"^\s*[\d.,]+(?:\s*[KkMmBb])?\s*views\s*[·•]\s*[\d.,]+(?:\s*[KkMmBb])?\s*"
        r"(?:reactions|likes|comments)\s*\|\s*",
        re.IGNORECASE,
    )
    pat_views_pipe = re.compile(
        r"^\s*[\d.,]+(?:\s*[KkMmBb])?\s*views\s*\|\s*",
        re.IGNORECASE,
    )

    def _strip_leading_stats(s: str) -> str:
        if not s:
            return s
        s2 = pat_stats_bar.sub("", s, count=1).strip()
        if s2 != s:
            return s2
        return pat_views_pipe.sub("", s, count=1).strip()

    cleaned = _strip_leading_stats(raw)
    if "|" in cleaned:
        left, _, right = cleaned.partition("|")
        left_s, right_s = left.strip(), right.strip()
        if right_s and re.search(r"views", left_s, re.I) and re.search(
            r"(reactions|likes|comments)", left_s, re.I
        ):
            cleaned = right_s.strip()

    if not cleaned and desc:
        d2 = _strip_leading_stats(desc)
        if d2:
            cleaned = d2
        elif not re.search(r"views", desc, re.I):
            cleaned = desc

    return (cleaned or str(title or "").strip()).strip()


def _ve_media_display_name(media: dict[str, Any], *, fallback: str = "media") -> str:
    """Tên hiển thị trong combobox — ưu tiên name / original_name / tên file."""
    for key in ("name", "original_name", "title"):
        v = str(media.get(key) or "").strip()
        if v:
            return v
    for key in ("local_path", "path"):
        p = str(media.get(key) or "").strip()
        if p:
            try:
                return Path(p).name
            except (TypeError, ValueError):
                return p
    mid = str(media.get("id") or "").strip()
    return mid or fallback


def _ve_build_media_combo_maps(
    media_items: list[dict[str, Any]],
    *,
    fallback: str = "media",
    include_empty: bool = True,
) -> tuple[list[str], dict[str, str], dict[str, str]]:
    """Danh sách nhãn combobox, map nhãn→id và id→nhãn (tránh trùng tên)."""
    labels: list[str] = []
    label_to_id: dict[str, str] = {}
    id_to_label: dict[str, str] = {}
    if include_empty:
        labels.append("")
    name_count: dict[str, int] = {}
    for m in media_items:
        if not isinstance(m, dict):
            continue
        mid = str(m.get("id") or "").strip()
        if not mid:
            continue
        base = _ve_media_display_name(m, fallback=fallback)
        key = base.casefold()
        name_count[key] = name_count.get(key, 0) + 1
        n = name_count[key]
        lbl = base if n == 1 else f"{base} ({n})"
        while lbl in label_to_id and label_to_id[lbl] != mid:
            lbl = f"{base} [{mid[-8:]}]"
        labels.append(lbl)
        label_to_id[lbl] = mid
        id_to_label[mid] = lbl
    return labels, label_to_id, id_to_label


def _ve_media_id_from_combo(raw: str, label_to_id: dict[str, str] | None = None) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    if label_to_id and s in label_to_id:
        return label_to_id[s]
    if "|" in s:
        return s.split("|", 1)[0].strip()
    return s


def _ve_resolve_combo_display(
    raw: str,
    *,
    id_to_label: dict[str, str],
    label_to_id: dict[str, str],
    media_items: list[dict[str, Any]] | None = None,
    fallback: str = "media",
) -> str:
    """Chuyển id cũ / nhãn cũ «id|tên» sang nhãn hiển thị trong combobox."""
    s = str(raw or "").strip()
    if not s:
        return ""
    if s in label_to_id:
        return s
    mid = _ve_media_id_from_combo(s, label_to_id)
    if mid and mid in id_to_label:
        return id_to_label[mid]
    if media_items and mid:
        for m in media_items:
            if isinstance(m, dict) and str(m.get("id") or "") == mid:
                return _ve_media_display_name(m, fallback=fallback)
    return s


def build_video_editor_tab(parent: ttk.Frame, root: tk.Tk) -> tuple[Callable[[], None], Callable[[], None]]:
    ve_paths = ensure_video_editor_layout()
    pm = VideoEditorProjectManager(paths=ve_paths)
    mm = MediaManager(paths=ve_paths)
    dl_store = DownloadMetadataStore(paths=ensure_downloader_layout())
    tm = TimelineManager(project_manager=pm)
    builder = FFmpegCommandBuilder()
    preview_worker = RenderWorker()
    export_worker = RenderWorker()
    # Hai worker tách cancel/log; FFmpeg thực tế chỉ một lúc (khóa toàn cục trong RenderWorker).
    _export_run_state: dict[str, int] = {"running": 0}
    _export_start_lock = threading.Lock()
    _export_user_abort_ref: dict[str, bool] = {"v": False}
    _selected_export_busy_ref: dict[str, bool] = {"busy": False}

    sub_mgr = SubtitleManager()
    wf_gen = WaveformGenerator()
    vf_mgr = VideoFilterManager()
    tr_mgr = TransitionManager()
    tmplate_mgr = TemplateManager()
    amix_mgr = AudioMixManager()
    kf_mgr = KeyframeAnimationManager()

    project: dict[str, Any] | None = None
    selected_clip_id: str | None = None

    _apply_batch_video_ref: dict[str, Any] = {"fn": None}
    _apply_transform_subset_ref: dict[str, Any] = {"fn": None}
    _ve_batch_reset_bar_ref: dict[str, Any] = {"fr": None}
    _q_logo_media_combo_refresh: dict[str, Any] = {"fn": None, "label_to_id": {}, "id_to_label": {}}
    _batch_edit_draft: dict[str, Any] = {}
    q_font_label_to_path: dict[str, str] = {}
    q_font_label_to_preview_font: dict[str, tuple[str, int, str, str]] = {}
    _suppress_tl_inspector_refresh: dict[str, bool] = {"v": False}

    def _clip_is_text_track_payload(cl: Any) -> bool:
        """Clip trên track ``text``: thiếu ``type`` vẫn là chữ (FFmpeg vẫn vẽ); trước đây bỏ qua → không replace được."""
        if not isinstance(cl, dict):
            return False
        t = str(cl.get("type") or "").strip().lower()
        return (not t) or t == "text"

    top = ttk.LabelFrame(parent, text="Luồng nhanh Video Editor", padding=6)
    top.pack(fill=tk.X, padx=4, pady=(4, 2))
    top_project = ttk.Frame(top)
    top_project.pack(fill=tk.X, pady=(0, 4))
    top_job = ttk.Frame(top)
    top_job.pack(fill=tk.X)
    ttk.Label(
        top,
        text="Gợi ý: 1) chọn/tạo dự án → 2) nạp job tải → 3) import video vào Media → chỉnh timeline → preview / xuất (luồng xuất đang được thay mới).",
        foreground="#555",
        font=("Segoe UI", 8),
        wraplength=980,
        justify="left",
    ).pack(anchor="w", pady=(4, 0))
    ttk.Label(top_project, text="B1 - Dự án:").pack(side=tk.LEFT, padx=(0, 6))

    project_ids = [p["id"] for p in pm.list_projects()]
    sync_p2_ui_ref: dict[str, Any] = {"fn": None}
    stock_audio_refresh_ref: dict[str, Any] = {"fn": None}
    stock_preview_proc_ref: dict[str, Any] = {"proc": None, "after_id": None, "last_fp": ""}

    _main_thread_ui_q: queue.Queue[Callable[[], None]] = queue.Queue()
    _main_ui_pump_pending: dict[str, bool] = {"v": False}

    def _schedule_on_main_thread(fn: Callable[[], None]) -> None:
        """Tkinter: không gọi root.after từ worker thread (Python 3.14+ có thể lỗi)."""
        _main_thread_ui_q.put(fn)
        if not _main_ui_pump_pending["v"]:
            _main_ui_pump_pending["v"] = True

            def _kick() -> None:
                _pump_main_thread_ui_queue()

            try:
                root.after(0, _kick)
            except tk.TclError:
                _main_ui_pump_pending["v"] = False

    _MAIN_UI_DRAIN_PER_TICK = 48

    def _pump_main_thread_ui_queue() -> None:
        n = 0
        try:
            while n < _MAIN_UI_DRAIN_PER_TICK:
                cb = _main_thread_ui_q.get_nowait()
                n += 1
                try:
                    cb()
                except Exception:
                    pass
        except queue.Empty:
            pass
        try:
            if not _main_thread_ui_q.empty():
                root.after(50, _pump_main_thread_ui_queue)
            else:
                _main_ui_pump_pending["v"] = False
        except tk.TclError:
            _main_ui_pump_pending["v"] = False

    def refresh_project_combo() -> None:
        nonlocal project_ids
        project_ids = [p["id"] for p in pm.list_projects()]
        cb_projects["values"] = project_ids

    var_project = tk.StringVar(value="")
    cb_projects = ttk.Combobox(top_project, textvariable=var_project, width=36, state="readonly")
    cb_projects.pack(side=tk.LEFT, padx=(0, 8))
    refresh_project_combo()

    def load_project_id(pid: str) -> None:
        nonlocal project, selected_clip_id
        if not pid:
            return
        try:
            project = pm.load_project(pid)
            selected_clip_id = None
            var_project.set(pid)
            refresh_all()
            fn = sync_p2_ui_ref.get("fn")
            if callable(fn):
                fn()
            notify(f"Đã mở project: {pid}")
        except Exception as e:
            messagebox.showerror("Video Editor", f"Không load được project: {e}")

    def on_pick_project(_e: Any = None) -> None:
        pid = var_project.get().strip()
        if pid:
            load_project_id(pid)

    cb_projects.bind("<<ComboboxSelected>>", on_pick_project)

    def new_project() -> None:
        nonlocal project
        name = simpledialog.askstring("Project mới", "Tên project:", parent=root)
        if not name:
            return
        project = pm.create_project(name.strip())
        refresh_project_combo()
        var_project.set(str(project.get("id")))
        refresh_all()
        fn = sync_p2_ui_ref.get("fn")
        if callable(fn):
            fn()
        notify(f"Đã tạo project mới: {project.get('name')} ({project.get('id')})")

    def save_project_btn() -> None:
        if not project:
            notify("Chưa có project để lưu.")
            return
        try:
            pm.save_project(project)
            notify("Đã lưu project ra JSON.")
        except Exception as e:
            messagebox.showerror("Video Editor", str(e))

    def _prune_empty_parent_dirs(start_path: Path, *, stop_at_parent: Path | None = None) -> None:
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
            parent_cur = cur.parent
            if parent_cur == cur:
                break
            cur = parent_cur

    def _collect_media_local_paths(proj: dict[str, Any]) -> list[Path]:
        out: list[Path] = []
        for m in proj.get("media") or []:
            if not isinstance(m, dict):
                continue
            lp = str(m.get("local_path") or "").strip()
            if not lp:
                continue
            p = Path(lp).expanduser()
            if p.exists():
                out.append(p.resolve())
        return out

    def _collect_other_project_local_paths(exclude_project_id: str) -> set[str]:
        used: set[str] = set()
        for row in pm.list_projects():
            pid = str(row.get("id") or "").strip()
            if not pid or pid == exclude_project_id:
                continue
            try:
                p = pm.load_project(pid)
            except Exception:
                continue
            for m in p.get("media") or []:
                if not isinstance(m, dict):
                    continue
                lp = str(m.get("local_path") or "").strip()
                if lp:
                    used.add(str(Path(lp).expanduser().resolve()))
        return used

    def _delete_local_media_files_if_safe(paths: list[Path], *, skip_paths: set[str]) -> int:
        deleted = 0
        media_root = ve_paths["media"].resolve()
        for p in paths:
            sp = str(p.resolve())
            if sp in skip_paths:
                continue
            # Chỉ xóa file local trong thư viện media của Video Editor (an toàn, không xóa file nguồn ngoài)
            try:
                rp = p.resolve()
                if media_root not in rp.parents and rp != media_root:
                    continue
                if rp.is_file():
                    rp.unlink(missing_ok=True)
                    deleted += 1
                    _prune_empty_parent_dirs(rp, stop_at_parent=media_root.parent)
            except Exception:
                continue
        return deleted

    def delete_current_project() -> None:
        nonlocal project, selected_clip_id
        if not project:
            messagebox.showinfo("Video Editor", "Chưa có project để xóa.", parent=root)
            return
        pid = str(project.get("id") or "").strip()
        pname = str(project.get("name") or "").strip() or pid
        if not pid:
            messagebox.showwarning("Video Editor", "Project hiện tại thiếu ID, không thể xóa.", parent=root)
            return
        if not messagebox.askyesno(
            "Video Editor",
            f"Xóa project hiện tại?\n\n{pname} ({pid})",
            parent=root,
        ):
            return
        remove_files = messagebox.askyesnocancel(
            "Video Editor",
            "Bạn muốn xóa luôn file media local của project này không?\n\n"
            "Có = xóa project + xóa file local trong thư viện Video Editor (nếu không dùng bởi project khác)\n"
            "Không = chỉ xóa project JSON\n"
            "Hủy = không thực hiện",
            parent=root,
        )
        if remove_files is None:
            return
        local_paths = _collect_media_local_paths(project)
        used_elsewhere = _collect_other_project_local_paths(pid) if remove_files else set()
        try:
            pm.delete_project(pid)
            deleted_files = (
                _delete_local_media_files_if_safe(local_paths, skip_paths=used_elsewhere) if remove_files else 0
            )
            project = None
            selected_clip_id = None
            var_project.set("")
            refresh_project_combo()
            refresh_all()
            notify(
                f"Đã xóa project {pid}"
                + (f" và {deleted_files} file media local không còn dùng." if remove_files else ".")
            )
        except Exception as e:
            messagebox.showerror("Video Editor", f"Không xóa được project: {e}", parent=root)

    ttk.Button(top_project, text="1) Dự án mới", command=new_project).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(top_project, text="Lưu dự án", command=save_project_btn).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(top_project, text="Xóa dự án...", command=delete_current_project).pack(side=tk.LEFT, padx=(0, 4))

    dl_job_map: dict[str, str] = {}
    var_show_empty_jobs = tk.BooleanVar(value=False)
    var_dl_job = tk.StringVar(value="")
    _auto_import_pending_job = {"armed": False}
    ttk.Label(top_job, text="B2 - Job tải:").pack(side=tk.LEFT, padx=(0, 4))
    cb_dl_job = ttk.Combobox(top_job, textvariable=var_dl_job, width=50, state="readonly")
    cb_dl_job.pack(side=tk.LEFT, padx=(0, 4))
    pending_editor_job_file = ensure_downloader_layout()["root"] / "pending_video_editor_job.json"

    def _consume_pending_editor_job_id() -> str:
        if not pending_editor_job_file.is_file():
            return ""
        try:
            raw = json.loads(pending_editor_job_file.read_text(encoding="utf-8"))
        except Exception:
            pending_editor_job_file.unlink(missing_ok=True)
            return ""
        pending_editor_job_file.unlink(missing_ok=True)
        if not isinstance(raw, dict):
            return ""
        return str(raw.get("job_id") or "").strip()

    def refresh_download_job_combo() -> None:
        keep_label = str(var_dl_job.get() or "").strip()
        keep_current_jid = dl_job_map.get(keep_label, "").strip()
        dl_job_map.clear()
        vals: list[str] = []
        all_videos = dl_store.list_downloaded_videos()
        count_by_job: dict[str, int] = {}
        for r in all_videos:
            jid = str(r.get("download_job_id") or "").strip()
            if not jid:
                continue
            count_by_job[jid] = int(count_by_job.get(jid, 0)) + 1
        def _parse_job_time(raw: Any) -> float:
            s = str(raw or "").strip()
            if not s:
                return 0.0
            try:
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                return datetime.fromisoformat(s).timestamp()
            except Exception:
                return 0.0

        jobs_raw = [j for j in dl_store.list_jobs() if isinstance(j, dict)]
        jobs_sorted = sorted(
            jobs_raw,
            key=lambda j: (
                int(count_by_job.get(str(j.get("id") or "").strip(), 0)),
                _parse_job_time(j.get("updated_at")),
                _parse_job_time(j.get("created_at")),
            ),
            reverse=True,
        )

        for j in jobs_sorted:
            jid = str(j.get("id") or "").strip()
            if not jid:
                continue
            plat = str(j.get("platform") or "").strip() or "unknown"
            st = str(j.get("status") or "").strip() or "-"
            jname = str(j.get("name") or "").strip()
            vcount = int(count_by_job.get(jid, 0))
            if vcount <= 0 and not bool(var_show_empty_jobs.get()):
                continue
            short_id = jid[-6:] if len(jid) > 6 else jid
            if jname:
                label = f"{jname} | {plat} | {vcount} video | {st} | #{short_id}"
            else:
                label = f"{plat} | {vcount} video | {st} | #{short_id}"
            vals.append(label)
            dl_job_map[label] = jid
        cb_dl_job.configure(values=vals)
        pending_jid = _consume_pending_editor_job_id()
        if pending_jid:
            for label, jid in dl_job_map.items():
                if jid == pending_jid:
                    var_dl_job.set(label)
                    _auto_import_pending_job["armed"] = True
                    break
            else:
                if vals and not str(var_dl_job.get() or "").strip():
                    var_dl_job.set(vals[0])
        elif vals and not str(var_dl_job.get() or "").strip():
            var_dl_job.set(vals[0])
        elif keep_current_jid:
            for label, jid in dl_job_map.items():
                if jid == keep_current_jid:
                    var_dl_job.set(label)
                    break
            else:
                var_dl_job.set(vals[0] if vals else "")
        if _auto_import_pending_job["armed"]:
            _auto_import_pending_job["armed"] = False
            root.after(80, import_from_download_job)

    def import_from_download_job() -> None:
        nonlocal project
        if not project:
            messagebox.showinfo("Video Editor", "Tạo hoặc chọn project trước.")
            return
        picked = str(var_dl_job.get() or "").strip()
        jid = dl_job_map.get(picked, "").strip()
        if not jid:
            messagebox.showwarning("Video Editor", "Chưa chọn job tải.", parent=root)
            return
        rows = [r for r in dl_store.list_downloaded_videos() if str(r.get("download_job_id") or "").strip() == jid]
        if not rows:
            messagebox.showwarning("Video Editor", "Job này chưa có video đã tải.", parent=root)
            return
        existing = {str(m.get("path") or "") for m in (project.get("media") or []) if isinstance(m, dict)}
        added = 0
        failed = 0
        source_video_ids: list[str] = []
        source_video_meta_by_id: dict[str, dict[str, Any]] = {}

        def _merge_hashtags_from_download_row(row: dict[str, Any], desc: str) -> list[str]:
            out: list[str] = []
            seen: set[str] = set()

            def add(tag: str) -> None:
                s = str(tag or "").strip()
                if not s:
                    return
                if not s.startswith("#"):
                    s = "#" + s.lstrip("#")
                k = s.lower()
                if k in seen:
                    return
                seen.add(k)
                out.append(s)

            for x in (row.get("hashtags") or []):
                add(str(x))
            for s in _extract_hashtags_from_text(desc):
                add(s)
            return out

        for r in rows:
            src_vid = str(r.get("id") or "").strip()
            vp = Path(str(r.get("video_path") or "")).expanduser().resolve()
            if not vp.is_file():
                continue
            raw_t = str(r.get("title") or "").strip()
            raw_d = str(r.get("description") or "").strip()
            src_meta = {
                "title": _normalize_post_caption_title(raw_t, description=raw_d),
                "description": raw_d,
                "hashtags": _merge_hashtags_from_download_row(r, raw_d),
            }
            if src_vid:
                source_video_meta_by_id[src_vid] = src_meta
            if str(vp) in existing:
                if src_vid:
                    source_video_ids.append(src_vid)
                continue
            try:
                rec = mm.import_media(str(vp), "video", copy_to_library=False)
            except Exception:
                # Máy yếu có thể timeout ffprobe khi import nhiều file lớn.
                # Fallback: vẫn nạp media record tối thiểu từ dữ liệu downloader.
                failed += 1
                rec = {
                    "id": f"media_{uuid.uuid4().hex[:10]}",
                    "type": "video",
                    "path": str(vp),
                    "local_path": "",
                    "original_name": vp.name,
                    "duration": float(r.get("duration") or 0.0),
                    "width": int(r.get("width") or 0),
                    "height": int(r.get("height") or 0),
                    "fps": float(r.get("fps") or 30.0),
                    "has_audio": True,
                    "created_at": datetime.now().replace(microsecond=0).isoformat(),
                }
            rec["source_download_video_id"] = src_vid
            rec["source_download_job_id"] = jid
            rec["source_title"] = src_meta["title"]
            rec["source_description"] = src_meta["description"]
            rec["source_hashtags"] = list(src_meta["hashtags"])
            project.setdefault("media", []).append(rec)
            existing.add(str(vp))
            if src_vid:
                source_video_ids.append(src_vid)
            added += 1
        pipe = dict(project.get("pipeline") or {})
        pipe["source_download_job_id"] = jid
        job_label = next((k for k, v in dl_job_map.items() if v == jid), "")
        pipe["source_download_job_label"] = job_label
        pipe["source_download_video_ids"] = source_video_ids
        pipe["source_download_video_meta_by_id"] = source_video_meta_by_id
        pipe["source_download_video_count"] = len(source_video_ids)
        project["pipeline"] = pipe
        pm.save_project(project)
        refresh_media_tree()
        refresh_timeline()
        tail = f" | fallback nhanh: {failed}" if failed else ""
        notify(f"Đã nạp {added} video từ job tải {jid} vào project (tổng liên kết: {len(source_video_ids)}){tail}.")

    ttk.Button(top_job, text="2) Nạp danh sách job", command=refresh_download_job_combo).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(top_job, text="3) Import video vào Media", command=import_from_download_job).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Checkbutton(
        top_job,
        text="Hiện cả job rỗng/lỗi",
        variable=var_show_empty_jobs,
        command=refresh_download_job_combo,
    ).pack(side=tk.LEFT, padx=(8, 0))
    refresh_download_job_combo()

    status_fr = ttk.Frame(parent, padding=(6, 2, 6, 4))
    status_fr.pack(fill=tk.X)
    lbl_status = ttk.Label(
        status_fr,
        text="Trạng thái: sẵn sàng. Mỗi lần chỉnh/lưu/import/export sẽ hiện dòng có giờ bên dưới.",
        foreground="gray",
        wraplength=960,
    )
    lbl_status.pack(side=tk.LEFT, anchor="w")

    def notify(msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        lbl_status.configure(text=line, foreground="#0a0a0a")
        try:
            logger.info(f"[Video Editor] {msg}")
        except Exception:
            pass

    def _ve_batch_status_progress(done: int, total: int, stage: str) -> None:
        if total < VE_BATCH_SHOW_PROGRESS_MIN_CLIPS:
            return
        pct = min(100, int(100 * done / max(1, total)))
        lbl_status.configure(
            text=f"Hàng loạt — {stage}: {done}/{total} ({pct}%)",
            foreground="#1a4480",
        )
        try:
            root.update_idletasks()
        except tk.TclError:
            pass

    def _ve_batch_progress_step(total: int) -> int:
        """Khoảng cách cập nhật UI (~tối đa 100 lần cho cả job)."""
        if total < VE_BATCH_SHOW_PROGRESS_MIN_CLIPS:
            return 0
        return max(1, total // 100)

    main = ttk.PanedWindow(parent, orient=tk.HORIZONTAL)
    main.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

    # --- Media panel ---
    media_fr = ttk.LabelFrame(main, text="Thư viện media — nhập file vào dự án", padding=4)
    main.add(media_fr, weight=2)
    mb = ttk.Frame(media_fr)
    mb.pack(fill=tk.X)

    def import_kind(kind: str) -> None:
        nonlocal project
        if not project:
            messagebox.showinfo("Video Editor", "Tạo hoặc chọn project trước.")
            return
        ft = [("Media", "*.mp4 *.mov *.mkv *.webm *.png *.jpg *.jpeg *.webp *.mp3 *.wav *.m4a")]
        paths = filedialog.askopenfilenames(parent=root, title="Chọn file", filetypes=ft)
        if not paths:
            return
        n_ok = 0
        last_image_rec: dict[str, Any] | None = None
        for fp in paths:
            try:
                rec = mm.import_media(fp, kind, copy_to_library=True)
                project.setdefault("media", []).append(rec)
                pm.save_project(project)
                n_ok += 1
                if kind == "image":
                    last_image_rec = rec
            except Exception as e:
                messagebox.showerror("Import lỗi", str(e))
                return
        refresh_media_tree()
        refresh_timeline()
        if kind == "image" and n_ok > 0 and last_image_rec:
            qlogo = _q_logo_media_combo_refresh.get("fn")
            if callable(qlogo):
                qlogo()
            mid0 = str(last_image_rec.get("id") or "").strip()
            if mid0:
                lbl0 = _ve_media_display_name(last_image_rec, fallback="image")
                i2l = _q_logo_media_combo_refresh.get("id_to_label") or {}
                lbl0 = str(i2l.get(mid0) or lbl0)
                try:
                    var_q_logo_media.set(lbl0)
                except Exception:
                    pass
                try:
                    mem0 = _batch_meta_store()
                    mem0["quick_logo_media_pick"] = lbl0
                    pm.save_project(project)
                except Exception:
                    pass
        notify(f"Đã import {n_ok} file ({kind}).")

    ttk.Button(mb, text="Thêm video", command=lambda: import_kind("video")).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(mb, text="Thêm logo / ảnh", command=lambda: import_kind("image")).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(mb, text="Thêm file nhạc", command=lambda: import_kind("audio")).pack(side=tk.LEFT, padx=(0, 4))

    media_inner = _pack_scrollable_vertical(media_fr)
    help_media = ttk.LabelFrame(media_inner, text="Logo & watermark — các bước", padding=6)
    help_media.pack(fill=tk.X, pady=(6, 4))
    ttk.Label(
        help_media,
        text=(
            "① «Thêm logo / ảnh» → PNG/JPG.\n"
            "② Chọn dòng media → «Thêm lên timeline» (overlay).\n"
            "③ Timeline → clip → «Chỉnh clip» / vị trí động.\n"
            "④ «Lưu dự án» → Preview / Xuất MP4.\n"
            "Cuộn cột này bằng thanh bên phải nếu màn hình thấp."
        ),
        foreground="#1a4480",
        font=("Segoe UI", 9),
        wraplength=280,
        justify="left",
    ).pack(anchor="w")

    cols_m = ("name", "type", "duration", "resolution")
    var_media_name_filter = tk.StringVar(value="")
    var_media_only_timeline = tk.BooleanVar(value=False)
    var_stock_topic = tk.StringVar(value=STOCK_TOPIC_FILTER_ALL)
    var_stock_autoplay = tk.BooleanVar(value=True)
    var_stock_count = tk.StringVar(value="0/0 file")
    var_stock_auto_fill_status = tk.StringVar(value="")
    stock_paths_mem: list[str] = []

    mf_wrap = ttk.Frame(media_inner)
    mf_wrap.pack(fill=tk.BOTH, expand=True, pady=(4, 0))
    mf_wrap.columnconfigure(0, weight=1)
    mf_wrap.rowconfigure(2, weight=1)
    ttk.Label(
        mf_wrap,
        text="Bước 4 — Tab «Âm thanh có sẵn» = kho stock_audio (khác media dự án). Tab Video / Logo / File nhạc = file đã import.",
        foreground="#555",
        font=("Segoe UI", 8),
        wraplength=520,
        justify="left",
    ).grid(row=0, column=0, sticky="w", pady=(0, 4))

    media_filter_fr = ttk.Frame(mf_wrap)
    media_filter_fr.grid(row=1, column=0, sticky="ew", pady=(0, 6))
    media_filter_fr.columnconfigure(1, weight=1)
    ttk.Label(media_filter_fr, text="Lọc tên:").grid(row=0, column=0, sticky="w")
    ent_media_filter = ttk.Entry(media_filter_fr, textvariable=var_media_name_filter, width=24)
    ent_media_filter.grid(row=0, column=1, sticky="ew", padx=(6, 12))
    cb_media_timeline_only = ttk.Checkbutton(
        media_filter_fr,
        text="Chỉ media đang dùng (timeline + BGM)",
        variable=var_media_only_timeline,
    )
    cb_media_timeline_only.grid(row=0, column=2, sticky="w")

    def _clear_media_name_filter() -> None:
        var_media_name_filter.set("")
        refresh_media_tree()

    btn_clear_media_filter = ttk.Button(media_filter_fr, text="Xóa lọc tên", command=_clear_media_name_filter)
    btn_clear_media_filter.grid(row=0, column=3, sticky="w", padx=(10, 0))

    media_nb = ttk.Notebook(mf_wrap)
    media_nb.grid(row=2, column=0, sticky="nsew")

    def _make_media_tree(host: ttk.Frame) -> ttk.Treeview:
        host.columnconfigure(0, weight=1)
        host.rowconfigure(0, weight=1)
        tr = ttk.Treeview(host, columns=cols_m, show="headings", height=10, selectmode="extended")
        for c, t, w in (
            ("name", "Tên file", 140),
            ("type", "Loại", 56),
            ("duration", "Độ dài (s)", 72),
            ("resolution", "Kích thước", 88),
        ):
            tr.heading(c, text=t)
            tr.column(c, width=w)
        sy = ttk.Scrollbar(host, orient=tk.VERTICAL, command=tr.yview)
        tr.configure(yscrollcommand=sy.set)
        tr.grid(row=0, column=0, sticky="nsew")
        sy.grid(row=0, column=1, sticky="ns")
        return tr

    def _stop_stock_preview() -> None:
        aid = stock_preview_proc_ref.get("after_id")
        if aid is not None:
            try:
                root.after_cancel(aid)
            except Exception:
                pass
            stock_preview_proc_ref["after_id"] = None
        proc = stock_preview_proc_ref.get("proc")
        if proc is None:
            return
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=0.25)
                except Exception:
                    proc.kill()
        except Exception:
            pass
        stock_preview_proc_ref["proc"] = None
        stock_preview_proc_ref["last_fp"] = ""

    tab_media_stock = ttk.Frame(media_nb, padding=2)
    tab_media_stock.columnconfigure(0, weight=1)
    tab_media_stock.rowconfigure(0, weight=1)
    fr_stock_tree = ttk.Frame(tab_media_stock)
    fr_stock_tree.grid(row=0, column=0, sticky="nsew")
    tree_stock = _make_media_tree(fr_stock_tree)

    tab_media_video = ttk.Frame(media_nb, padding=2)
    tab_media_image = ttk.Frame(media_nb, padding=2)
    tab_media_audio = ttk.Frame(media_nb, padding=2)
    media_nb.add(tab_media_stock, text="Âm thanh có sẵn (stock)")
    media_nb.add(tab_media_video, text="Video")
    media_nb.add(tab_media_image, text="Logo / Ảnh")
    media_nb.add(tab_media_audio, text="File nhạc (dự án)")

    media_trees: dict[str, ttk.Treeview] = {
        "stock": tree_stock,
        "video": _make_media_tree(tab_media_video),
        "image": _make_media_tree(tab_media_image),
        "audio": _make_media_tree(tab_media_audio),
    }
    tree_media = media_trees["video"]

    def refresh_stock_audio_box() -> None:
        _stop_stock_preview()
        cfg = load_remote_audio_config(mm._paths)
        auto_fill_on = bool(cfg.get("background_fill_enabled", False))
        var_stock_auto_fill_status.set(
            "Tự động tải kho: ĐANG BẬT" if auto_fill_on else "Tự động tải kho: ĐANG TẮT"
        )
        stock_paths_all = [str(p) for p in list_stock_audio_paths(mm._paths)]
        topic = str(var_stock_topic.get()).strip()
        filtered = filter_stock_paths_by_topic([Path(p) for p in stock_paths_all], topic)
        needle = str(var_media_name_filter.get() or "").strip().lower()
        stock_paths_mem.clear()
        for p in filtered:
            fp = str(p)
            if needle and needle not in Path(fp).name.lower():
                continue
            stock_paths_mem.append(fp)
        tr = media_trees["stock"]
        tr.delete(*tr.get_children())
        for i, fp in enumerate(stock_paths_mem):
            tr.insert("", tk.END, iid=f"s{i}", values=(Path(fp).name, "stock", "—", "—"))
        var_stock_count.set(f"{len(stock_paths_mem)}/{len(stock_paths_all)} file")

    def _play_stock_audio_system_default(path: str) -> None:
        """
        Mở file âm thanh bằng ứng dụng mặc định khi không có ffplay (preview stock / kho có sẵn).
        """
        p = Path(path)
        if not p.is_file():
            messagebox.showwarning("Stock audio", "Không thấy file trên đĩa.")
            return
        try:
            if os.name == "nt":
                os.startfile(str(p))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(p)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                subprocess.Popen(["xdg-open", str(p)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            messagebox.showerror("Stock audio", str(e))
            return
        notify(
            "Không có ffplay — đã mở bằng trình phát mặc định. "
            "«Dừng nghe» chỉ dừng tiến trình ffplay; với trình hệ thống hãy đóng cửa sổ phát đó."
        )

    def _play_selected_stock_preview() -> None:
        tr = media_trees["stock"]
        sel = tr.selection()
        if not sel:
            return
        if not stock_paths_mem:
            return
        iid = str(sel[0])
        if not iid.startswith("s"):
            return
        try:
            idx = int(iid[1:])
        except ValueError:
            return
        if idx < 0 or idx >= len(stock_paths_mem):
            return
        fp = stock_paths_mem[idx]
        if str(stock_preview_proc_ref.get("last_fp") or "") == fp:
            return
        _stop_stock_preview()
        expected_iid = str(iid)

        def _finish_stock_ffplay(ffplay: str | None) -> None:
            if str(stock_preview_proc_ref.get("last_fp") or "") == fp:
                return
            sel2 = tr.selection()
            if not sel2 or str(sel2[0]) != expected_iid:
                return
            try:
                idx2 = int(str(sel2[0])[1:])
            except ValueError:
                return
            if idx2 < 0 or idx2 >= len(stock_paths_mem) or stock_paths_mem[idx2] != fp:
                return
            if ffplay:
                try:
                    proc = subprocess.Popen(
                        [ffplay, "-nodisp", "-autoexit", "-loglevel", "error", fp],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    stock_preview_proc_ref["proc"] = proc
                    stock_preview_proc_ref["last_fp"] = fp
                except Exception as e:
                    messagebox.showerror("Stock audio", str(e))
                return
            _play_stock_audio_system_default(fp)
            stock_preview_proc_ref["proc"] = None
            stock_preview_proc_ref["last_fp"] = fp

        if ffplay_resolve_skips_ensure_heavy_work():
            _finish_stock_ffplay(resolve_ffplay_executable())
            return
        _resolve_ffplay_async(
            busy_message="Đang chuẩn bị ffplay (tìm / copy / tải portable) — lần đầu có thể vài chục giây…",
            on_ready=_finish_stock_ffplay,
            notify_busy=True,
        )

    def _on_stock_tree_select(_event: Any = None) -> None:
        aid = stock_preview_proc_ref.get("after_id")
        if aid is not None:
            try:
                root.after_cancel(aid)
            except Exception:
                pass
        if not var_stock_autoplay.get():
            return
        stock_preview_proc_ref["after_id"] = root.after(120, _play_selected_stock_preview)

    def open_stock_audio_folder() -> None:
        d = Path(stock_audio_dir_display_hint(mm._paths))
        try:
            if os.name == "nt":
                os.startfile(str(d))  # type: ignore[attr-defined]
            else:
                subprocess.Popen(["xdg-open", str(d)])
        except Exception as e:
            messagebox.showerror("Stock audio", str(e))

    def add_selected_stock_to_project(*, as_bgm: bool) -> None:
        if not project:
            messagebox.showinfo("Stock audio", "Mở hoặc tạo project trước.")
            return
        tr = media_trees["stock"]
        sel = tr.selection()
        if not sel:
            messagebox.showinfo("Stock audio", "Chọn một file trong tab «Âm thanh có sẵn (stock)».")
            return
        iid = str(sel[0])
        if not iid.startswith("s"):
            return
        try:
            idx = int(iid[1:])
        except ValueError:
            return
        if idx < 0 or idx >= len(stock_paths_mem):
            messagebox.showinfo("Stock audio", "Chọn lại file trong danh sách.")
            return
        fp = stock_paths_mem[idx]
        try:
            rec = mm.import_media(fp, "audio", copy_to_library=True)
            project.setdefault("media", []).append(rec)
            pm.save_project(project)
            refresh_media_tree()
            if as_bgm:
                vol = float(var_bgm_vol.get().strip() or "0.25")
                amix_mgr.add_background_music(project, str(rec.get("id")), vol)
                pm.save_project(project)
                notify("Đã thêm stock vào Media và BGM (có trong file xuất / preview).")
            else:
                tm.add_clip(project, str(rec.get("id")), "audio")
                refresh_timeline()
                notify(
                    "Đã thêm stock vào Media và lớp âm thanh timeline — xuất / preview sẽ trộn track này."
                )
        except Exception as e:
            messagebox.showerror("Stock audio", str(e))

    fr_stock_controls = ttk.Frame(tab_media_stock)
    fr_stock_controls.grid(row=1, column=0, sticky="ew", pady=(6, 0))
    ttk.Label(
        fr_stock_controls,
        text=f"Thư mục: {stock_audio_dir_display_hint(mm._paths)} — chép .mp3, .wav, .m4a… vào đây.",
        foreground="#555",
        font=("Segoe UI", 8),
        wraplength=480,
        justify="left",
    ).pack(anchor="w")
    row_sc = ttk.Frame(fr_stock_controls)
    row_sc.pack(fill=tk.X, pady=(4, 0))
    ttk.Label(row_sc, text="Lọc chủ đề:").pack(side=tk.LEFT)
    cb_stock_topic = ttk.Combobox(
        row_sc,
        state="readonly",
        width=20,
        textvariable=var_stock_topic,
        values=stock_topic_filter_labels(),
    )
    cb_stock_topic.pack(side=tk.LEFT, padx=(6, 0))
    cb_stock_topic.bind("<<ComboboxSelected>>", lambda _e: refresh_stock_audio_box())
    ttk.Checkbutton(row_sc, text="Nghe khi chọn", variable=var_stock_autoplay).pack(side=tk.LEFT, padx=(10, 0))
    ttk.Button(row_sc, text="Dừng nghe", command=_stop_stock_preview).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Label(row_sc, textvariable=var_stock_count, foreground="#666", font=("Segoe UI", 8)).pack(side=tk.RIGHT)
    row_bt = ttk.Frame(fr_stock_controls)
    row_bt.pack(fill=tk.X, pady=(6, 0))
    def _toggle_stock_auto_fill() -> None:
        cfg = load_remote_audio_config(mm._paths)
        is_on = bool(cfg.get("background_fill_enabled", False))
        cfg["background_fill_enabled"] = not is_on
        save_remote_audio_config(cfg, mm._paths)
        refresh_stock_audio_box()
        if bool(cfg["background_fill_enabled"]):
            notify("Đã BẬT tự động tải thêm âm thanh cho kho stock.")
        else:
            notify("Đã TẮT tự động tải thêm âm thanh cho kho stock.")

    ttk.Button(row_bt, text="Bật/Tắt tự động tải kho", command=_toggle_stock_auto_fill).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(row_bt, text="Làm mới", command=refresh_stock_audio_box).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(row_bt, text="Nghe file đã chọn", command=_play_selected_stock_preview).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(row_bt, text="Mở thư mục stock", command=open_stock_audio_folder).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(row_bt, text="Thêm vào Media", command=lambda: add_selected_stock_to_project(as_bgm=False)).pack(
        side=tk.LEFT, padx=(0, 4)
    )
    ttk.Button(row_bt, text="Thêm Media + BGM", command=lambda: add_selected_stock_to_project(as_bgm=True)).pack(side=tk.LEFT)
    ttk.Label(fr_stock_controls, textvariable=var_stock_auto_fill_status, foreground="#666", font=("Segoe UI", 8)).pack(
        anchor="w", pady=(4, 0)
    )

    tree_stock.bind("<<TreeviewSelect>>", _on_stock_tree_select)

    def _active_media_kind() -> str:
        cur = media_nb.select()
        if cur == str(tab_media_stock):
            return "stock"
        if cur == str(tab_media_video):
            return "video"
        if cur == str(tab_media_image):
            return "image"
        if cur == str(tab_media_audio):
            return "audio"
        return "video"

    def _active_media_tree() -> ttk.Treeview:
        return media_trees.get(_active_media_kind(), tree_media)

    def _selected_media_ids() -> list[str]:
        tr = _active_media_tree()
        return [str(i) for i in tr.selection() if str(i).strip()]

    def _selected_media_id() -> str | None:
        ids = _selected_media_ids()
        return ids[0] if ids else None

    refresh_stock_audio_box()

    # --- Center: preview + timeline ---
    center = ttk.PanedWindow(main, orient=tk.VERTICAL)
    main.add(center, weight=5)

    prev_fr = ttk.LabelFrame(center, text="Xem trước — kiểm tra trước khi xuất file", padding=6)
    center.add(prev_fr, weight=1)
    lbl_preview = ttk.Label(
        prev_fr,
        text=(
            "• «Thumbnail»: ảnh tĩnh từ video đang chọn (nhanh).\n"
            "• «Preview nháp»: render ~20 giây đầu giống bản xuất thật (có logo, chữ, phụ đề nếu có) — nên xem trước khi bấm Xuất MP4.\n"
            "• «Mở bằng app» / «ffplay»: luôn mở file preview nháp (bản composite đã ghép timeline). Chưa có file thì sẽ render ~20s trước.\n"
            "• Double-click một dòng timeline: render nháp ~20s theo clip đang chọn, xong tự mở ffplay (đợi vài giây).\n"
            "• Chuột phải timeline → «Xem file nguồn clip (ffplay)» — file media gốc (có trim theo clip), không phải bản composite."
        ),
        wraplength=560,
        justify="left",
    )
    lbl_preview.pack(anchor=tk.W)
    preview_path_var = tk.StringVar(value="")
    preview_busy_ref: dict[str, bool] = {"busy": False}
    preview_run_ref: dict[str, int] = {"id": 0}
    preview_watchdog_ref: dict[str, str | None] = {"after": None}
    # Một ffplay do tab quản lý — đóng trước khi mở mới (tránh chồng giải mã/RAM khi đổi clip).
    ffplay_managed_proc_ref: dict[str, Any] = {"p": None}
    # Double-click / mở xem: render preview xong — «with» = ffplay hoặc app mặc định (file composite).
    preview_open_after_done_ref: dict[str, Any] = {"v": False, "with": "ffplay"}
    # Clip_id tại dòng chuột phải (timeline) — «Xem file nguồn» không nhầm khi multi-select / focus lệch.
    tl_ctx_menu_video_cid_ref: dict[str, str | None] = {"cid": None}
    var_ffplay_view_size = tk.StringVar(value="Khung vừa")

    def _ffplay_window_args(*, window_title: str) -> list[str]:
        try:
            sw = int(root.winfo_screenwidth() or 1920)
            sh = int(root.winfo_screenheight() or 1080)
        except Exception:
            sw, sh = 1920, 1080
        mode = str(var_ffplay_view_size.get() or "Khung vừa").strip().lower()
        if "nhỏ" in mode:
            w_ratio, h_ratio = 0.34, 0.62
        elif "lớn" in mode:
            w_ratio, h_ratio = 0.52, 0.86
        else:
            w_ratio, h_ratio = 0.42, 0.78
        win_w = max(380, min(1080, int(sw * w_ratio)))
        win_h = max(360, min(900, int(sh * h_ratio)))
        return ["-x", str(win_w), "-y", str(win_h), "-window_title", window_title]

    def _ffplay_video_decode_display_fix() -> list[str]:
        """
        Tránh màn đen vẫn có tiếng khi xem MP4 trong ffplay (thường gặp trên Windows):
        ép về 8-bit yuv420p để SDL/ffplay vẽ ổn (HEVC 10-bit, pixel format lạ).

        Không dùng ``-hwaccel no``: ffplay cũ (ví dụ 2.7.x) không có tùy chọn này → lỗi
        «Failed to set value 'no' for option 'hwaccel': Option not found».
        Trên ffplay/ffmpeg mới, nếu vẫn màn đen có tiếng nên cập nhật ffplay portable trong app.
        """
        return ["-vf", "format=yuv420p"]

    def _stop_managed_ffplay(*, wait_s: float = 2.0) -> None:
        p = ffplay_managed_proc_ref.get("p")
        ffplay_managed_proc_ref["p"] = None
        if p is None:
            return
        try:
            if p.poll() is not None:
                return
        except Exception:
            return
        try:
            p.terminate()
        except Exception:
            pass
        t0 = time.monotonic()
        try:
            while p.poll() is None and (time.monotonic() - t0) < wait_s:
                time.sleep(0.05)
        except Exception:
            pass
        try:
            if p.poll() is None:
                p.kill()
        except Exception:
            pass

    def _popen_ffplay_managed(argv: list[str]) -> Any:
        """Đóng ffplay ToolFB trước đó rồi chạy một tiến trình mới; trả về Popen hoặc None."""
        try:
            _stop_stock_preview()
        except Exception:
            pass
        _stop_managed_ffplay(wait_s=1.5)
        popen_kw: dict[str, Any] = {}
        # Không dùng CREATE_NO_WINDOW: ffplay cần cửa sổ SDL — cờ này trên Windows có thể khiến
        # không thấy cửa sổ phát dù log vẫn báo «Đã mở ffplay».
        try:
            proc = subprocess.Popen(argv, **popen_kw)
            ffplay_managed_proc_ref["p"] = proc
            return proc
        except Exception:
            ffplay_managed_proc_ref["p"] = None
            raise

    _ffplay_resolve_gen_ref: dict[str, int] = {"v": 0}

    def _resolve_ffplay_async(
        *,
        busy_message: str,
        on_ready: Callable[[str | None], None],
        notify_busy: bool = True,
    ) -> None:
        """
        Gọi ``resolve_ffplay_executable()`` trên thread nền — tránh treo UI khi copy/tải ffplay portable.

        Mỗi lần gọi tăng generation; callback ``on_ready`` chỉ chạy nếu vẫn là yêu cầu mới nhất.
        """
        _ffplay_resolve_gen_ref["v"] = int(_ffplay_resolve_gen_ref.get("v") or 0) + 1
        my_gen = int(_ffplay_resolve_gen_ref["v"])

        def _worker() -> None:
            fp: str | None
            try:
                fp = resolve_ffplay_executable()
            except Exception as exc:
                logger.exception("resolve_ffplay_executable (nền): {}", exc)
                fp = None

            def _apply() -> None:
                if int(_ffplay_resolve_gen_ref.get("v") or 0) != my_gen:
                    return
                try:
                    on_ready(fp)
                except Exception:
                    logger.exception("on_ready ffplay async")

            try:
                root.after(0, _apply)
            except Exception:
                pass

        if notify_busy:
            try:
                notify(busy_message)
            except Exception:
                pass
        threading.Thread(target=_worker, daemon=True).start()

    def open_path_with_default_player(path: str) -> None:
        p = path.strip()
        if not p or not Path(p).is_file():
            notify("Không có file để mở.")
            return
        try:
            if os.name == "nt":
                os.startfile(p)  # type: ignore[attr-defined]
            else:
                subprocess.Popen(["xdg-open", p])
            notify(f"Đã mở trình phát: {Path(p).name}")
        except Exception as e:
            messagebox.showerror("Preview", str(e))

    def open_preview_file() -> None:
        """Luôn mở file preview nháp (composite timeline). Nếu chưa có — render rồi mở bằng app mặc định."""
        p = str(preview_path_var.get() or "").strip()
        if p and Path(p).is_file():
            open_path_with_default_player(p)
            return
        if preview_busy_ref.get("busy"):
            notify("Đang render preview nháp — đợi xong rồi bấm lại.")
            return
        preview_open_after_done_ref["v"] = True
        preview_open_after_done_ref["with"] = "default"
        notify("Chưa có preview — đang render nháp ~20s (composite), xong sẽ mở bằng app mặc định.")
        run_preview_draft()

    def open_draft_preview_with_ffplay(*, force_composite: bool = False) -> None:
        """Mở preview nháp đã render (bản composite). File chưa có thì chạy «Preview nháp» rồi tự mở ffplay."""
        _ = force_composite
        p = str(preview_path_var.get() or "").strip()
        if p and Path(p).is_file():
            if preview_busy_ref.get("busy"):
                notify("Preview đang render nền — đợi xong rồi bấm lại.")
                return

            def _open_draft_ff_done(ffplay: str | None) -> None:
                if not ffplay:
                    notify("Không tìm thấy ffplay — dùng «Mở bằng app mặc định».")
                    open_path_with_default_player(p)
                    return
                try:
                    _popen_ffplay_managed(
                        [
                            ffplay,
                            *_ffplay_video_decode_display_fix(),
                            *_ffplay_window_args(window_title="ToolFB - ffplay preview nháp"),
                            "-autoexit",
                            p,
                        ]
                    )
                    notify(f"Đã mở ffplay (preview nháp / composite): {Path(p).name}")
                except Exception as e:
                    messagebox.showerror("ffplay", str(e))

            if ffplay_resolve_skips_ensure_heavy_work():
                _open_draft_ff_done(resolve_ffplay_executable())
            else:
                _resolve_ffplay_async(
                    busy_message="Đang chuẩn bị ffplay (copy/tải portable nếu cần)…",
                    on_ready=_open_draft_ff_done,
                    notify_busy=True,
                )
            return
        if preview_busy_ref.get("busy"):
            notify("Đang render preview nháp — đợi xong rồi bấm lại.")
            return
        preview_open_after_done_ref["v"] = True
        preview_open_after_done_ref["with"] = "ffplay"
        notify("Chưa có preview composite — đang render nháp ~20s, xong sẽ mở ffplay.")
        run_preview_draft()

    def _open_source_path_in_ffplay(p: str, trim_pre: list[str], *, window_title: str) -> None:
        """Một file nguồn + trim tùy chọn — cùng lệnh ffplay cho timeline và tab Media (Video)."""
        if not str(p or "").strip():
            notify("Không có đường dẫn file để mở.")
            return

        def _open_src_ff_done(ffplay: str | None) -> None:
            if not ffplay:
                notify("Không tìm thấy ffplay — dùng «Mở file preview» hoặc trình phát mặc định.")
                open_path_with_default_player(p)
                return
            try:
                _popen_ffplay_managed(
                    [
                        ffplay,
                        *_ffplay_video_decode_display_fix(),
                        *trim_pre,
                        *_ffplay_window_args(window_title=window_title),
                        "-autoexit",
                        p,
                    ]
                )
                notify(f"Đã mở ffplay: {Path(p).name}")
            except Exception as e:
                messagebox.showerror("ffplay", str(e))

        if ffplay_resolve_skips_ensure_heavy_work():
            _open_src_ff_done(resolve_ffplay_executable())
        else:
            _resolve_ffplay_async(
                busy_message="Đang chuẩn bị ffplay (copy/tải portable nếu cần)…",
                on_ready=_open_src_ff_done,
                notify_busy=True,
            )

    def open_with_ffplay(*, prefer_selected_library_media: bool = False) -> None:
        """Menu timeline «Xem file nguồn clip» hoặc tab Video: cùng pipeline ffplay (file gốc + trim nếu có)."""
        if prefer_selected_library_media:
            mid = _selected_media_id()
            if not mid:
                notify("Chọn một video trong danh sách Media.")
                return
            media = _find_media(mid)
            if not media or str(media.get("type") or "") != "video":
                notify("Chọn một dòng video trong tab Video.")
                return
            mp = mm.resolve_media_path_on_disk(media)
            if not mp or not mp.is_file():
                notify("Không tìm thấy file media đã chọn.")
                return
            _open_source_path_in_ffplay(
                str(mp),
                [],
                window_title="ToolFB - ffplay (nguồn timeline)",
            )
            return

        # Menu timeline «Xem file nguồn clip» — file media gốc, không phải bản composite đã render.
        p = ""
        media_path: Path | str | None = None
        cl_pick: dict[str, Any] | None = None
        try:
            rows = _selected_video_timeline_rows()
        except Exception:
            rows = []
        anchor_cid = str(tl_ctx_menu_video_cid_ref.get("cid") or "").strip()
        if not anchor_cid:
            anchor_cid = _primary_timeline_clip_id_for_preview()
        if anchor_cid:
            for cid, cl in rows:
                if str(cid) == anchor_cid:
                    cl_pick = cl
                    break
            if cl_pick is None:
                _tr_a, _cl_a = _find_clip(anchor_cid)
                if _tr_a and _cl_a and str(_tr_a.get("type") or "") == "video" and str(_cl_a.get("type") or "") == "video":
                    cl_pick = _cl_a
        if cl_pick is None and rows:
            focus_cid = str(tree_tl.focus() or "").strip()
            if focus_cid:
                for cid, cl in rows:
                    if str(cid) == focus_cid:
                        cl_pick = cl
                        break
            if cl_pick is None:
                cl_pick = rows[0][1] if len(rows[0]) > 1 else {}
        if cl_pick is not None:
            cid_mid = str(
                (cl_pick or {}).get("media_id")
                or (cl_pick or {}).get("source_media_id")
                or ""
            ).strip()
            media2 = _find_media(cid_mid) if cid_mid else None
            media_path2 = mm.resolve_media_path_on_disk(media2) if media2 else None
            if media_path2 is not None and media_path2.is_file():
                media_path = media_path2
        if media_path is None or (isinstance(media_path, Path) and not media_path.is_file()):
            mid = _selected_media_id()
            media = _find_media(mid) if mid else None
            media_path = mm.resolve_media_path_on_disk(media) if media else None
        trim_pre: list[str] = []
        if media_path is not None:
            mp_ok = media_path if isinstance(media_path, Path) else Path(str(media_path))
            if mp_ok.is_file():
                p = str(mp_ok)
                if cl_pick is not None:
                    ss = float((cl_pick or {}).get("source_start") or 0.0)
                    du = max(0.05, float((cl_pick or {}).get("duration") or 0.0))
                    trim_pre = ["-ss", f"{ss:.3f}", "-t", f"{du:.3f}"]
                    notify(
                        "Mở đoạn nguồn theo clip (ffplay có trim). Bản đã ghép timeline: «Preview nháp» hoặc double-click dòng."
                    )
                else:
                    notify("Mở file nguồn (ffplay) — cùng luồng «Xem file nguồn clip» trên timeline.")
        if not p:
            p = preview_path_var.get().strip()
            if not p or not Path(p).is_file():
                notify("Chưa có file preview hoặc media hợp lệ để mở.")
                return
            if preview_busy_ref.get("busy"):
                notify("Preview đang render nền — mở bản preview gần nhất bằng ffplay.")

        _open_source_path_in_ffplay(p, trim_pre, window_title="ToolFB - ffplay (nguồn timeline)")

    def _primary_timeline_clip_id_for_preview() -> str:
        """Clip ưu tiên: dòng đang chọn đúng — không dùng focus lệch; không lấy phần tử đầu tuple selection (thường là clip đầu cây)."""
        try:
            on_grouped = str(tl_nb.select()) == str(tl_tab_grouped)
        except Exception:
            on_grouped = False
        if on_grouped:
            gsel = list(tree_tl_grouped.selection())
            if gsel:
                cid = str(grouped_video_to_clip.get(str(gsel[-1]) or "") or "").strip()
                if cid:
                    return cid
        sel_list = list(tree_tl.selection())
        if not sel_list:
            return ""
        sel_set = set(sel_list)
        fc = str(tree_tl.focus() or "").strip()
        if fc and fc in sel_set:
            try:
                if tree_tl.exists(fc):
                    return fc
            except Exception:
                pass
        return str(sel_list[-1])

    def open_selected_imported_media_default() -> None:
        """Tab Logo/Ảnh hoặc File nhạc: double-click / menu → mở file bằng ứng dụng hệ thống."""
        mid = _selected_media_id()
        if not mid:
            notify("Chọn một dòng trong danh sách Media.")
            return
        media = _find_media(mid)
        media_path = mm.resolve_media_path_on_disk(media) if media else None
        if not media_path or not media_path.is_file():
            notify("Không tìm thấy file media đã chọn.")
            return
        open_path_with_default_player(str(media_path))

    def run_preview_draft() -> None:
        """Xuất nháp timeline (giới hạn thời lượng) để xem trước export đầy đủ."""
        nonlocal project
        if int(_export_run_state.get("running") or 0) > 0:
            notify("Đang xuất MP4 — đợi xong rồi mới preview nháp (tránh treo / chồng FFmpeg).")
            preview_open_after_done_ref["v"] = False
            preview_open_after_done_ref["with"] = "ffplay"
            _auto_preview_pending_ref["want"] = True
            return
        if preview_busy_ref.get("busy"):
            notify("Hủy preview đang chạy — render lại theo clip hiện tại…")
            try:
                preview_worker.cancel_all()
            except Exception:
                pass
            try:
                preview_worker.clear_cancel()
            except Exception:
                pass
            aid0 = preview_watchdog_ref.get("after")
            if aid0:
                try:
                    root.after_cancel(aid0)
                except Exception:
                    pass
            preview_watchdog_ref["after"] = None
        if not project:
            notify("Chưa có project.")
            preview_open_after_done_ref["v"] = False
            preview_open_after_done_ref["with"] = "ffplay"
            return
        ffmpeg_bin = resolve_ffmpeg_executable()
        if not ffmpeg_bin:
            notify("Không có ffmpeg.")
            preview_open_after_done_ref["v"] = False
            preview_open_after_done_ref["with"] = "ffplay"
            return
        _cancel_auto_preview_debounce()
        _stop_managed_ffplay()
        # Giữ chỗ sớm — tránh hai lần gọi lọt qua kiểm tra busy rồi chạy hai FFmpeg chồng nhau.
        preview_busy_ref["busy"] = True
        preview_run_ref["id"] = int(preview_run_ref.get("id") or 0) + 1
        run_id = int(preview_run_ref["id"])
        pid = str(project.get("id") or "pv")
        safe = "".join(c for c in pid if c.isalnum() or c in "-_")[:48]
        # Một file preview cố định / project — ghi đè sau khi đã đóng ffplay (tránh phình temp + nhiều bản giải mã).
        out_p = ensure_video_editor_layout()["temp"] / f"preview_draft_{safe}.mp4"
        run_ref: dict[str, int] = {"attempt": 0}
        preview_timeline_window_sec = 20.0
        try:
            selected_rows = _selected_video_timeline_rows()
        except Exception:
            selected_rows = []
        primary_cid = _primary_timeline_clip_id_for_preview()
        preview_base_project = copy.deepcopy(project)

        def _project_solo_primary_video_clip(src_project: dict[str, Any], keep_cid: str) -> dict[str, Any]:
            """Preview nháp chỉ một clip video được chọn — tránh hai clip cùng mốc T hoặc chồng layer vẫn hiện clip khác."""
            k = str(keep_cid or "").strip()
            if not k:
                return src_project
            out = copy.deepcopy(src_project)
            vid_lo = 0.0
            vid_hi = 0.0
            found = False
            for tr in out.get("tracks") or []:
                if not isinstance(tr, dict) or str(tr.get("type") or "") != "video":
                    continue
                clips = [c for c in (tr.get("clips") or []) if isinstance(c, dict)]
                target = next(
                    (
                        c
                        for c in clips
                        if str(c.get("type") or "") == "video" and str(c.get("id") or "") == k
                    ),
                    None,
                )
                if target is None:
                    continue
                vid_lo = float(target.get("timeline_start") or 0.0)
                vid_hi = vid_lo + max(0.0, float(target.get("duration") or 0.0))
                kept: list[dict[str, Any]] = []
                for cl in clips:
                    if str(cl.get("type") or "") == "video":
                        if str(cl.get("id") or "") == k:
                            kept.append(dict(cl))
                        continue
                    kept.append(dict(cl))
                tr["clips"] = kept
                found = True
                break
            if not found:
                return preview_base_project

            def _ov(a0: float, a1: float, b0: float, b1: float) -> bool:
                return min(a1, b1) > max(a0, b0)

            for tr in out.get("tracks") or []:
                if not isinstance(tr, dict) or str(tr.get("type") or "") == "video":
                    continue
                nclips: list[dict[str, Any]] = []
                for cl in tr.get("clips") or []:
                    if not isinstance(cl, dict):
                        continue
                    cs = float(cl.get("timeline_start") or 0.0)
                    ce = cs + max(0.0, float(cl.get("duration") or 0.0))
                    if _ov(cs, ce, vid_lo, vid_hi):
                        nclips.append(dict(cl))
                tr["clips"] = nclips
            return out

        _tr_pv, _cl_pv = _find_clip(primary_cid) if primary_cid else (None, None)
        if primary_cid and _cl_pv and str(_cl_pv.get("type") or "") == "video":
            preview_base_project = _project_solo_primary_video_clip(preview_base_project, primary_cid)

        def _preview_timeline_start_sec() -> float:
            if primary_cid:
                for cid, cl in selected_rows:
                    if str(cid) == primary_cid:
                        return float((cl or {}).get("timeline_start") or 0.0)
                _tr_p, _cl_p = _find_clip(primary_cid)
                if _cl_p and str(_cl_p.get("type") or "") == "video":
                    return float((_cl_p or {}).get("timeline_start") or 0.0)
            if selected_rows:
                starts = [float((cl or {}).get("timeline_start") or 0.0) for _cid, cl in selected_rows]
                return min(starts) if starts else 0.0
            return 0.0

        def _trim_project_timeline_window(src_project: dict[str, Any], start_sec: float, length_sec: float) -> dict[str, Any]:
            """Cắt project theo cửa sổ timeline để preview đúng vùng đã chọn."""
            out = copy.deepcopy(src_project)
            win_s = max(0.0, float(start_sec))
            win_e = win_s + max(0.1, float(length_sec))
            new_tracks: list[dict[str, Any]] = []
            for tr in out.get("tracks") or []:
                if not isinstance(tr, dict):
                    continue
                ntr = dict(tr)
                nclips: list[dict[str, Any]] = []
                for cl in tr.get("clips") or []:
                    if not isinstance(cl, dict):
                        continue
                    cs = float(cl.get("timeline_start") or 0.0)
                    cd = max(0.0, float(cl.get("duration") or 0.0))
                    ce = cs + cd
                    ov_s = max(cs, win_s)
                    ov_e = min(ce, win_e)
                    if ov_e <= ov_s:
                        continue
                    ncl = dict(cl)
                    ncl["timeline_start"] = max(0.0, ov_s - win_s)
                    ncl["duration"] = max(0.05, ov_e - ov_s)
                    # Đồng bộ source_start/source_end cho clip có nguồn media.
                    if "source_start" in ncl:
                        try:
                            ss = float(cl.get("source_start") or 0.0)
                            ncl["source_start"] = ss + max(0.0, ov_s - cs)
                        except Exception:
                            pass
                    if "source_end" in ncl:
                        try:
                            ncl["source_end"] = float(ncl.get("source_start") or 0.0) + float(ncl.get("duration") or 0.0)
                        except Exception:
                            pass
                    nclips.append(ncl)
                ntr["clips"] = nclips
                new_tracks.append(ntr)
            out["tracks"] = new_tracks
            out["duration"] = max(0.1, float(length_sec))
            return out

        if primary_cid or selected_rows:
            try:
                preview_start = _preview_timeline_start_sec()
                preview_base_project = _trim_project_timeline_window(preview_base_project, preview_start, preview_timeline_window_sec)
                notify(f"Preview theo clip đang chọn (timeline ~{preview_start:.2f}s).")
            except Exception:
                preview_base_project = copy.deepcopy(project)
        ass_path: str | None = None
        if preview_base_project.get("subtitles"):
            try:
                ass_path = sub_mgr.export_ass(
                    preview_base_project,
                    str(ensure_video_editor_layout()["subtitles"] / f"{safe}_preview_burn.ass"),
                )
            except Exception as ex:
                notify(f"Cảnh báo ASS: {ex}")

        def _build_preview_project(max_preview_side: int) -> dict[str, Any]:
            preview_project = copy.deepcopy(preview_base_project)
            # Preview chỉ cần đủ nhìn, ưu tiên nhanh + ổn định để tránh crash ffmpeg trên timeline nặng.
            try:
                pw = int(preview_project.get("width") or 1080)
                ph = int(preview_project.get("height") or 1920)
            except (TypeError, ValueError):
                pw, ph = 1080, 1920
            longest = max(1, pw, ph)
            if longest > max_preview_side:
                scale = float(max_preview_side) / float(longest)
                nw = max(2, int(round(pw * scale)))
                nh = max(2, int(round(ph * scale)))
                if nw % 2:
                    nw += 1
                if nh % 2:
                    nh += 1
                preview_project["width"] = nw
                preview_project["height"] = nh
            return preview_project

        preview_path_var.set(str(out_p))
        lbl_preview.configure(text=f"Đang tạo preview nháp (~20s đầu)…\n{out_p}")
        notify("Đang render preview nháp (nền)…")

        dur = max(5.0, min(25.0, float(preview_base_project.get("duration") or 20)))
        _pv_prog_t = {"t": 0.0, "last_update": time.monotonic(), "started": time.monotonic()}

        def _cancel_preview_watchdog() -> None:
            aid = preview_watchdog_ref.get("after")
            if aid:
                try:
                    root.after_cancel(aid)
                except Exception:
                    pass
            preview_watchdog_ref["after"] = None

        def _arm_preview_watchdog() -> None:
            def _tick() -> None:
                if (not preview_busy_ref.get("busy")) or int(preview_run_ref.get("id") or 0) != run_id:
                    return
                # Đang xuất MP4: không được gọi cancel_all (kể cả preview_busy còn True do race sau prime).
                if int(_export_run_state.get("running") or 0) > 0:
                    preview_watchdog_ref["after"] = root.after(1000, _tick)
                    return
                now = time.monotonic()
                idle_s = now - float(_pv_prog_t.get("last_update") or now)
                elapsed_s = now - float(_pv_prog_t.get("started") or now)
                # Preview nháp 20s mà không có tiến triển quá lâu => coi như treo.
                if elapsed_s >= 120.0 or idle_s >= 60.0:
                    preview_busy_ref["busy"] = False
                    preview_open_after_done_ref["v"] = False
                    preview_open_after_done_ref["with"] = "ffplay"
                    try:
                        exporting_busy = bool(_selected_export_busy_ref.get("busy"))  # type: ignore[name-defined]
                    except Exception:
                        exporting_busy = False
                    if int(_export_run_state.get("running") or 0) > 0:
                        exporting_busy = True
                    if not exporting_busy:
                        try:
                            preview_worker.cancel_all()
                        except Exception:
                            pass
                    notify("Preview nháp bị treo quá lâu, đã tự dừng. Bạn có thể bấm chạy lại.")
                    _schedule_on_main_thread(
                        lambda: lbl_status.configure(text="Preview nháp bị treo, đã dừng.")
                    )
                    try:
                        _flush_auto_preview_pending()
                    except Exception:
                        pass
                    return
                preview_watchdog_ref["after"] = root.after(1000, _tick)

            _cancel_preview_watchdog()
            preview_watchdog_ref["after"] = root.after(1000, _tick)

        def _start_preview_render(*, max_side: int, crf: int, threads: int) -> None:
            # Export đang chạy — không khởi động lại preview (kể cả retry sau cancel_all).
            if int(_export_run_state.get("running") or 0) > 0:
                preview_busy_ref["busy"] = False
                preview_open_after_done_ref["v"] = False
                preview_open_after_done_ref["with"] = "ffplay"
                _cancel_preview_watchdog()
                return
            preview_project = _build_preview_project(max_side)
            errs = validate_export(
                preview_project,
                ffmpeg_path=ffmpeg_bin,
                output_path=str(out_p),
                media_resolver=mm,
                require_contiguous_video_timeline=False,
            )
            if errs:
                preview_busy_ref["busy"] = False
                preview_open_after_done_ref["v"] = False
                preview_open_after_done_ref["with"] = "ffplay"
                _cancel_preview_watchdog()
                messagebox.showerror("Preview nháp", "\n".join(errs))
                try:
                    _flush_auto_preview_pending()
                except Exception:
                    pass
                return
            try:
                cmd = builder.build_export_command(
                    preview_project,
                    str(out_p),
                    ffmpeg_bin=ffmpeg_bin,
                    ass_path=ass_path,
                    output_duration_limit_sec=20.0,
                    encoding_overrides={"preset": "ultrafast", "crf": crf, "threads": threads},
                    lightweight_mode_override=True,
                )
            except Exception as e:
                preview_busy_ref["busy"] = False
                preview_open_after_done_ref["v"] = False
                preview_open_after_done_ref["with"] = "ffplay"
                _cancel_preview_watchdog()
                messagebox.showerror("Preview nháp", str(e))
                try:
                    _flush_auto_preview_pending()
                except Exception:
                    pass
                return

            def done(res: dict[str, Any]) -> None:
                def ui() -> None:
                    if int(preview_run_ref.get("id") or 0) != run_id:
                        return
                    _cancel_preview_watchdog()
                    if res.get("ok"):
                        preview_busy_ref["busy"] = False
                        lbl_preview.configure(text=f"Preview nháp:\n{out_p}\nMở bằng «Mở preview» hoặc ffplay.")
                        notify("Preview nháp xong — mở file để xem.")
                        try:
                            _flush_auto_preview_pending()
                        except Exception:
                            pass
                        if preview_open_after_done_ref.get("v"):
                            how = str(preview_open_after_done_ref.get("with") or "ffplay")
                            preview_open_after_done_ref["v"] = False
                            preview_open_after_done_ref["with"] = "ffplay"
                            if how == "default":
                                _schedule_on_main_thread(
                                    lambda: open_path_with_default_player(str(preview_path_var.get() or ""))
                                )
                            else:
                                _schedule_on_main_thread(
                                    lambda: open_draft_preview_with_ffplay(force_composite=True)
                                )
                        return
                    run_ref["attempt"] += 1
                    if run_ref["attempt"] <= 1:
                        if int(_export_run_state.get("running") or 0) > 0:
                            preview_busy_ref["busy"] = False
                            preview_open_after_done_ref["v"] = False
                            preview_open_after_done_ref["with"] = "ffplay"
                            _cancel_preview_watchdog()
                            try:
                                _flush_auto_preview_pending()
                            except Exception:
                                pass
                            return
                        notify("Preview lỗi lần 1, đang thử lại cấu hình an toàn hơn…")
                        _start_preview_render(max_side=640, crf=36, threads=1)
                        return
                    preview_busy_ref["busy"] = False
                    preview_open_after_done_ref["v"] = False
                    preview_open_after_done_ref["with"] = "ffplay"
                    notify("Lỗi preview nháp — xem hộp thoại.")
                    messagebox.showerror("Preview nháp", res.get("error_message") or "Lỗi")
                    try:
                        _flush_auto_preview_pending()
                    except Exception:
                        pass

                _schedule_on_main_thread(ui)

            def _on_pv_prog(x: float) -> None:
                now = time.monotonic()
                _pv_prog_t["last_update"] = now
                if x < 0.999 and (now - _pv_prog_t["t"]) < 0.25:
                    return
                _pv_prog_t["t"] = now
                _schedule_on_main_thread(
                    lambda: lbl_status.configure(text=f"Preview nháp… {int(x * 100)}%")
                )

            preview_worker.render_thread(
                preview_project,
                str(out_p),
                cmd,
                duration_sec=dur,
                progress_callback=_on_pv_prog,
                done_callback=done,
            )
            _arm_preview_watchdog()

        # Mặc định ưu tiên tốc độ phản hồi khi preview nháp.
        _start_preview_render(max_side=960, crf=33, threads=1)

    _auto_preview_schedule_ref: dict[str, Any] = {"after_id": None}

    def _cancel_auto_preview_debounce() -> None:
        """Hủy preview tự động đang chờ (after) để tránh render nháp từ trạng thái cũ chồng lên bản mới."""
        aid = _auto_preview_schedule_ref.get("after_id")
        if aid:
            try:
                root.after_cancel(aid)
            except Exception:
                pass
        _auto_preview_schedule_ref["after_id"] = None

    _auto_preview_pending_ref: dict[str, Any] = {"want": False, "reason": ""}

    def _flush_auto_preview_pending() -> None:
        """Chạy preview tự động đã xếp hàng sau khi lượt render hiện tại kết thúc."""
        if not _auto_preview_pending_ref.get("want"):
            return
        _auto_preview_pending_ref["want"] = False
        reason = str(_auto_preview_pending_ref.get("reason") or "")
        _auto_preview_pending_ref["reason"] = ""
        if not project:
            return
        if int(_export_run_state.get("running") or 0) > 0:
            _auto_preview_pending_ref["want"] = True
            if reason:
                _auto_preview_pending_ref["reason"] = reason
            return
        if preview_busy_ref.get("busy"):
            _auto_preview_pending_ref["want"] = True
            if reason:
                _auto_preview_pending_ref["reason"] = reason
            return
        _cancel_auto_preview_debounce()
        if reason:
            notify(f"Tự cập nhật preview sau khi áp dụng ({reason})…")
        else:
            notify("Tự cập nhật preview sau khi áp dụng…")
        run_preview_draft()

    def _auto_preview_after_apply(reason: str = "") -> None:
        """Sau thao tác 'Áp dụng', tự render preview để chỉ cần bấm mở xem."""
        if not project:
            return
        if int(_export_run_state.get("running") or 0) > 0:
            _auto_preview_pending_ref["want"] = True
            if reason:
                _auto_preview_pending_ref["reason"] = reason
            return
        _cancel_auto_preview_debounce()
        if preview_busy_ref.get("busy"):
            _auto_preview_pending_ref["want"] = True
            if reason:
                _auto_preview_pending_ref["reason"] = reason
            return
        if reason:
            notify(f"Tự cập nhật preview sau khi áp dụng ({reason})…")
        else:
            notify("Tự cập nhật preview sau khi áp dụng…")
        run_preview_draft()

    def make_thumbnail() -> None:
        if not project:
            return
        if _active_media_kind() == "stock":
            messagebox.showinfo(
                "Thumbnail",
                "Tab «Âm thanh có sẵn» là kho stock — chuyển sang tab Video và chọn video đã import.",
            )
            return
        mid = _selected_media_id()
        if not mid:
            messagebox.showinfo("Thumbnail", "Chọn một dòng media (video).")
            return
        media = _find_media(mid)
        if not media or str(media.get("type")) != "video":
            messagebox.showinfo("Thumbnail", "Chọn media loại video.")
            return
        vp = mm.resolve_media_path_on_disk(media)
        if not vp:
            messagebox.showerror("Thumbnail", "Không tìm thấy file.")
            return
        thumbs = ensure_video_editor_layout()["thumbnails"]
        out = thumbs / f"{mid}_preview.jpg"
        try:
            mm.create_thumbnail(str(vp), str(out))
            preview_path_var.set(str(out))
            lbl_preview.configure(text=f"Thumbnail: {out}")
            notify("Đã tạo thumbnail.")
        except Exception as e:
            messagebox.showerror("Thumbnail", str(e))

    pbar = ttk.Frame(prev_fr)
    pbar.pack(fill=tk.X, pady=6)
    ttk.Button(pbar, text="Ảnh thumbnail", command=make_thumbnail).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(pbar, text="Preview nháp (~20s)", command=run_preview_draft).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(pbar, text="Mở bằng app mặc định", command=open_preview_file).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(pbar, text="Mở bằng ffplay", command=open_draft_preview_with_ffplay).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Label(pbar, text="Khung ffplay").pack(side=tk.LEFT, padx=(10, 4))
    ttk.Combobox(
        pbar,
        textvariable=var_ffplay_view_size,
        values=("Khung nhỏ", "Khung vừa", "Khung lớn"),
        state="readonly",
        width=12,
    ).pack(side=tk.LEFT)

    tl_fr = ttk.LabelFrame(center, text="Timeline — thứ tự clip trên video (Ctrl/Shift: chọn nhiều)", padding=4)
    center.add(tl_fr, weight=2)
    try:
        center.paneconfigure(prev_fr, minsize=72)
        center.paneconfigure(tl_fr, minsize=100)
    except tk.TclError:
        pass
    tl_wrap = ttk.Frame(tl_fr)
    tl_wrap.pack(fill=tk.BOTH, expand=True)
    tl_wrap.columnconfigure(0, weight=1)
    tl_wrap.rowconfigure(0, weight=1)
    tl_nb = ttk.Notebook(tl_wrap)
    tl_nb.grid(row=0, column=0, sticky="nsew")
    tl_tab_detail = ttk.Frame(tl_nb, padding=2)
    tl_tab_grouped = ttk.Frame(tl_nb, padding=2)
    tl_tab_detail.columnconfigure(0, weight=1)
    tl_tab_detail.rowconfigure(0, weight=0)
    tl_tab_detail.rowconfigure(1, weight=1)
    tl_tab_grouped.columnconfigure(0, weight=1)
    tl_tab_grouped.rowconfigure(0, weight=0)
    tl_tab_grouped.rowconfigure(1, weight=1)
    tl_nb.add(tl_tab_detail, text="Chi tiết clip")
    tl_nb.add(tl_tab_grouped, text="Gộp theo video")
    var_tl_video_only = tk.BooleanVar(value=True)
    _tl_sort: dict[str, Any] = {"col": "start", "asc": True}
    _tlg_sort: dict[str, Any] = {"col": "start", "asc": True}
    _edit_sum_sort: dict[str, Any] = {"col": "ts", "asc": True}
    var_tl_name_filter = tk.StringVar(value="")
    var_tlg_name_filter = tk.StringVar(value="")
    var_edit_sum_name_filter = tk.StringVar(value="")
    cols_tl = ("track", "clip_id", "start", "dur", "src0", "src1", "kind", "col_logo", "col_audio", "col_text")
    tree_tl = ttk.Treeview(tl_tab_detail, columns=cols_tl, show="headings", height=10, selectmode="extended")
    heads = {
        "track": "Lớp",
        "clip_id": "Mã clip",
        "start": "Bắt đầu TL (s)",
        "dur": "Độ dài (s)",
        "src0": "Điểm nguồn đầu",
        "src1": "Điểm nguồn cuối",
        "kind": "Loại clip",
        "col_logo": "Logo",
        "col_audio": "Âm thanh",
        "col_text": "Chữ",
    }
    widths_tl = {
        "track": 72,
        "clip_id": 96,
        "start": 56,
        "dur": 56,
        "src0": 72,
        "src1": 72,
        "kind": 52,
        "col_logo": 52,
        "col_audio": 64,
        "col_text": 44,
    }
    heads_tl_base = dict(heads)
    for c in cols_tl:
        tree_tl.column(c, width=widths_tl[c], stretch=c in ("clip_id", "kind"))
        tree_tl.heading(c, text=heads_tl_base[c])
    tree_tl.tag_configure("all_done", foreground="#0b4f2f", background="#d8f3dc")
    tree_tl.tag_configure("partial_done", foreground="#7a4b00", background="#fff3cd")
    tree_tl.tag_configure("not_done", foreground="#495057", background="#f1f3f5")
    st = ttk.Scrollbar(tl_tab_detail, orient=tk.VERTICAL, command=tree_tl.yview)
    tree_tl.configure(yscrollcommand=st.set)
    filt_tl_fr = ttk.Frame(tl_tab_detail)
    filt_tl_fr.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 2))
    ttk.Label(filt_tl_fr, text="Lọc:").pack(side=tk.LEFT)
    ttk.Entry(filt_tl_fr, textvariable=var_tl_name_filter, width=22).pack(side=tk.LEFT, padx=(4, 2))
    ttk.Label(filt_tl_fr, text="(mã clip, lớp, loại…)", font=("Segoe UI", 8), foreground="#666").pack(side=tk.LEFT)
    btn_tl_filt_clear = ttk.Button(filt_tl_fr, text="Xóa", command=lambda: None)
    btn_tl_filt_clear.pack(side=tk.LEFT, padx=(6, 0))
    tree_tl.grid(row=1, column=0, sticky="nsew")
    st.grid(row=1, column=1, sticky="ns")
    cols_tlg = ("idx", "video_name", "start", "dur", "audio", "logo", "text")
    tree_tl_grouped = ttk.Treeview(tl_tab_grouped, columns=cols_tlg, show="headings", height=10, selectmode="extended")
    heads_tlg = {
        "idx": "#",
        "video_name": "Video",
        "start": "Bắt đầu TL (s)",
        "dur": "Độ dài (s)",
        "audio": "Âm thanh",
        "logo": "Logo",
        "text": "Chữ",
    }
    widths_tlg = {"idx": 34, "video_name": 220, "start": 96, "dur": 86, "audio": 80, "logo": 72, "text": 62}
    heads_tlg_base = dict(heads_tlg)
    for c in cols_tlg:
        tree_tl_grouped.column(c, width=widths_tlg[c], stretch=True if c == "video_name" else False)
        tree_tl_grouped.heading(c, text=heads_tlg_base[c])
    stg = ttk.Scrollbar(tl_tab_grouped, orient=tk.VERTICAL, command=tree_tl_grouped.yview)
    tree_tl_grouped.configure(yscrollcommand=stg.set)
    filt_tlg_fr = ttk.Frame(tl_tab_grouped)
    filt_tlg_fr.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 2))
    ttk.Label(filt_tlg_fr, text="Lọc video:").pack(side=tk.LEFT)
    ttk.Entry(filt_tlg_fr, textvariable=var_tlg_name_filter, width=26).pack(side=tk.LEFT, padx=(4, 2))
    btn_tlg_filt_clear = ttk.Button(filt_tlg_fr, text="Xóa", command=lambda: None)
    btn_tlg_filt_clear.pack(side=tk.LEFT, padx=(6, 0))
    tree_tl_grouped.grid(row=1, column=0, sticky="nsew")
    stg.grid(row=1, column=1, sticky="ns")
    grouped_video_to_clip: dict[str, str] = {}

    ttk.Label(
        tl_fr,
        text="Chọn một dòng để chỉnh bên phải; chọn nhiều clip «video» rồi mở tab «Chỉnh clip» để sửa âm lượng / tốc độ hàng loạt.",
        foreground="#444",
        font=("Segoe UI", 8),
        wraplength=520,
    ).pack(fill=tk.X, anchor="w", padx=2, pady=(2, 0))
    tl_actions = ttk.Frame(tl_fr)
    tl_actions.pack(fill=tk.X, pady=4)
    ttk.Checkbutton(
        tl_actions,
        text="Gọn: chỉ hiện 1 hàng / video (ẩn logo, audio, chữ)",
        variable=var_tl_video_only,
        command=lambda: (_cancel_tl_tlg_filter_debounce(), refresh_timeline()),
    ).pack(side=tk.LEFT, padx=(0, 10))
    ttk.Label(
        tl_fr,
        text="Cắt đầu/cuối file nguồn: dùng tab «Chỉnh clip» (1 clip hoặc hàng loạt) — tránh lệch độ dài với âm thanh.",
        foreground="#1a4480",
        font=("Segoe UI", 8),
        wraplength=520,
    ).pack(fill=tk.X, anchor="w", padx=2, pady=(0, 4))
    edit_sum_fr = ttk.LabelFrame(
        tl_fr,
        text="Nhận diện chỉnh sửa theo video (mỗi video 1 dòng)",
        padding=4,
    )
    edit_sum_fr.pack(fill=tk.BOTH, expand=False, pady=(2, 4))
    edit_sum_fr.columnconfigure(0, weight=1)
    edit_sum_fr.rowconfigure(2, weight=1)
    legend_fr = ttk.Frame(edit_sum_fr)
    legend_fr.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 4))
    tk.Label(legend_fr, text="   ", bg="#d8f3dc").pack(side=tk.LEFT)
    ttk.Label(legend_fr, text="Đủ logo + âm thanh + chữ").pack(side=tk.LEFT, padx=(4, 10))
    tk.Label(legend_fr, text="   ", bg="#fff3cd").pack(side=tk.LEFT)
    ttk.Label(legend_fr, text="Đã chỉnh một phần").pack(side=tk.LEFT, padx=(4, 10))
    tk.Label(legend_fr, text="   ", bg="#f1f3f5").pack(side=tk.LEFT)
    ttk.Label(legend_fr, text="Chưa chỉnh").pack(side=tk.LEFT, padx=(4, 0))
    cols_edit = ("idx", "video", "logo", "audio", "text", "status")
    tree_edit_sum = ttk.Treeview(edit_sum_fr, columns=cols_edit, show="headings", height=5, selectmode="extended")
    heads_edit = {
        "idx": "#",
        "video": "Video",
        "logo": "Logo",
        "audio": "Âm thanh",
        "text": "Chữ",
        "status": "Tình trạng",
    }
    widths_edit = {"idx": 36, "video": 210, "logo": 62, "audio": 82, "text": 52, "status": 300}
    heads_edit_base = dict(heads_edit)
    for c in cols_edit:
        tree_edit_sum.column(c, width=widths_edit[c], stretch=True if c in ("video", "status") else False)
        tree_edit_sum.heading(c, text=heads_edit_base[c])
    sy_edit = ttk.Scrollbar(edit_sum_fr, orient=tk.VERTICAL, command=tree_edit_sum.yview)
    tree_edit_sum.configure(yscrollcommand=sy_edit.set)
    filt_sum_fr = ttk.Frame(edit_sum_fr)
    filt_sum_fr.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 4))
    ttk.Label(filt_sum_fr, text="Lọc:").pack(side=tk.LEFT)
    ttk.Entry(filt_sum_fr, textvariable=var_edit_sum_name_filter, width=28).pack(side=tk.LEFT, padx=(4, 2))
    ttk.Label(filt_sum_fr, text="(tên video, tình trạng…)", font=("Segoe UI", 8), foreground="#666").pack(side=tk.LEFT)
    btn_edit_sum_filt_clear = ttk.Button(filt_sum_fr, text="Xóa", command=lambda: None)
    btn_edit_sum_filt_clear.pack(side=tk.LEFT, padx=(6, 0))
    tree_edit_sum.grid(row=2, column=0, sticky="nsew")
    sy_edit.grid(row=2, column=1, sticky="ns")
    tree_edit_sum.tag_configure("all_done", foreground="#0b4f2f", background="#d8f3dc")
    tree_edit_sum.tag_configure("partial_done", foreground="#7a4b00", background="#fff3cd")
    tree_edit_sum.tag_configure("not_done", foreground="#495057", background="#f1f3f5")

    def _tree_copy_selected_rows(tv: ttk.Treeview) -> None:
        sel = list(tv.selection())
        if not sel:
            return
        lines: list[str] = []
        for iid in sel:
            vals = [str(v) for v in (tv.item(iid, "values") or ())]
            if vals:
                lines.append("\t".join(vals))
        if not lines:
            return
        txt = "\n".join(lines)
        root.clipboard_clear()
        root.clipboard_append(txt)
        notify(f"Đã copy {len(lines)} dòng.")

    def _tree_copy_selected_links(tv: ttk.Treeview) -> None:
        sel = list(tv.selection())
        if not sel:
            return
        links: list[str] = []
        for iid in sel:
            vals = [str(v) for v in (tv.item(iid, "values") or ())]
            for v in vals:
                s = v.strip()
                if not s:
                    continue
                low = s.lower()
                if "http://" in low or "https://" in low:
                    links.append(s)
        if not links:
            notify("Không thấy link trong dòng đã chọn.")
            return
        txt = "\n".join(dict.fromkeys(links))
        root.clipboard_clear()
        root.clipboard_append(txt)
        notify(f"Đã copy {len(txt.splitlines())} link.")

    def _install_tree_multi_actions(
        tv: ttk.Treeview,
        *,
        view_selected_label: str | None = None,
        view_selected_fn: Callable[[], None] | None = None,
    ) -> None:
        def _select_all(_e: Any = None) -> str:
            kids = tv.get_children("")
            if kids:
                tv.selection_set(kids)
                tv.focus(kids[0])
            return "break"

        def _clear_sel() -> None:
            cur = tv.selection()
            if cur:
                tv.selection_remove(*cur)

        def _on_context_menu(e: Any) -> None:
            row = tv.identify_row(e.y)
            if row:
                cur_sel = set(tv.selection())
                if row not in cur_sel:
                    tv.selection_set((row,))
                    try:
                        refresh_inspector()
                    except Exception:
                        pass
            m = tk.Menu(root, tearoff=0)
            m.add_command(label="Chọn hết (Ctrl+A)", command=lambda: _select_all())
            m.add_command(label="Bỏ chọn", command=_clear_sel)
            m.add_separator()
            if view_selected_label and callable(view_selected_fn):
                m.add_command(label=view_selected_label, command=view_selected_fn)
                m.add_separator()
            m.add_command(label="Copy dòng đã chọn", command=lambda: _tree_copy_selected_rows(tv))
            m.add_command(label="Copy link trong dòng đã chọn", command=lambda: _tree_copy_selected_links(tv))
            try:
                m.tk_popup(e.x_root, e.y_root)
            finally:
                m.grab_release()

        tv.bind("<Control-a>", _select_all, add="+")
        tv.bind("<Control-A>", _select_all, add="+")
        tv.bind("<Button-3>", _on_context_menu, add="+")
        if callable(view_selected_fn):

            def _on_treeview_double_click(e: Any) -> str | None:
                # Đồng bộ selection với dòng dưới con trỏ — double-click đôi khi không cập nhật selection trước khi handler chạy.
                try:
                    region = str(tv.identify_region(e.x, e.y) or "")
                except Exception:
                    region = ""
                if region == "heading":
                    return None
                row = ""
                try:
                    row = str(tv.identify_row(e.y) or "")
                except Exception:
                    row = ""
                if row:
                    try:
                        tv.selection_set(row)
                        tv.focus(row)
                    except Exception:
                        pass
                view_selected_fn()
                return "break"

            tv.bind("<Double-1>", _on_treeview_double_click, add="+")

    _install_tree_multi_actions(tree_tl)
    _install_tree_multi_actions(tree_tl_grouped)
    _install_tree_multi_actions(tree_edit_sum)
    for _kind, _tv in media_trees.items():
        if _kind == "video":
            _install_tree_multi_actions(
                _tv,
                view_selected_label="Xem file nguồn (ffplay — như timeline)",
                view_selected_fn=lambda: open_with_ffplay(prefer_selected_library_media=True),
            )
        elif _kind in ("image", "audio"):
            _install_tree_multi_actions(
                _tv,
                view_selected_label="Mở file bằng ứng dụng mặc định",
                view_selected_fn=open_selected_imported_media_default,
            )
        else:
            _install_tree_multi_actions(_tv)

    def _find_media(mid: str) -> dict[str, Any] | None:
        if not project:
            return None
        for m in project.get("media") or []:
            if isinstance(m, dict) and str(m.get("id")) == mid:
                return m
        return None

    def _media_id_valid_for_type(mid: str, want_type: str) -> bool:
        m = _find_media(str(mid or "").strip())
        return bool(m) and str(m.get("type") or "") == want_type

    def _collect_timeline_and_bgm_media_ids() -> set[str]:
        out: set[str] = set()
        if not project:
            return out
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict):
                continue
            for cl in tr.get("clips") or []:
                if not isinstance(cl, dict):
                    continue
                mid = str(cl.get("media_id") or "").strip()
                if mid:
                    out.add(mid)
        au = project.get("audio_settings") or {}
        if isinstance(au, dict):
            for bg in au.get("bgm") or []:
                if isinstance(bg, dict):
                    mid = str(bg.get("media_id") or "").strip()
                    if mid:
                        out.add(mid)
        return out

    def _clip_end_time(clip: dict[str, Any]) -> float:
        start = float(clip.get("timeline_start") or 0.0)
        dur = float(clip.get("duration") or 0.0)
        return start + max(0.0, dur)

    def _is_overlap(a0: float, a1: float, b0: float, b1: float) -> bool:
        return min(a1, b1) > max(a0, b0)

    def refresh_video_edit_summary() -> None:
        tree_edit_sum.delete(*tree_edit_sum.get_children())
        if not project:
            _ve_apply_tree_heading_marks(tree_edit_sum, cols_edit, heads_edit_base, "__none__", True)
            return
        tracks = [t for t in (project.get("tracks") or []) if isinstance(t, dict)]
        media_by_id: dict[str, dict[str, Any]] = {}
        for m in project.get("media") or []:
            if isinstance(m, dict):
                mid = str(m.get("id") or "").strip()
                if mid:
                    media_by_id[mid] = m

        video_rows: list[tuple[str, float, float, str]] = []
        overlay_ranges: list[tuple[float, float]] = []
        text_ranges: list[tuple[float, float]] = []
        audio_ranges: list[tuple[float, float]] = []

        for tr in tracks:
            ttype = str(tr.get("type") or "")
            for cl in tr.get("clips") or []:
                if not isinstance(cl, dict):
                    continue
                ctype = str(cl.get("type") or "")
                ts = float(cl.get("timeline_start") or 0.0)
                te = _clip_end_time(cl)
                if ttype == "video" and ctype == "video":
                    mid = str(cl.get("media_id") or "").strip()
                    m = media_by_id.get(mid) or {}
                    name = str(m.get("original_name") or m.get("name") or mid or "video")
                    video_rows.append((str(cl.get("id") or ""), ts, te, name))
                elif ttype == "overlay" and ctype == "image":
                    overlay_ranges.append((ts, te))
                elif ctype == "text":
                    text_ranges.append((ts, te))
                elif ttype == "audio" and ctype == "audio":
                    audio_ranges.append((ts, te))

        for bg in (project.get("audio_settings") or {}).get("bgm") or []:
            if not isinstance(bg, dict):
                continue
            ts = float(bg.get("timeline_start") or 0.0)
            dur = float(bg.get("duration") or project.get("duration") or 0.0)
            audio_ranges.append((ts, ts + max(0.0, dur)))

        specs: list[dict[str, Any]] = []
        for cid, ts, te, name in video_rows:
            has_logo = any(_is_overlap(ts, te, a0, a1) for a0, a1 in overlay_ranges)
            has_audio = any(_is_overlap(ts, te, a0, a1) for a0, a1 in audio_ranges)
            has_text = any(_is_overlap(ts, te, a0, a1) for a0, a1 in text_ranges)
            tags: list[str] = []
            if has_logo:
                tags.append("logo")
            if has_audio:
                tags.append("âm thanh")
            if has_text:
                tags.append("chữ")
            status = "Đã thêm " + ", ".join(tags) if tags else "Chưa thêm logo/âm thanh/chữ"
            done_count = int(has_logo) + int(has_audio) + int(has_text)
            row_tag = "all_done" if done_count == 3 else "partial_done" if done_count > 0 else "not_done"
            specs.append(
                {
                    "cid": str(cid or ""),
                    "ts": float(ts),
                    "te": float(te),
                    "name": str(name),
                    "has_logo": bool(has_logo),
                    "has_audio": bool(has_audio),
                    "has_text": bool(has_text),
                    "status": status,
                    "row_tag": row_tag,
                }
            )

        needle = str(var_edit_sum_name_filter.get() or "").strip().lower()
        if needle:
            kept: list[dict[str, Any]] = []
            for s in specs:
                blob = f"{s['cid']} {s['name']} {s['status']}".lower()
                if needle in blob:
                    kept.append(s)
            specs = kept

        scol = str(_edit_sum_sort.get("col") or "ts")
        asc = bool(_edit_sum_sort.get("asc", True))

        def _sum_sort_key(s: dict[str, Any]) -> Any:
            if scol == "ts":
                return float(s["ts"])
            if scol == "video":
                return s["name"].lower()
            if scol == "logo":
                return 1 if s["has_logo"] else 0
            if scol == "audio":
                return 1 if s["has_audio"] else 0
            if scol == "text":
                return 1 if s["has_text"] else 0
            if scol == "status":
                return s["status"].lower()
            return float(s["ts"])

        specs.sort(key=_sum_sort_key, reverse=not asc)
        arrow_col = "idx" if scol == "ts" else scol
        _ve_apply_tree_heading_marks(tree_edit_sum, cols_edit, heads_edit_base, arrow_col, asc)

        for i, s in enumerate(specs, start=1):
            tree_edit_sum.insert(
                "",
                tk.END,
                iid=f"sum_{s['cid'] or i}",
                values=(
                    i,
                    s["name"],
                    "Có" if s["has_logo"] else "Chưa",
                    "Có" if s["has_audio"] else "Chưa",
                    "Có" if s["has_text"] else "Chưa",
                    s["status"],
                ),
                tags=(s["row_tag"],),
            )

    def refresh_media_tree() -> None:
        for key in ("video", "image", "audio"):
            tw = media_trees[key]
            tw.delete(*tw.get_children())
        if not project:
            refresh_stock_audio_box()
            return
        needle = str(var_media_name_filter.get() or "").strip().lower()
        only_tl = bool(var_media_only_timeline.get())
        used_mids = _collect_timeline_and_bgm_media_ids() if only_tl else set()
        for m in project.get("media") or []:
            if not isinstance(m, dict):
                continue
            mid = str(m.get("id"))
            mt = str(m.get("type") or "")
            if only_tl and mid not in used_mids:
                continue
            display_name = str(m.get("original_name") or "")
            if needle:
                blob = f"{display_name} {mid}".lower()
                if needle not in blob:
                    continue
            w, h = int(m.get("width") or 0), int(m.get("height") or 0)
            res = f"{w}x{h}" if w and h else ""
            vals = (
                display_name,
                mt,
                f"{float(m.get('duration') or 0):.2f}",
                res,
            )

            def _insert_to(tr: ttk.Treeview) -> None:
                tr.insert("", tk.END, iid=mid, values=vals, tags=(mid,))

            if mt in ("video", "image", "audio"):
                _insert_to(media_trees[mt])
        refresh_stock_audio_box()

    cb_media_timeline_only.configure(command=refresh_media_tree)

    def _on_media_filter_key(_e: Any = None) -> None:
        refresh_media_tree()

    ent_media_filter.bind("<KeyRelease>", _on_media_filter_key)

    def refresh_timeline() -> None:
        # Giữ selection qua lần rebuild; nếu mất selection thì inspector tưởng user bỏ chọn
        # và mọi thao tác «tự áp dụng» trông như «không chạy».
        prev_tl_sel = [str(x) for x in tree_tl.selection()]
        prev_focus_tl = str(tree_tl.focus() or "").strip()
        prev_grp_sel = [str(x) for x in tree_tl_grouped.selection()]
        prev_grouped_map = dict(grouped_video_to_clip)
        tree_tl.delete(*tree_tl.get_children())
        tree_tl_grouped.delete(*tree_tl_grouped.get_children())
        grouped_video_to_clip.clear()
        refresh_video_edit_summary()
        if not project:
            _ve_apply_tree_heading_marks(tree_tl, cols_tl, heads_tl_base, "__none__", True)
            _ve_apply_tree_heading_marks(tree_tl_grouped, cols_tlg, heads_tlg_base, "__none__", True)
            return
        media_by_id: dict[str, dict[str, Any]] = {}
        for m in project.get("media") or []:
            if isinstance(m, dict):
                mid = str(m.get("id") or "").strip()
                if mid:
                    media_by_id[mid] = m
        overlay_ranges: list[tuple[float, float]] = []
        text_ranges: list[tuple[float, float]] = []
        audio_ranges: list[tuple[float, float]] = []
        grouped_rows: list[tuple[str, float, float, str]] = []
        tl_rows: list[tuple[str, str, float, float, Any, Any, str, str]] = []
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict):
                continue
            tname = str(tr.get("type") or "")
            for cl in tr.get("clips") or []:
                if not isinstance(cl, dict):
                    continue
                cid = str(cl.get("id"))
                ts = float(cl.get("timeline_start") or 0)
                du = float(cl.get("duration") or 0)
                ss = float(cl.get("source_start") or 0) if "source_start" in cl else ""
                se = float(cl.get("source_end") or 0) if "source_end" in cl else ""
                kind = str(cl.get("type") or "")
                tl_rows.append((cid, tname, ts, du, ss, se, kind))
                if tname == "video" and kind == "video":
                    mid = str(cl.get("media_id") or "").strip()
                    media = media_by_id.get(mid) or {}
                    vname = str(media.get("original_name") or media.get("name") or mid or cid)
                    grouped_rows.append((cid, ts, ts + max(0.0, du), vname))
                elif tname == "overlay" and kind == "image":
                    overlay_ranges.append((ts, ts + max(0.0, du)))
                elif tname == "audio" and kind == "audio":
                    audio_ranges.append((ts, ts + max(0.0, du)))
                elif kind == "text":
                    text_ranges.append((ts, ts + max(0.0, du)))
        for bg in (project.get("audio_settings") or {}).get("bgm") or []:
            if not isinstance(bg, dict):
                continue
            ts = float(bg.get("timeline_start") or 0.0)
            dur = float(bg.get("duration") or project.get("duration") or 0.0)
            audio_ranges.append((ts, ts + max(0.0, dur)))
        video_only = bool(var_tl_video_only.get())
        needle_tl = str(var_tl_name_filter.get() or "").strip().lower()
        detail_specs: list[dict[str, Any]] = []
        for cid, tname, ts, du, ss, se, kind in tl_rows:
            show_row = (not video_only) or (tname == "video" and kind == "video")
            if not show_row:
                continue
            te = ts + max(0.0, du)
            if tname == "video" and kind == "video":
                has_logo = any(_is_overlap(ts, te, a0, a1) for a0, a1 in overlay_ranges)
                has_audio = any(_is_overlap(ts, te, a0, a1) for a0, a1 in audio_ranges)
                has_text = any(_is_overlap(ts, te, a0, a1) for a0, a1 in text_ranges)
                lv = "Có" if has_logo else "Chưa"
                av = "Có" if has_audio else "Chưa"
                tv = "Có" if has_text else "Chưa"
                done_count = int(has_logo) + int(has_audio) + int(has_text)
                row_tag = "all_done" if done_count == 3 else "partial_done" if done_count > 0 else "not_done"
                row_tags = (row_tag,)
            else:
                lv, av, tv = "—", "—", "—"
                row_tags = ()
            if needle_tl:
                blob = f"{cid} {tname} {kind}".lower()
                if needle_tl not in blob:
                    continue
            detail_specs.append(
                {
                    "cid": cid,
                    "tname": tname,
                    "ts": ts,
                    "du": du,
                    "ss": ss,
                    "se": se,
                    "kind": kind,
                    "lv": lv,
                    "av": av,
                    "tv": tv,
                    "row_tags": row_tags,
                }
            )

        dcol = str(_tl_sort.get("col") or "start")
        dasc = bool(_tl_sort.get("asc", True))

        def _detail_sort_key(d: dict[str, Any]) -> Any:
            if dcol == "track":
                return str(d["tname"]).lower()
            if dcol == "clip_id":
                return str(d["cid"]).lower()
            if dcol == "start":
                return float(d["ts"])
            if dcol == "dur":
                return float(d["du"])
            if dcol == "src0":
                if d["ss"] == "":
                    return float("-1e30")
                return float(d["ss"])
            if dcol == "src1":
                if d["se"] == "":
                    return float("-1e30")
                return float(d["se"])
            if dcol == "kind":
                return str(d["kind"]).lower()
            if dcol == "col_logo":
                return str(d["lv"]).lower()
            if dcol == "col_audio":
                return str(d["av"]).lower()
            if dcol == "col_text":
                return str(d["tv"]).lower()
            return float(d["ts"])

        detail_specs.sort(key=_detail_sort_key, reverse=not dasc)
        _ve_apply_tree_heading_marks(tree_tl, cols_tl, heads_tl_base, dcol, dasc)

        for d in detail_specs:
            cid = str(d["cid"])
            tree_tl.insert(
                "",
                tk.END,
                iid=cid,
                values=(
                    d["tname"],
                    cid[:12] + "…",
                    f"{float(d['ts']):.2f}",
                    f"{float(d['du']):.2f}",
                    f"{float(d['ss']):.2f}" if d["ss"] != "" else "",
                    f"{float(d['se']):.2f}" if d["se"] != "" else "",
                    d["kind"],
                    d["lv"],
                    d["av"],
                    d["tv"],
                ),
                tags=d["row_tags"],
            )

        needle_g = str(var_tlg_name_filter.get() or "").strip().lower()
        grouped_specs: list[dict[str, Any]] = []
        for cid, ts, te, vname in grouped_rows:
            if needle_g and needle_g not in str(vname).lower() and needle_g not in str(cid).lower():
                continue
            has_audio = any(_is_overlap(ts, te, b0, b1) for b0, b1 in audio_ranges)
            has_logo = any(_is_overlap(ts, te, b0, b1) for b0, b1 in overlay_ranges)
            has_text = any(_is_overlap(ts, te, b0, b1) for b0, b1 in text_ranges)
            grouped_specs.append(
                {
                    "cid": cid,
                    "ts": float(ts),
                    "te": float(te),
                    "vname": str(vname),
                    "has_audio": has_audio,
                    "has_logo": has_logo,
                    "has_text": has_text,
                }
            )

        gcol = str(_tlg_sort.get("col") or "start")
        gasc = bool(_tlg_sort.get("asc", True))

        def _grouped_sort_key(s: dict[str, Any]) -> Any:
            if gcol == "idx":
                return float(s["ts"])
            if gcol == "video_name":
                return str(s["vname"]).lower()
            if gcol == "start":
                return float(s["ts"])
            if gcol == "dur":
                return max(0.0, float(s["te"]) - float(s["ts"]))
            if gcol == "audio":
                return 1 if s["has_audio"] else 0
            if gcol == "logo":
                return 1 if s["has_logo"] else 0
            if gcol == "text":
                return 1 if s["has_text"] else 0
            return float(s["ts"])

        grouped_specs.sort(key=_grouped_sort_key, reverse=not gasc)
        _ve_apply_tree_heading_marks(tree_tl_grouped, cols_tlg, heads_tlg_base, gcol, gasc)

        for i, s in enumerate(grouped_specs, start=1):
            gid = f"g_{i}"
            grouped_video_to_clip[gid] = s["cid"]
            ts = float(s["ts"])
            te = float(s["te"])
            tree_tl_grouped.insert(
                "",
                tk.END,
                iid=gid,
                values=(
                    i,
                    s["vname"],
                    f"{ts:.2f}",
                    f"{max(0.0, te - ts):.2f}",
                    "Có" if s["has_audio"] else "Chưa",
                    "Có" if s["has_logo"] else "Chưa",
                    "Có" if s["has_text"] else "Chưa",
                ),
            )

        restore_tl: list[str] = []
        for cid in prev_tl_sel:
            if tree_tl.exists(cid):
                restore_tl.append(cid)
        if not restore_tl and prev_grp_sel:
            seen_gr: set[str] = set()
            for g in prev_grp_sel:
                cid = str(prev_grouped_map.get(str(g)) or "").strip()
                if cid and tree_tl.exists(cid) and cid not in seen_gr:
                    seen_gr.add(cid)
                    restore_tl.append(cid)
        if restore_tl:
            try:
                _suppress_tl_inspector_refresh["v"] = True
                try:
                    tree_tl.selection_set(tuple(restore_tl))
                    focus_tl = prev_focus_tl if prev_focus_tl in restore_tl else restore_tl[-1]
                    tree_tl.focus(focus_tl)
                except Exception:
                    pass
                if len(restore_tl) == 1:
                    cid0 = restore_tl[0]
                    for gid, cid in grouped_video_to_clip.items():
                        if str(cid) == str(cid0):
                            try:
                                tree_tl_grouped.selection_set((gid,))
                                tree_tl_grouped.focus(gid)
                            except Exception:
                                pass
                            break
            finally:
                _suppress_tl_inspector_refresh["v"] = False
            refresh_inspector()
        elif prev_tl_sel or prev_grp_sel:
            # Đã rebuild timeline nhưng không khôi phục được selection (clip mất / lọc) — tránh inspector treo clip cũ.
            refresh_inspector()

        if bool(var_media_only_timeline.get()):
            refresh_media_tree()

    _tl_filter_debounce_after: dict[str, Any] = {"id": None}
    _edit_sum_filter_debounce_after: dict[str, Any] = {"id": None}
    _ve_ft_sup: dict[str, str] = {"k": ""}

    def _cancel_tl_tlg_filter_debounce() -> None:
        aid = _tl_filter_debounce_after.get("id")
        if aid is not None:
            try:
                root.after_cancel(aid)
            except Exception:
                pass
        _tl_filter_debounce_after["id"] = None

    def _fire_debounced_refresh_timeline() -> None:
        _tl_filter_debounce_after["id"] = None
        refresh_timeline()

    def _schedule_debounced_refresh_timeline() -> None:
        _cancel_tl_tlg_filter_debounce()
        _tl_filter_debounce_after["id"] = root.after(
            _VE_TIMELINE_FILTER_DEBOUNCE_MS, _fire_debounced_refresh_timeline
        )

    def _cancel_edit_sum_filter_debounce() -> None:
        aid = _edit_sum_filter_debounce_after.get("id")
        if aid is not None:
            try:
                root.after_cancel(aid)
            except Exception:
                pass
        _edit_sum_filter_debounce_after["id"] = None

    def _fire_debounced_refresh_edit_sum() -> None:
        _edit_sum_filter_debounce_after["id"] = None
        refresh_video_edit_summary()

    def _schedule_debounced_refresh_edit_sum() -> None:
        _cancel_edit_sum_filter_debounce()
        _edit_sum_filter_debounce_after["id"] = root.after(
            _VE_TIMELINE_FILTER_DEBOUNCE_MS, _fire_debounced_refresh_edit_sum
        )

    def _clear_tl_name_filter_cmd() -> None:
        _cancel_tl_tlg_filter_debounce()
        _ve_ft_sup["k"] = "tl"
        try:
            var_tl_name_filter.set("")
        finally:
            _ve_ft_sup["k"] = ""
        refresh_timeline()

    def _clear_tlg_name_filter_cmd() -> None:
        _cancel_tl_tlg_filter_debounce()
        _ve_ft_sup["k"] = "tlg"
        try:
            var_tlg_name_filter.set("")
        finally:
            _ve_ft_sup["k"] = ""
        refresh_timeline()

    def _clear_edit_sum_name_filter_cmd() -> None:
        _cancel_edit_sum_filter_debounce()
        _ve_ft_sup["k"] = "sum"
        try:
            var_edit_sum_name_filter.set("")
        finally:
            _ve_ft_sup["k"] = ""
        refresh_video_edit_summary()

    def _on_tl_heading_click(col: str) -> None:
        _cancel_tl_tlg_filter_debounce()
        _ve_tree_sort_toggle(_tl_sort, col)
        refresh_timeline()

    def _on_tlg_heading_click(col: str) -> None:
        _cancel_tl_tlg_filter_debounce()
        _ve_tree_sort_toggle(_tlg_sort, col)
        refresh_timeline()

    def _on_edit_sum_heading_click(col: str) -> None:
        _cancel_edit_sum_filter_debounce()
        real = "ts" if col == "idx" else col
        _ve_tree_sort_toggle(_edit_sum_sort, real)
        refresh_video_edit_summary()

    for _c in cols_tl:
        tree_tl.heading(_c, command=lambda cc=_c: _on_tl_heading_click(cc))
    for _c in cols_tlg:
        tree_tl_grouped.heading(_c, command=lambda cc=_c: _on_tlg_heading_click(cc))
    for _c in cols_edit:
        tree_edit_sum.heading(_c, command=lambda cc=_c: _on_edit_sum_heading_click(cc))

    def _on_tl_filter_trace(*_a: Any) -> None:
        if _ve_ft_sup.get("k") == "tl":
            return
        _schedule_debounced_refresh_timeline()

    def _on_tlg_filter_trace(*_a: Any) -> None:
        if _ve_ft_sup.get("k") == "tlg":
            return
        _schedule_debounced_refresh_timeline()

    def _on_edit_sum_filter_trace(*_a: Any) -> None:
        if _ve_ft_sup.get("k") == "sum":
            return
        _schedule_debounced_refresh_edit_sum()

    var_tl_name_filter.trace_add("write", _on_tl_filter_trace)
    var_tlg_name_filter.trace_add("write", _on_tlg_filter_trace)
    var_edit_sum_name_filter.trace_add("write", _on_edit_sum_filter_trace)

    btn_tl_filt_clear.configure(command=_clear_tl_name_filter_cmd)
    btn_tlg_filt_clear.configure(command=_clear_tlg_name_filter_cmd)
    btn_edit_sum_filt_clear.configure(command=_clear_edit_sum_name_filter_cmd)

    def _clip_ids_from_edit_sum_selection() -> list[str]:
        """Các clip_id từ bảng «Nhận diện chỉnh sửa» (iid dạng sum_<clip_id>)."""
        out: list[str] = []
        seen: set[str] = set()
        try:
            for iid in tree_edit_sum.selection():
                s = str(iid)
                if not s.startswith("sum_"):
                    continue
                cid = s[4:].strip()
                if not cid:
                    continue
                try:
                    ok = bool(tree_tl.exists(cid))
                except Exception:
                    ok = False
                if ok and cid not in seen:
                    seen.add(cid)
                    out.append(cid)
        except Exception:
            pass
        return out

    def _timeline_merged_clip_ids_from_trees() -> list[str]:
        """
        Chỉ lấy clip từ tab timeline đang xem (chi tiết vs gộp) — không gộp hai nguồn,
        tránh preview / chỉnh nhanh dùng nhầm clip do selection cũ ở tab kia.
        """
        selected_ids: list[str] = []
        try:
            on_grouped_tab = str(tl_nb.select()) == str(tl_tab_grouped)
        except Exception:
            on_grouped_tab = False
        if on_grouped_tab:
            for gid in tree_tl_grouped.selection():
                cid = str(grouped_video_to_clip.get(str(gid) or "") or "").strip()
                if cid:
                    selected_ids.append(cid)
            if not selected_ids:
                selected_ids.extend(str(x) for x in tree_tl.selection())
        else:
            selected_ids.extend(str(x) for x in tree_tl.selection())
        return selected_ids

    def refresh_inspector() -> None:
        # Bỏ ref cũ trước khi dựng lại UI — tránh nút «Áp dụng tất cả» gọi hàm áp của lần chọn trước.
        _apply_batch_video_ref["fn"] = None
        _apply_transform_subset_ref["fn"] = None
        _br = _ve_batch_reset_bar_ref.get("fr")
        if _br is not None:
            try:
                _br.pack_forget()
            except tk.TclError:
                pass
            for _w in _br.winfo_children():
                _w.destroy()
        for w in insp_grid.winfo_children():
            w.destroy()
        nonlocal selected_clip_id
        if not project:
            ttk.Label(insp_grid, text="Chưa có project.").grid(row=0, column=0, sticky="w")
            return
        sel_list: list[str] = []
        seen_sel: set[str] = set()
        for cid in _timeline_merged_clip_ids_from_trees():
            s = str(cid).strip()
            if s and s not in seen_sel:
                seen_sel.add(s)
                sel_list.append(s)
        for cid in _clip_ids_from_edit_sum_selection():
            s = str(cid).strip()
            if s and s not in seen_sel:
                seen_sel.add(s)
                sel_list.append(s)
        sel = tuple(sel_list)
        if not sel:
            selected_clip_id = None
            ttk.Label(
                insp_grid,
                text="Chọn clip trên timeline (Ctrl/Shift = nhiều clip video để sửa hàng loạt).",
                wraplength=320,
            ).grid(row=0, column=0, sticky="w")
            return
        # Không dùng sel[0]: thứ tự tuple selection của Treeview thường là thứ tự cây, không phải clip vừa chọn.
        sel_set = set(sel_list)
        primary_tl = str(_primary_timeline_clip_id_for_preview() or "").strip()
        if primary_tl and primary_tl in sel_set:
            selected_clip_id = primary_tl
        else:
            selected_clip_id = sel_list[0]

        if len(sel) >= 1:
            rows: list[tuple[str, dict[str, Any]]] = []
            for cid in sel:
                fc = _find_clip(cid)
                if fc and fc[1]:
                    rows.append((cid, fc[1]))
            if not rows:
                ttk.Label(insp_grid, text="Không đọc được clip.").grid(row=0, column=0, sticky="w")
                return
            all_video = all(str(r[1].get("type")) == "video" for r in rows)
            # Chỉ UI hàng loạt khi chọn ≥2 clip video; 1 clip (video hay khác) → inspector chi tiết bên dưới.
            if all_video and len(rows) >= 2:
                sel_key = "|".join(sorted(str(cid) for cid, _cl in rows))
                mem = _batch_meta_store()
                prev = mem.get(sel_key)
                loaded_prev = False
                if isinstance(prev, dict):
                    saved_draft = prev.get("draft")
                    if isinstance(saved_draft, dict):
                        _batch_edit_draft.clear()
                        _batch_edit_draft.update(saved_draft)
                        loaded_prev = True
                if not loaded_prev:
                    # Không giữ draft/_applied của nhóm clip khác — tránh «đã áp» sai hoặc áp nhầm thông số cũ.
                    _batch_edit_draft.clear()
                    _batch_edit_draft["_applied"] = {}
                insp_grid.columnconfigure(0, weight=1, minsize=72)
                insp_grid.columnconfigure(1, weight=1, minsize=72)
                insp_grid.columnconfigure(2, minsize=68)
                hdr_batch = ttk.Frame(insp_grid)
                hdr_batch.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 10))
                hdr_batch.columnconfigure(0, weight=1)
                hdr_batch.columnconfigure(0, weight=1)
                _bat_h0 = ttk.Label(
                    hdr_batch,
                    text=f"Sửa hàng loạt — {len(rows)} clip video",
                    font=("Segoe UI", 10, "bold"),
                    justify=tk.LEFT,
                )
                _bat_h0.grid(row=0, column=0, sticky="ew")
                _bind_label_wrap_to_frame(_bat_h0, hdr_batch, inset=8)
                _bat_h2 = ttk.Label(
                    hdr_batch,
                    text="Ô trống = giữ nguyên từng clip · «Áp dụng tất cả» ở cuối tab.",
                    font=("Segoe UI", 8),
                    foreground="#555",
                    justify=tk.LEFT,
                )
                _bat_h2.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(4, 0))
                _bind_label_wrap_to_frame(_bat_h2, hdr_batch, inset=8)
                if loaded_prev:
                    ttk.Label(
                        hdr_batch,
                        text="Đã nạp bản nháp trước.",
                        foreground="#1a4480",
                        font=("Segoe UI", 8),
                    ).grid(row=0, column=1, rowspan=2, sticky="ne", padx=(12, 0))

                def _persist_batch_draft(*, save_project_file: bool = False) -> None:
                    if not project:
                        return
                    store = _batch_meta_store()
                    store[sel_key] = {"draft": copy.deepcopy(_batch_edit_draft), "updated_at": int(time.time())}
                    # Giữ gọn lịch sử tránh phình project file.
                    if len(store) > 40:
                        drop = sorted(store.items(), key=lambda kv: int((kv[1] or {}).get("updated_at") or 0))
                        for k, _v in drop[: max(0, len(store) - 40)]:
                            store.pop(k, None)
                    if save_project_file:
                        try:
                            pm.save_project(project)
                        except Exception:
                            pass
                var_b_vol = tk.StringVar(value=str(_batch_edit_draft.get("volume") or ""))
                var_b_sp = tk.StringVar(value=str(_batch_edit_draft.get("speed") or ""))
                var_b_fi = tk.StringVar(value=str(_batch_edit_draft.get("fade_in") or ""))
                var_b_fo = tk.StringVar(value=str(_batch_edit_draft.get("fade_out") or ""))
                var_b_rot = tk.StringVar(value=str(_batch_edit_draft.get("rotation") or ""))
                var_b_canvas = tk.StringVar(value=str(_batch_edit_draft.get("canvas_mode") or ""))
                var_b_zoom = tk.StringVar(value=str(_batch_edit_draft.get("zoom") or ""))
                var_b_flip_h = tk.BooleanVar(value=bool(_batch_edit_draft.get("flip_h", False)))
                var_b_flip_v = tk.BooleanVar(value=bool(_batch_edit_draft.get("flip_v", False)))
                var_b_mute = tk.BooleanVar(value=bool(_batch_edit_draft.get("mute", False)))
                var_b_set_flip = tk.BooleanVar(value=bool(_batch_edit_draft.get("set_flip", False)))
                var_b_set_mute = tk.BooleanVar(value=bool(_batch_edit_draft.get("set_mute", False)))
                var_b_brightness = tk.StringVar(value=str(_batch_edit_draft.get("brightness") or ""))
                var_b_light_fx = tk.StringVar(
                    value=VideoFilterManager.light_effect_normalize_to_label_ui(str(_batch_edit_draft.get("light_effect") or ""))
                )

                def _set_field_status(lbl: ttk.Label, changed: bool) -> None:
                    lbl.configure(
                        text="Đã chỉnh" if changed else "Chưa chỉnh",
                        foreground="#1a7f37" if changed else "#888888",
                    )

                def _bind_status_text(
                    key: str,
                    var: tk.StringVar,
                    lbl: ttk.Label,
                    changed_fn: Callable[[str], bool] | None = None,
                ) -> None:
                    fn = changed_fn or (lambda s: bool(str(s or "").strip()))

                    def _on_write(*_a: Any) -> None:
                        cur = str(var.get() or "")
                        _batch_edit_draft[key] = cur
                        _set_field_status(lbl, fn(cur))
                        _persist_batch_draft(save_project_file=False)

                    var.trace_add("write", _on_write)
                    _on_write()

                def _bind_status_bool(
                    key: str,
                    var: tk.BooleanVar,
                    lbl: ttk.Label,
                    changed_fn: Callable[[bool], bool] | None = None,
                ) -> None:
                    fn = changed_fn or (lambda v: bool(v))

                    def _on_write(*_a: Any) -> None:
                        cur = bool(var.get())
                        _batch_edit_draft[key] = cur
                        _set_field_status(lbl, fn(cur))
                        _persist_batch_draft(save_project_file=False)

                    var.trace_add("write", _on_write)
                    _on_write()

                var_b_trim_head = tk.StringVar(value=str(_batch_edit_draft.get("trim_head") or ""))
                var_b_trim_tail = tk.StringVar(value=str(_batch_edit_draft.get("trim_tail") or ""))

                for r, lab, var, draft_key in (
                    (1, "Cắt đầu nguồn (giây; trống = không)", var_b_trim_head, "trim_head"),
                    (2, "Cắt đuôi nguồn (giây; trống = không)", var_b_trim_tail, "trim_tail"),
                    (3, "Âm lượng (0–1)", var_b_vol, "volume"),
                    (4, "Tốc độ (1 = bình thường)", var_b_sp, "speed"),
                    (5, "Fade vào (giây)", var_b_fi, "fade_in"),
                    (6, "Fade ra (giây)", var_b_fo, "fade_out"),
                ):
                    _lb_r = ttk.Label(insp_grid, text=lab, justify=tk.LEFT)
                    _lb_r.grid(row=r, column=0, sticky="ew", pady=(0, 4))
                    _bind_label_wrap_to_frame(_lb_r, insp_grid, inset=18)
                    _ent = ttk.Entry(insp_grid, textvariable=var, width=13)
                    _ent.grid(row=r, column=1, sticky="ew", pady=(0, 4), padx=(0, 6))
                    st = ttk.Label(insp_grid, text="Chưa chỉnh", foreground="#888888", width=11)
                    st.grid(row=r, column=2, sticky="w", padx=(4, 0), pady=(0, 4))
                    _bind_status_text(draft_key, var, st)

                _lb_br_b = ttk.Label(
                    insp_grid,
                    text="Độ sáng (-1…1; 0 = gốc; âm = tối hơn, dương = sáng hơn; để trống = giữ)",
                    justify=tk.LEFT,
                )
                _lb_br_b.grid(row=7, column=0, sticky="ew", pady=(0, 4))
                _bind_label_wrap_to_frame(_lb_br_b, insp_grid, inset=18)
                ttk.Entry(insp_grid, textvariable=var_b_brightness, width=13).grid(
                    row=7, column=1, sticky="ew", pady=(0, 4), padx=(0, 6)
                )
                st_br_b = ttk.Label(insp_grid, text="Chưa chỉnh", foreground="#888888", width=11)
                st_br_b.grid(row=7, column=2, sticky="w", padx=(4, 0), pady=(0, 4))
                _bind_status_text("brightness", var_b_brightness, st_br_b)
                _lb_lx_b = ttk.Label(
                    insp_grid,
                    text="Hiệu ứng ánh sáng (chọn mô tả bên dưới; để trống = giữ từng clip)",
                    justify=tk.LEFT,
                )
                _lb_lx_b.grid(row=8, column=0, sticky="ew", pady=(0, 4))
                _bind_label_wrap_to_frame(_lb_lx_b, insp_grid, inset=18)
                ttk.Combobox(
                    insp_grid,
                    textvariable=var_b_light_fx,
                    values=VideoFilterManager.light_effect_batch_combo_display_values(),
                    state="readonly",
                    width=36,
                ).grid(row=8, column=1, sticky="ew", pady=(0, 4), padx=(0, 6))
                st_lx_b = ttk.Label(insp_grid, text="Chưa chỉnh", foreground="#888888", width=11)
                st_lx_b.grid(row=8, column=2, sticky="w", padx=(4, 0), pady=(0, 4))
                _bind_status_text("light_effect", var_b_light_fx, st_lx_b)

                ttk.Separator(insp_grid, orient=tk.HORIZONTAL).grid(row=9, column=0, columnspan=3, sticky="ew", pady=(10, 10))
                _lb_tf_hdr = ttk.Label(insp_grid, text="Transform & canvas", font=("Segoe UI", 9, "bold"), justify=tk.LEFT)
                _lb_tf_hdr.grid(row=10, column=0, columnspan=3, sticky="ew", pady=(0, 6))
                _bind_label_wrap_to_frame(_lb_tf_hdr, insp_grid, inset=8)

                def _explain_toggle(msg_on: str, msg_off: str, val: tk.BooleanVar) -> None:
                    notify(msg_on if bool(val.get()) else msg_off)

                rf = ttk.Frame(insp_grid)
                rf.grid(row=11, column=0, columnspan=2, sticky="ew", pady=(2, 0))
                rf.columnconfigure(0, weight=1)
                ttk.Checkbutton(
                    rf,
                    text="Đặt flip",
                    variable=var_b_set_flip,
                    command=lambda: _explain_toggle(
                        "Đặt flip: BẬT — lật ngang/dọc sẽ được ghi khi bạn bấm «Áp dụng tất cả».",
                        "Đặt flip: TẮT — giữ nguyên trạng thái lật hiện tại của clip.",
                        var_b_set_flip,
                    ),
                ).pack(side=tk.LEFT)
                ttk.Checkbutton(
                    rf,
                    text="Lật ngang",
                    variable=var_b_flip_h,
                    command=lambda: _explain_toggle(
                        "Lật ngang: BẬT — sẽ soi gương ngang sau «Áp dụng tất cả» (khi Đặt flip đang bật).",
                        "Lật ngang: TẮT — không soi gương ngang.",
                        var_b_flip_h,
                    ),
                ).pack(side=tk.LEFT, padx=(8, 0))
                st_flip = ttk.Label(insp_grid, text="Chưa chỉnh", foreground="#888888")
                st_flip.grid(row=11, column=2, sticky="w", padx=(8, 0), pady=(2, 0))
                _bind_status_bool("set_flip", var_b_set_flip, st_flip)
                ttk.Checkbutton(
                    rf,
                    text="Lật dọc",
                    variable=var_b_flip_v,
                    command=lambda: _explain_toggle(
                        "Lật dọc: BẬT — sẽ lật dọc sau «Áp dụng tất cả» (khi Đặt flip đang bật).",
                        "Lật dọc: TẮT — không lật dọc.",
                        var_b_flip_v,
                    ),
                ).pack(side=tk.LEFT, padx=(8, 0))

                rm = ttk.Frame(insp_grid)
                rm.grid(row=12, column=0, columnspan=2, sticky="ew", pady=(2, 0))
                rm.columnconfigure(0, weight=1)
                ttk.Checkbutton(
                    rm,
                    text="Áp dụng tùy chọn âm gốc",
                    variable=var_b_set_mute,
                    command=lambda: _explain_toggle(
                        "Áp dụng tùy chọn âm gốc: BẬT — tắt/bật âm gốc sẽ được ghi khi bạn bấm «Áp dụng tất cả».",
                        "Áp dụng tùy chọn âm gốc: TẮT — giữ nguyên âm gốc hiện tại của clip.",
                        var_b_set_mute,
                    ),
                ).pack(side=tk.LEFT)
                ttk.Checkbutton(
                    rm,
                    text="Tắt âm gốc clip",
                    variable=var_b_mute,
                    command=lambda: _explain_toggle(
                        "Tắt âm gốc clip: BẬT — âm gốc sẽ tắt sau «Áp dụng tất cả» (khi mục áp dụng âm gốc đang bật).",
                        "Tắt âm gốc clip: TẮT — giữ/bật lại âm thanh gốc clip.",
                        var_b_mute,
                    ),
                ).pack(side=tk.LEFT, padx=(8, 0))
                st_mute = ttk.Label(insp_grid, text="Chưa chỉnh", foreground="#888888")
                st_mute.grid(row=12, column=2, sticky="w", padx=(8, 0), pady=(2, 0))
                _bind_status_bool("set_mute", var_b_set_mute, st_mute)

                _lb_rot = ttk.Label(insp_grid, text="Xoay (để trống / 0 / 90 / 180 / 270)", justify=tk.LEFT)
                _lb_rot.grid(row=13, column=0, sticky="ew", pady=2)
                _bind_label_wrap_to_frame(_lb_rot, insp_grid, inset=18)
                ttk.Combobox(
                    insp_grid,
                    textvariable=var_b_rot,
                    values=["", "0", "90", "180", "270"],
                    state="readonly",
                    width=12,
                ).grid(row=13, column=1, sticky="ew", pady=2)
                st_rot = ttk.Label(insp_grid, text="Chưa chỉnh", foreground="#888888")
                st_rot.grid(row=13, column=2, sticky="w", padx=(8, 0), pady=2)
                _bind_status_text("rotation", var_b_rot, st_rot)
                _lb_cv = ttk.Label(insp_grid, text="Vào khung (Fit/Fill/Stretch)", justify=tk.LEFT)
                _lb_cv.grid(row=14, column=0, sticky="ew", pady=2)
                _bind_label_wrap_to_frame(_lb_cv, insp_grid, inset=18)
                ttk.Combobox(
                    insp_grid,
                    textvariable=var_b_canvas,
                    values=["", "fit", "fill", "stretch"],
                    state="readonly",
                    width=12,
                ).grid(row=14, column=1, sticky="ew", pady=2)
                st_canvas = ttk.Label(insp_grid, text="Chưa chỉnh", foreground="#888888")
                st_canvas.grid(row=14, column=2, sticky="w", padx=(8, 0), pady=2)
                _bind_status_text("canvas_mode", var_b_canvas, st_canvas)
                _lb_zm = ttk.Label(insp_grid, text="Zoom (1 = vừa khung; để trống = giữ)", justify=tk.LEFT)
                _lb_zm.grid(row=15, column=0, sticky="ew", pady=2)
                _bind_label_wrap_to_frame(_lb_zm, insp_grid, inset=18)
                ttk.Entry(insp_grid, textvariable=var_b_zoom, width=12).grid(row=15, column=1, sticky="ew", pady=2)
                st_zoom = ttk.Label(insp_grid, text="Chưa chỉnh", foreground="#888888")
                st_zoom.grid(row=15, column=2, sticky="w", padx=(8, 0), pady=2)
                _bind_status_text("zoom", var_b_zoom, st_zoom)

                media_audios = [
                    m for m in (project.get("media") or []) if isinstance(m, dict) and str(m.get("type") or "") == "audio"
                ]
                audio_opts, audio_label_to_id, audio_id_to_label = _ve_build_media_combo_maps(
                    media_audios, fallback="audio"
                )
                var_b_audio = tk.StringVar(
                    value=_ve_resolve_combo_display(
                        str(_batch_edit_draft.get("audio_media") or ""),
                        id_to_label=audio_id_to_label,
                        label_to_id=audio_label_to_id,
                        media_items=media_audios,
                        fallback="audio",
                    )
                )
                var_b_audio_vol = tk.StringVar(value=str(_batch_edit_draft.get("audio_vol") or "1.0"))
                var_b_audio_sp = tk.StringVar(value=str(_batch_edit_draft.get("audio_sp") or "1.0"))

                _lm_d = str(_batch_edit_draft.get("logo_media") or "").strip()
                try:
                    if _lm_d:
                        _logo_i2l = _q_logo_media_combo_refresh.get("id_to_label") or {}
                        _logo_l2i = _q_logo_media_combo_refresh.get("label_to_id") or {}
                        _logo_imgs = [
                            m
                            for m in (project.get("media") or [])
                            if isinstance(m, dict) and str(m.get("type") or "") == "image"
                        ]
                        _lm_show = _ve_resolve_combo_display(
                            _lm_d,
                            id_to_label=_logo_i2l,
                            label_to_id=_logo_l2i,
                            media_items=_logo_imgs,
                            fallback="image",
                        )
                        var_q_logo_media.set(_lm_show or _AUTO_LOGO_MEDIA_LBL)
                    else:
                        var_q_logo_media.set(_AUTO_LOGO_MEDIA_LBL)
                except NameError:
                    pass
                try:
                    var_q_logo_opacity.set(str(_batch_edit_draft.get("logo_opacity") or "0.92"))
                    var_q_logo_size_ratio.set(str(_batch_edit_draft.get("logo_ratio") or "0.15"))
                    var_ov_text.set(str(_batch_edit_draft.get("text_overlay") or ""))
                except NameError:
                    pass

                def _batch_sync_quick_overlay_to_draft(*_a: Any) -> None:
                    try:
                        _batch_edit_draft["logo_media"] = str(var_q_logo_media.get() or "")
                        _batch_edit_draft["logo_opacity"] = str(var_q_logo_opacity.get() or "")
                        _batch_edit_draft["logo_ratio"] = str(var_q_logo_size_ratio.get() or "")
                        _batch_edit_draft["text_overlay"] = str(var_ov_text.get() or "")
                        _persist_batch_draft(save_project_file=False)
                    except (NameError, tk.TclError):
                        pass

                for _bv in (var_q_logo_media, var_q_logo_opacity, var_q_logo_size_ratio, var_ov_text):
                    try:
                        _bv.trace_add("write", lambda *_x: _batch_sync_quick_overlay_to_draft())
                    except (NameError, tk.TclError):
                        pass

                ttk.Separator(insp_grid, orient=tk.HORIZONTAL).grid(row=14, column=0, columnspan=3, sticky="ew", pady=6)

                batch_media_fr = ttk.Frame(insp_grid)
                batch_media_fr.grid(row=15, column=0, columnspan=3, sticky="ew")
                batch_media_fr.columnconfigure(0, weight=1)

                ov_hint_b = ttk.Label(
                    batch_media_fr,
                    text="Logo & chữ: dùng khung «Logo & Chữ» cuối tab. Âm phụ: chỉnh trong khung dưới.",
                    foreground="#1a4480",
                    font=("Segoe UI", 8),
                    justify=tk.LEFT,
                )
                ov_hint_b.pack(anchor="w", fill=tk.X, pady=(0, 8))
                _bind_label_wrap_to_frame(ov_hint_b, batch_media_fr, inset=8)

                lf_b_audio = ttk.LabelFrame(batch_media_fr, text="Âm thanh — track phụ", padding=(8, 6, 8, 8))
                lf_b_audio.pack(fill=tk.BOTH, expand=True, pady=(0, 6))
                lf_b_audio.columnconfigure(0, weight=0)
                lf_b_audio.columnconfigure(1, weight=1)
                lf_b_audio.columnconfigure(2, weight=0)
                _lb_af = ttk.Label(lf_b_audio, text="File âm thanh", justify=tk.LEFT)
                _lb_af.grid(row=0, column=0, sticky="ew", pady=2)
                _bind_label_wrap_to_frame(_lb_af, lf_b_audio, inset=12)
                ttk.Combobox(lf_b_audio, textvariable=var_b_audio, values=audio_opts, state="readonly").grid(
                    row=0, column=1, sticky="ew", padx=(8, 8), pady=2
                )
                st_audio = ttk.Label(lf_b_audio, text="Chưa chỉnh", foreground="#888888")
                st_audio.grid(row=0, column=2, sticky="w", pady=2)
                _bind_status_text("audio_media", var_b_audio, st_audio)
                ttk.Label(lf_b_audio, text="Âm lượng").grid(row=1, column=0, sticky="w", pady=(4, 2))
                ttk.Entry(lf_b_audio, textvariable=var_b_audio_vol, width=14).grid(row=1, column=1, sticky="w", pady=(4, 2))
                st_audio_vol = ttk.Label(lf_b_audio, text="Chưa chỉnh", foreground="#888888")
                st_audio_vol.grid(row=1, column=2, sticky="w", padx=(8, 0), pady=(4, 2))
                _bind_status_text("audio_vol", var_b_audio_vol, st_audio_vol)
                ttk.Label(lf_b_audio, text="Tốc độ track phụ").grid(row=2, column=0, sticky="w", pady=(4, 2))
                ttk.Entry(lf_b_audio, textvariable=var_b_audio_sp, width=14).grid(row=2, column=1, sticky="w", pady=(4, 2))
                st_audio_sp = ttk.Label(lf_b_audio, text="Chưa chỉnh", foreground="#888888")
                st_audio_sp.grid(row=2, column=2, sticky="w", padx=(8, 0), pady=(4, 2))
                _bind_status_text("audio_sp", var_b_audio_sp, st_audio_sp)

                _bat_scope = ttk.Label(
                    insp_grid,
                    text="Để trống = giữ nguyên. Chọn phạm vi ở cuối tab trước khi «Áp dụng tất cả».",
                    foreground="gray",
                    font=("Segoe UI", 8),
                    justify=tk.LEFT,
                )
                _bat_scope.grid(row=18, column=0, columnspan=3, sticky="ew", pady=(8, 6))
                _bind_label_wrap_to_frame(_bat_scope, insp_grid, inset=8)

                var_b_reset_mode = tk.StringVar(
                    value=str(_batch_edit_draft.get("reset_mode") or "Reset về ban đầu")
                )

                def _remember_reset_mode(*_a: Any) -> None:
                    _batch_edit_draft["reset_mode"] = str(var_b_reset_mode.get() or "")

                var_b_reset_mode.trace_add("write", _remember_reset_mode)
                _remember_reset_mode()

                def _reset_selected_videos_to_default() -> None:
                    if not project:
                        return
                    targets = _selected_video_timeline_rows()
                    if not targets:
                        messagebox.showinfo("Hàng loạt", "Chọn các clip video cần reset.", parent=root)
                        return
                    reset_mode = str(var_b_reset_mode.get() or "Reset về ban đầu").strip()
                    if not messagebox.askyesno(
                        "Hàng loạt",
                        f"{reset_mode}: áp dụng cho {len(targets)} clip video?\n"
                        "Có thể chỉnh lại ngay sau khi reset.",
                        parent=root,
                    ):
                        return
                    pw = int(project.get("width") or 1080)
                    ph = int(project.get("height") or 1920)
                    patch_full = {
                        "speed": 1.0,
                        "volume": 1.0,
                        "muted": False,
                        "fade_in": 0.0,
                        "fade_out": 0.0,
                        "x": 0,
                        "y": 0,
                        "width": pw,
                        "height": ph,
                        "opacity": 1.0,
                        "flip_horizontal": False,
                        "flip_vertical": False,
                        "rotation": 0,
                        "crop": {
                            "enabled": False,
                            "x": 0,
                            "y": 0,
                            "width": pw,
                            "height": ph,
                        },
                        "scale": {
                            "enabled": False,
                            "width": pw,
                            "height": ph,
                            "keep_aspect": True,
                        },
                        "canvas_mode": "fit",
                        "zoom": 1.0,
                        "brightness": 0.0,
                        "light_effect": "none",
                    }
                    patch_basic = {
                        "speed": 1.0,
                        "volume": 1.0,
                        "muted": False,
                        "fade_in": 0.0,
                        "fade_out": 0.0,
                        "flip_horizontal": False,
                        "flip_vertical": False,
                        "rotation": 0,
                        "brightness": 0.0,
                        "light_effect": "none",
                    }
                    patch_frame = {
                        "x": 0,
                        "y": 0,
                        "width": pw,
                        "height": ph,
                        "opacity": 1.0,
                        "crop": {
                            "enabled": False,
                            "x": 0,
                            "y": 0,
                            "width": pw,
                            "height": ph,
                        },
                        "scale": {
                            "enabled": False,
                            "width": pw,
                            "height": ph,
                            "keep_aspect": True,
                        },
                        "canvas_mode": "fit",
                        "zoom": 1.0,
                    }
                    if reset_mode == "Reset cơ bản (âm/tốc độ/fade/xoay)":
                        patch_default = patch_basic
                    elif reset_mode == "Reset khung hình (crop/scale/canvas)":
                        patch_default = patch_frame
                    else:
                        # «Reset về ban đầu», «Reset đầy đủ», hoặc giá trị lạ → full patch
                        patch_default = patch_full
                    n_rows = len(targets)
                    defer = n_rows > 1
                    for cid, _cl in targets:
                        tm.update_clip(
                            project,
                            cid,
                            patch_default,
                            persist=False,
                            recompute_duration=not defer,
                        )
                    if defer:
                        tm.refresh_project_duration(project)
                    # Xóa trạng thái đã áp dụng để có thể chạy lại từ đầu.
                    if isinstance(_batch_edit_draft.get("_applied"), dict):
                        _batch_edit_draft["_applied"] = {}
                    pm.save_project(project)
                    notify(f"Hàng loạt: {reset_mode} cho {n_rows} clip video.")
                    refresh_timeline()
                    refresh_inspector()

                _br_bar = _ve_batch_reset_bar_ref.get("fr")
                if _br_bar is not None:
                    lf_reset_bt = ttk.LabelFrame(
                        _br_bar,
                        text="Reset hàng loạt — đưa clip về mặc định",
                        padding=(10, 8, 10, 8),
                    )
                    lf_reset_bt.pack(fill=tk.X)
                    lf_reset_bt.columnconfigure(1, weight=1)
                    ttk.Label(lf_reset_bt, text="Kiểu reset").grid(row=0, column=0, sticky="w", padx=(0, 8))
                    ttk.Combobox(
                        lf_reset_bt,
                        textvariable=var_b_reset_mode,
                        values=(
                            "Reset về ban đầu",
                            "Reset đầy đủ",
                            "Reset cơ bản (âm/tốc độ/fade/xoay)",
                            "Reset khung hình (crop/scale/canvas)",
                        ),
                        state="readonly",
                        width=36,
                    ).grid(row=0, column=1, columnspan=2, sticky="ew", padx=(0, 0))
                    ttk.Button(
                        lf_reset_bt,
                        text="Reset theo kiểu đã chọn",
                        command=_reset_selected_videos_to_default,
                    ).grid(row=1, column=0, columnspan=3, sticky="ew", pady=(8, 0))
                    ttk.Label(
                        lf_reset_bt,
                        text="Chọn kiểu trong danh sách trước; reset chỉ chạy khi bấm nút (đổi menu không tự áp dụng).",
                        font=("Segoe UI", 8),
                        foreground="#666",
                        wraplength=680,
                        justify=tk.LEFT,
                    ).grid(row=2, column=0, columnspan=3, sticky="w", pady=(6, 0))
                    try:
                        _br_bar.pack(fill=tk.X, pady=(4, 6), before=fr_quick_edit_host)
                    except tk.TclError:
                        _br_bar.pack(fill=tk.X, pady=(4, 6))

                def _pick_media_id(raw: str) -> str:
                    return _ve_media_id_from_combo(raw, audio_label_to_id)

                def _find_track_clips(track_type: str) -> list[dict[str, Any]]:
                    for tr in project.get("tracks") or []:
                        if isinstance(tr, dict) and str(tr.get("type") or "") == track_type:
                            return tr.setdefault("clips", [])
                    return []

                def _batch_applied_map() -> dict[str, str]:
                    raw = _batch_edit_draft.get("_applied")
                    if isinstance(raw, dict):
                        return raw  # type: ignore[return-value]
                    m: dict[str, str] = {}
                    _batch_edit_draft["_applied"] = m
                    return m

                def _sig(v: Any) -> str:
                    return str(v if v is not None else "")

                def apply_batch_video() -> None:
                    if not project:
                        return
                    current_rows = _selected_video_timeline_rows()
                    if not current_rows:
                        messagebox.showinfo("Hàng loạt", "Chọn các clip video rồi bấm lại.")
                        return
                    patch: dict[str, Any] = {}
                    applied = _batch_applied_map()
                    for var, key in (
                        (var_b_vol, "volume"),
                        (var_b_sp, "speed"),
                        (var_b_fi, "fade_in"),
                        (var_b_fo, "fade_out"),
                    ):
                        s = var.get().strip()
                        if s:
                            if applied.get(key) == _sig(s):
                                continue
                            try:
                                patch[key] = float(s)
                            except ValueError:
                                messagebox.showerror("Hàng loạt", f"Số không hợp lệ: {key}")
                                return
                    flip_sig_ui = f"{int(bool(var_b_flip_h.get()))}:{int(bool(var_b_flip_v.get()))}"
                    # «Đặt flip»; hoặc checkbox khác lần áp trước; hoặc lần đầu đã bật lật mà chưa có applied["flip"].
                    if (
                        var_b_set_flip.get()
                        or (applied.get("flip") is not None and applied.get("flip") != flip_sig_ui)
                        or (applied.get("flip") is None and flip_sig_ui != "0:0")
                    ):
                        patch["flip_horizontal"] = bool(var_b_flip_h.get())
                        patch["flip_vertical"] = bool(var_b_flip_v.get())
                    if var_b_set_mute.get():
                        mute_sig = str(int(bool(var_b_mute.get())))
                        if applied.get("mute") != mute_sig:
                            patch["muted"] = bool(var_b_mute.get())
                    rot_s = var_b_rot.get().strip()
                    if rot_s:
                        try:
                            rot_i = int(rot_s)
                            if rot_i not in (0, 90, 180, 270):
                                raise ValueError("rotation")
                            patch["rotation"] = rot_i
                        except Exception:
                            messagebox.showerror("Hàng loạt", "Xoay chỉ nhận 0/90/180/270.")
                            return
                    canvas_s = var_b_canvas.get().strip().lower()
                    if canvas_s:
                        if applied.get("canvas_mode") == _sig(canvas_s):
                            canvas_s = ""
                    if canvas_s:
                        if canvas_s not in ("fit", "fill", "stretch"):
                            messagebox.showerror("Hàng loạt", "Canvas mode chỉ nhận fit/fill/stretch.")
                            return
                        patch["canvas_mode"] = canvas_s
                    zoom_s = var_b_zoom.get().strip()
                    if zoom_s:
                        if applied.get("zoom") == _sig(zoom_s):
                            zoom_s = ""
                    if zoom_s:
                        try:
                            zz = float(zoom_s)
                            if zz < 0.1 or zz > 8.0:
                                messagebox.showerror("Hàng loạt", "Zoom hợp lệ trong khoảng 0.1 — 8 (1 = vừa khung).", parent=root)
                                return
                            patch["zoom"] = zz
                        except ValueError:
                            messagebox.showerror("Hàng loạt", "Giá trị Zoom không hợp lệ.", parent=root)
                            return

                    bright_s = str(var_b_brightness.get() or "").strip()
                    if bright_s:
                        if applied.get("brightness") == _sig(bright_s):
                            bright_s = ""
                    if bright_s:
                        try:
                            bb = float(bright_s)
                            patch["brightness"] = max(-1.0, min(1.0, bb))
                        except ValueError:
                            messagebox.showerror("Hàng loạt", "Độ sáng phải là số (vd. 0, 0.15, -0.2).", parent=root)
                            return
                    le_raw = VideoFilterManager.light_effect_label_ui_to_key(str(var_b_light_fx.get() or "").strip())
                    if le_raw:
                        if applied.get("light_effect") == _sig(le_raw):
                            le_raw = ""
                    if le_raw:
                        if le_raw not in VideoFilterManager.LIGHT_EFFECT_PRESETS:
                            messagebox.showerror("Hàng loạt", "Giá trị hiệu ứng ánh sáng không hợp lệ.", parent=root)
                            return
                        patch["light_effect"] = le_raw

                    logo_mid = _resolve_logo_media_id_from_ui(
                        str(var_q_logo_media.get() or ""), allow_auto=True
                    )
                    if logo_mid and not _media_id_valid_for_type(logo_mid, "image"):
                        logo_mid = ""
                    add_logo = bool(logo_mid)
                    logo_opa = 0.92
                    logo_ratio = 0.15
                    if add_logo:
                        logo_sig = "|".join(
                            [
                                str(logo_mid),
                                str(var_q_logo_opacity.get().strip() or "0.92"),
                                str(var_q_logo_size_ratio.get().strip() or "0.15"),
                            ]
                        )
                        if applied.get("logo") == logo_sig:
                            add_logo = False
                    if add_logo:
                        try:
                            logo_opa = max(0.0, min(1.0, float(var_q_logo_opacity.get().strip() or "0.92")))
                            logo_ratio = max(0.02, min(0.6, float(var_q_logo_size_ratio.get().strip() or "0.15")))
                        except ValueError:
                            messagebox.showerror("Hàng loạt", "Thông số logo không hợp lệ.")
                            return

                    audio_mid = _pick_media_id(var_b_audio.get())
                    if audio_mid and not _media_id_valid_for_type(audio_mid, "audio"):
                        audio_mid = ""
                    add_audio = bool(audio_mid)
                    text_val = str(var_ov_text.get() or "").strip()
                    add_text = bool(text_val)
                    if add_audio:
                        audio_sig = "|".join(
                            [
                                str(audio_mid),
                                str(var_b_audio_vol.get().strip() or "1.0"),
                                str(var_b_audio_sp.get().strip() or "1.0"),
                            ]
                        )
                        if applied.get("audio") == audio_sig:
                            add_audio = False
                    # Không bỏ qua chữ chỉ vì applied trùng — clip mới / đổi vị trí vẫn cần đồng bộ như logo.
                    audio_vol = 1.0
                    batch_audio_sp = 1.0
                    if add_audio:
                        try:
                            audio_vol = max(0.0, float(var_b_audio_vol.get().strip() or "1.0"))
                            batch_audio_sp = float(str(var_b_audio_sp.get()).strip() or "1.0")
                        except ValueError:
                            messagebox.showerror("Hàng loạt", "Âm lượng / tốc độ track phụ không hợp lệ.")
                            return
                        if batch_audio_sp <= 0:
                            messagebox.showerror("Hàng loạt", "Tốc độ track phụ phải > 0.")
                            return

                    q_logo_has_input = any(
                        str(v.get() or "").strip()
                        for v in (
                            var_q_logo_opacity,
                            var_q_logo_size_ratio,
                            var_q_logo_motion_mode,
                            var_q_logo_motion_interval,
                            var_q_logo_motion_seed,
                            var_q_logo_position,
                        )
                    )
                    q_text_font_pick = str(var_q_text_font.get() or "").strip()
                    if q_text_font_pick == "Mặc định (trống)":
                        q_text_font_pick = ""
                    q_text_has_input = any(
                        str(v.get() or "").strip()
                        for v in (
                            var_ov_text,
                            var_q_text_size,
                            var_q_text_color,
                            var_q_text_follow_logo,
                            var_q_text_position,
                        )
                    ) or bool(q_text_font_pick)

                    sc_clip_b = bool(var_ve_apply_clip.get())
                    sc_b_l = bool(var_ve_apply_ov_logo.get())
                    sc_b_a = bool(var_ve_apply_ov_audio.get())
                    sc_b_t = bool(var_ve_apply_ov_text.get())
                    sc_b_ql = bool(var_ve_apply_quick_logo.get())
                    sc_b_qt = bool(var_ve_apply_quick_text.get())
                    add_logo = bool(add_logo and sc_b_l)
                    add_audio = bool(add_audio and sc_b_a)
                    add_text = bool(add_text and sc_b_t)
                    effective_patch = patch if sc_clip_b else {}
                    quick_will_logo = bool(q_logo_has_input and sc_b_ql)
                    quick_will_text = bool(q_text_has_input and sc_b_qt)
                    try:
                        trim_h_b = max(0.0, float(str(var_b_trim_head.get()).strip() or "0"))
                        trim_t_b = max(0.0, float(str(var_b_trim_tail.get()).strip() or "0"))
                    except ValueError:
                        messagebox.showerror("Hàng loạt", "Giá trị cắt đầu/cắt đuôi không hợp lệ.")
                        return
                    will_trim_batch = trim_h_b > 0 or trim_t_b > 0

                    if (
                        not effective_patch
                        and not add_logo
                        and not add_audio
                        and not add_text
                        and not quick_will_logo
                        and not quick_will_text
                        and not will_trim_batch
                    ):
                        notify(
                            "Hàng loạt: không có thay đổi cần chạy — kiểm tra «Phạm vi áp dụng» (cuối tab) "
                            "và các ô đã điền / mục chưa áp trước đó."
                        )
                        return

                    loc_b = aoc_b = toc_b = "replace"
                    if add_logo or add_audio or add_text:
                        ov_l, ov_a, ov_t = _conflicts_for_video_rows(current_rows)
                        if (add_logo and ov_l) or (add_audio and ov_a) or (add_text and ov_t):
                            modes_b = _prompt_apply_conflict_modes(
                                ask_logo=ov_l,
                                ask_audio=ov_a,
                                ask_text=ov_t,
                                batch_count=len(current_rows),
                            )
                            if modes_b is None:
                                return
                            loc_b, aoc_b, toc_b = modes_b

                    _cancel_auto_preview_debounce()
                    lg_ops_b = au_ops_b = tx_ops_b = 0
                    logo_corner_b = str(var_q_logo_position.get() or "").strip()
                    n_batch = len(current_rows)
                    defer_tm = n_batch > 1
                    prog_step = _ve_batch_progress_step(n_batch)
                    if will_trim_batch:
                        u_tr, sk_tr = _trim_video_rows_source(
                            current_rows, trim_h_b, trim_t_b, sync_audio=True
                        )
                        if u_tr <= 0:
                            messagebox.showwarning(
                                "Hàng loạt",
                                "Không clip nào được cắt (quá ngắn hoặc giá trị không hợp lệ).",
                            )
                            return
                        _notify_trim_result(u_tr, n_batch, trim_h_b, trim_t_b, sk_tr)
                    for j, (cid, c) in enumerate(current_rows, start=1):
                        try:
                            if effective_patch:
                                tm.update_clip(
                                    project,
                                    cid,
                                    effective_patch,
                                    persist=False,
                                    recompute_duration=not defer_tm,
                                )
                        except Exception as ex:
                            messagebox.showerror("Hàng loạt", str(ex))
                            return
                        if prog_step and (j == n_batch or j % prog_step == 0):
                            _ve_batch_status_progress(j, n_batch, "áp dụng clip")

                    _sync_keys = frozenset({"duration", "timeline_start", "source_start", "source_end", "speed"})
                    if effective_patch and _sync_keys & set(effective_patch.keys()):
                        sp_batch = None
                        if "speed" in effective_patch:
                            try:
                                sp_batch = float(effective_patch["speed"])
                            except (TypeError, ValueError):
                                sp_batch = 1.0
                            if sp_batch <= 0:
                                sp_batch = 1.0
                        for cid_b, _c_b in current_rows:
                            _sync_overlapping_timeline_audio_to_video(cid_b, sp_batch)

                    if add_logo or add_audio or add_text:
                        lg_ops_b, au_ops_b, tx_ops_b = apply_logo_audio_text_to_video_rows(
                            current_rows,
                            logo_mid=str(logo_mid or "").strip() if add_logo else "",
                            logo_opacity=logo_opa,
                            logo_ratio=logo_ratio,
                            logo_corner=logo_corner_b,
                            audio_mid=str(audio_mid or "").strip() if add_audio else "",
                            audio_volume=audio_vol,
                            audio_speed=batch_audio_sp if add_audio else None,
                            text_content=text_val if add_text else "",
                            logo_on_conflict=loc_b if add_logo else "skip",
                            audio_on_conflict=aoc_b if add_audio else "skip",
                            text_on_conflict=toc_b if add_text else "skip",
                        )

                    if defer_tm:
                        tm.refresh_project_duration(project)
                    pm.save_project(project)
                    if (quick_will_logo or quick_will_text) and n_batch >= VE_BATCH_SHOW_PROGRESS_MIN_CLIPS:
                        lbl_status.configure(text="Hàng loạt — sửa nhanh logo/chữ…", foreground="#1a4480")
                        try:
                            root.update_idletasks()
                        except tk.TclError:
                            pass
                    keys = list(effective_patch.keys())
                    msg = f"Hàng loạt: đã cập nhật {len(current_rows)} clip"
                    if keys:
                        msg += f" ({', '.join(keys)})"
                    if lg_ops_b:
                        msg += f", logo: {lg_ops_b} thao tác"
                    if au_ops_b:
                        msg += f", âm thanh: {au_ops_b} thao tác"
                    if tx_ops_b:
                        msg += f", chữ: {tx_ops_b} thao tác"
                    quick_logo_changed = (
                        quick_edit_logo_for_selected_videos(quiet=True) if quick_will_logo else 0
                    )
                    quick_text_changed = (
                        quick_edit_text_for_selected_videos(quiet=True) if quick_will_text else 0
                    )
                    if quick_logo_changed or quick_text_changed:
                        pm.save_project(project)
                    if quick_logo_changed:
                        msg += f", sửa nhanh {quick_logo_changed} logo"
                    if quick_text_changed:
                        msg += f", sửa nhanh {quick_text_changed} chữ"
                    notify(msg + ".")
                    # Lưu trạng thái đã áp để lần bấm sau chỉ chạy phần chưa chạy.
                    if sc_clip_b:
                        for k in patch.keys():
                            if k in (
                                "volume",
                                "speed",
                                "fade_in",
                                "fade_out",
                                "rotation",
                                "canvas_mode",
                                "zoom",
                                "brightness",
                                "light_effect",
                            ):
                                applied[k] = _sig(_batch_edit_draft.get(k))
                            elif k in ("flip_horizontal", "flip_vertical"):
                                applied["flip"] = f"{int(bool(var_b_flip_h.get()))}:{int(bool(var_b_flip_v.get()))}"
                            elif k == "muted":
                                applied["mute"] = str(int(bool(var_b_mute.get())))
                    if add_logo:
                        applied["logo"] = "|".join(
                            [
                                str(logo_mid),
                                str(var_q_logo_opacity.get().strip() or "0.92"),
                                str(var_q_logo_size_ratio.get().strip() or "0.15"),
                            ]
                        )
                    if add_audio:
                        applied["audio"] = "|".join(
                            [
                                str(audio_mid),
                                str(var_b_audio_vol.get().strip() or "1.0"),
                                str(var_b_audio_sp.get().strip() or "1.0"),
                            ]
                        )
                    if add_text:
                        applied["text"] = _sig(text_val)
                    _persist_batch_draft(save_project_file=False)
                    refresh_timeline()
                    refresh_inspector()
                    try:
                        root.update_idletasks()
                    except tk.TclError:
                        pass
                    if au_ops_b > 0:
                        notify(
                            "Đã áp dụng hàng loạt — bấm «Preview nháp» khi cần "
                            "(không tự render sau khi thêm âm thanh, tránh treo)."
                        )
                    elif n_batch <= VE_BATCH_AUTO_PREVIEW_MAX_CLIPS:
                        root.after_idle(lambda: _auto_preview_after_apply("áp dụng thay đổi (hàng loạt)"))
                    else:
                        notify("Đã bỏ qua preview tự động (quá nhiều clip) — bấm xem preview khi cần.")

                _apply_batch_video_ref["fn"] = apply_batch_video
                insp_grid.columnconfigure(1, weight=1)
                return
            if len(rows) >= 2:
                ttk.Label(
                    insp_grid,
                    text="Chọn một clip để sửa chi tiết, hoặc chọn từ 2 clip «video» trở lên để sửa hàng loạt.",
                    wraplength=360,
                ).grid(row=0, column=0, sticky="w")
                return
            # len(rows) == 1 → tiếp tục inspector đơn (fc_res)

        fc_res = _find_clip(selected_clip_id)
        if not fc_res or fc_res[1] is None:
            ttk.Label(insp_grid, text="Không tìm thấy clip.").grid(row=0, column=0, sticky="w")
            return
        _, cl = fc_res
        ctype = str(cl.get("type"))
        row = 0

        def add_num(label: str, key: str, r: int) -> None:
            raw = cl.get(key, "")
            if key == "speed" and raw in ("", None):
                raw = 1.0
            var = tk.StringVar(value=str(raw))
            ttk.Label(insp_grid, text=label).grid(row=r, column=0, sticky="nw", pady=2)
            e = ttk.Entry(insp_grid, textvariable=var, width=18)
            e.grid(row=r, column=1, sticky="ew", pady=2)

            def save_val() -> None:
                if not project:
                    return
                try:
                    v = float(var.get().strip())
                    tm.update_clip(project, selected_clip_id or "", {key: v})
                    notify(f"Đã lưu clip: {key} = {v}")
                except Exception as ex:
                    messagebox.showerror("Inspector", str(ex))

            e.bind("<FocusOut>", lambda _e: save_val())
            e.bind("<Return>", lambda _e: save_val())

        def add_txt(label: str, key: str, r: int) -> None:
            var = tk.StringVar(value=str(cl.get(key, "")))
            ttk.Label(insp_grid, text=label).grid(row=r, column=0, sticky="nw", pady=2)
            e = ttk.Entry(insp_grid, textvariable=var, width=24)
            e.grid(row=r, column=1, sticky="ew", pady=2)

            def save_val() -> None:
                if not project:
                    return
                tm.update_clip(project, selected_clip_id or "", {key: var.get()})
                notify(f"Đã lưu clip: {key}")

            e.bind("<FocusOut>", lambda _e: save_val())
            e.bind("<Return>", lambda _e: save_val())

        insp_grid.columnconfigure(1, weight=1)
        if ctype == "video":
            insp_grid.columnconfigure(0, weight=1)
            sv_cid = str(selected_clip_id or "").strip()
            _vh_intro = ttk.Label(
                insp_grid,
                text=(
                    "Một clip video: timeline / cắt nguồn / tốc độ / fade / độ sáng & hiệu ứng ánh sáng… là bản nháp — bấm «Áp dụng tất cả» "
                    "ở cuối tab để ghi (theo «Phạm vi»). Transform & canvas (lật, khung, zoom, tắt âm gốc) cũng bật "
                    "trong «Phạm vi» — không đụng timeline hay cắt nguồn khi chỉ chọn các mục đó."
                ),
                foreground="#1a4480",
                font=("Segoe UI", 8),
                justify=tk.LEFT,
            )
            _vh_intro.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
            _bind_label_wrap_to_frame(_vh_intro, insp_grid, inset=8)

            vr = 1
            ttk.Label(insp_grid, text="Bắt đầu trên timeline (giây)").grid(row=vr, column=0, sticky="nw", pady=2)
            var_sv_ts = tk.StringVar(value=str(cl.get("timeline_start") or 0))
            ttk.Entry(insp_grid, textvariable=var_sv_ts, width=18).grid(row=vr, column=1, sticky="ew", pady=2)
            vr += 1
            ttk.Label(insp_grid, text="Điểm cắt đầu trong file nguồn (giây)").grid(row=vr, column=0, sticky="nw", pady=2)
            var_sv_ss = tk.StringVar(value=str(cl.get("source_start") or 0))
            ttk.Entry(insp_grid, textvariable=var_sv_ss, width=18).grid(row=vr, column=1, sticky="ew", pady=2)
            vr += 1
            ttk.Label(insp_grid, text="Điểm cắt cuối trong file nguồn (giây)").grid(row=vr, column=0, sticky="nw", pady=2)
            var_sv_se = tk.StringVar(value=str(cl.get("source_end") or 0))
            ttk.Entry(insp_grid, textvariable=var_sv_se, width=18).grid(row=vr, column=1, sticky="ew", pady=2)
            vr += 1
            ttk.Label(insp_grid, text="Độ dài clip trên timeline (giây)").grid(row=vr, column=0, sticky="nw", pady=2)
            var_sv_du = tk.StringVar(value=str(cl.get("duration") or 0))
            ttk.Entry(insp_grid, textvariable=var_sv_du, width=18).grid(row=vr, column=1, sticky="ew", pady=2)
            vr += 1
            ttk.Separator(insp_grid, orient=tk.HORIZONTAL).grid(row=vr, column=0, columnspan=2, sticky="ew", pady=6)
            vr += 1
            ttk.Label(insp_grid, text="Cắt nguồn (nhanh)", font=("Segoe UI", 9, "bold")).grid(
                row=vr, column=0, columnspan=2, sticky="w"
            )
            vr += 1
            _trim_hint = ttk.Label(
                insp_grid,
                text="Cắt trực tiếp file gốc (giây). Tự khớp độ dài timeline và track âm chồng clip — không cần «Áp dụng tất cả».",
                foreground="#1a4480",
                font=("Segoe UI", 8),
                justify=tk.LEFT,
            )
            _trim_hint.grid(row=vr, column=0, columnspan=2, sticky="ew", pady=(0, 4))
            _bind_label_wrap_to_frame(_trim_hint, insp_grid, inset=8)
            vr += 1
            fr_trim = ttk.Frame(insp_grid)
            fr_trim.grid(row=vr, column=0, columnspan=2, sticky="ew", pady=(0, 4))
            var_sv_trim_head = tk.StringVar(value="")
            var_sv_trim_tail = tk.StringVar(value="")
            ttk.Label(fr_trim, text="Cắt đầu").pack(side=tk.LEFT)
            ttk.Entry(fr_trim, textvariable=var_sv_trim_head, width=7).pack(side=tk.LEFT, padx=(4, 10))
            ttk.Label(fr_trim, text="Cắt đuôi").pack(side=tk.LEFT)
            ttk.Entry(fr_trim, textvariable=var_sv_trim_tail, width=7).pack(side=tk.LEFT, padx=(4, 10))

            def _inspector_trim_source_now() -> None:
                if not project or not sv_cid:
                    return
                try:
                    th = max(0.0, float(str(var_sv_trim_head.get()).strip() or "0"))
                    tt = max(0.0, float(str(var_sv_trim_tail.get()).strip() or "0"))
                except ValueError:
                    messagebox.showerror("Cắt nguồn", "Giá trị cắt đầu/cắt đuôi không hợp lệ.", parent=root)
                    return
                if th <= 0 and tt <= 0:
                    messagebox.showinfo("Cắt nguồn", "Nhập số giây cắt đầu hoặc cắt đuôi (> 0).", parent=root)
                    return
                fc_t = _find_clip(sv_cid)
                if not fc_t or not fc_t[1] or str(fc_t[1].get("type")) != "video":
                    messagebox.showerror("Cắt nguồn", "Chọn clip video trên timeline.", parent=root)
                    return
                try:
                    u, sk = _trim_video_rows_source([(sv_cid, fc_t[1])], th, tt, sync_audio=True)
                    if u <= 0:
                        messagebox.showwarning("Cắt nguồn", "Clip quá ngắn sau khi cắt.", parent=root)
                        return
                    tm.refresh_project_duration(project)
                    pm.save_project(project)
                    refresh_timeline()
                    refresh_inspector()
                    _notify_trim_result(u, 1, th, tt, sk)
                except Exception as ex:
                    messagebox.showerror("Cắt nguồn", str(ex), parent=root)

            ttk.Button(fr_trim, text="Cắt ngay", command=_inspector_trim_source_now).pack(side=tk.LEFT)
            vr += 1
            sp0 = cl.get("speed", 1.0)
            if sp0 in ("", None):
                sp0 = 1.0
            ttk.Label(insp_grid, text="Tốc độ phát (1 = bình thường)").grid(row=vr, column=0, sticky="nw", pady=2)
            var_sv_sp = tk.StringVar(value=str(sp0))
            ttk.Entry(insp_grid, textvariable=var_sv_sp, width=18).grid(row=vr, column=1, sticky="ew", pady=2)
            vr += 1
            vol0 = cl.get("volume", 1.0)
            if vol0 in ("", None):
                vol0 = 1.0
            ttk.Label(insp_grid, text="Âm lượng (0–1)").grid(row=vr, column=0, sticky="nw", pady=2)
            var_sv_vol = tk.StringVar(value=str(vol0))
            ttk.Entry(insp_grid, textvariable=var_sv_vol, width=18).grid(row=vr, column=1, sticky="ew", pady=2)
            vr += 1
            ttk.Label(insp_grid, text="Fade vào (giây)").grid(row=vr, column=0, sticky="nw", pady=2)
            var_sv_fi = tk.StringVar(value=str(cl.get("fade_in") or 0))
            ttk.Entry(insp_grid, textvariable=var_sv_fi, width=18).grid(row=vr, column=1, sticky="ew", pady=2)
            vr += 1
            ttk.Label(insp_grid, text="Fade ra (giây)").grid(row=vr, column=0, sticky="nw", pady=2)
            var_sv_fo = tk.StringVar(value=str(cl.get("fade_out") or 0))
            ttk.Entry(insp_grid, textvariable=var_sv_fo, width=18).grid(row=vr, column=1, sticky="ew", pady=2)
            vr += 1

            try:
                _br_sv0 = float(cl.get("brightness") or 0)
            except (TypeError, ValueError):
                _br_sv0 = 0.0
            var_sv_brightness = tk.StringVar(value=str(_br_sv0))
            ttk.Label(
                insp_grid,
                text="Độ sáng (-1…1; 0 = gốc; âm = tối hơn, dương = sáng hơn)",
            ).grid(row=vr, column=0, sticky="nw", pady=2)
            ttk.Entry(insp_grid, textvariable=var_sv_brightness, width=18).grid(row=vr, column=1, sticky="ew", pady=2)
            vr += 1
            _le_sv0 = str(cl.get("light_effect") or "none").strip().lower()
            if _le_sv0 not in VideoFilterManager.LIGHT_EFFECT_PRESETS:
                _le_sv0 = "none"
            var_sv_light_fx = tk.StringVar(
                value=VideoFilterManager.light_effect_normalize_to_label_ui(_le_sv0)
            )
            ttk.Label(
                insp_grid,
                text="Hiệu ứng ánh sáng (mô tả góc nhìn màu — áp khi «Áp dụng tất cả»)",
            ).grid(row=vr, column=0, sticky="nw", pady=2)
            ttk.Combobox(
                insp_grid,
                textvariable=var_sv_light_fx,
                values=VideoFilterManager.light_effect_single_combo_display_values(),
                width=36,
                state="readonly",
            ).grid(row=vr, column=1, sticky="ew", pady=2)
            vr += 1

            ttk.Separator(insp_grid, orient=tk.HORIZONTAL).grid(row=vr, column=0, columnspan=2, sticky="ew", pady=8)
            vr += 1
            ttk.Label(insp_grid, text="Transform & canvas", font=("Segoe UI", 9, "bold")).grid(
                row=vr, column=0, columnspan=2, sticky="w"
            )
            vr += 1

            var_sv_fh = tk.BooleanVar(value=bool(cl.get("flip_horizontal")))
            var_sv_fv = tk.BooleanVar(value=bool(cl.get("flip_vertical")))
            ttk.Checkbutton(insp_grid, text="Lật ngang", variable=var_sv_fh).grid(row=vr, column=0, sticky="w")
            ttk.Checkbutton(insp_grid, text="Lật dọc", variable=var_sv_fv).grid(row=vr, column=1, sticky="w")
            vr += 1

            var_sv_rot = tk.StringVar(value=str(int(cl.get("rotation") or 0)))
            ttk.Label(insp_grid, text="Xoay").grid(row=vr, column=0, sticky="w")
            ttk.Combobox(
                insp_grid,
                textvariable=var_sv_rot,
                values=["0", "90", "180", "270"],
                width=8,
                state="readonly",
            ).grid(row=vr, column=1, sticky="w")
            vr += 1

            var_sv_canvas = tk.StringVar(value=str(cl.get("canvas_mode") or "fit"))
            ttk.Label(insp_grid, text="Vào khung (Fit/Fill/Stretch)").grid(row=vr, column=0, sticky="w")
            ttk.Combobox(
                insp_grid,
                textvariable=var_sv_canvas,
                values=["fit", "fill", "stretch"],
                width=12,
                state="readonly",
            ).grid(row=vr, column=1, sticky="w")
            vr += 1

            zm0 = cl.get("zoom", 1.0)
            if zm0 in ("", None):
                zm0 = 1.0
            ttk.Label(insp_grid, text="Zoom (1 = vừa khung; >1 phóng to, <1 thu nhỏ)").grid(row=vr, column=0, sticky="w")
            var_sv_zoom = tk.StringVar(value=str(zm0))
            ttk.Entry(insp_grid, textvariable=var_sv_zoom, width=18).grid(row=vr, column=1, sticky="w")
            vr += 1

            var_sv_mute = tk.BooleanVar(value=bool(cl.get("muted")))
            ttk.Checkbutton(insp_grid, text="Tắt âm tiếng gốc clip", variable=var_sv_mute).grid(
                row=vr, column=0, columnspan=2, sticky="w"
            )
            vr += 1

            def _ve_validate_rotation() -> int:
                try:
                    rot_i = int(str(var_sv_rot.get()).strip())
                except ValueError as exc:
                    raise ValueError("Góc xoay không hợp lệ.") from exc
                if rot_i not in (0, 90, 180, 270):
                    raise ValueError("Xoay chỉ 0 / 90 / 180 / 270.")
                return rot_i

            def _ve_validate_canvas_zoom() -> tuple[str, float]:
                cm = str(var_sv_canvas.get() or "fit").strip().lower()
                if cm not in ("fit", "fill", "stretch"):
                    raise ValueError("Canvas: chọn fit, fill hoặc stretch.")
                try:
                    zm = float(str(var_sv_zoom.get()).strip())
                except ValueError as exc:
                    raise ValueError("Giá trị Zoom không hợp lệ.") from exc
                if zm < 0.1 or zm > 8.0:
                    raise ValueError("Zoom hợp lệ trong khoảng 0.1 — 8 (1 = vừa khung).")
                return cm, zm

            def _ve_apply_transform_canvas_subset(
                *,
                flip_rot: bool,
                layout: bool,
                mute_u: bool,
                defer_finalize: bool = False,
            ) -> list[str] | None:
                """
                Ghi lật/xoay, khung & zoom, tắt âm gốc — không đổi timeline / trim / tốc độ.

                Khi ``defer_finalize=True``: chỉ cập nhật project trong RAM, trả về nhãn đã áp;
                không ``save_project`` / không ``notify`` / không làm mới timeline & inspector (dùng khi gộp «Áp dụng tất cả»).

                Returns:
                    ``None`` nếu lỗi (đã hiện ``messagebox``). Danh sách nhãn (có thể rỗng) khi thành công.
                """
                if not project:
                    return [] if defer_finalize else None
                cid = sv_cid
                if not cid:
                    return [] if defer_finalize else None
                fc_tf = _find_clip(cid)
                if not fc_tf or not fc_tf[1] or str(fc_tf[1].get("type")) != "video":
                    messagebox.showerror("Transform & canvas", "Không tìm thấy clip video.", parent=root)
                    return None
                rot_i = 0
                try:
                    if flip_rot:
                        rot_i = _ve_validate_rotation()
                    if layout:
                        cm, zm = _ve_validate_canvas_zoom()
                except ValueError as ve:
                    messagebox.showerror("Transform & canvas", str(ve), parent=root)
                    return None
                try:
                    if flip_rot:
                        tm.update_clip(
                            project,
                            cid,
                            {
                                "flip_horizontal": bool(var_sv_fh.get()),
                                "flip_vertical": bool(var_sv_fv.get()),
                                "rotation": rot_i,
                            },
                            persist=False,
                            recompute_duration=False,
                        )
                    if layout:
                        tm.update_clip(
                            project,
                            cid,
                            {"zoom": zm},
                            persist=False,
                            recompute_duration=False,
                        )
                        tm.set_canvas_mode(project, cid, cm, persist=False, recompute_duration=False)
                    if mute_u:
                        tm.mute_clip(
                            project,
                            cid,
                            bool(var_sv_mute.get()),
                            persist=False,
                            recompute_duration=False,
                        )
                except Exception as ex:
                    messagebox.showerror("Transform & canvas", str(ex), parent=root)
                    return None
                bits: list[str] = []
                if flip_rot:
                    bits.append("lật & xoay")
                if layout:
                    bits.append("khung & zoom")
                if mute_u:
                    bits.append("âm gốc clip")
                if defer_finalize:
                    return bits
                pm.save_project(project)
                notify("Đã áp dụng: " + " · ".join(bits))
                refresh_timeline()
                refresh_inspector()
                _auto_preview_after_apply("transform & canvas")
                return bits

            _apply_transform_subset_ref["fn"] = _ve_apply_transform_canvas_subset

            pw = int(project.get("width") or 1080)
            ph = int(project.get("height") or 1920)

            media_imgs_ov = [
                m
                for m in (project.get("media") or [])
                if isinstance(m, dict) and str(m.get("type") or "") == "image"
            ]
            media_aud_ov = [
                m
                for m in (project.get("media") or [])
                if isinstance(m, dict) and str(m.get("type") or "") == "audio"
            ]
            audio_opts_ov, audio_ov_label_to_id, audio_ov_id_to_label = _ve_build_media_combo_maps(
                media_aud_ov, fallback="audio"
            )
            _, _logo_ov_l2i, logo_ov_id_to_label = _ve_build_media_combo_maps(
                media_imgs_ov, fallback="image", include_empty=False
            )

            def _defaults_logo_audio_text_ov() -> tuple[str, str, str]:
                ts_v = float(cl.get("timeline_start") or 0)
                te_v = ts_v + max(0.0, float(cl.get("duration") or 0))
                lv, av, tv = "", "", ""
                for oc in _find_track_clips_local("overlay"):
                    if not isinstance(oc, dict) or str(oc.get("type") or "") != "image":
                        continue
                    ots = float(oc.get("timeline_start") or 0)
                    ote = ots + max(0.0, float(oc.get("duration") or 0))
                    if _ve_iv_overlap(ts_v, te_v, ots, ote):
                        mid = str(oc.get("media_id") or "").strip()
                        if mid:
                            lv = logo_ov_id_to_label.get(mid, "")
                            if not lv:
                                for m in media_imgs_ov:
                                    if str(m.get("id")) == mid:
                                        lv = _ve_media_display_name(m, fallback="image")
                                        break
                        break
                for ac in _find_track_clips_local("audio"):
                    if not isinstance(ac, dict) or str(ac.get("type") or "") != "audio":
                        continue
                    ats = float(ac.get("timeline_start") or 0)
                    ate = ats + max(0.0, float(ac.get("duration") or 0))
                    if _ve_iv_overlap(ts_v, te_v, ats, ate):
                        mid = str(ac.get("media_id") or "").strip()
                        if mid:
                            av = audio_ov_id_to_label.get(mid, "")
                            if not av:
                                for m in media_aud_ov:
                                    if str(m.get("id")) == mid:
                                        av = _ve_media_display_name(m, fallback="audio")
                                        break
                        break
                for tc in _find_track_clips_local("text"):
                    if not isinstance(tc, dict) or not _clip_is_text_track_payload(tc):
                        continue
                    tts = float(tc.get("timeline_start") or 0)
                    tte = tts + max(0.0, float(tc.get("duration") or 0))
                    if _ve_iv_overlap(ts_v, te_v, tts, tte):
                        tv = str(tc.get("text") or "")
                        break
                return lv, av, tv

            d_logo_ov, d_audio_ov, d_text_ov = _defaults_logo_audio_text_ov()
            _ts0 = float(cl.get("timeline_start") or 0)
            _te0 = _ts0 + max(0.0, float(cl.get("duration") or 0))
            d_ov_vol = "1.0"
            _vsp0 = cl.get("speed", 1.0)
            if _vsp0 in ("", None):
                _vsp0 = 1.0
            d_ov_sp = str(_vsp0)
            try:
                if float(d_ov_sp) <= 0:
                    d_ov_sp = "1.0"
            except (TypeError, ValueError):
                d_ov_sp = "1.0"
            for _ac in _find_track_clips_local("audio"):
                if not isinstance(_ac, dict) or str(_ac.get("type") or "") != "audio":
                    continue
                _as = float(_ac.get("timeline_start") or 0.0)
                _ae = _as + max(0.0, float(_ac.get("duration") or 0.0))
                if not _ve_iv_overlap(_ts0, _te0, _as, _ae):
                    continue
                try:
                    d_ov_vol = str(float(_ac.get("volume") or 1.0))
                except (TypeError, ValueError):
                    d_ov_vol = "1.0"
                _aspeed = _ac.get("speed", None)
                if _aspeed not in (None, ""):
                    d_ov_sp = str(_aspeed)
                try:
                    if float(d_ov_sp) <= 0:
                        d_ov_sp = "1.0"
                except (TypeError, ValueError):
                    d_ov_sp = "1.0"
                break

            try:
                ts_q = float(cl.get("timeline_start") or 0)
                te_q = ts_q + max(0.0, float(cl.get("duration") or 0))
                canvas_w_q = max(1, int(project.get("width") or 1080))
                q_synced_logo = False
                for oc in _find_track_clips_local("overlay"):
                    if not isinstance(oc, dict) or str(oc.get("type") or "") != "image":
                        continue
                    ots = float(oc.get("timeline_start") or 0.0)
                    ote = ots + max(0.0, float(oc.get("duration") or 0.0))
                    if not _ve_iv_overlap(ts_q, te_q, ots, ote):
                        continue
                    q_synced_logo = True
                    mid0 = str(oc.get("media_id") or "").strip()
                    if mid0:
                        found_lbl = logo_ov_id_to_label.get(mid0, "")
                        if not found_lbl:
                            for m in media_imgs_ov:
                                if str(m.get("id")) == mid0:
                                    found_lbl = _ve_media_display_name(m, fallback="image")
                                    break
                        var_q_logo_media.set(found_lbl or _AUTO_LOGO_MEDIA_LBL)
                    elif d_logo_ov:
                        var_q_logo_media.set(d_logo_ov)
                    else:
                        var_q_logo_media.set(_AUTO_LOGO_MEDIA_LBL)
                    try:
                        opa0 = float(oc.get("opacity"))
                        if opa0 > 0:
                            var_q_logo_opacity.set(str(opa0))
                    except (TypeError, ValueError):
                        pass
                    try:
                        w0 = float(oc.get("width") or 0)
                        if w0 > 0:
                            rr = max(0.02, min(0.6, w0 / float(canvas_w_q)))
                            var_q_logo_size_ratio.set(f"{rr:g}")
                    except (TypeError, ValueError):
                        pass
                    var_q_logo_motion_mode.set("Bật mượt" if bool(oc.get("random_motion_enabled")) else "Tắt")
                    try:
                        var_q_logo_motion_interval.set(str(float(oc.get("random_motion_interval") or 2.0)))
                    except (TypeError, ValueError):
                        var_q_logo_motion_interval.set("2.0")
                    try:
                        var_q_logo_motion_seed.set(str(int(oc.get("random_motion_seed") or 0)))
                    except (TypeError, ValueError):
                        var_q_logo_motion_seed.set("0")
                    ph_q = int(project.get("height") or 1920)
                    lab_lp = _infer_logo_position_label(
                        oc.get("x"),
                        oc.get("y"),
                        oc.get("width"),
                        oc.get("height"),
                        canvas_w_q,
                        ph_q,
                    )
                    var_q_logo_position.set(lab_lp if lab_lp else "")
                    break
                if not q_synced_logo:
                    if d_logo_ov:
                        var_q_logo_media.set(d_logo_ov)
                    else:
                        var_q_logo_media.set(_AUTO_LOGO_MEDIA_LBL)
                    var_q_logo_position.set("")
                for tc in _find_track_clips_local("text"):
                    if not isinstance(tc, dict) or not _clip_is_text_track_payload(tc):
                        continue
                    tts = float(tc.get("timeline_start") or 0.0)
                    tte = tts + max(0.0, float(tc.get("duration") or 0.0))
                    if not _ve_iv_overlap(ts_q, te_q, tts, tte):
                        continue
                    try:
                        var_q_text_size.set(str(int(tc.get("font_size") or 48)))
                    except (TypeError, ValueError):
                        pass
                    col0 = str(tc.get("color") or "").strip()
                    if col0:
                        var_q_text_color.set(col0)
                    ff0 = str(tc.get("font_file") or "").strip()
                    if ff0:
                        hit_lab = ""
                        for lab, pth in q_font_label_to_path.items():
                            if str(pth) == ff0:
                                hit_lab = str(lab)
                                break
                        var_q_text_font.set(hit_lab if hit_lab else "Mặc định (trống)")
                    var_q_text_follow_logo.set("Theo logo" if bool(tc.get("random_motion_enabled")) else "Không theo logo")
                    break
            except NameError:
                pass

            ov_lf = ttk.Frame(insp_grid)
            ov_lf.grid(row=vr, column=0, columnspan=2, sticky="ew", pady=(8, 4))
            ov_lf.columnconfigure(0, weight=1)
            vr += 1

            var_ov_audio = tk.StringVar(value=d_audio_ov)
            var_ov_audio_vol = tk.StringVar(value=d_ov_vol)
            var_ov_audio_sp = tk.StringVar(value=d_ov_sp)
            var_ov_text.set(d_text_ov)

            def _pick_ov_id(raw: str) -> str:
                return _ve_media_id_from_combo(raw, audio_ov_label_to_id)

            def _ve_float_eq(a: Any, b: Any, *, eps: float = 1e-4) -> bool:
                try:
                    return abs(float(a) - float(b)) < eps
                except (TypeError, ValueError):
                    return a == b

            def _ve_inspector_clip_diff_patch(
                cur_clip: dict[str, Any],
                *,
                ts: float,
                ss: float,
                se: float,
                du: float,
                sp: float,
                vol: float,
                fi: float,
                fo: float,
                fh: bool,
                fv: bool,
                rot_i: int,
                zm: float,
                cm: str,
                muted: bool,
                brightness: float,
                light_effect: str,
            ) -> tuple[dict[str, Any], bool, bool, list[str]]:
                """
                So sánh UI với clip đang lưu trong project.

                Returns:
                    (patch cho ``update_clip``, đổi canvas_mode, đổi muted, nhãn ngắn cho thông báo)
                """
                labels: list[str] = []
                patch: dict[str, Any] = {}
                if not _ve_float_eq(ts, cur_clip.get("timeline_start") or 0):
                    patch["timeline_start"] = ts
                    labels.append("vị trí timeline")
                if not _ve_float_eq(ss, cur_clip.get("source_start") or 0):
                    patch["source_start"] = ss
                    labels.append("điểm cắt đầu")
                if not _ve_float_eq(se, cur_clip.get("source_end") or 0):
                    patch["source_end"] = se
                    labels.append("điểm cắt cuối")
                if not _ve_float_eq(du, cur_clip.get("duration") or 0):
                    patch["duration"] = du
                    labels.append("độ dài clip")
                if not _ve_float_eq(sp, cur_clip.get("speed") or 1.0):
                    patch["speed"] = sp
                    labels.append("tốc độ")
                if not _ve_float_eq(vol, cur_clip.get("volume") or 1.0):
                    patch["volume"] = vol
                    labels.append("âm lượng")
                if not _ve_float_eq(fi, cur_clip.get("fade_in") or 0):
                    patch["fade_in"] = fi
                    labels.append("fade vào")
                if not _ve_float_eq(fo, cur_clip.get("fade_out") or 0):
                    patch["fade_out"] = fo
                    labels.append("fade ra")
                if fh != bool(cur_clip.get("flip_horizontal")):
                    patch["flip_horizontal"] = fh
                    labels.append("lật ngang")
                if fv != bool(cur_clip.get("flip_vertical")):
                    patch["flip_vertical"] = fv
                    labels.append("lật dọc")
                if int(cur_clip.get("rotation") or 0) != rot_i:
                    patch["rotation"] = rot_i
                    labels.append("xoay")
                if not _ve_float_eq(zm, cur_clip.get("zoom") or 1.0):
                    patch["zoom"] = zm
                    labels.append("zoom")
                cm_cur = str(cur_clip.get("canvas_mode") or "fit").strip().lower()
                canvas_changed = cm != cm_cur
                if canvas_changed:
                    labels.append("vào khung")
                mute_changed = muted != bool(cur_clip.get("muted"))
                if mute_changed:
                    labels.append("tắt âm gốc")
                try:
                    br_cur = float(cur_clip.get("brightness") or 0)
                except (TypeError, ValueError):
                    br_cur = 0.0
                if not _ve_float_eq(brightness, br_cur):
                    patch["brightness"] = float(brightness)
                    labels.append("độ sáng")
                le_cur = str(cur_clip.get("light_effect") or "none").strip().lower()
                if le_cur not in VideoFilterManager.LIGHT_EFFECT_PRESETS:
                    le_cur = "none"
                le_new = str(light_effect or "none").strip().lower()
                if le_new not in VideoFilterManager.LIGHT_EFFECT_PRESETS:
                    le_new = "none"
                if le_new != le_cur:
                    patch["light_effect"] = le_new
                    labels.append("hiệu ứng ánh sáng")
                return patch, canvas_changed, mute_changed, labels

            def _ve_transform_branches_dirty(
                cur_clip: dict[str, Any],
                *,
                fh: bool,
                fv: bool,
                rot_i: int,
                cm: str,
                zm: float,
                muted: bool,
            ) -> tuple[bool, bool, bool]:
                """
                So sánh UI với clip trong project: nhánh nào còn lệch thì True (lật/xoay, khung&zoom, mute).
                """
                flip_d = (
                    fh != bool(cur_clip.get("flip_horizontal"))
                    or fv != bool(cur_clip.get("flip_vertical"))
                    or int(cur_clip.get("rotation") or 0) != rot_i
                )
                cm_cur = str(cur_clip.get("canvas_mode") or "fit").strip().lower()
                layout_d = cm != cm_cur or not _ve_float_eq(zm, cur_clip.get("zoom") or 1.0)
                mute_d = muted != bool(cur_clip.get("muted"))
                return flip_d, layout_d, mute_d

            def apply_single_video_inspector() -> None:
                if not project:
                    return
                cid = sv_cid
                if not cid:
                    return
                fc0 = _find_clip(cid)
                if not fc0 or not fc0[1] or str(fc0[1].get("type")) != "video":
                    messagebox.showerror("Clip", "Không tìm thấy clip video.", parent=root)
                    return
                sc_clip = bool(var_ve_apply_clip.get())
                sc_l = bool(var_ve_apply_ov_logo.get())
                sc_a = bool(var_ve_apply_ov_audio.get())
                sc_t = bool(var_ve_apply_ov_text.get())
                sc_ov = sc_l or sc_a or sc_t
                sc_ql = bool(var_ve_apply_quick_logo.get())
                sc_qt = bool(var_ve_apply_quick_text.get())
                sc_quick = sc_ql or sc_qt
                sc_tf_full = bool(var_ve_apply_tf_full.get())
                sc_tf_flip = bool(var_ve_apply_tf_flip.get())
                sc_tf_layout = bool(var_ve_apply_tf_layout.get())
                sc_tf_mute = bool(var_ve_apply_tf_mute.get())
                sc_tf_any = sc_tf_full or sc_tf_flip or sc_tf_layout or sc_tf_mute
                if not (sc_clip or sc_ov or sc_quick or sc_tf_any):
                    messagebox.showinfo(
                        "Áp dụng tất cả",
                        "Chọn ít nhất một mục trong «Phạm vi khi bấm Áp dụng tất cả» (cuối tab).",
                        parent=root,
                    )
                    return
                fn_tf_call = _apply_transform_subset_ref.get("fn")
                if sc_tf_any and fn_tf_call is None:
                    messagebox.showwarning(
                        "Áp dụng tất cả",
                        "Đang bật «Transform & canvas» trong «Phạm vi» nhưng cần inspector clip video đang mở — "
                        "chọn clip video trên timeline hoặc bỏ các mục Transform.",
                        parent=root,
                    )
                    return
                w_dim = h_dim = 0
                clip_scope_msg: str | None = None
                rot_i = 0
                cm = "fit"
                zm = 1.0
                fh = fv = muted = False
                ts = ss = se = du = sp = vol = fi = fo = 0.0
                br_ui = 0.0
                le_ui = "none"
                if sc_clip or sc_tf_any:
                    try:
                        rot_i = int(str(var_sv_rot.get()).strip())
                    except ValueError:
                        messagebox.showerror("Clip", "Góc xoay không hợp lệ.", parent=root)
                        return
                    if rot_i not in (0, 90, 180, 270):
                        messagebox.showerror("Clip", "Xoay chỉ 0 / 90 / 180 / 270.", parent=root)
                        return
                    cm = str(var_sv_canvas.get() or "fit").strip().lower()
                    if cm not in ("fit", "fill", "stretch"):
                        messagebox.showerror("Clip", "Canvas: chọn fit, fill hoặc stretch.", parent=root)
                        return
                    try:
                        zm = float(str(var_sv_zoom.get()).strip())
                    except ValueError:
                        messagebox.showerror("Clip", "Giá trị Zoom không hợp lệ.", parent=root)
                        return
                    if zm < 0.1 or zm > 8.0:
                        messagebox.showerror("Clip", "Zoom hợp lệ trong khoảng 0.1 — 8 (1 = vừa khung).", parent=root)
                        return
                    fh = bool(var_sv_fh.get())
                    fv = bool(var_sv_fv.get())
                    muted = bool(var_sv_mute.get())
                if sc_clip:
                    try:
                        ts = float(str(var_sv_ts.get()).strip())
                        ss = float(str(var_sv_ss.get()).strip())
                        se = float(str(var_sv_se.get()).strip())
                        du = float(str(var_sv_du.get()).strip())
                        sp = float(str(var_sv_sp.get()).strip())
                        vol = float(str(var_sv_vol.get()).strip())
                        fi = float(str(var_sv_fi.get()).strip())
                        fo = float(str(var_sv_fo.get()).strip())
                        br_ui = float(str(var_sv_brightness.get()).strip())
                        le_ui = VideoFilterManager.light_effect_label_ui_to_key(str(var_sv_light_fx.get() or "").strip())
                        if not le_ui:
                            le_ui = "none"
                    except ValueError:
                        messagebox.showerror(
                            "Clip",
                            "Giá trị số không hợp lệ (timeline / nguồn / độ dài / tốc độ / âm / fade / độ sáng).",
                            parent=root,
                        )
                        return
                    br_ui = max(-1.0, min(1.0, br_ui))
                    if sp <= 0:
                        messagebox.showerror("Clip", "Tốc độ phải > 0.", parent=root)
                        return
                    if se <= ss + 0.05:
                        messagebox.showerror(
                            "Clip",
                            "Điểm cắt cuối phải lớn hơn điểm cắt đầu.",
                            parent=root,
                        )
                        return
                    du = timeline_duration_from_source(ss, se, sp)
                    cur = fc0[1]
                    only_diff = bool(var_ve_clip_only_diff_vs_file.get())
                    if only_diff:
                        patch, canvas_changed, mute_changed, diff_labels = _ve_inspector_clip_diff_patch(
                            cur,
                            ts=ts,
                            ss=ss,
                            se=se,
                            du=du,
                            sp=sp,
                            vol=vol,
                            fi=fi,
                            fo=fo,
                            fh=fh,
                            fv=fv,
                            rot_i=rot_i,
                            zm=zm,
                            cm=cm,
                            muted=muted,
                            brightness=br_ui,
                            light_effect=le_ui,
                        )
                    else:
                        patch = {
                            "timeline_start": ts,
                            "source_start": ss,
                            "source_end": se,
                            "duration": du,
                            "speed": sp,
                            "volume": vol,
                            "fade_in": fi,
                            "fade_out": fo,
                            "brightness": br_ui,
                            "light_effect": le_ui,
                            "flip_horizontal": fh,
                            "flip_vertical": fv,
                            "rotation": rot_i,
                            "zoom": zm,
                        }
                        cm_cur = str(cur.get("canvas_mode") or "fit").strip().lower()
                        canvas_changed = cm != cm_cur
                        mute_changed = muted != bool(cur.get("muted"))
                        diff_labels = ["ghi đủ từ inspector (không lọc trùng file)"]
                    w_dim = max(2, int(project.get("width") or 1080))
                    h_dim = max(2, int(project.get("height") or 1920))
                    _layout_refresh_keys = {
                        "flip_horizontal",
                        "flip_vertical",
                        "rotation",
                        "zoom",
                        "timeline_start",
                        "source_start",
                        "source_end",
                        "duration",
                        "speed",
                    }
                    need_crop_scale = canvas_changed or bool(_layout_refresh_keys & patch.keys())
                    try:
                        if not patch and not canvas_changed and not mute_changed:
                            clip_scope_msg = f"clip {w_dim}×{h_dim} (không đổi so với đã lưu)"
                        else:
                            if patch:
                                tm.update_clip(project, cid, patch, persist=False, recompute_duration=False)
                            if canvas_changed:
                                tm.set_canvas_mode(project, cid, cm, persist=False, recompute_duration=False)
                            if mute_changed:
                                tm.mute_clip(project, cid, muted, persist=False, recompute_duration=False)
                            if need_crop_scale:
                                tm.crop_clip(
                                    project,
                                    cid,
                                    {
                                        "enabled": False,
                                        "x": 0,
                                        "y": 0,
                                        "width": w_dim,
                                        "height": h_dim,
                                    },
                                    persist=False,
                                    recompute_duration=False,
                                )
                                tm.update_clip(
                                    project,
                                    cid,
                                    {
                                        "scale": {
                                            "enabled": True,
                                            "width": w_dim,
                                            "height": h_dim,
                                            "keep_aspect": True,
                                        }
                                    },
                                    persist=False,
                                    recompute_duration=True,
                                )
                            elif patch:
                                tm.update_clip(project, cid, {}, persist=False, recompute_duration=True)
                            _sync_keys_ins = frozenset(
                                {"duration", "timeline_start", "source_start", "source_end", "speed"}
                            )
                            if _sync_keys_ins & set(patch.keys()):
                                sp_ins = float(patch["speed"]) if "speed" in patch else None
                                _sync_overlapping_timeline_audio_to_video(cid, sp_ins)
                            clip_scope_msg = f"clip {w_dim}×{h_dim}: {', '.join(diff_labels)}"
                    except Exception as ex:
                        messagebox.showerror("Clip", str(ex), parent=root)
                        return
                else:
                    w_dim = max(2, int(project.get("width") or 1080))
                    h_dim = max(2, int(project.get("height") or 1920))

                tf_defer_bits: list[str] = []
                tf_skip_note: str | None = None
                if sc_tf_any and fn_tf_call is not None:
                    fc_tf_l = _find_clip(cid)
                    cur_tf_l = fc_tf_l[1] if fc_tf_l and fc_tf_l[1] else {}
                    tf_diff = bool(var_ve_tf_only_diff_vs_file.get())
                    d_fr, d_lay, d_mu = _ve_transform_branches_dirty(
                        cur_tf_l, fh=fh, fv=fv, rot_i=rot_i, cm=cm, zm=zm, muted=muted
                    )
                    if sc_tf_full:
                        want_fr = True if not tf_diff else d_fr
                        want_lay = True if not tf_diff else d_lay
                        want_mu = True if not tf_diff else d_mu
                    else:
                        want_fr = bool(sc_tf_flip) and (not tf_diff or d_fr)
                        want_lay = bool(sc_tf_layout) and (not tf_diff or d_lay)
                        want_mu = bool(sc_tf_mute) and (not tf_diff or d_mu)
                    if not (want_fr or want_lay or want_mu):
                        tf_skip_note = "transform (đã khớp file — bỏ qua)"
                    else:
                        res_tf = fn_tf_call(
                            flip_rot=want_fr,
                            layout=want_lay,
                            mute_u=want_mu,
                            defer_finalize=True,
                        )
                        if res_tf is None:
                            return
                        tf_defer_bits = list(res_tf)

                lg = au = txc = 0
                if sc_ov:
                    try:
                        lo = (
                            float(str(var_q_logo_opacity.get() or "").strip() or "0.92") if sc_l else 0.92
                        )
                        lr = (
                            float(str(var_q_logo_size_ratio.get() or "").strip() or "0.15") if sc_l else 0.15
                        )
                        avl = 1.0
                        av_sp_apply: float | None = None
                        if sc_a:
                            avl = float(str(var_ov_audio_vol.get()).strip() or "1.0")
                            av_sp_apply = float(str(var_ov_audio_sp.get()).strip() or "1.0")
                    except ValueError:
                        messagebox.showerror("Clip", "Opacity logo / tỉ lệ / âm lượng track / tốc độ âm không hợp lệ.", parent=root)
                        return
                    if sc_a and av_sp_apply is not None and av_sp_apply <= 0:
                        messagebox.showerror("Clip", "Tốc độ âm track phụ phải > 0.", parent=root)
                        return

                    rows_one = [(cid, fc0[1])]
                    _lm_ins = (
                        _resolve_logo_media_id_from_ui(
                            str(var_q_logo_media.get() or ""), allow_auto=True
                        )
                        if sc_l
                        else ""
                    )
                    if sc_l and not _lm_ins:
                        notify(
                            "Logo: chưa có ảnh trong Media — bấm «Thêm logo / ảnh» hoặc chọn tên file trong combobox."
                        )
                    if _lm_ins and not _media_id_valid_for_type(_lm_ins, "image"):
                        _lm_ins = ""
                    _am_ins = _pick_ov_id(var_ov_audio.get()) if sc_a else ""
                    if _am_ins and not _media_id_valid_for_type(_am_ins, "audio"):
                        _am_ins = ""
                    _tx_ins = str(var_ov_text.get() or "").strip() if sc_t else ""
                    _want_l = bool(_lm_ins)
                    _want_a = bool(_am_ins)
                    _want_t = bool(_tx_ins)
                    loc_m = aoc_m = toc_m = "replace"
                    if _want_l or _want_a or _want_t:
                        cf_l, cf_a, cf_t = _conflicts_for_video_rows(rows_one)
                        if (_want_l and cf_l) or (_want_a and cf_a) or (_want_t and cf_t):
                            modes_ins = _prompt_apply_conflict_modes(
                                ask_logo=cf_l,
                                ask_audio=cf_a,
                                ask_text=cf_t,
                                batch_count=1,
                            )
                            if modes_ins is None:
                                return
                            loc_m, aoc_m, toc_m = modes_ins
                        lg, au, txc = apply_logo_audio_text_to_video_rows(
                            rows_one,
                            logo_mid=_lm_ins,
                            logo_opacity=lo,
                            logo_ratio=lr,
                            logo_corner=str(var_q_logo_position.get() or "").strip(),
                            audio_mid=_am_ins,
                            audio_volume=avl,
                            audio_speed=av_sp_apply if sc_a else None,
                            text_content=_tx_ins,
                            logo_on_conflict=loc_m if _want_l else "skip",
                            audio_on_conflict=aoc_m if _want_a else "skip",
                            text_on_conflict=toc_m if _want_t else "skip",
                        )
                prev_tl_sel2 = list(tree_tl.selection())
                q_lo = q_tx = 0
                if sc_quick:
                    _suppress_tl_inspector_refresh["v"] = True
                    try:
                        tree_tl.selection_set((cid,))
                        try:
                            for gx in list(tree_tl_grouped.selection()):
                                tree_tl_grouped.selection_remove(gx)
                        except Exception:
                            pass
                        if sc_ql:
                            q_lo = quick_edit_logo_for_selected_videos(quiet=True)
                        if sc_qt:
                            q_tx = quick_edit_text_for_selected_videos(quiet=True)
                    finally:
                        _suppress_tl_inspector_refresh["v"] = False
                        try:
                            if prev_tl_sel2:
                                tree_tl.selection_set(tuple(prev_tl_sel2))
                        except Exception:
                            pass
                pm.save_project(project)
                parts: list[str] = []
                if clip_scope_msg is not None:
                    parts.append(clip_scope_msg)
                if tf_defer_bits:
                    parts.append("transform: " + " · ".join(tf_defer_bits))
                if tf_skip_note:
                    parts.append(tf_skip_note)
                if sc_ov and (lg or au or txc):
                    parts.append(f"inspector L/A/T = {lg}/{au}/{txc}")
                if sc_quick and (q_lo or q_tx):
                    parts.append(f"nhanh logo/chữ = {q_lo}/{q_tx}")
                msg = "Đã áp dụng: " + " · ".join(parts) if parts else "Đã áp dụng (không ghi thêm thao tác)."
                notify(msg)
                refresh_timeline()
                refresh_inspector()
                if sc_ov and au:
                    notify(
                        msg
                        + " — bấm «Preview nháp» khi cần (không tự render sau khi thêm âm, tránh treo)."
                    )
                else:
                    _auto_preview_after_apply("chỉnh clip video")

            _apply_batch_video_ref["fn"] = apply_single_video_inspector

            r_ov = 0
            ttk.Label(
                ov_lf,
                text=(
                    "Logo (ảnh, độ mờ, vị trí): khung «Logo» ở cuối tab. "
                    "Nội dung chữ + cỡ/màu/font: khung «Chữ» ngay bên dưới (đồng bộ clip này). "
                    "Âm track phụ: nhóm dưới đây."
                ),
                foreground="#1a4480",
                font=("Segoe UI", 8),
                wraplength=440,
                justify=tk.LEFT,
            ).grid(row=r_ov, column=0, sticky="ew", pady=(0, 6))
            r_ov += 1
            lf_ov_audio = ttk.LabelFrame(ov_lf, text="Âm thanh — track phụ", padding=6)
            lf_ov_audio.grid(row=r_ov, column=0, sticky="ew", pady=(0, 6))
            lf_ov_audio.columnconfigure(1, weight=1)
            ttk.Label(lf_ov_audio, text="File âm thanh").grid(row=0, column=0, sticky="w")
            ttk.Combobox(lf_ov_audio, textvariable=var_ov_audio, values=audio_opts_ov, state="readonly").grid(
                row=0, column=1, sticky="ew", padx=(8, 0)
            )
            ttk.Label(lf_ov_audio, text="Âm lượng").grid(row=1, column=0, sticky="w", pady=(6, 0))
            ttk.Entry(lf_ov_audio, textvariable=var_ov_audio_vol, width=12).grid(row=1, column=1, sticky="w", padx=(8, 0), pady=(6, 0))
            ttk.Label(lf_ov_audio, text="Tốc độ track phụ (1 = bình thường)").grid(row=2, column=0, sticky="nw", pady=(6, 0))
            fr_ov_sp = ttk.Frame(lf_ov_audio)
            fr_ov_sp.grid(row=2, column=1, sticky="w", padx=(8, 0), pady=(6, 0))
            ttk.Entry(fr_ov_sp, textvariable=var_ov_audio_sp, width=10).pack(side=tk.LEFT)
            ttk.Button(
                fr_ov_sp,
                text="Khớp video",
                command=lambda: var_ov_audio_sp.set(str(var_sv_sp.get()).strip() or "1.0"),
            ).pack(side=tk.LEFT, padx=(6, 0))
            ttk.Label(
                lf_ov_audio,
                text="Khi «Áp clip», tốc độ track phụ tự khớp tốc độ video; chỉnh ô này rồi bật phạm vi «track phụ» để ghi đè.",
                font=("Segoe UI", 8),
                foreground="gray",
                wraplength=420,
                justify=tk.LEFT,
            ).grid(row=3, column=0, columnspan=2, sticky="w", pady=(4, 0))
            r_ov += 1
            ttk.Label(
                ov_lf,
                text=(
                    "«Áp dụng tất cả» ghi theo ô «Phạm vi» (cuối tab): clip đầy đủ, transform & canvas (không timeline), "
                    "track phủ, chỉnh nhanh. Logo inspector chỉ áp khi đã chọn file ảnh cụ thể (không dùng «Tự động» ở đây)."
                ),
                foreground="gray",
                font=("Segoe UI", 8),
                wraplength=420,
            ).grid(row=r_ov, column=0, sticky="w", pady=(6, 0))
        elif ctype == "image":
            pw = int(project.get("width") or 1080)
            ph = int(project.get("height") or 1920)
            ttk.Label(
                insp_grid,
                text=(
                    f"Logo / ảnh phủ trên video — canvas dự án {pw}×{ph} px. "
                    "Góc (X,Y) là mép trái-trên của logo; W/H là kích thước hiển thị."
                ),
                foreground="#1a4480",
                font=("Segoe UI", 8),
                wraplength=320,
            ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(0, 6))
            row += 1

            for lab, key in (
                ("Bắt đầu hiện trên timeline (giây)", "timeline_start"),
                ("Độ dài hiện logo (giây)", "duration"),
            ):
                add_num(lab, key, row)
                row += 1

            POS_CORNER = (
                "Giữa màn hình",
                "Góc trái trên",
                "Góc phải trên",
                "Góc trái dưới",
                "Góc phải dưới",
            )
            var_logo_corner = tk.StringVar(value=POS_CORNER[0])
            var_logo_margin = tk.StringVar(value="24")

            def apply_logo_corner(_e: Any = None) -> None:
                if not project or not selected_clip_id:
                    return
                picked = var_logo_corner.get()
                try:
                    mg = max(0, int(float(str(var_logo_margin.get()).strip() or "24")))
                except ValueError:
                    mg = 24
                fc_res = _find_clip(selected_clip_id)
                if not fc_res or not fc_res[1]:
                    return
                clp = fc_res[1]
                ow = max(2, int(float(clp.get("width") or 180)))
                oh = max(2, int(float(clp.get("height") or 180)))

                if picked == POS_CORNER[0]:
                    nx = max(0, (pw - ow) // 2)
                    ny = max(0, (ph - oh) // 2)
                elif picked == POS_CORNER[1]:
                    nx, ny = mg, mg
                elif picked == POS_CORNER[2]:
                    nx = max(mg, pw - ow - mg)
                    ny = mg
                elif picked == POS_CORNER[3]:
                    nx = mg
                    ny = max(mg, ph - oh - mg)
                else:
                    nx = max(mg, pw - ow - mg)
                    ny = max(mg, ph - oh - mg)

                try:
                    tm.update_clip(project, selected_clip_id, {"x": float(nx), "y": float(ny)})
                    pm.save_project(project)
                    refresh_inspector()
                    notify(f"Logo: {picked} (lề {mg}px).")
                except Exception as ex:
                    messagebox.showerror("Logo", str(ex))

            lf_corner = ttk.LabelFrame(insp_grid, text=f"Vị trí nhanh (canvas {pw}×{ph})", padding=6)
            lf_corner.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(0, 8))
            row += 1
            r0 = ttk.Frame(lf_corner)
            r0.pack(fill=tk.X)
            ttk.Label(r0, text="Góc / vị trí:").pack(side=tk.LEFT)
            cb_corner = ttk.Combobox(
                r0,
                textvariable=var_logo_corner,
                values=list(POS_CORNER),
                width=22,
                state="readonly",
            )
            cb_corner.pack(side=tk.LEFT, padx=(6, 0))
            cb_corner.bind("<<ComboboxSelected>>", apply_logo_corner)
            ttk.Label(r0, text="Lề (px):").pack(side=tk.LEFT, padx=(12, 0))
            em = ttk.Entry(r0, textvariable=var_logo_margin, width=5)
            em.pack(side=tk.LEFT)
            em.bind("<FocusOut>", apply_logo_corner)
            ttk.Button(r0, text="Áp vị trí", command=apply_logo_corner).pack(side=tk.LEFT, padx=(8, 0))

            def set_logo_opacity(val: float) -> None:
                if not project or not selected_clip_id:
                    return
                try:
                    v = max(0.0, min(1.0, float(val)))
                    tm.update_clip(project, selected_clip_id, {"opacity": v})
                    pm.save_project(project)
                    refresh_inspector()
                    notify(f"Độ mờ logo = {v:.0%}")
                except Exception as ex:
                    messagebox.showerror("Logo", str(ex))

            lf_op = ttk.LabelFrame(insp_grid, text="Độ mờ nhanh", padding=6)
            lf_op.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(0, 8))
            row += 1
            fro = ttk.Frame(lf_op)
            fro.pack(fill=tk.X)
            for txt, v in (("100%", 1.0), ("80%", 0.8), ("60%", 0.6), ("40%", 0.4), ("20%", 0.2)):
                ttk.Button(fro, text=txt, width=7, command=lambda vv=v: set_logo_opacity(vv)).pack(side=tk.LEFT, padx=2)

            var_rand_mv = tk.BooleanVar(value=bool(cl.get("random_motion_enabled")))
            var_rand_int = tk.StringVar(value=str(cl.get("random_motion_interval") or 2.0))
            var_rand_seed = tk.StringVar(value=str(int(cl.get("random_motion_seed") or 0)))
            var_rand_smooth = tk.BooleanVar(value=bool(cl.get("random_motion_smooth", False)))

            def save_rand_motion_img(_e: Any = None) -> None:
                if not project or not selected_clip_id:
                    return
                try:
                    ri = float(str(var_rand_int.get()).strip() or "2")
                    ri = max(0.25, min(120.0, ri))
                    rs = int(str(var_rand_seed.get()).strip() or "0")
                    tm.update_clip(
                        project,
                        selected_clip_id,
                        {
                            "random_motion_enabled": bool(var_rand_mv.get()),
                            "random_motion_interval": ri,
                            "random_motion_seed": rs,
                            "random_motion_smooth": bool(var_rand_smooth.get()),
                        },
                    )
                    pm.save_project(project)
                    sm = "mượt" if var_rand_smooth.get() else "nhảy ô"
                    notify(
                        f"Logo động: {'bật' if var_rand_mv.get() else 'tắt'}, bước {ri}s, seed {rs}, {sm}."
                    )
                except Exception as ex:
                    messagebox.showerror("Logo", str(ex))

            lf_rm = ttk.LabelFrame(insp_grid, text="Vị trí đổi theo thời gian (toàn khung video)", padding=6)
            lf_rm.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(0, 8))
            row += 1
            rrm = ttk.Frame(lf_rm)
            rrm.pack(fill=tk.X)
            ttk.Checkbutton(
                rrm,
                text="Bật (quỹ đạo theo seed + bước giây)",
                variable=var_rand_mv,
                command=save_rand_motion_img,
            ).pack(side=tk.LEFT)
            ttk.Label(rrm, text="Bước (giây):").pack(side=tk.LEFT, padx=(10, 0))
            eri = ttk.Entry(rrm, textvariable=var_rand_int, width=6)
            eri.pack(side=tk.LEFT)
            eri.bind("<FocusOut>", save_rand_motion_img)
            ttk.Label(rrm, text="Seed:").pack(side=tk.LEFT, padx=(10, 0))
            ers = ttk.Entry(rrm, textvariable=var_rand_seed, width=8)
            ers.pack(side=tk.LEFT)
            ers.bind("<FocusOut>", save_rand_motion_img)
            rrm2 = ttk.Frame(lf_rm)
            rrm2.pack(fill=tk.X, pady=(4, 0))
            ttk.Checkbutton(
                rrm2,
                text="Chuyển động mượt (nội suy giữa các điểm; tắt = nhảy ô)",
                variable=var_rand_smooth,
                command=save_rand_motion_img,
            ).pack(side=tk.LEFT)
            ttk.Label(
                lf_rm,
                text="Cùng seed + cùng bước → cùng quỹ đạo khi xuất lại. Seed 0 = hệ số cũ (tương thích dự án trước).",
                foreground="#555",
                font=("Segoe UI", 8),
                wraplength=320,
            ).pack(anchor="w", pady=(4, 0))
            ttk.Label(
                lf_rm,
                text="Khi bật động: X/Y cố định không dùng khi render — W/H logo vẫn có hiệu lực.",
                foreground="#555",
                font=("Segoe UI", 8),
                wraplength=320,
            ).pack(anchor="w", pady=(2, 0))

            for lab, key in (
                ("Vị trí X (khi tắt «đổi theo thời gian»)", "x"),
                ("Vị trí Y (khi tắt «đổi theo thời gian»)", "y"),
                ("Chiều rộng hiển thị (px)", "width"),
                ("Chiều cao hiển thị (px)", "height"),
                ("Độ mờ / alpha (0–1, 1 = rõ nét nhất)", "opacity"),
            ):
                add_num(lab, key, row)
                row += 1
            ap_row = row
            ttk.Label(insp_grid, text="Hiệu ứng chuyển động (fade logo — không xung đột vị trí động)").grid(row=ap_row, column=0, sticky="nw", pady=2)
            anim_vals = list(KeyframeAnimationManager.PRESETS)
            var_anim = tk.StringVar(value=str(cl.get("animation_preset") or "none"))
            cb_anim = ttk.Combobox(insp_grid, textvariable=var_anim, values=anim_vals, width=16, state="readonly")
            cb_anim.grid(row=ap_row, column=1, sticky="w", pady=2)

            def save_anim(_e: Any = None) -> None:
                if project and selected_clip_id:
                    try:
                        kf_mgr.add_animation_preset(project, selected_clip_id, var_anim.get())
                        pm.save_project(project)
                        notify(f"Animation = {var_anim.get()}")
                    except Exception as ex:
                        messagebox.showerror("Animation", str(ex))

            cb_anim.bind("<<ComboboxSelected>>", save_anim)
            row += 1
        elif ctype == "text":
            add_txt("Nội dung chữ", "text", row)
            row += 1
            for lab, key in (
                ("Bắt đầu trên timeline (giây)", "timeline_start"),
                ("Độ dài hiện chữ (giây)", "duration"),
            ):
                add_num(lab, key, row)
                row += 1

            var_rand_txt = tk.BooleanVar(value=bool(cl.get("random_motion_enabled")))
            var_rand_txt_int = tk.StringVar(value=str(cl.get("random_motion_interval") or 2.0))
            var_rand_txt_seed = tk.StringVar(value=str(int(cl.get("random_motion_seed") or 0)))
            var_rand_txt_smooth = tk.BooleanVar(value=bool(cl.get("random_motion_smooth", False)))

            def save_rand_motion_txt(_e: Any = None) -> None:
                if not project or not selected_clip_id:
                    return
                try:
                    ri = float(str(var_rand_txt_int.get()).strip() or "2")
                    ri = max(0.25, min(120.0, ri))
                    rs = int(str(var_rand_txt_seed.get()).strip() or "0")
                    tm.update_clip(
                        project,
                        selected_clip_id,
                        {
                            "random_motion_enabled": bool(var_rand_txt.get()),
                            "random_motion_interval": ri,
                            "random_motion_seed": rs,
                            "random_motion_smooth": bool(var_rand_txt_smooth.get()),
                        },
                    )
                    pm.save_project(project)
                    sm = "mượt" if var_rand_txt_smooth.get() else "nhảy ô"
                    notify(f"Chữ động: {'bật' if var_rand_txt.get() else 'tắt'}, bước {ri}s, seed {rs}, {sm}.")
                except Exception as ex:
                    messagebox.showerror("Chữ", str(ex))

            lf_rmt = ttk.LabelFrame(insp_grid, text="Chữ chạy / đổi vị trí theo thời gian", padding=6)
            lf_rmt.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(0, 8))
            row += 1
            rrt = ttk.Frame(lf_rmt)
            rrt.pack(fill=tk.X)
            ttk.Checkbutton(
                rrt,
                text="Bật (quỹ đạo theo seed + bước)",
                variable=var_rand_txt,
                command=save_rand_motion_txt,
            ).pack(side=tk.LEFT)
            ttk.Label(rrt, text="Bước (giây):").pack(side=tk.LEFT, padx=(10, 0))
            ert = ttk.Entry(rrt, textvariable=var_rand_txt_int, width=6)
            ert.pack(side=tk.LEFT)
            ert.bind("<FocusOut>", save_rand_motion_txt)
            ttk.Label(rrt, text="Seed:").pack(side=tk.LEFT, padx=(10, 0))
            erts = ttk.Entry(rrt, textvariable=var_rand_txt_seed, width=8)
            erts.pack(side=tk.LEFT)
            erts.bind("<FocusOut>", save_rand_motion_txt)
            rrt2 = ttk.Frame(lf_rmt)
            rrt2.pack(fill=tk.X, pady=(4, 0))
            ttk.Checkbutton(
                rrt2,
                text="Chuyển động mượt (nội suy; tắt = nhảy ô)",
                variable=var_rand_txt_smooth,
                command=save_rand_motion_txt,
            ).pack(side=tk.LEFT)
            ttk.Label(
                lf_rmt,
                text="Cùng seed + bước → cùng quỹ đạo. Seed 0 = hệ số cũ. Khi bật: X/Y cố định không dùng khi render.",
                foreground="#555",
                font=("Segoe UI", 8),
                wraplength=320,
            ).pack(anchor="w", pady=(4, 0))

            for lab, key in (
                ("Vị trí X (pixel, khi tắt động)", "x"),
                ("Vị trí Y (pixel, khi tắt động)", "y"),
                ("Cỡ chữ (px)", "font_size"),
                ("Fade vào (giây)", "fade_in"),
                ("Fade ra (giây)", "fade_out"),
            ):
                add_num(lab, key, row)
                row += 1
            add_txt("Màu (#RRGGBB hoặc white)", "color", row)
            row += 1
            add_txt("Đường dẫn file font (.ttf)", "font_file", row)
            row += 1

    def _find_clip(cid: str) -> tuple[dict[str, Any], dict[str, Any]] | tuple[None, None]:
        if not project:
            return None, None
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict):
                continue
            for cl in tr.get("clips") or []:
                if isinstance(cl, dict) and str(cl.get("id")) == cid:
                    return tr, cl
        return None, None

    def on_tl_select(_e: Any) -> None:
        nonlocal selected_clip_id
        sel = tree_tl.selection()
        if not sel:
            selected_clip_id = None
        else:
            sel_lst = list(sel)
            sel_set = set(sel_lst)
            fc = str(tree_tl.focus() or "").strip()
            if fc and fc in sel_set:
                selected_clip_id = fc
            else:
                selected_clip_id = sel_lst[-1]
        if _suppress_tl_inspector_refresh.get("v"):
            return
        # Tránh trộn selection cũ ở tab «Gộp theo video» khi user chọn trực tiếp trên bảng chi tiết.
        try:
            for g in tree_tl_grouped.selection():
                tree_tl_grouped.selection_remove(g)
        except Exception:
            pass
        refresh_inspector()

    tree_tl.bind("<<TreeviewSelect>>", on_tl_select)

    def on_grouped_tl_select(_e: Any = None) -> None:
        if _suppress_tl_inspector_refresh.get("v"):
            return
        sel = tree_tl_grouped.selection()
        if not sel:
            return
        cids: list[str] = []
        seen_g: set[str] = set()
        for gid in sel:
            cid = str(grouped_video_to_clip.get(str(gid) or "") or "").strip()
            if cid and cid not in seen_g:
                seen_g.add(cid)
                cids.append(cid)
        if not cids:
            return
        try:
            tree_tl.selection_set(tuple(cids))
            tree_tl.see(cids[0])
            tl_nb.select(tl_tab_detail)
            refresh_inspector()
        except Exception:
            pass

    tree_tl_grouped.bind("<<TreeviewSelect>>", on_grouped_tl_select)

    def on_edit_sum_select(_e: Any = None) -> None:
        if _suppress_tl_inspector_refresh.get("v"):
            return
        cids = _clip_ids_from_edit_sum_selection()
        if not cids:
            return
        try:
            _suppress_tl_inspector_refresh["v"] = True
            tree_tl.selection_set(tuple(cids))
            tree_tl.see(cids[0])
            tl_nb.select(tl_tab_detail)
        finally:
            _suppress_tl_inspector_refresh["v"] = False
        refresh_inspector()

    tree_edit_sum.bind("<<TreeviewSelect>>", on_edit_sum_select)

    def refresh_all() -> None:
        refresh_media_tree()
        refresh_timeline()
        refresh_inspector()
        sfn = stock_audio_refresh_ref.get("fn")
        if callable(sfn):
            sfn()
        qlogo = _q_logo_media_combo_refresh.get("fn")
        if callable(qlogo):
            qlogo()

    def media_action_add_timeline() -> None:
        if not project:
            return
        if _active_media_kind() == "stock":
            add_selected_stock_to_project(as_bgm=False)
            return
        sel = _selected_media_ids()
        if not sel:
            messagebox.showinfo("Timeline", "Chọn media.")
            return
        added = 0
        skipped = 0
        errs: list[str] = []
        type_counts: dict[str, int] = {"video": 0, "image": 0, "audio": 0}
        batch_n = len(sel)
        defer_tm = batch_n > 1
        prog_step = _ve_batch_progress_step(batch_n)
        _new_v: list[dict[str, Any]] = []
        for j, mid in enumerate(sel, start=1):
            media = _find_media(mid)
            if not media:
                skipped += 1
                if prog_step and (j == batch_n or j % prog_step == 0):
                    _ve_batch_status_progress(j, batch_n, "thêm lên timeline")
                continue
            mt = str(media.get("type") or "")
            try:
                if mt == "video":
                    _new_v.clear()
                    tm.add_clip(
                        project,
                        mid,
                        "video",
                        persist=False,
                        recompute_duration=not defer_tm,
                        out_new_clip=_new_v,
                    )
                    if _new_v:
                        _new_v[0]["timeline_start"] = 0.0
                    type_counts["video"] += 1
                elif mt == "image":
                    tm.add_clip(project, mid, "overlay", persist=False, recompute_duration=not defer_tm)
                    type_counts["image"] += 1
                elif mt == "audio":
                    tm.add_clip(project, mid, "audio", persist=False, recompute_duration=not defer_tm)
                    type_counts["audio"] += 1
                else:
                    skipped += 1
                    if prog_step and (j == batch_n or j % prog_step == 0):
                        _ve_batch_status_progress(j, batch_n, "thêm lên timeline")
                    continue
                added += 1
            except Exception as e:
                errs.append(f"{media.get('name') or mid}: {e}")
            if prog_step and (j == batch_n or j % prog_step == 0):
                _ve_batch_status_progress(j, batch_n, "thêm lên timeline")
        if defer_tm and added > 0:
            tm.refresh_project_duration(project)
        if added > 0:
            pm.save_project(project)
            refresh_timeline()
            parts: list[str] = []
            if type_counts["video"]:
                parts.append(f"{type_counts['video']} video")
            if type_counts["image"]:
                parts.append(f"{type_counts['image']} ảnh/logo")
            if type_counts["audio"]:
                parts.append(f"{type_counts['audio']} audio")
            extra = f", bỏ qua {skipped}" if skipped else ""
            notify(f"Đã thêm {added} clip lên timeline ({', '.join(parts)}){extra}.")
        elif skipped > 0:
            messagebox.showinfo("Timeline", "Các mục đã chọn không thuộc loại hỗ trợ (video/ảnh/audio).")
        if errs:
            messagebox.showwarning(
                "Timeline",
                "Một số mục không thêm được:\n" + "\n".join(errs[:8]) + (f"\n… (+{len(errs) - 8})" if len(errs) > 8 else ""),
            )

    def media_delete() -> None:
        if not project:
            return
        if _active_media_kind() == "stock":
            messagebox.showinfo(
                "Video Editor",
                "Đây là kho file stock trên đĩa — không xóa qua đây.\n"
                "Để gỡ nhạc khỏi dự án, dùng tab «File nhạc (dự án)» (media đã import).",
            )
            return
        sel = _selected_media_ids()
        if not sel:
            return
        mids = {str(x) for x in sel if str(x).strip()}
        if not mids:
            return
        if not messagebox.askyesno(
            "Video Editor",
            f"Xóa {len(mids)} media khỏi project?\n(Các clip liên quan trên timeline cũng sẽ bị gỡ)",
            parent=root,
        ):
            return
        remove_files = messagebox.askyesnocancel(
            "Video Editor",
            "Bạn muốn xóa luôn file media local tương ứng không?\n\n"
            "Có = xóa khỏi project + xóa file local trong thư viện Video Editor\n"
            "Không = chỉ gỡ khỏi project\n"
            "Hủy = không thực hiện",
            parent=root,
        )
        if remove_files is None:
            return
        media_map = {
            str(m.get("id") or ""): m for m in (project.get("media") or []) if isinstance(m, dict) and str(m.get("id") or "")
        }
        local_paths = _collect_media_local_paths({"media": [media_map[mid] for mid in mids if mid in media_map]})
        project["media"] = [m for m in (project.get("media") or []) if str(m.get("id")) not in mids]
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict):
                continue
            tr["clips"] = [c for c in (tr.get("clips") or []) if str(c.get("media_id")) not in mids]
        pm.save_project(project)
        deleted_files = 0
        if remove_files:
            pid = str(project.get("id") or "").strip()
            used_elsewhere = _collect_other_project_local_paths(pid)
            deleted_files = _delete_local_media_files_if_safe(local_paths, skip_paths=used_elsewhere)
        refresh_media_tree()
        refresh_timeline()
        refresh_inspector()
        notify(
            f"Đã xóa {len(mids)} media khỏi project (đã gỡ clip liên quan)"
            + (f"; đã dọn {deleted_files} file local." if remove_files else ".")
        )

    def media_open_file() -> None:
        if _active_media_kind() == "stock":
            tr = media_trees["stock"]
            sel = tr.selection()
            if not sel or not stock_paths_mem:
                messagebox.showinfo("Mở file", "Chọn một file trong tab Âm thanh có sẵn.")
                return
            iid = str(sel[0])
            if not iid.startswith("s"):
                return
            try:
                idx = int(iid[1:])
            except ValueError:
                return
            if idx < 0 or idx >= len(stock_paths_mem):
                return
            open_path_with_default_player(stock_paths_mem[idx])
            return
        mid = _selected_media_id()
        if not mid:
            return
        media = _find_media(mid)
        if not media:
            return
        p = mm.resolve_media_path_on_disk(media)
        if not p:
            messagebox.showerror("Mở file", "Không tìm thấy file.")
            return
        try:
            if os.name == "nt":
                os.startfile(str(p))  # type: ignore[attr-defined]
            else:
                subprocess.Popen(["xdg-open", str(p)])
        except Exception as e:
            messagebox.showerror("Mở file", str(e))

    ma_fr = ttk.Frame(media_fr)
    ma_fr.pack(fill=tk.X, pady=(4, 0))
    ttk.Button(ma_fr, text="Thêm lên timeline", command=media_action_add_timeline).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(ma_fr, text="Xóa media khỏi project...", command=media_delete).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(ma_fr, text="Mở file gốc", command=media_open_file).pack(side=tk.LEFT, padx=(0, 4))

    def media_extract_audio() -> None:
        if _active_media_kind() == "stock":
            messagebox.showinfo("Tách âm", "Chọn video trong tab Video (media dự án). Tab stock chỉ nghe/chép file vào dự án.")
            return
        mid = _selected_media_id()
        if not mid:
            messagebox.showinfo("Tách âm", "Chọn một video trong bảng Media.")
            return
        media = _find_media(mid)
        if not media or str(media.get("type")) != "video":
            messagebox.showinfo("Tách âm", "Chọn media loại video.")
            return
        p = mm.resolve_media_path_on_disk(media)
        if not p:
            messagebox.showerror("Tách âm", "Không tìm thấy file.")
            return
        save_p = filedialog.asksavefilename(
            parent=root,
            defaultextension=".mp3",
            filetypes=[("MP3", "*.mp3"), ("AAC", "*.aac"), ("WAV", "*.wav")],
        )
        if not save_p:
            return
        ff = resolve_ffmpeg_executable()
        if not ff:
            messagebox.showerror("Tách âm", "Không có ffmpeg.")
            return
        ext = Path(save_p).suffix.lower()
        fmt = "mp3" if ext == ".mp3" else "aac" if ext in (".aac", ".m4a") else "wav" if ext == ".wav" else "mp3"
        try:
            AudioExtractor().extract_audio(str(p), save_p, ffmpeg_bin=ff, fmt=fmt)
            notify(f"Đã tách audio: {save_p}")
        except Exception as e:
            messagebox.showerror("Tách âm", str(e))

    ttk.Button(ma_fr, text="Tách âm ra file", command=media_extract_audio).pack(side=tk.LEFT)
    ttk.Label(
        media_fr,
        text="Lưu ý: «Xóa media khỏi project» sẽ gỡ cả clip liên quan khỏi timeline; có hỏi giữ/xóa file local để tránh dư thừa.",
        foreground="#666",
        font=("Segoe UI", 8),
        wraplength=520,
        justify="left",
    ).pack(fill=tk.X, anchor="w", padx=2, pady=(4, 0))

    var_split_at = tk.StringVar(value="")
    var_move_to = tk.StringVar(value="")
    var_new_text = tk.StringVar(value="Nhập nội dung chữ...")
    var_batch_logo_motion = tk.StringVar(value="Bật mượt")
    var_batch_logo_iv = tk.StringVar(value="2.0")
    var_batch_logo_seed = tk.StringVar(value="0")
    var_batch_audio_vol = tk.StringVar(value="1.0")
    var_batch_text_content = tk.StringVar(value="Nội dung chữ")
    var_batch_text_size = tk.StringVar(value="44")
    var_batch_text_color = tk.StringVar(value="white")
    var_batch_text_font = tk.StringVar(value="")
    var_batch_text_follow_logo = tk.StringVar(value="Theo logo")

    def _trim_video_rows_source(
        rows: list[tuple[str, dict[str, Any]]],
        trim_head: float,
        trim_tail: float,
        *,
        sync_audio: bool = True,
    ) -> tuple[int, int]:
        """Cắt đầu/cuối nguồn; duration timeline = (nguồn)/speed; đồng bộ audio chồng khung."""
        updated = 0
        skipped = 0
        for cid, cl in rows:
            ss = float(cl.get("source_start") or 0.0)
            se_raw = float(cl.get("source_end") or 0.0)
            try:
                sp = float(cl.get("speed") or 1.0)
            except (TypeError, ValueError):
                sp = 1.0
            if sp <= 0:
                sp = 1.0
            du = max(0.0, float(cl.get("duration") or 0.0))
            se = se_raw if se_raw > ss + 1e-6 else (ss + du * sp)
            ns = ss + trim_head
            ne = se - trim_tail
            if ne <= ns + 0.05:
                skipped += 1
                continue
            tm.trim_clip(
                project,
                str(cid),
                ns,
                ne,
                persist=False,
                recompute_duration=False,
            )
            if sync_audio:
                fc_sync = _find_clip(str(cid))
                sp_sync = (
                    float(fc_sync[1].get("speed") or 1.0)
                    if fc_sync and fc_sync[1]
                    else sp
                )
                if sp_sync <= 0:
                    sp_sync = 1.0
                _sync_overlapping_timeline_audio_to_video(str(cid), sp_sync)
            updated += 1
        return updated, skipped

    def _notify_trim_result(updated: int, total: int, trim_head: float, trim_tail: float, skipped: int) -> None:
        msg = (
            f"Cắt nguồn: đã cập nhật {updated}/{total} clip "
            f"(đầu {trim_head:.2f}s, đuôi {trim_tail:.2f}s; độ dài timeline khớp tốc độ)"
        )
        if skipped:
            msg += f"; bỏ qua {skipped} clip quá ngắn"
        notify(msg + ".")

    def clip_split() -> None:
        if not project:
            return
        sel = tree_tl.selection()
        if len(sel) != 1:
            notify("Split: chọn đúng một clip.")
            return
        fc = _find_clip(sel[0])
        cl0 = fc[1] if fc and fc[1] else None
        init_split = 1.0
        if cl0:
            init_split = float(cl0.get("timeline_start") or 0) + max(0.5, float(cl0.get("duration") or 0) / 2)
        try:
            st = float(str(var_split_at.get()).strip() or str(init_split))
        except ValueError:
            messagebox.showerror("Split", "Thời điểm split không hợp lệ.")
            return
        try:
            tm.split_clip(project, sel[0], st)
            refresh_timeline()
            notify(f"Đã tách clip tại t={st:.2f}s")
        except Exception as e:
            messagebox.showerror("Split", str(e))

    def clip_move() -> None:
        if not project:
            return
        sel = tree_tl.selection()
        if len(sel) != 1:
            notify("Move: chọn đúng một clip.")
            return
        try:
            nt = float(str(var_move_to.get()).strip())
        except ValueError:
            messagebox.showerror("Move", "Giá trị timeline_start mới không hợp lệ.")
            return
        try:
            tm.move_clip(project, sel[0], nt)
            refresh_timeline()
            refresh_inspector()
            notify(f"Đã di chuyển clip → timeline_start={nt:.2f}")
        except Exception as e:
            messagebox.showerror("Move", str(e))

    def _selected_timeline_clip_ids() -> list[str]:
        selected_ids = _timeline_merged_clip_ids_from_trees()
        out: list[str] = []
        seen: set[str] = set()
        for cid in selected_ids:
            c = str(cid or "").strip()
            if not c or c in seen:
                continue
            seen.add(c)
            out.append(c)
        return out

    def _all_timeline_clip_ids() -> list[str]:
        if not project:
            return []
        out: list[str] = []
        seen: set[str] = set()
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict):
                continue
            for cl in tr.get("clips") or []:
                if not isinstance(cl, dict):
                    continue
                cid = str(cl.get("id") or "").strip()
                if not cid or cid in seen:
                    continue
                seen.add(cid)
                out.append(cid)
        return out

    def clip_delete(*, delete_all: bool = False) -> None:
        if not project:
            return
        target_ids = _all_timeline_clip_ids() if delete_all else _selected_timeline_clip_ids()
        if not target_ids:
            if delete_all:
                notify("Timeline chưa có clip để xóa.")
            else:
                notify("Chọn ít nhất một clip để xóa.")
            return
        ask_text = (
            f"Xóa TẤT CẢ {len(target_ids)} clip khỏi timeline?\n(Không xóa file trong Media)"
            if delete_all
            else f"Xóa {len(target_ids)} clip khỏi timeline?\n(Không xóa file trong Media)"
        )
        if not messagebox.askyesno(
            "Video Editor",
            ask_text,
            parent=root,
        ):
            return
        try:
            eps_link = 0.08
            base_ids: list[str] = []
            seen_del: set[str] = set()
            for cid in target_ids:
                s = str(cid or "").strip()
                if s and s not in seen_del:
                    seen_del.add(s)
                    base_ids.append(s)

            def _linked_assist_ids_for_video_span(vs: float, vd: float) -> list[str]:
                vd0 = max(0.0, float(vd))
                found: list[str] = []
                for tr in project.get("tracks") or []:
                    if not isinstance(tr, dict):
                        continue
                    ttype = str(tr.get("type") or "")
                    if ttype not in ("overlay", "audio", "text"):
                        continue
                    for cl in tr.get("clips") or []:
                        if not isinstance(cl, dict):
                            continue
                        if ttype == "overlay" and str(cl.get("type") or "") != "image":
                            continue
                        if ttype == "audio" and str(cl.get("type") or "") != "audio":
                            continue
                        if ttype == "text" and not _clip_is_text_track_payload(cl):
                            continue
                        cs = float(cl.get("timeline_start") or 0.0)
                        cd = max(0.0, float(cl.get("duration") or 0.0))
                        if abs(cs - vs) <= eps_link and abs(cd - vd0) <= eps_link:
                            lid = str(cl.get("id") or "").strip()
                            if lid:
                                found.append(lid)
                return found

            extra: list[str] = []
            for cid in base_ids:
                fc = _find_clip(cid)
                if fc and fc[1] and str(fc[1].get("type") or "") == "video":
                    vs = float(fc[1].get("timeline_start") or 0.0)
                    vd = max(0.0, float(fc[1].get("duration") or 0.0))
                    for lid in _linked_assist_ids_for_video_span(vs, vd):
                        if lid not in seen_del:
                            seen_del.add(lid)
                            extra.append(lid)

            to_delete = base_ids + extra
            for cid in to_delete:
                tm.delete_clip(project, cid, persist=False, recompute_duration=False)
            tm.refresh_project_duration(project)
            pm.save_project(project)
            refresh_timeline()
            refresh_inspector()
            if delete_all:
                notify(f"Đã xóa toàn bộ {len(target_ids)} clip khỏi timeline.")
            else:
                notify(f"Đã xóa {len(target_ids)} clip khỏi timeline.")
        except Exception as e:
            messagebox.showerror("Xóa", str(e))

    def _find_track_clips_local(track_type: str) -> list[dict[str, Any]]:
        if not project:
            return []
        for tr in project.get("tracks") or []:
            if isinstance(tr, dict) and str(tr.get("type") or "") == track_type:
                return tr.setdefault("clips", [])
        return []

    def _selected_video_timeline_rows() -> list[tuple[str, dict[str, Any]]]:
        out: list[tuple[str, dict[str, Any]]] = []
        selected_ids: list[str] = list(_timeline_merged_clip_ids_from_trees())
        for cid in _clip_ids_from_edit_sum_selection():
            selected_ids.append(str(cid))
        seen: set[str] = set()
        for cid in selected_ids:
            if not cid or cid in seen:
                continue
            seen.add(cid)
            tr, cl = _find_clip(str(cid))
            if not tr or not cl:
                continue
            if str(tr.get("type") or "") == "video" and str(cl.get("type") or "") == "video":
                out.append((str(cid), cl))
        return out

    def _ve_iv_overlap(a0: float, a1: float, b0: float, b1: float) -> bool:
        return min(a1, b1) > max(a0, b0)

    def _sync_overlapping_timeline_audio_to_video(
        video_cid: str,
        video_speed: float | None = None,
        *,
        align_timeline_start: bool = False,
    ) -> int:
        """
        Đồng bộ clip audio chồng khung với clip video (tốc độ, độ dài timeline, cắt nguồn, loop).
        """
        if not project:
            return 0
        sp: float | None = None
        if video_speed is not None:
            try:
                sp = float(video_speed)
            except (TypeError, ValueError):
                sp = 1.0
            if sp is not None and sp <= 0:
                sp = 1.0
        return tm.sync_overlapping_audio_to_video(
            project,
            str(video_cid),
            align_timeline_start=align_timeline_start,
            speed=sp,
        )

    def _logo_corner_xy_from_label(
        pos_pick: str,
        pw: int,
        ph: int,
        logo_w: int,
        logo_h: int | None = None,
        margin: int = 24,
    ) -> tuple[int, int]:
        lw = max(1, int(logo_w))
        lh = max(1, int(logo_h if logo_h is not None else logo_w))
        if pos_pick == "Giữa trên":
            return max(margin, pw // 2 - lw // 2), margin
        if pos_pick == "Trái trên":
            return margin, margin
        if pos_pick == "Phải trên":
            return max(margin, pw - lw - margin), margin
        if pos_pick == "Trái dưới":
            return margin, max(margin, ph - lh - margin)
        if pos_pick == "Phải dưới":
            return max(margin, pw - lw - margin), max(margin, ph - lh - margin)
        if pos_pick == "Giữa dưới":
            return max(margin, pw // 2 - lw // 2), max(margin, ph - lh - margin)
        return 24, 24

    def _first_image_media_id_in_project() -> str:
        if not project:
            return ""
        for m in reversed([x for x in (project.get("media") or []) if isinstance(x, dict)]):
            if str(m.get("type") or "") == "image":
                mid = str(m.get("id") or "").strip()
                if mid:
                    return mid
        return ""

    def _resolve_logo_media_id_from_ui(raw: str, *, allow_auto: bool) -> str:
        """Chọn logo từ combobox hoặc ảnh import gần nhất (Tự động)."""
        pick = str(raw or "").strip()
        auto_lbl = "Tự động (ảnh trong Media — ưu tiên file import gần nhất)"
        if not pick or pick == auto_lbl:
            return _first_image_media_id_in_project() if allow_auto else ""
        mid = _ve_media_id_from_combo(pick, _q_logo_media_combo_refresh.get("label_to_id") or {})
        if mid and _media_id_valid_for_type(mid, "image"):
            return mid
        return _first_image_media_id_in_project() if allow_auto else ""

    def _infer_logo_position_label(x: Any, y: Any, w: Any, h: Any, pw: int, ph: int) -> str:
        try:
            xi, yi, wi, hi = int(x), int(y), int(w), int(h)
        except (TypeError, ValueError):
            return ""
        if wi <= 0 or hi <= 0:
            return ""
        side = max(1, min(wi, hi))
        labels = ("Giữa dưới", "Giữa trên", "Trái trên", "Phải trên", "Trái dưới", "Phải dưới")
        best = ""
        best_d = 10**12
        for lb in labels:
            lx, ly = _logo_corner_xy_from_label(lb, pw, ph, side)
            d = (lx - xi) ** 2 + (ly - yi) ** 2
            if d < best_d:
                best_d, best = d, lb
        if best_d > (max(pw, ph) * 0.12) ** 2:
            return ""
        return best

    def _span_has_logo_overlay(ts: float, te: float) -> bool:
        for _oc in _find_track_clips_local("overlay"):
            if not isinstance(_oc, dict) or str(_oc.get("type") or "") != "image":
                continue
            _os = float(_oc.get("timeline_start") or 0.0)
            _oe = _os + max(0.0, float(_oc.get("duration") or 0.0))
            if _ve_iv_overlap(ts, te, _os, _oe):
                return True
        return False

    def _span_has_audio_overlay(ts: float, te: float) -> bool:
        for _ac in _find_track_clips_local("audio"):
            if not isinstance(_ac, dict) or str(_ac.get("type") or "") != "audio":
                continue
            _as = float(_ac.get("timeline_start") or 0.0)
            _ae = _as + max(0.0, float(_ac.get("duration") or 0.0))
            if _ve_iv_overlap(ts, te, _as, _ae):
                return True
        return False

    def _span_has_text_overlay(ts: float, te: float) -> bool:
        for _tc in _find_track_clips_local("text"):
            if not isinstance(_tc, dict) or not _clip_is_text_track_payload(_tc):
                continue
            _tts = float(_tc.get("timeline_start") or 0.0)
            _tte = _tts + max(0.0, float(_tc.get("duration") or 0.0))
            if _ve_iv_overlap(ts, te, _tts, _tte):
                return True
        return False

    def _conflicts_for_video_rows(
        rows: list[tuple[str, dict[str, Any]]],
        *,
        check_logo: bool = True,
        check_audio: bool = True,
        check_text: bool = True,
    ) -> tuple[bool, bool, bool]:
        """
        True = trên timeline đã có overlay (logo ảnh / âm phụ / chữ) chồng khung thời gian clip video.

        ``check_*``: chỉ quét loại tương ứng (ví dụ hàng loạt chỉ kiểm logo khi thực sự định áp logo).
        """
        any_logo = any_audio = any_text = False
        for _cid, c in rows:
            ts = float(c.get("timeline_start") or 0)
            du = max(0.1, float(c.get("duration") or 0))
            te = ts + du
            if check_logo and _span_has_logo_overlay(ts, te):
                any_logo = True
            if check_audio and _span_has_audio_overlay(ts, te):
                any_audio = True
            if check_text and _span_has_text_overlay(ts, te):
                any_text = True
        return any_logo, any_audio, any_text

    def _prompt_apply_conflict_modes(
        *,
        ask_logo: bool,
        ask_audio: bool,
        ask_text: bool,
        batch_count: int = 1,
    ) -> tuple[str, str, str] | None:
        """
        Chọn cách xử lý khi timeline đã có logo/âm/chữ chồng lên clip.
        Trả về (logo_mode, audio_mode, text_mode) với mỗi giá trị replace | skip | add; None = hủy áp overlay.
        """
        if not (ask_logo or ask_audio or ask_text):
            return ("replace", "replace", "replace")
        labels = (
            "replace — Cập nhật (ghi đè clip đầu, gộp trùng)",
            "skip — Giữ nguyên trên timeline (bỏ qua phần này)",
            "add — Thêm clip overlay / âm / chữ mới (chồng thêm)",
        )
        mode_by_label = {
            labels[0]: "replace",
            labels[1]: "skip",
            labels[2]: "add",
        }

        def _read_combo(cb: ttk.Combobox | None) -> str:
            if cb is None:
                return "replace"
            raw = str(cb.get() or "").strip()
            if raw in mode_by_label:
                return mode_by_label[raw]
            # Windows/ttk: sau khi chọn từ list, .get() đôi khi rỗng tới khi đổi focus — dùng chỉ số dòng.
            try:
                ci = cb.current()
            except tk.TclError:
                ci = -1
            if isinstance(ci, int) and 0 <= ci <= 2:
                return ("replace", "skip", "add")[ci]
            rl = raw.lower()
            if rl.startswith("skip"):
                return "skip"
            if rl.startswith("add"):
                return "add"
            return "replace"

        dlg = tk.Toplevel(root)
        dlg.title("Trùng logo / âm thanh / chữ")
        dlg.transient(root)
        dlg.grab_set()
        bx = max(420, int(root.winfo_rootx()) + 40)
        by = max(40, int(root.winfo_rooty()) + 60)
        dlg.geometry(f"560x380+{bx}+{by}")
        intro = (
            "Timeline đã có ít nhất một trong: logo ảnh, track âm phụ, hoặc chữ — trùng khung thời gian với clip đang áp."
            + (f" ({batch_count} clip)" if batch_count > 1 else "")
            + " Chọn cách xử lý:"
        )
        outer = ttk.Frame(dlg, padding=12)
        outer.pack(fill=tk.BOTH, expand=True)
        ttk.Label(outer, text=intro, wraplength=520, justify=tk.LEFT).pack(anchor="w", pady=(0, 10))
        form = ttk.Frame(outer)
        form.pack(fill=tk.X)
        cb_logo: ttk.Combobox | None = None
        cb_audio: ttk.Combobox | None = None
        cb_text: ttk.Combobox | None = None
        ri = 0

        def _add_row(title: str, show: bool) -> ttk.Combobox | None:
            nonlocal ri
            if not show:
                return None
            ttk.Label(form, text=title).grid(row=ri, column=0, sticky="nw", pady=6)
            cb = ttk.Combobox(form, values=labels, state="readonly", width=52)
            cb.set(labels[0])
            cb.grid(row=ri, column=1, sticky="ew", padx=(8, 0), pady=6)
            ri += 1
            return cb

        cb_logo = _add_row("Logo ảnh:", ask_logo)
        cb_audio = _add_row("Âm thanh phụ:", ask_audio)
        cb_text = _add_row("Chữ:", ask_text)
        form.columnconfigure(1, weight=1)
        out: dict[str, tuple[str, str, str] | None] = {"v": None}

        def _ok() -> None:
            out["v"] = (_read_combo(cb_logo), _read_combo(cb_audio), _read_combo(cb_text))
            dlg.destroy()

        def _cancel() -> None:
            out["v"] = None
            dlg.destroy()

        bf = ttk.Frame(outer)
        bf.pack(fill=tk.X, pady=(14, 0))
        ttk.Button(bf, text="Tiếp tục với các chế độ này", command=_ok).pack(side=tk.RIGHT)
        ttk.Button(bf, text="Hủy", command=_cancel).pack(side=tk.RIGHT, padx=(0, 8))
        dlg.protocol("WM_DELETE_WINDOW", _cancel)
        dlg.wait_window()
        return out["v"]

    def apply_logo_audio_text_to_video_rows(
        rows: list[tuple[str, dict[str, Any]]],
        *,
        logo_mid: str = "",
        logo_opacity: float = 0.92,
        logo_ratio: float = 0.15,
        logo_corner: str = "",
        audio_mid: str = "",
        audio_volume: float = 1.0,
        audio_speed: float | None = None,
        text_content: str = "",
        logo_on_conflict: str = "replace",
        audio_on_conflict: str = "replace",
        text_on_conflict: str = "replace",
    ) -> tuple[int, int, int]:
        """
        Gán logo (overlay ảnh), track âm thanh và clip chữ lên các clip video — cùng logic «sửa hàng loạt».
        logo_on_conflict / audio_on_conflict / text_on_conflict: replace | skip | add.
        ``audio_speed``: tốc độ phát track phụ (atempo); ``None`` = lấy theo ``speed`` của từng clip video tương ứng.
        Trả về (số thao tác logo, audio, chữ) đã thực hiện (tạo mới hoặc cập nhật).
        """
        if not project or not rows:
            return (0, 0, 0)
        requested_lm = str(logo_mid or "").strip()
        lm = requested_lm
        if lm and not _media_id_valid_for_type(lm, "image"):
            lm = ""
        am = str(audio_mid or "").strip()
        if am and not _media_id_valid_for_type(am, "audio"):
            am = ""
        tx = str(text_content or "").strip()
        add_logo = bool(lm)
        add_audio = bool(am)
        add_text = bool(tx)
        if requested_lm and not add_logo:
            notify(
                "Logo: không tìm thấy file ảnh hợp lệ — «Thêm logo / ảnh» rồi chọn đúng tên trong combobox."
            )
            if not (add_audio or add_text):
                return (0, 0, 0)
        if not (add_logo or add_audio or add_text):
            return (0, 0, 0)
        loc = str(logo_on_conflict or "replace").strip().lower()
        aoc = str(audio_on_conflict or "replace").strip().lower()
        toc = str(text_on_conflict or "replace").strip().lower()
        if loc not in ("replace", "skip", "add"):
            loc = "replace"
        if aoc not in ("replace", "skip", "add"):
            aoc = "replace"
        if toc not in ("replace", "skip", "add"):
            toc = "replace"
        canvas_w = int(project.get("width") or 1080)
        canvas_h = int(project.get("height") or 1920)
        logo_media_rec = _find_media(lm) if lm else None
        logo_w, logo_h = compute_logo_overlay_dimensions(
            logo_media_rec,
            canvas_w=canvas_w,
            logo_ratio=float(logo_ratio),
        )
        logo_opa = max(0.0, min(1.0, float(logo_opacity)))
        corner_s = str(logo_corner or "").strip()
        if corner_s:
            logo_x0, logo_y0 = _logo_corner_xy_from_label(corner_s, canvas_w, canvas_h, logo_w, logo_h)
        else:
            logo_x0, logo_y0 = 24, 24
        audio_media_duration = 0.0
        if add_audio:
            for m in project.get("media") or []:
                if isinstance(m, dict) and str(m.get("id") or "") == am:
                    audio_media_duration = max(0.0, float(m.get("duration") or 0.0))
                    break
        try:
            a_vol = max(0.0, float(audio_volume))
        except (TypeError, ValueError):
            a_vol = 1.0
        n_batch = len(rows)
        defer_tm = n_batch > 1
        logo_ops = audio_ops = text_ops = 0
        _new_overlay: list[dict[str, Any]] = []
        _new_audio: list[dict[str, Any]] = []
        _new_text: list[dict[str, Any]] = []

        def _patch_timeline_audio_clip(ac: dict[str, Any], *, ts: float, du: float, a_sp: float) -> None:
            ss, se, loop = audio_source_bounds_for_timeline(
                timeline_duration=du,
                speed=a_sp,
                media_duration=audio_media_duration,
                source_start=0.0,
            )
            ac["timeline_start"] = ts
            ac["source_start"] = ss
            ac["source_end"] = se
            ac["duration"] = du
            ac["loop"] = loop
            ac["volume"] = a_vol
            ac["speed"] = a_sp

        for cid, c in rows:
            ts = float(c.get("timeline_start") or 0)
            du = max(0.1, float(c.get("duration") or 0))
            # Trùng độ dài clip video — giới hạn 8s cũ khiến chữ biến mất sớm trên video dài, dễ hiểu như «chưa áp».
            tdur = du
            try:
                a_sp_row = float(audio_speed) if audio_speed is not None else float(c.get("speed") or 1.0)
            except (TypeError, ValueError):
                a_sp_row = 1.0
            if a_sp_row <= 0:
                a_sp_row = 1.0
            if add_logo:
                logo_hits: list[dict[str, Any]] = []
                for _oc in _find_track_clips_local("overlay"):
                    if not isinstance(_oc, dict) or str(_oc.get("type") or "") != "image":
                        continue
                    _os = float(_oc.get("timeline_start") or 0.0)
                    _oe = _os + max(0.0, float(_oc.get("duration") or 0.0))
                    if _ve_iv_overlap(ts, ts + du, _os, _oe):
                        logo_hits.append(_oc)
                if logo_hits and loc == "skip":
                    pass
                elif logo_hits and loc == "replace":
                    oc0 = logo_hits[0]
                    oc0["media_id"] = lm
                    oc0["timeline_start"] = ts
                    oc0["duration"] = du
                    oc0["x"] = logo_x0
                    oc0["y"] = logo_y0
                    oc0["width"] = logo_w
                    oc0["height"] = logo_h
                    oc0["opacity"] = logo_opa
                    for _extra in logo_hits[1:]:
                        _eid = str((_extra or {}).get("id") or "").strip()
                        if _eid:
                            tm.delete_clip(project, _eid, persist=False, recompute_duration=False)
                    logo_ops += 1
                elif logo_hits and loc == "add":
                    _new_overlay.clear()
                    tm.add_clip(
                        project,
                        lm,
                        "overlay",
                        persist=False,
                        recompute_duration=not defer_tm,
                        out_new_clip=_new_overlay,
                    )
                    if _new_overlay:
                        oc = _new_overlay[0]
                        oc["timeline_start"] = ts
                        oc["duration"] = du
                        oc["x"] = logo_x0
                        oc["y"] = logo_y0
                        oc["width"] = logo_w
                        oc["height"] = logo_h
                        oc["opacity"] = logo_opa
                        logo_ops += 1
                elif not logo_hits:
                    _new_overlay.clear()
                    tm.add_clip(
                        project,
                        lm,
                        "overlay",
                        persist=False,
                        recompute_duration=not defer_tm,
                        out_new_clip=_new_overlay,
                    )
                    if _new_overlay:
                        oc = _new_overlay[0]
                        oc["timeline_start"] = ts
                        oc["duration"] = du
                        oc["x"] = logo_x0
                        oc["y"] = logo_y0
                        oc["width"] = logo_w
                        oc["height"] = logo_h
                        oc["opacity"] = logo_opa
                        logo_ops += 1
            if add_audio:
                audio_hits: list[dict[str, Any]] = []
                for _ac in _find_track_clips_local("audio"):
                    if not isinstance(_ac, dict) or str(_ac.get("type") or "") != "audio":
                        continue
                    _as = float(_ac.get("timeline_start") or 0.0)
                    _ae = _as + max(0.0, float(_ac.get("duration") or 0.0))
                    if _ve_iv_overlap(ts, ts + du, _as, _ae):
                        audio_hits.append(_ac)
                if audio_hits and aoc == "skip":
                    pass
                elif audio_hits and aoc == "replace":
                    ac0 = audio_hits[0]
                    ac0["media_id"] = am
                    _patch_timeline_audio_clip(ac0, ts=ts, du=du, a_sp=a_sp_row)
                    for _extra in audio_hits[1:]:
                        _eid = str((_extra or {}).get("id") or "").strip()
                        if _eid:
                            tm.delete_clip(project, _eid, persist=False, recompute_duration=False)
                    audio_ops += 1
                elif audio_hits and aoc == "add":
                    _new_audio.clear()
                    tm.add_clip(
                        project,
                        am,
                        "audio",
                        persist=False,
                        recompute_duration=not defer_tm,
                        out_new_clip=_new_audio,
                    )
                    if _new_audio:
                        ac = _new_audio[0]
                        _patch_timeline_audio_clip(ac, ts=ts, du=du, a_sp=a_sp_row)
                        audio_ops += 1
                elif not audio_hits:
                    _new_audio.clear()
                    tm.add_clip(
                        project,
                        am,
                        "audio",
                        persist=False,
                        recompute_duration=not defer_tm,
                        out_new_clip=_new_audio,
                    )
                    if _new_audio:
                        ac = _new_audio[0]
                        _patch_timeline_audio_clip(ac, ts=ts, du=du, a_sp=a_sp_row)
                        audio_ops += 1
            if add_text:
                text_hits: list[dict[str, Any]] = []
                for _tc in _find_track_clips_local("text"):
                    if not isinstance(_tc, dict) or not _clip_is_text_track_payload(_tc):
                        continue
                    _ts = float(_tc.get("timeline_start") or 0.0)
                    _te = _ts + max(0.0, float(_tc.get("duration") or 0.0))
                    if _ve_iv_overlap(ts, ts + du, _ts, _te):
                        text_hits.append(_tc)
                if text_hits and toc == "skip":
                    pass
                elif text_hits and toc == "replace":
                    tc0 = text_hits[0]
                    tc0["text"] = tx
                    tc0["timeline_start"] = ts
                    tc0["duration"] = tdur
                    for _extra in text_hits[1:]:
                        _eid = str((_extra or {}).get("id") or "").strip()
                        if _eid:
                            tm.delete_clip(project, _eid, persist=False, recompute_duration=False)
                    text_ops += 1
                elif text_hits and toc == "add":
                    _new_text.clear()
                    tm.add_text_clip(
                        project,
                        tx,
                        timeline_start=ts,
                        duration=tdur,
                        persist=False,
                        recompute_duration=not defer_tm,
                        out_new_clip=_new_text,
                    )
                    if _new_text:
                        _new_text[0]["timeline_start"] = ts
                        _new_text[0]["duration"] = tdur
                        text_ops += 1
                elif not text_hits:
                    _new_text.clear()
                    tm.add_text_clip(
                        project,
                        tx,
                        timeline_start=ts,
                        duration=tdur,
                        persist=False,
                        recompute_duration=not defer_tm,
                        out_new_clip=_new_text,
                    )
                    if _new_text:
                        _new_text[0]["timeline_start"] = ts
                        _new_text[0]["duration"] = tdur
                        text_ops += 1
        if add_audio:
            for _cid, _c in rows:
                try:
                    _sp_row = float(audio_speed) if audio_speed is not None else float(_c.get("speed") or 1.0)
                except (TypeError, ValueError):
                    _sp_row = 1.0
                if _sp_row <= 0:
                    _sp_row = 1.0
                _sync_overlapping_timeline_audio_to_video(
                    _cid, _sp_row, align_timeline_start=True
                )
        if defer_tm:
            tm.refresh_project_duration(project)
        pm.save_project(project)
        return (logo_ops, audio_ops, text_ops)

    def _pick_media_id_by_type(media_type: str, title: str) -> str:
        if not project:
            return ""
        rows = [
            m for m in (project.get("media") or []) if isinstance(m, dict) and str(m.get("type") or "") == media_type
        ]
        if not rows:
            messagebox.showinfo(title, f"Không có media loại {media_type} trong project.")
            return ""
        _dlg_labels, _dlg_l2i, _dlg_i2l = _ve_build_media_combo_maps(
            rows, fallback=media_type, include_empty=False
        )
        if not _dlg_labels:
            messagebox.showinfo(title, f"Không có media loại {media_type} trong project.")
            return ""
        choices = _dlg_labels
        picked = tk.StringVar(value=choices[0])
        dlg = tk.Toplevel(root)
        dlg.title(title)
        dlg.transient(root)
        dlg.grab_set()
        dlg.geometry("640x180")
        dlg.minsize(420, 160)
        frm = ttk.Frame(dlg, padding=10)
        frm.pack(fill=tk.BOTH, expand=True)
        ttk.Label(frm, text=f"Chọn media {media_type}:").pack(anchor="w")
        cb = ttk.Combobox(frm, values=choices, textvariable=picked, state="readonly", width=56)
        cb.pack(fill=tk.X, pady=(6, 8))
        btns = ttk.Frame(frm)
        btns.pack(fill=tk.X)
        ok_flag = {"v": False}

        def _ok() -> None:
            ok_flag["v"] = True
            dlg.destroy()

        ttk.Button(btns, text="OK", command=_ok).pack(side=tk.RIGHT)
        ttk.Button(btns, text="Hủy", command=dlg.destroy).pack(side=tk.RIGHT, padx=(0, 6))
        cb.focus_set()
        dlg.wait_window()
        if not ok_flag["v"]:
            return ""
        return _ve_media_id_from_combo(str(picked.get() or ""), _dlg_l2i)

    def add_logo_to_selected_video_clips() -> None:
        if not project:
            return
        rows = _selected_video_timeline_rows()
        if not rows:
            messagebox.showinfo("Thêm logo", "Chọn ít nhất 1 clip video trong Timeline.")
            return
        logo_mid = _pick_media_id_by_type("image", "Thêm logo")
        if not logo_mid:
            return
        enable_logo_motion = str(var_batch_logo_motion.get() or "").strip().lower().startswith("bật")
        try:
            logo_motion_interval = max(0.25, min(120.0, float(str(var_batch_logo_iv.get()).strip() or "2.0")))
            logo_motion_seed = int(str(var_batch_logo_seed.get()).strip() or "0")
        except ValueError:
            messagebox.showerror("Logo", "Thông số chuyển động logo không hợp lệ.")
            return
        overlay_added = 0
        canvas_w = int(project.get("width") or 1080)
        logo_media_rec = _find_media(logo_mid)
        logo_w, logo_h = compute_logo_overlay_dimensions(
            logo_media_rec, canvas_w=canvas_w, logo_ratio=0.15
        )
        n_rows = len(rows)
        defer = n_rows > 1
        prog_step = _ve_batch_progress_step(n_rows)
        _new_oc: list[dict[str, Any]] = []
        for j, (_cid, c) in enumerate(rows, start=1):
            ts = float(c.get("timeline_start") or 0.0)
            du = max(0.1, float(c.get("duration") or 0.0))
            _new_oc.clear()
            tm.add_clip(
                project,
                logo_mid,
                "overlay",
                persist=False,
                recompute_duration=not defer,
                out_new_clip=_new_oc,
            )
            if _new_oc:
                oc = _new_oc[0]
                oc["timeline_start"] = ts
                oc["duration"] = du
                oc["x"] = 24
                oc["y"] = 24
                oc["width"] = logo_w
                oc["height"] = logo_h
                oc["opacity"] = 0.92
                oc["random_motion_enabled"] = bool(enable_logo_motion)
                oc["random_motion_interval"] = float(logo_motion_interval)
                oc["random_motion_seed"] = int(logo_motion_seed)
                oc["random_motion_smooth"] = bool(enable_logo_motion)
                overlay_added += 1
            if prog_step and (j == n_rows or j % prog_step == 0):
                _ve_batch_status_progress(j, n_rows, "thêm logo")
        if defer:
            tm.refresh_project_duration(project)
        pm.save_project(project)
        refresh_timeline()
        refresh_inspector()
        notify(f"Đã thêm logo cho {overlay_added} clip video.")

    def add_audio_to_selected_video_clips() -> None:
        if not project:
            return
        rows = _selected_video_timeline_rows()
        if not rows:
            messagebox.showinfo("Thêm âm thanh", "Chọn ít nhất 1 clip video trong Timeline.")
            return
        audio_mid = _pick_media_id_by_type("audio", "Thêm âm thanh")
        if not audio_mid:
            return
        try:
            audio_vol = float(str(var_batch_audio_vol.get()).strip() or "1.0")
        except ValueError:
            messagebox.showerror("Âm thanh", "Âm lượng audio không hợp lệ.")
            return
        audio_media_duration = 0.0
        for m in project.get("media") or []:
            if isinstance(m, dict) and str(m.get("id") or "") == audio_mid:
                audio_media_duration = max(0.0, float(m.get("duration") or 0.0))
                break
        audio_added = 0
        n_rows = len(rows)
        defer = n_rows > 1
        prog_step = _ve_batch_progress_step(n_rows)
        _new_ac: list[dict[str, Any]] = []
        for j, (_cid, c) in enumerate(rows, start=1):
            ts = float(c.get("timeline_start") or 0.0)
            du = max(0.1, float(c.get("duration") or 0.0))
            try:
                sp_row = float(c.get("speed") or 1.0)
            except (TypeError, ValueError):
                sp_row = 1.0
            if sp_row <= 0:
                sp_row = 1.0
            audio_hits: list[dict[str, Any]] = []
            for _ac in _find_track_clips_local("audio"):
                if not isinstance(_ac, dict) or str(_ac.get("type") or "") != "audio":
                    continue
                _as = float(_ac.get("timeline_start") or 0.0)
                _ae = _as + max(0.0, float(_ac.get("duration") or 0.0))
                if _ve_iv_overlap(ts, ts + du, _as, _ae):
                    audio_hits.append(_ac)
            if audio_hits:
                ac0 = audio_hits[0]
                ac0["media_id"] = audio_mid
                ss, se, loop = audio_source_bounds_for_timeline(
                    timeline_duration=du,
                    speed=sp_row,
                    media_duration=audio_media_duration,
                    source_start=0.0,
                )
                ac0["timeline_start"] = ts
                ac0["source_start"] = ss
                ac0["source_end"] = se
                ac0["duration"] = du
                ac0["loop"] = loop
                ac0["volume"] = max(0.0, float(audio_vol))
                ac0["speed"] = sp_row
                for _extra in audio_hits[1:]:
                    _eid = str((_extra or {}).get("id") or "").strip()
                    if _eid:
                        tm.delete_clip(project, _eid, persist=False, recompute_duration=False)
                audio_added += 1
            else:
                _new_ac.clear()
                tm.add_clip(
                    project,
                    audio_mid,
                    "audio",
                    persist=False,
                    recompute_duration=not defer,
                    out_new_clip=_new_ac,
                )
                if _new_ac:
                    ac = _new_ac[0]
                    ss, se, loop = audio_source_bounds_for_timeline(
                        timeline_duration=du,
                        speed=sp_row,
                        media_duration=audio_media_duration,
                        source_start=0.0,
                    )
                    ac["timeline_start"] = ts
                    ac["source_start"] = ss
                    ac["source_end"] = se
                    ac["duration"] = du
                    ac["loop"] = loop
                    ac["volume"] = max(0.0, float(audio_vol))
                    ac["speed"] = sp_row
                    audio_added += 1
            _sync_overlapping_timeline_audio_to_video(_cid, sp_row, align_timeline_start=True)
            if prog_step and (j == n_rows or j % prog_step == 0):
                _ve_batch_status_progress(j, n_rows, "thêm âm thanh")
        if defer:
            tm.refresh_project_duration(project)
        pm.save_project(project)
        refresh_timeline()
        refresh_inspector()
        notify(f"Đã thêm âm thanh cho {audio_added} clip video.")

    def add_text_to_selected_video_clips() -> None:
        if not project:
            return
        rows = _selected_video_timeline_rows()
        if not rows:
            messagebox.showinfo("Thêm chữ", "Chọn ít nhất 1 clip video trong Timeline.")
            return
        tx = str(var_batch_text_content.get() or "").strip()
        if not tx:
            messagebox.showinfo("Thêm chữ", "Nhập nội dung chữ ở ô tùy chọn trước.")
            return
        try:
            fs = max(10, min(220, int(str(var_batch_text_size.get()).strip() or "44")))
        except ValueError:
            messagebox.showerror("Thêm chữ", "Cỡ chữ không hợp lệ.")
            return
        color = str(var_batch_text_color.get() or "").strip() or "white"
        font_file = str(var_batch_text_font.get() or "").strip()
        use_logo_motion = str(var_batch_text_follow_logo.get() or "").strip().lower().startswith("theo")

        def _find_overlay_covering(ts: float, te: float) -> dict[str, Any] | None:
            if not project:
                return None
            for tr in project.get("tracks") or []:
                if not isinstance(tr, dict) or str(tr.get("type") or "") != "overlay":
                    continue
                for cl in tr.get("clips") or []:
                    if not isinstance(cl, dict) or str(cl.get("type") or "") != "image":
                        continue
                    os = float(cl.get("timeline_start") or 0.0)
                    oe = os + max(0.0, float(cl.get("duration") or 0.0))
                    if _has_overlap(ts, te, os, oe):
                        return cl
            return None

        text_added = 0
        n_rows = len(rows)
        defer = n_rows > 1
        prog_step = _ve_batch_progress_step(n_rows)
        _new_tc: list[dict[str, Any]] = []
        for j, (_cid, c) in enumerate(rows, start=1):
            ts = float(c.get("timeline_start") or 0.0)
            du = max(0.1, float(c.get("duration") or 0.0))
            tdur = max(1.0, min(du, 8.0))
            _new_tc.clear()
            tm.add_text_clip(
                project,
                tx,
                timeline_start=ts,
                duration=tdur,
                persist=False,
                recompute_duration=not defer,
                out_new_clip=_new_tc,
            )
            if _new_tc:
                tc = _new_tc[0]
                ov = _find_overlay_covering(ts, ts + du)
                tc["timeline_start"] = ts
                tc["duration"] = tdur
                tc["font_size"] = int(fs)
                tc["color"] = str(color)
                tc["font_file"] = str(font_file or "")
                if ov is not None:
                    ox = int(ov.get("x") or 0)
                    oy = int(ov.get("y") or 0)
                    oh = int(ov.get("height") or 0)
                    tc["x"] = ox
                    tc["y"] = oy + max(24, oh + 8)
                if use_logo_motion and ov is not None:
                    tc["random_motion_enabled"] = True
                    tc["random_motion_interval"] = float(ov.get("random_motion_interval") or 2.0)
                    tc["random_motion_seed"] = int(ov.get("random_motion_seed") or 0)
                    tc["random_motion_smooth"] = bool(ov.get("random_motion_smooth", True))
                text_added += 1
            if prog_step and (j == n_rows or j % prog_step == 0):
                _ve_batch_status_progress(j, n_rows, "thêm chữ")
        if defer:
            tm.refresh_project_duration(project)
        pm.save_project(project)
        refresh_timeline()
        refresh_inspector()
        notify(f"Đã thêm chữ cho {text_added} clip video.")

    def _selected_video_time_ranges() -> list[tuple[float, float]]:
        ranges: list[tuple[float, float]] = []
        for _cid, c in _selected_video_timeline_rows():
            ts = float(c.get("timeline_start") or 0.0)
            du = max(0.0, float(c.get("duration") or 0.0))
            ranges.append((ts, ts + du))
        return ranges

    def _has_overlap(a0: float, a1: float, b0: float, b1: float) -> bool:
        return min(a1, b1) > max(a0, b0)

    def remove_logo_from_selected_video_clips() -> None:
        if not project:
            return
        ranges = _selected_video_time_ranges()
        if not ranges:
            messagebox.showinfo("Xóa logo", "Chọn ít nhất 1 clip video trong Timeline.")
            return
        removed = 0
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict) or str(tr.get("type") or "") != "overlay":
                continue
            kept: list[dict[str, Any]] = []
            for cl in tr.get("clips") or []:
                if not isinstance(cl, dict) or str(cl.get("type") or "") != "image":
                    kept.append(cl)
                    continue
                ts = float(cl.get("timeline_start") or 0.0)
                te = ts + max(0.0, float(cl.get("duration") or 0.0))
                if any(_has_overlap(ts, te, a0, a1) for a0, a1 in ranges):
                    removed += 1
                    continue
                kept.append(cl)
            tr["clips"] = kept
        pm.save_project(project)
        refresh_timeline()
        refresh_inspector()
        notify(f"Đã xóa {removed} logo (overlay) trong vùng clip đã chọn.")

    def remove_audio_from_selected_video_clips() -> None:
        if not project:
            return
        ranges = _selected_video_time_ranges()
        if not ranges:
            messagebox.showinfo("Xóa âm thanh", "Chọn ít nhất 1 clip video trong Timeline.")
            return
        removed_audio = 0
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict) or str(tr.get("type") or "") != "audio":
                continue
            kept: list[dict[str, Any]] = []
            for cl in tr.get("clips") or []:
                if not isinstance(cl, dict) or str(cl.get("type") or "") != "audio":
                    kept.append(cl)
                    continue
                ts = float(cl.get("timeline_start") or 0.0)
                te = ts + max(0.0, float(cl.get("duration") or 0.0))
                if any(_has_overlap(ts, te, a0, a1) for a0, a1 in ranges):
                    removed_audio += 1
                    continue
                kept.append(cl)
            tr["clips"] = kept
        removed_bgm = 0
        au = project.setdefault("audio_settings", {})
        bgm = au.get("bgm") or []
        kept_bgm: list[dict[str, Any]] = []
        for bg in bgm:
            if not isinstance(bg, dict):
                kept_bgm.append(bg)
                continue
            ts = float(bg.get("timeline_start") or 0.0)
            te = ts + max(0.0, float(bg.get("duration") or project.get("duration") or 0.0))
            if any(_has_overlap(ts, te, a0, a1) for a0, a1 in ranges):
                removed_bgm += 1
                continue
            kept_bgm.append(bg)
        au["bgm"] = kept_bgm
        pm.save_project(project)
        refresh_timeline()
        refresh_inspector()
        notify(f"Đã xóa {removed_audio} audio track + {removed_bgm} BGM trong vùng clip đã chọn.")

    def remove_text_from_selected_video_clips() -> None:
        if not project:
            return
        ranges = _selected_video_time_ranges()
        if not ranges:
            messagebox.showinfo("Xóa chữ", "Chọn ít nhất 1 clip video trong Timeline.")
            return
        removed = 0
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict) or str(tr.get("type") or "") != "text":
                continue
            kept: list[dict[str, Any]] = []
            for cl in tr.get("clips") or []:
                if not isinstance(cl, dict) or not _clip_is_text_track_payload(cl):
                    kept.append(cl)
                    continue
                ts = float(cl.get("timeline_start") or 0.0)
                te = ts + max(0.0, float(cl.get("duration") or 0.0))
                if any(_has_overlap(ts, te, a0, a1) for a0, a1 in ranges):
                    removed += 1
                    continue
                kept.append(cl)
            tr["clips"] = kept
        pm.save_project(project)
        refresh_timeline()
        refresh_inspector()
        notify(f"Đã xóa {removed} clip chữ trong vùng clip đã chọn.")

    def add_text_clip() -> None:
        if not project:
            return
        tx = str(var_new_text.get() or "").strip()
        if not tx:
            messagebox.showinfo("Text", "Nhập nội dung chữ ở ô «Text mới».")
            return
        try:
            tm.add_text_clip(project, tx, timeline_start=0.0, duration=5.0)
            refresh_timeline()
            notify("Đã thêm clip text.")
        except Exception as e:
            messagebox.showerror("Text", str(e))

    tl_ctx = tk.Menu(root, tearoff=0)
    tl_ctx.add_command(label="Thêm logo", command=add_logo_to_selected_video_clips)
    tl_ctx.add_command(label="Thêm âm thanh", command=add_audio_to_selected_video_clips)
    tl_ctx.add_command(label="Thêm chữ", command=add_text_to_selected_video_clips)
    tl_ctx.add_separator()
    tl_ctx.add_command(label="Xóa logo", command=remove_logo_from_selected_video_clips)
    tl_ctx.add_command(label="Xóa âm thanh", command=remove_audio_from_selected_video_clips)
    tl_ctx.add_command(label="Xóa chữ", command=remove_text_from_selected_video_clips)
    tl_ctx.add_separator()
    tl_ctx.add_command(label="Xóa clip đã chọn", command=lambda: clip_delete())
    tl_ctx.add_command(label="Xóa toàn bộ clip timeline", command=lambda: clip_delete(delete_all=True))
    tl_ctx.add_separator()
    tl_ctx.add_command(label="Chọn hết (Ctrl+A)", command=lambda: tree_tl.selection_set(tree_tl.get_children("")))
    tl_ctx.add_command(label="Copy dòng đã chọn", command=lambda: _tree_copy_selected_rows(tree_tl))
    tl_ctx.add_command(label="Copy link trong dòng đã chọn", command=lambda: _tree_copy_selected_links(tree_tl))
    tl_ctx.add_separator()
    tl_ctx.add_command(label="Xem file nguồn clip (ffplay)", command=open_with_ffplay)

    def _on_timeline_context_menu(event: Any) -> None:
        row = tree_tl.identify_row(event.y)
        tl_ctx_menu_video_cid_ref["cid"] = str(row) if row else None
        if row:
            cur_sel = set(tree_tl.selection())
            if row not in cur_sel:
                tree_tl.selection_set((row,))
                refresh_inspector()
        try:
            tl_ctx.tk_popup(event.x_root, event.y_root)
        finally:
            tl_ctx.grab_release()

    def _on_timeline_grouped_context_menu(event: Any) -> None:
        row = tree_tl_grouped.identify_row(event.y)
        _cid_ctx = str(grouped_video_to_clip.get(str(row) or "") or "").strip() if row else ""
        tl_ctx_menu_video_cid_ref["cid"] = _cid_ctx if _cid_ctx else None
        if row:
            cur_sel = set(tree_tl_grouped.selection())
            if row not in cur_sel:
                tree_tl_grouped.selection_set((row,))
            cid = grouped_video_to_clip.get(str(row) or "")
            if cid:
                tree_tl.selection_set((cid,))
                refresh_inspector()
        try:
            tl_ctx.tk_popup(event.x_root, event.y_root)
        finally:
            tl_ctx.grab_release()

    def _on_timeline_double_click(event: Any) -> str:
        row = tree_tl.identify_row(event.y)
        if row:
            try:
                tree_tl.selection_set((row,))
                tree_tl.focus(row)
                refresh_inspector()
            except Exception:
                pass
            preview_open_after_done_ref["v"] = True
            preview_open_after_done_ref["with"] = "ffplay"
            notify("Đang render preview nháp theo clip đã chọn…")
            run_preview_draft()
            return "break"
        return "break"

    def _on_timeline_grouped_double_click(event: Any) -> str:
        row = tree_tl_grouped.identify_row(event.y)
        if not row:
            return "break"
        cid = grouped_video_to_clip.get(str(row) or "")
        if not cid:
            return "break"
        try:
            tree_tl_grouped.selection_set((row,))
            tree_tl.selection_set((cid,))
            tree_tl.focus(cid)
            tl_nb.select(tl_tab_detail)
            refresh_inspector()
        except Exception:
            pass
        preview_open_after_done_ref["v"] = True
        preview_open_after_done_ref["with"] = "ffplay"
        notify("Đang render preview nháp theo clip đã chọn…")
        run_preview_draft()
        return "break"

    tree_tl.bind("<Button-3>", _on_timeline_context_menu)
    tree_tl_grouped.bind("<Button-3>", _on_timeline_grouped_context_menu)
    tree_tl.bind("<Double-1>", _on_timeline_double_click, add="+")
    tree_tl_grouped.bind("<Double-1>", _on_timeline_grouped_double_click, add="+")
    tree_tl.bind("<Control-a>", lambda _e: (tree_tl.selection_set(tree_tl.get_children("")), "break")[1], add="+")
    tree_tl.bind("<Control-A>", lambda _e: (tree_tl.selection_set(tree_tl.get_children("")), "break")[1], add="+")
    tree_tl_grouped.bind(
        "<Control-a>",
        lambda _e: (tree_tl_grouped.selection_set(tree_tl_grouped.get_children("")), "break")[1],
        add="+",
    )
    tree_tl_grouped.bind(
        "<Control-A>",
        lambda _e: (tree_tl_grouped.selection_set(tree_tl_grouped.get_children("")), "break")[1],
        add="+",
    )

    # --- Inspector + Export ---
    right = ttk.PanedWindow(main, orient=tk.VERTICAL)
    main.add(right, weight=3)
    try:
        main.paneconfigure(media_fr, minsize=96)
        main.paneconfigure(center, minsize=140)
        # Tránh panel phải bị bó quá hẹp làm che nút trong inspector/export.
        main.paneconfigure(right, minsize=320)
    except tk.TclError:
        pass

    insp_fr = ttk.LabelFrame(
        right,
        text="Chỉnh sửa theo clip đã chọn",
        padding=(6, 6, 6, 4),
    )
    right.add(insp_fr, weight=2)
    _exp_layout_state: dict[str, Any] = {"inspector_hidden": False}

    def _set_inspector_visible(show: bool) -> None:
        hidden = bool(_exp_layout_state.get("inspector_hidden"))
        if show and hidden:
            try:
                right.insert(0, insp_fr, weight=2)
            except Exception:
                right.add(insp_fr, weight=2)
            _exp_layout_state["inspector_hidden"] = False
            return
        if (not show) and (not hidden):
            try:
                right.forget(insp_fr)
            except Exception:
                pass
            _exp_layout_state["inspector_hidden"] = True
            return

    def _toggle_inspector_for_export() -> None:
        _set_inspector_visible(bool(_exp_layout_state.get("inspector_hidden")))
        try:
            _sync_export_toggle_btn_text()
        except Exception:
            pass

    insp_nb = ttk.Notebook(insp_fr)
    insp_nb.pack(fill=tk.BOTH, expand=True)
    tab_insp_clip = ttk.Frame(insp_nb, padding=(8, 6, 8, 4))
    insp_nb.add(tab_insp_clip, text="Chỉnh clip")
    # Hàng 0: chỉnh clip (cuộn). Hàng 1: Logo/Phạm vi/Gợi ý trong canvas cuộn + thanh footer cố định «Áp dụng tất cả»
    # (tránh PanedWindow / cửa sổ thấp cắt mất nút khi nội dung dưới quá cao).
    clip_tab_bottom = ttk.Frame(tab_insp_clip)
    clip_tab_scroll_host = ttk.Frame(tab_insp_clip)
    tab_insp_clip.columnconfigure(0, weight=1)
    # Ưu tiên còn chỗ cho footer «Áp dụng tất cả» khi cửa sổ thấp.
    tab_insp_clip.rowconfigure(0, weight=1, minsize=72)
    tab_insp_clip.rowconfigure(1, weight=0, minsize=92)
    clip_tab_scroll_host.grid(row=0, column=0, sticky="nsew")
    clip_tab_bottom.grid(row=1, column=0, sticky="nsew")
    clip_tab_bottom.columnconfigure(0, weight=1)
    clip_tab_bottom.rowconfigure(0, weight=1, minsize=56)
    clip_tab_bottom.rowconfigure(1, weight=0)
    clip_bottom_scroll_host = ttk.Frame(clip_tab_bottom)
    clip_bottom_scroll_host.grid(row=0, column=0, sticky="nsew")
    clip_bottom_scroll_host.columnconfigure(0, weight=1)
    clip_bottom_scroll_host.rowconfigure(0, weight=1)
    insp_bottom_inner = _pack_scrollable_vertical(clip_bottom_scroll_host)
    clip_footer = ttk.Frame(clip_tab_bottom)
    clip_footer.grid(row=1, column=0, sticky="ew", pady=(4, 0))
    _ve_batch_reset_bar_ref["fr"] = ttk.Frame(insp_bottom_inner)
    insp_grid = _pack_scrollable_vertical(clip_tab_scroll_host)
    fr_quick_edit_host = ttk.Frame(insp_bottom_inner)
    _AUTO_LOGO_MEDIA_LBL = "Tự động (ảnh trong Media — ưu tiên file import gần nhất)"

    def _section_collapsible(parent: ttk.Widget, title: str, *, start_open: bool = True) -> tuple[ttk.Frame, ttk.Frame, tk.BooleanVar]:
        """Một block có nút ▼/▶ để thu gọn nội dung — dễ tập trung từng nhóm chỉnh."""
        outer = ttk.Frame(parent)
        var_open = tk.BooleanVar(value=bool(start_open))
        head = ttk.Frame(outer)
        head.pack(fill=tk.X)
        inner_wrap = ttk.Frame(outer)
        inner = ttk.Frame(inner_wrap)

        def _btn_text() -> str:
            return (f"▶  {title}" if not var_open.get() else f"▼  {title}")

        def _sync() -> None:
            if var_open.get():
                inner_wrap.pack(fill=tk.X, pady=(2, 2))
                inner.pack(fill=tk.BOTH, expand=True, padx=(8, 0), pady=(0, 2))
            else:
                inner.pack_forget()
                inner_wrap.pack_forget()
            try:
                btn.configure(text=_btn_text())
            except tk.TclError:
                pass

        def _toggle() -> None:
            var_open.set(not var_open.get())
            _sync()

        btn = ttk.Button(head, text=_btn_text(), command=_toggle)
        btn.pack(anchor="w")
        _sync()
        return outer, inner, var_open

    def _bind_label_wrap_to_frame(lbl: ttk.Label, frame: ttk.Widget, *, inset: int = 12) -> None:
        """Cập nhật ``wraplength`` theo chiều rộng thực (frame + canvas cuộn nếu có)."""

        def _lbl_alive(w: ttk.Widget) -> bool:
            """Tránh TclError khi refresh inspector đã destroy widget nhưng <Configure> vẫn bắn."""
            try:
                return int(w.winfo_exists()) != 0
            except tk.TclError:
                return False

        def _apply(cw: int | None = None) -> None:
            if not _lbl_alive(lbl):
                return
            try:
                fw = int(frame.winfo_width())
            except tk.TclError:
                fw = 0
            try:
                base = fw if fw > 24 else int(cw or 0)
            except (TypeError, ValueError):
                base = fw
            if base > inset + 32:
                try:
                    lbl.configure(wraplength=max(48, int(base) - inset))
                except tk.TclError:
                    pass

        def _local(_e: Any = None) -> None:
            _apply(None)

        frame.bind("<Configure>", lambda _e: _local(), add="+")
        cur: Any = frame
        while cur is not None:
            reg = getattr(cur, "_ve_on_canvas_width", None)
            if callable(reg):

                def _cb(cw: int, _lbl=lbl, _fr=frame, _ins=inset) -> None:
                    if not _lbl_alive(_lbl):
                        return
                    try:
                        fw = int(_fr.winfo_width())
                    except tk.TclError:
                        fw = 0
                    usable = max(fw, int(cw) - 8) if int(cw) > 16 else fw
                    if usable > _ins + 28:
                        try:
                            _lbl.configure(wraplength=max(48, int(usable) - _ins))
                        except tk.TclError:
                            pass

                reg(_cb)
                break
            cur = getattr(cur, "master", None)
        try:
            frame.after_idle(lambda: _apply(None))
        except tk.TclError:
            pass

    def _scope_checkbox_wrapped(parent: ttk.Widget, *, text: str, variable: tk.BooleanVar) -> ttk.Frame:
        """Checkbox kèm nhãn xuống dòng — tránh ttk.Checkbutton cắt chữ dài trên Windows."""
        row = ttk.Frame(parent)
        row.columnconfigure(1, weight=1)
        ttk.Checkbutton(row, variable=variable).grid(row=0, column=0, sticky="nw", pady=2)
        lab = ttk.Label(row, text=text, justify=tk.LEFT, cursor="hand2")

        def _tog(_e: Any = None) -> None:
            try:
                variable.set(not bool(variable.get()))
            except tk.TclError:
                pass

        lab.bind("<Button-1>", _tog)

        def _lab_alive() -> bool:
            try:
                return int(lab.winfo_exists()) != 0
            except tk.TclError:
                return False

        def _apply(cw: int | None = None) -> None:
            if not _lab_alive():
                return
            try:
                rw = int(row.winfo_width())
            except tk.TclError:
                rw = 0
            try:
                cw_i = int(cw) if cw is not None else 0
            except (TypeError, ValueError):
                cw_i = 0
            base = max(rw, cw_i - 8) if cw_i > 16 else rw
            pad = 36
            if base > pad + 32:
                try:
                    lab.configure(wraplength=max(48, int(base) - pad))
                except tk.TclError:
                    pass

        def _local(_e: Any = None) -> None:
            _apply(None)

        lab.grid(row=0, column=1, sticky="ew", pady=2)
        row.bind("<Configure>", lambda _e: _local(), add="+")
        cur: Any = row
        while cur is not None:
            reg = getattr(cur, "_ve_on_canvas_width", None)
            if callable(reg):
                reg(lambda w: _apply(w))
                break
            cur = getattr(cur, "master", None)
        try:
            row.after_idle(lambda: _apply(None))
        except tk.TclError:
            pass
        return row

    def _batch_meta_store() -> dict[str, Any]:
        if not project:
            return {}
        meta = project.setdefault("meta", {})
        if not isinstance(meta, dict):
            meta = {}
            project["meta"] = meta
        mem = meta.get("batch_edit_memory")
        if not isinstance(mem, dict):
            mem = {}
            meta["batch_edit_memory"] = mem
        return mem

    def _schedule_quick_batch_if_active(reason: str = "chỉnh nhanh") -> None:
        # Theo yêu cầu mới: không tự động áp dụng khi thay đổi tùy chọn.
        _ = reason
        return

    var_ve_apply_clip = tk.BooleanVar(value=True)
    var_ve_apply_tf_full = tk.BooleanVar(value=False)
    var_ve_apply_tf_flip = tk.BooleanVar(value=False)
    var_ve_apply_tf_layout = tk.BooleanVar(value=False)
    var_ve_apply_tf_mute = tk.BooleanVar(value=False)
    var_ve_clip_only_diff_vs_file = tk.BooleanVar(value=True)
    var_ve_tf_only_diff_vs_file = tk.BooleanVar(value=True)
    var_ve_apply_ov_logo = tk.BooleanVar(value=True)
    var_ve_apply_ov_audio = tk.BooleanVar(value=True)
    var_ve_apply_ov_text = tk.BooleanVar(value=True)
    var_ve_apply_quick_logo = tk.BooleanVar(value=True)
    var_ve_apply_quick_text = tk.BooleanVar(value=True)
    sec_scope_outer, sec_scope_inner, _var_scope_open = _section_collapsible(
        insp_bottom_inner,
        "Phạm vi nút «Áp dụng tất cả»",
        start_open=False,
    )
    lf_apply_scope = ttk.Frame(sec_scope_inner, padding=(0, 0, 0, 4))
    lf_apply_scope.pack(fill=tk.X, expand=True)
    lf_apply_scope.columnconfigure(0, weight=1)

    lf_clip = ttk.LabelFrame(lf_apply_scope, text="Clip video & timeline", padding=(6, 4, 6, 6))
    lf_clip.grid(row=0, column=0, sticky="ew", pady=(0, 6))
    lf_clip.columnconfigure(0, weight=1)
    _scope_checkbox_wrapped(
        lf_clip,
        text="Chỉnh clip đầy đủ (timeline, cắt nguồn, tốc độ, fade, độ sáng / hiệu ứng ánh sáng, lật trong inspector clip…)",
        variable=var_ve_apply_clip,
    ).grid(row=0, column=0, sticky="ew")
    _scope_checkbox_wrapped(
        lf_clip,
        text="Chỉ ghi các trường clip khác file đã lưu — bỏ qua nhóm đã khớp (chỉ đẩy thông số mới)",
        variable=var_ve_clip_only_diff_vs_file,
    ).grid(row=1, column=0, sticky="ew", pady=(4, 0))
    _lbl_clip_hint1 = ttk.Label(
        lf_clip,
        text="Bỏ tích dòng trên nếu muốn ghi đủ mọi trường từ inspector (ép đồng bộ, kể cả trùng file).",
        font=("Segoe UI", 8),
        foreground="gray",
        justify=tk.LEFT,
    )
    _lbl_clip_hint1.grid(row=2, column=0, sticky="ew", pady=(0, 2))
    _bind_label_wrap_to_frame(_lbl_clip_hint1, lf_clip, inset=10)
    _scope_checkbox_wrapped(
        lf_clip,
        text="Transform (các ô dưới): chỉ áp nhánh còn lệch file — bỏ qua nhánh đã khớp",
        variable=var_ve_tf_only_diff_vs_file,
    ).grid(row=3, column=0, sticky="ew", pady=(4, 0))
    _lbl_tf_intro = ttk.Label(
        lf_clip,
        text="Transform & canvas — không đụng timeline / cắt nguồn / tốc độ:",
        font=("Segoe UI", 8),
        foreground="#444",
        justify=tk.LEFT,
    )
    _lbl_tf_intro.grid(row=4, column=0, sticky="ew", pady=(8, 2))
    _bind_label_wrap_to_frame(_lbl_tf_intro, lf_clip, inset=10)
    _f_tf_pad5 = ttk.Frame(lf_clip)
    _f_tf_pad5.grid(row=5, column=0, sticky="ew", padx=(12, 0))
    _f_tf_pad5.columnconfigure(0, weight=1)
    _scope_checkbox_wrapped(
        _f_tf_pad5,
        text="Đủ: lật + khung & zoom + âm gốc clip",
        variable=var_ve_apply_tf_full,
    ).grid(row=0, column=0, sticky="ew")
    _f_tf_pad6 = ttk.Frame(lf_clip)
    _f_tf_pad6.grid(row=6, column=0, sticky="w", padx=(12, 0))
    ttk.Checkbutton(_f_tf_pad6, text="Chỉ lật / xoay", variable=var_ve_apply_tf_flip).pack(side=tk.LEFT)
    _f_tf_pad7 = ttk.Frame(lf_clip)
    _f_tf_pad7.grid(row=7, column=0, sticky="ew", padx=(12, 0))
    _f_tf_pad7.columnconfigure(0, weight=1)
    _scope_checkbox_wrapped(
        _f_tf_pad7,
        text="Chỉ khung & zoom (fit/fill/stretch + zoom)",
        variable=var_ve_apply_tf_layout,
    ).grid(row=0, column=0, sticky="ew")
    _f_tf_pad8 = ttk.Frame(lf_clip)
    _f_tf_pad8.grid(row=8, column=0, sticky="ew", padx=(12, 0))
    _f_tf_pad8.columnconfigure(0, weight=1)
    _scope_checkbox_wrapped(
        _f_tf_pad8,
        text="Chỉ tắt / bật âm gốc clip",
        variable=var_ve_apply_tf_mute,
    ).grid(row=0, column=0, sticky="ew")
    _lbl_clip_hint2 = ttk.Label(
        lf_clip,
        text="«Đủ» = một lần áp cả ba nhóm dưới. Không chọn «Đủ» thì có thể tích nhiều ô để gộp.",
        font=("Segoe UI", 8),
        foreground="gray",
        justify=tk.LEFT,
    )
    _lbl_clip_hint2.grid(row=9, column=0, sticky="ew", pady=(4, 0))
    _bind_label_wrap_to_frame(_lbl_clip_hint2, lf_clip, inset=10)

    lf_logo = ttk.LabelFrame(lf_apply_scope, text="Logo", padding=(6, 4, 6, 6))
    lf_logo.grid(row=1, column=0, sticky="ew", pady=(0, 6))
    lf_logo.columnconfigure(0, weight=1)
    _scope_checkbox_wrapped(
        lf_logo,
        text="Inspector — ảnh logo track phủ (khối «Logo» cuối tab, đồng bộ clip)",
        variable=var_ve_apply_ov_logo,
    ).grid(row=0, column=0, sticky="ew")
    _scope_checkbox_wrapped(
        lf_logo,
        text="Chỉnh nhanh — ô Logo & ảnh phía trên tab (độ mờ, cỡ, vị trí…)",
        variable=var_ve_apply_quick_logo,
    ).grid(row=1, column=0, sticky="ew", pady=(4, 0))

    lf_audio = ttk.LabelFrame(lf_apply_scope, text="Âm thanh", padding=(6, 4, 6, 6))
    lf_audio.grid(row=2, column=0, sticky="ew", pady=(0, 6))
    lf_audio.columnconfigure(0, weight=1)
    _scope_checkbox_wrapped(
        lf_audio,
        text="Inspector — track âm phụ (file âm, âm lượng, tốc độ track phụ)",
        variable=var_ve_apply_ov_audio,
    ).grid(row=0, column=0, sticky="ew")
    _lbl_audio_hint = ttk.Label(
        lf_audio,
        text="Âm gốc của clip video nằm ở nhóm «Transform & canvas» hoặc trong «Chỉnh clip đầy đủ».",
        font=("Segoe UI", 8),
        foreground="gray",
        justify=tk.LEFT,
    )
    _lbl_audio_hint.grid(row=1, column=0, sticky="ew", pady=(4, 0))
    _bind_label_wrap_to_frame(_lbl_audio_hint, lf_audio, inset=10)

    lf_text = ttk.LabelFrame(lf_apply_scope, text="Chữ", padding=(6, 4, 6, 6))
    lf_text.grid(row=3, column=0, sticky="ew", pady=(0, 0))
    lf_text.columnconfigure(0, weight=1)
    _scope_checkbox_wrapped(
        lf_text,
        text="Inspector — nội dung chữ track phủ (khối «Chữ» cuối tab, đồng bộ clip)",
        variable=var_ve_apply_ov_text,
    ).grid(row=0, column=0, sticky="ew")
    _scope_checkbox_wrapped(
        lf_text,
        text="Chỉnh nhanh — ô Chữ phía trên tab (cỡ, màu, font…)",
        variable=var_ve_apply_quick_text,
    ).grid(row=1, column=0, sticky="ew", pady=(4, 0))

    def _apply_all_from_bottom() -> None:
        fn = _apply_batch_video_ref.get("fn")
        if callable(fn):
            fn()
            return
        rows_v = _selected_video_timeline_rows()
        if rows_v:
            wql = bool(var_ve_apply_quick_logo.get())
            wqt = bool(var_ve_apply_quick_text.get())
            if not (wql or wqt):
                messagebox.showinfo(
                    "Áp dụng tất cả",
                    "Bật ít nhất một mục trong «Phạm vi nút Áp dụng tất cả» (ví dụ Logo hoặc Chữ — chỉnh nhanh), "
                    "hoặc mở inspector clip video để áp clip / inspector.",
                    parent=root,
                )
                return
            c1 = quick_edit_logo_for_selected_videos(quiet=True) if wql else 0
            c2 = quick_edit_text_for_selected_videos(quiet=True) if wqt else 0
            if c1 or c2:
                pm.save_project(project)
                notify(f"Đã áp dụng chỉnh nhanh: {c1} logo, {c2} chữ.")
                refresh_timeline()
                refresh_inspector()
                _auto_preview_after_apply("áp dụng tất cả (logo/chữ)")
            else:
                messagebox.showinfo(
                    "Áp dụng tất cả",
                    "Chưa có thay đổi để áp — điền ô tương ứng ở khung «Logo · Chữ» (độ mờ, cỡ, vị trí, cỡ chữ, màu…).",
                    parent=root,
                )
            return
        messagebox.showinfo(
            "Chỉnh clip",
            "Chọn ít nhất một clip video trên timeline, rồi chỉnh khung «Logo · Chữ» hoặc mở inspector clip đơn.\n"
            "Một nút «Áp dụng tất cả» ghi clip (nếu có) + logo + chữ.",
            parent=root,
        )

    btn_apply_all_bottom = ttk.Button(
        clip_footer,
        text="Áp dụng tất cả",
        command=_apply_all_from_bottom,
    )
    var_q_logo_opacity = tk.StringVar(value="")
    var_q_logo_size_ratio = tk.StringVar(value="")
    var_q_logo_motion_mode = tk.StringVar(value="")
    var_q_logo_motion_interval = tk.StringVar(value="")
    var_q_logo_motion_seed = tk.StringVar(value="")
    var_q_text_size = tk.StringVar(value="")
    var_q_text_color = tk.StringVar(value="")
    var_q_text_font = tk.StringVar(value="")
    var_q_text_follow_logo = tk.StringVar(value="")
    var_q_text_position = tk.StringVar(value="")
    var_q_logo_position = tk.StringVar(value="")
    var_q_logo_media = tk.StringVar(value=_AUTO_LOGO_MEDIA_LBL)
    var_q_font_status = tk.StringVar(value="")
    var_ov_text = tk.StringVar(value="")
    _text_pos_opts = ("Giữa dưới", "Giữa trên", "Trái trên", "Phải trên", "Trái dưới", "Phải dưới")

    sec_logo_outer, sec_logo_inner, _var_sec_logo_open = _section_collapsible(
        fr_quick_edit_host,
        "Logo — độ mờ, kích, vị trí, chuyển động",
        start_open=True,
    )
    sec_logo_outer.pack(fill=tk.X, pady=(0, 2))
    lf_q_logo = ttk.Frame(sec_logo_inner, padding=(0, 0, 0, 4))
    lf_q_logo.pack(fill=tk.X)
    lf_q_logo.columnconfigure(0, minsize=108)
    lf_q_logo.columnconfigure(1, weight=1, uniform="qe")
    lf_q_logo.columnconfigure(2, minsize=118)
    lf_q_logo.columnconfigure(3, weight=1, uniform="qe")

    qr = 0
    ttk.Label(lf_q_logo, text="Độ mờ").grid(row=qr, column=0, sticky="w", padx=(0, 8))
    ttk.Combobox(
        lf_q_logo,
        textvariable=var_q_logo_opacity,
        values=("", "1.0", "0.92", "0.8", "0.65", "0.5", "0.35"),
        width=12,
    ).grid(row=qr, column=1, sticky="ew", padx=(0, 12))
    ttk.Label(lf_q_logo, text="Kích (tỉ lệ ngang)").grid(row=qr, column=2, sticky="w", padx=(0, 8))
    ttk.Combobox(
        lf_q_logo,
        textvariable=var_q_logo_size_ratio,
        values=("", "0.10", "0.12", "0.15", "0.18", "0.22", "0.28"),
        width=12,
    ).grid(row=qr, column=3, sticky="ew")
    qr += 1
    ttk.Label(lf_q_logo, text="Vị trí").grid(row=qr, column=0, sticky="w", padx=(0, 8), pady=(8, 0))
    cb_q_logo_position = ttk.Combobox(
        lf_q_logo,
        textvariable=var_q_logo_position,
        values=_text_pos_opts,
        width=12,
        state="readonly",
    )
    cb_q_logo_position.grid(row=qr, column=1, sticky="ew", padx=(0, 12), pady=(8, 0))
    ttk.Label(lf_q_logo, text="Chuyển động").grid(row=qr, column=2, sticky="w", padx=(0, 8), pady=(8, 0))
    ttk.Combobox(
        lf_q_logo,
        textvariable=var_q_logo_motion_mode,
        values=("", "Tắt", "Bật mượt"),
        width=12,
        state="normal",
    ).grid(row=qr, column=3, sticky="ew", pady=(8, 0))
    qr += 1
    ttk.Label(lf_q_logo, text="Bước (giây)").grid(row=qr, column=0, sticky="w", padx=(0, 8), pady=(8, 0))
    ttk.Combobox(
        lf_q_logo,
        textvariable=var_q_logo_motion_interval,
        values=("", "0.5", "1.0", "1.5", "2.0", "3.0", "5.0"),
        width=12,
    ).grid(row=qr, column=1, sticky="ew", padx=(0, 12), pady=(8, 0))
    ttk.Label(lf_q_logo, text="Seed").grid(row=qr, column=2, sticky="w", padx=(0, 8), pady=(8, 0))
    ttk.Combobox(
        lf_q_logo,
        textvariable=var_q_logo_motion_seed,
        values=("", "0", "1", "2", "7", "42", "99"),
        width=12,
    ).grid(row=qr, column=3, sticky="ew", pady=(8, 0))
    qr += 1
    ttk.Label(lf_q_logo, text="Ảnh khi tạo mới").grid(row=qr, column=0, sticky="nw", padx=(0, 8), pady=(8, 0))
    cb_q_logo_media = ttk.Combobox(
        lf_q_logo,
        textvariable=var_q_logo_media,
        values=[_AUTO_LOGO_MEDIA_LBL],
        state="readonly",
    )
    cb_q_logo_media.grid(row=qr, column=1, columnspan=3, sticky="ew", pady=(8, 0))

    def _reload_q_logo_media_combo() -> None:
        vals = [_AUTO_LOGO_MEDIA_LBL]
        label_to_id: dict[str, str] = {}
        id_to_label: dict[str, str] = {}
        if project:
            imgs = [
                m for m in (project.get("media") or []) if isinstance(m, dict) and str(m.get("type") or "") == "image"
            ]
            img_labels, label_to_id, id_to_label = _ve_build_media_combo_maps(
                imgs, fallback="image", include_empty=False
            )
            vals.extend(img_labels)
        _q_logo_media_combo_refresh["label_to_id"] = label_to_id
        _q_logo_media_combo_refresh["id_to_label"] = id_to_label
        try:
            cb_q_logo_media.configure(values=vals)
        except Exception:
            return
        cur = str(var_q_logo_media.get() or "").strip()
        if cur == _AUTO_LOGO_MEDIA_LBL:
            return
        if cur and cur in vals:
            return
        resolved = _ve_resolve_combo_display(
            cur,
            id_to_label=id_to_label,
            label_to_id=label_to_id,
            media_items=[
                m for m in (project.get("media") or [])
                if isinstance(m, dict) and str(m.get("type") or "") == "image"
            ]
            if project
            else None,
            fallback="image",
        )
        if resolved and resolved in vals:
            var_q_logo_media.set(resolved)
            return
        try:
            mem = _batch_meta_store()
            saved = str(mem.get("quick_logo_media_pick") or "").strip()
        except Exception:
            saved = ""
        if saved:
            saved_show = _ve_resolve_combo_display(
                saved,
                id_to_label=id_to_label,
                label_to_id=label_to_id,
                media_items=[
                    m for m in (project.get("media") or [])
                    if isinstance(m, dict) and str(m.get("type") or "") == "image"
                ]
                if project
                else None,
                fallback="image",
            )
            if saved_show and saved_show in vals:
                var_q_logo_media.set(saved_show)
                return
        if cur and "|" in cur:
            mid_legacy = cur.split("|", 1)[0].strip()
            if mid_legacy in id_to_label:
                var_q_logo_media.set(id_to_label[mid_legacy])
                return
        if not cur:
            var_q_logo_media.set(_AUTO_LOGO_MEDIA_LBL)

    def _persist_quick_logo_media_pick(_e: Any = None) -> None:
        if not project:
            return
        mem = _batch_meta_store()
        mem["quick_logo_media_pick"] = str(var_q_logo_media.get() or "").strip()
        try:
            pm.save_project(project)
        except Exception:
            pass

    cb_q_logo_media.bind("<<ComboboxSelected>>", _persist_quick_logo_media_pick)
    _q_logo_media_combo_refresh["fn"] = _reload_q_logo_media_combo

    def _persist_quick_logo_position_pick(_e: Any = None) -> None:
        if not project:
            return
        mem = _batch_meta_store()
        mem["quick_logo_position_pick"] = str(var_q_logo_position.get() or "").strip()
        try:
            pm.save_project(project)
        except Exception:
            pass

    cb_q_logo_position.bind("<<ComboboxSelected>>", _persist_quick_logo_position_pick)

    sec_text_outer, sec_text_inner, _var_sec_text_open = _section_collapsible(
        fr_quick_edit_host,
        "Chữ — nội dung, cỡ, màu, font, vị trí",
        start_open=True,
    )
    sec_text_outer.pack(fill=tk.X, pady=(6, 0))
    lf_q_text = ttk.Frame(sec_text_inner, padding=(0, 0, 0, 4))
    lf_q_text.pack(fill=tk.X)
    lf_q_text.columnconfigure(0, minsize=108)
    lf_q_text.columnconfigure(1, weight=1, uniform="qt")
    lf_q_text.columnconfigure(2, minsize=72)
    lf_q_text.columnconfigure(3, weight=1, uniform="qt")

    qt = 0
    ttk.Label(lf_q_text, text="Nội dung").grid(row=qt, column=0, sticky="nw", padx=(0, 8))
    ttk.Entry(lf_q_text, textvariable=var_ov_text).grid(row=qt, column=1, columnspan=3, sticky="ew")
    qt += 1
    ttk.Label(lf_q_text, text="Cỡ chữ").grid(row=qt, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
    ttk.Combobox(
        lf_q_text,
        textvariable=var_q_text_size,
        values=("", "24", "32", "40", "44", "52", "60", "72"),
        width=12,
    ).grid(row=qt, column=1, sticky="ew", padx=(0, 12), pady=(6, 0))
    ttk.Label(lf_q_text, text="Màu").grid(row=qt, column=2, sticky="w", padx=(0, 8), pady=(6, 0))
    ttk.Combobox(
        lf_q_text,
        textvariable=var_q_text_color,
        values=("", "white", "yellow", "black", "#00FF00", "#FFCC00", "#00BFFF", "#FF66CC"),
        width=12,
    ).grid(row=qt, column=3, sticky="ew", pady=(6, 0))
    qt += 1
    ttk.Label(lf_q_text, text="Font (.ttf/.otf)").grid(row=qt, column=0, sticky="w", padx=(0, 8), pady=(8, 0))
    var_q_text_font.set("Mặc định (trống)")
    fr_q_font_pick = ttk.Frame(lf_q_text)
    fr_q_font_pick.grid(row=qt, column=1, columnspan=3, sticky="ew", pady=(8, 0))
    cb_q_font_pick = ttk.Combobox(
        fr_q_font_pick,
        textvariable=var_q_text_font,
        values=["Mặc định (trống)"],
        width=42,
    )
    cb_q_font_pick.pack(side=tk.LEFT, fill=tk.X, expand=True)
    var_q_font_preview_text = tk.StringVar(value="Xem trước: AaBbYy 0123 Đậm/Nghiêng")
    _q_font_preview_ref: dict[str, Any] = {"font": None}
    qt += 1

    fr_q_font_info = ttk.Frame(lf_q_text)
    fr_q_font_info.grid(row=qt, column=0, columnspan=4, sticky="ew", pady=(4, 0))
    ttk.Label(
        fr_q_font_info,
        textvariable=var_q_font_status,
        foreground="#666",
        font=("Segoe UI", 8),
        wraplength=420,
        justify=tk.LEFT,
    ).pack(anchor="w")
    lbl_q_font_preview = tk.Label(
        fr_q_font_info,
        textvariable=var_q_font_preview_text,
        fg="#0f172a",
        bg=root.cget("bg"),
        anchor="w",
    )
    lbl_q_font_preview.pack(fill=tk.X, anchor="w", pady=(2, 0))
    qt += 1
    ttk.Label(lf_q_text, text="Chữ theo logo").grid(row=qt, column=0, sticky="w", padx=(0, 8), pady=(8, 0))
    ttk.Combobox(
        lf_q_text,
        textvariable=var_q_text_follow_logo,
        values=("Theo logo", "Không theo logo"),
        width=12,
        state="readonly",
    ).grid(row=qt, column=1, sticky="ew", padx=(0, 12), pady=(8, 0))
    ttk.Label(lf_q_text, text="Vị trí chữ").grid(row=qt, column=2, sticky="w", padx=(0, 8), pady=(8, 0))
    ttk.Combobox(
        lf_q_text,
        textvariable=var_q_text_position,
        values=_text_pos_opts,
        width=12,
        state="readonly",
    ).grid(row=qt, column=3, sticky="ew", pady=(8, 0))
    qt += 1
    ttk.Label(
        lf_q_text,
        text="Gợi ý: logo 1.0 = rõ nhất; «Theo logo» = chữ đi cùng chuyển động logo.",
        foreground="#666",
        font=("Segoe UI", 8),
        wraplength=560,
        justify=tk.LEFT,
    ).grid(row=qt, column=0, columnspan=4, sticky="w", pady=(10, 0))

    def _font_tokens(name: str) -> set[str]:
        base = str(name or "").lower().replace("-", " ").replace("_", " ")
        return {x for x in base.split() if x}

    def _guess_preview_font_family(file_stem: str, installed_map: dict[str, str]) -> str:
        raw = str(file_stem or "").strip()
        if not raw:
            return "Segoe UI"
        # Drop common style suffix tokens in filename to infer family name.
        style_tokens = {
            "bold",
            "italic",
            "oblique",
            "black",
            "heavy",
            "light",
            "thin",
            "medium",
            "semibold",
            "demibold",
            "condensed",
            "narrow",
            "regular",
            "bd",
            "it",
        }
        parts = [p for p in _font_tokens(raw) if p not in style_tokens]
        cand = " ".join(parts).strip()
        if cand and cand.lower() in installed_map:
            return installed_map[cand.lower()]
        raw_norm = raw.replace("_", " ").replace("-", " ").strip()
        if raw_norm.lower() in installed_map:
            return installed_map[raw_norm.lower()]
        # Fallback: pick the first installed family with matching prefix.
        for lk, fam in installed_map.items():
            if lk.startswith(raw_norm.lower()) or raw_norm.lower().startswith(lk):
                return fam
        return "Segoe UI"

    def _guess_preview_font_style(file_stem: str) -> tuple[str, str]:
        raw = str(file_stem or "").lower().strip()
        t = _font_tokens(raw)
        # Support both tokenized style names and compact Windows names (e.g. verdanaz, arialbi).
        is_bold = any(x in t for x in ("bold", "black", "heavy", "semibold", "demibold", "bd")) or raw.endswith(
            ("bd", "b")
        ) or raw.endswith(("bi", "z"))
        is_italic = any(x in t for x in ("italic", "oblique", "it")) or raw.endswith(("i", "it", "bi", "z"))
        weight = "bold" if is_bold else "normal"
        slant = "italic" if is_italic else "roman"
        return weight, slant

    def _scan_system_font_files() -> list[Path]:
        # Windows default fonts directory
        windir = os.environ.get("WINDIR") or "C:\\Windows"
        sys_fonts_dir = Path(windir) / "Fonts"
        out: list[Path] = []
        try:
            if sys_fonts_dir.exists():
                out.extend(sys_fonts_dir.glob("*.ttf"))
                out.extend(sys_fonts_dir.glob("*.otf"))
        except Exception:
            # Fallback: return empty -> keep "Mặc định (trống)"
            return []
        return out

    def _pick_preview_tuple_from_selection(pick: str) -> tuple[str, int, str, str]:
        p = str(pick or "").strip()
        if not p:
            return ("Segoe UI", 10, "normal", "roman")
        if p in q_font_label_to_preview_font:
            return q_font_label_to_preview_font[p]
        # Manual path input fallback.
        try:
            pp = Path(p)
            stem = pp.stem if pp.suffix.lower() in (".ttf", ".otf") else p
        except Exception:
            stem = p
        try:
            installed_families = sorted(set(tkfont.families(root)))
        except Exception:
            installed_families = ["Segoe UI"]
        installed_map = {f.lower(): f for f in installed_families}
        fam = _guess_preview_font_family(stem, installed_map)
        w, sl = _guess_preview_font_style(stem)
        return (fam, 10, w, sl)

    def _apply_inline_font_preview(_e: Any = None) -> None:
        family, size, weight, slant = _pick_preview_tuple_from_selection(var_q_text_font.get())
        try:
            fobj = tkfont.Font(root, family=family, size=size, weight=weight, slant=slant)
            _q_font_preview_ref["font"] = fobj
            lbl_q_font_preview.configure(font=fobj)
            tag = []
            if weight == "bold":
                tag.append("Đậm")
            if slant == "italic":
                tag.append("Nghiêng")
            if not tag:
                tag.append("Thường")
            var_q_font_preview_text.set(f"Xem trước ({'/'.join(tag)}): AaBbYy 0123 Tiếng Việt")
        except Exception:
            # Fallback to UI default
            _q_font_preview_ref["font"] = None
            lbl_q_font_preview.configure(font=("Segoe UI", 9))
            var_q_font_preview_text.set("Xem trước: AaBbYy 0123 Đậm/Nghiêng")

    def _reload_font_library() -> None:
        var_q_font_status.set("Đang quét font…")
        cb_q_font_pick.configure(state="disabled")

        def work() -> None:
            font_paths = _scan_system_font_files()
            # Build label -> absolute path mapping.
            mapping: dict[str, str] = {"Mặc định (trống)": ""}
            preview_map: dict[str, tuple[str, int, str, str]] = {"Mặc định (trống)": ("Segoe UI", 10, "normal", "roman")}
            try:
                installed_families = sorted(set(tkfont.families(root)))
            except Exception:
                installed_families = ["Segoe UI"]
            installed_map = {f.lower(): f for f in installed_families}
            # Limit to keep UI responsive if system has tons of fonts.
            max_fonts = 1200
            font_paths = sorted(font_paths, key=lambda p: p.name.lower())[:max_fonts]
            for p in font_paths:
                stem = p.stem.strip() or p.name
                # Use full filename to avoid duplicate label collisions.
                label = f"{stem} — {p.name}"
                # If collision, append suffix.
                if label in mapping:
                    label = f"{stem} — {p.name} ({len(mapping)})"
                mapping[label] = str(p)
                fam = _guess_preview_font_family(stem, installed_map)
                w, sl = _guess_preview_font_style(stem)
                preview_map[label] = (fam, 10, w, sl)

            labels = list(mapping.keys())
            # Ensure current selection is valid; otherwise reset to default.
            def done() -> None:
                nonlocal q_font_label_to_path, q_font_label_to_preview_font
                q_font_label_to_path = mapping
                q_font_label_to_preview_font = preview_map
                cb_q_font_pick.configure(values=labels, state="normal")
                if var_q_text_font.get() not in mapping:
                    var_q_text_font.set("Mặc định (trống)")
                _apply_inline_font_preview()
                var_q_font_status.set(f"Đã có {len(labels) - 1} font (trống = mặc định).")

            _schedule_on_main_thread(done)

        threading.Thread(target=work, daemon=True).start()

    def _open_font_preview_picker() -> None:
        labels_all = list(q_font_label_to_path.keys())
        if not labels_all:
            messagebox.showinfo("Font", "Chưa có font để hiển thị. Hãy bấm «Nạp font».", parent=root)
            return
        win = tk.Toplevel(root)
        win.title("Thư viện font — xem style thật")
        win.geometry("860x520")
        win.minsize(680, 380)
        win.transient(root)
        win.grab_set()

        top = ttk.Frame(win, padding=8)
        top.pack(fill=tk.X)
        ttk.Label(top, text="Tìm font").pack(side=tk.LEFT)
        var_kw = tk.StringVar(value="")
        ent_kw = ttk.Entry(top, textvariable=var_kw)
        ent_kw.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(6, 6))
        ttk.Label(top, text="(Click đôi để chọn)", foreground="#666").pack(side=tk.LEFT)

        body = ttk.Frame(win, padding=(8, 0, 8, 8))
        body.pack(fill=tk.BOTH, expand=True)
        lb = tk.Listbox(body, activestyle="none")
        sy = ttk.Scrollbar(body, orient="vertical", command=lb.yview)
        lb.configure(yscrollcommand=sy.set)
        lb.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sy.pack(side=tk.LEFT, fill=tk.Y)

        cache_fonts: list[tkfont.Font] = []
        idx_to_label: list[str] = []
        preview_text = "  AaBbYy  0123456789  Tiếng Việt: Đậm · Nghiêng"

        def refill() -> None:
            nonlocal cache_fonts, idx_to_label
            kw = str(var_kw.get() or "").strip().lower()
            lb.delete(0, tk.END)
            cache_fonts = []
            idx_to_label = []
            for label in labels_all:
                if kw and kw not in label.lower():
                    continue
                family, size, weight, slant = q_font_label_to_preview_font.get(label, ("Segoe UI", 10, "normal", "roman"))
                item_txt = f"{label}  |{preview_text}|"
                idx = lb.size()
                lb.insert(tk.END, item_txt)
                try:
                    fobj = tkfont.Font(win, family=family, size=size, weight=weight, slant=slant)
                    lb.itemconfig(idx, font=fobj)
                    cache_fonts.append(fobj)
                except Exception:
                    # Fallback to default list font.
                    pass
                idx_to_label.append(label)

        def choose_selected(_e: Any = None) -> None:
            sel = lb.curselection()
            if not sel:
                return
            i = int(sel[0])
            if i < 0 or i >= len(idx_to_label):
                return
            var_q_text_font.set(idx_to_label[i])
            _apply_inline_font_preview()
            try:
                win.destroy()
            except Exception:
                pass

        ent_kw.bind("<KeyRelease>", lambda _e: refill())
        lb.bind("<Double-Button-1>", choose_selected)
        lb.bind("<Return>", choose_selected)
        ttk.Button(top, text="Chọn font đang bôi", command=choose_selected).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(top, text="Đóng", command=win.destroy).pack(side=tk.LEFT, padx=(6, 0))
        refill()
        ent_kw.focus_set()

    ttk.Button(fr_q_font_pick, text="Nạp font", command=_reload_font_library).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(fr_q_font_pick, text="Xem style font", command=_open_font_preview_picker).pack(side=tk.LEFT, padx=(6, 0))

    def _on_q_font_pick_changed(_e: Any = None) -> None:
        _apply_inline_font_preview()
        _schedule_quick_batch_if_active("font chữ")

    cb_q_font_pick.bind("<<ComboboxSelected>>", _on_q_font_pick_changed)
    cb_q_font_pick.bind("<KeyRelease>", _on_q_font_pick_changed)

    _reload_font_library()
    _apply_inline_font_preview()
    _reload_q_logo_media_combo()
    try:
        _mem_lp = _batch_meta_store()
        _sp_lp = str(_mem_lp.get("quick_logo_position_pick") or "").strip()
        if _sp_lp in _text_pos_opts:
            var_q_logo_position.set(_sp_lp)
    except Exception:
        pass

    _sep_clip_logo_scope = ttk.Separator(insp_bottom_inner, orient=tk.HORIZONTAL)
    _sep_clip_scope_hint = ttk.Separator(insp_bottom_inner, orient=tk.HORIZONTAL)

    fr_quick_edit_host.pack(fill=tk.X, pady=(0, 2))
    _sep_clip_logo_scope.pack(fill=tk.X, pady=(8, 8))
    sec_scope_outer.pack(fill=tk.X, pady=(0, 2))
    _sep_clip_scope_hint.pack(fill=tk.X, pady=(8, 8))
    sec_hint_outer, sec_hint_inner, _var_hint_open = _section_collapsible(
        insp_bottom_inner,
        "Gợi ý nhanh",
        start_open=False,
    )
    lbl_clip_quick_hint = ttk.Label(
        sec_hint_inner,
        text=(
            "Cuộn khung phía trên để sửa chi tiết từng clip. "
            "Cuộn vùng dưới (Logo / Phạm vi — Clip · Logo · Âm · Chữ / gợi ý) nếu không thấy hết. "
            "Nút «Áp dụng tất cả» luôn ở thanh dưới cùng của tab. "
            "«Ảnh khi tạo mới»: chọn file hoặc «Tự động» (ảnh trong Media)."
        ),
        foreground="#5c5c5c",
        font=("Segoe UI", 8),
        wraplength=680,
        justify=tk.LEFT,
    )
    lbl_clip_quick_hint.pack(anchor="w", pady=(0, 2))
    sec_hint_outer.pack(fill=tk.X, pady=(0, 4))
    ttk.Separator(clip_footer, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(0, 4))
    btn_apply_all_bottom.pack(fill=tk.X, pady=(0, 2), ipady=4)

    for _vq in (
        var_q_logo_opacity,
        var_q_logo_size_ratio,
        var_q_logo_motion_mode,
        var_q_logo_motion_interval,
        var_q_logo_motion_seed,
        var_q_logo_position,
        var_ov_text,
        var_q_text_size,
        var_q_text_color,
        var_q_text_font,
        var_q_text_follow_logo,
        var_q_text_position,
        var_q_logo_media,
    ):
        _vq.trace_add("write", lambda *_a: _schedule_quick_batch_if_active("chỉnh nhanh logo/chữ"))

    tab_phase2 = ttk.Frame(insp_nb, padding=4)
    insp_nb.add(tab_phase2, text="Nhạc & phụ đề")

    p2_fr = _pack_scrollable_vertical(tab_phase2)

    ttk.Label(
        p2_fr,
        text=(
            "Thiết lập nhanh cho toàn dự án: phụ đề, BGM, kho nhạc, chuyển cảnh. "
            "Mẹo: hãy chọn clip video trên Timeline trước khi gán filter/chuyển cảnh."
        ),
        foreground="#444",
        font=("Segoe UI", 8),
        wraplength=380,
        justify="left",
    ).grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 8))
    p2_fr.columnconfigure(0, weight=0)
    p2_fr.columnconfigure(1, weight=1)
    p2_fr.columnconfigure(2, weight=0)

    ttk.Label(p2_fr, text="Filter màu (chọn clip trên Timeline):").grid(row=2, column=0, sticky="w", pady=(6, 0))
    var_filt = tk.StringVar(value="normal")
    cb_filt = ttk.Combobox(
        p2_fr,
        textvariable=var_filt,
        values=list(VideoFilterManager.PRESETS.keys()),
        width=14,
        state="readonly",
    )
    fr_filter = ttk.Frame(p2_fr)
    fr_filter.grid(row=2, column=1, columnspan=2, sticky="w", padx=4, pady=(6, 0))
    cb_filt.configure(width=18)
    cb_filt.pack(in_=fr_filter, side=tk.LEFT)

    def apply_color_filter() -> None:
        if not project or not selected_clip_id:
            messagebox.showinfo("Filter", "Chọn clip video trên timeline.")
            return
        _, cl = _find_clip(selected_clip_id)
        if not cl or str(cl.get("type")) != "video":
            messagebox.showinfo("Filter", "Chọn clip video.")
            return
        try:
            vf_mgr.apply_filter(project, selected_clip_id, {"type": var_filt.get()})
            pm.save_project(project)
            notify(f"Đã gán filter màu: {var_filt.get()}")
        except Exception as e:
            messagebox.showerror("Filter", str(e))

    ttk.Button(fr_filter, text="Gán filter", command=apply_color_filter).pack(side=tk.LEFT, padx=(6, 0))

    ttk.Separator(p2_fr, orient=tk.HORIZONTAL).grid(row=3, column=0, columnspan=3, sticky="ew", pady=8)

    ttk.Label(p2_fr, text="Phụ đề khi xuất video:").grid(row=4, column=0, sticky="nw")

    def import_srt() -> None:
        if not project:
            return
        fp = filedialog.askopenfilename(parent=root, filetypes=[("SRT", "*.srt"), ("VTT", "*.vtt"), ("All", "*.*")])
        if not fp:
            return
        try:
            if fp.lower().endswith(".vtt"):
                sub_mgr.import_vtt(project, fp)
            else:
                sub_mgr.import_srt(project, fp)
            pm.save_project(project)
            notify("Đã import phụ đề SRT/VTT.")
        except Exception as e:
            messagebox.showerror("Subtitle", str(e))

    ttk.Button(p2_fr, text="Nhập SRT / VTT", command=import_srt).grid(row=4, column=1, columnspan=2, sticky="w")

    ttk.Label(p2_fr, text="Nhạc nền BGM (chọn từ tab «File nhạc (dự án)»):").grid(row=5, column=0, columnspan=3, sticky="w", pady=(6, 0))
    var_bgm_vol = tk.StringVar(value="0.25")

    def add_bgm() -> None:
        if not project:
            return
        if _active_media_kind() == "stock":
            messagebox.showinfo("BGM", "Chọn audio trong tab «File nhạc (dự án)» — hoặc trong tab stock dùng «Thêm Media + BGM».")
            return
        mid = _selected_media_id()
        if not mid:
            messagebox.showinfo("BGM", "Chọn media audio trong bảng Media.")
            return
        media = _find_media(mid)
        if not media or str(media.get("type")) != "audio":
            messagebox.showinfo("BGM", "Chọn file audio đã import.")
            return
        try:
            vol = float(var_bgm_vol.get().strip() or "0.25")
            amix_mgr.add_background_music(project, mid, vol)
            pm.save_project(project)
            notify("Đã thêm nhạc nền (audio_settings).")
            if bool(var_media_only_timeline.get()):
                refresh_media_tree()
        except Exception as e:
            messagebox.showerror("BGM", str(e))

    def auto_add_bgm_from_existing_audio() -> None:
        if not project:
            return
        try:
            vol = float(var_bgm_vol.get().strip() or "0.25")
            n = amix_mgr.auto_add_existing_audio_as_bgm(project, vol, only_first=True, skip_existing_media=True)
            if n <= 0:
                messagebox.showinfo("BGM", "Không có audio khả dụng để thêm (hoặc đã thêm trước đó).")
                return
            pm.save_project(project)
            notify("Đã tự thêm audio có sẵn vào BGM.")
            if bool(var_media_only_timeline.get()):
                refresh_media_tree()
        except Exception as e:
            messagebox.showerror("BGM", str(e))

    fr_bgm = ttk.Frame(p2_fr)
    fr_bgm.grid(row=6, column=0, columnspan=3, sticky="w", padx=4, pady=(4, 0))
    ttk.Entry(fr_bgm, textvariable=var_bgm_vol, width=8).pack(side=tk.LEFT)
    ttk.Button(fr_bgm, text="Thêm BGM", command=add_bgm).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(p2_fr, text="Tự thêm audio có sẵn", command=auto_add_bgm_from_existing_audio).grid(
        row=7, column=0, columnspan=3, sticky="w", pady=(4, 0)
    )

    var_duck_start = tk.StringVar(value="0.0")
    var_duck_end = tk.StringVar(value="5.0")
    var_duck_vol = tk.StringVar(value="0.15")

    def add_duck() -> None:
        if not project:
            return
        try:
            a = float(str(var_duck_start.get()).strip() or "0")
            b = float(str(var_duck_end.get()).strip() or "0")
            v = float(str(var_duck_vol.get()).strip() or "0.15")
        except ValueError:
            messagebox.showerror("Ducking", "Giá trị ducking không hợp lệ.")
            return
        if b <= a:
            messagebox.showerror("Ducking", "Thời điểm kết thúc phải lớn hơn bắt đầu.")
            return
        v = max(0.0, min(1.0, v))
        try:
            amix_mgr.add_ducking_range(project, a, b, v)
            pm.save_project(project)
            notify("Đã thêm vùng ducking BGM.")
        except Exception as e:
            messagebox.showerror("Ducking", str(e))

    fr_duck = ttk.Frame(p2_fr)
    fr_duck.grid(row=8, column=0, columnspan=2, sticky="w", pady=(4, 0))
    ttk.Label(fr_duck, text="Ducking từ").pack(side=tk.LEFT)
    ttk.Entry(fr_duck, textvariable=var_duck_start, width=7).pack(side=tk.LEFT, padx=(6, 6))
    ttk.Label(fr_duck, text="đến").pack(side=tk.LEFT)
    ttk.Entry(fr_duck, textvariable=var_duck_end, width=7).pack(side=tk.LEFT, padx=(6, 6))
    ttk.Label(fr_duck, text="vol").pack(side=tk.LEFT)
    ttk.Entry(fr_duck, textvariable=var_duck_vol, width=6).pack(side=tk.LEFT, padx=(6, 6))
    ttk.Button(fr_duck, text="Thêm vùng ducking BGM", command=add_duck).pack(side=tk.LEFT)
    def _ovlp(a0: float, a1: float, b0: float, b1: float) -> bool:
        return min(a1, b1) > max(a0, b0)

    def quick_edit_logo_for_selected_videos(*, quiet: bool = False) -> int:
        if not project:
            return 0
        rows = _selected_video_timeline_rows()
        if not rows:
            if not quiet:
                messagebox.showinfo("Logo", "Chọn ít nhất 1 clip video ở Timeline trước.")
            return 0
        pos_pick = str(var_q_logo_position.get() or "").strip()
        logo_patch: dict[str, Any] = {}
        try:
            opa_s = str(var_q_logo_opacity.get() or "").strip()
            if opa_s:
                logo_patch["opacity"] = max(0.0, min(1.0, float(opa_s)))
            ratio_s = str(var_q_logo_size_ratio.get() or "").strip()
            if ratio_s:
                logo_ratio = max(0.02, min(0.6, float(ratio_s)))
                canvas_w = max(1, int(project.get("width") or 1080))
                _lm_q = _resolve_logo_media_id_from_ui(
                    str(var_q_logo_media.get() or ""), allow_auto=True
                )
                logo_w, logo_h = compute_logo_overlay_dimensions(
                    _find_media(_lm_q) if _lm_q else None,
                    canvas_w=canvas_w,
                    logo_ratio=logo_ratio,
                )
                logo_patch["width"] = logo_w
                logo_patch["height"] = logo_h
            motion_mode_s = str(var_q_logo_motion_mode.get() or "").strip().lower()
            if motion_mode_s:
                use_motion = motion_mode_s.startswith("bật")
                logo_patch["random_motion_enabled"] = bool(use_motion)
            iv_s = str(var_q_logo_motion_interval.get() or "").strip()
            if iv_s:
                logo_patch["random_motion_interval"] = max(0.25, min(120.0, float(iv_s)))
            seed_s = str(var_q_logo_motion_seed.get() or "").strip()
            if seed_s:
                logo_patch["random_motion_seed"] = int(seed_s)
            if "random_motion_enabled" in logo_patch:
                logo_patch["random_motion_smooth"] = bool(logo_patch["random_motion_enabled"])
        except ValueError:
            if quiet:
                return 0
            messagebox.showerror("Logo", "Thông số logo không hợp lệ.")
            return 0
        if not logo_patch and not pos_pick:
            if not quiet:
                notify("Logo nhanh: chưa nhập thông số, bỏ qua.")
            return 0

        logo_mid = _resolve_logo_media_id_from_ui(
            str(var_q_logo_media.get() or ""), allow_auto=True
        )
        pw2 = max(1, int(project.get("width") or 1080))
        ph2 = int(project.get("height") or 1920)
        rows_sorted = sorted(rows, key=lambda r: float(r[1].get("timeline_start") or 0.0))
        changed = 0
        for _cid, vc in rows_sorted:
            overlay_intervals: list[tuple[float, float, dict[str, Any]]] = []
            for oc in _find_track_clips_local("overlay"):
                if not isinstance(oc, dict) or str(oc.get("type") or "") != "image":
                    continue
                os = float(oc.get("timeline_start") or 0.0)
                oe = os + max(0.0, float(oc.get("duration") or 0.0))
                overlay_intervals.append((os, oe, oc))
            overlay_intervals.sort(key=lambda t: t[0])
            n_ov = len(overlay_intervals)
            oi = 0
            vs = float(vc.get("timeline_start") or 0.0)
            ve = vs + max(0.0, float(vc.get("duration") or 0.0))
            while oi < n_ov and overlay_intervals[oi][1] <= vs:
                oi += 1
            ok = oi
            first_hit: dict[str, Any] | None = None
            dup_ids: list[str] = []
            while ok < n_ov and overlay_intervals[ok][0] < ve:
                os, oe, oc = overlay_intervals[ok]
                if _ovlp(vs, ve, os, oe):
                    if first_hit is None:
                        first_hit = oc
                    else:
                        _eid = str((oc or {}).get("id") or "").strip()
                        if _eid:
                            dup_ids.append(_eid)
                ok += 1
            patch_apply = dict(logo_patch)
            if pos_pick:
                side_i = int(patch_apply.get("width") or 0)
                if first_hit is not None:
                    if side_i <= 0:
                        try:
                            side_i = max(
                                1,
                                int(first_hit.get("width") or first_hit.get("height") or 80),
                            )
                        except (TypeError, ValueError):
                            side_i = 80
                else:
                    if side_i <= 0:
                        ratio_s2 = str(var_q_logo_size_ratio.get() or "").strip()
                        try:
                            rr2 = max(0.02, min(0.6, float(ratio_s2))) if ratio_s2 else 0.15
                        except ValueError:
                            rr2 = 0.15
                        side_i = max(80, int(pw2 * rr2))
                        patch_apply.setdefault("width", side_i)
                        patch_apply.setdefault("height", side_i)
                lx, ly = _logo_corner_xy_from_label(pos_pick, pw2, ph2, side_i)
                patch_apply["x"] = lx
                patch_apply["y"] = ly
            if first_hit is not None:
                for k, v in patch_apply.items():
                    first_hit[k] = v
                changed += 1
            elif logo_mid:
                _new_ov: list[dict[str, Any]] = []
                try:
                    tm.add_clip(
                        project,
                        logo_mid,
                        "overlay",
                        persist=False,
                        recompute_duration=False,
                        out_new_clip=_new_ov,
                    )
                except Exception as e:
                    if not quiet:
                        messagebox.showerror("Logo", f"Không thêm logo lên timeline: {e}")
                else:
                    if _new_ov:
                        oc2 = _new_ov[0]
                        oc2["timeline_start"] = vs
                        oc2["duration"] = max(0.1, ve - vs)
                        for k, v in patch_apply.items():
                            oc2[k] = v
                        changed += 1
            for _eid in dup_ids:
                tm.delete_clip(project, _eid, persist=False, recompute_duration=False)
        if changed and project:
            try:
                tm.refresh_project_duration(project)
            except Exception:
                pass
        if not quiet:
            pm.save_project(project)
            refresh_timeline()
            refresh_inspector()
            notify(f"Đã cập nhật {changed} logo theo clip video đã chọn.")
        return changed

    def quick_edit_text_for_selected_videos(*, quiet: bool = False) -> int:
        if not project:
            return 0
        rows = _selected_video_timeline_rows()
        if not rows:
            if not quiet:
                messagebox.showinfo("Chữ", "Chọn ít nhất 1 clip video ở Timeline trước.")
            return 0
        text_patch: dict[str, Any] = {}
        size_s = str(var_q_text_size.get() or "").strip()
        if size_s:
            try:
                text_patch["font_size"] = max(10, min(220, int(size_s)))
            except ValueError:
                if quiet:
                    return 0
                messagebox.showerror("Chữ", "Cỡ chữ không hợp lệ.")
                return 0
        col = str(var_q_text_color.get() or "").strip()
        if col:
            text_patch["color"] = col
        ff_pick = str(var_q_text_font.get() or "").strip()
        if ff_pick == "Mặc định (trống)":
            ff_pick = ""
        ff_path = q_font_label_to_path.get(ff_pick, "") if ff_pick else ""
        if not ff_path and ff_pick and Path(ff_pick).exists():
            # Allow manual path input besides dropdown label selection.
            ff_path = ff_pick
        if ff_pick:
            text_patch["font_file"] = str(ff_path or "")
        follow_s = str(var_q_text_follow_logo.get() or "").strip().lower()
        jump_logo = follow_s.startswith("theo")
        pos_pick = str(var_q_text_position.get() or "").strip()
        content_s = str(var_ov_text.get() or "").strip()
        if content_s:
            text_patch["text"] = content_s
        if not text_patch and not ff_pick and not follow_s and not pos_pick:
            if not quiet:
                notify("Chữ nhanh: chưa nhập thông số, bỏ qua.")
            return 0
        pw = int(project.get("width") or 1080)
        ph = int(project.get("height") or 1920)
        margin = 40

        def _pick_text_pos() -> tuple[int, int]:
            if pos_pick == "Giữa trên":
                return max(margin, pw // 2 - 140), margin
            if pos_pick == "Trái trên":
                return margin, margin
            if pos_pick == "Phải trên":
                return max(margin, pw - 280), margin
            if pos_pick == "Trái dưới":
                return margin, max(margin, ph - 160)
            if pos_pick == "Phải dưới":
                return max(margin, pw - 280), max(margin, ph - 160)
            return max(margin, pw // 2 - 140), max(margin, ph - 160)
        rows_sorted = sorted(rows, key=lambda r: float(r[1].get("timeline_start") or 0.0))
        changed = 0
        for _cid, vc in rows_sorted:
            overlay_intervals: list[tuple[float, float, dict[str, Any]]] = []
            for oc in _find_track_clips_local("overlay"):
                if not isinstance(oc, dict) or str(oc.get("type") or "") != "image":
                    continue
                os = float(oc.get("timeline_start") or 0.0)
                oe = os + max(0.0, float(oc.get("duration") or 0.0))
                overlay_intervals.append((os, oe, oc))
            overlay_intervals.sort(key=lambda t: t[0])
            text_intervals: list[tuple[float, float, dict[str, Any]]] = []
            for tc in _find_track_clips_local("text"):
                if not isinstance(tc, dict) or not _clip_is_text_track_payload(tc):
                    continue
                ts = float(tc.get("timeline_start") or 0.0)
                te = ts + max(0.0, float(tc.get("duration") or 0.0))
                text_intervals.append((ts, te, tc))
            text_intervals.sort(key=lambda t: t[0])
            n_ov = len(overlay_intervals)
            n_txt = len(text_intervals)
            vs = float(vc.get("timeline_start") or 0.0)
            ve = vs + max(0.0, float(vc.get("duration") or 0.0))
            oi = 0
            while oi < n_ov and overlay_intervals[oi][1] <= vs:
                oi += 1
            ov_match: dict[str, Any] | None = None
            ok = oi
            while ok < n_ov and overlay_intervals[ok][0] < ve:
                os, oe, oc = overlay_intervals[ok]
                if _ovlp(vs, ve, os, oe):
                    ov_match = oc
                    break
                ok += 1
            tti = 0
            while tti < n_txt and text_intervals[tti][1] <= vs:
                tti += 1
            tk = tti
            first_txt: dict[str, Any] | None = None
            dup_txt_ids: list[str] = []
            while tk < n_txt and text_intervals[tk][0] < ve:
                ts, te, tc = text_intervals[tk]
                if _ovlp(vs, ve, ts, te):
                    if first_txt is None:
                        first_txt = tc
                    else:
                        _eid = str((tc or {}).get("id") or "").strip()
                        if _eid:
                            dup_txt_ids.append(_eid)
                tk += 1
            if first_txt is None and (text_patch or ff_pick or follow_s or pos_pick):
                _nt: list[dict[str, Any]] = []
                try:
                    tdur = max(0.1, max(0.0, ve - vs))
                    tm.add_text_clip(
                        project,
                        content_s or "Chữ",
                        timeline_start=vs,
                        duration=tdur,
                        persist=False,
                        recompute_duration=False,
                        out_new_clip=_nt,
                    )
                except Exception as e:
                    if not quiet:
                        messagebox.showerror("Chữ", f"Không thêm clip chữ: {e}")
                else:
                    if _nt:
                        first_txt = _nt[0]
            if first_txt is not None:
                for k, v in text_patch.items():
                    first_txt[k] = v
                if jump_logo and ov_match is not None:
                    ox = int(ov_match.get("x") or 0)
                    oy = int(ov_match.get("y") or 0)
                    oh = int(ov_match.get("height") or 0)
                    first_txt["x"] = ox
                    first_txt["y"] = oy + max(24, oh + 8)
                    first_txt["random_motion_enabled"] = bool(ov_match.get("random_motion_enabled"))
                    first_txt["random_motion_interval"] = float(ov_match.get("random_motion_interval") or 2.0)
                    first_txt["random_motion_seed"] = int(ov_match.get("random_motion_seed") or 0)
                    first_txt["random_motion_smooth"] = bool(ov_match.get("random_motion_smooth", True))
                elif pos_pick:
                    tx, ty = _pick_text_pos()
                    first_txt["x"] = tx
                    first_txt["y"] = ty
                    if follow_s:
                        first_txt["random_motion_enabled"] = False
                changed += 1
            for _eid in dup_txt_ids:
                tm.delete_clip(project, _eid, persist=False, recompute_duration=False)
        if changed and project:
            try:
                tm.refresh_project_duration(project)
            except Exception:
                pass
        if not quiet:
            pm.save_project(project)
            refresh_timeline()
            refresh_inspector()
            notify(f"Đã cập nhật {changed} clip chữ theo clip video đã chọn.")
        return changed

    var_p2_show_advanced = tk.BooleanVar(value=False)

    def _set_p2_advanced_visible(show: bool) -> None:
        for w in p2_fr.grid_slaves():
            try:
                row = int(w.grid_info().get("row", 0))
            except Exception:
                row = 0
            if row >= 9:
                if show:
                    w.grid()
                else:
                    w.grid_remove()
        btn_p2_adv.configure(text=("Ẩn chức năng nâng cao ▴" if show else "Hiện chức năng nâng cao ▾"))
        var_p2_show_advanced.set(show)

    btn_p2_adv = ttk.Button(
        p2_fr,
        text="Hiện chức năng nâng cao ▾",
        command=lambda: _set_p2_advanced_visible(not bool(var_p2_show_advanced.get())),
    )
    btn_p2_adv.grid(row=8, column=2, sticky="e", pady=(6, 0))

    fr_remote_shell = ttk.Frame(p2_fr)
    fr_remote_shell.grid(row=9, column=0, columnspan=3, sticky="ew", pady=(4, 2))
    fr_remote_shell.columnconfigure(0, weight=1)
    fr_remote_hdr = ttk.Frame(fr_remote_shell)
    fr_remote_hdr.grid(row=0, column=0, sticky="ew")
    ttk.Label(
        fr_remote_hdr,
        text="Tìm & tải nhạc từ mạng (Openverse, …) → stock_audio. Thường chỉ cần tab «Âm thanh có sẵn» bên trái.",
        foreground="#555",
        font=("Segoe UI", 8),
        wraplength=420,
        justify=tk.LEFT,
    ).pack(anchor="w", fill=tk.X, expand=True)
    _remote_panel_visible = {"v": False}

    _topic_query_map = dict(FREE_AUDIO_TOPIC_QUERIES)
    remote_hits_mem: list[Any] = []
    _r_cfg0 = load_remote_audio_config(mm._paths)
    var_remote_source = tk.StringVar(value="Openverse (CC — không cần khóa)")
    var_remote_topic = tk.StringVar(value=FREE_AUDIO_TOPIC_QUERIES[0][0])
    var_remote_query = tk.StringVar(value="")
    var_freesound_key = tk.StringVar(value=str(_r_cfg0.get("freesound_api_key") or ""))
    var_jamendo_client_id = tk.StringVar(value=str(_r_cfg0.get("jamendo_client_id") or ""))
    var_remote_auto_dl = tk.BooleanVar(value=bool(_r_cfg0.get("auto_download_to_stock")))
    var_remote_auto_max = tk.StringVar(value=str(int(_r_cfg0.get("auto_download_max") or 5)))
    var_bg_fill = tk.BooleanVar(value=bool(_r_cfg0.get("background_fill_enabled", False)))
    var_bg_max = tk.StringVar(value=str(int(_r_cfg0.get("background_fill_max") or 8)))
    var_bg_iv = tk.StringVar(value=str(int(_r_cfg0.get("background_fill_interval_minutes") or 0)))
    bg_timer_ref: dict[str, Any] = {"id": None}
    ve_bg_fill_launch_ref: dict[str, bool] = {"armed": False}

    def _persist_remote_cfg(_e: Any = None) -> None:
        cfg = load_remote_audio_config(mm._paths)
        cfg["freesound_api_key"] = str(var_freesound_key.get()).strip()
        cfg["jamendo_client_id"] = str(var_jamendo_client_id.get()).strip()
        cfg["auto_download_to_stock"] = bool(var_remote_auto_dl.get())
        try:
            m = int(str(var_remote_auto_max.get()).strip() or "5")
        except ValueError:
            m = 5
        cfg["auto_download_max"] = max(1, min(30, m))
        var_remote_auto_max.set(str(cfg["auto_download_max"]))
        cfg["background_fill_enabled"] = bool(var_bg_fill.get())
        try:
            cfg["background_fill_max"] = max(1, min(25, int(str(var_bg_max.get()).strip() or "8")))
        except ValueError:
            cfg["background_fill_max"] = 8
        try:
            cfg["background_fill_interval_minutes"] = max(
                0, min(1440, int(str(var_bg_iv.get()).strip() or "0"))
            )
        except ValueError:
            cfg["background_fill_interval_minutes"] = 0
        var_bg_max.set(str(cfg["background_fill_max"]))
        var_bg_iv.set(str(cfg["background_fill_interval_minutes"]))
        save_remote_audio_config(cfg, mm._paths)
        if bg_timer_ref.get("id") is not None:
            if not cfg.get("background_fill_enabled") or int(cfg.get("background_fill_interval_minutes") or 0) <= 0:
                try:
                    root.after_cancel(bg_timer_ref["id"])
                except tk.TclError:
                    pass
                bg_timer_ref["id"] = None

    lf_remote = ttk.LabelFrame(
        fr_remote_shell,
        text="Kho âm thanh miễn phí (nâng cao)",
        padding=6,
    )
    lf_remote.grid(row=1, column=0, sticky="ew", pady=(4, 0))
    lf_remote.columnconfigure(0, weight=1)

    def _toggle_remote_audio_panel() -> None:
        _remote_panel_visible["v"] = not bool(_remote_panel_visible["v"])
        if _remote_panel_visible["v"]:
            lf_remote.grid(row=1, column=0, sticky="ew", pady=(4, 0))
            btn_toggle_remote.configure(text="Ẩn kho tìm nhạc từ mạng ▴")
        else:
            lf_remote.grid_remove()
            btn_toggle_remote.configure(text="Hiện kho tìm nhạc từ mạng ▾")

    btn_toggle_remote = ttk.Button(fr_remote_hdr, text="Hiện kho tìm nhạc từ mạng ▾", command=_toggle_remote_audio_panel)
    btn_toggle_remote.pack(anchor="w", pady=(4, 0))
    lf_remote.grid_remove()
    ttk.Label(
        lf_remote,
        text=(
            "Openverse: tổng hợp CC. Wikimedia Commons: file âm thanh (api, không cần khóa). "
            "Jamendo: nhạc CC — cần client_id (devportal.jamendo.com). "
            "Freesound: API key (freesound.org/apiv2/apply). "
            "Tuân thủ giấy phép / ghi nguồn từng bản ghi."
        ),
        foreground="#555",
        font=("Segoe UI", 8),
        wraplength=440,
    ).pack(anchor="w")
    r1 = ttk.Frame(lf_remote)
    r1.pack(fill=tk.X, pady=(4, 0))
    ttk.Label(r1, text="Nguồn tìm kiếm:").pack(side=tk.LEFT)
    cb_remote_src = ttk.Combobox(
        r1,
        textvariable=var_remote_source,
        values=(
            "Openverse (CC — không cần khóa)",
            "Wikimedia Commons (âm thanh)",
            "Jamendo (client_id)",
            "Freesound (API key)",
        ),
        width=34,
        state="readonly",
    )
    cb_remote_src.pack(side=tk.LEFT, padx=(6, 0))
    r1b = ttk.Frame(lf_remote)
    r1b.pack(fill=tk.X, pady=(4, 0))
    ttk.Label(r1b, text="Freesound API key:").pack(side=tk.LEFT)
    ent_fs = ttk.Entry(r1b, textvariable=var_freesound_key, width=28, show="*")

    def save_remote_keys() -> None:
        _persist_remote_cfg()
        notify("Đã lưu Freesound / Jamendo và tùy chọn tự động tải.")

    ent_fs.pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(r1b, text="Lưu khóa", command=save_remote_keys).pack(side=tk.LEFT, padx=(4, 0))
    r1c = ttk.Frame(lf_remote)
    r1c.pack(fill=tk.X, pady=(4, 0))
    ttk.Label(r1c, text="Jamendo client_id:").pack(side=tk.LEFT)
    ent_jamendo = ttk.Entry(r1c, textvariable=var_jamendo_client_id, width=28)
    ent_jamendo.pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(r1c, text="Lưu client_id", command=save_remote_keys).pack(side=tk.LEFT, padx=(4, 0))
    r2 = ttk.Frame(lf_remote)
    r2.pack(fill=tk.X, pady=(4, 0))
    ttk.Label(r2, text="Chủ đề:").pack(side=tk.LEFT)
    cb_topic = ttk.Combobox(
        r2,
        textvariable=var_remote_topic,
        values=[t[0] for t in FREE_AUDIO_TOPIC_QUERIES],
        width=32,
        state="readonly",
    )
    cb_topic.pack(side=tk.LEFT, padx=(6, 0))
    ttk.Label(r2, text="Từ khóa riêng:").pack(side=tk.LEFT, padx=(8, 0))
    ttk.Entry(r2, textvariable=var_remote_query, width=20).pack(side=tk.LEFT, padx=(4, 0))
    r2b = ttk.Frame(lf_remote)
    r2b.pack(fill=tk.X, pady=(2, 0))
    lbl_remote_status = ttk.Label(r2b, text="", foreground="gray", font=("Segoe UI", 8))
    lbl_remote_status.pack(anchor="w")

    r2bg = ttk.Frame(lf_remote)
    r2bg.pack(fill=tk.X, pady=(4, 0))
    ttk.Checkbutton(
        r2bg,
        text="Tự động làm đầy kho (không cần bấm Tìm)",
        variable=var_bg_fill,
        command=_persist_remote_cfg,
    ).pack(side=tk.LEFT)
    ttk.Label(r2bg, text="Tối đa/lần:").pack(side=tk.LEFT, padx=(8, 0))
    sp_bg_max = ttk.Spinbox(
        r2bg, from_=1, to=25, width=3, textvariable=var_bg_max, command=_persist_remote_cfg
    )
    sp_bg_max.pack(side=tk.LEFT, padx=(4, 0))
    sp_bg_max.bind("<FocusOut>", _persist_remote_cfg)
    ttk.Label(r2bg, text="Lặp (phút, 0=một lần khi mở tab):").pack(side=tk.LEFT, padx=(8, 0))
    sp_bg_iv = ttk.Spinbox(
        r2bg, from_=0, to=1440, width=5, textvariable=var_bg_iv, command=_persist_remote_cfg
    )
    sp_bg_iv.pack(side=tk.LEFT, padx=(4, 0))
    sp_bg_iv.bind("<FocusOut>", _persist_remote_cfg)

    r2a = ttk.Frame(lf_remote)
    r2a.pack(fill=tk.X, pady=(4, 0))
    ttk.Checkbutton(
        r2a,
        text="Tự động tải vào kho sau khi tìm (bỏ qua bản không tải được)",
        variable=var_remote_auto_dl,
        command=_persist_remote_cfg,
    ).pack(side=tk.LEFT)
    ttk.Label(r2a, text="Mục tiêu (file OK):").pack(side=tk.LEFT, padx=(10, 0))
    sp_auto_max = ttk.Spinbox(r2a, from_=1, to=30, width=4, textvariable=var_remote_auto_max, command=_persist_remote_cfg)
    sp_auto_max.pack(side=tk.LEFT, padx=(4, 0))
    sp_auto_max.bind("<FocusOut>", _persist_remote_cfg)
    ttk.Label(
        r2a,
        text="(1–30, lưu trong cấu hình)",
        foreground="#888",
        font=("Segoe UI", 8),
    ).pack(side=tk.LEFT, padx=(6, 0))

    f_remote_lb = ttk.Frame(lf_remote)
    f_remote_lb.pack(fill=tk.BOTH, expand=True, pady=(4, 0))
    sb_remote = ttk.Scrollbar(f_remote_lb)
    lb_remote = tk.Listbox(f_remote_lb, height=7, width=68, yscrollcommand=sb_remote.set)
    sb_remote.config(command=lb_remote.yview)
    lb_remote.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    sb_remote.pack(side=tk.RIGHT, fill=tk.Y)
    r3 = ttk.Frame(lf_remote)
    r3.pack(fill=tk.X, pady=(4, 0))

    _AUTO_DL_MAX_ATTEMPTS = 80

    def _start_background_fill_job() -> None:
        cfg_chk = load_remote_audio_config(mm._paths)
        if not cfg_chk.get("background_fill_enabled"):
            return
        try:
            cap = int(cfg_chk.get("background_fill_max") or 8)
        except (TypeError, ValueError):
            cap = 8
        cap = max(1, min(25, cap))
        key = str(cfg_chk.get("freesound_api_key") or "").strip()
        jam = str(cfg_chk.get("jamendo_client_id") or "").strip()

        def work() -> None:
            _schedule_on_main_thread(
                lambda: lbl_remote_status.configure(text="Đang tự động làm đầy kho nền…")
            )
            q = take_next_background_fill_topic(mm._paths)
            hits = gather_background_fill_hits(q, freesound_api_key=key, jamendo_client_id=jam)
            total_ok = 0
            for i, hit in enumerate(hits[:_AUTO_DL_MAX_ATTEMPTS], start=1):
                if total_ok >= cap:
                    break
                try:
                    download_hit_to_stock(hit, freesound_api_key=key, paths=mm._paths)
                    total_ok += 1
                except Exception:
                    pass
                tot = total_ok
                _schedule_on_main_thread(
                    lambda tot=tot, i=i, cap=cap: lbl_remote_status.configure(
                        text=f"Làm đầy kho nền: {tot}/{cap} OK — đã thử {i} URL"
                    )
                )

            def finish() -> None:
                lbl_remote_status.configure(text="")
                refresh_stock_audio_box()
                if total_ok > 0:
                    notify(f"Tự động làm đầy kho: đã tải thêm {total_ok} file (chủ đề luân phiên).")
                cfg2 = load_remote_audio_config(mm._paths)
                iv = int(cfg2.get("background_fill_interval_minutes") or 0)
                if iv > 0 and bool(cfg2.get("background_fill_enabled")):
                    if bg_timer_ref["id"] is not None:
                        try:
                            root.after_cancel(bg_timer_ref["id"])
                        except tk.TclError:
                            pass
                        bg_timer_ref["id"] = None
                    bg_timer_ref["id"] = root.after(iv * 60_000, _start_background_fill_job)

            _schedule_on_main_thread(finish)

        threading.Thread(target=work, daemon=True).start()

    def _start_auto_download_batch(hits: list[Any], target_ok: int) -> None:
        if not hits or target_ok <= 0:
            return
        key = str(var_freesound_key.get()).strip()
        to_try = hits[: _AUTO_DL_MAX_ATTEMPTS]
        n_try = len(to_try)

        def work() -> None:
            ok = 0
            last_err: Exception | None = None
            for i, hit in enumerate(to_try, start=1):
                if ok >= target_ok:
                    break
                try:
                    download_hit_to_stock(hit, freesound_api_key=key, paths=mm._paths)
                    ok += 1
                except Exception as e:
                    last_err = e
                _schedule_on_main_thread(
                    lambda o=ok, ii=i: lbl_remote_status.configure(
                        text=f"Tự động tải vào kho: {o}/{target_ok} OK — đã thử {ii}/{n_try}"
                    )
                )

            def finish() -> None:
                lbl_remote_status.configure(text="")
                refresh_stock_audio_box()
                if ok == 0 and last_err is not None:
                    messagebox.showerror("Kho âm thanh", f"Tự động tải thất bại: {last_err}")
                elif ok < target_ok:
                    notify(
                        f"Đã tải được {ok}/{target_ok} file vào kho (đã thử tới {n_try} kết quả; "
                        "còn lại không tải được hoặc hết danh sách)."
                    )
                else:
                    notify(f"Đã tự động tải {ok} file vào kho stock_audio.")

            _schedule_on_main_thread(finish)

        threading.Thread(target=work, daemon=True).start()

    def do_remote_search() -> None:
        custom = str(var_remote_query.get()).strip()
        if custom:
            q = custom
        else:
            q = _topic_query_map.get(str(var_remote_topic.get()), FREE_AUDIO_TOPIC_QUERIES[0][1])
        src = var_remote_source.get()

        def work() -> None:
            err: Exception | None = None
            hits: list[Any] = []
            try:
                if src.startswith("Freesound"):
                    key = str(var_freesound_key.get()).strip()
                    if not key:
                        raise ValueError("Nhập Freesound API key và bấm «Lưu khóa».")
                    hits = search_freesound(q, key)
                elif src.startswith("Jamendo"):
                    jc = str(var_jamendo_client_id.get()).strip()
                    if not jc:
                        raise ValueError(
                            "Jamendo cần client_id — tạo app miễn phí tại devportal.jamendo.com, "
                            "dán vào ô «Jamendo client_id» rồi bấm «Lưu client_id»."
                        )
                    hits = search_jamendo(q, jc)
                elif src.startswith("Wikimedia"):
                    hits = search_commons_audio(q)
                else:
                    hits = search_openverse(q)
            except Exception as e:
                err = e

            def done() -> None:
                nonlocal remote_hits_mem
                lbl_remote_status.configure(text="")
                if err is not None:
                    messagebox.showerror("Kho âm thanh", str(err))
                    return
                remote_hits_mem = hits
                lb_remote.delete(0, tk.END)
                for h in hits:
                    ds = ""
                    if h.duration_sec is not None:
                        ds = f"{float(h.duration_sec):.0f}s"
                    lb_remote.insert(
                        tk.END,
                        f"{str(h.title)[:52]} | {h.provider} | {ds} | {h.license_}",
                    )
                notify(f"Tìm thấy {len(hits)} kết quả.")
                if var_remote_auto_dl.get() and hits:
                    try:
                        nmax = int(str(var_remote_auto_max.get()).strip() or "5")
                    except ValueError:
                        nmax = 5
                    nmax = max(1, min(30, nmax))
                    _start_auto_download_batch(hits, nmax)

            _schedule_on_main_thread(done)

        lbl_remote_status.configure(text="Đang tìm…")
        threading.Thread(target=work, daemon=True).start()

    def download_remote_selected() -> None:
        sel = lb_remote.curselection()
        if not sel:
            messagebox.showinfo("Kho âm thanh", "Chọn một dòng kết quả rồi bấm Tải về.")
            return
        hit = remote_hits_mem[int(sel[0])]

        def work() -> None:
            err: Exception | None = None
            try:
                download_hit_to_stock(hit, freesound_api_key=str(var_freesound_key.get()).strip(), paths=mm._paths)
            except Exception as e:
                err = e

            def done() -> None:
                lbl_remote_status.configure(text="")
                if err is not None:
                    messagebox.showerror("Kho âm thanh", str(err))
                    return
                refresh_stock_audio_box()
                notify("Đã tải file vào thư mục stock_audio — chọn ở danh sách trên nếu cần thêm vào Media/BGM.")

            _schedule_on_main_thread(done)

        lbl_remote_status.configure(text="Đang tải…")
        threading.Thread(target=work, daemon=True).start()

    ttk.Button(r3, text="Tìm kiếm", command=do_remote_search).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(r3, text="Tải về stock_audio", command=download_remote_selected).pack(side=tk.LEFT)

    def schedule_ve_background_audio_fill() -> None:
        """Gọi khi người dùng mở tab Video Editor — không tự chạy tải/API nền (bật «Tự động làm đầy kho» + lưu cấu hình nếu cần)."""
        ve_bg_fill_launch_ref["armed"] = True

    stock_audio_refresh_ref["fn"] = refresh_stock_audio_box
    refresh_stock_audio_box()

    ttk.Separator(p2_fr, orient=tk.HORIZONTAL).grid(row=10, column=0, columnspan=3, sticky="ew", pady=8)

    var_transition_dur = tk.StringVar(value="0.5")
    var_transition_type = tk.StringVar(value="crossfade")

    def add_transition_ui() -> None:
        if not project or not selected_clip_id:
            messagebox.showinfo("Transition", "Chọn clip video; transition áp với clip kế tiếp.")
            return
        vclips = []
        for tr in project.get("tracks") or []:
            if isinstance(tr, dict) and str(tr.get("type")) == "video":
                for c in tr.get("clips") or []:
                    if isinstance(c, dict) and str(c.get("type")) == "video":
                        vclips.append(c)
        vclips.sort(key=lambda x: float(x.get("timeline_start") or 0))
        ids = [str(c.get("id")) for c in vclips]
        if selected_clip_id not in ids:
            return
        i = ids.index(selected_clip_id)
        if i >= len(ids) - 1:
            messagebox.showinfo("Transition", "Không có clip video sau clip này.")
            return
        try:
            dur = float(str(var_transition_dur.get()).strip() or "0.5")
        except ValueError:
            messagebox.showerror("Transition", "Độ dài transition không hợp lệ.")
            return
        dur = max(0.1, min(5.0, dur))
        typ = str(var_transition_type.get() or "").strip()
        if not typ:
            messagebox.showerror("Transition", "Chọn loại transition.")
            return
        try:
            tr_mgr.add_transition(project, ids[i], ids[i + 1], typ.strip(), float(dur))
            pm.save_project(project)
            notify("Đã thêm transition (xuất với xfade/acrossfade).")
        except Exception as e:
            messagebox.showerror("Transition", str(e))

    fr_transition = ttk.Frame(p2_fr)
    fr_transition.grid(row=11, column=0, columnspan=3, sticky="w")
    ttk.Label(fr_transition, text="Transition").pack(side=tk.LEFT)
    ttk.Combobox(
        fr_transition,
        textvariable=var_transition_type,
        values=("crossfade", "fadeblack", "fadewhite", "slide_left", "slide_right", "wipeleft", "wiperight"),
        width=14,
    ).pack(side=tk.LEFT, padx=(6, 8))
    ttk.Label(fr_transition, text="Độ dài (s)").pack(side=tk.LEFT)
    ttk.Entry(fr_transition, textvariable=var_transition_dur, width=7).pack(side=tk.LEFT, padx=(6, 8))
    ttk.Button(fr_transition, text="Chuyển cảnh sang clip video kế", command=add_transition_ui).pack(side=tk.LEFT)

    def gen_waveform() -> None:
        if not project:
            return
        if _active_media_kind() == "stock":
            messagebox.showinfo("Waveform", "Chọn media trong tab Video hoặc File nhạc (dự án).")
            return
        mid = _selected_media_id()
        if not mid:
            messagebox.showinfo("Waveform", "Chọn một dòng trong bảng Media trước.")
            return
        media = _find_media(mid)
        if not media:
            return
        mt = str(media.get("type") or "")
        if mt == "image":
            messagebox.showinfo(
                "Waveform",
                "Waveform là ảnh sóng âm thanh — chỉ dùng cho file audio hoặc video có tiếng.\n"
                "Logo/ảnh PNG không có luồng audio nên không tạo được.",
            )
            return
        if mt not in ("audio", "video"):
            messagebox.showinfo("Waveform", "Chọn media loại audio hoặc video trong bảng Media.")
            return
        p = mm.resolve_media_path_on_disk(media)
        if not p:
            return
        ffmpeg_bin = resolve_ffmpeg_executable()
        if not ffmpeg_bin:
            messagebox.showerror("Waveform", "Không có ffmpeg.")
            return
        out = ensure_video_editor_layout()["waveforms"] / f"{mid}_wave.png"
        try:
            wf_gen.generate_waveform(str(p), str(out), ffmpeg_bin=ffmpeg_bin)
            notify(f"Waveform: {out.name}")
        except Exception as e:
            messagebox.showerror("Waveform", str(e))

    def gen_proxy() -> None:
        if not project:
            return
        if _active_media_kind() == "stock":
            messagebox.showinfo("Proxy", "Chọn video trong tab Video (dự án).")
            return
        mid = _selected_media_id()
        if not mid:
            messagebox.showinfo("Proxy", "Chọn media video.")
            return
        media = _find_media(mid)
        if not media or str(media.get("type")) != "video":
            messagebox.showinfo("Proxy", "Chọn media video.")
            return
        ffmpeg_bin = resolve_ffmpeg_executable()
        if not ffmpeg_bin:
            return
        try:
            mm.generate_proxy(media, ffmpeg_bin=ffmpeg_bin)
            pm.save_project(project)
            notify(f"Đã tạo proxy: {media.get('proxy_path')}")
        except Exception as e:
            messagebox.showerror("Proxy", str(e))

    ttk.Button(p2_fr, text="Waveform ảnh", command=gen_waveform).grid(row=12, column=0, sticky="w", pady=(8, 0))
    ttk.Button(p2_fr, text="Tạo proxy preview", command=gen_proxy).grid(row=12, column=1, sticky="w", padx=4, pady=(8, 0))

    var_template_name = tk.StringVar(value="")
    var_template_pick = tk.StringVar(value="")
    _template_pick_to_id: dict[str, str] = {}

    def _refresh_template_choices() -> None:
        nonlocal _template_pick_to_id
        items = tmplate_mgr.list_templates() or []
        _template_pick_to_id = {}
        picks: list[str] = []
        for t in items:
            if not isinstance(t, dict):
                continue
            tid = str(t.get("id") or "").strip()
            if not tid:
                continue
            name = str(t.get("name") or "").strip()
            label = f"{tid} | {name}" if name else tid
            picks.append(label)
            _template_pick_to_id[label] = tid
        cb_template_pick.configure(values=picks)
        if picks and var_template_pick.get() not in picks:
            var_template_pick.set(picks[0])
        elif not picks:
            var_template_pick.set("")

    def save_tpl() -> None:
        if not project:
            return
        name = str(var_template_name.get() or "").strip()
        if not name:
            messagebox.showinfo("Template", "Nhập tên template trước khi lưu.")
            return
        try:
            tmplate_mgr.save_template(project, name)
            pm.save_project(project)
            notify("Đã lưu template.")
            _refresh_template_choices()
        except Exception as e:
            messagebox.showerror("Template", str(e))

    def apply_tpl() -> None:
        if not project:
            return
        if not _template_pick_to_id:
            _refresh_template_choices()
        if not _template_pick_to_id:
            messagebox.showinfo("Template", "Chưa có template.")
            return
        pick = str(var_template_pick.get() or "").strip()
        tid = _template_pick_to_id.get(pick, "")
        if not tid:
            messagebox.showinfo("Template", "Chọn template trong danh sách.")
            return
        try:
            tmplate_mgr.apply_template(project, tid.strip())
            pm.save_project(project)
            refresh_all()
            notify("Đã áp template — kiểm tra timeline/media.")
        except Exception as e:
            messagebox.showerror("Template", str(e))

    fr_template = ttk.Frame(p2_fr)
    fr_template.grid(row=13, column=0, columnspan=3, sticky="w", pady=(6, 0))
    ttk.Label(fr_template, text="Tên template").pack(side=tk.LEFT)
    ttk.Entry(fr_template, textvariable=var_template_name, width=18).pack(side=tk.LEFT, padx=(6, 8))
    ttk.Button(fr_template, text="Lưu template", command=save_tpl).pack(side=tk.LEFT)
    ttk.Label(fr_template, text="Template đã lưu").pack(side=tk.LEFT, padx=(12, 6))
    cb_template_pick = ttk.Combobox(fr_template, textvariable=var_template_pick, width=30, state="readonly")
    cb_template_pick.pack(side=tk.LEFT)
    ttk.Button(fr_template, text="Nạp lại", command=_refresh_template_choices).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(fr_template, text="Áp template", command=apply_tpl).pack(side=tk.LEFT, padx=(6, 0))
    _refresh_template_choices()

    ttk.Separator(p2_fr, orient=tk.HORIZONTAL).grid(row=14, column=0, columnspan=3, sticky="ew", pady=8)
    ttk.Label(p2_fr, text="Canvas dự án (khung xuất):").grid(row=15, column=0, sticky="w")
    ASPECT_LABELS = (
        "9:16 (1080×1920) Reels / Shorts / TikTok",
        "16:9 (1920×1080) YouTube",
        "1:1 (1080×1080) Vuông",
        "4:5 (1080×1350) Feed",
    )
    var_aspect_pick = tk.StringVar(value=ASPECT_LABELS[0])

    def _sync_aspect_combo_from_project() -> None:
        if not project:
            return
        aw = int(project.get("width") or 1080)
        ah = int(project.get("height") or 1920)
        pick = ASPECT_LABELS[0]
        if aw == 1920 and ah == 1080:
            pick = ASPECT_LABELS[1]
        elif aw == 1080 and ah == 1080:
            pick = ASPECT_LABELS[2]
        elif aw == 1080 and ah == 1350:
            pick = ASPECT_LABELS[3]
        elif aw == 1080 and ah == 1920:
            pick = ASPECT_LABELS[0]
        var_aspect_pick.set(pick)

    def apply_aspect_pick(_e: Any = None) -> None:
        if not project:
            return
        m = {
            ASPECT_LABELS[0]: (1080, 1920, "9:16"),
            ASPECT_LABELS[1]: (1920, 1080, "16:9"),
            ASPECT_LABELS[2]: (1080, 1080, "1:1"),
            ASPECT_LABELS[3]: (1080, 1350, "4:5"),
        }
        s = var_aspect_pick.get()
        if s not in m:
            return
        ww, hh, ar = m[s]
        project["width"], project["height"], project["aspect_ratio"] = ww, hh, ar
        merge_phase2_defaults(project)
        pm.save_project(project)
        notify(f"Canvas dự án: {ww}×{hh} ({ar}).")

    cb_aspect = ttk.Combobox(
        p2_fr,
        textvariable=var_aspect_pick,
        values=list(ASPECT_LABELS),
        width=34,
        state="readonly",
    )
    cb_aspect.grid(row=15, column=1, columnspan=2, sticky="w")
    cb_aspect.bind("<<ComboboxSelected>>", apply_aspect_pick)

    ttk.Label(p2_fr, text="Âm thanh khi xuất:").grid(row=16, column=0, sticky="w", pady=(6, 0))
    var_audio_mode = tk.StringVar(value="mix")

    def apply_audio_mode(_e: Any = None) -> None:
        if not project:
            return
        project["audio_mode"] = var_audio_mode.get().strip().lower()
        merge_phase2_defaults(project)
        pm.save_project(project)
        notify(f"Chế độ audio: {project['audio_mode']} — «replace» = tắt tiếng timeline gốc (chỉ nghe BGM nếu đã thêm).")

    cb_audio_mode = ttk.Combobox(
        p2_fr,
        textvariable=var_audio_mode,
        values=["mix", "replace"],
        width=10,
        state="readonly",
    )
    cb_audio_mode.grid(row=16, column=1, sticky="w", pady=(6, 0))
    cb_audio_mode.bind("<<ComboboxSelected>>", apply_audio_mode)
    ttk.Label(
        p2_fr,
        text="mix = giữ tiếng clip + BGM; replace = tắt tiếng gốc (dùng với nhạc nền).",
        foreground="#555",
        font=("Segoe UI", 8),
        wraplength=320,
    ).grid(row=17, column=0, columnspan=3, sticky="w")

    def sync_p2_ui() -> None:
        if not project:
            return
        merge_phase2_defaults(project)
        var_audio_mode.set(str(project.get("audio_mode") or "mix"))
        _sync_aspect_combo_from_project()

    sync_p2_ui_ref["fn"] = sync_p2_ui
    root.after(0, lambda: _set_p2_advanced_visible(False))

    exp_fr = ttk.LabelFrame(right, text="Xuất video (Export)", padding=4)
    right.add(exp_fr, weight=1)
    try:
        right.paneconfigure(insp_fr, minsize=150)
        right.paneconfigure(exp_fr, minsize=110)
    except tk.TclError:
        pass
    exp_inner = _pack_scrollable_vertical(exp_fr)
    exp_top_actions = ttk.Frame(exp_inner)
    exp_top_actions.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 4))
    btn_toggle_insp = ttk.Button(exp_top_actions, text="Ẩn phần Chỉnh sửa để tập trung Xuất", command=_toggle_inspector_for_export)
    btn_toggle_insp.pack(side=tk.LEFT)

    def _sync_export_toggle_btn_text() -> None:
        if bool(_exp_layout_state.get("inspector_hidden")):
            btn_toggle_insp.configure(text="Hiện lại phần Chỉnh sửa")
        else:
            btn_toggle_insp.configure(text="Ẩn phần Chỉnh sửa để tập trung Xuất")

    _sync_export_toggle_btn_text()
    ttk.Label(
        exp_inner,
        text=(
            "Xuất MP4 theo cấu hình bạn chọn: tỷ lệ khung hình, độ phân giải và FPS.\n"
            "Luồng này xuất toàn timeline hiện tại thành 1 file MP4."
        ),
        foreground="#555",
        font=("Segoe UI", 9),
        wraplength=620,
        justify="left",
    ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 8))

    fr_export_cfg = ttk.LabelFrame(exp_inner, text="Cấu hình xuất", padding=6)
    fr_export_cfg.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 6))
    fr_export_cfg.columnconfigure(1, weight=1)
    fr_export_cfg.columnconfigure(3, weight=1)

    var_exp_ratio = tk.StringVar(value="Giữ theo project")
    var_exp_quality = tk.StringVar(value="1080")
    var_exp_width = tk.StringVar(value="")
    var_exp_height = tk.StringVar(value="")
    var_exp_fps = tk.StringVar(value="")
    var_exp_status = tk.StringVar(value="Sẵn sàng xuất.")

    ratio_map: dict[str, tuple[int, int] | None] = {
        "Giữ theo project": None,
        "9:16": (9, 16),
        "16:9": (16, 9),
        "1:1": (1, 1),
        "4:5": (4, 5),
        "3:4": (3, 4),
        "21:9": (21, 9),
    }

    def _even_int(v: int) -> int:
        x = max(2, int(v))
        return x - (x % 2)

    def _sync_default_export_fields() -> None:
        if not project:
            return
        try:
            pw = int(project.get("width") or 1080)
            ph = int(project.get("height") or 1920)
            pfps = int(project.get("fps") or 30)
        except (TypeError, ValueError):
            pw, ph, pfps = 1080, 1920, 30
        var_exp_width.set(str(_even_int(pw)))
        var_exp_height.set(str(_even_int(ph)))
        var_exp_fps.set(str(max(1, pfps)))

    def _apply_ratio_quality_to_wh(_e: Any = None) -> None:
        if not project:
            return
        ratio_sel = str(var_exp_ratio.get() or "").strip()
        ratio = ratio_map.get(ratio_sel)
        if ratio is None:
            _sync_default_export_fields()
            return
        try:
            target_long = int(float(str(var_exp_quality.get() or "1080").strip()))
        except (TypeError, ValueError):
            target_long = 1080
        target_long = max(240, target_long)
        rw, rh = ratio
        max_side = max(rw, rh)
        scale = float(target_long) / float(max_side)
        w = _even_int(int(round(rw * scale)))
        h = _even_int(int(round(rh * scale)))
        var_exp_width.set(str(w))
        var_exp_height.set(str(h))

    ttk.Label(fr_export_cfg, text="Tỷ lệ khung hình").grid(row=0, column=0, sticky="w")
    cb_exp_ratio = ttk.Combobox(
        fr_export_cfg,
        textvariable=var_exp_ratio,
        values=list(ratio_map.keys()),
        state="readonly",
        width=22,
    )
    cb_exp_ratio.grid(row=0, column=1, sticky="ew", padx=(8, 12))
    cb_exp_ratio.bind("<<ComboboxSelected>>", _apply_ratio_quality_to_wh)

    ttk.Label(fr_export_cfg, text="Preset độ phân giải").grid(row=0, column=2, sticky="w")
    cb_exp_quality = ttk.Combobox(
        fr_export_cfg,
        textvariable=var_exp_quality,
        values=["720", "1080", "1440", "2160"],
        state="readonly",
        width=10,
    )
    cb_exp_quality.grid(row=0, column=3, sticky="w")
    cb_exp_quality.bind("<<ComboboxSelected>>", _apply_ratio_quality_to_wh)

    ttk.Label(fr_export_cfg, text="Width").grid(row=1, column=0, sticky="w", pady=(6, 0))
    ttk.Entry(fr_export_cfg, textvariable=var_exp_width, width=12).grid(row=1, column=1, sticky="w", padx=(8, 12), pady=(6, 0))
    ttk.Label(fr_export_cfg, text="Height").grid(row=1, column=2, sticky="w", pady=(6, 0))
    ttk.Entry(fr_export_cfg, textvariable=var_exp_height, width=12).grid(row=1, column=3, sticky="w", pady=(6, 0))

    ttk.Label(fr_export_cfg, text="FPS").grid(row=2, column=0, sticky="w", pady=(6, 0))
    cb_exp_fps = ttk.Combobox(
        fr_export_cfg,
        textvariable=var_exp_fps,
        values=["24", "25", "30", "50", "60"],
        width=10,
    )
    cb_exp_fps.grid(row=2, column=1, sticky="w", padx=(8, 12), pady=(6, 0))
    ttk.Label(
        fr_export_cfg,
        text="Bạn có thể sửa tay Width/Height/FPS trước khi xuất.",
        foreground="#666",
        font=("Segoe UI", 8),
    ).grid(row=2, column=2, columnspan=2, sticky="w", pady=(6, 0))

    fr_export_out = ttk.LabelFrame(exp_inner, text="Thư mục lưu MP4", padding=6)
    fr_export_out.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(0, 6))
    fr_export_out.columnconfigure(1, weight=1)

    var_exp_output_dir = tk.StringVar(value=str(ve_paths["renders"].resolve()))
    var_exp_output_subdir_job = tk.BooleanVar(value=True)

    def _load_export_ui_prefs() -> dict[str, Any]:
        p = video_editor_export_ui_prefs_path()
        if not p.is_file():
            return {}
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            return dict(raw) if isinstance(raw, dict) else {}
        except Exception:
            return {}

    def _save_export_ui_prefs() -> None:
        p = video_editor_export_ui_prefs_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "output_dir": str(var_exp_output_dir.get() or "").strip(),
            "subdir_per_job": bool(var_exp_output_subdir_job.get()),
        }
        try:
            p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        except Exception:
            pass

    def _pick_export_output_dir() -> None:
        cur = str(var_exp_output_dir.get() or "").strip()
        initial = cur if cur and Path(cur).expanduser().is_dir() else str(ve_paths["renders"].resolve())
        picked = filedialog.askdirectory(parent=root, title="Chọn thư mục lưu file MP4 xuất ra", initialdir=initial)
        if not picked:
            return
        var_exp_output_dir.set(str(Path(picked).expanduser().resolve()))
        _save_export_ui_prefs()
        notify(f"Thư mục lưu: {var_exp_output_dir.get()}")

    def _reset_export_output_dir() -> None:
        var_exp_output_dir.set(str(ve_paths["renders"].resolve()))
        _save_export_ui_prefs()
        notify("Đã đặt lại thư mục mặc định (renders).")

    def _safe_job_folder_name(raw: str) -> str:
        s = "".join(c for c in str(raw or "").strip() if c.isalnum() or c in "-_ ")
        s = s.replace(" ", "_").strip("_")
        return (s[:64] or "export")

    def _resolve_export_output_dir(*, for_job_name: str = "") -> Path:
        raw = str(var_exp_output_dir.get() or "").strip()
        base = Path(raw).expanduser() if raw else ve_paths["renders"]
        try:
            base = base.resolve()
        except OSError:
            base = ve_paths["renders"].resolve()
        if not base.is_dir():
            base = ve_paths["renders"].resolve()
        base.mkdir(parents=True, exist_ok=True)
        if bool(var_exp_output_subdir_job.get()):
            job_raw = str(for_job_name or "").strip()
            if not job_raw:
                job_raw = str(var_exp_saved_job_name.get() or "").strip()
            if not job_raw:
                pid = str((project or {}).get("id") or "export")
                job_raw = f"{pid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            sub = base / _safe_job_folder_name(job_raw)
            sub.mkdir(parents=True, exist_ok=True)
            return sub
        return base

    _exp_prefs = _load_export_ui_prefs()
    if str(_exp_prefs.get("output_dir") or "").strip():
        var_exp_output_dir.set(str(Path(str(_exp_prefs["output_dir"])).expanduser()))
    if "subdir_per_job" in _exp_prefs:
        var_exp_output_subdir_job.set(bool(_exp_prefs.get("subdir_per_job")))

    ttk.Label(fr_export_out, text="Thư mục gốc").grid(row=0, column=0, sticky="w")
    ent_exp_out = ttk.Entry(fr_export_out, textvariable=var_exp_output_dir, state="readonly")
    ent_exp_out.grid(row=0, column=1, sticky="ew", padx=(8, 8))
    row_out_btns = ttk.Frame(fr_export_out)
    row_out_btns.grid(row=0, column=2, sticky="e")
    ttk.Button(row_out_btns, text="Chọn…", command=_pick_export_output_dir, width=8).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(row_out_btns, text="Mặc định", command=_reset_export_output_dir, width=9).pack(side=tk.LEFT)
    ttk.Checkbutton(
        fr_export_out,
        text="Tạo thư mục con theo «Tên job chờ đăng» (mỗi lần xuất 1 folder — dễ quản lý từng job)",
        variable=var_exp_output_subdir_job,
        command=_save_export_ui_prefs,
    ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(6, 0))
    ttk.Label(
        fr_export_out,
        text="File MP4 được ghi trực tiếp vào thư mục bạn chọn (không copy sang renders nếu chọn folder khác).",
        foreground="#666",
        font=("Segoe UI", 8),
        wraplength=620,
        justify="left",
    ).grid(row=2, column=0, columnspan=3, sticky="w", pady=(4, 0))

    fr_export_prog = ttk.LabelFrame(exp_inner, text="Tiến trình", padding=6)
    fr_export_prog.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(0, 6))
    fr_export_prog.columnconfigure(0, weight=1)
    prog_exp = ttk.Progressbar(fr_export_prog, maximum=100)
    prog_exp.grid(row=0, column=0, sticky="ew")
    ttk.Label(fr_export_prog, textvariable=var_exp_status, wraplength=620).grid(row=1, column=0, sticky="w", pady=(4, 0))
    txt_exp_log = tk.Text(fr_export_prog, height=5, wrap="char")
    txt_exp_log.grid(row=2, column=0, sticky="ew", pady=(6, 0))
    txt_exp_log.configure(state="disabled")

    def _append_export_log(msg: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        line = f"[{stamp}] {str(msg or '').strip()}\n"
        try:
            txt_exp_log.configure(state="normal")
            txt_exp_log.insert("end", line)
            txt_exp_log.see("end")
            # Giới hạn dung lượng log UI để không phình RAM khi chạy dài.
            ln = int(txt_exp_log.index("end-1c").split(".")[0])
            if ln > 220:
                txt_exp_log.delete("1.0", f"{ln-200}.0")
            txt_exp_log.configure(state="disabled")
        except Exception:
            pass

    def _set_export_status(msg: str, *, log: bool = False) -> None:
        var_exp_status.set(str(msg or "").strip())
        if log:
            _append_export_log(msg)

    def _queue_export_status(msg: str, *, log: bool = False) -> None:
        _schedule_on_main_thread(lambda m=msg, lg=log: _set_export_status(m, log=lg))

    fr_export_post = ttk.LabelFrame(exp_inner, text="Sau khi xuất", padding=6)
    fr_export_post.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(0, 6))
    fr_export_post.columnconfigure(1, weight=1)
    var_exp_save_pending = tk.BooleanVar(value=True)
    var_exp_open_jobs_after_save = tk.BooleanVar(value=True)
    var_exp_saved_job_name = tk.StringVar(value=f"job_xuat_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    var_exp_per_clip = tk.BooleanVar(value=True)
    var_exp_name_tpl = tk.StringVar(value="{index:02d}_{source_id}_{title}.mp4")
    ttk.Checkbutton(
        fr_export_post,
        text="Lưu thành job chờ đăng (nối metadata nguồn)",
        variable=var_exp_save_pending,
    ).grid(row=0, column=0, columnspan=2, sticky="w")
    ttk.Checkbutton(
        fr_export_post,
        text="Sau khi lưu job, chuyển qua tab «7.Job chờ đăng từ Video Editor»",
        variable=var_exp_open_jobs_after_save,
    ).grid(row=1, column=0, columnspan=2, sticky="w")
    ttk.Label(
        fr_export_post,
        text="Đích đăng (Facebook / TikTok / Page): cấu hình trong popup «Nạp job chờ đăng từ Export» (mở từ tab «7.Job chờ đăng từ Video Editor») — có «Chờ chọn» và «Lưu gợi ý đích».",
        foreground="#555",
        font=("Segoe UI", 9),
        wraplength=620,
        justify="left",
    ).grid(row=2, column=0, columnspan=2, sticky="w", pady=(4, 2))

    ttk.Label(fr_export_post, text="Tên job chờ đăng").grid(row=3, column=0, sticky="w", pady=(4, 0))
    ttk.Entry(fr_export_post, textvariable=var_exp_saved_job_name).grid(row=3, column=1, sticky="ew", padx=(8, 0), pady=(4, 0))
    ttk.Checkbutton(
        fr_export_post,
        text="Xuất mỗi clip video thành 1 file riêng (auto map metadata chuẩn nhất)",
        variable=var_exp_per_clip,
    ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(6, 0))
    ttk.Label(fr_export_post, text="Mẫu tên file nhiều clip").grid(row=5, column=0, sticky="w", pady=(4, 0))
    ttk.Entry(fr_export_post, textvariable=var_exp_name_tpl).grid(row=5, column=1, sticky="ew", padx=(8, 0), pady=(4, 0))

    btns_fr = ttk.Frame(exp_inner)
    btns_fr.grid(row=6, column=0, columnspan=2, sticky="w", pady=(0, 4))
    btn_exp_refs: dict[str, Any] = {"run": None, "stop": None}

    def _sync_export_running_ui() -> None:
        running = int(_export_run_state.get("running") or 0) > 0
        b_run = btn_exp_refs.get("run")
        b_stop = btn_exp_refs.get("stop")
        if b_run is not None:
            try:
                b_run.configure(state=("disabled" if running else "normal"))
            except Exception:
                pass
        if b_stop is not None:
            try:
                b_stop.configure(state=("normal" if running else "disabled"))
            except Exception:
                pass

    def _mark_export_started() -> None:
        _export_run_state["running"] = int(_export_run_state.get("running") or 0) + 1
        _sync_export_running_ui()

    def _mark_export_finished() -> None:
        cur = int(_export_run_state.get("running") or 0)
        _export_run_state["running"] = max(0, cur - 1)
        _sync_export_running_ui()

    def _stop_export_now() -> None:
        if int(_export_run_state.get("running") or 0) <= 0:
            return
        _export_user_abort_ref["v"] = True
        export_worker.cancel_all()
        _set_export_status("Đang gửi yêu cầu dừng export…", log=True)
        notify("Đã gửi yêu cầu dừng export.")

    def _prime_render_for_export() -> None:
        """Dừng preview (worker riêng); giải phóng CPU/RAM trước khi export."""
        try:
            preview_worker.cancel_all()
        except Exception:
            pass
        aid = preview_watchdog_ref.get("after")
        if aid is not None:
            try:
                root.after_cancel(aid)
            except Exception:
                pass
        preview_watchdog_ref["after"] = None
        preview_busy_ref["busy"] = False
        preview_open_after_done_ref["v"] = False
        preview_open_after_done_ref["with"] = "ffplay"
        time.sleep(0.15)
        try:
            preview_worker.clear_cancel()
        except Exception:
            pass
        try:
            export_worker.clear_cancel()
        except Exception:
            pass

    def _build_default_out_path(*, export_dir: Path | None = None) -> str:
        pid = str((project or {}).get("id") or "export")
        safe = "".join(c for c in pid if c.isalnum() or c in "-_")[:48] or "export"
        out_dir = export_dir if export_dir is not None else _resolve_export_output_dir()
        out_dir.mkdir(parents=True, exist_ok=True)
        return str(out_dir / f"{safe}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4")

    def _load_saved_export_jobs() -> list[dict[str, Any]]:
        p = video_editor_schedule_jobs_json_path()
        if not p.is_file():
            return []
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            return [dict(x) for x in raw] if isinstance(raw, list) else []
        except Exception:
            return []

    def _save_saved_export_jobs(rows: list[dict[str, Any]]) -> None:
        p = video_editor_schedule_jobs_json_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _append_saved_export_job(
        job_name: str,
        items: list[dict[str, Any]],
        source_type: str,
        *,
        publish_extra: dict[str, Any] | None = None,
    ) -> str:
        rows = _load_saved_export_jobs()
        jid = f"expjob_{uuid.uuid4().hex[:10]}"
        pipe = dict((project or {}).get("pipeline") or {})
        row_out: dict[str, Any] = {
            "id": jid,
            "job_name": str(job_name or "").strip() or jid,
            "status": "saved",
            "source_type": source_type,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "source_project_id": str((project or {}).get("id") or ""),
            "source_download_job_id": str(pipe.get("source_download_job_id") or ""),
            "source_download_job_label": str(pipe.get("source_download_job_label") or ""),
            "items": list(items),
        }
        if isinstance(publish_extra, dict):
            for k, v in publish_extra.items():
                row_out[str(k)] = v
        rows.append(row_out)
        _save_saved_export_jobs(rows)
        return jid

    def _open_schedule_jobs_tab(saved_export_job_id: str = "") -> None:
        try:
            setattr(root, "_ve_saved_export_job_id", str(saved_export_job_id or "").strip())
        except Exception:
            pass
        try:
            root.event_generate("<<OpenScheduleJobsTab>>", when="tail")
        except Exception:
            pass

    def _pick_primary_source_meta_for_schedule() -> dict[str, Any]:
        if not isinstance(project, dict):
            return {}
        pipe = dict(project.get("pipeline") or {})
        meta_by_id = pipe.get("source_download_video_meta_by_id")
        if not isinstance(meta_by_id, dict):
            meta_by_id = {}
        media_by_id: dict[str, dict[str, Any]] = {}
        for m in project.get("media") or []:
            if not isinstance(m, dict):
                continue
            mid = str(m.get("id") or "").strip()
            if mid:
                media_by_id[mid] = m
        best_ts = 10**15
        best_meta: dict[str, Any] = {}
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict) or str(tr.get("type") or "") != "video":
                continue
            for cl in tr.get("clips") or []:
                if not isinstance(cl, dict):
                    continue
                mid = str(cl.get("media_id") or "").strip()
                m = media_by_id.get(mid) or {}
                src_vid = str(m.get("source_download_video_id") or "").strip()
                meta = meta_by_id.get(src_vid) if src_vid else None
                if not isinstance(meta, dict):
                    st0 = str(m.get("source_title") or "").strip()
                    sd0 = str(m.get("source_description") or "").strip()
                    meta = {
                        "title": _normalize_post_caption_title(st0, description=sd0),
                        "description": sd0,
                        "hashtags": [str(x).strip() for x in (m.get("source_hashtags") or []) if str(x).strip()],
                    }
                ts = float(cl.get("timeline_start") or 0.0)
                if ts < best_ts:
                    best_ts = ts
                    bm = dict(meta)
                    t1 = str(bm.get("title") or "").strip()
                    d1 = str(bm.get("description") or "").strip()
                    bm["title"] = _normalize_post_caption_title(t1, description=d1)
                    best_meta = bm
                    if src_vid:
                        best_meta["source_download_video_id"] = src_vid
        return best_meta

    def _all_video_timeline_rows() -> list[tuple[str, dict[str, Any]]]:
        out: list[tuple[str, dict[str, Any]]] = []
        if not isinstance(project, dict):
            return out
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict) or str(tr.get("type") or "") != "video":
                continue
            for cl in tr.get("clips") or []:
                if not isinstance(cl, dict) or str(cl.get("type") or "") != "video":
                    continue
                cid = str(cl.get("id") or "").strip()
                if cid:
                    out.append((cid, cl))
        out.sort(key=lambda x: float((x[1] or {}).get("timeline_start") or 0.0))
        return out

    def _source_meta_for_media_id(media_id: str) -> dict[str, Any]:
        mid = str(media_id or "").strip()
        if not mid or not isinstance(project, dict):
            return {}
        for m in project.get("media") or []:
            if not isinstance(m, dict):
                continue
            if str(m.get("id") or "").strip() != mid:
                continue
            st = str(m.get("source_title") or m.get("name") or "").strip()
            sd = str(m.get("source_description") or "").strip()
            return {
                "title": _normalize_post_caption_title(st, description=sd),
                "description": sd,
                "hashtags": [str(x).strip() for x in (m.get("source_hashtags") or []) if str(x).strip()],
                "source_download_video_id": str(m.get("source_download_video_id") or "").strip(),
            }
        return {}

    def _build_clip_only_project(src_project: dict[str, Any], clip_id: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        tracks = src_project.get("tracks") or []
        src_video_clip: dict[str, Any] | None = None
        for tr in tracks:
            if not isinstance(tr, dict) or str(tr.get("type") or "") != "video":
                continue
            for cl in tr.get("clips") or []:
                if isinstance(cl, dict) and str(cl.get("id") or "") == clip_id:
                    src_video_clip = cl
                    break
            if src_video_clip is not None:
                break
        if src_video_clip is None:
            return None, {}
        ts = float(src_video_clip.get("timeline_start") or 0.0)
        dur = max(0.1, float(src_video_clip.get("duration") or 0.0))
        te = ts + dur
        out = copy.deepcopy(src_project)
        out_tracks: list[dict[str, Any]] = []
        for tr in out.get("tracks") or []:
            if not isinstance(tr, dict):
                continue
            tr_type = str(tr.get("type") or "")
            new_clips: list[dict[str, Any]] = []
            for cl in tr.get("clips") or []:
                if not isinstance(cl, dict):
                    continue
                cs = float(cl.get("timeline_start") or 0.0)
                cd = max(0.0, float(cl.get("duration") or 0.0))
                ce = cs + cd
                ov_start = max(ts, cs)
                ov_end = min(te, ce)
                if ov_end <= ov_start:
                    continue
                if tr_type == "video" and str(cl.get("id") or "") != clip_id:
                    continue
                ncl = dict(cl)
                old_cs = cs
                ncl["timeline_start"] = max(0.0, ov_start - ts)
                ncl["duration"] = max(0.05, ov_end - ov_start)
                if "source_start" in ncl:
                    try:
                        ss = float(cl.get("source_start") or 0.0)
                        ncl["source_start"] = ss + max(0.0, ov_start - old_cs)
                    except Exception:
                        pass
                if "source_end" in ncl:
                    try:
                        ncl["source_end"] = float(ncl.get("source_start") or 0.0) + float(ncl.get("duration") or 0.0)
                    except Exception:
                        pass
                new_clips.append(ncl)
            ntr = dict(tr)
            ntr["clips"] = new_clips
            out_tracks.append(ntr)
        out["tracks"] = out_tracks
        out["duration"] = dur
        meta = _source_meta_for_media_id(str(src_video_clip.get("media_id") or ""))
        return out, meta

    def _safe_name_piece(s: str, fallback: str = "clip") -> str:
        t = "".join(ch for ch in str(s or "") if ch.isalnum() or ch in "-_ ").strip()
        t = "_".join(t.split())
        return t[:80] or fallback

    def _export_log_ellipsis(s: str, *, max_len: int = 72) -> str:
        """Rút nhãn cho log UI (wrap=word từng làm tràn ngang với tên dài không khoảng trắng)."""
        t = str(s or "").strip()
        m = max(24, int(max_len))
        if len(t) <= m:
            return t
        head = max(8, m // 2 - 2)
        tail = m - head - 1
        return f"{t[:head]}…{t[-tail:]}"

    def _short_export_filename_for_dir(base_dir: Path, filename: str) -> str:
        """Rút tên file: Windows dễ lỗi/treo với basename dài; sau đó còn kiểm tra tổng đường dẫn."""
        raw = str(filename or "").strip().replace("/", "_").replace("\\", "_")
        if not raw.lower().endswith(".mp4"):
            raw += ".mp4"
        try:
            cap_env = str(os.environ.get("TOOLFB_EXPORT_MAX_BASENAME", "") or "").strip()
            cap0 = int(float(cap_env)) if cap_env else (100 if os.name == "nt" else 180)
        except (TypeError, ValueError):
            cap0 = 100 if os.name == "nt" else 180
        cap0 = max(40, min(200, cap0))
        if len(raw) > cap0:
            p = Path(raw)
            ext = p.suffix or ".mp4"
            dig = hashlib.sha1(raw.encode("utf-8", errors="replace")).hexdigest()[:10]
            room = cap0 - len(ext) - len(dig) - 1
            stem = (p.stem or "v")[: max(6, room)]
            raw = f"{stem}_{dig}{ext}"
        try:
            base_s = str(base_dir.resolve())
        except Exception:
            base_s = str(base_dir)
        max_full = 248 if os.name == "nt" else 4096
        budget = max(48, max_full - len(base_s) - 2)
        if len(raw) <= budget:
            return raw
        p2 = Path(raw)
        ext2 = p2.suffix or ".mp4"
        dig2 = hashlib.sha1(raw.encode("utf-8", errors="replace")).hexdigest()[:10]
        room2 = budget - len(ext2) - len(dig2) - 1
        if room2 < 8:
            return f"{dig2}{ext2}"
        return f"{(p2.stem or 'v')[:room2]}_{dig2}{ext2}"

    def _pick_unique_dest_in_dir(base_dir: Path, filename: str) -> Path:
        fn = str(filename or "").strip().replace("/", "_").replace("\\", "_")
        if not fn.lower().endswith(".mp4"):
            fn += ".mp4"
        p = base_dir / fn
        if not p.exists():
            return p
        stem, ext = Path(fn).stem, Path(fn).suffix or ".mp4"
        n = 2
        while True:
            c = base_dir / f"{stem}__{n}{ext}"
            if not c.exists():
                return c
            n += 1

    def _ensure_output_in_renders(src_path: Path, *, export_dir: Path | None = None) -> Path:
        """Giữ file tại thư mục xuất đã chọn; chỉ copy sang ``renders`` nếu xuất vào folder mặc định renders."""
        renders_dir = ve_paths["renders"].resolve()
        try:
            parent = src_path.resolve().parent
        except OSError:
            parent = src_path.parent
        if export_dir is not None:
            try:
                if parent == export_dir.resolve():
                    return src_path
            except OSError:
                pass
        try:
            if parent == renders_dir:
                return src_path
        except Exception:
            pass
        if export_dir is not None:
            try:
                if export_dir.resolve() != renders_dir:
                    return src_path
            except OSError:
                return src_path
        renders_dir.mkdir(parents=True, exist_ok=True)
        base = src_path.stem or "export"
        dst = renders_dir / f"{base}.mp4"
        idx = 2
        while dst.exists():
            dst = renders_dir / f"{base}__{idx}.mp4"
            idx += 1
        shutil.copy2(src_path, dst)
        return dst

    def _run_export_with_custom_settings() -> None:
        nonlocal project
        if not project:
            messagebox.showinfo("Export", "Chưa có project.")
            return
        ffmpeg_bin = resolve_ffmpeg_executable()
        if not ffmpeg_bin:
            messagebox.showerror("Export", "Không tìm thấy ffmpeg.")
            return
        try:
            w = _even_int(int(float(str(var_exp_width.get() or "").strip())))
            h = _even_int(int(float(str(var_exp_height.get() or "").strip())))
            fps = max(1, int(float(str(var_exp_fps.get() or "").strip())))
        except (TypeError, ValueError):
            messagebox.showerror("Export", "Width/Height/FPS không hợp lệ.")
            return
        all_rows = _all_video_timeline_rows()
        if not all_rows:
            messagebox.showinfo("Export", "Timeline chưa có clip video để xuất.")
            return
        per_clip = bool(var_exp_per_clip.get()) and len(all_rows) > 1
        with _export_start_lock:
            if int(_export_run_state.get("running") or 0) > 0:
                notify("Đang có tiến trình export chạy.")
                return
            _mark_export_started()
        _save_export_ui_prefs()
        job_label = str(var_exp_saved_job_name.get() or "").strip()
        export_dir_used = _resolve_export_output_dir(for_job_name=job_label)
        out_input = Path(_build_default_out_path(export_dir=export_dir_used))
        _export_user_abort_ref["v"] = False
        _prime_render_for_export()
        prog_exp.configure(value=0)
        _set_export_status(f"Đang chuẩn bị export… → {export_dir_used}", log=True)
        def work() -> None:
            exports_done: list[Path] = []
            failures: list[str] = []
            exported_meta_rows: list[dict[str, Any]] = []
            export_dir_ref = export_dir_used
            try:
                export_worker.clear_cancel()
            except Exception:
                pass
            _queue_export_status(
                "Xuất MP4: mã hóa full độ dài + độ phân giải dự án (preview «Áp dụng» chỉ ~20s đầu và thu nhỏ khung hình) — có thể chậm hơn nhiều; xem log nhịp tim nếu nghi ngờ treo.",
                log=True,
            )

            def _export_fail_line(label: str, res: dict[str, Any]) -> str:
                msg = str(res.get("error_message") or "Lỗi export").strip()
                lf = str(res.get("log_file") or "").strip()
                if lf:
                    msg = f"{msg}\n(Log FFmpeg: {lf})"
                return f"{label}: {msg}"

            def _ffmpeg_heartbeat_log(prefix: str, out_p: Path, started_monotonic: float) -> None:
                elapsed = int(time.monotonic() - float(started_monotonic))
                sz_b = 0
                try:
                    if out_p.is_file():
                        sz_b = int(out_p.stat().st_size)
                except OSError:
                    pass
                mb = sz_b / (1024 * 1024)
                nm = out_p.name
                line = f"{prefix} — FFmpeg vẫn chạy (~{elapsed}s, file ~{mb:.1f} MB): {nm}"
                _schedule_on_main_thread(lambda m=line: _queue_export_status(m, log=True))

            try:
                if per_clip:
                    if out_input.suffix.lower() == ".mp4":
                        base_dir = out_input.parent
                        name_prefix = _safe_name_piece(out_input.stem, "export")
                    else:
                        base_dir = out_input
                        name_prefix = _safe_name_piece(str((project or {}).get("id") or "export"), "export")
                    base_dir.mkdir(parents=True, exist_ok=True)
                    used_logical: set[str] = set()
                    tpl = str(var_exp_name_tpl.get() or "").strip() or "{index:02d}_{source_id}_{title}.mp4"
                    n = len(all_rows)
                    for idx, (cid, _cl) in enumerate(all_rows, start=1):
                        _queue_export_status(f"Clip {idx}/{n} — chuẩn bị project…", log=True)
                        bproj, src_meta = _build_clip_only_project(project or {}, cid)
                        if not isinstance(bproj, dict):
                            failures.append(f"{cid}: không dựng được clip.")
                            _queue_export_status(f"Clip {idx}/{n} lỗi dựng project ({cid}).", log=True)
                            continue
                        bproj["width"] = w
                        bproj["height"] = h
                        bproj["fps"] = fps
                        sid = _safe_name_piece(str(src_meta.get("source_download_video_id") or ""), "nosource")
                        ttl = _safe_name_piece(str(src_meta.get("title") or f"clip_{idx}"), f"clip_{idx}")
                        try:
                            out_name = tpl.format(index=idx, clip_id=cid, source_id=sid, title=ttl).strip()
                        except Exception:
                            out_name = f"{idx:02d}_{sid}_{ttl}.mp4"
                        if not out_name.lower().endswith(".mp4"):
                            out_name += ".mp4"
                        out_name = f"{name_prefix}_{out_name}"
                        base_stem = Path(out_name).stem
                        ext = Path(out_name).suffix or ".mp4"
                        dedup = 2
                        while out_name.lower() in used_logical:
                            out_name = f"{base_stem}__{dedup}{ext}"
                            dedup += 1
                        used_logical.add(out_name.lower())
                        logical_name = out_name
                        # Một bước: FFmpeg ghi thẳng tên an toàn (ngắn, trùng thì __2…) — tránh temp + rename (dễ lỗi Windows).
                        disk_fn = _short_export_filename_for_dir(base_dir, logical_name)
                        out_file = _pick_unique_dest_in_dir(base_dir, disk_fn)
                        _queue_export_status(
                            f"Clip {idx}/{n} — validate «{out_file.name}» "
                            f"(mẫu «{_export_log_ellipsis(logical_name)}»)…",
                            log=True,
                        )
                        errs = validate_export(
                            bproj,
                            ffmpeg_path=ffmpeg_bin,
                            output_path=str(out_file),
                            media_resolver=mm,
                            require_contiguous_video_timeline=False,
                        )
                        if errs:
                            failures.append(f"{logical_name}: {'; '.join(errs)}")
                            _queue_export_status(f"Clip {idx}/{n} lỗi validate: {logical_name}", log=True)
                            try:
                                if out_file.exists():
                                    out_file.unlink()
                            except Exception:
                                pass
                            continue

                        def on_prog(x: float, *, _i: int = idx, _n: int = n) -> None:
                            xf = max(0.0, min(1.0, float(x)))
                            # FFmpeg thường đạt ~100% theo out_time trước khi mux/ghi xong — tránh nhãn "100%" gây hiểu nhầm.
                            inner_ui = min(0.995, xf)
                            pct_bar = int(inner_ui * 100.0)
                            total_frac = ((_i - 1) + inner_ui) / max(1.0, float(_n))
                            tail = " · đang hoàn tất/ghi MP4…" if xf >= 0.99 else ""

                            def _ui() -> None:
                                prog_exp.configure(value=min(99.0, total_frac * 100.0))
                                _set_export_status(
                                    f"Clip {_i}/{_n} — ~{pct_bar}% clip (~{int(total_frac * 100)}% cả lô){tail}"
                                )

                            _schedule_on_main_thread(_ui)

                        # Mặc định chất lượng theo project (chuẩn, không ép «light export»).
                        # Nếu lỗi/timeout: fallback ultrafast như preview nháp.
                        enc_plans: list[dict[str, Any]] = [
                            {"tag": "chuẩn (chất lượng)", "mp4_faststart": False, "enc": None, "light": False},
                            {
                                "tag": "fallback nhanh",
                                "mp4_faststart": False,
                                "enc": {"preset": "ultrafast", "crf": 34, "threads": 1},
                                "light": True,
                            },
                        ]
                        res: dict[str, Any] = {"ok": False, "error_message": ""}
                        for plan_i, plan in enumerate(enc_plans):
                            if export_worker.is_cancel_requested() or _export_user_abort_ref.get("v"):
                                break
                            if plan_i > 0:
                                _queue_export_status(
                                    f"Clip {idx}/{n} — thử lại ({plan['tag']}) «{logical_name}»",
                                    log=True,
                                )
                                try:
                                    if out_file.exists():
                                        out_file.unlink()
                                except Exception:
                                    pass
                            try:
                                _queue_export_status(
                                    f"Clip {idx}/{n} — build lệnh ({plan['tag']}) "
                                    f"«{_export_log_ellipsis(logical_name)}»…",
                                    log=True,
                                )
                                cmd = builder.build_export_command(
                                    bproj,
                                    str(out_file),
                                    ffmpeg_bin=ffmpeg_bin,
                                    ass_path=None,
                                    mp4_faststart=bool(plan.get("mp4_faststart")),
                                    encoding_overrides=plan.get("enc"),
                                    lightweight_mode_override=plan.get("light"),
                                )
                            except Exception as ex:
                                res = {"ok": False, "error_message": str(ex)}
                                if plan_i == len(enc_plans) - 1:
                                    failures.append(f"{logical_name}: {ex}")
                                    _queue_export_status(f"Clip {idx}/{n} lỗi build command: {logical_name}", log=True)
                                continue

                            _queue_export_status(
                                f"Clip {idx}/{n} — FFmpeg ({plan['tag']}) → {out_file.name}…",
                                log=True,
                            )

                            clip_enc_t0 = time.monotonic()

                            def _hb_clip() -> None:
                                _ffmpeg_heartbeat_log(f"Clip {idx}/{n}", out_file, clip_enc_t0)

                            res = export_worker.render(
                                bproj,
                                str(out_file),
                                cmd,
                                duration_sec=max(0.1, float(bproj.get("duration") or 0.1)),
                                progress_callback=on_prog,
                                wait_heartbeat_callback=_hb_clip,
                            )
                            if res.get("ok"):
                                break

                        if not res.get("ok"):
                            failures.append(_export_fail_line(logical_name, res))
                            _queue_export_status(f"Clip {idx}/{n} lỗi render: {logical_name}", log=True)
                            try:
                                if out_file.exists():
                                    out_file.unlink()
                            except Exception:
                                pass
                            if export_worker.is_cancel_requested() or _export_user_abort_ref.get("v"):
                                break
                            continue
                        if logical_name.lower() != out_file.name.lower():
                            _queue_export_status(
                                f"Clip {idx}/{n} — file «{out_file.name}» (tên mẫu dài đã rút gọn; metadata job giữ đủ).",
                                log=True,
                            )
                        exports_done.append(out_file)
                        exported_meta_rows.append(
                            {
                                "clip_id": cid,
                                "src_meta": dict(src_meta),
                                "render_path": str(out_file.resolve()),
                            }
                        )
                        try:
                            export_worker.clear_cancel()
                        except Exception:
                            pass
                        _queue_export_status(f"Clip {idx}/{n} xong: {out_file.name}", log=True)
                        if idx % 10 == 0:
                            try:
                                gc.collect()
                            except Exception:
                                pass
                else:
                    base_full = out_input.parent
                    logical_full_name = str(out_input.name)
                    base_full.mkdir(parents=True, exist_ok=True)
                    disk_full = _short_export_filename_for_dir(base_full, logical_full_name)
                    out_file = _pick_unique_dest_in_dir(base_full, disk_full)
                    export_project = copy.deepcopy(project)
                    export_project["width"] = w
                    export_project["height"] = h
                    export_project["fps"] = fps
                    export_project["duration"] = float(export_project.get("duration") or 0.0)
                    _queue_export_status(
                        f"Full timeline — validate/output «{out_file.name}» (mẫu «{logical_full_name}»)…",
                        log=True,
                    )
                    errs = validate_export(
                        export_project,
                        ffmpeg_path=ffmpeg_bin,
                        output_path=str(out_file),
                        media_resolver=mm,
                        require_contiguous_video_timeline=False,
                    )
                    if errs:
                        failures.append("; ".join(errs))
                        try:
                            if out_file.exists():
                                out_file.unlink()
                        except Exception:
                            pass
                    else:
                        def on_prog_full(x: float) -> None:
                            xf = max(0.0, min(1.0, float(x)))
                            inner_ui = min(0.995, xf)
                            pct_bar = int(inner_ui * 100.0)
                            tail = " · đang hoàn tất/ghi MP4…" if xf >= 0.99 else ""

                            def _ui() -> None:
                                prog_exp.configure(value=min(99, pct_bar))
                                _set_export_status(f"Đang mã hóa ~{pct_bar}%{tail}")

                            _schedule_on_main_thread(_ui)

                        full_plans: list[dict[str, Any]] = [
                            {"tag": "chuẩn (chất lượng)", "mp4_faststart": True, "enc": None, "light": False},
                            {
                                "tag": "fallback nhanh",
                                "mp4_faststart": False,
                                "enc": {"preset": "ultrafast", "crf": 34, "threads": 1},
                                "light": True,
                            },
                        ]
                        res_full: dict[str, Any] = {"ok": False, "error_message": ""}
                        for fpi, fplan in enumerate(full_plans):
                            if export_worker.is_cancel_requested() or _export_user_abort_ref.get("v"):
                                break
                            if fpi > 0:
                                _queue_export_status(f"Full timeline — thử lại ({fplan['tag']})…", log=True)
                                try:
                                    if out_file.exists():
                                        out_file.unlink()
                                except Exception:
                                    pass
                            try:
                                cmd = builder.build_export_command(
                                    export_project,
                                    str(out_file),
                                    ffmpeg_bin=ffmpeg_bin,
                                    ass_path=None,
                                    mp4_faststart=bool(fplan.get("mp4_faststart")),
                                    encoding_overrides=fplan.get("enc"),
                                    lightweight_mode_override=fplan.get("light"),
                                )
                            except Exception as ex:
                                res_full = {"ok": False, "error_message": str(ex)}
                                if fpi == len(full_plans) - 1:
                                    failures.append(str(ex))
                                continue

                            _queue_export_status(
                                f"Chạy FFmpeg full timeline ({fplan['tag']}) → {out_file.name}…",
                                log=True,
                            )

                            full_enc_t0 = time.monotonic()

                            def _hb_full() -> None:
                                _ffmpeg_heartbeat_log("Full timeline", out_file, full_enc_t0)

                            res_full = export_worker.render(
                                export_project,
                                str(out_file),
                                cmd,
                                duration_sec=max(0.1, float(export_project.get("duration") or 0.1)),
                                progress_callback=on_prog_full,
                                wait_heartbeat_callback=_hb_full,
                            )
                            if res_full.get("ok"):
                                break

                        try:
                            export_worker.clear_cancel()
                        except Exception:
                            pass
                        if res_full.get("ok"):
                            if logical_full_name.lower() != out_file.name.lower():
                                _queue_export_status(
                                    f"Full timeline — lưu «{out_file.name}» (tên mẫu dài đã rút gọn).",
                                    log=True,
                                )
                            exports_done.append(out_file)
                            src_meta = _pick_primary_source_meta_for_schedule()
                            exported_meta_rows.append(
                                {
                                    "clip_id": "",
                                    "src_meta": dict(src_meta),
                                    "render_path": str(out_file.resolve()),
                                }
                            )
                        else:
                            failures.append(_export_fail_line("Full timeline", res_full))
                            try:
                                if out_file.exists():
                                    out_file.unlink()
                            except Exception:
                                pass
            except Exception as ex:
                failures.append(f"Lỗi runtime export: {ex}")

            def done_ui() -> None:
                _mark_export_finished()
                if exports_done:
                    managed_list: list[Path] = []
                    for p0 in exports_done:
                        try:
                            managed_list.append(_ensure_output_in_renders(p0, export_dir=export_dir_ref))
                        except Exception:
                            managed_list.append(p0)
                    items_for_job: list[dict[str, Any]] = []
                    for i, mp in enumerate(managed_list):
                        src_row = exported_meta_rows[i] if i < len(exported_meta_rows) else {}
                        src_meta = dict(src_row.get("src_meta") or {})
                        desc = str(src_meta.get("description") or "").strip()
                        meta_title = str(src_meta.get("title") or mp.stem).strip()
                        line = internal_post_title_from_body(desc, fallback="")
                        if not line:
                            line = internal_post_title_from_body(meta_title, fallback=meta_title)
                        items_for_job.append(
                            {
                                "video_path": str(mp.resolve()),
                                "title": line,
                                "content": line,
                                "hashtags": [str(x).strip() for x in (src_meta.get("hashtags") or []) if str(x).strip()],
                                "source_download_video_id": str(src_meta.get("source_download_video_id") or ""),
                            }
                        )
                    prog_exp.configure(value=100)
                    _set_export_status(f"Xuất xong {len(managed_list)} file.", log=True)
                    if bool(var_exp_save_pending.get()) and items_for_job:
                        saved_jid = _append_saved_export_job(
                            str(var_exp_saved_job_name.get() or "").strip(),
                            items_for_job,
                            ("multi_clip_custom" if len(items_for_job) > 1 else "single_export_custom"),
                            publish_extra={
                                "publish_target": "unspecified",
                                "preset_fb_account_id": "",
                                "preset_fb_page_id": "",
                                "preset_tiktok_account_id": "",
                            },
                        )
                        notify(f"Đã xuất {len(managed_list)} file + lưu job chờ đăng: {saved_jid}")
                        if bool(var_exp_open_jobs_after_save.get()):
                            _open_schedule_jobs_tab(saved_jid)
                    else:
                        notify(f"Đã xuất {len(managed_list)} file.")
                    try:
                        rdir = export_dir_ref
                        lines = [
                            f"Đã xuất {len(managed_list)} file.",
                            "",
                            f"Thư mục: {rdir.resolve()}",
                        ]
                        for mp in managed_list[:6]:
                            lines.append(f"• {mp.name}")
                        if len(managed_list) > 6:
                            lines.append(f"… và {len(managed_list) - 6} file khác")
                        messagebox.showinfo("Xuất MP4", "\n".join(lines), parent=root)
                    except Exception:
                        messagebox.showinfo(
                            "Xuất MP4",
                            f"Đã xuất {len(managed_list)} file.",
                            parent=root,
                        )
                    if failures:
                        messagebox.showwarning(
                            "Export",
                            "Có file lỗi:\n" + "\n".join(failures[:8]) + (f"\n… (+{len(failures)-8})" if len(failures) > 8 else ""),
                            parent=root,
                        )
                    return
                if export_worker.is_cancel_requested() or _export_user_abort_ref.get("v"):
                    _set_export_status("Đã dừng export theo yêu cầu.", log=True)
                    notify("Đã dừng export.")
                    try:
                        export_worker.clear_cancel()
                    except Exception:
                        pass
                    return
                err = failures[0] if failures else "Lỗi export."
                _set_export_status(f"Export lỗi: {err}", log=True)
                messagebox.showerror("Export", err)

            _schedule_on_main_thread(done_ui)

        threading.Thread(target=work, daemon=True).start()

    btn_export_run = ttk.Button(btns_fr, text="Xuất MP4", command=_run_export_with_custom_settings)
    btn_export_run.pack(side=tk.LEFT)
    btn_exp_refs["run"] = btn_export_run
    btn_export_stop = ttk.Button(btns_fr, text="Dừng export", command=_stop_export_now)
    btn_export_stop.pack(side=tk.LEFT, padx=(8, 0))
    btn_exp_refs["stop"] = btn_export_stop

    def open_export_output_folder() -> None:
        try:
            r = _resolve_export_output_dir(for_job_name=str(var_exp_saved_job_name.get() or ""))
        except Exception:
            r = ve_paths["renders"]
        r.mkdir(parents=True, exist_ok=True)
        try:
            if os.name == "nt":
                os.startfile(str(r))  # type: ignore[attr-defined]
            else:
                subprocess.Popen(["xdg-open", str(r)])
        except Exception as e:
            messagebox.showerror("Folder", str(e))

    ttk.Button(exp_inner, text="Mở thư mục lưu MP4", command=open_export_output_folder).grid(
        row=7, column=0, sticky="w", pady=(0, 4)
    )
    exp_inner.columnconfigure(0, weight=1)
    _sync_export_running_ui()
    _sync_default_export_fields()
    _apply_ratio_quality_to_wh()

    def _load_first_project_when_idle() -> None:
        if project_ids:
            load_project_id(project_ids[0])

    root.after_idle(_load_first_project_when_idle)

    def shutdown_video_editor_subprocesses() -> None:
        try:
            preview_worker.shutdown_active_encoders()
        except Exception:
            pass
        try:
            export_worker.shutdown_active_encoders()
        except Exception:
            pass
        try:
            _stop_stock_preview()
        except Exception:
            pass
        try:
            _stop_managed_ffplay(wait_s=0.35)
        except Exception:
            pass

    return schedule_ve_background_audio_fill, shutdown_video_editor_subprocesses
