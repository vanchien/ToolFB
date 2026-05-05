"""
Tab Video Editor — Media / Preview / Timeline / Inspector / Export (MVP + Phase 2 tùy chọn).
"""

from __future__ import annotations

import copy
import os
import subprocess
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk
from typing import Any

from loguru import logger

from src.services.video_editor import (
    AudioExtractor,
    AudioMixManager,
    ExportPresetManager,
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
    add_editor_export_to_library,
    ensure_video_editor_layout,
    merge_phase2_defaults,
    validate_export,
)
from src.services.video_editor.batch_branding_export import (
    create_branded_project_for_video,
    list_videos_in_folder,
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
from src.utils.ffmpeg_paths import resolve_ffmpeg_executable, resolve_ffplay_executable

# Preset FFmpeg libx264 + CRF cho dropdown «Chất lượng xuất» (nhãn, preset, crf)
QUALITY_EXPORT_ITEMS: tuple[tuple[str, str, int], ...] = (
    ("Rất nhanh — nhỏ gọn (nháp)", "ultrafast", 28),
    ("Nhanh — mặc định", "veryfast", 23),
    ("Cân bằng — khuyên dùng", "medium", 20),
    ("Chất lượng cao", "slow", 18),
    ("Tối đa — file lớn", "slower", 16),
)


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
    win = canvas.create_window((0, 0), window=inner, anchor="nw")

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


def _match_quality_export_label(preset: str, crf: Any) -> str:
    pr = str(preset or "veryfast")
    try:
        cr = int(crf)
    except (TypeError, ValueError):
        cr = 23
    for lab, p, c in QUALITY_EXPORT_ITEMS:
        if p == pr and c == cr:
            return lab
    for lab, p, c in QUALITY_EXPORT_ITEMS:
        if p == pr:
            return lab
    return QUALITY_EXPORT_ITEMS[1][0]


def build_video_editor_tab(parent: ttk.Frame, root: tk.Tk) -> None:
    ensure_video_editor_layout()
    pm = VideoEditorProjectManager()
    mm = MediaManager()
    tm = TimelineManager(project_manager=pm)
    builder = FFmpegCommandBuilder()
    worker = RenderWorker()
    ep_mgr = ExportPresetManager()
    ep_mgr.ensure_file()
    sub_mgr = SubtitleManager()
    wf_gen = WaveformGenerator()
    vf_mgr = VideoFilterManager()
    tr_mgr = TransitionManager()
    tmplate_mgr = TemplateManager()
    amix_mgr = AudioMixManager()
    kf_mgr = KeyframeAnimationManager()

    project: dict[str, Any] | None = None
    selected_clip_id: str | None = None

    top = ttk.Frame(parent, padding=4)
    top.pack(fill=tk.X)
    ttk.Label(top, text="Dự án (project):").pack(side=tk.LEFT, padx=(0, 6))

    project_ids = [p["id"] for p in pm.list_projects()]
    sync_p2_ui_ref: dict[str, Any] = {"fn": None}
    sync_export_quality_ref: dict[str, Any] = {"fn": None}
    stock_audio_refresh_ref: dict[str, Any] = {"fn": None}

    def refresh_project_combo() -> None:
        nonlocal project_ids
        project_ids = [p["id"] for p in pm.list_projects()]
        cb_projects["values"] = project_ids

    var_project = tk.StringVar(value="")
    cb_projects = ttk.Combobox(top, textvariable=var_project, width=36, state="readonly")
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

    ttk.Button(top, text="Dự án mới", command=new_project).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(top, text="Lưu dự án", command=save_project_btn).pack(side=tk.LEFT, padx=(0, 4))

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
        for fp in paths:
            try:
                rec = mm.import_media(fp, kind, copy_to_library=True)
                project.setdefault("media", []).append(rec)
                pm.save_project(project)
                n_ok += 1
            except Exception as e:
                messagebox.showerror("Import lỗi", str(e))
                return
        refresh_media_tree()
        refresh_timeline()
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
    mf_wrap = ttk.Frame(media_inner)
    mf_wrap.pack(fill=tk.BOTH, expand=True, pady=(4, 0))
    mf_wrap.columnconfigure(0, weight=1)
    mf_wrap.rowconfigure(0, weight=1)
    tree_media = ttk.Treeview(mf_wrap, columns=cols_m, show="headings", height=10, selectmode="extended")
    for c, t, w in (
        ("name", "Tên file", 140),
        ("type", "Loại", 56),
        ("duration", "Độ dài (s)", 72),
        ("resolution", "Kích thước", 88),
    ):
        tree_media.heading(c, text=t)
        tree_media.column(c, width=w)
    sm = ttk.Scrollbar(mf_wrap, orient=tk.VERTICAL, command=tree_media.yview)
    tree_media.configure(yscrollcommand=sm.set)
    tree_media.grid(row=0, column=0, sticky="nsew")
    sm.grid(row=0, column=1, sticky="ns")

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
            "• «Mở preview» / «ffplay»: mở file vừa tạo (ảnh hoặc video nháp)."
        ),
        wraplength=560,
        justify="left",
    )
    lbl_preview.pack(anchor=tk.W)
    preview_path_var = tk.StringVar(value="")

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
        open_path_with_default_player(preview_path_var.get())

    def open_with_ffplay() -> None:
        p = preview_path_var.get().strip()
        if not p or not Path(p).is_file():
            notify("Chưa có file preview.")
            return
        ffplay = resolve_ffplay_executable()
        if not ffplay:
            notify("Không tìm thấy ffplay — dùng «Mở file preview».")
            open_path_with_default_player(p)
            return
        try:
            subprocess.Popen([ffplay, "-autoexit", p])
            notify(f"Đã mở ffplay: {Path(p).name}")
        except Exception as e:
            messagebox.showerror("ffplay", str(e))

    def run_preview_draft() -> None:
        """Xuất nháp timeline (giới hạn thời lượng) để xem trước export đầy đủ."""
        nonlocal project
        if not project:
            notify("Chưa có project.")
            return
        ffmpeg_bin = resolve_ffmpeg_executable()
        if not ffmpeg_bin:
            notify("Không có ffmpeg.")
            return
        pid = str(project.get("id") or "pv")
        safe = "".join(c for c in pid if c.isalnum() or c in "-_")[:48]
        out_p = ensure_video_editor_layout()["temp"] / f"preview_draft_{safe}.mp4"
        errs = validate_export(project, ffmpeg_path=ffmpeg_bin, output_path=str(out_p), media_resolver=mm)
        if errs:
            messagebox.showerror("Preview nháp", "\n".join(errs))
            return
        ass_path: str | None = None
        if project.get("subtitles"):
            try:
                ass_path = sub_mgr.export_ass(project, str(ensure_video_editor_layout()["subtitles"] / f"{safe}_preview_burn.ass"))
            except Exception as ex:
                notify(f"Cảnh báo ASS: {ex}")
        try:
            cmd = builder.build_export_command(
                copy.deepcopy(project),
                str(out_p),
                ffmpeg_bin=ffmpeg_bin,
                ass_path=ass_path,
                output_duration_limit_sec=20.0,
                encoding_overrides={"preset": "ultrafast", "crf": 28},
            )
        except Exception as e:
            messagebox.showerror("Preview nháp", str(e))
            return
        preview_path_var.set(str(out_p))
        lbl_preview.configure(text=f"Đang tạo preview nháp (~20s đầu)…\n{out_p}")
        notify("Đang render preview nháp (nền)…")

        dur = max(5.0, min(25.0, float(project.get("duration") or 20)))

        def done(res: dict[str, Any]) -> None:
            def ui() -> None:
                if res.get("ok"):
                    lbl_preview.configure(text=f"Preview nháp:\n{out_p}\nMở bằng «Mở preview» hoặc ffplay.")
                    notify("Preview nháp xong — mở file để xem.")
                else:
                    notify("Lỗi preview nháp — xem hộp thoại.")
                    messagebox.showerror("Preview nháp", res.get("error_message") or "Lỗi")

            root.after(0, ui)

        worker.render_thread(
            project,
            str(out_p),
            cmd,
            duration_sec=dur,
            progress_callback=lambda x: root.after(0, lambda v=x: lbl_status.configure(text=f"Preview nháp… {int(v * 100)}%")),
            done_callback=done,
        )

    def make_thumbnail() -> None:
        if not project:
            return
        sel = tree_media.selection()
        if not sel:
            messagebox.showinfo("Thumbnail", "Chọn một dòng media (video).")
            return
        iid = sel[0]
        mid = str(tree_media.item(iid).get("tags") or ("",))[0]
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
    ttk.Button(pbar, text="Mở bằng ffplay", command=open_with_ffplay).pack(side=tk.LEFT, padx=(0, 6))

    tl_fr = ttk.LabelFrame(center, text="Timeline — thứ tự clip trên video (Ctrl/Shift: chọn nhiều)", padding=4)
    center.add(tl_fr, weight=2)
    tl_wrap = ttk.Frame(tl_fr)
    tl_wrap.pack(fill=tk.BOTH, expand=True)
    tl_wrap.columnconfigure(0, weight=1)
    tl_wrap.rowconfigure(0, weight=1)
    cols_tl = ("track", "clip_id", "start", "dur", "src0", "src1", "kind")
    tree_tl = ttk.Treeview(tl_wrap, columns=cols_tl, show="headings", height=10, selectmode="extended")
    heads = {
        "track": "Lớp",
        "clip_id": "Mã clip",
        "start": "Bắt đầu TL (s)",
        "dur": "Độ dài (s)",
        "src0": "Điểm nguồn đầu",
        "src1": "Điểm nguồn cuối",
        "kind": "Loại clip",
    }
    widths = (72, 96, 56, 56, 72, 72, 52)
    for c, w in zip(cols_tl, widths):
        tree_tl.heading(c, text=heads[c])
        tree_tl.column(c, width=w)
    st = ttk.Scrollbar(tl_wrap, orient=tk.VERTICAL, command=tree_tl.yview)
    tree_tl.configure(yscrollcommand=st.set)
    tree_tl.grid(row=0, column=0, sticky="nsew")
    st.grid(row=0, column=1, sticky="ns")

    ttk.Label(
        tl_fr,
        text="Chọn một dòng để chỉnh bên phải; chọn nhiều clip «video» rồi mở tab «Chỉnh clip» để sửa âm lượng / tốc độ hàng loạt.",
        foreground="#444",
        font=("Segoe UI", 8),
        wraplength=520,
    ).pack(fill=tk.X, anchor="w", padx=2, pady=(2, 0))
    tl_actions = ttk.Frame(tl_fr)
    tl_actions.pack(fill=tk.X, pady=4)

    def _find_media(mid: str) -> dict[str, Any] | None:
        if not project:
            return None
        for m in project.get("media") or []:
            if isinstance(m, dict) and str(m.get("id")) == mid:
                return m
        return None

    def refresh_media_tree() -> None:
        tree_media.delete(*tree_media.get_children())
        if not project:
            return
        for m in project.get("media") or []:
            if not isinstance(m, dict):
                continue
            mid = str(m.get("id"))
            w, h = int(m.get("width") or 0), int(m.get("height") or 0)
            res = f"{w}x{h}" if w and h else ""
            tree_media.insert(
                "",
                tk.END,
                iid=mid,
                values=(
                    m.get("original_name", ""),
                    m.get("type", ""),
                    f"{float(m.get('duration') or 0):.2f}",
                    res,
                ),
                tags=(mid,),
            )

    def refresh_timeline() -> None:
        tree_tl.delete(*tree_tl.get_children())
        if not project:
            return
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
                tree_tl.insert(
                    "",
                    tk.END,
                    iid=cid,
                    values=(
                        tname,
                        cid[:12] + "…",
                        f"{ts:.2f}",
                        f"{du:.2f}",
                        f"{ss:.2f}" if ss != "" else "",
                        f"{se:.2f}" if se != "" else "",
                        kind,
                    ),
                    tags=(cid,),
                )

    def refresh_inspector() -> None:
        for w in insp_grid.winfo_children():
            w.destroy()
        nonlocal selected_clip_id
        if not project:
            ttk.Label(insp_grid, text="Chưa có project.").grid(row=0, column=0, sticky="w")
            return
        sel = tree_tl.selection()
        if not sel:
            selected_clip_id = None
            ttk.Label(
                insp_grid,
                text="Chọn clip trên timeline (Ctrl/Shift = nhiều clip video để sửa hàng loạt).",
                wraplength=320,
            ).grid(row=0, column=0, sticky="w")
            return
        selected_clip_id = sel[0]

        if len(sel) > 1:
            rows: list[tuple[str, dict[str, Any]]] = []
            for cid in sel:
                fc = _find_clip(cid)
                if fc and fc[1]:
                    rows.append((cid, fc[1]))
            if not rows:
                ttk.Label(insp_grid, text="Không đọc được clip.").grid(row=0, column=0, sticky="w")
                return
            if all(str(r[1].get("type")) == "video" for r in rows):
                ttk.Label(insp_grid, text=f"Sửa hàng loạt — {len(rows)} clip video").grid(
                    row=0, column=0, columnspan=2, sticky="w", pady=(0, 6)
                )
                var_b_vol = tk.StringVar()
                var_b_sp = tk.StringVar()
                var_b_fi = tk.StringVar()
                var_b_fo = tk.StringVar()
                var_b_rot = tk.StringVar(value="")
                var_b_canvas = tk.StringVar(value="")
                var_b_blur = tk.StringVar(value="")
                var_b_flip_h = tk.BooleanVar(value=False)
                var_b_flip_v = tk.BooleanVar(value=False)
                var_b_mute = tk.BooleanVar(value=False)
                var_b_set_flip = tk.BooleanVar(value=False)
                var_b_set_mute = tk.BooleanVar(value=False)

                for r, lab, var in (
                    (1, "Âm lượng (0–1)", var_b_vol),
                    (2, "Tốc độ (1 = bình thường)", var_b_sp),
                    (3, "Fade vào (giây)", var_b_fi),
                    (4, "Fade ra (giây)", var_b_fo),
                ):
                    ttk.Label(insp_grid, text=lab).grid(row=r, column=0, sticky="w", pady=2)
                    ttk.Entry(insp_grid, textvariable=var, width=14).grid(row=r, column=1, sticky="w", pady=2)

                ttk.Label(insp_grid, text="Xoay (để trống / 0 / 90 / 180 / 270)").grid(row=5, column=0, sticky="w", pady=2)
                ttk.Combobox(
                    insp_grid,
                    textvariable=var_b_rot,
                    values=["", "0", "90", "180", "270"],
                    state="readonly",
                    width=12,
                ).grid(row=5, column=1, sticky="w", pady=2)
                ttk.Label(insp_grid, text="Canvas mode (để trống / fit / fill / stretch)").grid(
                    row=6, column=0, sticky="w", pady=2
                )
                ttk.Combobox(
                    insp_grid,
                    textvariable=var_b_canvas,
                    values=["", "fit", "fill", "stretch"],
                    state="readonly",
                    width=12,
                ).grid(row=6, column=1, sticky="w", pady=2)
                ttk.Label(insp_grid, text="Blur nền (để trống = giữ nguyên, 0 = tắt)").grid(
                    row=7, column=0, sticky="w", pady=2
                )
                ttk.Entry(insp_grid, textvariable=var_b_blur, width=14).grid(row=7, column=1, sticky="w", pady=2)

                rf = ttk.Frame(insp_grid)
                rf.grid(row=8, column=0, columnspan=2, sticky="w", pady=(2, 0))
                ttk.Checkbutton(rf, text="Đặt flip", variable=var_b_set_flip).pack(side=tk.LEFT)
                ttk.Checkbutton(rf, text="Lật ngang", variable=var_b_flip_h).pack(side=tk.LEFT, padx=(8, 0))
                ttk.Checkbutton(rf, text="Lật dọc", variable=var_b_flip_v).pack(side=tk.LEFT, padx=(8, 0))

                rm = ttk.Frame(insp_grid)
                rm.grid(row=9, column=0, columnspan=2, sticky="w", pady=(2, 0))
                ttk.Checkbutton(rm, text="Đặt muted", variable=var_b_set_mute).pack(side=tk.LEFT)
                ttk.Checkbutton(rm, text="Tắt âm clip", variable=var_b_mute).pack(side=tk.LEFT, padx=(8, 0))

                media_images = [m for m in (project.get("media") or []) if isinstance(m, dict) and str(m.get("type") or "") == "image"]
                media_audios = [m for m in (project.get("media") or []) if isinstance(m, dict) and str(m.get("type") or "") == "audio"]
                logo_opts = [""] + [f"{str(m.get('id'))}|{str(m.get('name') or 'image')}" for m in media_images]
                audio_opts = [""] + [f"{str(m.get('id'))}|{str(m.get('name') or 'audio')}" for m in media_audios]
                var_b_logo = tk.StringVar(value="")
                var_b_audio = tk.StringVar(value="")
                var_b_logo_opacity = tk.StringVar(value="0.92")
                var_b_logo_ratio = tk.StringVar(value="0.15")
                var_b_audio_vol = tk.StringVar(value="1.0")

                ttk.Separator(insp_grid, orient=tk.HORIZONTAL).grid(row=10, column=0, columnspan=2, sticky="ew", pady=6)
                ttk.Label(insp_grid, text="Thêm logo cho mỗi clip video đã chọn").grid(row=11, column=0, sticky="w", pady=2)
                ttk.Combobox(insp_grid, textvariable=var_b_logo, values=logo_opts, width=36, state="readonly").grid(
                    row=11, column=1, sticky="ew", pady=2
                )
                ttk.Label(insp_grid, text="Logo ratio theo ngang canvas (vd 0.15), opacity").grid(row=12, column=0, sticky="w", pady=2)
                fl = ttk.Frame(insp_grid)
                fl.grid(row=12, column=1, sticky="w", pady=2)
                ttk.Entry(fl, textvariable=var_b_logo_ratio, width=8).pack(side=tk.LEFT)
                ttk.Entry(fl, textvariable=var_b_logo_opacity, width=8).pack(side=tk.LEFT, padx=(6, 0))

                ttk.Label(insp_grid, text="Thêm audio cho mỗi clip video đã chọn").grid(row=13, column=0, sticky="w", pady=2)
                ttk.Combobox(insp_grid, textvariable=var_b_audio, values=audio_opts, width=36, state="readonly").grid(
                    row=13, column=1, sticky="ew", pady=2
                )
                ttk.Label(insp_grid, text="Âm lượng audio mới").grid(row=14, column=0, sticky="w", pady=2)
                ttk.Entry(insp_grid, textvariable=var_b_audio_vol, width=14).grid(row=14, column=1, sticky="w", pady=2)

                ttk.Label(
                    insp_grid,
                    text="Để trống = giữ nguyên. Logo/audio sẽ bám theo timeline_start + duration của từng clip video.",
                    foreground="gray",
                    wraplength=320,
                ).grid(row=15, column=0, columnspan=2, sticky="w", pady=4)

                def _pick_media_id(raw: str) -> str:
                    s = str(raw or "").strip()
                    return s.split("|", 1)[0].strip() if "|" in s else s

                def _find_track_clips(track_type: str) -> list[dict[str, Any]]:
                    for tr in project.get("tracks") or []:
                        if isinstance(tr, dict) and str(tr.get("type") or "") == track_type:
                            return tr.setdefault("clips", [])
                    return []

                def apply_batch_video() -> None:
                    if not project:
                        return
                    patch: dict[str, Any] = {}
                    for var, key in (
                        (var_b_vol, "volume"),
                        (var_b_sp, "speed"),
                        (var_b_fi, "fade_in"),
                        (var_b_fo, "fade_out"),
                    ):
                        s = var.get().strip()
                        if s:
                            try:
                                patch[key] = float(s)
                            except ValueError:
                                messagebox.showerror("Hàng loạt", f"Số không hợp lệ: {key}")
                                return
                    if var_b_set_flip.get():
                        patch["flip_horizontal"] = bool(var_b_flip_h.get())
                        patch["flip_vertical"] = bool(var_b_flip_v.get())
                    if var_b_set_mute.get():
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
                        if canvas_s not in ("fit", "fill", "stretch"):
                            messagebox.showerror("Hàng loạt", "Canvas mode chỉ nhận fit/fill/stretch.")
                            return
                        patch["canvas_mode"] = canvas_s
                    blur_s = var_b_blur.get().strip()
                    if blur_s:
                        try:
                            blur_i = int(float(blur_s))
                            patch["blur_background"] = {"enabled": blur_i > 0, "blur": max(1, abs(blur_i))}
                        except ValueError:
                            messagebox.showerror("Hàng loạt", "Blur phải là số nguyên.")
                            return

                    logo_mid = _pick_media_id(var_b_logo.get())
                    add_logo = bool(logo_mid)
                    logo_opa = 0.92
                    logo_ratio = 0.15
                    if add_logo:
                        try:
                            logo_opa = max(0.0, min(1.0, float(var_b_logo_opacity.get().strip() or "0.92")))
                            logo_ratio = max(0.02, min(0.6, float(var_b_logo_ratio.get().strip() or "0.15")))
                        except ValueError:
                            messagebox.showerror("Hàng loạt", "Thông số logo không hợp lệ.")
                            return

                    audio_mid = _pick_media_id(var_b_audio.get())
                    add_audio = bool(audio_mid)
                    audio_vol = 1.0
                    audio_media_duration = 0.0
                    if add_audio:
                        try:
                            audio_vol = max(0.0, float(var_b_audio_vol.get().strip() or "1.0"))
                        except ValueError:
                            messagebox.showerror("Hàng loạt", "Âm lượng audio không hợp lệ.")
                            return
                        for m in project.get("media") or []:
                            if isinstance(m, dict) and str(m.get("id") or "") == audio_mid:
                                audio_media_duration = max(0.0, float(m.get("duration") or 0.0))
                                break

                    if not patch and not add_logo and not add_audio:
                        notify("Hàng loạt: chưa chọn thay đổi nào.")
                        return

                    overlay_added = 0
                    audio_added = 0
                    canvas_w = int(project.get("width") or 1080)
                    logo_w = max(80, int(canvas_w * logo_ratio))
                    for cid, c in rows:
                        try:
                            if patch:
                                tm.update_clip(project, cid, patch)
                            ts = float(c.get("timeline_start") or 0)
                            du = max(0.1, float(c.get("duration") or 0))
                            if add_logo:
                                before_ids = {str(x.get("id") or "") for x in _find_track_clips("overlay") if isinstance(x, dict)}
                                tm.add_clip(project, logo_mid, "overlay")
                                for oc in _find_track_clips("overlay"):
                                    if not isinstance(oc, dict):
                                        continue
                                    oid = str(oc.get("id") or "")
                                    if not oid or oid in before_ids:
                                        continue
                                    oc["timeline_start"] = ts
                                    oc["duration"] = du
                                    oc["x"] = 24
                                    oc["y"] = 24
                                    oc["width"] = logo_w
                                    oc["height"] = logo_w
                                    oc["opacity"] = logo_opa
                                    overlay_added += 1
                                    break
                            if add_audio:
                                before_ids = {str(x.get("id") or "") for x in _find_track_clips("audio") if isinstance(x, dict)}
                                tm.add_clip(project, audio_mid, "audio")
                                for ac in _find_track_clips("audio"):
                                    if not isinstance(ac, dict):
                                        continue
                                    aid = str(ac.get("id") or "")
                                    if not aid or aid in before_ids:
                                        continue
                                    ac["timeline_start"] = ts
                                    ac["duration"] = du
                                    ac["source_start"] = 0.0
                                    ac["source_end"] = min(du, audio_media_duration) if audio_media_duration > 0 else du
                                    ac["duration"] = max(0.1, float(ac["source_end"]) - float(ac["source_start"]))
                                    ac["volume"] = audio_vol
                                    audio_added += 1
                                    break
                        except Exception as ex:
                            messagebox.showerror("Hàng loạt", str(ex))
                            return

                    pm.save_project(project)
                    keys = list(patch.keys())
                    msg = f"Hàng loạt: đã cập nhật {len(rows)} clip"
                    if keys:
                        msg += f" ({', '.join(keys)})"
                    if overlay_added:
                        msg += f", +{overlay_added} logo"
                    if audio_added:
                        msg += f", +{audio_added} audio"
                    notify(msg + ".")
                    refresh_timeline()
                    refresh_inspector()

                ttk.Button(insp_grid, text="Áp dụng cho clip đã chọn", command=apply_batch_video).grid(
                    row=16, column=0, columnspan=2, sticky="w", pady=8
                )
                insp_grid.columnconfigure(1, weight=1)
                return
            ttk.Label(
                insp_grid,
                text="Chọn nhiều clip cùng loại «video» để sửa hàng loạt — hoặc chọn một clip.",
                wraplength=300,
            ).grid(row=0, column=0, sticky="w")
            return

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
            for lab, key in (
                ("Bắt đầu trên timeline (giây)", "timeline_start"),
                ("Điểm cắt đầu trong file nguồn (giây)", "source_start"),
                ("Điểm cắt cuối trong file nguồn (giây)", "source_end"),
                ("Độ dài clip trên timeline (giây)", "duration"),
                ("Tốc độ phát (1 = bình thường)", "speed"),
                ("Âm lượng (0–1)", "volume"),
                ("Fade vào (giây)", "fade_in"),
                ("Fade ra (giây)", "fade_out"),
            ):
                add_num(lab, key, row)
                row += 1

            ttk.Separator(insp_grid, orient=tk.HORIZONTAL).grid(row=row, column=0, columnspan=2, sticky="ew", pady=8)
            row += 1
            ttk.Label(insp_grid, text="Transform & canvas", font=("Segoe UI", 9, "bold")).grid(row=row, column=0, columnspan=2, sticky="w")
            row += 1

            var_fh = tk.BooleanVar(value=bool(cl.get("flip_horizontal")))
            var_fv = tk.BooleanVar(value=bool(cl.get("flip_vertical")))

            def save_flips() -> None:
                if project and selected_clip_id:
                    tm.update_clip(
                        project,
                        selected_clip_id,
                        {"flip_horizontal": var_fh.get(), "flip_vertical": var_fv.get()},
                    )
                    pm.save_project(project)
                    notify("Đã lưu lật ảnh.")

            ttk.Checkbutton(insp_grid, text="Lật ngang", variable=var_fh, command=save_flips).grid(row=row, column=0, sticky="w")
            ttk.Checkbutton(insp_grid, text="Lật dọc", variable=var_fv, command=save_flips).grid(row=row, column=1, sticky="w")
            row += 1

            var_rot = tk.StringVar(value=str(int(cl.get("rotation") or 0)))
            ttk.Label(insp_grid, text="Xoay").grid(row=row, column=0, sticky="w")
            cb_rot = ttk.Combobox(insp_grid, textvariable=var_rot, values=["0", "90", "180", "270"], width=8, state="readonly")
            cb_rot.grid(row=row, column=1, sticky="w")

            def save_rot(_e: Any = None) -> None:
                if project and selected_clip_id:
                    try:
                        r = int(var_rot.get())
                        tm.rotate_clip(project, selected_clip_id, r)
                        pm.save_project(project)
                        notify(f"Đã xoay {r}°.")
                    except Exception as ex:
                        messagebox.showerror("Transform", str(ex))

            cb_rot.bind("<<ComboboxSelected>>", save_rot)
            row += 1

            var_canvas = tk.StringVar(value=str(cl.get("canvas_mode") or "fit"))
            ttk.Label(insp_grid, text="Vào khung (Fit/Fill/Stretch)").grid(row=row, column=0, sticky="w")
            cb_canvas = ttk.Combobox(insp_grid, textvariable=var_canvas, values=["fit", "fill", "stretch"], width=12, state="readonly")
            cb_canvas.grid(row=row, column=1, sticky="w")

            def save_canvas(_e: Any = None) -> None:
                if project and selected_clip_id:
                    try:
                        tm.set_canvas_mode(project, selected_clip_id, var_canvas.get())
                        pm.save_project(project)
                        notify(f"Canvas: {var_canvas.get()}.")
                    except Exception as ex:
                        messagebox.showerror("Canvas", str(ex))

            cb_canvas.bind("<<ComboboxSelected>>", save_canvas)
            row += 1

            bb = cl.get("blur_background") if isinstance(cl.get("blur_background"), dict) else {}
            var_blen = tk.BooleanVar(value=bool(bb.get("enabled")))
            var_bamt = tk.StringVar(value=str(bb.get("blur") or 20))

            def save_blur(_e: Any = None) -> None:
                if project and selected_clip_id:
                    try:
                        b = int(str(var_bamt.get()).strip() or "20")
                        tm.set_blur_background(project, selected_clip_id, var_blen.get(), b)
                        pm.save_project(project)
                        notify("Đã lưu blur nền.")
                    except Exception as ex:
                        messagebox.showerror("Blur", str(ex))

            fr_blur = ttk.Frame(insp_grid)
            fr_blur.grid(row=row, column=0, columnspan=2, sticky="w")
            ttk.Checkbutton(fr_blur, text="Blur nền (plate)", variable=var_blen, command=save_blur).pack(side=tk.LEFT)
            ttk.Label(fr_blur, text="mạnh").pack(side=tk.LEFT, padx=(8, 2))
            eb = ttk.Entry(fr_blur, textvariable=var_bamt, width=5)
            eb.pack(side=tk.LEFT)
            eb.bind("<FocusOut>", save_blur)
            row += 1

            var_mute = tk.BooleanVar(value=bool(cl.get("muted")))

            def save_mute() -> None:
                if project and selected_clip_id:
                    tm.mute_clip(project, selected_clip_id, var_mute.get())
                    pm.save_project(project)
                    notify("Đã cập nhật tắt âm.")

            ttk.Checkbutton(insp_grid, text="Tắt âm tiếng gốc clip", variable=var_mute, command=save_mute).grid(row=row, column=0, columnspan=2, sticky="w")
            row += 1

            cr = cl.get("crop") if isinstance(cl.get("crop"), dict) else {}
            pw = int(project.get("width") or 1080)
            ph = int(project.get("height") or 1920)
            var_c_en = tk.BooleanVar(value=bool(cr.get("enabled")))
            var_cx = tk.StringVar(value=str(cr.get("x", 0)))
            var_cy = tk.StringVar(value=str(cr.get("y", 0)))
            var_cw = tk.StringVar(value=str(cr.get("width", pw)))
            var_ch = tk.StringVar(value=str(cr.get("height", ph)))

            var_crop_preset = tk.StringVar(
                value=_match_res_wh_label(int(float(str(var_cw.get() or "0"))), int(float(str(var_ch.get() or "0"))))
            )

            def save_crop(_e: Any = None) -> None:
                if not project or not selected_clip_id:
                    return
                try:
                    crop_d = {
                        "enabled": var_c_en.get(),
                        "x": max(0, int(float(var_cx.get() or 0))),
                        "y": max(0, int(float(var_cy.get() or 0))),
                        "width": max(2, int(float(var_cw.get() or 2))),
                        "height": max(2, int(float(var_ch.get() or 2))),
                    }
                    tm.crop_clip(project, selected_clip_id, crop_d)
                    pm.save_project(project)
                    try:
                        var_crop_preset.set(_match_res_wh_label(crop_d["width"], crop_d["height"]))
                    except Exception:
                        pass
                    notify("Đã lưu crop.")
                except Exception as ex:
                    messagebox.showerror("Crop", str(ex))

            def apply_crop_preset(_e: Any = None) -> None:
                picked = var_crop_preset.get()
                for lab, dims in RES_WH_PRESETS:
                    if lab != picked:
                        continue
                    if dims is None:
                        return
                    w, h = dims
                    var_cw.set(str(w))
                    var_ch.set(str(h))
                    var_cx.set("0")
                    var_cy.set("0")
                    save_crop()
                    return

            crop_preset_labels = [x[0] for x in RES_WH_PRESETS]

            crop_lf = ttk.LabelFrame(insp_grid, text="Crop — chọn nhanh kích thước hoặc nhập X/Y/W/H (pixel trên nguồn)", padding=6)
            crop_lf.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(0, 6))
            crop_lf.columnconfigure(1, weight=1)

            ttk.Label(crop_lf, text="Tỉ lệ / kích thước:").grid(row=0, column=0, sticky="w")
            cb_crop_p = ttk.Combobox(
                crop_lf,
                textvariable=var_crop_preset,
                values=crop_preset_labels,
                state="readonly",
            )
            cb_crop_p.grid(row=0, column=1, sticky="ew", padx=(6, 0))
            cb_crop_p.bind("<<ComboboxSelected>>", apply_crop_preset)

            fcr = ttk.Frame(crop_lf)
            fcr.grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 0))
            ttk.Checkbutton(fcr, text="Bật crop", variable=var_c_en, command=save_crop).pack(side=tk.LEFT)
            ttk.Label(fcr, text="Chỉnh tay:", foreground="#555").pack(side=tk.LEFT, padx=(10, 0))
            for lab, var in (("X", var_cx), ("Y", var_cy), ("W", var_cw), ("H", var_ch)):
                ttk.Label(fcr, text=lab).pack(side=tk.LEFT, padx=(6, 0))
                e = ttk.Entry(fcr, textvariable=var, width=6)
                e.pack(side=tk.LEFT)
                e.bind("<FocusOut>", save_crop)
            row += 1

            sc = cl.get("scale") if isinstance(cl.get("scale"), dict) else {}
            var_s_en = tk.BooleanVar(value=bool(sc.get("enabled")))
            var_sw = tk.StringVar(value=str(sc.get("width", pw)))
            var_sh = tk.StringVar(value=str(sc.get("height", ph)))
            var_s_ka = tk.BooleanVar(value=bool(sc.get("keep_aspect", True)))

            var_scale_preset = tk.StringVar(
                value=_match_res_wh_label(int(float(str(var_sw.get() or "0"))), int(float(str(var_sh.get() or "0"))))
            )

            def save_scale(_e: Any = None) -> None:
                if not project or not selected_clip_id:
                    return
                try:
                    sd = {
                        "enabled": var_s_en.get(),
                        "width": max(2, int(float(var_sw.get() or 2))),
                        "height": max(2, int(float(var_sh.get() or 2))),
                        "keep_aspect": var_s_ka.get(),
                    }
                    tm.update_clip(project, selected_clip_id, {"scale": sd})
                    pm.save_project(project)
                    try:
                        var_scale_preset.set(_match_res_wh_label(sd["width"], sd["height"]))
                    except Exception:
                        pass
                    notify("Đã lưu scale.")
                except Exception as ex:
                    messagebox.showerror("Scale", str(ex))

            def apply_scale_preset(_e: Any = None) -> None:
                picked = var_scale_preset.get()
                for lab, dims in RES_WH_PRESETS:
                    if lab != picked:
                        continue
                    if dims is None:
                        return
                    w, h = dims
                    var_sw.set(str(w))
                    var_sh.set(str(h))
                    save_scale()
                    return

            scale_lf = ttk.LabelFrame(insp_grid, text="Scale trước canvas (tuỳ chọn) — chọn tỉ lệ hoặc nhập W/H", padding=6)
            scale_lf.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(0, 6))
            scale_lf.columnconfigure(1, weight=1)

            ttk.Label(scale_lf, text="Tỉ lệ / kích thước:").grid(row=0, column=0, sticky="w")
            cb_scale_p = ttk.Combobox(
                scale_lf,
                textvariable=var_scale_preset,
                values=crop_preset_labels,
                state="readonly",
            )
            cb_scale_p.grid(row=0, column=1, sticky="ew", padx=(6, 0))
            cb_scale_p.bind("<<ComboboxSelected>>", apply_scale_preset)

            fsc = ttk.Frame(scale_lf)
            fsc.grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 0))
            ttk.Checkbutton(fsc, text="Bật scale", variable=var_s_en, command=save_scale).pack(side=tk.LEFT)
            ttk.Label(fsc, text="W", foreground="#555").pack(side=tk.LEFT, padx=(10, 0))
            ew = ttk.Entry(fsc, textvariable=var_sw, width=7)
            ew.pack(side=tk.LEFT)
            ew.bind("<FocusOut>", save_scale)
            ttk.Label(fsc, text="H").pack(side=tk.LEFT, padx=(4, 0))
            eh = ttk.Entry(fsc, textvariable=var_sh, width=7)
            eh.pack(side=tk.LEFT)
            eh.bind("<FocusOut>", save_scale)
            ttk.Checkbutton(fsc, text="Giữ tỉ lệ", variable=var_s_ka, command=save_scale).pack(side=tk.LEFT, padx=(8, 0))
            row += 1
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
            selected_clip_id = sel[0]
        refresh_inspector()

    tree_tl.bind("<<TreeviewSelect>>", on_tl_select)

    def refresh_all() -> None:
        refresh_media_tree()
        refresh_timeline()
        refresh_inspector()
        try:
            if project:
                ex = project.get("export") or {}
                var_preset.set(f"{ex.get('preset', 'veryfast')} / crf {ex.get('crf', 23)}")
            else:
                var_preset.set("—")
        except Exception:
            pass
        qfn = sync_export_quality_ref.get("fn")
        if callable(qfn):
            qfn()
        sfn = stock_audio_refresh_ref.get("fn")
        if callable(sfn):
            sfn()

    def media_action_add_timeline() -> None:
        if not project:
            return
        sel = tree_media.selection()
        if not sel:
            messagebox.showinfo("Timeline", "Chọn media.")
            return
        added = 0
        skipped = 0
        errs: list[str] = []
        type_counts: dict[str, int] = {"video": 0, "image": 0, "audio": 0}
        for mid in sel:
            media = _find_media(mid)
            if not media:
                skipped += 1
                continue
            mt = str(media.get("type") or "")
            try:
                if mt == "video":
                    tm.add_clip(project, mid, "video")
                    type_counts["video"] += 1
                elif mt == "image":
                    tm.add_clip(project, mid, "overlay")
                    type_counts["image"] += 1
                elif mt == "audio":
                    tm.add_clip(project, mid, "audio")
                    type_counts["audio"] += 1
                else:
                    skipped += 1
                    continue
                added += 1
            except Exception as e:
                errs.append(f"{media.get('name') or mid}: {e}")
        if added > 0:
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
        sel = tree_media.selection()
        if not sel:
            return
        mid = sel[0]
        project["media"] = [m for m in (project.get("media") or []) if str(m.get("id")) != mid]
        for tr in project.get("tracks") or []:
            if not isinstance(tr, dict):
                continue
            tr["clips"] = [c for c in (tr.get("clips") or []) if str(c.get("media_id")) != mid]
        pm.save_project(project)
        refresh_media_tree()
        refresh_timeline()
        notify("Đã xóa media khỏi project (clip liên quan cũng bị gỡ).")

    def media_open_file() -> None:
        sel = tree_media.selection()
        if not sel:
            return
        media = _find_media(sel[0])
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
    ttk.Button(ma_fr, text="Xóa khỏi thư viện", command=media_delete).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(ma_fr, text="Mở file gốc", command=media_open_file).pack(side=tk.LEFT, padx=(0, 4))

    def media_extract_audio() -> None:
        sel = tree_media.selection()
        if not sel:
            messagebox.showinfo("Tách âm", "Chọn một video trong bảng Media.")
            return
        media = _find_media(sel[0])
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

    def clip_trim() -> None:
        if not project:
            return
        sel = tree_tl.selection()
        if len(sel) != 1:
            notify("Trim: chọn đúng một clip.")
            return
        cid = sel[0]
        _, cl = _find_clip(cid)
        if not cl or str(cl.get("type")) != "video":
            messagebox.showinfo("Trim", "Chọn clip video.")
            return
        s0 = simpledialog.askfloat("Trim", "source_start:", initialvalue=float(cl.get("source_start") or 0), parent=root)
        s1 = simpledialog.askfloat("Trim", "source_end:", initialvalue=float(cl.get("source_end") or 0), parent=root)
        if s0 is None or s1 is None:
            return
        try:
            tm.trim_clip(project, cid, s0, s1)
            refresh_timeline()
            refresh_inspector()
            notify(f"Trim clip: source {s0:.2f}–{s1:.2f}")
        except Exception as e:
            messagebox.showerror("Trim", str(e))

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
        st = simpledialog.askfloat(
            "Split",
            "Thời điểm trên timeline (giây):",
            initialvalue=init_split,
            parent=root,
        )
        if st is None:
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
        nt = simpledialog.askfloat("Move", "timeline_start mới:", parent=root)
        if nt is None:
            return
        try:
            tm.move_clip(project, sel[0], nt)
            refresh_timeline()
            refresh_inspector()
            notify(f"Đã di chuyển clip → timeline_start={nt:.2f}")
        except Exception as e:
            messagebox.showerror("Move", str(e))

    def clip_delete() -> None:
        if not project:
            return
        sel = tree_tl.selection()
        if not sel:
            notify("Chọn ít nhất một clip để xóa.")
            return
        try:
            for cid in sel:
                tm.delete_clip(project, cid)
            selected_clip_id = None
            refresh_timeline()
            refresh_inspector()
            notify(f"Đã xóa {len(sel)} clip khỏi timeline.")
        except Exception as e:
            messagebox.showerror("Xóa", str(e))

    def add_text_clip() -> None:
        if not project:
            return
        tx = simpledialog.askstring("Text clip", "Nội dung:", parent=root)
        if not tx:
            return
        try:
            tm.add_text_clip(project, tx, timeline_start=0.0, duration=5.0)
            refresh_timeline()
            notify("Đã thêm clip text.")
        except Exception as e:
            messagebox.showerror("Text", str(e))

    ttk.Button(tl_actions, text="Cắt nguồn (trim)", command=clip_trim).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(tl_actions, text="Chia đôi clip", command=clip_split).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(tl_actions, text="Dời trên timeline", command=clip_move).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(tl_actions, text="Xóa khỏi timeline", command=clip_delete).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(tl_actions, text="Thêm chữ trên video", command=add_text_clip).pack(side=tk.LEFT, padx=(12, 0))
    ttk.Label(
        tl_fr,
        text=(
            "Gợi ý: «Cắt nguồn» = giới hạn đoạn lấy từ file gốc; «Chia đôi» = tách 1 clip thành 2; "
            "«Dời» = đổi thời điểm bắt đầu trên timeline; «Xóa» chỉ xóa khỏi timeline, không xóa file trong Media."
        ),
        foreground="#555",
        font=("Segoe UI", 8),
        wraplength=520,
        justify="left",
    ).pack(fill=tk.X, anchor="w", padx=2, pady=(0, 2))

    # --- Inspector + Export ---
    right = ttk.PanedWindow(main, orient=tk.VERTICAL)
    main.add(right, weight=3)

    insp_fr = ttk.LabelFrame(
        right,
        text="Chỉnh sửa — chọn clip trên Timeline (tab dưới có thanh cuộn)",
        padding=4,
    )
    right.add(insp_fr, weight=2)
    insp_nb = ttk.Notebook(insp_fr)
    insp_nb.pack(fill=tk.BOTH, expand=True)
    tab_insp_clip = ttk.Frame(insp_nb, padding=2)
    insp_nb.add(tab_insp_clip, text="Chỉnh clip")
    insp_grid = _pack_scrollable_vertical(tab_insp_clip)

    tab_phase2 = ttk.Frame(insp_nb, padding=4)
    insp_nb.add(tab_phase2, text="Dự án: nhạc · phụ đề · preset")

    p2_fr = _pack_scrollable_vertical(tab_phase2)

    ttk.Label(
        p2_fr,
        text=(
            "Thiết lập cả dự án: canvas, phụ đề, BGM, kho nhạc, chuyển cảnh… "
            "Cuộn bằng thanh bên phải hoặc lăn chuột trên vùng trống. "
            "Filter / transition: chọn clip video trên Timeline trước."
        ),
        foreground="#444",
        font=("Segoe UI", 8),
        wraplength=380,
        justify="left",
    ).grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 8))
    p2_fr.columnconfigure(1, weight=1)

    ttk.Label(p2_fr, text="Preset xuất (canvas + codec FFmpeg):").grid(row=1, column=0, sticky="w")
    preset_ids = [str(p.get("id")) for p in ep_mgr.list_presets()]
    var_ep = tk.StringVar(value=preset_ids[0] if preset_ids else "")

    def apply_export_preset() -> None:
        if not project:
            return
        pid = var_ep.get().strip()
        if not pid:
            return
        try:
            ep_mgr.apply_to_project(project, pid)
            pm.save_project(project)
            ex = project.get("export") or {}
            var_preset.set(f"{ex.get('preset', 'veryfast')} / crf {ex.get('crf', 23)}")
            qfn = sync_export_quality_ref.get("fn")
            if callable(qfn):
                qfn()
            notify(f"Đã áp preset xuất: {pid}")
        except Exception as e:
            messagebox.showerror("Preset", str(e))

    cb_ep = ttk.Combobox(p2_fr, textvariable=var_ep, values=preset_ids, width=28, state="readonly")
    cb_ep.grid(row=1, column=1, sticky="w", padx=4)
    ttk.Button(p2_fr, text="Áp preset", command=apply_export_preset).grid(row=1, column=2, padx=4)

    ttk.Label(p2_fr, text="Filter màu (cần 1 clip video đang chọn trên Timeline):").grid(row=2, column=0, sticky="w", pady=(6, 0))
    var_filt = tk.StringVar(value="normal")
    cb_filt = ttk.Combobox(
        p2_fr,
        textvariable=var_filt,
        values=list(VideoFilterManager.PRESETS.keys()),
        width=14,
        state="readonly",
    )
    cb_filt.grid(row=2, column=1, sticky="w", padx=4, pady=(6, 0))

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

    ttk.Button(p2_fr, text="Gán filter", command=apply_color_filter).grid(row=2, column=2, padx=4, pady=(6, 0))

    ttk.Separator(p2_fr, orient=tk.HORIZONTAL).grid(row=3, column=0, columnspan=3, sticky="ew", pady=8)

    ttk.Label(p2_fr, text="Phụ đề (toàn bộ video khi xuất):").grid(row=4, column=0, sticky="nw")

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

    ttk.Button(p2_fr, text="Nhập SRT / VTT", command=import_srt).grid(row=4, column=1, sticky="w")

    ttk.Label(p2_fr, text="Nhạc nền BGM (chọn dòng audio trong bảng Media bên trái):").grid(row=5, column=0, sticky="w", pady=(6, 0))
    var_bgm_vol = tk.StringVar(value="0.25")

    def add_bgm() -> None:
        if not project:
            return
        sel = tree_media.selection()
        if not sel:
            messagebox.showinfo("BGM", "Chọn media audio trong bảng Media.")
            return
        mid = sel[0]
        media = _find_media(mid)
        if not media or str(media.get("type")) != "audio":
            messagebox.showinfo("BGM", "Chọn file audio đã import.")
            return
        try:
            vol = float(var_bgm_vol.get().strip() or "0.25")
            amix_mgr.add_background_music(project, mid, vol)
            pm.save_project(project)
            notify("Đã thêm nhạc nền (audio_settings).")
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
        except Exception as e:
            messagebox.showerror("BGM", str(e))

    ttk.Entry(p2_fr, textvariable=var_bgm_vol, width=8).grid(row=5, column=1, sticky="w", padx=4, pady=(6, 0))
    ttk.Button(p2_fr, text="Thêm BGM", command=add_bgm).grid(row=5, column=2, padx=4, pady=(6, 0))
    ttk.Button(p2_fr, text="Tự thêm audio có sẵn", command=auto_add_bgm_from_existing_audio).grid(row=6, column=1, columnspan=2, sticky="w", pady=(4, 0))

    def add_duck() -> None:
        if not project:
            return
        a = simpledialog.askfloat("Ducking", "Bắt đầu (s):", parent=root)
        b = simpledialog.askfloat("Ducking", "Kết thúc (s):", parent=root)
        v = simpledialog.askfloat("Ducking", "Volume BGM trong đoạn (0–1):", initialvalue=0.15, parent=root)
        if a is None or b is None or v is None:
            return
        try:
            amix_mgr.add_ducking_range(project, a, b, v)
            pm.save_project(project)
            notify("Đã thêm vùng ducking BGM.")
        except Exception as e:
            messagebox.showerror("Ducking", str(e))

    ttk.Button(p2_fr, text="Thêm vùng ducking BGM", command=add_duck).grid(row=7, column=1, columnspan=2, sticky="w", pady=(4, 0))

    stock_paths_mem: list[str] = []

    def refresh_stock_audio_box() -> None:
        nonlocal stock_paths_mem
        stock_paths_mem = [str(p) for p in list_stock_audio_paths(mm._paths)]
        lb_stock.delete(0, tk.END)
        for p in stock_paths_mem:
            lb_stock.insert(tk.END, Path(p).name)

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
        sel = lb_stock.curselection()
        if not sel:
            messagebox.showinfo("Stock audio", "Chọn một file trong danh sách thư viện.")
            return
        fp = stock_paths_mem[int(sel[0])]
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

    lf_stock = ttk.LabelFrame(p2_fr, text="Thư viện âm thanh có sẵn (stock_audio)", padding=6)
    lf_stock.grid(row=8, column=0, columnspan=3, sticky="ew", pady=(4, 6))
    ttk.Label(
        lf_stock,
        text=f"Thư mục: {stock_audio_dir_display_hint(mm._paths)} — chép file .mp3, .wav, .m4a… vào đây.",
        foreground="#555",
        font=("Segoe UI", 8),
        wraplength=420,
    ).pack(anchor="w")
    f_stock = ttk.Frame(lf_stock)
    f_stock.pack(fill=tk.BOTH, expand=True, pady=(4, 0))
    sb_stock = ttk.Scrollbar(f_stock)
    lb_stock = tk.Listbox(f_stock, height=6, width=50, selectmode=tk.SINGLE, yscrollcommand=sb_stock.set)
    sb_stock.config(command=lb_stock.yview)
    lb_stock.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    sb_stock.pack(side=tk.RIGHT, fill=tk.Y)
    bf_stock = ttk.Frame(lf_stock)
    bf_stock.pack(fill=tk.X, pady=(6, 0))
    ttk.Button(bf_stock, text="Làm mới", command=refresh_stock_audio_box).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(bf_stock, text="Mở thư mục", command=open_stock_audio_folder).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(bf_stock, text="Thêm vào Media", command=lambda: add_selected_stock_to_project(as_bgm=False)).pack(
        side=tk.LEFT, padx=(0, 4)
    )
    ttk.Button(bf_stock, text="Thêm Media + BGM", command=lambda: add_selected_stock_to_project(as_bgm=True)).pack(side=tk.LEFT)

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
    var_bg_fill = tk.BooleanVar(value=bool(_r_cfg0.get("background_fill_enabled", True)))
    var_bg_max = tk.StringVar(value=str(int(_r_cfg0.get("background_fill_max") or 8)))
    var_bg_iv = tk.StringVar(value=str(int(_r_cfg0.get("background_fill_interval_minutes") or 0)))
    bg_timer_ref: dict[str, Any] = {"id": None}

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

    lf_remote = ttk.LabelFrame(lf_stock, text="Kho âm thanh miễn phí — tự tải theo chủ đề", padding=6)
    lf_remote.pack(fill=tk.X, pady=(8, 0))
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
            root.after(
                0,
                lambda: lbl_remote_status.configure(text="Đang tự động làm đầy kho nền…"),
            )
            q = take_next_background_fill_topic(mm._paths)
            hits = gather_background_fill_hits(q, freesound_api_key=key, jamendo_client_id=jam)
            total_ok = 0
            last_err: Exception | None = None
            for i, hit in enumerate(hits[:_AUTO_DL_MAX_ATTEMPTS], start=1):
                if total_ok >= cap:
                    break
                try:
                    download_hit_to_stock(hit, freesound_api_key=key, paths=mm._paths)
                    total_ok += 1
                except Exception as e:
                    last_err = e
                tot = total_ok
                root.after(
                    0,
                    lambda tot=tot, i=i, cap=cap: lbl_remote_status.configure(
                        text=f"Làm đầy kho nền: {tot}/{cap} OK — đã thử {i} URL"
                    ),
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

            root.after(0, finish)

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
                root.after(
                    0,
                    lambda o=ok, ii=i: lbl_remote_status.configure(
                        text=f"Tự động tải vào kho: {o}/{target_ok} OK — đã thử {ii}/{n_try}"
                    ),
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

            root.after(0, finish)

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

            root.after(0, done)

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

            root.after(0, done)

        lbl_remote_status.configure(text="Đang tải…")
        threading.Thread(target=work, daemon=True).start()

    ttk.Button(r3, text="Tìm kiếm", command=do_remote_search).pack(side=tk.LEFT, padx=(0, 4))
    ttk.Button(r3, text="Tải về stock_audio", command=download_remote_selected).pack(side=tk.LEFT)

    root.after(2500, _start_background_fill_job)

    stock_audio_refresh_ref["fn"] = refresh_stock_audio_box
    refresh_stock_audio_box()

    ttk.Separator(p2_fr, orient=tk.HORIZONTAL).grid(row=9, column=0, columnspan=3, sticky="ew", pady=8)

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
        dur = simpledialog.askfloat("Transition", "Độ dài hiệu ứng (giây):", initialvalue=0.5, parent=root)
        if dur is None:
            return
        typ = simpledialog.askstring("Transition", "Loại (crossfade, slide_left, wipe, …):", initialvalue="crossfade", parent=root)
        if not typ:
            return
        try:
            tr_mgr.add_transition(project, ids[i], ids[i + 1], typ.strip(), float(dur))
            pm.save_project(project)
            notify("Đã thêm transition (xuất với xfade/acrossfade).")
        except Exception as e:
            messagebox.showerror("Transition", str(e))

    ttk.Button(p2_fr, text="Chuyển cảnh sang clip video kế", command=add_transition_ui).grid(
        row=10, column=0, columnspan=2, sticky="w"
    )

    def gen_waveform() -> None:
        if not project:
            return
        sel = tree_media.selection()
        if not sel:
            messagebox.showinfo("Waveform", "Chọn một dòng trong bảng Media trước.")
            return
        media = _find_media(sel[0])
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
        out = ensure_video_editor_layout()["waveforms"] / f"{sel[0]}_wave.png"
        try:
            wf_gen.generate_waveform(str(p), str(out), ffmpeg_bin=ffmpeg_bin)
            notify(f"Waveform: {out.name}")
        except Exception as e:
            messagebox.showerror("Waveform", str(e))

    def gen_proxy() -> None:
        if not project:
            return
        sel = tree_media.selection()
        if not sel:
            messagebox.showinfo("Proxy", "Chọn media video.")
            return
        media = _find_media(sel[0])
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

    ttk.Button(p2_fr, text="Waveform ảnh", command=gen_waveform).grid(row=11, column=0, sticky="w", pady=(8, 0))
    ttk.Button(p2_fr, text="Tạo proxy preview", command=gen_proxy).grid(row=11, column=1, sticky="w", padx=4, pady=(8, 0))

    def save_tpl() -> None:
        if not project:
            return
        name = simpledialog.askstring("Template", "Tên template:", parent=root)
        if not name:
            return
        try:
            tmplate_mgr.save_template(project, name.strip())
            pm.save_project(project)
            notify("Đã lưu template.")
        except Exception as e:
            messagebox.showerror("Template", str(e))

    def apply_tpl() -> None:
        if not project:
            return
        opts = [t["id"] for t in tmplate_mgr.list_templates()]
        if not opts:
            messagebox.showinfo("Template", "Chưa có template.")
            return
        tid = simpledialog.askstring("Template", f"ID template ({', '.join(opts[:5])}…):", parent=root)
        if not tid:
            return
        try:
            tmplate_mgr.apply_template(project, tid.strip())
            pm.save_project(project)
            refresh_all()
            notify("Đã áp template — kiểm tra timeline/media.")
        except Exception as e:
            messagebox.showerror("Template", str(e))

    ttk.Button(p2_fr, text="Lưu template", command=save_tpl).grid(row=12, column=0, sticky="w", pady=(6, 0))
    ttk.Button(p2_fr, text="Áp template", command=apply_tpl).grid(row=12, column=1, sticky="w", padx=4, pady=(6, 0))

    ttk.Separator(p2_fr, orient=tk.HORIZONTAL).grid(row=13, column=0, columnspan=3, sticky="ew", pady=8)
    ttk.Label(p2_fr, text="Canvas dự án (khung xuất):").grid(row=14, column=0, sticky="w")
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
    cb_aspect.grid(row=14, column=1, columnspan=2, sticky="w")
    cb_aspect.bind("<<ComboboxSelected>>", apply_aspect_pick)

    ttk.Label(p2_fr, text="Âm thanh khi xuất:").grid(row=15, column=0, sticky="w", pady=(6, 0))
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
    cb_audio_mode.grid(row=15, column=1, sticky="w", pady=(6, 0))
    cb_audio_mode.bind("<<ComboboxSelected>>", apply_audio_mode)
    ttk.Label(
        p2_fr,
        text="mix = giữ tiếng clip + BGM; replace = tắt tiếng gốc (dùng với nhạc nền).",
        foreground="#555",
        font=("Segoe UI", 8),
        wraplength=320,
    ).grid(row=16, column=0, columnspan=3, sticky="w")

    def sync_p2_ui() -> None:
        if not project:
            return
        merge_phase2_defaults(project)
        var_audio_mode.set(str(project.get("audio_mode") or "mix"))
        _sync_aspect_combo_from_project()

    sync_p2_ui_ref["fn"] = sync_p2_ui

    exp_fr = ttk.LabelFrame(right, text="Xuất video (Export)", padding=4)
    right.add(exp_fr, weight=1)
    exp_inner = _pack_scrollable_vertical(exp_fr)
    var_outname = tk.StringVar(value="export.mp4")
    ttk.Label(exp_inner, text="Tên file MP4 (lưu trong folder renders của Video Editor):").grid(row=0, column=0, sticky="w")
    ttk.Entry(exp_inner, textvariable=var_outname, width=28).grid(row=0, column=1, sticky="ew", padx=4)

    var_preset = tk.StringVar(value="veryfast / crf 23")
    quality_labels = [x[0] for x in QUALITY_EXPORT_ITEMS]
    var_quality_pick = tk.StringVar(value=quality_labels[1])

    def sync_quality_combo_from_project() -> None:
        if not project:
            return
        ex = project.get("export") or {}
        lab = _match_quality_export_label(str(ex.get("preset", "veryfast")), ex.get("crf", 23))
        var_quality_pick.set(lab)

    def apply_quality_pick(_e: Any = None) -> None:
        if not project:
            return
        picked = var_quality_pick.get()
        for lab, preset, crf in QUALITY_EXPORT_ITEMS:
            if lab == picked:
                exp = project.setdefault("export", {})
                exp["preset"] = preset
                exp["crf"] = int(crf)
                merge_phase2_defaults(project)
                pm.save_project(project)
                var_preset.set(f"{preset} / crf {crf}")
                notify(f"Chất lượng xuất: {preset}, CRF {crf}.")
                return

    ttk.Label(exp_inner, text="Chất lượng video (preset + CRF):").grid(row=1, column=0, sticky="nw")
    cb_quality = ttk.Combobox(
        exp_inner,
        textvariable=var_quality_pick,
        values=quality_labels,
        width=38,
        state="readonly",
    )
    cb_quality.grid(row=1, column=1, sticky="ew", padx=4)
    cb_quality.bind("<<ComboboxSelected>>", apply_quality_pick)

    ttk.Label(exp_inner, text="Chi tiết encode hiện tại:").grid(row=2, column=0, sticky="nw")
    ttk.Label(exp_inner, textvariable=var_preset, foreground="gray").grid(row=2, column=1, sticky="w")
    ttk.Label(
        exp_inner,
        text=(
            "Chọn mức nhanh → file nhỏ; mức cao → chất lượng tốt, render lâu hơn. "
            "Tab «Dự án: nhạc · phụ đề · preset» có canvas + codec; «Lưu dự án» giữ thiết lập."
        ),
        foreground="#555",
        font=("Segoe UI", 8),
        wraplength=320,
        justify="left",
    ).grid(row=3, column=0, columnspan=2, sticky="w", pady=(2, 4))
    exp_inner.columnconfigure(1, weight=1)

    prog = ttk.Progressbar(exp_inner, maximum=100, length=280)
    prog.grid(row=4, column=0, columnspan=2, sticky="ew", pady=6)
    lbl_prog = ttk.Label(exp_inner, text="")
    lbl_prog.grid(row=5, column=0, columnspan=2, sticky="w")
    var_outpath = tk.StringVar(value="")
    ttk.Label(exp_inner, textvariable=var_outpath, wraplength=320, foreground="gray").grid(row=6, column=0, columnspan=2, sticky="w")

    add_lib_flag = tk.BooleanVar(value=True)

    def open_renders_folder() -> None:
        r = ensure_video_editor_layout()["renders"]
        r.mkdir(parents=True, exist_ok=True)
        try:
            if os.name == "nt":
                os.startfile(str(r))  # type: ignore[attr-defined]
            else:
                subprocess.Popen(["xdg-open", str(r)])
        except Exception as e:
            messagebox.showerror("Folder", str(e))

    def run_export() -> None:
        nonlocal project
        if not project:
            messagebox.showinfo("Export", "Chưa có project.")
            return
        ffmpeg_bin = resolve_ffmpeg_executable()
        pid = str(project.get("id") or "out")
        safe = "".join(c for c in pid if c.isalnum() or c in "-_")[:48]
        name = var_outname.get().strip() or f"{safe}.mp4"
        if not name.lower().endswith(".mp4"):
            name += ".mp4"
        out_p = ensure_video_editor_layout()["renders"] / name
        duration = float(project.get("duration") or 0)

        errs = validate_export(project, ffmpeg_path=ffmpeg_bin, output_path=str(out_p), media_resolver=mm)
        if errs:
            messagebox.showerror("Không export được", "\n".join(errs))
            return

        ass_path: str | None = None
        if project.get("subtitles"):
            try:
                ass_path = sub_mgr.export_ass(
                    project,
                    str(ensure_video_editor_layout()["subtitles"] / f"{safe}_burn.ass"),
                )
            except Exception as e:
                messagebox.showwarning("Subtitle ASS", str(e))

        try:
            cmd = builder.build_export_command(
                project,
                str(out_p),
                ffmpeg_bin=ffmpeg_bin or "ffmpeg",
                ass_path=ass_path,
            )
        except Exception as e:
            messagebox.showerror("FFmpeg command", str(e))
            return

        prog["value"] = 0
        lbl_prog.configure(text="Đang render…")
        var_outpath.set(str(out_p))

        def on_prog(x: float) -> None:
            def ui() -> None:
                prog["value"] = min(99.0, x * 100.0)
                lbl_prog.configure(text=f"{int(prog['value'])} %")

            root.after(0, ui)

        def done(res: dict[str, Any]) -> None:
            def ui() -> None:
                if res.get("ok"):
                    prog["value"] = 100
                    lbl_prog.configure(text="Hoàn tất.")
                    notify(f"Export xong: {out_p.name}")
                    if add_lib_flag.get():
                        try:
                            add_editor_export_to_library(
                                project_id=str(project.get("id") or ""),
                                output_video_path=str(out_p),
                                title=str(project.get("name") or ""),
                                duration_sec=duration,
                            )
                            notify("Đã thêm file export vào Video Library.")
                        except Exception as ex:
                            messagebox.showwarning("Video Library", f"Export OK nhưng không ghi thư viện: {ex}")
                else:
                    lbl_prog.configure(text="Lỗi.")
                    messagebox.showerror(
                        "Export",
                        res.get("error_message") or "Lỗi không xác định.",
                    )

            root.after(0, ui)

        worker.render_thread(
            project,
            str(out_p),
            cmd,
            duration_sec=max(0.1, duration),
            progress_callback=on_prog,
            done_callback=done,
        )

    ttk.Button(exp_inner, text="Xuất MP4", command=run_export).grid(row=7, column=0, sticky="w", pady=(6, 2))
    ttk.Checkbutton(
        exp_inner,
        text="Sau khi xuất xong, thêm file vào Video Library",
        variable=add_lib_flag,
    ).grid(row=8, column=0, columnspan=2, sticky="w")
    ttk.Button(exp_inner, text="Mở folder chứa file đã render", command=open_renders_folder).grid(
        row=9, column=0, sticky="w", pady=(4, 0)
    )

    batch_in_var = tk.StringVar(value="")
    batch_logo_var = tk.StringVar(value="")
    batch_audio_var = tk.StringVar(value="")
    batch_out_var = tk.StringVar(value="")
    var_batch_replace_audio = tk.BooleanVar(value=True)

    lf_batch = ttk.LabelFrame(
        exp_inner,
        text="Xuất hàng loạt — cùng logo & nhạc cho mọi video trong thư mục",
        padding=6,
    )
    lf_batch.grid(row=10, column=0, columnspan=2, sticky="ew", pady=(14, 4))
    rba = ttk.Frame(lf_batch)
    rba.pack(fill=tk.X)
    ttk.Label(rba, text="Thư mục video vào:", width=18).pack(side=tk.LEFT)
    ttk.Entry(rba, textvariable=batch_in_var, width=36).pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)
    ttk.Button(rba, text="Chọn…", width=6, command=lambda: _pick_batch_dir(batch_in_var)).pack(side=tk.LEFT)
    rbb = ttk.Frame(lf_batch)
    rbb.pack(fill=tk.X, pady=(4, 0))
    ttk.Label(rbb, text="Logo (ảnh):", width=18).pack(side=tk.LEFT)
    ttk.Entry(rbb, textvariable=batch_logo_var, width=36).pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)
    ttk.Button(
        rbb,
        text="Chọn…",
        width=6,
        command=lambda: _pick_batch_file(
            batch_logo_var,
            [
                ("Images", "*.png *.jpg *.jpeg *.webp"),
                ("PNG", "*.png"),
                ("JPEG", "*.jpg;*.jpeg"),
                ("WebP", "*.webp"),
                ("All", "*.*"),
            ],
        ),
    ).pack(side=tk.LEFT)
    rbc = ttk.Frame(lf_batch)
    rbc.pack(fill=tk.X, pady=(4, 0))
    ttk.Label(rbc, text="Nhạc nền:", width=18).pack(side=tk.LEFT)
    ttk.Entry(rbc, textvariable=batch_audio_var, width=36).pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)
    ttk.Button(
        rbc,
        text="Chọn…",
        width=6,
        command=lambda: _pick_batch_file(
            batch_audio_var,
            [
                ("Audio", "*.mp3 *.wav *.m4a *.aac *.ogg *.flac"),
                ("MP3", "*.mp3"),
                ("WAV", "*.wav"),
                ("All", "*.*"),
            ],
        ),
    ).pack(side=tk.LEFT)
    rbd = ttk.Frame(lf_batch)
    rbd.pack(fill=tk.X, pady=(4, 0))
    ttk.Label(rbd, text="Thư mục xuất MP4:", width=18).pack(side=tk.LEFT)
    ttk.Entry(rbd, textvariable=batch_out_var, width=36).pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)
    ttk.Button(rbd, text="Chọn…", width=6, command=lambda: _pick_batch_out(batch_out_var)).pack(side=tk.LEFT)
    rbe = ttk.Frame(lf_batch)
    rbe.pack(fill=tk.X, pady=(6, 0))
    ttk.Checkbutton(
        rbe,
        text="Tắt tiếng video gốc — chỉ nhạc nền (replace)",
        variable=var_batch_replace_audio,
    ).pack(side=tk.LEFT)
    ttk.Label(
        lf_batch,
        text=(
            "Canvas & chất lượng encode lấy từ dự án đang mở (nếu có); không có thì 1080×1920, veryfast. "
            "Logo ~15% chiều ngang, góc trên trái. Nhạc lặp nếu ngắn hơn video."
        ),
        foreground="#555",
        font=("Segoe UI", 8),
        wraplength=400,
        justify="left",
    ).pack(anchor="w", pady=(4, 0))
    lbl_batch_status = ttk.Label(lf_batch, text="", foreground="gray", font=("Segoe UI", 8))
    lbl_batch_status.pack(anchor="w", pady=(4, 0))

    def _pick_batch_dir(var: tk.StringVar) -> None:
        d = filedialog.askdirectory(title="Thư mục chứa file video", parent=root)
        if d:
            var.set(d)

    def _pick_batch_out(var: tk.StringVar) -> None:
        d = filedialog.askdirectory(title="Thư mục lưu MP4 đã xử lý", parent=root)
        if d:
            var.set(d)

    def _pick_batch_file(var: tk.StringVar, patterns: list[tuple[str, str]]) -> None:
        p = filedialog.askopenfilename(title="Chọn file", filetypes=patterns, parent=root)
        if p:
            var.set(p)

    btn_batch_run: dict[str, Any] = {}

    def run_batch_export() -> None:
        in_dir = Path(batch_in_var.get().strip())
        out_dir = Path(batch_out_var.get().strip())
        logo_s = batch_logo_var.get().strip()
        audio_s = batch_audio_var.get().strip()
        if not in_dir.is_dir():
            messagebox.showerror("Hàng loạt", "Chọn thư mục chứa video đầu vào.")
            return
        if not out_dir.is_dir():
            messagebox.showerror("Hàng loạt", "Chọn thư mục xuất (hoặc tạo thư mục rồi chọn lại).")
            return
        vids = list_videos_in_folder(in_dir)
        if not vids:
            messagebox.showinfo("Hàng loạt", "Không thấy file video (mp4, mov, mkv…) trong thư mục.")
            return
        if not logo_s and not audio_s:
            messagebox.showwarning("Hàng loạt", "Chọn ít nhất logo hoặc file nhạc nền.")
            return
        logo_p = Path(logo_s) if logo_s else None
        audio_p = Path(audio_s) if audio_s else None
        if logo_p is not None and not logo_p.is_file():
            messagebox.showerror("Hàng loạt", f"Không tìm thấy logo:\n{logo_p}")
            return
        if audio_p is not None and not audio_p.is_file():
            messagebox.showerror("Hàng loạt", f"Không tìm thấy file nhạc:\n{audio_p}")
            return
        ffmpeg_bin = resolve_ffmpeg_executable()
        if not ffmpeg_bin:
            messagebox.showerror("Hàng loạt", "Không tìm thấy ffmpeg.")
            return
        tpl = copy.deepcopy(project) if project else None
        mode = "replace" if var_batch_replace_audio.get() else "mix"
        if audio_p is None:
            mode = "mix"
        vol_bgm = float(var_bgm_vol.get().strip() or "0.25")
        n = len(vids)
        btn = btn_batch_run.get("btn")
        if btn is not None:
            btn.configure(state=tk.DISABLED)

        def work() -> None:
            ok = 0
            failures: list[str] = []
            for i, vp in enumerate(vids):
                pid = ""

                def ui_prog(msg: str) -> None:
                    root.after(0, lambda m=msg: lbl_batch_status.configure(text=m))

                ui_prog(f"Đang xử lý {i + 1}/{n}: {vp.name}…")
                try:
                    bproj = create_branded_project_for_video(
                        vp,
                        template_project=tpl,
                        logo_path=logo_p,
                        audio_path=audio_p,
                        audio_mode=mode,
                        bgm_volume=vol_bgm,
                        copy_inputs_to_library=False,
                        paths=mm._paths,
                    )
                    pid = str(bproj.get("id") or "")
                    out_p = out_dir / f"{vp.stem}.mp4"
                    errs = validate_export(
                        bproj,
                        ffmpeg_path=ffmpeg_bin,
                        output_path=str(out_p),
                        media_resolver=mm,
                    )
                    if errs:
                        failures.append(f"{vp.name}: {'; '.join(errs)}")
                        continue
                    try:
                        cmd = builder.build_export_command(
                            bproj,
                            str(out_p),
                            ffmpeg_bin=ffmpeg_bin,
                            ass_path=None,
                        )
                    except Exception as ex:
                        failures.append(f"{vp.name}: {ex}")
                        continue
                    dur = float(bproj.get("duration") or 0)

                    def on_p(x: float) -> None:
                        frac = (i + x) / max(1.0, float(n))
                        root.after(0, lambda f=frac: prog.configure(value=min(99.0, f * 100.0)))

                    res = worker.render(
                        bproj,
                        str(out_p),
                        cmd,
                        duration_sec=max(0.1, dur),
                        progress_callback=on_p,
                    )
                    if res.get("ok"):
                        ok += 1
                    else:
                        failures.append(f"{vp.name}: {res.get('error_message') or 'Lỗi FFmpeg'}")
                except Exception as ex:
                    failures.append(f"{vp.name}: {ex}")
                finally:
                    if pid:
                        try:
                            pm.delete_project(pid)
                        except Exception:
                            pass

            def done_ui() -> None:
                prog.configure(value=100 if ok == n else prog["value"])
                lbl_batch_status.configure(text=f"Xong: {ok}/{n} file.")
                if btn is not None:
                    btn.configure(state=tk.NORMAL)
                if failures:
                    messagebox.showwarning(
                        "Hàng loạt",
                        f"Thành công {ok}/{n}.\n\nLỗi:\n" + "\n".join(failures[:12])
                        + (f"\n… (+{len(failures) - 12})" if len(failures) > 12 else ""),
                    )
                else:
                    notify(f"Hàng loạt: đã xuất {ok} file vào {out_dir}")

            root.after(0, done_ui)

        threading.Thread(target=work, daemon=True).start()

    btn_b = ttk.Button(lf_batch, text="Bắt đầu xuất hàng loạt", command=run_batch_export)
    btn_b.pack(anchor="w", pady=(6, 0))
    btn_batch_run["btn"] = btn_b

    sync_export_quality_ref["fn"] = sync_quality_combo_from_project

    # load first project if any
    if project_ids:
        load_project_id(project_ids[0])
