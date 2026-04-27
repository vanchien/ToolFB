from __future__ import annotations

import json
import os
import re
import uuid
import hashlib
import threading
import platform
from datetime import datetime
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk
from typing import Any
try:
    from PIL import Image, ImageTk  # type: ignore
except Exception:  # noqa: BLE001
    Image = None  # type: ignore[assignment]
    ImageTk = None  # type: ignore[assignment]

from src.services.ai_video_config import load_ai_video_config
from src.services.ai_video_generation_service import AIVideoGenerationService
from src.ai.image_generation import (
    _canonical_nano_banana_pro_gemini_model,
    generate_post_images_nano_banana_browser,
)
from src.services.ai_image_config import nano_banana_pro_settings
from src.services.ai_image_service import AIImageService
from src.services.character_profile_normalize import (
    migrate_auto_character_profiles,
    normalize_character_image_generations,
)
from src.services.ai_video_prompt_presets import load_prompt_presets, save_prompt_presets
from src.services.ai_provider_factory import AIProviderFactory
from src.services.ai_styles_registry import (
    default_style_id,
    load_style_registry,
    style_items,
    style_name,
    style_prompt_addon,
)
from src.services.auto_style_selector import AutoStyleSelector
from src.services.ai_video_store import (
    ai_video_project_output_dir,
    clear_prepared_prompt_preview,
    delete_ai_video_project_output_dir,
    delete_ai_video_project_file,
    load_ai_video_project_file,
    load_prepared_prompt_preview,
    list_ai_video_project_summaries,
    save_ai_video_project_file,
    save_prepared_prompt_preview,
    write_resilient_text_file,
    ensure_ai_video_layout,
)
from src.services.ai_video_styles import load_video_styles, save_video_styles
from src.services.google_flow_veo_generate import GOOGLE_FLOW_URL, sync_flow_model_choices_from_profile
from src.services.text_to_video_prompt_builder import TextToVideoPromptBuilder
from src.utils.paths import project_root


def _child_name_gender_slots(story_lower: str, child_count: int, *, is_vi: bool) -> list[tuple[str, str]]:
    """
    Gán tên + gender cho từng slot trẻ em theo ngữ cảnh (vd: 2 con + một trai một gái -> Con trai, Con gái).

    Trả về list cùng độ dài child_count; mỗi phần tử là (tên_hiển_thị, gender) với gender: male|female|unspecified.
    """
    if child_count <= 0:
        return []
    low = story_lower
    # Một trai một gái / anh em khác giới (không kích hoạt nếu chỉ có "2 con" mơ hồ).
    mixed_pair = any(
        p in low
        for p in (
            "một trai một gái",
            "mot trai mot gai",
            "1 trai 1 gái",
            "1 trai 1 gai",
            "trai và gái",
            "trai va gai",
            "trai, gái",
            "trai, gai",
            "con trai và con gái",
            "con trai con gái",
            "bé trai và bé gái",
            "be trai va be gai",
            "anh trai em gái",
            "anh trai em gai",
            "chị gái em trai",
            "chi gai em trai",
            "hai con một trai một gái",
            "hai con mot trai mot gai",
            "đứa trai và đứa gái",
            "dua trai va dua gai",
            "đứa trai đứa gái",
            "dua trai dua gai",
            "boy and girl",
            "son and daughter",
        )
    ) or ("đứa trai" in low and "đứa gái" in low) or ("dua trai" in low and "dua gai" in low)
    if not mixed_pair and "con trai" in low and "con gái" in low:
        mixed_pair = True
    if child_count == 2 and mixed_pair:
        if is_vi:
            return [("Con trai", "male"), ("Con gái", "female")]
        return [("Son", "male"), ("Daughter", "female")]
    two_boys = any(
        p in low
        for p in (
            "hai con trai",
            "2 con trai",
            "hai bé trai",
            "2 bé trai",
            "hai anh em trai",
            "hai cậu bé",
            "hai cau be",
            "two sons",
            "two boys",
        )
    )
    if child_count == 2 and two_boys:
        if is_vi:
            return [("Con trai 1", "male"), ("Con trai 2", "male")]
        return [("Boy 1", "male"), ("Boy 2", "male")]
    two_girls = any(
        p in low
        for p in (
            "hai con gái",
            "2 con gái",
            "hai bé gái",
            "2 bé gái",
            "hai chị em gái",
            "two daughters",
            "two girls",
        )
    )
    if child_count == 2 and two_girls:
        if is_vi:
            return [("Con gái 1", "female"), ("Con gái 2", "female")]
        return [("Girl 1", "female"), ("Girl 2", "female")]
    return [(f"Con {i + 1}" if is_vi else f"Child {i + 1}", "unspecified") for i in range(child_count)]


def _story_triggers_family_cast(story_lower: str) -> bool:
    """
    True nếu kịch bản gợi ý nhiều vai gia đình (không bắt buộc phải có chữ 'gia đình').

    Tránh dùng 'ông'/'bà' rời vì dễ trùng tiếng Việt khác; ưu tiên cụm rõ nghĩa.
    """
    low = story_lower
    if any(k in low for k in ("gia đình", "family", "grandparent", "grandma", "grandpa")):
        return True
    if "ông bà" in low or "ong ba" in low:
        return True
    if "bố mẹ" in low or "bo me" in low:
        return True
    if re.search(r"\b\d+\s*người\s*con\b", low):
        return True
    if re.search(r"\b(hai|ba|bốn|tư|năm)\s+người\s*con\b", low):
        return True
    # Kịch bản nông trại / gia đình liệt kê nhân vật (kể cả gõ nhầm 'nuông trại').
    farm_kw = ("nông trại", "nuông trại", "nong trai", "nuong trai", "trang trại", "trang trai", "farm")
    if any(k in low for k in farm_kw) and any(
        k in low for k in ("nhân vật", "nhan vat", "ông bà", "bố mẹ", "người con", "cún", "chó", "chim")
    ):
        return True
    if "nhân vật" in low and any(
        k in low for k in ("ông bà", "ong ba", "bố mẹ", "bo me", "người con", "cún", "chó", "ông và bà")
    ):
        return True
    return False


class _SimpleTooltip:
    def __init__(self, widget: tk.Widget, text: str) -> None:
        self._widget = widget
        self._text = text
        self._tip: tk.Toplevel | None = None
        self._widget.bind("<Enter>", self._on_enter, add="+")
        self._widget.bind("<Leave>", self._on_leave, add="+")

    def _on_enter(self, _event: tk.Event) -> None:
        if self._tip is not None:
            return
        x = self._widget.winfo_rootx() + 12
        y = self._widget.winfo_rooty() + self._widget.winfo_height() + 6
        tip = tk.Toplevel(self._widget)
        tip.wm_overrideredirect(True)
        tip.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            tip,
            text=self._text,
            justify=tk.LEFT,
            background="#fff9db",
            foreground="#1f2937",
            relief=tk.SOLID,
            borderwidth=1,
            padx=6,
            pady=4,
            wraplength=420,
            font=("Segoe UI", 9),
        )
        label.pack()
        self._tip = tip

    def _on_leave(self, _event: tk.Event) -> None:
        if self._tip is not None:
            self._tip.destroy()
            self._tip = None


def ai_video_project_gate_dialog(parent: tk.Misc) -> dict[str, Any] | None:
    """
    Bước đầu khi mở AI Video: chọn dự án có sẵn, tạo mới, hoặc hủy.
    Trả về ``{project_id, project_name, payload}`` với ``payload`` đầy đủ khi mở file dự án; ``None`` nếu hủy.
    """
    svc = AIVideoGenerationService()
    top = tk.Toplevel(parent)
    top.title("AI Video — Chọn dự án")
    top.geometry("560x460")
    top.minsize(480, 380)
    try:
        top.transient(parent)  # type: ignore[arg-type]
    except Exception:
        pass
    top.grab_set()

    decision: dict[str, Any] | None = None

    root = ttk.Frame(top, padding=10)
    root.pack(fill=tk.BOTH, expand=True)
    ttk.Label(
        root,
        text="Chọn dự án để tiếp tục (form, prompt preview, job sẽ gắn với dự án).\nHoặc tạo dự án mới để bắt đầu từ đầu.",
        wraplength=520,
        justify=tk.LEFT,
    ).pack(anchor="w", pady=(0, 8))

    lb_fr = ttk.Frame(root)
    lb_fr.pack(fill=tk.BOTH, expand=True, pady=(0, 8))
    sy = ttk.Scrollbar(lb_fr, orient=tk.VERTICAL)
    lb = tk.Listbox(lb_fr, height=12, exportselection=False, yscrollcommand=sy.set, font=("Segoe UI", 10))
    sy.config(command=lb.yview)
    lb.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    sy.pack(side=tk.RIGHT, fill=tk.Y)

    id_by_index: list[str] = []

    def refresh_list() -> None:
        lb.delete(0, tk.END)
        id_by_index.clear()
        rows = list_ai_video_project_summaries()
        if not rows:
            lb.insert(tk.END, "(Chưa có dự án nào — tạo mới bên dưới)")
            return
        for r in rows:
            pid = str(r.get("project_id", "")).strip()
            nm = str(r.get("project_name", "") or "").strip() or "Dự án"
            upd = str(r.get("updated_at", "") or "").strip()
            nj = int(r.get("video_job_count", 0) or 0)
            line = f"{nm}  |  {pid}  |  {nj} job  |  {upd[:19]}"
            lb.insert(tk.END, line)
            id_by_index.append(pid)

    refresh_list()

    new_fr = ttk.LabelFrame(root, text="Dự án mới", padding=6)
    new_fr.pack(fill=tk.X, pady=(0, 8))
    ttk.Label(new_fr, text="Tên dự án:").pack(side=tk.LEFT, padx=(0, 6))
    ent_name = ttk.Entry(new_fr, width=36)
    ent_name.pack(side=tk.LEFT, fill=tk.X, expand=True)
    ent_name.insert(0, f"Dự án {datetime.now().strftime('%d/%m %H:%M')}")

    btn_row = ttk.Frame(root)
    btn_row.pack(fill=tk.X, pady=(4, 0))

    def finish(spec: dict[str, Any] | None) -> None:
        nonlocal decision
        decision = spec
        try:
            top.grab_release()
        except Exception:
            pass
        top.destroy()

    def do_open() -> None:
        sel = lb.curselection()
        if not sel or not id_by_index:
            messagebox.showwarning("Dự án", "Chọn một dự án trong danh sách.", parent=top)
            return
        idx = int(sel[0])
        if idx < 0 or idx >= len(id_by_index):
            return
        pid = id_by_index[idx]
        pl = load_ai_video_project_file(pid)
        if not isinstance(pl, dict) or not pl:
            messagebox.showerror("Dự án", "Không đọc được file dự án.", parent=top)
            return
        finish(
            {
                "project_id": str(pl.get("project_id", pid)).strip(),
                "project_name": str(pl.get("project_name", "") or "").strip() or "Dự án",
                "payload": dict(pl),
            }
        )

    def do_open_project_output() -> None:
        sel = lb.curselection()
        if not sel or not id_by_index:
            messagebox.showwarning("Dự án", "Chọn một dự án trong danh sách.", parent=top)
            return
        idx = int(sel[0])
        if idx < 0 or idx >= len(id_by_index):
            return
        pid = id_by_index[idx]
        out = ai_video_project_output_dir(pid)
        try:
            os.startfile(str(out))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Dự án", f"Không mở được thư mục output:\n{exc}", parent=top)

    def do_new() -> None:
        pid = uuid.uuid4().hex[:10]
        nm = ent_name.get().strip() or f"Dự án {datetime.now().strftime('%d/%m %H:%M')}"
        finish({"project_id": pid, "project_name": nm, "payload": {}})

    def do_delete() -> None:
        sel = lb.curselection()
        if not sel or not id_by_index:
            messagebox.showwarning("Dự án", "Chọn một dự án để xóa.", parent=top)
            return
        idx = int(sel[0])
        if idx < 0 or idx >= len(id_by_index):
            return
        pid = id_by_index[idx]
        nm = ""
        try:
            pl = load_ai_video_project_file(pid)
            if isinstance(pl, dict):
                nm = str(pl.get("project_name", "") or "").strip()
        except Exception:
            pass
        if not messagebox.askyesno(
            "Xóa dự án",
            f"Xóa dự án '{nm or pid}'?\n\n"
            "Có thể xóa luôn các job video trong bảng trạng thái gắn với dự án này.",
            parent=top,
        ):
            return
        del_jobs = messagebox.askyesno(
            "Xóa job video",
            "Xóa luôn các bản ghi video (metadata) thuộc dự án này?\nChọn Không nếu chỉ xóa file dự án (giữ lịch sử job).",
            parent=top,
        )
        del_outputs = messagebox.askyesno(
            "Xóa output video",
            "Xóa luôn thư mục output video của dự án này?\n"
            "Thư mục sẽ bị xóa: data/ai_video/outputs/{project_id}",
            parent=top,
        )
        if del_jobs:
            n = svc.delete_records_for_project(pid)
            messagebox.showinfo("Dự án", f"Đã xóa {n} bản ghi video.", parent=top)
        if del_outputs:
            ok_out = delete_ai_video_project_output_dir(pid)
            if not ok_out:
                messagebox.showwarning(
                    "Dự án",
                    "Không tìm thấy hoặc không xóa được thư mục output của dự án.",
                    parent=top,
                )
        delete_ai_video_project_file(pid)
        refresh_list()

    ttk.Button(btn_row, text="Mở dự án đã chọn", command=do_open).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(btn_row, text="Mở output dự án", command=do_open_project_output).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(btn_row, text="Tạo dự án mới", command=do_new).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(btn_row, text="Xóa dự án", command=do_delete).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(btn_row, text="Hủy", command=lambda: finish(None)).pack(side=tk.RIGHT)

    lb.bind("<Double-Button-1>", lambda _e: do_open())

    top.protocol("WM_DELETE_WINDOW", lambda: finish(None))
    top.wait_window()
    return decision


class AIVideoDialog:
    _STYLE_NONE = "(Không dùng style)"
    _STYLE_AUTO = "Auto (theo ý tưởng/prompt)"
    _PRESET_AUTO = "Tự động theo ý tưởng/prompt (auto)"

    def __init__(self, parent: tk.Tk | tk.Toplevel, *, project_spec: dict[str, Any]) -> None:
        self._svc = AIVideoGenerationService()
        self._img_svc = AIImageService()
        self._cfg = load_ai_video_config()
        self._t2v_builder = TextToVideoPromptBuilder()
        self._prepared_requests: list[dict[str, Any]] = []
        self._prepared_signature = ""
        self._preview_worker_running = False
        self._var_preview_progress = tk.StringVar(value="")
        self._top = tk.Toplevel(parent)
        self._top.title("AI Video Gemini/Veo")
        self._top.geometry("1080x760")
        self._top.minsize(920, 680)

        root = ttk.Frame(self._top, padding=8)
        root.pack(fill=tk.BOTH, expand=True)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(5, weight=1)

        self._mode_registry = dict(self._cfg.get("modes") or {})
        self._mode_display_to_key, self._mode_key_to_display = self._build_mode_maps()
        self._model_choices = self._build_model_choices()
        self._prompt_presets = load_prompt_presets()
        self._sync_models_running = False
        self._status_filter = tk.StringVar(value="all")
        self._status_view = tk.StringVar(value="grid")
        self._selected_grid_video_id = ""

        default_mode = str(self._cfg.get("default_mode", "text_to_video")).strip() or "text_to_video"
        default_mode_display = self._mode_key_to_display.get(default_mode) or next(iter(self._mode_display_to_key), "text_to_video")
        default_model = (
            os.environ.get("VEO_MODEL", "").strip()
            or os.environ.get("GEMINI_VIDEO_MODEL", "").strip()
            or str(self._cfg.get("providers", {}).get("gemini", {}).get("default_model", "")).strip()
            or "veo-3.1-generate-preview"
        )

        self._var_provider = tk.StringVar(value="gemini")
        self._var_model = tk.StringVar(value=self._to_flow_model_label(default_model))
        self._var_mode_display = tk.StringVar(value=default_mode_display)
        self._var_mode_help = tk.StringVar(value="")
        self._var_style = tk.StringVar(value=self._STYLE_AUTO)
        self._var_style_prompt = tk.StringVar(value="")
        self._var_aspect = tk.StringVar(value="9:16")
        self._var_duration = tk.StringVar(value="8")
        self._var_resolution = tk.StringVar(value="720p")
        self._var_outputs = tk.StringVar(value="x1")
        self._var_count = tk.StringVar(value="1")
        self._var_language = tk.StringVar(value="Tiếng Việt")
        self._var_prompt_stats = tk.StringVar(value="Prompt: 0 | Video sẽ tạo: 0")
        self._var_output_dir = tk.StringVar(value=str((Path("data") / "ai_video" / "outputs").as_posix()))
        self._var_lock_output_by_project = tk.BooleanVar(value=True)
        self._var_ref_type = tk.StringVar(value="subject")
        self._var_topic = tk.StringVar(value="")
        self._var_goal = tk.StringVar(value="viral")
        self._var_visual_style = tk.StringVar(value=self._PRESET_AUTO)
        self._var_camera_style = tk.StringVar(value=self._PRESET_AUTO)
        self._var_lighting = tk.StringVar(value=self._PRESET_AUTO)
        self._var_motion_style = tk.StringVar(value=self._PRESET_AUTO)
        self._var_mood = tk.StringVar(value=self._PRESET_AUTO)
        self._var_auto_style_enable = tk.BooleanVar(value=False)
        self._var_auto_style_reason = tk.StringVar(value="")
        self._var_auto_style_mood = tk.StringVar(value="")
        self._var_ai_tag_visual = tk.StringVar(value="")
        self._var_ai_tag_mood = tk.StringVar(value="")
        self._var_ai_tag_camera = tk.StringVar(value="")
        self._var_ai_tag_lighting = tk.StringVar(value="")
        self._var_ai_tag_motion = tk.StringVar(value="")
        self._var_video_style_id = tk.StringVar(
            value=default_style_id("video_style_id", "video_cinematic_realistic")
        )
        self._var_camera_style_id = tk.StringVar(
            value=default_style_id("camera_style_id", "smooth_dolly_in")
        )
        self._var_lighting_style_id = tk.StringVar(
            value=default_style_id("lighting_style_id", "soft_natural_light")
        )
        self._var_motion_style_id = tk.StringVar(
            value=default_style_id("motion_style_id", "slow_and_smooth")
        )
        self._var_image_style_id = tk.StringVar(
            value=default_style_id("character_image_style_id", "character_cinematic_realistic")
        )
        self._var_character_image_style_id = tk.StringVar(
            value=default_style_id("character_image_style_id", "character_cinematic_realistic")
        )
        self._var_environment_style_id = tk.StringVar(
            value=default_style_id("environment_style_id", "environment_cinematic")
        )
        self._var_character_mode = tk.StringVar(value="Tự tạo (auto)")
        self._var_character_name = tk.StringVar(value="")
        self._var_character_appearance = tk.StringVar(value="")
        self._var_character_outfit = tk.StringVar(value="")
        self._var_character_personality = tk.StringVar(value="")
        self._reuse_character_profile = tk.BooleanVar(value=True)
        self._var_lock_character_roles = tk.BooleanVar(value=True)
        self._auto_character_profiles: list[dict[str, Any]] = []
        self._auto_char_thumb_refs: list[Any] = []
        self._var_auto_char_summary = tk.StringVar(value="")
        self._style_registry = load_style_registry()
        self._auto_style_selector = AutoStyleSelector(
            AIProviderFactory.text("gemini"),
            self._style_registry,
        )
        self._var_image = tk.StringVar(value="")
        self._var_first = tk.StringVar(value="")
        self._var_last = tk.StringVar(value="")
        self._var_refs = tk.StringVar(value="")
        self._var_src_video = tk.StringVar(value="")
        spec = dict(project_spec or {})
        pl0 = dict(spec.get("payload") or {}) if isinstance(spec.get("payload"), dict) else {}
        self._loaded_project_payload: dict[str, Any] = pl0
        self._current_project_id = str(spec.get("project_id") or "").strip() or uuid.uuid4().hex[:10]
        self._var_project_name = tk.StringVar(value=str(spec.get("project_name") or "Dự án").strip() or "Dự án")
        self._var_output_dir.set(self._default_output_dir_for_project(self._current_project_id))
        self._var_project_list_filter = tk.StringVar(value="Tất cả dự án")
        self._project_filter_id_list: list[str] = ["*"]
        self._filter_project_id = "*"
        self._last_status_view_fingerprint: str | None = None

        self._build_top_form(root)
        self._build_style_form(root)
        self._build_mode_form(root)
        self._build_action_bar(root)
        self._build_progress_panel(root)
        self._build_table(root)
        self._load_styles()
        if self._loaded_project_payload:
            self._load_project_from_disk_payload(self._loaded_project_payload)
        else:
            self._prepared_requests = []
            self._prepared_signature = ""
            clear_prepared_prompt_preview()
            self._refresh_prepared_preview_badge()
        self._filter_project_id = str(self._current_project_id or "*")
        self._last_status_view_fingerprint = None
        self._rebuild_project_filter_combo(self._svc.list_records())
        self._refresh_rows()
        self._on_mode_changed()
        self._start_auto_refresh()

        def _on_close_dialog_request() -> None:
            try:
                self._save_current_project_to_disk()
            except Exception:
                pass
            self._top.destroy()

        self._top.protocol("WM_DELETE_WINDOW", _on_close_dialog_request)
        try:
            self._save_current_project_to_disk()
        except Exception:
            pass
        self._ensure_dialog_size_fits_content()

    def _build_top_form(self, root: ttk.Frame) -> None:
        fr = ttk.LabelFrame(root, text="A — Thiết lập chính", padding=8)
        fr.grid(row=0, column=0, sticky="ew")
        fr.columnconfigure(1, weight=1)
        fr.columnconfigure(3, weight=1)
        fr.columnconfigure(5, weight=1)
        fr.columnconfigure(7, weight=1)
        lbl_provider = ttk.Label(fr, text="Provider")
        lbl_provider.grid(row=0, column=0, sticky="w")
        cb_provider = ttk.Combobox(fr, state="readonly", values=("gemini",), textvariable=self._var_provider, width=14)
        cb_provider.grid(
            row=0, column=1, sticky="w", padx=(6, 12)
        )
        lbl_model = ttk.Label(fr, text="Model")
        lbl_model.grid(row=0, column=2, sticky="w")
        self._cb_model = ttk.Combobox(
            fr,
            textvariable=self._var_model,
            values=self._model_choices,
            width=44,
            state="readonly",
        )
        self._cb_model.grid(row=0, column=3, columnspan=5, sticky="ew", padx=(6, 0))

        lbl_mode = ttk.Label(fr, text="Mode")
        lbl_mode.grid(row=1, column=0, sticky="w", pady=(6, 0))
        cb_mode = ttk.Combobox(
            fr,
            state="readonly",
            textvariable=self._var_mode_display,
            values=tuple(self._mode_display_to_key.keys()),
            width=44,
        )
        cb_mode.grid(row=1, column=1, columnspan=5, sticky="ew", padx=(6, 12), pady=(6, 0))
        cb_mode.bind("<<ComboboxSelected>>", lambda _e: self._on_mode_changed())
        lbl_model_hint = ttk.Label(fr, text="Gợi ý model")
        lbl_model_hint.grid(row=1, column=6, sticky="e", pady=(6, 0))
        btn_reload_models = ttk.Button(fr, text="Nạp model", command=self._reload_model_choices, width=12)
        btn_reload_models.grid(
            row=1, column=7, sticky="e", pady=(6, 0)
        )
        self._btn_sync_models_now = ttk.Button(
            fr,
            text="Đồng bộ model từ Flow",
            command=self._sync_models_from_flow_now,
            width=22,
        )
        self._btn_sync_models_now.grid(row=2, column=6, columnspan=2, sticky="e", pady=(6, 0))

        ttk.Label(
            fr,
            textvariable=self._var_mode_help,
            foreground="#1f4e79",
            wraplength=980,
            justify=tk.LEFT,
        ).grid(row=2, column=0, columnspan=8, sticky="ew", pady=(6, 0))

        lbl_prompt = ttk.Label(fr, text="Prompt mô tả video")
        lbl_prompt.grid(row=3, column=0, sticky="w", pady=(6, 0))
        prompt_wrap = ttk.Frame(fr)
        prompt_wrap.grid(row=3, column=1, columnspan=7, sticky="ew", padx=(6, 0), pady=(6, 0))
        prompt_wrap.columnconfigure(0, weight=1)
        prompt_wrap.rowconfigure(0, weight=1)
        self._txt_prompt = tk.Text(prompt_wrap, height=4, wrap="word", font=("Segoe UI", 10))
        self._txt_prompt.grid(row=0, column=0, sticky="ew")
        sp_y = ttk.Scrollbar(prompt_wrap, orient=tk.VERTICAL, command=self._txt_prompt.yview)
        sp_y.grid(row=0, column=1, sticky="ns")
        self._txt_prompt.configure(yscrollcommand=sp_y.set)
        self._prompt_lines_height = 4
        self._prompt_drag_start_y = 0
        self._prompt_drag_start_height = 4
        grip = ttk.Label(prompt_wrap, text="Kéo để đổi chiều cao ô prompt", foreground="gray", cursor="sb_v_double_arrow")
        grip.grid(row=1, column=0, sticky="e", pady=(2, 0))
        grip.bind("<ButtonPress-1>", self._on_prompt_resize_start, add="+")
        grip.bind("<B1-Motion>", self._on_prompt_resizing, add="+")
        _SimpleTooltip(grip, "Giữ chuột và kéo lên/xuống để thu phóng ô Prompt.")
        ttk.Label(
            fr,
            text="Mẹo: nhập nhiều dòng, mỗi dòng 1 prompt => tạo 1 video riêng.",
            foreground="gray",
        ).grid(row=4, column=1, columnspan=7, sticky="w", padx=(6, 0), pady=(2, 0))
        ttk.Label(
            fr,
            textvariable=self._var_prompt_stats,
            foreground="#1f4e79",
            font=("Segoe UI", 9, "bold"),
        ).grid(row=4, column=7, sticky="e", padx=(0, 0), pady=(2, 0))

        lbl_aspect = ttk.Label(fr, text="Tỉ lệ khung hình")
        lbl_aspect.grid(row=5, column=0, sticky="w", pady=(6, 0))
        cb_aspect = ttk.Combobox(fr, values=("16:9", "9:16"), state="readonly", textvariable=self._var_aspect, width=14)
        cb_aspect.grid(
            row=5, column=1, sticky="w", padx=(6, 12), pady=(6, 0)
        )
        lbl_duration = ttk.Label(fr, text="Thời lượng (giây)")
        lbl_duration.grid(row=5, column=2, sticky="w", pady=(6, 0))
        cb_duration = ttk.Combobox(fr, values=("4", "6", "8"), state="readonly", textvariable=self._var_duration, width=10)
        cb_duration.grid(
            row=5, column=3, sticky="w", padx=(6, 12), pady=(6, 0)
        )
        lbl_resolution = ttk.Label(fr, text="Độ phân giải")
        lbl_resolution.grid(row=5, column=4, sticky="w", pady=(6, 0))
        cb_resolution = ttk.Combobox(fr, values=("720p", "1080p"), state="readonly", textvariable=self._var_resolution, width=10)
        cb_resolution.grid(
            row=5, column=5, sticky="w", padx=(6, 12), pady=(6, 0)
        )
        lbl_count = ttk.Label(fr, text="Số biến thể prompt")
        lbl_count.grid(row=5, column=6, sticky="w", pady=(6, 0))
        cb_count = ttk.Combobox(
            fr,
            values=tuple(str(i) for i in range(1, 21)),
            state="readonly",
            textvariable=self._var_count,
            width=6,
        )
        cb_count.grid(
            row=5, column=7, sticky="w", padx=(6, 0), pady=(6, 0)
        )
        cb_count.bind("<<ComboboxSelected>>", lambda _e: self._refresh_prompt_stats())
        # Luôn reset mặc định 1 khi mở dialog mới để tránh giữ giá trị phiên cũ trên UI.
        self._var_count.set("1")
        lbl_outputs = ttk.Label(fr, text="Outputs / prompt")
        lbl_outputs.grid(row=6, column=6, sticky="w", pady=(6, 0))
        cb_outputs = ttk.Combobox(
            fr,
            values=("x1", "x2", "x3", "x4"),
            state="readonly",
            textvariable=self._var_outputs,
            width=6,
        )
        cb_outputs.grid(row=6, column=7, sticky="w", padx=(6, 0), pady=(6, 0))
        cb_outputs.bind("<<ComboboxSelected>>", lambda _e: self._refresh_prompt_stats())
        self._var_outputs.set("x1")

        ttk.Label(fr, text="Ngôn ngữ video").grid(row=6, column=0, sticky="w", pady=(6, 0))
        cb_lang = ttk.Combobox(
            fr,
            state="readonly",
            textvariable=self._var_language,
            values=(
                "Tiếng Việt",
                "English",
                "Español",
                "Português",
                "Français",
                "Deutsch",
                "Italiano",
                "日本語",
                "한국어",
                "中文 (简体)",
                "中文 (繁體)",
                "ไทย",
                "Bahasa Indonesia",
                "हिन्दी",
            ),
            width=24,
        )
        cb_lang.grid(row=6, column=1, sticky="w", padx=(6, 12), pady=(6, 0))
        lbl_topic = ttk.Label(fr, text="Chủ đề (Topic)")
        lbl_topic.grid(row=6, column=2, sticky="w", pady=(6, 0))
        ent_topic = ttk.Entry(fr, textvariable=self._var_topic, width=24)
        ent_topic.grid(row=6, column=3, sticky="w", padx=(6, 12), pady=(6, 0))
        _SimpleTooltip(
            lbl_topic,
            "Chủ đề cụ thể của video. Ví dụ: skincare buổi sáng, review quán cafe, mẹo học tiếng Anh.",
        )
        _SimpleTooltip(
            ent_topic,
            "Topic giúp AI hiểu bối cảnh rõ hơn ngoài ý tưởng chính, từ đó tạo scene và prompt chính xác hơn.",
        )
        ttk.Label(fr, text="Goal").grid(row=6, column=4, sticky="w", pady=(6, 0))
        ttk.Combobox(
            fr,
            state="readonly",
            textvariable=self._var_goal,
            values=("viral", "bán hàng", "giới thiệu sản phẩm", "kể chuyện", "giáo dục", "cinematic reel"),
            width=18,
        ).grid(row=6, column=5, sticky="w", padx=(6, 12), pady=(6, 0))
        self._bind_top_tooltips(
            lbl_provider=lbl_provider,
            cb_provider=cb_provider,
            lbl_model=lbl_model,
            cb_mode=cb_mode,
            btn_reload_models=btn_reload_models,
            lbl_prompt=lbl_prompt,
            ent_prompt=self._txt_prompt,
            lbl_aspect=lbl_aspect,
            cb_aspect=cb_aspect,
            lbl_duration=lbl_duration,
            cb_duration=cb_duration,
            lbl_resolution=lbl_resolution,
            cb_resolution=cb_resolution,
            lbl_count=lbl_count,
            cb_count=cb_count,
            lbl_outputs=lbl_outputs,
            cb_outputs=cb_outputs,
        )
        self._txt_prompt.bind("<FocusOut>", lambda _e: self._refresh_auto_style_preview(), add="+")
        self._txt_prompt.bind("<KeyRelease>", lambda _e: self._on_prompt_text_changed(), add="+")
        self._refresh_prompt_stats()

    def _ensure_dialog_size_fits_content(self) -> None:
        """
        Tự nới kích thước dialog theo content thực tế để tránh bị cắt phần dưới
        trên các máy có DPI scaling hoặc font metrics khác nhau.
        """
        try:
            self._top.update_idletasks()
            req_w = int(self._top.winfo_reqwidth())
            req_h = int(self._top.winfo_reqheight())
            screen_w = int(self._top.winfo_screenwidth())
            screen_h = int(self._top.winfo_screenheight())
            target_w = min(max(1080, req_w + 24), max(980, screen_w - 80))
            target_h = min(max(760, req_h + 24), max(700, screen_h - 90))
            self._top.geometry(f"{target_w}x{target_h}")
            self._top.minsize(min(target_w, max(920, req_w)), min(target_h, max(680, req_h)))
        except Exception:
            self._top.geometry("1120x820")
            self._top.minsize(980, 740)

    def _build_style_form(self, root: ttk.Frame) -> None:
        fr = ttk.LabelFrame(root, text="B — Style video", padding=8)
        fr.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        fr.columnconfigure(1, weight=1)
        fr.columnconfigure(3, weight=1)

        ttk.Label(fr, text="Chọn style").grid(row=0, column=0, sticky="w")
        self._cb_style = ttk.Combobox(fr, textvariable=self._var_style, state="readonly", width=38)
        self._cb_style.grid(row=0, column=1, sticky="ew", padx=(6, 12))
        self._cb_style.bind("<<ComboboxSelected>>", lambda _e: self._on_style_selected())

        ttk.Label(fr, text="Mô tả style đang chọn").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ent_style_preview = ttk.Entry(fr, textvariable=self._var_style_prompt, width=96, state="readonly")
        ent_style_preview.grid(
            row=1, column=1, columnspan=3, sticky="ew", padx=(6, 0), pady=(6, 0)
        )
        _SimpleTooltip(ent_style_preview, "Preview style hiện tại. Dùng Thêm/Sửa để cập nhật.")

        bar = ttk.Frame(fr)
        bar.grid(row=2, column=1, columnspan=3, sticky="w", pady=(6, 0))
        ttk.Button(bar, text="Thêm style", command=self._open_add_style_popup).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(bar, text="Sửa style", command=self._open_edit_style_popup).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(bar, text="Xóa style", command=self._on_delete_style).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(bar, text="Reset mặc định", command=self._on_reset_default_styles).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Checkbutton(
            bar,
            text="Auto chọn style bằng AI",
            variable=self._var_auto_style_enable,
        ).pack(side=tk.LEFT, padx=(12, 6))
        ttk.Button(bar, text="Phân tích style", command=self._on_auto_select_styles).pack(side=tk.LEFT)

        adv = ttk.Frame(fr)
        adv.grid(row=3, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        adv.columnconfigure(1, weight=1)
        adv.columnconfigure(3, weight=1)
        ttk.Label(adv, text="Phong cách hình ảnh").grid(row=0, column=0, sticky="w")
        self._cb_visual_style = ttk.Combobox(
            adv,
            textvariable=self._var_visual_style,
            values=tuple(self._preset_combo_values("visual_style")),
            state="readonly",
            width=26,
        )
        self._cb_visual_style.grid(row=0, column=1, sticky="w", padx=(6, 12))
        ttk.Label(adv, textvariable=self._var_ai_tag_visual, foreground="#0b5ed7").grid(
            row=0, column=1, sticky="e", padx=(0, 6)
        )
        ttk.Label(adv, text="Tâm trạng").grid(row=0, column=2, sticky="w")
        self._cb_mood = ttk.Combobox(
            adv,
            textvariable=self._var_mood,
            values=tuple(self._preset_combo_values("mood")),
            state="readonly",
            width=18,
        )
        self._cb_mood.grid(row=0, column=3, sticky="w", padx=(6, 0))
        ttk.Label(adv, textvariable=self._var_ai_tag_mood, foreground="#0b5ed7").grid(
            row=0, column=3, sticky="e", padx=(0, 6)
        )

        ttk.Label(adv, text="Góc máy / Camera").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self._cb_camera = ttk.Combobox(
            adv,
            textvariable=self._var_camera_style,
            values=tuple(self._preset_combo_values("camera_style")),
            state="readonly",
            width=26,
        )
        self._cb_camera.grid(row=1, column=1, sticky="w", padx=(6, 12), pady=(6, 0))
        ttk.Label(adv, textvariable=self._var_ai_tag_camera, foreground="#0b5ed7").grid(
            row=1, column=1, sticky="e", padx=(0, 6), pady=(6, 0)
        )
        ttk.Label(adv, text="Ánh sáng").grid(row=1, column=2, sticky="w", pady=(6, 0))
        self._cb_lighting = ttk.Combobox(
            adv,
            textvariable=self._var_lighting,
            values=tuple(self._preset_combo_values("lighting")),
            state="readonly",
            width=18,
        )
        self._cb_lighting.grid(row=1, column=3, sticky="w", padx=(6, 0), pady=(6, 0))
        ttk.Label(adv, textvariable=self._var_ai_tag_lighting, foreground="#0b5ed7").grid(
            row=1, column=3, sticky="e", padx=(0, 6), pady=(6, 0)
        )

        ttk.Label(adv, text="Chuyển động").grid(row=2, column=0, sticky="w", pady=(6, 0))
        self._cb_motion = ttk.Combobox(
            adv,
            textvariable=self._var_motion_style,
            values=tuple(self._preset_combo_values("motion_style")),
            state="readonly",
            width=26,
        )
        self._cb_motion.grid(row=2, column=1, sticky="w", padx=(6, 12), pady=(6, 0))
        ttk.Label(adv, textvariable=self._var_ai_tag_motion, foreground="#0b5ed7").grid(
            row=2, column=1, sticky="e", padx=(0, 6), pady=(6, 0)
        )
        _v_name_to_id = {str(x.get("name", "")).strip(): str(x.get("id", "")).strip() for x in style_items("video_styles")}
        _c_name_to_id = {str(x.get("name", "")).strip(): str(x.get("id", "")).strip() for x in style_items("camera_styles")}
        _l_name_to_id = {str(x.get("name", "")).strip(): str(x.get("id", "")).strip() for x in style_items("lighting_styles")}
        _m_name_to_id = {str(x.get("name", "")).strip(): str(x.get("id", "")).strip() for x in style_items("motion_styles")}
        self._cb_visual_style.bind(
            "<<ComboboxSelected>>",
            lambda _e: self._var_video_style_id.set(
                _v_name_to_id.get(self._var_visual_style.get().strip(), self._var_video_style_id.get().strip())
            ),
            add="+",
        )
        self._cb_visual_style.bind("<<ComboboxSelected>>", lambda _e: self._var_ai_tag_visual.set("thủ công"), add="+")
        self._cb_camera.bind(
            "<<ComboboxSelected>>",
            lambda _e: self._var_camera_style_id.set(
                _c_name_to_id.get(self._var_camera_style.get().strip(), self._var_camera_style_id.get().strip())
            ),
            add="+",
        )
        self._cb_camera.bind("<<ComboboxSelected>>", lambda _e: self._var_ai_tag_camera.set("thủ công"), add="+")
        self._cb_lighting.bind(
            "<<ComboboxSelected>>",
            lambda _e: self._var_lighting_style_id.set(
                _l_name_to_id.get(self._var_lighting.get().strip(), self._var_lighting_style_id.get().strip())
            ),
            add="+",
        )
        self._cb_lighting.bind("<<ComboboxSelected>>", lambda _e: self._var_ai_tag_lighting.set("thủ công"), add="+")
        self._cb_motion.bind(
            "<<ComboboxSelected>>",
            lambda _e: self._var_motion_style_id.set(
                _m_name_to_id.get(self._var_motion_style.get().strip(), self._var_motion_style_id.get().strip())
            ),
            add="+",
        )
        self._cb_motion.bind("<<ComboboxSelected>>", lambda _e: self._var_ai_tag_motion.set("thủ công"), add="+")
        self._cb_mood.bind("<<ComboboxSelected>>", lambda _e: self._var_ai_tag_mood.set("thủ công"), add="+")
        ttk.Label(adv, text="Chế độ nhân vật").grid(row=2, column=2, sticky="w", pady=(6, 0))
        cb_char_mode = ttk.Combobox(
            adv,
            textvariable=self._var_character_mode,
            values=("Tự tạo (auto)", "Thủ công (manual)"),
            state="readonly",
            width=18,
        )
        cb_char_mode.grid(row=2, column=3, sticky="w", padx=(6, 0), pady=(6, 0))
        cb_char_mode.bind("<<ComboboxSelected>>", lambda _e: self._on_character_mode_changed())

        preset_bar = ttk.Frame(adv)
        preset_bar.grid(row=2, column=4, sticky="w", padx=(12, 0))
        ttk.Label(preset_bar, text="Quản lý preset").pack(side=tk.LEFT, padx=(0, 6))
        self._var_preset_kind = tk.StringVar(value="Phong cách")
        self._cb_preset_kind = ttk.Combobox(
            preset_bar,
            state="readonly",
            textvariable=self._var_preset_kind,
            values=("Phong cách", "Tâm trạng", "Camera", "Ánh sáng", "Chuyển động"),
            width=14,
        )
        self._cb_preset_kind.pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(preset_bar, text="Thêm", command=self._on_add_prompt_preset).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(preset_bar, text="Sửa", command=self._on_edit_prompt_preset).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(preset_bar, text="Xóa", command=self._on_delete_prompt_preset).pack(side=tk.LEFT, padx=(0, 4))

        ttk.Label(adv, text="Lý do AI chọn style").grid(row=3, column=0, sticky="w", pady=(6, 0))
        ttk.Label(
            adv,
            textvariable=self._var_auto_style_reason,
            foreground="#475569",
            wraplength=860,
            justify=tk.LEFT,
        ).grid(row=3, column=1, columnspan=3, sticky="ew", padx=(6, 0), pady=(6, 0))

        self._char_manual_fr = ttk.Frame(adv)
        self._char_manual_fr.grid(row=4, column=0, columnspan=4, sticky="ew", pady=(6, 0))
        self._char_manual_fr.columnconfigure(1, weight=1)
        self._char_manual_fr.columnconfigure(3, weight=1)
        ttk.Label(self._char_manual_fr, text="Tên nhân vật").grid(row=0, column=0, sticky="w")
        ttk.Entry(self._char_manual_fr, textvariable=self._var_character_name, width=22).grid(row=0, column=1, sticky="w", padx=(6, 12))
        ttk.Label(self._char_manual_fr, text="Ngoại hình").grid(row=0, column=2, sticky="w")
        ttk.Entry(self._char_manual_fr, textvariable=self._var_character_appearance, width=48).grid(
            row=0, column=3, sticky="ew", padx=(6, 0)
        )
        ttk.Label(self._char_manual_fr, text="Trang phục").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(self._char_manual_fr, textvariable=self._var_character_outfit, width=22).grid(
            row=1, column=1, sticky="w", padx=(6, 12), pady=(6, 0)
        )
        ttk.Label(self._char_manual_fr, text="Tính cách").grid(row=1, column=2, sticky="w", pady=(6, 0))
        ttk.Entry(self._char_manual_fr, textvariable=self._var_character_personality, width=48).grid(
            row=1, column=3, sticky="ew", padx=(6, 0), pady=(6, 0)
        )
        self._char_auto_fr = ttk.Frame(adv)
        self._char_auto_fr.grid(row=4, column=0, columnspan=4, sticky="ew", pady=(6, 0))
        self._char_auto_fr.columnconfigure(2, weight=1)
        ttk.Button(self._char_auto_fr, text="Tạo nhân vật từ ý tưởng", command=self._on_generate_auto_characters).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Button(self._char_auto_fr, text="Xem/Sửa danh sách nhân vật", command=self._open_auto_characters_popup).grid(
            row=0, column=1, sticky="w", padx=(8, 0)
        )
        ttk.Label(self._char_auto_fr, textvariable=self._var_auto_char_summary, foreground="#1f4e79", wraplength=780).grid(
            row=0, column=2, sticky="ew", padx=(8, 0)
        )
        self._char_auto_thumb_fr = ttk.Frame(self._char_auto_fr)
        self._char_auto_thumb_fr.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(6, 0))
        ttk.Checkbutton(
            adv,
            text="Reuse character profile trong batch này (giữ đồng nhất nhân vật)",
            variable=self._reuse_character_profile,
        ).grid(row=5, column=0, columnspan=4, sticky="w", pady=(6, 0))
        ttk.Checkbutton(
            adv,
            text="Khóa vai trò theo tên nhân vật (main/support) để tránh swap vai",
            variable=self._var_lock_character_roles,
        ).grid(row=6, column=0, columnspan=4, sticky="w", pady=(4, 0))
        self._on_character_mode_changed()

    def _build_mode_form(self, root: ttk.Frame) -> None:
        self._mode_fr = ttk.LabelFrame(root, text="C — Input theo mode", padding=8)
        self._mode_fr.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        self._mode_fr.columnconfigure(1, weight=1)
        self._mode_widgets: list[tk.Widget] = []

    def _clear_mode_widgets(self) -> None:
        for w in self._mode_widgets:
            w.destroy()
        self._mode_widgets = []

    def _row_file(self, r: int, label: str, var: tk.StringVar, *, multiple: bool = False) -> None:
        ttk.Label(self._mode_fr, text=label).grid(row=r, column=0, sticky="w", pady=2)
        e = ttk.Entry(self._mode_fr, textvariable=var, width=90)
        e.grid(row=r, column=1, sticky="ew", pady=2, padx=(6, 6))
        self._mode_widgets.extend([e])

        def pick() -> None:
            if multiple:
                xs = filedialog.askopenfilenames(parent=self._top, title=label)
                if xs:
                    var.set("|".join(xs))
            else:
                x = filedialog.askopenfilename(parent=self._top, title=label)
                if x:
                    var.set(x)

        b = ttk.Button(self._mode_fr, text="Chọn…", command=pick)
        b.grid(row=r, column=2, sticky="w", pady=2)
        self._mode_widgets.append(b)

    def _on_mode_changed(self) -> None:
        self._clear_mode_widgets()
        mode = self._current_mode_key()
        self._var_mode_help.set(self._mode_help_text(mode))
        if mode in {"prompt_to_vertical_video", "image_to_vertical_video"}:
            self._var_aspect.set("9:16")
        r = 0
        if mode in {"image_to_video", "image_to_vertical_video"}:
            self._row_file(r, "Ảnh đầu vào", self._var_image)
            r += 1
        elif mode == "first_last_frame_to_video":
            self._row_file(r, "Ảnh khung đầu (first frame)", self._var_first)
            r += 1
            self._row_file(r, "Ảnh khung cuối (last frame)", self._var_last)
            r += 1
        elif mode == "ingredients_to_video":
            self._row_file(r, "Ảnh tham chiếu (nhiều ảnh)", self._var_refs, multiple=True)
            r += 1
            ttk.Label(self._mode_fr, text="Loại tham chiếu").grid(row=r, column=0, sticky="w", pady=2)
            cb = ttk.Combobox(
                self._mode_fr,
                values=("subject", "object", "scene", "style"),
                state="readonly",
                textvariable=self._var_ref_type,
                width=20,
            )
            cb.grid(row=r, column=1, sticky="w", padx=(6, 6), pady=2)
            self._mode_widgets.append(cb)
            r += 1
        elif mode == "extend_video":
            self._row_file(r, "Video nguồn cần kéo dài", self._var_src_video)
            r += 1
        ttk.Label(self._mode_fr, text="Thư mục output").grid(row=r, column=0, sticky="w", pady=(4, 0))
        eout = ttk.Entry(self._mode_fr, textvariable=self._var_output_dir, width=90)
        eout.grid(row=r, column=1, sticky="ew", pady=(4, 0), padx=(6, 6))
        bout = ttk.Button(
            self._mode_fr,
            text="Chọn…",
            command=lambda: self._pick_out(),
        )
        bout.grid(row=r, column=2, sticky="w", pady=(4, 0))
        self._entry_output_dir = eout
        self._btn_pick_output_dir = bout
        self._mode_widgets.extend([eout, bout])
        self._sync_output_controls_state()

    def _pick_out(self) -> None:
        if bool(self._var_lock_output_by_project.get()):
            messagebox.showinfo(
                "AI Video",
                "Output đang khóa theo dự án. Bỏ chọn « Khóa output theo dự án » nếu muốn đổi thủ công.",
                parent=self._top,
            )
            return
        x = filedialog.askdirectory(parent=self._top, title="Output folder")
        if x:
            picked = Path(x).expanduser().resolve()
            workspace = project_root().resolve()
            try:
                picked.relative_to(workspace)
            except Exception:
                if not messagebox.askyesno(
                    "Output ngoài thư mục dự án",
                    "Bạn đang chọn thư mục output nằm ngoài thư mục ToolFB hiện tại.\n"
                    "Việc này vẫn dùng được nhưng có thể khó quản lý dữ liệu theo dự án.\n\n"
                    "Bạn có muốn tiếp tục dùng thư mục này?",
                    parent=self._top,
                ):
                    return
            self._var_output_dir.set(picked.as_posix())

    def _on_toggle_lock_output_by_project(self) -> None:
        self._sync_output_controls_state()

    def _sync_output_controls_state(self) -> None:
        """Đồng bộ trạng thái readonly của ô output theo tùy chọn khóa dự án."""
        locked = bool(self._var_lock_output_by_project.get())
        if locked:
            self._var_output_dir.set(self._default_output_dir_for_project(self._current_project_id))
        entry = getattr(self, "_entry_output_dir", None)
        btn = getattr(self, "_btn_pick_output_dir", None)
        if entry is not None:
            try:
                entry.configure(state="readonly" if locked else "normal")
            except Exception:
                pass
        if btn is not None:
            try:
                btn.configure(state=("disabled" if locked else "normal"))
            except Exception:
                pass

    def _build_action_bar(self, root: ttk.Frame) -> None:
        fr = ttk.Frame(root)
        fr.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        fr.columnconfigure(20, weight=1)
        self._btn_preview_prompts = ttk.Button(fr, text="Tạo prompt preview", command=self._on_build_prompt_preview)
        self._btn_preview_prompts.pack(side=tk.LEFT, padx=(0, 6))
        btn_create = ttk.Button(fr, text="Tạo video", command=self._on_create)
        btn_create.pack(side=tk.LEFT, padx=(0, 6))
        btn_poll = ttk.Button(fr, text="Poll lại", command=self._on_poll)
        btn_poll.pack(side=tk.LEFT, padx=(0, 6))
        btn_sync = ttk.Button(fr, text="Sync pending", command=self._on_sync)
        btn_sync.pack(side=tk.LEFT, padx=(0, 6))
        btn_cancel = ttk.Button(fr, text="Hủy", command=self._on_cancel)
        btn_cancel.pack(side=tk.LEFT, padx=(0, 6))
        btn_delete = ttk.Button(fr, text="Xóa dòng chọn", command=self._on_delete_selected)
        btn_delete.pack(side=tk.LEFT, padx=(0, 6))
        btn_delete_all = ttk.Button(fr, text="Xóa tất cả", command=self._on_delete_all)
        btn_delete_all.pack(side=tk.LEFT, padx=(0, 6))
        btn_open_output = ttk.Button(fr, text="Mở thư mục output", command=self._on_open_output)
        btn_open_output.pack(side=tk.LEFT, padx=(0, 6))
        btn_play_selected = ttk.Button(fr, text="Xem video chọn", command=self._on_play_selected_video)
        btn_play_selected.pack(side=tk.LEFT, padx=(0, 6))
        btn_use_for_job = ttk.Button(fr, text="Dùng video này để tạo job đăng", command=self._on_use_for_job)
        btn_use_for_job.pack(side=tk.LEFT, padx=(12, 0))
        self._btn_open_saved_preview = ttk.Button(fr, text="Mở prompt preview đã lưu", command=self._on_open_saved_prompt_preview)
        self._btn_open_saved_preview.pack(side=tk.LEFT, padx=(12, 0))
        btn_import_bundle = ttk.Button(fr, text="Nạp bộ prompt từ file…", command=self._on_import_prompt_bundle_file)
        btn_import_bundle.pack(side=tk.LEFT, padx=(8, 0))
        self._lbl_prepared_preview = ttk.Label(fr, text="Prompt preview đã lưu: 0", foreground="gray")
        self._lbl_prepared_preview.pack(side=tk.LEFT, padx=(8, 0))
        self._lbl_preview_progress = ttk.Label(fr, textvariable=self._var_preview_progress, foreground="#2563eb")
        self._lbl_preview_progress.pack(side=tk.LEFT, padx=(8, 0))

        _SimpleTooltip(btn_create, "Tạo video mới từ prompt đang nhập. Nếu nhiều dòng prompt: mỗi dòng tạo 1 video.")
        _SimpleTooltip(
            self._btn_preview_prompts,
            "Sinh prompt xem trước để bạn sửa/xóa. « Lưu preview » = lưu nội bộ để mở lại app vẫn còn. Trong cửa sổ preview có thêm « Lưu tất cả ra file » để giữ bản JSON tùy ý.",
        )
        _SimpleTooltip(btn_poll, "Kiểm tra lại trạng thái video đang chọn ngay lúc này.")
        _SimpleTooltip(btn_sync, "Tự động đồng bộ và tiếp tục xử lý các video đang chờ.")
        _SimpleTooltip(btn_cancel, "Hủy video đang chọn (không xử lý tiếp).")
        _SimpleTooltip(btn_delete, "Xóa video đang chọn khỏi bảng trạng thái (không hoàn tác).")
        _SimpleTooltip(btn_delete_all, "Xóa toàn bộ video trong bảng trạng thái (không hoàn tác).")
        _SimpleTooltip(btn_open_output, "Mở nhanh thư mục chứa file video output đã tạo.")
        _SimpleTooltip(btn_play_selected, "Phát video output đầu tiên của dòng đang chọn (mở bằng ứng dụng mặc định).")
        _SimpleTooltip(btn_use_for_job, "Lấy video đã chọn để dùng trong luồng tạo job đăng.")
        _SimpleTooltip(
            self._btn_open_saved_preview,
            "Mở lại bộ prompt preview đã lưu để sửa/kiểm tra. Preview được lưu trên máy kể cả khi đóng chương trình.",
        )
        _SimpleTooltip(
            btn_import_bundle,
            "Chọn file JSON đã xuất (Lưu tất cả ra file trong cửa sổ preview) để nạp lại toàn bộ prompt và bấm Tạo video.",
        )
        self._refresh_prepared_preview_badge()

    def _build_table(self, root: ttk.Frame) -> None:
        fr = ttk.LabelFrame(root, text="D — Trạng thái video", padding=6)
        fr.grid(row=5, column=0, sticky="nsew", pady=(8, 0))
        fr.columnconfigure(0, weight=1)
        fr.rowconfigure(2, weight=1)

        bar = ttk.Frame(fr)
        bar.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        bar.columnconfigure(10, weight=1)
        self._btn_filter_all = ttk.Button(bar, text="Tất cả (0)", command=lambda: self._set_status_filter("all"))
        self._btn_filter_all.grid(row=0, column=0, sticky="w", padx=(0, 6))
        self._btn_filter_completed = ttk.Button(
            bar, text="Hoàn thành (0)", command=lambda: self._set_status_filter("completed")
        )
        self._btn_filter_completed.grid(row=0, column=1, sticky="w", padx=(0, 6))
        self._btn_filter_failed = ttk.Button(bar, text="Thất bại (0)", command=lambda: self._set_status_filter("failed"))
        self._btn_filter_failed.grid(row=0, column=2, sticky="w", padx=(0, 12))
        ttk.Label(bar, text="View").grid(row=0, column=11, sticky="e", padx=(0, 6))
        self._btn_view_grid = ttk.Button(bar, text="Grid", command=lambda: self._set_status_view("grid"), width=8)
        self._btn_view_grid.grid(row=0, column=12, sticky="e", padx=(0, 4))
        self._btn_view_list = ttk.Button(bar, text="List", command=lambda: self._set_status_view("list"), width=8)
        self._btn_view_list.grid(row=0, column=13, sticky="e")

        proj_bar = ttk.Frame(fr)
        proj_bar.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(proj_bar, text="Dự án (job mới gắn vào):").pack(side=tk.LEFT, padx=(0, 6))
        ttk.Entry(proj_bar, textvariable=self._var_project_name, width=26).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(proj_bar, text="Dự án mới", command=self._new_video_project).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Checkbutton(
            proj_bar,
            text="Khóa output theo dự án",
            variable=self._var_lock_output_by_project,
            command=self._on_toggle_lock_output_by_project,
        ).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Label(proj_bar, text="Lọc list:").pack(side=tk.LEFT, padx=(0, 4))
        self._cb_project_filter = ttk.Combobox(
            proj_bar, textvariable=self._var_project_list_filter, width=34, state="readonly"
        )
        self._cb_project_filter.pack(side=tk.LEFT, padx=(0, 0))
        self._cb_project_filter.bind("<<ComboboxSelected>>", lambda _e: self._on_project_filter_changed())
        self._cb_project_filter.configure(values=("Tất cả dự án",))
        self._project_filter_id_list = ["*"]

        body = ttk.Frame(fr)
        body.grid(row=2, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)
        body.rowconfigure(0, weight=1)

        self._status_grid_wrap = ttk.Frame(body)
        self._status_grid_wrap.grid(row=0, column=0, sticky="nsew")
        self._status_grid_wrap.columnconfigure(0, weight=1)
        self._status_grid_wrap.rowconfigure(0, weight=1)
        self._status_grid_canvas = tk.Canvas(self._status_grid_wrap, highlightthickness=0)
        self._status_grid_canvas.grid(row=0, column=0, sticky="nsew")
        self._status_grid_scroll = ttk.Scrollbar(self._status_grid_wrap, orient=tk.VERTICAL, command=self._status_grid_canvas.yview)
        self._status_grid_scroll.grid(row=0, column=1, sticky="ns")
        self._status_grid_canvas.configure(yscrollcommand=self._status_grid_scroll.set)
        self._status_grid_content = ttk.Frame(self._status_grid_canvas)
        self._status_grid_window = self._status_grid_canvas.create_window((0, 0), window=self._status_grid_content, anchor="nw")
        self._status_grid_content.bind(
            "<Configure>",
            lambda _e: self._status_grid_canvas.configure(scrollregion=self._status_grid_canvas.bbox("all")),
        )
        self._status_grid_canvas.bind(
            "<Configure>",
            lambda e: self._status_grid_canvas.itemconfigure(self._status_grid_window, width=e.width),
        )

        cols = ("id", "mode", "prompt", "model", "aspect", "duration", "status", "operation", "outputs", "error", "created")
        self._tree_wrap = ttk.Frame(body)
        self._tree_wrap.grid(row=0, column=0, sticky="nsew")
        self._tree_wrap.columnconfigure(0, weight=1)
        self._tree_wrap.rowconfigure(0, weight=1)
        self._tree = ttk.Treeview(self._tree_wrap, columns=cols, show="headings", height=12, selectmode="browse")
        for c, w in (
            ("id", 120),
            ("mode", 130),
            ("prompt", 180),
            ("model", 170),
            ("aspect", 70),
            ("duration", 70),
            ("status", 95),
            ("operation", 160),
            ("outputs", 170),
            ("error", 180),
            ("created", 140),
        ):
            self._tree.heading(c, text=c)
            self._tree.column(c, width=w, stretch=c in {"prompt", "model", "outputs", "error"})
        sy = ttk.Scrollbar(self._tree_wrap, orient=tk.VERTICAL, command=self._tree.yview)
        sx = ttk.Scrollbar(self._tree_wrap, orient=tk.HORIZONTAL, command=self._tree.xview)
        self._tree.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)
        self._tree.grid(row=0, column=0, sticky="nsew")
        sy.grid(row=0, column=1, sticky="ns")
        sx.grid(row=1, column=0, sticky="ew")
        self._tree.bind("<<TreeviewSelect>>", lambda _e: self._refresh_progress_panel())
        self._tree.bind("<Double-1>", lambda _e: self._on_play_selected_video())
        self._set_status_view("grid")

    def _set_status_filter(self, mode: str) -> None:
        self._status_filter.set(mode)
        self._last_status_view_fingerprint = None
        self._refresh_rows()

    def _set_status_view(self, mode: str) -> None:
        self._status_view.set(mode)
        if mode == "list":
            self._status_grid_wrap.grid_remove()
            self._tree_wrap.grid()
        else:
            self._tree_wrap.grid_remove()
            self._status_grid_wrap.grid()
        self._last_status_view_fingerprint = None
        self._refresh_rows()

    def _build_progress_panel(self, root: ttk.Frame) -> None:
        fr = ttk.LabelFrame(root, text="D — Tiến trình tạo video (video đang chọn)", padding=8)
        fr.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        fr.columnconfigure(0, weight=1)

        self._var_step_main = tk.StringVar(value="Chưa chọn video")
        self._var_step_detail = tk.StringVar(value="Hãy chọn 1 dòng ở bảng bên dưới để xem tiến trình chi tiết.")
        self._var_step_hint = tk.StringVar(
            value="Quy trình chuẩn: Bước 1 (Submit) -> Bước 2 (Generate/Poll) -> Bước 3 (Download) -> Hoàn tất"
        )
        self._progress_full_detail = ""
        self._progress_expanded = False

        ttk.Label(fr, textvariable=self._var_step_main, font=("Segoe UI", 10, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        detail_fr = ttk.Frame(fr)
        detail_fr.grid(row=1, column=0, sticky="ew", pady=(4, 0))
        detail_fr.columnconfigure(0, weight=1)
        detail_fr.rowconfigure(0, weight=1)
        self._txt_progress_detail = tk.Text(
            detail_fr,
            height=4,
            wrap="word",
            font=("Segoe UI", 9),
            foreground="#1f4e79",
        )
        self._txt_progress_detail.grid(row=0, column=0, sticky="ew")
        sy = ttk.Scrollbar(detail_fr, orient=tk.VERTICAL, command=self._txt_progress_detail.yview)
        sy.grid(row=0, column=1, sticky="ns")
        self._txt_progress_detail.configure(yscrollcommand=sy.set)
        self._txt_progress_detail.insert("1.0", self._var_step_detail.get())
        self._txt_progress_detail.configure(state="disabled")
        bar = ttk.Frame(fr)
        bar.grid(row=2, column=0, sticky="w", pady=(4, 0))
        self._btn_progress_toggle = ttk.Button(bar, text="Hiện thêm", command=self._toggle_progress_detail)
        self._btn_progress_toggle.pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(bar, text="Mở chi tiết", command=self._open_progress_detail_popup).pack(side=tk.LEFT)
        ttk.Label(fr, textvariable=self._var_step_hint, foreground="gray", wraplength=980, justify=tk.LEFT).grid(
            row=3, column=0, sticky="ew", pady=(4, 0)
        )

    def _build_request(self) -> dict[str, Any]:
        mode = self._current_mode_key()
        assets: dict[str, Any] = {
            "image_path": self._var_image.get().strip(),
            "first_frame_path": self._var_first.get().strip(),
            "last_frame_path": self._var_last.get().strip(),
            "reference_images": [x for x in self._var_refs.get().split("|") if x.strip()],
            "source_video_path": self._var_src_video.get().strip(),
        }
        req: dict[str, Any] = {
            "provider": self._var_provider.get().strip().lower(),
            "model": self._var_model.get().strip(),
            "mode": mode,
            "prompt": self._compose_prompt_with_style(self._prompt_text()),
            "input_assets": assets,
            "options": {
                "aspect_ratio": self._var_aspect.get().strip(),
                "duration_sec": int(self._var_duration.get().strip() or "8"),
                "output_count": self._outputs_count(),
                "outputs": self._var_outputs.get().strip() or "x1",
                "output_dir": self._effective_output_dir(),
                "resolution": self._var_resolution.get().strip(),
                "language": self._var_language.get().strip(),
                "reference_type": self._var_ref_type.get().strip(),
            },
        }
        return req

    def _selected_video_id(self) -> str:
        if self._status_view.get() == "grid":
            return str(self._selected_grid_video_id).strip()
        sel = self._tree.selection()
        if not sel:
            return ""
        vals = self._tree.item(sel[0], "values") or ()
        return str(vals[0]).strip() if vals else ""

    def _on_create(self) -> None:
        try:
            self._validate_character_reference_before_create()
            requests: list[dict[str, Any]] | None = None
            current_sig = self._current_preview_signature()
            if self._prepared_requests and self._prepared_signature == current_sig:
                requests = [dict(x) for x in self._prepared_requests]
            elif self._prepared_requests and self._prepared_signature != current_sig:
                if messagebox.askyesno(
                    "Prompt preview đã lưu",
                    "Form hiện tại khác với lúc bạn lưu prompt preview.\n\n"
                    "Chọn Có: dùng prompt preview đã lưu (phù hợp khi vừa mở lại chương trình).\n"
                    "Chọn Không: bỏ preview và tạo lại từ ô prompt theo form hiện tại.",
                    parent=self._top,
                ):
                    requests = [dict(x) for x in self._prepared_requests]
                else:
                    self._clear_prepared_preview_storage()
            if requests is None:
                prompts = self._prompt_lines()
                if not prompts:
                    messagebox.showwarning("AI Video", "Nhập ít nhất 1 prompt (mỗi dòng 1 prompt).", parent=self._top)
                    return
                requests = self._build_requests_for_prompts(prompts)
                if not requests:
                    messagebox.showwarning("AI Video", "Không có request hợp lệ để tạo video.", parent=self._top)
                    return
                if not self._show_prompt_preview_and_confirm(requests):
                    return
            self._merge_current_generation_settings_into_requests(requests)
            requests = self._maybe_bundle_requests_for_browser_queue(requests)
            self._attach_project_meta_to_requests(requests)
            created = 0
            for req in requests:
                rec = self._svc.create_video_record(req)
                self._svc.start_background_worker(rec["id"])
                created += 1
            self._refresh_rows()
            self._clear_prepared_preview_storage()
            self._save_current_project_to_disk()
            if created == 1 and requests and int(requests[0].get("prompt_queue_count") or 0) > 1:
                messagebox.showinfo(
                    "AI Video",
                    f"Đã tạo 1 job browser queue với {int(requests[0].get('prompt_queue_count') or 0)} prompt (chạy tuần tự).",
                    parent=self._top,
                )
            elif created > 1:
                messagebox.showinfo("AI Video", f"Đã tạo {created} video (mỗi dòng prompt = 1 video).", parent=self._top)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", str(exc), parent=self._top)

    def _validate_character_reference_before_create(self) -> None:
        """
        Chế độ khóa nhân vật (text_to_video / prompt_to_vertical_video):
        - Auto: bắt buộc có danh sách nhân vật; nếu thiếu ảnh map, luồng prompt sẽ dùng mô tả text
          + bối cảnh + phân cảnh (xem TextToVideoPromptBuilder) thay vì chặn tạo video.
        - Manual: vẫn gợi ý dùng auto khi muốn khóa theo ảnh thực tế (form nhanh chưa có chọn ảnh).
        """
        mode = str(self._current_mode_key() or "").strip().lower()
        if mode not in {"text_to_video", "prompt_to_vertical_video"}:
            return
        char_mode = self._normalize_character_mode(self._var_character_mode.get().strip())
        if char_mode == "manual":
            # Manual hiện chưa có trường chọn ảnh riêng trong form nhanh -> cảnh báo gợi ý chuyển qua auto list.
            raise ValueError(
                "Để khóa đúng nhân vật theo ảnh thực tế, vui lòng dùng chế độ nhân vật 'Tự tạo (auto)' "
                "và map ảnh cho từng nhân vật trong 'Xem/Sửa danh sách nhân vật'."
            )
        chars = migrate_auto_character_profiles(list(self._auto_character_profiles or []))
        if not chars:
            raise ValueError(
                "Chưa có danh sách nhân vật. Hãy bấm 'Tạo nhân vật từ ý tưởng' rồi map ảnh cho từng nhân vật."
            )

    def _maybe_bundle_requests_for_browser_queue(self, requests: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Khi chạy VEO3 browser mode và có nhiều prompt, gom thành 1 request chứa prompt_queue
        để chạy tuần tự trong cùng 1 phiên Flow (đúng quy trình user yêu cầu).
        """
        if len(requests) <= 1:
            return requests
        if not self._is_browser_queue_enabled():
            return requests

        prompts: list[str] = []
        for req in requests:
            p = str(req.get("prompt", "")).strip()
            if p:
                prompts.append(p)
        if len(prompts) <= 1:
            return requests

        base = dict(requests[0])
        opts = dict(base.get("options") or {})
        opts["prompt_queue"] = prompts
        base["options"] = opts
        base["prompt_queue_count"] = len(prompts)
        base["prompt"] = prompts[0]
        return [base]

    def _is_browser_queue_enabled(self) -> bool:
        mode = str(self._current_mode_key() or "").strip().lower()
        if mode not in {"text_to_video", "prompt_to_vertical_video"}:
            return False
        provider = str(self._var_provider.get() or "").strip().lower()
        if provider != "gemini":
            return False
        # Luôn gom queue cho mode browser Gemini để đảm bảo 1 phiên Flow chạy tuần tự,
        # tránh mở lại browser cho từng prompt.
        return True

    def _on_build_prompt_preview(self) -> None:
        if self._preview_worker_running:
            messagebox.showinfo("Prompt preview", "Đang tạo prompt preview, vui lòng chờ hoàn tất.", parent=self._top)
            return
        prompts = self._prompt_lines()
        if not prompts:
            messagebox.showwarning("AI Video", "Nhập ít nhất 1 prompt để tạo preview.", parent=self._top)
            return
        snapshot = self._capture_prompt_build_snapshot()
        est_total = self._estimate_preview_total(prompts=prompts, snapshot=snapshot)
        self._set_preview_worker_running(True)
        self._set_preview_progress(current=0, total=est_total)

        def worker() -> None:
            try:
                requests = self._build_requests_for_prompts_with_snapshot(
                    prompts,
                    snapshot,
                    on_progress=lambda cur: self._top.after(
                        0,
                        lambda c=cur, t=est_total: self._set_preview_progress(current=c, total=t),
                    ),
                )
                self._top.after(0, lambda: self._on_preview_worker_done(requests=requests, error=""))
            except Exception as exc:  # noqa: BLE001
                self._top.after(0, lambda: self._on_preview_worker_done(requests=None, error=str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _on_preview_worker_done(self, *, requests: list[dict[str, Any]] | None, error: str) -> None:
        self._set_preview_worker_running(False)
        if error:
            messagebox.showerror("Prompt preview", error, parent=self._top)
            return
        if not requests:
            messagebox.showwarning("AI Video", "Không sinh được prompt preview.", parent=self._top)
            return
        edited = self._open_prompt_editor_popup(requests)
        if edited is not None:
            self._prepared_requests = edited
            self._prepared_signature = self._current_preview_signature()
            self._refresh_prepared_preview_badge()
            self._persist_prepared_preview_to_disk()
            messagebox.showinfo("Prompt preview", f"Đã lưu {len(edited)} prompt preview để tạo video.", parent=self._top)

    def _set_preview_worker_running(self, running: bool) -> None:
        self._preview_worker_running = running
        if running:
            self._btn_preview_prompts.state(["disabled"])
            self._btn_preview_prompts.configure(text="Đang tạo preview...")
        else:
            self._btn_preview_prompts.state(["!disabled"])
            self._btn_preview_prompts.configure(text="Tạo prompt preview")
            self._var_preview_progress.set("")

    def _set_preview_progress(self, *, current: int, total: int) -> None:
        t = max(1, int(total or 1))
        c = max(0, min(t, int(current or 0)))
        self._var_preview_progress.set(f"Đang tạo prompt: {c}/{t}")

    def _estimate_preview_total(self, *, prompts: list[str], snapshot: dict[str, Any]) -> int:
        mode = str(snapshot.get("mode", "")).strip()
        if mode in {"text_to_video", "prompt_to_vertical_video"}:
            variants = max(1, int(snapshot.get("variants_per_prompt") or 1))
            return max(1, len(prompts) * variants)
        return max(1, len(prompts))

    def _on_open_saved_prompt_preview(self) -> None:
        if not self._prepared_requests:
            messagebox.showinfo("Prompt preview", "Chưa có prompt preview đã lưu.", parent=self._top)
            return
        edited = self._open_prompt_editor_popup(self._prepared_requests)
        if edited is not None:
            self._prepared_requests = edited
            self._prepared_signature = self._current_preview_signature()
            self._refresh_prepared_preview_badge()
            self._persist_prepared_preview_to_disk()
            messagebox.showinfo("Prompt preview", f"Đã cập nhật {len(edited)} prompt preview đã lưu.", parent=self._top)

    def _build_requests_for_prompts(self, prompts: list[str]) -> list[dict[str, Any]]:
        snapshot = self._capture_prompt_build_snapshot()
        return self._build_requests_for_prompts_with_snapshot(prompts, snapshot)

    def _capture_prompt_build_snapshot(self) -> dict[str, Any]:
        return {
            "provider": self._var_provider.get().strip().lower(),
            "model": self._var_model.get().strip(),
            "mode": self._current_mode_key(),
            "assets": {
                "image_path": self._var_image.get().strip(),
                "first_frame_path": self._var_first.get().strip(),
                "last_frame_path": self._var_last.get().strip(),
                "reference_images": [x for x in self._var_refs.get().split("|") if x.strip()],
                "source_video_path": self._var_src_video.get().strip(),
            },
            "options": {
                "aspect_ratio": self._var_aspect.get().strip(),
                "duration_sec": int(self._var_duration.get().strip() or "8"),
                "output_count": self._outputs_count(),
                "outputs": self._var_outputs.get().strip() or "x1",
                "output_dir": self._effective_output_dir(),
                "resolution": self._var_resolution.get().strip(),
                "language": self._var_language.get().strip(),
                "reference_type": self._var_ref_type.get().strip(),
            },
            "topic": self._var_topic.get().strip(),
            "goal": self._var_goal.get().strip(),
            "character_mode": self._normalize_character_mode(self._var_character_mode.get().strip()),
            "character_manual": {
                "name": self._var_character_name.get().strip(),
                "appearance": self._var_character_appearance.get().strip(),
                "outfit": self._var_character_outfit.get().strip(),
                "personality": self._var_character_personality.get().strip(),
            },
            "auto_character_profiles": migrate_auto_character_profiles(list(self._auto_character_profiles)),
            "lock_character_roles": bool(self._var_lock_character_roles.get()),
            "reuse_character_profile": bool(self._reuse_character_profile.get()),
            "variants_per_prompt": max(1, int(self._var_count.get().strip() or "1")),
            "style_prompt": self._selected_style_prompt(),
            "language_hint": self._language_hint_text(self._var_language.get().strip()),
            "visual_name": self._var_visual_style.get().strip(),
            "mood_name": self._var_mood.get().strip(),
            "camera_name": self._var_camera_style.get().strip(),
            "lighting_name": self._var_lighting.get().strip(),
            "motion_name": self._var_motion_style.get().strip(),
            "character_image_style_id": self._var_character_image_style_id.get().strip(),
            "environment_style_id": self._var_environment_style_id.get().strip(),
            "image_style_id": self._var_image_style_id.get().strip(),
            "video_style_id": self._var_video_style_id.get().strip(),
            "camera_style_id": self._var_camera_style_id.get().strip(),
            "lighting_style_id": self._var_lighting_style_id.get().strip(),
            "motion_style_id": self._var_motion_style_id.get().strip(),
            "auto_style_enable": bool(self._var_auto_style_enable.get()),
            "auto_style_reason": self._var_auto_style_reason.get().strip(),
            "auto_style_mood": self._var_auto_style_mood.get().strip(),
            "lock_output_by_project": bool(self._var_lock_output_by_project.get()),
            "prompt_editor_text": self._prompt_text(),
        }

    def _pipeline_characters_to_auto_profiles(self, rows: list[Any]) -> list[dict[str, Any]]:
        """
        Chuyển mảng ``characters`` (Character Bible trong request) sang dạng ``auto_character_profiles`` cho UI.
        Dùng khi nạp file bundle cũ không có form_snapshot.
        Hỗ trợ bible mở rộng (face/hair/body thay vì appearance).
        """
        out: list[dict[str, Any]] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            appearance = str(row.get("appearance", "") or "").strip()
            if not appearance:
                parts = [
                    str(row.get("ethnicity_or_look", "") or "").strip(),
                    str(row.get("face", "") or "").strip(),
                    str(row.get("hair", "") or "").strip(),
                    str(row.get("body", "") or "").strip(),
                ]
                appearance = "; ".join(p for p in parts if p)
            facial = str(row.get("facial_features", "") or "").strip() or str(row.get("face", "") or "").strip()
            cons = str(row.get("consistency_note", "") or "").strip()
            if not cons:
                crules = row.get("consistency_rules")
                if isinstance(crules, list):
                    cons = "; ".join(str(x) for x in crules[:8] if str(x).strip())
            out.append(
                {
                    "character_id": str(row.get("character_id", "") or "").strip(),
                    "name": str(row.get("name", "") or "").strip() or "Nhân vật",
                    "role": str(row.get("role", "") or "").strip(),
                    "gender": str(row.get("gender", "") or "").strip(),
                    "age": str(row.get("age", "") or "").strip(),
                    "appearance": appearance or "consistent appearance",
                    "outfit": str(row.get("outfit", "") or "").strip(),
                    "facial_features": facial or "stable facial identity",
                    "personality": str(row.get("personality", "") or "").strip(),
                    "consistency_note": cons,
                    "reference_image_path": str(row.get("reference_image_path", "") or "").strip(),
                    "character_image_generations": row.get("character_image_generations") or [],
                    "character_image_prompt": str(row.get("character_image_prompt", "") or "").strip(),
                    "image_provider": str(row.get("image_provider", "") or "").strip(),
                    "image_model": str(row.get("image_model", "") or "").strip(),
                }
            )
        return migrate_auto_character_profiles(out)

    def _extract_character_bible_rows(self, r0: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Lấy danh sách dict bible nhân vật từ request (nhiều phiên bản lưu file khác nhau).
        """
        ch = r0.get("characters")
        if isinstance(ch, list) and ch and isinstance(ch[0], dict):
            if str(ch[0].get("name", "")).strip() or "face" in ch[0] or "appearance" in ch[0]:
                return list(ch)
        if isinstance(ch, dict):
            for key in ("rows", "cast", "items"):
                inner = ch.get(key)
                if isinstance(inner, list) and inner and isinstance(inner[0], dict):
                    return list(inner)
        anal = r0.get("analysis")
        if isinstance(anal, dict):
            ac = anal.get("characters")
            if isinstance(ac, list) and ac and isinstance(ac[0], dict):
                return list(ac)
        vm = r0.get("video_map")
        if isinstance(vm, dict):
            vc = vm.get("characters")
            if isinstance(vc, list) and vc and isinstance(vc[0], dict):
                return list(vc)
        return []

    def _parse_supporting_character_string(self, s: str) -> dict[str, Any] | None:
        """Parse một dòng supporting_characters (chuỗi) trong character_profile legacy."""
        raw = str(s or "").strip()
        if not raw or re.match(r"^-\s*none\b", raw, re.I):
            return None
        appearance = ""
        head = raw
        if ":" in raw:
            head, appearance = raw.split(":", 1)
            appearance = appearance.strip()
        name_guess = head.strip()
        if "(" in name_guess:
            name_guess = name_guess.split("(", 1)[0].strip()
        role = "support_character"
        gender = ""
        age = ""
        rm = re.search(r"role\s*=\s*([^,)]+)", raw, re.I)
        if rm:
            role = str(rm.group(1) or "").strip() or role
        gm = re.search(r"gender\s*=\s*([^,)]+)", raw, re.I)
        if gm:
            gender = str(gm.group(1) or "").strip()
        agm = re.search(r"age\s*=\s*([^)]+)", raw, re.I)
        if agm:
            age = str(agm.group(1) or "").strip().rstrip(")").strip()
        return {
            "character_id": "",
            "name": (name_guess or "Nhân vật")[:160],
            "role": role,
            "gender": gender,
            "age": age,
            "appearance": appearance or raw,
            "outfit": "",
            "facial_features": "",
            "personality": "",
            "consistency_note": "",
            "reference_image_path": "",
            "character_image_generations": [],
            "character_image_prompt": "",
            "image_provider": "",
            "image_model": "",
        }

    def _legacy_character_profile_to_auto_profiles(self, r0: dict[str, Any]) -> list[dict[str, Any]]:
        """
        File cũ: chỉ có ``character_profile`` (không có mảng ``characters``).
        """
        prof = r0.get("character_profile")
        if not isinstance(prof, dict):
            return []
        name = str(prof.get("character_name", "")).strip() or "Nhân vật chính"
        cons_parts: list[str] = []
        crules = prof.get("consistency_rules")
        if isinstance(crules, list):
            cons_parts.extend(str(x) for x in crules if str(x).strip())
        main_row: dict[str, Any] = {
            "character_id": str(prof.get("character_id", "") or "").strip(),
            "name": name,
            "role": "main_character",
            "gender": "",
            "age": "",
            "appearance": str(prof.get("character_description", "") or "").strip(),
            "outfit": str(prof.get("outfit", "") or "").strip(),
            "facial_features": str(prof.get("facial_features", "") or "").strip(),
            "personality": str(prof.get("personality", "") or "").strip(),
            "consistency_note": "; ".join(cons_parts) if cons_parts else "",
            "reference_image_path": str(prof.get("reference_image_path", "") or "").strip(),
            "character_image_generations": prof.get("character_image_generations") or [],
            "character_image_prompt": str(prof.get("character_image_prompt", "") or "").strip(),
            "image_provider": str(prof.get("image_provider", "") or "").strip(),
            "image_model": str(prof.get("image_model", "") or "").strip(),
        }
        out: list[dict[str, Any]] = [main_row]
        sup = prof.get("supporting_characters")
        if isinstance(sup, list):
            for item in sup:
                if isinstance(item, str):
                    row = self._parse_supporting_character_string(item)
                    if row:
                        out.append(row)
                elif isinstance(item, dict):
                    out.extend(self._pipeline_characters_to_auto_profiles([item]))
        refs = r0.get("input_assets") if isinstance(r0.get("input_assets"), dict) else {}
        ref_list = refs.get("reference_images") if isinstance(refs, dict) else None
        if isinstance(ref_list, list) and ref_list and not str(main_row.get("reference_image_path", "")).strip():
            fp = str(ref_list[0]).strip()
            if fp:
                main_row["reference_image_path"] = fp
        return migrate_auto_character_profiles(out)

    def _derive_form_snapshot_from_requests(self, reqs: list[dict[str, Any]]) -> dict[str, Any] | None:
        """
        Suy ra snapshot form từ request đầu (file JSON cũ / thiếu form_snapshot).
        """
        if not reqs or not isinstance(reqs[0], dict):
            return None
        r0 = reqs[0]
        snap: dict[str, Any] = {}
        t = str(r0.get("topic", "")).strip()
        if t:
            snap["topic"] = t
        g = str(r0.get("goal", "")).strip()
        if g:
            snap["goal"] = g
        oo = r0.get("options")
        if isinstance(oo, dict) and oo:
            snap["options"] = dict(oo)
        acp = r0.get("auto_character_profiles")
        if isinstance(acp, list) and acp:
            snap["auto_character_profiles"] = list(acp)
            snap["character_mode"] = str(r0.get("character_mode", "auto")).strip().lower() or "auto"
        else:
            bible_rows = self._extract_character_bible_rows(r0)
            if bible_rows:
                snap["auto_character_profiles"] = self._pipeline_characters_to_auto_profiles(bible_rows)
                snap["character_mode"] = "auto"
            else:
                legacy = self._legacy_character_profile_to_auto_profiles(r0)
                if legacy:
                    snap["auto_character_profiles"] = legacy
                    snap["character_mode"] = "auto"
        if isinstance(r0.get("character_manual"), dict):
            snap["character_manual"] = dict(r0["character_manual"])
        for k in ("lock_character_roles", "reuse_character_profile"):
            if k in r0:
                snap[k] = bool(r0[k])
        has_prof = bool(snap.get("auto_character_profiles"))
        if not has_prof and not t and not g and "options" not in snap:
            return None
        return snap

    def _apply_form_snapshot(self, snap: dict[str, Any]) -> None:
        """
        Khôi phục form (nhất là danh sách nhân vật auto) từ snapshot đã lưu trong file bundle hoặc preview disk.
        """
        cm = str(snap.get("character_mode", "auto")).strip().lower()
        self._var_character_mode.set("Thủ công (manual)" if cm == "manual" else "Tự tạo (auto)")
        self._on_character_mode_changed()
        man = snap.get("character_manual")
        if isinstance(man, dict):
            self._var_character_name.set(str(man.get("name", "")).strip())
            self._var_character_appearance.set(str(man.get("appearance", "")).strip())
            self._var_character_outfit.set(str(man.get("outfit", "")).strip())
            self._var_character_personality.set(str(man.get("personality", "")).strip())
        if "auto_character_profiles" in snap:
            raw = snap.get("auto_character_profiles")
            if isinstance(raw, list):
                self._auto_character_profiles = migrate_auto_character_profiles(raw)
        try:
            if "lock_character_roles" in snap:
                self._var_lock_character_roles.set(bool(snap.get("lock_character_roles")))
        except Exception:
            pass
        try:
            if "reuse_character_profile" in snap:
                self._reuse_character_profile.set(bool(snap.get("reuse_character_profile")))
        except Exception:
            pass
        if "topic" in snap:
            self._var_topic.set(str(snap.get("topic", "")).strip())
        if "goal" in snap:
            self._var_goal.set(str(snap.get("goal", "")).strip())
        opts = snap.get("options")
        if isinstance(opts, dict):
            lang = str(opts.get("language", "")).strip()
            if lang:
                self._var_language.set(lang)
            ar = str(opts.get("aspect_ratio", "")).strip()
            if ar and ar in ("16:9", "9:16"):
                self._var_aspect.set(ar)
            res = str(opts.get("resolution", "")).strip()
            if res and res in ("720p", "1080p"):
                self._var_resolution.set(res)
            try:
                d = int(opts.get("duration_sec") or 0)
                if d in (4, 6, 8):
                    self._var_duration.set(str(d))
            except Exception:
                pass
            try:
                oc = int(opts.get("output_count") or 0)
                if 1 <= oc <= 4:
                    self._var_outputs.set(f"x{oc}")
            except Exception:
                pass
            out_dir = str(opts.get("output_dir", "")).strip()
            if out_dir:
                self._var_output_dir.set(out_dir)
        if "variants_per_prompt" in snap:
            try:
                self._var_count.set(str(max(1, int(snap["variants_per_prompt"]))))
            except Exception:
                pass
        for snap_key, var, cb_name in (
            ("visual_name", self._var_visual_style, "_cb_visual_style"),
            ("mood_name", self._var_mood, "_cb_mood"),
            ("camera_name", self._var_camera_style, "_cb_camera"),
            ("lighting_name", self._var_lighting, "_cb_lighting"),
            ("motion_name", self._var_motion_style, "_cb_motion"),
        ):
            if snap_key not in snap:
                continue
            val = str(snap.get(snap_key, "")).strip()
            if not val:
                continue
            cb = getattr(self, cb_name, None)
            if cb is not None:
                vals = list(cb.cget("values") or ())
                if val in vals:
                    var.set(val)
        if "character_image_style_id" in snap:
            self._var_character_image_style_id.set(str(snap.get("character_image_style_id", "")).strip())
        if "environment_style_id" in snap:
            self._var_environment_style_id.set(str(snap.get("environment_style_id", "")).strip())
        for k, var in (
            ("image_style_id", self._var_image_style_id),
            ("video_style_id", self._var_video_style_id),
            ("camera_style_id", self._var_camera_style_id),
            ("lighting_style_id", self._var_lighting_style_id),
            ("motion_style_id", self._var_motion_style_id),
        ):
            if k in snap:
                var.set(str(snap.get(k, "")).strip())
        if "auto_style_enable" in snap:
            self._var_auto_style_enable.set(bool(snap.get("auto_style_enable")))
        if "auto_style_reason" in snap:
            self._var_auto_style_reason.set(str(snap.get("auto_style_reason", "")).strip())
        if "auto_style_mood" in snap:
            self._var_auto_style_mood.set(str(snap.get("auto_style_mood", "")).strip())
        if "lock_output_by_project" in snap:
            self._var_lock_output_by_project.set(bool(snap.get("lock_output_by_project")))
        assets = snap.get("assets")
        if isinstance(assets, dict):
            if str(assets.get("image_path", "")).strip():
                self._var_image.set(str(assets["image_path"]).strip())
            if str(assets.get("first_frame_path", "")).strip():
                self._var_first.set(str(assets["first_frame_path"]).strip())
            if str(assets.get("last_frame_path", "")).strip():
                self._var_last.set(str(assets["last_frame_path"]).strip())
            if str(assets.get("source_video_path", "")).strip():
                self._var_src_video.set(str(assets["source_video_path"]).strip())
            refs = assets.get("reference_images")
            if isinstance(refs, list) and refs:
                self._var_refs.set("|".join(str(x).strip() for x in refs if str(x).strip()))
        if str(snap.get("provider", "")).strip():
            self._var_provider.set(str(snap["provider"]).strip().lower())
        if str(snap.get("model", "")).strip():
            self._var_model.set(str(snap["model"]).strip())
        if "prompt_editor_text" in snap:
            try:
                self._txt_prompt.delete("1.0", tk.END)
                self._txt_prompt.insert("1.0", str(snap.get("prompt_editor_text", "")))
            except Exception:
                pass
        mk = str(snap.get("mode", "")).strip()
        if mk and mk in self._mode_key_to_display:
            self._var_mode_display.set(self._mode_key_to_display[mk])
            self._on_mode_changed()
        self._sync_output_controls_state()
        self._refresh_auto_character_summary()
        try:
            self._on_style_selected()
        except Exception:
            pass

    def _merge_snap_profile_media_into_bible_characters(
        self,
        snap_profiles: list[dict[str, Any]],
        bible_chars: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Gộp ``reference_image_path``, ``character_image_generations`` và metadata ảnh từ UI
        (``auto_character_profiles`` trong snapshot) vào ``characters`` của request (bible Gemini).

        Bible từ pipeline không chứa các field này; nếu không gộp thì file bundle / nạp lại sẽ mất
        danh sách ảnh Nano Banana Pro đã tạo.
        """
        if not bible_chars:
            return []
        profs = migrate_auto_character_profiles(list(snap_profiles or []))
        if not profs:
            return [dict(x) if isinstance(x, dict) else {} for x in bible_chars]
        out: list[dict[str, Any]] = []
        for i, ch in enumerate(bible_chars):
            if not isinstance(ch, dict):
                out.append({})
                continue
            row = dict(ch)
            prof: dict[str, Any] | None = None
            if i < len(profs):
                prof = profs[i]
            else:
                cid = str(row.get("character_id", "")).strip().lower()
                nm = str(row.get("name", "")).strip().lower()
                for p in profs:
                    if not isinstance(p, dict):
                        continue
                    pc = str(p.get("character_id", "")).strip().lower()
                    pn = str(p.get("name", "")).strip().lower()
                    if cid and pc == cid:
                        prof = p
                        break
                    if nm and pn == nm:
                        prof = p
                        break
            if not prof:
                out.append(row)
                continue
            gens = normalize_character_image_generations(prof.get("character_image_generations"))
            if gens:
                row["character_image_generations"] = gens
            cpp = str(prof.get("character_image_prompt", "")).strip()
            if cpp:
                row["character_image_prompt"] = cpp
            for k in ("image_provider", "image_model"):
                v = str(prof.get(k, "")).strip()
                if v:
                    row[k] = v
            pref = str(prof.get("reference_image_path", "")).strip()
            if pref and not str(row.get("reference_image_path", "")).strip():
                row["reference_image_path"] = pref
            pcid = str(prof.get("character_id", "")).strip()
            if pcid and not str(row.get("character_id", "")).strip():
                row["character_id"] = pcid
            out.append(row)
        return out

    def _merge_bible_media_into_auto_profiles(
        self,
        profiles: list[dict[str, Any]],
        bible_chars: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Bổ sung ảnh đã tạo từ ``characters[]`` trong request vào ``_auto_character_profiles``
        khi snapshot thiếu hoặc từ bundle cũ.
        """
        profs = migrate_auto_character_profiles(list(profiles or []))
        if not profs or not bible_chars:
            return profs
        bible = [dict(x) if isinstance(x, dict) else {} for x in bible_chars]
        for i, prof in enumerate(profs):
            bib: dict[str, Any] | None = None
            if i < len(bible):
                bib = bible[i]
            else:
                cid = str(prof.get("character_id", "")).strip().lower()
                nm = str(prof.get("name", "")).strip().lower()
                for b in bible:
                    bc = str(b.get("character_id", "")).strip().lower()
                    bn = str(b.get("name", "")).strip().lower()
                    if cid and bc == cid:
                        bib = b
                        break
                    if nm and bn == nm:
                        bib = b
                        break
            if not isinstance(bib, dict):
                continue
            bg = normalize_character_image_generations(bib.get("character_image_generations"))
            if not bg:
                continue
            pg = normalize_character_image_generations(prof.get("character_image_generations"))
            paths = {str(g.get("character_image_path", "")).strip() for g in pg}
            for g in bg:
                pth = str(g.get("character_image_path", "")).strip()
                if pth and pth not in paths:
                    pg.append(g)
                    paths.add(pth)
            prof["character_image_generations"] = pg
            if str(bib.get("character_image_prompt", "")).strip() and not str(prof.get("character_image_prompt", "")).strip():
                prof["character_image_prompt"] = str(bib.get("character_image_prompt", "")).strip()
            for k in ("image_provider", "image_model"):
                vb = str(bib.get(k, "")).strip()
                if vb and not str(prof.get(k, "")).strip():
                    prof[k] = vb
            br = str(bib.get("reference_image_path", "")).strip()
            if br and not str(prof.get("reference_image_path", "")).strip():
                prof["reference_image_path"] = br
        return migrate_auto_character_profiles(profs)

    def _build_requests_for_prompts_with_snapshot(
        self,
        prompts: list[str],
        snapshot: dict[str, Any],
        *,
        on_progress: Any | None = None,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        shared_profile: dict[str, Any] | None = None
        made = 0

        def progress_tick() -> None:
            nonlocal made
            made += 1
            if callable(on_progress):
                try:
                    on_progress(made)
                except Exception:
                    pass
        variants_per_prompt = int(snapshot.get("variants_per_prompt") or 1)
        requested_outputs = max(1, min(4, int((snapshot.get("options") or {}).get("output_count") or 1)))
        outputs_label = f"x{requested_outputs}"
        for p in prompts:
            req = {
                "provider": str(snapshot.get("provider", "")).strip().lower(),
                "model": str(snapshot.get("model", "")).strip(),
                "mode": str(snapshot.get("mode", "")).strip(),
                "prompt": self._compose_prompt_with_style_snapshot(prompt=p, snapshot=snapshot),
                "input_assets": dict(snapshot.get("assets") or {}),
                "options": {
                    **dict(snapshot.get("options") or {}),
                    "output_count": requested_outputs,
                    "outputs": outputs_label,
                },
            }
            mode = str(req.get("mode", "")).strip()
            if mode in {"text_to_video", "prompt_to_vertical_video"}:
                variants = self._expand_text_prompt_variants(p, variants_per_prompt)
                resolved = self._resolve_auto_story_preset_values_snapshot(idea_text=p, snapshot=snapshot)
                snap_auto_profiles = migrate_auto_character_profiles(list(snapshot.get("auto_character_profiles") or []))
                prebuilt_pipeline_for_variants: dict[str, Any] | None = None
                for variant_idx, pv in enumerate(variants, start=1):
                    req_variant = dict(req)
                    build_input = {
                        "idea": pv,
                        "topic": str(snapshot.get("topic", "")).strip(),
                        "goal": str(snapshot.get("goal", "")).strip(),
                        "language": str((snapshot.get("options") or {}).get("language", "")).strip(),
                        "visual_style": resolved["visual_style"],
                        "camera_style": resolved["camera_style"],
                        "lighting": resolved["lighting"],
                        "motion_style": resolved["motion_style"],
                        "mood": resolved["mood"],
                        "aspect_ratio": str((snapshot.get("options") or {}).get("aspect_ratio", "")).strip(),
                        "duration_sec": int((snapshot.get("options") or {}).get("duration_sec") or 8),
                        "resolution": str((snapshot.get("options") or {}).get("resolution", "")).strip(),
                        "character_mode": str(snapshot.get("character_mode", "auto")).strip(),
                        "character_manual": dict(snapshot.get("character_manual") or {}),
                        "auto_character_profiles": snap_auto_profiles,
                        "lock_character_roles": bool(snapshot.get("lock_character_roles", True)),
                        "style_prompt": str(snapshot.get("style_prompt", "")).strip(),
                        "video_style_id": str(snapshot.get("video_style_id", "")).strip(),
                        "camera_style_id": str(snapshot.get("camera_style_id", "")).strip(),
                        "lighting_style_id": str(snapshot.get("lighting_style_id", "")).strip(),
                        "motion_style_id": str(snapshot.get("motion_style_id", "")).strip(),
                        "mood_text": str(snapshot.get("auto_style_mood", "")).strip(),
                        "environment_style_prompt": style_prompt_addon(
                            "environment_styles",
                            str(snapshot.get("environment_style_id", "")).strip(),
                        ),
                        "character_image_style_prompt": style_prompt_addon(
                            "character_image_styles",
                            str(snapshot.get("character_image_style_id", "")).strip(),
                        ),
                    }
                    # Tối ưu tốc độ preview: với biến thể thứ 2+, tái sử dụng pipeline Gemini của biến thể đầu.
                    # Vẫn giữ khác biệt giữa variants bằng cách thêm "Variant focus" vào final prompt.
                    if variant_idx > 1 and isinstance(prebuilt_pipeline_for_variants, dict) and prebuilt_pipeline_for_variants:
                        build_input["prebuilt_pipeline"] = prebuilt_pipeline_for_variants
                    build = self._t2v_builder.build(
                        build_input,
                        existing_character_profile=shared_profile if bool(snapshot.get("reuse_character_profile")) else None,
                    )
                    if variant_idx == 1:
                        prebuilt_pipeline_for_variants = {
                            "analysis": dict(build.analysis or {}),
                            "characters": list(build.characters or []),
                            "environments": list(build.environments or []),
                            "scenes": list(build.scenes or []),
                            "video_map": dict(build.video_map or {}),
                            "final_prompt": str(build.final_prompt or "").strip(),
                        }
                    if bool(snapshot.get("reuse_character_profile")) and shared_profile is None:
                        shared_profile = dict(build.character_profile)
                    lock_id = str(build.character_profile.get("character_lock_id", "")).strip()
                    base_prompt = str(build.final_prompt or "").strip()
                    if variant_idx > 1:
                        base_prompt = f"{base_prompt}\n\nVariant focus:\n{pv}".strip()
                    anchored_prompt = self._with_character_anchor_prompt(
                        base_prompt=base_prompt,
                        character_profile=build.character_profile,
                    )
                    req_variant["prompt"] = anchored_prompt
                    req_variant["idea"] = build.normalized.get("idea", "")
                    req_variant["topic"] = build.normalized.get("topic", "")
                    req_variant["goal"] = build.normalized.get("goal", "")
                    req_variant["language"] = build.normalized.get("language_provider_label", "")
                    req_variant["visual_style"] = build.normalized.get("visual_style", "")
                    req_variant["scene_plan"] = dict(build.scene_plan)
                    req_variant["character_profile"] = dict(build.character_profile)
                    req_variant["analysis"] = dict(build.analysis or {})
                    bible_chars = list(build.characters or [])
                    req_variant["characters"] = self._merge_snap_profile_media_into_bible_characters(
                        snap_auto_profiles,
                        bible_chars,
                    )
                    # Dự phòng cho nạp bundle thiếu form_snapshot: suy ra profile đầy đủ từ request.
                    req_variant["auto_character_profiles"] = snap_auto_profiles
                    req_variant["environments"] = list(build.environments or [])
                    req_variant["scenes"] = list(build.scenes or [])
                    req_variant["video_map"] = dict(build.video_map or {})
                    req_variant["final_prompt"] = anchored_prompt
                    req_variant["character_lock_id"] = lock_id
                    req_variant["character_profile_id"] = (
                        str(shared_profile.get("character_name", "char_auto")) if shared_profile else "char_auto"
                    )
                    req_variant["variant_index"] = variant_idx
                    req_variant["variant_total"] = len(variants)
                    # Đẩy ảnh map nhân vật vào reference_images để provider có thể dùng trực tiếp.
                    char_refs = [
                        str(x.get("reference_image_path", "")).strip()
                        for x in snap_auto_profiles
                        if str(x.get("reference_image_path", "")).strip()
                    ]
                    if char_refs:
                        assets = dict(req_variant.get("input_assets") or {})
                        refs = [str(x).strip() for x in list(assets.get("reference_images") or []) if str(x).strip()]
                        for p in char_refs:
                            if p not in refs:
                                refs.append(p)
                        assets["reference_images"] = refs
                        req_variant["input_assets"] = assets
                    # Mỗi prompt biến thể tương ứng 1 video.
                    req_variant["options"] = {
                        **dict(req_variant.get("options") or {}),
                        "output_count": requested_outputs,
                        "outputs": outputs_label,
                        "seed": self._stable_seed_from_lock(lock_id),
                    }
                    out.append(req_variant)
                    progress_tick()
            else:
                req["prompt"] = self._compose_prompt_with_style_snapshot(prompt=p, snapshot=snapshot)
                req["options"] = {
                    **dict(req.get("options") or {}),
                    "output_count": requested_outputs,
                    "outputs": outputs_label,
                }
                out.append(req)
                progress_tick()
        return out

    def _outputs_count(self) -> int:
        raw = str(self._var_outputs.get().strip() or "x1").lower()
        if raw.startswith("x"):
            raw = raw[1:]
        try:
            n = int(raw)
        except Exception:
            n = 1
        return max(1, min(4, n))

    def _sanitize_aspect_for_current_mode(self, aspect: str) -> str:
        """9:16 bắt buộc cho mode vertical; còn lại chỉ chấp nhận 16:9 / 9:16."""
        mode = self._current_mode_key()
        if mode in {"prompt_to_vertical_video", "image_to_vertical_video"}:
            return "9:16"
        a = str(aspect or "").strip()
        if a in ("16:9", "9:16"):
            return a
        return "9:16"

    def _patch_prompt_opening_aspect_duration(self, text: str, *, aspect: str, duration_sec: int) -> str:
        """
        Cập nhật dòng mở đầu kiểu Veo/T2V nếu có, để prompt khớp combobox (preview có thể snapshot cũ).
        """
        s = str(text or "")
        if not s.strip():
            return s
        d = max(1, min(120, int(duration_sec)))
        a = self._sanitize_aspect_for_current_mode(aspect)
        s2 = re.sub(
            r"(?mi)^Create\s+a\s+\d+-second\s+(?:9:16|16:9)\s+video\.",
            f"Create a {d}-second {a} video.",
            s,
            count=1,
        )
        return re.sub(
            r"(?mi)^Create\s+an\s+\d+-second\s+(?:9:16|16:9)\s+video\s+for\s+Google\s+Flow",
            f"Create an {d}-second {a} video for Google Flow",
            s2,
            count=1,
        )

    def _merge_current_generation_settings_into_requests(self, requests: list[dict[str, Any]]) -> None:
        """
        Luôn áp dụng combobox trên form chính (tỉ lệ, thời lượng, độ phân giải, outputs, model…)
        vào từng request trước khi submit — tránh preview/bundle JSON giữ options cũ.
        """
        if not requests:
            return
        ar = self._sanitize_aspect_for_current_mode(self._var_aspect.get())
        try:
            dur = int(self._var_duration.get().strip() or "8")
        except ValueError:
            dur = 8
        if dur not in (4, 6, 8):
            dur = 8
        res = str(self._var_resolution.get().strip() or "720p")
        if res not in ("720p", "1080p"):
            res = "720p"
        out_lbl = str(self._var_outputs.get().strip() or "x1")
        oc = self._outputs_count()
        out_dir = self._effective_output_dir()
        prov = self._var_provider.get().strip().lower() or "gemini"
        model = self._var_model.get().strip()
        lang = self._var_language.get().strip()

        for req in requests:
            if model:
                req["model"] = model
            req["provider"] = prov
            opts = dict(req.get("options") or {})
            opts["aspect_ratio"] = ar
            opts["duration_sec"] = dur
            opts["resolution"] = res
            opts["outputs"] = out_lbl
            opts["output_count"] = oc
            if out_dir:
                opts["output_dir"] = out_dir
            if lang:
                opts["language"] = lang
            if model:
                opts["model"] = model
            req["options"] = opts
            vm = req.get("video_map")
            if isinstance(vm, dict) and vm:
                vm2 = dict(vm)
                st = dict(vm2.get("style_settings") or {})
                st["aspect_ratio"] = ar
                st["duration_sec"] = dur
                vm2["style_settings"] = st
                req["video_map"] = vm2
            base = str(req.get("final_prompt") or req.get("prompt") or "")
            if base:
                patched = self._patch_prompt_opening_aspect_duration(base, aspect=ar, duration_sec=dur)
                if patched != base:
                    req["final_prompt"] = patched
                    req["prompt"] = patched

    def _compose_prompt_with_style_snapshot(self, *, prompt: str, snapshot: dict[str, Any]) -> str:
        p = str(prompt or "").strip()
        style = str(snapshot.get("style_prompt", "")).strip()
        lang_hint = str(snapshot.get("language_hint", "")).strip()
        if not style:
            return f"{p}\n\n{lang_hint}".strip()
        if not p:
            return f"Style: {style}\n\n{lang_hint}".strip()
        return f"{p}\n\nStyle guidance: {style}\n\n{lang_hint}".strip()

    def _resolve_auto_story_preset_values_snapshot(self, *, idea_text: str, snapshot: dict[str, Any]) -> dict[str, str]:
        video_sid = str(snapshot.get("video_style_id", "")).strip()
        camera_sid = str(snapshot.get("camera_style_id", "")).strip()
        lighting_sid = str(snapshot.get("lighting_style_id", "")).strip()
        motion_sid = str(snapshot.get("motion_style_id", "")).strip()
        mood_text = str(snapshot.get("auto_style_mood", "")).strip()
        if not mood_text:
            mood_text = str(snapshot.get("mood_text", "")).strip()
        if not mood_text:
            mood_text = str(snapshot.get("mood_name", "")).strip()
        video_addon = style_prompt_addon("video_styles", video_sid)
        camera_addon = style_prompt_addon("camera_styles", camera_sid)
        lighting_addon = style_prompt_addon("lighting_styles", lighting_sid)
        motion_addon = style_prompt_addon("motion_styles", motion_sid)
        if bool(snapshot.get("auto_style_enable")) and (video_addon or camera_addon or lighting_addon or motion_addon):
            return {
                "visual_style": video_addon or self._preset_description("visual_style", str(snapshot.get("visual_name", "")).strip()),
                "mood": mood_text or self._preset_description("mood", str(snapshot.get("mood_name", "")).strip()),
                "camera_style": camera_addon or self._preset_description("camera_style", str(snapshot.get("camera_name", "")).strip()),
                "lighting": lighting_addon or self._preset_description("lighting", str(snapshot.get("lighting_name", "")).strip()),
                "motion_style": motion_addon or self._preset_description("motion_style", str(snapshot.get("motion_name", "")).strip()),
            }

        base = " ".join(
            [
                str(idea_text or "").strip(),
                str(snapshot.get("topic", "")).strip(),
                str(snapshot.get("goal", "")).strip(),
            ]
        ).lower()
        visual_name = str(snapshot.get("visual_name", "")).strip()
        mood_name = str(snapshot.get("mood_name", "")).strip()
        camera_name = str(snapshot.get("camera_name", "")).strip()
        lighting_name = str(snapshot.get("lighting_name", "")).strip()
        motion_name = str(snapshot.get("motion_name", "")).strip()

        if visual_name == self._PRESET_AUTO:
            if any(k in base for k in ("anime", "manga", "2d", "hoạt hình")):
                visual_name = "Anime"
            elif any(k in base for k in ("product", "sản phẩm", "quảng cáo", "review")):
                visual_name = "Quảng cáo sản phẩm"
            elif any(k in base for k in ("street", "đường phố", "travel", "du lịch")):
                visual_name = "Đường phố"
            elif any(k in base for k in ("dark", "bí ẩn", "horror", "kinh dị", "thriller")):
                visual_name = "Tối, bí ẩn"
            elif any(k in base for k in ("premium", "luxury", "cao cấp", "sang trọng")):
                visual_name = "Sang trọng"
            else:
                visual_name = "Điện ảnh"

        if mood_name == self._PRESET_AUTO:
            if any(k in base for k in ("viral", "trend", "fun", "vui", "hài")):
                mood_name = "Vui tươi"
            elif any(k in base for k in ("emotional", "cảm xúc", "drama", "kể chuyện")):
                mood_name = "Cảm xúc"
            elif any(k in base for k in ("energetic", "năng động", "thể thao", "gym")):
                mood_name = "Năng động"
            elif any(k in base for k in ("mysterious", "bí ẩn", "dark")):
                mood_name = "Bí ẩn"
            else:
                mood_name = "Truyền cảm hứng"

        if camera_name == self._PRESET_AUTO:
            if any(k in base for k in ("product", "chi tiết", "close", "cận")):
                camera_name = "Macro sản phẩm"
            elif any(k in base for k in ("vlog", "street", "handheld", "cầm tay")):
                camera_name = "Cầm tay chân thực"
            elif any(k in base for k in ("cinematic", "film", "điện ảnh")):
                camera_name = "Pan điện ảnh"
            else:
                camera_name = "Theo dõi mượt"

        if lighting_name == self._PRESET_AUTO:
            if any(k in base for k in ("night", "đêm", "neon")):
                lighting_name = "Đêm neon"
            elif any(k in base for k in ("studio", "product", "quảng cáo")):
                lighting_name = "Ánh sáng studio"
            elif any(k in base for k in ("golden", "hoàng hôn", "sunset", "giờ vàng")):
                lighting_name = "Giờ vàng"
            else:
                lighting_name = "Ánh sáng tự nhiên dịu"

        if motion_name == self._PRESET_AUTO:
            if any(k in base for k in ("action", "dance", "năng động", "gym", "sport")):
                motion_name = "Năng động"
            elif any(k in base for k in ("product", "xoay", "showcase")):
                motion_name = "Xoay sản phẩm"
            elif any(k in base for k in ("walk", "đi bộ", "street")):
                motion_name = "Cảnh đi bộ"
            else:
                motion_name = "Chậm và mượt"

        return {
            "visual_style": self._preset_description("visual_style", visual_name),
            "mood": self._preset_description("mood", mood_name),
            "camera_style": self._preset_description("camera_style", camera_name),
            "lighting": self._preset_description("lighting", lighting_name),
            "motion_style": self._preset_description("motion_style", motion_name),
        }

    def _show_prompt_preview_and_confirm(self, requests: list[dict[str, Any]]) -> bool:
        top = tk.Toplevel(self._top)
        top.title("Preview Prompt trước khi submit")
        top.geometry("980x640")
        top.transient(self._top)
        top.grab_set()

        root = ttk.Frame(top, padding=8)
        root.pack(fill=tk.BOTH, expand=True)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(2, weight=1)

        ttk.Label(
            root,
            text=f"Sẽ tạo {len(requests)} video. Kiểm tra prompt từng video trước khi submit.",
            font=("Segoe UI", 10, "bold"),
        ).grid(row=0, column=0, sticky="w")

        pick_fr = ttk.Frame(root)
        pick_fr.grid(row=1, column=0, sticky="ew", pady=(6, 6))
        pick_fr.columnconfigure(1, weight=1)
        ttk.Label(pick_fr, text="Video preview").grid(row=0, column=0, sticky="w")
        labels = [
            f"#{i+1} - {str((r.get('idea') or str(r.get('prompt', ''))[:60])).strip()[:90]}"
            for i, r in enumerate(requests)
        ]
        idx_var = tk.StringVar(value=labels[0] if labels else "")
        cb = ttk.Combobox(pick_fr, state="readonly", values=labels, textvariable=idx_var)
        cb.grid(row=0, column=1, sticky="ew", padx=(8, 0))
        anchor_var = tk.StringVar(value="Character anchor: -")
        ttk.Label(pick_fr, textvariable=anchor_var, foreground="#475569").grid(
            row=1, column=0, columnspan=2, sticky="w", pady=(6, 0)
        )

        txt_fr = ttk.Frame(root)
        txt_fr.grid(row=2, column=0, sticky="nsew")
        txt_fr.columnconfigure(0, weight=1)
        txt_fr.rowconfigure(0, weight=1)
        txt = tk.Text(txt_fr, wrap="word", font=("Consolas", 9))
        txt.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(txt_fr, orient=tk.VERTICAL, command=txt.yview)
        sy.grid(row=0, column=1, sticky="ns")
        sx = ttk.Scrollbar(txt_fr, orient=tk.HORIZONTAL, command=txt.xview)
        sx.grid(row=1, column=0, sticky="ew")
        txt.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)

        def render_current() -> None:
            if not labels:
                return
            try:
                idx = labels.index(idx_var.get())
            except ValueError:
                idx = 0
            r = requests[idx]
            anchor_var.set(f"Character anchor: {self._character_anchor_of_request(r)}")
            txt.configure(state="normal")
            txt.delete("1.0", tk.END)
            txt.insert("1.0", str(r.get("final_prompt") or r.get("prompt") or ""))
            txt.configure(state="disabled")

        cb.bind("<<ComboboxSelected>>", lambda _e: render_current())
        render_current()

        decision = {"ok": False}
        act = ttk.Frame(root)
        act.grid(row=3, column=0, sticky="e", pady=(8, 0))

        def do_confirm() -> None:
            decision["ok"] = True
            top.destroy()

        ttk.Button(act, text="Hủy", command=top.destroy).pack(side=tk.RIGHT)
        ttk.Button(act, text="Xác nhận & Submit", command=do_confirm).pack(side=tk.RIGHT, padx=(0, 6))
        top.wait_window()
        return bool(decision["ok"])

    def _open_prompt_editor_popup(self, requests: list[dict[str, Any]]) -> list[dict[str, Any]] | None:
        data = [dict(x) for x in requests]
        if not data:
            return None
        top = tk.Toplevel(self._top)
        top.title("Prompt preview — Sửa/Xóa trước khi tạo video")
        top.geometry("1050x700")
        top.transient(self._top)
        top.grab_set()

        root = ttk.Frame(top, padding=8)
        root.pack(fill=tk.BOTH, expand=True)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(2, weight=3)
        root.rowconfigure(3, weight=2)

        lbl_count = ttk.Label(root, text="")
        lbl_count.grid(row=0, column=0, sticky="w")

        pick_fr = ttk.Frame(root)
        pick_fr.grid(row=1, column=0, sticky="ew", pady=(6, 6))
        pick_fr.columnconfigure(1, weight=1)
        ttk.Label(pick_fr, text="Prompt item").grid(row=0, column=0, sticky="w")
        idx_var = tk.StringVar(value="")
        cb = ttk.Combobox(pick_fr, state="readonly", textvariable=idx_var)
        cb.grid(row=0, column=1, sticky="ew", padx=(8, 0))
        anchor_var = tk.StringVar(value="Character anchor: -")
        ttk.Label(pick_fr, textvariable=anchor_var, foreground="#475569").grid(
            row=1, column=0, columnspan=2, sticky="w", pady=(6, 0)
        )

        txt_fr = ttk.Frame(root)
        txt_fr.grid(row=2, column=0, sticky="nsew")
        txt_fr.columnconfigure(0, weight=1)
        txt_fr.rowconfigure(0, weight=1)
        txt = tk.Text(txt_fr, wrap="word", font=("Consolas", 9))
        txt.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(txt_fr, orient=tk.VERTICAL, command=txt.yview)
        sy.grid(row=0, column=1, sticky="ns")
        sx = ttk.Scrollbar(txt_fr, orient=tk.HORIZONTAL, command=txt.xview)
        sx.grid(row=1, column=0, sticky="ew")
        txt.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)

        comp_fr = ttk.LabelFrame(root, text="Prompt thành phần (có thể chỉnh sửa)", padding=6)
        comp_fr.grid(row=3, column=0, sticky="nsew", pady=(8, 0))
        comp_fr.columnconfigure(1, weight=1)
        comp_fr.rowconfigure(1, weight=1)
        ttk.Label(comp_fr, text="Thành phần").grid(row=0, column=0, sticky="w")
        comp_var = tk.StringVar(value="")
        comp_cb = ttk.Combobox(comp_fr, state="readonly", textvariable=comp_var)
        comp_cb.grid(row=0, column=1, sticky="ew", padx=(8, 0))
        comp_txt = tk.Text(comp_fr, wrap="word", font=("Consolas", 9))
        comp_txt.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(6, 0))
        comp_sy = ttk.Scrollbar(comp_fr, orient=tk.VERTICAL, command=comp_txt.yview)
        comp_sy.grid(row=1, column=2, sticky="ns", pady=(6, 0))
        comp_txt.configure(yscrollcommand=comp_sy.set)

        state = {"current": 0}
        comp_state: dict[str, Any] = {"sections": []}

        def label_of(i: int, r: dict[str, Any]) -> str:
            idea = str(r.get("idea", "") or "").strip()
            return f"#{i+1} - {idea[:80] if idea else 'Prompt item'}"

        def refresh_labels(select_idx: int | None = None) -> None:
            labels = [label_of(i, r) for i, r in enumerate(data)]
            cb.configure(values=labels)
            lbl_count.configure(text=f"Tổng prompt preview: {len(data)}")
            if not labels:
                idx_var.set("")
                txt.delete("1.0", tk.END)
                return
            si = state["current"] if select_idx is None else max(0, min(len(labels) - 1, select_idx))
            state["current"] = si
            idx_var.set(labels[si])
            render_current()

        def save_current_text() -> None:
            if not data:
                return
            i = state["current"]
            data[i]["prompt"] = txt.get("1.0", tk.END).strip()
            if str(data[i].get("final_prompt", "")).strip():
                data[i]["final_prompt"] = data[i]["prompt"]

        def render_current() -> None:
            if not data:
                txt.delete("1.0", tk.END)
                anchor_var.set("Character anchor: -")
                comp_cb.configure(values=[])
                comp_var.set("")
                comp_txt.delete("1.0", tk.END)
                return
            i = state["current"]
            anchor_var.set(f"Character anchor: {self._character_anchor_of_request(data[i])}")
            txt.delete("1.0", tk.END)
            txt.insert("1.0", str(data[i].get("prompt", "") or ""))
            sections = self._component_prompt_sections(data[i])
            comp_state["sections"] = sections
            labels = [str(x[0]) for x in sections]
            comp_cb.configure(values=labels)
            if labels:
                comp_var.set(labels[0])
                render_component()
            else:
                comp_var.set("")
                comp_txt.delete("1.0", tk.END)
                comp_txt.insert("1.0", "(Không có dữ liệu prompt thành phần)")

        def save_current_component_text() -> None:
            sections = list(comp_state.get("sections") or [])
            if not sections:
                return
            selected = str(comp_var.get() or "").strip()
            body = comp_txt.get("1.0", tk.END).strip()
            updated: list[tuple[str, str]] = []
            for title, old_body in sections:
                if str(title) == selected:
                    updated.append((str(title), body))
                else:
                    updated.append((str(title), str(old_body)))
            comp_state["sections"] = updated

        def render_component() -> None:
            sections = list(comp_state.get("sections") or [])
            selected = str(comp_var.get() or "").strip()
            text = ""
            for title, body in sections:
                if str(title) == selected:
                    text = str(body or "")
                    break
            if not text and sections:
                text = str(sections[0][1] or "")
            comp_txt.delete("1.0", tk.END)
            comp_txt.insert("1.0", text or "(Trống)")

        def on_pick(_e: tk.Event) -> None:
            save_current_text()
            save_current_component_text()
            labels = list(cb.cget("values") or [])
            try:
                state["current"] = labels.index(idx_var.get())
            except ValueError:
                state["current"] = 0
            render_current()

        cb.bind("<<ComboboxSelected>>", on_pick)
        def on_pick_component(_e: tk.Event) -> None:
            save_current_component_text()
            render_component()

        comp_cb.bind("<<ComboboxSelected>>", on_pick_component)

        regen_fr = ttk.LabelFrame(root, text="Tạo lại từng thành phần", padding=6)
        regen_fr.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        act = ttk.Frame(root)
        act.grid(row=5, column=0, sticky="ew", pady=(8, 0))
        act.columnconfigure(0, weight=1)

        def do_export_bundle() -> None:
            """Xuất toàn bộ prompt đang preview ra file JSON để dùng sau (kèm Nạp từ file trên màn chính)."""
            save_current_text()
            save_current_component_text()
            if data:
                cur = state["current"]
                self._apply_component_sections_to_request(data[cur], list(comp_state.get("sections") or []))
            if not data:
                messagebox.showwarning("Prompt preview", "Chưa có prompt để xuất.", parent=top)
                return
            default_dir = self._prompt_bundle_export_dir()
            default_name = f"prompt_bundle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            path = filedialog.asksaveasfilename(
                parent=top,
                title="Lưu toàn bộ prompt ra file",
                initialdir=str(default_dir),
                initialfile=default_name,
                defaultextension=".json",
                filetypes=[("JSON", "*.json"), ("Tất cả", "*.*")],
            )
            if not path:
                return
            sig = self._current_preview_signature()
            payload: dict[str, Any] = {
                "version": 2,
                "signature": sig,
                "requests": list(data),
                "form_snapshot": self._capture_prompt_build_snapshot(),
                "saved_at": datetime.now().replace(microsecond=0).isoformat(),
            }
            text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
            try:
                write_resilient_text_file(Path(path), text, tmp_prefix="prompt_export_")
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("Prompt preview", f"Không ghi được file:\n{exc}", parent=top)
                return
            messagebox.showinfo("Prompt preview", f"Đã lưu {len(data)} prompt vào:\n{path}", parent=top)

        def do_delete() -> None:
            if not data:
                return
            save_current_component_text()
            i = state["current"]
            if not messagebox.askyesno("Xóa prompt", f"Xóa prompt #{i+1} khỏi preview?", parent=top):
                return
            data.pop(i)
            refresh_labels(select_idx=max(0, i - 1))

        def do_save_close() -> None:
            save_current_text()
            save_current_component_text()
            if data:
                cur = state["current"]
                self._apply_component_sections_to_request(data[cur], list(comp_state.get("sections") or []))
            if not data:
                messagebox.showwarning("Prompt preview", "Danh sách prompt đang trống.", parent=top)
                return
            top.destroy()

        result: dict[str, Any] = {"ok": False}

        def do_confirm() -> None:
            do_save_close()
            if data:
                result["ok"] = True

        def do_apply_component_to_final_prompt() -> None:
            if not data:
                return
            save_current_component_text()
            i = state["current"]
            req = data[i]
            try:
                self._apply_component_sections_to_request(req, list(comp_state.get("sections") or []))
                self._remap_request_from_components(req)
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror(
                    "Prompt thành phần",
                    f"Không map lại được Final Prompt:\n{exc}",
                    parent=top,
                )
                return
            data[i] = req
            txt.delete("1.0", tk.END)
            txt.insert("1.0", str(req.get("prompt") or ""))
            comp_state["sections"] = self._component_prompt_sections(req)
            render_component()
            messagebox.showinfo(
                "Prompt thành phần",
                "Đã map lại Video Map và Final Prompt từ các thành phần vừa chỉnh.",
                parent=top,
            )

        def do_regenerate_component(kind: str) -> None:
            if not data:
                return
            save_current_text()
            save_current_component_text()
            i = state["current"]
            req = data[i]
            try:
                self._apply_component_sections_to_request(req, list(comp_state.get("sections") or []))
                input_data = self._request_to_builder_input(req)
                override_idea = txt_regen_idea.get("1.0", tk.END).strip()
                analysis_override: dict[str, Any] = {}
                if override_idea:
                    input_data["idea"] = override_idea
                    # Nếu user nhập yêu cầu nâng cao, ưu tiên phân tích lại theo ý tưởng mới
                    # để các thành phần regenerate bám đúng intent vừa nhập.
                    analysis_override = self._t2v_builder.analyze_video_idea_with_gemini(input_data)
                if kind == "analysis":
                    analysis = analysis_override or self._t2v_builder.analyze_video_idea_with_gemini(input_data)
                    if not analysis:
                        raise ValueError("Gemini không trả analysis.")
                    req["analysis"] = analysis
                elif kind == "characters":
                    analysis = analysis_override or dict(req.get("analysis") or {})
                    if not analysis:
                        raise ValueError("Thiếu analysis. Hãy tạo lại Analysis trước.")
                    rows = self._t2v_builder.build_character_bible_with_gemini(analysis=analysis, input_data=input_data)
                    if not rows:
                        raise ValueError("Gemini không trả character bible.")
                    if analysis_override:
                        req["analysis"] = analysis
                    req["characters"] = rows
                elif kind == "environments":
                    analysis = analysis_override or dict(req.get("analysis") or {})
                    if not analysis:
                        raise ValueError("Thiếu analysis. Hãy tạo lại Analysis trước.")
                    rows = self._t2v_builder.build_environment_bible_with_gemini(analysis=analysis, input_data=input_data)
                    if not rows:
                        raise ValueError("Gemini không trả environment bible.")
                    if analysis_override:
                        req["analysis"] = analysis
                    req["environments"] = rows
                elif kind == "scenes":
                    analysis = analysis_override or dict(req.get("analysis") or {})
                    characters = list(req.get("characters") or [])
                    environments = list(req.get("environments") or [])
                    if not analysis:
                        raise ValueError("Thiếu analysis. Hãy tạo lại Analysis trước.")
                    if not characters:
                        raise ValueError("Thiếu characters. Hãy tạo lại Character Bible trước.")
                    if not environments:
                        raise ValueError("Thiếu environments. Hãy tạo lại Environment Bible trước.")
                    rows = self._t2v_builder.build_scene_breakdown_with_gemini(
                        analysis=analysis,
                        characters=characters,
                        environments=environments,
                        input_data=input_data,
                    )
                    if not rows:
                        raise ValueError("Gemini không trả scene breakdown.")
                    if analysis_override:
                        req["analysis"] = analysis
                    req["scenes"] = rows
                elif kind == "video_map":
                    self._remap_request_from_components(req)
                elif kind == "final_prompt":
                    vm = dict(req.get("video_map") or {})
                    if not vm:
                        self._remap_request_from_components(req)
                    else:
                        fp = self._t2v_builder.build_final_veo_prompt_from_video_map(vm)
                        if not fp.strip():
                            raise ValueError("Không build được final prompt.")
                        req["prompt"] = fp
                        req["final_prompt"] = fp
                else:
                    raise ValueError(f"Loại thành phần không hỗ trợ: {kind}")
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("Tạo lại thành phần", f"Không tạo lại được {kind}:\n{exc}", parent=top)
                return
            data[i] = req
            txt.delete("1.0", tk.END)
            txt.insert("1.0", str(req.get("prompt") or ""))
            comp_state["sections"] = self._component_prompt_sections(req)
            labels = [str(x[0]) for x in list(comp_state.get("sections") or [])]
            comp_cb.configure(values=labels)
            if labels:
                if kind == "analysis":
                    comp_var.set("1) Idea Analysis")
                elif kind == "characters":
                    comp_var.set("2) Character Bible")
                elif kind == "environments":
                    comp_var.set("3) Environment Bible")
                elif kind == "scenes":
                    comp_var.set("4) Scene Breakdown")
                elif kind == "video_map":
                    comp_var.set("5) Video Prompt Map")
                else:
                    comp_var.set("6) Final Prompt")
            render_component()
            messagebox.showinfo("Tạo lại thành phần", f"Đã tạo lại {kind} thành công.", parent=top)

        ttk.Button(regen_fr, text="Analysis", command=lambda: do_regenerate_component("analysis")).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(regen_fr, text="Character Bible", command=lambda: do_regenerate_component("characters")).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(regen_fr, text="Environment", command=lambda: do_regenerate_component("environments")).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(regen_fr, text="Scene", command=lambda: do_regenerate_component("scenes")).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(regen_fr, text="Video Map", command=lambda: do_regenerate_component("video_map")).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(regen_fr, text="Final Prompt", command=lambda: do_regenerate_component("final_prompt")).pack(
            side=tk.LEFT
        )
        regen_input_fr = ttk.Frame(regen_fr)
        regen_input_fr.pack(side=tk.TOP, fill=tk.X, pady=(8, 0))
        ttk.Label(
            regen_input_fr,
            text="Ý tưởng nâng cao khi tạo lại (tùy chọn, sẽ override cho lần regenerate này):",
        ).pack(side=tk.TOP, anchor="w")
        txt_regen_idea = tk.Text(regen_input_fr, height=3, wrap="word", font=("Segoe UI", 9))
        txt_regen_idea.pack(side=tk.TOP, fill=tk.X, pady=(4, 0))

        ttk.Button(act, text="Lưu tất cả ra file…", command=do_export_bundle).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(act, text="Map lại Final Prompt từ thành phần", command=do_apply_component_to_final_prompt).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(act, text="Xóa prompt hiện tại", command=do_delete).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(act, text="Hủy", command=top.destroy).pack(side=tk.RIGHT)
        ttk.Button(act, text="Lưu preview", command=do_confirm).pack(side=tk.RIGHT, padx=(0, 6))

        refresh_labels(0)
        top.wait_window()
        if result["ok"]:
            return data
        return None

    def _component_prompt_sections(self, req: dict[str, Any]) -> list[tuple[str, str]]:
        sections: list[tuple[str, str]] = []
        analysis = dict(req.get("analysis") or {})
        if analysis:
            sections.append(("1) Idea Analysis", json.dumps(analysis, ensure_ascii=False, indent=2)))
        characters = list(req.get("characters") or [])
        if characters:
            sections.append(("2) Character Bible", json.dumps(characters, ensure_ascii=False, indent=2)))
        environments = list(req.get("environments") or [])
        if environments:
            sections.append(("3) Environment Bible", json.dumps(environments, ensure_ascii=False, indent=2)))
        scenes = list(req.get("scenes") or [])
        if scenes:
            sections.append(("4) Scene Breakdown", json.dumps(scenes, ensure_ascii=False, indent=2)))
        video_map = dict(req.get("video_map") or {})
        if video_map:
            sections.append(("5) Video Prompt Map", json.dumps(video_map, ensure_ascii=False, indent=2)))
        final_prompt = str(req.get("final_prompt") or req.get("prompt") or "").strip()
        if final_prompt:
            sections.append(("6) Final Prompt", final_prompt))
        if not sections:
            sections.append(("Final Prompt", str(req.get("prompt") or "").strip()))
        return sections

    def _apply_component_sections_to_request(self, req: dict[str, Any], sections: list[tuple[str, str]]) -> None:
        for title, body in sections:
            t = str(title or "").strip().lower()
            raw = str(body or "").strip()
            if not raw:
                continue
            if "analysis" in t:
                try:
                    req["analysis"] = json.loads(raw)
                except Exception as exc:  # noqa: BLE001
                    raise ValueError(f"Analysis JSON không hợp lệ: {exc}") from exc
            elif "character bible" in t:
                try:
                    req["characters"] = json.loads(raw)
                except Exception as exc:  # noqa: BLE001
                    raise ValueError(f"Character Bible JSON không hợp lệ: {exc}") from exc
            elif "environment bible" in t:
                try:
                    req["environments"] = json.loads(raw)
                except Exception as exc:  # noqa: BLE001
                    raise ValueError(f"Environment Bible JSON không hợp lệ: {exc}") from exc
            elif "scene breakdown" in t:
                try:
                    req["scenes"] = json.loads(raw)
                except Exception as exc:  # noqa: BLE001
                    raise ValueError(f"Scene Breakdown JSON không hợp lệ: {exc}") from exc
            elif "video prompt map" in t:
                try:
                    req["video_map"] = json.loads(raw)
                except Exception as exc:  # noqa: BLE001
                    raise ValueError(f"Video Prompt Map JSON không hợp lệ: {exc}") from exc
            elif "final prompt" in t:
                req["prompt"] = raw
                req["final_prompt"] = raw

    def _request_to_builder_input(self, req: dict[str, Any]) -> dict[str, Any]:
        opts = dict(req.get("options") or {})
        vm_style = dict(req.get("video_map", {}).get("style_settings") or {})
        return {
            "idea": str(req.get("idea") or "").strip(),
            "topic": str(req.get("topic") or "").strip(),
            "goal": str(req.get("goal") or "").strip() or "viral",
            "language_provider_label": str(req.get("language") or "").strip() or "Vietnamese",
            "visual_style": str(req.get("visual_style") or vm_style.get("visual_style") or "").strip(),
            "aspect_ratio": str(opts.get("aspect_ratio") or vm_style.get("aspect_ratio") or "9:16").strip(),
            "duration_sec": int(opts.get("duration_sec") or vm_style.get("duration_sec") or 8),
            "camera_style": str(vm_style.get("camera_style") or "").strip(),
            "lighting": str(vm_style.get("lighting") or "").strip(),
            "mood": str(vm_style.get("mood") or "").strip(),
        }

    def _remap_request_from_components(self, req: dict[str, Any]) -> None:
        analysis = dict(req.get("analysis") or {})
        characters = list(req.get("characters") or [])
        environments = list(req.get("environments") or [])
        scenes = list(req.get("scenes") or [])
        if not analysis:
            raise ValueError("Thiếu analysis.")
        if not characters:
            raise ValueError("Thiếu character bible.")
        if not environments:
            raise ValueError("Thiếu environment bible.")
        if not scenes:
            raise ValueError("Thiếu scene breakdown.")
        input_data = self._request_to_builder_input(req)
        video_map = self._t2v_builder.build_video_prompt_map(
            analysis=analysis,
            characters=characters,
            environments=environments,
            scenes=scenes,
            input_data=input_data,
        )
        final_prompt = self._t2v_builder.build_final_veo_prompt_from_video_map(video_map)
        req["video_map"] = video_map
        req["prompt"] = final_prompt
        req["final_prompt"] = final_prompt

    def _character_anchor_of_request(self, req: dict[str, Any]) -> str:
        lock_id = str(req.get("character_lock_id", "")).strip()
        if lock_id:
            return lock_id
        prof = req.get("character_profile")
        if isinstance(prof, dict):
            lock_id = str(prof.get("character_lock_id", "")).strip()
            if lock_id:
                return lock_id
        fallback = str(req.get("character_profile_id", "")).strip()
        return fallback or "N/A"

    def _stable_seed_from_lock(self, lock_id: str) -> int:
        raw = str(lock_id or "").strip().lower() or "char-default"
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
        return int(digest, 16)

    def _with_character_anchor_prompt(self, *, base_prompt: str, character_profile: dict[str, Any]) -> str:
        lock_id = str(character_profile.get("character_lock_id", "")).strip() or "char-default"
        name = str(character_profile.get("character_name", "")).strip() or "Main Character"
        face = str(character_profile.get("facial_features", "")).strip()
        outfit = str(character_profile.get("outfit", "")).strip()
        anchor = (
            f"Character Continuity Anchor:\n"
            f"- LOCK_ID: {lock_id}\n"
            f"- CAST_MAIN: {name}\n"
            f"- FACE_SIGNATURE: {face}\n"
            f"- OUTFIT_SIGNATURE: {outfit}\n"
            "- Hard rule: preserve identical cast identity across every generated video under this lock id.\n"
        ).strip()
        return f"{anchor}\n\n{str(base_prompt or '').strip()}".strip()

    def _restore_prepared_preview_from_disk(self) -> None:
        """Khôi phục prompt preview đã lưu (chưa bấm Tạo video) sau khi mở lại chương trình."""
        data = load_prepared_prompt_preview()
        if not data:
            return
        sig, reqs, form_snap = data
        self._prepared_requests = reqs
        self._prepared_signature = sig
        self._refresh_prepared_preview_badge()
        if isinstance(form_snap, dict) and form_snap:
            self._apply_form_snapshot(form_snap)
        try:
            if reqs:
                r0 = reqs[0]
                if isinstance(r0, dict):
                    self._auto_character_profiles = self._merge_bible_media_into_auto_profiles(
                        list(self._auto_character_profiles or []),
                        r0.get("characters") or [],
                    )
                    self._refresh_auto_character_summary()
        except Exception:
            pass

    def _load_project_from_disk_payload(self, pl: dict[str, Any]) -> None:
        """Nạp trạng thái dự án từ file ``projects/{id}.json`` (form + preview + nhân vật)."""
        fs = pl.get("form_snapshot")
        if isinstance(fs, dict) and fs:
            self._apply_form_snapshot(fs)
        pr = pl.get("prepared_requests")
        if isinstance(pr, list):
            self._prepared_requests = [dict(x) for x in pr if isinstance(x, dict)]
        else:
            self._prepared_requests = []
        self._prepared_signature = str(pl.get("prepared_signature", "") or "")
        self._refresh_prepared_preview_badge()
        try:
            if self._prepared_requests:
                r0 = self._prepared_requests[0]
                if isinstance(r0, dict):
                    self._auto_character_profiles = self._merge_bible_media_into_auto_profiles(
                        list(self._auto_character_profiles or []),
                        r0.get("characters") or [],
                    )
                    self._refresh_auto_character_summary()
        except Exception:
            pass
        self._effective_output_dir()

    def _collect_video_job_ids_for_current_project(self) -> list[str]:
        pid = str(self._current_project_id or "").strip()
        if not pid:
            return []
        out: list[str] = []
        for r in self._svc.list_records():
            if str(r.get("project_id", "") or "").strip() == pid:
                vid = str(r.get("id", "") or "").strip()
                if vid:
                    out.append(vid)
        return out

    def _default_output_dir_for_project(self, project_id: str) -> str:
        """Thư mục output mặc định theo từng dự án."""
        return ai_video_project_output_dir(project_id).as_posix()

    def _effective_output_dir(self) -> str:
        """
        Trả thư mục output hợp lệ cho project hiện tại.
        - Nếu ô output trống: tự fallback về ``outputs/{project_id}``.
        - Luôn tạo sẵn thư mục để các bước sau không bị lỗi path.
        """
        if bool(self._var_lock_output_by_project.get()):
            normalized = self._default_output_dir_for_project(self._current_project_id)
            if self._var_output_dir.get().strip() != normalized:
                self._var_output_dir.set(normalized)
            return normalized
        raw = str(self._var_output_dir.get().strip() or "")
        try:
            if raw:
                out = Path(raw).expanduser().resolve()
                out.mkdir(parents=True, exist_ok=True)
            else:
                out = ai_video_project_output_dir(self._current_project_id)
            normalized = out.as_posix()
        except Exception:
            normalized = self._default_output_dir_for_project(self._current_project_id)
        if self._var_output_dir.get().strip() != normalized:
            self._var_output_dir.set(normalized)
        return normalized

    def _save_current_project_to_disk(self) -> None:
        """Lưu snapshot dự án (form, ô prompt, preview, danh sách job id) vào ``data/ai_video/projects``."""
        try:
            body: dict[str, Any] = {
                "version": 1,
                "project_id": str(self._current_project_id or "").strip(),
                "project_name": self._var_project_name.get().strip() or "Dự án",
                "form_snapshot": self._capture_prompt_build_snapshot(),
                "prepared_signature": self._prepared_signature,
                "prepared_requests": [dict(x) for x in self._prepared_requests],
                "video_job_ids": self._collect_video_job_ids_for_current_project(),
            }
            save_ai_video_project_file(body)
        except Exception:
            pass

    def _persist_prepared_preview_to_disk(self) -> None:
        """Ghi prompt preview + signature form ra `data/ai_video/temp/prepared_prompt_preview.json`."""
        save_prepared_prompt_preview(
            signature=self._prepared_signature,
            requests=self._prepared_requests,
            form_snapshot=self._capture_prompt_build_snapshot(),
        )
        self._save_current_project_to_disk()

    def _clear_prepared_preview_storage(self) -> None:
        """Xóa preview trong RAM và trên disk."""
        self._prepared_requests = []
        self._prepared_signature = ""
        self._refresh_prepared_preview_badge()
        clear_prepared_prompt_preview()

    def _prompt_bundle_export_dir(self) -> Path:
        """Thư mục mặc định để xuất/nạp file JSON bộ prompt."""
        from src.services.ai_video_store import ensure_ai_video_layout

        p = ensure_ai_video_layout()["root"] / "inputs" / "prompts"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _on_import_prompt_bundle_file(self) -> None:
        """Nạp toàn bộ prompt từ file JSON (xuất bằng « Lưu tất cả ra file » hoặc cùng format với preview đã lưu)."""
        initial = self._prompt_bundle_export_dir()
        path = filedialog.askopenfilename(
            parent=self._top,
            title="Chọn file JSON bộ prompt",
            initialdir=str(initial),
            filetypes=[("JSON", "*.json"), ("Tất cả", "*.*")],
        )
        if not path:
            return
        try:
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", f"Không đọc được file:\n{exc}", parent=self._top)
            return
        if not isinstance(raw, dict):
            messagebox.showerror("AI Video", "File không đúng định dạng (cần object JSON).", parent=self._top)
            return
        reqs = raw.get("requests")
        if not isinstance(reqs, list) or not reqs:
            messagebox.showerror("AI Video", "File thiếu mảng requests hoặc đang rỗng.", parent=self._top)
            return
        out_reqs: list[dict[str, Any]] = []
        for x in reqs:
            if isinstance(x, dict):
                out_reqs.append(dict(x))
        if not out_reqs:
            messagebox.showerror("AI Video", "Không có request hợp lệ trong file.", parent=self._top)
            return
        self._prepared_requests = out_reqs
        self._prepared_signature = str(raw.get("signature") or "").strip() or self._current_preview_signature()
        self._refresh_prepared_preview_badge()
        form_snap = raw.get("form_snapshot")
        if not isinstance(form_snap, dict) or not form_snap:
            form_snap = self._derive_form_snapshot_from_requests(out_reqs)
        if isinstance(form_snap, dict) and form_snap:
            self._apply_form_snapshot(form_snap)
        try:
            self._auto_character_profiles = self._merge_bible_media_into_auto_profiles(
                list(self._auto_character_profiles or []),
                out_reqs[0].get("characters") or [],
            )
            self._refresh_auto_character_summary()
        except Exception:
            pass
        self._persist_prepared_preview_to_disk()
        messagebox.showinfo("AI Video", f"Đã nạp {len(out_reqs)} prompt từ file.\nBấm « Tạo video » khi sẵn sàng.", parent=self._top)

    def _refresh_prepared_preview_badge(self) -> None:
        n = len(self._prepared_requests)
        self._lbl_prepared_preview.configure(
            text=f"Prompt preview đã lưu: {n}",
            foreground="#1f4e79" if n > 0 else "gray",
        )
        if n > 0:
            self._btn_open_saved_preview.state(["!disabled"])
        else:
            self._btn_open_saved_preview.state(["disabled"])

    def _current_preview_signature(self) -> str:
        """Gồm cả output settings để đổi tỉ lệ/thời lượng/… không âm thầm dùng preview cũ."""
        return "|".join(
            [
                self._current_mode_key(),
                self._prompt_text(),
                self._var_count.get().strip(),
                self._var_topic.get().strip(),
                self._var_goal.get().strip(),
                self._var_language.get().strip(),
                self._var_visual_style.get().strip(),
                self._var_camera_style.get().strip(),
                self._var_lighting.get().strip(),
                self._var_motion_style.get().strip(),
                self._var_mood.get().strip(),
                self._var_character_mode.get().strip(),
                self._var_provider.get().strip().lower(),
                self._var_model.get().strip(),
                self._sanitize_aspect_for_current_mode(self._var_aspect.get()),
                self._var_duration.get().strip(),
                self._var_resolution.get().strip(),
                self._var_outputs.get().strip(),
            ]
        )

    def _on_poll(self) -> None:
        vid = self._selected_video_id()
        if not vid:
            messagebox.showwarning("AI Video", "Chọn một dòng trước.", parent=self._top)
            return
        try:
            self._svc.poll_video_generation(vid)
            self._refresh_rows()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", str(exc), parent=self._top)

    def _on_sync(self) -> None:
        self._svc.sync_pending_videos()
        self._refresh_rows()

    def _on_cancel(self) -> None:
        vid = self._selected_video_id()
        if not vid:
            messagebox.showwarning("AI Video", "Chọn một dòng trước.", parent=self._top)
            return
        self._svc.cancel_video(vid)
        self._refresh_rows()

    def _on_delete_selected(self) -> None:
        vid = self._selected_video_id()
        if not vid:
            messagebox.showwarning("AI Video", "Chọn một dòng trước khi xóa.", parent=self._top)
            return
        if not messagebox.askyesno("Xóa video", f"Xóa video đã chọn?\n{vid}", parent=self._top):
            return
        ok = self._svc.delete_video(vid)
        if not ok:
            messagebox.showwarning("AI Video", "Không tìm thấy video để xóa.", parent=self._top)
            return
        self._refresh_rows()

    def _on_delete_all(self) -> None:
        rows = self._svc.list_records()
        if not rows:
            messagebox.showinfo("AI Video", "Không có video nào để xóa.", parent=self._top)
            return
        if not messagebox.askyesno(
            "Xóa tất cả",
            f"Bạn có chắc muốn xóa toàn bộ {len(rows)} video trong bảng?\nHành động này không hoàn tác.",
            parent=self._top,
        ):
            return
        n = self._svc.delete_all_videos()
        self._refresh_rows()
        messagebox.showinfo("AI Video", f"Đã xóa {n} video.", parent=self._top)

    def _on_open_output(self) -> None:
        out = Path(self._effective_output_dir()).resolve()
        try:
            os.startfile(str(out))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", f"Không mở được thư mục:\n{exc}", parent=self._top)

    def _open_video_file(self, path: str) -> None:
        p = Path(str(path or "").strip())
        if not p.is_file():
            messagebox.showwarning("AI Video", f"Không tìm thấy file video:\n{p}", parent=self._top)
            return
        try:
            os.startfile(str(p))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", f"Không mở được video:\n{exc}", parent=self._top)

    def _on_play_selected_video(self) -> None:
        vid = self._selected_video_id()
        if not vid:
            messagebox.showwarning("AI Video", "Chọn một video trước.", parent=self._top)
            return
        rows = self._svc.list_records()
        row = next((x for x in rows if str(x.get("id", "")) == vid), None)
        if not row:
            messagebox.showerror("AI Video", "Không tìm thấy record.", parent=self._top)
            return
        outs = list(row.get("output_files") or [])
        if not outs:
            messagebox.showwarning("AI Video", "Video chưa có output file.", parent=self._top)
            return
        self._open_video_file(str(outs[0]))

    def _on_use_for_job(self) -> None:
        vid = self._selected_video_id()
        if not vid:
            messagebox.showwarning("AI Video", "Chọn một video trước.", parent=self._top)
            return
        rows = self._svc.list_records()
        row = next((x for x in rows if str(x.get("id", "")) == vid), None)
        if not row:
            messagebox.showerror("AI Video", "Không tìm thấy record.", parent=self._top)
            return
        outs = list(row.get("output_files") or [])
        if not outs:
            messagebox.showwarning("AI Video", "Video chưa có output file.", parent=self._top)
            return
        messagebox.showinfo(
            "AI Video",
            "Luồng chuyển sang job đăng đang để manual theo yêu cầu.\n"
            f"Bạn có thể dùng file này để tạo job video:\n{outs[0]}",
            parent=self._top,
        )

    def _request_from_record(self, row: dict[str, Any]) -> dict[str, Any]:
        """Dựng lại request từ record cũ để tạo lại video lỗi."""
        req: dict[str, Any] = {
            "provider": str(row.get("provider") or "gemini").strip().lower(),
            "model": str(row.get("model") or "").strip(),
            "mode": str(row.get("mode") or "").strip(),
            "prompt": str(row.get("prompt") or "").strip(),
            "idea": str(row.get("idea") or "").strip(),
            "topic": str(row.get("topic") or "").strip(),
            "goal": str(row.get("goal") or "").strip(),
            "language": str(row.get("language") or "").strip(),
            "visual_style": str(row.get("visual_style") or "").strip(),
            "input_assets": dict(row.get("input_assets") or {}),
            "options": dict(row.get("options") or {}),
            "character_profile_id": str(row.get("character_profile_id") or "").strip(),
            "character_profile": dict(row.get("character_profile") or {}),
            "scene_plan": dict(row.get("scene_plan") or {}),
            "analysis": dict(row.get("analysis") or {}),
            "characters": list(row.get("characters") or []),
            "environments": list(row.get("environments") or []),
            "scenes": list(row.get("scenes") or []),
            "video_map": dict(row.get("video_map") or {}),
            "final_prompt": str(row.get("final_prompt") or "").strip(),
        }
        return req

    def _choose_regenerate_mode(self) -> str:
        """
        Chọn chế độ regenerate:
        - keep_all
        - scene_only
        - character_only
        - prompt_only
        """
        val = simpledialog.askstring(
            "Tạo lại video",
            "Chọn chế độ regenerate:\n"
            "1 = Giữ nguyên mọi thứ\n"
            "2 = Regenerate scene\n"
            "3 = Regenerate character\n"
            "4 = Regenerate prompt only\n\n"
            "Nhập 1/2/3/4 (Enter mặc định = 1):",
            parent=self._top,
        )
        v = str(val or "").strip().lower()
        if v in {"", "1", "keep", "keep_all"}:
            return "keep_all"
        if v in {"2", "scene", "scene_only"}:
            return "scene_only"
        if v in {"3", "character", "character_only"}:
            return "character_only"
        if v in {"4", "prompt", "prompt_only"}:
            return "prompt_only"
        raise ValueError("Giá trị chế độ không hợp lệ (chỉ nhận 1/2/3/4).")

    def _builder_input_from_record(self, row: dict[str, Any]) -> dict[str, Any]:
        opts = dict(row.get("options") or {})
        language = str(row.get("language") or "").strip() or "Vietnamese"
        return {
            "idea": str(row.get("idea") or "").strip(),
            "topic": str(row.get("topic") or "").strip(),
            "goal": str(row.get("goal") or "").strip() or "viral",
            "language_provider_label": language,
            "visual_style": str(row.get("visual_style") or "").strip() or "cinematic",
            "aspect_ratio": str(opts.get("aspect_ratio") or "9:16").strip(),
            "duration_sec": int(opts.get("duration_sec") or 8),
            "camera_style": str(row.get("video_map", {}).get("style_settings", {}).get("camera_style", "") or "").strip(),
            "lighting": str(row.get("video_map", {}).get("style_settings", {}).get("lighting", "") or "").strip(),
            "mood": str(row.get("video_map", {}).get("style_settings", {}).get("mood", "") or "").strip(),
        }

    def _rebuild_request_for_mode(self, *, row: dict[str, Any], mode: str) -> dict[str, Any]:
        req = self._request_from_record(row)
        if mode == "keep_all":
            req["prompt"] = str(row.get("final_prompt") or row.get("prompt") or "").strip()
            req["final_prompt"] = req["prompt"]
            return req

        analysis = dict(row.get("analysis") or {})
        characters = list(row.get("characters") or [])
        environments = list(row.get("environments") or [])
        scenes = list(row.get("scenes") or [])
        video_map = dict(row.get("video_map") or {})
        input_data = self._builder_input_from_record(row)

        if mode == "character_only":
            if not analysis:
                raise ValueError("Không có analysis để regenerate character.")
            characters = self._t2v_builder.build_character_bible_with_gemini(analysis=analysis, input_data=input_data)
            if not characters:
                raise ValueError("Gemini không trả character bible mới.")
            if not environments:
                environments = self._t2v_builder.build_environment_bible_with_gemini(analysis=analysis, input_data=input_data)
            scenes = self._t2v_builder.build_scene_breakdown_with_gemini(
                analysis=analysis,
                characters=characters,
                environments=environments,
                input_data=input_data,
            )
        elif mode == "scene_only":
            if not analysis:
                raise ValueError("Không có analysis để regenerate scene.")
            if not characters:
                raise ValueError("Không có character bible để regenerate scene.")
            if not environments:
                environments = self._t2v_builder.build_environment_bible_with_gemini(analysis=analysis, input_data=input_data)
            scenes = self._t2v_builder.build_scene_breakdown_with_gemini(
                analysis=analysis,
                characters=characters,
                environments=environments,
                input_data=input_data,
            )
        elif mode == "prompt_only":
            if not video_map:
                if not analysis or not characters or not environments or not scenes:
                    raise ValueError("Thiếu metadata để regenerate prompt-only.")
                video_map = self._t2v_builder.build_video_prompt_map(
                    analysis=analysis,
                    characters=characters,
                    environments=environments,
                    scenes=scenes,
                    input_data=input_data,
                )
        else:
            raise ValueError(f"Chế độ regenerate không hỗ trợ: {mode}")

        if mode in {"character_only", "scene_only"}:
            video_map = self._t2v_builder.build_video_prompt_map(
                analysis=analysis,
                characters=characters,
                environments=environments,
                scenes=scenes,
                input_data=input_data,
            )
        final_prompt = self._t2v_builder.build_final_veo_prompt_from_video_map(video_map)
        if not final_prompt.strip():
            raise ValueError("Không build được final prompt mới.")
        req["analysis"] = analysis
        req["characters"] = characters
        req["environments"] = environments
        req["scenes"] = scenes
        req["video_map"] = video_map
        req["prompt"] = final_prompt
        req["final_prompt"] = final_prompt
        return req

    def _retry_video_record(self, video_id: str) -> None:
        vid = str(video_id or "").strip()
        if not vid:
            return
        rows = self._svc.list_records()
        row = next((x for x in rows if str(x.get("id", "")).strip() == vid), None)
        if not row:
            messagebox.showerror("AI Video", f"Không tìm thấy video: {vid}", parent=self._top)
            return
        st = str(row.get("status", "")).strip().lower()
        if st not in {"failed", "cancelled"}:
            messagebox.showinfo("AI Video", "Chỉ có thể tạo lại video lỗi hoặc đã hủy.", parent=self._top)
            return
        try:
            mode = self._choose_regenerate_mode()
            req = self._rebuild_request_for_mode(row=row, mode=mode)
            rec = self._svc.create_video_record(req)
            self._svc.start_background_worker(rec["id"])
            self._refresh_rows()
            label = {
                "keep_all": "Giữ nguyên mọi thứ",
                "scene_only": "Regenerate scene",
                "character_only": "Regenerate character",
                "prompt_only": "Regenerate prompt-only",
            }.get(mode, mode)
            messagebox.showinfo(
                "AI Video",
                f"Đã tạo lại video ({label}) từ bản ghi lỗi.\nMã mới: {rec['id']}",
                parent=self._top,
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", f"Tạo lại video thất bại:\n{exc}", parent=self._top)

    def _output_summary_text(self, outs: list[str]) -> str:
        if not outs:
            return ""
        if len(outs) == 1:
            return str(outs[0])
        return f"{Path(str(outs[0])).name} (+{len(outs)-1} file)"

    def _expand_rows_for_grid(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Bung record thành item grid theo từng output file để hiển thị rõ ràng hơn.
        """
        expanded: list[dict[str, Any]] = []
        for r in rows:
            outs = [str(x).strip() for x in list(r.get("output_files") or []) if str(x).strip()]
            if not outs:
                expanded.append(
                    {
                        "row": r,
                        "video_id": str(r.get("id", "")).strip(),
                        "output_path": "",
                        "output_index": 0,
                        "output_total": 0,
                    }
                )
                continue
            total = len(outs)
            for idx, out in enumerate(outs, start=1):
                expanded.append(
                    {
                        "row": r,
                        "video_id": str(r.get("id", "")).strip(),
                        "output_path": out,
                        "output_index": idx,
                        "output_total": total,
                    }
                )
        return expanded

    def _new_video_project(self) -> None:
        """Lưu dự án hiện tại rồi tạo project_id mới; preview cũ được xóa (dự án mới sạch)."""
        try:
            self._save_current_project_to_disk()
        except Exception:
            pass
        self._current_project_id = uuid.uuid4().hex[:10]
        self._var_project_name.set(f"Dự án {datetime.now().strftime('%d/%m %H:%M')}")
        self._var_output_dir.set(self._default_output_dir_for_project(self._current_project_id))
        self._sync_output_controls_state()
        self._effective_output_dir()
        self._filter_project_id = str(self._current_project_id or "*")
        self._prepared_requests = []
        self._prepared_signature = ""
        clear_prepared_prompt_preview()
        self._refresh_prepared_preview_badge()
        self._last_status_view_fingerprint = None
        self._rebuild_project_filter_combo(self._svc.list_records())
        self._refresh_rows()

    def _on_project_filter_changed(self) -> None:
        vals = list(self._cb_project_filter.cget("values") or ())
        sel = self._var_project_list_filter.get().strip()
        try:
            idx = vals.index(sel)
            self._filter_project_id = self._project_filter_id_list[idx]
        except Exception:
            self._filter_project_id = "*"
        self._last_status_view_fingerprint = None
        self._refresh_rows()

    def _rebuild_project_filter_combo(self, all_rows: list[dict[str, Any]]) -> None:
        if not hasattr(self, "_cb_project_filter"):
            return
        seen: dict[str, str] = {}
        for r in all_rows:
            pid = str(r.get("project_id") or "").strip()
            if not pid:
                continue
            pname = str(r.get("project_name") or "").strip() or "Dự án"
            seen.setdefault(pid, pname)
        cur_pid = str(getattr(self, "_current_project_id", "") or "").strip()
        if cur_pid and cur_pid not in seen:
            seen[cur_pid] = self._var_project_name.get().strip() or "Dự án"
        labels = ["Tất cả dự án"]
        ids = ["*"]
        for pid in sorted(seen.keys()):
            labels.append(f"{seen[pid]} · {pid}")
            ids.append(pid)
        self._project_filter_id_list = ids
        prev = str(getattr(self, "_filter_project_id", "*") or "*")
        self._cb_project_filter.configure(values=labels)
        if prev in ids:
            self._var_project_list_filter.set(labels[ids.index(prev)])
        else:
            self._filter_project_id = "*"
            self._var_project_list_filter.set(labels[0])

    def _attach_project_meta_to_requests(self, requests: list[dict[str, Any]]) -> None:
        pid = str(self._current_project_id or "").strip()
        pname = self._var_project_name.get().strip() or "Dự án"
        for r in requests:
            r["project_id"] = pid
            r["project_name"] = pname

    def _status_view_fingerprint(self, rows: list[dict[str, Any]]) -> str:
        """Chuỗi ổn định để tránh rebuild grid/list khi dữ liệu UI không đổi (giảm nhấp nháy)."""
        parts: list[str] = [
            str(self._status_filter.get()),
            str(self._status_view.get()),
            str(getattr(self, "_filter_project_id", "*") or "*"),
        ]
        for r in rows:
            oid = str(r.get("id", ""))
            st = str(r.get("status", ""))
            op = str(r.get("operation_id", ""))[:120]
            err = str(r.get("error_message", "") or "")[:120]
            outs = [Path(str(x)).name for x in (r.get("output_files") or []) if str(x).strip()]
            parts.append(f"{oid}|{st}|{op}|{err}|{len(outs)}|{'+'.join(outs)}")
        return hashlib.sha1("\x1e".join(parts).encode("utf-8")).hexdigest()

    def _short_line_for_grid(self, text: str, *, max_len: int = 72) -> str:
        s = " ".join(str(text or "").split())
        if len(s) <= max_len:
            return s
        return s[: max_len - 1].rstrip() + "…"

    def _grid_card_prompt_label(
        self,
        row: dict[str, Any],
        *,
        output_path: str,
        output_index: int,
        output_total: int,
    ) -> str:
        """
        Nhãn prompt trên thẻ grid: nếu job có prompt_queue + file *_pNN_*, map đúng prompt theo lô.
        """
        opts = dict(row.get("options") or {})
        pq = opts.get("prompt_queue")
        raw_path = str(output_path or "").strip()
        fname = Path(raw_path).name if raw_path else ""
        m = re.search(r"_p(\d+)_", fname, flags=re.IGNORECASE)
        if isinstance(pq, list) and pq and m:
            try:
                pi = int(m.group(1)) - 1
            except ValueError:
                pi = -1
            if 0 <= pi < len(pq):
                line = self._short_line_for_grid(str(pq[pi] or ""))
                if line:
                    return f"Prompt #{pi + 1}/{len(pq)}: {line}"
        base = self._display_prompt_in_table(row)
        if output_total > 1:
            return f"Biến thể {output_index}/{output_total}: {base}".strip()
        return base

    def _refresh_rows(self) -> None:
        selected_id = self._selected_video_id()
        all_rows = self._svc.list_records()
        rows = self._rows_with_filter(all_rows)
        fp = self._status_view_fingerprint(rows)
        if fp == getattr(self, "_last_status_view_fingerprint", None) and self._last_status_view_fingerprint is not None:
            self._refresh_status_header(all_rows=all_rows)
            self._rebuild_project_filter_combo(all_rows)
            self._refresh_progress_panel()
            return
        self._last_status_view_fingerprint = fp

        for iid in self._tree.get_children():
            self._tree.delete(iid)
        selected_iid = ""
        for r in rows:
            opts = dict(r.get("options") or {})
            outs = list(r.get("output_files") or [])
            mode_key = str(r.get("mode", "")).strip()
            iid = self._tree.insert(
                "",
                tk.END,
                values=(
                    r.get("id", ""),
                    self._mode_key_to_display.get(mode_key, mode_key),
                    self._display_prompt_in_table(r),
                    r.get("model", ""),
                    opts.get("aspect_ratio", ""),
                    opts.get("duration_sec", ""),
                    r.get("status", ""),
                    r.get("operation_id", ""),
                    self._output_summary_text(outs),
                    str(r.get("error_message", ""))[:100],
                    r.get("created_at", ""),
                ),
            )
            if selected_id and str(r.get("id", "")) == selected_id:
                selected_iid = iid
        if selected_iid:
            self._tree.selection_set(selected_iid)
            self._tree.focus(selected_iid)
        self._refresh_status_header(all_rows=all_rows)
        self._rebuild_project_filter_combo(all_rows)
        self._refresh_grid_cards(rows)
        self._refresh_progress_panel()

    def _rows_with_filter(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        flt = str(self._status_filter.get()).strip().lower()
        if flt == "completed":
            rows = [r for r in rows if str(r.get("status", "")).strip().lower() == "completed"]
        elif flt == "failed":
            rows = [r for r in rows if str(r.get("status", "")).strip().lower() in {"failed", "cancelled"}]
        fp = str(getattr(self, "_filter_project_id", "*") or "*").strip()
        if fp and fp != "*":
            rows = [r for r in rows if str(r.get("project_id", "") or "").strip() == fp]
        return rows

    def _refresh_status_header(self, *, all_rows: list[dict[str, Any]]) -> None:
        """Đếm job theo metadata + ô grid (bung output) để khớp thẻ hiển thị."""
        expanded_all = self._expand_rows_for_grid(all_rows)
        total_jobs = len(all_rows)
        total_cells = len(expanded_all)
        done = sum(1 for r in all_rows if str(r.get("status", "")).strip().lower() == "completed")
        fail = sum(1 for r in all_rows if str(r.get("status", "")).strip().lower() in {"failed", "cancelled"})
        self._btn_filter_all.configure(text=f"Tất cả ({total_jobs} job · {total_cells} ô grid)")
        self._btn_filter_completed.configure(text=f"Hoàn thành ({done})")
        self._btn_filter_failed.configure(text=f"Thất bại ({fail})")

    def _refresh_grid_cards(self, rows: list[dict[str, Any]]) -> None:
        if not hasattr(self, "_status_grid_content"):
            return
        for w in self._status_grid_content.winfo_children():
            w.destroy()
        cols = 4
        for c in range(cols):
            self._status_grid_content.columnconfigure(c, weight=1)
        items = self._expand_rows_for_grid(rows)
        for idx, it in enumerate(items):
            rr, cc = divmod(idx, cols)
            card = ttk.Frame(self._status_grid_content, padding=8, relief=tk.GROOVE, borderwidth=1)
            card.grid(row=rr, column=cc, sticky="nsew", padx=4, pady=4)
            r = dict(it.get("row") or {})
            vid = str(it.get("video_id") or "").strip()
            status = str(r.get("status", "")).strip().lower()
            out_path = str(it.get("output_path") or "").strip()
            out_idx = int(it.get("output_index") or 0)
            out_total = int(it.get("output_total") or 0)
            prompt = self._grid_card_prompt_label(
                r, output_path=out_path, output_index=out_idx, output_total=out_total
            )
            head = f"#{idx + 1}  {vid[:12] if vid else 'video'}"
            if out_total > 0:
                head += f"  [{out_idx}/{out_total}]"
            pn = str(r.get("project_name") or "").strip()
            if pn:
                head += f"  ·  {pn[:16]}"
            ttk.Label(card, text=head, font=("Segoe UI", 9, "bold")).grid(
                row=0, column=0, sticky="w"
            )
            ttk.Label(card, text=(prompt or "(không có prompt)")[:88], wraplength=220, justify=tk.LEFT).grid(
                row=1, column=0, sticky="w", pady=(4, 2)
            )
            s_text = status.upper() if status else "UNKNOWN"
            s_color = "#198754" if status == "completed" else ("#dc3545" if status in {"failed", "cancelled"} else "#1f4e79")
            ttk.Label(card, text=s_text, foreground=s_color).grid(row=2, column=0, sticky="w")
            if out_path:
                ttk.Label(card, text=Path(out_path).name, foreground="gray").grid(row=3, column=0, sticky="w", pady=(2, 0))
            elif out_total > 0:
                ttk.Label(card, text=f"{out_total} output file", foreground="gray").grid(row=3, column=0, sticky="w", pady=(2, 0))
            acts = ttk.Frame(card)
            acts.grid(row=4, column=0, sticky="w", pady=(6, 0))
            if out_path:
                ttk.Button(acts, text="▶ Mở", width=10, command=lambda p=out_path: self._open_video_file(p)).pack(
                    side=tk.LEFT, padx=(0, 4)
                )
            if status in {"failed", "cancelled"}:
                ttk.Button(acts, text="Tạo lại", width=10, command=lambda v=vid: self._retry_video_record(v)).pack(
                    side=tk.LEFT
                )
            if out_path:
                card.bind("<Button-1>", lambda _e, v=vid, p=out_path: self._on_grid_open_video(video_id=v, output_path=p))
                for ch in card.winfo_children():
                    ch.bind(
                        "<Button-1>",
                        lambda _e, v=vid, p=out_path: self._on_grid_open_video(video_id=v, output_path=p),
                    )
            else:
                card.bind("<Button-1>", lambda _e, v=vid: self._on_grid_select(v))
                for ch in card.winfo_children():
                    ch.bind("<Button-1>", lambda _e, v=vid: self._on_grid_select(v))

    def _on_grid_select(self, video_id: str) -> None:
        self._selected_grid_video_id = str(video_id or "").strip()
        self._refresh_progress_panel()

    def _on_grid_open_video(self, *, video_id: str, output_path: str) -> None:
        """
        Click card video để mở ngay file output.
        """
        self._selected_grid_video_id = str(video_id or "").strip()
        self._refresh_progress_panel()
        path = str(output_path or "").strip()
        if path:
            self._open_video_file(path)

    def _display_prompt_in_table(self, row: dict[str, Any]) -> str:
        idea = str(row.get("idea", "")).strip()
        if idea:
            return idea[:60]
        prompt = str(row.get("prompt", "")).strip()
        if not prompt:
            return ""
        marker = "Character Continuity Anchor:"
        if marker in prompt:
            # Bỏ phần anchor kỹ thuật khỏi cột table để tránh rối hiển thị.
            prompt = prompt.split(marker, 1)[-1].strip()
            if "\n\n" in prompt:
                prompt = prompt.split("\n\n", 1)[-1].strip()
        marker2 = "Browser Character Bible (strict continuity):"
        if marker2 in prompt:
            prompt = prompt.split(marker2, 1)[-1].strip()
            if "\n\n" in prompt:
                prompt = prompt.split("\n\n", 1)[-1].strip()
        return prompt[:60]

    def _refresh_progress_panel(self) -> None:
        vid = self._selected_video_id()
        if not vid:
            self._var_step_main.set("Chưa chọn video")
            self._set_progress_detail("Hãy chọn 1 dòng ở bảng bên dưới để xem tiến trình chi tiết.")
            return
        rows = self._svc.list_records()
        row = next((x for x in rows if str(x.get("id", "")) == vid), None)
        if not row:
            self._var_step_main.set("Không tìm thấy bản ghi")
            self._set_progress_detail("Video có thể đã bị xóa hoặc chưa kịp đồng bộ.")
            return
        status = str(row.get("status", "")).strip().lower()
        step_main, detail = self._status_to_step(status=status, row=row)
        self._var_step_main.set(step_main)
        self._set_progress_detail(detail)

    def _status_to_step(self, *, status: str, row: dict[str, Any]) -> tuple[str, str]:
        op = str(row.get("operation_id", "")).strip()
        err = str(row.get("error_message", "")).strip()
        outs = list(row.get("output_files") or [])
        if status in {"queued", "draft"}:
            return ("Bước 0/3 — Đang chờ", "Video đang ở hàng đợi, chưa bắt đầu gửi yêu cầu.")
        if status == "submitting":
            return ("Bước 1/3 — Đang gửi yêu cầu", "Đang submit request tạo video lên provider.")
        if status in {"generating", "polling"}:
            suffix = f" (operation: {op})" if op else ""
            return ("Bước 2/3 — Đang tạo video", f"Provider đang xử lý/generate video{suffix}.")
        if status == "downloading":
            return ("Bước 3/3 — Đang tải kết quả", "Video đã tạo xong, app đang tải file output về máy.")
        if status == "completed":
            fp = str(outs[0]) if outs else "đã hoàn tất"
            return ("Hoàn tất", f"Đã tạo xong video. File đầu tiên: {fp}")
        if status == "failed":
            return ("Thất bại", err or "Quá trình tạo video bị lỗi, vui lòng kiểm tra log/lỗi chi tiết.")
        if status == "cancelled":
            return ("Đã hủy", "Video đã bị hủy theo thao tác người dùng.")
        return ("Trạng thái không xác định", f"Status hiện tại: {status or '(trống)'}")

    def _start_auto_refresh(self) -> None:
        def tick() -> None:
            if not self._top.winfo_exists():
                return
            try:
                self._refresh_rows()
            except Exception:
                pass
            self._top.after(3500, tick)

        self._top.after(3500, tick)

    def _set_progress_detail(self, text: str) -> None:
        self._progress_full_detail = str(text or "").strip()
        short = self._progress_full_detail
        if not self._progress_expanded and len(short) > 420:
            short = short[:420].rstrip() + " ... (bấm Hiện thêm)"
        self._var_step_detail.set(short)
        try:
            self._txt_progress_detail.configure(state="normal")
            self._txt_progress_detail.delete("1.0", tk.END)
            self._txt_progress_detail.insert("1.0", short or "(Không có nội dung)")
            self._txt_progress_detail.configure(state="disabled")
            self._txt_progress_detail.yview_moveto(0.0)
        except Exception:
            pass
        self._btn_progress_toggle.configure(text="Ẩn bớt" if self._progress_expanded else "Hiện thêm")

    def _toggle_progress_detail(self) -> None:
        self._progress_expanded = not self._progress_expanded
        self._set_progress_detail(self._progress_full_detail)

    def _open_progress_detail_popup(self) -> None:
        top = tk.Toplevel(self._top)
        top.title("Chi tiết tiến trình video")
        top.geometry("900x520")
        top.transient(self._top)
        frm = ttk.Frame(top, padding=8)
        frm.pack(fill=tk.BOTH, expand=True)
        frm.columnconfigure(0, weight=1)
        frm.rowconfigure(0, weight=1)
        txt = tk.Text(frm, wrap="word", font=("Consolas", 9))
        txt.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(frm, orient=tk.VERTICAL, command=txt.yview)
        sy.grid(row=0, column=1, sticky="ns")
        sx = ttk.Scrollbar(frm, orient=tk.HORIZONTAL, command=txt.xview)
        sx.grid(row=1, column=0, sticky="ew")
        txt.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)
        txt.insert("1.0", self._progress_full_detail or "(Không có nội dung)")
        txt.configure(state="disabled")

    def _build_mode_maps(self) -> tuple[dict[str, str], dict[str, str]]:
        label_vi: dict[str, str] = {
            "text_to_video": "Text -> Video (Từ prompt văn bản)",
            "image_to_video": "Image -> Video (Ảnh thành video)",
            "first_last_frame_to_video": "First+Last Frame (Nội suy 2 ảnh đầu/cuối)",
            "ingredients_to_video": "Ingredients (Trộn nhiều ảnh tham chiếu)",
            "extend_video": "Extend Video (Kéo dài video nguồn)",
            "prompt_to_vertical_video": "Prompt -> Video dọc 9:16",
            "image_to_vertical_video": "Image -> Video dọc 9:16",
        }
        m: dict[str, str] = {}
        rm: dict[str, str] = {}
        for key in (
            "text_to_video",
            "image_to_video",
            "first_last_frame_to_video",
            "ingredients_to_video",
            "extend_video",
            "prompt_to_vertical_video",
            "image_to_vertical_video",
        ):
            display = label_vi.get(key, key)
            m[display] = key
            rm[key] = display
        return m, rm

    def _build_model_choices(self) -> list[str]:
        pv = dict(self._cfg.get("providers", {}).get("gemini", {}))
        flow_cached = self._flow_cached_model_choices()
        candidates = [
            self._to_flow_model_label(os.environ.get("VEO_MODEL", "").strip()),
            self._to_flow_model_label(os.environ.get("GEMINI_VIDEO_MODEL", "").strip()),
            self._to_flow_model_label(str(pv.get("default_model", "")).strip()),
            self._to_flow_model_label(str(pv.get("fast_model", "")).strip()),
            "Veo 3.1 - Quality",
            "Veo 3.1 - Fast",
            "Veo 3.1 - Lite",
            "Veo 3.1 - Lite [Lower Priority]",
            "Veo 3.1 - Fast [Lower Priority]",
        ]
        candidates = [*flow_cached, *candidates]
        out: list[str] = []
        for x in candidates:
            if x and x not in out:
                out.append(x)
        return out

    def _to_flow_model_label(self, raw_model: str) -> str:
        """
        Chuẩn hóa model slug/API về label hiển thị giống Flow UI.
        """
        m = str(raw_model or "").strip()
        if not m:
            return ""
        ml = m.lower()
        if "veo" in ml:
            if "fast" in ml:
                return "Veo 3.1 - Fast"
            if "lite" in ml:
                return "Veo 3.1 - Lite"
            if "quality" in ml or "generate-preview" in ml:
                return "Veo 3.1 - Quality"
        return m

    def _flow_cached_model_choices(self) -> list[str]:
        """
        Đọc danh sách model Flow đã discover từ browser runtime để hiển thị trong combobox.
        """
        p = Path("data") / "google_flow_video" / "temp" / "flow_model_choices.json"
        if not p.is_file():
            return []
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return []
        rows = list((raw or {}).get("models") or [])
        out: list[str] = []
        for x in rows:
            s = str(x).strip()
            if s and s not in out:
                out.append(s)
        return out

    def _reload_model_choices(self) -> None:
        self._cfg = load_ai_video_config()
        self._model_choices = self._build_model_choices()
        self._cb_model.configure(values=self._model_choices)
        if not self._var_model.get().strip() and self._model_choices:
            self._var_model.set(self._model_choices[0])

    def _resolve_flow_profile_dir(self) -> Path:
        p = os.environ.get("VEO3_BROWSER_PROFILE_DIR", "").strip() or os.environ.get("NANOBANANA_BROWSER_PROFILE_DIR", "").strip()
        d = Path(p) if p else (Path("data") / "nanobanana" / "browser_profile")
        d.mkdir(parents=True, exist_ok=True)
        return d.resolve()

    def _sync_models_from_flow_now(self) -> None:
        if self._sync_models_running:
            messagebox.showinfo("AI Video", "Đang đồng bộ model từ Flow, vui lòng chờ.", parent=self._top)
            return
        self._sync_models_running = True
        self._btn_sync_models_now.state(["disabled"])
        self._btn_sync_models_now.configure(text="Đang đồng bộ...")
        self._top.configure(cursor="watch")

        def worker() -> None:
            err = ""
            models: list[str] = []
            try:
                profile_dir = self._resolve_flow_profile_dir()
                web_url = (
                    os.environ.get("VEO3_WEB_URL", "").strip()
                    or os.environ.get("NANOBANANA_WEB_URL", "").strip()
                    or GOOGLE_FLOW_URL
                )
                models = sync_flow_model_choices_from_profile(profile_dir=str(profile_dir), web_url=web_url)
            except Exception as exc:  # noqa: BLE001
                err = str(exc)
            self._top.after(0, lambda: self._on_sync_models_from_flow_done(models=models, error=err))

        threading.Thread(target=worker, daemon=True, name="ai_video_sync_flow_models").start()

    def _on_sync_models_from_flow_done(self, *, models: list[str], error: str) -> None:
        self._sync_models_running = False
        self._btn_sync_models_now.state(["!disabled"])
        self._btn_sync_models_now.configure(text="Đồng bộ model từ Flow")
        self._top.configure(cursor="")
        if error:
            messagebox.showerror(
                "AI Video",
                f"Không đồng bộ được model từ Flow:\n{error}\n\nHãy kiểm tra profile đã đăng nhập Google Flow.",
                parent=self._top,
            )
            return
        self._reload_model_choices()
        if models:
            if self._var_model.get().strip() not in self._model_choices:
                self._var_model.set(models[0])
            messagebox.showinfo("AI Video", f"Đã đồng bộ {len(models)} model từ Flow.", parent=self._top)
            return
        messagebox.showwarning("AI Video", "Không đọc được model nào từ Flow menu.", parent=self._top)

    def _current_mode_key(self) -> str:
        display = self._var_mode_display.get().strip()
        return self._mode_display_to_key.get(display, "text_to_video")

    def _mode_help_text(self, mode_key: str) -> str:
        tips: dict[str, str] = {
            "text_to_video": "Chỉ cần prompt văn bản. Phù hợp tạo video mới hoàn toàn.",
            "image_to_video": "Cần 1 ảnh đầu vào + prompt để tạo chuyển động từ ảnh.",
            "first_last_frame_to_video": "Cần 2 ảnh (đầu/cuối) + prompt để nội suy chuyển cảnh.",
            "ingredients_to_video": "Cần nhiều ảnh tham chiếu + prompt. Dùng khi muốn trộn style/chủ thể.",
            "extend_video": "Cần video nguồn + prompt để kéo dài video hiện có.",
            "prompt_to_vertical_video": "Giống text_to_video nhưng khóa tỉ lệ dọc 9:16 (phù hợp Reels/TikTok).",
            "image_to_vertical_video": "Giống image_to_video nhưng khóa tỉ lệ dọc 9:16.",
        }
        return f"Giải thích mode: {tips.get(mode_key, 'Mode không có mô tả.')}"

    def _compose_prompt_with_style(self, prompt: str) -> str:
        prompt = (prompt or "").strip()
        style = self._selected_style_prompt()
        lang = self._var_language.get().strip()
        lang_hint = self._language_hint_text(lang)

        if not style:
            return f"{prompt}\n\n{lang_hint}".strip()
        if not prompt:
            return f"Style: {style}\n\n{lang_hint}".strip()
        return f"{prompt}\n\nStyle guidance: {style}\n\n{lang_hint}".strip()

    def _selected_style_prompt(self) -> str:
        name = self._var_style.get().strip()
        if not name or name == self._STYLE_NONE:
            return ""
        if name == self._STYLE_AUTO:
            return self._infer_style_from_prompt(self._prompt_text())
        for row in self._styles:
            if row["name"] == name:
                return row["prompt"]
        return ""

    def _load_styles(self) -> None:
        self._styles = load_video_styles()
        names = [self._STYLE_NONE, self._STYLE_AUTO] + [x["name"] for x in self._styles]
        self._cb_style.configure(values=names)
        if self._var_style.get().strip() not in names:
            self._var_style.set(names[0])
        self._on_style_selected()

    def _on_style_selected(self) -> None:
        name = self._var_style.get().strip()
        if not name or name == self._STYLE_NONE:
            self._var_style_prompt.set("")
            return
        if name == self._STYLE_AUTO:
            self._var_style_prompt.set(self._infer_style_from_prompt(self._prompt_text()))
            return
        row = next((x for x in self._styles if x["name"] == name), None)
        if not row:
            return
        self._var_style_prompt.set(row["prompt"])

    def _refresh_auto_style_preview(self) -> None:
        if self._var_style.get().strip() == self._STYLE_AUTO:
            self._var_style_prompt.set(self._infer_style_from_prompt(self._prompt_text()))

    def _on_prompt_text_changed(self) -> None:
        self._refresh_auto_style_preview()
        self._refresh_prompt_stats()

    def _prompt_text(self) -> str:
        try:
            return self._txt_prompt.get("1.0", tk.END).strip()
        except Exception:
            return ""

    def _prompt_lines(self) -> list[str]:
        raw = self._prompt_text()
        lines = [x.strip() for x in raw.splitlines() if x.strip()]
        return lines

    def _refresh_prompt_stats(self) -> None:
        n = len(self._prompt_lines())
        mode = self._current_mode_key()
        count = max(1, int(self._var_count.get().strip() or "1"))
        outputs = self._outputs_count()
        if mode in {"text_to_video", "prompt_to_vertical_video"}:
            total_prompts = n * count
            total = total_prompts * outputs
            self._var_prompt_stats.set(
                f"Ý tưởng/dòng: {n} | Số tập mỗi ý tưởng: {count} | Prompt tạo ra: {total_prompts} | Outputs/prompt: x{outputs} | Video sẽ tạo: {total}"
            )
        else:
            self._var_prompt_stats.set(f"Prompt: {n} | Outputs/prompt: x{outputs} | Video sẽ tạo: {n * outputs}")

    def _on_character_mode_changed(self) -> None:
        mode = self._normalize_character_mode(self._var_character_mode.get().strip())
        if mode == "manual":
            self._char_manual_fr.grid()
            self._char_auto_fr.grid_remove()
        else:
            self._char_manual_fr.grid_remove()
            self._char_auto_fr.grid()

    def _on_generate_auto_characters(self) -> None:
        idea = (self._prompt_text() or "").strip()
        topic = self._var_topic.get().strip()
        if not idea and not topic:
            messagebox.showwarning(
                "Nhân vật",
                "Nhập kịch bản/ý tưởng (hoặc chủ đề) trước khi tạo nhân vật.",
                parent=self._top,
            )
            return
        style_hint = str(self._selected_style_prompt() or "").strip()
        style_merge = style_hint[:220] if style_hint else ""

        chars, gem_err = self._t2v_builder.infer_characters_from_script_via_gemini(
            script=idea,
            topic=topic,
            language_display=self._var_language.get().strip() or "Tiếng Việt",
            style_hint=style_hint[:600] if style_hint else "",
        )
        if chars:
            if style_merge:
                for c in chars:
                    ap = str(c.get("appearance", "")).strip()
                    c["appearance"] = f"{ap}; style reference: {style_merge}".strip("; ")
            self._auto_character_profiles = migrate_auto_character_profiles(chars)
            self._refresh_auto_character_summary()
            return

        if self._t2v_builder.gemini_api_key_configured():
            messagebox.showerror(
                "Nhân vật (Gemini)",
                gem_err or "Không nhận được danh sách nhân vật hợp lệ từ Gemini.",
                parent=self._top,
            )
            return

        text = f"{idea} {topic}".strip().lower()
        chars = self._infer_characters_from_story(text, language=self._var_language.get().strip())
        if style_merge:
            for c in chars:
                ap = str(c.get("appearance", "")).strip()
                c["appearance"] = f"{ap}; style reference: {style_merge}".strip("; ")
        self._auto_character_profiles = migrate_auto_character_profiles(chars)
        self._refresh_auto_character_summary()
        messagebox.showinfo(
            "Nhân vật",
            f"{gem_err}\nĐã dùng phân tích nhanh trên máy (fallback). Cấu hình Gemini API key trong tab AI để phân tích theo kịch bản.",
            parent=self._top,
        )

    def _infer_characters_from_story(self, text: str, *, language: str) -> list[dict[str, str]]:
        """
        Heuristic sinh số lượng nhân vật theo kịch bản (ưu tiên đúng số lượng cast).
        Hỗ trợ các cụm phổ biến: ông bà, bố/mẹ, N người con / N con, chú cún/mèo...
        Không cần chữ 'gia đình' nếu đã có nông trại + liệt kê nhân vật, hoặc có 'ông bà' / 'bố mẹ' / 'N người con'.
        Với 2 con: nếu câu chuyện gợi ý giới (một trai một gái, con trai + con gái, …)
        thì tạo sẵn tên Con trai / Con gái thay vì Con 1 / Con 2.
        """
        lang = str(language or "").strip().lower()
        is_vi = "việt" in lang or "vietnamese" in lang
        t = str(text or "").strip()
        low = t.lower()
        chars: list[dict[str, str]] = []
        add = chars.append
        if "mẹ chồng" in low and "nàng dâu" in low:
            if is_vi:
                add({"name": "Mẹ chồng", "role": "mother_in_law", "gender": "female", "age": "52", "appearance": "Vietnamese middle-aged woman, strict expression, neat hairstyle", "facial_features": "oval face, sharp eyes", "outfit": "traditional elegant home wear", "personality": "strong, conservative, emotionally layered", "consistency_note": "keep same face/outfit palette through all episodes"})
                add({"name": "Nàng dâu", "role": "daughter_in_law", "gender": "female", "age": "26", "appearance": "Vietnamese young woman, calm face, expressive eyes", "facial_features": "soft jawline, natural makeup", "outfit": "modern modest outfit", "personality": "patient, resilient, warm", "consistency_note": "keep same facial identity and outfit style"})
                add({"name": "Người chồng", "role": "husband", "gender": "male", "age": "30", "appearance": "Vietnamese young man, neutral expression", "facial_features": "short black hair, clean-shaven", "outfit": "casual clean shirt and trousers", "personality": "conflicted, caring, responsible", "consistency_note": "maintain hairstyle and costume style"})
            else:
                add({"name": "Margaret", "role": "mother_in_law", "gender": "female", "age": "52", "appearance": "middle-aged woman with elegant but strict demeanor", "facial_features": "defined cheekbones, steady gaze", "outfit": "classic cardigan and tailored trousers", "personality": "strong, conservative, emotionally layered", "consistency_note": "keep same face/hair/outfit style across episodes"})
                add({"name": "Emily", "role": "daughter_in_law", "gender": "female", "age": "26", "appearance": "young woman with warm expression and modern natural look", "facial_features": "soft facial line, clear skin texture", "outfit": "casual blouse and neutral-tone pants", "personality": "patient, resilient, warm", "consistency_note": "keep same identity and visual style each episode"})
                add({"name": "Daniel", "role": "husband", "gender": "male", "age": "30", "appearance": "young man with composed demeanor", "facial_features": "short hair, clean-shaven", "outfit": "smart-casual shirt and dark trousers", "personality": "conflicted, caring, responsible", "consistency_note": "keep facial traits and wardrobe consistent"})
        if not chars and _story_triggers_family_cast(low):
            # Parse cụm thành viên gia đình theo số lượng (ông bà, bố mẹ, N con, thú cưng).
            if "ông bà" in low or "ong ba" in low:
                add(
                    {
                        "name": "Ông" if is_vi else "Grandfather",
                        "role": "grandfather",
                        "gender": "male",
                        "age": "68",
                        "appearance": "elderly Vietnamese man, kind eyes, gentle farmer vibe",
                        "facial_features": "wrinkles, stable elder face identity",
                        "outfit": "simple rural shirt and hat optional",
                        "personality": "wise, warm, calm",
                        "consistency_note": "keep same grandfather identity",
                    }
                )
                add(
                    {
                        "name": "Bà" if is_vi else "Grandmother",
                        "role": "grandmother",
                        "gender": "female",
                        "age": "66",
                        "appearance": "elderly Vietnamese woman, gentle smile",
                        "facial_features": "soft elder features, stable identity",
                        "outfit": "simple rural blouse or áo bà ba style",
                        "personality": "caring, cheerful",
                        "consistency_note": "keep same grandmother identity",
                    }
                )
            if any(k in low for k in ("bố", "ba", "cha", "father", "dad")):
                add(
                    {
                        "name": "Bố" if is_vi else "Father",
                        "role": "father",
                        "gender": "male",
                        "age": "38",
                        "appearance": "adult male in family context",
                        "facial_features": "stable face identity",
                        "outfit": "simple farm-family outfit",
                        "personality": "responsible, caring",
                        "consistency_note": "keep same father identity",
                    }
                )
            if any(k in low for k in ("mẹ", "mother", "mom", "mum")):
                add(
                    {
                        "name": "Mẹ" if is_vi else "Mother",
                        "role": "mother",
                        "gender": "female",
                        "age": "36",
                        "appearance": "adult female in family context",
                        "facial_features": "stable face identity",
                        "outfit": "simple farm-family outfit",
                        "personality": "warm, patient",
                        "consistency_note": "keep same mother identity",
                    }
                )
            # Parse "2 người con", "2 con", "hai người con", "3 đứa trẻ", "two kids"
            child_count = 0
            m_people = re.search(r"\b(\d+)\s*người\s*con\b", low)
            if m_people:
                try:
                    child_count = max(0, min(5, int(m_people.group(1))))
                except Exception:
                    child_count = 0
            if child_count == 0:
                m_word_children = re.search(
                    r"\b(hai|ba|bốn|tư|năm)\s+người\s*con\b",
                    low,
                )
                if m_word_children:
                    child_count = {"hai": 2, "ba": 3, "bốn": 4, "tư": 4, "năm": 5}.get(m_word_children.group(1), 0)
            if child_count == 0:
                m = re.search(r"\b(\d+)\s*(con|đứa trẻ|tre|trẻ em)\b", low)
                if m:
                    try:
                        child_count = max(0, min(5, int(m.group(1))))
                    except Exception:
                        child_count = 0
            # Chỉ mặc định 1 con khi có từ "con/trẻ" theo nghĩa con cái, tránh nhầm chỗ khác.
            if child_count == 0 and any(
                k in low for k in ("đứa trẻ", "trẻ em", "kids", "children", "đứa con", "cậu bé", "cô bé", "bé con")
            ):
                child_count = 1
            elif child_count == 0 and "người con" in low and not re.search(r"\b\d+\s*người\s*con\b", low):
                # Có nhắc con nhưng không ghi số — mặc định 1.
                child_count = 1
            child_slots = _child_name_gender_slots(low, child_count, is_vi=is_vi)
            for i, (ch_name, ch_gender) in enumerate(child_slots):
                if ch_gender == "male":
                    appear = "young boy with consistent look, age-appropriate proportions"
                    pers = "playful boy, innocent energy"
                elif ch_gender == "female":
                    appear = "young girl with consistent look, age-appropriate proportions"
                    pers = "playful girl, innocent warmth"
                else:
                    appear = "young child with consistent look"
                    pers = "playful, innocent"
                add(
                    {
                        "name": ch_name,
                        "role": "child",
                        "gender": ch_gender,
                        "age": "8",
                        "appearance": appear,
                        "facial_features": "stable child face identity",
                        "outfit": "simple kid outfit",
                        "personality": pers,
                        "consistency_note": f"keep same {ch_name} identity (do not swap with other children)",
                    }
                )
            # Parse thú cưng.
            if any(k in low for k in ("chó", "cún", "dog", "puppy")):
                dog_pers = (
                    "mischievous, playful, cheeky energy"
                    if any(p in low for p in ("tinh nghịch", "tinh nghich", "nghịch ngợm", "nghich ngom", "naughty"))
                    else "friendly, energetic"
                )
                add(
                    {
                        "name": "Chú cún" if is_vi else "Dog",
                        "role": "pet_dog",
                        "gender": "unspecified",
                        "age": "4",
                        "appearance": "family dog with consistent fur color and body size",
                        "facial_features": "stable muzzle and eye shape",
                        "outfit": "none",
                        "personality": dog_pers,
                        "consistency_note": "keep same dog identity across all shots",
                    }
                )
            if any(k in low for k in ("mèo", "cat", "kitten")):
                add(
                    {
                        "name": "Chú mèo" if is_vi else "Cat",
                        "role": "pet_cat",
                        "gender": "unspecified",
                        "age": "4",
                        "appearance": "family cat with consistent fur pattern and body shape",
                        "facial_features": "stable whisker and eye shape",
                        "outfit": "none",
                        "personality": "curious, gentle",
                        "consistency_note": "keep same cat identity across all shots",
                    }
                )
            if not chars:
                add({"name": "Nhân vật chính" if is_vi else "Main Character", "role": "main_character", "gender": "unspecified", "age": "28", "appearance": "young adult with natural look fitting family context", "facial_features": "friendly eyes, natural expression", "outfit": "casual modern outfit", "personality": "warm, relatable", "consistency_note": "maintain same facial identity and outfit"})
                add({"name": "Người thân" if is_vi else "Family Member", "role": "support_character", "gender": "unspecified", "age": "45", "appearance": "middle-aged family member with gentle expression", "facial_features": "kind face, stable expression", "outfit": "simple home wear", "personality": "protective, emotional", "consistency_note": "keep age impression and role consistent"})
        if not chars:
            add({"name": "Nhân vật chính" if is_vi else "Main Character", "role": "main_character", "gender": "unspecified", "age": "27", "appearance": "young creator with clear facial identity", "facial_features": "clear face silhouette, stable expression", "outfit": "consistent neutral outfit", "personality": "confident, natural", "consistency_note": "keep same face/hair/outfit palette in all episodes"})
        return chars

    def _refresh_auto_character_summary(self) -> None:
        if not self._auto_character_profiles:
            self._var_auto_char_summary.set("Chưa có nhân vật auto. Bấm nút để tạo hoặc thêm thủ công.")
            self._refresh_auto_character_thumbnails()
            return
        names = ", ".join([str(x.get("name", "")).strip() for x in self._auto_character_profiles if str(x.get("name", "")).strip()])
        self._var_auto_char_summary.set(f"Đã có {len(self._auto_character_profiles)} nhân vật: {names}")
        self._refresh_auto_character_thumbnails()

    def _refresh_auto_character_thumbnails(self) -> None:
        if not hasattr(self, "_char_auto_thumb_fr"):
            return
        for w in self._char_auto_thumb_fr.winfo_children():
            w.destroy()
        self._auto_char_thumb_refs = []
        chars = migrate_auto_character_profiles(list(self._auto_character_profiles or []))
        if not chars:
            ttk.Label(self._char_auto_thumb_fr, text="(Chưa map ảnh nhân vật)", foreground="gray").pack(side=tk.LEFT)
            return
        for row in chars[:6]:
            nm = str(row.get("name", "")).strip() or "Nhân vật"
            role = str(row.get("role", "")).strip().lower()
            is_main = role in {"main", "main_character", "protagonist", "lead"}
            p = Path(str(row.get("reference_image_path", "")).strip())
            holder = ttk.Frame(self._char_auto_thumb_fr)
            holder.pack(side=tk.LEFT, padx=(0, 8))
            if p.is_file() and Image is not None and ImageTk is not None:
                try:
                    im = Image.open(p)  # type: ignore[operator]
                    im.thumbnail((72, 72))
                    tk_img = ImageTk.PhotoImage(im)  # type: ignore[misc]
                    self._auto_char_thumb_refs.append(tk_img)
                    ttk.Label(holder, image=tk_img).pack()
                    role_text = "MAIN" if is_main else "SUPPORT"
                    role_color = "#0b5ed7" if is_main else "#6f42c1"
                    ttk.Label(holder, text=role_text, foreground=role_color).pack()
                    ttk.Label(holder, text=nm[:12]).pack()
                    continue
                except Exception:
                    pass
            ttk.Label(holder, text="🧍", font=("Segoe UI Emoji", 18)).pack()
            role_text = "MAIN" if is_main else "SUPPORT"
            role_color = "#0b5ed7" if is_main else "#6f42c1"
            ttk.Label(holder, text=role_text, foreground=role_color).pack()
            ttk.Label(holder, text=nm[:12]).pack()

    def _open_auto_characters_popup(self) -> None:
        top = tk.Toplevel(self._top)
        top.title("Quản lý nhân vật tự động")
        top.geometry("980x620")
        top.transient(self._top)
        top.grab_set()

        chars = migrate_auto_character_profiles(list(self._auto_character_profiles))

        if not chars:
            chars = [
                {
                    "name": "Nhân vật chính",
                    "appearance": "",
                    "outfit": "",
                    "personality": "",
                    "reference_image_path": "",
                    "character_id": "",
                    "character_image_generations": [],
                }
            ]

        root = ttk.Frame(top, padding=8)
        root.pack(fill=tk.BOTH, expand=True)
        root.columnconfigure(0, weight=1)
        root.columnconfigure(1, weight=1)
        root.rowconfigure(1, weight=1)

        left = ttk.LabelFrame(root, text="Danh sách nhân vật", padding=6)
        left.grid(row=0, column=0, rowspan=2, sticky="nsew", padx=(0, 8))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(0, weight=1)

        listbox = tk.Listbox(left, exportselection=False)
        listbox.grid(row=0, column=0, sticky="nsew")
        lsb = ttk.Scrollbar(left, orient=tk.VERTICAL, command=listbox.yview)
        lsb.grid(row=0, column=1, sticky="ns")
        listbox.configure(yscrollcommand=lsb.set)

        right = ttk.LabelFrame(root, text="Chi tiết nhân vật", padding=8)
        right.grid(row=0, column=1, sticky="nsew")
        right.columnconfigure(1, weight=1)
        right.rowconfigure(1, weight=1)

        var_name = tk.StringVar(value="")
        var_role = tk.StringVar(value="")
        var_gender = tk.StringVar(value="")
        var_age = tk.StringVar(value="")
        var_outfit = tk.StringVar(value="")
        var_facial = tk.StringVar(value="")
        var_personality = tk.StringVar(value="")
        var_consistency = tk.StringVar(value="")
        var_ref_image = tk.StringVar(value="")
        ttk.Label(right, text="Tên").grid(row=0, column=0, sticky="w")
        ent_name = ttk.Entry(right, textvariable=var_name, width=34)
        ent_name.grid(row=0, column=1, sticky="ew", padx=(8, 0))
        ttk.Label(right, text="Vai trò").grid(row=1, column=0, sticky="w", pady=(8, 0))
        cb_role = ttk.Combobox(
            right,
            textvariable=var_role,
            state="readonly",
            values=(
                "main_character",
                "mother_in_law",
                "daughter_in_law",
                "husband",
                "wife",
                "support_character",
                "friend",
                "narrator",
            ),
            width=32,
        )
        cb_role.grid(row=1, column=1, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Label(right, text="Giới tính").grid(row=2, column=0, sticky="w", pady=(8, 0))
        cb_gender = ttk.Combobox(
            right,
            textvariable=var_gender,
            state="readonly",
            values=("female", "male", "non-binary", "unspecified"),
            width=32,
        )
        cb_gender.grid(row=2, column=1, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Label(right, text="Tuổi").grid(row=3, column=0, sticky="w", pady=(8, 0))
        cb_age = ttk.Combobox(
            right,
            textvariable=var_age,
            state="readonly",
            values=("18", "20", "22", "25", "28", "30", "35", "40", "45", "50", "55", "60"),
            width=32,
        )
        cb_age.grid(row=3, column=1, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Label(right, text="Ngoại hình").grid(row=4, column=0, sticky="nw", pady=(8, 0))
        txt_appearance = tk.Text(right, height=5, wrap="word", font=("Segoe UI", 9))
        txt_appearance.grid(row=4, column=1, sticky="nsew", padx=(8, 0), pady=(8, 0))
        ttk.Label(right, text="Trang phục").grid(row=5, column=0, sticky="w", pady=(8, 0))
        ent_outfit = ttk.Entry(right, textvariable=var_outfit, width=34)
        ent_outfit.grid(row=5, column=1, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Label(right, text="Đặc điểm khuôn mặt").grid(row=6, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(right, textvariable=var_facial, width=34).grid(row=6, column=1, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Label(right, text="Tính cách").grid(row=7, column=0, sticky="w", pady=(8, 0))
        ent_personality = ttk.Entry(right, textvariable=var_personality, width=34)
        ent_personality.grid(row=7, column=1, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Label(right, text="Ghi chú đồng nhất").grid(row=8, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(right, textvariable=var_consistency, width=34).grid(row=8, column=1, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Label(right, text="Ảnh map nhân vật").grid(row=9, column=0, sticky="w", pady=(8, 0))
        ref_bar = ttk.Frame(right)
        ref_bar.grid(row=9, column=1, sticky="ew", padx=(8, 0), pady=(8, 0))
        ref_bar.columnconfigure(0, weight=1)
        ent_ref = ttk.Entry(ref_bar, textvariable=var_ref_image)
        ent_ref.grid(row=0, column=0, sticky="ew")
        char_style_rows = style_items("character_image_styles")
        env_style_rows = style_items("environment_styles")
        char_style_name_to_id = {
            str(x.get("name", "")).strip(): str(x.get("id", "")).strip()
            for x in char_style_rows
            if str(x.get("name", "")).strip() and str(x.get("id", "")).strip()
        }
        env_style_name_to_id = {
            str(x.get("name", "")).strip(): str(x.get("id", "")).strip()
            for x in env_style_rows
            if str(x.get("name", "")).strip() and str(x.get("id", "")).strip()
        }
        char_style_names = list(char_style_name_to_id.keys()) or ["Cinematic Realistic Character"]
        env_style_names = list(env_style_name_to_id.keys()) or ["Cinematic Environment"]
        var_char_style_name = tk.StringVar(
            value=style_name(
                "character_image_styles",
                self._var_character_image_style_id.get().strip(),
                fallback=char_style_names[0],
            )
            or char_style_names[0]
        )
        var_env_style_name = tk.StringVar(
            value=style_name(
                "environment_styles",
                self._var_environment_style_id.get().strip(),
                fallback=env_style_names[0],
            )
            or env_style_names[0]
        )
        self._var_character_image_style_id.set(
            char_style_name_to_id.get(var_char_style_name.get().strip(), self._var_character_image_style_id.get().strip())
        )
        self._var_environment_style_id.set(
            env_style_name_to_id.get(var_env_style_name.get().strip(), self._var_environment_style_id.get().strip())
        )
        ttk.Label(ref_bar, text="Phong cách ảnh nhân vật").grid(row=1, column=0, sticky="w", pady=(6, 0))
        cb_char_style = ttk.Combobox(
            ref_bar,
            state="readonly",
            values=tuple(char_style_names),
            textvariable=var_char_style_name,
            width=28,
        )
        cb_char_style.grid(row=1, column=1, sticky="w", padx=(6, 0), pady=(6, 0))
        ttk.Label(ref_bar, text="Phong cách bối cảnh").grid(row=1, column=2, sticky="w", padx=(12, 0), pady=(6, 0))
        cb_env_style = ttk.Combobox(
            ref_bar,
            state="readonly",
            values=tuple(env_style_names),
            textvariable=var_env_style_name,
            width=24,
        )
        cb_env_style.grid(row=1, column=3, sticky="w", padx=(6, 0), pady=(6, 0))
        thumb_holder = ttk.Label(right, text="(Chưa có ảnh nhân vật)", foreground="gray")
        thumb_holder.grid(row=10, column=1, sticky="w", padx=(8, 0), pady=(6, 0))
        thumb_state: dict[str, Any] = {"img": None}
        generated_list_fr = ttk.LabelFrame(right, text="Danh sách ảnh nhân vật đã tạo", padding=6)
        generated_list_fr.grid(row=11, column=1, sticky="nsew", padx=(8, 0), pady=(8, 0))
        generated_list_fr.columnconfigure(0, weight=1)
        generated_list_fr.rowconfigure(0, weight=1)
        gen_list = tk.Listbox(generated_list_fr, height=5, exportselection=False)
        gen_list.grid(row=0, column=0, sticky="nsew")
        gen_scroll = ttk.Scrollbar(generated_list_fr, orient=tk.VERTICAL, command=gen_list.yview)
        gen_scroll.grid(row=0, column=1, sticky="ns")
        gen_list.configure(yscrollcommand=gen_scroll.set)
        gen_status_var = tk.StringVar(value="Chưa tạo ảnh cho nhân vật này.")
        ttk.Label(generated_list_fr, textvariable=gen_status_var, foreground="#475569").grid(
            row=1, column=0, columnspan=2, sticky="w", pady=(6, 0)
        )
        right.rowconfigure(11, weight=1)
        image_gen_state = {"running": False}

        def _render_char_thumbnail(path: str) -> None:
            p = Path(str(path or "").strip())
            if not p.is_file():
                thumb_holder.configure(text="(Chưa có ảnh nhân vật)", image="")
                thumb_state["img"] = None
                return
            if Image is None or ImageTk is None:
                thumb_holder.configure(text=p.name, image="")
                thumb_state["img"] = None
                return
            try:
                im = Image.open(p)  # type: ignore[operator]
                im.thumbnail((180, 120))
                tk_img = ImageTk.PhotoImage(im)  # type: ignore[misc]
                thumb_holder.configure(text="", image=tk_img)
                thumb_state["img"] = tk_img
            except Exception:
                thumb_holder.configure(text=p.name, image="")
                thumb_state["img"] = None

        def choose_ref_image() -> None:
            fp = filedialog.askopenfilename(
                parent=top,
                title="Chọn ảnh map nhân vật",
                filetypes=[
                    ("Image files", "*.png;*.jpg;*.jpeg;*.webp"),
                    ("All files", "*.*"),
                ],
            )
            if not fp:
                return
            var_ref_image.set(str(Path(fp).resolve()))
            _render_char_thumbnail(var_ref_image.get())

        ttk.Button(ref_bar, text="Chọn ảnh", command=choose_ref_image).grid(row=0, column=1, padx=(6, 0))
        btn_gen_char_img = ttk.Button(
            ref_bar,
            text="Tạo ảnh bằng Nano Banana Pro",
            command=lambda: generate_character_images(),
        )
        btn_gen_char_img.grid(row=0, column=2, padx=(6, 0))
        _SimpleTooltip(
            btn_gen_char_img,
            "Mở trình duyệt (profile data/nanobanana/browser_profile) tại gemini.google.com/app, "
            "bật Tools → Create image → Pro rồi tạo ảnh map nhân vật. "
            "Đặt AUTO_CHARACTER_NANO_BANANA_USE_API=1 nếu muốn dùng Gemini API thay vì web.",
        )
        ttk.Button(ref_bar, text="Dùng ảnh đã chọn", command=lambda: apply_selected_generated_image()).grid(
            row=0, column=3, padx=(6, 0)
        )
        btn_gen_all = ttk.Button(
            ref_bar,
            text="Tạo ảnh cho tất cả nhân vật (Gemini web)",
            command=lambda: generate_all_character_images(),
        )
        btn_gen_all.grid(row=2, column=0, columnspan=4, sticky="w", pady=(6, 0))
        _SimpleTooltip(
            btn_gen_all,
            "Lần lượt mở Gemini cho từng nhân vật; ảnh lưu riêng theo character_id trong data/characters/.../images/. "
            "Danh sách ảnh bên dưới luôn theo nhân vật đang chọn ở cột trái.",
        )
        cb_char_style.bind(
            "<<ComboboxSelected>>",
            lambda _e: self._var_character_image_style_id.set(
                char_style_name_to_id.get(var_char_style_name.get().strip(), self._var_character_image_style_id.get().strip())
            ),
        )
        cb_env_style.bind(
            "<<ComboboxSelected>>",
            lambda _e: self._var_environment_style_id.set(
                env_style_name_to_id.get(var_env_style_name.get().strip(), self._var_environment_style_id.get().strip())
            ),
        )

        state = {"idx": 0}

        def _make_character_id(i: int, name: str, existing: str) -> str:
            e = str(existing or "").strip()
            if e:
                return e
            base = str(name or "").strip() or f"character_{i+1}"
            safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", base).strip("_") or f"character_{i+1}"
            return f"char_{i+1:03d}_{safe}"[:96]

        def refresh_list(select_idx: int = 0) -> None:
            listbox.delete(0, tk.END)
            for i, c in enumerate(chars, start=1):
                nm = str(c.get("name", "")).strip() or f"Nhân vật {i}"
                listbox.insert(tk.END, f"{i}. {nm}")
            if chars:
                idx = max(0, min(len(chars) - 1, select_idx))
                state["idx"] = idx
                listbox.selection_clear(0, tk.END)
                listbox.selection_set(idx)
                listbox.activate(idx)
                render_current()
            else:
                state["idx"] = -1
                var_name.set("")
                var_role.set("")
                var_gender.set("")
                var_age.set("")
                txt_appearance.delete("1.0", tk.END)
                var_outfit.set("")
                var_facial.set("")
                var_personality.set("")
                var_consistency.set("")
                var_ref_image.set("")
                _render_char_thumbnail("")
                refresh_generated_images_list()

        def save_current() -> None:
            idx = state["idx"]
            if idx < 0 or idx >= len(chars):
                return
            gens = normalize_character_image_generations(chars[idx].get("character_image_generations"))
            cid = _make_character_id(idx, var_name.get().strip(), str(chars[idx].get("character_id", "")))
            chars[idx] = {
                "character_id": cid,
                "name": var_name.get().strip(),
                "role": var_role.get().strip(),
                "gender": var_gender.get().strip(),
                "age": var_age.get().strip(),
                "appearance": txt_appearance.get("1.0", tk.END).strip(),
                "outfit": var_outfit.get().strip(),
                "facial_features": var_facial.get().strip(),
                "personality": var_personality.get().strip(),
                "consistency_note": var_consistency.get().strip(),
                "reference_image_path": var_ref_image.get().strip(),
                "character_image_generations": gens,
                "character_image_prompt": str(chars[idx].get("character_image_prompt", "")).strip(),
                "image_provider": str(chars[idx].get("image_provider", "")).strip(),
                "image_model": str(chars[idx].get("image_model", "")).strip(),
            }

        def render_current() -> None:
            idx = state["idx"]
            if idx < 0 or idx >= len(chars):
                return
            c = chars[idx]
            var_name.set(str(c.get("name", "")).strip())
            var_role.set(str(c.get("role", "")).strip())
            var_gender.set(str(c.get("gender", "")).strip())
            var_age.set(str(c.get("age", "")).strip())
            txt_appearance.delete("1.0", tk.END)
            txt_appearance.insert("1.0", str(c.get("appearance", "")).strip())
            var_outfit.set(str(c.get("outfit", "")).strip())
            var_facial.set(str(c.get("facial_features", "")).strip())
            var_personality.set(str(c.get("personality", "")).strip())
            var_consistency.set(str(c.get("consistency_note", "")).strip())
            var_ref_image.set(str(c.get("reference_image_path", "")).strip())
            _render_char_thumbnail(var_ref_image.get())
            refresh_generated_images_list()

        def on_pick(_e: tk.Event) -> None:
            save_current()
            sel = listbox.curselection()
            if not sel:
                return
            state["idx"] = int(sel[0])
            render_current()

        listbox.bind("<<ListboxSelect>>", on_pick)

        def _character_image_prompt(c: dict[str, Any]) -> str:
            name = str(c.get("name", "")).strip() or "Character"
            role = str(c.get("role", "")).strip() or "unspecified"
            gender = str(c.get("gender", "")).strip() or "unspecified"
            age = str(c.get("age", "")).strip() or "unspecified"
            appearance = str(c.get("appearance", "")).strip() or "as described for the role"
            facial_features = str(c.get("facial_features", "")).strip() or "natural, clear facial identity"
            outfit = str(c.get("outfit", "")).strip() or "consistent outfit"
            personality = str(c.get("personality", "")).strip() or "natural demeanor"
            consistency_note = str(c.get("consistency_note", "")).strip() or (
                "keep face, hair, skin tone, and outfit consistent across all future shots"
            )
            char_style_addon = style_prompt_addon(
                "character_image_styles",
                self._var_character_image_style_id.get().strip(),
                fallback="cinematic realistic portrait, natural skin texture, professional composition, soft cinematic lighting, high detail",
            )
            auto = self._PRESET_AUTO
            style_bits: list[str] = []
            style_bits.append(f"- Image style:\n{char_style_addon}")
            vis = str(self._var_visual_style.get() or "").strip()
            if vis and vis != auto:
                style_bits.append(f"- Visual preset (video project): {vis}")
            mood = str(self._var_mood.get() or "").strip()
            if mood and mood != auto:
                style_bits.append(f"- Mood: {mood}")
            cam = str(self._var_camera_style.get() or "").strip()
            if cam and cam != auto:
                style_bits.append(f"- Camera / composition tendency: {cam}")
            lit = str(self._var_lighting.get() or "").strip()
            if lit and lit != auto:
                style_bits.append(f"- Lighting: {lit}")
            mot = str(self._var_motion_style.get() or "").strip()
            if mot and mot != auto:
                style_bits.append(f"- Motion / energy (subtle for still portrait): {mot}")
            sp = str(self._selected_style_prompt() or "").strip()
            if sp:
                style_bits.append(
                    "- Locked style prompt (rendering look, palette, film grain, realism vs stylization — follow strictly):\n"
                    f"{sp[:900]}"
                )
            style_section = ""
            if style_bits:
                style_section = (
                    "\n\nGlobal visual direction for this video production "
                    "(the portrait MUST match this project; if any bullet below conflicts, follow THIS block):\n"
                    + "\n".join(style_bits)
                )

            core = (
                "Create a high-quality character reference portrait for AI video generation.\n\n"
                f"Character name:\n{name}\n\n"
                f"Role:\n{role}\n\n"
                f"Gender and age:\n{gender}, {age}\n\n"
                f"Appearance:\n{appearance}\n\n"
                f"Facial features:\n{facial_features}\n\n"
                f"Outfit:\n{outfit}\n\n"
                f"Personality:\n{personality}\n\n"
                f"Consistency requirements:\n{consistency_note}\n\n"
                "Image requirements:\n"
                "- Single character only\n"
                "- Clear face\n"
                "- Front-facing or slight 3/4 portrait\n"
                "- Upper body portrait\n"
                "- Neutral clean background unless style prompt says otherwise\n"
                "- Cinematic or style-appropriate lighting\n"
                "- Professional composition\n"
                "- High detail\n"
                "- Natural or style-appropriate skin / surface texture\n"
                "- No text\n"
                "- No logo\n"
                "- No watermark\n"
                "- No extra people\n"
                "- No distorted hands\n"
                "- No exaggerated facial features\n\n"
                "Purpose:\n"
                "This image will be used as a consistent character reference for future AI video prompts."
            )
            return core + style_section

        def _save_generated_character_images(
            idx: int,
            blobs: list[bytes],
            *,
            prompt: str,
            image_provider: str,
            image_model: str,
        ) -> list[dict[str, str]]:
            if idx < 0 or idx >= len(chars):
                return []
            cid = _make_character_id(idx, str(chars[idx].get("name", "")).strip(), str(chars[idx].get("character_id", "")))
            chars[idx]["character_id"] = cid
            out_dir = project_root() / "data" / "characters" / cid / "images"
            out_dir.mkdir(parents=True, exist_ok=True)
            display_name = str(chars[idx].get("name", "")).strip() or f"char_{idx+1}"
            safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", display_name).strip("_") or f"char_{idx+1}"
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            saved: list[dict[str, str]] = []
            for i, blob in enumerate(blobs, start=1):
                p = out_dir / f"{safe_name}_{ts}_{i}.png"
                p.write_bytes(blob)
                saved.append(
                    {
                        "character_image_path": str(p.resolve()),
                        "character_image_prompt": prompt,
                        "image_provider": image_provider,
                        "image_model": image_model,
                    }
                )
            return saved

        def sync_generated_image_preview_from_list() -> None:
            """Khi chọn dòng trong danh sách ảnh đã tạo: cập nhật thumbnail phía trên (chưa ghi map cho đến khi bấm Dùng ảnh đã chọn)."""
            idx = state["idx"]
            if idx < 0 or idx >= len(chars):
                return
            items = normalize_character_image_generations(chars[idx].get("character_image_generations"))
            if not items:
                return
            sel = gen_list.curselection()
            pick = int(sel[0]) if sel else 0
            pick = max(0, min(len(items) - 1, pick))
            fp = str(items[pick].get("character_image_path", "")).strip()
            pth = Path(fp)
            if fp and pth.is_file():
                _render_char_thumbnail(str(pth.resolve()))

        def refresh_generated_images_list() -> None:
            idx = state["idx"]
            gen_list.delete(0, tk.END)
            if idx < 0 or idx >= len(chars):
                gen_status_var.set("Chưa chọn nhân vật.")
                return
            items = normalize_character_image_generations(chars[idx].get("character_image_generations"))
            chars[idx]["character_image_generations"] = items
            if not items:
                gen_status_var.set("Chưa tạo ảnh cho nhân vật này.")
                return
            for i, meta in enumerate(items, start=1):
                gen_list.insert(tk.END, f"{i}. {Path(meta['character_image_path']).name}")
            gen_status_var.set(f"Đã có {len(items)} ảnh tạo sẵn. Chọn 1 ảnh rồi bấm 'Dùng ảnh đã chọn'.")
            ref = str(var_ref_image.get() or "").strip()
            select_idx = 0
            if ref:
                try:
                    ref_res = Path(ref).resolve()
                except Exception:
                    ref_res = None
                for j, meta in enumerate(items):
                    mp = str(meta.get("character_image_path", "")).strip()
                    if not mp:
                        continue
                    if ref_res is not None:
                        try:
                            if Path(mp).resolve() == ref_res:
                                select_idx = j
                                break
                        except Exception:
                            if mp == ref:
                                select_idx = j
                                break
                    elif mp == ref:
                        select_idx = j
                        break
            gen_list.selection_clear(0, tk.END)
            gen_list.selection_set(select_idx)
            gen_list.activate(select_idx)
            sync_generated_image_preview_from_list()

        gen_list.bind("<<ListboxSelect>>", lambda _e: sync_generated_image_preview_from_list())

        def apply_selected_generated_image() -> None:
            idx = state["idx"]
            if idx < 0:
                return
            items = normalize_character_image_generations(chars[idx].get("character_image_generations"))
            if not items:
                messagebox.showwarning("Ảnh nhân vật", "Chưa có ảnh tạo sẵn để chọn.", parent=top)
                return
            sel = gen_list.curselection()
            pick = int(sel[0]) if sel else 0
            pick = max(0, min(len(items) - 1, pick))
            meta = items[pick]
            fp = str(meta.get("character_image_path", "")).strip()
            if not fp:
                messagebox.showwarning("Ảnh nhân vật", "Mục đã chọn không có đường dẫn hợp lệ.", parent=top)
                return
            var_ref_image.set(fp)
            chars[idx]["character_image_prompt"] = str(meta.get("character_image_prompt", "")).strip()
            chars[idx]["image_provider"] = str(meta.get("image_provider", "")).strip()
            chars[idx]["image_model"] = str(meta.get("image_model", "")).strip()
            chars[idx]["reference_image_path"] = fp
            _render_char_thumbnail(fp)
            save_current()
            messagebox.showinfo("Ảnh nhân vật", f"Đã map ảnh:\n{fp}", parent=top)

        def _nb_pro_blobs_for_character_prompt(prompt: str) -> tuple[list[bytes], str]:
            """
            Mặc định: trình duyệt ``generate_post_images_nano_banana_browser`` (gemini.google.com theo NANOBANANA_WEB_URL).
            API: đặt ``AUTO_CHARACTER_NANO_BANANA_USE_API=1``.
            """
            use_api = os.environ.get("AUTO_CHARACTER_NANO_BANANA_USE_API", "").strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
            nb_cfg = nano_banana_pro_settings()
            raw_m = str(nb_cfg.get("model") or "gemini-3-pro-image-preview").strip()
            model = _canonical_nano_banana_pro_gemini_model(raw_m) or raw_m
            if use_api:
                blobs = self._img_svc.generate_images(
                    prompt=prompt,
                    number_of_images=4,
                    provider="nano_banana_pro",
                    model=model,
                )
                return blobs, model
            blobs = generate_post_images_nano_banana_browser(prompt=prompt, number_of_images=4)
            return blobs, "gemini.google.com/app"

        def generate_all_character_images() -> None:
            if image_gen_state["running"]:
                messagebox.showinfo("Ảnh nhân vật", "Đang tạo ảnh, vui lòng chờ.", parent=top)
                return
            if not chars:
                return
            save_current()
            n = len(chars)
            if not messagebox.askyesno(
                "Tạo ảnh hàng loạt",
                f"Sẽ lần lượt mở trình duyệt Gemini cho {n} nhân vật (tối đa 4 ảnh mỗi người).\n"
                "Cần profile đăng nhập tại data/nanobanana/browser_profile.\nTiếp tục?",
                parent=top,
            ):
                return
            image_gen_state["running"] = True
            start_idx = state["idx"]

            def worker_all() -> None:
                errs: list[str] = []
                try:
                    for i, ch in enumerate(chars):
                        nm = str(ch.get("name", "") or f"#{i + 1}").strip()
                        ii, nn, tot = i, nm, n
                        self._top.after(
                            0,
                            lambda ii=ii, nn=nn, tot=tot: gen_status_var.set(
                                f"[{ii + 1}/{tot}] {nn}: Chuẩn bị…"
                            ),
                        )
                        pr = _character_image_prompt(dict(ch))
                        if not str(pr).strip():
                            errs.append(f"{nn}: bỏ qua (thiếu mô tả)")
                            continue
                        try:
                            self._top.after(
                                0,
                                lambda ii=ii, nn=nn, tot=tot: gen_status_var.set(
                                    f"[{ii + 1}/{tot}] {nn}: Đang mở Gemini / tạo ảnh…"
                                ),
                            )
                            blobs, model_used = _nb_pro_blobs_for_character_prompt(pr)
                        except Exception as exc:  # noqa: BLE001
                            errs.append(f"{nn}: {exc}")
                            continue
                        if not blobs:
                            errs.append(f"{nn}: không nhận được ảnh")
                            continue
                        saved_meta = _save_generated_character_images(
                            i,
                            blobs,
                            prompt=pr,
                            image_provider="nano_banana_pro",
                            image_model=str(model_used),
                        )

                        def merge_saved(
                            idx_char: int = ii,
                            sm: list[dict[str, str]] = list(saved_meta),
                        ) -> None:
                            prior = normalize_character_image_generations(
                                chars[idx_char].get("character_image_generations")
                            )
                            chars[idx_char]["character_image_generations"] = prior + sm
                            if state["idx"] == idx_char:
                                refresh_generated_images_list()

                        self._top.after(0, merge_saved)

                    def finish() -> None:
                        image_gen_state["running"] = False
                        refresh_list(select_idx=start_idx)
                        gen_status_var.set(
                            "Hoàn tất tạo ảnh hàng loạt." if not errs else f"Hoàn tất có {len(errs)} lỗi/cảnh báo."
                        )
                        if errs:
                            messagebox.showwarning(
                                "Ảnh nhân vật",
                                "Một số mục lỗi hoặc bị bỏ qua:\n" + "\n".join(errs[:14]) + ("\n…" if len(errs) > 14 else ""),
                                parent=top,
                            )
                        else:
                            messagebox.showinfo("Ảnh nhân vật", f"Đã tạo ảnh xong cho {n} nhân vật.", parent=top)

                    self._top.after(0, finish)
                except Exception as exc:  # noqa: BLE001

                    def fail() -> None:
                        image_gen_state["running"] = False
                        gen_status_var.set("Tạo ảnh hàng loạt thất bại.")
                        messagebox.showerror("Ảnh nhân vật", str(exc), parent=top)

                    self._top.after(0, fail)

            threading.Thread(target=worker_all, daemon=True, name="auto_character_image_gen_all").start()

        def generate_character_images() -> None:
            if image_gen_state["running"]:
                messagebox.showinfo("Ảnh nhân vật", "Đang tạo ảnh, vui lòng chờ.", parent=top)
                return
            idx = state["idx"]
            if idx < 0 or idx >= len(chars):
                return
            save_current()
            c = dict(chars[idx])
            prompt = _character_image_prompt(c)
            if not prompt.strip():
                messagebox.showwarning("Ảnh nhân vật", "Mô tả nhân vật đang trống.", parent=top)
                return
            image_gen_state["running"] = True
            gen_status_var.set("Đang tạo ảnh nhân vật (Gemini web / Nano Banana Pro)...")

            def worker() -> None:
                try:
                    blobs, model_used = _nb_pro_blobs_for_character_prompt(prompt)
                    saved_meta = _save_generated_character_images(
                        idx,
                        blobs,
                        prompt=prompt,
                        image_provider="nano_banana_pro",
                        image_model=str(model_used),
                    )

                    def done_ok() -> None:
                        image_gen_state["running"] = False
                        prior = normalize_character_image_generations(chars[idx].get("character_image_generations"))
                        chars[idx]["character_image_generations"] = prior + saved_meta
                        refresh_generated_images_list()
                        if saved_meta:
                            first_path = saved_meta[0]["character_image_path"]
                            var_ref_image.set(first_path)
                            _render_char_thumbnail(first_path)
                            save_current()
                        gen_status_var.set(
                            f"Tạo xong {len(saved_meta)} ảnh cho {str(c.get('name', 'Nhân vật')).strip()}."
                        )

                    self._top.after(0, done_ok)
                except Exception as exc:  # noqa: BLE001
                    err_text = str(exc)

                    def done_fail() -> None:
                        image_gen_state["running"] = False
                        gen_status_var.set("Tạo ảnh thất bại (Nano Banana Pro).")
                        messagebox.showerror(
                            "Ảnh nhân vật",
                            f"Tạo ảnh thất bại (Nano Banana Pro):\n{err_text}",
                            parent=top,
                        )

                    self._top.after(0, done_fail)

            threading.Thread(target=worker, daemon=True, name="auto_character_image_gen").start()

        act = ttk.Frame(root)
        act.grid(row=1, column=1, sticky="ew", pady=(8, 0))
        ttk.Button(act, text="Thêm nhân vật", command=lambda: add_character()).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(act, text="Xóa nhân vật", command=lambda: delete_character()).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(act, text="Lưu danh sách", command=lambda: save_and_close()).pack(side=tk.RIGHT)
        ttk.Button(act, text="Hủy", command=top.destroy).pack(side=tk.RIGHT, padx=(0, 6))

        def add_character() -> None:
            save_current()
            chars.append(
                {
                    "character_id": "",
                    "name": f"Nhân vật {len(chars)+1}",
                    "role": "",
                    "gender": "",
                    "age": "",
                    "appearance": "",
                    "outfit": "",
                    "facial_features": "",
                    "personality": "",
                    "consistency_note": "",
                    "reference_image_path": "",
                    "character_image_generations": [],
                    "character_image_prompt": "",
                    "image_provider": "",
                    "image_model": "",
                }
            )
            refresh_list(select_idx=len(chars) - 1)

        def delete_character() -> None:
            idx = state["idx"]
            if idx < 0 or idx >= len(chars):
                return
            if not messagebox.askyesno("Xóa", "Xóa nhân vật đang chọn?", parent=top):
                return
            chars.pop(idx)
            refresh_list(select_idx=max(0, idx - 1))

        def save_and_close() -> None:
            save_current()
            cleaned: list[dict[str, Any]] = []
            for c in chars:
                name = str(c.get("name", "")).strip()
                role = str(c.get("role", "")).strip()
                gender = str(c.get("gender", "")).strip()
                age = str(c.get("age", "")).strip()
                appearance = str(c.get("appearance", "")).strip()
                outfit = str(c.get("outfit", "")).strip()
                facial = str(c.get("facial_features", "")).strip()
                personality = str(c.get("personality", "")).strip()
                consistency_note = str(c.get("consistency_note", "")).strip()
                reference_image_path = str(c.get("reference_image_path", "")).strip()
                character_id = str(c.get("character_id", "")).strip()
                character_image_generations = normalize_character_image_generations(c.get("character_image_generations"))
                character_image_prompt = str(c.get("character_image_prompt", "")).strip()
                image_provider = str(c.get("image_provider", "")).strip()
                image_model = str(c.get("image_model", "")).strip()
                if not name and not appearance:
                    continue
                cleaned.append(
                    {
                        "character_id": character_id,
                        "name": name or "Nhân vật",
                        "role": role,
                        "gender": gender,
                        "age": age,
                        "appearance": appearance or "consistent appearance",
                        "outfit": outfit or "consistent outfit",
                        "facial_features": facial or "keep facial identity consistent",
                        "personality": personality or "natural personality",
                        "consistency_note": consistency_note or "keep identity consistent across all episodes",
                        "reference_image_path": reference_image_path,
                        "character_image_generations": character_image_generations,
                        "character_image_prompt": character_image_prompt,
                        "image_provider": image_provider,
                        "image_model": image_model,
                    }
                )
            self._auto_character_profiles = migrate_auto_character_profiles(cleaned)
            self._refresh_auto_character_summary()
            top.destroy()

        refresh_list(0)

    def _normalize_character_mode(self, value: str) -> str:
        v = str(value or "").strip().lower()
        if "manual" in v or "thủ công" in v:
            return "manual"
        return "auto"

    def _preset_kind_key(self) -> str:
        m = {
            "Phong cách": "visual_style",
            "Tâm trạng": "mood",
            "Camera": "camera_style",
            "Ánh sáng": "lighting",
            "Chuyển động": "motion_style",
        }
        return m.get(self._var_preset_kind.get().strip(), "visual_style")

    def _preset_kind_var(self) -> tk.StringVar:
        m: dict[str, tk.StringVar] = {
            "visual_style": self._var_visual_style,
            "mood": self._var_mood,
            "camera_style": self._var_camera_style,
            "lighting": self._var_lighting,
            "motion_style": self._var_motion_style,
        }
        return m[self._preset_kind_key()]

    def _refresh_prompt_preset_combos(self) -> None:
        self._cb_visual_style.configure(values=tuple(self._preset_combo_values("visual_style")))
        self._cb_mood.configure(values=tuple(self._preset_combo_values("mood")))
        self._cb_camera.configure(values=tuple(self._preset_combo_values("camera_style")))
        self._cb_lighting.configure(values=tuple(self._preset_combo_values("lighting")))
        self._cb_motion.configure(values=tuple(self._preset_combo_values("motion_style")))

    def _on_add_prompt_preset(self) -> None:
        kind = self._preset_kind_key()
        label = self._var_preset_kind.get().strip()
        self._open_prompt_preset_popup(title=f"Thêm preset: {label}", kind=kind)

    def _on_edit_prompt_preset(self) -> None:
        kind = self._preset_kind_key()
        label = self._var_preset_kind.get().strip()
        current = self._preset_kind_var().get().strip()
        if not current:
            messagebox.showwarning("Preset", "Chọn giá trị hiện tại để sửa.", parent=self._top)
            return
        self._open_prompt_preset_popup(title=f"Sửa preset: {label}", kind=kind, old_value=current, initial=current)

    def _on_delete_prompt_preset(self) -> None:
        kind = self._preset_kind_key()
        current = self._preset_kind_var().get().strip()
        if not current:
            messagebox.showwarning("Preset", "Chọn giá trị để xóa.", parent=self._top)
            return
        if current == self._PRESET_AUTO:
            messagebox.showwarning("Preset", "Không thể xóa preset Auto mặc định.", parent=self._top)
            return
        vals = list(self._prompt_presets.get(kind, []))
        names = [str(x.get("name", "")).strip() for x in vals if isinstance(x, dict)]
        if current not in names:
            messagebox.showwarning("Preset", "Giá trị không nằm trong danh sách preset.", parent=self._top)
            return
        if len(vals) <= 1:
            messagebox.showwarning("Preset", "Cần giữ lại ít nhất 1 preset.", parent=self._top)
            return
        if not messagebox.askyesno("Xóa preset", f"Xóa preset '{current}'?", parent=self._top):
            return
        vals = [x for x in vals if str(x.get("name", "")).strip() != current]
        self._prompt_presets[kind] = vals
        save_prompt_presets(self._prompt_presets)
        self._refresh_prompt_preset_combos()
        first = str(vals[0].get("name", "")).strip() if vals else ""
        self._preset_kind_var().set(first)

    def _open_prompt_preset_popup(self, *, title: str, kind: str, old_value: str | None = None, initial: str = "") -> None:
        top = tk.Toplevel(self._top)
        top.title(title)
        top.geometry("760x300")
        top.transient(self._top)
        top.grab_set()
        fr = ttk.Frame(top, padding=10)
        fr.pack(fill=tk.BOTH, expand=True)
        fr.columnconfigure(1, weight=1)
        fr.rowconfigure(1, weight=1)

        old_desc = self._preset_description(kind, old_value or "")

        ttk.Label(fr, text="Tên preset").grid(row=0, column=0, sticky="w")
        v_name = tk.StringVar(value=initial)
        ent = ttk.Entry(fr, textvariable=v_name, width=60)
        ent.grid(row=0, column=1, sticky="ew", padx=(8, 0))

        ttk.Label(fr, text="Mô tả chi tiết").grid(row=1, column=0, sticky="nw", pady=(8, 0))
        txt = tk.Text(fr, wrap="word", height=8, font=("Segoe UI", 9))
        txt.grid(row=1, column=1, sticky="nsew", padx=(8, 0), pady=(8, 0))
        sy = ttk.Scrollbar(fr, orient=tk.VERTICAL, command=txt.yview)
        sy.grid(row=1, column=2, sticky="ns", pady=(8, 0))
        txt.configure(yscrollcommand=sy.set)
        if old_desc:
            txt.insert("1.0", old_desc)

        def save_now() -> None:
            new_name = v_name.get().strip()
            new_desc = txt.get("1.0", tk.END).strip()
            if not new_name:
                messagebox.showwarning("Preset", "Tên preset không được để trống.", parent=top)
                return
            if not new_desc:
                messagebox.showwarning("Preset", "Mô tả chi tiết không được để trống.", parent=top)
                return
            vals = list(self._prompt_presets.get(kind, []))
            names = [str(x.get("name", "")).strip() for x in vals if isinstance(x, dict)]
            if old_value:
                if new_name != old_value and new_name in names:
                    messagebox.showwarning("Preset", "Tên preset đã tồn tại.", parent=top)
                    return
                replaced = False
                for i, row in enumerate(vals):
                    if str(row.get("name", "")).strip() == old_value:
                        vals[i] = {"name": new_name, "description": new_desc}
                        replaced = True
                        break
                if not replaced:
                    vals.append({"name": new_name, "description": new_desc})
                else:
                    pass
            else:
                if new_name in names:
                    messagebox.showwarning("Preset", "Tên preset đã tồn tại.", parent=top)
                    return
                vals.append({"name": new_name, "description": new_desc})
            # unique giữ thứ tự
            out: list[dict[str, str]] = []
            seen: set[str] = set()
            for x in vals:
                name = str(x.get("name", "")).strip()
                desc = str(x.get("description", "")).strip()
                if not name or name in seen:
                    continue
                seen.add(name)
                out.append({"name": name, "description": desc or name})
            self._prompt_presets[kind] = out
            save_prompt_presets(self._prompt_presets)
            self._refresh_prompt_preset_combos()
            self._preset_kind_var().set(new_name)
            top.destroy()

        bar = ttk.Frame(fr)
        bar.grid(row=1, column=1, sticky="e", pady=(12, 0))
        ttk.Button(bar, text="Hủy", command=top.destroy).pack(side=tk.RIGHT)
        ttk.Button(bar, text="Lưu", command=save_now).pack(side=tk.RIGHT, padx=(0, 6))
        ent.focus_set()

    def _style_display_name_from_id(self, group: str, sid: str, fallback: str = "") -> str:
        return style_name(group, str(sid or "").strip(), fallback=fallback)

    def _on_auto_select_styles(self) -> None:
        if not bool(self._var_auto_style_enable.get()):
            messagebox.showinfo(
                "Auto style",
                "Bật checkbox « Auto chọn style bằng AI » trước khi phân tích.",
                parent=self._top,
            )
            return
        idea = self._prompt_text().strip()
        if not idea:
            messagebox.showwarning("Auto style", "Nhập prompt mô tả video trước.", parent=self._top)
            return

        platform_hint = "Facebook Reels"
        mode_key = self._current_mode_key()
        if mode_key in {"prompt_to_video", "text_to_video"}:
            platform_hint = "Facebook Reels"
        goal = str(self._var_goal.get() or "").strip() or "viral cinematic story"
        language = str(self._var_language.get() or "").strip() or "Tiếng Việt"

        try:
            self._style_registry = load_style_registry()
            self._auto_style_selector = AutoStyleSelector(AIProviderFactory.text("gemini"), self._style_registry)
            picked = self._auto_style_selector.select_styles(
                idea=idea,
                target_platform=platform_hint,
                content_goal=goal,
                language=language,
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Auto style", f"Phân tích style lỗi:\n{exc}", parent=self._top)
            return

        self._var_image_style_id.set(str(picked.get("image_style_id", "")).strip())
        self._var_character_image_style_id.set(str(picked.get("image_style_id", "")).strip())
        self._var_video_style_id.set(str(picked.get("video_style_id", "")).strip())
        self._var_camera_style_id.set(str(picked.get("camera_style_id", "")).strip())
        self._var_lighting_style_id.set(str(picked.get("lighting_style_id", "")).strip())
        self._var_motion_style_id.set(str(picked.get("motion_style_id", "")).strip())
        self._var_auto_style_mood.set(str(picked.get("mood", "")).strip())
        self._var_auto_style_reason.set(str(picked.get("reason", "")).strip())
        self._var_ai_tag_visual.set("được AI chọn")
        self._var_ai_tag_mood.set("được AI chọn")
        self._var_ai_tag_camera.set("được AI chọn")
        self._var_ai_tag_lighting.set("được AI chọn")
        self._var_ai_tag_motion.set("được AI chọn")

        video_name = self._style_display_name_from_id("video_styles", self._var_video_style_id.get().strip(), "")
        camera_name = self._style_display_name_from_id("camera_styles", self._var_camera_style_id.get().strip(), "")
        lighting_name = self._style_display_name_from_id("lighting_styles", self._var_lighting_style_id.get().strip(), "")
        motion_name = self._style_display_name_from_id("motion_styles", self._var_motion_style_id.get().strip(), "")
        if video_name:
            self._var_visual_style.set(video_name)
        if camera_name:
            self._var_camera_style.set(camera_name)
        if lighting_name:
            self._var_lighting.set(lighting_name)
        if motion_name:
            self._var_motion_style.set(motion_name)
        ar = str(picked.get("aspect_ratio", "")).strip()
        if ar in {"9:16", "16:9", "1:1"}:
            self._var_aspect.set(ar)
        try:
            d = int(picked.get("duration_sec") or 8)
            if d in (4, 6, 8):
                self._var_duration.set(str(d))
        except Exception:
            pass
        self._var_mood.set(str(picked.get("mood", "")).strip() or self._var_mood.get().strip())

    def _preset_names(self, kind: str) -> list[str]:
        vals = list(self._prompt_presets.get(kind, []))
        out: list[str] = []
        for row in vals:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name", "")).strip()
            if name:
                out.append(name)
        return out

    def _preset_combo_values(self, kind: str) -> list[str]:
        return [self._PRESET_AUTO] + self._preset_names(kind)

    def _preset_description(self, kind: str, name: str) -> str:
        n = str(name or "").strip()
        for row in self._prompt_presets.get(kind, []):
            if not isinstance(row, dict):
                continue
            if str(row.get("name", "")).strip() == n:
                return str(row.get("description", "")).strip() or n
        return n

    def _first_preset_name(self, kind: str, fallback: str) -> str:
        names = self._preset_names(kind)
        return names[0] if names else fallback

    def _resolve_auto_story_preset_values(self, *, idea_text: str) -> dict[str, str]:
        video_addon = style_prompt_addon("video_styles", self._var_video_style_id.get().strip())
        camera_addon = style_prompt_addon("camera_styles", self._var_camera_style_id.get().strip())
        lighting_addon = style_prompt_addon("lighting_styles", self._var_lighting_style_id.get().strip())
        motion_addon = style_prompt_addon("motion_styles", self._var_motion_style_id.get().strip())
        mood_txt = self._var_auto_style_mood.get().strip() or self._var_mood.get().strip()
        if bool(self._var_auto_style_enable.get()) and (video_addon or camera_addon or lighting_addon or motion_addon):
            return {
                "visual_style": video_addon or self._preset_description("visual_style", self._var_visual_style.get().strip()),
                "mood": mood_txt or "cinematic and polished",
                "camera_style": camera_addon or self._preset_description("camera_style", self._var_camera_style.get().strip()),
                "lighting": lighting_addon or self._preset_description("lighting", self._var_lighting.get().strip()),
                "motion_style": motion_addon or self._preset_description("motion_style", self._var_motion_style.get().strip()),
            }

        base = " ".join(
            [
                str(idea_text or "").strip(),
                str(self._var_topic.get() or "").strip(),
                str(self._var_goal.get() or "").strip(),
            ]
        ).lower()
        visual_name = self._var_visual_style.get().strip()
        mood_name = self._var_mood.get().strip()
        camera_name = self._var_camera_style.get().strip()
        lighting_name = self._var_lighting.get().strip()
        motion_name = self._var_motion_style.get().strip()

        if visual_name == self._PRESET_AUTO:
            if any(k in base for k in ("anime", "manga", "2d", "hoạt hình")):
                visual_name = "Anime"
            elif any(k in base for k in ("product", "sản phẩm", "quảng cáo", "review")):
                visual_name = "Quảng cáo sản phẩm"
            elif any(k in base for k in ("street", "đường phố", "travel", "du lịch")):
                visual_name = "Đường phố"
            elif any(k in base for k in ("dark", "bí ẩn", "horror", "kinh dị", "thriller")):
                visual_name = "Tối, bí ẩn"
            elif any(k in base for k in ("premium", "luxury", "cao cấp", "sang trọng")):
                visual_name = "Sang trọng"
            else:
                visual_name = "Điện ảnh"

        if mood_name == self._PRESET_AUTO:
            if any(k in base for k in ("viral", "trend", "fun", "vui", "hài")):
                mood_name = "Vui tươi"
            elif any(k in base for k in ("emotional", "cảm xúc", "drama", "kể chuyện")):
                mood_name = "Cảm xúc"
            elif any(k in base for k in ("energetic", "năng động", "thể thao", "gym")):
                mood_name = "Năng động"
            elif any(k in base for k in ("mysterious", "bí ẩn", "dark")):
                mood_name = "Bí ẩn"
            else:
                mood_name = "Truyền cảm hứng"

        if camera_name == self._PRESET_AUTO:
            if any(k in base for k in ("product", "chi tiết", "close", "cận")):
                camera_name = "Macro sản phẩm"
            elif any(k in base for k in ("vlog", "street", "handheld", "cầm tay")):
                camera_name = "Cầm tay chân thực"
            elif any(k in base for k in ("cinematic", "film", "điện ảnh")):
                camera_name = "Pan điện ảnh"
            else:
                camera_name = "Theo dõi mượt"

        if lighting_name == self._PRESET_AUTO:
            if any(k in base for k in ("night", "đêm", "neon")):
                lighting_name = "Đêm neon"
            elif any(k in base for k in ("studio", "product", "quảng cáo")):
                lighting_name = "Ánh sáng studio"
            elif any(k in base for k in ("golden", "hoàng hôn", "sunset", "giờ vàng")):
                lighting_name = "Giờ vàng"
            else:
                lighting_name = "Ánh sáng tự nhiên dịu"

        if motion_name == self._PRESET_AUTO:
            if any(k in base for k in ("action", "dance", "năng động", "gym", "sport")):
                motion_name = "Năng động"
            elif any(k in base for k in ("product", "xoay", "showcase")):
                motion_name = "Xoay sản phẩm"
            elif any(k in base for k in ("walk", "đi bộ", "street")):
                motion_name = "Cảnh đi bộ"
            else:
                motion_name = "Chậm và mượt"

        return {
            "visual_style": self._preset_description("visual_style", visual_name),
            "mood": self._preset_description("mood", mood_name),
            "camera_style": self._preset_description("camera_style", camera_name),
            "lighting": self._preset_description("lighting", lighting_name),
            "motion_style": self._preset_description("motion_style", motion_name),
        }

    def _expand_text_prompt_variants(self, base_prompt: str, count: int) -> list[str]:
        """
        Tạo chuỗi prompt theo dạng nhiều tập (episode) từ cùng 1 ý tưởng.
        Mỗi tập là 1 prompt riêng nhưng phải liền mạch thành 1 câu chuyện.
        """
        bp = str(base_prompt or "").strip()
        n = max(1, int(count))
        if n == 1:
            return [bp]
        arc_steps = [
            "Episode purpose: Establish the world, main character, and core problem hook.",
            "Episode purpose: Escalate conflict with a new obstacle tied to the main problem.",
            "Episode purpose: Deepen emotional stakes and reveal a key turning point.",
            "Episode purpose: Build toward climax with high tension and decisive action.",
            "Episode purpose: Resolve the story with clear payoff and satisfying ending.",
        ]
        out: list[str] = []
        for i in range(1, n + 1):
            progress = i / n
            if progress <= 0.2:
                arc = arc_steps[0]
            elif progress <= 0.45:
                arc = arc_steps[1]
            elif progress <= 0.65:
                arc = arc_steps[2]
            elif progress <= 0.85:
                arc = arc_steps[3]
            else:
                arc = arc_steps[4]

            prev_hint = (
                f"Continuity from previous episode: Continue naturally from Episode {i-1} outcomes."
                if i > 1
                else "Continuity from previous episode: This is the first episode of the story."
            )
            next_hint = (
                f"Ending bridge: End Episode {i} with a hook that leads to Episode {i+1}."
                if i < n
                else "Ending bridge: This is the final episode, provide clear closure."
            )

            out.append(
                f"{bp}\n\n"
                f"Series mode: {n}-episode coherent story.\n"
                f"Current episode: {i}/{n}.\n"
                f"{arc}\n"
                f"{prev_hint}\n"
                f"{next_hint}\n"
                "Story continuity rules: keep the same characters, setting logic, outfits (unless story-justified), "
                "time progression, object states, and visual identity across all episodes.\n"
                "Episode-specific direction: each episode must show new progression, not a duplicate of previous episodes."
            )
        return out

    def _on_prompt_resize_start(self, event: tk.Event) -> None:
        self._prompt_drag_start_y = int(getattr(event, "y_root", 0) or 0)
        self._prompt_drag_start_height = int(self._prompt_lines_height)

    def _on_prompt_resizing(self, event: tk.Event) -> None:
        y_now = int(getattr(event, "y_root", 0) or 0)
        dy = y_now - self._prompt_drag_start_y
        # Khoảng 18px ~= 1 dòng. Giới hạn theo chiều cao cửa sổ để tránh che panel/bảng dưới.
        try:
            win_h = max(680, int(self._top.winfo_height()))
        except Exception:
            win_h = 760
        dynamic_max = max(6, min(14, int((win_h - 470) / 18)))
        new_height = max(3, min(dynamic_max, self._prompt_drag_start_height + int(dy / 18)))
        if new_height == self._prompt_lines_height:
            # Nếu đã chạm trần mà user vẫn kéo xuống thì nới nhẹ chiều cao cửa sổ.
            if dy > 0:
                self._try_expand_dialog_height(extra_px=24)
            return
        self._prompt_lines_height = new_height
        self._txt_prompt.configure(height=new_height)

    def _try_expand_dialog_height(self, *, extra_px: int = 24) -> None:
        try:
            self._top.update_idletasks()
            cur_w = int(self._top.winfo_width())
            cur_h = int(self._top.winfo_height())
            max_h = int(self._top.winfo_screenheight() * 0.92)
            new_h = min(max_h, cur_h + max(8, int(extra_px)))
            if new_h <= cur_h:
                return
            self._top.geometry(f"{cur_w}x{new_h}")
        except Exception:
            pass

    def _language_hint_text(self, lang_label: str) -> str:
        m: dict[str, str] = {
            "Tiếng Việt": "Language requirement: Use Vietnamese for any spoken lines, on-screen text, and narrative tone.",
            "English": "Language requirement: Use English for any spoken lines, on-screen text, and narrative tone.",
            "Español": "Language requirement: Use Spanish for any spoken lines, on-screen text, and narrative tone.",
            "Português": "Language requirement: Use Portuguese for any spoken lines, on-screen text, and narrative tone.",
            "Français": "Language requirement: Use French for any spoken lines, on-screen text, and narrative tone.",
            "Deutsch": "Language requirement: Use German for any spoken lines, on-screen text, and narrative tone.",
            "Italiano": "Language requirement: Use Italian for any spoken lines, on-screen text, and narrative tone.",
            "日本語": "Language requirement: Use Japanese for any spoken lines, on-screen text, and narrative tone.",
            "한국어": "Language requirement: Use Korean for any spoken lines, on-screen text, and narrative tone.",
            "中文 (简体)": "Language requirement: Use Simplified Chinese for any spoken lines, on-screen text, and narrative tone.",
            "中文 (繁體)": "Language requirement: Use Traditional Chinese for any spoken lines, on-screen text, and narrative tone.",
            "ไทย": "Language requirement: Use Thai for any spoken lines, on-screen text, and narrative tone.",
            "Bahasa Indonesia": "Language requirement: Use Indonesian for any spoken lines, on-screen text, and narrative tone.",
            "हिन्दी": "Language requirement: Use Hindi for any spoken lines, on-screen text, and narrative tone.",
        }
        return m.get(lang_label, "Language requirement: keep the language consistent with the selected locale.")

    def _open_add_style_popup(self) -> None:
        self._open_style_editor_popup(title="Thêm style mới")

    def _open_edit_style_popup(self) -> None:
        selected = self._var_style.get().strip()
        if not selected or selected in {self._STYLE_NONE, self._STYLE_AUTO}:
            messagebox.showwarning("Style", "Chọn style cần sửa trước.", parent=self._top)
            return
        row = next((x for x in self._styles if x["name"] == selected), None)
        if not row:
            messagebox.showwarning("Style", "Không tìm thấy style để sửa.", parent=self._top)
            return
        self._open_style_editor_popup(
            title="Sửa style",
            editing_original_name=selected,
            initial_name=row["name"],
            initial_prompt=row["prompt"],
        )

    def _open_style_editor_popup(
        self,
        *,
        title: str,
        editing_original_name: str | None = None,
        initial_name: str = "",
        initial_prompt: str = "",
    ) -> None:
        top = tk.Toplevel(self._top)
        top.title(title)
        top.geometry("860x360")
        top.minsize(760, 320)
        top.transient(self._top)
        top.grab_set()

        frm = ttk.Frame(top, padding=12)
        frm.pack(fill=tk.BOTH, expand=True)
        frm.columnconfigure(0, weight=0)
        frm.columnconfigure(1, weight=1)
        frm.rowconfigure(2, weight=1)

        var_name = tk.StringVar(value=initial_name)

        ttk.Label(frm, text="Tên style").grid(row=0, column=0, sticky="nw", pady=(0, 6))
        ent_name = ttk.Entry(frm, textvariable=var_name, width=38)
        ent_name.grid(row=0, column=1, sticky="ew", padx=(10, 0), pady=(0, 6))

        ttk.Label(frm, text="Mô tả style").grid(row=1, column=0, sticky="nw")
        hint = ttk.Label(
            frm,
            text="Nhập mô tả nhiều dòng (tone, ánh sáng, camera, màu sắc...).",
            foreground="gray",
        )
        hint.grid(row=1, column=1, sticky="nw", padx=(10, 0))

        txt_fr = ttk.Frame(frm)
        txt_fr.grid(row=2, column=1, sticky="nsew", padx=(10, 0), pady=(4, 0))
        txt_fr.columnconfigure(0, weight=1)
        txt_fr.rowconfigure(0, weight=1)
        txt_prompt = tk.Text(txt_fr, wrap="word", height=9, font=("Segoe UI", 10))
        txt_prompt.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(txt_fr, orient=tk.VERTICAL, command=txt_prompt.yview)
        sy.grid(row=0, column=1, sticky="ns")
        sx = ttk.Scrollbar(txt_fr, orient=tk.HORIZONTAL, command=txt_prompt.xview)
        sx.grid(row=1, column=0, sticky="ew")
        txt_prompt.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)
        if initial_prompt:
            txt_prompt.insert("1.0", initial_prompt)

        action = ttk.Frame(frm)
        action.grid(row=3, column=1, sticky="e", pady=(12, 0))

        def do_save() -> None:
            name = var_name.get().strip()
            prompt = txt_prompt.get("1.0", tk.END).strip()
            if not name or not prompt:
                messagebox.showwarning("Style", "Tên style và mô tả style không được để trống.", parent=top)
                return
            if editing_original_name is None:
                if any(x["name"] == name for x in self._styles):
                    messagebox.showwarning("Style", "Tên style đã tồn tại.", parent=top)
                    return
                self._styles.append({"name": name, "prompt": prompt})
            else:
                if name != editing_original_name and any(x["name"] == name for x in self._styles):
                    messagebox.showwarning("Style", "Tên style mới đã tồn tại.", parent=top)
                    return
                replaced = False
                for i, row in enumerate(self._styles):
                    if row["name"] == editing_original_name:
                        self._styles[i] = {"name": name, "prompt": prompt}
                        replaced = True
                        break
                if not replaced:
                    messagebox.showwarning("Style", "Không tìm thấy style để cập nhật.", parent=top)
                    return

            save_video_styles(self._styles)
            self._load_styles()
            self._var_style.set(name)
            self._on_style_selected()
            top.destroy()

        ttk.Button(action, text="Lưu", command=do_save).pack(side=tk.RIGHT)
        ttk.Button(action, text="Hủy", command=top.destroy).pack(side=tk.RIGHT, padx=(0, 6))
        ent_name.focus_set()

    def _on_delete_style(self) -> None:
        name = self._var_style.get().strip()
        if not name or name in {self._STYLE_NONE, self._STYLE_AUTO}:
            messagebox.showwarning("Style", "Chọn style cần xóa.", parent=self._top)
            return
        if not messagebox.askyesno("Xóa style", f"Xóa style '{name}'?", parent=self._top):
            return
        self._styles = [x for x in self._styles if x["name"] != name]
        save_video_styles(self._styles)
        self._load_styles()

    def _on_reset_default_styles(self) -> None:
        if not messagebox.askyesno("Style", "Khôi phục bộ style mặc định?", parent=self._top):
            return
        from src.services.ai_video_styles import default_video_styles

        self._styles = default_video_styles()
        save_video_styles(self._styles)
        self._load_styles()

    def _infer_style_from_prompt(self, prompt: str) -> str:
        p = (prompt or "").strip().lower()
        if not p:
            return "cinematic lighting, natural camera movement, clear subject, clean composition"
        rules: list[tuple[tuple[str, ...], str]] = [
            (("anime", "manga", "hoạt hình", "2d", "cartoon"), "anime style, vibrant colors, expressive framing, clean outlines"),
            (("quảng cáo", "product", "sản phẩm", "brand", "commercial"), "commercial advertising style, polished look, studio lighting, product-focused framing"),
            (("documentary", "phóng sự", "đời thực", "real life", "street"), "documentary style, handheld realism, authentic atmosphere, natural transitions"),
            (("retro", "vintage", "film", "analog"), "retro cinematic film look, warm tones, subtle grain, nostalgic mood"),
            (("sci-fi", "cyberpunk", "futuristic", "tương lai"), "cinematic sci-fi style, neon accents, atmospheric lighting, high contrast"),
            (("cute", "dễ thương", "kids", "trẻ em"), "soft playful style, bright friendly palette, smooth motion, wholesome mood"),
            (("drama", "kịch tính", "hành động", "action", "epic"), "dramatic cinematic style, dynamic camera movement, high energy composition"),
            (("realistic", "chân thực", "photoreal", "real"), "photorealistic style, natural lighting, lifelike textures, realistic motion"),
        ]
        for keys, style in rules:
            if any(k in p for k in keys):
                return style
        return "cinematic lighting, professional composition, smooth camera motion, high detail"

    def _bind_top_tooltips(
        self,
        *,
        lbl_provider: tk.Widget,
        cb_provider: tk.Widget,
        lbl_model: tk.Widget,
        cb_mode: tk.Widget,
        btn_reload_models: tk.Widget,
        lbl_prompt: tk.Widget,
        ent_prompt: tk.Widget,
        lbl_aspect: tk.Widget,
        cb_aspect: tk.Widget,
        lbl_duration: tk.Widget,
        cb_duration: tk.Widget,
        lbl_resolution: tk.Widget,
        cb_resolution: tk.Widget,
        lbl_count: tk.Widget,
        cb_count: tk.Widget,
        lbl_outputs: tk.Widget,
        cb_outputs: tk.Widget,
    ) -> None:
        _SimpleTooltip(lbl_provider, "Nhà cung cấp AI video. Bản này đang dùng Gemini/Veo.")
        _SimpleTooltip(cb_provider, "Provider hiện tại để gọi model video.")
        _SimpleTooltip(lbl_model, "Chọn model Veo phù hợp: bản nhanh để test, bản thường cho chất lượng ổn định.")
        _SimpleTooltip(self._cb_model, "Có thể chọn trong danh sách hoặc tự nhập model custom.")
        _SimpleTooltip(cb_mode, "Chọn kiểu tạo video: text, image, nội suy 2 frame, kéo dài video...")
        _SimpleTooltip(btn_reload_models, "Nạp lại danh sách model từ config/env.")
        _SimpleTooltip(lbl_prompt, "Mô tả nội dung video mong muốn. Viết rõ hành động, bối cảnh, phong cách.")
        _SimpleTooltip(ent_prompt, "Ví dụ: Cảnh biển hoàng hôn, máy quay pan chậm, ánh sáng cinematic.")
        _SimpleTooltip(lbl_aspect, "9:16 phù hợp Reels/TikTok, 16:9 phù hợp YouTube ngang.")
        _SimpleTooltip(cb_aspect, "Tỉ lệ khung hình của video output.")
        _SimpleTooltip(lbl_duration, "Thời lượng video sinh ra (giây).")
        _SimpleTooltip(cb_duration, "Thời lượng ngắn giúp test nhanh và giảm chi phí.")
        _SimpleTooltip(lbl_resolution, "Độ phân giải video đầu ra.")
        _SimpleTooltip(cb_resolution, "720p nhanh hơn, 1080p đẹp hơn nhưng có thể lâu hơn.")
        _SimpleTooltip(lbl_count, "Số biến thể prompt cho mỗi ý tưởng/dòng (dùng để đa dạng hóa nội dung).")
        _SimpleTooltip(cb_count, "Mỗi biến thể sẽ tạo prompt riêng; nên để 1 khi test cho ổn định.")
        _SimpleTooltip(lbl_outputs, "Số video xuất ra cho mỗi prompt (x1..x4).")
        _SimpleTooltip(cb_outputs, "Flow sẽ tạo nhiều biến thể video từ cùng một prompt.")

