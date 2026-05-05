"""Hộp thoại thêm / sửa job trong ``config/schedule_posts.json`` (lịch + AI theo job)."""

from __future__ import annotations

import tkinter as tk
from datetime import date, datetime, timezone
from tkinter import messagebox, ttk
from typing import Any, Literal
from zoneinfo import ZoneInfo

from src.utils.page_schedule import (
    format_once_schedule_24h,
    parse_cron_hh_mm,
    parse_date_only_yyyy_mm_dd,
    scheduler_tz,
)
from src.utils.pages_manager import PagesManager
from src.utils.schedule_batch_preview import build_schedule_by_daily_slots, page_post_style_for_post_type
from src.utils.schedule_job_content import (
    build_schedule_slot_hhmm,
    deserialize_job_schedule_for_ui,
    once_local_wall_to_utc_iso,
)
from src.utils.reel_thumbnail_choice import REEL_THUMBNAIL_METHOD1_FIRST_AUTO, normalize_reel_thumbnail_choice
from src.utils.schedule_posts_manager import SchedulePostJob, SchedulePostsManager


def _split_comma_list(s: str) -> list[str]:
    return [p.strip() for p in str(s).split(",") if p.strip()]

_AI_LANG_OPTIONS: tuple[str, ...] = (
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


def _coerce_schedule_delay(val: Any) -> int:
    try:
        return max(0, min(180, int(val)))
    except (TypeError, ValueError):
        return 0


def _normalize_daily_slots_csv(raw: str) -> str:
    """Chuẩn hoá chuỗi HH:MM,HH:MM từ JSON; rỗng / lỗi → chuỗi rỗng."""
    out: list[str] = []
    for token in str(raw).split(","):
        s = token.strip()
        if not s:
            continue
        parts = s.split(":")
        if len(parts) != 2:
            continue
        try:
            h, m = int(parts[0]), int(parts[1])
        except ValueError:
            continue
        if 0 <= h <= 23 and 0 <= m <= 59:
            out.append(f"{h:02d}:{m:02d}")
    return ",".join(sorted(set(out)))


def _parse_slot_base_local_date_hm(s: str) -> tuple[date | None, str | None]:
    """``YYYY-MM-DD HH:MM`` từ preview batch → (ngày, HH:MM)."""
    s = str(s).strip()
    if not s or s.upper() == "NOW":
        return (None, None)
    parts = s.split()
    if len(parts) < 2:
        return (None, None)
    d_part, t_part = parts[0], parts[1].strip()
    if len(t_part) != 5 or t_part[2] != ":":
        return (None, None)
    try:
        h, m = int(t_part[:2]), int(t_part[3:5])
    except ValueError:
        return (None, None)
    if not (0 <= h <= 23 and 0 <= m <= 59):
        return (None, None)
    try:
        d_only = parse_date_only_yyyy_mm_dd(d_part)
    except Exception:
        return (None, None)
    return (d_only, f"{h:02d}:{m:02d}")


class SchedulePostJobDialog:
    def __init__(
        self,
        parent: tk.Tk | tk.Toplevel,
        store: SchedulePostsManager,
        pages: PagesManager,
        owner_account_ids: list[str],
        *,
        title: str,
        initial: SchedulePostJob | None = None,
    ) -> None:
        self._store = store
        self._pages = pages
        self._owner_ids = [str(x).strip() for x in owner_account_ids if str(x).strip()]
        self._result: dict[str, Any] | None = None
        self._init = dict(initial) if initial else {}

        self._top = tk.Toplevel(parent)
        self._top.title(title)
        self._top.transient(parent)
        self._top.grab_set()
        self._top.geometry("680x900")

        outer = ttk.Frame(self._top, padding=8)
        outer.pack(fill=tk.BOTH, expand=True)
        canvas = tk.Canvas(outer, highlightthickness=0)
        sy = ttk.Scrollbar(outer, orient=tk.VERTICAL, command=canvas.yview)
        form = ttk.Frame(canvas, padding=6)
        form.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        canvas.create_window((0, 0), window=form, anchor="nw")
        canvas.configure(yscrollcommand=sy.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sy.pack(side=tk.RIGHT, fill=tk.Y)
        form.columnconfigure(1, weight=1)

        def row(r: int, label: str, w: ttk.Widget) -> int:
            ttk.Label(form, text=label).grid(row=r, column=0, sticky="nw", pady=3, padx=(0, 8))
            w.grid(row=r, column=1, sticky="ew", pady=3)
            return r + 1

        n = 0
        self._cb_acc = ttk.Combobox(form, values=self._owner_ids or [""], state="readonly", width=48)
        aid0 = str(self._init.get("account_id", "")).strip()
        if aid0 and aid0 in self._owner_ids:
            self._cb_acc.set(aid0)
        elif self._owner_ids:
            self._cb_acc.set(self._owner_ids[0])
        n = row(n, "Tài khoản *", self._cb_acc)

        self._cb_page = ttk.Combobox(form, state="readonly", width=48)
        n = row(n, "Page *", self._cb_page)
        self._cb_acc.bind("<<ComboboxSelected>>", lambda _e: self._refresh_page_combo())
        self._refresh_page_combo()

        pid0 = str(self._init.get("page_id", "")).strip()
        if pid0:
            vals = list(self._cb_page["values"])
            if pid0 in vals:
                self._cb_page.set(pid0)

        pt_vals = ("text", "image", "video", "text_image", "text_video", "reel")
        self._cb_pt = ttk.Combobox(form, values=pt_vals, state="readonly", width=46)
        pt = str(self._init.get("post_type", "text")).strip().lower()
        self._cb_pt.set(pt if pt in pt_vals else "text")
        self._cb_pt.bind("<<ComboboxSelected>>", lambda _e: self._sync_reel_thumbnail_visibility())
        n = row(n, "Loại bài (post_type) *", self._cb_pt)

        self._lbl_reel_thumb = ttk.Label(form, text="Reel thumbnail (wizard Meta, Cách 1)")
        self._cb_reel_thumb = ttk.Combobox(
            form,
            values=("Mặc định (Meta tự chọn)", "Cách 1 — Thumbnail auto đầu tiên"),
            state="readonly",
            width=44,
        )
        init_thumb = normalize_reel_thumbnail_choice(self._init.get("reel_thumbnail_choice"))
        self._cb_reel_thumb.set(
            "Cách 1 — Thumbnail auto đầu tiên" if init_thumb == REEL_THUMBNAIL_METHOD1_FIRST_AUTO else "Mặc định (Meta tự chọn)"
        )
        self._lbl_reel_thumb.grid(row=n, column=0, sticky="nw", pady=3, padx=(0, 8))
        self._cb_reel_thumb.grid(row=n, column=1, sticky="ew", pady=3)
        n += 1

        sch_fr = ttk.LabelFrame(form, text="Lịch đăng", padding=8)
        sch_fr.grid(row=n, column=0, columnspan=2, sticky="ew", pady=(8, 4))
        sch_fr.columnconfigure(1, weight=1)
        n += 1

        _kind_ui, d0, h24, mi = deserialize_job_schedule_for_ui(self._init)
        rec_ll = str(self._init.get("schedule_recurrence", "")).strip().lower()
        slot_st = str(self._init.get("schedule_slot", "")).strip()
        raw_sa = str(self._init.get("scheduled_at", "")).strip()
        tz_init = (self._init.get("timezone") or "").strip() or getattr(scheduler_tz(), "key", str(scheduler_tz()))
        slots_csv = str(self._init.get("schedule_daily_slots", "")).strip()
        slot_base = str(self._init.get("slot_base_local", "")).strip()
        created_batch = str(self._init.get("created_by", "")).strip() == "gui_batch"
        delay_min_init = _coerce_schedule_delay(self._init.get("schedule_delay_min"))
        delay_max_init = _coerce_schedule_delay(self._init.get("schedule_delay_max"))
        if (
            not slots_csv
            and delay_min_init == 0
            and delay_max_init == 0
            and "schedule_delay_applied_min" in self._init
        ):
            da = _coerce_schedule_delay(self._init.get("schedule_delay_applied_min"))
            delay_min_init = delay_max_init = da

        def _local_date_from_sched_iso(raw: str, tz_name: str, fallback: date) -> date:
            tz_use = scheduler_tz()
            try:
                if (tz_name or "").strip():
                    tz_use = ZoneInfo(tz_name.strip())
            except Exception:
                pass
            s = (raw or "").strip()
            if not s:
                return fallback
            s = s.replace("Z", "+00:00")
            dtu = datetime.fromisoformat(s)
            if dtu.tzinfo is None:
                dtu = dtu.replace(tzinfo=timezone.utc)
            return dtu.astimezone(tz_use).date()

        start_d_init = _local_date_from_sched_iso(raw_sa, tz_init, d0)
        rule_init = "Một lần"
        slots_init = "04:30,10:15,22:30"
        h_once, m_once = h24, mi

        if slots_csv:
            norm = _normalize_daily_slots_csv(slots_csv)
            if norm:
                rule_init = "Theo khung giờ mỗi ngày"
                slots_init = norm
                ssd = str(self._init.get("schedule_start_date", "")).strip()
                if ssd:
                    try:
                        start_d_init = parse_date_only_yyyy_mm_dd(ssd)
                    except Exception:
                        pass
                tz_job = (self._init.get("timezone") or "").strip()
                if tz_job:
                    tz_init = tz_job
        elif rec_ll == "daily" and slot_st:
            try:
                sh, sm = parse_cron_hh_mm(slot_st)
                slots_init = f"{sh:02d}:{sm:02d}"
                h_once, m_once = sh, sm
            except Exception:
                slots_init = "09:00"
                h_once, m_once = h24, mi
            rule_init = "Theo khung giờ mỗi ngày"
        elif created_batch and slot_base:
            bd, hm = _parse_slot_base_local_date_hm(slot_base)
            if bd is not None and hm is not None:
                rule_init = "Theo khung giờ mỗi ngày"
                slots_init = hm
                start_d_init = bd
                tz_job = (self._init.get("timezone") or "").strip()
                if tz_job:
                    tz_init = tz_job

        sr = 0
        self._sched_rule = ttk.Combobox(
            sch_fr,
            values=("Đăng ngay", "Một lần", "Theo khung giờ mỗi ngày"),
            state="readonly",
            width=44,
        )
        self._sched_rule.set(rule_init)
        self._sched_rule.bind("<<ComboboxSelected>>", lambda _e: self._on_schedule_rule_changed())
        ttk.Label(sch_fr, text="Kiểu lịch").grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=2)
        self._sched_rule.grid(row=sr, column=1, sticky="w", pady=2)
        sr += 1

        self._lbl_start_date = ttk.Label(sch_fr, text="Ngày bắt đầu (YYYY-MM-DD)")
        self._lbl_start_date.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        self._e_start_date = ttk.Entry(sch_fr, width=14)
        self._e_start_date.insert(0, start_d_init.strftime("%Y-%m-%d"))
        self._e_start_date.grid(row=sr, column=1, sticky="w", pady=4)
        sr += 1

        self._lbl_once_time = ttk.Label(sch_fr, text="Giờ/phút (cho kiểu Một lần)")
        self._lbl_once_time.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        self._sched_once_time_frame = ttk.Frame(sch_fr)
        self._sched_once_time_frame.grid(row=sr, column=1, sticky="w", pady=4)
        ttk.Label(self._sched_once_time_frame, text="Giờ:").pack(side=tk.LEFT)
        self._sp_hour = ttk.Spinbox(self._sched_once_time_frame, from_=0, to=23, width=4, format="%.0f")
        self._sp_hour.set(str(h_once))
        self._sp_hour.pack(side=tk.LEFT, padx=4)
        ttk.Label(self._sched_once_time_frame, text="Phút:").pack(side=tk.LEFT)
        self._sp_min = ttk.Spinbox(self._sched_once_time_frame, from_=0, to=59, width=4, format="%.0f")
        self._sp_min.set(str(m_once))
        self._sp_min.pack(side=tk.LEFT, padx=4)
        sr += 1

        self._lbl_daily_slots = ttk.Label(sch_fr, text="Khung giờ/ngày (HH:MM, phẩy)")
        self._lbl_daily_slots.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        self._e_daily_slots = ttk.Entry(sch_fr, width=32)
        self._e_daily_slots.insert(0, slots_init)
        self._e_daily_slots.grid(row=sr, column=1, sticky="w", pady=4)
        sr += 1

        self._lbl_delay_min = ttk.Label(sch_fr, text="Delay tối thiểu (phút)")
        self._lbl_delay_min.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        self._delay_min = ttk.Spinbox(sch_fr, from_=0, to=180, width=6)
        self._delay_min.insert(0, str(delay_min_init))
        self._delay_min.grid(row=sr, column=1, sticky="w", pady=4)
        sr += 1

        self._lbl_delay_max = ttk.Label(sch_fr, text="Delay tối đa (phút)")
        self._lbl_delay_max.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        self._delay_max = ttk.Spinbox(sch_fr, from_=0, to=180, width=6)
        self._delay_max.insert(0, str(delay_max_init))
        self._delay_max.grid(row=sr, column=1, sticky="w", pady=4)
        sr += 1

        self._lbl_timezone = ttk.Label(sch_fr, text="Múi giờ")
        self._lbl_timezone.grid(row=sr, column=0, sticky="nw", padx=(0, 8), pady=4)
        self._e_timezone = ttk.Entry(sch_fr, width=30)
        self._e_timezone.insert(0, str(tz_init))
        self._e_timezone.grid(row=sr, column=1, sticky="w", pady=4)
        sr += 1

        self._lbl_schedule_hint = ttk.Label(
            sch_fr,
            text="Giống block C «Thêm batch job»: Đăng ngay / Một lần / Theo khung giờ mỗi ngày. Một khung giờ + lặp hàng ngày: sau đăng thành công job tự hẹn lần sau (theo schedule_slot, SCHEDULER_TZ).",
            foreground="gray",
            font=("Segoe UI", 8),
            wraplength=560,
        )
        self._lbl_schedule_hint.grid(row=sr, column=0, columnspan=2, sticky="w", pady=(4, 0))
        self._default_label_fg = self._lbl_daily_slots.cget("foreground")

        self._on_schedule_rule_changed()
        self._sync_reel_thumbnail_visibility()

        ttk.Label(
            form,
            text="Lưu: kiểu lịch + ngày/giờ hoặc khung giờ → scheduled_at (UTC). Nhiều khung giờ/ngày: chỉ lần chạy đầu được lưu vào scheduled_at (không tự lặp theo từng slot).",
            foreground="gray",
            font=("Segoe UI", 8),
            wraplength=560,
        ).grid(row=n, column=0, columnspan=2, sticky="w")
        n += 1

        ai_fr = ttk.LabelFrame(form, text="AI (theo job — để trống nội dung = dùng AI)", padding=8)
        ai_fr.grid(row=n, column=0, columnspan=2, sticky="ew", pady=(6, 4))
        ai_fr.columnconfigure(1, weight=1)
        n += 1
        ttk.Label(ai_fr, text="Tiêu đề nội bộ").grid(row=0, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._e_title = ttk.Entry(ai_fr, width=48)
        self._e_title.insert(0, str(self._init.get("title", "")))
        self._e_title.grid(row=0, column=1, sticky="ew", pady=2)
        ttk.Label(ai_fr, text="Nội dung (trống = AI)").grid(row=1, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._txt_body = tk.Text(ai_fr, height=5, width=48, wrap="word", font=("Segoe UI", 9))
        self._txt_body.grid(row=1, column=1, sticky="ew", pady=2)
        self._txt_body.insert("1.0", str(self._init.get("content", "")))
        ttk.Label(ai_fr, text="ai_topic").grid(row=2, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._e_ai_topic = ttk.Entry(ai_fr, width=48)
        self._e_ai_topic.insert(0, str(self._init.get("ai_topic", "")))
        self._e_ai_topic.grid(row=2, column=1, sticky="ew", pady=2)
        ttk.Label(ai_fr, text="ai_content_style").grid(row=3, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._e_ai_style = ttk.Entry(ai_fr, width=48)
        self._e_ai_style.insert(0, str(self._init.get("ai_content_style", "")))
        self._e_ai_style.grid(row=3, column=1, sticky="ew", pady=2)
        ttk.Label(ai_fr, text="ai_language").grid(row=4, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._cb_ai_lang = ttk.Combobox(ai_fr, values=_AI_LANG_OPTIONS, state="readonly", width=46)
        self._cb_ai_lang.set(str(self._init.get("ai_language", "")).strip() or "Tiếng Việt")
        self._cb_ai_lang.grid(row=4, column=1, sticky="w", pady=2)
        ttk.Label(ai_fr, text="AI provider (text/image)").grid(row=5, column=0, sticky="nw", pady=2, padx=(0, 8))
        pfr = ttk.Frame(ai_fr)
        pfr.grid(row=5, column=1, sticky="w", pady=2)
        self._cb_ai_provider_text = ttk.Combobox(pfr, values=("gemini", "openai"), state="readonly", width=12)
        self._cb_ai_provider_text.set(str(self._init.get("ai_provider_text", "gemini")).strip().lower() or "gemini")
        self._cb_ai_provider_text.pack(side=tk.LEFT, padx=(0, 6))
        self._cb_ai_provider_image = ttk.Combobox(
            pfr,
            values=("gemini", "openai", "nanobanana"),
            state="readonly",
            width=12,
        )
        self._cb_ai_provider_image.set(str(self._init.get("ai_provider_image", "gemini")).strip().lower() or "gemini")
        self._cb_ai_provider_image.pack(side=tk.LEFT)
        self._cb_ai_provider_image.bind("<<ComboboxSelected>>", lambda _e: self._sync_ai_image_model_options())
        ttk.Label(ai_fr, text="AI model (text/image)").grid(row=6, column=0, sticky="nw", pady=2, padx=(0, 8))
        mfr = ttk.Frame(ai_fr)
        mfr.grid(row=6, column=1, sticky="ew", pady=2)
        self._e_ai_model_text = ttk.Combobox(
            mfr,
            values=("auto", "gpt-4o-mini", "gpt-4.1-mini", "gemini-2.5-flash"),
            width=20,
        )
        self._e_ai_model_text.set(str(self._init.get("ai_model_text", "")).strip() or "auto")
        self._e_ai_model_text.pack(side=tk.LEFT, padx=(0, 6))
        self._e_ai_model_image = ttk.Combobox(
            mfr,
            values=("auto", "gpt-image-2", "gpt-image-1", "imagen-3.0-generate-002"),
            width=20,
        )
        self._e_ai_model_image.set(str(self._init.get("ai_model_image", "")).strip() or "auto")
        self._e_ai_model_image.pack(side=tk.LEFT)
        self._sync_ai_image_model_options()
        ttk.Label(ai_fr, text="job_post_image_path").grid(row=7, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._e_job_img = ttk.Entry(ai_fr, width=48)
        self._e_job_img.insert(0, str(self._init.get("job_post_image_path", "")))
        self._e_job_img.grid(row=7, column=1, sticky="ew", pady=2)
        ttk.Label(ai_fr, text="video_path (cho reel/video)").grid(row=8, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._e_video_path = ttk.Entry(ai_fr, width=48)
        self._e_video_path.insert(0, str(self._init.get("video_path", "")))
        self._e_video_path.grid(row=8, column=1, sticky="ew", pady=2)
        ttk.Label(ai_fr, text="hashtags (phẩy)").grid(row=9, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._e_hashtags = ttk.Entry(ai_fr, width=48)
        ht = self._init.get("hashtags") or []
        self._e_hashtags.insert(0, ", ".join(str(x) for x in ht) if isinstance(ht, list) else "")
        self._e_hashtags.grid(row=9, column=1, sticky="ew", pady=2)
        ttk.Label(ai_fr, text="cta").grid(row=10, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._e_cta = ttk.Entry(ai_fr, width=48)
        self._e_cta.insert(0, str(self._init.get("cta", "")))
        self._e_cta.grid(row=10, column=1, sticky="ew", pady=2)

        cfg_fr = ttk.LabelFrame(form, text="AI nâng cao (ai_config)", padding=8)
        cfg_fr.grid(row=n, column=0, columnspan=2, sticky="ew", pady=(4, 4))
        cfg_fr.columnconfigure(1, weight=1)
        n += 1
        cfg = self._init.get("ai_config") if isinstance(self._init.get("ai_config"), dict) else {}
        cr = 0
        ttk.Label(cfg_fr, text="brand_voice").grid(row=cr, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._txt_brand = tk.Text(cfg_fr, height=3, width=50, wrap="word", font=("Segoe UI", 9))
        self._txt_brand.insert("1.0", str(cfg.get("brand_voice", "")))
        self._txt_brand.grid(row=cr, column=1, sticky="ew", pady=2)
        cr += 1
        ttk.Label(cfg_fr, text="target_audience").grid(row=cr, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._e_aud = ttk.Entry(cfg_fr, width=48)
        self._e_aud.insert(0, str(cfg.get("target_audience", "")))
        self._e_aud.grid(row=cr, column=1, sticky="ew", pady=2)
        cr += 1
        ttk.Label(cfg_fr, text="content_pillars (phẩy)").grid(row=cr, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._e_pillars = ttk.Entry(cfg_fr, width=48)
        pl = cfg.get("content_pillars") or []
        self._e_pillars.insert(0, ", ".join(str(x) for x in pl) if isinstance(pl, list) else "")
        self._e_pillars.grid(row=cr, column=1, sticky="ew", pady=2)
        cr += 1
        ttk.Label(cfg_fr, text="cfg hashtags (phẩy)").grid(row=cr, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._e_cfg_ht = ttk.Entry(cfg_fr, width=48)
        ch = cfg.get("hashtags") or []
        self._e_cfg_ht.insert(0, ", ".join(str(x) for x in ch) if isinstance(ch, list) else "")
        self._e_cfg_ht.grid(row=cr, column=1, sticky="ew", pady=2)
        cr += 1
        ttk.Label(cfg_fr, text="image_style").grid(row=cr, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._e_imgst = ttk.Entry(cfg_fr, width=48)
        self._e_imgst.insert(0, str(cfg.get("image_style", "")))
        self._e_imgst.grid(row=cr, column=1, sticky="ew", pady=2)
        cr += 1
        ttk.Label(cfg_fr, text="avoid_keywords (phẩy)").grid(row=cr, column=0, sticky="nw", pady=2, padx=(0, 8))
        self._e_avoid = ttk.Entry(cfg_fr, width=48)
        av = cfg.get("avoid_keywords") or []
        self._e_avoid.insert(0, ", ".join(str(x) for x in av) if isinstance(av, list) else "")
        self._e_avoid.grid(row=cr, column=1, sticky="ew", pady=2)
        cr += 1
        self._var_auto_img = tk.BooleanVar(value=bool(cfg.get("auto_generate_image")))
        self._var_auto_cap = tk.BooleanVar(value=bool(cfg.get("auto_generate_caption")))
        ofr = ttk.Frame(cfg_fr)
        ofr.grid(row=cr, column=0, columnspan=2, sticky="w", pady=(4, 0))
        ttk.Checkbutton(ofr, text="auto_generate_image", variable=self._var_auto_img).pack(side=tk.LEFT, padx=(0, 12))
        ttk.Checkbutton(ofr, text="auto_generate_caption", variable=self._var_auto_cap).pack(side=tk.LEFT)

        self._e_draft = ttk.Entry(form, width=50)
        self._e_draft.insert(0, str(self._init.get("draft_id", "")))
        n = row(n, "draft_id (tùy chọn)", self._e_draft)

        btnf = ttk.Frame(self._top, padding=8)
        btnf.pack(fill=tk.X)
        ttk.Button(btnf, text="Hủy", command=self._cancel).pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Button(btnf, text="Lưu", command=self._ok).pack(side=tk.RIGHT)

        self._top.protocol("WM_DELETE_WINDOW", self._cancel)
        self._top.update_idletasks()
        canvas.configure(scrollregion=canvas.bbox("all"))
        self._top.wait_window()

    def _sync_reel_thumbnail_visibility(self) -> None:
        pt = self._cb_pt.get().strip().lower()
        show = pt in ("video", "text_video", "reel")
        if show:
            self._lbl_reel_thumb.grid()
            self._cb_reel_thumb.grid()
        else:
            self._lbl_reel_thumb.grid_remove()
            self._cb_reel_thumb.grid_remove()

    def _on_schedule_rule_changed(self) -> None:
        rule = self._schedule_rule_key()
        if rule == "immediate":
            self._lbl_start_date.grid_remove()
            self._e_start_date.grid_remove()
            self._lbl_once_time.grid_remove()
            self._sched_once_time_frame.grid_remove()
            self._lbl_daily_slots.grid_remove()
            self._e_daily_slots.grid_remove()
            self._lbl_delay_min.grid_remove()
            self._delay_min.grid_remove()
            self._lbl_delay_max.grid_remove()
            self._delay_max.grid_remove()
            self._lbl_timezone.grid_remove()
            self._e_timezone.grid_remove()
        elif rule == "once":
            self._lbl_start_date.grid()
            self._e_start_date.grid()
            self._lbl_once_time.grid()
            self._sched_once_time_frame.grid()
            self._lbl_daily_slots.grid_remove()
            self._e_daily_slots.grid_remove()
            self._lbl_delay_min.grid_remove()
            self._delay_min.grid_remove()
            self._lbl_delay_max.grid_remove()
            self._delay_max.grid_remove()
            self._lbl_timezone.grid_remove()
            self._e_timezone.grid_remove()
        else:
            self._lbl_start_date.grid()
            self._e_start_date.grid()
            self._lbl_once_time.grid_remove()
            self._sched_once_time_frame.grid_remove()
            self._lbl_daily_slots.grid()
            self._e_daily_slots.grid()
            self._lbl_delay_min.grid()
            self._delay_min.grid()
            self._lbl_delay_max.grid()
            self._delay_max.grid()
            self._lbl_timezone.grid()
            self._e_timezone.grid()

    def _schedule_rule_key(self) -> Literal["immediate", "once", "daily_slots"]:
        s = self._sched_rule.get()
        if "Đăng ngay" in s:
            return "immediate"
        if "Theo khung giờ" in s:
            return "daily_slots"
        return "once"

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
        return sorted(set(out))

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

    def _refresh_page_combo(self) -> None:
        aid = self._cb_acc.get().strip()
        opts: list[str] = []
        try:
            for p in self._pages.load_all():
                if str(p.get("account_id", "")).strip() == aid:
                    pid = str(p.get("id", "")).strip()
                    if pid:
                        opts.append(pid)
        except Exception:  # noqa: BLE001
            pass
        self._cb_page.configure(values=opts or [""])
        if opts:
            cur = self._cb_page.get().strip()
            if cur not in opts:
                self._cb_page.set(opts[0])
        else:
            self._cb_page.set("")

    @property
    def result(self) -> dict[str, Any] | None:
        return self._result

    def _ok(self) -> None:
        aid = self._cb_acc.get().strip()
        pid = self._cb_page.get().strip()
        if not aid or not pid:
            messagebox.showerror("Thiếu dữ liệu", "Chọn tài khoản và Page.", parent=self._top)
            return
        pt = self._cb_pt.get().strip().lower()
        self._clear_schedule_validation_marks()
        rule = self._schedule_rule_key()
        rec = ""
        slot = ""
        sched_iso = ""
        tz_out = getattr(scheduler_tz(), "key", str(scheduler_tz()))
        slots_list: list[str] | None = None
        delay_pair: tuple[int, int] | None = None

        try:
            if rule == "immediate":
                sched_iso = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()
            elif rule == "once":
                try:
                    h24 = int(str(self._sp_hour.get()).strip())
                    mi = int(str(self._sp_min.get()).strip())
                except ValueError:
                    messagebox.showerror("Lỗi", "Giờ và phút phải là số.", parent=self._top)
                    return
                if not (0 <= h24 <= 23 and 0 <= mi <= 59):
                    messagebox.showerror("Lỗi", "Giờ 0–23, phút 0–59.", parent=self._top)
                    return
                slot = build_schedule_slot_hhmm(h24, mi)
                rec = "once"
                d = parse_date_only_yyyy_mm_dd(self._e_start_date.get())
                once_s = format_once_schedule_24h(d, h24, mi)
                sched_iso = once_local_wall_to_utc_iso(once_s)
            else:
                tz_out = self._resolved_timezone_name()
                d = parse_date_only_yyyy_mm_dd(self._e_start_date.get())
                slots_list = self._parse_daily_slot_strings()
                dmin = self._delay_min_value()
                dmax = self._delay_max_value()
                delay_pair = (dmin, dmax)
                planned = build_schedule_by_daily_slots(
                    start_date=d,
                    time_slots=slots_list,
                    job_count=1,
                    delay_min_minutes=dmin,
                    delay_max_minutes=dmax,
                    timezone_name=tz_out,
                )
                sched_iso = str(planned[0]["scheduled_at"])
                if len(slots_list) == 1:
                    rec = "daily"
                    slot = slots_list[0]
                else:
                    rec = ""
                    slot = ""
        except ValueError as exc:
            messagebox.showerror("Lịch", str(exc), parent=self._top)
            return
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Lịch", str(exc), parent=self._top)
            return

        pps = page_post_style_for_post_type(pt)

        ai_cfg: dict[str, Any] = {
            "brand_voice": self._txt_brand.get("1.0", tk.END).strip(),
            "target_audience": self._e_aud.get().strip(),
            "content_pillars": _split_comma_list(self._e_pillars.get()),
            "hashtags": _split_comma_list(self._e_cfg_ht.get()),
            "image_style": self._e_imgst.get().strip(),
            "avoid_keywords": _split_comma_list(self._e_avoid.get()),
            "auto_generate_image": bool(self._var_auto_img.get()),
            "auto_generate_caption": bool(self._var_auto_cap.get()),
        }

        body = self._txt_body.get("1.0", tk.END).strip()
        draft = self._e_draft.get().strip()
        row: dict[str, Any] = {
            "account_id": aid,
            "page_id": pid,
            "post_type": pt,
            "title": self._e_title.get().strip(),
            "content": body,
            "scheduled_at": sched_iso,
            "status": "pending",
            "created_by": str(self._init.get("created_by", "") or "gui").strip() or "gui",
            "page_post_style": pps,
            "schedule_recurrence": rec,
            "schedule_slot": slot,
            "timezone": tz_out,
            "hashtags": _split_comma_list(self._e_hashtags.get()),
            "cta": self._e_cta.get().strip(),
        }
        if rule == "daily_slots" and slots_list and delay_pair is not None:
            row["schedule_daily_slots"] = ",".join(slots_list)
            row["schedule_delay_min"] = delay_pair[0]
            row["schedule_delay_max"] = delay_pair[1]
            row["schedule_start_date"] = self._e_start_date.get().strip()
        if self._e_ai_topic.get().strip():
            row["ai_topic"] = self._e_ai_topic.get().strip()
        ai_lang = self._cb_ai_lang.get().strip() or "Tiếng Việt"
        row["ai_language"] = ai_lang
        row["ai_provider_text"] = self._cb_ai_provider_text.get().strip().lower() or "gemini"
        row["ai_provider_image"] = self._cb_ai_provider_image.get().strip().lower() or "gemini"
        m_text = self._e_ai_model_text.get().strip()
        m_image = self._e_ai_model_image.get().strip()
        if m_text and m_text.lower() != "auto":
            row["ai_model_text"] = m_text
        if m_image and m_image.lower() != "auto":
            row["ai_model_image"] = m_image
        ai_style_raw = self._e_ai_style.get().strip()
        ai_lang_instr = f"Ngôn ngữ bắt buộc: {ai_lang}. Không trộn ngôn ngữ khác."
        if ai_style_raw:
            row["ai_content_style"] = (
                ai_style_raw
                if ai_lang_instr.lower() in ai_style_raw.lower()
                else f"{ai_style_raw} | {ai_lang_instr}"
            )
        else:
            row["ai_content_style"] = ai_lang_instr
        if self._e_job_img.get().strip():
            row["job_post_image_path"] = self._e_job_img.get().strip()
        video_path = self._e_video_path.get().strip()
        if video_path:
            row["video_path"] = video_path
            if pt in ("video", "text_video", "reel"):
                row["media_files"] = [video_path]
        if pt == "reel":
            purl = str(self._init.get("page_url") or "").strip()
            if not purl:
                try:
                    for p in self._pages.load_all():
                        if str(p.get("id", "")).strip() == pid:
                            purl = str(p.get("page_url", "")).strip()
                            break
                except Exception:
                    purl = ""
            if purl:
                row["page_url"] = purl
        if pt in ("video", "text_video", "reel"):
            if "Cách 1" in self._cb_reel_thumb.get():
                row["reel_thumbnail_choice"] = REEL_THUMBNAIL_METHOD1_FIRST_AUTO
        else:
            row.pop("reel_thumbnail_choice", None)
        row["ai_config"] = ai_cfg
        if draft:
            row["draft_id"] = draft
        if self._init.get("id"):
            row["id"] = str(self._init["id"])
        if self._init.get("created_at"):
            row["created_at"] = str(self._init["created_at"])
        if self._init.get("retry_count") is not None:
            row["retry_count"] = int(self._init.get("retry_count", 0))
        if self._init.get("media_files"):
            row["media_files"] = list(self._init["media_files"])
        if pt == "reel":
            if not str(row.get("video_path", "")).strip():
                messagebox.showerror("Lỗi", "Job reel bắt buộc có video_path.", parent=self._top)
                return
            if not str(row.get("page_url", "")).strip():
                messagebox.showerror("Lỗi", "Job reel bắt buộc có page_url hợp lệ của Page.", parent=self._top)
                return
        hb_init = str(self._init.get("hide_browser") or "").strip().lower()
        if hb_init in ("hide", "show"):
            row["hide_browser"] = hb_init
        try:
            self._store.validate_record(row)
        except ValueError as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self._top)
            return
        self._result = row
        self._top.grab_release()
        self._top.destroy()

    def _sync_ai_image_model_options(self) -> None:
        """Đổi gợi ý model ảnh theo provider để giảm nhập sai."""
        p = self._cb_ai_provider_image.get().strip().lower() or "gemini"
        if p == "openai":
            choices = ("auto", "gpt-image-2", "gpt-image-1")
        elif p == "nanobanana":
            choices = ("auto", "nano-banana-pro")
        else:
            choices = ("auto", "imagen-3.0-generate-002", "imagen-4.0-generate-preview")
        self._e_ai_model_image.configure(values=choices)
        cur = self._e_ai_model_image.get().strip()
        if not cur or cur.lower() == "auto":
            self._e_ai_model_image.set("auto")
            return
        if cur not in choices:
            self._e_ai_model_image.configure(values=(cur, *choices))

    def _cancel(self) -> None:
        self._result = None
        try:
            self._top.grab_release()
        except tk.TclError:
            pass
        self._top.destroy()
