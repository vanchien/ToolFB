from __future__ import annotations

import json
import os
import random
import re
import time
from pathlib import Path
from typing import Any

from loguru import logger
from playwright.sync_api import BrowserContext, Page, sync_playwright
from playwright_stealth import Stealth

from src.services.google_flow_prompt_builder import (
    build_google_flow_text_to_video_prompt,
    build_or_load_character_profile,
    build_start_end_scene_plan,
    normalize_flow_video_input,
)
from src.services.google_flow_video_store import GoogleFlowVideoStore, ensure_google_flow_layout, now_iso
from src.services.google_flow_veo_generate import (
    GOOGLE_FLOW_URL,
    check_google_flow_login,
    click_generate_and_wait_started,
    find_flow_generate_button,
    find_flow_prompt_box,
    has_prompt_box,
    input_prompt_to_flow,
    open_or_create_flow_project,
    run_google_flow_create_project_and_generate,
    select_text_to_video_mode_if_needed,
    split_text_chunks,
    wait_flow_generation_done,
    wait_generate_button_ready,
)

# Tương thích tên cũ
split_prompt_chunks_by_lines = split_text_chunks
FLOW_STATUSES: tuple[str, ...] = (
    "pending",
    "running",
    "open_browser",
    "open_flow",
    "check_login",
    "prepare_prompt",
    "open_text_to_video",
    "input_prompt",
    "apply_settings",
    "click_generate",
    "click_start",
    "waiting_generation",
    "download_video",
    "completed",
    "failed",
    "need_manual_check",
    "cancelled",
)


def validate_flow_veo_job(job: dict[str, Any]) -> None:
    """
    Kiểm tra job trước khi mở browser (chỉ UI Flow, không API).
    Hỗ trợ khóa ``browser`` (spec mới) và ``browser_profile`` (tương thích cũ).
    """
    if not str(job.get("idea", "")).strip() and not str(job.get("final_prompt", "")).strip():
        raise ValueError("Thiếu idea hoặc final_prompt.")
    merged = {**dict(job.get("browser_profile") or {}), **dict(job.get("browser") or {})}
    profile_path = str(merged.get("profile_path", "")).strip()
    exe = str(merged.get("browser_exe_path", "")).strip()
    dl = str(job.get("download_dir", "")).strip()
    if not profile_path:
        raise ValueError("Thiếu profile_path trong browser / browser_profile.")
    if not dl:
        raise ValueError("Thiếu download_dir.")
    allow_no_exe = os.environ.get("GOOGLE_FLOW_ALLOW_MISSING_BROWSER_EXE", "").strip().lower() in {"1", "true", "yes", "on"}
    if not exe and not allow_no_exe:
        raise ValueError(
            "Thiếu browser_exe_path trong browser / browser_profile. "
            "Đặt GOOGLE_FLOW_ALLOW_MISSING_BROWSER_EXE=1 nếu muốn dùng Chromium mặc định của Playwright."
        )


class GoogleFlowVeoRunner:
    """
    Runner tạo video bằng Google Flow / Veo 3 qua browser automation (không API key).
    """

    def __init__(self) -> None:
        self._store = GoogleFlowVideoStore()
        self._paths = ensure_google_flow_layout()
        self._pw = None
        # Tăng mặc định để tránh miss-click khi Flow render chậm.
        self._settings_delay_ms = int(float(os.environ.get("FLOW_SETTINGS_DELAY_MS", "1300")))

    def run(self, job: dict[str, Any]) -> dict[str, Any]:
        """Chạy toàn bộ tiến trình tạo video một job."""
        job_id = str(job.get("id", "")).strip()
        if not job_id:
            raise ValueError("Job thiếu id.")
        job = dict(job)
        if not str(job.get("download_dir", "")).strip():
            job["download_dir"] = str(self._paths["downloads"])
        try:
            validate_flow_veo_job(job)
        except ValueError as exc:
            return self._mark(job_id, status="failed", error_message=str(exc), completed_at=now_iso())

        self._mark(job_id, status="running", started_at=now_iso(), error_message="")
        max_tries = max(1, int(job.get("max_retry") or 3))
        context: BrowserContext | None = None
        try:
            normalized = normalize_flow_video_input(job)
            self._mark(job_id, status="prepare_prompt")
            final_prompt, scene_plan, profile = self._ensure_prompt_parts(job=job, normalized=normalized)
            self._mark(job_id, final_prompt=final_prompt, scene_plan=scene_plan, character_profile=profile)

            self._mark(job_id, status="open_browser")
            context = self.open_browser(job)
            page = context.pages[0] if context.pages else context.new_page()

            last_exc: Exception | None = None
            for attempt in range(max_tries):
                try:
                    self._mark(job_id, retry_count=attempt, status="open_flow")
                    self.open_flow(page)

                    self._mark(job_id, status="check_login")
                    if not self.check_login(page):
                        self.save_error_screenshot(page, job_id, "need_login")
                        return self._mark(
                            job_id,
                            status="need_manual_check",
                            error_message="Profile Google Flow chưa đăng nhập. Vui lòng đăng nhập thủ công rồi chạy lại.",
                            completed_at=now_iso(),
                        )

                    open_or_create_flow_project(page)
                    self._mark(job_id, status="open_text_to_video")
                    select_text_to_video_mode_if_needed(page)

                    self._mark(job_id, status="apply_settings")
                    self.apply_settings(page, settings=normalized["settings"])
                    # Chỉ nhập prompt sau khi đã áp xong toàn bộ setting.
                    self._mark(job_id, status="input_prompt")
                    self.input_prompt_like_human(page, final_prompt=final_prompt)

                    self._mark(job_id, status="click_generate")
                    self.click_generate(page)

                    self._mark(job_id, status="waiting_generation")
                    wait_flow_generation_done(page)

                    self._mark(job_id, status="download_video")
                    output = self.download_video(page, job=job)
                    return self._mark(
                        job_id,
                        status="completed",
                        output_files=[output],
                        completed_at=now_iso(),
                        error_message="",
                    )
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    logger.warning("Google Flow job {} lần thử {}/{}: {}", job_id, attempt + 1, max_tries, exc)
                    try:
                        self.save_error_screenshot(page, job_id, f"attempt_{attempt + 1}")
                    except Exception:
                        pass
                    self._mark(job_id, retry_count=attempt + 1, error_message=str(exc))
                    if attempt >= max_tries - 1:
                        raise
                    time.sleep(random.uniform(2.0, 4.5))

            raise RuntimeError(str(last_exc) if last_exc else "Google Flow: lỗi không xác định sau retry.")
        except Exception as exc:  # noqa: BLE001
            logger.exception("Google Flow runner lỗi: {}", exc)
            msg = str(exc)
            patch: dict[str, Any] = {"status": "failed", "error_message": msg, "completed_at": now_iso()}
            return self._mark(job_id, **patch)
        finally:
            try:
                if context is not None:
                    context.close()
            except Exception:
                pass
            try:
                if self._pw is not None:
                    self._pw.stop()
            except Exception:
                pass
            self._pw = None

    def open_browser(self, job: dict[str, Any]) -> BrowserContext:
        """Mở persistent context — profile Flow riêng, luôn headless=False cho ổn định Flow."""
        merged = {**dict(job.get("browser_profile") or {}), **dict(job.get("browser") or {})}
        profile_path = str(merged.get("profile_path", "")).strip() or str(self._paths["temp"] / "google_flow_profile")
        exe_path = str(merged.get("browser_exe_path", "")).strip() or None
        download_dir = str(job.get("download_dir", self._paths["downloads"])).strip()
        Path(download_dir).mkdir(parents=True, exist_ok=True)

        self._pw = sync_playwright().start()
        browser_type = str(merged.get("browser_type", "chrome")).strip().lower()
        engine = self._pw.chromium if browser_type in {"chrome", "chromium"} else self._pw.firefox
        launch_kwargs: dict[str, Any] = {}
        if browser_type in {"chrome", "chromium"} and not exe_path:
            ch = str(os.environ.get("GOOGLE_FLOW_CHROMIUM_CHANNEL", "chrome")).strip().lower()
            if ch and ch not in {"0", "false", "off", "bundled", "playwright", "chromium"}:
                launch_kwargs["channel"] = ch
        ctx = engine.launch_persistent_context(
            user_data_dir=profile_path,
            executable_path=exe_path,
            headless=False,
            accept_downloads=True,
            downloads_path=download_dir,
            viewport={"width": 1280, "height": 900},
            locale="vi-VN",
            **launch_kwargs,
        )
        try:
            stealth = Stealth()
            stealth.apply_stealth_sync(ctx)
        except Exception:
            pass
        return ctx

    def open_flow(self, page: Page) -> None:
        """Mở Google Flow."""
        page.goto(GOOGLE_FLOW_URL, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(3000)

    def check_login(self, page: Page) -> bool:
        """Kiểm tra đã đăng nhập Google / Flow (body + control, không dùng API)."""
        if not check_google_flow_login(page):
            return False
        login_like = [
            "text=Sign in",
            "text=Đăng nhập",
            "text=Login",
            "text=Choose an account",
            "input[type='email']",
        ]
        for sel in login_like:
            try:
                if page.locator(sel).first.is_visible(timeout=800):
                    return False
            except Exception:
                continue
        return self._has_prompt_box(page) or self._has_project_button(page)

    def build_final_prompt(self, job: dict[str, Any], normalized: dict[str, Any]) -> str:
        """Ghép prompt chuẩn nếu job chưa có ``final_prompt``."""
        if str(job.get("final_prompt", "")).strip():
            return str(job.get("final_prompt", "")).strip()
        profile = build_or_load_character_profile(normalized, job)
        scene_plan = build_start_end_scene_plan(normalized, profile)
        return build_google_flow_text_to_video_prompt(normalized, profile, scene_plan)

    def find_prompt_box(self, page: Page):
        """Tìm ô nhập prompt trên Flow."""
        return find_flow_prompt_box(page)

    def find_generate_button(self, page: Page, prompt_box=None):
        """Tìm nút tạo video (delegate logic chuẩn)."""
        pb = prompt_box if prompt_box is not None else find_flow_prompt_box(page)
        return find_flow_generate_button(page, pb)

    def input_prompt_like_human(self, page: Page, final_prompt: str) -> None:
        """
        Nhập prompt kiểu người dùng: ``press_sequentially``, không ``fill``/inject DOM.
        """
        input_prompt_to_flow(page, final_prompt)

    def input_prompt(self, page: Page, final_prompt: str) -> None:
        """Alias tương thích — gọi ``input_prompt_like_human``."""
        self.input_prompt_like_human(page, final_prompt=final_prompt)

    def click_generate_and_wait_started(self, page: Page) -> None:
        """Bấm tạo video và chờ generating (timeout 30s, có tín hiệu video tùy chọn)."""
        click_generate_and_wait_started(
            page,
            screenshot_prefix="google_flow_runner_generate",
            generation_started_timeout_ms=30_000,
            include_video_signal=True,
        )

    def click_generate(self, page: Page) -> None:
        """Bấm nút tạo + screenshot khi lỗi."""
        try:
            self.click_generate_and_wait_started(page)
        except Exception:
            self._save_failure_screenshot(page, "click_generate_failed")
            raise

    def click_start(self, page: Page) -> None:
        """Tên cũ trong DB — tương đương ``click_generate``."""
        self.click_generate(page)

    def wait_generate_button_ready(self, button) -> None:
        """Kiểm tra nút generate sẵn sàng (public cho test / tích hợp)."""
        wait_generate_button_ready(button)

    def wait_generation_done(self, page: Page) -> None:
        """Chờ video xong (video visible hoặc nút Download), tối đa 30 phút."""
        wait_flow_generation_done(page)

    def download_video(self, page: Page, job: dict[str, Any]) -> str:
        """Tải video: ``expect_download`` hoặc fallback ``src`` / request."""
        out = self._paths["outputs"]
        out.mkdir(parents=True, exist_ok=True)
        job_id = str(job.get("id", "flow_vid")).strip() or "flow_vid"
        download_names = re.compile(r"Download|Tải xuống|Save|Lưu", re.I)
        try:
            btn = page.get_by_role("button", name=download_names).first
            btn.wait_for(state="visible", timeout=10_000)
            with page.expect_download(timeout=120_000) as dl_info:
                btn.click(timeout=5000)
            dl = dl_info.value
            target = out / f"{job_id}.mp4"
            dl.save_as(str(target))
            return str(target)
        except Exception:
            pass

        for sel in [
            "button:has-text('Download')",
            "button:has-text('Tải xuống')",
            "button[aria-label*='download' i]",
            "[role='button'][aria-label*='download' i]",
        ]:
            try:
                btn = page.locator(sel).first
                if not btn.is_visible(timeout=1200):
                    continue
                with page.expect_download(timeout=120_000) as dl_info:
                    btn.click(timeout=5000)
                dl = dl_info.value
                target = out / f"{job_id}.mp4"
                dl.save_as(str(target))
                return str(target)
            except Exception:
                continue

        src = self._video_src(page)
        if src:
            resp = page.context.request.get(src, timeout=120_000)
            if resp.ok:
                target = out / f"{job_id}.mp4"
                target.write_bytes(bytes(resp.body()))
                return str(target)
        raise RuntimeError("Không tải được video output từ Google Flow.")

    def save_error_screenshot(self, page: Page, job_id: str, step: str) -> Path | None:
        """Screenshot lỗi theo job + bước, lưu dưới ``logs/screenshots`` của module."""
        try:
            safe_job = re.sub(r"[^a-zA-Z0-9_-]+", "_", job_id).strip("_")[:48] or "job"
            safe_step = re.sub(r"[^a-zA-Z0-9_-]+", "_", step).strip("_")[:48] or "step"
            target = self._paths["screenshots"] / f"{safe_job}_{safe_step}_{int(time.time())}.png"
            page.screenshot(path=str(target), full_page=True)
            logger.warning("Google Flow screenshot lỗi: {}", target)
            return target
        except Exception:
            return None

    def _ensure_prompt_parts(self, job: dict[str, Any], normalized: dict[str, Any]) -> tuple[str, dict[str, str], dict[str, Any]]:
        """Build final prompt và scene plan khi job chưa có ``final_prompt``."""
        if str(job.get("final_prompt", "")).strip():
            return (
                str(job.get("final_prompt", "")).strip(),
                dict(job.get("scene_plan") or {}),
                dict(job.get("character_profile") or {}),
            )
        profile = build_or_load_character_profile(normalized, job)
        scene_plan = build_start_end_scene_plan(normalized, profile)
        final_prompt = build_google_flow_text_to_video_prompt(normalized, profile, scene_plan)
        return final_prompt, scene_plan, profile

    def _mark(self, job_id: str, **patch: Any) -> dict[str, Any]:
        """Cập nhật job trong ``flow_video_jobs.json``."""
        if "status" in patch:
            st = str(patch["status"]).strip().lower()
            if st and st not in FLOW_STATUSES:
                patch["status"] = "failed"
                patch["error_message"] = f"Status không hợp lệ: {st}"
        return self._store.update_job(job_id, patch)

    def _prompt_box(self, page: Page):
        """Ô prompt visible (heuristic nhẹ)."""
        try:
            return find_flow_prompt_box(page)
        except Exception:
            return None

    def _has_prompt_box(self, page: Page) -> bool:
        return has_prompt_box(page)

    def open_or_create_project(self, page: Page) -> None:
        """Tự tạo / mở project Flow (home → workspace)."""
        open_or_create_flow_project(page)

    def select_text_to_video(self, page: Page) -> None:
        """Chọn Text-to-Video / Veo 3 nếu UI yêu cầu."""
        select_text_to_video_mode_if_needed(page)

    def apply_settings(self, page: Page, settings: dict[str, Any]) -> None:
        """Áp dụng đúng bộ setting Flow (Video/Khung hình/aspect/outputs/model/duration)."""
        logger.info("Flow settings: bắt đầu áp dụng cấu hình trước khi nhập prompt.")
        self._ensure_video_and_frames_tabs(page)
        self._flow_step_delay(page)

        aspect = str(settings.get("aspect_ratio", "")).strip()
        if aspect in {"9:16", "16:9"}:
            self._set_aspect_ratio(page, aspect)
            self._flow_step_delay(page)

        outputs = self._normalize_outputs(settings)
        if outputs:
            self._set_outputs_per_prompt(page, outputs)
            self._flow_step_delay(page)

        model = self._normalize_model(settings)
        if model:
            self._select_model(page, model)
            self._flow_step_delay(page)

        duration = self._normalize_duration(settings)
        if duration:
            self._set_duration(page, duration)
            self._flow_step_delay(page)
        # Settling delay cuối để UI ổn định hoàn toàn rồi mới nhập prompt.
        self._flow_step_delay(page, min_ms=1400)
        logger.info("Flow settings: áp dụng xong, tiếp tục bước nhập prompt.")

    def _flow_step_delay(self, page: Page, *, min_ms: int = 250) -> None:
        """
        Delay giữa các bước chọn setting để giảm miss-click / anti-bot trigger.
        """
        try:
            page.wait_for_timeout(max(min_ms, self._settings_delay_ms))
        except Exception:
            pass

    def _ensure_video_and_frames_tabs(self, page: Page) -> None:
        """Đảm bảo đang ở tab Video + Khung hình trước khi chọn setting."""
        self._click_setting_chip(page, ["Video"], required=True, label="mode_video")
        self._flow_step_delay(page)
        self._click_setting_chip(page, ["Khung hình", "Frames"], required=True, label="tab_frames")
        self._flow_step_delay(page)

    def _normalize_outputs(self, settings: dict[str, Any]) -> str:
        raw = str(settings.get("outputs", "") or settings.get("output_count", "")).strip().lower()
        if not raw:
            return ""
        if raw.startswith("x"):
            raw = raw[1:]
        try:
            n = int(raw)
        except Exception:
            return ""
        if n < 1:
            n = 1
        if n > 4:
            n = 4
        return f"x{n}"

    def _normalize_duration(self, settings: dict[str, Any]) -> str:
        raw = str(settings.get("duration", "") or settings.get("duration_sec", "")).strip().lower()
        if not raw:
            return ""
        raw = raw.replace("seconds", "s").replace("sec", "s").replace(" ", "")
        if raw in {"4", "4s"}:
            return "4s"
        if raw in {"6", "6s"}:
            return "6s"
        if raw in {"8", "8s"}:
            return "8s"
        return ""

    def _normalize_model(self, settings: dict[str, Any]) -> str:
        raw = str(settings.get("model", "")).strip()
        return raw

    def _set_aspect_ratio(self, page: Page, aspect: str) -> None:
        """
        Chọn tỉ lệ khung hình theo tab slider Radix của Flow:
        - 9:16 => nút id *trigger-PORTRAIT* (icon crop_9_16 + text 9:16)
        - 16:9 => nút *trigger-LANDSCAPE* (crop_16_9 + 16:9)
        Ưu tiên tablist có đủ hai tab để không nhầm slider Outputs/Duration.
        """
        target = "PORTRAIT" if aspect == "9:16" else "LANDSCAPE"
        selectors: list[str] = [
            # DOM thực tế: div[role=tablist] > button.flow_tab_slider_trigger#radix-…-trigger-PORTRAIT|LANDSCAPE
            (
                "div[role='tablist']:has(button[id*='trigger-PORTRAIT'])"
                ":has(button[id*='trigger-LANDSCAPE']) "
                f"button.flow_tab_slider_trigger[id*='trigger-{target}']"
            ),
            f"button.flow_tab_slider_trigger[id$='-trigger-{target}']",
            f"button[role='tab'][aria-controls*='-content-{target}']",
            f"button[role='tab'][aria-controls*='{target}']",
            f"button[role='tab'][id*='trigger-{target}']",
            f"button.flow_tab_slider_trigger[aria-controls*='{target}']",
            f"button[role='tab']:has-text('{aspect}')",
        ]

        def _click_until_active(tab: Locator) -> bool:
            if tab.count() == 0 or not tab.is_visible(timeout=900):
                return False
            if str(tab.get_attribute("aria-selected") or "").lower() == "true":
                return True
            if str(tab.get_attribute("data-state") or "").lower() == "active":
                return True
            for _ in range(2):
                try:
                    tab.click(timeout=2500)
                except Exception:
                    tab.click(timeout=2500, force=True)
                page.wait_for_timeout(320)
                if str(tab.get_attribute("aria-selected") or "").lower() == "true":
                    return True
                if str(tab.get_attribute("data-state") or "").lower() == "active":
                    return True
            return False

        for sel in selectors:
            try:
                tab = page.locator(sel).first
                if _click_until_active(tab):
                    return
            except Exception:
                continue
        # Fallback: get_by_role theo nhãn hiển thị (text sau icon crop_*).
        try:
            role_tab = page.get_by_role("tab", name=aspect, exact=True)
            if role_tab.count() > 0 and _click_until_active(role_tab.first):
                return
        except Exception:
            pass
        try:
            hard = page.locator(f"button[role='tab'].flow_tab_slider_trigger:has-text('{aspect}')").first
            if _click_until_active(hard):
                return
        except Exception:
            pass
        self._click_setting_chip(page, [aspect], required=True, label="aspect_ratio")

    def _set_outputs_per_prompt(self, page: Page, outputs: str) -> None:
        """
        Chọn Outputs/prompt theo tab slider Flow:
        x1..x4 tương ứng trigger/content 1..4.
        """
        m = re.match(r"^x([1-4])$", str(outputs or "").strip().lower())
        target_num = m.group(1) if m else ""
        target = f"x{target_num}" if target_num else str(outputs or "").strip().lower()
        selectors = []
        if target_num:
            selectors.extend(
                [
                    f"button[role='tab'][aria-controls*='content-{target_num}']",
                    f"button[role='tab'][id*='trigger-{target_num}']",
                    f"button.flow_tab_slider_trigger[aria-controls*='content-{target_num}']",
                ]
            )
        selectors.append(f"button[role='tab']:has-text('{target}')")
        for sel in selectors:
            try:
                tab = page.locator(sel).first
                if tab.count() == 0 or not tab.is_visible(timeout=900):
                    continue
                # Đã selected đúng thì thoát sớm.
                if str(tab.get_attribute("aria-selected") or "").lower() == "true":
                    return
                # Retry click 2 lần để chịu được overlay/animation của Flow.
                for _ in range(2):
                    try:
                        tab.click(timeout=2500)
                    except Exception:
                        tab.click(timeout=2500, force=True)
                    page.wait_for_timeout(320)
                    aria_selected = str(tab.get_attribute("aria-selected") or "").lower()
                    data_state = str(tab.get_attribute("data-state") or "").lower()
                    if aria_selected == "true" or data_state == "active":
                        return
            except Exception:
                continue
        # Fallback cứng theo HTML bạn cung cấp: role=tab + text xN
        if target_num:
            try:
                hard = page.locator(f"button[role='tab'].flow_tab_slider_trigger:has-text('x{target_num}')").first
                if hard.count() > 0 and hard.is_visible(timeout=1200):
                    for _ in range(2):
                        try:
                            hard.click(timeout=2500)
                        except Exception:
                            hard.click(timeout=2500, force=True)
                        page.wait_for_timeout(320)
                        aria_selected = str(hard.get_attribute("aria-selected") or "").lower()
                        data_state = str(hard.get_attribute("data-state") or "").lower()
                        if aria_selected == "true" or data_state == "active":
                            return
            except Exception:
                pass
        self._click_setting_chip(page, [target], required=True, label="outputs")

    def _set_duration(self, page: Page, duration: str) -> None:
        """
        Chọn thời lượng theo tab slider Flow: 4s/6s/8s.
        """
        m = re.match(r"^([468])s$", str(duration or "").strip().lower())
        sec = m.group(1) if m else ""
        target = f"{sec}s" if sec else str(duration or "").strip().lower()
        selectors = []
        if sec:
            selectors.extend(
                [
                    f"button[role='tab'][aria-controls*='content-{sec}']",
                    f"button[role='tab'][id*='trigger-{sec}']",
                    f"button.flow_tab_slider_trigger[aria-controls*='content-{sec}']",
                ]
            )
        selectors.append(f"button[role='tab']:has-text('{target}')")
        for sel in selectors:
            try:
                tab = page.locator(sel).first
                if tab.count() == 0 or not tab.is_visible(timeout=900):
                    continue
                if str(tab.get_attribute("aria-selected") or "").lower() == "true":
                    return
                for _ in range(2):
                    try:
                        tab.click(timeout=2500)
                    except Exception:
                        tab.click(timeout=2500, force=True)
                    page.wait_for_timeout(320)
                    aria_selected = str(tab.get_attribute("aria-selected") or "").lower()
                    data_state = str(tab.get_attribute("data-state") or "").lower()
                    if aria_selected == "true" or data_state == "active":
                        return
            except Exception:
                continue
        # Fallback cứng theo HTML tab Flow.
        try:
            hard = page.locator(f"button[role='tab'].flow_tab_slider_trigger:has-text('{target}')").first
            if hard.count() > 0 and hard.is_visible(timeout=1200):
                for _ in range(2):
                    try:
                        hard.click(timeout=2500)
                    except Exception:
                        hard.click(timeout=2500, force=True)
                    page.wait_for_timeout(320)
                    aria_selected = str(hard.get_attribute("aria-selected") or "").lower()
                    data_state = str(hard.get_attribute("data-state") or "").lower()
                    if aria_selected == "true" or data_state == "active":
                        return
        except Exception:
            pass
        self._click_setting_chip(page, [target], required=True, label="duration")

    def _click_setting_chip(self, page: Page, labels: list[str], *, required: bool, label: str) -> bool:
        """Click chip/button theo text; dùng force click nếu bị overlay che."""
        for text in labels:
            t = str(text or "").strip()
            if not t:
                continue
            for sel in (
                f"button:has-text('{t}')",
                f"[role='tab']:has-text('{t}')",
                f"[role='button']:has-text('{t}')",
                f"text=/{re.escape(t)}/i",
            ):
                try:
                    loc = page.locator(sel).first
                    if loc.count() == 0 or not loc.is_visible(timeout=900):
                        continue
                    try:
                        loc.click(timeout=2500)
                    except Exception:
                        loc.click(timeout=2500, force=True)
                    self._flow_step_delay(page, min_ms=200)
                    return True
                except Exception:
                    continue
        if required:
            raise RuntimeError(f"Không chọn được setting bắt buộc: {label}")
        return False

    def _select_model(self, page: Page, model_name: str) -> None:
        """Mở dropdown model, đọc model thật từ Flow, chọn model phù hợp và cache cho UI."""
        # Mở dropdown model (thường đang hiện "Veo 3.1 - Fast")
        opened = self._click_setting_chip(
            page,
            ["Veo 3.1 - Fast", "Veo 3.1", "Veo", "Nano Banana 2"],
            required=False,
            label="model_dropdown",
        )
        if opened:
            time.sleep(0.35)
        choices = self._discover_flow_model_choices(page) if opened else []
        if choices:
            self._save_flow_model_cache(choices)
        target = self._best_model_match(model_name, choices) if model_name else ""
        labels = [x for x in (target, model_name) if str(x).strip()]
        if not labels and choices:
            labels = [choices[0]]
        if labels:
            self._click_setting_chip(page, labels, required=True, label="model")

    def _flow_model_cache_path(self) -> Path:
        return self._paths["temp"] / "flow_model_choices.json"

    def _save_flow_model_cache(self, models: list[str]) -> None:
        uniq: list[str] = []
        for x in models:
            s = str(x).strip()
            if s and s not in uniq:
                uniq.append(s)
        payload = {"models": uniq, "updated_at": now_iso()}
        p = self._flow_model_cache_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _discover_flow_model_choices(self, page: Page) -> list[str]:
        """Đọc model options đang hiển thị trong dropdown Flow."""
        script = """
(() => {
  const out = [];
  const re = /(Veo\\s*3(\\.1)?\\s*[-–]?\\s*(Fast|Lite|Quality)?(?:\\s*\\[Lower Priority\\])?(?:\\s*\\(.*?\\))?|Nano\\s*Banana\\s*2)/i;
  const menus = Array.from(document.querySelectorAll('[role="menu"][data-state="open"], [data-radix-dropdown-menu-content][data-state="open"]'));
  const menuItems = menus.length > 0
    ? menus.flatMap((m) => Array.from(m.querySelectorAll('[role="menuitem"], [role="option"]')))
    : Array.from(document.querySelectorAll('[role="menuitem"], [role="option"]'));
  for (const item of menuItems) {
    const spans = Array.from(item.querySelectorAll('span'));
    let txt = '';
    for (const sp of spans) {
      const t = (sp.textContent || '').replace(/\\s+/g, ' ').trim();
      if (t.length > txt.length) txt = t;
    }
    if (!txt) txt = (item.textContent || '').replace(/\\s+/g, ' ').trim();
    if (!txt || !re.test(txt)) continue;
    out.push(txt);
  }
  return Array.from(new Set(out));
})()
"""
        try:
            got = page.evaluate(script)
        except Exception:
            return []
        if not isinstance(got, list):
            return []
        out: list[str] = []
        for x in got:
            s = str(x).strip()
            if s and s not in out:
                out.append(s)
        return out

    def _best_model_match(self, requested: str, choices: list[str]) -> str:
        """Khớp model user chọn với model thực tế của Flow."""
        req = str(requested or "").strip()
        if not req or not choices:
            return req
        req_l = req.lower()

        def _norm(x: str) -> str:
            return re.sub(r"[^a-z0-9]+", "", str(x or "").lower())

        def _flavor(x: str) -> str:
            xl = str(x or "").lower()
            # map slug/API style -> flavor UI
            if "fast" in xl:
                return "fast"
            if "lite" in xl:
                return "lite"
            if "quality" in xl:
                return "quality"
            # veo-3.1-generate-preview thường là model thường (không fast/lite)
            if "generate-preview" in xl:
                return "quality"
            return ""

        # 1) exact
        for c in choices:
            if c.lower() == req_l:
                return c
        # 2) normalized contains (bỏ '-', '.', khoảng trắng)
        req_n = _norm(req)
        for c in choices:
            c_n = _norm(c)
            if req_n and (req_n in c_n or c_n in req_n):
                return c
        # 3) ưu tiên cùng family + flavor
        flavor = _flavor(req)
        family = "veo" if "veo" in req_l else ("nano" if "nano" in req_l else "")
        if family and flavor:
            for c in choices:
                cl = c.lower()
                if family in cl and flavor in cl:
                    return c
        # 4) nếu là Veo mà req không có flavor rõ, ưu tiên bản quality/non-fast
        if "veo" in req_l and not flavor:
            for c in choices:
                cl = c.lower()
                if "veo" in cl and "fast" not in cl and "lite" not in cl:
                    return c
        # 5) fallback theo family
        if "veo" in req_l:
            for c in choices:
                if "veo" in c.lower():
                    return c
        if "nano" in req_l:
            for c in choices:
                if "nano" in c.lower():
                    return c
        return choices[0]

    def _has_project_button(self, page: Page) -> bool:
        for sel in ["button:has-text('New project')", "button:has-text('Tạo dự án mới')"]:
            try:
                if page.locator(sel).first.is_visible(timeout=1000):
                    return True
            except Exception:
                continue
        return False

    def _select_by_text_if_exists(self, page: Page, values: list[str]) -> None:
        for value in values:
            v = str(value or "").strip()
            if not v:
                continue
            sel = f"button:has-text('{v}')"
            try:
                btn = page.locator(sel).first
                if btn.is_visible(timeout=800):
                    btn.click(timeout=2000)
                    time.sleep(0.3)
                    return
            except Exception:
                continue

    def _has_download_button(self, page: Page) -> bool:
        download_names = re.compile(r"Download|Tải xuống|Save|Lưu", re.I)
        try:
            if page.get_by_role("button", name=download_names).first.is_visible(timeout=700):
                return True
        except Exception:
            pass
        for sel in [
            "button:has-text('Download')",
            "button:has-text('Tải xuống')",
            "button[aria-label*='download' i]",
            "[role='button'][aria-label*='download' i]",
        ]:
            try:
                if page.locator(sel).first.is_visible(timeout=700):
                    return True
            except Exception:
                continue
        return False

    def _has_video_node(self, page: Page) -> bool:
        try:
            return page.locator("video").first.is_visible(timeout=700)
        except Exception:
            return False

    def _video_src(self, page: Page) -> str:
        script = """
(() => {
  const out = [];
  for (const v of Array.from(document.querySelectorAll('video'))) {
    const src = (v.currentSrc || v.src || '').trim();
    if (src && !src.startsWith('blob:')) out.push(src);
    for (const s of Array.from(v.querySelectorAll('source'))) {
      const u = (s.src || '').trim();
      if (u && !u.startsWith('blob:')) out.push(u);
    }
  }
  return Array.from(new Set(out));
})()
"""
        try:
            urls = page.evaluate(script)
        except Exception:
            return ""
        if not isinstance(urls, list):
            return ""
        for u in urls:
            su = str(u).strip()
            if re.search(r"\.(mp4|webm|mov)(\?|$)", su, flags=re.I):
                return su
        return str(urls[0]).strip() if urls else ""

    def _save_failure_screenshot(self, page: Page, name: str) -> None:
        try:
            safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", name).strip("_")[:80] or "google_flow_err"
            target = self._paths["screenshots"] / f"{safe}_{int(time.time())}.png"
            page.screenshot(path=str(target), full_page=True)
            logger.warning("Google Flow screenshot lỗi: {}", target)
        except Exception:
            pass


def run_google_flow_veo_job(job: dict[str, Any]) -> dict[str, Any]:
    """Chạy một job tạo video Google Flow / Veo 3 (browser only, không API)."""
    return GoogleFlowVeoRunner().run(job)


GoogleFlowTextToVideoRunner = GoogleFlowVeoRunner
