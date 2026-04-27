from __future__ import annotations

import random
import time
from pathlib import Path
from typing import Any

from loguru import logger
from playwright.sync_api import BrowserContext, Page

from src.services.google_flow_text_to_video_runner import GoogleFlowVeoRunner, validate_flow_veo_job
from src.services.google_flow_veo_generate import (
    GOOGLE_FLOW_URL,
    check_google_flow_login,
    click_generate_and_wait_started,
    input_prompt_to_flow,
    open_or_create_flow_project,
    select_text_to_video_mode_if_needed,
    wait_flow_generation_done,
)
from src.services.google_flow_video_store import GoogleFlowVideoStore, ensure_google_flow_layout, now_iso


PRODUCTION_STEPS: tuple[str, ...] = (
    "INIT",
    "LOAD_JOB",
    "OPEN_BROWSER",
    "OPEN_FLOW",
    "CHECK_LOGIN",
    "CREATE_PROJECT",
    "SELECT_VIDEO_MODE",
    "APPLY_SETTINGS",
    "INPUT_PROMPT",
    "CLICK_GENERATE",
    "WAIT_GENERATION_STARTED",
    "WAIT_GENERATION_DONE",
    "DOWNLOAD_VIDEO",
    "SAVE_RESULT",
    "PREVIEW_READY",
    "COMPLETED",
    "FAILED",
    "NEED_MANUAL_CHECK",
    "CANCELLED",
)

TERMINAL_STATUSES = {"completed", "failed", "need_manual_check", "cancelled"}
RETRYABLE_KEYS = (
    "không tìm thấy",
    "timeout",
    "disabled",
    "chưa sẵn sàng",
    "không xác nhận được",
    "không tải được",
)
NON_RETRYABLE_KEYS = ("chưa đăng nhập", "quota", "bị chặn", "not available")


class GoogleFlowProductionRunner:
    """
    Production queue runner cho Google Flow / Veo 3:
    - 1 profile = 1 job chạy tuần tự
    - state machine rõ ràng theo step
    - screenshot + retry + metadata output_path
    """

    def __init__(self) -> None:
        self._store = GoogleFlowVideoStore()
        self._paths = ensure_google_flow_layout()
        self._single = GoogleFlowVeoRunner()
        self._lock_owner = f"flow_runner_{int(time.time())}"

    def run_queue(self, *, idle_sleep_sec: float = 5.0, stop_after_one: bool = False) -> None:
        """Loop queue: mỗi lượt lock 1 job pending rồi chạy."""
        logger.info("Google Flow production queue started (owner={})", self._lock_owner)
        while True:
            job = self._store.lock_next_pending_job(lock_owner=self._lock_owner)
            if not job:
                if stop_after_one:
                    return
                time.sleep(max(1.0, idle_sleep_sec))
                continue
            try:
                self.run_single_flow_job(job)
            finally:
                try:
                    self._store.unlock_job(str(job.get("id", "")).strip())
                except Exception:
                    pass
            if stop_after_one:
                return

    def run_single_flow_job(self, job: dict[str, Any]) -> dict[str, Any]:
        """Chạy full state machine cho một job."""
        job_id = str(job.get("id", "")).strip()
        if not job_id:
            raise ValueError("Job thiếu id.")
        self._mark(job_id, status="running", step="LOAD_JOB", started_at=now_iso(), error_message="")

        try:
            validate_flow_veo_job(job)
            final_prompt = str(job.get("final_prompt", "")).strip() or str(job.get("idea", "")).strip()
            if not final_prompt:
                raise RuntimeError("Thiếu prompt/final_prompt.")

            self._mark(job_id, step="OPEN_BROWSER")
            context = self._single.open_browser(job)
        except Exception as exc:  # noqa: BLE001
            return self._mark(job_id, status="failed", step="FAILED", error_message=str(exc), completed_at=now_iso())

        page: Page = context.pages[0] if context.pages else context.new_page()
        try:
            self._mark(job_id, step="OPEN_FLOW")
            page.goto(GOOGLE_FLOW_URL, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(2500)

            self._mark(job_id, step="CHECK_LOGIN")
            if not check_google_flow_login(page):
                self._save_screenshot(page, job_id, "need_login")
                return self._mark(
                    job_id,
                    status="need_manual_check",
                    step="NEED_MANUAL_CHECK",
                    error_message="Google Flow chưa login.",
                    completed_at=now_iso(),
                )

            self._mark(job_id, step="CREATE_PROJECT")
            open_or_create_flow_project(page)

            self._mark(job_id, step="SELECT_VIDEO_MODE")
            select_text_to_video_mode_if_needed(page)

            self._mark(job_id, step="APPLY_SETTINGS")
            self._single.apply_settings(page, dict(job.get("settings") or {}))
            page.wait_for_timeout(random.randint(500, 1200))

            self._mark(job_id, step="INPUT_PROMPT")
            input_prompt_to_flow(page, final_prompt)

            self._mark(job_id, step="CLICK_GENERATE")
            click_generate_and_wait_started(
                page,
                screenshot_prefix=f"{job_id}_generate",
                generation_started_timeout_ms=30_000,
                include_video_signal=True,
            )
            self._mark(job_id, step="WAIT_GENERATION_STARTED")

            self._mark(job_id, step="WAIT_GENERATION_DONE")
            wait_flow_generation_done(page, timeout_ms=30 * 60 * 1000)

            self._mark(job_id, step="DOWNLOAD_VIDEO")
            out = self._single.download_video(page, job)
            if not Path(out).is_file():
                raise RuntimeError("Download trả về file không tồn tại.")

            self._mark(job_id, step="SAVE_RESULT", output_path=out, output_files=[out])
            self._mark(job_id, step="PREVIEW_READY")
            return self._mark(
                job_id,
                status="completed",
                step="COMPLETED",
                output_path=out,
                output_files=[out],
                completed_at=now_iso(),
                error_message="",
            )
        except Exception as exc:  # noqa: BLE001
            return self._handle_job_error(job, page, exc)
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                if self._single._pw is not None:  # noqa: SLF001
                    self._single._pw.stop()  # noqa: SLF001
            except Exception:
                pass
            self._single._pw = None  # noqa: SLF001

    def regenerate_job(self, job_id: str, *, edited_prompt: str = "") -> dict[str, Any]:
        """Tạo job mới để regenerate bằng project mới."""
        return self._store.create_regenerate_job(job_id, edited_prompt=edited_prompt)

    def _mark(self, job_id: str, **patch: Any) -> dict[str, Any]:
        if "step" in patch:
            step = str(patch["step"] or "").strip().upper()
            if step and step not in PRODUCTION_STEPS:
                patch["step"] = "FAILED"
        return self._store.update_job(job_id, patch)

    def _save_screenshot(self, page: Page, job_id: str, step: str) -> None:
        self._single.save_error_screenshot(page, job_id, step)

    def _is_retryable(self, msg: str) -> bool:
        m = (msg or "").strip().lower()
        if not m:
            return False
        if any(k in m for k in NON_RETRYABLE_KEYS):
            return False
        return any(k in m for k in RETRYABLE_KEYS)

    def _handle_job_error(self, job: dict[str, Any], page: Page, exc: Exception) -> dict[str, Any]:
        job_id = str(job.get("id", "")).strip()
        msg = str(exc).strip() or "Google Flow job failed."
        self._save_screenshot(page, job_id, str(job.get("step", "FAILED")))
        retry_count = int(job.get("retry_count") or 0)
        max_retry = max(1, int(job.get("max_retry") or 3))
        if self._is_retryable(msg) and retry_count < max_retry:
            return self._mark(
                job_id,
                status="pending",
                step="INIT",
                retry_count=retry_count + 1,
                error_message=msg,
            )
        if "chưa login" in msg.lower() or "đăng nhập" in msg.lower():
            return self._mark(
                job_id,
                status="need_manual_check",
                step="NEED_MANUAL_CHECK",
                error_message=msg,
                completed_at=now_iso(),
            )
        return self._mark(
            job_id,
            status="failed",
            step="FAILED",
            retry_count=retry_count,
            error_message=msg,
            completed_at=now_iso(),
        )


def run_google_flow_queue(*, idle_sleep_sec: float = 5.0) -> None:
    """Entry point chạy queue production liên tục."""
    GoogleFlowProductionRunner().run_queue(idle_sleep_sec=idle_sleep_sec)

