"""
Thực thi đăng bài Playwright.

``execute_facebook_post_sequence`` / ``capture_failure_screenshot`` tách khỏi ``scheduler``
để ``scheduler`` import module này mà không tạo vòng (``run_for_account`` lazy-import ``scheduler``).

Luồng đăng tách theo profile: ``posting_engine`` = ``chromium`` | ``firefox`` (theo ``browser_type`` tài khoản).
"""

from __future__ import annotations

import os
import random
import time
import ctypes
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from src.automation.facebook_actions import (
    _disable_view_only_guard,
    _enable_view_only_guard,
    _is_meta_business_composer_context,
    click_post_button,
    complete_meta_business_reel_post_wizard,
    ensure_content_present,
    ensure_facebook_session_for_post,
    fill_content,
    go_to_posting_target_and_open_composer,
    prime_facebook_session_page,
    resolve_posting_entity,
    set_reel_strict_log_job_id,
    upload_photo,
    upload_video,
    verify_post_submitted,
    wait_meta_reel_details_wizard,
)
from src.services.job_post_runtime import (
    STEP_COMPOSER,
    STEP_FILL_CONTENT,
    STEP_MEDIA,
    STEP_NAV_TARGET,
    STEP_SESSION_ENSURE,
    STEP_SUBMIT,
    STEP_VERIFY_RESULT,
    JobRunTracker,
)
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.posting_browser import PostingBrowserEngine
from src.utils.screenshot import capture_page_screenshot

_VIDEO_SUFFIXES = frozenset({".mp4", ".mov", ".avi", ".mkv"})


def _browser_interaction_lock_enabled() -> bool:
    raw = str(os.environ.get("FB_LOCK_BROWSER_DURING_JOB", "1")).strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _pipeline_perf_enabled() -> bool:
    raw = str(os.environ.get("FB_PIPELINE_PERF_LOG", "0")).strip().lower()
    return raw in {"1", "true", "on", "yes"}


def _human_step_delay(*, label: str = "") -> None:
    """
    Delay giữa các bước lớn (upload -> caption -> publish) để giống thao tác người dùng thật.
    Cấu hình qua env (ms):
    - FB_STEP_DELAY_MIN_MS (mặc định 900)
    - FB_STEP_DELAY_MAX_MS (mặc định 1800)
    """
    min_ms = max(120, int(str(os.environ.get("FB_STEP_DELAY_MIN_MS", "900")).strip() or "900"))
    max_ms = max(min_ms, int(str(os.environ.get("FB_STEP_DELAY_MAX_MS", "1800")).strip() or "1800"))
    d_ms = random.randint(min_ms, max_ms)
    if label:
        logger.info("[FB human-delay] {}: {} ms", label, d_ms)
    time.sleep(d_ms / 1000.0)


def _process_memory_mb() -> float | None:
    try:
        if os.name == "nt":
            class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("cb", ctypes.c_ulong),
                    ("PageFaultCount", ctypes.c_ulong),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = PROCESS_MEMORY_COUNTERS()
            counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
            kernel32 = ctypes.windll.kernel32
            psapi = ctypes.windll.psapi
            ok = psapi.GetProcessMemoryInfo(
                kernel32.GetCurrentProcess(),
                ctypes.byref(counters),
                counters.cb,
            )
            if ok:
                return float(counters.WorkingSetSize) / (1024.0 * 1024.0)
            return None
    except Exception:
        return None
    return None


def _perf_mark(enabled: bool, stage: str, started_at: float) -> float:
    now = time.perf_counter()
    if enabled:
        elapsed_ms = int((now - started_at) * 1000)
        mem = _process_memory_mb()
        if mem is None:
            logger.info("[FB PERF] stage={} | elapsed_ms={}", stage, elapsed_ms)
        else:
            logger.info("[FB PERF] stage={} | elapsed_ms={} | rss_mb={:.1f}", stage, elapsed_ms, mem)
    return now


def capture_failure_screenshot(page: Any | None, account_id: str) -> Optional[Path]:
    """Chụp màn hình khi pipeline đăng lỗi (Playwright ``page`` còn mở)."""
    if page is None:
        return None
    stem = f"post_fail_{account_id}_{int(time.time())}"
    try:
        return capture_page_screenshot(page, stem)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không chụp screenshot lỗi đăng ({}): {}", account_id, exc)
        return None


def _run_chromium_posting_flow(
    page: Any,
    *,
    browser_for_log: str = "chromium",
    cookie_path: str | None,
    entity_dict: dict[str, Any] | None,
    pages_json_row: dict[str, Any] | None,
    text_body: str,
    draft_media_paths: list[Path],
    page_extra_image: Path | None,
    post_type: str,
    tracker: JobRunTracker | None,
    force_share_now: bool,
    job_scheduled_at_iso: str | None,
    reel_tags: list[str] | None = None,
    reel_description_override: str | None = None,
) -> None:
    """
    Các bước đăng Meta Business Composer (Reel / caption / Publish) — dùng chung mọi engine Playwright.

    ``browser_for_log`` chỉ phục vụ log (profile thật: Firefox / Chromium từ ``BrowserFactory``).
    """
    resolved = resolve_posting_entity(entity_dict, pages_json_row)
    bfl = str(browser_for_log or "chromium").strip().lower() or "chromium"
    logger.info(
        "[FB pipeline] browser={} | pipeline=meta_composer | entity_dict={!r} | resolved={!r}",
        bfl,
        entity_dict,
        resolved,
    )
    row = pages_json_row or {}
    page_display_name = str(row.get("page_name", "")).strip() or None
    pt = str(post_type or "text").strip().lower()
    use_video_upload = pt in ("video", "text_video")
    share_now_fb = bool(force_share_now)
    if not share_now_fb and job_scheduled_at_iso:
        try:
            raw = str(job_scheduled_at_iso).strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
            share_now_fb = datetime.now(timezone.utc) + timedelta(minutes=2) >= dt
        except Exception:
            share_now_fb = True
    elif not share_now_fb:
        share_now_fb = True

    def _track(step: str, message: str = "") -> None:
        if tracker is not None:
            tracker.set_step(step, message)

    lock_ui = _browser_interaction_lock_enabled()
    perf_on = _pipeline_perf_enabled()
    t0 = time.perf_counter()
    if lock_ui:
        try:
            _enable_view_only_guard(page)
            logger.info("[FB lock-ui] Đã khóa thao tác người dùng trên browser trong lúc chạy job.")
        except Exception as exc:  # noqa: BLE001
            logger.debug("[FB lock-ui] Không bật được lock UI: {}", exc)

    set_reel_strict_log_job_id(getattr(tracker, "job_id", None))
    try:
        prime_facebook_session_page(page)
        t0 = _perf_mark(perf_on, "prime_session_page", t0)
        _track(STEP_SESSION_ENSURE, "Đảm bảo phiên Facebook")
        ensure_facebook_session_for_post(page, cookie_path)
        t0 = _perf_mark(perf_on, "ensure_session_for_post", t0)
        _track(STEP_NAV_TARGET, "Điều hướng tới đích đăng")
        go_to_posting_target_and_open_composer(page, resolved, page_display_name=page_display_name)
        t0 = _perf_mark(perf_on, "open_composer", t0)
        _track(STEP_COMPOSER, "Composer sẵn sàng")

        def _extra_image_if_not_dup(paths: list[Path], extra: Path | None) -> Path | None:
            """Bỏ ``page_extra_image`` nếu đã có trong ``draft_media_paths`` (so sánh path tuyệt đối)."""
            if extra is None:
                return None
            try:
                ext_key = Path(str(extra)).resolve().as_posix().lower()
            except Exception:
                ext_key = str(extra).strip().lower()
            existing: set[str] = set()
            for p in paths:
                try:
                    existing.add(Path(str(p)).resolve().as_posix().lower())
                except Exception:
                    existing.add(str(p).strip().lower())
            if ext_key in existing:
                logger.info(
                    "[FB pipeline] Bỏ qua page_extra_image vì đã trùng với draft_media_paths: {}",
                    extra,
                )
                return None
            return extra

        use_reel_wizard = (
            use_video_upload
            and bool(draft_media_paths)
            and _is_meta_business_composer_context(page)
        )
        reel_upload_done = False
        if use_reel_wizard:
            _track(STEP_MEDIA, "Đính kèm video (Meta Business → Reels)")
            for mp in draft_media_paths:
                if use_video_upload or Path(mp).suffix.lower() in _VIDEO_SUFFIXES:
                    upload_video(page, mp)
                else:
                    upload_photo(page, str(mp))
                _human_step_delay(label="sau đính kèm media")
            safe_extra = _extra_image_if_not_dup(draft_media_paths, page_extra_image)
            if safe_extra is not None:
                upload_photo(page, str(safe_extra))
                _human_step_delay(label="sau đính kèm ảnh phụ")
            t0 = _perf_mark(perf_on, "reel_upload_media", t0)
            reel_upload_done = True
            if wait_meta_reel_details_wizard(page, timeout_ms=120_000):
                t0 = _perf_mark(perf_on, "reel_wait_details_wizard", t0)
                _track(STEP_FILL_CONTENT, "Reel: mô tả / hashtag")
                _track(STEP_SUBMIT, "Reel: Next → Share / Schedule")
                unlock_for_reel_wizard = lock_ui
                if unlock_for_reel_wizard:
                    try:
                        _disable_view_only_guard(page)
                        logger.info("[FB lock-ui] Tạm mở thao tác browser trong bước Reel wizard (Next/Share).")
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("[FB lock-ui] Không tắt tạm lock UI cho Reel wizard: {}", exc)
                reel_submit_clicked = False
                try:
                    _human_step_delay(label="trước Reel wizard (Next/Share)")
                    reel_submit_clicked = complete_meta_business_reel_post_wizard(
                        page,
                        description=str(reel_description_override or text_body or "").strip(),
                        reel_tags=list(reel_tags or []),
                        share_now=share_now_fb,
                        scheduled_at_utc_iso=(str(job_scheduled_at_iso).strip() or None) if not share_now_fb else None,
                    )
                    t0 = _perf_mark(perf_on, "reel_complete_wizard", t0)
                finally:
                    if unlock_for_reel_wizard:
                        try:
                            _enable_view_only_guard(page)
                            logger.info("[FB lock-ui] Đã khóa lại browser sau bước Reel wizard.")
                        except Exception as exc:  # noqa: BLE001
                            logger.debug("[FB lock-ui] Không khóa lại UI sau Reel wizard: {}", exc)
                _track(STEP_VERIFY_RESULT, "Xác nhận đã đăng (Reel)")
                verify_timeout = 180_000
                snippet = (text_body or "").strip()[:200] or None
                verify_post_submitted(
                    page,
                    text_snippet=snippet,
                    timeout_ms=verify_timeout,
                    require_submit_signal=True,
                    submit_clicked=reel_submit_clicked,
                )
                _perf_mark(perf_on, "reel_verify_submitted", t0)
                return
            # Fallback cách 2: composer UI mới (wizard không hiện theo luồng cũ).
            cur_url = ""
            try:
                cur_url = str(page.url or "").strip().lower()
            except Exception:
                cur_url = ""
            logger.warning(
                "[FB pipeline] Không vào được màn Reel details/Next theo luồng chuẩn. "
                "Fallback cách 2 (composer trực tiếp) | url={}",
                cur_url or "(unknown)",
            )
            if (text_body or "").strip():
                _track(STEP_FILL_CONTENT, "Reel fallback: nhập nội dung")
                fill_content(page, text_body)
                _human_step_delay(label="sau nhập caption fallback")
                t0 = _perf_mark(perf_on, "reel_fallback_fill_content", t0)
            else:
                logger.info("[FB pipeline] Reel fallback: caption trống, bỏ qua fill_content.")
            _track(STEP_SUBMIT, "Reel fallback: gửi bài (Post)")
            if (text_body or "").strip():
                try:
                    ensure_content_present(page, text_body)
                except Exception as _ecp2:  # noqa: BLE001
                    logger.warning("[FB pipeline] reel fallback ensure_content_present lỗi (bỏ qua): {}", _ecp2)
            _human_step_delay(label="trước bấm Publish fallback")
            click_post_button(page)
            _human_step_delay(label="sau bấm Publish fallback")
            t0 = _perf_mark(perf_on, "reel_fallback_click_post_button", t0)
            _track(STEP_VERIFY_RESULT, "Reel fallback: xác nhận đã đăng")
            snippet = (text_body or "").strip()[:200] or None
            verify_post_submitted(page, text_snippet=snippet, timeout_ms=180_000)
            _perf_mark(perf_on, "reel_fallback_verify_submitted", t0)
            return

        # LƯU Ý: upload media TRƯỚC, fill_content SAU để composer re-render khi thêm
        # ảnh/video không làm mất nội dung đã nhập (bug Lexical/DraftJS state reset).
        if not reel_upload_done:
            _track(STEP_MEDIA, "Đính kèm media")
            for mp in draft_media_paths:
                if use_video_upload or Path(mp).suffix.lower() in _VIDEO_SUFFIXES:
                    upload_video(page, mp)
                else:
                    upload_photo(page, str(mp))
                _human_step_delay(label="sau đính kèm media")
            safe_extra = _extra_image_if_not_dup(draft_media_paths, page_extra_image)
            if safe_extra is not None:
                upload_photo(page, str(safe_extra))
                _human_step_delay(label="sau đính kèm ảnh phụ")
            t0 = _perf_mark(perf_on, "upload_media", t0)
        if (text_body or "").strip():
            _track(STEP_FILL_CONTENT, "Nhập nội dung")
            fill_content(page, text_body)
            _human_step_delay(label="sau nhập caption")
            t0 = _perf_mark(perf_on, "fill_content", t0)
        else:
            logger.info("[FB pipeline] Bỏ qua fill_content (caption trống).")
        _track(STEP_SUBMIT, "Gửi bài (Post)")
        # Re-verify nội dung ngay trước khi Publish; nếu editor trống nhưng job có
        # text_body → nhập lại để chắc chắn Lexical state có caption.
        if (text_body or "").strip():
            try:
                ensure_content_present(page, text_body)
            except Exception as _ecp:  # noqa: BLE001
                logger.warning("[FB pipeline] ensure_content_present lỗi (bỏ qua): {}", _ecp)
        _human_step_delay(label="trước bấm Publish/Post")
        click_post_button(page)
        _human_step_delay(label="sau bấm Publish/Post")
        t0 = _perf_mark(perf_on, "click_post_button", t0)
        _track(STEP_VERIFY_RESULT, "Xác nhận đã đăng")
        verify_timeout = 180_000 if use_video_upload else 120_000
        snippet = (text_body or "").strip()[:200] or None
        verify_post_submitted(page, text_snippet=snippet, timeout_ms=verify_timeout)
        _perf_mark(perf_on, "verify_submitted", t0)
    finally:
        set_reel_strict_log_job_id(None)
        if lock_ui:
            try:
                _disable_view_only_guard(page)
                logger.info("[FB lock-ui] Đã mở lại thao tác người dùng trên browser.")
            except Exception as exc:  # noqa: BLE001
                logger.debug("[FB lock-ui] Không gỡ được lock UI: {}", exc)


def _run_firefox_posting_flow(
    page: Any,
    *,
    cookie_path: str | None,
    entity_dict: dict[str, Any] | None,
    pages_json_row: dict[str, Any] | None,
    text_body: str,
    draft_media_paths: list[Path],
    page_extra_image: Path | None,
    post_type: str,
    tracker: JobRunTracker | None,
    force_share_now: bool,
    job_scheduled_at_iso: str | None,
    reel_tags: list[str] | None = None,
    reel_description_override: str | None = None,
) -> None:
    """
    Luồng đăng khi profile là Firefox (Gecko).

    Hiện tại gọi chung ``_run_chromium_posting_flow`` (pipeline ``meta_composer``, API Playwright giống nhau);
    sau này tách selector / timeout riêng cho Firefox tại đây nếu cần.
    """
    logger.info("[FB pipeline] browser=firefox | Luồng job đăng bài Firefox (profile portable Gecko).")
    _run_chromium_posting_flow(
        page,
        browser_for_log="firefox",
        cookie_path=cookie_path,
        entity_dict=entity_dict,
        pages_json_row=pages_json_row,
        text_body=text_body,
        draft_media_paths=draft_media_paths,
        page_extra_image=page_extra_image,
        post_type=post_type,
        tracker=tracker,
        force_share_now=force_share_now,
        job_scheduled_at_iso=job_scheduled_at_iso,
        reel_tags=reel_tags,
        reel_description_override=reel_description_override,
    )


def execute_facebook_post_sequence(
    page: Any,
    *,
    cookie_path: str | None,
    entity_dict: dict[str, Any] | None,
    pages_json_row: dict[str, Any] | None = None,
    text_body: str,
    draft_media_paths: list[Path],
    page_extra_image: Path | None,
    post_type: str = "text",
    tracker: JobRunTracker | None = None,
    force_share_now: bool = False,
    job_scheduled_at_iso: str | None = None,
    posting_engine: PostingBrowserEngine | str | None = None,
    reel_tags: list[str] | None = None,
    reel_description_override: str | None = None,
) -> None:
    """
    Mở Facebook → phiên → composer → nội dung / Reel → đăng → verify.

    ``posting_engine``: ``firefox`` hoặc ``chromium`` — mặc định scheduler theo ``browser_type`` (hiện ưu tiên Firefox).
    Phải khớp context đã mở (``BrowserFactory``).
    """
    eng = str(posting_engine or "firefox").strip().lower()
    if eng == "firefox":
        _run_firefox_posting_flow(
            page,
            cookie_path=cookie_path,
            entity_dict=entity_dict,
            pages_json_row=pages_json_row,
            text_body=text_body,
            draft_media_paths=draft_media_paths,
            page_extra_image=page_extra_image,
            post_type=post_type,
            tracker=tracker,
            force_share_now=force_share_now,
            job_scheduled_at_iso=job_scheduled_at_iso,
            reel_tags=reel_tags,
            reel_description_override=reel_description_override,
        )
        return
    _run_chromium_posting_flow(
        page,
        browser_for_log=eng,
        cookie_path=cookie_path,
        entity_dict=entity_dict,
        pages_json_row=pages_json_row,
        text_body=text_body,
        draft_media_paths=draft_media_paths,
        page_extra_image=page_extra_image,
        post_type=post_type,
        tracker=tracker,
        force_share_now=force_share_now,
        job_scheduled_at_iso=job_scheduled_at_iso,
        reel_tags=reel_tags,
        reel_description_override=reel_description_override,
    )


class PostExecutor:
    """API gọn cho worker / test gọi pipeline đăng."""

    def run_for_account(
        self,
        account_id: str,
        *,
        entity_id: str | None = None,
        page_id: str | None = None,
        draft_id: str | None = None,
        schedule_post_job_id: str | None = None,
        accounts: AccountsDatabaseManager | None = None,
        browser_pool: Any | None = None,
        headless: bool | None = None,
    ) -> bool:
        from src.scheduler import run_scheduled_post_for_account

        return run_scheduled_post_for_account(
            account_id,
            entity_id=entity_id,
            page_id=page_id,
            draft_id=draft_id,
            accounts=accounts,
            browser_pool=browser_pool,
            headless=headless,
            schedule_post_job_id=schedule_post_job_id,
        )
