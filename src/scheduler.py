"""
Điều phối lịch đăng bài: APScheduler + giới hạn 3 trình duyệt đồng thời + fail-safe log.

Đọc ``config/accounts.json`` và ``config/pages.json``; job queue có thể ghi đè AI/lịch theo từng job.
"""

from __future__ import annotations

import json
import os
import shutil
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from src.ai.content_creator import generate_post
from src.automation.browser_factory import (
    BrowserFactory,
    _project_root,
    account_use_proxy_enabled,
    apply_viewport_from_env_to_page,
    sync_close_persistent_context,
)
from src.automation.facebook_actions import (
    _facebook_url_points_at_surface,
    entity_dict_from_pages_row,
    facebook_urls_align_as_target_surface,
    register_view_only_page_hooks,
)
from src.services.job_post_runtime import (
    STEP_LOAD_ACCOUNT,
    STEP_OPEN_BROWSER,
    STEP_VALIDATE_ACCOUNT,
    STEP_VALIDATE_JOB,
    STEP_VALIDATE_PAGE,
    JobRunTracker,
    format_post_job_error,
    job_run_monitor_path,
    log_job_step,
    validate_account_for_post_job,
    validate_page_for_post_job,
    validate_queue_job_payload,
)
from src.services.post_executor import capture_failure_screenshot, execute_facebook_post_sequence
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.posting_browser import resolve_posting_browser_engine
from src.utils.drafts_store import load_draft, save_draft
from src.utils.entities_manager import get_default_entities_manager
from src.utils.page_schedule import parse_page_schedule_for_apscheduler
from src.utils.pages_manager import get_default_pages_manager
from src.utils.reel_thumbnail_choice import normalize_reel_thumbnail_choice
from src.utils.schedule_job_content import (
    compute_next_daily_scheduled_utc_iso,
    merge_queue_job_content_into_page_row,
    prepare_queue_job_post_fields,
    strip_image_note_from_text,
)
from src.utils.schedule_posts_manager import get_default_schedule_posts_manager
from src.utils.account_proxy_mapper import prepare_account_dict_for_browser_run
from src.utils.proxy_check import verify_browser_facebook_via_proxy
from src.services.cross_platform_schedule_ctx import unified_chain_is_active
from src.services.schedule_queue_dispatcher import (
    compute_smart_delay_ms,
    count_pending_due,
    ensure_schedule_queue_recovery,
    note_job_dispatched,
    promote_due_pending_to_ready,
    select_dispatch_batch,
    sort_queue_jobs,
    write_queue_monitor,
)

_schedule_posts_tick_lock = threading.Lock()
_schedule_dispatch_lock = threading.Lock()
_schedule_dispatch_pool: ThreadPoolExecutor | None = None
_dispatch_pending_lock = threading.Lock()
_dispatch_pending_by_engine: dict[str, int] = {"firefox": 0, "chromium": 0, "webkit": 0}
_queue_next_due_hint_utc: datetime | None = None
_queue_prefetched_until_iso_by_job: dict[str, str] = {}
_queue_idle_probe_after_utc: datetime | None = None
_queue_hint_refresh_after_utc: datetime | None = None
_account_run_lock = threading.Lock()
_account_run_inflight: dict[str, int] = {}
_page_run_lock = threading.Lock()
_page_run_inflight: dict[str, int] = {}


def _cpu_count_safe() -> int:
    """Số CPU logic an toàn (fallback 4)."""
    try:
        return max(1, int(os.cpu_count() or 4))
    except Exception:
        return 4


def _auto_browser_concurrency_default() -> int:
    """
    Tự chọn BROWSER_CONCURRENCY theo CPU (không cần chỉnh tay).
    """
    cpu = _cpu_count_safe()
    # Máy 4 core -> 2 slot; 8 core -> 4 slot; chặn trần 8 để tránh ngốn RAM.
    return max(2, min(8, cpu // 2))


def _auto_scheduler_pool_threads_default() -> int:
    """
    Tự chọn số worker của APScheduler theo mức browser concurrency.
    """
    c = _auto_browser_concurrency_default()
    # Ưu tiên nhẹ RAM hơn: giảm trần worker, vẫn đủ cho tác vụ I/O.
    return max(4, min(16, c * 3))


def _auto_aging_defaults() -> tuple[float, float]:
    """
    Tự chọn tham số aging (window_sec, max_boost) theo mức concurrency.
    """
    c = float(_auto_browser_concurrency_default())
    window = max(12.0, 36.0 - c * 3.0)
    max_boost = min(5.0, 2.0 + c * 0.5)
    return window, max_boost


def _schedule_dispatch_workers_default() -> int:
    """
    Số worker cho dispatcher queue-job (tách khỏi APScheduler worker).

    Returns:
        Số worker >= 1.
    """
    raw = os.environ.get("SCHEDULE_POSTS_DISPATCH_WORKERS", "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            logger.warning(
                "SCHEDULE_POSTS_DISPATCH_WORKERS={!r} không hợp lệ, fallback theo BROWSER_CONCURRENCY.",
                raw,
            )
    try:
        b = int(os.environ.get("BROWSER_CONCURRENCY", "").strip() or _auto_browser_concurrency_default())
        return max(1, b)
    except ValueError:
        return _auto_browser_concurrency_default()


def get_schedule_posts_dispatch_pool() -> ThreadPoolExecutor:
    """
    Trả về pool worker dùng để chạy song song các job đến hạn từ ``schedule_posts.json``.
    """
    global _schedule_dispatch_pool
    with _schedule_dispatch_lock:
        if _schedule_dispatch_pool is None:
            workers = _schedule_dispatch_workers_default()
            _schedule_dispatch_pool = ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="fb_sched_dispatch",
            )
            logger.info(
                "Khởi tạo queue dispatcher: {} worker (auto hoặc env SCHEDULE_POSTS_DISPATCH_WORKERS).",
                workers,
            )
        return _schedule_dispatch_pool


def _mark_dispatch_submitted(engine: str) -> None:
    """Tăng backlog đang chờ/chạy cho engine."""
    ek = str(engine).strip().lower()
    if ek not in _dispatch_pending_by_engine:
        return
    with _dispatch_pending_lock:
        _dispatch_pending_by_engine[ek] = _dispatch_pending_by_engine.get(ek, 0) + 1


def _mark_dispatch_done(engine: str) -> None:
    """Giảm backlog cho engine khi future hoàn tất."""
    ek = str(engine).strip().lower()
    if ek not in _dispatch_pending_by_engine:
        return
    with _dispatch_pending_lock:
        _dispatch_pending_by_engine[ek] = max(0, _dispatch_pending_by_engine.get(ek, 0) - 1)


def _dispatch_load_score(engine: str) -> int:
    """
    Điểm tải hiện tại của engine (thấp hơn = ưu tiên dispatch trước).
    """
    ek = str(engine).strip().lower()
    with _dispatch_pending_lock:
        return int(_dispatch_pending_by_engine.get(ek, 0))


def _dispatch_priority_key(engine: str, *, waited_seconds: float) -> tuple[float, float]:
    """
    Tính key ưu tiên dispatch:
    - engine đang nhẹ hơn được ưu tiên (load thấp).
    - job chờ càng lâu càng được boost (aging) để tránh starvation.
    """
    load = float(_dispatch_load_score(engine))
    auto_window, auto_max = _auto_aging_defaults()
    raw_window = os.environ.get("DISPATCH_AGING_WINDOW_SEC", str(auto_window)).strip()
    raw_max = os.environ.get("DISPATCH_AGING_MAX_BOOST", str(auto_max)).strip()
    try:
        aging_window_sec = max(1.0, float(raw_window))
    except ValueError:
        aging_window_sec = 30.0
    try:
        aging_max_boost = max(0.0, float(raw_max))
    except ValueError:
        aging_max_boost = 3.0
    aging_boost = min(aging_max_boost, max(0.0, waited_seconds) / aging_window_sec)
    effective = load - aging_boost
    # sort tăng dần: effective thấp hơn sẽ đi trước; tie-break bằng waited_seconds lớn hơn.
    return (effective, -max(0.0, waited_seconds))


def _resolve_engine_for_account(
    account_id: str,
    *,
    accounts: AccountsDatabaseManager | None = None,
) -> str:
    """
    Resolve posting engine từ account để scheduler cân bằng queue theo engine.
    """
    mgr = accounts or AccountsDatabaseManager()
    acc = mgr.get_by_id(account_id)
    if not acc:
        return "firefox"
    return resolve_posting_browser_engine(dict(acc))


def _log_dispatch_done(
    fut: Future[bool],
    *,
    job_id: str,
    account_id: str,
    page_id: str,
    engine: str,
) -> None:
    """Ghi log khi một job dispatch hoàn tất."""
    _mark_dispatch_done(engine)
    try:
        ok = fut.result()
        logger.info(
            "[Queue dispatcher] Hoàn tất job={} account={} page={} | engine={} | ok={}",
            job_id,
            account_id,
            page_id,
            engine,
            ok,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "[Queue dispatcher] Job={} account={} page={} | engine={} lỗi ngoài dự kiến: {}",
            job_id,
            account_id,
            page_id,
            engine,
            exc,
        )
    if unified_chain_is_active():
        return
    # Job vừa xong -> thử đẩy ngay hàng đợi due đang chờ slot, không phải đợi poll interval kế tiếp.
    if os.environ.get("SCHEDULE_DRAIN_QUEUE_ON_DONE", "1").strip().lower() in {"1", "true", "yes", "on"}:
        try:
            tick_schedule_post_jobs()
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Queue dispatcher] Drain queue sau job done lỗi (bỏ qua): {}", exc)
        try:
            from src.services.tiktok.schedule_tick import tick_tiktok_upload_jobs

            tick_tiktok_upload_jobs()
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Queue dispatcher] Drain TikTok queue sau job done (bỏ qua): {}", exc)


def _cron_timezone() -> Any:
    """
    Múi giờ cho CronTrigger (mặc định ``Asia/Ho_Chi_Minh``, ghi đè bằng ``SCHEDULER_TZ``).

    Returns:
        ``zoneinfo.ZoneInfo`` hoặc ``None`` nếu không áp dụng được (APScheduler dùng local).
    """
    tz_name = os.environ.get("SCHEDULER_TZ", "Asia/Ho_Chi_Minh").strip()
    if not tz_name:
        return None
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(tz_name)
    except Exception:
        logger.warning("Không load được múi giờ SCHEDULER_TZ={!r}, dùng local.", tz_name)
        return None


def _failed_log_path() -> Path:
    """
    Đường dẫn file log tài khoản đăng thất bại.

    Returns:
        ``logs/failed_accounts.log``
    """
    p = _project_root() / "logs" / "failed_accounts.log"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def append_failed_account_log(account_id: str, message: str) -> None:
    """
    Ghi một dòng lỗi vào ``logs/failed_accounts.log`` (append UTF-8).

    Args:
        account_id: id tài khoản.
        message: Mô tả lỗi (một dòng hoặc nhiều dòng — sẽ được thay newline).
    """
    path = _failed_log_path()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    safe_msg = message.replace("\r", " ").replace("\n", " | ")
    line = f"{ts} | {account_id} | {safe_msg}\n"
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line)
    logger.warning("Đã ghi lỗi đăng bài vào {}", path)


class BrowserSlotPool:
    """
    Giới hạn số trình duyệt Playwright chạy đồng thời (mặc định 3) để tránh quá tải RAM.
    """

    def __init__(self, max_concurrent: int = 3) -> None:
        """
        Khởi tạo pool slot.

        Args:
            max_concurrent: Số trình duyệt tối đa cùng lúc (>=1).
        """
        if max_concurrent < 1:
            raise ValueError("max_concurrent phải >= 1")
        self._max = max_concurrent
        self._sem = threading.BoundedSemaphore(max_concurrent)
        self._engine_limits = self._resolve_engine_limits(max_concurrent)
        self._engine_sems: dict[str, threading.BoundedSemaphore] = {
            k: threading.BoundedSemaphore(v) for k, v in self._engine_limits.items()
        }
        self._engine_in_use: dict[str, int] = {k: 0 for k in self._engine_limits}
        self._lock = threading.Lock()
        self._in_use = 0

    @property
    def max_concurrent(self) -> int:
        """
        Số slot tối đa đã cấu hình.

        Returns:
            Giới hạn đồng thời.
        """
        return self._max

    @property
    def engine_limits(self) -> dict[str, int]:
        """Giới hạn slot theo engine (firefox/chromium/webkit)."""
        return dict(self._engine_limits)

    def _resolve_engine_limits(self, max_concurrent: int) -> dict[str, int]:
        """
        Đọc giới hạn theo engine từ env; nếu không có env thì tự cân bằng theo max_concurrent.

        Env hỗ trợ:
            - BROWSER_CONCURRENCY_FIREFOX
            - BROWSER_CONCURRENCY_CHROMIUM
            - BROWSER_CONCURRENCY_WEBKIT
        """

        def _read(name: str, auto_default: int) -> int:
            raw = os.environ.get(name, "").strip()
            if not raw:
                return auto_default
            try:
                n = int(raw)
            except ValueError:
                logger.warning("{}={!r} không hợp lệ, dùng {}.", name, raw, auto_default)
                return auto_default
            return max(1, min(max_concurrent, n))

        ff_auto = max(1, min(max_concurrent, int(round(max_concurrent * 0.7))))
        ch_auto = max(1, min(max_concurrent, int(round(max_concurrent * 0.7))))
        wk_auto = max(1, min(max_concurrent, int(round(max_concurrent * 0.4))))
        return {
            "firefox": _read("BROWSER_CONCURRENCY_FIREFOX", ff_auto),
            "chromium": _read("BROWSER_CONCURRENCY_CHROMIUM", ch_auto),
            "webkit": _read("BROWSER_CONCURRENCY_WEBKIT", wk_auto),
        }

    def acquire_slot(self, account_id: str, engine: str | None = None) -> None:
        """
        Chờ tới khi có slot trình duyệt rồi chiếm một slot.

        Args:
            account_id: id tài khoản (phục vụ log terminal).
            engine: ``firefox``/``chromium``/``webkit`` để áp giới hạn riêng theo loại browser.
        """
        ek = str(engine or "").strip().lower()
        if ek not in self._engine_sems:
            ek = ""
        logger.info(
            "[Hàng chờ trình duyệt] Tài khoản {} đang chờ slot (tối đa {} đồng thời)...",
            account_id,
            self._max,
        )
        if ek:
            logger.info(
                "[Hàng chờ engine] {} chờ engine={} (tối đa {} slot engine).",
                account_id,
                ek,
                self._engine_limits[ek],
            )
            self._engine_sems[ek].acquire()
        self._sem.acquire()
        with self._lock:
            self._in_use += 1
            free = self._max - self._in_use
            if ek:
                self._engine_in_use[ek] += 1
                e_in_use = self._engine_in_use[ek]
                e_free = self._engine_limits[ek] - e_in_use
            else:
                e_in_use = 0
                e_free = 0
        logger.info(
            "[Trình duyệt] Đã cấp slot cho {} — đang dùng {}/{} (còn {} slot).",
            account_id,
            self._in_use,
            self._max,
            free,
        )
        try:
            from src.utils.concurrency_runtime import reconcile_multi_task_limits

            reconcile_multi_task_limits(browser_slots_in_use=self._in_use)
        except Exception:
            pass
        if ek:
            logger.info(
                "[Engine] {} dùng engine={} {}/{} (còn {} slot engine).",
                account_id,
                ek,
                e_in_use,
                self._engine_limits[ek],
                e_free,
            )

    def release_slot(self, account_id: str, engine: str | None = None) -> None:
        """
        Trả một slot sau khi đóng trình duyệt.

        Args:
            account_id: id tài khoản (log).
            engine: ``firefox``/``chromium``/``webkit`` tương ứng slot engine đã acquire.
        """
        ek = str(engine or "").strip().lower()
        if ek not in self._engine_sems:
            ek = ""
        with self._lock:
            self._in_use = max(0, self._in_use - 1)
            free = self._max - self._in_use
            if ek:
                self._engine_in_use[ek] = max(0, self._engine_in_use[ek] - 1)
                e_in_use = self._engine_in_use[ek]
                e_free = self._engine_limits[ek] - e_in_use
            else:
                e_in_use = 0
                e_free = 0
        self._sem.release()
        if ek:
            self._engine_sems[ek].release()
        try:
            from src.utils.concurrency_runtime import reconcile_multi_task_limits

            reconcile_multi_task_limits(browser_slots_in_use=self._in_use)
        except Exception:
            pass
        logger.info(
            "[Trình duyệt] Đã giải phóng slot cho {} — đang dùng {}/{} (còn {} slot).",
            account_id,
            self._in_use,
            self._max,
            free,
        )
        if ek:
            logger.info(
                "[Engine] {} trả engine={} -> {}/{} (còn {} slot engine).",
                account_id,
                ek,
                e_in_use,
                self._engine_limits[ek],
                e_free,
            )


def _parse_schedule_hh_mm(value: str) -> tuple[int, int]:
    """
    Parse chuỗi ``HH:MM`` thành (hour, minute).

    Args:
        value: Chuỗi lịch trong JSON.

    Returns:
        Bộ giờ, phút 24h.

    Raises:
        ValueError: Định dạng không hợp lệ.
    """
    parts = str(value).strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"schedule_time không hợp lệ: {value!r}")
    h, m = int(parts[0]), int(parts[1])
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError(f"Giờ/phút ngoài phạm vi: {value!r}")
    return h, m


def _page_record_to_entity_dict(page: dict[str, Any]) -> dict[str, Any]:
    """Chuyển một bản ghi Page sang dict đích dùng cho ``go_to_posting_target_and_open_composer``."""
    return entity_dict_from_pages_row(page)


def _record_post_run_outcome(
    *,
    account_id: str,
    accounts_mgr: AccountsDatabaseManager,
    page_row: dict[str, Any] | None,
    used_entities_json: bool,
    success: bool,
) -> None:
    """
    Ghi nhận đăng thành công/thất bại: entity (``entities.json``) → cập nhật tài khoản;
    Page (``pages.json``) → cập nhật bản ghi Page; không có Page → chỉ tài khoản (tương thích cũ).
    """
    if used_entities_json:
        try:
            accounts_mgr.record_post_outcome(account_id, success=success)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không ghi outcome account (entity job, {}): {}", account_id, exc)
        return
    pid = str((page_row or {}).get("id", "")).strip()
    if pid:
        try:
            get_default_pages_manager().record_post_outcome(pid, success=success)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không ghi outcome Page {} ({}): {}", pid, account_id, exc)
        return
    try:
        accounts_mgr.record_post_outcome(account_id, success=success)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không ghi outcome account ({}): {}", account_id, exc)


def _maybe_append_post_history(
    *,
    page_row: dict[str, Any] | None,
    used_entities_json: bool,
    text_body: str,
    draft_media_paths: list[Path],
    schedule_post_job_id: str | None,
) -> None:
    """Ghi ``history/published_posts.json`` khi đăng Page thành công (không áp dụng entity-only)."""
    if used_entities_json:
        return
    pid = str((page_row or {}).get("id", "")).strip()
    if not pid:
        return
    try:
        from src.services.post_history_service import PostHistoryService

        job: dict[str, Any] = {}
        jid = str(schedule_post_job_id or "").strip()
        if jid:
            row = get_default_schedule_posts_manager().get_by_id(jid)
            if row:
                job = dict(row)
        hook = str(job.get("title") or (page_row or {}).get("topic") or "").strip()
        hashtags_raw = job.get("hashtags")
        hashtags = list(hashtags_raw) if isinstance(hashtags_raw, list) else []
        cta = str(job.get("cta") or "").strip()
        imgs: list[str] = []
        for p in draft_media_paths:
            try:
                if p.is_file():
                    imgs.append(str(p.resolve()))
            except OSError:
                imgs.append(str(p))
        PostHistoryService().append_entry(
            pid,
            hook=hook,
            caption=text_body,
            hashtags=hashtags,
            cta=cta,
            image_paths=imgs,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không ghi post history Page {}: {}", pid, exc)


def _select_page_for_scheduled_post(account_id: str, account_schedule: str) -> dict[str, Any] | None:
    """
    Chọn một Page trong ``pages.json`` thuộc ``account_id``.

    Ưu tiên bản ghi có ``schedule_time`` trùng với lịch tài khoản; nếu không có thì lấy Page đầu tiên.
    """
    try:
        rows = get_default_pages_manager().list_for_account(account_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không đọc pages.json để chọn Page đăng ({}): {}", account_id, exc)
        return None
    if not rows:
        return None
    acc_s = str(account_schedule).strip()
    if acc_s:
        for p in rows:
            if str(p.get("schedule_time", "")).strip() == acc_s:
                return dict(p)
    return dict(rows[0])


def _resolve_image_path(acc: dict[str, Any], page_row: dict[str, Any] | None = None) -> Path | None:
    """
    Trả về đường dẫn ảnh đính kèm nếu có cấu hình và file tồn tại.

    Args:
        acc: Bản ghi tài khoản.
        page_row: Page từ ``pages.json`` (ưu tiên ``post_image_path`` trên Page).

    Returns:
        Path tuyệt đối hoặc None nếu bỏ qua bước upload ảnh.
    """
    raw = None
    if page_row:
        raw = page_row.get("post_image_path")
    if not raw:
        raw = acc.get("post_image_path")
    if not raw:
        return None
    p = Path(str(raw))
    if not p.is_absolute():
        p = _project_root() / p
    p = p.resolve()
    if p.is_file():
        return p
    logger.warning("post_image_path không tồn tại, bỏ qua upload ảnh: {}", p)
    return None


def _build_body_and_draft_media(
    acc: dict[str, Any],
    draft_id: str | None,
    page_row: dict[str, Any] | None = None,
) -> tuple[str, list[Path]]:
    """
    Chuẩn bị nội dung: ``draft_id`` (JSON + media) hoặc AI.

    AI: ưu tiên ``topic`` / ``content_style`` trên Page (``pages.json``), sau đó tới tài khoản (legacy).

    Args:
        acc: Bản ghi từ ``accounts.json``.
        draft_id: id file ``*.draft.json`` hoặc None / rỗng để dùng AI.
        page_row: Page đang đăng (nếu có) — chủ đề AI theo từng Page.

    Returns:
        (chuỗi đăng, danh sách file media từ draft).

    Raises:
        ValueError: Draft không tồn tại.
    """
    if draft_id and str(draft_id).strip():
        did = str(draft_id).strip()
        rec = load_draft(did)
        if rec is None:
            raise ValueError(f"Không tìm thấy draft_id={did!r}")
        body = str(rec.get("body", "")).strip()
        text = body
        paths: list[Path] = []
        for rel in rec.get("media_paths") or []:
            p = Path(str(rel))
            if not p.is_absolute():
                p = _project_root() / p
            p = p.resolve()
            if p.is_file():
                paths.append(p)
            else:
                logger.warning("[Draft {}] Bỏ qua media không tồn tại: {}", did, p)
        return text, paths

    topic = str(
        (page_row or {}).get("topic")
        or (page_row or {}).get("page_name")
        or acc.get("topic")
        or acc.get("name")
        or "Cập nhật thông tin hữu ích"
    ).strip()
    style = (page_row or {}).get("content_style")
    if style is None or not str(style).strip():
        style = acc.get("content_style")
    style_str = str(style).strip() if style else None
    post = generate_post(topic, style=style_str)
    text_body = str(post.get("body", "")).strip()
    return text_body, []


def _strip_image_note_from_text(text: str) -> str:
    """Loại bỏ dòng chú thích ảnh dạng ``(Ảnh: ...)`` / ``(Image: ...)`` khỏi caption."""
    return strip_image_note_from_text(text)


def _compose_job_text_payload(text_body: str, queue_job: dict[str, Any] | None) -> str:
    """Tạo payload text cuối cùng để paste — dedupe title/content/hashtag một lần."""
    return prepare_queue_job_post_fields(queue_job, fallback_body=text_body)["caption_text"]


def _extract_reel_tags_from_queue_job(queue_job: dict[str, Any] | None, *, limit: int = 12) -> list[str]:
    """Tags Reel đã dedupe từ job queue."""
    if not queue_job:
        return []
    tags = prepare_queue_job_post_fields(queue_job).get("reel_tags") or []
    return list(tags)[:limit]


def _extract_reel_description_from_queue_job(queue_job: dict[str, Any] | None, fallback: str) -> str:
    """Mô tả Reel (title+content dedupe, không hashtag)."""
    if not queue_job:
        return str(fallback or "").strip()
    prepared = prepare_queue_job_post_fields(queue_job, fallback_body=fallback)
    if prepared.get("post_type") in {"video", "text_video", "reel"}:
        desc = str(prepared.get("reel_description") or "").strip()
        if desc:
            return desc
    return str(fallback or "").strip()


def _finalize_schedule_post_job_record(job_id: str | None, success: bool, error_note: str = "") -> None:
    """Cập nhật ``schedule_posts.json`` khi job queue gọi ``run_scheduled_post_for_account``."""
    if not job_id:
        return
    try:
        sp = get_default_schedule_posts_manager()
        jid = str(job_id).strip()
        now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        job = sp.get_by_id(jid)
        rc = int((job or {}).get("retry_count", 0))
        if success:
            rec = str((job or {}).get("schedule_recurrence", "")).strip().lower()
            slot = str((job or {}).get("schedule_slot", "")).strip()
            note = str(error_note or "")[:900]
            if rec == "daily" and slot:
                try:
                    nxt = compute_next_daily_scheduled_utc_iso(slot)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Job {}: không hẹn lại daily ({}), giữ success một lần.", jid, exc)
                    sp.update_job_fields(jid, status="success", posted_at=now_iso, error_note=note)
                else:
                    sp.update_job_fields(
                        jid,
                        status="pending",
                        posted_at=now_iso,
                        scheduled_at=nxt,
                        error_note=note,
                        retry_count=0,
                    )
            else:
                sp.update_job_fields(jid, status="success", posted_at=now_iso, error_note=note, retry_count=0)
        else:
            note = (error_note or "Đăng thất bại")[:900]
            manual = "need_manual_check" in note.lower()
            # Rule mới: lỗi thì retry tối đa 3 lần, mỗi lần cách nhau 5 phút.
            # Nếu 1 lần thành công thì dừng ngay (đã xử lý ở nhánh success phía trên).
            max_retry = 3
            retry_delay_min = 5
            new_rc = rc + 1
            if manual:
                sp.update_job_fields(jid, status="need_manual_check", error_note=note, retry_count=new_rc)
            elif new_rc <= max_retry:
                nxt = (datetime.now(timezone.utc) + timedelta(minutes=retry_delay_min)).replace(microsecond=0)
                nxt_iso = nxt.isoformat()
                sp.update_job_fields(
                    jid,
                    status="pending",
                    scheduled_at=nxt_iso,
                    error_note=note,
                    retry_count=new_rc,
                )
                logger.info(
                    "Job {} — thử lại ({}/{}), hẹn pending lúc {}",
                    jid,
                    new_rc,
                    max_retry,
                    nxt_iso,
                )
            else:
                sp.update_job_fields(jid, status="failed", error_note=note, retry_count=new_rc)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không cập nhật schedule_posts job {}: {}", job_id, exc)


def _parse_queue_job_scheduled_at(raw: Any) -> datetime:
    """ISO 8601 → UTC; chuỗi rỗng = coi như đến hạn ngay."""
    s = str(raw or "").strip()
    if not s:
        return datetime.now(timezone.utc)
    s2 = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


def _prefetch_window_seconds() -> int:
    """Số giây prefetch trước lịch đăng (mặc định 60s, giới hạn 10..900)."""
    raw = os.environ.get("SCHEDULE_POSTS_PREFETCH_SEC", "60").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 60
    return max(10, min(900, n))


def _idle_probe_seconds() -> int:
    """
    Khi không có pending job: chờ thêm một khoảng rồi mới quét lại file.
    Mặc định 300s để giảm I/O khi hệ thống đang rảnh.
    """
    raw = os.environ.get("SCHEDULE_POSTS_IDLE_PROBE_SEC", "300").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 300
    return max(30, min(3600, n))


def _hint_refresh_seconds() -> int:
    """
    Chu kỳ làm tươi hint lịch gần nhất để bắt kịp job mới được thêm/sửa.
    Mặc định 90s: đủ nhẹ nhưng vẫn phản ứng nhanh.
    """
    raw = os.environ.get("SCHEDULE_POSTS_HINT_REFRESH_SEC", "90").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 90
    return max(20, min(600, n))


def _draft_id_for_queue_job(job: dict[str, Any]) -> str:
    """draft_id có sẵn, hoặc tạo draft từ ``content`` + ``media_files`` (id cố định ``schj<id>``)."""
    jid = str(job.get("id", "")).strip()
    explicit = str(job.get("draft_id", "")).strip()
    if explicit and load_draft(explicit):
        return explicit
    body = str(job.get("content", "")).strip()
    media = job.get("media_files") or []
    paths: list[str] = [str(p).strip() for p in media if str(p).strip()]
    # Job video có thể không cần caption nhưng vẫn phải đính media vào draft.
    if not body and not paths:
        return ""
    did = f"schj{jid}"[:32]
    topic = str(job.get("title") or job.get("page_id") or "Post").strip()[:200]
    image_alt = str(job.get("image_alt", "")).strip()
    save_draft(topic=topic, body=body, image_alt=image_alt, media_paths=paths, draft_id=did)
    return did


def _parallel_same_account_enabled() -> bool:
    """Cho phép chạy nhiều job cùng account bằng profile runtime tách biệt."""
    raw = os.environ.get("SCHEDULE_ALLOW_SAME_ACCOUNT_PARALLEL", "0").strip().lower()
    if _per_account_parallel_limit() <= 1:
        return False
    return raw in {"1", "true", "yes", "on"}


def _per_account_parallel_limit() -> int:
    """Giới hạn số job đồng thời cho mỗi account (mặc định 1 browser/account)."""
    raw = os.environ.get("SCHEDULE_PER_ACCOUNT_MAX_PARALLEL", "1").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 1
    return max(1, min(4, n))


def _per_page_parallel_limit() -> int:
    """Giới hạn job đồng thời trên cùng một page_id (mặc định 1 — tránh lẫn Page khi đăng)."""
    raw = os.environ.get("SCHEDULE_PER_PAGE_MAX_PARALLEL", "1").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 1
    return max(1, min(4, n))


def _runtime_profile_root() -> Path:
    p = _project_root() / "data" / "runtime" / "parallel_profiles"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _acquire_account_run_slot(account_id: str) -> int:
    with _account_run_lock:
        cur = _account_run_inflight.get(account_id, 0) + 1
        _account_run_inflight[account_id] = cur
        return cur


def _account_inflight_count(account_id: str) -> int:
    with _account_run_lock:
        return int(_account_run_inflight.get(account_id, 0))


def _release_account_run_slot(account_id: str) -> None:
    with _account_run_lock:
        cur = max(0, _account_run_inflight.get(account_id, 0) - 1)
        if cur <= 0:
            _account_run_inflight.pop(account_id, None)
        else:
            _account_run_inflight[account_id] = cur


def _page_inflight_count(page_id: str) -> int:
    pid = str(page_id or "").strip()
    if not pid:
        return 0
    with _page_run_lock:
        return int(_page_run_inflight.get(pid, 0))


def _acquire_page_run_slot(page_id: str) -> int:
    pid = str(page_id or "").strip()
    if not pid:
        return 1
    with _page_run_lock:
        cur = _page_run_inflight.get(pid, 0) + 1
        _page_run_inflight[pid] = cur
        return cur


def _release_page_run_slot(page_id: str) -> None:
    pid = str(page_id or "").strip()
    if not pid:
        return
    with _page_run_lock:
        cur = max(0, _page_run_inflight.get(pid, 0) - 1)
        if cur <= 0:
            _page_run_inflight.pop(pid, None)
        else:
            _page_run_inflight[pid] = cur


def _prepare_account_for_parallel_run(
    *,
    account: dict[str, Any],
    account_id: str,
    schedule_post_job_id: str | None,
    run_slot: int,
) -> tuple[dict[str, Any], Path | None]:
    """
    Nếu cùng account chạy song song, dùng profile runtime riêng từng job để tránh lock profile gốc.
    """
    if run_slot <= 1 or not _parallel_same_account_enabled():
        return dict(account), None
    base = dict(account)
    jid = str(schedule_post_job_id or "").strip() or f"run{int(time.time())}"
    d = _runtime_profile_root() / str(account_id).strip() / f"job_{jid}_{run_slot}"
    d.mkdir(parents=True, exist_ok=True)
    base["portable_path"] = str(d)
    logger.info(
        "[Parallel account] account={} slot={} dùng runtime profile riêng: {}",
        account_id,
        run_slot,
        d,
    )
    return base, d


def tick_schedule_post_jobs() -> None:
    """
    Quét ``schedule_posts.json`` mỗi ``SCHEDULE_POSTS_POLL_SEC`` (mặc định 10s):

    1. Load jobs từ disk
    2. ``pending`` + đến ``scheduled_at`` → ``ready_queue`` (job trễ không bỏ)
    3. Sort ``ready_queue``: ``scheduled_at``, ``created_at``
    4. Dispatch: 1 browser/account; account busy → chờ tick sau
    """
    global _queue_next_due_hint_utc, _queue_idle_probe_after_utc, _queue_hint_refresh_after_utc
    accounts_mgr = AccountsDatabaseManager()
    now = datetime.now(timezone.utc)
    prefetch_sec = _prefetch_window_seconds()
    per_acc_limit = _per_account_parallel_limit()
    jobs_to_dispatch: list[tuple[str, str, str, str | None, str, float]] = []
    queued_waiting = 0
    smart_ms = 0

    with _schedule_posts_tick_lock:
        try:
            sp = get_default_schedule_posts_manager()
            ensure_schedule_queue_recovery(sp)
            jobs = sp.reload_from_disk()
        except Exception as exc:  # noqa: BLE001
            logger.debug("schedule_posts tick: không đọc được: {}", exc)
            return
        next_due: datetime | None = None
        for job in jobs:
            st = str(job.get("status", "")).strip().lower()
            if st not in {"pending", "ready_queue"}:
                continue
            when = _parse_queue_job_scheduled_at(job.get("scheduled_at"))
            if next_due is None or when < next_due:
                next_due = when
            if st != "pending":
                continue
            jid = str(job.get("id", "")).strip()
            if (
                jid
                and now < when <= now + timedelta(seconds=prefetch_sec)
                and _queue_prefetched_until_iso_by_job.get(jid) != str(job.get("scheduled_at", ""))
            ):
                try:
                    _draft_id_for_queue_job(dict(job))
                    _queue_prefetched_until_iso_by_job[jid] = str(job.get("scheduled_at", ""))
                except Exception as exc:  # noqa: BLE001
                    logger.debug("[Queue prefetch] job={} lỗi: {}", jid, exc)

        promote_due_pending_to_ready(sp, now=now)
        jobs = sp.reload_from_disk()

        ready_jobs = [
            dict(j)
            for j in jobs
            if str(j.get("status", "")).strip().lower() == "ready_queue"
        ]
        running_jobs = [
            dict(j)
            for j in jobs
            if str(j.get("status", "")).strip().lower() == "running"
        ]
        pending_due = count_pending_due(jobs, now=now)
        sorted_ready = sort_queue_jobs(ready_jobs)  # type: ignore[arg-type]

        batch = select_dispatch_batch(
            sorted_ready,  # type: ignore[arg-type]
            account_inflight=_account_inflight_count,
            per_account_limit=per_acc_limit,
        )

        jobs_to_dispatch = []
        queued_waiting = max(0, len(sorted_ready) - len(batch))
        for job in batch:
            jid = str(job.get("id", "")).strip()
            aid = str(job.get("account_id", "")).strip()
            pid = str(job.get("page_id", "")).strip()
            if not jid or not aid or not pid:
                continue
            fresh = sp.get_by_id(jid)
            if not fresh or str(fresh.get("status", "")).strip().lower() != "ready_queue":
                continue
            try:
                sp.update_job_fields(jid, status="running")
            except Exception as exc:  # noqa: BLE001
                logger.warning("Job {} không chuyển running: {}", jid, exc)
                continue
            did = _draft_id_for_queue_job(dict(job))
            engine = _resolve_engine_for_account(aid, accounts=accounts_mgr)
            when = _parse_queue_job_scheduled_at(job.get("scheduled_at"))
            waited_seconds = max(0.0, (now - when).total_seconds())
            jobs_to_dispatch.append((jid, aid, pid, did or None, engine, waited_seconds))
            note_job_dispatched(aid)

        inflight_map = {aid: _account_inflight_count(aid) for aid in {str(j.get("account_id", "")) for j in jobs if j.get("account_id")}}
        smart_ms = compute_smart_delay_ms(
            ready_count=len(sorted_ready),
            running_count=len(running_jobs) + len(jobs_to_dispatch),
        )
        write_queue_monitor(
            ready_jobs=sorted_ready,  # type: ignore[arg-type]
            running_jobs=running_jobs + [{"id": x[0], "account_id": x[1], "page_id": x[2]} for x in jobs_to_dispatch],  # type: ignore[arg-type]
            pending_due=pending_due,
            dispatched_ids=[x[0] for x in jobs_to_dispatch],
            account_inflight=inflight_map,
            smart_delay_ms=smart_ms,
            extra={"queued_waiting_account": queued_waiting},
        )

        _queue_next_due_hint_utc = next_due
        if next_due is None:
            _queue_idle_probe_after_utc = now + timedelta(seconds=_idle_probe_seconds())
        else:
            _queue_idle_probe_after_utc = None
        _queue_hint_refresh_after_utc = now + timedelta(seconds=_hint_refresh_seconds())

    if queued_waiting > 0:
        logger.info(
            "[Queue dispatcher] {} job ready_queue chờ account rảnh (không bỏ qua).",
            queued_waiting,
        )
    if not jobs_to_dispatch:
        return

    pool = get_schedule_posts_dispatch_pool()
    logger.info(
        "[Queue dispatcher] Dispatch {} job(s) từ ready_queue (sort schedule_time+created_at).",
        len(jobs_to_dispatch),
    )
    for jid, aid, pid, did, engine, waited_seconds in jobs_to_dispatch:
        _mark_dispatch_submitted(engine)
        logger.info(
            "[Queue dispatcher] Submit job={} account={} page={} | engine={} | waited={:.1f}s",
            jid,
            aid,
            pid,
            engine,
            waited_seconds,
        )
        if smart_ms > 0 and len(jobs_to_dispatch) > 1:
            time.sleep(min(smart_ms, 3000) / 1000.0)
        fut = pool.submit(
            run_scheduled_post_for_account,
            aid,
            page_id=pid,
            draft_id=did,
            schedule_post_job_id=jid,
        )
        fut.add_done_callback(
            lambda f, _jid=jid, _aid=aid, _pid=pid, _eng=engine: _log_dispatch_done(
                f,
                job_id=_jid,
                account_id=_aid,
                page_id=_pid,
                engine=_eng,
            )
        )


def _peek_facebook_due_pending() -> bool:
    now = datetime.now(timezone.utc)
    try:
        sp = get_default_schedule_posts_manager()
        for job in sp.load_all():
            st = str(job.get("status", "")).strip().lower()
            if st == "ready_queue":
                return True
            if st != "pending":
                continue
            when = _parse_queue_job_scheduled_at(job.get("scheduled_at"))
            if when <= now:
                jid = str(job.get("id", "")).strip()
                aid = str(job.get("account_id", "")).strip()
                pid = str(job.get("page_id", "")).strip()
                if jid and aid and pid:
                    return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("peek facebook due: {}", exc)
    return False


def _peek_tiktok_due_pending() -> bool:
    now = datetime.now(timezone.utc)
    try:
        from src.services.tiktok.job_manager import TikTokJobStore
        from src.services.tiktok.schedule_tick import parse_tiktok_scheduled_utc

        store = TikTokJobStore()
        for job in store.load_all():
            if str(job.get("status", "")).strip().lower() != "pending":
                continue
            if not bool(job.get("schedule_enabled")):
                continue
            raw = str(job.get("scheduled_at") or job.get("schedule_time") or "").strip()
            when = parse_tiktok_scheduled_utc(raw)
            if when is None or when > now:
                continue
            jid = str(job.get("id", "")).strip()
            aid = str(job.get("account_id", "")).strip()
            if jid and aid:
                return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("peek tiktok due: {}", exc)
    return False


def _tick_merged_serial_facebook_tiktok() -> None:
    """
    Cùng một cửa sổ poll: cả Facebook queue và TikTok đều có job đến hạn →
    sắp xếp một hàng (mặc định Facebook trước TikTok; env ``CROSS_PLATFORM_SCHEDULE_PRIORITY=tiktok_first`` đổi thứ tự),
    chạy tuần tự qua cùng pool — không bỏ qua job vì trùng giờ / tranh browser.
    """
    global _queue_next_due_hint_utc, _queue_idle_probe_after_utc, _queue_hint_refresh_after_utc
    from src.services.cross_platform_schedule_ctx import unified_chain_begin, unified_chain_end
    from src.services.tiktok.job_manager import TikTokJobStore
    from src.services.tiktok.schedule_tick import (
        _tiktok_worker_done,
        parse_tiktok_scheduled_utc,
        run_tiktok_scheduled_slot_job,
        tiktok_schedule_tick_lock,
    )

    accounts_mgr = AccountsDatabaseManager()
    now = datetime.now(timezone.utc)
    prefetch_sec = _prefetch_window_seconds()
    fb_batch: list[tuple[datetime, str, str, str, str | None, str, float]] = []
    tt_batch: list[tuple[datetime, str, float]] = []
    queued_due_count = 0

    with _schedule_posts_tick_lock:
        with tiktok_schedule_tick_lock():
            try:
                sp = get_default_schedule_posts_manager()
                jobs = sp.load_all()
            except Exception as exc:  # noqa: BLE001
                logger.debug("merged tick: không đọc schedule_posts: {}", exc)
                return
            promote_due_pending_to_ready(sp, now=now)
            jobs = sp.reload_from_disk()
            next_due: datetime | None = None
            per_acc_limit = _per_account_parallel_limit()
            for job in jobs:
                st = str(job.get("status", "")).strip().lower()
                if st not in ("pending", "ready_queue"):
                    continue
                when = _parse_queue_job_scheduled_at(job.get("scheduled_at"))
                if next_due is None or when < next_due:
                    next_due = when
                jid = str(job.get("id", "")).strip()
                if (
                    st == "pending"
                    and jid
                    and now < when <= now + timedelta(seconds=prefetch_sec)
                    and _queue_prefetched_until_iso_by_job.get(jid) != str(job.get("scheduled_at", ""))
                ):
                    try:
                        _draft_id_for_queue_job(dict(job))
                        _queue_prefetched_until_iso_by_job[jid] = str(job.get("scheduled_at", ""))
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("[Queue prefetch merged] job={} lỗi: {}", jid, exc)

            ready_jobs = [
                dict(j)
                for j in jobs
                if str(j.get("status", "")).strip().lower() == "ready_queue"
            ]
            sorted_ready = sort_queue_jobs(ready_jobs)  # type: ignore[arg-type]
            batch = select_dispatch_batch(
                sorted_ready,  # type: ignore[arg-type]
                account_inflight=_account_inflight_count,
                per_account_limit=per_acc_limit,
            )
            queued_due_count = max(0, len(sorted_ready) - len(batch))
            for job in batch:
                jid = str(job.get("id", "")).strip()
                aid = str(job.get("account_id", "")).strip()
                pid = str(job.get("page_id", "")).strip()
                if not jid or not aid or not pid:
                    continue
                fresh = sp.get_by_id(jid)
                if not fresh or str(fresh.get("status", "")).strip().lower() != "ready_queue":
                    continue
                when = _parse_queue_job_scheduled_at(job.get("scheduled_at"))
                did = _draft_id_for_queue_job(dict(job))
                engine = _resolve_engine_for_account(aid, accounts=accounts_mgr)
                waited_seconds = max(0.0, (now - when).total_seconds())
                fb_batch.append((when, jid, aid, pid, did or None, engine, waited_seconds))
                note_job_dispatched(aid)

            running_jobs = [
                dict(j)
                for j in jobs
                if str(j.get("status", "")).strip().lower() == "running"
            ]
            pending_due = count_pending_due(jobs, now=now)
            smart_ms = compute_smart_delay_ms(
                ready_count=len(sorted_ready),
                running_count=len(running_jobs) + len(fb_batch),
            )
            inflight_map = {
                aid: _account_inflight_count(aid)
                for aid in {str(j.get("account_id", "")) for j in jobs if j.get("account_id")}
            }
            write_queue_monitor(
                ready_jobs=sorted_ready,  # type: ignore[arg-type]
                running_jobs=running_jobs,
                pending_due=pending_due,
                dispatched_ids=[x[1] for x in fb_batch],
                account_inflight=inflight_map,
                smart_delay_ms=smart_ms,
                extra={"queued_waiting_account": queued_due_count, "mode": "merged_cross_platform"},
            )

            job_store = TikTokJobStore()
            try:
                rows = job_store.load_all()
            except Exception as exc:  # noqa: BLE001
                logger.debug("merged tick: không đọc TikTok jobs: {}", exc)
                rows = []
            for job in rows:
                if str(job.get("status", "")).strip().lower() != "pending":
                    continue
                if not bool(job.get("schedule_enabled")):
                    continue
                raw = str(job.get("scheduled_at") or job.get("schedule_time") or "").strip()
                tw = parse_tiktok_scheduled_utc(raw)
                if tw is None:
                    continue
                if next_due is None or tw < next_due:
                    next_due = tw
                if tw > now:
                    continue
                jid = str(job.get("id", "")).strip()
                aid = str(job.get("account_id", "")).strip()
                if not jid or not aid:
                    continue
                fresh_tt = job_store.get_by_id(jid)
                if not fresh_tt or str(fresh_tt.get("status", "")).strip().lower() != "pending":
                    continue
                if not bool(fresh_tt.get("schedule_enabled")):
                    continue
                waited_seconds = max(0.0, (now - tw).total_seconds())
                tt_batch.append((tw, jid, waited_seconds))

            _queue_next_due_hint_utc = next_due
            if next_due is None:
                _queue_idle_probe_after_utc = now + timedelta(seconds=_idle_probe_seconds())
            else:
                _queue_idle_probe_after_utc = None
            _queue_hint_refresh_after_utc = now + timedelta(seconds=_hint_refresh_seconds())

            fb_first = (
                os.environ.get("CROSS_PLATFORM_SCHEDULE_PRIORITY", "facebook_first").strip().lower() != "tiktok_first"
            )
            merged: list[Any] = []
            for when, jid, aid, pid, did, engine, waited in fb_batch:
                merged.append(("fb", when, jid, aid, pid, did, engine, waited))
            for tw, jid, waited in tt_batch:
                merged.append(("tt", tw, jid, waited))
            merged.sort(
                key=lambda x: (
                    x[1],
                    (0 if x[0] == "fb" else 1) if fb_first else (1 if x[0] == "fb" else 0),
                    x[2],
                )
            )

            chain_steps: list[Any] = []
            for item in merged:
                if item[0] == "fb":
                    _, when, jid, aid, pid, did, engine, waited = item
                    try:
                        sp.update_job_fields(jid, status="running")
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Merged: không chuyển running FB job {}: {}", jid, exc)
                        continue
                    chain_steps.append(("fb", jid, aid, pid, did, engine, waited))
                else:
                    _, tw, jid, waited = item
                    fresh2 = job_store.get_by_id(jid)
                    if not fresh2 or str(fresh2.get("status", "")).strip().lower() != "pending":
                        continue
                    row = dict(fresh2)
                    row["status"] = "running"
                    row["step"] = "DISPATCHED"
                    try:
                        job_store.upsert(row)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Merged: không chuyển running TikTok job {}: {}", jid, exc)
                        continue
                    chain_steps.append(("tt", jid, waited))

    if queued_due_count > 0:
        logger.info(
            "[Cross-platform queue] {} job FB đến hạn đang chờ slot account — vẫn xử lý phần gộp.",
            queued_due_count,
        )
    if not chain_steps:
        return

    pool = get_schedule_posts_dispatch_pool()
    n = len(chain_steps)
    logger.info(
        "[Cross-platform queue] Gộp FB+TikTok: chạy tuần tự {} bước (ưu tiên {}).",
        n,
        "Facebook trước" if fb_first else "TikTok trước",
    )

    unified_chain_begin()

    def drain_both() -> None:
        unified_chain_end()
        try:
            tick_schedule_post_jobs()
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Cross-platform queue] Drain FB: {}", exc)
        try:
            from src.services.tiktok.schedule_tick import tick_tiktok_upload_jobs

            tick_tiktok_upload_jobs()
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Cross-platform queue] Drain TikTok: {}", exc)

    def submit_one(idx: int) -> None:
        step = chain_steps[idx]
        kind = step[0]
        if kind == "fb":
            _, jid, aid, pid, did, engine, waited = step
            _mark_dispatch_submitted(engine)
            logger.info(
                "[Cross-platform queue] ({}/{}) FB job={} account={} page={} | waited={:.1f}s",
                idx + 1,
                n,
                jid,
                aid,
                pid,
                waited,
            )

            def on_fb_done(fut: Future[bool], i: int = idx) -> None:
                _log_dispatch_done(
                    fut,
                    job_id=jid,
                    account_id=aid,
                    page_id=pid,
                    engine=engine,
                )
                if i + 1 < n:
                    submit_one(i + 1)
                else:
                    drain_both()

            fut2 = pool.submit(
                run_scheduled_post_for_account,
                aid,
                page_id=pid,
                draft_id=did,
                schedule_post_job_id=jid,
            )
            fut2.add_done_callback(on_fb_done)
        else:
            _, jid, waited = step
            logger.info(
                "[Cross-platform queue] ({}/{}) TikTok job={} | waited={:.1f}s",
                idx + 1,
                n,
                jid,
                waited,
            )

            def on_tt_done(fut: Future[Any], i: int = idx) -> None:
                _tiktok_worker_done(fut)
                if i + 1 < n:
                    submit_one(i + 1)
                else:
                    drain_both()

            fut3 = pool.submit(run_tiktok_scheduled_slot_job, jid)
            fut3.add_done_callback(on_tt_done)

    try:
        submit_one(0)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[Cross-platform queue] Lỗi khởi chạy chuỗi: {}", exc)
        drain_both()


def tick_cross_platform_schedule_jobs() -> None:
    """
    Một tick cho cả ``schedule_posts.json`` và TikTok: nếu **cùng lúc** cả hai nền tảng có job đến hạn,
    chạy tuần tự có thứ tự ưu tiên; không thì giữ hành vi song song như trước.
    """
    if _peek_facebook_due_pending() and _peek_tiktok_due_pending():
        _tick_merged_serial_facebook_tiktok()
    else:
        tick_schedule_post_jobs()
        try:
            from src.services.tiktok.schedule_tick import tick_tiktok_upload_jobs

            tick_tiktok_upload_jobs()
        except Exception as exc:  # noqa: BLE001
            logger.warning("tick TikTok: {}", exc)


def run_scheduled_post_for_account(
    account_id: str,
    *,
    entity_id: str | None = None,
    page_id: str | None = None,
    draft_id: str | None = None,
    accounts: AccountsDatabaseManager | None = None,
    browser_pool: BrowserSlotPool | None = None,
    headless: bool | None = None,
    schedule_post_job_id: str | None = None,
    force_post_now: bool = False,
) -> bool:
    """
    Pipeline một lượt đăng: nội dung (draft hoặc AI) → (chờ slot) → profile/proxy → cookie → đích đăng → đăng.

    Thất bại ở bất kỳ bước nào: ghi ``failed_accounts.log``, **không** ném ra ngoài để lịch tiếp tục.

    Args:
        account_id: id trong accounts.json.
        entity_id: id trong ``entities.json`` (Page/Group); None hoặc rỗng = dùng ``pages.json``.
        page_id: id Page trong ``pages.json`` (cron theo lịch Page); ưu tiên hơn chọn Page mặc định.
        draft_id: id bản thảo trong ``data/drafts/``; None hoặc rỗng = sinh nội dung bằng AI.
            Nếu có ``schedule_post_job_id`` và job queue có ``content``/``media_files`` thì sẽ tự tạo draft
            (cùng logic ``tick_schedule_post_jobs``) để «Đăng luôn» vẫn đính được video.
        accounts: Manager JSON (mặc định khởi tạo mới).
        browser_pool: Pool giới hạn trình duyệt (mặc định singleton 3 slot).
        headless: Ghi đè headless; None lấy từ env ``HEADLESS`` (mặc định true).
        schedule_post_job_id: Nếu có — cập nhật trạng thái job trong ``schedule_posts.json``.
        force_post_now: True khi «Đăng luôn» — luôn ``Share now`` ở wizard Reel (không dùng lịch FB).

    Luồng đăng (``posting_engine``) lấy từ ``browser_type`` tài khoản: Firefox → nhánh Firefox trong
    ``execute_facebook_post_sequence``; Chrome/Chromium → nhánh Chromium.

    Returns:
        True nếu đăng (bấm Post) thành công; False nếu không.
    """
    outcome_ok = False
    err_msg = ""
    runtime_profile_dir: Path | None = None
    account_run_slot = 0
    page_run_slot = 0
    page_slot_id = ""
    try:
        mgr = accounts or AccountsDatabaseManager()
        pool = browser_pool or get_default_browser_pool()
        acc = mgr.get_by_id(account_id)
        if acc is None:
            append_failed_account_log(account_id, "Không tìm thấy trong accounts.json")
            logger.error("Bỏ qua job: không có account {}", account_id)
            err_msg = format_post_job_error(
                "account",
                f"account_id={account_id} không có trong config/accounts.json "
                f"(sau cập nhật: chạy «Di chuyển dữ liệu» hoặc thêm lại tài khoản).",
            )
            return False
        account_run_slot = _acquire_account_run_slot(account_id)
        session_status_before = str(acc.get("session_status") or "active").strip() or "active"
        try:
            log_job_step(STEP_LOAD_ACCOUNT, "Chuẩn bị tài khoản (registry, profile, proxy).", account_id=account_id)
            acc_prepared = prepare_account_dict_for_browser_run(dict(acc))
        except ValueError as exc:
            msg = format_post_job_error("account", str(exc))
            append_failed_account_log(account_id, f"Chuẩn bị account: {msg}")
            logger.error("Chuẩn bị account thất bại: {}", exc)
            err_msg = msg
            return False
        acc_runtime, runtime_profile_dir = _prepare_account_for_parallel_run(
            account=acc_prepared,
            account_id=account_id,
            schedule_post_job_id=schedule_post_job_id,
            run_slot=account_run_slot,
        )

        try:
            log_job_step(STEP_VALIDATE_ACCOUNT, f"Kiểm tra tài khoản trước khi mở browser", account_id=account_id)
            validate_account_for_post_job(dict(acc_runtime))
        except ValueError as exc:
            msg = format_post_job_error("account", str(exc))
            append_failed_account_log(account_id, f"Validate account: {msg}")
            logger.error("Validate account thất bại: {}", exc)
            err_msg = msg
            return False

        name = str(acc_runtime.get("name", account_id))
        entity_dict: dict[str, Any] | None = None
        page_row: dict[str, Any] | None = None
        used_entities_json = False
        eid_raw = str(entity_id).strip() if entity_id else ""
        pid_raw = str(page_id).strip() if page_id else ""

        if eid_raw:
            used_entities_json = True
            ent = get_default_entities_manager().get_by_id(eid_raw)
            if ent is None:
                logger.warning(
                    "[Chuẩn bị đăng] id={} | entity_id={} không tồn tại — dùng timeline mặc định.",
                    account_id,
                    eid_raw,
                )
            elif str(ent.get("account_id", "")).strip() != str(account_id).strip():
                append_failed_account_log(
                    account_id,
                    f"Entity {eid_raw} không thuộc tài khoản này (account_id mismatch).",
                )
                logger.error("Bỏ qua job: entity không khớp account_id={}", account_id)
                err_msg = format_post_job_error(
                    "page",
                    f"entity_id={eid_raw} không thuộc account_id={account_id}.",
                )
                return False
            else:
                entity_dict = dict(ent)
        elif pid_raw:
            pr = get_default_pages_manager().get_by_id(pid_raw)
            if pr is None:
                append_failed_account_log(account_id, f"page_id={pid_raw} không tồn tại trong pages.json")
                logger.error("Bỏ qua job: không có page_id={}", pid_raw)
                err_msg = format_post_job_error(
                    "page",
                    f"page_id={pid_raw} không có trong config/pages.json.",
                )
                return False
            if str(pr.get("account_id", "")).strip() != str(account_id).strip():
                append_failed_account_log(
                    account_id,
                    f"Page {pid_raw} không thuộc tài khoản này (account_id mismatch).",
                )
                logger.error("Bỏ qua job: page không khớp account_id={}", account_id)
                err_msg = format_post_job_error(
                    "page",
                    f"page_id={pid_raw} không thuộc account_id={account_id}.",
                )
                return False
            page_row = dict(pr)
            entity_dict = _page_record_to_entity_dict(page_row)
            logger.info(
                "[Chuẩn bị đăng] id={} | pages.json page_id={} | page_name={!r}",
                account_id,
                page_row.get("id"),
                page_row.get("page_name", ""),
            )
        else:
            page_row = _select_page_for_scheduled_post(account_id, str(acc_runtime.get("schedule_time", "")))
            if page_row:
                entity_dict = _page_record_to_entity_dict(page_row)
                logger.info(
                    "[Chuẩn bị đăng] id={} | pages.json page_id={} | page_name={!r}",
                    account_id,
                    page_row.get("id"),
                    page_row.get("page_name", ""),
                )

        queue_job: dict[str, Any] | None = None
        if schedule_post_job_id:
            try:
                qrow = get_default_schedule_posts_manager().get_by_id(str(schedule_post_job_id).strip())
                if qrow:
                    queue_job = dict(qrow)
            except Exception:  # noqa: BLE001
                queue_job = None
        content_page_row = merge_queue_job_content_into_page_row(page_row, queue_job)
        page_slot_id = str((page_row or content_page_row or {}).get("id") or pid_raw or "").strip()
        if page_slot_id:
            page_run_slot = _acquire_page_run_slot(page_slot_id)
            logger.info(
                "[Page slot] account={} page_id={} slot={}",
                account_id,
                page_slot_id,
                page_run_slot,
            )

        if queue_job:
            if str(queue_job.get("post_type", "")).strip().lower() == "reel":
                media = queue_job.get("media_files")
                has_media = isinstance(media, list) and any(str(x).strip() for x in media)
                if not has_media:
                    vp = str(queue_job.get("video_path", "")).strip()
                    if vp:
                        queue_job["media_files"] = [vp]
            qpid = str(queue_job.get("page_id", "")).strip()
            if qpid:
                prq = get_default_pages_manager().get_by_id(qpid)
                if prq and str(prq.get("account_id", "")).strip() == str(account_id).strip():
                    purl = str(prq.get("page_url", "")).strip()
                    eu_url = str((entity_dict or {}).get("target_url", "")).strip()
                    tt_low = str((entity_dict or {}).get("target_type", "")).strip().lower()
                    need = entity_dict is None
                    need |= not eu_url
                    need |= bool(
                        purl
                        and _facebook_url_points_at_surface(purl)
                        and tt_low == "timeline"
                    )
                    # Entity còn URL feed/home hoặc không cùng bề mặt với Page trong job → lấy lại từ pages.json.
                    need |= bool(
                        purl
                        and _facebook_url_points_at_surface(purl)
                        and eu_url
                        and not _facebook_url_points_at_surface(eu_url)
                    )
                    need |= bool(
                        purl
                        and _facebook_url_points_at_surface(purl)
                        and eu_url
                        and _facebook_url_points_at_surface(eu_url)
                        and not facebook_urls_align_as_target_surface(eu_url, purl)
                    )
                    if need:
                        page_row = dict(prq)
                        entity_dict = _page_record_to_entity_dict(page_row)
                        content_page_row = merge_queue_job_content_into_page_row(page_row, queue_job)
                        logger.info(
                            "[Chuẩn bị đăng] entity khôi phục từ job.page_id={} → target_url={!r}",
                            qpid,
                            entity_dict.get("target_url"),
                        )

        row_for_validate = content_page_row or page_row
        if row_for_validate:
            try:
                log_job_step(STEP_VALIDATE_PAGE, "Kiểm tra page (mapping, URL điều hướng).")
                validate_page_for_post_job(dict(row_for_validate), account_id)
            except ValueError as exc:
                msg = format_post_job_error("page", str(exc))
                append_failed_account_log(account_id, f"Validate page: {msg}")
                logger.error("Validate page thất bại: {}", exc)
                err_msg = msg
                return False
        if queue_job:
            try:
                log_job_step(STEP_VALIDATE_JOB, "Kiểm tra job (post_type, media_files).")
                validate_queue_job_payload(dict(queue_job))
            except ValueError as exc:
                msg = format_post_job_error("job", str(exc))
                append_failed_account_log(account_id, f"Validate job: {msg}")
                logger.error("Validate job thất bại: {}", exc)
                err_msg = msg
                return False

        resolved_draft_id = str(draft_id).strip() if draft_id else ""
        if not resolved_draft_id and queue_job:
            resolved_draft_id = _draft_id_for_queue_job(dict(queue_job))
            if resolved_draft_id:
                logger.info(
                    "[Chuẩn bị đăng] id={} | schedule_job={} | draft từ queue: {}",
                    account_id,
                    str(schedule_post_job_id or "").strip() or "—",
                    resolved_draft_id,
                )

        tgt = eid_raw or pid_raw or (str(page_row.get("id")) if page_row else "—")
        logger.info(
            "[Chuẩn bị đăng] id={} | name={} | đích={} | draft={} | nội dung...",
            account_id,
            name,
            tgt or "—",
            (resolved_draft_id or "AI"),
        )

        prepared: dict[str, Any] = {}
        try:
            text_body, draft_media_paths = _build_body_and_draft_media(
                acc_runtime, resolved_draft_id or None, page_row=content_page_row
            )
            prepared = prepare_queue_job_post_fields(queue_job, fallback_body=text_body)
            text_body = str(prepared.get("caption_text") or "").strip()
        except Exception as exc:  # noqa: BLE001
            append_failed_account_log(account_id, f"Nội dung: {exc!r}")
            logger.exception("Tài khoản {} — lỗi chuẩn bị nội dung.", account_id)
            err_msg = format_post_job_error("content", str(exc))
            try:
                _record_post_run_outcome(
                    account_id=account_id,
                    accounts_mgr=mgr,
                    page_row=page_row,
                    used_entities_json=used_entities_json,
                    success=False,
                )
            except Exception as exc2:  # noqa: BLE001
                logger.warning("Không ghi status=failed sau lỗi nội dung ({}): {}", account_id, exc2)
            return False

        use_headless = headless if headless is not None else __env_headless_default()
        # Per-job override từ schedule_posts.json ('hide_browser': inherit|hide|show).
        # Chỉ áp dụng khi caller không truyền headless tường minh — để «Đăng ngay» của GUI vẫn theo toggle.
        if headless is None and queue_job:
            raw_hb = str(queue_job.get("hide_browser") or "").strip().lower()
            if raw_hb == "hide":
                use_headless = True
                logger.info("[Đăng bài] {} — job yêu cầu ẩn browser (hide_browser=hide).", account_id)
            elif raw_hb == "show":
                use_headless = False
                logger.info("[Đăng bài] {} — job yêu cầu hiện browser (hide_browser=show).", account_id)
        posting_engine = resolve_posting_browser_engine(dict(acc_runtime))
        factory: BrowserFactory | None = None
        ctx = None
        page = None
        post_ok = False
        q_post_type = ""
        pool.acquire_slot(account_id, engine=posting_engine)
        try:
            try:
                from src.utils.playwright_browser_lock import enforce_bundled_browser_policy

                bundle_ok, bundle_msgs = enforce_bundled_browser_policy(project_root=_project_root())
                for bm in bundle_msgs:
                    if bundle_ok:
                        logger.warning("[Đăng bài] Trình duyệt bundle: {}", bm)
                    else:
                        logger.error("[Đăng bài] Trình duyệt bundle: {}", bm)
                if not bundle_ok:
                    raise RuntimeError(
                        "Trình duyệt Playwright không khớp bản build. "
                        + " ".join(bundle_msgs[:3])
                    )
            except RuntimeError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("[Đăng bài] Không kiểm tra được bundle trình duyệt (bỏ qua): {}", exc)
            logger.info("[Đăng bài] {} — mở trình duyệt (headless={})...", account_id, use_headless)
            factory = BrowserFactory(headless=use_headless)
            ctx = factory.launch_persistent_context_from_account_dict(acc_runtime, headless=use_headless)
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            register_view_only_page_hooks(page)
            apply_viewport_from_env_to_page(page, playwright=factory.playwright)
            if account_use_proxy_enabled(acc_runtime):
                log_job_step(
                    STEP_OPEN_BROWSER,
                    "Kiểm tra proxy qua trình duyệt (Facebook).",
                    account_id=account_id,
                )
                ok_bf, px_bf = verify_browser_facebook_via_proxy(page)
                if not ok_bf:
                    raise RuntimeError(px_bf)
                logger.info("[Đăng bài] {} account={}", px_bf, account_id)
            ck_raw = acc_runtime.get("cookie_path")
            cookie_arg = str(ck_raw).strip() if ck_raw else None
            logger.info(
                "[Đăng bài] {} — posting_engine={} (browser_type={!r})",
                account_id,
                posting_engine,
                acc_runtime.get("browser_type"),
            )
            img = _resolve_image_path(acc_runtime, page_row=content_page_row)
            tracker: JobRunTracker | None = None
            if schedule_post_job_id:
                tracker = JobRunTracker(schedule_post_job_id)
                tracker.set_step(STEP_OPEN_BROWSER, "Đã mở trình duyệt / context")
            q_post_type = str(prepared.get("post_type") or "text").strip().lower()
            job_sched = str((queue_job or {}).get("scheduled_at", "")).strip() or None
            reel_title = str(prepared.get("title") or "").strip()
            reel_content = str(prepared.get("content") or "").strip()
            reel_tags = list(prepared.get("reel_tags") or [])
            reel_description = str(prepared.get("reel_description") or "").strip()
            reel_thumb_choice = normalize_reel_thumbnail_choice((queue_job or {}).get("reel_thumbnail_choice"))
            reel_video_path = str((queue_job or {}).get("video_path") or "").strip()
            execute_facebook_post_sequence(
                page,
                cookie_path=cookie_arg,
                entity_dict=entity_dict,
                pages_json_row=content_page_row or page_row,
                text_body=text_body,
                draft_media_paths=draft_media_paths,
                page_extra_image=img,
                post_type=q_post_type,
                tracker=tracker,
                force_share_now=bool(force_post_now),
                job_scheduled_at_iso=job_sched,
                posting_engine=posting_engine,
                reel_tags=reel_tags,
                reel_description_override=reel_description,
                reel_thumbnail_choice=reel_thumb_choice,
                reel_title=reel_title,
                reel_content=reel_content,
                reel_video_path=reel_video_path,
                account_record=acc_runtime,
            )
            post_ok = True
        except Exception as exc:  # noqa: BLE001
            append_failed_account_log(account_id, f"Đăng bài: {exc!r}")
            logger.exception("Tài khoản {} — đăng thất bại, tiếp tục hàng chờ khác.", account_id)
            logger.error("Lỗi đăng Facebook — đã thử chụp screenshot (nếu còn trang mở): {}", exc)
            capture_failure_screenshot(page, account_id)
            err_msg = format_post_job_error("post", str(exc))
            try:
                mon = job_run_monitor_path()
                if mon.is_file() and schedule_post_job_id:
                    data = json.loads(mon.read_text(encoding="utf-8"))
                    if str(data.get("job_id") or "").strip() == str(schedule_post_job_id).strip():
                        step = str(data.get("step") or "").strip()
                        sm = str(data.get("message") or "").strip()
                        if step and step not in err_msg:
                            err_msg = f"{err_msg} (bước cuối: {step}" + (f" — {sm}" if sm else "") + ")"
            except Exception:  # noqa: BLE001
                pass
        finally:
            keep_open = _keep_browser_open_after_post_debug()
            if keep_open:
                logger.warning(
                    "[FB debug] Giữ browser mở để kiểm tra sau Post. account_id={} | post_type={}",
                    account_id,
                    q_post_type or "unknown",
                )
            else:
                sync_close_persistent_context(ctx, log_label=account_id)
                if factory is not None:
                    try:
                        factory.close()
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Lỗi khi đóng BrowserFactory ({}): {}", account_id, exc)
            try:
                st = str(acc_runtime.get("session_status") or "").strip()
                if st and st != session_status_before:
                    mgr.update_account_fields(account_id, {"session_status": st})
                    logger.info("[Session] Đã lưu session_status={} account={}", st, account_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[Session] Không lưu session_status ({}): {}", account_id, exc)
            pool.release_slot(account_id, engine=posting_engine)

        if not post_ok and not err_msg:
            err_msg = "Đăng thất bại"

        if post_ok:
            try:
                _record_post_run_outcome(
                    account_id=account_id,
                    accounts_mgr=mgr,
                    page_row=page_row,
                    used_entities_json=used_entities_json,
                    success=True,
                )
                logger.info("[Hoàn tất] {} — đã ghi nhận đăng thành công.", account_id)
            except Exception as exc:  # noqa: BLE001
                append_failed_account_log(account_id, f"Cập nhật JSON: {exc!r}")
                logger.exception("Đăng thành công nhưng không ghi được status cho {}.", account_id)
            try:
                _maybe_append_post_history(
                    page_row=page_row,
                    used_entities_json=used_entities_json,
                    text_body=text_body,
                    draft_media_paths=draft_media_paths,
                    schedule_post_job_id=schedule_post_job_id,
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("Bỏ qua post history ({}): {}", account_id, exc)
        else:
            try:
                _record_post_run_outcome(
                    account_id=account_id,
                    accounts_mgr=mgr,
                    page_row=page_row,
                    used_entities_json=used_entities_json,
                    success=False,
                )
            except Exception as exc:  # noqa: BLE001
                append_failed_account_log(account_id, f"Cập nhật JSON (failed): {exc!r}")
                logger.exception("Không ghi được status=failed cho {}.", account_id)

        outcome_ok = post_ok
        return outcome_ok
    finally:
        if schedule_post_job_id:
            _finalize_schedule_post_job_record(schedule_post_job_id, outcome_ok, err_msg)
        if account_run_slot > 0:
            _release_account_run_slot(account_id)
        if page_run_slot > 0 and page_slot_id:
            _release_page_run_slot(page_slot_id)
        if runtime_profile_dir is not None:
            try:
                shutil.rmtree(runtime_profile_dir, ignore_errors=True)
            except Exception:
                pass


def __env_headless_default() -> bool:
    """
    Đọc biến môi trường ``HEADLESS`` (1/true → headless).

    Returns:
        True nếu chạy headless mặc định cho máy chủ 24/7.
    """
    v = os.environ.get("HEADLESS", "1").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _keep_browser_open_after_post_debug() -> bool:
    raw = str(os.environ.get("FB_KEEP_BROWSER_OPEN_AFTER_POST", "")).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    raw2 = str(os.environ.get("FB_REEL_PAUSE_AFTER_POST", "")).strip().lower()
    return raw2 in {"1", "true", "yes", "on"}


_default_pool: BrowserSlotPool | None = None
_default_pool_lock = threading.Lock()


def get_default_browser_pool() -> BrowserSlotPool:
    """
    Trả về pool trình duyệt mặc định (singleton), tự động theo CPU hoặc ``BROWSER_CONCURRENCY``.

    Returns:
        ``BrowserSlotPool`` dùng chung toàn process.
    """
    global _default_pool
    with _default_pool_lock:
        if _default_pool is None:
            raw = os.environ.get("BROWSER_CONCURRENCY", "").strip()
            if raw:
                try:
                    n = max(1, int(raw))
                except ValueError:
                    n = _auto_browser_concurrency_default()
                    logger.warning("BROWSER_CONCURRENCY={!r} không hợp lệ, tự động chọn {}.", raw, n)
            else:
                n = _auto_browser_concurrency_default()
                logger.info("Tự động chọn BROWSER_CONCURRENCY={} theo CPU={} logical cores.", n, _cpu_count_safe())
            _default_pool = BrowserSlotPool(max_concurrent=n)
        return _default_pool


def log_accounts_overview(accounts: AccountsDatabaseManager) -> None:
    """
    In tổng quan tài khoản + Page (lịch / trạng thái đăng) ra log terminal.

    Args:
        accounts: Manager đọc JSON.
    """
    rows = accounts.load_all()
    logger.info("——— Tổng quan tài khoản ({}) ———", len(rows))
    for acc in rows:
        logger.info("· id={} | name={}", acc.get("id"), acc.get("name"))
    show_pages = os.environ.get("STATUS_LOG_INCLUDE_PAGES", "0").strip().lower() in {"1", "true", "yes", "on"}
    if not show_pages:
        logger.info("——— Page / Group: (ẩn chi tiết, bật STATUS_LOG_INCLUDE_PAGES=1 để xem) ———")
        return
    try:
        pages = get_default_pages_manager().load_all()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không đọc pages.json cho log tổng quan: {}", exc)
        pages = []
    logger.info("——— Page / Group ({}) ———", len(pages))
    raw_lim = os.environ.get("PAGE_OVERVIEW_LOG_LIMIT", "8").strip()
    try:
        limit = max(0, int(raw_lim))
    except ValueError:
        limit = 8
    shown = 0
    for p in pages:
        if shown >= limit:
            break
        logger.info(
            "· page_id={} | owner={} | schedule={} | status={} | last={}",
            p.get("id"),
            p.get("account_id"),
            p.get("schedule_time", "—"),
            p.get("status", "pending"),
            p.get("last_post_at", "—"),
        )
        shown += 1
    if len(pages) > shown:
        logger.info("· ... và {} page khác (ẩn bớt để giảm log spam).", len(pages) - shown)
    pool = get_default_browser_pool()
    logger.info(
        "——— Giới hạn trình duyệt đồng thời: {} (slot) ———",
        pool.max_concurrent,
    )
    logger.info(
        "——— Giới hạn theo engine: firefox={} | chromium={} | webkit={} ———",
        pool.engine_limits.get("firefox", pool.max_concurrent),
        pool.engine_limits.get("chromium", pool.max_concurrent),
        pool.engine_limits.get("webkit", pool.max_concurrent),
    )


def build_scheduler(
    accounts: AccountsDatabaseManager | None = None,
    *,
    job: Callable[..., None] | None = None,
) -> BackgroundScheduler:
    """
    Tạo ``BackgroundScheduler``: ưu tiên một cron job mỗi **Page** có ``schedule_time``;
    nếu không có Page nào hợp lệ thì tạo job theo ``schedule_time`` trên tài khoản (tương thích cũ).

    Args:
        accounts: Manager JSON.
        job: Callable nhận ``account_id`` và tùy chọn ``page_id=...``; mặc định ``run_scheduled_post_for_account``.

    Returns:
        Scheduler đã add job nhưng **chưa** ``start``.
    """
    mgr = accounts or AccountsDatabaseManager()
    fn = job or run_scheduled_post_for_account
    raw_pool_threads = os.environ.get("SCHEDULER_POOL_THREADS", "").strip()
    if raw_pool_threads:
        try:
            pool_threads = max(4, int(raw_pool_threads))
        except ValueError:
            pool_threads = _auto_scheduler_pool_threads_default()
            logger.warning(
                "SCHEDULER_POOL_THREADS={!r} không hợp lệ, tự động chọn {}.",
                raw_pool_threads,
                pool_threads,
            )
    else:
        pool_threads = _auto_scheduler_pool_threads_default()
        logger.info("Tự động chọn SCHEDULER_POOL_THREADS={}.", pool_threads)
    executors = {
        "default": APSThreadPoolExecutor(
            max_workers=pool_threads,
            pool_kwargs={"thread_name_prefix": "fb_job"},
        )
    }
    scheduler = BackgroundScheduler(
        job_defaults={"coalesce": True, "max_instances": 1},
        executors=executors,
    )
    cron_tz = _cron_timezone()
    page_jobs = 0
    legacy_cron = os.environ.get("SCHEDULE_LEGACY_PAGE_CRON", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if legacy_cron:
        try:
            page_rows = get_default_pages_manager().load_all()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không đọc pages.json khi build scheduler: {}", exc)
            page_rows = []
        for p in page_rows:
            sch = str(p.get("schedule_time", "")).strip()
            if not sch:
                continue
            try:
                spec = parse_page_schedule_for_apscheduler(sch, tz=cron_tz)
            except ValueError:
                logger.warning("Bỏ qua page id={} — schedule_time không hợp lệ: {!r}", p.get("id"), sch)
                continue
            aid = str(p.get("account_id", "")).strip()
            pid = str(p.get("id", "")).strip()
            if not aid or not pid:
                continue
            if spec[0] == "cron":
                hh, mm = spec[1], spec[2]
                trigger = CronTrigger(hour=hh, minute=mm, timezone=cron_tz)
                logger.info("Đã đăng ký lịch {}:{} hàng ngày cho page_id={} (owner={})", hh, mm, pid, aid)
            else:
                run_at = spec[1]
                now = datetime.now(run_at.tzinfo) if run_at.tzinfo else datetime.now(timezone.utc)
                if run_at <= now:
                    logger.warning(
                        "Bỏ qua page id={} — lịch một lần đã qua ({}) so với hiện tại ({})",
                        p.get("id"),
                        run_at.isoformat(),
                        now.isoformat(),
                    )
                    continue
                trigger = DateTrigger(run_date=run_at)
                logger.info(
                    "Đã đăng ký lịch một lần {} cho page_id={} (owner={})",
                    run_at.isoformat(),
                    pid,
                    aid,
                )
            scheduler.add_job(
                fn,
                trigger,
                id=f"fb_post_page_{pid}",
                kwargs={"account_id": aid, "page_id": pid},
                replace_existing=True,
            )
            page_jobs += 1
    else:
        logger.info(
            "Lịch theo Page/Account cron tắt — chỉ chạy job schedule_posts theo scheduled_at "
            "(bật lại: SCHEDULE_LEGACY_PAGE_CRON=1)."
        )

    if legacy_cron and page_jobs == 0:
        logger.info("Không có Page nào có schedule_time — dùng lịch theo tài khoản (legacy).")
        for acc in mgr.load_all():
            aid = str(acc.get("id", "")).strip()
            if not aid:
                continue
            hh, mm = _parse_schedule_hh_mm(str(acc.get("schedule_time", "09:00")))
            trigger = CronTrigger(hour=hh, minute=mm, timezone=cron_tz)
            scheduler.add_job(
                fn,
                trigger,
                id=f"fb_post_{aid}",
                kwargs={"account_id": aid},
                replace_existing=True,
            )
            logger.info("Đã đăng ký lịch {}:{} hàng ngày cho account id={} (legacy)", hh, mm, aid)
    poll_sec = int(os.environ.get("SCHEDULE_POSTS_POLL_SEC", "10"))
    min_poll = 5
    if poll_sec >= min_poll:
        scheduler.add_job(
            tick_cross_platform_schedule_jobs,
            IntervalTrigger(seconds=poll_sec),
            id="cross_platform_schedule_tick",
            replace_existing=True,
        )
        logger.info(
            "Đã đăng ký quét lịch job queue (schedule_posts + TikTok) mỗi {} giây — "
            "pending→ready_queue→dispatch theo schedule_time.",
            poll_sec,
        )
    else:
        logger.info("Bỏ qua quét schedule_posts (SCHEDULE_POSTS_POLL_SEC={} < {}).", poll_sec, min_poll)
    return scheduler


def run_forever(
    *,
    accounts: AccountsDatabaseManager | None = None,
    status_interval_sec: int = 600,
    stop_event: threading.Event | None = None,
) -> None:
    """
    Khởi động scheduler nền và giữ process sống, định kỳ log tổng quan tài khoản.

    Args:
        accounts: Manager JSON.
        status_interval_sec: Chu kỳ (giây) giữa các lần log tổng quan (ghi đè bởi ``STATUS_LOG_INTERVAL_SEC``).
        stop_event: Nếu có, ``set()`` để thoát vòng lặp và tắt scheduler (dùng cho GUI / tích hợp).
    """
    mgr = accounts or AccountsDatabaseManager()
    interval = int(os.environ.get("STATUS_LOG_INTERVAL_SEC", str(status_interval_sec)))
    if interval < 10:
        interval = 10
    sched = build_scheduler(mgr)
    try:
        ensure_schedule_queue_recovery(get_default_schedule_posts_manager())
    except Exception as exc:  # noqa: BLE001
        logger.warning("Schedule queue recovery lỗi: {}", exc)
    log_accounts_overview(mgr)
    sched.start()
    logger.info(
        "APScheduler đã start (múi giờ cron: {}) — log tổng quan mỗi {}s. {}",
        os.environ.get("SCHEDULER_TZ", "Asia/Ho_Chi_Minh"),
        interval,
        "Dừng: Ctrl+C hoặc nút GUI." if stop_event is not None else "Dừng: Ctrl+C.",
    )
    try:
        while True:
            if stop_event is not None:
                if stop_event.wait(timeout=interval):
                    logger.info("Nhận tín hiệu dừng (stop_event).")
                    break
            else:
                time.sleep(interval)
            log_accounts_overview(mgr)
    except KeyboardInterrupt:
        logger.info("Đang shutdown scheduler (Ctrl+C)...")
    finally:
        try:
            sched.shutdown(wait=False)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Shutdown scheduler: {}", exc)
        logger.info("Scheduler đã dừng.")
