"""
Điều phối lịch đăng bài: APScheduler + giới hạn 3 trình duyệt đồng thời + fail-safe log.

Đọc ``config/accounts.json`` và ``config/pages.json``; job queue có thể ghi đè AI/lịch theo từng job.
"""

from __future__ import annotations

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
    STEP_OPEN_BROWSER,
    STEP_VALIDATE_ACCOUNT,
    STEP_VALIDATE_JOB,
    STEP_VALIDATE_PAGE,
    JobRunTracker,
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
from src.utils.schedule_job_content import compute_next_daily_scheduled_utc_iso, merge_queue_job_content_into_page_row
from src.utils.schedule_posts_manager import get_default_schedule_posts_manager

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
    # Job vừa xong -> thử đẩy ngay hàng đợi due đang chờ slot, không phải đợi poll interval kế tiếp.
    if os.environ.get("SCHEDULE_DRAIN_QUEUE_ON_DONE", "1").strip().lower() in {"1", "true", "yes", "on"}:
        try:
            tick_schedule_post_jobs()
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Queue dispatcher] Drain queue sau job done lỗi (bỏ qua): {}", exc)


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
    raw = str(text or "").strip()
    if not raw:
        return raw
    out_lines: list[str] = []
    for ln in raw.splitlines():
        s = ln.strip().lower()
        if s.startswith("(ảnh:") or s.startswith("(anh:") or s.startswith("(image:"):
            continue
        out_lines.append(ln)
    cleaned = "\n".join(out_lines).strip()
    return cleaned or raw


def _compose_job_text_payload(text_body: str, queue_job: dict[str, Any] | None) -> str:
    """Tạo payload text cuối cùng để paste: title + body + hashtags."""
    base = _strip_image_note_from_text(str(text_body or ""))
    if not queue_job:
        return base
    pt = str(queue_job.get("post_type", "")).strip().lower()
    if pt in {"video", "text_video"}:
        # Job video thuần: không fallback caption AI từ Page nếu job không có nội dung.
        # Chỉ dùng caption user nhập trực tiếp trong queue job (title/content).
        base = _strip_image_note_from_text(str(queue_job.get("content") or ""))
    title = str(queue_job.get("title") or "").strip()
    if title:
        # Tránh lặp title nếu body đã bắt đầu bằng title.
        if not base.lower().startswith(title.lower()):
            base = (title + "\n\n" + base).strip()
    # Với job video/reel, tags được nhập vào ô Tags riêng trong wizard -> không append vào mô tả.
    if pt in {"video", "text_video"}:
        return base
    raw = queue_job.get("hashtags")
    if not isinstance(raw, list):
        return base
    tags: list[str] = []
    for h in raw:
        s = str(h or "").strip()
        if not s:
            continue
        if not s.startswith("#"):
            s = "#" + s.lstrip("#")
        # Bỏ khoảng trắng trong hashtag để tránh FB tách sai.
        s = s.replace(" ", "")
        if s and s not in tags:
            tags.append(s)
    if not tags:
        return base
    joined = " ".join(tags)
    if joined.lower() in base.lower():
        return base
    return (base + "\n\n" + joined).strip()


def _extract_reel_tags_from_queue_job(queue_job: dict[str, Any] | None, *, limit: int = 12) -> list[str]:
    """Ưu tiên field `tags`; fallback `hashtags` để tương thích dữ liệu cũ."""
    if not queue_job:
        return []
    raw = queue_job.get("tags")
    if not isinstance(raw, list):
        raw = queue_job.get("hashtags")
    if isinstance(raw, str):
        raw = [x.strip() for x in raw.split(",") if x.strip()]
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for x in raw:
        s = str(x or "").strip().lstrip("#").strip()
        if not s:
            continue
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(s[:80])
        if len(out) >= limit:
            break
    return out


def _extract_reel_description_from_queue_job(queue_job: dict[str, Any] | None, fallback: str) -> str:
    """
    Với job video/reel, ưu tiên đúng tiêu đề + nội dung đã lưu trong job.
    Không ghép hashtag vào mô tả.
    """
    if not queue_job:
        return str(fallback or "").strip()
    pt = str(queue_job.get("post_type", "")).strip().lower()
    if pt not in {"video", "text_video"}:
        return str(fallback or "").strip()
    title = str(queue_job.get("title") or "").strip()
    content = _strip_image_note_from_text(str(queue_job.get("content") or ""))
    if title and content:
        return f"{title}\n\n{content}".strip()
    if title:
        return title
    if content:
        return content
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
    raw = os.environ.get("SCHEDULE_ALLOW_SAME_ACCOUNT_PARALLEL", "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _per_account_parallel_limit() -> int:
    """Giới hạn số job chạy đồng thời cho mỗi account."""
    raw = os.environ.get("SCHEDULE_PER_ACCOUNT_MAX_PARALLEL", "2").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 2
    return max(1, min(8, n))


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
    Quét ``schedule_posts.json``: job ``pending`` đến hạn → ``running`` → đăng (draft/AI).

    Chu kỳ gọi bởi APScheduler (``SCHEDULE_POSTS_POLL_SEC``). Dùng lock để tránh hai tick cùng chiếm một job.
    """
    global _queue_next_due_hint_utc, _queue_idle_probe_after_utc, _queue_hint_refresh_after_utc
    jobs_to_dispatch: list[tuple[str, str, str, str | None, str, float]] = []
    accounts_mgr = AccountsDatabaseManager()
    now = datetime.now(timezone.utc)
    prefetch_sec = _prefetch_window_seconds()
    hint = _queue_next_due_hint_utc
    idle_probe = _queue_idle_probe_after_utc
    hint_refresh = _queue_hint_refresh_after_utc
    if idle_probe is not None and now < idle_probe:
        return
    # Nếu còn xa lịch tiếp theo thì bỏ qua tick này để giảm I/O/CPU.
    if (
        hint is not None
        and now + timedelta(seconds=prefetch_sec) < hint
        and hint_refresh is not None
        and now < hint_refresh
    ):
        return
    with _schedule_posts_tick_lock:
        try:
            sp = get_default_schedule_posts_manager()
            jobs = sp.load_all()
        except Exception as exc:  # noqa: BLE001
            logger.debug("schedule_posts tick: không đọc được: {}", exc)
            return
        next_due: datetime | None = None
        account_planned: dict[str, int] = {}
        per_acc_limit = _per_account_parallel_limit()
        queued_due_count = 0
        for job in jobs:
            if str(job.get("status", "")).strip().lower() != "pending":
                continue
            when = _parse_queue_job_scheduled_at(job.get("scheduled_at"))
            if next_due is None or when < next_due:
                next_due = when
            jid = str(job.get("id", "")).strip()
            if (
                jid
                and now < when <= now + timedelta(seconds=prefetch_sec)
                and _queue_prefetched_until_iso_by_job.get(jid) != str(job.get("scheduled_at", ""))
            ):
                # Prefetch trước giờ chạy: dựng draft/cache media để đến giờ chỉ cần mở browser và đăng.
                try:
                    _draft_id_for_queue_job(dict(job))
                    _queue_prefetched_until_iso_by_job[jid] = str(job.get("scheduled_at", ""))
                    logger.debug(
                        "[Queue prefetch] job={} prefetch trước lịch {} ({}s).",
                        jid,
                        when.isoformat(),
                        prefetch_sec,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.debug("[Queue prefetch] job={} lỗi prefetch: {}", jid, exc)
            if when > now:
                continue
            aid = str(job.get("account_id", "")).strip()
            pid = str(job.get("page_id", "")).strip()
            if not jid or not aid or not pid:
                continue
            inflight = _account_inflight_count(aid)
            planned = account_planned.get(aid, 0)
            if (inflight + planned) >= per_acc_limit:
                queued_due_count += 1
                continue
            fresh = sp.get_by_id(jid)
            if not fresh or str(fresh.get("status", "")).strip().lower() != "pending":
                continue
            try:
                sp.update_job_fields(jid, status="running")
            except Exception as exc:  # noqa: BLE001
                logger.warning("Job {} không chuyển running: {}", jid, exc)
                continue
            did = _draft_id_for_queue_job(dict(job))
            engine = _resolve_engine_for_account(aid, accounts=accounts_mgr)
            waited_seconds = max(0.0, (now - when).total_seconds())
            jobs_to_dispatch.append((jid, aid, pid, did or None, engine, waited_seconds))
            account_planned[aid] = planned + 1
        _queue_next_due_hint_utc = next_due
        if next_due is None:
            _queue_idle_probe_after_utc = now + timedelta(seconds=_idle_probe_seconds())
        else:
            _queue_idle_probe_after_utc = None
        _queue_hint_refresh_after_utc = now + timedelta(seconds=_hint_refresh_seconds())
    if queued_due_count > 0:
        logger.info(
            "[Queue dispatcher] Có {} job đến hạn đang chờ slot (không bỏ qua). Sẽ tự đẩy tiếp khi có job hoàn tất.",
            queued_due_count,
        )
    if not jobs_to_dispatch:
        return
    # Ưu tiên engine đang có backlog thấp hơn để giảm nghẽn cục bộ.
    jobs_to_dispatch.sort(
        key=lambda item: _dispatch_priority_key(
            item[4],
            waited_seconds=item[5],
        )
    )
    pool = get_schedule_posts_dispatch_pool()
    logger.info(
        "[Queue dispatcher] Dispatch {} job(s) đến hạn (song song, ưu tiên engine rảnh).",
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
    try:
        mgr = accounts or AccountsDatabaseManager()
        pool = browser_pool or get_default_browser_pool()
        acc = mgr.get_by_id(account_id)
        if acc is None:
            append_failed_account_log(account_id, "Không tìm thấy trong accounts.json")
            logger.error("Bỏ qua job: không có account {}", account_id)
            err_msg = "Không tìm thấy trong accounts.json"
            return False
        account_run_slot = _acquire_account_run_slot(account_id)
        acc_runtime, runtime_profile_dir = _prepare_account_for_parallel_run(
            account=dict(acc),
            account_id=account_id,
            schedule_post_job_id=schedule_post_job_id,
            run_slot=account_run_slot,
        )

        try:
            log_job_step(STEP_VALIDATE_ACCOUNT, f"Kiểm tra tài khoản trước khi mở browser", account_id=account_id)
            validate_account_for_post_job(dict(acc_runtime))
        except ValueError as exc:
            msg = str(exc)[:900]
            append_failed_account_log(account_id, f"Validate account: {msg}")
            logger.error("Validate account thất bại: {}", exc)
            if schedule_post_job_id:
                _finalize_schedule_post_job_record(schedule_post_job_id, False, msg)
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
                err_msg = "entity account_id mismatch"
                return False
            else:
                entity_dict = dict(ent)
        elif pid_raw:
            pr = get_default_pages_manager().get_by_id(pid_raw)
            if pr is None:
                append_failed_account_log(account_id, f"page_id={pid_raw} không tồn tại trong pages.json")
                logger.error("Bỏ qua job: không có page_id={}", pid_raw)
                err_msg = "page_id không tồn tại"
                return False
            if str(pr.get("account_id", "")).strip() != str(account_id).strip():
                append_failed_account_log(
                    account_id,
                    f"Page {pid_raw} không thuộc tài khoản này (account_id mismatch).",
                )
                logger.error("Bỏ qua job: page không khớp account_id={}", account_id)
                err_msg = "page account_id mismatch"
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

        if queue_job:
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
                msg = str(exc)[:900]
                append_failed_account_log(account_id, f"Validate page: {msg}")
                logger.error("Validate page thất bại: {}", exc)
                if schedule_post_job_id:
                    _finalize_schedule_post_job_record(schedule_post_job_id, False, msg)
                return False
        if queue_job:
            try:
                log_job_step(STEP_VALIDATE_JOB, "Kiểm tra job (post_type, media_files).")
                validate_queue_job_payload(dict(queue_job))
            except ValueError as exc:
                msg = str(exc)[:900]
                append_failed_account_log(account_id, f"Validate job: {msg}")
                logger.error("Validate job thất bại: {}", exc)
                if schedule_post_job_id:
                    _finalize_schedule_post_job_record(schedule_post_job_id, False, msg)
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

        try:
            text_body, draft_media_paths = _build_body_and_draft_media(
                acc_runtime, resolved_draft_id or None, page_row=content_page_row
            )
            text_body = _compose_job_text_payload(text_body, queue_job)
        except Exception as exc:  # noqa: BLE001
            append_failed_account_log(account_id, f"Nội dung: {exc!r}")
            logger.exception("Tài khoản {} — lỗi chuẩn bị nội dung.", account_id)
            err_msg = f"Lỗi nội dung: {exc!r}"[:900]
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
        pool.acquire_slot(account_id, engine=posting_engine)
        try:
            logger.info("[Đăng bài] {} — mở trình duyệt (headless={})...", account_id, use_headless)
            factory = BrowserFactory(headless=use_headless)
            ctx = factory.launch_persistent_context_from_account_dict(acc_runtime, headless=use_headless)
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            register_view_only_page_hooks(page)
            apply_viewport_from_env_to_page(page, playwright=factory.playwright)
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
            q_post_type = str((queue_job or {}).get("post_type", "text")).strip().lower()
            job_sched = str((queue_job or {}).get("scheduled_at", "")).strip() or None
            reel_tags = _extract_reel_tags_from_queue_job(queue_job, limit=12) if q_post_type in {"video", "text_video"} else []
            reel_description = _extract_reel_description_from_queue_job(queue_job, text_body)
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
            )
            post_ok = True
        except Exception as exc:  # noqa: BLE001
            append_failed_account_log(account_id, f"Đăng bài: {exc!r}")
            logger.exception("Tài khoản {} — đăng thất bại, tiếp tục hàng chờ khác.", account_id)
            logger.error("Lỗi đăng Facebook — đã thử chụp screenshot (nếu còn trang mở): {}", exc)
            capture_failure_screenshot(page, account_id)
            err_msg = str(exc)[:900]
        finally:
            sync_close_persistent_context(ctx, log_label=account_id)
            if factory is not None:
                try:
                    factory.close()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Lỗi khi đóng BrowserFactory ({}): {}", account_id, exc)
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

    if page_jobs == 0:
        logger.info("Không có Page nào có schedule_time — dùng lịch theo tài khoản (tương thích cũ).")
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
            logger.info("Đã đăng ký lịch {}:{} hàng ngày cho account id={}", hh, mm, aid)
    poll_sec = int(os.environ.get("SCHEDULE_POSTS_POLL_SEC", "60"))
    if poll_sec >= 15:
        scheduler.add_job(
            tick_schedule_post_jobs,
            IntervalTrigger(seconds=poll_sec),
            id="fb_schedule_posts_tick",
            replace_existing=True,
        )
        logger.info("Đã đăng ký quét schedule_posts.json mỗi {} giây.", poll_sec)
    else:
        logger.info("Bỏ qua quét schedule_posts (SCHEDULE_POSTS_POLL_SEC={} < 15).", poll_sec)
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
