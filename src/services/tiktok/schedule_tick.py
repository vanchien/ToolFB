"""
Quét job TikTok đến hạn — tái dùng ThreadPoolExecutor từ ``get_schedule_posts_dispatch_pool`` (scheduler.py)
và cùng chu kỳ ``SCHEDULE_POSTS_POLL_SEC``; không nhân thêm process hay scheduler riêng.
"""

from __future__ import annotations

import os
import threading
from concurrent.futures import Future
from datetime import datetime, timezone
from typing import Any

from loguru import logger

from src.services.cross_platform_schedule_ctx import unified_chain_is_active
from src.scheduler import get_schedule_posts_dispatch_pool
from src.services.tiktok.account_lock import try_run_for_account
from src.services.tiktok.account_manager import TikTokAccountStore
from src.services.tiktok.job_manager import TikTokJobStore
from src.services.tiktok.upload_runner import run_tiktok_upload_job_sync

_tiktok_tick_lock = threading.Lock()


def tiktok_schedule_tick_lock() -> threading.Lock:
    """Khóa đọc/ghi job TikTok. Khi gộp với Facebook: giữ ``_schedule_posts_tick_lock`` trước, rồi khóa này."""
    return _tiktok_tick_lock


def _parse_scheduled_at_utc_strict(raw: Any) -> datetime | None:
    """ISO 8601 → UTC; chuỗi rỗng / lỗi parse → None (không coi là đến hạn)."""
    s = str(raw or "").strip()
    if not s:
        return None
    s2 = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def parse_tiktok_scheduled_utc(raw: Any) -> datetime | None:
    """Public alias (dùng từ ``scheduler`` khi gộp lịch FB + TikTok)."""
    return _parse_scheduled_at_utc_strict(raw)


def run_tiktok_scheduled_slot_job(job_id: str) -> None:
    """Worker upload TikTok cho job đã chuyển ``running`` (dùng chung tick gộp hoặc tick TikTok)."""
    _tiktok_run_scheduled_job_impl(job_id)


def _tiktok_run_scheduled_job_impl(job_id: str) -> None:
    job_store = TikTokJobStore()
    acc_store = TikTokAccountStore()
    job = job_store.get_by_id(job_id)
    if not job:
        return
    if str(job.get("status", "")).strip().lower() != "running":
        return
    aid = str(job.get("account_id", "")).strip()
    acc = acc_store.get_by_id(aid) if aid else None
    if not acc:
        cur = dict(job)
        cur["status"] = "failed"
        cur["error_message"] = "Không tìm thấy tài khoản TikTok."
        cur["step"] = "VALIDATE"
        job_store.upsert(cur)
        return

    j = dict(job)

    def patch(p: dict[str, Any]) -> None:
        cur2 = job_store.get_by_id(job_id)
        if not cur2:
            return
        cur2.update(p)
        job_store.upsert(cur2)

    def log_tt(msg: str) -> None:
        logger.info(msg)

    def inner() -> None:
        run_tiktok_upload_job_sync(j, dict(acc), log=log_tt, patch_job=patch)

    ok, msg = try_run_for_account(aid, inner)
    if not ok:
        patch({"status": "pending", "step": "WAIT_ACCOUNT_LOCK", "error_message": msg})


def _tiktok_worker_done(fut: Future[Any]) -> None:
    try:
        fut.result()
    except Exception as exc:  # noqa: BLE001
        logger.exception("[TikTok queue] Worker lỗi: {}", exc)
    if unified_chain_is_active():
        return
    if os.environ.get("SCHEDULE_DRAIN_QUEUE_ON_DONE", "1").strip().lower() not in {"1", "true", "yes", "on"}:
        return
    try:
        tick_tiktok_upload_jobs()
    except Exception as exc:  # noqa: BLE001
        logger.debug("[TikTok queue] Drain tiktok: {}", exc)
    try:
        from src.scheduler import tick_schedule_post_jobs

        tick_schedule_post_jobs()
    except Exception as exc:  # noqa: BLE001
        logger.debug("[TikTok queue] Drain schedule_posts: {}", exc)


def tick_tiktok_upload_jobs() -> None:
    """
    Job ``pending`` + ``schedule_enabled`` + ``scheduled_at`` (ISO UTC) đến hạn → ``running`` → pool worker.
    Cần «Bắt đầu lịch» (APScheduler) như job Facebook trong ``schedule_posts.json``.
    """
    jobs_to_dispatch: list[tuple[str, float]] = []
    now = datetime.now(timezone.utc)
    with _tiktok_tick_lock:
        job_store = TikTokJobStore()
        try:
            rows = job_store.load_all()
        except Exception as exc:  # noqa: BLE001
            logger.debug("TikTok tick: không đọc jobs: {}", exc)
            return
        for job in rows:
            if str(job.get("status", "")).strip().lower() != "pending":
                continue
            if not bool(job.get("schedule_enabled")):
                continue
            raw = str(job.get("scheduled_at") or job.get("schedule_time") or "").strip()
            when = _parse_scheduled_at_utc_strict(raw)
            if when is None:
                continue
            if when > now:
                continue
            jid = str(job.get("id", "")).strip()
            aid = str(job.get("account_id", "")).strip()
            if not jid or not aid:
                continue
            fresh = job_store.get_by_id(jid)
            if not fresh or str(fresh.get("status", "")).strip().lower() != "pending":
                continue
            if not bool(fresh.get("schedule_enabled")):
                continue
            merged = dict(fresh)
            merged["status"] = "running"
            merged["step"] = "DISPATCHED"
            try:
                job_store.upsert(merged)
            except Exception as exc:  # noqa: BLE001
                logger.warning("TikTok job {} không chuyển running: {}", jid, exc)
                continue
            waited_seconds = max(0.0, (now - when).total_seconds())
            jobs_to_dispatch.append((jid, waited_seconds))
    if not jobs_to_dispatch:
        return
    jobs_to_dispatch.sort(key=lambda x: -x[1])
    pool = get_schedule_posts_dispatch_pool()
    logger.info("[TikTok queue] Dispatch {} job(s) đến hạn.", len(jobs_to_dispatch))
    for jid, waited in jobs_to_dispatch:
        logger.info("[TikTok queue] Submit job={} | waited={:.1f}s", jid, waited)
        fut = pool.submit(run_tiktok_scheduled_slot_job, jid)
        fut.add_done_callback(_tiktok_worker_done)
