"""
Hàng đợi tạo Page: một job mỗi account, delay sau khi job kết thúc, sống sau restart.

Delay chỉ là nhịp điều phối nội bộ. Không dùng để né CAPTCHA, checkpoint hay rate limit.
"""

from __future__ import annotations

import random
import threading
import time
from datetime import datetime
from typing import Any, Callable

from loguru import logger

from .provider import PageCreationOutcome, PageCreationProvider, PageCreationRequest
from .states import (
    ACCOUNT_LOCK_READY,
    ACCOUNT_LOCK_SECURITY_VERIFICATION_REQUIRED,
    BATCH_CANCELLED,
    BATCH_COMPLETED,
    BATCH_DELAYING,
    BATCH_FAILED,
    BATCH_PAUSED,
    BATCH_PAUSED_FOR_USER,
    BATCH_READY,
    BATCH_RUNNING,
    INCOMPLETE_BATCH_STATUSES,
    JOB_ACCOUNT_SECURITY_CHECK,
    JOB_ADDING_ADMIN,
    JOB_BLOCKED_BY_ACCOUNT_VERIFICATION,
    JOB_CANCELLED,
    JOB_CAPTCHA_DETECTED,
    JOB_CHECKPOINT,
    JOB_CHECKING_SESSION,
    JOB_COMPLETED,
    JOB_CREATING,
    JOB_DELAYED,
    JOB_FAILED,
    JOB_FILLING_DETAILS,
    JOB_PENDING,
    JOB_RATE_LIMITED,
    JOB_READY_TO_RESUME,
    JOB_RECOVERY_REQUIRED,
    JOB_SMS_VERIFICATION_REQUIRED,
    JOB_UPLOADING_AVATAR,
    JOB_VALIDATING,
    JOB_VERIFYING,
    JOB_WAITING_REAUTH,
    RECOVERY_JOB_STATUSES,
    SECURITY_PAUSE_CODES,
    TEMPORARY_ERROR_CODES,
    USER_ACTION_CODES,
    USER_HELD_JOB_STATUSES,
)
from .store import PageCreationStore

MAX_ACTIVE_CREATION_JOB = 1
_USER_STATUS = {
    "SESSION_EXPIRED": JOB_WAITING_REAUTH,
    "CAPTCHA_DETECTED": JOB_CAPTCHA_DETECTED,
    "CHECKPOINT": JOB_CHECKPOINT,
    "RATE_LIMITED": JOB_RATE_LIMITED,
    "SMS_VERIFICATION_REQUIRED": JOB_SMS_VERIFICATION_REQUIRED,
    "SECURITY_VERIFICATION_REQUIRED": JOB_SMS_VERIFICATION_REQUIRED,
}
_SMS_MESSAGE = "Facebook yêu cầu hoàn tất SMS verification trên mobile app."
_PAUSED_BATCH = {BATCH_PAUSED, BATCH_PAUSED_FOR_USER}
_ACTIONABLE = {
    JOB_PENDING,
    JOB_DELAYED,
    JOB_WAITING_REAUTH,
    JOB_CHECKPOINT,
    JOB_CAPTCHA_DETECTED,
    JOB_RATE_LIMITED,
    JOB_SMS_VERIFICATION_REQUIRED,
    JOB_READY_TO_RESUME,
    JOB_ACCOUNT_SECURITY_CHECK,
    JOB_RECOVERY_REQUIRED,
    JOB_CREATING,
    JOB_VERIFYING,
    JOB_UPLOADING_AVATAR,
    JOB_FILLING_DETAILS,
    JOB_ADDING_ADMIN,
    JOB_CHECKING_SESSION,
    JOB_VALIDATING,
}
_TERMINAL = {JOB_COMPLETED, JOB_FAILED, JOB_CANCELLED}


def _stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def calculate_delay_seconds(batch: dict[str, Any], rng: random.Random) -> int:
    """FIXED hoặc RANGE. 0 nếu tắt delay."""
    if not bool(batch.get("delay_enabled", True)):
        return 0
    mode = str(batch.get("delay_mode") or "fixed").strip().lower()
    if mode == "range":
        lo = int(batch.get("delay_min_seconds") or 0)
        hi = int(batch.get("delay_max_seconds") or 0)
        if hi < lo:
            lo, hi = hi, lo
        if hi <= 0:
            return 0
        return int(rng.randint(lo, hi))
    return max(0, int(batch.get("delay_fixed_seconds") or 0))


class PageCreationEngine:
    """Chạy batch tuần tự. ``sleep`` và ``rng`` tiêm được để test không chờ thật."""

    def __init__(
        self,
        store: PageCreationStore,
        provider: PageCreationProvider,
        *,
        sleep: Callable[[float], None] | None = None,
        rng: random.Random | None = None,
        on_completed: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.store = store
        self.provider = provider
        self._sleep = sleep or time.sleep
        self._rng = rng or random.Random()
        self._on_completed = on_completed
        self._locks: dict[str, threading.Lock] = {}
        self._lock_guard = threading.Lock()
        self._gates: dict[str, threading.Event] = {}
        self._cancel: set[str] = set()
        self._active: set[str] = set()
        self._active_guard = threading.Lock()

    def _account_lock(self, account_id: str) -> threading.Lock:
        with self._lock_guard:
            lk = self._locks.get(account_id)
            if lk is None:
                lk = threading.Lock()
                self._locks[account_id] = lk
            return lk

    def _gate(self, batch_id: str) -> threading.Event:
        ev = self._gates.get(batch_id)
        if ev is None:
            ev = threading.Event()
            ev.set()
            self._gates[batch_id] = ev
        return ev

    def is_running(self, batch_id: str) -> bool:
        """Thread của batch này còn vòng lặp hay không."""
        return batch_id in self._active

    def pause(self, batch_id: str) -> None:
        """Dừng job mới và đóng băng countdown. Không xóa queue."""
        self._gate(batch_id).clear()
        batch = self.store.get_batch(batch_id)
        if batch and str(batch.get("status")) not in {BATCH_COMPLETED, BATCH_CANCELLED, BATCH_FAILED}:
            batch["status"] = BATCH_PAUSED
            self.store.save_batch(batch)
            self._note(batch_id, None, "Batch tạm dừng")

    def resume(self, batch_id: str) -> None:
        """Chạy tiếp job đang chờ. Không tự vượt checkpoint — người dùng xử lý trước khi bấm."""
        self._cancel.discard(batch_id)
        batch = self.store.get_batch(batch_id)
        if batch and str(batch.get("status")) in _PAUSED_BATCH:
            if str(batch.get("account_creation_lock") or "") == ACCOUNT_LOCK_SECURITY_VERIFICATION_REQUIRED:
                self._note(batch_id, None, "Account còn khóa xác minh. Hãy bấm Kiểm tra lại.")
                return
            batch["status"] = BATCH_RUNNING
            self.store.save_batch(batch)
            self._note(batch_id, None, "Batch tiếp tục")
        self._gate(batch_id).set()

    def check_again(self, batch_id: str) -> PageCreationOutcome:
        """
        Sau khi user xử lý SMS/CAPTCHA: kiểm tra lại account.
        Không tạo Page. Nếu Facebook cho phép thì mở khóa queue và chạy tiếp job đang dừng.
        """
        batch = self.store.get_batch(batch_id)
        if batch is None:
            return PageCreationOutcome(ok=False, error_code="FAILED", error_message="Không tìm thấy batch.")
        account_id = str(batch.get("account_id") or "")
        business_id = str(batch.get("business_id") or "")
        held = self._held_security_job(batch_id)
        if held is None:
            return PageCreationOutcome(ok=False, error_code="FAILED", error_message="Không có job đang chờ xác minh.")
        held["status"] = JOB_ACCOUNT_SECURITY_CHECK
        held["error_message"] = "Đang kiểm tra lại trạng thái account trên Facebook."
        self.store.save_job(held, flush=False)
        self.store.save_batch(batch, flush=False)
        self._persist(force=True)
        self._note(batch_id, held, "ACCOUNT_SECURITY_CHECK")

        probe = getattr(self.provider, "probe_page_creation_gate", None)
        if callable(probe):
            create_mode = str(batch.get("create_mode") or held.get("create_mode") or "bm")
            try:
                outcome = probe(account_id, business_id, create_mode)
            except TypeError:
                outcome = probe(account_id, business_id)
        else:
            outcome = self.provider.check_session(account_id)

        if not outcome.ok or outcome.error_code in SECURITY_PAUSE_CODES | USER_ACTION_CODES:
            code = outcome.error_code or "SMS_VERIFICATION_REQUIRED"
            message = outcome.error_message or _SMS_MESSAGE
            held["status"] = _USER_STATUS.get(code, JOB_SMS_VERIFICATION_REQUIRED)
            held["error_code"] = code
            held["error_message"] = message
            batch["status"] = BATCH_PAUSED_FOR_USER
            batch["account_creation_lock"] = ACCOUNT_LOCK_SECURITY_VERIFICATION_REQUIRED
            batch["pause_reason"] = "PAUSED_FOR_USER"
            self.store.save_job(held, flush=False)
            self.store.save_batch(batch, flush=False)
            self._persist(force=True)
            self._note(batch_id, held, f"Vẫn bị chặn: {code}")
            self._gate(batch_id).clear()
            return PageCreationOutcome(ok=False, error_code=code, error_message=message)

        held["status"] = JOB_READY_TO_RESUME
        held["error_code"] = ""
        held["error_message"] = ""
        self.store.save_job(held, flush=False)
        self._clear_account_security_lock(account_id, resume_job_id=str(held.get("id") or ""))
        batch = self.store.get_batch(batch_id) or batch
        batch["status"] = BATCH_RUNNING
        batch["account_creation_lock"] = ACCOUNT_LOCK_READY
        batch["pause_reason"] = ""
        self.store.save_batch(batch, flush=False)
        held = self._reload(batch_id, str(held.get("id"))) or held
        held["status"] = JOB_PENDING
        self.store.save_job(held, flush=False)
        self._persist(force=True)
        self._note(batch_id, held, "Account READY — tiếp tục queue từ job đang dừng")
        self._gate(batch_id).set()
        return PageCreationOutcome(ok=True, error_message="Account sẵn sàng. Tiếp tục tạo Page.")

    def _held_security_job(self, batch_id: str) -> dict[str, Any] | None:
        for job in self.store.jobs_for_batch(batch_id):
            if str(job.get("status")) in USER_HELD_JOB_STATUSES:
                return job
        return None

    def _clear_account_security_lock(self, account_id: str, *, resume_job_id: str = "") -> None:
        """Mở khóa account: job bị BLOCKED về PENDING. COMPLETED không đụng."""
        for batch in self.store.list_batches():
            if str(batch.get("account_id") or "") != account_id:
                continue
            if str(batch.get("status")) in {BATCH_COMPLETED, BATCH_CANCELLED}:
                continue
            changed = False
            if str(batch.get("account_creation_lock") or "") == ACCOUNT_LOCK_SECURITY_VERIFICATION_REQUIRED:
                batch["account_creation_lock"] = ACCOUNT_LOCK_READY
                batch["pause_reason"] = ""
                changed = True
            for job in self.store.jobs_for_batch(str(batch.get("id"))):
                status = str(job.get("status") or "")
                if status == JOB_COMPLETED:
                    continue
                if status == JOB_BLOCKED_BY_ACCOUNT_VERIFICATION:
                    job["status"] = JOB_PENDING
                    job["error_code"] = ""
                    job["error_message"] = ""
                    self.store.save_job(job, flush=False)
                elif status == JOB_READY_TO_RESUME and str(job.get("id")) == resume_job_id:
                    job["status"] = JOB_PENDING
                    job["error_code"] = ""
                    job["error_message"] = ""
                    self.store.save_job(job, flush=False)
            if changed:
                self.store.save_batch(batch, flush=False)

    def cancel(self, batch_id: str) -> None:
        """Hủy job PENDING. Job đang chạy dừng an toàn. Page đã COMPLETED được giữ."""
        self._cancel.add(batch_id)
        self._gate(batch_id).set()
        for job in self.store.jobs_for_batch(batch_id):
            if str(job.get("status")) in {JOB_PENDING, JOB_DELAYED, JOB_BLOCKED_BY_ACCOUNT_VERIFICATION}:
                job["status"] = JOB_CANCELLED
                job["finished_at"] = datetime.now().isoformat(timespec="seconds")
                self.store.save_job(job)
        batch = self.store.get_batch(batch_id)
        if batch and str(batch.get("status")) not in {BATCH_COMPLETED, BATCH_CANCELLED}:
            batch["status"] = BATCH_CANCELLED
            batch["finished_at"] = datetime.now().isoformat(timespec="seconds")
            batch["delay_remaining_seconds"] = 0
            batch["account_creation_lock"] = ""
            batch["pause_reason"] = ""
            self.store.save_batch(batch)
        self.store.refresh_counts(batch_id)
        self._note(batch_id, None, "Batch đã hủy")

    def retry_job(self, batch_id: str, job_id: str) -> None:
        """Đưa job FAILED về PENDING để chạy lại một vòng (vẫn tôn trọng max_retry mới)."""
        for job in self.store.jobs_for_batch(batch_id):
            if str(job.get("id")) != str(job_id):
                continue
            if str(job.get("status")) != JOB_FAILED:
                return
            job["status"] = JOB_PENDING
            job["retry_count"] = 0
            job["error_code"] = ""
            job["error_message"] = ""
            job["finished_at"] = ""
            self.store.save_job(job)
        batch = self.store.get_batch(batch_id)
        if batch and str(batch.get("status")) in {BATCH_COMPLETED, BATCH_FAILED, BATCH_CANCELLED, BATCH_PAUSED}:
            batch["status"] = BATCH_RUNNING
            batch["finished_at"] = ""
            self.store.save_batch(batch)
        self._gate(batch_id).set()

    def progress(self, batch_id: str) -> dict[str, Any]:
        """Ảnh tiến độ nhẹ cho UI. Không chép log của từng job."""
        batch, rows = self.store.job_summaries(batch_id)
        batch = batch or {}
        current = None
        nxt = None
        done = 0
        quiet = _TERMINAL | {JOB_PENDING, JOB_DELAYED}
        for row in rows:
            status = row[3]
            if status == JOB_COMPLETED:
                done += 1
            if current is None and status not in quiet:
                current = row
            elif nxt is None and status in {JOB_PENDING, JOB_DELAYED}:
                nxt = row
        total = int(batch.get("total_jobs") or len(rows) or 0)
        progress_message = ""
        jobs = self.store.jobs_for_batch(batch_id)
        target_id = None
        if current is not None:
            target_id = current[0]
        elif nxt is not None:
            target_id = nxt[0]
        if target_id is not None:
            for job in jobs:
                if str(job.get("id")) == str(target_id):
                    progress_message = str(job.get("progress_message") or "").strip()
                    break
        if not progress_message:
            for job in reversed(jobs):
                msg = str(job.get("progress_message") or "").strip()
                if msg:
                    progress_message = msg
                    break
        return {
            "batch": batch,
            "rows": rows,
            "total": total,
            "completed": done,
            "percent": int(done * 100 / total) if total else 0,
            "current": current,
            "next": nxt,
            "delay_remaining": int(batch.get("delay_remaining_seconds") or 0),
            "status": str(batch.get("status") or ""),
            "progress_message": progress_message,
        }

    def _persist(self, *, force: bool = False) -> None:
        """Ghi đĩa mỗi mốc. fsync theo cụm để batch lớn không nghẽn, mốc quan trọng thì fsync ngay."""
        self._persist_every = getattr(self, "_persist_every", 0) + 1
        self.store.flush(sync=force or self._persist_every % 20 == 0)

    def run_batch(self, batch_id: str) -> None:
        """Chạy đến khi hết job, pause, hoặc cancel. Gọi lại sau resume nếu thread đã thoát."""
        with self._active_guard:
            if batch_id in self._active:
                return
            self._active.add(batch_id)
        try:
            self._run_loop(batch_id)
        finally:
            with self._active_guard:
                self._active.discard(batch_id)

    def recover_incomplete(self) -> list[str]:
        """
        Sau khi mở app: job COMPLETED giữ nguyên; CREATING/VERIFYING xác minh trước;
        PENDING giữ hàng đợi. Trả id batch nên chạy tiếp.
        """
        resume_ids: list[str] = []
        for batch in self.store.list_batches():
            status = str(batch.get("status") or "")
            jobs = self.store.jobs_for_batch(str(batch.get("id")))
            needs_job = any(str(j.get("status")) in RECOVERY_JOB_STATUSES for j in jobs)
            if status not in INCOMPLETE_BATCH_STATUSES and not needs_job:
                continue
            bid = str(batch["id"])
            for job in jobs:
                st = str(job.get("status") or "")
                if st == JOB_COMPLETED:
                    continue
                if st == JOB_DELAYED:
                    job["status"] = JOB_PENDING
                    self.store.save_job(job)
                    continue
                if st not in RECOVERY_JOB_STATUSES:
                    continue
                job["status"] = JOB_RECOVERY_REQUIRED
                self.store.save_job(job)
                self._note(bid, job, "Recovery: xác minh Page đã tạo trước khi tạo lại")
                found = self.provider.verify_existing(self._request(job))
                if found.ok and found.page_id:
                    self._mark_completed(job, found)
                else:
                    job["status"] = JOB_PENDING
                    job["error_code"] = ""
                    job["error_message"] = ""
                    self.store.save_job(job)
            fresh_jobs = self.store.jobs_for_batch(bid)
            waiting_user = any(
                str(j.get("status")) in USER_HELD_JOB_STATUSES | {JOB_BLOCKED_BY_ACCOUNT_VERIFICATION}
                for j in fresh_jobs
            )
            if status == BATCH_DELAYING:
                batch["resume_with_delay"] = True
            if waiting_user or status in _PAUSED_BATCH:
                batch["status"] = BATCH_PAUSED_FOR_USER if waiting_user else BATCH_PAUSED
                if waiting_user:
                    batch["account_creation_lock"] = ACCOUNT_LOCK_SECURITY_VERIFICATION_REQUIRED
                    batch["pause_reason"] = "PAUSED_FOR_USER"
                self._gate(bid).clear()
                self.store.save_batch(batch)
            elif status in {BATCH_RUNNING, BATCH_DELAYING, "CREATING"} and bool(batch.get("auto_resume_after_restart", False)):
                batch["status"] = BATCH_RUNNING
                self.store.save_batch(batch)
                resume_ids.append(bid)
            self.store.refresh_counts(bid)
        return resume_ids

    def _run_loop(self, batch_id: str) -> None:
        """Giữ một phiên trình duyệt cho cả batch, đóng khi batch dừng hẳn."""
        batch = self.store.get_batch(batch_id)
        if batch is None:
            return
        if str(batch.get("status")) in {BATCH_COMPLETED, BATCH_CANCELLED}:
            return
        account_id = str(batch.get("account_id") or "")
        begin = getattr(self.provider, "begin_account", None)
        end = getattr(self.provider, "end_account", None)
        if callable(begin):
            try:
                begin(account_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Không mở được phiên tạo Page: {}", exc)
        try:
            self._run_open_batch(batch_id)
        finally:
            if callable(end):
                try:
                    end(account_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Không đóng được phiên tạo Page: {}", exc)

    def _run_open_batch(self, batch_id: str) -> None:
        batch = self.store.get_batch(batch_id)
        if batch is None:
            return
        if str(batch.get("status")) in {BATCH_COMPLETED, BATCH_CANCELLED}:
            return
        if batch_id not in self._cancel:
            self._gate(batch_id).set()
        if str(batch.get("status")) != BATCH_PAUSED and str(batch.get("status")) != BATCH_PAUSED_FOR_USER:
            batch["status"] = BATCH_RUNNING
            if not batch.get("started_at"):
                batch["started_at"] = datetime.now().isoformat(timespec="seconds")
            self.store.save_batch(batch)
        hold_delay = bool(batch.get("resume_with_delay"))

        while True:
            if batch_id in self._cancel:
                self._finish_cancelled(batch_id)
                return
            if not self._wait_if_paused(batch_id):
                self._finish_cancelled(batch_id)
                return
            batch = self.store.get_batch(batch_id) or batch
            if str(batch.get("status")) in _PAUSED_BATCH:
                continue
            job = self._next_job(batch_id)
            if job is None:
                batch["status"] = BATCH_COMPLETED
                batch["finished_at"] = datetime.now().isoformat(timespec="seconds")
                batch["delay_remaining_seconds"] = 0
                batch["current_job_id"] = ""
                self.store.save_batch(batch)
                self.store.refresh_counts(batch_id)
                self._note(batch_id, None, "Batch hoàn tất")
                return
            if hold_delay:
                hold_delay = False
                batch["resume_with_delay"] = False
                self.store.save_batch(batch)
                if not self._delay_before_next(batch, job):
                    self._finish_cancelled(batch_id)
                    return
                continue

            lock = self._account_lock(str(job.get("account_id") or ""))
            lock.acquire()
            try:
                self._execute_job(batch, job)
            finally:
                lock.release()

            batch = self.store.get_batch(batch_id) or batch
            job = self._reload(batch_id, str(job.get("id"))) or job
            if batch_id in self._cancel:
                self._finish_cancelled(batch_id)
                return
            if str(batch.get("status")) in _PAUSED_BATCH:
                continue
            if str(job.get("status")) == JOB_FAILED and not bool(batch.get("continue_on_error", True)):
                batch["status"] = BATCH_PAUSED
                self.store.save_batch(batch)
                self._gate(batch_id).clear()
                continue
            if str(job.get("status")) in USER_HELD_JOB_STATUSES | {JOB_BLOCKED_BY_ACCOUNT_VERIFICATION}:
                batch["status"] = BATCH_PAUSED_FOR_USER
                self.store.save_batch(batch, flush=False)
                self._persist()
                self._gate(batch_id).clear()
                continue
            if str(job.get("status")) in {JOB_COMPLETED, JOB_FAILED, JOB_CANCELLED}:
                nxt = self._next_job(batch_id)
                if nxt is not None and not self._delay_before_next(batch, nxt):
                    self._finish_cancelled(batch_id)
                    return

    def _execute_job(self, batch: dict[str, Any], job: dict[str, Any]) -> None:
        batch_id = str(batch.get("id"))
        if str(job.get("status")) == JOB_COMPLETED:
            return
        if str(job.get("status")) == JOB_BLOCKED_BY_ACCOUNT_VERIFICATION:
            return
        if str(job.get("status")) in USER_HELD_JOB_STATUSES and str(job.get("status")) != JOB_READY_TO_RESUME:
            # Đang chờ user (SMS/CAPTCHA/checkpoint). Không tự tạo lại.
            return
        if str(batch.get("account_creation_lock") or "") == ACCOUNT_LOCK_SECURITY_VERIFICATION_REQUIRED:
            # Account còn khóa xác minh — chỉ check_again mới mở.
            return
        batch["current_job_id"] = str(job.get("id"))
        self.store.save_batch(batch, flush=False)
        if str(job.get("status")) in RECOVERY_JOB_STATUSES | {JOB_READY_TO_RESUME}:
            self._note(batch_id, job, "Verifying kết quả đã có trước khi tạo")
            job["status"] = JOB_VERIFYING
            self.store.save_job(job, flush=False)
            self._persist()
            found = self.provider.verify_existing(self._request(job))
            if found.ok and found.page_id:
                self._mark_completed(job, found)
                return
            if found.error_code in SECURITY_PAUSE_CODES | USER_ACTION_CODES:
                self._pause_for_user(batch, job, found)
                return
        if not job.get("started_at"):
            job["started_at"] = datetime.now().isoformat(timespec="seconds")
        job["status"] = JOB_VALIDATING
        self.store.save_job(job, flush=False)
        self._note(batch_id, job, "Validation OK")

        job["status"] = JOB_CHECKING_SESSION
        self.store.save_job(job, flush=False)
        session = self.provider.check_session(str(job.get("account_id") or ""))
        if not session.ok:
            if not session.error_code:
                session.error_code = "SESSION_EXPIRED"
            self._pause_for_user(batch, job, session)
            return
        self._note(batch_id, job, "Session OK")

        while True:
            if batch_id in self._cancel or self._stopped(job):
                if str(job.get("status")) != JOB_COMPLETED:
                    job["status"] = JOB_CANCELLED
                    job["finished_at"] = datetime.now().isoformat(timespec="seconds")
                    self.store.save_job(job, flush=False)
                    self._persist(force=True)
                return
            prior = str(job.get("status") or "")
            if prior in {
                JOB_CREATING,
                JOB_VERIFYING,
                JOB_UPLOADING_AVATAR,
                JOB_FILLING_DETAILS,
                JOB_ADDING_ADMIN,
                JOB_RECOVERY_REQUIRED,
            }:
                self._note(batch_id, job, "Đang xác minh kết quả cũ trước khi tạo lại")
                job["status"] = JOB_VERIFYING
                self.store.save_job(job, flush=False)
                self._persist()
                found = self.provider.verify_existing(self._request(job))
                if found.ok and found.page_id:
                    self._mark_completed(job, found)
                    return
                if found.error_code in SECURITY_PAUSE_CODES | USER_ACTION_CODES:
                    self._pause_for_user(batch, job, found)
                    return

            known_id = str(job.get("page_id") or "").strip()
            if known_id:
                known_url = str(job.get("page_url") or "").strip() or f"https://www.facebook.com/{known_id}"
                self._mark_completed(
                    job,
                    PageCreationOutcome(
                        ok=True,
                        page_id=known_id,
                        page_url=known_url,
                        page_name=str(job.get("page_name") or ""),
                    ),
                )
                return

            job["status"] = JOB_CREATING
            self._note(batch_id, job, "Creating Page")
            self.store.save_job(job, flush=False)
            self._persist()
            try:
                outcome = self.provider.create_and_verify(self._request(job))
            except Exception as exc:  # noqa: BLE001
                logger.exception("Tạo Page lỗi tạm: {}", exc)
                outcome = PageCreationOutcome(ok=False, error_code="TEMPORARY_ERROR", error_message=str(exc))

            if str(outcome.page_id or "").strip():
                if not str(outcome.page_url or "").strip():
                    outcome.page_url = f"https://www.facebook.com/{outcome.page_id}"
                self._note(batch_id, job, f"UID {outcome.page_id} · {outcome.page_url}")
                self._mark_completed(job, outcome)
                return

            job["status"] = JOB_VERIFYING
            self._note(batch_id, job, "Verifying")
            self.store.save_job(job, flush=False)
            self._persist()
            try:
                found = self.provider.verify_existing(self._request(job))
            except Exception as exc:  # noqa: BLE001
                found = PageCreationOutcome(ok=False, error_code="TEMPORARY_ERROR", error_message=str(exc))
            if found.ok and found.page_id:
                self._note(batch_id, job, "Page verified")
                self._mark_completed(job, found)
                return

            if outcome.error_code == "CANCELLED" or batch_id in self._cancel:
                job["status"] = JOB_CANCELLED
                job["finished_at"] = datetime.now().isoformat(timespec="seconds")
                self.store.save_job(job, flush=False)
                self._persist(force=True)
                return

            if outcome.requires_user_action or outcome.error_code in USER_ACTION_CODES:
                self._pause_for_user(batch, job, outcome)
                return

            if found.error_code in SECURITY_PAUSE_CODES | USER_ACTION_CODES:
                self._pause_for_user(batch, job, found)
                return

            if outcome.error_code not in TEMPORARY_ERROR_CODES:
                self._mark_failed(job, outcome.error_code or "FAILED", outcome.error_message or "Tạo Page thất bại")
                return

            # Page có thể đã tạo dù chưa đọc được UID ở lượt đầu — tìm lại trước khi báo thất bại.
            limit = int(job.get("max_retry") or batch.get("max_retry") or 3)
            if int(job.get("retry_count") or 0) >= limit:
                try:
                    again = self.provider.verify_existing(self._request(job))
                except Exception as exc:  # noqa: BLE001
                    again = PageCreationOutcome(ok=False, error_code="TEMPORARY_ERROR", error_message=str(exc))
                if str(again.page_id or "").strip():
                    if not str(again.page_url or "").strip():
                        again.page_url = f"https://www.facebook.com/{again.page_id}"
                    self._mark_completed(job, again)
                    return
                self._mark_failed(job, outcome.error_code, outcome.error_message or "Hết số lần thử")
                return
            job["retry_count"] = int(job.get("retry_count") or 0) + 1
            job["error_code"] = outcome.error_code
            job["error_message"] = outcome.error_message
            job["status"] = JOB_PENDING
            self._note(batch_id, job, f"Retry {job['retry_count']}/{limit}")
            self.store.save_job(job, flush=False)

    def _delay_before_next(self, batch: dict[str, Any], next_job: dict[str, Any]) -> bool:
        """Chờ sau khi job trước đã xong. Trả False nếu batch bị hủy."""
        batch_id = str(batch.get("id"))
        seconds = int(batch.get("delay_remaining_seconds") or 0)
        if seconds <= 0:
            seconds = calculate_delay_seconds(batch, self._rng)
        if seconds <= 0:
            return True
        next_job["status"] = JOB_DELAYED
        batch["status"] = BATCH_DELAYING
        batch["delay_remaining_seconds"] = seconds
        self._note(batch_id, next_job, f"Delay started: {seconds}s")
        self.store.save_job(next_job, flush=False)
        self.store.save_batch(batch, flush=False)
        self._persist(force=True)
        remaining = seconds
        while remaining > 0:
            if batch_id in self._cancel:
                return False
            if not self._gate(batch_id).is_set():
                batch = self.store.get_batch(batch_id) or batch
                batch["status"] = BATCH_PAUSED
                batch["delay_remaining_seconds"] = remaining
                self.store.save_batch(batch)
                if not self._wait_if_paused(batch_id):
                    return False
                continue
            batch = self.store.get_batch(batch_id) or batch
            batch["status"] = BATCH_DELAYING
            batch["delay_remaining_seconds"] = remaining
            self.store.save_batch(batch, flush=False)
            self.store.flush(sync=False)
            self._sleep(1 if remaining >= 1 else float(remaining))
            if batch_id in self._cancel:
                return False
            if not self._gate(batch_id).is_set():
                continue
            remaining -= 1
        next_job["status"] = JOB_PENDING
        batch = self.store.get_batch(batch_id) or batch
        batch["status"] = BATCH_RUNNING
        batch["delay_remaining_seconds"] = 0
        batch["resume_with_delay"] = False
        self._note(batch_id, next_job, "Delay finished")
        self.store.save_job(next_job, flush=False)
        self.store.save_batch(batch, flush=False)
        self._persist()
        return True

    def _pause_for_user(self, batch: dict[str, Any], job: dict[str, Any], outcome: PageCreationOutcome) -> None:
        """Dừng job hiện tại vì SMS/CAPTCHA/checkpoint. Không retry, không chạy Page tiếp theo trên account này."""
        code = outcome.error_code or "CHECKPOINT"
        message = outcome.error_message or (
            _SMS_MESSAGE if code == "SMS_VERIFICATION_REQUIRED" else code
        )
        job["status"] = _USER_STATUS.get(code, JOB_CHECKPOINT)
        job["error_code"] = code
        job["error_message"] = message
        batch["status"] = BATCH_PAUSED_FOR_USER
        batch["pause_reason"] = "PAUSED_FOR_USER"
        batch["account_creation_lock"] = ACCOUNT_LOCK_SECURITY_VERIFICATION_REQUIRED
        account_id = str(job.get("account_id") or batch.get("account_id") or "")
        if code == "SMS_VERIFICATION_REQUIRED":
            self._note(str(batch.get("id")), job, _SMS_MESSAGE)
        elif code == "SESSION_EXPIRED":
            self._note(str(batch.get("id")), job, "Account cần xác minh/đăng nhập lại.")
        elif code == "RATE_LIMITED":
            self._note(str(batch.get("id")), job, "Platform yêu cầu chờ/giới hạn thao tác.")
        else:
            self._note(str(batch.get("id")), job, "Cần người dùng xử lý checkpoint/CAPTCHA trên trình duyệt.")
        self.store.save_job(job, flush=False)
        self.store.save_batch(batch, flush=False)
        self._block_pending_for_account(account_id, except_job_id=str(job.get("id") or ""))
        self._persist(force=True)
        self._gate(str(batch.get("id"))).clear()

    def _block_pending_for_account(self, account_id: str, *, except_job_id: str = "") -> None:
        """Pending/Delayed cùng account → BLOCKED. COMPLETED giữ nguyên. Account khác không đụng."""
        if not account_id:
            return
        for batch in self.store.list_batches():
            if str(batch.get("account_id") or "") != account_id:
                continue
            if str(batch.get("status")) in {BATCH_COMPLETED, BATCH_CANCELLED}:
                continue
            batch["account_creation_lock"] = ACCOUNT_LOCK_SECURITY_VERIFICATION_REQUIRED
            if str(batch.get("status")) in {BATCH_RUNNING, BATCH_DELAYING, BATCH_READY}:
                batch["status"] = BATCH_PAUSED_FOR_USER
                batch["pause_reason"] = "PAUSED_FOR_USER"
                self._gate(str(batch.get("id"))).clear()
            self.store.save_batch(batch, flush=False)
            for other in self.store.jobs_for_batch(str(batch.get("id"))):
                if str(other.get("id")) == except_job_id:
                    continue
                status = str(other.get("status") or "")
                if status in {JOB_PENDING, JOB_DELAYED}:
                    other["status"] = JOB_BLOCKED_BY_ACCOUNT_VERIFICATION
                    other["error_code"] = "SECURITY_VERIFICATION_REQUIRED"
                    other["error_message"] = "Account đang chờ xác minh bảo mật. Không chạy Page tiếp theo."
                    self.store.save_job(other, flush=False)

    def _mark_completed(self, job: dict[str, Any], outcome: PageCreationOutcome) -> None:
        job["status"] = JOB_COMPLETED
        job["page_id"] = outcome.page_id
        page_url = str(outcome.page_url or "").strip() or f"https://www.facebook.com/{outcome.page_id}"
        job["page_url"] = page_url
        if str(outcome.page_name or "").strip():
            job["page_name"] = str(outcome.page_name).strip()
        job["error_code"] = ""
        job["error_message"] = ""
        job["progress_message"] = f"UID {outcome.page_id} · {page_url}"
        job["finished_at"] = datetime.now().isoformat(timespec="seconds")
        self._note(str(job.get("batch_id")), job, f"COMPLETED UID {outcome.page_id} · {page_url}")
        self.store.save_job(job, flush=False)
        self.store.refresh_counts(str(job.get("batch_id")), flush=False)
        self._persist()
        if self._on_completed is not None:
            try:
                self._on_completed(dict(job))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Không ghi được Page đã xác minh vào pages.json: {}", exc)

    def _mark_failed(self, job: dict[str, Any], code: str, message: str) -> None:
        job["status"] = JOB_FAILED
        job["error_code"] = code
        job["error_message"] = message
        job["finished_at"] = datetime.now().isoformat(timespec="seconds")
        self._note(str(job.get("batch_id")), job, f"FAILED {code}")
        self.store.save_job(job, flush=False)
        self.store.refresh_counts(str(job.get("batch_id")), flush=False)
        self._persist()

    def _wait_if_paused(self, batch_id: str) -> bool:
        gate = self._gate(batch_id)
        while not gate.is_set():
            if batch_id in self._cancel:
                return False
            self._sleep(0.05)
        return batch_id not in self._cancel

    def _finish_cancelled(self, batch_id: str) -> None:
        for job in self.store.jobs_for_batch(batch_id):
            if str(job.get("status")) in {JOB_PENDING, JOB_DELAYED, JOB_BLOCKED_BY_ACCOUNT_VERIFICATION}:
                job["status"] = JOB_CANCELLED
                job["finished_at"] = datetime.now().isoformat(timespec="seconds")
                self.store.save_job(job)
        batch = self.store.get_batch(batch_id)
        if batch:
            batch["status"] = BATCH_CANCELLED
            batch["delay_remaining_seconds"] = 0
            batch["finished_at"] = batch.get("finished_at") or datetime.now().isoformat(timespec="seconds")
            self.store.save_batch(batch)
        self.store.refresh_counts(batch_id)

    def _next_job(self, batch_id: str) -> dict[str, Any] | None:
        for job in self.store.jobs_for_batch(batch_id):
            if str(job.get("status")) in _ACTIONABLE:
                return job
        return None

    def _reload(self, batch_id: str, job_id: str) -> dict[str, Any] | None:
        for job in self.store.jobs_for_batch(batch_id):
            if str(job.get("id")) == job_id:
                return job
        return None

    def _request(self, job: dict[str, Any]) -> PageCreationRequest:
        batch_id = str(job.get("batch_id") or "")

        def _state(name: str) -> None:
            job["status"] = name
            self.store.save_job(job, flush=False)

        def _progress(message: str) -> None:
            text = str(message or "").strip()
            if not text:
                return
            job["progress_message"] = text
            self._note(batch_id, job, text)
            self.store.save_job(job, flush=False)

        return PageCreationRequest(
            job_id=str(job.get("id") or ""),
            batch_id=batch_id,
            account_id=str(job.get("account_id") or ""),
            business_id=str(job.get("business_id") or ""),
            page_name=str(job.get("page_name") or ""),
            avatar_path=str(job.get("avatar_path") or ""),
            create_mode=str(job.get("create_mode") or "bm"),
            admin_targets=[str(item).strip() for item in (job.get("admin_targets") or []) if str(item).strip()],
            category=str(job.get("category") or "").strip(),
            bio=str(job.get("bio") or "").strip(),
            phone=str(job.get("phone") or "").strip(),
            email=str(job.get("email") or "").strip(),
            website=str(job.get("website") or "").strip(),
            address=str(job.get("address") or "").strip(),
            should_stop=lambda: batch_id in self._cancel,
            on_state=_state,
            on_progress=_progress,
        )

    def _stopped(self, job: dict[str, Any]) -> bool:
        return str(job.get("batch_id") or "") in self._cancel

    def _note(self, batch_id: str, job: dict[str, Any] | None, message: str) -> None:
        line = f"[{_stamp()}] {message}"
        logger.info("[PageCreator] batch={} {}", batch_id, line)
        if job is None:
            return
        logs = list(job.get("logs") or [])
        logs.append(line)
        job["logs"] = logs[-8:]
        self.store.save_job(job, flush=False)
