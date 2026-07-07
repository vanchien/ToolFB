"""
Điều phối hàng đợi tương tác: giới hạn luồng đồng thời, grid layout, không trùng proxy.
"""

from __future__ import annotations

import os
import queue
import random
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from loguru import logger

from src.models.mapped_account import MappedAccount
from src.services.human_interaction_profile import HumanInteractionProfile, resolve_profile
from src.services.human_interaction_worker import run_human_interaction_worker
from src.utils.account_proxy_mapper import AccountProxyMappingError
from src.utils.grid_layout_manager import GridWindowSlot, compute_grid_layout

StatusCallback = Callable[[MappedAccount, str, str], None]
DoneCallback = Callable[[], None]


def compute_pool_join_timeout_sec(
    account_count: int,
    *,
    max_concurrent: int = 4,
    login_only: bool = False,
) -> float:
    """
    Thời gian chờ pool hoàn tất — tính theo số đợt × thời gian worker × (1 + retry).

    Mặc định đủ cho 9 TK / 4 luồng kể cả establish + F5 + retry (~25–40 phút nếu cần).

    Cấu hình: ``FB_POOL_TARGET_MINUTES``, ``FB_HUMAN_WORKER_MAX_SEC``,
    ``FB_HUMAN_INTERACTION_MAX_RETRIES``, ``FB_POOL_JOIN_BUFFER_SEC``,
    ``FB_POOL_JOIN_MIN_SEC``, ``FB_POOL_JOIN_MAX_SEC``.
    """
    n = max(0, int(account_count))
    mc = max(1, int(max_concurrent))
    if n <= 0:
        return 120.0
    target_min = float(
        os.environ.get(
            "FB_POOL_TARGET_MINUTES",
            "10" if login_only else "18",
        )
    )
    buffer = float(os.environ.get("FB_POOL_JOIN_BUFFER_SEC", "120"))
    establish_pad = float(os.environ.get("FB_POOL_ESTABLISH_PAD_SEC", "150"))
    floor = float(os.environ.get("FB_POOL_JOIN_MIN_SEC", "900"))  # 15 phút
    cap = float(os.environ.get("FB_POOL_JOIN_MAX_SEC", "3600"))  # 60 phút
    worker_sec = float(os.environ.get("FB_HUMAN_WORKER_MAX_SEC", "300"))
    retries = max(0, int(os.environ.get("FB_HUMAN_INTERACTION_MAX_RETRIES", "2")))
    attempts = max(1, 1 + retries)
    waves = max(1, (n + mc - 1) // mc)
    # Wall-clock: mỗi đợt có thể chạy đủ worker_sec × số lần thử lại / TK.
    worker_est = waves * worker_sec * attempts + establish_pad + buffer
    target_est = target_min * 60.0 + buffer + establish_pad
    if waves > 3:
        target_est += min(240.0, (waves - 3) * 75.0)
    est = max(worker_est, target_est)
    return max(floor, min(cap, est))


def format_human_pool_error(exc: BaseException) -> str:
    """Chuyển lỗi Playwright/proxy sang tiếng Việt ngắn cho cột trạng thái."""
    msg = str(exc or "").strip()
    low = msg.lower()
    if "timeout" in low and ("goto" in low or "navigation" in low):
        return "Facebook tải chậm (timeout) — kiểm tra proxy/mạng, bấm «Chạy lại»"
    if "timeout" in low and "launch" in low:
        return "Mở Firefox chậm — giảm số luồng hoặc thử lại sau vài phút"
    if "target closed" in low or "has been closed" in low:
        return "Trình duyệt đóng bất ngờ — sẽ thử lại nếu còn lượt"
    if "ns_error" in low or "net::" in low or "proxy" in low:
        return f"Lỗi mạng/proxy: {msg[:140]}"
    return msg[:200]


class HumanInteractionPool:
    """
    Pool worker với ``max_concurrent`` luồng; mỗi tài khoản lấy một slot lưới cố định.
    """

    def __init__(
        self,
        accounts: list[MappedAccount],
        *,
        max_concurrent: int = 4,
        headless: bool = False,
        profile: HumanInteractionProfile | None = None,
        auto_profile: bool = False,
        login_only: bool = False,
        max_cols: int = 4,
        on_status: StatusCallback | None = None,
        on_done: DoneCallback | None = None,
    ) -> None:
        self._accounts = list(accounts)
        self._max_concurrent = max(1, int(max_concurrent))
        self._headless = bool(headless)
        self._profile = profile or resolve_profile("normal")
        self._auto_profile = bool(auto_profile)
        self._login_only = bool(login_only)
        self._on_status = on_status
        self._on_done = on_done
        self._user_cancel = threading.Event()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._monitor_thread: threading.Thread | None = None
        self._in_use_proxies: set[str] = set()
        self._in_use_accounts: set[str] = set()
        self._proxy_lock = threading.Lock()
        self._account_lock = threading.Lock()
        self._work_q: queue.Queue[MappedAccount | None] = queue.Queue()
        self._grid_slots = compute_grid_layout(self._max_concurrent, max_cols=max(1, int(max_cols)))
        self._state_lock = threading.Lock()
        self._state_cv = threading.Condition(self._state_lock)
        self._running = 0
        self._dynamic_limit = self._max_concurrent
        self._recent_results: deque[str] = deque(maxlen=30)
        self._workload_registered = False
        self._shutting_down = False
        self._total_accounts = len(self._accounts)
        self._completed_accounts = 0
        self._join_workers_alive = False
        self._retry_counts: dict[str, int] = {}
        self._graceful_shutdown = False
        # Đăng nhập: luôn chạy đủ N luồng, không tự hạ concurrency.
        if self._login_only:
            self._recent_results.clear()

    @property
    def grid_slots(self) -> list[GridWindowSlot]:
        return list(self._grid_slots)

    def health_snapshot(self) -> dict[str, Any]:
        with self._state_lock:
            recent = list(self._recent_results)
            pending = max(0, int(self._total_accounts) - int(self._completed_accounts))
            return {
                "dynamic_limit": self._dynamic_limit,
                "configured_limit": self._max_concurrent,
                "running": self._running,
                "profile": self._profile.name,
                "auto_profile": self._auto_profile,
                "recent_total": len(recent),
                "recent_proxy_error": sum(1 for r in recent if r == "proxy_error"),
                "recent_error": sum(1 for r in recent if r in {"error", "login_failed"}),
                "recent_success": sum(1 for r in recent if r in {"success", "login_ok"}),
                "total_accounts": int(self._total_accounts),
                "completed_accounts": int(self._completed_accounts),
                "pending_accounts": pending,
            }

    def is_stopped(self) -> bool:
        """True sau khi người dùng bấm Dừng (không gồm pool join/shutdown nội bộ)."""
        return self._user_cancel.is_set()

    def should_abort_worker(self) -> bool:
        """
        Worker/module dừng sớm khi user bấm Dừng hoặc pool join timeout.

        Không dùng khi ``shutdown_gracefully`` — để worker kịp ghi profile/cookie.
        """
        if self._user_cancel.is_set():
            return True
        if self._stop.is_set() and not self._graceful_shutdown:
            return True
        return False

    def stop(self) -> None:
        """
        Yêu cầu dừng — hủy tài khoản còn trong hàng đợi; worker đang chạy sẽ thoát sớm khi có thể.
        """
        self._shutting_down = True
        self._user_cancel.set()
        self._stop.set()
        with self._state_cv:
            self._state_cv.notify_all()
        cancelled = 0
        while True:
            try:
                item = self._work_q.get_nowait()
            except queue.Empty:
                break
            if item is not None:
                cancelled += 1
                self._emit_status(item, "cancelled", "Đã hủy — người dùng bấm Dừng")
            # Mỗi ``put`` phải có ``task_done`` — kể cả item bị hủy trước khi worker ``get()``.
            self._work_q.task_done()
        if cancelled:
            logger.info("[Human pool] Đã hủy {} tài khoản còn trong hàng đợi.", cancelled)
        for _ in range(self._max_concurrent):
            self._work_q.put(None)

    def shutdown_gracefully(self, timeout: float | None = 60.0) -> bool:
        """
        Chờ worker đóng Firefox và ghi profile — dùng khi thoát app (không đánh dấu «Dừng»).

        Không ``stop()``/``user_cancel`` — worker đang mở browser vẫn chạy ``finally`` lưu cookie/profile.
        """
        if not self._threads:
            return True
        self._graceful_shutdown = True
        self._shutting_down = True
        logger.info(
            "[Human pool] Graceful shutdown — chờ {} worker ghi profile (timeout={}s)",
            len(self._threads),
            timeout,
        )
        self._signal_workers_exit()
        return self.join(timeout=timeout, allow_firefox_terminate=False)

    def start(self) -> None:
        """Nạp hàng đợi và khởi động worker threads."""
        if self._threads:
            return
        self._user_cancel.clear()
        self._stop.clear()
        self._retry_counts.clear()
        self._dynamic_limit = self._max_concurrent
        try:
            from src.utils.concurrency_runtime import KIND_BROWSER_POST, workload_begin

            workload_begin(KIND_BROWSER_POST)
            self._workload_registered = True
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Human pool] workload_begin: {}", exc)
        for acc in self._accounts:
            acc.status = "pending"
            try:
                from src.utils.account_proxy_mapper import prepare_mapped_account_for_browser_run

                prepare_mapped_account_for_browser_run(acc)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[Human pool] Chuẩn bị profile/cookie account={}: {}",
                    acc.account_id,
                    exc,
                )
            self._work_q.put(acc)

        for wi in range(self._max_concurrent):
            t = threading.Thread(
                target=self._worker_loop,
                args=(wi,),
                name=f"human-interaction-{wi}",
                daemon=True,
            )
            self._threads.append(t)
            t.start()
        self._monitor_thread = threading.Thread(
            target=self._health_monitor_loop,
            name="human-health-monitor",
            daemon=True,
        )
        self._monitor_thread.start()
        mode = "login" if self._login_only else "interaction"
        ids_preview = ", ".join(a.display_uid() for a in self._accounts[:8])
        if len(self._accounts) > 8:
            ids_preview += f", … (+{len(self._accounts) - 8})"
        logger.info(
            "[Human pool] Start {} worker | {} tài khoản [{}] | grid {} slot | profile={} | mode={}",
            self._max_concurrent,
            len(self._accounts),
            ids_preview,
            len(self._grid_slots),
            self._profile.name,
            mode,
        )

    def _signal_workers_exit(self) -> None:
        """Gửi sentinel ``None`` để worker thoát vòng lặp (mỗi worker một lần)."""
        for _ in range(max(1, len(self._threads))):
            try:
                self._work_q.put_nowait(None)
            except Exception:
                pass

    def _release_runtime(self) -> None:
        """Giải phóng workload registry + monitor — gọi sau khi worker đã join."""
        if self._monitor_thread is not None:
            try:
                self._monitor_thread.join(timeout=4.0)
            except Exception:
                pass
            self._monitor_thread = None
        if self._workload_registered:
            try:
                from src.utils.concurrency_runtime import KIND_BROWSER_POST, workload_end

                workload_end(KIND_BROWSER_POST)
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human pool] workload_end: {}", exc)
            self._workload_registered = False

    def _drain_queue_task_done(self) -> None:
        """``task_done`` item còn trong queue — đánh dấu TK chưa chạy thay vì bỏ qua."""
        stranded: list[MappedAccount] = []
        while True:
            try:
                item = self._work_q.get_nowait()
            except queue.Empty:
                break
            if item is not None:
                stranded.append(item)
            self._work_q.task_done()
        for ma in stranded:
            if self._user_cancel.is_set():
                self._emit_status(ma, "cancelled", "Đã hủy — người dùng bấm Dừng")
            else:
                self._emit_status(
                    ma,
                    "pending",
                    "Chưa kịp chạy trong lượt này — sẽ tự động chạy tiếp",
                )

    def _terminate_pool_browser_processes(self, *, force: bool = False) -> None:
        """
        Ép đóng Firefox còn sót khi worker không thoát sạch (Windows + profile portable).

        Không gọi sau join bình thường — ``terminate`` quá sớm làm Firefox không kịp ghi ``cookies.sqlite``.
        """
        if os.name != "nt":
            return
        if not force and not getattr(self, "_join_workers_alive", False):
            logger.debug("[Human pool] Bỏ terminate Firefox — worker đã join sạch")
            return
        from src.utils.win_browser_window import terminate_firefox_for_profile

        grace = max(500, int(float(os.environ.get("FB_FIREFOX_KILL_GRACE_MS", "2500"))))
        seen: set[str] = set()
        for ma in self._accounts:
            prof = str(getattr(getattr(ma, "storage", None), "profile_path", "") or "").strip()
            if not prof:
                try:
                    from src.utils.account_proxy_mapper import mapped_account_to_account_dict

                    acc = mapped_account_to_account_dict(ma)
                    prof = str(acc.get("portable_path") or acc.get("profile_path") or "").strip()
                except Exception:
                    prof = ""
            if not prof:
                continue
            key = str(Path(prof).resolve()).lower()
            if key in seen:
                continue
            seen.add(key)
            try:
                terminate_firefox_for_profile(prof, grace_ms=grace)
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human pool] terminate profile {}: {}", prof[-48:], exc)

    def has_live_workers(self) -> bool:
        """True nếu còn worker thread đang chạy (có thể vẫn mở browser)."""
        return any(t.is_alive() for t in self._threads)

    def join(self, timeout: float | None = None, *, allow_firefox_terminate: bool = True) -> bool:
        """
        Chờ hết hàng đợi, đóng worker threads và monitor.

        ``allow_firefox_terminate=False``: không kill Firefox khi join (thoát app an toàn).

        Returns:
            True nếu tất cả worker đã kết thúc trong ``timeout``; False nếu hết giờ (đã cố ép dừng).
        """
        deadline = None if timeout is None else time.monotonic() + max(0.0, float(timeout))
        join_started = time.monotonic()
        abs_cap = float(os.environ.get("FB_POOL_JOIN_ABSOLUTE_MAX_SEC", "4200"))

        def _remaining() -> float | None:
            if deadline is None:
                return None
            return max(0.0, deadline - time.monotonic())

        def _absolute_exceeded() -> bool:
            return (time.monotonic() - join_started) >= abs_cap

        self._shutting_down = False
        completed = True
        workers_alive = False
        try:
            # Chờ hết hàng đợi + mọi TK đã ``task_done`` — không gửi _stop trong lúc chờ.
            while True:
                all_accounts_done = self._completed_accounts >= self._total_accounts
                queue_drained = self._work_q.unfinished_tasks <= 0
                if all_accounts_done and queue_drained:
                    break
                rem = _remaining()
                if rem is not None and rem <= 0:
                    if self.has_live_workers() and not _absolute_exceeded():
                        # Gia hạn mềm — worker vẫn chạy, chưa vượt trần tuyệt đối.
                        if rem <= 0 and deadline is not None:
                            extra_soft = float(os.environ.get("FB_POOL_JOIN_SOFT_EXTEND_SEC", "180"))
                            deadline = time.monotonic() + max(60.0, extra_soft)
                            logger.info(
                                "[Human pool] Gia hạn mềm join {:.0f}s — {}/{} TK, worker còn chạy",
                                extra_soft,
                                self._completed_accounts,
                                self._total_accounts,
                            )
                        time.sleep(0.35)
                        continue
                    completed = False
                    logger.warning(
                        "[Human pool] join timeout — done {}/{} TK, còn {} task chưa task_done.",
                        self._completed_accounts,
                        self._total_accounts,
                        self._work_q.unfinished_tasks,
                    )
                    break
                time.sleep(min(0.35, rem or 0.35))

            if (
                not completed
                and (
                    self._completed_accounts < self._total_accounts
                    or self._work_q.unfinished_tasks > 0
                )
                and self.has_live_workers()
            ):
                extra = float(os.environ.get("FB_POOL_JOIN_EXTRA_SEC", "300"))
                logger.warning(
                    "[Human pool] Gia hạn join {:.0f}s — còn {} task, worker vẫn chạy",
                    extra,
                    self._work_q.unfinished_tasks,
                )
                extra_deadline = time.monotonic() + max(60.0, extra)
                while (
                    time.monotonic() < extra_deadline
                    and self._completed_accounts < self._total_accounts
                    and self.has_live_workers()
                ):
                    if self._work_q.unfinished_tasks <= 0 and self._completed_accounts >= self._total_accounts:
                        break
                    time.sleep(0.35)
                if self._completed_accounts >= self._total_accounts and self._work_q.unfinished_tasks <= 0:
                    completed = True
                    logger.info("[Human pool] Gia hạn join — đã xử lý hết tài khoản.")
                elif self._work_q.unfinished_tasks > 0 or self._completed_accounts < self._total_accounts:
                    completed = False
                else:
                    completed = True
                    logger.info("[Human pool] Gia hạn join — đã xử lý hết task trong hàng đợi.")

            self._shutting_down = True
            self._stop.set()
            with self._state_cv:
                self._state_cv.notify_all()
            self._signal_workers_exit()

            thread_grace = max(
                15.0,
                float(os.environ.get("FB_POOL_JOIN_THREAD_GRACE_SEC", "45")),
            )
            for t in list(self._threads):
                rem = _remaining()
                if rem is None:
                    t.join()
                elif rem > 0:
                    t.join(timeout=rem)
                else:
                    t.join(timeout=thread_grace)
                    completed = False
                if t.is_alive():
                    completed = False
                    workers_alive = True
                    logger.warning("[Human pool] Worker {} vẫn chạy sau join.", t.name)
        finally:
            self._join_workers_alive = workers_alive
            if self._work_q.unfinished_tasks > 0:
                logger.warning(
                    "[Human pool] join — còn {} task, drain queue.",
                    self._work_q.unfinished_tasks,
                )
                self._drain_queue_task_done()
            try:
                if allow_firefox_terminate:
                    self._terminate_pool_browser_processes(force=workers_alive)
                else:
                    logger.info(
                        "[Human pool] Bỏ terminate Firefox — graceful shutdown (workers_alive={})",
                        workers_alive,
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning("[Human pool] terminate pool browsers: {}", exc)
            self._threads.clear()
            self._release_runtime()
            self._shutting_down = False
            with self._state_lock:
                self._running = 0
        return completed

    def _proxy_key(self, mapped: MappedAccount) -> str:
        from src.utils.account_proxy_mapper import proxy_identity_key_for_account

        return proxy_identity_key_for_account(mapped)

    def _acquire_proxy(self, mapped: MappedAccount) -> bool:
        key = self._proxy_key(mapped)
        if not key:
            return True
        with self._proxy_lock:
            if key in self._in_use_proxies:
                return False
            self._in_use_proxies.add(key)
            return True

    def _release_proxy(self, mapped: MappedAccount) -> None:
        key = self._proxy_key(mapped)
        if not key:
            return
        with self._proxy_lock:
            self._in_use_proxies.discard(key)

    def _account_key(self, mapped: MappedAccount) -> str:
        return str(mapped.account_id or "").strip()

    def _acquire_account(self, mapped: MappedAccount) -> bool:
        key = self._account_key(mapped)
        if not key:
            return True
        with self._account_lock:
            if key in self._in_use_accounts:
                return False
            self._in_use_accounts.add(key)
            return True

    def _release_account(self, mapped: MappedAccount) -> None:
        key = self._account_key(mapped)
        if not key:
            return
        with self._account_lock:
            self._in_use_accounts.discard(key)

    def _emit_status(self, mapped: MappedAccount, status: str, detail: str) -> None:
        mapped.status = status
        mapped.status_detail = detail
        if self._on_status:
            try:
                self._on_status(mapped, status, detail)
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human pool] on_status callback lỗi: {}", exc)

    def _on_worker_done(self, result: str) -> None:
        with self._state_cv:
            self._running = max(0, self._running - 1)
            self._recent_results.append(str(result or "error"))
            self._state_cv.notify_all()

    def _release_concurrency_slot(self) -> None:
        """Giảm ``_running`` khi chưa chạy worker (ví dụ chờ proxy) — không ghi vào health."""
        with self._state_cv:
            self._running = max(0, self._running - 1)
            self._state_cv.notify_all()

    def _enter_slot(self) -> bool:
        """
        Chờ slot concurrency; trả False nếu pool đã dừng trước khi vào slot.

        Chế độ ``login_only``: luôn dùng ``max_concurrent`` (không bị health monitor hạ).
        """
        limit = self._max_concurrent if self._login_only else self._dynamic_limit
        with self._state_cv:
            while not self._stop.is_set() and self._running >= limit:
                self._state_cv.wait(timeout=0.5)
            if self._stop.is_set():
                return False
            self._running += 1
            return True

    def _health_monitor_loop(self) -> None:
        while not self._stop.is_set() and not self._shutting_down:
            if self._stop.wait(timeout=8.0):
                break
            if self._login_only or self._shutting_down:
                continue
            with self._state_cv:
                recent = list(self._recent_results)
                if not recent:
                    continue
                proxy_errors = sum(1 for r in recent if r == "proxy_error")
                runtime_errors = sum(1 for r in recent if r in {"error", "login_failed", "browser_closed"})
                successes = sum(1 for r in recent if r == "success")
                n = len(recent)
                proxy_ratio = proxy_errors / float(n)
                err_ratio = runtime_errors / float(n)
                # Tự hạ tốc khi lỗi cao; tự nâng nhẹ khi ổn định.
                new_limit = self._dynamic_limit
                if proxy_ratio >= 0.30 or err_ratio >= 0.40:
                    new_limit = max(1, self._dynamic_limit - 1)
                elif successes >= max(8, int(n * 0.7)) and proxy_errors == 0 and err_ratio < 0.15:
                    new_limit = min(self._max_concurrent, self._dynamic_limit + 1)
                if new_limit != self._dynamic_limit:
                    self._dynamic_limit = new_limit
                    logger.warning(
                        "[Human health] Điều chỉnh concurrency động: {} (proxy_ratio={:.0%}, err_ratio={:.0%}, recent={})",
                        self._dynamic_limit,
                        proxy_ratio,
                        err_ratio,
                        n,
                    )
                    self._state_cv.notify_all()
                if self._auto_profile:
                    new_profile = self._profile.name
                    if proxy_ratio >= 0.30 or err_ratio >= 0.40:
                        new_profile = "safe"
                    elif successes >= max(8, int(n * 0.7)) and proxy_errors == 0 and err_ratio < 0.15:
                        new_profile = "fast"
                    else:
                        new_profile = "normal"
                    if new_profile != self._profile.name:
                        prev = self._profile
                        new_base = resolve_profile(new_profile)
                        self._profile = replace(
                            new_base,
                            like_rate_pct=prev.like_rate_pct,
                            comment_rate_pct=prev.comment_rate_pct,
                            virtual_cursor=prev.virtual_cursor,
                            ai_comments=prev.ai_comments,
                        )
                        logger.warning("[Human health] Auto profile -> {}", self._profile.name)

    @staticmethod
    def _sleep_interruptible(seconds: float, stop_event: threading.Event) -> None:
        """Ngủ có thể bị ngắt bởi stop — tránh worker kẹt lâu khi bấm Dừng."""
        end = time.monotonic() + max(0.0, float(seconds))
        while not stop_event.is_set() and time.monotonic() < end:
            time.sleep(min(0.25, max(0.05, end - time.monotonic())))

    @staticmethod
    def _max_account_retries() -> int:
        return max(0, int(os.environ.get("FB_HUMAN_INTERACTION_MAX_RETRIES", "2")))

    @staticmethod
    def _retryable_result(result: str) -> bool:
        return str(result or "") in {"browser_closed", "error", "interrupted"}

    def _worker_loop(self, worker_index: int) -> None:
        """Mỗi worker giữ một ô lưới cố định — cửa sổ không nhảy vị trí giữa các tài khoản."""
        slot = self._grid_slots[worker_index % len(self._grid_slots)]
        first_account_in_worker = True
        # Tránh 4 Firefox cùng lúc tranh CPU/GPU ngay khi pool start.
        warm_ms = max(0, int(float(os.environ.get("FB_POOL_WORKER_WARMUP_MS", "350"))))
        if worker_index > 0 and warm_ms > 0:
            self._sleep_interruptible((worker_index * warm_ms) / 1000.0, self._stop)
        while True:
            try:
                mapped = self._work_q.get(timeout=0.5)
            except queue.Empty:
                if self._stop.is_set():
                    self._drain_queue_task_done()
                    break
                continue
            if mapped is None:
                self._work_q.task_done()
                break
            if self._user_cancel.is_set():
                self._emit_status(mapped, "cancelled", "Đã hủy — người dùng bấm Dừng")
                self._work_q.task_done()
                continue

            mapped.grid_slot_index = slot.index
            if not self._enter_slot():
                if self._user_cancel.is_set():
                    self._emit_status(mapped, "cancelled", "Đã hủy — người dùng bấm Dừng")
                else:
                    self._work_q.put(mapped)
                self._work_q.task_done()
                continue

            counted_done = False

            def _account_work_done(result: str) -> None:
                nonlocal counted_done
                if counted_done:
                    return
                counted_done = True
                self._on_worker_done(result)
                with self._state_lock:
                    self._completed_accounts += 1

            try:
                if not self._acquire_proxy(mapped):
                    if self._user_cancel.is_set():
                        self._emit_status(mapped, "cancelled", "Đã hủy — người dùng bấm Dừng")
                    else:
                        self._emit_status(mapped, "waiting", "Chờ proxy trống — đưa lại hàng đợi")
                        self._sleep_interruptible(random.uniform(1.0, 2.8), self._stop)
                        if not self._user_cancel.is_set():
                            self._work_q.put(mapped)
                        else:
                            self._emit_status(mapped, "cancelled", "Đã hủy — người dùng bấm Dừng")
                    continue

                if not self._acquire_account(mapped):
                    self._release_proxy(mapped)
                    if self._user_cancel.is_set():
                        self._emit_status(mapped, "cancelled", "Đã hủy — người dùng bấm Dừng")
                    else:
                        self._emit_status(mapped, "waiting", "Chờ tài khoản trống — đưa lại hàng đợi")
                        self._sleep_interruptible(random.uniform(0.8, 2.0), self._stop)
                        if not self._user_cancel.is_set():
                            self._work_q.put(mapped)
                        else:
                            self._emit_status(mapped, "cancelled", "Đã hủy — người dùng bấm Dừng")
                    continue

                self._emit_status(
                    mapped,
                    "waiting",
                    f"Ô lưới {slot.index + 1} ({slot.width}×{slot.height} @ {slot.x},{slot.y})",
                )

                def _cb(st: str, detail: str) -> None:
                    self._emit_status(mapped, st, detail)

                max_retries = self._max_account_retries()
                aid = str(mapped.account_id or "")
                result = run_human_interaction_worker(
                    mapped,
                    grid_slot=slot,
                    headless=self._headless,
                    profile=self._profile,
                    on_status=_cb,
                    login_only=self._login_only,
                    should_stop=self.should_abort_worker,
                    is_user_cancelled=self.is_stopped,
                    skip_launch_stagger=not first_account_in_worker,
                )
                first_account_in_worker = False
                while (
                    self._retryable_result(result)
                    and self._retry_counts.get(aid, 0) < max_retries
                    and not self._user_cancel.is_set()
                ):
                    self._retry_counts[aid] = self._retry_counts.get(aid, 0) + 1
                    attempt_n = self._retry_counts[aid]
                    logger.warning(
                        "[Human pool] Thử lại account={} lần {}/{} (kết quả={})",
                        aid,
                        attempt_n,
                        max_retries,
                        result,
                    )
                    self._emit_status(
                        mapped,
                        "waiting",
                        f"Lỗi tạm — đóng và thử lại ({attempt_n}/{max_retries})…",
                    )
                    self._sleep_interruptible(random.uniform(3.5, 8.0), self._stop)
                    if self._user_cancel.is_set():
                        result = "cancelled"
                        self._emit_status(mapped, "cancelled", "Đã hủy — người dùng bấm Dừng")
                        break
                    result = run_human_interaction_worker(
                        mapped,
                        grid_slot=slot,
                        headless=self._headless,
                        profile=self._profile,
                        on_status=_cb,
                        login_only=self._login_only,
                        should_stop=self.should_abort_worker,
                        is_user_cancelled=self.is_stopped,
                        skip_launch_stagger=True,
                    )
                if not counted_done:
                    _account_work_done(result)
            except Exception as exc:  # noqa: BLE001
                terminal = str(mapped.status or "").strip()
                if terminal in ("success", "login_ok"):
                    logger.warning(
                        "[Human pool] Lỗi sau khi TK {} đã thành công — giữ trạng thái: {}",
                        mapped.account_id,
                        exc,
                    )
                    if not counted_done:
                        _account_work_done("success" if terminal == "success" else "login_ok")
                else:
                    logger.exception("[Human pool] Worker lỗi account={}: {}", mapped.account_id, exc)
                    self._emit_status(mapped, "error", format_human_pool_error(exc))
                    if not counted_done:
                        _account_work_done("error")
            finally:
                if not counted_done:
                    self._release_concurrency_slot()
                self._release_account(mapped)
                self._release_proxy(mapped)
                self._work_q.task_done()


def validate_pool_start(
    account_count: int,
    proxy_count: int,
    max_concurrent: int,
    *,
    unique_proxy_count: int | None = None,
    accounts: list[Any] | None = None,
) -> None:
    """
    Kiểm tra ràng buộc trước khi chạy: đủ proxy riêng, không trùng, mỗi TK có proxy.

    Raises:
        AccountProxyMappingError: proxy < max_concurrent hoặc trùng/thiếu proxy.
    """
    from src.models.mapped_account import MappedAccount
    from src.utils.account_proxy_mapper import (
        accounts_without_proxy,
        duplicate_proxy_assignments,
    )

    if account_count == 0:
        raise AccountProxyMappingError("Không có tài khoản trong hàng đợi.")
    mc = max(1, int(max_concurrent))
    need = min(mc, account_count)
    px_n = int(unique_proxy_count if unique_proxy_count is not None else proxy_count)
    if px_n < need:
        raise AccountProxyMappingError(
            f"Cần ít nhất {need} IP:port proxy khác nhau cho {need} luồng (đang có {px_n}). "
            "Mỗi IP chỉ gắn một tài khoản — kiểm tra danh sách proxy trùng."
        )

    if accounts:
        mapped = [a for a in accounts if isinstance(a, MappedAccount)]
        if mapped:
            seen_ids: set[str] = set()
            dup_ids: list[str] = []
            for item in mapped:
                aid = str(getattr(item, "account_id", "") or "").strip()
                if not aid:
                    continue
                if aid in seen_ids:
                    if aid not in dup_ids:
                        dup_ids.append(aid)
                else:
                    seen_ids.add(aid)
            if dup_ids:
                raise AccountProxyMappingError(
                    "Trùng account_id trong hàng đợi (mỗi TK chỉ chạy một luồng): "
                    + ", ".join(dup_ids[:8])
                    + ("…" if len(dup_ids) > 8 else "")
                )
            missing = accounts_without_proxy(mapped)
            if missing:
                raise AccountProxyMappingError(
                    f"{len(missing)} tài khoản thiếu proxy: {', '.join(missing[:6])}"
                    + ("…" if len(missing) > 6 else "")
                )
            dups = duplicate_proxy_assignments(mapped)
            if dups:
                lines: list[str] = []
                for px_key, aids in list(dups.items())[:6]:
                    short_px = px_key if len(px_key) <= 48 else px_key[:45] + "..."
                    lines.append(f"• {short_px} → {', '.join(aids)}")
                raise AccountProxyMappingError(
                    "Có IP:port proxy trùng (mỗi IP chỉ một tài khoản, kể cả đã login hay chưa):\n"
                    + "\n".join(lines)
                )
