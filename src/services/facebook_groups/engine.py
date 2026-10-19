"""Hàng đợi tìm nhóm, tham gia và chia sẻ link. Một account một job đang chạy."""

from __future__ import annotations

import logging
import os
import random
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from src.services.page_creation.engine import calculate_delay_seconds
from src.utils.feature_busy import (
    acquire_share_after_schedule,
    get_conflict_gap_minutes,
    normalize_page_keys,
    parse_conflict_gap_minutes,
)

from .fingerprint import assert_link_only, classify_source_url, normalize_share_images, share_fingerprint
from .video_engage import (
    allocate_wave_page_engage,
    parse_comment_lines,
    pick_page_watch_seconds,
    pick_share_comment,
)
from .match import passes_discovery_filters, qualify_group, topic_match_score
from .states import (
    BATCH_CANCELLED,
    BATCH_COMPLETED,
    BATCH_DELAYING,
    BATCH_FAILED,
    BATCH_PAUSED,
    BATCH_READY,
    BATCH_RUNNING,
    BATCH_VALIDATING,
    DISCOVERY_ACTIVE,
    DISCOVERY_COLLECTING,
    DISCOVERY_COMPLETED,
    DISCOVERY_DEDUPLICATING,
    DISCOVERY_FAILED,
    DISCOVERY_FILTERING,
    DISCOVERY_PAUSED,
    DISCOVERY_RATE_LIMITED,
    DISCOVERY_RUNNING,
    DISCOVERY_SAVING,
    ERR_RATE,
    ERR_SESSION,
    JOIN_ACTIVE,
    JOIN_ALREADY,
    JOIN_APPROVAL,
    JOIN_CHECKING_STATUS,
    JOIN_DELAYED,
    JOIN_FAILED,
    JOIN_JOINED,
    JOIN_JOINING,
    JOIN_NOT_AVAILABLE,
    JOIN_PAUSED,
    JOIN_PENDING,
    JOIN_VERIFYING,
    JOIN_WAITING_REAUTH,
    MAX_RETRY,
    PAUSE_CODES,
    SHARE_ACTIVE,
    SHARE_CHECKING_PERMISSION,
    SHARE_COMPLETED,
    SHARE_DUPLICATE,
    SHARE_FAILED,
    SHARE_PAUSED,
    SHARE_PENDING,
    SHARE_SKIPPED,
    SHARE_SUBMIT,
    SHARE_VERIFYING,
    SHARE_VERIFY_FIRST,
    SHARE_WAITING_REAUTH,
    SKIP_CODES,
    TEMPORARY_ERROR_CODES,
)
from .store import GroupStore

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_group_uids(raw: str) -> list[str]:
    """Lấy UID nhóm từ số hoặc link ``facebook.com/groups/123``. Bỏ trùng, giữ thứ tự."""
    found: list[str] = []
    seen: set[str] = set()
    for token in re.split(r"[\s,;]+", str(raw or "").strip()):
        if not token:
            continue
        match = re.search(r"/groups/(\d{5,20})", token, flags=re.I)
        value = match.group(1) if match else token.strip()
        if not re.fullmatch(r"\d{5,20}", value) or value in seen:
            continue
        seen.add(value)
        found.append(value)
    return found


class GroupEngine:
    """Chạy tuần tự trong một account. Account khác không bị dừng theo."""

    def __init__(
        self,
        store: GroupStore,
        provider: Any,
        *,
        sleep: Callable[[float], None] | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.store = store
        self.provider = provider
        self._sleep = sleep or time.sleep
        self._rng = rng or random.Random()
        self._rng_lock = threading.Lock()
        self._step_lock = threading.Lock()
        self._live_lock = threading.Lock()
        self._live_batches: set[str] = set()
        self._live_steps: dict[str, dict[str, str]] = {}
        self._account_labels: dict[str, str] = {}
        # Mỗi tài khoản tối đa 1 browser / 1 Page share tại một thời điểm (nhiều TK vẫn song song).
        self._account_run_guard = threading.Lock()
        self._account_run_locks: dict[str, threading.Lock] = {}
        self._tls = threading.local()
        self._cancel: set[str] = set()

    def _account_share_lock(self, account_id: str) -> threading.Lock:
        """Khóa theo account_id — không cho cùng TK mở 2 Firefox / chạy 2 Page cùng lúc."""
        aid = str(account_id or "").strip() or "_unknown_"
        with self._account_run_guard:
            lock = self._account_run_locks.get(aid)
            if lock is None:
                lock = threading.Lock()
                self._account_run_locks[aid] = lock
            return lock

    def recover(self) -> dict[str, int]:
        """
        Sau khi mở lại app: job đang dở được xác minh lại; batch RUNNING/DELAYING mồ côi
        (process cũ chết) → READY để không khóa tài khoản mãi và còn chạy tiếp được.
        """
        counts = {"discovery": 0, "join": 0, "share": 0, "share_batches": 0}
        for job in self.store.discovery_jobs():
            if job.get("status") in DISCOVERY_ACTIVE:
                job["status"] = "PENDING"
                job["error_message"] = "Khôi phục — chạy lại từ đầu vì lượt tìm chưa xong"
                self.store.save_discovery_job(job)
                counts["discovery"] += 1
        for job in self.store.join_jobs():
            if job.get("status") == JOIN_JOINING or job.get("status") in JOIN_ACTIVE:
                job["status"] = JOIN_VERIFYING
                job["error_message"] = "Khôi phục — kiểm tra đã vào nhóm chưa trước khi tham gia lại"
                self.store.save_join_job(job)
                counts["join"] += 1
        for job in self.store.share_jobs():
            if job.get("status") in SHARE_ACTIVE or job.get("status") == SHARE_SUBMIT:
                job["status"] = SHARE_VERIFY_FIRST
                job["error_message"] = "Khôi phục — kiểm tra bài đã đăng chưa trước khi đăng lại"
                self.store.save_share_job(job)
                counts["share"] += 1
        orphan_status = {BATCH_VALIDATING, BATCH_RUNNING, BATCH_DELAYING}
        for batch in self.store.share_batches():
            if batch.get("status") not in orphan_status:
                continue
            batch_id = str(batch.get("id") or "")
            pending = [
                job
                for job in self._batch_jobs(batch_id)
                if job.get("status") in {SHARE_PENDING, SHARE_VERIFY_FIRST}
            ]
            if pending:
                batch["status"] = BATCH_READY
                batch["cooldown_remaining"] = 0
                batch["error_message"] = "Khôi phục sau tắt/mở app — chờ chạy tiếp"
            else:
                failed = int(batch.get("failed_jobs") or 0)
                done = int(batch.get("completed_jobs") or 0)
                batch["status"] = BATCH_FAILED if failed and not done else BATCH_COMPLETED
                batch["finished_at"] = batch.get("finished_at") or _now()
                batch["cooldown_remaining"] = 0
            self.store.save_share_batch(batch)
            counts["share_batches"] += 1
        if counts["share_batches"] or counts["share"]:
            logger.info(
                "Khôi phục chia sẻ: %s job, %s batch mồ côi → READY/COMPLETED.",
                counts["share"],
                counts["share_batches"],
            )
        return counts

    def incomplete_share_batch_ids_for_url(self, source_url: str) -> list[str]:
        """Batch còn bài chưa xong của đúng video — dùng khi mở lại app để chạy tiếp, không tạo wave trùng."""
        want = str(source_url or "").strip()
        if not want:
            return []
        found: list[str] = []
        for batch in self.store.share_batches():
            if str(batch.get("source_url") or "").strip() != want:
                continue
            if batch.get("status") not in {BATCH_READY, BATCH_PAUSED}:
                continue
            batch_id = str(batch.get("id") or "")
            if not batch_id:
                continue
            pending = [
                job
                for job in self._batch_jobs(batch_id)
                if job.get("status") in {SHARE_PENDING, SHARE_VERIFY_FIRST}
            ]
            if pending:
                found.append(batch_id)
        return found

    def run_discovery(self, job_id: str) -> dict[str, Any]:
        job = self._discovery(job_id)
        account_id = str(job.get("account_id") or "")
        blocked = self._session_block(account_id)
        if blocked:
            job["status"] = DISCOVERY_PAUSED if blocked != ERR_RATE else DISCOVERY_RATE_LIMITED
            job["error_code"] = blocked
            self.store.save_discovery_job(job)
            return job
        job["status"] = DISCOVERY_RUNNING
        job["started_at"] = _now()
        self.store.save_discovery_job(job)
        job["status"] = DISCOVERY_COLLECTING
        self.store.save_discovery_job(job)
        self._begin(account_id)
        try:
            return self._collect_discovery(job, account_id)
        finally:
            self._end(account_id)

    def pause_discovery(self, job_id: str) -> None:
        """Dừng trước query kế tiếp. Không mở thêm lượt tìm."""
        self._cancel.add(str(job_id))

    def resume_discovery(self, job_id: str) -> dict[str, Any]:
        """Chạy tiếp từ query đã lưu sau khi người dùng xử lý xong."""
        self._cancel.discard(str(job_id))
        return self.run_discovery(job_id)

    def _catalog_names(self, kind: str, codes: list[str]) -> list[tuple[str, str]]:
        catalog = {
            str(row.get("code") or ""): str(row.get("name") or "")
            for row in (self.store.catalog().get(kind) or [])
        }
        chosen = [str(item).strip() for item in codes if str(item).strip()]
        if not chosen:
            return [("", "")]
        return [(code, catalog.get(code) or code) for code in chosen]

    def _country_places(self, codes: list[str]) -> list[tuple[str, str]]:
        return self._catalog_names("countries", codes)

    def _search_queries(self, job: dict[str, Any]) -> list[dict[str, str]]:
        """Ô tìm Facebook chỉ nhận từ khóa. Tên nước và tên tiếng không được ghép vào câu tìm."""
        keywords = [str(item).strip() for item in (job.get("keywords") or []) if str(item).strip()]
        topic = str(job.get("topic") or "").strip()
        terms: list[str] = []
        if topic and topic.casefold() not in {item.casefold() for item in keywords}:
            terms.append(topic)
        terms.extend(keywords)
        if not terms:
            terms = [topic] if topic else [""]
        return [
            {"keyword": term, "country": "", "place": "", "language": "", "language_name": ""}
            for term in terms
        ]

    def _collect_discovery(self, job: dict[str, Any], account_id: str) -> dict[str, Any]:
        collected: list[dict[str, Any]] = []
        queries = self._search_queries(job)
        start = int(job.get("next_query_index") or 0)
        for index, query in enumerate(queries):
            if index < start:
                continue
            if str(job.get("id")) in self._cancel:
                job["status"] = DISCOVERY_PAUSED
                job["next_query_index"] = index
                job["error_message"] = "Đã tạm dừng. Bấm Tiếp tục để quét query còn lại."
                self.store.save_discovery_job(job)
                return job
            keyword = query["keyword"]
            place = " ".join(part for part in (query["place"], query["language_name"]) if part)
            label = f"{keyword} {place}".strip()
            self._note(f"Đang quét “{label}” — đã thấy {len(collected)} nhóm")
            result = self.provider.search_groups(
                topic=str(job.get("topic") or ""),
                keyword=keyword,
                country=query["country"],
                language=query["language"],
                account_id=account_id,
                place=place,
            )
            code = str(getattr(result, "error_code", "") or (result.get("error_code") if isinstance(result, dict) else ""))
            rows = list(getattr(result, "groups", None) or (result.get("groups") if isinstance(result, dict) else []) or [])
            if code in PAUSE_CODES:
                job["status"] = DISCOVERY_RATE_LIMITED if code == ERR_RATE else DISCOVERY_PAUSED
                job["error_code"] = code
                job["error_message"] = str(result.get("error_message") or "") if isinstance(result, dict) else ""
                job["next_query_index"] = index
                self.store.save_discovery_job(job)
                return job
            if code:
                job["status"] = DISCOVERY_FAILED
                job["error_code"] = code
                job["error_message"] = str(result.get("error_message") or "") if isinstance(result, dict) else ""
                self.store.save_discovery_job(job)
                return job
            source = " ".join(part for part in (keyword, place) if part)
            for row in rows:
                row["source_query"] = row.get("source_query") or source
                if query["country"] and not row.get("country"):
                    row["country"] = query["country"]
                if query["language"] and not row.get("language"):
                    row["language"] = query["language"]
            collected.extend(rows)
            job["next_query_index"] = index + 1
            self._note(f"Đang quét “{label}” — đã thấy {len(collected)} nhóm")
        job["status"] = DISCOVERY_DEDUPLICATING
        self.store.save_discovery_job(job)
        merged: dict[str, dict[str, Any]] = {}
        for row in collected:
            group_id = str(row.get("group_id") or "").strip()
            if not group_id:
                continue
            current = merged.get(group_id)
            if current is None:
                merged[group_id] = dict(row)
                continue
            words = list(current.get("matched_keywords") or [])
            for word in row.get("matched_keywords") or []:
                if word not in words:
                    words.append(word)
            current["matched_keywords"] = words
            incoming = int(row.get("member_count") or 0)
            if incoming > int(current.get("member_count") or 0):
                current["member_count"] = row.get("member_count")
                current["member_count_status"] = row.get("member_count_status") or current.get("member_count_status")
        minimum = job.get("min_members")
        minimum = None if minimum in (None, "", 0) else int(minimum)
        strict = bool(job.get("strict_member_count", True))
        country_labels = [item for pair in self._country_places(list(job.get("countries") or [])) for item in pair if item]
        language_labels = [item for pair in self._catalog_names("languages", list(job.get("languages") or [])) for item in pair if item]
        job["status"] = DISCOVERY_SAVING
        self.store.save_discovery_job(job)
        saved_ids: list[str] = []
        rejections: list[dict[str, str]] = []
        stats = {"member_passed": 0, "topic_matched": 0, "country_matched": 0, "language_matched": 0}
        for row in merged.values():
            if not row.get("member_count_status"):
                row["member_count_status"] = "EXACT" if row.get("member_count") not in (None, "") else "UNKNOWN"
            passed, reason = qualify_group(
                row,
                topic=str(job.get("topic") or ""),
                keywords=list(job.get("keywords") or []),
                countries=country_labels,
                languages=language_labels,
                min_members=minimum,
                strict_member_count=strict,
            )
            if not passed:
                rejections.append({"group_id": str(row.get("group_id") or ""), "reject_reason": reason})
                if reason == "TOPIC_NOT_MATCHED":
                    stats["member_passed"] += 1
                elif reason == "COUNTRY_NOT_MATCHED":
                    stats["member_passed"] += 1
                    stats["topic_matched"] += 1
                elif reason == "LANGUAGE_NOT_MATCHED":
                    stats["member_passed"] += 1
                    stats["topic_matched"] += 1
                    stats["country_matched"] += 1
                continue
            stats["member_passed"] += 1
            stats["topic_matched"] += 1
            stats["country_matched"] += 1
            stats["language_matched"] += 1
            row["qualified"] = True
            row["topic_match"] = True
            row["topic_match_score"] = topic_match_score(
                row,
                topic=str(job.get("topic") or ""),
                keywords=list(job.get("keywords") or []),
                countries=list(job.get("countries") or []),
                languages=list(job.get("languages") or []),
            )
            saved = self.store.upsert_group(row)
            saved_ids.append(str(saved.get("group_id")))
        job["status"] = DISCOVERY_FILTERING
        visible = self.visible_groups(job.get("filters") or {}, account_id=account_id, page_id="")
        visible_ids = [str(item.get("group_id")) for item in visible if str(item.get("group_id")) in set(saved_ids)]
        job["status"] = DISCOVERY_COMPLETED
        job["result_ids"] = visible_ids
        job["total_collected"] = len(collected)
        job["total_unique"] = len(merged)
        job["total_qualified"] = len(saved_ids)
        job["total_rejected"] = len(rejections)
        job["rejections"] = rejections
        job["stats"] = stats
        kept = f"giữ {len(saved_ids)} nhóm từ {minimum} thành viên" if minimum else f"giữ {len(saved_ids)} nhóm"
        job["note"] = (
            f"Đã thấy {len(collected)} kết quả, {len(merged)} nhóm. {kept}. "
            f"Đủ thành viên {stats['member_passed']} · đúng chủ đề {stats['topic_matched']} · "
            f"đúng nước {stats['country_matched']} · đúng ngôn ngữ {stats['language_matched']}"
        )
        if not merged:
            job["note"] = "Không thấy nhóm trên trang tìm. Kiểm tra trình duyệt đã đăng nhập Facebook."
        job["finished_at"] = _now()
        self.store.save_discovery_job(job)
        return job

    def visible_groups(self, filters: dict[str, Any], *, account_id: str, page_id: str) -> list[dict[str, Any]]:
        rows = []
        for group in self.store.groups():
            member = self.store.membership_for(
                account_id=account_id,
                page_id=page_id,
                group_id=str(group.get("group_id") or ""),
            )
            if passes_discovery_filters(group, member, filters):
                rows.append(group)
        rows.sort(key=lambda item: int(item.get("topic_match_score") or 0), reverse=True)
        return rows

    def enqueue_joins(self, *, account_id: str, page_id: str, group_ids: list[str]) -> list[dict[str, Any]]:
        """Không tạo thêm job nếu nhóm đó đang chờ hoặc đang tham gia."""
        busy = {
            str(job.get("group_id") or "")
            for job in self.store.join_jobs()
            if str(job.get("account_id") or "") == account_id
            and str(job.get("page_id") or "") == str(page_id or "")
            and job.get("status") in {JOIN_PENDING, JOIN_DELAYED, JOIN_VERIFYING} | JOIN_ACTIVE
        }
        created = []
        for group_id in group_ids:
            if group_id in busy:
                continue
            created.append(self.store.add_join_job({"account_id": account_id, "page_id": page_id, "group_id": group_id}))
            busy.add(group_id)
        return created

    def run_join_queue(self, account_id: str, *, continue_on_error: bool = True) -> list[dict[str, Any]]:
        """Một account chỉ một join đang chạy. Xong thì chờ cooldown rồi mới nhóm sau."""
        self._begin(account_id)
        try:
            return self._run_join_queue(account_id, continue_on_error=continue_on_error)
        finally:
            self._end(account_id)

    def _run_join_queue(self, account_id: str, *, continue_on_error: bool = True) -> list[dict[str, Any]]:
        finished: list[dict[str, Any]] = []
        while True:
            pending = [
                job
                for job in self.store.join_jobs()
                if str(job.get("account_id")) == account_id and job.get("status") in {JOIN_PENDING, JOIN_DELAYED, JOIN_VERIFYING}
            ]
            if not pending:
                break
            if account_id in self._cancel:
                break
            active = [
                job
                for job in self.store.join_jobs()
                if str(job.get("account_id")) == account_id and job.get("status") in JOIN_ACTIVE and job.get("status") != JOIN_VERIFYING
            ]
            if len(active) >= 1:
                break
            job = pending[0]
            outcome = self._run_one_join(job)
            finished.append(outcome)
            if outcome.get("status") in {JOIN_PAUSED, JOIN_WAITING_REAUTH}:
                break
            if outcome.get("status") == JOIN_FAILED and not continue_on_error:
                break
            nxt = [
                item
                for item in self.store.join_jobs()
                if str(item.get("account_id")) == account_id and item.get("status") == JOIN_PENDING
            ]
            if nxt and outcome.get("status") in {JOIN_JOINED, JOIN_ALREADY, JOIN_APPROVAL, JOIN_NOT_AVAILABLE, JOIN_FAILED}:
                self._cooldown_marker(account_id)
        return finished

    def _run_one_join(self, job: dict[str, Any]) -> dict[str, Any]:
        account_id = str(job.get("account_id") or "")
        blocked = self._session_block(account_id)
        if blocked:
            job["status"] = JOIN_WAITING_REAUTH if blocked == ERR_SESSION else JOIN_PAUSED
            job["error_code"] = blocked
            job["finished_at"] = _now()
            self.store.save_join_job(job)
            return job
        if job.get("status") == JOIN_VERIFYING:
            verified = self._call(
                "verify_membership",
                account_id=account_id,
                page_id=str(job.get("page_id") or ""),
                group_id=str(job.get("group_id") or ""),
            )
            if self._hold_join(job, verified):
                return job
            if str(verified.get("membership_status") or "") == "JOINED":
                return self._finish_join(job, JOIN_JOINED, "JOINED", str(verified.get("approval_status") or "UNKNOWN"), str(verified.get("posting_permission") or "UNKNOWN"))
            return self._retry_or_fail(job, str(verified.get("error_code") or "TEMPORARY_ERROR"), "Chưa xác minh được vào nhóm")
        job["status"] = JOIN_CHECKING_STATUS
        job["started_at"] = job.get("started_at") or _now()
        self.store.save_join_job(job)
        checked = self._call(
            "check_membership",
            account_id=account_id,
            page_id=str(job.get("page_id") or ""),
            group_id=str(job.get("group_id") or ""),
        )
        if self._hold_join(job, checked):
            return job
        membership = str(checked.get("membership_status") or "UNKNOWN")
        approval = str(checked.get("approval_status") or "UNKNOWN")
        if membership == "JOINED":
            return self._finish_join(job, JOIN_ALREADY, membership, approval, checked.get("posting_permission") or "UNKNOWN")
        if approval == "APPROVAL_REQUIRED":
            return self._finish_join(job, JOIN_APPROVAL, membership or "NOT_JOINED", approval, "UNKNOWN")
        if approval != "NO_APPROVAL_INDICATED":
            return self._finish_join(job, JOIN_NOT_AVAILABLE, membership or "UNKNOWN", approval or "UNKNOWN", "UNKNOWN")
        job["status"] = JOIN_JOINING
        self.store.save_join_job(job)
        joined = self._call(
            "request_join_if_available",
            account_id=account_id,
            page_id=str(job.get("page_id") or ""),
            group_id=str(job.get("group_id") or ""),
        )
        if self._hold_join(job, joined):
            return job
        job["status"] = JOIN_VERIFYING
        self.store.save_join_job(job)
        verified = self._call(
            "verify_membership",
            account_id=account_id,
            page_id=str(job.get("page_id") or ""),
            group_id=str(job.get("group_id") or ""),
        )
        if self._hold_join(job, verified):
            return job
        if str(verified.get("membership_status") or "") != "JOINED":
            return self._retry_or_fail(job, str(verified.get("error_code") or "TEMPORARY_ERROR"), "Chưa xác minh được vào nhóm")
        return self._finish_join(
            job,
            JOIN_JOINED,
            "JOINED",
            str(verified.get("approval_status") or approval),
            str(verified.get("posting_permission") or "UNKNOWN"),
        )

    def create_share_batch(
        self,
        *,
        account_id: str,
        page_id: str,
        source_url: str,
        text: str,
        group_ids: list[str] | None = None,
        image_paths: list[str] | None = None,
        destination: str = "group",
        target_url: str = "",
        cooldown_mode: str = "fixed",
        cooldown_fixed: int = 30,
        cooldown_min: int = 30,
        cooldown_max: int = 90,
    ) -> dict[str, Any]:
        """Chia sẻ link bài, chữ và ảnh đã chọn. Không tải video."""
        assert_link_only({"source_url": source_url, "text": text})
        images = normalize_share_images(image_paths)
        target_id = page_id or account_id
        source_type = classify_source_url(source_url)
        place = "page" if destination == "page" else destination
        targets = self._share_targets(
            account_id=account_id,
            page_id=page_id,
            group_ids=list(group_ids or []),
            destination=place,
        )
        jobs: list[dict[str, Any]] = []
        duplicates: list[str] = []
        skipped: list[str] = list(targets["skipped"])
        done_fps = self.store.completed_share_fingerprints()
        for group_id in targets["group_ids"]:
            fp = share_fingerprint(target_id, group_id, source_url, text, images)
            if fp in done_fps:
                duplicates.append(group_id or page_id)
                continue
            jobs.append(
                {
                    "account_id": account_id,
                    "page_id": page_id,
                    "group_id": group_id,
                    "destination": place,
                    "target_url": target_url,
                    "source_type": source_type,
                    "source_url": source_url,
                    "text": text,
                    "image_paths": images,
                    "status": SHARE_PENDING,
                    "post_id": "",
                    "post_url": "",
                    "retry_count": 0,
                    "max_retry": MAX_RETRY,
                    "error_code": "",
                    "error_message": "",
                    "fingerprint": fp,
                    "started_at": "",
                    "finished_at": "",
                }
            )
        batch = self.store.add_share_batch(
            {
                "account_id": account_id,
                "page_id": page_id,
                "source_type": source_type,
                "source_url": source_url,
                "text": text,
                "cooldown_mode": cooldown_mode,
                "cooldown_fixed": cooldown_fixed,
                "cooldown_min": cooldown_min,
                "cooldown_max": cooldown_max,
                "delay_mode": cooldown_mode,
                "delay_fixed_seconds": cooldown_fixed,
                "delay_min_seconds": cooldown_min,
                "delay_max_seconds": cooldown_max,
            },
            jobs,
        )
        if duplicates:
            for group_id in duplicates:
                self.store.save_share_job(
                    {
                        "id": f"dup-{batch['id']}-{group_id}",
                        "batch_id": batch["id"],
                        "account_id": account_id,
                        "page_id": page_id,
                        "group_id": group_id,
                        "source_type": source_type,
                        "source_url": source_url,
                        "text": text,
                        "status": SHARE_DUPLICATE,
                        "post_id": "",
                        "post_url": "",
                        "retry_count": 0,
                        "error_code": "DUPLICATE",
                        "error_message": "Đã chia sẻ cùng URL và nội dung vào nhóm này",
                        "fingerprint": share_fingerprint(target_id, group_id, source_url, text, images),
                        "created_at": _now(),
                        "started_at": "",
                        "finished_at": _now(),
                    }
                )
        batch["status"] = BATCH_READY if jobs else BATCH_COMPLETED
        self.store.save_share_batch(batch)
        self.store.refresh_batch_counts(batch["id"])
        return {"batch": batch, "created": jobs, "duplicates": duplicates, "skipped": skipped}

    def create_share_wave(
        self,
        *,
        pages: list[dict[str, Any]],
        source_url: str,
        text: str,
        image_paths: list[str] | None = None,
        destination: str = "page",
        group_ids: list[str] | None = None,
        cooldown_fixed: int = 30,
        watch_seconds: int = 0,
        watch_max_seconds: int = 0,
        reel_seconds: int = 0,
        comment: str = "",
        like_video: bool = False,
        engage_mode: str = "",
        source_page: str = "",
    ) -> dict[str, Any]:
        """Cùng một link cho nhiều page. Mỗi page xem một số giây ngẫu nhiên trong khoảng đã đặt.

        ``like_video`` / ``comment``: ý định cấp video (Like-only / comment-only / cả hai / không).
        Trong lượt: chỉ một phần Page nhận Like (~15–25%) hoặc bình luận (~23–31%),
        còn lại chỉ chia sẻ — tránh 13 share → 13 comment.
        """
        assert_link_only({"source_url": source_url, "text": text})
        images = normalize_share_images(image_paths)
        place = destination if destination in {"page", "joined", "both"} else "page"
        source_type = classify_source_url(source_url)
        mode = str(engage_mode or "").strip().casefold()
        if not mode:
            mode = "both" if like_video and str(comment or "").strip() else (
                "like" if like_video else ("comment" if str(comment or "").strip() else "none")
            )
        want_like = bool(like_video) or mode in {"like", "both"}
        want_comment = bool(str(comment or "").strip()) or mode in {"comment", "both"}
        if mode == "none":
            want_like = False
            want_comment = False
        if mode == "like":
            want_comment = False
        if mode == "comment":
            want_like = False
        watch_low = max(0, int(watch_seconds or 0))
        watch_high = max(watch_low, int(watch_max_seconds or 0))
        self._account_labels = {}
        for page in pages:
            aid = str(page.get("account_id") or "").strip()
            label = str(page.get("account_name") or "").strip()
            if aid and label:
                self._account_labels[aid] = label
        by_account: dict[str, list[dict[str, Any]]] = {}
        page_watch: dict[tuple[str, str], int] = {}
        duplicates: list[str] = []
        skipped: list[str] = []
        done_fps = self.store.completed_share_fingerprints()
        for page in pages:
            account_id = str(page.get("account_id") or "").strip()
            page_id = str(page.get("page_id") or "").strip()
            target_url = str(page.get("target_url") or page.get("page_url") or "").strip()
            if not account_id or not page_id:
                skipped.append(page_id or "page")
                continue
            specs = self._wave_specs(
                account_id=account_id,
                page_id=page_id,
                target_url=target_url,
                destination=place,
                group_ids=list(group_ids or []),
            )
            if not specs:
                skipped.append(page_id)
                continue
            watch_key = (account_id, page_id)
            if watch_key not in page_watch:
                page_watch[watch_key] = pick_page_watch_seconds(watch_low, watch_high, self._rng)
            bucket = by_account.setdefault(account_id, [])
            for spec in specs:
                group_id = spec["group_id"]
                fp = share_fingerprint(page_id, group_id, source_url, text, images)
                if fp in done_fps:
                    duplicates.append(group_id or page_id)
                    continue
                bucket.append(
                    {
                        "account_id": account_id,
                        "page_id": page_id,
                        "group_id": group_id,
                        "destination": spec["destination"],
                        "target_url": spec["target_url"],
                        "source_type": source_type,
                        "source_url": source_url,
                        "text": text,
                        "image_paths": images,
                        "status": SHARE_PENDING,
                        "post_id": "",
                        "post_url": "",
                        "retry_count": 0,
                        "max_retry": MAX_RETRY,
                        "error_code": "",
                        "error_message": "",
                        "fingerprint": fp,
                        "page_name": str(page.get("page_name") or ""),
                        "group_name": self._group_name(group_id),
                        "watch_seconds": page_watch[watch_key],
                        "comment": "",
                        "started_at": "",
                        "finished_at": "",
                    }
                )
        batches: list[dict[str, Any]] = []
        created = 0
        # Phân bổ Like/comment thưa trên toàn bộ Page nhận (không gắn mọi Page).
        unique_page_ids: list[str] = []
        seen_pid: set[str] = set()
        for jobs in by_account.values():
            for job in jobs:
                pid = str(job.get("page_id") or "").strip()
                if pid and pid not in seen_pid:
                    seen_pid.add(pid)
                    unique_page_ids.append(pid)
        like_page_ids, comment_page_ids = allocate_wave_page_engage(
            unique_page_ids,
            want_like=want_like,
            want_comment=want_comment and bool(parse_comment_lines(comment)),
            rng=self._rng,
        )
        comment_pool = parse_comment_lines(comment) if comment_page_ids else []
        used_comments: list[str] = []
        page_comments: dict[tuple[str, str], str] = {}
        for jobs in by_account.values():
            for job in jobs:
                key = (str(job.get("account_id") or ""), str(job.get("page_id") or ""))
                pid = str(job.get("page_id") or "").strip()
                if pid not in comment_page_ids:
                    job["comment"] = ""
                    continue
                if key not in page_comments:
                    chosen = pick_share_comment(comment_pool, used_comments, self._rng)
                    if chosen:
                        used_comments.append(chosen)
                    page_comments[key] = chosen
                job["comment"] = page_comments[key]
        for account_id, jobs in by_account.items():
            if not jobs:
                continue
            chosen_comment = next((str(job.get("comment") or "") for job in jobs if job.get("comment")), "")
            batch_like_ids = sorted(
                {
                    str(job.get("page_id") or "").strip()
                    for job in jobs
                    if str(job.get("page_id") or "").strip() in like_page_ids
                }
            )
            batch = self.store.add_share_batch(
                {
                    "account_id": account_id,
                    "page_id": "",
                    "source_type": source_type,
                    "source_url": source_url,
                    "text": text,
                    "cooldown_mode": "fixed",
                    "cooldown_fixed": int(cooldown_fixed or 0),
                    "cooldown_min": int(cooldown_fixed or 0),
                    "cooldown_max": int(cooldown_fixed or 0),
                    "watch_seconds": watch_low,
                    "watch_min_seconds": watch_low,
                    "watch_max_seconds": watch_high,
                    "reel_seconds": max(0, int(reel_seconds or 0)),
                    "comment": chosen_comment,
                    "like_video": bool(batch_like_ids),
                    "like_page_ids": batch_like_ids,
                    "engage_mode": mode,
                    "source_page": str(source_page or "").strip(),
                },
                jobs,
            )
            batch["status"] = BATCH_READY
            self.store.save_share_batch(batch)
            self.store.refresh_batch_counts(batch["id"])
            batches.append(batch)
            created += len(jobs)
        return {
            "batches": batches,
            "batch_ids": [str(item.get("id") or "") for item in batches],
            "created": created,
            "accounts": len(batches),
            "duplicates": duplicates,
            "skipped": skipped,
        }

    def resume_share_wave(self, batch_ids: list[str]) -> dict[str, Any]:
        """Bỏ tạm dừng rồi chạy tiếp các batch còn bài chưa gửi."""
        for batch_id in batch_ids:
            self._cancel.discard(batch_id)
            try:
                batch = self._batch(batch_id)
            except KeyError:
                continue
            if batch.get("status") == BATCH_PAUSED:
                batch["status"] = BATCH_READY
                self.store.save_share_batch(batch)
        return self.run_share_wave(batch_ids)

    @staticmethod
    def _share_parallel_account_limit() -> int:
        """
        Số tài khoản chia sẻ song song tối đa (mỗi TK một browser).

        Ưu tiên ``SHARE_PARALLEL_ACCOUNTS``, không thì ``BROWSER_CONCURRENCY`` (mặc định 3).
        Tránh mở hàng chục Firefox cùng lúc khi wave lớn.
        """
        for key in ("SHARE_PARALLEL_ACCOUNTS", "BROWSER_CONCURRENCY"):
            raw = str(os.environ.get(key) or "").strip()
            if not raw:
                continue
            try:
                return max(1, int(raw))
            except ValueError:
                logger.warning("{}={!r} không hợp lệ — dùng mặc định 3.", key, raw)
        return 3

    def _pending_share_batch_ids(self, batch_ids: list[str]) -> list[str]:
        """Batch READY/PAUSED còn job PENDING/VERIFY_FIRST — cần chạy tiếp."""
        out: list[str] = []
        for batch_id in batch_ids:
            bid = str(batch_id or "").strip()
            if not bid or bid in self._cancel:
                continue
            try:
                batch = self._batch(bid)
            except KeyError:
                continue
            if batch.get("status") not in {BATCH_READY, BATCH_PAUSED}:
                continue
            pending = [
                job
                for job in self._batch_jobs(bid)
                if job.get("status") in {SHARE_PENDING, SHARE_VERIFY_FIRST}
            ]
            if pending:
                out.append(bid)
        return out

    def run_share_wave(self, batch_ids: list[str], *, _auto_resume_pass: int = 0) -> dict[str, Any]:
        """
        Nhiều tài khoản song song (``SHARE_PARALLEL_ACCOUNTS`` / ``BROWSER_CONCURRENCY``).

        Cùng một tài khoản: **một** browser, tuần tự từng batch rồi từng Page —
        không bao giờ 2 Page / 2 Firefox của cùng TK cùng lúc.
        """
        # Gom batch theo account — tránh 2 thread cùng một TK (resume nhiều batch dở).
        by_account: dict[str, list[str]] = {}
        account_pages: dict[str, int] = {}
        all_batch_ids: list[str] = []
        for batch_id in batch_ids:
            bid = str(batch_id or "").strip()
            if not bid or bid in self._cancel:
                continue
            try:
                batch = self._batch(bid)
            except KeyError:
                continue
            account_id = str(batch.get("account_id") or "").strip() or "_unknown_"
            jobs = self._batch_jobs(bid)
            pages = len({str(item.get("page_id") or "") for item in jobs})
            by_account.setdefault(account_id, []).append(bid)
            account_pages[account_id] = account_pages.get(account_id, 0) + pages
            all_batch_ids.append(bid)
        if not by_account:
            return {"batches": []}
        with self._step_lock:
            self._live_steps.clear()

        account_specs = list(by_account.items())  # (account_id, [batch_ids...])

        def _run_account_batches(acc: str, tokens: list[str]) -> list[dict[str, Any]]:
            """Chạy tuần tự mọi batch của một TK (đã giữ khóa account)."""
            out: list[dict[str, Any]] = []
            self._tls.provider = self._spawn_provider()
            self._tls.session_ok = {}
            try:
                for token in tokens:
                    if token in self._cancel:
                        continue
                    try:
                        out.append(self.run_share_batch(token))
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Lượt chia sẻ account %s batch %s lỗi. TK khác vẫn chạy.",
                            acc,
                            token,
                        )
                        try:
                            batch = self._batch(token)
                            if batch.get("status") in {
                                BATCH_RUNNING,
                                BATCH_VALIDATING,
                                BATCH_DELAYING,
                            }:
                                pending = [
                                    job
                                    for job in self._batch_jobs(token)
                                    if job.get("status") in {SHARE_PENDING, SHARE_VERIFY_FIRST}
                                ]
                                if pending:
                                    batch["status"] = BATCH_READY
                                    batch["error_message"] = (
                                        "Lỗi giữa chừng — giữ batch để chạy tiếp"
                                    )
                                    self.store.save_share_batch(batch)
                            out.append(batch)
                        except Exception:  # noqa: BLE001
                            pass
            finally:
                self._tls.provider = None
            return out

        if len(account_specs) == 1:
            account_id, tokens = account_specs[0]
            label = self._account_labels.get(account_id) or account_id
            pages = account_pages.get(account_id, 0)
            self._note(f"{label}: {pages} page · 1 browser (lần lượt từng Page)")
            finished_one = _run_account_batches(account_id, tokens)
            return self._finish_share_wave(
                all_batch_ids, finished_one, auto_resume_pass=_auto_resume_pass
            )

        limit = self._share_parallel_account_limit()
        labels = [
            f"{self._account_labels.get(aid) or aid} ({account_pages.get(aid, 0)} page)"
            for aid, _toks in account_specs
        ]
        self._note(
            f"{len(account_specs)} tài khoản · tối đa {limit} browser cùng lúc "
            f"(mỗi TK 1 Page): " + " · ".join(labels)
        )
        logger.info(
            "Chia sẻ wave: {} TK (đã gom batch), song song tối đa {} browser — mỗi TK tuần tự Page.",
            len(account_specs),
            limit,
        )
        finished_by_acc: list[list[dict[str, Any]] | None] = [None] * len(account_specs)
        gate = threading.Semaphore(limit)
        threads: list[threading.Thread] = []
        for index, (account_id, tokens) in enumerate(account_specs):
            label = self._account_labels.get(account_id) or account_id
            logger.info(
                "Hàng đợi luồng chia sẻ | account={} | batches={} | pages={}",
                label,
                len(tokens),
                account_pages.get(account_id, 0),
            )

            def _work(
                slot: int = index,
                acc: str = account_id,
                batch_list: list[str] = tokens,
            ) -> None:
                acquired = gate.acquire(timeout=3600)
                if not acquired:
                    logger.error("Hết giờ chờ slot browser cho account {} — bỏ lượt.", acc)
                    finished_by_acc[slot] = []
                    return
                try:
                    finished_by_acc[slot] = _run_account_batches(acc, batch_list)
                finally:
                    gate.release()

            thread = threading.Thread(target=_work, name=f"share-{account_id}", daemon=True)
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        done: list[dict[str, Any]] = []
        for rows in finished_by_acc:
            if rows:
                done.extend(rows)
        return self._finish_share_wave(all_batch_ids, done, auto_resume_pass=_auto_resume_pass)

    def _finish_share_wave(
        self,
        batch_ids: list[str],
        finished: list[dict[str, Any]],
        *,
        auto_resume_pass: int,
    ) -> dict[str, Any]:
        """Gộp kết quả; nếu còn batch dở (browser chết…) thì tự chạy lại 1 vòng."""
        leftover = self._pending_share_batch_ids(batch_ids)
        if leftover and auto_resume_pass < 1:
            logger.info(
                "Tự chạy tiếp {} batch còn dở sau lỗi giữa chừng (lần {}).",
                len(leftover),
                auto_resume_pass + 1,
            )
            self._note(f"Tự chạy tiếp {len(leftover)} batch còn dở…")
            more = self.run_share_wave(leftover, _auto_resume_pass=auto_resume_pass + 1)
            by_id = {
                str(row.get("id") or ""): row
                for row in finished
                if isinstance(row, dict) and str(row.get("id") or "")
            }
            for row in more.get("batches") or []:
                if isinstance(row, dict) and str(row.get("id") or ""):
                    by_id[str(row.get("id"))] = row
            return {"batches": list(by_id.values())}
        return {"batches": finished}

    def _spawn_provider(self) -> Any:
        """Profile riêng khi nhiều tài khoản chia sẻ cùng lúc. Test dùng chung provider giả."""
        maker = getattr(self.provider, "spawn", None)
        if callable(maker):
            return maker()
        return self.provider

    def _active_provider(self) -> Any:
        bound = getattr(self._tls, "provider", None)
        return bound if bound is not None else self.provider

    def _wave_specs(
        self,
        *,
        account_id: str,
        page_id: str,
        target_url: str,
        destination: str,
        group_ids: list[str] | None = None,
    ) -> list[dict[str, str]]:
        """Một page có thể nhận bài lên tường, vào nhóm đã tham gia, hoặc vào UID nhóm đã nhập."""
        specs: list[dict[str, str]] = []
        typed = [str(item).strip() for item in (group_ids or []) if str(item).strip()]
        if destination in {"page", "both"}:
            specs.append({"group_id": "", "destination": "page", "target_url": target_url})
        for group_id in typed:
            specs.append({"group_id": group_id, "destination": "typed", "target_url": ""})
        if typed or destination not in {"joined", "both"}:
            return specs
        if destination in {"joined", "both"}:
            for group_id in self._joined_group_ids(account_id, page_id):
                member = self.store.membership_for(account_id=account_id, page_id=page_id, group_id=group_id)
                if member is not None and str(member.get("posting_permission") or "") == "DENIED":
                    continue
                specs.append({"group_id": group_id, "destination": "joined", "target_url": ""})
        return specs

    def _share_targets(
        self,
        *,
        account_id: str,
        page_id: str,
        group_ids: list[str],
        destination: str,
    ) -> dict[str, list[str]]:
        """Page của mình là một đích. Nhóm chỉ khi đã tham gia."""
        if destination == "page":
            if not page_id:
                return {"group_ids": [], "skipped": ["page"]}
            return {"group_ids": [""], "skipped": []}
        chosen: list[str] = []
        skipped: list[str] = []
        rows = group_ids or self._joined_group_ids(account_id, page_id)
        for group_id in rows:
            member = self.store.membership_for(account_id=account_id, page_id=page_id, group_id=group_id)
            if member is None or member.get("membership_status") != "JOINED":
                skipped.append(group_id)
                continue
            posting = str(member.get("posting_permission") or "UNKNOWN")
            if destination == "group" and posting != "ALLOWED":
                skipped.append(group_id)
                continue
            if posting == "DENIED":
                skipped.append(group_id)
                continue
            chosen.append(group_id)
        return {"group_ids": chosen, "skipped": skipped}

    def _group_name(self, group_id: str) -> str:
        """Tên nhóm đã quét, để ô tìm trong hộp chia sẻ chọn đúng nhóm."""
        token = str(group_id or "").strip()
        if not token:
            return ""
        for row in self.store.groups():
            if str(row.get("group_id") or "") == token:
                return str(row.get("group_name") or "").strip()
        return ""

    def _joined_group_ids(self, account_id: str, page_id: str) -> list[str]:
        found: list[str] = []
        for row in self.store.load().get("memberships") or []:
            if str(row.get("account_id") or "") != account_id:
                continue
            if str(row.get("page_id") or "") != str(page_id or ""):
                continue
            if row.get("membership_status") != "JOINED":
                continue
            group_id = str(row.get("group_id") or "")
            if group_id and group_id not in found:
                found.append(group_id)
        return found

    def run_share_batch(self, batch_id: str) -> dict[str, Any]:
        batch = self._batch(batch_id)
        if batch.get("status") in {BATCH_PAUSED, BATCH_CANCELLED, BATCH_COMPLETED, BATCH_FAILED}:
            return batch
        account_id = str(batch.get("account_id") or "")
        acc_lock = self._account_share_lock(account_id)
        # Khóa cứng theo TK — dù wave/resume tạo nhiều batch, vẫn chỉ 1 browser/TK.
        waited = False
        while True:
            if acc_lock.acquire(blocking=False):
                break
            if batch_id in self._cancel:
                return self._batch(batch_id)
            batch = self._batch(batch_id)
            if batch.get("status") in {BATCH_PAUSED, BATCH_CANCELLED, BATCH_COMPLETED, BATCH_FAILED}:
                return batch
            if not waited:
                self._share_step(
                    batch,
                    page_id="",
                    page_name="—",
                    step="Chờ trong TK — đang có Page khác chia sẻ…",
                )
                waited = True
            self._sleep(1.0)
        batch_lease = None
        try:
            with self._live_lock:
                self._live_batches.add(str(batch_id))
            # Giữ FeatureBusy cả batch — watcher/quét nguồn không mở Firefox thứ 2 giữa các Page.
            try:
                from src.utils.feature_busy import FEATURE_SHARE, try_acquire, release as release_busy

                batch_lease = try_acquire(
                    feature=FEATURE_SHARE,
                    account_id=account_id,
                    detail=f"share-batch:{batch_id}",
                )
            except Exception:  # noqa: BLE001
                batch_lease = None
            self._share_step(
                batch,
                page_id="",
                page_name="—",
                step="Đã mở luồng — chuẩn bị chia sẻ…",
            )
            self._begin(account_id)
            try:
                return self._run_share_batch(batch_id, account_id)
            finally:
                with self._live_lock:
                    self._live_batches.discard(str(batch_id))
                self._end(account_id)
                self._clear_share_step(account_id)
                if batch_lease is not None:
                    try:
                        release_busy(batch_lease)
                    except Exception:  # noqa: BLE001
                        pass
                    batch_lease = None
        finally:
            if batch_lease is not None:
                try:
                    from src.utils.feature_busy import release as release_busy

                    release_busy(batch_lease)
                except Exception:  # noqa: BLE001
                    pass
            try:
                acc_lock.release()
            except RuntimeError:
                pass

    def _sync_conflict_gap_settings(self) -> None:
        """Đồng bộ giãn đăng→chia sẻ từ store (auto / tay) vào FeatureBusy."""
        from src.utils.feature_busy import set_conflict_gap_auto

        try:
            data = self.store.share_settings()
            parse_conflict_gap_minutes(
                data.get("conflict_gap_min"),
                data.get("conflict_gap_max"),
            )
            auto = data.get("conflict_gap_auto")
            set_conflict_gap_auto(True if auto is None else bool(auto))
        except Exception:  # noqa: BLE001
            lo, hi = get_conflict_gap_minutes()
            parse_conflict_gap_minutes(lo, hi)

    def _page_alias_index(self) -> dict[str, dict[str, Any]]:
        """Map local id / Meta id / URL → bản ghi Page (một lần mỗi engine)."""
        cached = getattr(self, "_page_alias_cache", None)
        if isinstance(cached, dict) and cached:
            return cached
        index: dict[str, dict[str, Any]] = {}
        try:
            from src.utils.pages_manager import get_default_pages_manager

            for item in get_default_pages_manager().load_all():
                row = dict(item)
                for key in (
                    str(row.get("id") or "").strip(),
                    str(row.get("fb_page_id") or "").strip(),
                    str(row.get("page_url") or "").strip().rstrip("/").casefold(),
                ):
                    if key:
                        index[key] = row
        except Exception:  # noqa: BLE001
            index = {}
        self._page_alias_cache = index
        return index

    def _share_page_keys(self, *, account_id: str, page_id: str, target_url: str = "") -> frozenset[str]:
        """Khóa Page để không trùng lịch đăng (local id + Meta id)."""
        pid = str(page_id or "").strip()
        want_url = str(target_url or "").strip().rstrip("/").casefold()
        index = self._page_alias_index()
        row = index.get(pid) or (index.get(want_url) if want_url else None)
        return normalize_page_keys(pid, target_url, page_row=row)

    def _run_share_batch(self, batch_id: str, account_id: str) -> dict[str, Any]:
        batch = self._batch(batch_id)
        batch["status"] = BATCH_VALIDATING
        batch["started_at"] = batch.get("started_at") or _now()
        self.store.save_share_batch(batch)
        batch["status"] = BATCH_RUNNING
        self.store.save_share_batch(batch)
        self._tls.session_ok = {}
        liked_pages: set[str] = set()
        failed_pages: set[str] = set()
        self._sync_conflict_gap_settings()
        while True:
            batch = self._batch(batch_id)
            if batch.get("status") in {BATCH_PAUSED, BATCH_CANCELLED} or batch_id in self._cancel:
                break
            job = self._next_share_job(batch_id)
            if job is None:
                break
            page_id = str(job.get("page_id") or "")
            if page_id in failed_pages:
                self._fail_share_job(job, "TEMPORARY_ERROR", "Chưa chuyển được sang Page này")
                continue
            page_keys = self._share_page_keys(
                account_id=str(job.get("account_id") or account_id),
                page_id=page_id,
                target_url=str(job.get("target_url") or ""),
            )
            name = str(job.get("page_name") or page_id)
            job_account = str(job.get("account_id") or account_id)

            def _stop_share() -> bool:
                # Ưu tiên cờ hủy in-memory — tránh đọc store mỗi 2s khi chờ lịch.
                return batch_id in self._cancel

            def _wait_msg(text: str) -> None:
                # Chỉ _share_step — giữ bảng nhiều tài khoản (không ghi đè 1 dòng chờ).
                self._share_step(batch, page_id=page_id, page_name=name, step=text)

            # Auto/tay: FeatureBusy chọn giãn theo tải lịch của TK (không truyền gap cố định).
            page_lease = acquire_share_after_schedule(
                account_id=job_account,
                page_keys=page_keys,
                detail=f"share:{batch_id}:{page_id}",
                poll=1.0,
                should_stop=_stop_share,
                on_wait=_wait_msg,
            )
            if page_lease is None:
                logger.info(
                    "Batch %s Page %s dừng chờ (đã hủy/tạm dừng) — giữ job để chạy lại sau.",
                    batch_id,
                    page_id,
                )
                break
            try:
                switched = self._switch_share_page(batch_id, job)
                if switched == "stop":
                    return self._batch(batch_id)
                if switched == "browser_dead":
                    # Giữ job PENDING — không fail cả list khi Firefox đã tắt.
                    logger.error(
                        "Batch %s dừng: không mở lại được trình duyệt account %s.",
                        batch_id,
                        job_account,
                    )
                    batch = self._batch(batch_id)
                    batch["status"] = BATCH_READY
                    batch["error_message"] = "Trình duyệt đóng — giữ batch để chạy tiếp sau"
                    self.store.save_share_batch(batch)
                    break
                if switched == "skip":
                    failed_pages.add(page_id)
                    continue
                if self._browse_reels(batch_id, page_id):
                    return self._batch(batch_id)
                allowed_like = {
                    str(x).strip() for x in (batch.get("like_page_ids") or []) if str(x).strip()
                }
                # Bản cũ không có like_page_ids: giữ like_video trên batch (mọi Page).
                if allowed_like:
                    share_like = page_id in allowed_like and page_id not in liked_pages
                else:
                    share_like = bool(batch.get("like_video")) and page_id not in liked_pages
                stop, liked = self._engage_page_before_share(batch_id, page_id, like=share_like)
                if stop:
                    return self._batch(batch_id)
                if liked:
                    liked_pages.add(page_id)
                    share_like = False
                self._run_one_share(batch, job, like_video=share_like)
                if share_like:
                    liked_pages.add(page_id)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Page %s lỗi. Chuyển sang Page tiếp theo.", page_id)
                self._share_step(
                    batch,
                    page_id=page_id,
                    page_name=str(job.get("page_name") or page_id),
                    step="Lỗi, đang sang Page tiếp theo",
                )
                self._fail_share_job(job, "TEMPORARY_ERROR", f"Page lỗi, chuyển sang Page sau: {exc}")
                if page_id:
                    failed_pages.add(page_id)
                continue
            finally:
                page_lease.release()
            batch = self.store.refresh_batch_counts(batch_id) or batch
            if job_hold := self._batch(batch_id).get("status") in {BATCH_PAUSED}:
                break
            remaining = self._next_share_job(batch_id)
            if remaining is None:
                break
            page_switch = str(remaining.get("page_id") or "") != str(job.get("page_id") or "")
            self._delay_batch(
                batch,
                page_switch=page_switch,
                page_id=str(job.get("page_id") or ""),
                page_name=str(job.get("page_name") or ""),
            )
        batch = self.store.refresh_batch_counts(batch_id) or self._batch(batch_id)
        self._clear_share_step(str(batch.get("account_id") or account_id))
        if batch.get("status") not in {BATCH_PAUSED, BATCH_CANCELLED, BATCH_FAILED}:
            pending = [item for item in self._batch_jobs(batch_id) if item.get("status") in {SHARE_PENDING, SHARE_VERIFY_FIRST}]
            if not pending:
                failed = int(batch.get("failed_jobs") or 0)
                batch["status"] = BATCH_FAILED if failed and not int(batch.get("completed_jobs") or 0) else BATCH_COMPLETED
                batch["finished_at"] = _now()
                self.store.save_share_batch(batch)
        return batch

    def _switch_share_page(self, batch_id: str, job: dict[str, Any]) -> str:
        """Vào đúng Page trước khi lướt, xem, like, bình luận và chia sẻ.

        Trả về ``stop`` khi phải dừng cả lượt, ``skip`` khi Page này chưa chuyển được.
        """
        page_id = str(job.get("page_id") or "")
        batch = self._batch(batch_id)
        if page_id and str(batch.get("active_page_id") or "") == page_id:
            return ""
        name = str(job.get("page_name") or page_id or "Page")
        self._share_step(batch, page_id=page_id, page_name=name, step="Đang chuyển Page")
        provider = self._active_provider()
        if hasattr(provider, "switch_to_page"):
            result = self._call(
                "switch_to_page",
                account_id=str(job.get("account_id") or batch.get("account_id") or ""),
                page_id=page_id,
                page_name=str(job.get("page_name") or ""),
                page_url=str(job.get("target_url") or ""),
            )
            if self._pause_prepare(batch_id, result):
                return "stop"
            if result.get("error_code"):
                err = str(result.get("error_message") or "")
                if "Không mở được trình duyệt" in err or str(result.get("error_code")) == "TIMEOUT":
                    return "browser_dead"
                self._fail_share_job(
                    job,
                    str(result.get("error_code") or "TEMPORARY_ERROR"),
                    err or f"Chưa chuyển được sang Page {name}",
                )
                return "skip"
        batch = self._batch(batch_id)
        batch["active_page_id"] = page_id
        self.store.save_share_batch(batch)
        return ""

    def _fail_share_job(self, job: dict[str, Any], code: str, message: str) -> None:
        """Đánh lỗi một bài để sang Page sau, không chia sẻ nhầm vai trò."""
        job["status"] = SHARE_FAILED
        job["error_code"] = code
        job["error_message"] = message
        job["finished_at"] = _now()
        self.store.save_share_job(job)

    def _browse_reels(self, batch_id: str, page_id: str = "") -> bool:
        """Lướt Reel sau khi đã vào Page. Mỗi Page một lần. True khi phải dừng."""
        batch = self._batch(batch_id)
        seen = [str(item) for item in (batch.get("reels_browsed_page_ids") or [])]
        if page_id and page_id in seen:
            return False
        seconds = int(batch.get("reel_seconds") or 0)
        if seconds <= 0 or not hasattr(self._active_provider(), "browse_reels"):
            return False
        self._share_step(
            batch,
            page_id=page_id,
            page_name=self._page_label(batch_id, page_id),
            step=f"Đang lướt Reel {seconds} giây",
        )
        result = self._call(
            "browse_reels",
            account_id=str(batch.get("account_id") or ""),
            reel_seconds=seconds,
            should_stop=lambda: batch_id in self._cancel,
        )
        if self._pause_prepare(batch_id, result):
            return True
        batch = self._batch(batch_id)
        seen = [str(item) for item in (batch.get("reels_browsed_page_ids") or [])]
        if page_id and page_id not in seen:
            seen.append(page_id)
        batch["reels_browsed_page_ids"] = seen
        batch["reels_browsed"] = True
        self.store.save_share_batch(batch)
        self._share_step(
            batch,
            page_id=page_id,
            page_name=self._page_label(batch_id, page_id),
            step="Đã lướt Reel, đang mở video",
        )
        return False

    def _page_share_comment(self, batch_id: str, page_id: str) -> str:
        """Câu bình luận đã bốc cho Page này. Trống thì dùng câu của cả batch."""
        token = str(page_id or "").strip()
        for job in self._batch_jobs(batch_id):
            if str(job.get("page_id") or "") == token and str(job.get("comment") or "").strip():
                return str(job.get("comment") or "").strip()
        return str(self._batch(batch_id).get("comment") or "").strip()

    def _engage_page_before_share(self, batch_id: str, page_id: str, *, like: bool) -> tuple[bool, bool]:
        """Một lần mở video: xem, trong lúc xem thì Like và bình luận lần lượt (không cùng lúc).

        Returns:
            ``(dừng, đã_like)`` — dừng khi Facebook chặn hoặc người dùng bấm Dừng.
        """
        batch = self._batch(batch_id)
        watched = [str(item) for item in (batch.get("watched_page_ids") or [])]
        commented = [str(item) for item in (batch.get("commented_page_ids") or [])]
        already = page_id in watched
        comment = self._page_share_comment(batch_id, page_id)
        posted = {
            self._page_share_comment(batch_id, token)
            for token in commented
            if self._page_share_comment(batch_id, token)
        }
        need_comment = bool(comment) and page_id not in commented and comment not in posted
        seconds = 0 if already else self._page_watch_seconds(batch_id, page_id)
        if seconds <= 0 and not need_comment and not like:
            return False, False
        provider = self._active_provider()
        page_name = self._page_label(batch_id, page_id)
        if not hasattr(provider, "prepare_source_video"):
            if not like or not hasattr(provider, "like_source_video"):
                return False, False
            self._share_step(batch, page_id=page_id, page_name=page_name, step="Đang bấm Like")
            result = self._call(
                "like_source_video",
                account_id=str(batch.get("account_id") or ""),
                source_url=str(batch.get("source_url") or ""),
            )
            return self._pause_prepare(batch_id, result), bool(result.get("liked"))
        doing = [part for part, on in (("Like", like), ("bình luận", need_comment)) if on]
        step = f"Đang xem video {seconds} giây" if seconds > 0 else "Đang mở video"
        if doing:
            step += ", vừa xem vừa " + " + ".join(doing)
        self._share_step(batch, page_id=page_id, page_name=page_name, step=step)
        result = self._call(
            "prepare_source_video",
            account_id=str(batch.get("account_id") or ""),
            source_url=str(batch.get("source_url") or ""),
            watch_seconds=seconds,
            comment=comment if need_comment else "",
            like=like,
            should_stop=lambda: batch_id in self._cancel,
        )
        if self._pause_prepare(batch_id, result):
            return True, False
        liked = bool(like and result.get("liked"))
        batch = self._batch(batch_id)
        if not already:
            batch["watched_page_ids"] = [*watched, page_id]
            batch["source_watched"] = True
        note = str(result.get("error_message") or "")
        if need_comment and not result.get("commented"):
            batch["comment_warning"] = note or "Chưa thấy ô bình luận — vẫn chia sẻ video"
            self._share_step(batch, page_id=page_id, page_name=page_name, step="Chưa bình luận được, vẫn chia sẻ")
        elif need_comment:
            batch["commented_page_ids"] = [*commented, page_id]
            batch["source_commented"] = True
            self._share_step(batch, page_id=page_id, page_name=page_name, step="Đã xem + bình luận, chuẩn bị chia sẻ")
        else:
            batch["source_commented"] = True
            self._share_step(batch, page_id=page_id, page_name=page_name, step="Đã xem, chuẩn bị chia sẻ")
        batch["source_prepared"] = True
        batch["watch_note"] = ""
        self.store.save_share_batch(batch)
        return False, liked

    def _page_watch_seconds(self, batch_id: str, page_id: str) -> int:
        """Số giây đã bốc cho page. Mọi đích của page đó dùng cùng một lần xem."""
        for job in self._batch_jobs(batch_id):
            if str(job.get("page_id") or "") == page_id:
                return int(job.get("watch_seconds") or 0)
        return int(self._batch(batch_id).get("watch_seconds") or 0)

    def _pause_prepare(self, batch_id: str, result: dict[str, Any]) -> bool:
        """Dừng khi Facebook chặn hoặc người dùng bấm Dừng."""
        code = str(result.get("error_code") or "")
        if code not in PAUSE_CODES and code != "CANCELLED":
            return False
        batch = self._batch(batch_id)
        batch["status"] = BATCH_PAUSED
        self.store.save_share_batch(batch)
        self._cancel.add(batch_id)
        return True

    def pause(self, batch_id: str) -> None:
        batch = self._batch(batch_id)
        batch["status"] = BATCH_PAUSED
        self.store.save_share_batch(batch)
        self._cancel.add(batch_id)

    def pause_share_accounts(self, account_ids: Iterable[str]) -> list[str]:
        """
        Dừng mọi batch chia sẻ của các tài khoản (READY/RUNNING/DELAYING/VALIDATING).

        Trả về danh sách account_id đã có batch bị pause — dùng để đóng Firefox sót.
        """
        want = {str(a).strip() for a in account_ids if str(a or "").strip()}
        if not want:
            return []
        paused_accounts: set[str] = set()
        active = {BATCH_READY, BATCH_RUNNING, BATCH_DELAYING, BATCH_VALIDATING}
        for batch in list(self.store.share_batches()):
            aid = str(batch.get("account_id") or "").strip()
            if aid not in want:
                continue
            if batch.get("status") not in active:
                continue
            bid = str(batch.get("id") or "")
            if not bid:
                continue
            try:
                self.pause(bid)
                paused_accounts.add(aid)
            except Exception:  # noqa: BLE001
                logger.exception("Không pause được batch {} (account={})", bid, aid)
        if paused_accounts:
            logger.info(
                "Đã dừng chia sẻ các TK ngoài phạm vi: {}",
                ", ".join(sorted(paused_accounts)),
            )
        return sorted(paused_accounts)

    def pause_share_outside_accounts(self, allowed_account_ids: set[str] | None) -> list[str]:
        """
        Chỉ giữ các TK trong ``allowed``; pause mọi TK khác đang chia sẻ.

        ``allowed is None`` → không giới hạn (Tất cả tài khoản).
        """
        if allowed_account_ids is None:
            return []
        allowed = {str(a).strip() for a in allowed_account_ids if str(a or "").strip()}
        if not allowed:
            # Không có TK hợp lệ → dừng hết đang chạy.
            all_aids = {
                str(b.get("account_id") or "").strip()
                for b in self.store.share_batches()
                if str(b.get("account_id") or "").strip()
            }
            return self.pause_share_accounts(all_aids)
        stop: set[str] = set()
        for batch in self.store.share_batches():
            aid = str(batch.get("account_id") or "").strip()
            if aid and aid not in allowed:
                stop.add(aid)
        return self.pause_share_accounts(stop)

    def resume(self, batch_id: str) -> dict[str, Any]:
        self._cancel.discard(batch_id)
        batch = self._batch(batch_id)
        if batch.get("status") == BATCH_PAUSED:
            batch["status"] = BATCH_RUNNING
            self.store.save_share_batch(batch)
        return self.run_share_batch(batch_id)

    def dashboard(self) -> dict[str, Any]:
        groups = self.store.groups()
        memberships = self.store.load().get("memberships") or []
        joined = sum(1 for item in memberships if item.get("membership_status") == "JOINED")
        joinable = 0
        approval = 0
        unknown = 0
        for group in groups:
            approval_status = str(group.get("approval_status") or "UNKNOWN")
            if approval_status == "APPROVAL_REQUIRED":
                approval += 1
            elif approval_status == "NO_APPROVAL_INDICATED":
                joinable += 1
            else:
                unknown += 1
        joins = self.store.join_jobs()
        shares = self.store.share_batches()
        return {
            "searches": len(self.store.discovery_jobs()),
            "groups_found": len(groups),
            "unique_groups": len({str(item.get("group_id")) for item in groups}),
            "joined": joined,
            "joinable": joinable,
            "approval_required": approval,
            "unknown": unknown,
            "join_total": len(joins),
            "join_completed": sum(1 for item in joins if item.get("status") in {JOIN_JOINED, JOIN_ALREADY}),
            "join_running": sum(1 for item in joins if item.get("status") in JOIN_ACTIVE),
            "join_pending": sum(1 for item in joins if item.get("status") == JOIN_PENDING),
            "join_failed": sum(1 for item in joins if item.get("status") == JOIN_FAILED),
            "share_batches": len(shares),
        }

    def _run_one_share(self, batch: dict[str, Any], job: dict[str, Any], *, like_video: bool = False) -> dict[str, Any]:
        account_id = str(job.get("account_id") or "")
        page_id = str(job.get("page_id") or "")
        group_id = str(job.get("group_id") or "")
        if job.get("status") != SHARE_VERIFY_FIRST:
            blocked = self._session_block(account_id)
            if blocked:
                return self._pause_share(batch, job, blocked)
            job["status"] = SHARE_CHECKING_PERMISSION
            self.store.save_share_job(job)
            if str(job.get("destination") or "") not in {"page", "typed"}:
                member = self.store.membership_for(account_id=account_id, page_id=page_id, group_id=group_id)
                if member is None or member.get("membership_status") != "JOINED":
                    return self._skip_share(job, "GROUP_NOT_JOINED")
                if member.get("posting_permission") == "DENIED":
                    return self._skip_share(job, "POSTING_NOT_ALLOWED")
                if str(job.get("destination") or "group") == "group" and member.get("posting_permission") != "ALLOWED":
                    return self._skip_share(job, "POSTING_NOT_ALLOWED")
            payload = {
                "source_url": job.get("source_url") or "",
                "text": job.get("text") or "",
                "source_type": job.get("source_type") or "",
            }
            assert_link_only(payload)
            job["status"] = SHARE_SUBMIT
            job["started_at"] = job.get("started_at") or _now()
            self.store.save_share_job(job)
            where = "Page" if str(job.get("destination") or "") == "page" else (group_id or "nhóm")
            self._share_step(
                batch,
                page_id=page_id,
                page_name=str(job.get("page_name") or page_id),
                step=f"Đang chia sẻ, bấm Share now → {where}",
            )
            published = self._call(
                "publish_link",
                account_id=account_id,
                page_id=page_id,
                group_id=group_id,
                source_url=payload["source_url"],
                text=payload["text"],
                image_paths=list(job.get("image_paths") or []),
                target_url=str(job.get("target_url") or ""),
                page_name=str(job.get("page_name") or ""),
                group_name=str(job.get("group_name") or ""),
                like_video=like_video,
            )
            if self._hold_share(batch, job, published):
                return job
            if published.get("post_id"):
                return self._complete_share(job, published)
            code = str(published.get("error_code") or "")
            if code in TEMPORARY_ERROR_CODES or code == "TIMEOUT":
                message = str(published.get("error_message") or "")
                if message:
                    return self._retry_share(batch, job, code or "TIMEOUT", message)
                job["status"] = SHARE_VERIFY_FIRST
                job["error_code"] = code or "TIMEOUT"
                self.store.save_share_job(job)
            elif code in SKIP_CODES:
                return self._skip_share(job, code)
            elif code:
                return self._retry_share(batch, job, code, str(published.get("error_message") or ""))
        job["status"] = SHARE_VERIFYING
        self.store.save_share_job(job)
        verified = self._call(
            "verify_published_post",
            account_id=account_id,
            page_id=page_id,
            group_id=group_id,
            source_url=str(job.get("source_url") or ""),
            text=str(job.get("text") or ""),
        )
        if verified.get("post_id"):
            return self._complete_share(job, verified)
        return self._retry_share(batch, job, str(verified.get("error_code") or "TEMPORARY_ERROR"), "Chưa thấy bài sau khi gửi")

    def _retry_share(self, batch: dict[str, Any], job: dict[str, Any], code: str, message: str) -> dict[str, Any]:
        if code in PAUSE_CODES:
            return self._pause_share(batch, job, code)
        if code not in TEMPORARY_ERROR_CODES and code not in {"TIMEOUT", ""}:
            job["status"] = SHARE_FAILED
            job["error_code"] = code
            job["error_message"] = message
            job["finished_at"] = _now()
            self.store.save_share_job(job)
            return job
        retry = int(job.get("retry_count") or 0)
        if retry >= int(job.get("max_retry") or MAX_RETRY):
            job["status"] = SHARE_FAILED
            job["error_code"] = code
            job["error_message"] = message or "Hết số lần thử"
            job["finished_at"] = _now()
            self.store.save_share_job(job)
            return job
        job["retry_count"] = retry + 1
        job["status"] = SHARE_PENDING
        job["error_code"] = code
        job["error_message"] = message
        self.store.save_share_job(job)
        return job

    def _complete_share(self, job: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
        job["status"] = SHARE_COMPLETED
        job["post_id"] = str(result.get("post_id") or "")
        job["post_url"] = str(result.get("post_url") or "")
        job["finished_at"] = _now()
        job["error_code"] = ""
        self.store.save_share_job(job)
        return job

    def _skip_share(self, job: dict[str, Any], code: str) -> dict[str, Any]:
        job["status"] = SHARE_SKIPPED
        job["error_code"] = code
        job["finished_at"] = _now()
        self.store.save_share_job(job)
        return job

    def _pause_share(self, batch: dict[str, Any], job: dict[str, Any], code: str) -> dict[str, Any]:
        job["status"] = SHARE_WAITING_REAUTH if code == ERR_SESSION else SHARE_PAUSED
        job["error_code"] = code
        self.store.save_share_job(job)
        batch["status"] = BATCH_PAUSED
        self.store.save_share_batch(batch)
        return job

    def _hold_share(self, batch: dict[str, Any], job: dict[str, Any], result: dict[str, Any]) -> bool:
        code = str(result.get("error_code") or "")
        if code in PAUSE_CODES:
            self._pause_share(batch, job, code)
            return True
        return False

    def _hold_join(self, job: dict[str, Any], result: dict[str, Any]) -> bool:
        code = str(result.get("error_code") or "")
        if code in PAUSE_CODES:
            job["status"] = JOIN_WAITING_REAUTH if code == ERR_SESSION else JOIN_PAUSED
            job["error_code"] = code
            job["finished_at"] = _now()
            self.store.save_join_job(job)
            return True
        return False

    def _finish_join(
        self,
        job: dict[str, Any],
        status: str,
        membership: str,
        approval: str,
        posting: str,
    ) -> dict[str, Any]:
        job["status"] = status
        job["membership_status"] = membership
        job["approval_status"] = approval
        job["finished_at"] = _now()
        self.store.save_join_job(job)
        self.store.upsert_membership(
            {
                "account_id": job.get("account_id") or "",
                "page_id": job.get("page_id") or "",
                "group_id": job.get("group_id") or "",
                "membership_status": membership if status != JOIN_ALREADY else "JOINED",
                "approval_status": approval,
                "posting_permission": posting,
            }
        )
        return job

    def _retry_or_fail(self, job: dict[str, Any], code: str, message: str) -> dict[str, Any]:
        if code in PAUSE_CODES:
            job["status"] = JOIN_PAUSED
            job["error_code"] = code
            self.store.save_join_job(job)
            return job
        if code not in TEMPORARY_ERROR_CODES:
            job["status"] = JOIN_FAILED
            job["error_code"] = code
            job["error_message"] = message
            job["finished_at"] = _now()
            self.store.save_join_job(job)
            return job
        retry = int(job.get("retry_count") or 0)
        if retry >= int(job.get("max_retry") or MAX_RETRY):
            job["status"] = JOIN_FAILED
            job["error_code"] = code
            job["error_message"] = message
            job["finished_at"] = _now()
            self.store.save_join_job(job)
            return job
        job["retry_count"] = retry + 1
        job["status"] = JOIN_PENDING
        job["error_code"] = code
        self.store.save_join_job(job)
        return job

    def _account_label(self, batch: dict[str, Any]) -> str:
        """Tên tài khoản để hiện trên trạng thái. Không có tên thì dùng mã."""
        account_id = str(batch.get("account_id") or "")
        return str(self._account_labels.get(account_id) or account_id or "Tài khoản")

    def _page_label(self, batch_id: str, page_id: str) -> str:
        """Tên Page đang chạy. Không có tên thì dùng mã Page."""
        for job in self._batch_jobs(batch_id):
            if str(job.get("page_id") or "") == str(page_id or ""):
                name = str(job.get("page_name") or "").strip()
                if name:
                    return name
        return str(page_id or "Page")

    def _share_step(self, batch: dict[str, Any], *, page_id: str = "", page_name: str = "", step: str) -> None:
        """Ghi bước của một tài khoản và báo cả các tài khoản đang chạy song song."""
        account_id = str(batch.get("account_id") or "")
        label = self._account_label(batch)
        page = str(page_name or "").strip() or "Page"
        line = f"{label} · {page} — {step}"
        with self._step_lock:
            self._live_steps[account_id or label] = {
                "account_id": account_id,
                "page_id": str(page_id or ""),
                "page_name": page,
                "step": step,
                "line": line,
            }
            board = "\n".join(item["line"] for item in self._live_steps.values())
        self._note(board)

    def _clear_share_step(self, account_id: str) -> None:
        """Bỏ tài khoản khỏi bảng đang chạy khi lượt của tài khoản đó kết thúc."""
        with self._step_lock:
            self._live_steps.pop(str(account_id or ""), None)
            board = "\n".join(item["line"] for item in self._live_steps.values())
        if board:
            self._note(board)

    def live_board(self) -> str:
        """Các tài khoản đang chạy, mỗi tài khoản một dòng: tên, Page và bước."""
        with self._step_lock:
            return "\n".join(item["line"] for item in self._live_steps.values())

    def live_steps_snapshot(self) -> dict[str, dict[str, str]]:
        """Bản sao bước đang chạy theo ``account_id`` — UI đọc không giữ khóa lâu."""
        with self._step_lock:
            return {key: dict(val) for key, val in self._live_steps.items()}

    def live_step_for(self, account_id: str, page_id: str = "") -> str:
        """
        Bước hiện tại của tài khoản trên bảng Page.

        - Đúng Page đang xử lý → câu bước đầy đủ.
        - Page khác cùng TK còn chờ → ``Chờ trong TK — đang: {Page}``.
        - Chưa gắn Page (mới mở luồng) → hiện bước chung cho mọi dòng TK đó.
        """
        with self._step_lock:
            item = self._live_steps.get(str(account_id or ""))
        if not item:
            return ""
        current = str(item.get("page_id") or "").strip()
        step = str(item.get("step") or "").strip()
        page_name = str(item.get("page_name") or "").strip()
        want = str(page_id or "").strip()
        if not want or not current or current == want:
            return step
        where = page_name if page_name and page_name != "—" else current
        return f"Chờ trong TK — đang: {where}"

    def _note(self, text: str) -> None:
        callback = getattr(self, "on_progress", None)
        if callable(callback):
            callback(text)

    def _begin(self, account_id: str) -> None:
        provider = self._active_provider()
        if account_id and hasattr(provider, "begin_account"):
            provider.begin_account(account_id)

    def _end(self, account_id: str) -> None:
        provider = self._active_provider()
        if account_id and hasattr(provider, "end_account"):
            try:
                provider.end_account(account_id)
            except Exception:  # noqa: BLE001
                return

    def _session_block(self, account_id: str) -> str:
        provider = self._active_provider()
        if not account_id or not hasattr(provider, "check_session"):
            return ""
        cache = getattr(self._tls, "session_ok", None)
        if not isinstance(cache, dict):
            cache = {}
            self._tls.session_ok = cache
        if account_id in cache:
            return cache[account_id]
        try:
            code = str(provider.check_session(account_id) or "")
        except Exception:
            return ""
        cache[account_id] = code
        return code

    def _call(self, name: str, **kwargs: Any) -> dict[str, Any]:
        fn = getattr(self._active_provider(), name)
        result = fn(**kwargs)
        if isinstance(result, dict):
            return result
        return {
            "error_code": str(getattr(result, "error_code", "") or ""),
            "membership_status": str(getattr(result, "membership_status", "") or ""),
            "approval_status": str(getattr(result, "approval_status", "") or ""),
            "posting_permission": str(getattr(result, "posting_permission", "") or ""),
            "post_id": str(getattr(result, "post_id", "") or ""),
            "post_url": str(getattr(result, "post_url", "") or ""),
            "error_message": str(getattr(result, "error_message", "") or ""),
        }

    def _cooldown_marker(self, account_id: str) -> None:
        seconds = calculate_delay_seconds(
            {"delay_enabled": True, "delay_mode": "fixed", "delay_fixed_seconds": 30},
            self._rng,
        )
        if seconds > 0:
            self._sleep(seconds)

    def _delay_batch(
        self,
        batch: dict[str, Any],
        *,
        page_switch: bool = False,
        page_id: str = "",
        page_name: str = "",
    ) -> None:
        """Nghỉ giữa các bài. Sang Page khác của cùng tài khoản thì nghỉ gấp đôi."""
        batch = dict(batch)
        batch["delay_enabled"] = True
        batch["delay_mode"] = batch.get("cooldown_mode") or "fixed"
        batch["delay_fixed_seconds"] = batch.get("cooldown_fixed") or 0
        batch["delay_min_seconds"] = batch.get("cooldown_min") or 0
        batch["delay_max_seconds"] = batch.get("cooldown_max") or 0
        with self._rng_lock:
            seconds = calculate_delay_seconds(batch, self._rng)
        if page_switch and seconds > 0:
            seconds *= 2
        if seconds <= 0:
            return
        live_pid = str(page_id or batch.get("live_page_id") or "").strip()
        live_name = str(page_name or "").strip() or self._page_label(
            str(batch.get("id") or ""), live_pid
        )
        if page_switch:
            step = f"Nghỉ {seconds} giây trước Page tiếp theo"
        else:
            step = f"Nghỉ {seconds} giây trước bài tiếp theo"
        self._share_step(batch, page_id=live_pid, page_name=live_name, step=step)
        batch_id = str(batch.get("id"))
        stored = self._batch(batch_id)
        stored["status"] = BATCH_DELAYING
        stored["cooldown_remaining"] = seconds
        if live_pid:
            stored["live_page_id"] = live_pid
        self.store.save_share_batch(stored)
        if seconds > 0 and batch_id not in self._cancel:
            self._sleep(seconds)
        stored = self._batch(batch_id)
        if stored.get("status") == BATCH_DELAYING:
            stored["status"] = BATCH_RUNNING
            stored["cooldown_remaining"] = 0
            self.store.save_share_batch(stored)

    def _publish_busy(self, account_id: str, page_id: str, *, except_batch: str) -> bool:
        """
        True nếu cùng tài khoản/Page đang chia sẻ **trong process này**.

        Chỉ nhìn ``_live_batches`` — batch RUNNING/DELAYING mồ côi sau tắt app
        không còn khóa tài khoản (đã đưa về READY ở ``recover``).
        """
        aid = str(account_id or "").strip()
        pid = str(page_id or "").strip()
        live = {str(x) for x in self._live_batches if str(x) != str(except_batch)}
        if not live:
            return False
        for bid in live:
            try:
                batch = self._batch(bid)
            except KeyError:
                continue
            if aid and str(batch.get("account_id") or "") == aid:
                return True
        for job in self.store.share_jobs():
            job_batch = str(job.get("batch_id") or "")
            if job_batch == except_batch or job_batch not in live:
                continue
            if job.get("status") not in SHARE_ACTIVE:
                continue
            if aid and str(job.get("account_id") or "") == aid:
                return True
            if pid and str(job.get("page_id") or "") == pid:
                return True
        return False

    def _next_share_job(self, batch_id: str) -> dict[str, Any] | None:
        for job in self._batch_jobs(batch_id):
            if job.get("status") in {SHARE_PENDING, SHARE_VERIFY_FIRST}:
                return job
        return None

    def _batch_jobs(self, batch_id: str) -> list[dict[str, Any]]:
        return [job for job in self.store.share_jobs() if str(job.get("batch_id")) == str(batch_id)]

    def _discovery(self, job_id: str) -> dict[str, Any]:
        for job in self.store.discovery_jobs():
            if str(job.get("id")) == str(job_id):
                return job
        raise KeyError(job_id)

    def _batch(self, batch_id: str) -> dict[str, Any]:
        for batch in self.store.share_batches():
            if str(batch.get("id")) == str(batch_id):
                return batch
        raise KeyError(batch_id)
