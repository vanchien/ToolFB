"""Hàng đợi tìm nhóm, tham gia và chia sẻ link. Một account một job đang chạy."""

from __future__ import annotations

import random
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable

from src.services.page_creation.engine import calculate_delay_seconds

from .fingerprint import assert_link_only, classify_source_url, normalize_share_images, share_fingerprint
from .video_engage import parse_comment_lines, pick_page_watch_seconds, pick_share_comment
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
        self._cancel: set[str] = set()

    def recover(self) -> dict[str, int]:
        """Sau khi mở lại app: job đang dở được xác minh lại, job tạm dừng giữ nguyên."""
        counts = {"discovery": 0, "join": 0, "share": 0}
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
        return counts

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
        for group_id in targets["group_ids"]:
            if self.store.completed_share_fingerprint(target_id, group_id, source_url, text, images):
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
                    "fingerprint": share_fingerprint(target_id, group_id, source_url, text, images),
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
    ) -> dict[str, Any]:
        """Cùng một link cho nhiều page. Mỗi page xem một số giây ngẫu nhiên trong khoảng đã đặt."""
        assert_link_only({"source_url": source_url, "text": text})
        images = normalize_share_images(image_paths)
        place = destination if destination in {"page", "joined", "both"} else "page"
        source_type = classify_source_url(source_url)
        watch_low = max(0, int(watch_seconds or 0))
        watch_high = max(watch_low, int(watch_max_seconds or 0))
        by_account: dict[str, list[dict[str, Any]]] = {}
        page_watch: dict[tuple[str, str], int] = {}
        duplicates: list[str] = []
        skipped: list[str] = []
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
                if self.store.completed_share_fingerprint(page_id, group_id, source_url, text, images):
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
                        "fingerprint": share_fingerprint(page_id, group_id, source_url, text, images),
                        "watch_seconds": page_watch[watch_key],
                        "started_at": "",
                        "finished_at": "",
                    }
                )
        batches: list[dict[str, Any]] = []
        created = 0
        comment_pool = parse_comment_lines(comment)
        used_comments: list[str] = []
        for account_id, jobs in by_account.items():
            if not jobs:
                continue
            chosen_comment = pick_share_comment(comment_pool, used_comments, self._rng)
            if chosen_comment:
                used_comments.append(chosen_comment)
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

    def run_share_wave(self, batch_ids: list[str]) -> dict[str, Any]:
        """Chạy lần lượt từng tài khoản. Trình duyệt đóng xong mới sang tài khoản sau."""
        finished: list[dict[str, Any]] = []
        for batch_id in batch_ids:
            if batch_id in self._cancel:
                break
            try:
                batch = self._batch(batch_id)
            except KeyError:
                continue
            account_id = str(batch.get("account_id") or "")
            jobs = self._batch_jobs(batch_id)
            pages = len({str(item.get("page_id") or "") for item in jobs})
            self._note(f"Tài khoản {account_id}: {pages} page, {len(jobs)} bài")
            finished.append(self.run_share_batch(batch_id))
            if str(self._batch(batch_id).get("status") or "") == BATCH_PAUSED:
                break
        return {"batches": finished}

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
        if batch.get("status") in {BATCH_PAUSED, BATCH_CANCELLED, BATCH_COMPLETED}:
            return batch
        account_id = str(batch.get("account_id") or "")
        if self._publish_busy(account_id, str(batch.get("page_id") or ""), except_batch=batch_id):
            return batch
        self._begin(account_id)
        try:
            return self._run_share_batch(batch_id, account_id)
        finally:
            self._end(account_id)

    def _run_share_batch(self, batch_id: str, account_id: str) -> dict[str, Any]:
        batch = self._batch(batch_id)
        batch["status"] = BATCH_VALIDATING
        batch["started_at"] = batch.get("started_at") or _now()
        self.store.save_share_batch(batch)
        batch["status"] = BATCH_RUNNING
        self.store.save_share_batch(batch)
        if self._browse_reels(batch_id):
            return self._batch(batch_id)
        while True:
            batch = self._batch(batch_id)
            if batch.get("status") in {BATCH_PAUSED, BATCH_CANCELLED} or batch_id in self._cancel:
                break
            job = self._next_share_job(batch_id)
            if job is None:
                break
            if self._watch_page_before_share(batch_id, str(job.get("page_id") or "")):
                return self._batch(batch_id)
            self._run_one_share(batch, job)
            batch = self.store.refresh_batch_counts(batch_id) or batch
            if job_hold := self._batch(batch_id).get("status") in {BATCH_PAUSED}:
                break
            remaining = self._next_share_job(batch_id)
            if remaining is None:
                break
            self._delay_batch(batch)
        batch = self.store.refresh_batch_counts(batch_id) or self._batch(batch_id)
        if batch.get("status") not in {BATCH_PAUSED, BATCH_CANCELLED, BATCH_FAILED}:
            pending = [item for item in self._batch_jobs(batch_id) if item.get("status") in {SHARE_PENDING, SHARE_VERIFY_FIRST}]
            if not pending:
                failed = int(batch.get("failed_jobs") or 0)
                batch["status"] = BATCH_FAILED if failed and not int(batch.get("completed_jobs") or 0) else BATCH_COMPLETED
                batch["finished_at"] = _now()
                self.store.save_share_batch(batch)
        return batch

    def _browse_reels(self, batch_id: str) -> bool:
        """Lướt Reel trước. True khi phải dừng cả lượt chia sẻ."""
        batch = self._batch(batch_id)
        if batch.get("reels_browsed"):
            return False
        seconds = int(batch.get("reel_seconds") or 0)
        if seconds <= 0 or not hasattr(self.provider, "browse_reels"):
            return False
        self._note(f"Đang lướt Reel {seconds} giây trước khi xem video")
        result = self._call(
            "browse_reels",
            account_id=str(batch.get("account_id") or ""),
            reel_seconds=seconds,
            should_stop=lambda: batch_id in self._cancel,
        )
        if self._pause_prepare(batch_id, result):
            return True
        batch = self._batch(batch_id)
        batch["reels_browsed"] = True
        self.store.save_share_batch(batch)
        self._note("Đã lướt Reel. Đang mở video để xem.")
        return False

    def _watch_page_before_share(self, batch_id: str, page_id: str) -> bool:
        """Xem video theo số giây của page này, rồi mới chia sẻ. True khi phải dừng."""
        batch = self._batch(batch_id)
        watched = [str(item) for item in (batch.get("watched_page_ids") or [])]
        already = page_id in watched
        comment = str(batch.get("comment") or "").strip()
        need_comment = bool(comment) and not batch.get("source_commented")
        if already and not need_comment:
            return False
        seconds = 0 if already else self._page_watch_seconds(batch_id, page_id)
        if seconds <= 0 and not need_comment:
            return False
        if not hasattr(self.provider, "prepare_source_video"):
            return False
        if already:
            self._note("Đang bình luận dưới video trước khi chia sẻ")
        else:
            self._note(
                f"Page {page_id}: đang xem video {seconds} giây"
                + (" rồi bình luận" if need_comment else "")
            )
        result = self._call(
            "prepare_source_video",
            account_id=str(batch.get("account_id") or ""),
            source_url=str(batch.get("source_url") or ""),
            watch_seconds=seconds,
            comment=comment if need_comment else "",
            should_stop=lambda: batch_id in self._cancel,
        )
        if self._pause_prepare(batch_id, result):
            return True
        batch = self._batch(batch_id)
        if not already:
            batch["watched_page_ids"] = [*watched, page_id]
            batch["source_watched"] = True
        note = str(result.get("error_message") or "")
        if need_comment and not result.get("commented"):
            batch["source_commented"] = False
            batch["source_prepared"] = False
            batch["watch_note"] = note or "Chưa bình luận được dưới video — chưa chia sẻ"
            batch["status"] = BATCH_PAUSED
            self.store.save_share_batch(batch)
            self._cancel.add(batch_id)
            self._note(str(batch["watch_note"]))
            return True
        if need_comment:
            batch["source_commented"] = True
            self._note("Đã bình luận dưới video. Đang chia sẻ link.")
        else:
            batch["source_commented"] = True
            self._note(f"Đã xem {seconds} giây. Đang chia sẻ link.")
        batch["source_prepared"] = True
        batch["watch_note"] = ""
        self.store.save_share_batch(batch)
        return False

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

    def _run_one_share(self, batch: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]:
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
            self._note(f"Đang gửi {page_id} → {where}")
            published = self._call(
                "publish_link",
                account_id=account_id,
                page_id=page_id,
                group_id=group_id,
                source_url=payload["source_url"],
                text=payload["text"],
                image_paths=list(job.get("image_paths") or []),
                target_url=str(job.get("target_url") or ""),
            )
            if self._hold_share(batch, job, published):
                return job
            if published.get("post_id"):
                return self._complete_share(job, published)
            code = str(published.get("error_code") or "")
            if code in TEMPORARY_ERROR_CODES or code == "TIMEOUT":
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

    def _note(self, text: str) -> None:
        callback = getattr(self, "on_progress", None)
        if callable(callback):
            callback(text)

    def _begin(self, account_id: str) -> None:
        if account_id and hasattr(self.provider, "begin_account"):
            self.provider.begin_account(account_id)

    def _end(self, account_id: str) -> None:
        if account_id and hasattr(self.provider, "end_account"):
            try:
                self.provider.end_account(account_id)
            except Exception:  # noqa: BLE001
                return

    def _session_block(self, account_id: str) -> str:
        if not account_id or not hasattr(self.provider, "check_session"):
            return ""
        try:
            code = self.provider.check_session(account_id)
        except Exception:
            return ""
        return str(code or "")

    def _call(self, name: str, **kwargs: Any) -> dict[str, Any]:
        fn = getattr(self.provider, name)
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

    def _delay_batch(self, batch: dict[str, Any]) -> None:
        batch = dict(batch)
        batch["delay_enabled"] = True
        batch["delay_mode"] = batch.get("cooldown_mode") or "fixed"
        batch["delay_fixed_seconds"] = batch.get("cooldown_fixed") or 0
        batch["delay_min_seconds"] = batch.get("cooldown_min") or 0
        batch["delay_max_seconds"] = batch.get("cooldown_max") or 0
        seconds = calculate_delay_seconds(batch, self._rng)
        batch_id = str(batch.get("id"))
        stored = self._batch(batch_id)
        stored["status"] = BATCH_DELAYING
        stored["cooldown_remaining"] = seconds
        self.store.save_share_batch(stored)
        if seconds > 0 and batch_id not in self._cancel:
            self._sleep(seconds)
        stored = self._batch(batch_id)
        if stored.get("status") == BATCH_DELAYING:
            stored["status"] = BATCH_RUNNING
            stored["cooldown_remaining"] = 0
            self.store.save_share_batch(stored)

    def _publish_busy(self, account_id: str, page_id: str, *, except_batch: str) -> bool:
        for job in self.store.share_jobs():
            if str(job.get("batch_id")) == except_batch:
                continue
            if job.get("status") not in SHARE_ACTIVE:
                continue
            if account_id and str(job.get("account_id")) == account_id:
                return True
            if page_id and str(job.get("page_id")) == page_id:
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
