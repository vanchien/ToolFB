"""Lưu Group, membership, discovery, join và share trong một file JSON hiện có của app."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.json_store_lock import json_file_lock
from src.utils.paths import project_root

from .catalog import seed_catalog
from .fingerprint import share_fingerprint
from .states import SHARE_COMPLETED, SHARE_DUPLICATE


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _kept_int(value: Any, default: int) -> int:
    """Giữ số 0. Chỉ dùng mặc định khi trường không được gửi."""
    if value is None or value == "":
        return default
    return int(value)


def default_store_path() -> Path:
    return project_root() / "config" / "facebook_groups.json"


def _empty() -> dict[str, Any]:
    return {
        "catalog": seed_catalog(),
        "profiles": [],
        "groups": [],
        "memberships": [],
        "discovery_jobs": [],
        "join_jobs": [],
        "share_batches": [],
        "share_jobs": [],
        "share_settings": {},
    }


class GroupStore:
    """Đọc/ghi ``config/facebook_groups.json``. Cùng kiểu khóa file với Page và lịch đăng."""

    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path) if path else default_store_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._cache_lock = threading.Lock()
        self._doc_cache: dict[str, Any] | None = None
        self._doc_mtime: float | None = None
        if not self.path.is_file():
            self._write(_empty())

    def _read(self) -> dict[str, Any]:
        raw: Any = {}
        if self.path.is_file():
            last_exc: Exception | None = None
            for attempt in range(4):
                try:
                    raw = json.loads(self.path.read_text(encoding="utf-8"))
                    last_exc = None
                    break
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    time.sleep(0.05 * (attempt + 1))
            if last_exc is not None:
                from loguru import logger

                logger.warning(
                    "Không đọc được facebook_groups.json — dùng bản trống tạm: {}",
                    last_exc,
                )
                raw = {}
        if not isinstance(raw, dict):
            raw = {}
        base = _empty()
        for key, value in base.items():
            if key not in raw or not isinstance(raw.get(key), type(value)):
                raw[key] = value
        return raw

    def _snapshot(self, data: dict[str, Any]) -> dict[str, Any]:
        """Bản sao nông: list/dict cấp 1 tách riêng, tránh caller làm bẩn cache."""
        out: dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, list):
                out[key] = list(value)
            elif isinstance(value, dict):
                out[key] = dict(value)
            else:
                out[key] = value
        return out

    def _file_mtime(self) -> float | None:
        try:
            return self.path.stat().st_mtime
        except OSError:
            return None

    def _remember(self, data: dict[str, Any], mtime: float | None = None) -> None:
        with self._cache_lock:
            self._doc_cache = data
            self._doc_mtime = self._file_mtime() if mtime is None else mtime

    def _write(self, data: dict[str, Any]) -> None:
        text = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
        fd, tmp = tempfile.mkstemp(prefix="facebook_groups_", suffix=".tmp.json", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        self._remember(data)

    def load(self) -> dict[str, Any]:
        """Đọc store — dùng cache theo mtime khi file không đổi (tối ưu wave lớn)."""
        with json_file_lock(self.path):
            mtime = self._file_mtime()
            with self._cache_lock:
                cached = self._doc_cache
                cached_mtime = self._doc_mtime
            if cached is not None and mtime is not None and cached_mtime == mtime:
                return self._snapshot(cached)
            doc = self._read()
            self._remember(doc, mtime)
            return self._snapshot(doc)

    def _mutate(self, fn) -> Any:
        with json_file_lock(self.path):
            doc = self._read()
            result = fn(doc)
            self._write(doc)
            return result

    def catalog(self) -> dict[str, Any]:
        return self.load().get("catalog") or seed_catalog()

    def save_catalog(self, catalog: dict[str, Any]) -> None:
        def _apply(doc: dict[str, Any]) -> None:
            doc["catalog"] = catalog

        self._mutate(_apply)

    def upsert_group(self, row: dict[str, Any]) -> dict[str, Any]:
        """Một ``group_id`` chỉ một bản ghi. Lần sau gộp từ khóa, không thêm dòng mới."""

        def _apply(doc: dict[str, Any]) -> dict[str, Any]:
            group_id = str(row.get("group_id") or "").strip()
            if not group_id:
                raise ValueError("Thiếu group_id")
            now = _now()
            found = None
            for item in doc["groups"]:
                if str(item.get("group_id")) == group_id:
                    found = item
                    break
            if found is None:
                found = {
                    "id": uuid.uuid4().hex,
                    "group_id": group_id,
                    "group_name": "",
                    "group_url": "",
                    "country": "",
                    "language": "",
                    "privacy_status": "",
                    "approval_status": "UNKNOWN",
                    "topic_tags": [],
                    "matched_keywords": [],
                    "member_count": None,
                    "last_discovered": now,
                    "last_checked": now,
                    "created_at": now,
                    "updated_at": now,
                    "topic_match_score": 0,
                }
                doc["groups"].append(found)
            for key in (
                "group_name",
                "group_url",
                "country",
                "language",
                "privacy_status",
                "approval_status",
                "member_count",
                "member_count_status",
                "description",
                "category",
                "country_status",
                "language_status",
                "source_query",
                "topic",
                "topic_match",
                "qualified",
                "topic_match_score",
            ):
                if row.get(key) not in (None, ""):
                    found[key] = row.get(key)
            for key in ("topic_tags", "matched_keywords"):
                merged: list[str] = []
                for item in list(found.get(key) or []) + list(row.get(key) or []):
                    text = str(item).strip()
                    if text and text not in merged:
                        merged.append(text)
                found[key] = merged
            found["last_discovered"] = now
            found["last_checked"] = now
            found["updated_at"] = now
            return dict(found)

        return self._mutate(_apply)

    def groups(self) -> list[dict[str, Any]]:
        return list(self.load().get("groups") or [])

    def group_by_facebook_id(self, group_id: str) -> dict[str, Any] | None:
        for row in self.groups():
            if str(row.get("group_id")) == str(group_id):
                return row
        return None

    def upsert_membership(self, row: dict[str, Any]) -> dict[str, Any]:
        """Membership của Account và của Page là hai bản ghi riêng."""

        def _apply(doc: dict[str, Any]) -> dict[str, Any]:
            account_id = str(row.get("account_id") or "").strip()
            page_id = str(row.get("page_id") or "").strip()
            group_id = str(row.get("group_id") or "").strip()
            now = _now()
            found = None
            for item in doc["memberships"]:
                if (
                    str(item.get("account_id") or "") == account_id
                    and str(item.get("page_id") or "") == page_id
                    and str(item.get("group_id") or "") == group_id
                ):
                    found = item
                    break
            if found is None:
                found = {
                    "id": uuid.uuid4().hex,
                    "account_id": account_id,
                    "page_id": page_id,
                    "group_id": group_id,
                    "membership_status": "UNKNOWN",
                    "approval_status": "UNKNOWN",
                    "posting_permission": "UNKNOWN",
                    "last_checked": now,
                    "created_at": now,
                    "updated_at": now,
                }
                doc["memberships"].append(found)
            for key in ("membership_status", "approval_status", "posting_permission"):
                if row.get(key) not in (None, ""):
                    found[key] = row.get(key)
            found["last_checked"] = now
            found["updated_at"] = now
            return dict(found)

        return self._mutate(_apply)

    def membership_for(self, *, account_id: str, page_id: str, group_id: str) -> dict[str, Any] | None:
        page_key = str(page_id or "").strip()
        for row in self.load().get("memberships") or []:
            if (
                str(row.get("account_id") or "") == str(account_id or "")
                and str(row.get("page_id") or "") == page_key
                and str(row.get("group_id") or "") == str(group_id or "")
            ):
                return dict(row)
        return None

    def save_profile(self, row: dict[str, Any]) -> dict[str, Any]:
        def _apply(doc: dict[str, Any]) -> dict[str, Any]:
            now = _now()
            profile_id = str(row.get("id") or "").strip() or uuid.uuid4().hex
            found = None
            for item in doc["profiles"]:
                if str(item.get("id")) == profile_id:
                    found = item
                    break
            if found is None:
                found = {"id": profile_id, "created_at": now}
                doc["profiles"].append(found)
            found.update(
                {
                    "name": str(row.get("name") or found.get("name") or "").strip(),
                    "topics": list(row.get("topics") or found.get("topics") or []),
                    "keywords": list(row.get("keywords") or found.get("keywords") or []),
                    "countries": list(row.get("countries") or found.get("countries") or []),
                    "languages": list(row.get("languages") or found.get("languages") or []),
                    "schedule": str(row.get("schedule") or found.get("schedule") or "manual"),
                    "filters": dict(row.get("filters") or found.get("filters") or {}),
                    "updated_at": now,
                }
            )
            return dict(found)

        return self._mutate(_apply)

    def profiles(self) -> list[dict[str, Any]]:
        return list(self.load().get("profiles") or [])

    def add_discovery_job(self, row: dict[str, Any]) -> dict[str, Any]:
        def _apply(doc: dict[str, Any]) -> dict[str, Any]:
            now = _now()
            job = {
                "id": uuid.uuid4().hex,
                "topic": str(row.get("topic") or ""),
                "keywords": list(row.get("keywords") or []),
                "countries": list(row.get("countries") or []),
                "languages": list(row.get("languages") or []),
                "filters": dict(row.get("filters") or {}),
                "profile_id": str(row.get("profile_id") or ""),
                "account_id": str(row.get("account_id") or ""),
                "min_members": None if row.get("min_members") in (None, "", 0) else int(row.get("min_members")),
                "strict_member_count": bool(row.get("strict_member_count", True)),
                "next_query_index": 0,
                "total_collected": 0,
                "total_unique": 0,
                "total_qualified": 0,
                "total_rejected": 0,
                "rejections": [],
                "stats": {},
                "status": "PENDING",
                "result_ids": [],
                "error_code": "",
                "error_message": "",
                "created_at": now,
                "started_at": "",
                "finished_at": "",
            }
            doc["discovery_jobs"].append(job)
            return dict(job)

        return self._mutate(_apply)

    def discovery_jobs(self) -> list[dict[str, Any]]:
        return list(self.load().get("discovery_jobs") or [])

    def save_discovery_job(self, job: dict[str, Any]) -> None:
        def _apply(doc: dict[str, Any]) -> None:
            for index, item in enumerate(doc["discovery_jobs"]):
                if str(item.get("id")) == str(job.get("id")):
                    doc["discovery_jobs"][index] = dict(job)
                    return

        self._mutate(_apply)

    def add_join_job(self, row: dict[str, Any]) -> dict[str, Any]:
        def _apply(doc: dict[str, Any]) -> dict[str, Any]:
            now = _now()
            job = {
                "id": uuid.uuid4().hex,
                "account_id": str(row.get("account_id") or ""),
                "page_id": str(row.get("page_id") or ""),
                "group_id": str(row.get("group_id") or ""),
                "status": "PENDING",
                "membership_status": "",
                "approval_status": "",
                "retry_count": 0,
                "max_retry": 3,
                "error_code": "",
                "error_message": "",
                "created_at": now,
                "started_at": "",
                "finished_at": "",
            }
            doc["join_jobs"].append(job)
            return dict(job)

        return self._mutate(_apply)

    def join_jobs(self) -> list[dict[str, Any]]:
        return list(self.load().get("join_jobs") or [])

    def save_join_job(self, job: dict[str, Any]) -> None:
        def _apply(doc: dict[str, Any]) -> None:
            for index, item in enumerate(doc["join_jobs"]):
                if str(item.get("id")) == str(job.get("id")):
                    doc["join_jobs"][index] = dict(job)
                    return

        self._mutate(_apply)

    def add_share_batch(self, row: dict[str, Any], jobs: list[dict[str, Any]]) -> dict[str, Any]:
        def _apply(doc: dict[str, Any]) -> dict[str, Any]:
            now = _now()
            batch = {
                "id": uuid.uuid4().hex,
                "account_id": str(row.get("account_id") or ""),
                "page_id": str(row.get("page_id") or ""),
                "source_type": str(row.get("source_type") or ""),
                "source_url": str(row.get("source_url") or ""),
                "text": str(row.get("text") or ""),
                "total_jobs": len(jobs),
                "completed_jobs": 0,
                "running_jobs": 0,
                "pending_jobs": len(jobs),
                "failed_jobs": 0,
                "cooldown_mode": str(row.get("cooldown_mode") or "fixed"),
                "cooldown_fixed": _kept_int(row.get("cooldown_fixed"), 30),
                "cooldown_min": _kept_int(row.get("cooldown_min"), 30),
                "cooldown_max": _kept_int(row.get("cooldown_max"), 90),
                "watch_seconds": int(row.get("watch_seconds") or 0),
                "watch_min_seconds": int(row.get("watch_min_seconds") or row.get("watch_seconds") or 0),
                "watch_max_seconds": int(row.get("watch_max_seconds") or row.get("watch_seconds") or 0),
                "reel_seconds": int(row.get("reel_seconds") or 0),
                "reels_browsed": False,
                "watched_page_ids": [],
                "commented_page_ids": [],
                "comment": str(row.get("comment") or ""),
                "like_video": bool(row.get("like_video")),
                "source_watched": False,
                "source_commented": False,
                "source_prepared": False,
                "delay_enabled": True,
                "status": "CREATED",
                "cooldown_remaining": 0,
                "created_at": now,
                "started_at": "",
                "finished_at": "",
            }
            doc["share_batches"].append(batch)
            stored = []
            for job in jobs:
                item = dict(job)
                item["id"] = item.get("id") or uuid.uuid4().hex
                item["batch_id"] = batch["id"]
                item.setdefault("created_at", now)
                doc["share_jobs"].append(item)
                stored.append(dict(item))
            batch["jobs"] = stored
            return dict(batch)

        return self._mutate(_apply)

    def share_batches(self) -> list[dict[str, Any]]:
        return list(self.load().get("share_batches") or [])

    def share_jobs(self) -> list[dict[str, Any]]:
        return list(self.load().get("share_jobs") or [])

    def save_share_batch(self, batch: dict[str, Any]) -> None:
        def _apply(doc: dict[str, Any]) -> None:
            clean = {key: value for key, value in batch.items() if key != "jobs"}
            for index, item in enumerate(doc["share_batches"]):
                if str(item.get("id")) == str(batch.get("id")):
                    doc["share_batches"][index] = clean
                    return

        self._mutate(_apply)

    def save_share_job(self, job: dict[str, Any]) -> None:
        def _apply(doc: dict[str, Any]) -> None:
            for index, item in enumerate(doc["share_jobs"]):
                if str(item.get("id")) == str(job.get("id")):
                    doc["share_jobs"][index] = dict(job)
                    return
            doc["share_jobs"].append(dict(job))

        self._mutate(_apply)

    def completed_share_fingerprints(self) -> set[str]:
        """Một lần đọc: tập fingerprint đã chia sẻ thành công (tránh O(n) mỗi Page)."""
        found: set[str] = set()
        for job in self.share_jobs():
            if str(job.get("status")) not in {SHARE_COMPLETED, SHARE_DUPLICATE}:
                continue
            target = str(job.get("page_id") or job.get("account_id") or "")
            found.add(
                share_fingerprint(
                    target,
                    str(job.get("group_id") or ""),
                    str(job.get("source_url") or ""),
                    str(job.get("text") or ""),
                    list(job.get("image_paths") or []),
                )
            )
        return found

    def completed_share_fingerprint(
        self,
        target_id: str,
        group_id: str,
        source_url: str,
        text: str,
        images: list[str] | None = None,
    ) -> bool:
        wanted = share_fingerprint(target_id, group_id, source_url, text, images)
        return wanted in self.completed_share_fingerprints()

    def refresh_batch_counts(self, batch_id: str) -> dict[str, Any] | None:
        def _apply(doc: dict[str, Any]) -> dict[str, Any] | None:
            jobs = [item for item in doc["share_jobs"] if str(item.get("batch_id")) == str(batch_id)]
            batch = None
            for item in doc["share_batches"]:
                if str(item.get("id")) == str(batch_id):
                    batch = item
                    break
            if batch is None:
                return None
            done = {"COMPLETED", "DUPLICATE", "SKIPPED"}
            failed = {"FAILED"}
            running = {
                "VALIDATING",
                "CHECKING_SESSION",
                "CHECKING_PERMISSION",
                "READY",
                "OPENING_GROUP",
                "CREATE_POST",
                "INSERT_TEXT",
                "INSERT_SOURCE_URL",
                "WAIT_LINK_PREVIEW",
                "SUBMIT",
                "VERIFYING",
                "VERIFY_FIRST",
            }
            batch["total_jobs"] = len(jobs)
            batch["completed_jobs"] = sum(1 for item in jobs if item.get("status") in done)
            batch["failed_jobs"] = sum(1 for item in jobs if item.get("status") in failed)
            batch["running_jobs"] = sum(1 for item in jobs if item.get("status") in running)
            batch["pending_jobs"] = sum(1 for item in jobs if item.get("status") == "PENDING")
            return dict(batch)

        return self._mutate(_apply)

    def share_settings(self) -> dict[str, Any]:
        """Cài đặt tab Chia sẻ: bình luận, thời lượng, lần chạy gần nhất."""
        raw = self.load().get("share_settings")
        return dict(raw) if isinstance(raw, dict) else {}

    def save_share_settings(self, settings: dict[str, Any]) -> None:
        """Ghi cài đặt tab Chia sẻ, giữ lại kết quả lần chạy để mở lại còn xem được."""

        def _apply(doc: dict[str, Any]) -> None:
            current = doc.get("share_settings") if isinstance(doc.get("share_settings"), dict) else {}
            merged = dict(current)
            merged.update(settings if isinstance(settings, dict) else {})
            doc["share_settings"] = merged

        self._mutate(_apply)

    def recent_share_batch_ids(self, limit: int = 20) -> list[str]:
        """Mã batch mới nhất, để bảng kết quả còn hiện sau khi đóng app."""
        rows = list(self.share_batches())
        rows.sort(key=lambda row: str(row.get("started_at") or row.get("created_at") or ""), reverse=True)
        found: list[str] = []
        for row in rows:
            token = str(row.get("id") or "").strip()
            if token and token not in found:
                found.append(token)
            if len(found) >= max(1, int(limit)):
                break
        return found
