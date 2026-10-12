"""Lưu hồ sơ Page Agent, policy và bản thảo. JSON có khóa file, không thêm database mới."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.json_store_lock import json_file_lock
from src.utils.paths import project_root

from .gate import content_hash


def fleet_store_path() -> Path:
    """File fleet. Test trỏ bằng ``TOOLFB_AI_FLEET_PATH``."""
    raw = os.environ.get("TOOLFB_AI_FLEET_PATH", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return project_root() / "config" / "ai_fleet.json"


def _same_text(left: object, right: object) -> bool:
    return " ".join(str(left or "").split()).casefold() == " ".join(str(right or "").split()).casefold()


def _unique_text(items: object) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for item in items or []:
        text = " ".join(str(item or "").split())
        key = text.casefold()
        if not text or key in seen:
            continue
        seen.add(key)
        found.append(text)
    return found


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _empty() -> dict[str, Any]:
    return {
        "emergency_stop": False,
        "profiles": {},
        "policies": {},
        "contents": {},
        "audit": [],
        "providers": {"text": "openai", "image": "openai"},
        "topic_catalog": [],
        "hour_catalog": [],
        "master_instruction": "",
        "master_lessons": [],
        "tasks": [],
        "attention": [],
        "auto_enabled": True,
        "verified_accounts": [],
        "selected_accounts": {"openai": [], "gemini": []},
        "skipped_slots": [],
    }


class FleetStore:
    """Một file cho mọi Page. Memory của Page này không ghi vào Page khác."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = Path(path or fleet_store_path())

    def load(self) -> dict[str, Any]:
        """Đọc kho. File chưa có thì trả cấu trúc rỗng."""
        with json_file_lock(self.path):
            return self._read_unlocked()

    def save(self, doc: dict[str, Any]) -> None:
        """Ghi cả kho sau khi sửa trong bộ nhớ."""
        with json_file_lock(self.path):
            self._write_unlocked(doc)

    def modify(self, mutator: Any) -> None:
        """Đọc, sửa và ghi trong một khóa. Lỗi giữa chừng thì không ghi đè file."""
        with json_file_lock(self.path):
            doc = self._read_unlocked()
            mutator(doc)
            self._write_unlocked(doc)

    def _read_unlocked(self) -> dict[str, Any]:
        if not self.path.is_file():
            return _empty()
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return _empty()
        if not isinstance(raw, dict):
            return _empty()
        base = _empty()
        base.update(raw)
        return base

    def _write_unlocked(self, doc: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def provider_choices(self) -> dict[str, Any]:
        """Nhà viết bài, nhà tạo ảnh, và có tự tạo ảnh hay không."""
        raw = self.load().get("providers") or {}
        return {
            "text": str(raw.get("text") or "openai").strip().lower() or "openai",
            "image": str(raw.get("image") or "openai").strip().lower() or "openai",
            "make_image": bool(raw.get("make_image", True)),
        }

    def set_provider_choices(self, *, text: str, image: str, make_image: bool | None = None) -> dict[str, Any]:
        """Lưu hai lựa chọn. Đổi nhà ảnh không đổi nhà viết bài."""
        doc = self.load()
        current = doc.get("providers") or {}
        chosen: dict[str, Any] = {
            "text": str(text or "").strip().lower(),
            "image": str(image or "").strip().lower(),
        }
        if make_image is None and "make_image" in current:
            chosen["make_image"] = bool(current.get("make_image"))
        elif make_image is not None:
            chosen["make_image"] = bool(make_image)
        doc["providers"] = chosen
        self.save(doc)
        return chosen

    def bind_asset(self, content_id: str, asset_id: str, *, image_provider: str, allow_approved: bool = False) -> dict[str, Any]:
        """Gắn một file thư viện vào đúng bài, trước khi duyệt. Bài đã duyệt không sửa asset."""
        doc = self.load()
        content = doc["contents"].get(content_id)
        if not content:
            raise ValueError("Không thấy nội dung")
        locked = {"PUBLISHED", "REJECTED"} if allow_approved else {"APPROVED", "PUBLISHED", "REJECTED"}
        if str(content.get("status") or "") in locked:
            raise ValueError("Bài đã chốt. Không gắn thêm ảnh.")
        number = int(content.get("current_version") or 0)
        version = next((row for row in content.get("versions") or [] if int(row.get("version") or 0) == number), None)
        if version is None:
            raise ValueError("Không thấy phiên bản nội dung")
        assets = [str(item).strip() for item in (version.get("asset_ids") or []) if str(item).strip()]
        name = str(asset_id or "").strip()
        if name and name not in assets:
            assets.append(name)
        version["asset_ids"] = assets
        version["image_provider"] = str(image_provider or "").strip().lower()
        version["content_hash"] = content_hash(version)
        self._audit(
            doc,
            "asset_bound",
            str(content.get("page_id") or ""),
            {"content_id": content_id, "asset_id": name, "image_provider": version["image_provider"]},
        )
        self.save(doc)
        return content

    def hour_catalog(self) -> list[str]:
        """Các mốc giờ đăng người dùng đã thêm, để hiện lại trong danh sách chọn."""
        return _unique_text(self.load().get("hour_catalog") or [])

    def save_hour_catalog(self, items: list[str]) -> list[str]:
        """Ghi các mốc giờ đã thêm. Không ghi đè giờ của từng Page."""
        cleaned = _unique_text(items)

        def change(doc: dict[str, Any]) -> None:
            doc["hour_catalog"] = cleaned

        self.modify(change)
        return cleaned

    def delete_hour_frame(self, frame: str) -> None:
        """Xóa một khung giờ khỏi danh sách chọn. Không đổi giờ Page đang dùng."""
        target = str(frame or "").strip()

        def change(doc: dict[str, Any]) -> None:
            doc["hour_catalog"] = [item for item in _unique_text(doc.get("hour_catalog") or []) if item != target]

        self.modify(change)

    def master_instruction(self) -> str:
        """Lệnh agent tổng đang áp dụng. Vòng đang chạy đọc lại mỗi lần viết."""
        return str(self.load().get("master_instruction") or "").strip()

    def set_master_instruction(self, text: str) -> None:
        """Ghi lệnh ngay và nhớ lại để lần viết sau vẫn theo. Không cần khởi động lại."""
        cleaned = str(text or "").strip()[:2000]

        def change(doc: dict[str, Any]) -> None:
            previous = str(doc.get("master_instruction") or "").strip()
            doc["master_instruction"] = cleaned
            if not cleaned or cleaned == previous:
                return
            lessons = [row for row in (doc.get("master_lessons") or []) if isinstance(row, dict)]
            lessons.append({"text": cleaned, "at": _now()})
            doc["master_lessons"] = lessons[-30:]

        self.modify(change)

    def delete_tasks(self, task_ids: list[str]) -> int:
        """Xóa tác vụ đã chọn. Tác vụ đang chạy thì giữ lại."""
        wanted = {str(item) for item in task_ids if str(item).strip()}
        removed = 0

        def change(doc: dict[str, Any]) -> None:
            nonlocal removed
            kept = []
            for row in doc.get("tasks") or []:
                if str(row.get("task_id") or "") in wanted and str(row.get("state") or "") != "running":
                    removed += 1
                    continue
                kept.append(row)
            doc["tasks"] = kept

        self.modify(change)
        return removed

    def topic_catalog(self) -> list[str]:
        """Chủ đề đã lưu để hiện trong danh sách chọn."""
        return _unique_text(self.load().get("topic_catalog") or [])

    def save_topic_catalog(self, items: list[str]) -> list[str]:
        """Ghi danh sách chủ đề, bỏ trùng và bỏ dòng trống."""
        cleaned = _unique_text(items)

        def change(doc: dict[str, Any]) -> None:
            doc["topic_catalog"] = cleaned

        self.modify(change)
        return cleaned

    def rename_topic(self, old: str, new: str) -> None:
        """Đổi tên một chủ đề trên danh sách chung và trên mọi Page đang dùng tên cũ."""
        source = str(old or "").strip()
        target = str(new or "").strip()
        if not source or not target:
            raise ValueError("Thiếu tên chủ đề")

        def change(doc: dict[str, Any]) -> None:
            catalog = [target if _same_text(item, source) else str(item) for item in (doc.get("topic_catalog") or [])]
            if not any(_same_text(item, target) for item in catalog):
                catalog.append(target)
            doc["topic_catalog"] = _unique_text(catalog)
            for profile in (doc.get("profiles") or {}).values():
                profile["topic"] = _unique_text(
                    [target if _same_text(item, source) else str(item) for item in (profile.get("topic") or [])]
                )

        self.modify(change)

    def delete_topic(self, name: str) -> None:
        """Xóa một chủ đề khỏi danh sách chung và khỏi mọi Page."""
        source = str(name or "").strip()

        def change(doc: dict[str, Any]) -> None:
            doc["topic_catalog"] = [item for item in _unique_text(doc.get("topic_catalog") or []) if not _same_text(item, source)]
            for profile in (doc.get("profiles") or {}).values():
                profile["topic"] = [item for item in _unique_text(profile.get("topic") or []) if not _same_text(item, source)]

        self.modify(change)

    def tasks(self) -> list[dict[str, Any]]:
        """Hàng tác vụ AI. Không gồm lịch đăng Facebook."""
        rows = self.load().get("tasks") or []
        return [dict(row) for row in rows if isinstance(row, dict)]

    def save_task(self, task: dict[str, Any]) -> None:
        """Ghi một tác vụ theo task_id. Không tự đổi trạng thái."""
        task_id = str(task.get("task_id") or "").strip()
        if not task_id:
            raise ValueError("Thiếu task_id")

        def change(doc: dict[str, Any]) -> None:
            rows = [row for row in (doc.get("tasks") or []) if str(row.get("task_id") or "") != task_id]
            rows.append(dict(task))
            doc["tasks"] = rows[-400:]

        self.modify(change)

    def note_attention(self, page_id: str, message: str) -> None:
        """Ghi một việc của Page cần người xem. Mỗi Page chỉ giữ lỗi mới nhất."""
        text = " ".join(str(message or "").split())[:240]
        if not text:
            return

        def change(doc: dict[str, Any]) -> None:
            rows = [row for row in (doc.get("attention") or []) if str(row.get("page_id") or "") != page_id]
            rows.append({"page_id": page_id, "message": text, "at": _now()})
            doc["attention"] = rows[-40:]

        self.modify(change)

    def clear_attention(self, page_id: str) -> None:
        """Xóa nhắc của một Page khi vòng tự động của Page đó đã chạy ổn."""

        def change(doc: dict[str, Any]) -> None:
            doc["attention"] = [row for row in (doc.get("attention") or []) if str(row.get("page_id") or "") != page_id]

        self.modify(change)

    def verified_accounts(self) -> list[dict[str, str]]:
        """Tài khoản đã kiểm tra đạt. Chỉ lưu loại và id, không lưu khóa."""
        rows = []
        for row in self.load().get("verified_accounts") or []:
            if not isinstance(row, dict):
                continue
            kind = str(row.get("kind") or "")
            key_id = str(row.get("id") or "")
            if kind in {"openai", "gemini"} and key_id:
                rows.append({"kind": kind, "id": key_id})
        return rows

    def mark_verified(self, kind: str, key_id: str) -> None:
        """Ghi một tài khoản đã kiểm tra đạt."""
        provider = str(kind or "").strip().lower()
        ident = str(key_id or "").strip()
        if provider not in {"openai", "gemini"} or not ident:
            return

        def change(doc: dict[str, Any]) -> None:
            rows = [row for row in (doc.get("verified_accounts") or []) if isinstance(row, dict)]
            rows = [row for row in rows if not (str(row.get("kind")) == provider and str(row.get("id")) == ident)]
            rows.append({"kind": provider, "id": ident})
            doc["verified_accounts"] = rows[-80:]

        self.modify(change)

    def forget_verified(self, kind: str, key_id: str) -> None:
        """Gỡ tài khoản khỏi danh sách đã kiểm tra và khỏi lựa chọn đang chạy."""
        provider = str(kind or "").strip().lower()
        ident = str(key_id or "").strip()

        def change(doc: dict[str, Any]) -> None:
            doc["verified_accounts"] = [
                row
                for row in (doc.get("verified_accounts") or [])
                if not (isinstance(row, dict) and str(row.get("kind")) == provider and str(row.get("id")) == ident)
            ]
            selected = dict(doc.get("selected_accounts") or {})
            pool = "openai" if provider == "openai" else "gemini"
            selected[pool] = [item for item in (selected.get(pool) or []) if str(item) != ident]
            doc["selected_accounts"] = selected

        self.modify(change)

    def selected_accounts(self) -> dict[str, list[str]]:
        """Tài khoản người dùng đang chọn để chạy."""
        raw = self.load().get("selected_accounts") or {}
        return {
            "openai": [str(item) for item in (raw.get("openai") or []) if str(item).strip()],
            "gemini": [str(item) for item in (raw.get("gemini") or []) if str(item).strip()],
        }

    def set_selected_accounts(self, openai_ids: list[str], gemini_ids: list[str]) -> None:
        """Lưu các id đã chọn. Không lưu khóa."""

        def change(doc: dict[str, Any]) -> None:
            doc["selected_accounts"] = {
                "openai": [str(item) for item in openai_ids if str(item).strip()],
                "gemini": [str(item) for item in gemini_ids if str(item).strip()],
            }

        self.modify(change)

    def auto_enabled(self) -> bool:
        """Bật thì lịch nền tự viết và đăng. File cũ chưa có cờ thì vẫn bật."""
        return bool(self.load().get("auto_enabled", True))

    def set_auto_enabled(self, enabled: bool) -> None:
        """Bật hoặc tắt vòng tự động. Không xóa bài đã xếp."""

        def change(doc: dict[str, Any]) -> None:
            doc["auto_enabled"] = bool(enabled)

        self.modify(change)

    def set_emergency_stop(self, stopped: bool) -> None:
        """Dừng khẩn cấp: chặn xếp lịch và đăng mới."""
        doc = self.load()
        doc["emergency_stop"] = bool(stopped)
        self._audit(doc, "emergency_stop", "", {"stopped": bool(stopped)})
        self.save(doc)

    def upsert_profile(self, profile: dict[str, Any]) -> dict[str, Any]:
        """Tạo hoặc cập nhật hồ sơ một Page. Không đụng memory Page khác."""
        page_id = str(profile.get("page_id") or "").strip()
        if not page_id:
            raise ValueError("Thiếu page_id")
        doc = self.load()
        current = dict(doc["profiles"].get(page_id) or {})
        merged = {
            "page_id": page_id,
            "agent_id": str(profile.get("agent_id") or current.get("agent_id") or f"page_agent_{page_id}"),
            "page_name": str(profile.get("page_name") or current.get("page_name") or page_id),
            "topic": list(profile.get("topic") or current.get("topic") or []),
            "keywords": list(profile.get("keywords") or current.get("keywords") or []),
            "country": str(profile.get("country") or current.get("country") or ""),
            "language": str(profile.get("language") or current.get("language") or ""),
            "target_audience": str(profile.get("target_audience") or current.get("target_audience") or ""),
            "brand_voice": str(profile.get("brand_voice") or current.get("brand_voice") or ""),
            "content_rules": str(profile.get("content_rules") or current.get("content_rules") or ""),
            "prohibited_topics": list(profile.get("prohibited_topics") or current.get("prohibited_topics") or []),
            "hashtag_rules": str(profile.get("hashtag_rules") or current.get("hashtag_rules") or ""),
            "cta_rules": str(profile.get("cta_rules") or current.get("cta_rules") or ""),
            "posting_schedule": str(profile.get("posting_schedule") or current.get("posting_schedule") or ""),
            "timezone": str(profile.get("timezone") or current.get("timezone") or ""),
            "daily_content_target": int(profile.get("daily_content_target") or current.get("daily_content_target") or 1),
            "approval_policy": str(profile.get("approval_policy") or current.get("approval_policy") or "master_ai_required"),
            "learning_profile_id": str(profile.get("learning_profile_id") or current.get("learning_profile_id") or f"learning_{page_id}"),
            "status": str(profile.get("status") or current.get("status") or "ACTIVE"),
            "memory": dict(current.get("memory") or {
                "brand_memory": [],
                "topic_memory": [],
                "content_memory": [],
                "performance_memory": [],
                "experiment_memory": [],
                "error_memory": [],
            }),
        }
        if "memory" in profile and isinstance(profile["memory"], dict):
            merged["memory"] = dict(profile["memory"])
        doc["profiles"][page_id] = merged
        self._audit(doc, "profile_upsert", page_id, {"agent_id": merged["agent_id"]})
        self.save(doc)
        return merged

    def remember(self, page_id: str, bucket: str, item: dict[str, Any]) -> None:
        """Ghi một bài học vào đúng Page. Bucket lạ thì bỏ."""
        allowed = {
            "brand_memory",
            "topic_memory",
            "content_memory",
            "performance_memory",
            "experiment_memory",
            "error_memory",
        }
        if bucket not in allowed:
            raise ValueError("Memory không hợp lệ")
        doc = self.load()
        profile = doc["profiles"].get(page_id)
        if not profile:
            raise ValueError("Chưa có hồ sơ Page")
        row = dict(item)
        row["saved_at"] = _now()
        profile.setdefault("memory", {})
        profile["memory"].setdefault(bucket, [])
        profile["memory"][bucket].append(row)
        self.save(doc)

    def add_policy(self, page_id: str, rules: dict[str, Any], *, requested_by: str) -> dict[str, Any]:
        """Thêm phiên bản policy. Bản đã đăng không bị sửa. Bản chưa đăng phải duyệt lại."""
        doc = self.load()
        if page_id not in doc["profiles"]:
            raise ValueError("Chưa có hồ sơ Page")
        book = doc["policies"].setdefault(page_id, {"current": 0, "versions": []})
        version = int(book.get("current") or 0) + 1
        row = {
            "version": version,
            "language": str(rules.get("language") or doc["profiles"][page_id].get("language") or ""),
            "prohibited_topics": list(rules.get("prohibited_topics") or []),
            "requires_source": bool(rules.get("requires_source", True)),
            "requires_media": bool(rules.get("requires_media", True)),
            "min_score": int(rules.get("min_score") or 80),
            "max_revision_attempts": int(rules.get("max_revision_attempts") or 2),
            "requested_by": str(requested_by or "").strip() or "user",
            "created_at": _now(),
        }
        book["versions"].append(row)
        book["current"] = version
        for content in doc["contents"].values():
            if str(content.get("page_id") or "") != page_id:
                continue
            if str(content.get("status") or "") in {"PUBLISHED", "REJECTED"}:
                continue
            content["status"] = "RECHECK_REQUIRED"
        self._audit(doc, "policy_version", page_id, {"version": version, "requested_by": row["requested_by"]})
        self.save(doc)
        return row

    def current_policy(self, page_id: str) -> dict[str, Any] | None:
        """Phiên bản policy đang dùng của một Page."""
        book = self.load()["policies"].get(page_id) or {}
        current = int(book.get("current") or 0)
        for row in book.get("versions") or []:
            if int(row.get("version") or 0) == current:
                return dict(row)
        return None

    def create_draft(self, page_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        """Tạo bản thảo phiên bản 1. Chưa được lên lịch."""
        policy = self.current_policy(page_id)
        if policy is None:
            raise ValueError("Page chưa có policy")
        doc = self.load()
        content_id = str(fields.get("content_id") or "").strip() or f"content_{uuid.uuid4().hex[:12]}"
        version = self._version_row(page_id, fields, policy_version=int(policy["version"]), number=1)
        content = {
            "content_id": content_id,
            "page_id": page_id,
            "status": "PENDING_REVIEW",
            "current_version": 1,
            "revision_attempt": 1,
            "versions": [version],
            "reviews": [],
            "published": False,
            "provider": str(fields.get("provider") or "").strip(),
            "model": str(fields.get("model") or "").strip(),
            "usage": dict(fields.get("usage") or {}),
        }
        doc["contents"][content_id] = content
        self._audit(doc, "draft_created", page_id, {"content_id": content_id})
        self.save(doc)
        return content

    def add_version(self, content_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        """Giữ bản cũ và thêm phiên bản mới. Bản đã duyệt nếu bị sửa thì phải duyệt lại."""
        doc = self.load()
        content = doc["contents"].get(content_id)
        if not content:
            raise ValueError("Không thấy nội dung")
        policy = self.current_policy(str(content["page_id"]))
        if policy is None:
            raise ValueError("Page chưa có policy")
        number = int(content.get("current_version") or 1) + 1
        version = self._version_row(
            str(content["page_id"]),
            fields,
            policy_version=int(policy["version"]),
            number=number,
        )
        content["versions"].append(version)
        content["current_version"] = number
        content["revision_attempt"] = int(content.get("revision_attempt") or 1) + 1
        content["status"] = "PENDING_REVIEW"
        self._audit(doc, "content_version", str(content["page_id"]), {"content_id": content_id, "version": number})
        self.save(doc)
        return content

    def skip_slots(self, page_id: str, slots: list[str]) -> None:
        """Nhớ giờ người dùng đã xóa để vòng tự động không viết lại đúng giờ đó."""
        page = str(page_id or "").strip()
        keys = [str(item).strip() for item in slots if str(item).strip()]
        if not page or not keys:
            return

        def change(doc: dict[str, Any]) -> None:
            rows = [row for row in (doc.get("skipped_slots") or []) if isinstance(row, dict)]
            have = {(str(row.get("page_id") or ""), str(row.get("slot") or "")) for row in rows}
            for key in keys:
                if (page, key) not in have:
                    rows.append({"page_id": page, "slot": key})
            doc["skipped_slots"] = rows[-300:]

        self.modify(change)

    def delete_content(self, content_id: str) -> None:
        """Xóa một bài chưa đăng khỏi kho."""
        ident = str(content_id or "").strip()

        def change(doc: dict[str, Any]) -> None:
            contents = dict(doc.get("contents") or {})
            content = contents.get(ident)
            if not content:
                raise ValueError("Không thấy bài.")
            if content.get("published") or str(content.get("status") or "") == "PUBLISHED":
                raise ValueError("Bài đã đăng. Không xóa.")
            contents.pop(ident, None)
            doc["contents"] = contents

        self.modify(change)

    def content(self, content_id: str) -> dict[str, Any] | None:
        """Một gói nội dung, hoặc None nếu chưa có."""
        row = self.load()["contents"].get(content_id)
        return dict(row) if row else None

    def current_version(self, content_id: str) -> dict[str, Any] | None:
        """Phiên bản nội dung đang xét."""
        content = self.content(content_id)
        if not content:
            return None
        number = int(content.get("current_version") or 0)
        for row in content.get("versions") or []:
            if int(row.get("version") or 0) == number:
                return dict(row)
        return None

    def save_content(self, content: dict[str, Any], audit: dict[str, Any] | None = None) -> None:
        """Ghi một gói nội dung. Có audit thì ghi cùng một lần."""
        doc = self.load()
        content_id = str(content.get("content_id") or "")
        if not content_id or content_id not in doc["contents"]:
            raise ValueError("Không thấy nội dung")
        doc["contents"][content_id] = content
        if audit:
            self._audit(doc, str(audit.get("action") or "content_review"), str(audit.get("page_id") or ""), dict(audit.get("detail") or {}))
        self.save(doc)

    def mark_published(self, content_id: str) -> None:
        """Đánh dấu đã đăng để lần chạy sau không đăng lại cùng một bản."""
        doc = self.load()
        content = doc["contents"].get(content_id)
        if not content:
            return
        content["published"] = True
        content["status"] = "PUBLISHED"
        self._audit(doc, "published", str(content.get("page_id") or ""), {"content_id": content_id})
        self.save(doc)

    def other_hashes(self, page_id: str, content_id: str) -> set[str]:
        """Hash bài khác cùng Page, để phát hiện trùng."""
        found: set[str] = set()
        for key, content in self.load()["contents"].items():
            if key == content_id or str(content.get("page_id") or "") != page_id:
                continue
            for version in content.get("versions") or []:
                digest = str(version.get("content_hash") or "")
                if digest:
                    found.add(digest)
        return found

    def _version_row(self, page_id: str, fields: dict[str, Any], *, policy_version: int, number: int) -> dict[str, Any]:
        row = {
            "version": number,
            "title": str(fields.get("title") or "").strip(),
            "body": str(fields.get("body") or "").strip(),
            "caption": str(fields.get("caption") or "").strip(),
            "hashtags": [str(item).strip() for item in (fields.get("hashtags") or []) if str(item).strip()],
            "source_urls": [str(item).strip() for item in (fields.get("source_urls") or []) if str(item).strip()],
            "asset_ids": [str(item).strip() for item in (fields.get("asset_ids") or []) if str(item).strip()],
            "language": str(fields.get("language") or "").strip(),
            "page_id": page_id,
            "policy_version": policy_version,
            "sensitive": bool(fields.get("sensitive")),
            "score": int(fields.get("score") or 0),
            "image_prompt": str(fields.get("image_prompt") or "").strip(),
            "provider": str(fields.get("provider") or "").strip(),
            "model": str(fields.get("model") or "").strip(),
        }
        row["content_hash"] = content_hash(row)
        return row

    def _audit(self, doc: dict[str, Any], action: str, page_id: str, detail: dict[str, Any]) -> None:
        doc.setdefault("audit", [])
        doc["audit"].append(
            {
                "audit_id": uuid.uuid4().hex[:12],
                "action": action,
                "page_id": page_id,
                "detail": detail,
                "at": _now(),
            }
        )
