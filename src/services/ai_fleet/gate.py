"""Cổng duyệt nội dung. Điểm cao không được ghi đè một kiểm tra cứng bị FAIL."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any

def content_hash(row: dict[str, Any]) -> str:
    """Hash phần được duyệt: chữ, hashtag, asset, Page và phiên bản policy."""
    payload = {
        "title": str(row.get("title") or ""),
        "body": str(row.get("body") or ""),
        "caption": str(row.get("caption") or ""),
        "hashtags": sorted(str(item) for item in (row.get("hashtags") or [])),
        "asset_ids": sorted(str(item) for item in (row.get("asset_ids") or [])),
        "page_id": str(row.get("page_id") or ""),
        "policy_version": int(row.get("policy_version") or 0),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def hard_checks(
    version: dict[str, Any],
    policy: dict[str, Any],
    *,
    known_hashes: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Các kiểm tra cứng. FAIL mức HIGH thì không được duyệt dù điểm cao."""
    checks: list[dict[str, Any]] = []
    title = str(version.get("title") or "").strip()
    body = str(version.get("body") or "").strip()
    caption = str(version.get("caption") or "").strip()
    hashtags = list(version.get("hashtags") or [])
    missing = [name for name, value in (("title", title), ("body", body), ("caption", caption)) if not value]
    if not hashtags:
        missing.append("hashtag")
    if not str(version.get("page_id") or "").strip():
        missing.append("page")
    checks.append(_check(
        "COMPLETENESS",
        not missing,
        "Thiếu " + ", ".join(missing) if missing else "Đủ trường bắt buộc",
        "Bổ sung trường còn thiếu",
    ))
    expected = str(policy.get("language") or "").strip()
    language = str(version.get("language") or "").strip()
    checks.append(_check(
        "LANGUAGE",
        not expected or language == expected,
        f"Ngôn ngữ bài là {language or 'trống'}, policy yêu cầu {expected}",
        "Viết lại đúng ngôn ngữ của Page",
    ))
    banned = [str(item).strip() for item in (policy.get("prohibited_topics") or []) if str(item).strip()]
    blob = " ".join((title, body, caption)).casefold()
    hit = next((item for item in banned if item.casefold() in blob), "")
    checks.append(_check(
        "PROHIBITED",
        not hit,
        f"Đụng chủ đề cấm: {hit}" if hit else "Không đụng chủ đề cấm",
        "Bỏ nội dung vi phạm",
    ))
    sources = [str(item).strip() for item in (version.get("source_urls") or []) if str(item).strip()]
    need_source = bool(policy.get("requires_source", True))
    checks.append(_check(
        "SOURCE",
        not need_source or bool(sources),
        "Thiếu nguồn cho thông tin cần kiểm chứng" if need_source and not sources else "Đã có nguồn",
        "Bổ sung nguồn hoặc bỏ claim",
    ))
    assets = [str(item).strip() for item in (version.get("asset_ids") or []) if str(item).strip()]
    need_media = bool(policy.get("requires_media", True))
    checks.append(_check(
        "MEDIA",
        not need_media or bool(assets),
        "Thiếu media" if need_media and not assets else "Đã có media",
        "Gắn asset hợp lệ",
    ))
    digest = str(version.get("content_hash") or "") or content_hash(version)
    duplicate = digest in (known_hashes or set())
    checks.append(_check(
        "ORIGINALITY",
        not duplicate,
        "Trùng nội dung đã có trên Page" if duplicate else "Không trùng bài khác",
        "Viết phương án khác",
    ))
    current = int(policy.get("version") or 0)
    used = int(version.get("policy_version") or 0)
    checks.append(_check(
        "POLICY_VERSION",
        used == current and current > 0,
        f"Bài theo policy {used}, hiện hành là {current}",
        "Duyệt lại theo policy mới",
    ))
    return checks


def decide(
    checks: list[dict[str, Any]],
    *,
    score: int,
    revision_attempt: int,
    policy: dict[str, Any],
    sensitive: bool,
) -> str:
    """APPROVED chỉ khi mọi kiểm tra cứng đạt và điểm đủ ngưỡng."""
    failed = [row for row in checks if row.get("status") == "FAIL"]
    prohibited = any(row.get("criterion") == "PROHIBITED" and row.get("status") == "FAIL" for row in failed)
    if prohibited:
        return "REJECTED"
    if sensitive:
        return "HUMAN_REVIEW_REQUIRED"
    limit = int(policy.get("max_revision_attempts") or 2)
    exhausted = int(revision_attempt) >= limit
    threshold = int(policy.get("min_score") or 80)
    if failed or int(score) < threshold:
        return "HUMAN_REVIEW_REQUIRED" if exhausted else "REVISION_REQUIRED"
    return "APPROVED"


def review_content(store: Any, content_id: str, *, score: int | None = None, require_image: bool = False) -> dict[str, Any]:
    """Duyệt phiên bản hiện tại và ghi audit. Không tự cho đăng."""
    doc_content = store.content(content_id)
    if not doc_content:
        raise ValueError("Không thấy nội dung")
    version = store.current_version(content_id)
    policy = store.current_policy(str(doc_content.get("page_id") or ""))
    if version is None or policy is None:
        raise ValueError("Thiếu phiên bản hoặc policy")
    if require_image:
        policy = dict(policy)
        policy["requires_media"] = True
    used_score = int(version.get("score") if score is None else score)
    checks = hard_checks(
        version,
        policy,
        known_hashes=store.other_hashes(str(doc_content["page_id"]), content_id),
    )
    decision = decide(
        checks,
        score=used_score,
        revision_attempt=int(doc_content.get("revision_attempt") or 1),
        policy=policy,
        sensitive=bool(version.get("sensitive")),
    )
    status = {
        "APPROVED": "APPROVED",
        "REVISION_REQUIRED": "REVISION_REQUIRED",
        "HUMAN_REVIEW_REQUIRED": "HUMAN_REVIEW_REQUIRED",
        "REJECTED": "REJECTED",
    }[decision]
    review = {
        "review_id": f"review_{uuid.uuid4().hex[:10]}",
        "content_id": content_id,
        "content_version": int(version.get("version") or 1),
        "page_id": str(doc_content.get("page_id") or ""),
        "policy_version": int(policy.get("version") or 0),
        "decision": decision,
        "score": used_score,
        "checks": checks,
        "summary": _summary(decision, checks),
        "revision_attempt": int(doc_content.get("revision_attempt") or 1),
        "approved_content_hash": str(version.get("content_hash") or "") if decision == "APPROVED" else "",
        "reviewed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    doc_content["status"] = status
    doc_content.setdefault("reviews", [])
    doc_content["reviews"].append(review)
    store.save_content(
        doc_content,
        audit={
            "action": "content_review",
            "page_id": review["page_id"],
            "detail": {"content_id": content_id, "decision": decision, "review_id": review["review_id"]},
            "at": review["reviewed_at"],
        },
    )
    return review


def schedule_block_reason(
    content_id: str,
    store: Any | None = None,
    job: dict[str, Any] | None = None,
) -> str:
    """Lý do không được đưa vào lịch. Rỗng nghĩa là được phép."""
    holder = store if store is not None else _default_store()
    reason = _block_reason(content_id, holder)
    if reason:
        return reason
    return _job_text_mismatch(holder, content_id, job)


def publish_block_reason(
    content_id: str,
    store: Any | None = None,
    *,
    posted_text: str | None = None,
) -> str:
    """Lý do không được đăng. Kiểm tra lại ngay trước khi xuất bản."""
    holder = store if store is not None else _default_store()
    reason = _block_reason(content_id, holder)
    if reason or posted_text is None:
        return reason
    version = holder.current_version(content_id)
    if version is None:
        return "Không thấy phiên bản nội dung."
    return _text_mismatch(posted_text, version)


def human_decide(store: Any, content_id: str, decision: str) -> dict[str, Any]:
    """Người dùng duyệt hoặc từ chối. Lỗi cứng còn FAIL thì không bấm duyệt được."""
    choice = str(decision or "").strip().upper()
    if choice not in {"APPROVED", "REJECTED"}:
        raise ValueError("Chỉ được duyệt hoặc từ chối.")
    content = store.content(content_id)
    version = store.current_version(content_id)
    if not content or version is None:
        raise ValueError("Không thấy nội dung.")
    policy = store.current_policy(str(content.get("page_id") or ""))
    if policy is None:
        raise ValueError("Page chưa có policy.")
    checks = hard_checks(
        version,
        policy,
        known_hashes=store.other_hashes(str(content["page_id"]), content_id),
    )
    if choice == "APPROVED" and any(row.get("status") == "FAIL" for row in checks):
        raise ValueError("Còn kiểm tra cứng chưa đạt. Sửa bài hoặc từ chối, không duyệt đè.")
    review = {
        "review_id": f"review_{uuid.uuid4().hex[:10]}",
        "content_id": content_id,
        "content_version": int(version.get("version") or 1),
        "page_id": str(content.get("page_id") or ""),
        "policy_version": int(policy.get("version") or 0),
        "decision": choice,
        "score": int(policy.get("min_score") or 80) if choice == "APPROVED" else 0,
        "checks": checks,
        "summary": "Người dùng duyệt." if choice == "APPROVED" else "Người dùng từ chối.",
        "revision_attempt": int(content.get("revision_attempt") or 1),
        "approved_content_hash": str(version.get("content_hash") or "") if choice == "APPROVED" else "",
        "reviewed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "reviewer": "human",
    }
    content["status"] = choice
    content.setdefault("reviews", [])
    content["reviews"].append(review)
    store.save_content(
        content,
        audit={
            "action": "human_review",
            "page_id": review["page_id"],
            "detail": {"content_id": content_id, "decision": choice, "review_id": review["review_id"]},
        },
    )
    return review


def is_approval_block(message: str) -> bool:
    """Lỗi duyệt không phải lỗi mạng, không hẹn đăng lại."""
    text = str(message or "")
    markers = ("APPROVED", "duyệt", "dừng khẩn cấp", "không khớp", "tạm dừng", "đã đăng")
    return any(marker in text for marker in markers)


def _block_reason(content_id: str, holder: Any) -> str:
    doc = holder.load()
    if doc.get("emergency_stop"):
        return "Đã dừng khẩn cấp. Không xếp lịch và không đăng bài mới."
    content = doc["contents"].get(content_id)
    if not content:
        return "Không thấy gói nội dung đã duyệt."
    profile = (doc.get("profiles") or {}).get(str(content.get("page_id") or "")) or {}
    if str(profile.get("status") or "").upper() == "PAUSED":
        return "Page Agent đang tạm dừng. Không xếp lịch và không đăng."
    if content.get("published"):
        return "Bài này đã có dấu đã đăng. Không đăng lại."
    status = str(content.get("status") or "")
    if status != "APPROVED":
        return f"Nội dung đang {status or 'DRAFT'}. Chỉ bài APPROVED mới được đăng."
    reviews = list(content.get("reviews") or [])
    if not reviews:
        return "Chưa có biên bản duyệt."
    review = reviews[-1]
    if str(review.get("decision") or "") != "APPROVED":
        return "Biên bản duyệt cuối không phải APPROVED."
    policy = holder.current_policy(str(content.get("page_id") or ""))
    if policy is None:
        return "Page chưa có policy hiện hành."
    if int(review.get("policy_version") or 0) != int(policy.get("version") or 0):
        return "Policy đã đổi. Phải duyệt lại trước khi đăng."
    version = holder.current_version(content_id)
    if version is None:
        return "Không thấy phiên bản nội dung."
    live_hash = content_hash(version)
    if str(review.get("approved_content_hash") or "") != live_hash:
        return "Nội dung hoặc asset đã đổi sau khi duyệt. Phải duyệt lại."
    if int(review.get("content_version") or 0) != int(version.get("version") or 0):
        return "Phiên bản sắp đăng không phải phiên bản đã duyệt."
    return ""


def _norm(text: object) -> str:
    return " ".join(str(text or "").split())


def _text_mismatch(posted_text: str, version: dict[str, Any]) -> str:
    """Chữ sắp đăng phải là đúng caption hoặc thân bài đã duyệt."""
    text = _norm(posted_text)
    if not text:
        return "Thiếu nội dung sắp đăng."
    allowed = {_norm(version.get("caption")), _norm(version.get("body"))}
    allowed.discard("")
    if text not in allowed:
        return "Nội dung sắp đăng không khớp bản đã duyệt."
    return ""


def _job_text_mismatch(holder: Any, content_id: str, job: dict[str, Any] | None) -> str:
    if not job:
        return ""
    version = holder.current_version(content_id)
    if version is None:
        return "Không thấy phiên bản nội dung."
    title = _norm(job.get("title"))
    if title and title != _norm(version.get("title")):
        return "Tiêu đề job không khớp bản đã duyệt."
    content = _norm(job.get("content"))
    if content:
        return _text_mismatch(content, version)
    return ""


def _check(criterion: str, ok: bool, reason: str, action: str) -> dict[str, Any]:
    return {
        "criterion": criterion,
        "status": "PASS" if ok else "FAIL",
        "severity": "HIGH",
        "reason": reason,
        "evidence": [],
        "required_action": "" if ok else action,
    }


def _summary(decision: str, checks: list[dict[str, Any]]) -> str:
    failed = [str(row.get("reason") or "") for row in checks if row.get("status") == "FAIL"]
    if decision == "APPROVED":
        return "Đạt kiểm tra cứng và đủ điểm."
    if not failed:
        return decision
    return "; ".join(failed)


def _default_store() -> Any:
    from src.services.ai_fleet.store import FleetStore

    return FleetStore()
