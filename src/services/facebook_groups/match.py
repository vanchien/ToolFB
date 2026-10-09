"""Điểm khớp chủ đề chỉ để sắp xếp. Không coi là dữ liệu chính thức của Facebook."""

from __future__ import annotations

from typing import Any


def topic_match_score(
    group: dict[str, Any],
    *,
    topic: str = "",
    keywords: list[str] | None = None,
    countries: list[str] | None = None,
    languages: list[str] | None = None,
) -> int:
    """Cộng điểm khi tên/tag khớp topic, từ khóa, quốc gia hoặc ngôn ngữ đã chọn."""
    blob = " ".join(
        [
            str(group.get("group_name") or ""),
            " ".join(str(item) for item in (group.get("topic_tags") or [])),
            " ".join(str(item) for item in (group.get("matched_keywords") or [])),
        ]
    ).casefold()
    score = 0
    wanted_topic = " ".join((topic or "").split()).casefold()
    if wanted_topic and wanted_topic in blob:
        score += 3
    for keyword in keywords or []:
        token = " ".join(str(keyword).split()).casefold()
        if token and token in blob:
            score += 2
    country = str(group.get("country") or "").strip().upper()
    if country and country in {str(item).strip().upper() for item in (countries or []) if str(item).strip()}:
        score += 2
    language = str(group.get("language") or "").strip().casefold()
    if language and language in {str(item).strip().casefold() for item in (languages or []) if str(item).strip()}:
        score += 2
    return score


def membership_bucket(membership_status: str, approval_status: str) -> str:
    """Nhóm hiển thị. UNKNOWN không bị gom vào nhóm không cần duyệt."""
    membership = (membership_status or "UNKNOWN").strip().upper()
    approval = (approval_status or "UNKNOWN").strip().upper()
    if membership == "JOINED":
        return "joined"
    if membership == "PENDING":
        return "pending"
    if approval == "APPROVAL_REQUIRED" or membership == "APPROVAL_REQUIRED":
        return "approval"
    if approval == "NO_APPROVAL_INDICATED" and membership == "NOT_JOINED":
        return "joinable"
    return "unknown"


def posting_bucket(permission: str) -> str:
    value = (permission or "UNKNOWN").strip().upper()
    if value == "ALLOWED":
        return "can_post"
    if value == "DENIED":
        return "cannot_post"
    return "unknown"


def qualify_group(
    group: dict[str, Any],
    *,
    topic: str = "",
    keywords: list[str] | None = None,
    countries: list[str] | None = None,
    languages: list[str] | None = None,
    min_members: int | None = None,
    strict_member_count: bool = True,
) -> tuple[bool, str]:
    """Giữ nhóm khi số thành viên >= mức tối thiểu. Không có mức tối đa."""
    reason = _member_reject_reason(
        group.get("member_count"),
        str(group.get("member_count_status") or "UNKNOWN"),
        min_members,
        strict_member_count,
    )
    if reason:
        return False, reason
    if not _topic_matches(group, topic, keywords or []):
        return False, "TOPIC_NOT_MATCHED"
    if not _label_matches(group, countries or [], ("country",), ("group_name", "description", "category", "source_query")):
        return False, "COUNTRY_NOT_MATCHED"
    if not _label_matches(group, languages or [], ("language",), ("group_name", "description", "category", "source_query")):
        return False, "LANGUAGE_NOT_MATCHED"
    return True, ""


def _member_reject_reason(count: Any, status: str, minimum: int | None, strict: bool) -> str:
    """Chỉ so với mức tối thiểu. Không loại nhóm vì số thành viên quá cao."""
    if minimum is None:
        return ""
    state = (status or "UNKNOWN").strip().upper()
    if state == "UNKNOWN" or count is None:
        return "MEMBER_COUNT_UNKNOWN"
    if strict and state == "APPROXIMATE":
        return "MEMBER_COUNT_APPROXIMATE"
    try:
        value = int(count)
    except (TypeError, ValueError):
        return "MEMBER_COUNT_UNKNOWN"
    if value < int(minimum):
        return "MEMBER_COUNT_TOO_LOW"
    return ""


def _topic_matches(group: dict[str, Any], topic: str, keywords: list[str]) -> bool:
    tokens = []
    for item in [topic, *keywords]:
        token = " ".join(str(item or "").split()).casefold()
        if token and token not in tokens:
            tokens.append(token)
    if not tokens:
        return True
    blob = " ".join(str(group.get(key) or "") for key in ("group_name", "description", "category")).casefold()
    return any(token in blob for token in tokens)


def _label_matches(
    group: dict[str, Any],
    selected: list[str],
    fields: tuple[str, ...],
    text_fields: tuple[str, ...],
) -> bool:
    """Khớp nhãn đã chọn. Không chọn gì thì không lọc. Không tự gán khi không có tín hiệu."""
    wanted = [str(item).strip() for item in selected if str(item).strip()]
    if not wanted:
        return True
    blob = " ".join(str(group.get(key) or "") for key in text_fields).casefold()
    stored = [str(group.get(key) or "").strip() for key in fields]
    for item in wanted:
        token = item.casefold()
        if token in blob or any(token == value.casefold() for value in stored if value):
            return True
    if not any(stored):
        return True
    return False


def passes_discovery_filters(
    group: dict[str, Any],
    membership: dict[str, Any] | None,
    filters: dict[str, Any] | None,
) -> bool:
    """Chỉ giữ Group đúng ô đã tick. Bỏ trống một chiều thì không lọc chiều đó."""
    chosen = filters or {}
    member = membership or {}
    membership_status = str(member.get("membership_status") or group.get("membership_status") or "UNKNOWN")
    approval_status = str(member.get("approval_status") or group.get("approval_status") or "UNKNOWN")
    posting = str(member.get("posting_permission") or group.get("posting_permission") or "UNKNOWN")
    mem_flags = chosen.get("membership") if isinstance(chosen.get("membership"), dict) else {}
    mem_on = [key for key, enabled in (mem_flags or {}).items() if enabled]
    if mem_on and membership_bucket(membership_status, approval_status) not in mem_on:
        return False
    post_flags = chosen.get("posting") if isinstance(chosen.get("posting"), dict) else {}
    post_on = [key for key, enabled in (post_flags or {}).items() if enabled]
    if post_on and posting_bucket(posting) not in post_on:
        return False
    return True
