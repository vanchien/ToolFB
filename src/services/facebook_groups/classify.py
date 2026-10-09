"""Đọc chữ trên trang Group. UNKNOWN không bị đổi thành được duyệt hoặc được đăng."""

from __future__ import annotations

import re
from typing import Any


_SKIP_GROUP_TOKENS = frozenset(
    {"feed", "joins", "discover", "create", "search", "suggested", "browse", "categories"}
)


def parse_group_search_html(
    html: str,
    *,
    keyword: str = "",
    country: str = "",
    language: str = "",
) -> list[dict[str, Any]]:
    """Lấy group_id từ link /groups/ trong HTML và JSON. Không gọi mạng."""
    found: list[dict[str, Any]] = []
    seen: set[str] = set()
    source = (html or "").replace("\\/", "/")
    pattern = re.compile(
        r'(?:https?:\/\/(?:www\.)?facebook\.com)?\/groups\/([A-Za-z0-9.\-_]+)',
        re.I,
    )
    for match in pattern.finditer(source):
        token = match.group(1).strip().strip(".")
        if not token or token.casefold() in _SKIP_GROUP_TOKENS or len(token) < 3:
            continue
        group_id = token
        if group_id in seen:
            continue
        seen.add(group_id)
        window = source[max(0, match.start() - 180) : match.end() + 220]
        aria = re.search(r'aria-label=["\']([^"\']{2,120})["\']', window)
        name_match = re.search(r">([^<]{2,80})<", window)
        raw_name = aria.group(1) if aria else (name_match.group(1) if name_match else group_id)
        name = " ".join(raw_name.split()) or group_id
        found.append(
            {
                "group_id": group_id,
                "group_name": name,
                "group_url": f"https://www.facebook.com/groups/{group_id}",
                "country": country,
                "language": language,
                "matched_keywords": [keyword] if keyword else [],
                "topic_tags": [],
                "approval_status": "UNKNOWN",
                "membership_status": "UNKNOWN",
                "posting_permission": "UNKNOWN",
            }
        )
    return found


def parse_member_count(text: str) -> int:
    """Số thành viên đã đọc được. Không thấy thì 0."""
    count, _status = inspect_member_count(text)
    return int(count or 0)


def inspect_member_count(text: str) -> tuple[int | None, str]:
    """Trả (số, EXACT|APPROXIMATE|UNKNOWN). Không đọc được thì (None, UNKNOWN)."""
    blob = " ".join((text or "").split())
    match = re.search(
        r"(\d+(?:[.,]\d+)*)\s*(k|m|triệu|trieu|nghìn|nghin)?\s*(\+)?\s*(?:members?|thành viên|thanh vien)",
        blob,
        re.I,
    )
    if match is None:
        match = re.search(
            r"(\d+(?:[.,]\d+)*)\s*(k|m|triệu|trieu|nghìn|nghin)\s*(\+)?",
            blob,
            re.I,
        )
    if match is None:
        return None, "UNKNOWN"
    count = _scale_count(match.group(1), match.group(2) or "")
    window = blob[max(0, match.start() - 24) : match.end() + 8].casefold()
    approximate = bool(match.group(3)) or any(
        token in window for token in ("about", "approx", "around", "more than", "over", "khoảng", "hơn", "xấp xỉ", "~")
    )
    return count, "APPROXIMATE" if approximate else "EXACT"


def parse_min_members(text: str) -> int:
    """Ô người dùng nhập. ``10000`` là mười nghìn, ``10000k`` là mười triệu."""
    raw = "".join((text or "").split()).casefold()
    if not raw:
        return 0
    match = re.fullmatch(r"(\d+(?:[.,]\d+)*)(k|m|triệu|trieu|nghìn|nghin)?", raw)
    if not match:
        raise ValueError("Số thành viên tối thiểu không hợp lệ")
    return _scale_count(match.group(1), match.group(2) or "")


def _scale_count(raw: str, suffix: str) -> int:
    suffix = suffix.casefold()
    multiplier = 1
    if suffix in {"k", "nghìn", "nghin"}:
        multiplier = 1_000
    elif suffix in {"m", "triệu", "trieu"}:
        multiplier = 1_000_000
    if multiplier > 1:
        return int(float(raw.replace(",", ".")) * multiplier)
    digits = raw.replace(",", "").replace(".", "")
    return int(digits) if digits.isdigit() else 0


def group_rows_from_cards(
    cards: list[dict[str, Any]] | None,
    *,
    keyword: str = "",
    country: str = "",
    language: str = "",
) -> list[dict[str, Any]]:
    """Đọc thẻ nhóm trên trang tìm: tên, link và số thành viên."""
    rows: list[dict[str, Any]] = []
    for card in cards or []:
        href = str(card.get("href") or "")
        parsed = parse_group_search_html(href, keyword=keyword, country=country, language=language)
        if not parsed:
            continue
        row = parsed[0]
        name = " ".join(str(card.get("name") or "").split())
        if name and not name.lower().startswith("http"):
            row["group_name"] = name.split("\n")[0][:120]
        count, status = inspect_member_count(str(card.get("text") or ""))
        if count is None:
            count, status = inspect_member_count(name)
        row["member_count"] = count
        row["member_count_status"] = status
        row["description"] = " ".join(str(card.get("text") or "").split())[:400]
        row["source_query"] = " ".join(part for part in (keyword, country, language) if part)
        rows.append(row)
    return rows


def classify_membership_text(text: str) -> tuple[str, str]:
    """Trả (membership, approval). Không suy UNKNOWN thành không cần duyệt."""
    blob = (text or "").casefold()
    if any(token in blob for token in ("request to join", "yêu cầu tham gia", "approval required", "cần phê duyệt")):
        return "NOT_JOINED", "APPROVAL_REQUIRED"
    if any(token in blob for token in ("pending approval", "đang chờ duyệt", "request pending")):
        return "PENDING", "APPROVAL_REQUIRED"
    if any(token in blob for token in ("joined", "đã tham gia", "you are a member", "bạn là thành viên")):
        return "JOINED", "NO_APPROVAL_INDICATED"
    if any(token in blob for token in ("join group", "tham gia nhóm")) and "request" not in blob:
        return "NOT_JOINED", "NO_APPROVAL_INDICATED"
    return "UNKNOWN", "UNKNOWN"


def join_action_from_label(label: str) -> str:
    """``join`` khi Facebook hiện nút tham gia thẳng. ``approval`` khi chỉ có xin duyệt."""
    text = " ".join((label or "").split()).casefold()
    if not text:
        return ""
    if any(token in text for token in ("request", "yêu cầu", "approval", "phê duyệt")):
        return "approval"
    if text in {"join", "join group", "tham gia", "tham gia nhóm"}:
        return "join"
    return ""


def classify_posting_text(text: str, *, membership_status: str) -> str:
    """ALLOWED chỉ khi trang nói được đăng. Không có chữ thì là UNKNOWN."""
    blob = (text or "").casefold()
    if any(token in blob for token in ("can't post", "cannot post", "không thể đăng", "posting is paused", "only admins can post")):
        return "DENIED"
    if any(token in blob for token in ("write something", "viết gì đó", "create a public post", "bạn có thể đăng")):
        return "ALLOWED" if membership_status == "JOINED" else "UNKNOWN"
    return "UNKNOWN"
