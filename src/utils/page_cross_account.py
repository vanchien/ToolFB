"""
Cùng một Page Facebook gắn nhiều tài khoản.

Chia lịch / chia sẻ / đăng: mỗi Page Facebook chỉ chạy trên **một** TK người dùng chọn —
tránh một ngày đăng lặp nhiều lần từ nhiều profile.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from loguru import logger

from src.utils.paths import project_root

_CHOICES_NAME = "page_owner_choices.json"
DUP_PAGE_SKIP_MARK = "chỉ tài khoản đã chọn nắm Page được đăng"


def page_owner_choices_path(*, root: Path | None = None) -> Path:
    """``config/page_owner_choices.json``."""
    return (root or project_root()) / "config" / _CHOICES_NAME


def facebook_page_group_key(row: Mapping[str, Any] | None) -> str:
    """
    Khóa nhận diện Page Facebook (không phụ thuộc bản ghi local / account_id).

    Ưu tiên ``fb_page_id`` số, rồi vanity/id từ URL, rồi tên Page (khi không phải UID).
    """
    if not isinstance(row, Mapping):
        return ""
    fb = str(row.get("fb_page_id") or "").strip()
    if fb.isdigit() and len(fb) >= 5:
        return f"id:{fb}"
    pid = str(row.get("page_id") or "").strip()
    if pid.isdigit() and len(pid) >= 8:
        return f"id:{pid}"
    url = str(row.get("page_url") or row.get("target_url") or "").strip()
    if url:
        try:
            from src.services.facebook_reels_catalog import facebook_page_identity

            ident = facebook_page_identity(url)
        except Exception:  # noqa: BLE001
            ident = ""
        if ident:
            return ident
    name = " ".join(str(row.get("page_name") or row.get("page_label") or "").split()).casefold()
    if not name or len(name) < 3:
        return ""
    if name.isdigit() or name.startswith("uid_"):
        return ""
    return f"name:{name}"


def unique_row_token(row: Mapping[str, Any] | None) -> str:
    """Khóa dự phòng khi không gom được Page Facebook (bản ghi local)."""
    if not isinstance(row, Mapping):
        return ""
    for field in ("iid", "id", "page_id"):
        val = str(row.get(field) or "").strip()
        if val:
            return f"row:{val}"
    return ""


def receiving_identity_key(row: Mapping[str, Any] | None) -> str:
    """Khóa chống trùng khi tích Page nhận / chia dest."""
    return facebook_page_group_key(row) or unique_row_token(row)


def _account_id(row: Mapping[str, Any]) -> str:
    return str(row.get("account_id") or "").strip()


def _page_title(row: Mapping[str, Any]) -> str:
    name = " ".join(str(row.get("page_name") or row.get("page_label") or "").split())
    if name:
        return name
    url = str(row.get("page_url") or row.get("target_url") or "").strip()
    return url or str(row.get("page_id") or row.get("id") or "Page")


@dataclass
class DuplicatePageGroup:
    """Một Page Facebook xuất hiện trên ≥2 tài khoản."""

    key: str
    title: str
    rows: list[dict[str, Any]] = field(default_factory=list)

    @property
    def account_ids(self) -> list[str]:
        seen: list[str] = []
        for row in self.rows:
            aid = _account_id(row)
            if aid and aid not in seen:
                seen.append(aid)
        return seen


def group_rows_by_facebook_page(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Gom bản ghi theo khóa Page Facebook (bỏ dòng không nhận diện được)."""
    buckets: dict[str, list[dict[str, Any]]] = {}
    for raw in rows or []:
        if not isinstance(raw, Mapping):
            continue
        key = facebook_page_group_key(raw)
        if not key:
            continue
        buckets.setdefault(key, []).append(dict(raw))
    return buckets


def find_cross_account_page_groups(rows: Iterable[Mapping[str, Any]]) -> list[DuplicatePageGroup]:
    """Chỉ các Page gắn từ 2 tài khoản trở lên."""
    groups: list[DuplicatePageGroup] = []
    for key, members in group_rows_by_facebook_page(rows).items():
        aids = []
        for row in members:
            aid = _account_id(row)
            if aid and aid not in aids:
                aids.append(aid)
        if len(aids) < 2:
            continue
        title = _page_title(members[0])
        for row in members:
            t = _page_title(row)
            if t and t != title and len(t) > len(title):
                title = t
        groups.append(DuplicatePageGroup(key=key, title=title, rows=members))
    groups.sort(key=lambda g: g.title.casefold())
    return groups


def unresolved_owner_groups(
    groups: Sequence[DuplicatePageGroup],
    choices: Mapping[str, str] | None,
) -> list[DuplicatePageGroup]:
    """Nhóm chưa có TK được chọn (hoặc TK đã chọn không còn trong danh sách)."""
    owners = dict(choices or {})
    out: list[DuplicatePageGroup] = []
    for group in groups:
        pick = str(owners.get(group.key) or "").strip()
        if pick and pick in group.account_ids:
            continue
        out.append(group)
    return out


def load_page_owner_choices(*, root: Path | None = None) -> dict[str, str]:
    """Đọc ``key → account_id`` đã chọn."""
    path = page_owner_choices_path(root=root)
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không đọc được {}: {}", path, exc)
        return {}
    owners = raw.get("owners") if isinstance(raw, dict) else None
    if not isinstance(owners, dict):
        return {}
    out: dict[str, str] = {}
    for key, aid in owners.items():
        k = str(key or "").strip()
        a = str(aid or "").strip()
        if k and a:
            out[k] = a
    return out


def save_page_owner_choices(
    patch: Mapping[str, str],
    *,
    root: Path | None = None,
) -> dict[str, str]:
    """Gộp và ghi lựa chọn TK cho Page trùng."""
    current = load_page_owner_choices(root=root)
    for key, aid in (patch or {}).items():
        k = str(key or "").strip()
        a = str(aid or "").strip()
        if not k:
            continue
        if a:
            current[k] = a
        else:
            current.pop(k, None)
    path = page_owner_choices_path(root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"owners": current}
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không ghi được {}: {}", path, exc)
    return current


def preferred_account_id(group: DuplicatePageGroup, choices: Mapping[str, str] | None) -> str:
    """TK đã chọn, không thì TK đầu tiên trong nhóm."""
    pick = str((choices or {}).get(group.key) or "").strip()
    if pick in group.account_ids:
        return pick
    return group.account_ids[0] if group.account_ids else ""


def pick_row_for_group(group: DuplicatePageGroup, account_id: str) -> dict[str, Any] | None:
    """Bản ghi của đúng tài khoản trong nhóm."""
    want = str(account_id or "").strip()
    for row in group.rows:
        if _account_id(row) == want:
            return row
    return group.rows[0] if group.rows else None


def filter_rows_one_account_per_facebook_page(
    rows: Sequence[Mapping[str, Any]],
    choices: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    """
    Mỗi Page Facebook giữ đúng một bản ghi.

    Trùng nhiều tài khoản: giữ TK người dùng đã chọn (không thì TK đầu).
    Trùng cùng một TK / không có account_id: giữ dòng đầu.
    """
    buckets = group_rows_by_facebook_page(rows)
    used_keys: set[str] = set()
    out: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, Mapping):
            continue
        key = facebook_page_group_key(raw)
        members = buckets.get(key) if key else None
        if not key or not members or len(members) <= 1:
            out.append(dict(raw))
            continue
        if key in used_keys:
            continue
        used_keys.add(key)
        aids: list[str] = []
        for row in members:
            aid = _account_id(row)
            if aid and aid not in aids:
                aids.append(aid)
        if len(aids) >= 2:
            group = DuplicatePageGroup(key=key, title=_page_title(members[0]), rows=list(members))
            aid = preferred_account_id(group, choices)
            picked = pick_row_for_group(group, aid)
            out.append(dict(picked) if picked is not None else dict(raw))
        else:
            out.append(dict(members[0]))
    return out


def should_run_page_on_account(
    page_row: Mapping[str, Any] | None,
    account_id: str,
    *,
    sibling_rows: Sequence[Mapping[str, Any]] | None = None,
    choices: Mapping[str, str] | None = None,
) -> bool:
    """
    False khi Page gắn ≥2 TK và ``account_id`` không phải TK đã chọn chạy.

    Chưa nhớ lựa chọn: giữ TK đầu trong danh sách (tránh mọi TK cùng đăng).
    """
    if not isinstance(page_row, Mapping):
        return True
    aid = str(account_id or "").strip()
    if not aid:
        return False
    key = facebook_page_group_key(page_row)
    if not key:
        return True
    holders: list[str] = []
    for row in sibling_rows or ():
        if not isinstance(row, Mapping):
            continue
        if facebook_page_group_key(row) != key:
            continue
        other = _account_id(row)
        if other and other not in holders:
            holders.append(other)
    owner = str((choices or {}).get(key) or "").strip()
    if len(holders) >= 2:
        if owner in holders:
            return aid == owner
        return aid == holders[0]
    if owner:
        return aid == owner
    return True


def duplicate_page_skip_reason(
    page_row: Mapping[str, Any] | None,
    account_id: str,
    *,
    sibling_rows: Sequence[Mapping[str, Any]] | None = None,
    choices: Mapping[str, str] | None = None,
) -> str:
    """Câu bỏ qua job khi Page trùng nhiều TK; rỗng = được chạy."""
    if should_run_page_on_account(
        page_row,
        account_id,
        sibling_rows=sibling_rows,
        choices=choices,
    ):
        return ""
    title = _page_title(page_row) if isinstance(page_row, Mapping) else "Page"
    return (
        f"Page «{title}» gắn nhiều tài khoản — {DUP_PAGE_SKIP_MARK}, "
        "tránh lặp bài trong ngày."
    )


def duplicate_summary(groups: Sequence[DuplicatePageGroup], choices: Mapping[str, str] | None = None) -> str:
    """Một câu tiếng Việt cho ô xem trước / thanh trạng thái."""
    n = len(groups)
    if n <= 0:
        return ""
    unresolved = unresolved_owner_groups(groups, choices)
    names = ", ".join(g.title for g in groups[:4])
    extra = f" (+{n - 4})" if n > 4 else ""
    if unresolved:
        return (
            f"{n} Page gắn nhiều tài khoản ({names}{extra}) — "
            f"chọn TK nào nắm Page đó trước khi chạy, tránh đăng lặp trong ngày."
        )
    return f"{n} Page trùng nhiều TK đã chốt người chạy ({names}{extra})."
