"""
Chia list video đã edit thành từng khúc, mỗi Page nhận N clip.

Tải chung một job → edit → job chờ đăng → gán Page theo số lượng người dùng đặt.
"""

from __future__ import annotations

import random
from typing import Any


def clamp_videos_per_page(raw: object, *, default: int = 5) -> int:
    """Số video mỗi Page: 1–500."""
    try:
        n = int(str(raw).strip())
    except (TypeError, ValueError):
        n = int(default)
    return max(1, min(500, n))


def item_has_facebook_page(it: dict[str, Any]) -> bool:
    """Clip đã có Page Facebook riêng."""
    if str(it.get("item_publish_target") or "").strip().lower() == "tiktok":
        return False
    return bool(str(it.get("item_preset_fb_page_id") or "").strip())


def collect_split_indexes(
    items: list[Any],
    *,
    item_indexes: list[int] | None = None,
    only_unassigned: bool = False,
) -> list[int]:
    """Index clip có file video và chưa có lịch, theo thứ tự list (hoặc theo ``item_indexes``)."""
    allowed = set(item_indexes) if item_indexes is not None else None
    out: list[int] = []
    for i, it in enumerate(items or []):
        if allowed is not None and i not in allowed:
            continue
        if not isinstance(it, dict):
            continue
        if not str(it.get("video_path") or "").strip():
            continue
        if str(it.get("item_scheduled_job_id") or "").strip():
            continue
        if only_unassigned and item_has_facebook_page(it):
            continue
        out.append(i)
    return out


def auto_split_counts(n_videos: int, n_pages: int) -> list[int]:
    """Chia đều: mỗi Page nhận n//p video, phần dư +1 cho các Page đầu."""
    videos = max(0, int(n_videos))
    pages = max(0, int(n_pages))
    if videos <= 0 or pages <= 0:
        return []
    pages = min(pages, videos)
    base, extra = divmod(videos, pages)
    return [base + (1 if i < extra else 0) for i in range(pages)]


def pages_already_used(items: list[Any]) -> set[str]:
    """Page Facebook đã nhận video trong job."""
    out: set[str] = set()
    for it in items or []:
        if isinstance(it, dict) and item_has_facebook_page(it):
            out.add(str(it.get("item_preset_fb_page_id") or "").strip())
    return out


def preview_page_split(
    n_videos: int,
    n_pages: int,
    videos_per_page: int | None,
) -> dict[str, int]:
    """Tính trước: cần bao nhiêu Page, gán bao nhiêu, thừa bao nhiêu. ``None``/0 = Auto chia đều."""
    videos = max(0, int(n_videos))
    pages = max(0, int(n_pages))
    if not videos_per_page:
        counts = auto_split_counts(videos, pages)
        return {
            "videos": videos,
            "pages": pages,
            "videos_per_page": 0,
            "auto": 1,
            "min_per_page": min(counts) if counts else 0,
            "max_per_page": max(counts) if counts else 0,
            "need_pages": len(counts) if pages else 0,
            "used_pages": len(counts),
            "assigned": sum(counts),
            "leftover": videos - sum(counts) if pages else videos,
            "unused_pages": max(0, pages - len(counts)),
        }
    vpp = clamp_videos_per_page(videos_per_page)
    need_pages = (videos + vpp - 1) // vpp if videos else 0
    used_pages = min(pages, need_pages)
    assigned = min(videos, used_pages * vpp)
    leftover = max(0, videos - assigned)
    unused_pages = max(0, pages - used_pages)
    return {
        "videos": videos,
        "pages": pages,
        "videos_per_page": vpp,
        "need_pages": need_pages,
        "used_pages": used_pages,
        "assigned": assigned,
        "leftover": leftover,
        "unused_pages": unused_pages,
    }


def format_split_preview(info: dict[str, int]) -> str:
    """Câu tiếng Việt cho ô xem trước."""
    vpp = int(info.get("videos_per_page") or 1)
    videos = int(info.get("videos") or 0)
    pages = int(info.get("pages") or 0)
    need = int(info.get("need_pages") or 0)
    assigned = int(info.get("assigned") or 0)
    leftover = int(info.get("leftover") or 0)
    unused = int(info.get("unused_pages") or 0)
    if videos <= 0:
        return "Chưa có video để chia."
    if info.get("auto"):
        if pages <= 0:
            return f"{videos} video · Auto chia đều → chọn Page."
        lo = int(info.get("min_per_page") or 0)
        hi = int(info.get("max_per_page") or 0)
        per = f"{lo}" if lo == hi else f"{lo}–{hi}"
        text = f"{videos} video · Auto chia đều cho {int(info.get('used_pages') or 0)} Page → {per} video/page"
        if unused:
            text += f" · {unused} Page không nhận (ít video hơn Page)"
        return text + "."
    if pages <= 0:
        return f"{videos} video · {vpp}/page → cần chọn {need} Page."
    parts = [f"{videos} video · {vpp}/page → {need} Page"]
    parts.append(f"đã chọn {pages}")
    parts.append(f"sẽ gán {assigned} video cho {int(info.get('used_pages') or 0)} Page")
    if leftover:
        parts.append(f"thừa {leftover} video (chưa có Page)")
    if unused:
        parts.append(f"{unused} Page không nhận video")
    return " · ".join(parts) + "."


def apply_page_split(
    items: list[Any],
    dests: list[dict[str, str]],
    videos_per_page: int | None,
    *,
    item_indexes: list[int] | None = None,
    only_unassigned: bool = False,
    shuffle_dests: bool = False,
    rng: random.Random | None = None,
) -> dict[str, Any]:
    """
    Gán lần lượt: Page 1 nhận N clip đầu, Page 2 nhận N clip tiếp, …

    ``videos_per_page`` rỗng/0 = Auto: chia đều mọi clip cho các Page đã chọn.
    ``dests``: ``{"account_id", "page_id"}`` theo thứ tự người dùng chọn.
    Sửa ``items`` tại chỗ. Clip thừa giữ nguyên đích cũ (lần sau chọn «Chưa có Page»).
    """
    auto = not videos_per_page
    vpp = 0 if auto else clamp_videos_per_page(videos_per_page)
    clean_dests: list[dict[str, str]] = []
    for row in dests or []:
        if not isinstance(row, dict):
            continue
        aid = str(row.get("account_id") or "").strip()
        pid = str(row.get("page_id") or "").strip()
        if aid and pid:
            clean_dests.append({"account_id": aid, "page_id": pid})
    if shuffle_dests and clean_dests:
        (rng or random.Random()).shuffle(clean_dests)
    candidates = collect_split_indexes(
        items,
        item_indexes=item_indexes,
        only_unassigned=only_unassigned,
    )
    if auto:
        sizes = auto_split_counts(len(candidates), len(clean_dests))
    else:
        sizes = [vpp] * len(clean_dests)
    chunks: list[dict[str, Any]] = []
    assigned = 0
    start = 0
    for dest, size in zip(clean_dests, sizes):
        chunk = candidates[start : start + size]
        start += size
        if not chunk:
            break
        for idx in chunk:
            it = items[idx]
            if not isinstance(it, dict):
                continue
            it["item_publish_target"] = "facebook"
            it["item_preset_fb_account_id"] = dest["account_id"]
            it["item_preset_fb_page_id"] = dest["page_id"]
            it.pop("item_preset_tiktok_account_id", None)
            assigned += 1
        chunks.append({"account_id": dest["account_id"], "page_id": dest["page_id"], "indexes": chunk})
    leftover = candidates[assigned:]
    return {
        "assigned": assigned,
        "pages_used": len(chunks),
        "leftover_indexes": leftover,
        "chunks": chunks,
        "videos_per_page": vpp,
        "preview": preview_page_split(len(candidates), len(clean_dests), vpp),
    }
