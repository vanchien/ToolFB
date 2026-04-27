"""
Lịch sử đăng theo Page — tránh trùng (ảnh 14 ngày, hook 7 ngày, hashtag liên tiếp).

File: ``data/pages/<page_id>/history/published_posts.json`` — mảng ``entries`` theo thời gian (cũ → mới).
"""

from __future__ import annotations

import json
import random
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from src.utils.page_workspace import ensure_page_workspace, page_workspace_root, sanitize_page_id


def _history_path(page_id: str) -> Path:
    sid = sanitize_page_id(page_id)
    return page_workspace_root(sid) / "history" / "published_posts.json"


def _parse_posted_at(raw: str) -> Optional[datetime]:
    s = str(raw or "").strip().replace("Z", "+00:00")
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def normalized_hashtag_set(tags: list[str] | None) -> tuple[str, ...]:
    """Chuẩn hóa hashtag để so sánh tập (thứ tự không quan trọng)."""
    if not tags:
        return tuple()
    cleaned: set[str] = set()
    for t in tags:
        s = str(t).strip().lower().lstrip("#")
        if s:
            cleaned.add(s)
    return tuple(sorted(cleaned))


def normalized_image_key(path: Path | str) -> str:
    """Khóa so sánh đường dẫn ảnh (resolve)."""
    try:
        return str(Path(path).expanduser().resolve())
    except OSError:
        return str(path)


def normalized_caption_fingerprint(caption: str, max_len: int = 200) -> str:
    """Chuỗi so sánh caption gần đây (đầu bài, chữ thường, gom khoảng trắng)."""
    s = " ".join(str(caption or "").strip().lower().split())
    return s[:max_len]


class PostHistoryService:
    """Đọc/ghi ``published_posts.json`` + rule đơn giản theo PRD."""

    def __init__(self, *, image_cooldown_days: int = 14, hook_cooldown_days: int = 7, max_same_hashtag_streak: int = 3) -> None:
        self.image_cooldown_days = image_cooldown_days
        self.hook_cooldown_days = hook_cooldown_days
        self.max_same_hashtag_streak = max_same_hashtag_streak

    def _load_raw(self, page_id: str) -> dict[str, Any]:
        ensure_page_workspace(page_id)
        path = _history_path(page_id)
        if not path.is_file():
            return {"entries": []}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"entries": []}
        if not isinstance(data, dict):
            return {"entries": []}
        entries = data.get("entries")
        if not isinstance(entries, list):
            return {"entries": []}
        return {"entries": [e for e in entries if isinstance(e, dict)]}

    def load_entries(self, page_id: str) -> list[dict[str, Any]]:
        return list(self._load_raw(page_id)["entries"])

    def _atomic_write(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
        d = path.parent
        fd, tmp = tempfile.mkstemp(prefix="hist_", suffix=".tmp.json", dir=str(d))
        try:
            import os

            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
        except Exception:
            try:
                import os

                os.unlink(tmp)
            except OSError:
                pass
            raise

    def append_entry(
        self,
        page_id: str,
        *,
        hook: str = "",
        caption: str = "",
        hashtags: list[str] | None = None,
        cta: str = "",
        image_paths: list[str] | None = None,
        posted_at: datetime | None = None,
    ) -> None:
        """Ghi một lần đăng thành công (append)."""
        sid = sanitize_page_id(page_id)
        raw = self._load_raw(sid)
        entries: list[dict[str, Any]] = list(raw["entries"])
        ts = posted_at or datetime.now(timezone.utc)
        entry = {
            "posted_at": ts.replace(microsecond=0).isoformat(),
            "hook": str(hook or "").strip(),
            "caption": str(caption or ""),
            "hashtags": list(hashtags or []),
            "cta": str(cta or "").strip(),
            "image_paths": [str(p) for p in (image_paths or []) if str(p).strip()],
        }
        entries.append(entry)
        self._atomic_write(_history_path(sid), {"entries": entries})
        logger.debug("Đã append history Page {} ({} entries).", sid, len(entries))

    def was_image_used_within_days(
        self,
        page_id: str,
        image_path: Path | str,
        *,
        days: int | None = None,
        now: datetime | None = None,
    ) -> bool:
        """True nếu cùng file ảnh (resolve) đã xuất hiện trong bài đăng gần ``days`` ngày."""
        d = days if days is not None else self.image_cooldown_days
        now_utc = now or datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(days=d)
        key = normalized_image_key(image_path)
        for row in reversed(self.load_entries(page_id)):
            posted = _parse_posted_at(str(row.get("posted_at", "")))
            if posted is None or posted < cutoff:
                break
            for p in row.get("image_paths") or []:
                if normalized_image_key(p) == key:
                    return True
        return False

    def was_hook_used_within_days(
        self,
        page_id: str,
        hook: str,
        *,
        days: int | None = None,
        now: datetime | None = None,
    ) -> bool:
        """True nếu ``hook`` (chuẩn hóa) đã dùng trong vòng ``days`` ngày."""
        h = str(hook or "").strip().casefold()
        if not h:
            return False
        d = days if days is not None else self.hook_cooldown_days
        now_utc = now or datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(days=d)
        for row in reversed(self.load_entries(page_id)):
            posted = _parse_posted_at(str(row.get("posted_at", "")))
            if posted is None or posted < cutoff:
                break
            rh = str(row.get("hook", "")).strip().casefold()
            if rh == h:
                return True
        return False

    def same_hashtag_set_streak_from_newest(self, page_id: str, tag_set: tuple[str, ...]) -> int:
        """Số bài liên tiếp từ mới nhất có cùng tập hashtag (sau chuẩn hóa) với ``tag_set``."""
        entries = self.load_entries(page_id)
        if not entries:
            return 0
        target = tag_set
        streak = 0
        for row in reversed(entries):
            row_set = normalized_hashtag_set(list(row.get("hashtags") or []))
            if row_set == target:
                streak += 1
            else:
                break
        return streak

    def would_block_hashtag_streak(self, page_id: str, hashtags: list[str] | None) -> bool:
        """
        True nếu đăng thêm một bài với ``hashtags`` sẽ làm **lần thứ 4** liên tiếp cùng tập tag.

        Cho phép tối đa 3 lần liên tiếp; lần thứ 4 bị chặn.
        """
        tag_set = normalized_hashtag_set(hashtags)
        streak = self.same_hashtag_set_streak_from_newest(page_id, tag_set)
        return streak >= self.max_same_hashtag_streak

    def caption_fingerprint_used_within_days(
        self,
        page_id: str,
        caption: str,
        *,
        days: int = 7,
        now: datetime | None = None,
    ) -> bool:
        """True nếu fingerprint đầu bài đã dùng trong ``days`` ngày gần đây."""
        fp = normalized_caption_fingerprint(caption)
        if not fp:
            return False
        now_utc = now or datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(days=days)
        for row in reversed(self.load_entries(page_id)):
            posted = _parse_posted_at(str(row.get("posted_at", "")))
            if posted is None or posted < cutoff:
                break
            prev = normalized_caption_fingerprint(str(row.get("caption", "")))
            if prev and prev == fp:
                return True
        return False


def pick_random_path(candidates: list[Path]) -> Optional[Path]:
    """Chọn ngẫu nhiên một path hoặc None nếu rỗng."""
    if not candidates:
        return None
    return random.choice(candidates)
