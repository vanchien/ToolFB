"""
Lưu snapshot thống kê Page (followers, views) — ``config/page_insights.json``.
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, TypedDict

from loguru import logger

from src.utils.json_store_lock import json_file_lock
from src.utils.paths import project_root

PageInsightsPeriod = Literal["7d", "28d"]


class PageInsightsSnapshot(TypedDict, total=False):
    period: str
    fetched_at: str
    followers: int | None
    views: int | None
    source_url: str
    error: str
    skipped: bool
    skip_reason: str


def _default_path() -> Path:
    return project_root() / "config" / "page_insights.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_fetched_at(iso: str) -> datetime | None:
    raw = str(iso or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def snapshot_age_hours(snap: PageInsightsSnapshot | dict[str, Any] | None) -> float | None:
    if not snap:
        return None
    dt = parse_fetched_at(str(snap.get("fetched_at", "") or ""))
    if dt is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)


class PageInsightsStore:
    def __init__(self, json_path: Path | str | None = None) -> None:
        self.file_path = Path(json_path).resolve() if json_path else _default_path()
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.file_path.is_file():
            self._write({"by_page_id": {}, "meta": {}})

    def _read(self) -> dict[str, Any]:
        with json_file_lock(self.file_path):
            raw = json.loads(self.file_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {"by_page_id": {}, "meta": {}}
        if "by_page_id" not in raw or not isinstance(raw["by_page_id"], dict):
            raw["by_page_id"] = {}
        if "meta" not in raw or not isinstance(raw["meta"], dict):
            raw["meta"] = {}
        return raw

    def _write(self, data: dict[str, Any]) -> None:
        text = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
        d = self.file_path.parent
        fd, tmp = tempfile.mkstemp(prefix="page_insights_", suffix=".tmp.json", dir=str(d))
        try:
            import os

            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.file_path)
        except Exception:
            try:
                import os

                os.unlink(tmp)
            except OSError:
                pass
            raise

    def get_snapshot(self, page_id: str, period: PageInsightsPeriod) -> PageInsightsSnapshot | None:
        pid = str(page_id or "").strip()
        if not pid:
            return None
        data = self._read()
        row = data.get("by_page_id", {}).get(pid)
        if not isinstance(row, dict):
            return None
        snap = row.get(period)
        return dict(snap) if isinstance(snap, dict) else None

    def save_snapshot(
        self,
        page_id: str,
        period: PageInsightsPeriod,
        *,
        followers: int | None,
        views: int | None,
        source_url: str = "",
        error: str = "",
    ) -> PageInsightsSnapshot:
        pid = str(page_id or "").strip()
        if not pid:
            raise ValueError("page_id rỗng")
        snap: PageInsightsSnapshot = {
            "period": period,
            "fetched_at": _now_iso(),
            "followers": followers,
            "views": views,
            "source_url": str(source_url or "").strip(),
        }
        if error:
            snap["error"] = str(error).strip()
        with json_file_lock(self.file_path):
            data = self._read()
            by = data.setdefault("by_page_id", {})
            if not isinstance(by, dict):
                by = {}
                data["by_page_id"] = by
            cur = by.get(pid)
            if not isinstance(cur, dict):
                cur = {}
            cur[period] = snap
            by[pid] = cur
            self._write(data)
        logger.info(
            "page_insights: lưu {} period={} followers={} views={}",
            pid,
            period,
            followers,
            views,
        )
        return snap

    def all_for_page(self, page_id: str) -> dict[str, PageInsightsSnapshot]:
        pid = str(page_id or "").strip()
        data = self._read()
        row = data.get("by_page_id", {}).get(pid)
        if not isinstance(row, dict):
            return {}
        out: dict[str, PageInsightsSnapshot] = {}
        for k in ("7d", "28d"):
            if isinstance(row.get(k), dict):
                out[k] = dict(row[k])  # type: ignore[arg-type]
        return out

    def should_skip_fetch(
        self,
        page_id: str,
        period: PageInsightsPeriod,
        *,
        min_hours_success: float,
        min_hours_error: float,
        force: bool = False,
    ) -> tuple[bool, str]:
        if force:
            return False, ""
        snap = self.get_snapshot(page_id, period)
        if not snap:
            return False, ""
        age = snapshot_age_hours(snap)
        if age is None:
            return False, ""
        has_data = snap.get("followers") is not None or snap.get("views") is not None
        if has_data and age < min_hours_success:
            return True, f"dữ liệu còn mới ({age:.1f}h / {min_hours_success:.0f}h)"
        if not has_data and str(snap.get("error", "") or "").strip() and age < min_hours_error:
            return True, f"thử lại sau ({min_hours_error:.0f}h, lỗi gần đây)"
        return False, ""

    def account_can_start_session(self, account_id: str, cooldown_min: int) -> tuple[bool, str]:
        aid = str(account_id or "").strip()
        if not aid or cooldown_min <= 0:
            return True, ""
        data = self._read()
        meta = data.get("meta", {})
        if not isinstance(meta, dict):
            return True, ""
        last_map = meta.get("last_account_fetch")
        if not isinstance(last_map, dict):
            return True, ""
        last_iso = str(last_map.get(aid, "") or "").strip()
        dt = parse_fetched_at(last_iso)
        if dt is None:
            return True, ""
        age_min = (datetime.now(timezone.utc) - dt).total_seconds() / 60.0
        if age_min < cooldown_min:
            wait = cooldown_min - age_min
            return False, f"tài khoản vừa quét ({age_min:.0f} phút trước), chờ thêm ~{wait:.0f} phút"
        return True, ""

    def touch_account_session(self, account_id: str) -> None:
        aid = str(account_id or "").strip()
        if not aid:
            return
        with json_file_lock(self.file_path):
            data = self._read()
            meta = data.setdefault("meta", {})
            if not isinstance(meta, dict):
                meta = {}
                data["meta"] = meta
            last_map = meta.setdefault("last_account_fetch", {})
            if not isinstance(last_map, dict):
                last_map = {}
                meta["last_account_fetch"] = last_map
            last_map[aid] = _now_iso()
            self._write(data)
