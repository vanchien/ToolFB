from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Iterable

from src.services.tiktok.json_io import read_json_list, write_json_resilient
from src.services.tiktok.layout import ensure_tiktok_layout


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def default_job_dict(
    *,
    account_id: str,
    video_path: str,
    caption: str = "",
    hashtags: list[str] | None = None,
    privacy: str = "public",
) -> dict[str, Any]:
    jid = f"tt_job_{uuid.uuid4().hex[:10]}"
    tags = hashtags if isinstance(hashtags, list) else []
    return {
        "id": jid,
        "account_id": str(account_id).strip(),
        "video_path": str(video_path).strip(),
        "caption": str(caption).strip(),
        "hashtags": [str(x).strip() for x in tags if str(x).strip()],
        "privacy": str(privacy or "public").strip().lower(),
        "allow_comments": True,
        "allow_duet": True,
        "allow_stitch": True,
        "schedule_enabled": False,
        "schedule_time": "",
        "scheduled_at": "",
        "status": "pending",
        "step": "",
        "retry_count": 0,
        "max_retry": 2,
        "error_message": "",
        "created_at": _now_iso(),
        "completed_at": "",
    }


class TikTokJobStore:
    def __init__(self) -> None:
        self._paths = ensure_tiktok_layout()

    def load_all(self) -> list[dict[str, Any]]:
        return read_json_list(self._paths["jobs"])

    def save_all(self, rows: list[dict[str, Any]]) -> None:
        write_json_resilient(self._paths["jobs"], rows, tmp_prefix="tt_job_")

    def get_by_id(self, job_id: str) -> dict[str, Any] | None:
        jid = str(job_id or "").strip()
        for r in self.load_all():
            if str(r.get("id", "")).strip() == jid:
                return dict(r)
        return None

    def upsert(self, row: dict[str, Any]) -> None:
        rows = self.load_all()
        rid = str(row.get("id", "")).strip()
        if not rid:
            row = dict(row)
            row["id"] = f"tt_job_{uuid.uuid4().hex[:10]}"
            rid = row["id"]
        found = False
        out: list[dict[str, Any]] = []
        for r in rows:
            if str(r.get("id", "")).strip() == rid:
                out.append(dict(row))
                found = True
            else:
                out.append(dict(r))
        if not found:
            out.append(dict(row))
        self.save_all(out)

    def upsert_many(self, rows: Iterable[dict[str, Any]]) -> int:
        """Upsert nhiều job TikTok một lần đọc/ghi file."""
        incoming: list[dict[str, Any]] = []
        for row in rows:
            r = dict(row)
            if not str(r.get("id", "")).strip():
                r["id"] = f"tt_job_{uuid.uuid4().hex[:10]}"
            incoming.append(r)
        if not incoming:
            return 0
        by_id = {str(r["id"]).strip(): r for r in incoming}
        cur = self.load_all()
        out: list[dict[str, Any]] = []
        applied: set[str] = set()
        for r in cur:
            rid = str(r.get("id", "")).strip()
            if rid in by_id:
                out.append(by_id[rid])
                applied.add(rid)
            else:
                out.append(dict(r))
        for r in incoming:
            rid = str(r.get("id", "")).strip()
            if rid not in applied:
                out.append(r)
        self.save_all(out)
        return len(incoming)

    def delete(self, job_id: str) -> bool:
        jid = str(job_id or "").strip()
        rows = self.load_all()
        new_rows = [r for r in rows if str(r.get("id", "")).strip() != jid]
        if len(new_rows) == len(rows):
            return False
        self.save_all(new_rows)
        return True
