from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from src.services.tiktok.json_io import read_json_list, write_json_resilient
from src.services.tiktok.layout import ensure_tiktok_layout, resolve_tiktok_profile_dir


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def default_account_dict(
    *,
    name: str,
    username: str = "",
    browser_type: str = "chrome",
    browser_exe_path: str = "",
    profile_path: str = "",
) -> dict[str, Any]:
    aid = f"tt_acc_{uuid.uuid4().hex[:10]}"
    return {
        "id": aid,
        "name": str(name).strip() or aid,
        "username": str(username).strip(),
        "browser_type": str(browser_type or "chrome").strip().lower(),
        "browser_exe_path": str(browser_exe_path).strip(),
        "profile_path": str(profile_path).strip(),
        "proxy": {
            "enabled": False,
            "server": "",
            "username": "",
            "password": "",
        },
        "status": "active",
        "last_check": "",
        "notes": "",
    }


class TikTokAccountStore:
    def __init__(self) -> None:
        self._paths = ensure_tiktok_layout()

    @property
    def path(self) -> Any:
        return self._paths["accounts"]

    def load_all(self) -> list[dict[str, Any]]:
        return read_json_list(self._paths["accounts"])

    def save_all(self, rows: list[dict[str, Any]]) -> None:
        write_json_resilient(self._paths["accounts"], rows, tmp_prefix="tt_acc_")

    def get_by_id(self, account_id: str) -> dict[str, Any] | None:
        aid = str(account_id or "").strip()
        for r in self.load_all():
            if str(r.get("id", "")).strip() == aid:
                return dict(r)
        return None

    def upsert(self, row: dict[str, Any]) -> None:
        rows = self.load_all()
        rid = str(row.get("id", "")).strip()
        if not rid:
            row = dict(row)
            row["id"] = f"tt_acc_{uuid.uuid4().hex[:10]}"
            rid = row["id"]
        # Chuẩn hóa profile runtime: mỗi account một profile nội bộ.
        row = dict(row)
        row["profile_path"] = str(resolve_tiktok_profile_dir({"id": rid, "profile_path": row.get("profile_path", "")}))
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

    def delete(self, account_id: str) -> bool:
        aid = str(account_id or "").strip()
        rows = self.load_all()
        new_rows = [r for r in rows if str(r.get("id", "")).strip() != aid]
        if len(new_rows) == len(rows):
            return False
        self.save_all(new_rows)
        return True

    def patch(self, account_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        acc = self.get_by_id(account_id)
        if acc is None:
            raise ValueError(f"Không tìm thấy TikTok account: {account_id}")
        acc.update(patch)
        if "last_check" in patch:
            acc["last_check"] = str(patch.get("last_check") or _now_iso())
        self.upsert(acc)
        return acc
