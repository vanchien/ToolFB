"""
Lưu cài đặt tab Tương tác người dùng vào config cục bộ.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from src.utils.paths import project_root

_SETTINGS_NAME = "human_interaction_settings.json"


def human_interaction_settings_path() -> Path:
    return project_root() / "config" / _SETTINGS_NAME


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    fd, tmp = tempfile.mkstemp(prefix="human_interaction_", suffix=".tmp.json", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def load_human_interaction_settings() -> dict[str, Any]:
    p = human_interaction_settings_path()
    if not p.is_file():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def save_human_interaction_settings(data: dict[str, Any]) -> None:
    p = human_interaction_settings_path()
    safe = data if isinstance(data, dict) else {}
    _atomic_write_json(p, safe)


def _valid_mapped_dicts(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict) and str(item.get("account_id") or item.get("id") or "").strip():
            out.append(item)
    return out


def load_mapped_accounts_from_settings(settings: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Đọc snapshot danh sách đã ghép (legacy — gộp login + tương tác)."""
    if not isinstance(settings, dict):
        return []
    login = _valid_mapped_dicts(settings.get("mapped_accounts_login"))
    interaction = _valid_mapped_dicts(settings.get("mapped_accounts_interaction"))
    if login or interaction:
        return login + interaction
    return _valid_mapped_dicts(settings.get("mapped_accounts"))


def load_login_queue_from_settings(settings: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Hàng đợi đăng nhập — chưa ``login_ok``."""
    if not isinstance(settings, dict):
        return []
    login = _valid_mapped_dicts(settings.get("mapped_accounts_login"))
    if login:
        return login
    legacy = _valid_mapped_dicts(settings.get("mapped_accounts"))
    return [x for x in legacy if str(x.get("status") or "") not in ("login_ok", "success")]


def load_interaction_queue_from_settings(settings: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Hàng đợi tương tác — đã đăng nhập thành công."""
    if not isinstance(settings, dict):
        return []
    interaction = _valid_mapped_dicts(settings.get("mapped_accounts_interaction"))
    if interaction:
        return interaction
    legacy = _valid_mapped_dicts(settings.get("mapped_accounts"))
    return [x for x in legacy if str(x.get("status") or "") in ("login_ok", "success")]

