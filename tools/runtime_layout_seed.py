"""Tạo thư mục runtime + JSON config mặc định (dùng chung cho portable_clean và bản EXE)."""

from __future__ import annotations

import json
from pathlib import Path


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def seed_default_runtime_at(dest_root: Path) -> None:
    """
    Đảm bảo ``config/*.json`` tối thiểu + ``data/`` + ``logs/`` để app khởi động được trên máy sạch.

    Args:
        dest_root: Thư mục gốc triển khai (thư mục portable hoặc thư mục chứa ``ToolFB_GUI.exe``).
    """
    (dest_root / "logs" / "screenshots").mkdir(parents=True, exist_ok=True)
    (dest_root / "data" / "cookies").mkdir(parents=True, exist_ok=True)
    (dest_root / "data" / "profiles").mkdir(parents=True, exist_ok=True)
    (dest_root / "data" / "drafts").mkdir(parents=True, exist_ok=True)
    (dest_root / "data" / "runtime").mkdir(parents=True, exist_ok=True)
    (dest_root / "data" / "pages").mkdir(parents=True, exist_ok=True)
    (dest_root / "data" / "nanobanana").mkdir(parents=True, exist_ok=True)
    (dest_root / "data" / "media_library").mkdir(parents=True, exist_ok=True)

    _write_json(dest_root / "config" / "accounts.json", [])
    _write_json(dest_root / "config" / "pages.json", [])
    _write_json(dest_root / "config" / "schedule_posts.json", [])
    _write_json(dest_root / "config" / "entities.json", [])
    _write_json(dest_root / "config" / "schedule.json", {})

    app_secrets = dest_root / "config" / "app_secrets.json"
    if app_secrets.exists():
        app_secrets.unlink()
