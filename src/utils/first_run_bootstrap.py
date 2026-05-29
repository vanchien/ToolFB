"""
Bootstrap lần đầu sau clone GitHub — tạo config/data mẫu, không ghi đè file user.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from loguru import logger

from src.utils.paths import project_root

_CONFIG_EXAMPLES: tuple[tuple[str, str], ...] = (
    ("accounts.example.json", "accounts.json"),
    ("pages.example.json", "pages.json"),
    ("schedule_posts.example.json", "schedule_posts.json"),
    ("app_secrets.example.json", "app_secrets.json"),
    ("update_channel.example.json", "update_channel.json"),
    ("account_credentials.example.json", "account_credentials.json"),
)

_RUNTIME_DIRS = (
    "data",
    "data/cookies",
    "data/profiles",
    "data/profiles/firefox",
    "data/profiles/chromium",
    "data/runtime",
    "data/drafts",
    "data/video_editor",
    "logs",
    "logs/screenshots",
)


def _copy_example_if_missing(cfg: Path, example_name: str, target_name: str) -> bool:
    ex = cfg / example_name
    tgt = cfg / target_name
    if tgt.is_file():
        return False
    if ex.is_file():
        shutil.copy2(ex, tgt)
        logger.info("Bootstrap: đã tạo {} từ {}", tgt.name, example_name)
        return True
    if target_name.endswith(".json"):
        tgt.write_text("[]\n" if "accounts" in target_name or "pages" in target_name or "schedule" in target_name else "{}\n", encoding="utf-8")
        logger.info("Bootstrap: đã tạo {} (mặc định rỗng)", tgt.name)
        return True
    return False


def ensure_runtime_directories(*, root: Path | None = None) -> None:
    base = root or project_root()
    for rel in _RUNTIME_DIRS:
        (base / rel).mkdir(parents=True, exist_ok=True)


def bootstrap_config_files(*, root: Path | None = None) -> list[str]:
    """
    Tạo file config thiếu từ ``*.example.json``.

    Returns:
        Danh sách tên file vừa tạo.
    """
    base = root or project_root()
    cfg = base / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    created: list[str] = []
    for ex_name, tgt_name in _CONFIG_EXAMPLES:
        if _copy_example_if_missing(cfg, ex_name, tgt_name):
            created.append(tgt_name)
    return created


def bootstrap_all(*, root: Path | None = None) -> dict[str, Any]:
    """Chạy toàn bộ bootstrap — gọi từ ``main.py`` trước GUI."""
    base = root or project_root()
    ensure_runtime_directories(root=base)
    created = bootstrap_config_files(root=base)
    return {"created_config": created, "root": str(base)}


def setup_status(*, root: Path | None = None) -> dict[str, Any]:
    """Trạng thái thiết lập cho banner GUI."""
    base = root or project_root()
    cfg = base / "config"
    accounts_path = cfg / "accounts.json"
    secrets_path = cfg / "app_secrets.json"
    n_accounts = 0
    if accounts_path.is_file():
        try:
            raw = json.loads(accounts_path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                n_accounts = len(raw)
        except Exception:
            n_accounts = -1
    has_secrets = secrets_path.is_file() and secrets_path.stat().st_size > 10
    return {
        "n_accounts": n_accounts,
        "has_secrets": has_secrets,
        "needs_account": n_accounts <= 0,
        "needs_secrets": not has_secrets,
        "ready": n_accounts > 0,
    }
