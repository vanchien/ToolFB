from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from src.utils.paths import project_root
from src.utils.safe_delete import safe_delete_path


def tiktok_data_root() -> Path:
    return project_root() / "data" / "tiktok"


def ensure_tiktok_layout() -> dict[str, Path]:
    root = tiktok_data_root()
    paths = {
        "root": root,
        "accounts": root / "accounts.json",
        "jobs": root / "upload_jobs.json",
        "logs": root / "logs",
        "screenshots": root / "logs" / "screenshots",
        "reports": root / "reports",
        "profiles": root / "profiles",
    }
    for key, p in paths.items():
        if key in ("accounts", "jobs"):
            p.parent.mkdir(parents=True, exist_ok=True)
            continue
        if p.suffix:
            p.parent.mkdir(parents=True, exist_ok=True)
        else:
            p.mkdir(parents=True, exist_ok=True)
    if not paths["accounts"].is_file():
        paths["accounts"].write_text("[]\n", encoding="utf-8")
    if not paths["jobs"].is_file():
        paths["jobs"].write_text("[]\n", encoding="utf-8")
    return paths


def resolve_tiktok_profile_dir(account: dict[str, Any]) -> Path:
    """
    Trả về profile TikTok do ToolFB quản lý nội bộ (không dùng profile người dùng OS).
    """
    paths = ensure_tiktok_layout()
    prof_root = paths["profiles"]
    aid = str(account.get("id", "")).strip() or "default"
    safe = "".join(ch if (ch.isalnum() or ch in ("-", "_")) else "_" for ch in aid)[:64] or "default"
    prof = (prof_root / safe).resolve()
    prof.mkdir(parents=True, exist_ok=True)
    _migrate_legacy_profile_once(account, prof)
    return prof


def delete_tiktok_profile_dir_by_account_id(account_id: str) -> bool:
    """
    Xóa sạch profile nội bộ của account, có chặn vượt thư mục + retry ngắn để giảm sót file lock.
    """
    aid = str(account_id or "").strip() or "default"
    prof = resolve_tiktok_profile_dir({"id": aid})
    profiles_root = ensure_tiktok_layout()["profiles"].resolve()
    try:
        prof.relative_to(profiles_root)
    except ValueError:
        # Chặn xóa nhầm ngoài data/tiktok/profiles.
        return False
    # Dọn lock file phổ biến trước khi xóa thư mục.
    for lock_name in ("parent.lock", "lock", ".parentlock", "SingletonLock"):
        try:
            (prof / lock_name).unlink(missing_ok=True)
        except OSError:
            pass
    return safe_delete_path(
        prof,
        allowed_roots=[profiles_root],
        kind="dir",
        missing_ok=True,
        retries=2,
        retry_sleep_sec=0.15,
    )


def _migrate_legacy_profile_once(account: dict[str, Any], target_dir: Path) -> None:
    """
    Copy profile_path cũ sang profile nội bộ đúng 1 lần (nếu target đang trống).
    """
    marker = target_dir / ".migrated_from_legacy"
    if marker.exists():
        return
    legacy_raw = str(account.get("profile_path", "")).strip()
    if not legacy_raw:
        return
    legacy = Path(legacy_raw).expanduser()
    if not legacy.is_absolute():
        legacy = (project_root() / legacy).resolve()
    else:
        legacy = legacy.resolve()
    if not legacy.is_dir():
        return
    if legacy == target_dir:
        return
    try:
        target_has_data = any(target_dir.iterdir())
    except OSError:
        target_has_data = True
    if target_has_data:
        return
    try:
        for child in legacy.iterdir():
            dst = target_dir / child.name
            if child.is_dir():
                shutil.copytree(child, dst, dirs_exist_ok=True)
            elif child.is_file():
                shutil.copy2(child, dst)
        marker.write_text(str(legacy), encoding="utf-8")
    except OSError:
        # Không chặn luồng chạy nếu migrate lỗi; app vẫn dùng profile nội bộ mới.
        return
