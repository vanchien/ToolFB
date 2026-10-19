"""
Cài đặt runtime của app: mở cùng Windows, nhớ lịch đang bật để khôi phục sau reboot.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loguru import logger

from src.utils.paths import project_root

_PREFS_NAME = "app_runtime.json"
_DEFAULTS: dict[str, Any] = {
    # Tool chạy 24/7: mặc định mở cùng Windows; tắt bằng tick trên thanh Lịch.
    "open_on_windows_startup": True,
    "scheduler_wanted": None,
    # Song song với watch.enabled trong share_settings — dự phòng khi file settings lệch.
    "share_watch_wanted": None,
}


def runtime_prefs_path(*, root: Path | None = None) -> Path:
    """Đường dẫn ``config/app_runtime.json``."""
    return (root or project_root()) / "config" / _PREFS_NAME


def load_runtime_prefs(*, root: Path | None = None) -> dict[str, Any]:
    """Đọc prefs; thiếu file thì trả mặc định."""
    path = runtime_prefs_path(root=root)
    data = dict(_DEFAULTS)
    if not path.is_file():
        return data
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không đọc được {}: {}", path, exc)
        return data
    if not isinstance(raw, dict):
        return data
    data.update(raw)
    return data


def save_runtime_prefs(patch: dict[str, Any], *, root: Path | None = None) -> dict[str, Any]:
    """Ghi đè từng khóa prefs; tạo file nếu chưa có."""
    path = runtime_prefs_path(root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    current = load_runtime_prefs(root=root)
    for key, value in (patch or {}).items():
        current[key] = value
    try:
        path.write_text(json.dumps(current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không ghi được {}: {}", path, exc)
    return current


def set_scheduler_wanted(wanted: bool, *, root: Path | None = None) -> None:
    """Nhớ người dùng vừa Bắt đầu / Dừng lịch — dùng để tự bật sau khi mở lại máy."""
    save_runtime_prefs({"scheduler_wanted": bool(wanted)}, root=root)


def set_share_watch_wanted(wanted: bool, *, root: Path | None = None) -> None:
    """Nhớ theo dõi chia sẻ đang bật — tự khôi phục sau reboot."""
    save_runtime_prefs({"share_watch_wanted": bool(wanted)}, root=root)


def should_auto_start_share_watch(prefs: dict[str, Any] | None = None) -> bool | None:
    """
    Gợi ý bật theo dõi chia sẻ khi mở app.

    ``True``/``False`` theo lần trước; ``None`` nếu chưa nhớ (dùng share_settings.watch.enabled).
    """
    data = prefs if isinstance(prefs, dict) else load_runtime_prefs()
    wanted = data.get("share_watch_wanted")
    if wanted is True:
        return True
    if wanted is False:
        return False
    return None


def set_open_on_windows_startup(enabled: bool, *, root: Path | None = None) -> dict[str, Any]:
    """Bật/tắt mở app cùng Windows (prefs + đăng ký registry nếu được gọi kèm sync)."""
    return save_runtime_prefs({"open_on_windows_startup": bool(enabled)}, root=root)


def has_actionable_schedule_jobs() -> bool:
    """Còn job pending / ready_queue / running cần lịch chạy tiếp."""
    try:
        from src.utils.schedule_posts_manager import get_default_schedule_posts_manager

        rows = get_default_schedule_posts_manager().load_all()
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không đọc schedule_posts để quyết định tự bật lịch: {}", exc)
        return False
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        st = str(row.get("status") or "").strip().lower()
        if st in {"pending", "ready_queue", "running"}:
            return True
    return False


def fleet_wants_scheduler() -> bool:
    """AI Fleet còn Page ACTIVE và không dừng khẩn cấp."""
    try:
        from src.services.ai_fleet.store import FleetStore

        doc = FleetStore().load()
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không đọc AI Fleet để tự bật lịch: {}", exc)
        return False
    if doc.get("emergency_stop"):
        return False
    return any(
        str(profile.get("status") or "").upper() == "ACTIVE"
        for profile in (doc.get("profiles") or {}).values()
        if isinstance(profile, dict)
    )


def should_auto_start_scheduler(prefs: dict[str, Any] | None = None) -> bool:
    """
    Có nên tự Bắt đầu lịch khi mở app (sau reboot / mở lại) hay không.

    - ``scheduler_wanted=True``: lần trước đang chạy → bật lại.
    - ``scheduler_wanted=False``: người dùng đã Dừng → không tự bật.
    - Chưa nhớ: bật nếu còn job lịch hoặc Fleet ACTIVE.
    """
    data = prefs if isinstance(prefs, dict) else load_runtime_prefs()
    wanted = data.get("scheduler_wanted")
    if wanted is True:
        return True
    if wanted is False:
        return False
    return has_actionable_schedule_jobs() or fleet_wants_scheduler()
