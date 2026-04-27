from __future__ import annotations

import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from src.utils.paths import project_root
from src.utils.profile_cleanup import iter_profile_leaf_dirs, profiles_data_dir


@dataclass
class CleanupStats:
    """Thống kê nhanh sau khi dọn dẹp startup."""

    removed_files: int = 0
    removed_dirs: int = 0
    freed_bytes: int = 0

    def add_file(self, size: int) -> None:
        self.removed_files += 1
        self.freed_bytes += max(0, int(size))

    def add_dir(self) -> None:
        self.removed_dirs += 1


def _safe_unlink(path: Path, stats: CleanupStats) -> None:
    try:
        sz = path.stat().st_size if path.exists() else 0
    except Exception:
        sz = 0
    try:
        path.unlink(missing_ok=True)
        stats.add_file(sz)
    except Exception:
        return


def _safe_rmdir(path: Path, stats: CleanupStats) -> None:
    try:
        path.rmdir()
        stats.add_dir()
    except Exception:
        return


def _cleanup_pycache(root: Path, stats: CleanupStats, *, older_than_hours: int = 24) -> None:
    cutoff = time.time() - max(1, older_than_hours) * 3600
    for p in root.glob("**/__pycache__"):
        if ".venv" in p.parts or "dist" in p.parts or "build" in p.parts:
            continue
        try:
            files = list(p.glob("*.pyc"))
        except Exception:
            continue
        stale = True
        for f in files:
            try:
                if f.stat().st_mtime >= cutoff:
                    stale = False
                    break
            except Exception:
                stale = False
                break
        if not stale:
            continue
        for f in files:
            _safe_unlink(f, stats)
        _safe_rmdir(p, stats)


def _cleanup_old_files_in_dir(
    folder: Path,
    *,
    max_age_days: int,
    keep_latest: int,
    patterns: tuple[str, ...],
    stats: CleanupStats,
) -> None:
    if not folder.is_dir():
        return
    files: list[Path] = []
    for pat in patterns:
        files.extend(folder.glob(pat))
    files = [f for f in files if f.is_file()]
    if not files:
        return
    files.sort(key=lambda x: x.stat().st_mtime if x.exists() else 0, reverse=True)
    keep = set(files[: max(0, keep_latest)])
    cutoff = time.time() - max(1, max_age_days) * 86400
    for f in files:
        if f in keep:
            continue
        try:
            if f.stat().st_mtime > cutoff:
                continue
        except Exception:
            continue
        _safe_unlink(f, stats)


def _cleanup_updater_artifacts(root: Path, stats: CleanupStats) -> None:
    updates = root / "data" / "updates"
    _cleanup_old_files_in_dir(
        updates,
        max_age_days=10,
        keep_latest=2,
        patterns=("update_*.zip",),
        stats=stats,
    )
    if not updates.is_dir():
        return
    backup_dirs = [p for p in updates.glob("backup_before_*") if p.is_dir()]
    backup_dirs.sort(key=lambda x: x.stat().st_mtime if x.exists() else 0, reverse=True)
    keep = set(backup_dirs[:2])
    cutoff = time.time() - 14 * 86400
    for d in backup_dirs:
        if d in keep:
            continue
        try:
            if d.stat().st_mtime > cutoff:
                continue
        except Exception:
            continue
        for sub in sorted(d.rglob("*"), reverse=True):
            if sub.is_file():
                _safe_unlink(sub, stats)
            elif sub.is_dir():
                _safe_rmdir(sub, stats)
        _safe_rmdir(d, stats)


def _cleanup_screenshots(root: Path, stats: CleanupStats) -> None:
    shots = root / "logs" / "screenshots"
    _cleanup_old_files_in_dir(
        shots,
        max_age_days=7,
        keep_latest=300,
        patterns=("*.png", "*.jpg", "*.jpeg", "*.webp"),
        stats=stats,
    )


def _cleanup_ai_video_artifacts(root: Path, stats: CleanupStats) -> None:
    """
    Dọn file phát sinh lớn nhưng có thể tái tạo lại trong AI video.
    Không đụng vào output video thật để tránh mất dữ liệu người dùng.
    """
    ai_root = root / "data" / "ai_video"
    thumbs = ai_root / "thumbnails" / "grid_cards"
    prompt_bundles = ai_root / "inputs" / "prompts"
    _cleanup_old_files_in_dir(
        thumbs,
        max_age_days=5,
        keep_latest=300,
        patterns=("*.jpg", "*.jpeg", "*.png", "*.webp"),
        stats=stats,
    )
    _cleanup_old_files_in_dir(
        prompt_bundles,
        max_age_days=14,
        keep_latest=40,
        patterns=("prompt_bundle_*.json", "*.json"),
        stats=stats,
    )


def _cleanup_backups_dir(root: Path, stats: CleanupStats) -> None:
    """
    Dọn backup reset profile cũ để giảm phình đĩa.
    Giữ lại vài bản mới nhất để vẫn có khả năng rollback khi cần.
    """
    d = root / "data" / "backups"
    if not d.is_dir():
        return
    dirs = [p for p in d.iterdir() if p.is_dir()]
    if not dirs:
        return
    dirs.sort(key=lambda x: x.stat().st_mtime if x.exists() else 0, reverse=True)
    # Aggressive mode: chỉ giữ đúng 1 bản backup mới nhất để giảm dung lượng tối đa.
    keep = set(dirs[:1])
    for old in dirs:
        if old in keep:
            continue
        for sub in sorted(old.rglob("*"), reverse=True):
            if sub.is_file():
                _safe_unlink(sub, stats)
            elif sub.is_dir():
                _safe_rmdir(sub, stats)
        _safe_rmdir(old, stats)


def _cleanup_browser_profile_caches(root: Path, stats: CleanupStats) -> None:
    """
    Dọn cache trong profile browser persistent (NanoBanana/Flow).
    Chỉ dọn thư mục cache tái tạo được, không đụng cookie/login state.
    """
    profile = root / "data" / "nanobanana" / "browser_profile"
    if not profile.is_dir():
        return
    cache_dirs = [
        profile / "Default" / "Cache",
        profile / "Default" / "Code Cache",
        profile / "Default" / "GPUCache",
        profile / "Default" / "DawnCache",
        profile / "Default" / "GrShaderCache",
        profile / "ShaderCache",
    ]
    for cd in cache_dirs:
        if not cd.is_dir():
            continue
        for sub in sorted(cd.rglob("*"), reverse=True):
            if sub.is_file():
                _safe_unlink(sub, stats)
            elif sub.is_dir():
                _safe_rmdir(sub, stats)
        _safe_rmdir(cd, stats)


def _cleanup_profiles_cache_dirs(root: Path, stats: CleanupStats) -> None:
    """
    Dọn cache nặng trong data/profiles/* để giảm dung lượng lớn.
    Chỉ xóa thư mục cache tái tạo được, không xóa cookie/login state.
    """
    profiles_root = profiles_data_dir(root)
    if not profiles_root.is_dir():
        return
    cache_rel_paths = (
        Path("Default") / "Cache",
        Path("Default") / "Code Cache",
        Path("Default") / "GPUCache",
        Path("Default") / "DawnCache",
        Path("Default") / "GrShaderCache",
        Path("Default") / "Service Worker" / "CacheStorage",
        Path("ShaderCache"),
    )
    for leaf in iter_profile_leaf_dirs(profiles_root):
        for rel in cache_rel_paths:
            cd = leaf / rel
            if not cd.is_dir():
                continue
            for sub in sorted(cd.rglob("*"), reverse=True):
                if sub.is_file():
                    _safe_unlink(sub, stats)
                elif sub.is_dir():
                    _safe_rmdir(sub, stats)
            _safe_rmdir(cd, stats)


def _cleanup_profiles_redundant_logs(root: Path, stats: CleanupStats) -> None:
    """
    Dọn file log/crash tái tạo được trong profile đang dùng.
    Không xóa DB/cookie/session chính.
    """
    profiles_root = profiles_data_dir(root)
    if not profiles_root.is_dir():
        return
    removable_dirs = (
        Path("Crashpad"),
        Path("BrowserMetrics"),
        Path("Default") / "BrowserMetrics",
    )
    removable_file_patterns = (
        "Default/**/LOG",
        "Default/**/LOG.old",
        "Default/**/*.log",
        "Default/**/*.tmp",
    )
    for leaf in iter_profile_leaf_dirs(profiles_root):
        for rel in removable_dirs:
            d = leaf / rel
            if not d.is_dir():
                continue
            for sub in sorted(d.rglob("*"), reverse=True):
                if sub.is_file():
                    _safe_unlink(sub, stats)
                elif sub.is_dir():
                    _safe_rmdir(sub, stats)
            _safe_rmdir(d, stats)
        for pat in removable_file_patterns:
            for f in leaf.glob(pat):
                if not f.is_file():
                    continue
                _safe_unlink(f, stats)


def _cleanup_temp_json(stats: CleanupStats) -> None:
    tmp = Path(tempfile.gettempdir())
    if not tmp.is_dir():
        return
    prefixes = (
        "schedule_",
        "schedule_posts_",
        "google_flow_jobs_",
        "queue_",
        "jsonlist_",
        "entities_",
        "pages_",
        "hist_",
        "app_secrets_",
        "toolfb_",
    )
    cutoff = time.time() - 2 * 86400
    for p in tmp.glob("*.tmp.json"):
        name = p.name.lower()
        if not any(name.startswith(pre) for pre in prefixes):
            continue
        try:
            if p.stat().st_mtime > cutoff:
                continue
        except Exception:
            continue
        _safe_unlink(p, stats)


def cleanup_runtime_junk() -> CleanupStats:
    """
    Dọn rác nhẹ lúc startup để giảm nặng đĩa/RAM I/O cache:
    - ``__pycache__`` cũ trong project (trừ ``.venv/dist/build``)
    - update zip/backup cũ trong ``data/updates``
    - backup profile cũ trong ``data/backups``
    - cache profile browser trong ``data/nanobanana/browser_profile``
    - cache nặng trong ``data/profiles/*`` (Cache/GPUCache/Code Cache...)
    - log/crash tái tạo được trong ``data/profiles/*``
    - thumbnail/prompt-bundle cũ trong ``data/ai_video``
    - screenshot lỗi cũ trong ``logs/screenshots``
    - file ``*.tmp.json`` cũ trong thư mục temp hệ thống.
    """
    root = project_root()
    stats = CleanupStats()
    try:
        _cleanup_pycache(root, stats)
        _cleanup_updater_artifacts(root, stats)
        _cleanup_backups_dir(root, stats)
        _cleanup_browser_profile_caches(root, stats)
        _cleanup_profiles_cache_dirs(root, stats)
        _cleanup_profiles_redundant_logs(root, stats)
        _cleanup_ai_video_artifacts(root, stats)
        _cleanup_screenshots(root, stats)
        _cleanup_temp_json(stats)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Runtime cleanup bị lỗi (bỏ qua): {}", exc)
    logger.info(
        "Runtime cleanup: xóa {} file, {} thư mục, giải phóng ~{:.1f} MB.",
        stats.removed_files,
        stats.removed_dirs,
        stats.freed_bytes / (1024 * 1024),
    )
    return stats

