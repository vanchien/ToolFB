"""
Dọn profile mồ côi dưới ``data/profiles/`` + dữ liệu đi kèm (cookie) dưới ``data/cookies/``.

Khi xóa một thư mục profile không còn trong ``accounts.json``, đồng thời xóa file cookie
chuẩn ``data/cookies/<tên_thư_mục_profile>.json`` nếu file đó **không** còn là ``cookie_path``
của bất kỳ tài khoản nào.

Không xóa cookie tùy tên khác tên thư mục profile (tránh nhầm file dùng chung).
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, Iterable, Optional

from loguru import logger

_BROWSER_SUBDIRS = frozenset(
    name.lower() for name in ("chromium", "chrome", "firefox", "webkit", "edge", "msedge")
)


def profiles_data_dir(project_root: Path | None = None) -> Path:
    """``<project>/data/profiles``."""
    if project_root is None:
        from src.utils.paths import project_root as pr

        project_root = pr()
    return (Path(project_root) / "data" / "profiles").resolve()


def cookies_data_dir(project_root: Path) -> Path:
    """``<project>/data/cookies``."""
    return (Path(project_root).resolve() / "data" / "cookies").resolve()


def _resolve_account_path(project_root: Path, raw: str) -> Optional[Path]:
    s = str(raw or "").strip()
    if not s:
        return None
    p = Path(s)
    if not p.is_absolute():
        p = project_root / p
    try:
        out = p.resolve()
    except OSError:
        return None
    return out if out.is_dir() else None


def collect_referenced_profile_paths(project_root: Path, accounts: Iterable[dict[str, Any]]) -> set[Path]:
    """Mọi thư mục ``portable_path`` / ``profile_path`` hợp lệ trên đĩa."""
    refs: set[Path] = set()
    root = project_root.resolve()
    for acc in accounts:
        for key in ("portable_path", "profile_path"):
            r = _resolve_account_path(root, str(acc.get(key) or ""))
            if r is not None:
                refs.add(r)
    return refs


def _resolve_cookie_file(project_root: Path, raw: str) -> Optional[Path]:
    s = str(raw or "").strip()
    if not s:
        return None
    p = Path(s)
    if not p.is_absolute():
        p = project_root / p
    try:
        out = p.resolve()
    except OSError:
        return None
    return out if out.is_file() else None


def collect_referenced_cookie_paths(project_root: Path, accounts: Iterable[dict[str, Any]]) -> set[Path]:
    """Mọi file ``cookie_path`` đang trỏ tới (chỉ khi file tồn tại)."""
    refs: set[Path] = set()
    root = project_root.resolve()
    for acc in accounts:
        r = _resolve_cookie_file(root, str(acc.get("cookie_path") or ""))
        if r is not None:
            refs.add(r)
    return refs


def _try_delete_orphan_cookie_for_stem(
    *,
    project_root: Path,
    profile_stem: str,
    referenced_cookies: set[Path],
    dry_run: bool,
    deleted_log: list[str],
) -> None:
    """Xóa ``data/cookies/<stem>.json`` nếu không còn tài khoản nào trỏ tới."""
    ck_root = cookies_data_dir(project_root)
    if not ck_root.is_dir() or not profile_stem.strip():
        return
    candidate = (ck_root / f"{profile_stem}.json").resolve()
    if not candidate.is_file():
        return
    if not _is_strict_child(ck_root, candidate):
        return
    if candidate in referenced_cookies:
        return
    if dry_run:
        deleted_log.append(str(candidate))
        return
    try:
        candidate.unlink()
        deleted_log.append(str(candidate))
        logger.info("Đã xóa cookie mồ côi (theo profile đã xóa): {}", candidate)
    except OSError as exc:
        logger.warning("Không xóa được cookie {}: {}", candidate, exc)


def iter_profile_leaf_dirs(profiles_root: Path) -> list[Path]:
    """
    Liệt kê thư mục profile dự kiến:

    - ``profiles/<browser>/<id>`` nếu ``<browser>`` là tên engine chuẩn;
    - ngược lại coi ``profiles/<name>`` là một profile (layout cũ).
    """
    root = profiles_root.resolve()
    if not root.is_dir():
        return []
    out: list[Path] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        if child.name.lower() in _BROWSER_SUBDIRS:
            for sub in child.iterdir():
                if sub.is_dir():
                    out.append(sub.resolve())
        else:
            out.append(child.resolve())
    return out


def _is_strict_child(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def cleanup_orphan_profile_directories(
    accounts: Iterable[dict[str, Any]],
    *,
    project_root: Path | None = None,
    dry_run: bool = False,
) -> list[str]:
    """
    Xóa thư mục con trong ``data/profiles`` không khớp ``portable_path`` / ``profile_path`` của bất kỳ tài khoản nào.

    Nếu danh sách tài khoản **rỗng**, không xóa gì (tránh mất dữ liệu khi file JSON lỗi/rỗng nhầm).

    Ghi đè bằng biến môi trường ``DISABLE_PROFILE_CLEANUP=1`` để tắt hoàn toàn.

    Returns:
        Danh sách đường dẫn đã xóa (hoặc sẽ xóa nếu ``dry_run``).
    """
    if os.environ.get("DISABLE_PROFILE_CLEANUP", "").strip().lower() in {"1", "true", "yes", "on"}:
        logger.info("Bỏ qua dọn profile mồ côi (DISABLE_PROFILE_CLEANUP).")
        return []

    from src.utils.paths import project_root as pr

    proot = Path(project_root).resolve() if project_root is not None else pr().resolve()
    rows = list(accounts)
    if not rows:
        logger.info("Bỏ qua dọn profile mồ côi: chưa có tài khoản trong accounts (tránh xóa nhầm).")
        return []

    profiles_root = profiles_data_dir(proot)
    referenced_profiles = collect_referenced_profile_paths(proot, rows)
    referenced_cookies = collect_referenced_cookie_paths(proot, rows)
    candidates = iter_profile_leaf_dirs(profiles_root)
    deleted: list[str] = []

    for folder in candidates:
        if folder in referenced_profiles:
            continue
        if not _is_strict_child(profiles_root, folder):
            logger.warning("Bỏ qua thư mục không nằm dưới data/profiles: {}", folder)
            continue
        stem = folder.name
        if dry_run:
            deleted.append(str(folder))
            _try_delete_orphan_cookie_for_stem(
                project_root=proot,
                profile_stem=stem,
                referenced_cookies=referenced_cookies,
                dry_run=True,
                deleted_log=deleted,
            )
            continue
        try:
            shutil.rmtree(folder, ignore_errors=False)
            deleted.append(str(folder))
            logger.info("Đã xóa profile mồ côi: {}", folder)
        except OSError as exc:
            logger.warning("Không xóa được profile {}: {}", folder, exc)
            continue
        _try_delete_orphan_cookie_for_stem(
            project_root=proot,
            profile_stem=stem,
            referenced_cookies=referenced_cookies,
            dry_run=False,
            deleted_log=deleted,
        )

    if deleted and not dry_run:
        logger.info("Dọn profile mồ côi — đã xóa {} mục (thư mục + cookie).", len(deleted))
    elif deleted and dry_run:
        logger.debug("dry_run: sẽ xóa {} mục (profile + cookie).", len(deleted))
    return deleted
