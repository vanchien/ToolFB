"""
Dọn profile mồ côi dưới ``data/profiles/`` + dữ liệu đi kèm (cookie) dưới ``data/cookies/``.

Khi xóa một thư mục profile không còn trong ``accounts.json``, đồng thời xóa file cookie
chuẩn ``data/cookies/<tên_thư_mục_profile>.json`` nếu file đó **không** còn là ``cookie_path``
của bất kỳ tài khoản nào.

Không xóa cookie tùy tên khác tên thư mục profile (tránh nhầm file dùng chung).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable, Optional

from loguru import logger
from src.utils.safe_delete import safe_delete_path

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
    """
    Mọi thư mục profile gắn với tài khoản — không chỉ ``portable_path`` trong JSON.

    Bổ sung alias UID_/acc_/facebook_uid và mọi ứng viên ``iter_profile_dirs_for_account``
    để startup không xóa nhầm profile có lịch sử khi tab dùng ``UID_…`` nhưng registry là ``acc_…``.
    """
    refs: set[Path] = set()
    root = project_root.resolve()
    rows = list(accounts)
    for acc in rows:
        for key in ("portable_path", "profile_path"):
            r = _resolve_account_path(root, str(acc.get(key) or ""))
            if r is not None:
                refs.add(r)
        try:
            from src.utils.account_browser_profile import iter_profile_dirs_for_account

            for pdir in iter_profile_dirs_for_account(acc, project_root_dir=root):
                if pdir.is_dir():
                    refs.add(pdir.resolve())
        except Exception as exc:  # noqa: BLE001
            logger.debug("collect_referenced iter_profile_dirs: {}", exc)
    # Marker ``.toolfb_account_id`` — giữ mọi profile đã gán chủ (kể cả path chưa sync JSON).
    try:
        from src.utils.account_browser_profile import _account_id_variants_for_storage  # noqa: PLC2701

        all_variants: set[str] = set()
        for acc in rows:
            all_variants.update(_account_id_variants_for_storage(acc))
        profiles_root = profiles_data_dir(root)
        for leaf in iter_profile_leaf_dirs(profiles_root):
            marker = leaf / ".toolfb_account_id"
            if not marker.is_file():
                continue
            owner = marker.read_text(encoding="utf-8").strip()
            if owner and owner in all_variants:
                refs.add(leaf.resolve())
    except Exception as exc:  # noqa: BLE001
        logger.debug("collect_referenced markers: {}", exc)
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
        ok = safe_delete_path(candidate, allowed_roots=[ck_root], kind="file", missing_ok=False)
        if ok:
            deleted_log.append(str(candidate))
            logger.info("Đã xóa cookie mồ côi (theo profile đã xóa): {}", candidate)
        else:
            logger.warning("Không xóa được cookie {}.", candidate)
    except OSError:
        logger.warning("Không xóa được cookie {}.", candidate)


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
        try:
            from src.utils.account_browser_profile import portable_profile_likely_has_session

            if portable_profile_likely_has_session(str(folder), project_root_dir=proot):
                logger.info(
                    "Giữ profile có phiên/lịch sử — không xóa dù chưa khớp accounts.json: {}",
                    folder,
                )
                continue
        except Exception as exc:  # noqa: BLE001
            logger.debug("Kiểm tra phiên profile {}: {}", folder, exc)
        marker = folder / ".toolfb_account_id"
        if marker.is_file():
            owner = marker.read_text(encoding="utf-8").strip()
            if owner:
                logger.info(
                    "Giữ profile có marker chủ «{}» — không xóa mồ côi: {}",
                    owner,
                    folder,
                )
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
        ok = safe_delete_path(folder, allowed_roots=[profiles_root], kind="dir", missing_ok=False)
        if ok:
            deleted.append(str(folder))
            logger.info("Đã xóa profile mồ côi: {}", folder)
        else:
            logger.warning("Không xóa được profile {}.", folder)
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
