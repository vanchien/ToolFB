"""
Profile Playwright/Firefox riêng cho từng ``account_id`` — tạo khi thêm, xóa khi gỡ tài khoản.

Mỗi tài khoản: ``data/profiles/<browser>/<id>`` + ``data/cookies/<id>.json`` (không dùng chung thư mục).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loguru import logger

from src.utils.paths import project_root
from src.utils.safe_delete import safe_delete_path

_BROWSER_DIRS = frozenset({"chromium", "chrome", "firefox", "webkit"})

_EMPTY_STORAGE_STATE = json.dumps({"cookies": [], "origins": []}, ensure_ascii=False, indent=2) + "\n"


def normalize_browser_storage(browser_type: str) -> str:
    bt = str(browser_type or "firefox").strip().lower()
    if bt == "chrome":
        return "chromium"
    if bt in _BROWSER_DIRS:
        return bt
    return "firefox"


def default_portable_path(account_id: str, browser_type: str = "firefox") -> str:
    aid = str(account_id or "").strip()
    sub = normalize_browser_storage(browser_type)
    return f"data/profiles/{sub}/{aid}"


def default_cookie_path(account_id: str) -> str:
    return f"data/cookies/{str(account_id or '').strip()}.json"


def relativize_account_storage_path(
    raw: str,
    *,
    project_root_dir: Path | None = None,
) -> str:
    """
    Chuyển đường dẫn tuyệt đối (máy cũ) → relative ``data/...`` để copy cả thư mục ToolFB sang máy khác.

    Giữ nguyên nếu đã relative hoặc nằm ngoài project (vd. Chrome hệ thống).
    """
    s = str(raw or "").strip()
    if not s:
        return s
    root = (project_root_dir or project_root()).resolve()
    p = Path(s).expanduser()
    if not p.is_absolute():
        return s.replace("\\", "/")
    try:
        rel = p.resolve().relative_to(root)
        return rel.as_posix()
    except ValueError:
        return str(p.resolve())


def resolve_account_path(project_root_dir: Path, raw: str) -> Path | None:
    s = str(raw or "").strip()
    if not s:
        return None
    p = Path(s)
    if not p.is_absolute():
        p = project_root_dir / p
    try:
        return p.resolve()
    except OSError:
        return None


def assert_portable_path_not_shared(
    account_id: str,
    portable_path: str,
    other_accounts: list[dict[str, Any]],
    *,
    project_root_dir: Path | None = None,
) -> None:
    """Không cho hai id khác nhau trỏ cùng một thư mục profile."""
    root = (project_root_dir or project_root()).resolve()
    aid = str(account_id or "").strip()
    mine = resolve_account_path(root, portable_path)
    if mine is None:
        return
    for other in other_accounts:
        oid = str(other.get("id", "")).strip()
        if not oid or oid == aid:
            continue
        for key in ("portable_path", "profile_path"):
            op = resolve_account_path(root, str(other.get(key) or ""))
            if op is not None and op == mine:
                raise ValueError(
                    f"portable_path trùng tài khoản «{oid}» ({mine}). "
                    "Mỗi tài khoản phải có profile Firefox/Playwright riêng."
                )


def provision_fresh_browser_profile(
    account_id: str,
    browser_type: str = "firefox",
    *,
    project_root_dir: Path | None = None,
    cookie_path: str | None = None,
) -> tuple[str, str]:
    """
    Tạo profile Playwright **mới** cho một tài khoản (xóa thư mục cũ cùng đường dẫn nếu có).

    Returns:
        ``(portable_path, cookie_path)`` dạng relative tới project root.
    """
    root = (project_root_dir or project_root()).resolve()
    aid = str(account_id or "").strip()
    if not aid:
        raise ValueError("account_id rỗng — không tạo profile.")
    portable_rel = default_portable_path(aid, browser_type)
    cookie_rel = str(cookie_path or "").strip() or default_cookie_path(aid)
    profile_dir = resolve_account_path(root, portable_rel)
    cookie_file = resolve_account_path(root, cookie_rel)
    if profile_dir is None or cookie_file is None:
        raise ValueError("Đường dẫn profile/cookie không hợp lệ.")

    profiles_root = (root / "data" / "profiles").resolve()
    cookies_root = (root / "data" / "cookies").resolve()
    if profile_dir.exists():
        logger.info("Xóa profile cũ trước khi tạo mới | account={} | {}", aid, profile_dir)
        safe_delete_path(profile_dir, allowed_roots=[profiles_root], kind="dir", missing_ok=True)
    profile_dir.mkdir(parents=True, exist_ok=True)
    _write_profile_owner_marker(profile_dir, aid)

    if cookie_file.exists():
        safe_delete_path(cookie_file, allowed_roots=[cookies_root], kind="file", missing_ok=True)
    cookie_file.parent.mkdir(parents=True, exist_ok=True)
    cookie_file.write_text(_EMPTY_STORAGE_STATE, encoding="utf-8")

    logger.info(
        "Đã tạo profile Playwright riêng | account={} | browser={} | profile={}",
        aid,
        normalize_browser_storage(browser_type),
        profile_dir,
    )
    return portable_rel, cookie_rel


def ensure_account_browser_profile_ready(acc: dict[str, Any]) -> None:
    """
    Chuẩn bị ``acc`` trước ``launch_persistent_context``: registry, đường dẫn profile, mkdir.

    Tránh lỗi «Thư mục profile không tồn tại» khi mở trình duyệt tay lần đầu.
    """
    from src.utils.account_proxy_mapper import enrich_account_dict_from_registry

    enrich_account_dict_from_registry(acc)
    aid = str(acc.get("id") or "").strip() or "manual"
    bt = normalize_browser_storage(str(acc.get("browser_type") or "firefox"))
    portable = str(acc.get("portable_path") or acc.get("profile_path") or "").strip()
    if not portable:
        portable = default_portable_path(aid, bt)
        acc["portable_path"] = portable
        acc["profile_path"] = portable
    cookie = str(acc.get("cookie_path") or "").strip()
    if not cookie:
        acc["cookie_path"] = default_cookie_path(aid)
    ensure_profile_directory_exists(aid, portable)


def ensure_profile_directory_exists(
    account_id: str,
    portable_path: str,
    *,
    project_root_dir: Path | None = None,
) -> None:
    """Import từ thư mục có sẵn — chỉ mkdir, không xóa dữ liệu cũ."""
    root = (project_root_dir or project_root()).resolve()
    profile_dir = resolve_account_path(root, portable_path)
    if profile_dir is None:
        raise ValueError(f"portable_path không hợp lệ: {portable_path!r}")
    profile_dir.mkdir(parents=True, exist_ok=True)
    _write_profile_owner_marker(profile_dir, account_id)


def _write_profile_owner_marker(profile_dir: Path, account_id: str) -> None:
    """Ghi marker chủ sở hữu profile (không ghi đè nếu đã thuộc tài khoản khác)."""
    aid = str(account_id or "").strip()
    if not aid:
        return
    marker = profile_dir / ".toolfb_account_id"
    if marker.is_file():
        owner = marker.read_text(encoding="utf-8").strip()
        if owner and owner != aid:
            raise ValueError(
                f"Thư mục profile {profile_dir} đã thuộc tài khoản «{owner}», "
                f"không gán cho «{aid}». Mỗi tài khoản phải có profile riêng."
            )
        return
    marker.write_text(aid + "\n", encoding="utf-8")


def assert_profile_directory_owned_by(
    account_id: str,
    profile_dir: Path,
    *,
    create_marker_if_missing: bool = True,
) -> None:
    """
    Đảm bảo thư mục profile chỉ dùng cho một ``account_id`` (file ``.toolfb_account_id``).
    """
    aid = str(account_id or "").strip()
    if not aid or not profile_dir.is_dir():
        return
    marker = profile_dir / ".toolfb_account_id"
    if not marker.is_file():
        if create_marker_if_missing:
            _write_profile_owner_marker(profile_dir, aid)
        return
    owner = marker.read_text(encoding="utf-8").strip()
    if owner and owner != aid:
        raise ValueError(
            f"Profile {profile_dir} thuộc tài khoản «{owner}», không mở cho «{aid}». "
            "Hai tài khoản không được dùng chung một thư mục profile."
        )


def iter_profile_dirs_for_account(
    account: dict[str, Any],
    *,
    project_root_dir: Path | None = None,
) -> list[Path]:
    """Mọi thư mục profile có thể gắn với tài khoản (đường dẫn tuyệt đối, không trùng)."""
    root = (project_root_dir or project_root()).resolve()
    aid = str(account.get("id", "")).strip()
    seen: set[Path] = set()
    out: list[Path] = []
    for key in ("portable_path", "profile_path"):
        pdir = resolve_account_path(root, str(account.get(key) or ""))
        if pdir is not None and pdir.is_dir() and pdir not in seen:
            seen.add(pdir)
            out.append(pdir)
    if aid:
        for sub in _BROWSER_DIRS:
            pdir = resolve_account_path(root, default_portable_path(aid, sub))
            if pdir is not None and pdir.is_dir() and pdir not in seen:
                seen.add(pdir)
                out.append(pdir)
    return out


def delete_account_browser_bundle(
    account: dict[str, Any],
    *,
    project_root_dir: Path | None = None,
) -> list[str]:
    """
    Xóa profile Firefox/Playwright, cookie và vault của một tài khoản.

    Returns:
        Danh sách đường dẫn đã xóa (hoặc cố gắng xóa).
    """
    root = (project_root_dir or project_root()).resolve()
    aid = str(account.get("id", "")).strip()
    deleted: list[str] = []
    profiles_root = (root / "data" / "profiles").resolve()
    cookies_root = (root / "data" / "cookies").resolve()

    for pdir in iter_profile_dirs_for_account(account, project_root_dir=root):
        if safe_delete_path(pdir, allowed_roots=[profiles_root], kind="dir", missing_ok=True):
            deleted.append(str(pdir))
            logger.info("Đã xóa profile Playwright | account={} | {}", aid, pdir)

    ck = resolve_account_path(root, str(account.get("cookie_path") or ""))
    if ck is not None and ck.is_file():
        if safe_delete_path(ck, allowed_roots=[cookies_root], kind="file", missing_ok=True):
            deleted.append(str(ck))

    cookie_default = cookies_root / f"{aid}.json"
    if cookie_default.is_file() and str(cookie_default.resolve()) not in deleted:
        if safe_delete_path(cookie_default, allowed_roots=[cookies_root], kind="file", missing_ok=True):
            deleted.append(str(cookie_default))

    try:
        from src.utils.account_credentials import delete_account_credentials

        if delete_account_credentials(aid):
            deleted.append(f"vault:account:{aid}")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không xóa vault credentials account={}: {}", aid, exc)

    return deleted
