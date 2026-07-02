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
    aid = str(acc.get("registry_id") or acc.get("id") or "").strip() or "manual"
    portable = resolve_account_portable_profile(acc)
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


def portable_profile_likely_has_session(
    profile_path: str,
    *,
    project_root_dir: Path | None = None,
) -> bool:
    """
    True nếu thư mục profile portable có dấu hiệu đã dùng đăng nhập (cookie DB / nhiều file).

    Dùng trước khi mở browser — không đọc ``c_user`` trực tiếp từ SQLite.
    """
    root = (project_root_dir or project_root()).resolve()
    pdir = resolve_account_path(root, str(profile_path or "").strip())
    if pdir is None or not pdir.is_dir():
        return False
    for pattern in ("cookies.sqlite", "**/cookies.sqlite", "Default/Cookies", "Cookies"):
        try:
            hit = next(pdir.glob(pattern), None)
            if hit is not None and hit.is_file() and hit.stat().st_size > 64:
                return True
        except OSError:
            continue
    try:
        places = next(pdir.glob("places.sqlite"), None)
        if places is not None and places.is_file() and places.stat().st_size > 1024:
            return True
    except OSError:
        pass
    return False


def resolve_account_portable_profile(acc: dict[str, Any]) -> str:
    """
    Gắn đúng ``portable_path`` — ưu tiên thư mục **đã có lịch sử/phiên**, không dùng profile trống mới mkdir.

    Gọi trước ``launch_persistent_context`` để mỗi lần mở lại đúng profile cũ của TK.
    """
    from src.utils.account_proxy_mapper import enrich_account_dict_from_registry

    enrich_account_dict_from_registry(acc)
    root = project_root().resolve()
    aid = str(acc.get("registry_id") or acc.get("id") or "").strip() or "manual"
    bt = normalize_browser_storage(str(acc.get("browser_type") or "firefox"))

    configured = str(acc.get("portable_path") or acc.get("profile_path") or "").strip()
    configured_dir: Path | None = None
    if configured:
        configured_dir = resolve_account_path(root, configured)

    # Đã có trong accounts.json + profile đã có phiên/lịch sử → dùng đúng acc_…
    if (
        acc.get("registry_id")
        and configured_dir is not None
        and configured_dir.is_dir()
        and portable_profile_likely_has_session(str(configured_dir))
    ):
        rel = relativize_account_storage_path(str(configured_dir), project_root_dir=root)
        acc["portable_path"] = rel
        acc["profile_path"] = rel
        logger.info(
            "[Profile] Dùng profile registry (có phiên) account={} (hiển thị={}) → {}",
            aid,
            acc.get("display_account_id") or acc.get("id"),
            rel,
        )
        _sync_resolved_profile_to_registry(acc, rel)
        return rel

    best = pick_best_profile_dir_for_account(acc, project_root_dir=root)
    if best is not None:
        best_score = _score_profile_dir(best, acc)[0]
        cfg_score = _score_profile_dir(configured_dir, acc)[0] if configured_dir and configured_dir.is_dir() else -1
        if configured_dir is None or not configured_dir.is_dir() or best_score > cfg_score:
            if configured_dir is not None and configured_dir.is_dir() and best.resolve() != configured_dir.resolve():
                logger.warning(
                    "[Profile] Chuyển sang profile có lịch sử account={} | cũ={} (score {}) → mới={} (score {})",
                    aid,
                    configured,
                    cfg_score,
                    best,
                    best_score,
                )
            rel = relativize_account_storage_path(str(best), project_root_dir=root)
            acc["portable_path"] = rel
            acc["profile_path"] = rel
            _sync_resolved_profile_to_registry(acc, rel)
            return rel

    if configured_dir is not None and configured_dir.is_dir():
        rel = relativize_account_storage_path(str(configured_dir), project_root_dir=root)
        acc["portable_path"] = rel
        acc["profile_path"] = rel
        logger.debug("[Profile] portable từ registry/cấu hình account={} → {}", aid, rel)
        return rel

    rel = default_portable_path(aid, bt)
    acc["portable_path"] = rel
    acc["profile_path"] = rel
    return rel


def _sync_resolved_profile_to_registry(acc: dict[str, Any], portable_rel: str) -> None:
    """Ghi lại portable_path đã khôi phục vào accounts.json để lần sau không mở nhầm profile trống."""
    aid = str(acc.get("registry_id") or acc.get("id") or "").strip()
    if not aid or not portable_rel:
        return
    try:
        from src.utils.db_manager import AccountsDatabaseManager

        db = AccountsDatabaseManager()
        rows = db.load_all()
        rec = next((r for r in rows if str(r.get("id") or "") == aid), None)
        if not rec:
            fb = str(acc.get("facebook_uid") or "").strip()
            if fb.isdigit():
                rec = next((r for r in rows if str(r.get("facebook_uid") or "").strip() == fb), None)
        if not rec:
            return
        cur = str(rec.get("portable_path") or rec.get("profile_path") or "").strip()
        if cur.replace("\\", "/") == portable_rel.replace("\\", "/"):
            return
        db.update_account_fields(
            aid,
            {"portable_path": portable_rel, "profile_path": portable_rel},
        )
        logger.info("[Profile] Đã sync portable_path registry account={} → {}", aid, portable_rel)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Profile] sync registry {}: {}", aid, exc)


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
    account: dict[str, Any] | None = None,
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
            _write_profile_owner_marker(profile_dir, str((account or {}).get("registry_id") or aid))
        return
    owner = marker.read_text(encoding="utf-8").strip()
    if account and _profile_owner_matches_account(owner, account):
        return
    if owner and owner != aid:
        raise ValueError(
            f"Profile {profile_dir} thuộc tài khoản «{owner}», không mở cho «{aid}». "
            "Hai tài khoản không được dùng chung một thư mục profile."
        )


def _account_id_variants_for_storage(account: dict[str, Any]) -> list[str]:
    """Mọi khóa có thể trỏ tới cùng profile/cookie trên đĩa (UID vs acc_…)."""
    seen: set[str] = set()
    out: list[str] = []

    def _add(raw: str) -> None:
        s = str(raw or "").strip()
        if not s or s in seen:
            return
        seen.add(s)
        out.append(s)

    _add(str(account.get("registry_id") or ""))
    _add(str(account.get("id") or ""))
    _add(str(account.get("display_account_id") or ""))
    fb = str(account.get("facebook_uid") or "").strip()
    if fb.isdigit():
        _add(fb)
        _add(f"UID_{fb}")
    aid = str(account.get("id") or "").strip()
    if aid.upper().startswith("UID_"):
        num = aid.split("_", 1)[-1]
        if num.isdigit():
            _add(num)
    try:
        from src.utils.db_manager import AccountsDatabaseManager

        for rec in AccountsDatabaseManager().load_all():
            rid = str(rec.get("id") or "").strip()
            rfb = str(rec.get("facebook_uid") or "").strip()
            if not rid:
                continue
            linked = {rid}
            if rfb.isdigit():
                linked.add(rfb)
                linked.add(f"UID_{rfb}")
            if seen & linked:
                for x in linked:
                    _add(x)
    except Exception:
        pass
    return out


def _profile_owner_matches_account(owner: str, account: dict[str, Any]) -> bool:
    """True nếu marker ``.toolfb_account_id`` thuộc cùng tài khoản (UID_ / acc_ / registry)."""
    o = str(owner or "").strip()
    if not o:
        return False
    return o in set(_account_id_variants_for_storage(account))


def _score_profile_dir(profile_dir: Path, account: dict[str, Any] | str) -> tuple[int, float]:
    """
    Điểm profile: có cookie DB / lịch sử > có marker đúng chủ > nhiều file > mới tạo.
    """
    variants = (
        {str(account).strip()}
        if isinstance(account, str)
        else set(_account_id_variants_for_storage(account))
    )
    score = 0
    if portable_profile_likely_has_session(str(profile_dir)):
        score += 200
    marker = profile_dir / ".toolfb_account_id"
    if marker.is_file():
        owner = marker.read_text(encoding="utf-8").strip()
        if owner and owner in variants:
            score += 80
        elif owner:
            score += 10
    try:
        entries = [e for e in profile_dir.iterdir() if e.name != ".toolfb_account_id"]
        score += min(len(entries), 25)
        mtime = profile_dir.stat().st_mtime
    except OSError:
        mtime = 0.0
    return score, mtime


def pick_best_profile_dir_for_account(
    account: dict[str, Any],
    *,
    project_root_dir: Path | None = None,
) -> Path | None:
    """Chọn thư mục profile tốt nhất trong các ứng viên (ưu tiên đã có phiên đăng nhập)."""
    root = (project_root_dir or project_root()).resolve()
    candidates = iter_profile_dirs_for_account(account, project_root_dir=root)
    if not candidates:
        return None
    scored = [(_score_profile_dir(p, account), p) for p in candidates]
    scored.sort(key=lambda item: (item[0][0], item[0][1]), reverse=True)
    return scored[0][1]


def iter_profile_dirs_for_account(
    account: dict[str, Any],
    *,
    project_root_dir: Path | None = None,
) -> list[Path]:
    """Mọi thư mục profile có thể gắn với tài khoản (đường dẫn tuyệt đối, không trùng)."""
    root = (project_root_dir or project_root()).resolve()
    seen: set[Path] = set()
    out: list[Path] = []
    reg_portable = str(account.get("portable_path") or account.get("profile_path") or "").strip()
    reg_path = resolve_account_path(root, reg_portable) if reg_portable else None
    reg_id = str(account.get("registry_id") or account.get("id") or "").strip()
    skip_uid_guess = bool(
        reg_id.startswith("acc_")
        and reg_path is not None
        and reg_path.is_dir()
        and portable_profile_likely_has_session(str(reg_path))
        and "profiles/firefox/acc_" in reg_portable.replace("\\", "/")
    )
    for key in ("portable_path", "profile_path"):
        pdir = resolve_account_path(root, str(account.get(key) or ""))
        if pdir is not None and pdir.is_dir() and pdir not in seen:
            seen.add(pdir)
            out.append(pdir)
    for aid in _account_id_variants_for_storage(account):
        if skip_uid_guess and aid.upper().startswith("UID_"):
            continue
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
