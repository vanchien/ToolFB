"""
Lưu / nạp cookie Facebook — tái sử dụng phiên, không đăng nhập lại mỗi lần chạy.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from loguru import logger
from playwright.sync_api import BrowserContext, Page

from src.services.facebook_session_recovery import save_session_to_cookie_path
from src.utils.paths import project_root


def resolve_cookie_file(cookie_path: str | Path | None) -> Path | None:
    """Chuẩn hóa đường dẫn file cookie (storage_state JSON)."""
    raw = str(cookie_path or "").strip()
    if not raw:
        return None
    p = Path(raw)
    if not p.is_absolute():
        p = (project_root() / p).resolve()
    return p


def cookie_file_has_session(cookie_path: str | Path | None) -> bool:
    """True nếu file cookie tồn tại và có ``c_user`` (phiên Facebook)."""
    p = resolve_cookie_file(cookie_path)
    if p is None or not p.is_file():
        return False
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return False
    cookies: list[Any]
    if isinstance(raw, dict) and isinstance(raw.get("cookies"), list):
        cookies = raw["cookies"]
    elif isinstance(raw, list):
        cookies = raw
    else:
        return False
    for c in cookies:
        if isinstance(c, dict) and str(c.get("name") or "") == "c_user" and str(c.get("value") or "").strip():
            return True
    return False


def bootstrap_cookies_into_context(context: BrowserContext, cookie_path: str | Path | None) -> bool:
    """
    Nạp cookie từ file vào context **ngay sau launch** (trước ``prime_facebook_session_page``).

    Returns:
        True nếu đã nạp ít nhất một cookie hợp lệ.
    """
    p = resolve_cookie_file(cookie_path)
    if p is None:
        return False
    if not cookie_file_has_session(p):
        logger.debug("[FB session] Chưa có phiên trong file cookie: {}", p)
        return False
    try:
        from src.automation.facebook_actions import _load_playwright_cookies

        cookies = _load_playwright_cookies(p)
        if not cookies:
            return False
        page = context.pages[0] if context.pages else context.new_page()
        try:
            page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=45_000)
        except Exception as nav_exc:  # noqa: BLE001
            logger.debug("[FB session] Bootstrap: goto www trước add_cookies: {}", nav_exc)
        context.add_cookies(cookies)
        logger.info("[FB session] Đã bootstrap {} cookie từ file {}", len(cookies), p)
        return True
    except FileNotFoundError:
        return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("[FB session] Bootstrap cookie từ {} thất bại: {}", p, exc)
        return False


def sync_session_to_accounts_registry(account: dict[str, Any], cookie_path: str | Path | None) -> None:
    """Ghi ``cookie_path`` (+ portable) vào ``accounts.json`` nếu tài khoản đã có trong registry."""
    aid = str(account.get("id") or "").strip()
    if not aid:
        return
    ck_rel = str(cookie_path or account.get("cookie_path") or "").strip()
    if not ck_rel:
        return
    updates: dict[str, Any] = {"cookie_path": ck_rel}
    portable = str(account.get("portable_path") or account.get("profile_path") or "").strip()
    if portable:
        updates["portable_path"] = portable
        updates["profile_path"] = portable
    try:
        from src.utils.db_manager import AccountsDatabaseManager

        db = AccountsDatabaseManager()
        rows = db.load_all()
        if not any(str(r.get("id") or "") == aid for r in rows):
            logger.debug("[FB session] account_id={} chưa có trong accounts.json — bỏ qua sync registry.", aid)
            return
        db.update_account_fields(aid, updates)
        logger.info("[FB session] Đã sync cookie/profile vào accounts.json | id={}", aid)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[FB session] Không sync accounts.json cho {}: {}", aid, exc)


def resolve_best_cookie_path_for_account(
    account: dict[str, Any],
    *,
    facebook_uid: str = "",
    extra_candidates: list[str] | None = None,
) -> str:
    """
    Chọn file cookie có ``c_user`` — ưu tiên registry, sau đó ``UID_<số>.json`` / ``<số>.json``.

    Tránh mở form login khi phiên nằm ở tên file khác ``account_id`` (vd. ``UID_1000…`` vs ``acc_…``).
    """
    from src.utils.account_browser_profile import default_cookie_path

    candidates: list[str] = []

    def _add(raw: str) -> None:
        s = str(raw or "").strip()
        if s and s not in candidates:
            candidates.append(s)

    _add(str(account.get("cookie_path") or ""))
    aid = str(account.get("id") or "").strip()
    if aid:
        _add(default_cookie_path(aid))
    fb_uid = str(facebook_uid or account.get("facebook_uid") or "").strip()
    if fb_uid.isdigit():
        _add(f"data/cookies/UID_{fb_uid}.json")
        _add(f"data/cookies/{fb_uid}.json")
    if extra_candidates:
        for item in extra_candidates:
            _add(str(item or ""))

    for path in candidates:
        if cookie_file_has_session(path):
            chosen = ensure_account_cookie_path(account, path)
            if path != str(account.get("cookie_path") or ""):
                logger.info(
                    "[FB session] Dùng cookie phiên {} (account={})",
                    chosen,
                    aid or fb_uid or "?",
                )
            return chosen
    return ensure_account_cookie_path(account, candidates[0] if candidates else None)


def ensure_account_cookie_path(
    account: dict[str, Any],
    cookie_path: str | Path | None = None,
) -> str:
    """
    Gán ``cookie_path`` chuẩn (``data/cookies/<id>.json``) — dùng trước mọi lần lưu/nạp phiên.

    Returns:
        Đường dẫn relative tới project root khi có thể.
    """
    from src.utils.account_browser_profile import default_cookie_path

    aid = str(account.get("id") or "").strip() or "unknown"
    raw = str(cookie_path or account.get("cookie_path") or "").strip()
    if not raw:
        raw = default_cookie_path(aid)
    p = resolve_cookie_file(raw)
    if p is None:
        account["cookie_path"] = raw
        return raw
    try:
        rel = p.resolve().relative_to(project_root().resolve()).as_posix()
        account["cookie_path"] = rel
        return rel
    except ValueError:
        account["cookie_path"] = str(p)
        return str(p)


def persist_confirmed_facebook_session(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    sync_registry: bool = True,
    log_label: str = "",
) -> bool:
    """
    Lưu cookie sau khi **xác nhận** đã vào bảng tin (tránh ghi phiên lỗi / giảm đăng nhập lại).

    Returns:
        True nếu đã lưu file cookie hợp lệ.
    """
    from src.services.facebook_session_recovery import (
        _set_session_status,
        confirm_facebook_session_logged_in,
        save_session_to_cookie_path,
    )

    ck_rel = ensure_account_cookie_path(account, cookie_path)
    ok, detail = confirm_facebook_session_logged_in(page, account)
    if not ok:
        logger.warning(
            "[FB session] Không lưu cookie — chưa xác nhận phiên account={}{}: {}",
            account.get("id"),
            f" ({log_label})" if log_label else "",
            detail,
        )
        _set_session_status(account, "reauth_required")
        return False
    account["login_confirm_detail"] = detail
    save_session_to_cookie_path(page, ck_rel)
    _set_session_status(account, "ready")
    if sync_registry:
        sync_session_to_accounts_registry(account, ck_rel)
    logger.info(
        "[FB session] Đã lưu cookie xác nhận account={}{} → {}",
        account.get("id"),
        f" ({log_label})" if log_label else "",
        ck_rel,
    )
    return cookie_file_has_session(ck_rel)


def apply_saved_cookie_path_to_mapped(mapped: Any, account: dict[str, Any]) -> str:
    """Đồng bộ ``cookie_path`` từ dict account sang ``MappedAccount`` (GUI/worker)."""
    ck = str(account.get("cookie_path") or getattr(mapped, "cookie_path", "") or "").strip()
    if ck:
        try:
            mapped.cookie_path = ck
        except Exception:
            pass
    return ck


def auto_save_session_for_account(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    mapped: Any | None = None,
    log_label: str = "",
    require_confirm: bool = True,
) -> tuple[bool, str]:
    """
    Tự động lưu cookie phiên + sync ``accounts.json``; cập nhật ``MappedAccount`` nếu có.

    Returns:
        ``(đã_lưu_thành_công, đường_dẫn_cookie)``.
    """
    ck = cookie_path if cookie_path is not None else account.get("cookie_path")
    saved = False
    if require_confirm:
        saved = persist_confirmed_facebook_session(
            page,
            account,
            cookie_path=ck,
            sync_registry=True,
            log_label=log_label or "auto_save",
        )
    if not saved:
        saved = persist_facebook_session(
            page,
            account,
            cookie_path=ck,
            sync_registry=True,
            require_confirm=False,
        )
    path = apply_saved_cookie_path_to_mapped(mapped, account) if mapped is not None else str(
        account.get("cookie_path") or ck or ""
    )
    if saved:
        logger.info(
            "[FB session] Tự động lưu cookie account={}{} → {}",
            account.get("id"),
            f" ({log_label})" if log_label else "",
            path,
        )
    return saved, path


def persist_facebook_session(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    sync_registry: bool = True,
    require_confirm: bool = False,
) -> bool:
    """
    Lưu ``storage_state`` ra file + (tuỳ chọn) cập nhật accounts.json.

    Args:
        require_confirm: True → chỉ lưu sau ``confirm_facebook_session_logged_in`` (khuyến nghị sau login).

    Returns:
        True nếu file cookie được ghi.
    """
    if require_confirm:
        return persist_confirmed_facebook_session(
            page,
            account,
            cookie_path=cookie_path,
            sync_registry=sync_registry,
            log_label="persist",
        )

    from src.automation.facebook_actions import facebook_session_appears_logged_in

    if not facebook_session_appears_logged_in(page):
        logger.debug("[FB session] Bỏ qua lưu cookie — chưa xác nhận phiên đăng nhập.")
        return False
    ck_rel = ensure_account_cookie_path(account, cookie_path)
    save_session_to_cookie_path(page, ck_rel)
    if sync_registry:
        sync_session_to_accounts_registry(account, ck_rel)
    return True


def mapped_account_ready_for_interaction(ma: Any) -> tuple[bool, str]:
    """
    Kiểm tra GUI: tài khoản đủ điều kiện vào hàng đợi «Tương tác» (đã login_ok / success).

    Returns:
        ``(True, "")`` hoặc ``(False, lý_do hiển thị)``.
    """
    from src.models.mapped_account import MappedAccount
    from src.utils.account_proxy_mapper import sync_mapped_account_storage_from_registry

    if isinstance(ma, MappedAccount):
        sync_mapped_account_storage_from_registry(ma)

    st = str(getattr(ma, "status", None) or "").strip()
    uid = str(getattr(ma, "account_id", "") or "").strip()
    disp = getattr(ma, "display_uid", None)
    if callable(disp):
        try:
            uid = str(disp()).strip() or uid
        except Exception:
            pass
    cp = str(getattr(ma, "cookie_path", "") or "").strip()
    if cookie_file_has_session(cp):
        if st not in ("login_ok", "success"):
            try:
                ma.status = "login_ok"
                if not str(getattr(ma, "status_detail", "") or "").strip():
                    ma.status_detail = "Sẵn sàng tương tác (cookie phiên hợp lệ)"
            except Exception:
                pass
        return True, ""

    if st in ("login_ok", "success"):
        return (
            False,
            f"{uid or 'TK'}: chưa có file cookie phiên — đăng nhập và «Lưu cookie» trước khi tương tác",
        )
    if st in ("pending", "waiting", ""):
        return False, f"{uid or 'TK'}: chưa có cookie phiên — lưu cookie sau đăng nhập tay"
    if st == "login_failed":
        return False, f"{uid or 'TK'}: đăng nhập thất bại — đăng nhập lại trước khi tương tác"
    if st == "proxy_error":
        return False, f"{uid or 'TK'}: lỗi proxy — sửa proxy rồi đăng nhập lại"
    return False, f"{uid or 'TK'}: trạng thái «{st}» — cần «Đăng nhập OK» trước"


def ensure_session_before_interaction(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    recover_fn: Callable[[], bool] | None = None,
) -> tuple[bool, str]:
    """
    Cổng bắt buộc trước module tương tác: xác nhận đã vào Facebook.

    Nếu chưa đăng nhập → gọi ``recover_fn()`` (đăng nhập lại) → xác nhận lần nữa.

    Returns:
        ``(True, mô_tả)`` khi được phép tương tác; ``(False, lý_do)`` nếu không.
    """
    from src.services.facebook_session_recovery import (
        _finalize_successful_recovery,
        confirm_facebook_session_logged_in,
    )

    cp = cookie_path if cookie_path is not None else account.get("cookie_path")
    ok, detail = confirm_facebook_session_logged_in(page, account)
    if ok:
        account["login_confirm_detail"] = detail
        if _finalize_successful_recovery(page, account, cp, log_label="interaction_ready"):
            logger.info(
                "[FB session] Đã xác nhận phiên trước tương tác account={}",
                account.get("id"),
            )
            return True, detail
        return False, "Không lưu được cookie sau xác nhận phiên"

    logger.info(
        "[FB session] Chưa xác nhận đăng nhập trước tương tác account={}: {}",
        account.get("id"),
        detail,
    )
    if recover_fn is None:
        return False, detail or "Chưa đăng nhập — bắt buộc đăng nhập lại"

    if not recover_fn():
        return False, detail or "Đăng nhập lại thất bại — chưa vào được tài khoản"

    ok2, detail2 = confirm_facebook_session_logged_in(page, account)
    if not ok2:
        return False, detail2 or "Đăng nhập lại xong nhưng chưa xác nhận được phiên"

    account["login_confirm_detail"] = detail2
    if _finalize_successful_recovery(page, account, cp, log_label="interaction_relogin"):
        logger.info(
            "[FB session] Đăng nhập lại OK — cho phép tương tác account={}",
            account.get("id"),
        )
        return True, detail2
    return False, detail2 or "Không lưu cookie sau đăng nhập lại"


def restore_facebook_session(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    prime: bool = True,
) -> tuple[bool, str]:
    """
    Khôi phục phiên Facebook — **ưu tiên profile persistent** (giống tab Tài khoản / job đăng bài).

    Không dùng ``bootstrap_cookies_into_context`` (tránh ghi đè cookie profile bằng file cũ).

    Returns:
        ``(True, "profile"|"cookie")`` hoặc ``(False, lý_do)``.
    """
    import os

    from src.automation.facebook_actions import (
        _force_www_facebook_if_mobile_redirect,
        facebook_session_appears_logged_in,
        login_with_cookie,
        prime_facebook_session_page,
    )

    aid = str(account.get("id") or "").strip()
    prof = str(account.get("portable_path") or account.get("profile_path") or "").strip()
    cp = ensure_account_cookie_path(account, cookie_path)
    prev_mobile = os.environ.pop("TOOLFB_NAV_MOBILE_FB", None)
    try:
        if prime or "facebook.com" not in (page.url or "").lower():
            prime_facebook_session_page(page)
        else:
            _force_www_facebook_if_mobile_redirect(page)
        if not facebook_session_appears_logged_in(page):
            try:
                page.reload(wait_until="domcontentloaded", timeout=45_000)
                _force_www_facebook_if_mobile_redirect(page)
            except Exception as reload_exc:  # noqa: BLE001
                logger.debug("[FB session] reload sau prime: {}", reload_exc)
        if facebook_session_appears_logged_in(page):
            logger.info(
                "[FB session] Profile portable đã đăng nhập account={} profile={}",
                aid,
                prof or "(mặc định)",
            )
            return True, "profile"
        if cookie_file_has_session(cp):
            logger.info("[FB session] Thử nạp cookie file account={} → {}", aid, cp)
            try:
                login_with_cookie(page, cp)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[FB session] login_with_cookie thất bại account={}: {}", aid, exc)
            if facebook_session_appears_logged_in(page):
                return True, "cookie"
        url = (page.url or "")[:100]
        if "/login" in url.lower():
            return False, "Trang login — profile chưa có phiên (kiểm tra portable_path trong accounts.json)"
        return False, "Chưa thấy phiên hợp lệ sau profile + cookie file"
    finally:
        if prev_mobile is not None:
            os.environ["TOOLFB_NAV_MOBILE_FB"] = prev_mobile


def try_reuse_saved_session(page: Page, account: dict[str, Any], *, cookie_path: str | Path | None = None) -> bool:
    """
    Tái sử dụng phiên: profile trước, cookie file sau — lưu lại storage_state nếu OK.

    Returns:
        True nếu đã đăng nhập (và đã lưu lại cookie).
    """
    from src.services.facebook_session_recovery import _finalize_successful_recovery

    cp = cookie_path if cookie_path is not None else account.get("cookie_path")
    ok, mode = restore_facebook_session(page, account, cookie_path=cp, prime=True)
    if not ok:
        return False
    return _finalize_successful_recovery(page, account, cp, log_label=f"reuse_{mode}")
