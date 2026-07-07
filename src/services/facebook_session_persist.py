"""
Lưu / nạp cookie Facebook — tái sử dụng phiên, không đăng nhập lại mỗi lần chạy.
"""

from __future__ import annotations

import json
import os
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


def _expected_facebook_uid_from_account(account: dict[str, Any] | None) -> str:
    """UID Facebook kỳ vọng từ ``id`` / ``facebook_uid`` / ``username``."""
    if not account:
        return ""
    from src.services.facebook_session_recovery import _normalize_facebook_uid

    for key in ("facebook_uid", "username", "id"):
        raw = str(account.get(key) or "").strip()
        if not raw:
            continue
        norm = _normalize_facebook_uid(raw)
        if norm.isdigit():
            return norm
    return ""


def profile_session_ready_for_interaction(
    page: Page,
    account: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    """
    Phiên **đã có trong profile persistent** (cookie ``c_user`` trong context) — không cần form login.

    Dùng trước khi ``login_with_cookie`` để tránh ghi đè phiên profile bằng file cookie cũ.
    """
    from src.automation.facebook_actions import facebook_session_appears_logged_in
    from src.services.facebook_session_recovery import (
        _facebook_uids_match,
        _read_facebook_c_user,
    )

    expected = _expected_facebook_uid_from_account(account)
    c_user = _read_facebook_c_user(page)
    if c_user:
        if expected and not _facebook_uids_match(c_user, expected):
            return False, f"Cookie c_user={c_user} khác UID cấu hình ({expected})"
        u = (page.url or "").lower()
        if "facebook.com" in u and ("/login" in u or "checkpoint" in u or "two_step" in u):
            from src.automation.facebook_actions import navigate_away_from_login_if_session_active

            navigate_away_from_login_if_session_active(page)
            u = (page.url or "").lower()
        if "facebook.com" in u and "/login" not in u and "checkpoint" not in u and "two_step" not in u:
            return True, f"Phiên profile sẵn có (UID {c_user})"
        if facebook_session_appears_logged_in(page):
            return True, f"Phiên profile (UID {c_user})"
    if facebook_session_appears_logged_in(page):
        cu = _read_facebook_c_user(page) or "?"
        return True, f"Đã đăng nhập Facebook (UID {cu})"
    return False, "Profile chưa có cookie c_user"


def wait_profile_session_ready(
    page: Page,
    account: dict[str, Any] | None = None,
    *,
    timeout_ms: int = 10_000,
) -> tuple[bool, str]:
    """Chờ profile persistent nạp cookie (Firefox đôi khi chậm sau launch)."""
    import time

    deadline = time.time() + max(1.0, timeout_ms / 1000.0)
    last = "Profile chưa có cookie c_user"
    while time.time() < deadline:
        ok, detail = profile_session_ready_for_interaction(page, account)
        if ok:
            return True, detail
        last = detail or last
        try:
            page.wait_for_timeout(450)
        except Exception:
            pass
    return False, last


def accept_facebook_session_after_restore(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None,
    session_mode: str = "profile",
    confirm_timeout_ms: int = 18_000,
) -> tuple[bool, bool, str]:
    """
    Chấp nhận phiên sau ``restore_facebook_session`` — một lần, không lặp confirm/profile.

    Returns:
        ``(recovered, nên_đồng_bộ_cookie, mô_tả)``.
    """
    from src.services.facebook_session_recovery import confirm_facebook_session_logged_in

    cp = str(cookie_path or account.get("cookie_path") or "")
    ok_fast, fast_detail = profile_session_ready_for_interaction(page, account)
    if ok_fast:
        return True, True, fast_detail

    ok_conf, conf_detail = confirm_facebook_session_logged_in(
        page, account, timeout_ms=confirm_timeout_ms
    )
    if ok_conf:
        persist = session_mode == "profile_probe" or cookie_file_has_session(cp)
        return True, persist, conf_detail or f"Phiên ({session_mode})"

    ok_fast2, fast_detail2 = profile_session_ready_for_interaction(page, account)
    if ok_fast2:
        return True, True, fast_detail2

    return False, False, conf_detail or "Chưa xác nhận phiên sau restore"


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
    reg_id = str(account.get("registry_id") or account.get("id") or "").strip()
    ck_rel = str(cookie_path or account.get("cookie_path") or "").strip()
    if not reg_id and not ck_rel:
        return
    updates: dict[str, Any] = {}
    if ck_rel:
        updates["cookie_path"] = ck_rel
    portable = str(account.get("portable_path") or account.get("profile_path") or "").strip()
    if portable:
        updates["portable_path"] = portable
        updates["profile_path"] = portable
    if not updates:
        return
    try:
        from src.utils.db_manager import AccountsDatabaseManager

        db = AccountsDatabaseManager()
        rows = db.load_all()
        rec = next((r for r in rows if str(r.get("id") or "") == reg_id), None)
        if not rec:
            from src.utils.account_proxy_mapper import _extract_facebook_uid

            fb = str(account.get("facebook_uid") or "").strip()
            if not fb.isdigit():
                fb = _extract_facebook_uid(reg_id)
            if fb.isdigit():
                rec = next(
                    (r for r in rows if str(r.get("facebook_uid") or "").strip() == fb),
                    None,
                )
        if not rec:
            logger.debug(
                "[FB session] Chưa có trong accounts.json — bỏ qua sync registry (id={})",
                reg_id,
            )
            return
        actual_id = str(rec.get("id") or "").strip()
        db.update_account_fields(actual_id, updates)
        logger.info(
            "[FB session] Đã sync cookie/profile vào accounts.json | id={} profile={}",
            actual_id,
            portable or "(giữ)",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[FB session] Không sync accounts.json cho {}: {}", reg_id, exc)


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
    """Đồng bộ ``cookie_path`` + ``profile_path`` từ dict account sang ``MappedAccount``."""
    ck = str(account.get("cookie_path") or getattr(mapped, "cookie_path", "") or "").strip()
    prof = str(account.get("portable_path") or account.get("profile_path") or "").strip()
    if ck:
        try:
            mapped.cookie_path = ck
        except Exception:
            pass
    if prof:
        try:
            mapped.storage.profile_path = prof
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
            ok_prof, det_prof = profile_session_ready_for_interaction(page, account)
            if ok_prof:
                logger.info(
                    "[FB session] Tự lưu cookie (profile có phiên){}: {}",
                    f" ({log_label})" if log_label else "",
                    det_prof,
                )
                saved = persist_facebook_session(
                    page,
                    account,
                    cookie_path=ck,
                    sync_registry=True,
                    require_confirm=False,
                )
    else:
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
        ok_prof, _ = profile_session_ready_for_interaction(page, account)
        if not ok_prof:
            logger.debug("[FB session] Bỏ qua lưu cookie — chưa xác nhận phiên đăng nhập.")
            return False
    ck_rel = ensure_account_cookie_path(account, cookie_path)
    save_session_to_cookie_path(page, ck_rel)
    if sync_registry:
        sync_session_to_accounts_registry(account, ck_rel)
    return True


def mapped_account_ready_for_interaction(ma: Any) -> tuple[bool, str]:
    """
    Kiểm tra GUI: tài khoản đủ điều kiện vào hàng đợi «Tương tác».

    Ưu tiên: file cookie ``c_user`` → profile portable có lịch sử đăng nhập.
    Trạng thái ``cancelled`` / ``running`` vẫn được tính nếu còn phiên đã lưu.

    Returns:
        ``(True, "")`` hoặc ``(False, lý_do hiển thị)``.
    """
    from src.models.mapped_account import MappedAccount
    from src.utils.account_browser_profile import portable_profile_likely_has_session
    from src.utils.account_proxy_mapper import (
        mapped_account_to_account_dict,
        sync_mapped_account_storage_from_registry,
    )

    if isinstance(ma, MappedAccount):
        sync_mapped_account_storage_from_registry(ma)

    uid = str(getattr(ma, "account_id", "") or "").strip()
    disp = getattr(ma, "display_uid", None)
    if callable(disp):
        try:
            uid = str(disp()).strip() or uid
        except Exception:
            pass

    st = str(getattr(ma, "status", None) or "").strip()
    acc = mapped_account_to_account_dict(ma) if isinstance(ma, MappedAccount) else {}
    fb_uid = str(getattr(getattr(ma, "auth", None), "username", "") or "").strip()
    cp = resolve_best_cookie_path_for_account(
        acc,
        facebook_uid=fb_uid,
        extra_candidates=[str(getattr(ma, "cookie_path", "") or "")],
    )
    prof = str(
        acc.get("portable_path")
        or acc.get("profile_path")
        or getattr(getattr(ma, "storage", None), "profile_path", "")
        or ""
    ).strip()

    try:
        if isinstance(ma, MappedAccount):
            ma.cookie_path = cp
            if prof:
                ma.storage.profile_path = prof
    except Exception:
        pass

    def _mark_ready(detail: str) -> None:
        if not isinstance(ma, MappedAccount):
            return
        try:
            if st not in ("login_ok", "success") or not str(getattr(ma, "status_detail", "") or "").strip():
                ma.status = "login_ok"
                ma.status_detail = detail
        except Exception:
            pass

    if cookie_file_has_session(cp):
        _mark_ready("Sẵn sàng tương tác (cookie phiên hợp lệ)")
        return True, ""

    if prof and portable_profile_likely_has_session(prof):
        _mark_ready("Sẵn sàng tương tác (phiên profile portable)")
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
    if st in ("cancelled", "running"):
        return (
            False,
            f"{uid or 'TK'}: đã dừng giữa chừng — «Lưu cookie» hoặc đăng nhập lại (chưa thấy phiên lưu)",
        )
    return False, f"{uid or 'TK'}: trạng thái «{st}» — cần «Đăng nhập OK» hoặc «Lưu cookie»"


def sync_firefox_profile_before_close(
    context: BrowserContext,
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    mapped: Any | None = None,
    log_label: str = "on_close",
) -> None:
    """
    Trước ``context.close()``: lưu storage_state JSON + nạp lại cookie vào context.

    Giúp Firefox ghi ``cookies.sqlite`` / ``places.sqlite`` khi thoát sạch — lần sau có phiên + lịch sử.
    """
    if page is None:
        return
    try:
        if page.is_closed():
            return
    except Exception:
        return
    ck = ensure_account_cookie_path(account, cookie_path)
    try:
        saved, ck_out = auto_save_session_for_account(
            page,
            account,
            cookie_path=ck,
            mapped=mapped,
            log_label=log_label,
            require_confirm=False,
        )
        if saved and ck_out:
            ck = str(ck_out)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[FB session] flush auto_save ({}): {}", log_label, exc)
    try:
        ok_page, _ = profile_session_ready_for_interaction(page, account)
        if ok_page and cookie_file_has_session(ck):
            bootstrap_cookies_into_context(context, ck)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[FB session] flush bootstrap ({}): {}", log_label, exc)
    flush_ms = max(1500, int(float(os.environ.get("FB_PROFILE_FLUSH_MS", "3000"))))
    try:
        page.wait_for_timeout(flush_ms)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[FB session] flush wait ({}): {}", log_label, exc)


def prepare_persistent_session_after_launch(
    context: BrowserContext,
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
) -> tuple[bool, str]:
    """
    Ngay sau ``launch_persistent_context``: chờ profile Firefox + nạp cookie file nếu cần.

    Tránh nhảy thẳng vào form login khi file ``data/cookies/*.json`` đã có ``c_user``.
    """
    from src.utils.account_browser_profile import portable_profile_likely_has_session

    aid = str(account.get("id") or "").strip()
    prof = str(account.get("portable_path") or account.get("profile_path") or "").strip()
    cp = ensure_account_cookie_path(account, cookie_path)
    has_prof_db = portable_profile_likely_has_session(prof) if prof else False
    has_ck = cookie_file_has_session(cp)
    logger.info(
        "[FB session] prepare launch account={} profile={} cookies_db={} cookie_file={}",
        aid,
        prof or "(mặc định)",
        has_prof_db,
        has_ck,
    )
    warm_ms = max(0, int(float(os.environ.get("FB_PROFILE_WARMUP_MS", "1200"))))
    if warm_ms > 0:
        try:
            page.wait_for_timeout(warm_ms)
        except Exception:  # noqa: BLE001
            pass
    ok_page, det_page = profile_session_ready_for_interaction(page, account)
    if not ok_page and has_ck:
        logger.info(
            "[FB session] Nạp cookie file — browser chưa có phiên account={} ({})",
            aid,
            det_page,
        )
        bootstrap_cookies_into_context(context, cp)
    ok, mode = restore_facebook_session(page, account, cookie_path=cp, prime=True)
    if ok:
        logger.info("[FB session] prepare OK account={} mode={}", aid, mode)
    else:
        logger.warning("[FB session] prepare chưa vào được account={}: {}", aid, mode)
    return ok, mode


def establish_facebook_session(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    allow_form_login: bool = False,
    form_recover_fn: Callable[[], bool] | None = None,
) -> tuple[bool, str]:
    """
    Luồng phiên **thống nhất** (Human / đăng nhập / tương tác):

    1. Mở facebook.com + chờ profile persistent (lịch sử đăng nhập Firefox)
    2. Nạp cookie file nếu profile chưa có ``c_user``
    3. Form login / 2FA / captcha — **chỉ** khi ①② thất bại và ``allow_form_login``

    Returns:
        ``(True, mô_tả)`` hoặc ``(False, lý_do)``.
    """
    from src.automation.facebook_actions import (
        _force_www_facebook_if_mobile_redirect,
        navigate_away_from_login_if_session_active,
        prime_facebook_session_page,
    )
    from src.services.facebook_session_recovery import (
        _finalize_successful_recovery,
        confirm_facebook_session_logged_in,
    )

    cp = ensure_account_cookie_path(account, cookie_path)
    aid = str(account.get("id") or "").strip()

    def _persist_ok(detail: str, label: str) -> tuple[bool, str]:
        account["login_confirm_detail"] = detail
        if _finalize_successful_recovery(page, account, cp, log_label=label):
            return True, detail
        if persist_facebook_session(
            page,
            account,
            cookie_path=cp,
            sync_registry=True,
            require_confirm=False,
        ):
            return True, detail
        ok_p, det_p = profile_session_ready_for_interaction(page, account)
        if ok_p:
            return True, det_p or detail
        return False, "Vào Facebook nhưng không lưu được cookie phiên"

    # Mở Facebook một lần — profile persistent cần goto để Firefox nạp cookie từ thư mục profile
    if "facebook.com" not in (page.url or "").lower():
        logger.info("[FB session] establish — mở facebook.com account={}", aid)
        prime_facebook_session_page(page)
    else:
        _force_www_facebook_if_mobile_redirect(page)
        navigate_away_from_login_if_session_active(page)

    # --- ① Profile đã login (lịch sử trình duyệt) ---
    prof_wait_ms = max(14_000, int(float(os.environ.get("FB_PROFILE_SESSION_WAIT_MS", "22_000"))))
    ok_prof, det_prof = wait_profile_session_ready(page, account, timeout_ms=prof_wait_ms)
    if ok_prof:
        logger.info("[FB session] establish① profile account={}: {}", aid, det_prof)
        return _persist_ok(det_prof, "establish_profile")

    try:
        page.reload(wait_until="domcontentloaded", timeout=45_000)
        _force_www_facebook_if_mobile_redirect(page)
        navigate_away_from_login_if_session_active(page)
    except Exception as reload_exc:  # noqa: BLE001
        logger.debug("[FB session] establish reload profile: {}", reload_exc)

    ok_prof_r, det_prof_r = wait_profile_session_ready(page, account, timeout_ms=8_000)
    if ok_prof_r:
        logger.info("[FB session] establish① profile (sau reload) account={}: {}", aid, det_prof_r)
        return _persist_ok(det_prof_r, "establish_profile_reload")

    # --- ② Nạp cookie file (import) ---
    if cookie_file_has_session(cp):
        logger.info("[FB session] establish② nạp cookie file account={} → {}", aid, cp)
        ok_ck, det_ck = try_reuse_saved_cookie_session(
            page,
            account,
            cookie_path=cp,
            timeout_ms=28_000,
        )
        if ok_ck:
            return _persist_ok(det_ck, "establish_cookie")

    ok_probe, det_probe = probe_existing_facebook_session(
        page,
        account,
        cookie_path=cp,
        timeout_ms=18_000,
    )
    if ok_probe:
        logger.info("[FB session] establish② probe profile account={}: {}", aid, det_probe)
        return _persist_ok(det_probe, "establish_probe")

    failure_reason = "Chưa có phiên profile/cookie — cần mật khẩu để đăng nhập form"

    # --- ③ Form login (cuối cùng) ---
    if not allow_form_login:
        pass
    elif form_recover_fn is None:
        failure_reason = "Chưa có phiên — thiếu mật khẩu để đăng nhập form"
    else:
        logger.info("[FB session] establish③ form login account={}", aid)
        if not form_recover_fn():
            failure_reason = "Đăng nhập form thất bại — kiểm tra pass/2FA/captcha"
        else:
            ok_conf, det_conf = confirm_facebook_session_logged_in(page, account, timeout_ms=22_000)
            if ok_conf:
                return _persist_ok(det_conf, "establish_form")
            ok_prof3, det3 = profile_session_ready_for_interaction(page, account)
            if ok_prof3:
                return _persist_ok(det3, "establish_form_profile")
            failure_reason = det_conf or "Form login xong nhưng chưa xác nhận phiên"

    f5_retries = max(0, int(os.environ.get("FB_ESTABLISH_F5_RETRIES", "2")))
    for attempt in range(1, f5_retries + 1):
        recovered = _establish_f5_recovery_pass(
            page,
            account,
            cookie_path=cp,
            allow_form_login=allow_form_login,
            form_recover_fn=form_recover_fn,
            _persist_ok=_persist_ok,
            aid=aid,
            attempt=attempt,
        )
        if recovered is not None:
            return recovered

    return False, failure_reason


def _establish_f5_recovery_pass(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path,
    allow_form_login: bool,
    form_recover_fn: Callable[[], bool] | None,
    _persist_ok: Callable[[str, str], tuple[bool, str]],
    aid: str,
    attempt: int,
) -> tuple[bool, str] | None:
    """Một vòng F5 + thử lại profile/cookie/probe (và form nếu được phép)."""
    from src.services.facebook_session_recovery import reload_facebook_page_f5

    logger.info("[FB session] establish F5 retry {} account={}", attempt, aid)
    reload_facebook_page_f5(page, label=f"establish_f5_{attempt}")

    ok_prof, det_prof = wait_profile_session_ready(page, account, timeout_ms=12_000)
    if ok_prof:
        return _persist_ok(det_prof, f"establish_f5_profile_{attempt}")

    if cookie_file_has_session(cookie_path):
        ok_ck, det_ck = try_reuse_saved_cookie_session(
            page, account, cookie_path=cookie_path, timeout_ms=22_000
        )
        if ok_ck:
            return _persist_ok(det_ck, f"establish_f5_cookie_{attempt}")

    ok_probe, det_probe = probe_existing_facebook_session(
        page, account, cookie_path=cookie_path, timeout_ms=16_000
    )
    if ok_probe:
        return _persist_ok(det_probe, f"establish_f5_probe_{attempt}")

    if allow_form_login and form_recover_fn is not None:
        if form_recover_fn():
            from src.services.facebook_session_recovery import confirm_facebook_session_logged_in

            ok_conf, det_conf = confirm_facebook_session_logged_in(page, account, timeout_ms=18_000)
            if ok_conf:
                return _persist_ok(det_conf, f"establish_f5_form_{attempt}")
            ok_prof3, det3 = profile_session_ready_for_interaction(page, account)
            if ok_prof3:
                return _persist_ok(det3, f"establish_f5_form_profile_{attempt}")

    return None


def ensure_session_before_interaction(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    recover_fn: Callable[[], bool] | None = None,
) -> tuple[bool, str]:
    """Cổng trước tương tác — ủy quyền ``establish_facebook_session``."""
    return establish_facebook_session(
        page,
        account,
        cookie_path=cookie_path,
        allow_form_login=recover_fn is not None,
        form_recover_fn=recover_fn,
    )


def last_resort_interaction_session(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    allow_form_login: bool = False,
    form_recover_fn: Callable[[], bool] | None = None,
) -> tuple[bool, str]:
    """Alias tương thích — cùng luồng ``establish_facebook_session``."""
    return establish_facebook_session(
        page,
        account,
        cookie_path=cookie_path,
        allow_form_login=allow_form_login,
        form_recover_fn=form_recover_fn if allow_form_login else None,
    )


def try_reuse_saved_cookie_session(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    timeout_ms: int = 22_000,
) -> tuple[bool, str]:
    """
    Nạp cookie file đã lưu + xác nhận phiên — **không** mở form email/password.

    Returns:
        ``(True, mô_tả)`` khi vào được Facebook; ``(False, lý_do)`` nếu không.
    """
    from src.automation.facebook_actions import login_with_cookie
    from src.services.facebook_session_recovery import confirm_facebook_session_logged_in

    cp = ensure_account_cookie_path(account, cookie_path)

    ok_prof, det_prof = profile_session_ready_for_interaction(page, account)
    if ok_prof:
        logger.info("[FB session] try_reuse — phiên profile sẵn có: {}", det_prof)
        return True, det_prof

    if not cookie_file_has_session(cp):
        return False, "Không có file cookie c_user"

    ok0, det0 = confirm_facebook_session_logged_in(
        page, account, timeout_ms=min(8_000, timeout_ms)
    )
    if ok0:
        return True, det0

    try:
        login_with_cookie(page, cp)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[FB session] try_reuse login_with_cookie: {}", exc)
        return False, str(exc)[:120]

    ok_after, det_after = profile_session_ready_for_interaction(page, account)
    if ok_after:
        return True, det_after

    ok1, det1 = confirm_facebook_session_logged_in(page, account, timeout_ms=timeout_ms)
    if ok1:
        return True, det1

    ok2, det2 = probe_existing_facebook_session(
        page, account, cookie_path=cp, timeout_ms=min(18_000, timeout_ms)
    )
    return ok2, det2 or det1 or "Chưa vào được sau nạp cookie"


def probe_existing_facebook_session(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    timeout_ms: int = 30_000,
) -> tuple[bool, str]:
    """
    Quét phiên đã có trong profile (đăng nhập tay / phiên cũ) trước khi nạp cookie file hay form login.

    Returns:
        ``(True, mô_tả)`` khi ``confirm_facebook_session_logged_in`` thành công; ``(False, lý_do)`` nếu không.
    """
    from src.automation.facebook_actions import (
        _facebook_context_cookie_names,
        _force_www_facebook_if_mobile_redirect,
        prime_facebook_session_page,
    )
    from src.services.facebook_session_recovery import confirm_facebook_session_logged_in

    ok_fast, fast_detail = wait_profile_session_ready(
        page, account, timeout_ms=min(10_000, int(timeout_ms))
    )
    if ok_fast:
        logger.info("[FB session] Probe nhanh — phiên profile: {}", fast_detail)
        return True, fast_detail

    u = (page.url or "").lower()
    if "facebook.com" not in u:
        try:
            prime_facebook_session_page(page)
        except Exception as exc:  # noqa: BLE001
            logger.debug("[FB session] probe prime: {}", exc)
    else:
        _force_www_facebook_if_mobile_redirect(page)

    per_try = max(6_000, int(timeout_ms) // 3)
    last_detail = "Profile chưa có cookie c_user"
    for attempt in range(3):
        try:
            page.wait_for_timeout(900 if attempt == 0 else 600)
        except Exception:
            pass
        names = _facebook_context_cookie_names(page)
        if "c_user" not in names:
            if attempt < 2:
                try:
                    page.reload(wait_until="domcontentloaded", timeout=45_000)
                    _force_www_facebook_if_mobile_redirect(page)
                except Exception as reload_exc:  # noqa: BLE001
                    logger.debug("[FB session] probe reload: {}", reload_exc)
                continue
            return False, last_detail

        if attempt > 0:
            try:
                page.reload(wait_until="domcontentloaded", timeout=45_000)
                _force_www_facebook_if_mobile_redirect(page)
                page.wait_for_timeout(800)
            except Exception as reload_exc:  # noqa: BLE001
                logger.debug("[FB session] probe reload (c_user): {}", reload_exc)

        ok, detail = confirm_facebook_session_logged_in(page, account, timeout_ms=per_try)
        if ok:
            logger.info(
                "[FB session] Probe phát hiện phiên profile account={}: {}",
                account.get("id"),
                detail,
            )
            return True, detail
        last_detail = detail or last_detail

    return False, last_detail


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
        login_with_cookie,
        navigate_away_from_login_if_session_active,
        prime_facebook_session_page,
    )

    aid = str(account.get("id") or "").strip()
    prof = str(account.get("portable_path") or account.get("profile_path") or "").strip()
    cp = ensure_account_cookie_path(account, cookie_path)
    prev_mobile = os.environ.pop("TOOLFB_NAV_MOBILE_FB", None)
    try:
        ok_pre, det_pre = wait_profile_session_ready(page, account, timeout_ms=4_500)
        if ok_pre:
            cur = (page.url or "").lower()
            if "facebook.com" not in cur:
                prime_facebook_session_page(page)
            else:
                _force_www_facebook_if_mobile_redirect(page)
                navigate_away_from_login_if_session_active(page)
            logger.info(
                "[FB session] Profile sẵn có trước prime account={}: {}",
                aid,
                det_pre,
            )
            return True, "profile"

        if prime or "facebook.com" not in (page.url or "").lower():
            prime_facebook_session_page(page)
        else:
            _force_www_facebook_if_mobile_redirect(page)
            navigate_away_from_login_if_session_active(page)

        ok_wait, wait_detail = wait_profile_session_ready(page, account, timeout_ms=10_000)
        if ok_wait:
            logger.info(
                "[FB session] Profile đăng nhập account={} profile={} — {}",
                aid,
                prof or "(mặc định)",
                wait_detail,
            )
            return True, "profile"

        try:
            page.reload(wait_until="domcontentloaded", timeout=45_000)
            _force_www_facebook_if_mobile_redirect(page)
            navigate_away_from_login_if_session_active(page)
        except Exception as reload_exc:  # noqa: BLE001
            logger.debug("[FB session] reload sau prime: {}", reload_exc)

        ok_wait2, wait_detail2 = wait_profile_session_ready(page, account, timeout_ms=6_000)
        if ok_wait2:
            logger.info("[FB session] Profile đăng nhập sau reload account={}: {}", aid, wait_detail2)
            return True, "profile"

        if cookie_file_has_session(cp):
            logger.info("[FB session] Thử nạp cookie file account={} → {}", aid, cp)
            ok_prof_ck, det_prof_ck = profile_session_ready_for_interaction(page, account)
            if ok_prof_ck:
                logger.info("[FB session] Bỏ qua nạp file — profile đã có phiên: {}", det_prof_ck)
                return True, "profile"
            try:
                login_with_cookie(page, cp)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[FB session] login_with_cookie thất bại account={}: {}", aid, exc)
            ok_ck, det_ck = profile_session_ready_for_interaction(page, account)
            if ok_ck:
                return True, "cookie"

        ok_probe, probe_detail = probe_existing_facebook_session(
            page,
            account,
            cookie_path=cp,
            timeout_ms=22_000,
        )
        if ok_probe:
            return True, "profile_probe"
        url = (page.url or "")[:100]
        if "/login" in url.lower():
            return False, "Trang login — profile chưa có phiên (kiểm tra portable_path trong accounts.json)"
        return False, probe_detail or "Chưa thấy phiên hợp lệ sau profile + cookie file"
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
