"""
Khôi phục phiên Facebook bằng email + mật khẩu + TOTP (user tự cấu hình).

Checkpoint: thử **email khôi phục** (nếu có) hoặc **reCAPTCHA qua CapSolver** (nếu cấu hình API key).
Captcha / tài khoản bị khóa — chuyển ``need_manual_check``.
"""

from __future__ import annotations

import contextlib
import contextvars
import os
import random
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal, Union

_manual_captcha_notifier: contextvars.ContextVar[Callable[[str], None] | None] = contextvars.ContextVar(
    "fb_manual_captcha_notifier",
    default=None,
)


@contextlib.contextmanager
def manual_captcha_notifier(callback: Callable[[str], None] | None):
    """
    Gắn callback thông báo GUI/worker khi cần tick captcha tay (thread-safe theo luồng worker).
    """
    token = _manual_captcha_notifier.set(callback)
    try:
        yield
    finally:
        _manual_captcha_notifier.reset(token)


def _emit_manual_captcha_notify(message: str) -> None:
    cb = _manual_captcha_notifier.get()
    if cb is None:
        return
    try:
        cb(message)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[FB recovery] manual_captcha notify: {}", exc)

from playwright.sync_api import Frame, Page

from loguru import logger
from src.services.totp_service import generate_totp_code
from src.utils.account_credentials import AccountCredentialBundle, load_account_credential_bundle
from src.utils.paths import project_root

PostLoginState = Literal["logged_in", "totp", "checkpoint", "timeout"]
TotpSubmitOutcome = Literal["logged_in", "remember_browser", "totp", "checkpoint", "timeout"]

_UNSUPPORTED_URL_MARKERS = (
    "/checkpoint/",
    "checkpoint?",
    "/recover/",
    "recover/initiate",
    "accountquality",
    "suspended",
    "captcha",
    "confirm_identity",
)

_TOTP_URL_MARKERS = (
    "two_step",
    "two_step_verification",
    "two-factor",
    "two_factor",
    "approvals_code",
    "2fa",
)

_TOTP_TEXT_MARKERS = (
    "authentication code",
    "authentication app",
    "go to your authentication app",
    "enter the 6-digit",
    "6-digit code",
    "authenticator app",
    "ứng dụng xác thực",
    "two-factor",
    "two factor",
    "security code",
    "login code",
    "mã xác thực",
    "mã đăng nhập",
    "xác thực hai lớp",
    "xác minh hai bước",
    "nhập mã",
    "6 chữ số",
    "6 chu so",
    "google authenticator",
    "duo mobile",
)

_EMAIL_SELECTORS = (
    "input#email",
    "input#m_login_email",
    "input[name='email']",
    "input[name='login']",
    "input[name='identification']",
    "input[name='username']",
    "input[type='email']",
    "input[type='text'][name='email']",
    "input[type='text'][autocomplete='username']",
    "input[autocomplete='username']",
    "input[data-testid='royal_email']",
    "input[data-testid='login_form_input']",
    "input[placeholder*='email' i]",
    "input[placeholder*='phone' i]",
    "input[placeholder*='mobile' i]",
    "input[placeholder*='số điện thoại' i]",
    "input[placeholder*='tài khoản' i]",
    "input[aria-label*='email' i]",
    "input[aria-label*='phone' i]",
    "input[aria-label*='mobile' i]",
    "input[aria-label*='số điện thoại' i]",
    "input[aria-label*='tài khoản' i]",
    "input[aria-label*='Email hoặc' i]",
    "input[aria-label*='email or phone' i]",
    "input[placeholder*='Email hoặc' i]",
    "form input[type='text']:visible",
    "form[data-testid='royal_login_form'] input[type='text']",
)

_SWITCH_LOGIN_ACCOUNT_SELECTORS = (
    "a:has-text('Log into another account')",
    "a:has-text('Log in to another account')",
    "a:has-text('Use another account')",
    "div[role='button']:has-text('Use another account')",
    "a:has-text('Đăng nhập tài khoản khác')",
    "div[role='button']:has-text('Đăng nhập tài khoản khác')",
    "span:has-text('Đăng nhập tài khoản khác')",
    "[data-testid='login_continue_as_another_account']",
)

_LOGIN_CONTINUE_AFTER_ID_SELECTORS = (
    "button:has-text('Continue')",
    "button:has-text('Tiếp tục')",
    "div[role='button']:has-text('Continue')",
    "div[role='button']:has-text('Tiếp tục')",
    "button[name='login']",
    "button[type='submit']",
)

_PASSWORD_SELECTORS = (
    "input#pass",
    "input#m_login_pass",
    "input[name='pass']",
    "input[type='password']",
    "input[autocomplete='current-password']",
    "input[data-testid='royal_pass']",
    "input[placeholder*='password' i]",
    "input[placeholder*='mật khẩu' i]",
    "input[aria-label*='password' i]",
    "input[aria-label*='mật khẩu' i]",
)

_LOGIN_BUTTON_SELECTORS = (
    "button[name='login']",
    "button[type='submit']",
    "input[type='submit']",
    "div[role='button']:has-text('Log in')",
    "div[role='button']:has-text('Đăng nhập')",
)

_TOTP_INPUT_SELECTORS = (
    "input[name='approvals_code']",
    "input#approvals_code",
    "input[autocomplete='one-time-code']",
    "input[data-testid='approvals_code']",
    "input[aria-label='Mã']",
    "input[aria-label='Code']",
    "input[aria-label*='code' i]",
    "input[aria-label*='mã' i]",
    "input[aria-label*='xác thực' i]",
    "input[placeholder*='code' i]",
    "input[placeholder*='mã' i]",
    "input[inputmode='numeric']",
    "input[inputmode='tel']",
    "input[type='text'][maxlength='6']",
    "input[type='text'][maxlength='8']",
    "input[type='tel'][maxlength='6']",
    "input[type='tel'][maxlength='8']",
    "input[type='number']",
)

_REMEMBER_BROWSER_MARKERS = (
    "remember_browser",
    "save_browser",
    "trusted_browser",
    "trust_this_browser",
)

# Màn sau đăng nhập / TOTP: «Bạn đã đăng nhập. Tin cậy thiết bị này?»
_TRUST_DEVICE_BODY_MARKERS = (
    "tin cậy thiết bị",
    "tin tuong thiet bi",
    "trust this device",
    "trust this browser",
    "bạn đã đăng nhập. tin cậy",
    "you've logged in. trust",
    "you logged in. trust",
)

_TRUST_DEVICE_PRIMARY_SELECTORS = (
    "button:has-text('Tin cậy thiết bị này')",
    "div[role='button']:has-text('Tin cậy thiết bị này')",
    "[role='button']:has-text('Tin cậy thiết bị này')",
    "button:has-text('Tin cậy thiết bị')",
    "div[role='button']:has-text('Tin cậy thiết bị')",
    "button:has-text('Trust this device')",
    "div[role='button']:has-text('Trust this device')",
    "[role='button']:has-text('Trust this device')",
)

_REMEMBER_BROWSER_SUBMIT_SELECTORS = (
    *_TRUST_DEVICE_PRIMARY_SELECTORS,
    "button:has-text('Tiếp tục')",
    "button:has-text('Continue')",
    "div[role='button']:has-text('Tiếp tục')",
    "div[role='button']:has-text('Continue')",
    "button:has-text('Lưu trình duyệt')",
    "button:has-text('Save browser')",
    "div[role='button']:has-text('Lưu trình')",
    "div[role='button']:has-text('Save browser')",
    "button:has-text('OK')",
    "div[role='button']:has-text('OK')",
    "button:has-text('Xong')",
    "div[role='button']:has-text('Xong')",
)

_TOTP_SUBMIT_SELECTORS = (
    "button[type='submit']",
    "button[name='submit[Continue]']",
    "button:has-text('Tiếp tục')",
    "button:has-text('Continue')",
    "div[role='button']:has-text('Continue')",
    "div[role='button']:has-text('Tiếp')",
    "div[role='button']:has-text('Submit')",
    "div[role='button']:has-text('Xác nhận')",
    "div[role='button']:has-text('Gửi')",
    "div[role='button']:has-text('Tiếp tục')",
    "[role='button']:has-text('Tiếp tục')",
)

_RECOVERY_EMAIL_BLOCKS = ("captcha", "suspended", "accountquality")

_RECOVERY_EMAIL_EXTRA_SELECTORS = (
    "input[name*='recovery' i]",
    "input[id*='recovery' i]",
    "input[placeholder*='recovery' i]",
    "input[aria-label*='recovery' i]",
    "input[aria-label*='khôi phục' i]",
    "input[aria-label*='dự phòng' i]",
    "input[aria-label*='contact' i]",
)


def _url_lower(page: Page) -> str:
    return str(page.url or "").strip().lower()


def facebook_page_is_hard_checkpoint(url: str) -> bool:
    """Checkpoint / captcha / xác minh danh tính — không tự xử lý (trừ thử email khôi phục)."""
    u = (url or "").strip().lower()
    if "facebook.com" not in u:
        return False
    return any(m in u for m in _UNSUPPORTED_URL_MARKERS)


def facebook_page_blocks_recovery_email(url: str) -> bool:
    """Captcha / suspended — không thử email khôi phục."""
    u = (url or "").strip().lower()
    if "facebook.com" not in u:
        return True
    return any(m in u for m in _RECOVERY_EMAIL_BLOCKS)


def facebook_page_is_remember_browser(url: str) -> bool:
    """Sau TOTP thành công — Meta hỏi «Lưu trình duyệt» (không phải màn nhập mã)."""
    u = (url or "").strip().lower()
    if "facebook.com" not in u:
        return False
    return any(m in u for m in _REMEMBER_BROWSER_MARKERS)


def facebook_page_is_trust_device_prompt(page: Page) -> bool:
    """
    Màn «Tin cậy thiết bị này» / Trust this device (URL remember_browser hoặc nội dung trang).

    Không nhầm với form nhập mã TOTP.
    """
    u = _url_lower(page)
    if "facebook.com" not in u:
        return False
    if facebook_page_is_remember_browser(u):
        return True
    try:
        body = (page.locator("body").inner_text(timeout=2_000) or "").lower()
    except Exception:
        body = ""
    if not body:
        return False
    if any(m in body for m in _TOTP_TEXT_MARKERS) and "tin cậy thiết bị" not in body:
        if _locate_totp_input(page, timeout_ms=400) is not None:
            return False
    if any(m in body for m in _TRUST_DEVICE_BODY_MARKERS):
        return True
    if "bạn đã đăng nhập" in body and ("tin cậy" in body or "trust this" in body):
        return True
    return False


def _auth_url_is_pre_captcha_gate(url: str) -> bool:
    """URL ``two_step_verification/authentication`` bước captcha (chưa phải TOTP)."""
    u = str(url or "").lower()
    return (
        "two_step" in u
        and "authentication" in u
        and ("pre_authentication" in u or "flow=pre_authentication" in u)
    )


def _recaptcha_still_blocking_auth(page: Page) -> bool:
    """
    Widget reCAPTCHA còn chặn nhận màn TOTP.

    Sau tick tay, iframe/textarea ``g-recaptcha-response`` có thể vẫn còn trong DOM.
    """
    try:
        from src.services.facebook_recaptcha import facebook_page_has_recaptcha
    except Exception:
        return False
    if not facebook_page_has_recaptcha(page):
        return False
    if _auth_url_is_pre_captcha_gate(page.url or ""):
        return True
    if _locate_totp_input(page, timeout_ms=500) is not None:
        return False
    try:
        body = (page.locator("body").inner_text(timeout=1_500) or "").lower()
    except Exception:
        body = ""
    if any(m in body for m in _TOTP_TEXT_MARKERS):
        return False
    try:
        filled = page.evaluate(
            """() => {
              const el = document.querySelector('textarea[name="g-recaptcha-response"]');
              return !!(el && String(el.value || '').length > 24);
            }"""
        )
        if filled:
            return False
    except Exception:
        pass
    return True


def _mark_auth_flow_active(account: dict[str, Any] | None) -> None:
    """Đánh dấu đang trong luồng 2FA/captcha — worker không reset form login."""
    if account is not None:
        account["_fb_auth_flow_active"] = True


def facebook_auth_flow_was_active(account: dict[str, Any] | None) -> bool:
    """True nếu phiên vừa qua bước captcha/2FA Meta (tránh gọi lại ``_goto_facebook_login``)."""
    return bool((account or {}).get("_fb_auth_flow_active"))


def _clear_auth_flow_active(account: dict[str, Any] | None) -> None:
    if account is not None:
        account.pop("_fb_auth_flow_active", None)


def facebook_page_looks_like_totp_prompt(page: Page) -> bool:
    u = _url_lower(page)
    if "facebook.com" not in u:
        return False
    if facebook_page_is_trust_device_prompt(page):
        return False
    if _recaptcha_still_blocking_auth(page):
        return False
    if facebook_page_is_hard_checkpoint(u):
        return False
    if _locate_totp_input(page, timeout_ms=800) is not None:
        return True
    if "two_factor" in u or "/two_factor/" in u:
        return True
    if "approvals_code" in u:
        return True
    if "two_step_verification" in u and "remember" not in u:
        if "authentication" in u:
            if _auth_url_is_pre_captcha_gate(u):
                return False
            if "post_authentication" in u or "flow=post_authentication" in u:
                return True
        else:
            return True
    try:
        body = (page.locator("body").inner_text(timeout=2_000) or "").lower()
    except Exception:
        body = ""
    if any(m in body for m in _TOTP_TEXT_MARKERS):
        if "remember" in body and "browser" in body and "nhập mã" not in body:
            return False
        return True
    return False


def _dismiss_remember_browser(page: Page, *, attempts: int = 3) -> bool:
    """Bấm «Tin cậy thiết bị này» / Tiếp tục trên màn tin cậy thiết bị sau đăng nhập."""
    if not facebook_page_is_trust_device_prompt(page):
        return False
    logger.info(
        "[FB recovery] Màn tin cậy thiết bị — url={} | sẽ bấm «Tin cậy thiết bị này» nếu có",
        page.url,
    )
    for attempt in range(max(1, int(attempts))):
        _recovery_pause(label=f"trước tin cậy thiết bị (lần {attempt + 1})", kind="step")
        clicked = False
        for ctx in _totp_browser_contexts(page):
            if _click_first(ctx, _TRUST_DEVICE_PRIMARY_SELECTORS, label="Tin cậy thiết bị"):
                clicked = True
                logger.info("[FB recovery] Đã bấm «Tin cậy thiết bị này».")
                break
            if _click_first(ctx, _REMEMBER_BROWSER_SUBMIT_SELECTORS, label="remember_browser"):
                clicked = True
                break
            for name_pat in (
                re.compile(r"tin cậy thiết bị", re.I),
                re.compile(r"trust this device", re.I),
                re.compile(r"tiếp tục", re.I),
                re.compile(r"continue", re.I),
                re.compile(r"lưu", re.I),
                re.compile(r"save", re.I),
                re.compile(r"xong", re.I),
                re.compile(r"not now", re.I),
                re.compile(r"không phải bây giờ", re.I),
            ):
                try:
                    btn = ctx.get_by_role("button", name=name_pat)
                    if btn.count() and btn.first.is_visible(timeout=2_000):
                        btn.first.click(timeout=5_000)
                        clicked = True
                        break
                except Exception:
                    continue
        if clicked:
            _recovery_pause(label="sau remember_browser", kind="step")
            try:
                page.wait_for_load_state("domcontentloaded", timeout=8_000)
            except Exception:
                pass
            page.wait_for_timeout(600)
            if _session_logged_in(page):
                return True
            if not facebook_page_is_trust_device_prompt(page):
                return True
        page.wait_for_timeout(500)
    return _session_logged_in(page) or not facebook_page_is_trust_device_prompt(page)


def _raise_manual(reason: str) -> None:
    raise RuntimeError(f"{reason} need_manual_check")


def _session_logged_in(page: Page) -> bool:
    from src.automation.facebook_actions import facebook_session_appears_logged_in

    return facebook_session_appears_logged_in(page)


def _recovery_flow_advanced(page: Page) -> bool:
    """
    Đã qua captcha / bước trung gian — có cookie, TOTP, hoặc không còn màn authentication+captcha.
    """
    if _read_facebook_c_user(page):
        return True
    if facebook_page_looks_like_totp_prompt(page):
        return True
    u = _url_lower(page)
    if "two_factor" in u:
        return True
    if "two_step" in u and not _auth_url_is_pre_captcha_gate(u):
        if not _recaptcha_still_blocking_auth(page):
            return True
    return _session_logged_in(page)


def _read_facebook_c_user(page: Page) -> str:
    """Giá trị cookie ``c_user`` (UID Facebook) trong context hiện tại."""
    try:
        for c in page.context.cookies():
            dom = str(c.get("domain", "")).lower()
            if "facebook" not in dom:
                continue
            if str(c.get("name", "")).strip().lower() == "c_user":
                return str(c.get("value", "")).strip()
    except Exception:
        pass
    return ""


def _normalize_facebook_uid(value: str | None) -> str:
    """Chuẩn hóa UID Facebook (``UID_100…`` → ``100…``) để so khớp cookie ``c_user``."""
    s = str(value or "").strip()
    if not s:
        return ""
    m = re.fullmatch(r"UID_(\d+)", s, re.I)
    if m:
        return m.group(1)
    if s.isdigit():
        return s
    digits = re.sub(r"\D", "", s)
    if digits and s.upper().startswith("UID_"):
        return digits
    return s


def _facebook_uids_match(actual: str, expected: str) -> bool:
    """So khớp ``c_user`` với UID cấu hình (bỏ tiền tố ``UID_``, chỉ so phần số)."""
    exp = _normalize_facebook_uid(expected)
    if not exp:
        return True
    act = _normalize_facebook_uid(actual)
    if not act:
        return False
    return act == exp


def _expected_facebook_uid(account: dict[str, Any] | None) -> str:
    if not account:
        return ""
    bundle = load_account_credential_bundle(account)
    if bundle and bundle.facebook_uid:
        return _normalize_facebook_uid(bundle.facebook_uid)
    uid = str(account.get("facebook_uid") or "").strip()
    if uid:
        return _normalize_facebook_uid(uid)
    aid = str(account.get("id") or "").strip()
    if re.fullmatch(r"UID_\d+", aid, re.I):
        return _normalize_facebook_uid(aid)
    return ""


def confirm_facebook_session_logged_in(
    page: Page,
    account: dict[str, Any] | None = None,
    *,
    timeout_ms: int = 25_000,
) -> tuple[bool, str]:
    """
    Xác nhận đã vào tài khoản (không còn login/2FA, có phiên hợp lệ; khớp UID nếu có).

    Returns:
        ``(True, mô_tả)`` khi ổn định 2 lần kiểm tra liên tiếp; ``(False, lý_do)`` nếu không.
    """
    import os

    from src.automation.facebook_actions import (
        _fb_normalize_client_url,
        _log_facebook_session_diagnostic,
        assert_safe_facebook_navigation_url,
    )

    raw_to = os.environ.get("FB_LOGIN_CONFIRM_TIMEOUT_MS", "").strip()
    if raw_to:
        try:
            timeout_ms = max(5_000, int(raw_to))
        except ValueError:
            pass

    expected_uid = _expected_facebook_uid(account)
    deadline = time.time() + max(3.0, timeout_ms / 1000.0)
    stable_hits = 0
    last_detail = "Chưa thấy bảng tin / menu tài khoản"
    navigated_home = False

    while time.time() < deadline:
        u = _url_lower(page)
        if facebook_page_is_hard_checkpoint(u):
            last_detail = f"Vẫn ở checkpoint ({page.url})"
            stable_hits = 0
            page.wait_for_timeout(500)
            continue
        if facebook_page_looks_like_totp_prompt(page):
            last_detail = f"Vẫn ở màn nhập mã 2FA ({page.url})"
            stable_hits = 0
            page.wait_for_timeout(500)
            continue
        if facebook_page_is_trust_device_prompt(page):
            _dismiss_remember_browser(page)
            stable_hits = 0
            page.wait_for_timeout(500)
            continue

        if not navigated_home and (
            "facebook.com" not in u or "/login" in u or "two_step" in u or "two_factor" in u
        ):
            try:
                home = _fb_normalize_client_url("https://www.facebook.com/")
                assert_safe_facebook_navigation_url(home, label="confirm_login")
                page.goto(home, wait_until="domcontentloaded", timeout=60_000)
                navigated_home = True
                page.wait_for_timeout(800)
            except Exception as nav_exc:  # noqa: BLE001
                logger.warning("[FB recovery] Không mở được trang chủ khi xác nhận login: {}", nav_exc)
                navigated_home = True

        if not _session_logged_in(page):
            last_detail = "Chưa có phiên hợp lệ (cookie/UI đăng nhập)"
            stable_hits = 0
            page.wait_for_timeout(500)
            continue

        c_user = _read_facebook_c_user(page)
        if expected_uid and c_user and not _facebook_uids_match(c_user, expected_uid):
            last_detail = f"Cookie c_user={c_user} không khớp UID {expected_uid}"
            stable_hits = 0
            page.wait_for_timeout(500)
            continue

        stable_hits += 1
        if stable_hits >= 2:
            if c_user:
                return True, f"Đã vào tài khoản Facebook (UID {c_user})"
            return True, "Đã vào tài khoản Facebook"
        page.wait_for_timeout(650)

    if not navigated_home:
        try:
            home = _fb_normalize_client_url("https://www.facebook.com/")
            assert_safe_facebook_navigation_url(home, label="confirm_login_final")
            page.goto(home, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1000)
        except Exception:
            pass

    if _session_logged_in(page):
        c_user = _read_facebook_c_user(page)
        if expected_uid and c_user and not _facebook_uids_match(c_user, expected_uid):
            _log_facebook_session_diagnostic(page, stage="confirm_uid_mismatch")
            return False, f"UID cookie ({c_user}) khác UID cấu hình ({expected_uid})"
        if c_user:
            return True, f"Đã vào tài khoản Facebook (UID {c_user})"
        return True, "Đã vào tài khoản Facebook"

    _log_facebook_session_diagnostic(page, stage="confirm_login_failed")
    return False, last_detail


def _recovery_pause(*, label: str = "", kind: str = "input") -> None:
    """Delay giữa nhập / bấm nút login (dùng env FB_INPUT_DELAY / FB_CLICK_DELAY / FB_STEP_DELAY)."""
    from src.automation.facebook_actions import human_pause

    human_pause(label=label or "recovery", kind=kind)


_BrowserContext = Union[Page, Frame]


def _first_visible(ctx: _BrowserContext, selectors: tuple[str, ...], *, timeout_ms: int = 2_000):
    for sel in selectors:
        try:
            loc = ctx.locator(sel)
            if loc.count() and loc.first.is_visible(timeout=timeout_ms):
                return loc.first
        except Exception:
            continue
    return None


def _login_browser_contexts(page: Page) -> list[_BrowserContext]:
    """Form đăng nhập đôi khi nằm trong iframe (checkpoint / mobile)."""
    out: list[_BrowserContext] = [page]
    try:
        for fr in page.frames:
            if fr == page.main_frame:
                continue
            u = str(fr.url or "").lower()
            if "facebook.com" in u or "/login" in u:
                out.append(fr)
    except Exception:
        pass
    return out


def _reveal_login_email_field(page: Page) -> bool:
    """
    Đảm bảo ô email/UID hiện — bấm «tài khoản khác» nếu Meta chỉ hiện ô mật khẩu (profile đã lưu).
    """
    if _locate_login_email_input(page, timeout_ms=800) is not None:
        return True
    if _locate_login_password_input(page, timeout_ms=600) is None:
        return False
    logger.info(
        "[FB recovery] Form chỉ có mật khẩu — thử mở lại nhập email/UID (url={}).",
        page.url,
    )
    for ctx in _login_browser_contexts(page):
        if _click_first(ctx, _SWITCH_LOGIN_ACCOUNT_SELECTORS, label="đăng nhập tài khoản khác"):
            break
    _dismiss_login_obstacles(page)
    try:
        page.wait_for_timeout(1_200)
    except Exception:
        time.sleep(1.2)
    return _locate_login_email_input(page, timeout_ms=2_500) is not None


def _locate_login_email_input(page: Page, *, timeout_ms: int = 1_200):
    """Tìm ô email / UID / SĐT trên form login (kể cả iframe)."""
    label_patterns = (
        re.compile(r"email", re.I),
        re.compile(r"phone", re.I),
        re.compile(r"mobile", re.I),
        re.compile(r"số điện thoại", re.I),
        re.compile(r"tài khoản", re.I),
        re.compile(r"uid", re.I),
    )
    for ctx in _login_browser_contexts(page):
        el = _first_visible(ctx, _EMAIL_SELECTORS, timeout_ms=timeout_ms)
        if el is not None:
            return el
        for pat in label_patterns:
            try:
                loc = ctx.get_by_label(pat)
                if loc.count() and loc.first.is_visible(timeout=timeout_ms):
                    return loc.first
            except Exception:
                continue
        try:
            role_box = ctx.get_by_role("textbox")
            n = min(role_box.count(), 8)
            first_non_password = None
            for i in range(n):
                cand = role_box.nth(i)
                if not cand.is_visible(timeout=400):
                    continue
                meta = " ".join(
                    filter(
                        None,
                        [
                            cand.get_attribute("aria-label") or "",
                            cand.get_attribute("placeholder") or "",
                            cand.get_attribute("name") or "",
                            cand.get_attribute("id") or "",
                            cand.get_attribute("type") or "",
                        ],
                    )
                ).lower()
                if "password" in meta or "pass" in meta or "mật khẩu" in meta:
                    continue
                if any(
                    k in meta
                    for k in ("email", "phone", "mobile", "số điện", "tài khoản", "username", "uid")
                ):
                    return cand
                if first_non_password is None:
                    first_non_password = cand
            if first_non_password is not None:
                return first_non_password
        except Exception:
            pass
    return None


def _locate_login_password_input(page: Page, *, timeout_ms: int = 1_200):
    """Tìm ô mật khẩu trên form login."""
    for ctx in _login_browser_contexts(page):
        el = _first_visible(ctx, _PASSWORD_SELECTORS, timeout_ms=timeout_ms)
        if el is not None:
            return el
        for pat in (re.compile(r"password", re.I), re.compile(r"mật khẩu", re.I)):
            try:
                loc = ctx.get_by_label(pat)
                if loc.count() and loc.first.is_visible(timeout_ms=timeout_ms):
                    return loc.first
            except Exception:
                continue
    return None


def _dismiss_login_obstacles(page: Page) -> None:
    """Đóng banner cookie / popup che form login nếu có."""
    for ctx in _login_browser_contexts(page):
        for sel in (
            "button[data-cookiebanner='accept_button']",
            "button:has-text('Allow all cookies')",
            "button:has-text('Cho phép tất cả cookie')",
            "button:has-text('Chấp nhận tất cả')",
            "div[role='button']:has-text('Allow all cookies')",
        ):
            try:
                btn = ctx.locator(sel).first
                if btn.is_visible(timeout=600):
                    btn.click(timeout=2_500)
                    page.wait_for_timeout(400)
                    return
            except Exception:
                continue


def _login_form_wait_ms() -> int:
    raw = os.environ.get("FB_LOGIN_FORM_WAIT_MS", "22000").strip()
    try:
        return max(5_000, int(float(raw)))
    except (TypeError, ValueError):
        return 22_000


def _wait_for_login_form(
    page: Page,
    *,
    timeout_ms: int | None = None,
    account: dict[str, Any] | None = None,
) -> bool:
    """
    Chờ form login xuất hiện sau ``goto`` (Meta render chậm / proxy chậm).

    reCAPTCHA: một lần ``wait_and_auto_solve_facebook_recaptcha`` (module ``facebook_recaptcha``).

    Returns:
        True nếu thấy ô email hoặc đã đăng nhập sẵn.
    """
    from src.services.facebook_recaptcha import (
        facebook_page_has_recaptcha,
        facebook_page_on_recaptcha_flow_url,
        resolve_facebook_recaptcha,
    )

    wait_ms = timeout_ms if timeout_ms is not None else _login_form_wait_ms()
    if account and (
        facebook_page_has_recaptcha(page) or facebook_page_on_recaptcha_flow_url(page)
    ):
        resolve_facebook_recaptcha(
            page,
            account,
            stage="recovery:login",
            wait_timeout_ms=min(12_000, wait_ms),
        )
    deadline = time.time() + max(3.0, wait_ms / 1000.0)
    while time.time() < deadline:
        if _session_logged_in(page):
            return True
        if facebook_page_is_hard_checkpoint(_url_lower(page)):
            return False
        _dismiss_login_obstacles(page)
        if _reveal_login_email_field(page):
            return True
        page.wait_for_timeout(450)
    if _session_logged_in(page):
        return True
    return _reveal_login_email_field(page)


def _totp_browser_contexts(page: Page) -> list[_BrowserContext]:
    """Trang TOTP đôi khi nằm trong iframe con."""
    out: list[_BrowserContext] = [page]
    try:
        for fr in page.frames:
            if fr == page.main_frame:
                continue
            u = str(fr.url or "").lower()
            if "facebook.com" in u or "two_step" in u or "two_factor" in u:
                out.append(fr)
    except Exception:
        pass
    return out


def _locate_totp_input_in_context(ctx: _BrowserContext, *, timeout_ms: int = 1_200):
    el = _first_visible(ctx, _TOTP_INPUT_SELECTORS, timeout_ms=timeout_ms)
    if el is not None:
        return el

    label_patterns = (
        re.compile(r"^mã$", re.I),
        re.compile(r"code", re.I),
        re.compile(r"authentication", re.I),
        re.compile(r"xác thực", re.I),
    )
    for pat in label_patterns:
        try:
            loc = ctx.get_by_label(pat)
            if loc.count() and loc.first.is_visible(timeout=timeout_ms):
                return loc.first
        except Exception:
            continue

    try:
        role_box = ctx.get_by_role("textbox")
        n = min(role_box.count(), 8)
        for i in range(n):
            cand = role_box.nth(i)
            if not cand.is_visible(timeout=400):
                continue
            meta = " ".join(
                filter(
                    None,
                    [
                        cand.get_attribute("aria-label") or "",
                        cand.get_attribute("placeholder") or "",
                        cand.get_attribute("name") or "",
                        cand.get_attribute("id") or "",
                    ],
                )
            ).lower()
            if any(
                k in meta
                for k in ("mã", "code", "otp", "authenticat", "xác thực", "one-time", "2fa")
            ):
                return cand
            if "two_step" in str(getattr(ctx, "url", "") or "").lower():
                return cand
    except Exception:
        pass

    try:
        for sel in ("input[type='text']", "input[type='tel']", "input[type='number']"):
            loc = ctx.locator(sel)
            n = min(loc.count(), 6)
            for i in range(n):
                cand = loc.nth(i)
                if not cand.is_visible(timeout=400):
                    continue
                name = (cand.get_attribute("name") or "").lower()
                if any(x in name for x in ("email", "pass", "password", "user")):
                    continue
                atype = (cand.get_attribute("type") or "").lower()
                if atype in ("email", "password", "hidden"):
                    continue
                return cand
    except Exception:
        pass
    return None


def _try_fill_totp_digit_boxes(ctx: _BrowserContext, code: str) -> bool:
    """Một số giao diện Meta: 6 ô maxlength=1."""
    digits = [c for c in str(code or "") if c.isdigit()]
    if len(digits) != 6:
        return False
    try:
        boxes = ctx.locator(
            "input[maxlength='1'], input[aria-label*='digit' i], input[data-index]"
        )
        if boxes.count() < 6:
            return False
        from src.utils.human_typing import human_type_locator

        for i, d in enumerate(digits):
            box = boxes.nth(i)
            if not box.is_visible(timeout=800):
                return False
            human_type_locator(box, d, submit_enter=False, clear_first=True, label=f"TOTP digit {i + 1}")
        return True
    except Exception:
        return False


def _locate_totp_input(page: Page, *, timeout_ms: int = 12_000):
    """Chờ và tìm ô nhập mã 2FA (form mới: nhãn «Mã», URL two_step_verification)."""
    deadline = time.time() + max(2.0, timeout_ms / 1000.0)
    while time.time() < deadline:
        for ctx in _totp_browser_contexts(page):
            el = _locate_totp_input_in_context(ctx, timeout_ms=900)
            if el is not None:
                return el
        page.wait_for_timeout(350)
    return None


def _input_value_matches(el: Any, expected: str) -> bool:
    """Kiểm tra ô input đã nhận đủ giá trị (UID/email)."""
    exp = str(expected or "").strip()
    if not exp:
        return False
    try:
        current = (el.input_value(timeout=2_000) or "").strip()
    except Exception:
        return False
    if current == exp:
        return True
    if exp.isdigit() and current.isdigit() and current == exp:
        return True
    return exp in current or current.endswith(exp[-min(8, len(exp)) :])


def _fill_locator(el: Any, value: str, *, label: str = "", submit_enter: bool = False) -> bool:
    """Gõ giá trị từng ký tự; fallback ``fill``/``type`` nếu Meta chặn ``press``."""
    from src.utils.human_typing import human_type_locator

    payload = str(value or "").strip()
    if not payload:
        return False
    _recovery_pause(label=f"trước nhập {label}".strip(), kind="input")
    try:
        el.scroll_into_view_if_needed(timeout=5_000)
    except Exception:
        pass
    try:
        el.click(timeout=6_000)
    except Exception as click_exc:  # noqa: BLE001
        logger.warning("[FB recovery] Click ô {} thất bại: {}", label, click_exc)

    filled = False
    try:
        human_type_locator(
            el,
            payload,
            submit_enter=submit_enter,
            clear_first=True,
            label=label or "recovery",
        )
        filled = _input_value_matches(el, payload)
    except Exception as type_exc:  # noqa: BLE001
        logger.warning("[FB recovery] human_type {} thất bại: {}", label, type_exc)

    if not filled:
        try:
            el.fill(payload, timeout=5_000)
            filled = _input_value_matches(el, payload)
            if filled:
                logger.info("[FB recovery] Đã nhập {} bằng fill() fallback.", label)
        except Exception as fill_exc:  # noqa: BLE001
            logger.warning("[FB recovery] fill() {} thất bại: {}", label, fill_exc)

    if not filled:
        try:
            page = el.page
            el.click(timeout=3_000)
            page.keyboard.type(payload, delay=random.randint(40, 120))
            if submit_enter:
                page.keyboard.press("Enter")
            filled = _input_value_matches(el, payload)
            if filled:
                logger.info("[FB recovery] Đã nhập {} bằng keyboard.type fallback.", label)
        except Exception as kb_exc:  # noqa: BLE001
            logger.warning("[FB recovery] keyboard.type {} thất bại: {}", label, kb_exc)

    _recovery_pause(label=f"sau nhập {label}".strip(), kind="input")
    return filled


def _fill_first(
    ctx: _BrowserContext,
    selectors: tuple[str, ...],
    value: str,
    *,
    label: str = "",
    submit_enter: bool = False,
) -> bool:
    el = _first_visible(ctx, selectors)
    if el is None:
        return False
    return _fill_locator(el, value, label=label, submit_enter=submit_enter)


def _wait_after_totp_code_submit(
    page: Page,
    *,
    timeout_ms: int = 22_000,
) -> TotpSubmitOutcome:
    """
    Chờ Meta xử lý mã TOTP (navigate sang remember_browser / feed / vẫn ở form mã).

    Tránh báo lỗi sớm khi URL/DOM chưa kịp chuyển sau Enter.
    """
    deadline = time.time() + max(3.0, timeout_ms / 1000.0)
    last_totp = False
    while time.time() < deadline:
        u = _url_lower(page)
        if _session_logged_in(page):
            return "logged_in"
        if facebook_page_is_trust_device_prompt(page):
            return "remember_browser"
        if facebook_page_is_hard_checkpoint(u):
            return "checkpoint"
        if facebook_page_looks_like_totp_prompt(page):
            last_totp = True
            page.wait_for_timeout(450)
            continue
        if any(m in u for m in ("two_factor", "two_step", "approvals_code")):
            page.wait_for_timeout(450)
            continue
        page.wait_for_timeout(400)
    u = _url_lower(page)
    if _session_logged_in(page):
        return "logged_in"
    if facebook_page_is_trust_device_prompt(page):
        return "remember_browser"
    if facebook_page_is_hard_checkpoint(u):
        return "checkpoint"
    if last_totp or facebook_page_looks_like_totp_prompt(page):
        return "totp"
    return "timeout"


def _handle_remember_browser_after_totp(page: Page) -> bool:
    """Xử lý màn remember_browser; True nếu đã vào feed / thoát màn."""
    if not facebook_page_is_trust_device_prompt(page):
        return _session_logged_in(page)
    _dismiss_remember_browser(page)
    state = _wait_post_login_state(page, account=None, timeout_ms=18_000)
    return state == "logged_in" or _session_logged_in(page)


def _click_totp_continue(page: Page) -> bool:
    """Bấm «Tiếp tục» / Continue trên form 2FA."""
    if facebook_page_is_trust_device_prompt(page):
        return False
    for ctx in _totp_browser_contexts(page):
        if _click_first(ctx, _TOTP_SUBMIT_SELECTORS, label="xác nhận TOTP"):
            return True
        for name_pat in (re.compile(r"tiếp tục", re.I), re.compile(r"continue", re.I)):
            try:
                btn = ctx.get_by_role("button", name=name_pat)
                if btn.count():
                    cand = btn.first
                    if cand.is_visible(timeout=1_500):
                        _recovery_pause(label="trước nút Tiếp tục TOTP", kind="click")
                        cand.click(timeout=4_000)
                        _recovery_pause(label="sau nút Tiếp tục TOTP", kind="click")
                        return True
            except Exception:
                continue
    return False


def _click_first(ctx: _BrowserContext, selectors: tuple[str, ...], *, label: str = "") -> bool:
    el = _first_visible(ctx, selectors, timeout_ms=2_500)
    if el is None:
        return False
    _recovery_pause(label=f"trước bấm {label}".strip(), kind="click")
    try:
        el.click(timeout=3_500)
        _recovery_pause(label=f"sau bấm {label}".strip(), kind="click")
        return True
    except Exception:
        return False


def _goto_facebook_login(page: Page) -> None:
    from src.automation.facebook_actions import (
        _fb_normalize_client_url,
        _force_www_facebook_if_mobile_redirect,
        assert_safe_facebook_navigation_url,
    )

    from src.services.facebook_recaptcha import reset_recaptcha_network_capture

    url = _fb_normalize_client_url("https://www.facebook.com/login")
    assert_safe_facebook_navigation_url(url, label="session_recovery_login")
    reset_recaptcha_network_capture(page, reason="goto_login")
    page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    _force_www_facebook_if_mobile_redirect(page)
    _dismiss_login_obstacles(page)
    try:
        page.wait_for_load_state("networkidle", timeout=25_000)
    except Exception:
        pass
    for _ in range(24):
        _dismiss_login_obstacles(page)
        if _locate_login_email_input(page, timeout_ms=600) is not None:
            break
        if _locate_login_password_input(page, timeout_ms=400) is not None:
            _reveal_login_email_field(page)
            if _locate_login_email_input(page, timeout_ms=600) is not None:
                break
        try:
            page.wait_for_timeout(500)
        except Exception:
            time.sleep(0.5)
    _recovery_pause(label="sau mở trang login", kind="step")


def _manual_captcha_wait_sec() -> float:
    """Thời gian chờ người dùng tick captcha tay khi CapSolver không hỗ trợ sitekey Meta."""
    raw = os.environ.get("FB_RECOVERY_MANUAL_CAPTCHA_WAIT_SEC", "180")
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 180.0


def _post_login_wait_timeout_ms(page: Page) -> int:
    """Thời gian chờ sau submit login — dài hơn khi đang ở luồng 2FA/captcha Meta."""
    u = _url_lower(page)
    if any(m in u for m in ("two_step", "authentication", "two_factor", "pre_authentication")):
        manual_sec = _manual_captcha_wait_sec()
        return int(max(120_000, manual_sec * 1000.0 + 45_000))
    return 18_000


def _advance_after_captcha_step(
    page: Page,
    account: dict[str, Any] | None,
    *,
    should_stop: Callable[[], bool] | None = None,
) -> bool:
    """
    Sau captcha (tay hoặc auto): bấm Tiếp tục nếu cần và chờ Meta chuyển sang TOTP/feed.
    """
    _mark_auth_flow_active(account)
    from src.services.facebook_recaptcha import submit_checkpoint_after_captcha

    try:
        submit_checkpoint_after_captcha(page)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[FB recovery] submit_checkpoint_after_captcha: {}", exc)
    deadline = time.time() + 28.0
    while time.time() < deadline:
        if should_stop and should_stop():
            return False
        if facebook_page_looks_like_totp_prompt(page) or _session_logged_in(page):
            return True
        if _recovery_flow_advanced(page):
            return True
        try:
            page.wait_for_timeout(500)
        except Exception:
            time.sleep(0.5)
    return facebook_page_looks_like_totp_prompt(page) or _recovery_flow_advanced(page)


def _manual_captcha_maybe_continue(page: Page) -> bool:
    """Đã tick captcha nhưng chưa bấm Tiếp tục — thử submit form checkpoint."""
    try:
        filled = page.evaluate(
            """() => {
              const el = document.querySelector('textarea[name="g-recaptcha-response"]');
              return !!(el && String(el.value || '').length > 24);
            }"""
        )
    except Exception:
        filled = False
    if not filled:
        return False
    from src.services.facebook_recaptcha import submit_checkpoint_after_captcha

    logger.info("[FB recovery] Captcha đã có token — thử bấm Tiếp tục tự động.")
    submit_checkpoint_after_captcha(page)
    try:
        page.wait_for_timeout(1_200)
    except Exception:
        time.sleep(1.2)
    return _recovery_flow_advanced(page) or facebook_page_looks_like_totp_prompt(page)


def _wait_manual_captcha_on_page(
    page: Page,
    *,
    should_stop: Callable[[], bool] | None = None,
    timeout_sec: float | None = None,
    account: dict[str, Any] | None = None,
) -> bool:
    """Chờ người dùng giải captcha trên trình duyệt đang mở (Cách 1 — một lần duy nhất)."""
    from src.services.facebook_recaptcha import facebook_page_has_recaptcha

    wait = _manual_captcha_wait_sec() if timeout_sec is None else max(0.0, float(timeout_sec))
    if wait <= 0:
        return False
    uid = str((account or {}).get("facebook_uid") or (account or {}).get("id") or "").strip()
    uid_lbl = uid[:24] + "…" if len(uid) > 24 else (uid or "tài khoản")
    logger.warning(
        "[FB recovery] Hết 3 tầng CapSolver/2Captcha — tick captcha THỦ CÔNG trên Firefox "
        "(tối đa {:.0f}s, profile Firefox giữ khóa — không nhả slot cho UID khác) | {}.",
        wait,
        uid_lbl,
    )
    try:
        page.bring_to_front()
    except Exception:
        pass
    _emit_manual_captcha_notify(
        f"🖐 CAPTCHA THỦ CÔNG ({uid_lbl}) — Mở cửa sổ Firefox của tool, "
        f"tích captcha + bấm Tiếp tục. Còn {int(wait)}s. (Sau khi qua, tool tự lưu cookie.)"
    )
    deadline = time.time() + wait
    last_notify = 0.0
    while time.time() < deadline:
        if should_stop and should_stop():
            logger.info("[FB recovery] Đã hủy chờ captcha thủ công.")
            return False
        if _recovery_flow_advanced(page):
            logger.info(
                "[FB recovery] Đã qua captcha/2FA bước trung gian (TOTP hoặc cookie) — url={}",
                (page.url or "")[:90],
            )
            _emit_manual_captcha_notify(
                f"✓ Đã qua captcha ({uid_lbl}) — đang xử lý bước tiếp (TOTP/lưu cookie)…"
            )
            _mark_auth_flow_active(account)
            _advance_after_captcha_step(page, account, should_stop=should_stop)
            return True
        if _manual_captcha_maybe_continue(page):
            logger.info(
                "[FB recovery] Đã tick captcha + Tiếp tục — url={}",
                (page.url or "")[:90],
            )
            _mark_auth_flow_active(account)
            _advance_after_captcha_step(page, account, should_stop=should_stop)
            return True
        now = time.time()
        if now - last_notify >= 20.0:
            left = max(0, int(deadline - now))
            _emit_manual_captcha_notify(
                f"🖐 Vẫn chờ captcha tay ({uid_lbl}) — còn {left}s. Tick trên Firefox rồi Tiếp tục."
            )
            last_notify = now
        try:
            page.wait_for_timeout(800)
        except Exception:
            time.sleep(0.8)
    ok = _recovery_flow_advanced(page)
    if ok:
        _mark_auth_flow_active(account)
        _advance_after_captcha_step(page, account, should_stop=should_stop)
    else:
        logger.warning(
            "[FB recovery] Hết {}s chờ captcha thủ công — chưa vào được. "
            "Thử lại hoặc cấu hình proxy User:Pass (Cách 2).",
            int(wait),
        )
    return ok


def _try_capsolver_if_checkpoint(
    page: Page,
    account: dict[str, Any] | None,
    *,
    should_stop: Callable[[], bool] | None = None,
    with_manual: bool = True,
) -> bool:
    """Giải reCAPTCHA checkpoint — 2Captcha trước, CapSolver dự phòng; tùy chọn captcha tay."""
    if not account:
        return False
    if should_stop and should_stop():
        return False
    if _session_logged_in(page):
        return False
    from src.services.facebook_recaptcha import (
        facebook_page_may_need_recaptcha,
        try_solve_facebook_recaptcha,
    )

    u = str(page.url or "").lower()
    if "facebook.com" not in u and "fb.com" not in u:
        return False
    if facebook_page_blocks_recovery_email(u) and not facebook_page_may_need_recaptcha(page):
        return False
    if not facebook_page_may_need_recaptcha(page):
        return False

    if try_solve_facebook_recaptcha(page, account, should_stop=should_stop):
        logger.info("[FB recovery] Auto reCAPTCHA (2Captcha/CapSolver) — đã inject token.")
        if _session_logged_in(page):
            return True
    elif _session_logged_in(page):
        return True
    if with_manual and _wait_manual_captcha_on_page(page, should_stop=should_stop, account=account):
        _mark_auth_flow_active(account)
        _advance_after_captcha_step(page, account, should_stop=should_stop)
        bundle = load_account_credential_bundle(account)
        if bundle and _continue_totp_after_captcha(
            page,
            bundle,
            account,
            cookie_path=None,
            should_stop=should_stop,
            log_label="manual_captcha_totp",
        ):
            return True
        if facebook_page_looks_like_totp_prompt(page):
            logger.info(
                "[FB recovery] Sau captcha tay — chờ nhập TOTP (chưa cấu hình secret hoặc form chưa render)."
            )
            _set_session_status(account, "waiting_manual")
            return False
        return _recovery_flow_advanced(page)
    return False


def _wait_post_login_state(
    page: Page,
    *,
    account: dict[str, Any] | None = None,
    timeout_ms: int | None = None,
    should_stop: Callable[[], bool] | None = None,
    captcha_auto_done: bool = False,
) -> PostLoginState:
    """
    Chờ trạng thái sau đăng nhập.

    Captcha: tối đa **một** lần auto (pipeline) + **một** lần chờ tay — không lặp mỗi 500ms.
    """
    from src.services.facebook_recaptcha import (
        facebook_page_may_need_recaptcha,
        try_solve_facebook_recaptcha,
    )

    if timeout_ms is None:
        timeout_ms = _post_login_wait_timeout_ms(page)
    deadline = time.time() + max(2.0, timeout_ms / 1000.0)
    captcha_auto_attempted = captcha_auto_done
    captcha_manual_attempted = False
    while time.time() < deadline:
        if should_stop and should_stop():
            return "timeout"
        if _session_logged_in(page):
            return "logged_in"
        u = _url_lower(page)
        if any(m in u for m in ("two_step", "authentication", "two_factor")):
            _mark_auth_flow_active(account)
        if account and (
            facebook_page_may_need_recaptcha(page) or facebook_page_is_hard_checkpoint(u)
        ):
            if not captcha_auto_attempted:
                captcha_auto_attempted = True
                _mark_auth_flow_active(account)
                solved = try_solve_facebook_recaptcha(page, account, should_stop=should_stop)
                # Pipeline auto có thể >18s — gia hạn để còn cửa sổ captcha tay + TOTP.
                manual_sec = _manual_captcha_wait_sec()
                deadline = max(
                    deadline,
                    time.time() + manual_sec + 20.0,
                )
                if solved and _session_logged_in(page):
                    return "logged_in"
                if solved:
                    _advance_after_captcha_step(page, account, should_stop=should_stop)
                    if facebook_page_looks_like_totp_prompt(page):
                        return "totp"
                    if _session_logged_in(page):
                        return "logged_in"
            elif not captcha_manual_attempted:
                captcha_manual_attempted = True
                if _manual_captcha_wait_sec() > 0 and _wait_manual_captcha_on_page(
                    page,
                    should_stop=should_stop,
                    account=account,
                ):
                    _advance_after_captcha_step(page, account, should_stop=should_stop)
                    if facebook_page_looks_like_totp_prompt(page):
                        return "totp"
                    if _session_logged_in(page):
                        return "logged_in"
                    if _recovery_flow_advanced(page):
                        return "totp" if facebook_page_looks_like_totp_prompt(page) else "logged_in"
                logger.warning(
                    "[FB recovery] reCAPTCHA chưa qua — tick captcha trên Firefox hoặc kiểm tra 2Captcha/CapSolver."
                )
                return "checkpoint"
            else:
                return "checkpoint"
        if _session_logged_in(page):
            return "logged_in"
        if facebook_page_is_trust_device_prompt(page):
            _dismiss_remember_browser(page)
            page.wait_for_timeout(600)
            if _session_logged_in(page):
                return "logged_in"
            continue
        if facebook_page_looks_like_totp_prompt(page):
            return "totp"
        page.wait_for_timeout(500)
    if facebook_page_looks_like_totp_prompt(page):
        return "totp"
    return "timeout"


def _fill_recovery_email_on_checkpoint(
    page: Page, recovery_email: str, *, login_email: str = ""
) -> bool:
    if _fill_first(page, _RECOVERY_EMAIL_EXTRA_SELECTORS, recovery_email, label="email khôi phục"):
        return True
    login = (login_email or "").strip().lower()
    for sel in _EMAIL_SELECTORS:
        try:
            loc = page.locator(sel)
            if not loc.count():
                continue
            el = loc.first
            if not el.is_visible(timeout=1_500):
                continue
            try:
                current = (el.input_value(timeout=1_000) or "").strip()
            except Exception:
                current = ""
            if current and login and current.lower() != login:
                continue
            _recovery_pause(label="trước nhập email khôi phục", kind="input")
            if _fill_first(page, (sel,), recovery_email, label="email khôi phục"):
                return True
        except Exception:
            continue
    return _fill_first(page, _EMAIL_SELECTORS, recovery_email, label="email khôi phục")


def _attempt_recovery_email_checkpoint(
    page: Page,
    bundle: AccountCredentialBundle,
    *,
    account: dict[str, Any] | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> PostLoginState:
    if not bundle.has_recovery_email:
        return "checkpoint"
    if facebook_page_blocks_recovery_email(_url_lower(page)):
        return "checkpoint"
    logger.info("[FB recovery] Thử điền email khôi phục trên checkpoint — url={}", page.url)
    _recovery_pause(label="trước checkpoint email khôi phục", kind="step")
    if not _fill_recovery_email_on_checkpoint(
        page, bundle.recovery_email, login_email=bundle.login_identifier
    ):
        logger.info("[FB recovery] Không tìm thấy ô email trên checkpoint.")
        return "checkpoint"
    _recovery_pause(label="trước nút tiếp checkpoint", kind="step")
    _click_first(page, _TOTP_SUBMIT_SELECTORS, label="tiếp checkpoint")
    _recovery_pause(label="sau nút tiếp checkpoint", kind="step")
    return _wait_post_login_state(
        page, account=account, timeout_ms=22_000, should_stop=should_stop
    )


def _finish_session_after_recovery_steps(
    page: Page,
    bundle: AccountCredentialBundle,
    *,
    cookie_path: str | Path | None,
    account: dict[str, Any],
    should_stop: Callable[[], bool] | None = None,
) -> bool:
    state = _attempt_recovery_email_checkpoint(
        page, bundle, account=account, should_stop=should_stop
    )
    if state == "totp" and bundle.has_totp:
        logger.info("[FB recovery] Sau checkpoint — Meta yêu cầu TOTP.")
        _submit_totp_code(page, bundle.totp_secret)
        state = _wait_post_login_state(
            page, account=account, timeout_ms=20_000, should_stop=should_stop
        )
    if state == "checkpoint":
        return False
    if state == "logged_in" or _session_logged_in(page):
        return _finalize_successful_recovery(
            page, account, cookie_path, log_label="checkpoint_email"
        )
    return False


def _submit_email_password(
    page: Page,
    bundle: AccountCredentialBundle,
    *,
    account: dict[str, Any] | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> PostLoginState:
    if should_stop and should_stop():
        return "timeout"
    if _session_logged_in(page):
        return "logged_in"
    _goto_facebook_login(page)
    if facebook_page_is_hard_checkpoint(_url_lower(page)):
        return "checkpoint"
    _dismiss_login_obstacles(page)
    if not _wait_for_login_form(page, account=account):
        logger.warning("[FB recovery] Form login chưa hiện — thử mở lại trang login url={}", page.url)
        _goto_facebook_login(page)
        _dismiss_login_obstacles(page)
        if not _wait_for_login_form(
            page, timeout_ms=min(14_000, _login_form_wait_ms()), account=account
        ):
            from src.automation.facebook_actions import save_ui_failure_screenshot

            save_ui_failure_screenshot(page, "FACEBOOK_LOGIN: Không tìm thấy ô email/UID")
            _raise_manual("FACEBOOK_LOGIN: Không tìm thấy ô email/UID.")
    if _session_logged_in(page):
        return "logged_in"
    login_id = bundle.login_identifier
    if not login_id:
        _raise_manual("FACEBOOK_LOGIN: Thiếu UID hoặc email đăng nhập.")
    id_label = "UID" if bundle.facebook_uid else "email"
    _recovery_pause(label=f"trước nhập {id_label}", kind="step")
    email_el = None
    for attempt in range(3):
        _dismiss_login_obstacles(page)
        try:
            page.bring_to_front()
        except Exception:
            pass
        if not _reveal_login_email_field(page):
            try:
                page.wait_for_timeout(900)
            except Exception:
                time.sleep(0.9)
        email_el = _locate_login_email_input(page, timeout_ms=3_000)
        if email_el is not None:
            break
        logger.info(
            "[FB recovery] Chưa thấy ô {} (lần {}/3) url={}",
            id_label,
            attempt + 1,
            page.url,
        )
    if email_el is None or not _fill_locator(email_el, login_id, label=id_label):
        from src.automation.facebook_actions import save_ui_failure_screenshot

        save_ui_failure_screenshot(page, "FACEBOOK_LOGIN: Không điền được email/UID")
        _raise_manual(
            f"FACEBOOK_LOGIN: Không tìm thấy ô email/UID (url={page.url!r}). "
            "Thử bấm «Đăng nhập tài khoản khác» tay hoặc mở https://www.facebook.com/login."
        )
    _recovery_pause(label=f"{id_label} → mật khẩu", kind="step")
    pass_el = _locate_login_password_input(page, timeout_ms=2_500)
    if pass_el is None:
        _click_first(page, _LOGIN_CONTINUE_AFTER_ID_SELECTORS, label="Tiếp tục sau UID")
        _recovery_pause(label="sau Tiếp tục sau UID", kind="step")
        try:
            page.wait_for_timeout(1_500)
        except Exception:
            time.sleep(1.5)
        pass_el = _locate_login_password_input(page, timeout_ms=3_500)
    if pass_el is None or not _fill_locator(
        pass_el, bundle.password, label="mật khẩu", submit_enter=True
    ):
        from src.automation.facebook_actions import save_ui_failure_screenshot

        save_ui_failure_screenshot(page, "FACEBOOK_LOGIN: Không tìm thấy ô mật khẩu")
        _raise_manual("FACEBOOK_LOGIN: Không tìm thấy ô mật khẩu.")
    _recovery_pause(label="sau Enter đăng nhập", kind="step")
    page.wait_for_timeout(800)
    if not _session_logged_in(page) and not facebook_page_looks_like_totp_prompt(page):
        if not _click_first(page, _LOGIN_BUTTON_SELECTORS, label="Đăng nhập"):
            logger.info("[FB recovery] Enter chưa chuyển trang — thử bấm nút Đăng nhập.")
        _recovery_pause(label="sau nút Đăng nhập", kind="step")
    u_after = _url_lower(page)
    if any(m in u_after for m in ("two_step", "authentication", "two_factor")):
        _mark_auth_flow_active(account)
    return _wait_post_login_state(page, account=account, should_stop=should_stop)


def _submit_totp_code(page: Page, totp_secret: str) -> None:
    if not facebook_page_looks_like_totp_prompt(page):
        return
    _recovery_pause(label="trước nhập TOTP", kind="step")
    code = generate_totp_code(totp_secret)
    if not code:
        _raise_manual("FACEBOOK_TOTP: Không sinh được mã TOTP (kiểm tra secret Base32 / pyotp).")
    logger.info("[FB recovery] Mã TOTP 6 số đã sinh — đang tìm ô nhập trên url={}", page.url)

    filled = False
    for ctx in _totp_browser_contexts(page):
        if _try_fill_totp_digit_boxes(ctx, code):
            filled = True
            logger.info("[FB recovery] Đã điền TOTP (6 ô riêng).")
            break

    if not filled:
        el = _locate_totp_input(page, timeout_ms=14_000)
        if el is None and not _fill_first(
            page, _TOTP_INPUT_SELECTORS, code, label="TOTP", submit_enter=True
        ):
            _raise_manual("FACEBOOK_TOTP: Không tìm thấy ô nhập mã xác thực.")
        elif el is not None:
            if not _fill_locator(el, code, label="TOTP", submit_enter=True):
                _raise_manual("FACEBOOK_TOTP: Không điền được mã vào ô xác thực.")
            logger.info("[FB recovery] Đã gõ mã TOTP + Enter (ô «Mã» / textbox).")
        else:
            logger.info("[FB recovery] Đã gõ mã TOTP (selector legacy).")

    try:
        page.wait_for_load_state("domcontentloaded", timeout=6_000)
    except Exception:
        pass

    outcome = _wait_after_totp_code_submit(page, timeout_ms=24_000)
    logger.info("[FB recovery] Sau gửi TOTP — outcome={} url={}", outcome, page.url)

    if outcome == "logged_in":
        logger.info("[FB recovery] Đã đăng nhập sau TOTP.")
        return

    if outcome in {"remember_browser", "timeout"} and facebook_page_is_trust_device_prompt(page):
        outcome = "remember_browser"

    if outcome == "remember_browser":
        if _handle_remember_browser_after_totp(page):
            logger.info("[FB recovery] Đã qua remember_browser sau TOTP.")
        else:
            logger.warning(
                "[FB recovery] remember_browser — chờ thêm (có thể user đã bấm Tiếp tục thủ công)."
            )
            _wait_post_login_state(page, account=None, timeout_ms=12_000)
        return

    if outcome == "checkpoint":
        return

    if outcome == "totp" or facebook_page_looks_like_totp_prompt(page):
        if facebook_page_is_trust_device_prompt(page):
            _handle_remember_browser_after_totp(page)
            return
        _recovery_pause(label="trước nút xác nhận TOTP", kind="step")
        if _click_totp_continue(page):
            _recovery_pause(label="sau nút Tiếp tục TOTP", kind="step")
            after_click = _wait_after_totp_code_submit(page, timeout_ms=20_000)
            logger.info("[FB recovery] Sau nút Tiếp tục TOTP — outcome={}", after_click)
            if after_click == "logged_in":
                return
            if after_click == "remember_browser" or facebook_page_is_trust_device_prompt(page):
                _handle_remember_browser_after_totp(page)
                return
            if after_click != "totp":
                return
        if facebook_page_is_trust_device_prompt(page):
            _handle_remember_browser_after_totp(page)
            return
        if _session_logged_in(page):
            return
        _raise_manual("FACEBOOK_TOTP: Không bấm được nút xác nhận mã (sau Enter).")

    if facebook_page_is_trust_device_prompt(page):
        _handle_remember_browser_after_totp(page)
        return


def clear_facebook_browser_session(page: Page) -> None:
    """Đăng xuất + xóa cookie rồi mở form login (Test Login / force reauth)."""
    from src.automation.facebook_actions import (
        _fb_normalize_client_url,
        assert_safe_facebook_navigation_url,
    )

    try:
        logout = _fb_normalize_client_url(
            "https://www.facebook.com/logout.php?next=https://www.facebook.com/login/?logout=1"
        )
        assert_safe_facebook_navigation_url(logout, label="session_logout")
        page.goto(logout, wait_until="domcontentloaded", timeout=60_000)
        _recovery_pause(label="sau logout", kind="step")
    except Exception as exc:  # noqa: BLE001
        logger.info("[FB recovery] Bỏ qua bước logout ({}): {}", page.url, exc)
    try:
        page.context.clear_cookies()
    except Exception as exc:  # noqa: BLE001
        logger.warning("[FB recovery] Không xóa được cookie context: {}", exc)
    _recovery_pause(label="sau xóa cookie", kind="step")
    _goto_facebook_login(page)
    logger.info("[FB recovery] Đã reset phiên — url={}", page.url)


def save_session_to_cookie_path(page: Page, cookie_path: str | Path | None) -> None:
    raw = str(cookie_path or "").strip()
    if not raw:
        return
    p = Path(raw)
    if not p.is_absolute():
        p = (project_root() / p).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    page.context.storage_state(path=str(p))
    logger.info("[FB recovery] Đã lưu storage_state → {}", p)


def _finalize_successful_recovery(
    page: Page,
    account: dict[str, Any],
    cookie_path: str | Path | None,
    *,
    log_label: str = "",
) -> bool:
    """
    Xác nhận đã vào tài khoản → lưu cookie → ``session_status=ready``.

    Chỉ trả True khi ``confirm_facebook_session_logged_in`` thành công.
    """
    ok, detail = confirm_facebook_session_logged_in(page, account)
    if not ok:
        from src.services.facebook_session_persist import profile_session_ready_for_interaction

        ok_prof, det_prof = profile_session_ready_for_interaction(page, account)
        if ok_prof:
            account["login_confirm_detail"] = det_prof
            from src.services.facebook_session_persist import (
                cookie_file_has_session,
                ensure_account_cookie_path,
                sync_session_to_accounts_registry,
            )

            ck_rel = ensure_account_cookie_path(account, cookie_path)
            save_session_to_cookie_path(page, ck_rel)
            try:
                sync_session_to_accounts_registry(account, ck_rel)
            except Exception as sync_exc:  # noqa: BLE001
                logger.warning("[FB recovery] Không sync accounts.json: {}", sync_exc)
            if cookie_file_has_session(ck_rel):
                _clear_auth_flow_active(account)
                _set_session_status(account, "ready")
                logger.info(
                    "[FB recovery] Đã lưu cookie (profile có phiên) account_id={}{} → {}",
                    account.get("id", ""),
                    f" ({log_label})" if log_label else "",
                    ck_rel,
                )
                return True
        logger.warning(
            "[FB recovery] Chưa xác nhận đăng nhập account_id={}{}: {}",
            account.get("id", ""),
            f" ({log_label})" if log_label else "",
            detail,
        )
        _set_session_status(account, "reauth_required")
        return False
    account["login_confirm_detail"] = detail
    from src.services.facebook_session_persist import (
        cookie_file_has_session,
        ensure_account_cookie_path,
    )

    ck_rel = ensure_account_cookie_path(account, cookie_path)
    save_session_to_cookie_path(page, ck_rel)
    try:
        from src.services.facebook_session_persist import sync_session_to_accounts_registry

        sync_session_to_accounts_registry(account, ck_rel)
    except Exception as sync_exc:  # noqa: BLE001
        logger.warning("[FB recovery] Không sync accounts.json sau lưu cookie: {}", sync_exc)
    if not cookie_file_has_session(ck_rel):
        logger.warning(
            "[FB recovery] Lưu cookie thất bại (thiếu c_user) account_id={}{}",
            account.get("id", ""),
            f" ({log_label})" if log_label else "",
        )
        _set_session_status(account, "reauth_required")
        return False
    _clear_auth_flow_active(account)
    _set_session_status(account, "ready")
    logger.info(
        "[FB recovery] Đã xác nhận + lưu cookie account_id={}{} → {} — {}",
        account.get("id", ""),
        f" ({log_label})" if log_label else "",
        ck_rel,
        detail,
    )
    return True


def _set_session_status(account: dict[str, Any], status: str) -> None:
    account["session_status"] = status


def _page_on_meta_auth_or_captcha_flow(page: Page) -> bool:
    """Đang ở 2FA/captcha Meta — không nên quay lại form /login."""
    if _session_logged_in(page):
        return False
    from src.services.facebook_recaptcha import (
        facebook_page_has_recaptcha,
        facebook_page_on_recaptcha_flow_url,
        is_plain_facebook_login_url,
    )

    u = _url_lower(page)
    if facebook_page_is_hard_checkpoint(u):
        return False
    if facebook_page_on_recaptcha_flow_url(page, url=u):
        return True
    if any(m in u for m in ("two_step_verification", "two_factor", "approvals_code")):
        if is_plain_facebook_login_url(u):
            return False
        return True
    return facebook_page_has_recaptcha(page) and not is_plain_facebook_login_url(u)


def continue_facebook_auth_flow(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> bool:
    """
    Tiếp tục luồng captcha/TOTP tại trang hiện tại — không điền lại UID/mật khẩu.

    Dùng khi worker recovery lần 1 thất bại nhưng vẫn trong phiên 2FA Meta.
    """
    bundle = load_account_credential_bundle(account)
    if not bundle:
        return False
    _mark_auth_flow_active(account)
    _set_session_status(account, "recovering")
    if facebook_page_looks_like_totp_prompt(page):
        return _continue_totp_after_captcha(
            page,
            bundle,
            account,
            cookie_path=cookie_path,
            should_stop=should_stop,
            log_label="continue_auth_totp",
        )
    return _recover_auth_flow_in_place(
        page,
        bundle,
        account,
        cookie_path=cookie_path,
        should_stop=should_stop,
    )


def _continue_totp_after_captcha(
    page: Page,
    bundle: AccountCredentialBundle,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None,
    should_stop: Callable[[], bool] | None,
    log_label: str,
) -> bool:
    """Sau captcha — điền TOTP nếu Meta chuyển sang màn Authenticator."""
    if not facebook_page_looks_like_totp_prompt(page):
        return False
    if not bundle.has_totp:
        logger.warning(
            "[FB recovery] Meta yêu cầu TOTP nhưng chưa cấu hình secret account_id={}",
            bundle.account_id,
        )
        _set_session_status(account, "waiting_manual")
        return False
    logger.info("[FB recovery] Màn TOTP sau captcha — điền mã account_id={}", bundle.account_id)
    _submit_totp_code(page, bundle.totp_secret)
    state = _wait_post_login_state(
        page, account=account, timeout_ms=25_000, should_stop=should_stop, captcha_auto_done=True
    )
    if state == "logged_in" or _recovery_flow_advanced(page):
        return _finalize_successful_recovery(page, account, cookie_path, log_label=log_label)
    return False


def _recover_auth_flow_in_place(
    page: Page,
    bundle: AccountCredentialBundle,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None,
    should_stop: Callable[[], bool] | None,
) -> bool:
    """Tiếp tục captcha/TOTP tại URL hiện tại — không reset form đăng nhập."""
    aid = bundle.account_id
    if facebook_page_looks_like_totp_prompt(page):
        if _continue_totp_after_captcha(
            page,
            bundle,
            account,
            cookie_path=cookie_path,
            should_stop=should_stop,
            log_label="auth_flow_totp",
        ):
            return True
        _set_session_status(account, "waiting_manual")
        return False
    if _try_capsolver_if_checkpoint(page, account, should_stop=should_stop):
        if _continue_totp_after_captcha(
            page,
            bundle,
            account,
            cookie_path=cookie_path,
            should_stop=should_stop,
            log_label="auth_flow_captcha_totp",
        ):
            return True
        return _finalize_successful_recovery(page, account, cookie_path, log_label="auth_flow_captcha")
    logger.warning(
        "[FB recovery] Chưa vượt captcha/2FA tại url={} — cần tick tay hoặc kiểm tra API.",
        (page.url or "")[:120],
    )
    _set_session_status(account, "waiting_manual")
    return False


def try_recover_facebook_session(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    force_fresh_login: bool = False,
    allow_form_login: bool = True,
    should_stop: Callable[[], bool] | None = None,
) -> bool:
    """
    Đăng nhập lại bằng email/password; nếu Meta hiện TOTP → điền mã (khi đã cấu hình).

    Args:
        force_fresh_login: True = xóa cookie profile và đăng nhập lại từ form (Test Login).
        allow_form_login: False = chỉ probe/nạp cookie file, không mở form email/password.

    Returns:
        True nếu phiên hợp lệ sau recovery.
    """
    bundle = load_account_credential_bundle(account)
    if not bundle or not bundle.has_password_login:
        logger.info("[FB recovery] Thiếu UID/email hoặc mật khẩu — bỏ qua recovery tự động.")
        return False

    aid = bundle.account_id
    if should_stop and should_stop():
        _set_session_status(account, "cancelled")
        return False
    if facebook_page_is_hard_checkpoint(_url_lower(page)):
        if _try_capsolver_if_checkpoint(page, account, should_stop=should_stop):
            return _finalize_successful_recovery(page, account, cookie_path, log_label="captcha")
        if bundle.has_recovery_email and not facebook_page_blocks_recovery_email(_url_lower(page)):
            if _finish_session_after_recovery_steps(
                page,
                bundle,
                cookie_path=cookie_path,
                account=account,
                should_stop=should_stop,
            ):
                return True
        logger.warning(
            "[FB recovery] Checkpoint — không vượt được (email khôi phục / CapSolver / Meta chặn)."
        )
        _set_session_status(account, "waiting_manual")
        return False

    if not force_fresh_login and (
        facebook_page_looks_like_totp_prompt(page) or _page_on_meta_auth_or_captcha_flow(page)
    ):
        _mark_auth_flow_active(account)
        logger.info(
            "[FB recovery] Tiếp tục luồng 2FA/captcha tại URL hiện tại — không reset form login account_id={}",
            aid,
        )
        _set_session_status(account, "recovering")
        return _recover_auth_flow_in_place(
            page,
            bundle,
            account,
            cookie_path=cookie_path,
            should_stop=should_stop,
        )

    mode = "force_fresh" if force_fresh_login else "reuse_session"
    logger.info("[FB recovery] Bắt đầu đăng nhập lại account_id={} mode={}", aid, mode)
    _set_session_status(account, "recovering")

    try:
        if should_stop and should_stop():
            _set_session_status(account, "cancelled")
            return False
        if force_fresh_login:
            clear_facebook_browser_session(page)
        elif _session_logged_in(page):
            return _finalize_successful_recovery(page, account, cookie_path, log_label="reuse_session")

        if not force_fresh_login:
            from src.services.facebook_session_persist import probe_existing_facebook_session

            ok_probe, _probe_detail = probe_existing_facebook_session(
                page,
                account,
                cookie_path=cookie_path,
                timeout_ms=22_000,
            )
            if ok_probe:
                return _finalize_successful_recovery(
                    page, account, cookie_path, log_label="probe_session"
                )

        if not force_fresh_login and cookie_path:
            from src.automation.facebook_actions import login_with_cookie
            from src.services.facebook_session_persist import cookie_file_has_session

            if cookie_file_has_session(cookie_path):
                try:
                    login_with_cookie(page, cookie_path)
                    ok_cookie, _cookie_detail = confirm_facebook_session_logged_in(
                        page, account, timeout_ms=22_000
                    )
                    if ok_cookie:
                        return _finalize_successful_recovery(
                            page, account, cookie_path, log_label="cookie_file"
                        )
                    from src.services.facebook_session_persist import probe_existing_facebook_session

                    ok_probe2, _probe2 = probe_existing_facebook_session(
                        page,
                        account,
                        cookie_path=cookie_path,
                        timeout_ms=18_000,
                    )
                    if ok_probe2:
                        return _finalize_successful_recovery(
                            page, account, cookie_path, log_label="cookie_probe"
                        )
                except FileNotFoundError:
                    logger.info(
                        "[FB recovery] Không có file cookie tại {} — tiếp tục form login.",
                        cookie_path,
                    )
                except Exception as cookie_exc:  # noqa: BLE001
                    logger.warning("[FB recovery] Nạp cookie thất bại: {}", cookie_exc)

        if (
            not force_fresh_login
            and not allow_form_login
            and cookie_path
        ):
            from src.services.facebook_session_persist import cookie_file_has_session

            if cookie_file_has_session(cookie_path):
                logger.info(
                    "[FB recovery] File cookie còn c_user nhưng chưa vào được — không mở form login "
                    "(allow_form_login=False) account_id={}",
                    aid,
                )
                _set_session_status(account, "login_failed")
                return False

        state: PostLoginState
        if facebook_page_looks_like_totp_prompt(page):
            state = "totp"
        else:
            state = _submit_email_password(
                page, bundle, account=account, should_stop=should_stop
            )

        if state == "checkpoint":
            if _try_capsolver_if_checkpoint(page, account, should_stop=should_stop):
                return _finalize_successful_recovery(page, account, cookie_path, log_label="post_login_captcha")
            if bundle.has_recovery_email and _finish_session_after_recovery_steps(
                page,
                bundle,
                cookie_path=cookie_path,
                account=account,
                should_stop=should_stop,
            ):
                return True
            _set_session_status(account, "waiting_manual")
            return False

        if state == "totp":
            if _continue_totp_after_captcha(
                page,
                bundle,
                account,
                cookie_path=cookie_path,
                should_stop=should_stop,
                log_label="post_login_totp",
            ):
                return True
            if bundle.has_totp:
                if _locate_totp_input(page, timeout_ms=3_000) is None and _first_visible(
                    page, _TOTP_INPUT_SELECTORS, timeout_ms=800
                ) is None:
                    from src.services.facebook_recaptcha import resolve_facebook_recaptcha

                    if resolve_facebook_recaptcha(
                        page,
                        account,
                        stage="recovery:totp_missing_input_wait_captcha",
                        wait_timeout_ms=15_000,
                        should_stop=should_stop,
                    ):
                        state = _wait_post_login_state(
                            page,
                            account=account,
                            timeout_ms=15_000,
                            should_stop=should_stop,
                            captcha_auto_done=True,
                        )
                        if state == "logged_in":
                            return _finalize_successful_recovery(
                                page, account, cookie_path, log_label="totp_captcha"
                            )
                logger.info("[FB recovery] Meta yêu cầu TOTP — điền mã Authenticator.")
                _submit_totp_code(page, bundle.totp_secret)
                state = _wait_post_login_state(
                    page, account=account, timeout_ms=20_000, should_stop=should_stop
                )
            else:
                from src.services.facebook_recaptcha import resolve_facebook_recaptcha

                if resolve_facebook_recaptcha(
                    page,
                    account,
                    stage="recovery:totp_without_secret_wait_captcha",
                    wait_timeout_ms=15_000,
                    should_stop=should_stop,
                ):
                    if _session_logged_in(page):
                        state = "logged_in"
                    else:
                        state = _wait_post_login_state(
                            page,
                            account=account,
                            timeout_ms=12_000,
                            should_stop=should_stop,
                            captcha_auto_done=True,
                        )
                if state == "logged_in":
                    return _finalize_successful_recovery(page, account, cookie_path, log_label="totp_no_secret")
                _raise_manual(
                    "FACEBOOK_TOTP: Meta yêu cầu mã Authenticator nhưng tài khoản chưa bật/ghi TOTP secret."
                )
        elif state == "timeout":
            logger.warning("[FB recovery] Hết thời gian chờ sau login — url={}", page.url)
            _mark_auth_flow_active(account)
            if facebook_page_looks_like_totp_prompt(page) and bundle.has_totp:
                if _continue_totp_after_captcha(
                    page,
                    bundle,
                    account,
                    cookie_path=cookie_path,
                    should_stop=should_stop,
                    log_label="post_login_timeout_totp",
                ):
                    return True
            if _page_on_meta_auth_or_captcha_flow(page) or facebook_page_looks_like_totp_prompt(page):
                if _recover_auth_flow_in_place(
                    page,
                    bundle,
                    account,
                    cookie_path=cookie_path,
                    should_stop=should_stop,
                ):
                    return True
            if facebook_auth_flow_was_active(account):
                logger.info(
                    "[FB recovery] Vẫn trong luồng 2FA — không reset form login account_id={}",
                    aid,
                )
                _set_session_status(account, "waiting_manual")
                return False

        if state == "checkpoint":
            if _try_capsolver_if_checkpoint(page, account, should_stop=should_stop):
                return _finalize_successful_recovery(page, account, cookie_path, log_label="final_captcha")
            if bundle.has_recovery_email and _finish_session_after_recovery_steps(
                page,
                bundle,
                cookie_path=cookie_path,
                account=account,
                should_stop=should_stop,
            ):
                return True
            _set_session_status(account, "waiting_manual")
            return False

        if should_stop and should_stop():
            _set_session_status(account, "cancelled")
            return False

        if state == "logged_in" or _session_logged_in(page):
            if _finalize_successful_recovery(page, account, cookie_path, log_label="post_login"):
                logger.info("[FB recovery] Phiên khôi phục thành công account_id={}", aid)
                return True
            return False

        logger.warning("[FB recovery] Chưa xác nhận phiên sau login/TOTP account_id={}", aid)
        _set_session_status(account, "reauth_required")
        return False
    except RuntimeError:
        _set_session_status(account, "waiting_manual")
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning("[FB recovery] Lỗi recovery account_id={}: {}", aid, exc)
        _set_session_status(account, "reauth_required")
        return False
