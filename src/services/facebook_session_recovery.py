"""
Khôi phục phiên Facebook bằng email + mật khẩu + TOTP (user tự cấu hình).

Không xử lý checkpoint / captcha / xác minh danh tính — chuyển ``need_manual_check``.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Literal

from loguru import logger
from playwright.sync_api import Page

from src.services.totp_service import generate_totp_code
from src.utils.account_credentials import AccountCredentialBundle, load_account_credential_bundle
from src.utils.paths import project_root

PostLoginState = Literal["logged_in", "totp", "checkpoint", "timeout"]

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
    "two-factor",
    "approvals_code",
    "2fa",
)

_TOTP_TEXT_MARKERS = (
    "authentication code",
    "two-factor",
    "two factor",
    "security code",
    "login code",
    "mã xác thực",
    "mã đăng nhập",
    "xác thực hai lớp",
    "xác minh hai bước",
    "nhập mã",
)

_EMAIL_SELECTORS = (
    "input#email",
    "input[name='email']",
    "input[type='email']",
    "input[autocomplete='username']",
)

_PASSWORD_SELECTORS = (
    "input#pass",
    "input[name='pass']",
    "input[type='password']",
    "input[autocomplete='current-password']",
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
    "input[aria-label*='code' i]",
    "input[aria-label*='mã' i]",
    "input[aria-label*='xác thực' i]",
    "input[placeholder*='code' i]",
    "input[placeholder*='mã' i]",
    "input[type='text'][maxlength='6']",
    "input[type='tel'][maxlength='6']",
)

_TOTP_SUBMIT_SELECTORS = (
    "button[type='submit']",
    "button[name='submit[Continue]']",
    "div[role='button']:has-text('Continue')",
    "div[role='button']:has-text('Tiếp')",
    "div[role='button']:has-text('Submit')",
    "div[role='button']:has-text('Xác nhận')",
    "div[role='button']:has-text('Gửi')",
    "div[role='button']:has-text('Tiếp tục')",
)


def _url_lower(page: Page) -> str:
    return str(page.url or "").strip().lower()


def facebook_page_is_hard_checkpoint(url: str) -> bool:
    """Checkpoint / captcha / xác minh danh tính — không tự xử lý."""
    u = (url or "").strip().lower()
    if "facebook.com" not in u:
        return False
    return any(m in u for m in _UNSUPPORTED_URL_MARKERS)


def facebook_page_looks_like_totp_prompt(page: Page) -> bool:
    u = _url_lower(page)
    if "facebook.com" not in u:
        return False
    if facebook_page_is_hard_checkpoint(u):
        return False
    if any(x in u for x in _TOTP_URL_MARKERS):
        return True
    try:
        body = (page.locator("body").inner_text(timeout=2_000) or "").lower()
    except Exception:
        body = ""
    return any(m in body for m in _TOTP_TEXT_MARKERS)


def _raise_manual(reason: str) -> None:
    raise RuntimeError(f"{reason} need_manual_check")


def _session_logged_in(page: Page) -> bool:
    from src.automation.facebook_actions import facebook_session_appears_logged_in

    return facebook_session_appears_logged_in(page)


def _recovery_pause(*, label: str = "", kind: str = "input") -> None:
    """Delay giữa nhập / bấm nút login (dùng env FB_INPUT_DELAY / FB_CLICK_DELAY / FB_STEP_DELAY)."""
    from src.automation.facebook_actions import human_pause

    human_pause(label=label or "recovery", kind=kind)


def _recovery_typing_delay_ms() -> int:
    from src.automation.facebook_actions import _env_int, _typing_delay_ms

    base = _typing_delay_ms()
    extra = max(0, min(200, _env_int("FB_RECOVERY_TYPING_EXTRA_MS", 40)))
    return base + extra


def _first_visible(page: Page, selectors: tuple[str, ...], *, timeout_ms: int = 2_000):
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() and loc.first.is_visible(timeout=timeout_ms):
                return loc.first
        except Exception:
            continue
    return None


def _fill_first(page: Page, selectors: tuple[str, ...], value: str, *, label: str = "") -> bool:
    el = _first_visible(page, selectors)
    if el is None:
        return False
    _recovery_pause(label=f"trước nhập {label}".strip(), kind="input")
    try:
        el.click(timeout=1_200)
    except Exception:
        pass
    _recovery_pause(label=f"sau focus {label}".strip(), kind="click")
    typed = False
    try:
        el.fill("")
        el.fill(value)
        typed = True
    except Exception:
        pass
    if not typed:
        try:
            el.press_sequentially(value, delay=_recovery_typing_delay_ms())
            typed = True
        except Exception:
            return False
    _recovery_pause(label=f"sau nhập {label}".strip(), kind="input")
    return True


def _click_first(page: Page, selectors: tuple[str, ...], *, label: str = "") -> bool:
    el = _first_visible(page, selectors, timeout_ms=2_500)
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

    url = _fb_normalize_client_url("https://www.facebook.com/login")
    assert_safe_facebook_navigation_url(url, label="session_recovery_login")
    page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    _force_www_facebook_if_mobile_redirect(page)
    _recovery_pause(label="sau mở trang login", kind="step")


def _wait_post_login_state(page: Page, *, timeout_ms: int = 18_000) -> PostLoginState:
    deadline = time.time() + max(2.0, timeout_ms / 1000.0)
    while time.time() < deadline:
        if facebook_page_is_hard_checkpoint(_url_lower(page)):
            return "checkpoint"
        if _session_logged_in(page):
            return "logged_in"
        if facebook_page_looks_like_totp_prompt(page):
            return "totp"
        page.wait_for_timeout(500)
    return "timeout"


def _submit_email_password(page: Page, bundle: AccountCredentialBundle) -> PostLoginState:
    _goto_facebook_login(page)
    if facebook_page_is_hard_checkpoint(_url_lower(page)):
        return "checkpoint"
    _recovery_pause(label="trước nhập email", kind="step")
    if not _fill_first(page, _EMAIL_SELECTORS, bundle.email, label="email"):
        _raise_manual("FACEBOOK_LOGIN: Không tìm thấy ô email.")
    _recovery_pause(label="email → mật khẩu", kind="step")
    if not _fill_first(page, _PASSWORD_SELECTORS, bundle.password, label="mật khẩu"):
        _raise_manual("FACEBOOK_LOGIN: Không tìm thấy ô mật khẩu.")
    _recovery_pause(label="trước nút Đăng nhập", kind="step")
    if not _click_first(page, _LOGIN_BUTTON_SELECTORS, label="Đăng nhập"):
        _raise_manual("FACEBOOK_LOGIN: Không bấm được nút Đăng nhập.")
    _recovery_pause(label="sau nút Đăng nhập", kind="step")
    return _wait_post_login_state(page)


def _submit_totp_code(page: Page, totp_secret: str) -> None:
    if not facebook_page_looks_like_totp_prompt(page):
        return
    _recovery_pause(label="trước nhập TOTP", kind="step")
    code = generate_totp_code(totp_secret)
    if not code:
        _raise_manual("FACEBOOK_TOTP: Không sinh được mã TOTP (kiểm tra secret Base32 / pyotp).")
    if not _fill_first(page, _TOTP_INPUT_SELECTORS, code, label="TOTP"):
        _raise_manual("FACEBOOK_TOTP: Không tìm thấy ô nhập mã xác thực.")
    logger.info("[FB recovery] Đã điền mã TOTP.")
    _recovery_pause(label="trước nút xác nhận TOTP", kind="step")
    if not _click_first(page, _TOTP_SUBMIT_SELECTORS, label="xác nhận TOTP"):
        _raise_manual("FACEBOOK_TOTP: Không bấm được nút xác nhận mã.")
    _recovery_pause(label="sau xác nhận TOTP", kind="step")


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


def _set_session_status(account: dict[str, Any], status: str) -> None:
    account["session_status"] = status


def try_recover_facebook_session(
    page: Page,
    account: dict[str, Any],
    *,
    cookie_path: str | Path | None = None,
    force_fresh_login: bool = False,
) -> bool:
    """
    Đăng nhập lại bằng email/password; nếu Meta hiện TOTP → điền mã (khi đã cấu hình).

    Args:
        force_fresh_login: True = xóa cookie profile và đăng nhập lại từ form (Test Login).

    Returns:
        True nếu phiên hợp lệ sau recovery.
    """
    bundle = load_account_credential_bundle(account)
    if not bundle or not bundle.has_password_login:
        logger.info("[FB recovery] Thiếu email/password — bỏ qua recovery tự động.")
        return False

    aid = bundle.account_id
    if facebook_page_is_hard_checkpoint(_url_lower(page)):
        logger.warning("[FB recovery] Checkpoint/captcha — không tự xử lý.")
        _set_session_status(account, "waiting_manual")
        return False

    mode = "force_fresh" if force_fresh_login else "reuse_session"
    logger.info("[FB recovery] Bắt đầu đăng nhập lại account_id={} mode={}", aid, mode)
    _set_session_status(account, "recovering")

    try:
        if force_fresh_login:
            clear_facebook_browser_session(page)
        elif _session_logged_in(page):
            save_session_to_cookie_path(page, cookie_path)
            _set_session_status(account, "ready")
            return True

        state: PostLoginState
        if facebook_page_looks_like_totp_prompt(page):
            state = "totp"
        else:
            state = _submit_email_password(page, bundle)

        if state == "checkpoint":
            _set_session_status(account, "waiting_manual")
            return False

        if state == "totp":
            if bundle.has_totp:
                logger.info("[FB recovery] Meta yêu cầu TOTP — điền mã Authenticator.")
                _submit_totp_code(page, bundle.totp_secret)
                state = _wait_post_login_state(page, timeout_ms=20_000)
            else:
                _raise_manual(
                    "FACEBOOK_TOTP: Meta yêu cầu mã Authenticator nhưng tài khoản chưa bật/ghi TOTP secret."
                )
        elif state == "timeout":
            logger.warning("[FB recovery] Hết thời gian chờ sau login — url={}", page.url)

        if state == "checkpoint":
            _set_session_status(account, "waiting_manual")
            return False

        if state == "logged_in" or _session_logged_in(page):
            save_session_to_cookie_path(page, cookie_path)
            _set_session_status(account, "ready")
            logger.info("[FB recovery] Phiên khôi phục thành công account_id={}", aid)
            return True

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
