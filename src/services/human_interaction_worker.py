"""

Worker một luồng: kiểm tra proxy → browser cô lập → đăng nhập → tương tác → giải phóng.



Tuần tự 4 bước theo thiết kế Giai đoạn 3 — proxy/profile/login đồng bộ với tab Tài khoản / job đăng bài.

"""



from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable



from loguru import logger

from playwright.sync_api import Page



from src.automation.browser_factory import BrowserFactory, sync_close_persistent_context

from src.models.mapped_account import MappedAccount

from src.services.email_otp_imap import fetch_facebook_email_otp

from src.services.human_interaction_profile import HumanInteractionProfile, resolve_profile

from src.services.facebook_session_recovery import (
    manual_captcha_notifier,
    try_recover_facebook_session,
)

from src.services.human_interaction_modules import run_shuffled_interaction_modules

from src.utils.account_browser_profile import ensure_account_browser_profile_ready
from src.utils.account_proxy_mapper import (
    apply_mapped_secrets_to_vault,
    ensure_mapped_proxy_live,
    sync_mapped_account_storage_from_registry,
)
from src.utils.proxy_check import verify_browser_facebook_via_proxy
from src.utils.grid_layout_manager import GridWindowSlot



StatusCallback = Callable[[str, str], None]





def _try_email_otp_checkpoint(page: Page, mapped: MappedAccount) -> bool:

    """Nếu Facebook yêu cầu mã email — lấy qua IMAP."""

    auth = mapped.auth

    if not auth.email or not auth.email_password:

        return False

    body = (page.content() or "").lower()

    if not any(

        k in body

        for k in (

            "enter the code",

            "nhập mã",

            "confirmation code",

            "mã xác nhận",

            "check your email",

        )

    ):

        return False

    otp = fetch_facebook_email_otp(auth.email, auth.email_password)

    if not otp:

        return False

    for sel in ('input[name="code"]', 'input[type="text"]', 'input[inputmode="numeric"]'):

        loc = page.locator(sel).first

        try:

            if loc.is_visible(timeout=3000):
                from src.utils.human_typing import human_type_locator

                human_type_locator(loc, otp, submit_enter=True, clear_first=True, label="email OTP")
                page.wait_for_timeout(3000)

                logger.info("[Human] Đã điền OTP email qua IMAP")

                return True

        except Exception:

            continue

    return False





def run_human_interaction_worker(

    mapped: MappedAccount,

    *,

    grid_slot: GridWindowSlot | None = None,

    headless: bool = False,

    profile: HumanInteractionProfile | None = None,

    on_status: StatusCallback | None = None,

    login_only: bool = False,
    should_stop: Callable[[], bool] | None = None,
    on_work_finished: Callable[[str], None] | None = None,
    skip_launch_stagger: bool = False,

) -> str:

    cfg = profile or resolve_profile("normal")

    """

    Chạy pipeline cho một MappedAccount.

    ``login_only=True``: chỉ kiểm tra proxy, mở profile và đăng nhập Facebook (lưu cookie).

    Returns:

        Trạng thái cuối: ``success`` | ``login_ok`` | ``proxy_error`` | ``login_failed`` | ``error``.

    """



    def _status(st: str, detail: str = "") -> None:

        mapped.status = st

        mapped.status_detail = detail

        if on_status:

            on_status(st, detail)

    def _stopped() -> bool:
        return bool(should_stop and should_stop())

    def _finish(result: str) -> str:
        """Báo pool trước khi đóng browser — giải phóng slot cho TK tiếp theo."""
        if on_work_finished is not None:
            try:
                on_work_finished(result)
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human] on_work_finished: {}", exc)
        return result

    if _stopped():
        _status("cancelled", "Đã hủy — người dùng bấm Dừng")
        return _finish("cancelled")

    apply_mapped_secrets_to_vault(mapped)



    _status("running", "Kiểm tra proxy")

    if _stopped():
        _status("cancelled", "Đã hủy — người dùng bấm Dừng")
        return _finish("cancelled")

    ok_px, px_msg = ensure_mapped_proxy_live(mapped)

    if not ok_px:
        logger.warning("[Human] Proxy lỗi account={}: {}", mapped.account_id, px_msg)
        _status("proxy_error", px_msg)
        return _finish("proxy_error")

    if mapped.use_proxy:
        logger.info("[Human] Proxy đã LIVE (kiểm tra trước mở browser) account={}", mapped.account_id)



    acc = sync_mapped_account_storage_from_registry(mapped)
    ensure_account_browser_profile_ready(acc)



    grid_vp = None

    win_pos = None

    if grid_slot is not None:
        grid_vp = (grid_slot.width, grid_slot.height)
        win_pos = (grid_slot.x, grid_slot.y)
        stagger_ms = max(0, int(float(os.environ.get("FB_GRID_LAUNCH_STAGGER_MS", "1200"))))
        if not skip_launch_stagger and grid_slot.index > 0 and stagger_ms > 0:
            delay_s = (grid_slot.index * stagger_ms) / 1000.0
            _status("waiting", f"Chờ mở cửa sổ ô {grid_slot.index + 1} ({delay_s:.1f}s)")
            end = time.monotonic() + delay_s
            while time.monotonic() < end:
                if _stopped():
                    _status("cancelled", "Đã hủy — người dùng bấm Dừng")
                    return _finish("cancelled")
                time.sleep(min(0.4, max(0.05, end - time.monotonic())))

    if _stopped():
        _status("cancelled", "Đã hủy — người dùng bấm Dừng")
        return _finish("cancelled")

    _status("running", "Khởi tạo trình duyệt")

    factory: BrowserFactory | None = None

    context = None
    cookie_path = str(mapped.cookie_path or acc.get("cookie_path") or "")

    try:

        factory = BrowserFactory(headless=headless)

        context = factory.launch_persistent_context_from_account_dict(

            acc,

            headless=headless,

            grid_viewport=grid_vp,

            window_position=win_pos,

            disable_notifications=True,

            # Đăng nhập + tương tác đều dùng www/desktop — tránh m.facebook (ô lưới hẹp).
            force_desktop_facebook=True,

        )

        page: Page = context.pages[0] if context.pages else context.new_page()
        os.environ.pop("TOOLFB_NAV_MOBILE_FB", None)
        if grid_slot is not None:
            prof = str(acc.get("portable_path") or acc.get("profile_path") or "").strip()
            if prof:
                try:
                    from src.utils.win_browser_window import reposition_browser_to_grid_slot

                    if reposition_browser_to_grid_slot(prof, grid_slot, timeout_s=20.0):
                        logger.info(
                            "[Human] Đã xếp cửa sổ ô {} @ ({}, {}) {}×{}",
                            grid_slot.index + 1,
                            grid_slot.x,
                            grid_slot.y,
                            grid_slot.width,
                            grid_slot.height,
                        )
                    else:
                        logger.warning(
                            "[Human] Chưa đặt được lưới cửa sổ ô {} — profile={}",
                            grid_slot.index + 1,
                            prof[-40:],
                        )
                except Exception as place_exc:  # noqa: BLE001
                    logger.warning("[Human] Lỗi xếp lưới cửa sổ: {}", place_exc)

        mapped.storage.profile_path = str(acc.get("portable_path") or acc.get("profile_path") or mapped.storage.profile_path or "")
        mapped.cookie_path = str(acc.get("cookie_path") or mapped.cookie_path or "")
        logger.info(
            "[Human] Browser account={} profile={} cookie={}",
            mapped.account_id,
            acc.get("portable_path"),
            acc.get("cookie_path"),
        )

        from src.services.facebook_session_persist import (
            auto_save_session_for_account,
            bootstrap_cookies_into_context,
            cookie_file_has_session,
            resolve_best_cookie_path_for_account,
            restore_facebook_session,
        )

        cookie_path = resolve_best_cookie_path_for_account(
            acc,
            facebook_uid=str(mapped.auth.username or ""),
            extra_candidates=[mapped.cookie_path] if mapped.cookie_path else None,
        )
        mapped.cookie_path = str(cookie_path or mapped.cookie_path or "")

        if cookie_file_has_session(cookie_path):
            _status("running", "Nạp cookie phiên đã lưu (profile-first)")
            if bootstrap_cookies_into_context(context, cookie_path):
                logger.info(
                    "[Human] Bootstrap cookie OK account={} file={}",
                    mapped.account_id,
                    cookie_path,
                )

        if mapped.use_proxy:
            _status("running", "Kiểm tra proxy qua trình duyệt (Facebook)")
            ok_bf, px_bf = verify_browser_facebook_via_proxy(page)
            if not ok_bf:
                logger.warning(
                    "[Human] Proxy qua browser FAIL account={}: {}",
                    mapped.account_id,
                    px_bf,
                )
                _status("proxy_error", px_bf[:220])
                return "proxy_error"
            logger.info("[Human] {} account={}", px_bf, mapped.account_id)

        session_label = (
            "Kiểm tra phiên — tái sử dụng (tab Tương tác, không form login)"
            if not login_only
            else "Mở facebook.com — kiểm tra phiên profile (accounts.json)"
        )
        _status("running", session_label)
        ok_session, session_mode = restore_facebook_session(page, acc, cookie_path=cookie_path)
        recovered = False
        if ok_session:
            _status("running", f"Đã vào Facebook ({session_mode}) — tái sử dụng phiên")
            recovered = True
            # Tab tương tác: cookie lưu ở cổng ensure_session_before_interaction (tránh ghi 2 lần).
            if login_only:
                saved0, ck0 = auto_save_session_for_account(
                    page,
                    acc,
                    cookie_path=cookie_path,
                    mapped=mapped,
                    log_label=f"session_{session_mode}",
                    require_confirm=True,
                )
                if saved0:
                    cookie_path = ck0 or cookie_path
                    mapped.cookie_path = str(cookie_path)
        elif not login_only:
            logger.warning(
                "[Human] Tab Tương tác — chưa có phiên profile/cookie account={} ({})",
                mapped.account_id,
                session_mode,
            )
        elif cookie_file_has_session(cookie_path):
            logger.info("[Human] Profile chưa phiên — đã thử cookie file account={}", mapped.account_id)
        else:
            logger.info(
                "[Human] Chưa có phiên profile/cookie — sẽ recovery account={} profile={}",
                mapped.account_id,
                acc.get("portable_path"),
            )

        _status("running", "Phiên OK" if recovered else ("Đăng nhập Facebook" if login_only else "Chờ phiên"))

        try:
            from src.utils.capsolver_config import (
                capsolver_auto_solve_enabled,
                capsolver_skip_meta_enterprise,
                capsolver_use_account_proxy_setting,
            )
            from src.utils.twocaptcha_config import (
                captcha_prefer_twocaptcha_first,
                captcha_tier_timeout_sec,
                twocaptcha_configured,
            )

            if captcha_prefer_twocaptcha_first() and twocaptcha_configured():
                logger.info(
                    "[Human] Captcha: ưu tiên 2Captcha trước (proxy → ProxyLess) | timeout/tầng={:.0f}s",
                    captcha_tier_timeout_sec(),
                )
            px = capsolver_use_account_proxy_setting()
            if twocaptcha_configured() and capsolver_auto_solve_enabled():
                logger.info("[Human] CapSolver dự phòng tầng 3 nếu 2Captcha thất bại.")
            elif px is False:
                logger.info(
                    "[Human] CapSolver ProxyLess (IP máy) — capsolver_use_account_proxy=false"
                )
            if not capsolver_auto_solve_enabled() and not twocaptcha_configured():
                logger.info("[Human] Không có API CapSolver/2Captcha — chỉ captcha thủ công 180s.")
            elif capsolver_skip_meta_enterprise():
                logger.info(
                    "[Human] Bỏ tầng 1 CapSolver (skip Meta Enterprise) — vẫn thử 2Captcha nếu có key."
                )
        except Exception:
            pass

        def _notify_manual_captcha(msg: str) -> None:
            _status("running", msg[:220])

        def _run_full_login_recovery() -> bool:
            """Đăng nhập lại đầy đủ (form / 2FA / captcha) — chỉ khi profile chưa có phiên."""
            nonlocal recovered
            if _stopped():
                return False
            # Tab Human: luôn tái sử dụng cookie/profile — không xóa phiên (tránh force_fresh).
            force_fresh = False
            if cookie_file_has_session(cookie_path):
                logger.info(
                    "[Human] Recovery reuse cookie/profile account={} file={}",
                    mapped.account_id,
                    cookie_path,
                )
                if bootstrap_cookies_into_context(context, cookie_path):
                    ok_retry, mode_retry = restore_facebook_session(
                        page, acc, cookie_path=cookie_path
                    )
                    if ok_retry:
                        recovered = True
                        logger.info(
                            "[Human] Vào Facebook qua cookie sau bootstrap account={} ({})",
                            mapped.account_id,
                            mode_retry,
                        )
                        return True
            try:
                from src.automation.facebook_actions import prime_facebook_session_page

                if "facebook.com/login" in (page.url or "").lower():
                    prime_facebook_session_page(page)
            except Exception:
                pass
            try:
                with manual_captcha_notifier(_notify_manual_captcha):
                    recovered = try_recover_facebook_session(
                        page,
                        acc,
                        cookie_path=cookie_path,
                        should_stop=should_stop,
                        force_fresh_login=force_fresh,
                    )
            except RuntimeError as rec_exc:
                msg = str(rec_exc).replace(" need_manual_check", "").strip()
                logger.warning("[Human] Recovery account={}: {}", mapped.account_id, msg)
                return False
            if recovered:
                return True
            if _stopped():
                return False
            from src.services.facebook_session_recovery import (
                _page_on_meta_auth_or_captcha_flow,
                continue_facebook_auth_flow,
                facebook_auth_flow_was_active,
                facebook_page_looks_like_totp_prompt,
            )

            if (
                _page_on_meta_auth_or_captcha_flow(page)
                or facebook_page_looks_like_totp_prompt(page)
                or facebook_auth_flow_was_active(acc)
            ):
                logger.info(
                    "[Human] Tiếp tục luồng 2FA/captcha account={}",
                    mapped.account_id,
                )
                try:
                    with manual_captcha_notifier(_notify_manual_captcha):
                        recovered = continue_facebook_auth_flow(
                            page, acc, cookie_path=cookie_path, should_stop=should_stop
                        )
                except RuntimeError:
                    return False
                return bool(recovered)
            _try_email_otp_checkpoint(page, mapped)
            try:
                with manual_captcha_notifier(_notify_manual_captcha):
                    recovered = try_recover_facebook_session(
                        page,
                        acc,
                        cookie_path=cookie_path,
                        should_stop=should_stop,
                        force_fresh_login=False,
                    )
            except RuntimeError:
                return False
            return bool(recovered)

        if login_only and not recovered:
            if not _run_full_login_recovery():
                if _stopped():
                    _status("cancelled", "Đã hủy — người dùng bấm Dừng")
                    return _finish("cancelled")
                _status("login_failed", "Không đăng nhập được / chưa vào tài khoản")
                return _finish("login_failed")
        elif not login_only and not recovered:
            if _stopped():
                _status("cancelled", "Đã hủy — người dùng bấm Dừng")
                return _finish("cancelled")
            if mapped.soft_login_if_needed:
                if not mapped.auth.password:
                    _status(
                        "login_failed",
                        "Chưa có phiên và thiếu mật khẩu — bổ sung pass trong dòng import",
                    )
                    return _finish("login_failed")
                _status(
                    "running",
                    "Chưa vào được Facebook — thử đăng nhập nhẹ (giữ cookie, không xóa phiên)",
                )
                logger.info(
                    "[Human] Soft login account={} — chỉ khi profile/cookie không đủ phiên",
                    mapped.account_id,
                )
                if not _run_full_login_recovery():
                    if _stopped():
                        _status("cancelled", "Đã hủy — người dùng bấm Dừng")
                        return _finish("cancelled")
                    _status("login_failed", "Không đăng nhập được sau khi thử nạp cookie/profile")
                    return _finish("login_failed")
                saved_sl, ck_sl = auto_save_session_for_account(
                    page,
                    acc,
                    cookie_path=cookie_path,
                    mapped=mapped,
                    log_label="soft_login",
                    require_confirm=True,
                )
                if saved_sl:
                    cookie_path = ck_sl or cookie_path
                    mapped.cookie_path = str(cookie_path)
                    _status("running", f"Đăng nhập OK — đã lưu cookie · {cookie_path[-40:]}")
            else:
                _status(
                    "login_failed",
                    "Chưa có phiên đăng nhập — chạy tab «Đăng nhập» hoặc kiểm tra profile/cookie trong accounts.json",
                )
                return _finish("login_failed")

        if login_only:
            from src.automation.facebook_actions import _force_www_facebook_if_mobile_redirect
            from src.services.facebook_session_recovery import confirm_facebook_session_logged_in

            _force_www_facebook_if_mobile_redirect(page)
            ok_confirm, confirm_msg = confirm_facebook_session_logged_in(page, acc)
            if not ok_confirm:
                _status(
                    "login_failed",
                    (confirm_msg or "Chưa xác nhận vào tài khoản Facebook (www)")[:220],
                )
                return _finish("login_failed")
            saved_lo, ck_lo = auto_save_session_for_account(
                page,
                acc,
                cookie_path=cookie_path,
                mapped=mapped,
                log_label="login_only",
                require_confirm=True,
            )
            if not saved_lo or not cookie_file_has_session(ck_lo):
                _status("login_failed", "Không lưu được cookie phiên — thử đăng nhập lại")
                return _finish("login_failed")
            cookie_path = ck_lo
            mapped.cookie_path = str(cookie_path)
            cookie_note = f" | Cookie: {cookie_path}"
            _status("login_ok", (confirm_msg + cookie_note)[:220])
            return _finish("login_ok")

        from src.services.facebook_session_persist import ensure_session_before_interaction

        _status("running", "Xác nhận phiên (không đăng nhập lại)")
        ok_gate, gate_detail = ensure_session_before_interaction(
            page,
            acc,
            cookie_path=cookie_path,
            recover_fn=None,
        )
        if not ok_gate:
            if _stopped():
                _status("cancelled", "Đã hủy — người dùng bấm Dừng")
                return _finish("cancelled")
            _status(
                "login_failed",
                (gate_detail or "Phiên hết hạn — đăng nhập lại ở tab «Đăng nhập»")[:220],
            )
            return _finish("login_failed")

        mapped.cookie_path = str(acc.get("cookie_path") or mapped.cookie_path or cookie_path or "")
        cookie_path = str(mapped.cookie_path or cookie_path or "")
        _status(
            "running",
            f"Đã xác nhận đăng nhập — bắt đầu tương tác · {gate_detail[:60]}",
        )

        _status("running", "Tương tác giống người dùng")

        if cfg.virtual_cursor:
            try:
                from src.utils.human_action import install_virtual_cursor_on_context

                install_virtual_cursor_on_context(context)
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human] Con trỏ ảo context: {}", exc)

        run_shuffled_interaction_modules(page, profile=cfg, on_status=on_status)

        _status("running", "Lưu cookie phiên sau tương tác")

        time.sleep(max(0.8, min(4.0, float(cfg.sync_wait_sec))))

        saved_done, ck_done = auto_save_session_for_account(
            page,
            acc,
            cookie_path=cookie_path,
            mapped=mapped,
            log_label="after_interaction",
            require_confirm=True,
        )
        if saved_done:
            mapped.cookie_path = str(ck_done or mapped.cookie_path)
            _status(
                "success",
                f"Hoàn thành — đã cập nhật cookie · {mapped.cookie_path[-48:]}",
            )
        else:
            _status("success", "Hoàn thành tương tác (chưa lưu được cookie — kiểm tra phiên)")

        return _finish("success")

    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if _stopped() or "has been closed" in msg or type(exc).__name__ == "TargetClosedError":
            logger.info("[Human] Worker dừng/đóng browser account={}: {}", mapped.account_id, msg[:120])
            _status("cancelled", "Đã hủy — người dùng bấm Dừng hoặc đóng trình duyệt")
            return _finish("cancelled")
        logger.exception("[Human] Worker lỗi account={}: {}", mapped.account_id, exc)
        _status("error", msg[:200])
        return _finish("error")

    finally:
        ctx = context
        fac = factory
        acc_id = str(mapped.account_id or "")
        ck_path = cookie_path
        acc_ref = acc

        def _teardown_browser() -> None:
            if ctx is not None:
                try:
                    from src.services.facebook_session_persist import auto_save_session_for_account

                    pg = ctx.pages[0] if ctx.pages else None
                    if pg is not None and not pg.is_closed():
                        saved_fin, ck_fin = auto_save_session_for_account(
                            pg,
                            acc_ref,
                            cookie_path=ck_path,
                            mapped=mapped,
                            log_label="on_close",
                            require_confirm=False,
                        )
                        if saved_fin:
                            logger.debug(
                                "[Human] Lưu cookie khi đóng account={} → {}",
                                acc_id,
                                ck_fin,
                            )
                except Exception as exc:  # noqa: BLE001
                    logger.debug("[Human] Lưu cookie khi đóng: {}", exc)
                try:
                    sync_close_persistent_context(ctx, log_label=acc_id, timeout_sec=22.0)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("[Human] Đóng context: {}", exc)
            if fac is not None:
                try:
                    fac.close()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("[Human] Đóng factory: {}", exc)

        if ctx is not None or fac is not None:
            threading.Thread(
                target=_teardown_browser,
                daemon=True,
                name=f"human-teardown-{acc_id[:16]}",
            ).start()


