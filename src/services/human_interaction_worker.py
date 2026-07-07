"""

Worker một luồng: kiểm tra proxy → browser cô lập → đăng nhập → tương tác → giải phóng.



Tuần tự 4 bước theo thiết kế Giai đoạn 3 — proxy/profile/login đồng bộ với tab Tài khoản / job đăng bài.

"""



from __future__ import annotations

import os
import time
from collections.abc import Callable



from typing import Any

from loguru import logger

from playwright.sync_api import Page



from src.automation.browser_factory import (
    BrowserFactory,
    is_playwright_target_closed_error,
    prepare_playwright_sync_thread,
    sync_close_persistent_context,
)

from src.models.mapped_account import MappedAccount

from src.services.email_otp_imap import fetch_facebook_email_otp

from src.services.human_interaction_profile import HumanInteractionProfile, resolve_profile

from src.services.facebook_session_recovery import (
    manual_captcha_notifier,
    try_recover_facebook_session,
)

from src.services.human_interaction_modules import run_shuffled_interaction_modules

from src.utils.account_proxy_mapper import (
    apply_mapped_secrets_to_vault,
    ensure_mapped_proxy_live,
    prepare_mapped_account_for_browser_run,
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





def _format_worker_error(exc: BaseException) -> str:
    """Thông báo lỗi ngắn gọn cho GUI — tránh dump Playwright Call log dài."""
    msg = str(exc or "").strip()
    low = msg.lower()
    if "timeout" in low and ("goto" in low or "navigation" in low):
        return "Facebook tải chậm (timeout) — kiểm tra proxy/mạng, bấm «Chạy lại»"
    if "timeout" in low and "launch" in low:
        return "Mở Firefox chậm — giảm số luồng hoặc thử lại"
    if "target closed" in low or "has been closed" in low:
        return "Trình duyệt đóng bất ngờ"
    if "ns_error" in low or "net::" in low:
        return f"Lỗi mạng/proxy: {msg[:140]}"
    return msg[:200]


def run_human_interaction_worker(

    mapped: MappedAccount,

    *,

    grid_slot: GridWindowSlot | None = None,

    headless: bool = False,

    profile: HumanInteractionProfile | None = None,

    on_status: StatusCallback | None = None,

    login_only: bool = False,
    should_stop: Callable[[], bool] | None = None,
    is_user_cancelled: Callable[[], bool] | None = None,
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

    worker_max = float(
        os.environ.get("FB_HUMAN_WORKER_MAX_SEC", str(getattr(cfg, "max_worker_sec", 300.0) or 300.0))
    )
    worker_deadline = time.monotonic() + max(120.0, worker_max)



    def _status(st: str, detail: str = "") -> None:

        mapped.status = st

        mapped.status_detail = detail

        if on_status:

            on_status(st, detail)

    def _user_stopped() -> bool:
        return bool(is_user_cancelled and is_user_cancelled())

    def _pool_abort() -> bool:
        return bool(should_stop and should_stop()) and not _user_stopped()

    def _deadline_reached() -> bool:
        return time.monotonic() >= worker_deadline

    def _stopped() -> bool:
        return _user_stopped() or _pool_abort() or _deadline_reached()

    def _abort_detail() -> tuple[str, str]:
        """Trả (status, detail) khi cần dừng; rỗng nếu không dừng."""
        if _user_stopped():
            return "cancelled", "Đã hủy — người dùng bấm Dừng"
        if _pool_abort():
            return "pending", "Dừng giữa module — tự động chạy tiếp lượt sau"
        if _deadline_reached():
            return "pending", "Hết thời gian lượt — tự động chạy tiếp lượt sau"
        return "", ""

    def _finish_if_aborted(*, notify_now: bool = True, allow_deadline_continue: bool = False) -> str | None:
        st, detail = _abort_detail()
        if not st:
            return None
        if allow_deadline_continue and st == "pending" and _deadline_reached() and not _pool_abort():
            return None
        _status(st, detail)
        result = "cancelled" if st == "cancelled" else "interrupted"
        _finish(result, notify_now=notify_now)
        return result

    _pending_pool_result: list[str | None] = [None]

    def _notify_pool_done(result: str) -> None:
        if on_work_finished is None:
            return
        try:
            on_work_finished(result)
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Human] on_work_finished: {}", exc)

    def _finish(result: str, *, notify_now: bool = False) -> str:
        """``notify_now=True``: không mở browser — báo pool ngay; mặc định báo sau khi đóng browser."""
        if notify_now:
            _notify_pool_done(result)
        else:
            _pending_pool_result[0] = result
        return result

    if _finish_if_aborted(notify_now=True):
        return "cancelled" if _user_stopped() else "interrupted"

    apply_mapped_secrets_to_vault(mapped)



    _status("running", "Kiểm tra proxy")

    if _finish_if_aborted(notify_now=True):
        return "cancelled" if _user_stopped() else "interrupted"

    ok_px, px_msg = ensure_mapped_proxy_live(mapped)

    if not ok_px:
        logger.warning("[Human] Proxy lỗi account={}: {}", mapped.account_id, px_msg)
        _status("proxy_error", px_msg)
        return _finish("proxy_error", notify_now=True)

    if mapped.use_proxy:
        logger.info("[Human] Proxy đã LIVE (kiểm tra trước mở browser) account={}", mapped.account_id)

    grid_vp = None

    win_pos = None

    if grid_slot is not None:
        grid_vp = (grid_slot.width, grid_slot.height)
        win_pos = (grid_slot.x, grid_slot.y)
        stagger_ms = max(0, int(float(os.environ.get("FB_GRID_LAUNCH_STAGGER_MS", "800"))))
        if not skip_launch_stagger and grid_slot.index > 0 and stagger_ms > 0:
            delay_s = (grid_slot.index * stagger_ms) / 1000.0
            _status("waiting", f"Chờ mở cửa sổ ô {grid_slot.index + 1} ({delay_s:.1f}s)")
            end = time.monotonic() + delay_s
            while time.monotonic() < end:
                if _finish_if_aborted(notify_now=True):
                    return "cancelled" if _user_stopped() else "interrupted"
                time.sleep(min(0.4, max(0.05, end - time.monotonic())))

    if _finish_if_aborted(notify_now=True):
        return "cancelled" if _user_stopped() else "interrupted"

    _status("running", "Khởi tạo trình duyệt")

    factory: BrowserFactory | None = None
    context = None
    session_persisted = False
    acc: dict[str, Any] = {}
    cookie_path = ""

    try:
        prepare_playwright_sync_thread(label=f"human:{mapped.account_id}")
        acc = prepare_mapped_account_for_browser_run(mapped)
        cookie_path = str(mapped.cookie_path or acc.get("cookie_path") or "")
        factory = BrowserFactory(headless=headless, playwright_shared=True)

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

        from src.services.facebook_session_persist import (
            apply_saved_cookie_path_to_mapped,
            auto_save_session_for_account,
            cookie_file_has_session,
            establish_facebook_session,
            prepare_persistent_session_after_launch,
            resolve_best_cookie_path_for_account,
        )

        def _persist_session_immediately(*, log_label: str, require_confirm: bool = True) -> bool:
            """Lưu cookie ngay khi vào được Facebook — dừng giữa chừng vẫn không cần login lại."""
            nonlocal cookie_path
            try:
                saved, ck = auto_save_session_for_account(
                    page,
                    acc,
                    cookie_path=cookie_path,
                    mapped=mapped,
                    log_label=log_label,
                    require_confirm=require_confirm,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[Human] Lưu cookie ngay ({}) account={}: {}",
                    log_label,
                    mapped.account_id,
                    exc,
                )
                return False
            if saved:
                cookie_path = str(ck or cookie_path or "")
                mapped.cookie_path = cookie_path
                acc["cookie_path"] = cookie_path
                logger.info(
                    "[Human] Đã lưu cookie ngay account={} ({}) → {}",
                    mapped.account_id,
                    log_label,
                    cookie_path,
                )
                _status("running", f"Đã lưu cookie · {cookie_path[-42:]}")
            return bool(saved)

        mapped.storage.profile_path = str(acc.get("portable_path") or acc.get("profile_path") or mapped.storage.profile_path or "")
        cookie_path = resolve_best_cookie_path_for_account(
            acc,
            facebook_uid=str(mapped.auth.username or acc.get("facebook_uid") or ""),
            extra_candidates=[mapped.cookie_path] if mapped.cookie_path else None,
        )
        mapped.cookie_path = str(cookie_path or mapped.cookie_path or "")
        acc["cookie_path"] = mapped.cookie_path
        logger.info(
            "[Human] Browser account={} profile={} cookie={}",
            mapped.account_id,
            acc.get("portable_path") or "",
            mapped.cookie_path,
        )

        _status("running", "Khôi phục phiên profile/cookie đã lưu…")
        ok_prep, prep_mode = prepare_persistent_session_after_launch(
            context,
            page,
            acc,
            cookie_path=cookie_path,
        )
        if ok_prep:
            _persist_session_immediately(log_label=f"prep_{prep_mode}", require_confirm=False)

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

        if mapped.use_proxy:
            _status("running", "Kiểm tra proxy qua trình duyệt (Facebook)")
            ok_bf, px_bf = verify_browser_facebook_via_proxy(page, timeout_ms=30_000)
            if not ok_bf:
                logger.warning(
                    "[Human] Proxy qua browser FAIL account={}: {}",
                    mapped.account_id,
                    px_bf,
                )
                _status("proxy_error", px_bf[:220])
                return _finish("proxy_error")
            logger.info("[Human] {} account={}", px_bf, mapped.account_id)

        session_label = (
            "Kiểm tra phiên Facebook (profile → cookie → form nếu cần)"
            if not login_only
            else "Mở facebook.com — profile → cookie → form nếu cần"
        )
        _status("running", session_label)

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

        has_cookie_file = cookie_file_has_session(cookie_path)
        has_password = bool(str(getattr(mapped.auth, "password", "") or "").strip())
        # Profile/cookie thất bại + có pass → luôn thử form (không phụ thuộc soft_login hay file cookie)
        allow_form_login = bool(
            login_only or (has_password and not ok_prep and not cookie_file_has_session(cookie_path))
        )

        def _form_login_recover() -> bool:
            """Form / 2FA / captcha — sau khi profile + cookie file đã thử."""
            if _stopped():
                return False
            _status("running", "Đăng nhập form Facebook (profile/cookie chưa đủ phiên)…")
            from src.services.facebook_session_recovery import (
                _page_on_meta_auth_or_captcha_flow,
                continue_facebook_auth_flow,
                facebook_auth_flow_was_active,
                facebook_page_looks_like_totp_prompt,
            )

            try:
                with manual_captcha_notifier(_notify_manual_captcha):
                    if try_recover_facebook_session(
                        page,
                        acc,
                        cookie_path=cookie_path,
                        should_stop=_stopped,
                        force_fresh_login=False,
                        allow_form_login=True,
                    ):
                        return True
                    if (
                        _page_on_meta_auth_or_captcha_flow(page)
                        or facebook_page_looks_like_totp_prompt(page)
                        or facebook_auth_flow_was_active(acc)
                    ):
                        if continue_facebook_auth_flow(
                            page, acc, cookie_path=cookie_path, should_stop=_stopped
                        ):
                            return True
                    _try_email_otp_checkpoint(page, mapped)
                    return bool(
                        try_recover_facebook_session(
                            page,
                            acc,
                            cookie_path=cookie_path,
                            should_stop=_stopped,
                            force_fresh_login=False,
                            allow_form_login=True,
                        )
                    )
            except RuntimeError as rec_exc:
                logger.warning("[Human] Form login account={}: {}", mapped.account_id, rec_exc)
                return False

        if ok_prep:
            ok_sess, sess_detail = True, prep_mode or "profile"
            logger.info(
                "[Human] Bỏ qua establish — đã khôi phục phiên account={} mode={}",
                mapped.account_id,
                prep_mode,
            )
        else:
            from src.services.facebook_session_recovery import reload_facebook_page_f5

            establish_retries = max(1, int(os.environ.get("FB_SESSION_ESTABLISH_F5_RETRIES", "2")))
            ok_sess = False
            sess_detail = ""
            for est_i in range(establish_retries):
                if est_i > 0:
                    _status(
                        "running",
                        f"F5 — thử lại xác nhận phiên ({est_i + 1}/{establish_retries})",
                    )
                    reload_facebook_page_f5(page, label=f"human_establish_{est_i}")
                    if aborted := _finish_if_aborted(notify_now=False):
                        return aborted
                ok_sess, sess_detail = establish_facebook_session(
                    page,
                    acc,
                    cookie_path=cookie_path,
                    allow_form_login=allow_form_login,
                    form_recover_fn=_form_login_recover if allow_form_login else None,
                )
                if ok_sess:
                    break

        if not ok_sess:
            if aborted := _finish_if_aborted(notify_now=False):
                return aborted
            hint = (
                " — thử «Lưu cookie» hoặc bổ sung pass để đăng nhập form"
                if has_cookie_file and not has_password
                else ""
            )
            _status(
                "login_failed",
                (sess_detail or "Không đăng nhập được")[:220] + hint[:80],
            )
            return _finish("login_failed")

        recovered = True
        session_persisted = True
        _persist_session_immediately(log_label="after_establish", require_confirm=False)
        ck_sync = apply_saved_cookie_path_to_mapped(mapped, acc)
        cookie_path = str(ck_sync or acc.get("cookie_path") or mapped.cookie_path or cookie_path or "")
        mapped.cookie_path = cookie_path
        _status("running", f"Đã vào Facebook — {sess_detail[:80]}")
        logger.info(
            "[Human] Phiên OK account={}: {}",
            mapped.account_id,
            sess_detail,
        )

        if login_only:
            if not cookie_file_has_session(cookie_path):
                saved_lo, ck_lo = auto_save_session_for_account(
                    page,
                    acc,
                    cookie_path=cookie_path,
                    mapped=mapped,
                    log_label="login_only",
                    require_confirm=True,
                )
                if saved_lo:
                    cookie_path = str(ck_lo or cookie_path)
                    mapped.cookie_path = cookie_path
            if not cookie_file_has_session(cookie_path):
                _status("login_failed", "Không lưu được cookie phiên — thử đăng nhập lại")
                return _finish("login_failed")
            cookie_note = f" | Cookie: {cookie_path}"
            _status("login_ok", (sess_detail + cookie_note)[:220])
            return _finish("login_ok")

        _status(
            "running",
            f"Đã xác nhận đăng nhập — bắt đầu tương tác · {sess_detail[:60]}",
        )

        _status("running", "Tương tác giống người dùng")

        if cfg.virtual_cursor:
            try:
                from src.utils.human_action import install_virtual_cursor_on_context

                install_virtual_cursor_on_context(context)
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human] Con trỏ ảo context: {}", exc)

        run_shuffled_interaction_modules(
            page,
            profile=cfg,
            on_status=on_status,
            should_stop=_stopped,
        )

        if _deadline_reached() and not _user_stopped():
            logger.info(
                "[Human] Đạt giới hạn thời gian lượt (~{:.0f}s) account={} — lưu phiên và kết thúc",
                worker_max,
                mapped.account_id,
            )

        aborted = _finish_if_aborted(notify_now=False, allow_deadline_continue=True)
        if aborted:
            return aborted

        _status("running", "Lưu cookie phiên sau tương tác")

        time.sleep(max(0.5, min(1.5, float(cfg.sync_wait_sec))))

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
            session_persisted = True
            apply_saved_cookie_path_to_mapped(mapped, acc)
            mapped.status = "login_ok"
            prof_note = str(mapped.storage.profile_path or "")[-42:]
            _status(
                "success",
                f"Hoàn thành — cookie + profile · …{prof_note}",
            )
        else:
            apply_saved_cookie_path_to_mapped(mapped, acc)
            if cookie_file_has_session(mapped.cookie_path):
                mapped.status = "login_ok"
            _status("success", "Hoàn thành tương tác (chưa lưu được cookie — kiểm tra phiên)")

        return _finish("success")

    except Exception as exc:  # noqa: BLE001
        if _stopped():
            if aborted := _finish_if_aborted(notify_now=False):
                logger.info("[Human] Worker dừng account={} ({})", mapped.account_id, aborted)
                return aborted
        if is_playwright_target_closed_error(exc):
            logger.warning(
                "[Human] Trình duyệt đóng bất ngờ account={}: {}",
                mapped.account_id,
                str(exc)[:160],
            )
            _status(
                "error",
                "Trình duyệt đóng bất ngờ — pool sẽ đóng sạch và thử lại lượt",
            )
            return _finish("browser_closed")
        logger.exception("[Human] Worker lỗi account={}: {}", mapped.account_id, exc)
        _status("error", _format_worker_error(exc))
        return _finish("error")

    finally:
        ctx = context
        fac = factory
        acc_id = str(mapped.account_id or "")
        ck_path = cookie_path
        acc_ref = acc
        already_saved = session_persisted

        # Playwright Sync API: mở/đóng bắt buộc trên cùng worker thread (không spawn thread phụ).
        if ctx is not None:
            try:
                from src.services.facebook_session_persist import (
                    apply_saved_cookie_path_to_mapped,
                    sync_firefox_profile_before_close,
                )

                pg = ctx.pages[0] if ctx.pages else None
                if pg is not None and not pg.is_closed():
                    if not already_saved:
                        sync_firefox_profile_before_close(
                            ctx,
                            pg,
                            acc_ref,
                            cookie_path=ck_path,
                            mapped=mapped,
                            log_label="on_close",
                        )
                    else:
                        sync_firefox_profile_before_close(
                            ctx,
                            pg,
                            acc_ref,
                            cookie_path=ck_path,
                            mapped=mapped,
                            log_label="after_success",
                        )
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human] Lưu cookie khi đóng: {}", exc)
            try:
                apply_saved_cookie_path_to_mapped(mapped, acc_ref)
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human] Sync mapped sau đóng: {}", exc)
            try:
                from src.utils.account_proxy_mapper import persist_mapped_storage_to_registry

                persist_mapped_storage_to_registry(mapped, acc_ref)
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Human] Lưu profile registry {}: {}", acc_id, exc)
            try:
                sync_close_persistent_context(
                    ctx,
                    log_label=acc_id,
                    same_thread=True,
                    force_kill_firefox=False,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("[Human] Đóng context: {}", exc)
        if fac is not None:
            try:
                fac.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning("[Human] Đóng factory: {}", exc)
        pending = _pending_pool_result[0]
        if pending is not None:
            _notify_pool_done(pending)
            _pending_pool_result[0] = None

